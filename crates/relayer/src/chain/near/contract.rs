use crate::chain::near::rpc::client::NearRpcClient;
use crate::chain::near::CONTRACT_ACCOUNT_ID;
use crate::chain::requests::{QueryChannelsRequest, QueryPacketAcknowledgementsRequest};
use crate::chain::requests::{
    QueryClientConnectionsRequest, QueryClientStatesRequest, QueryConnectionsRequest,
    QueryPacketCommitmentsRequest, QueryUnreceivedAcksRequest,
};
use crate::client_state::{AnyClientState, IdentifiedAnyClientState};
use crate::consensus_state::AnyConsensusState;
use alloc::sync::Arc;
use humantime_serde::re;
use ibc_proto::google::protobuf::Any;
use ibc_proto::ibc::core::channel::v1::PacketState;
use ibc_relayer_types::core::ics02_client::height::Height;
use ibc_relayer_types::core::ics03_connection::connection::{
    ConnectionEnd, IdentifiedConnectionEnd,
};
use ibc_relayer_types::core::ics04_channel::channel::{ChannelEnd, IdentifiedChannelEnd};
use ibc_relayer_types::core::ics04_channel::packet::Sequence;
use ibc_relayer_types::core::ics23_commitment::commitment::CommitmentPrefix;
use ibc_relayer_types::core::ics24_host::identifier::{ChannelId, ClientId, ConnectionId, PortId};
use near_account_id::AccountId;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::views::FinalExecutionOutcomeView;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime as TokioRuntime;
use tracing::{debug, info};

pub trait NearIbcContract {
    fn get_contract_id(&self) -> AccountId;
    fn get_client(&self) -> &NearRpcClient;
    fn get_rt(&self) -> &Arc<TokioRuntime>;

    fn get_latest_height(&self) -> anyhow::Result<Height> {
        info!("NearIbcContract: [get_latest_height]");

        let height: Height = self
            .get_rt()
            .block_on(self.get_client().view(
                self.get_contract_id().clone(),
                "get_latest_height".to_string(),
                json!({}).to_string().into_bytes(),
            ))
            .expect("Failed to get latest height")
            .json()?;

        // As we use solomachine client, we set the revision number to 0
        Ok(Height::new(0, height.revision_height())?)
    }

    fn get_connection_end(
        &self,
        connection_identifier: &ConnectionId,
    ) -> anyhow::Result<ConnectionEnd> {
        info!(
            "NearIbcContract: [get_connection_end] - connection_identifier: {:?}",
            connection_identifier
        );

        let connection_id = connection_identifier.to_string();
        // thread::sleep(Duration::from_secs(5));

        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_connection_end".to_string(),
                    json!({ "connection_id": connection_id })
                        .to_string()
                        .into_bytes(),
                ),
            )
            .expect("Failed to get_connection_end")
            .json()

        // self.get_client().view(
        //     self.get_contract_id().clone(),
        //     "query_connection_end".to_string(),
        //     json!({"connection_id": connection_id}).to_string().into_bytes()
        // ).await.expect("Failed to query_connection_end").json()
    }

    /// get channelEnd  by port_identifier, and channel_identifier
    fn get_channel_end(
        &self,
        port_id: &PortId,
        channel_id: &ChannelId,
    ) -> anyhow::Result<ChannelEnd> {
        info!(
            "NearIbcContract: [query_channel_end] - port_id: {:?} channel_id: {:?}",
            port_id, channel_id
        );

        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_channel_end".to_string(),
                    json!({"port_id": port_id, "channel_id": channel_id})
                        .to_string()
                        .into_bytes(),
                ),
            )
            .expect("Failed to query_channel_end.")
            .json()
    }

    // TODO(bob) need add query height
    /// get client_state by client_id
    fn get_client_state(&self, client_id: &ClientId) -> anyhow::Result<Vec<u8>> {
        info!(
            "NearIbcContract: [get_client_state] - client_id: {:?}",
            client_id
        );
        // let client_id = client_id.to_string();

        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_client_state".to_string(),
                    json!({"client_id": client_id.clone()})
                        .to_string()
                        .into_bytes(),
                ),
            )
            .expect("Failed to query_client_state.")
            .json()
    }

    fn get_client_consensus_heights(&self, client_id: &ClientId) -> anyhow::Result<Vec<Height>> {
        info!(
            "NearIbcContract: [get_client_consensus_heights] - client_id: {:?}",
            client_id,
        );
        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_client_consensus_heights".to_string(),
                    json!({"client_id": client_id.clone()})
                        .to_string()
                        .into_bytes(),
                ),
            )
            .expect("Failed to get_client_consensus_heights.")
            .json()
    }

    /// Performs a query to retrieve the consensus state for a specified height
    /// `consensus_height` that the specified light client stores.
    fn get_client_consensus(
        &self,
        client_id: &ClientId,
        consensus_height: &Height,
    ) -> anyhow::Result<Vec<u8>> {
        info!(
            "NearIbcContract: [get_client_consensus] - client_id: {:?} consensus_height: {:?}",
            client_id, consensus_height
        );
        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_client_consensus".to_string(),
                    json!({"client_id": client_id.clone(), "consensus_height": consensus_height })
                        .to_string()
                        .into_bytes(),
                ),
            )
            .expect("Failed to get_client_consensus.")
            .json()
    }

    fn get_consensus_state_with_height(
        &self,
        client_id: &ClientId,
    ) -> anyhow::Result<Vec<(Height, AnyConsensusState)>> {
        info!(
            "NearIbcContract: [get_consensus_state_with_height] - client_id: {:?}",
            client_id
        );
        let client_id = serde_json::to_string(client_id).unwrap();

        self.get_rt()
            .block_on(self.get_client().view(
                self.get_contract_id().clone(),
                "get_consensus_state_with_height".to_string(),
                json!({ "client_id": client_id }).to_string().into_bytes(),
            ))
            .expect("Failed to get_consensus_state_with_height.")
            .json()
    }

    fn get_unreceipt_packet(
        &self,
        port_id: &PortId,
        channel_id: &ChannelId,
        sequences: &[Sequence],
    ) -> anyhow::Result<Vec<u64>> {
        info!(
            "NearIbcContract: [get_unreceipt_packet] - port_id: {:?} channel_id: {:?} sequence: {:?}",
            port_id, channel_id, sequences
        );
        let port_id = serde_json::to_string(port_id).unwrap();
        let channel_id = serde_json::to_string(channel_id).unwrap();
        let sequences = serde_json::to_string(sequences).unwrap();

        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_unreceipt_packet".to_string(),
                    json!({
                        "port_id": port_id,
                        "channel_id": channel_id,
                        "sequences": sequences
                    })
                    .to_string()
                    .into_bytes(),
                ),
            )
            .expect("Failed to get_unreceipt_packet.")
            .json()
    }

    fn get_clients(
        &self,
        request: QueryClientStatesRequest,
    ) -> anyhow::Result<Vec<(ClientId, Vec<u8>)>> {
        info!("NearIbcContract: [get_clients] - request: {:?}", request);

        let request = serde_json::to_string(&request).unwrap();

        self.get_rt()
            .block_on(self.get_client().view(
                self.get_contract_id().clone(),
                "get_clients".to_string(),
                json!({ "request": request }).to_string().into_bytes(),
            ))
            .expect("Failed to get_clients.")
            .json()
    }

    fn get_connections(
        &self,
        request: QueryConnectionsRequest,
    ) -> anyhow::Result<Vec<IdentifiedConnectionEnd>> {
        info!(
            "NearIbcContract: [get_connections] - request: {:?}",
            request
        );

        let request = serde_json::to_string(&request).unwrap();

        self.get_rt()
            .block_on(self.get_client().view(
                self.get_contract_id().clone(),
                "get_connections".to_string(),
                json!({ "request": request }).to_string().into_bytes(),
            ))
            .expect("Failed to get_connections.")
            .json()
    }

    fn get_channels(
        &self,
        request: QueryChannelsRequest,
    ) -> anyhow::Result<Vec<IdentifiedChannelEnd>> {
        info!("NearIbcContract: [get_channels] - request: {:?}", request);

        let request = serde_json::to_string(&request).unwrap();

        self.get_rt()
            .block_on(self.get_client().view(
                self.get_contract_id().clone(),
                "get_channels".to_string(),
                json!({ "request": request }).to_string().into_bytes(),
            ))
            .expect("Failed to get_channels.")
            .json()
    }

    fn get_packet_commitments(
        &self,
        request: QueryPacketCommitmentsRequest,
    ) -> anyhow::Result<Vec<Sequence>> {
        info!(
            "NearIbcContract: [get_packet_commitments] - request: {:?}",
            request
        );
        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_packet_commitments".to_string(),
                    json!({
                        "port_id": request.port_id.to_string(),
                        "channel_id": request.channel_id.to_string()
                    })
                    .to_string()
                    .into_bytes(),
                ),
            )
            .expect("Failed to get_packet_commitments.")
            .json()
    }

    fn get_packet_acknowledgements(
        &self,
        request: QueryPacketAcknowledgementsRequest,
    ) -> anyhow::Result<Vec<Sequence>> {
        info!("NearIbcContract: [get_packet_acknowledgements]");
        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_packet_acknowledgements".to_string(),
                    json!({
                        "port_id": request.port_id.to_string(),
                        "channel_id": request.channel_id.to_string()
                    })
                    .to_string()
                    .into_bytes(),
                ),
            )
            .expect("Failed to get_packet_acknowledgements.")
            .json()
    }

    /// get connection_identifier vector by client_identifier
    fn get_client_connections(
        &self,
        request: &QueryClientConnectionsRequest,
    ) -> anyhow::Result<Vec<ConnectionId>> {
        let client_id = request.client_id.to_string();
        info!(
            "NearIbcContract: [get_client_connections] - client_id: {:?}",
            client_id
        );
        self.get_rt()
            .block_on(self.get_client().view(
                self.get_contract_id().clone(),
                "get_client_connections".to_string(),
                json!({ "client_id": client_id }).to_string().into_bytes(),
            ))
            .expect("Failed to get_client_connections.")
            .json()
    }

    fn get_connection_channels(
        &self,
        connection_id: &ConnectionId,
    ) -> anyhow::Result<Vec<IdentifiedChannelEnd>> {
        info!(
            "NearIbcContract: [get_connection_channels] - connection_id: {:?}",
            connection_id
        );
        let connection_id = connection_id.to_string();
        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_connection_channels".to_string(),
                    json!({ "connection_id": connection_id })
                        .to_string()
                        .into_bytes(),
                ),
            )
            .expect("Failed to get_connection_channels.")
            .json()
    }

    /// The function to submit IBC request to a Near chain
    /// This function handles most of the IBC reqeusts to Near, except the MMR root update
    fn deliver(&self, messages: Vec<Any>) -> anyhow::Result<FinalExecutionOutcomeView> {
        info!("NearIbcContract: [deliver] - messages: {:?}", messages);
        let msg = serde_json::to_string(&messages).unwrap();

        let signer = InMemorySigner::from_random("bob.testnet".parse().unwrap(), KeyType::ED25519);
        self.get_rt().block_on(self.get_client().call(
            &signer,
            &self.get_contract_id(),
            "deliver".to_string(),
            json!({ "messages": messages }).to_string().into_bytes(),
            300000000000000,
            0,
        ))
    }

    fn raw_transfer(&self, msgs: Vec<Any>) -> anyhow::Result<FinalExecutionOutcomeView> {
        info!("NearIbcContract: [raw_transfer] - msgs: {:?}", msgs);
        let msg = serde_json::to_string(&msgs).unwrap();

        let signer = InMemorySigner::from_random("bob.testnet".parse().unwrap(), KeyType::ED25519);
        self.get_rt().block_on(self.get_client().call(
            &signer,
            &self.get_contract_id(),
            "raw_transfer".to_string(),
            json!({ "messages": msg }).to_string().into_bytes(),
            300000000000000,
            1,
        ))
    }

    fn get_packet_commitment(
        &self,
        port_id: &PortId,
        channel_id: &ChannelId,
        sequence: &Sequence,
    ) -> anyhow::Result<Vec<u8>> {
        info!(
            "NearIbcContract: [get_packet_commitment] - port_id: {:?}, channel_id: {:?}, sequence: {:?}",
            port_id, channel_id, sequence
        );
        let port_id = serde_json::to_string(port_id).unwrap();
        let channel_id = serde_json::to_string(channel_id).unwrap();
        let sequence = serde_json::to_string(sequence).unwrap();
        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_packet_commitment".to_string(),
                    json!({
                        "port_id": port_id,
                        "channel_id": channel_id,
                       "sequence": sequence
                    })
                    .to_string()
                    .into_bytes(),
                ),
            )
            .expect("Failed to get_packet_commitment.")
            .json()
    }

    fn get_commitment_prefix(&self) -> anyhow::Result<CommitmentPrefix> {
        info!("NearIbcContract: [get_commitment_prefix]");
        self.get_rt()
            .block_on(self.get_client().view(
                self.get_contract_id().clone(),
                "get_commitment_prefix".to_string(),
                json!({}).to_string().into_bytes(),
            ))
            .expect("Failed to get_commitment_prefix.")
            .json()
    }

    fn get_packet_receipt(
        &self,
        port_id: &PortId,
        channel_id: &ChannelId,
        sequence: &Sequence,
    ) -> anyhow::Result<Vec<u8>> {
        info!(
            "NearIbcContract: [query_packet_receipt] - port_id: {:?}, channel_id: {:?}, sequence: {:?}",
            port_id, channel_id, sequence
        );
        let port_id = serde_json::to_string(port_id).unwrap();
        let channel_id = serde_json::to_string(channel_id).unwrap();
        let sequence = serde_json::to_string(sequence).unwrap();
        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "query_packet_receipt".to_string(),
                    json!({
                        "port_id": port_id,
                        "channel_id": channel_id,
                       "sequence": sequence
                    })
                    .to_string()
                    .into_bytes(),
                ),
            )
            .expect("Failed to query_packet_receipt.")
            .json()
    }

    fn get_next_sequence_receive(
        &self,
        port_id: &PortId,
        channel_id: &ChannelId,
    ) -> anyhow::Result<Sequence> {
        info!(
            "NearIbcContract: [get_next_sequence_receive] - port_id: {:?}, channel_id: {:?}",
            port_id, channel_id,
        );
        let port_id = serde_json::to_string(port_id).unwrap();
        let channel_id = serde_json::to_string(channel_id).unwrap();
        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_next_sequence_receive".to_string(),
                    json!({
                        "port_id": port_id,
                        "channel_id": channel_id,
                    })
                    .to_string()
                    .into_bytes(),
                ),
            )
            .expect("Failed to get_next_sequence_receive.")
            .json()
    }

    fn get_packet_acknowledgement(
        &self,
        port_id: &PortId,
        channel_id: &ChannelId,
        sequence: &Sequence,
    ) -> anyhow::Result<Vec<u8>> {
        info!(
            "NearIbcContract: [get_packet_acknowledgement] - port_id: {:?}, channel_id: {:?}, sequence: {:?}",
            port_id, channel_id, sequence
        );
        let port_id = serde_json::to_string(port_id).unwrap();
        let channel_id = serde_json::to_string(channel_id).unwrap();
        let sequence = serde_json::to_string(sequence).unwrap();
        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_packet_acknowledgement".to_string(),
                    json!({
                        "port_id": port_id,
                        "channel_id": channel_id,
                        "sequence": sequence
                    })
                    .to_string()
                    .into_bytes(),
                ),
            )
            .expect("Failed to get_packet_acknowledgement.")
            .json()
    }
}

// #[derive(Debug)]
// pub struct NearIbcContractInteractor<'s> {
//     pub account_id: AccountId,
//     pub client: &'s NearRpcClient,
// }
//
// impl<'s> NearIbcContractInteractor<'s>  {
//
//     pub async fn query_connection_end(
//         &self,
//         connection_id: ConnectionId
//     )->anyhow::Result<ConnectionEnd>{
//         let connection_id =  connection_id.to_string();
//         self.client.view(
//             self.account_id.clone(),
//             "query_connection_end".to_string(),
//                 json!({"connection_id": connection_id }).to_string().into_bytes()
//         ).await.and_then(|e|e.json())
//     }
//
//
//     pub async fn query_channel_end(&self, port_id: PortId, channel_id: ChannelId)-> anyhow::Result<ChannelEnd>{
//         let port_id = serde_json::to_string(&port_id).unwrap();
//         let channel_id = serde_json::to_string(&channel_id).unwrap();
//         self.client.view(
//             self.account_id.clone(),
//             "query_channel_end".to_string(),
//             json!({"port_id": port_id,"channel_id": channel_id}).to_string().into_bytes()
//         ).await.and_then(|e|e.json())
//     }
//
//     pub async fn query_client_state(&self, client_id: ClientId)-> anyhow::Result<AnyClientState>{
//         let client_id = serde_json::to_string(&client_id).unwrap();
//         self.client.view(
//             self.account_id.clone(),
//             "query_client_state".to_string(),
//             json!({"client_id": client_id}).to_string().into_bytes()
//         ).await.and_then(|e|e.json())
//     }
//
//     pub async fn query_client_consensus(&self, client_id: ClientId, consensus_height: Height)->anyhow::Result<AnyConsensusState>{
//         let client_id = serde_json::to_string(&client_id).unwrap();
//         self.client.view(
//             self.account_id.clone(),
//             "query_client_state".to_string(),
//             json!({"client_id": client_id}).to_string().into_bytes()
//         ).await.and_then(|e|e.json())
//     }
//
// }

#[tokio::test]
pub async fn test123() -> anyhow::Result<()> {
    let client =
        NearRpcClient::new("https://near-testnet.infura.io/v3/4f80a04e6eb2437a9ed20cb874e10d55");
    let connection_id = ConnectionId::new(5);
    let connection_id = serde_json::to_string(&connection_id).unwrap();
    dbg!(&connection_id);

    dbg!(&connection_id.eq("connection-5"));
    let result = client
        .view(
            CONTRACT_ACCOUNT_ID.parse()?,
            "get_connection_end".to_string(),
            json!({"connection_id": "connection-5".to_string()})
                .to_string()
                .into_bytes(),
        )
        .await
        .unwrap();

    // dbg!(&result);

    let result_raw: serde_json::value::Value = result.json().unwrap();
    dbg!(&result_raw);
    let connection_end: ConnectionEnd = serde_json::from_value(result_raw).unwrap();
    dbg!(&connection_end);
    Ok(())
}
