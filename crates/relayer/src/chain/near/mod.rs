use super::client::ClientSettings;
use crate::{
    account::Balance,
    chain::endpoint::ChainEndpoint,
    chain::endpoint::ChainStatus,
    chain::endpoint::HealthCheck,
    chain::handle::Subscription,
    chain::near::contract::NearIbcContract,
    chain::near::rpc::client::NearRpcClient,
    chain::near::rpc::rpc_provider::{NearEnv, RpcProvider},
    chain::near::rpc::tool::convert_ibc_event_to_hermes_ibc_event,
    chain::requests::QueryChannelRequest,
    chain::requests::QueryChannelsRequest,
    chain::requests::QueryClientConnectionsRequest,
    chain::requests::QueryClientStatesRequest,
    chain::requests::QueryConnectionChannelsRequest,
    chain::requests::QueryConnectionRequest,
    chain::requests::QueryConnectionsRequest,
    chain::requests::QueryConsensusStateRequest,
    chain::requests::QueryConsensusStatesRequest,
    chain::requests::QueryNextSequenceReceiveRequest,
    chain::requests::QueryPacketAcknowledgementsRequest,
    chain::requests::QueryPacketCommitmentsRequest,
    chain::requests::QueryPacketEventDataRequest,
    chain::requests::QueryTxRequest,
    chain::requests::QueryUnreceivedAcksRequest,
    chain::requests::QueryUnreceivedPacketsRequest,
    chain::requests::{
        CrossChainQueryRequest, QueryChannelClientStateRequest, QueryConsensusStateHeightsRequest,
    },
    chain::requests::{
        IncludeProof, QueryClientStateRequest, QueryHeight, QueryHostConsensusStateRequest,
        QueryPacketAcknowledgementRequest, QueryPacketCommitmentRequest, QueryPacketReceiptRequest,
        QueryUpgradedClientStateRequest, QueryUpgradedConsensusStateRequest,
    },
    chain::tracking::{TrackedMsgs, TrackingId},
    client_state::{AnyClientState, IdentifiedAnyClientState},
    config::AddressType,
    config::ChainConfig,
    consensus_state::{AnyConsensusState, AnyConsensusStateWithHeight},
    denom::DenomTrace,
    error::Error,
    event::monitor::EventBatch,
    event::IbcEventWithHeight,
    keyring::{KeyRing, Secp256k1KeyPair, SigningKeyPair, Test},
    misbehaviour::MisbehaviourEvidence,
};
use alloc::sync::Arc;
use anyhow::Result;
use bitcoin::util::bip32::ExtendedPubKey;
use core::{fmt::Debug, future::Future, str::FromStr};
use hdpath::StandardHDPath;
use ibc_proto::{google::protobuf::Any, ibc::core::channel::v1::PacketState, protobuf::Protobuf};
use ibc_relayer_types::{
    applications::ics31_icq::response::CrossChainQueryResponse,
    clients::{
        ics06_solomachine::ClientState as SmClientState,
        ics06_solomachine::ConsensusState as SmConsensusState,
        ics06_solomachine::{Header as SmHeader, HeaderData as SmHeaderData, PublicKey, SignBytes},
    },
    core::ics02_client::client_state::ClientState,
    core::ics02_client::events::UpdateClient,
    core::ics23_commitment::commitment::CommitmentRoot,
    core::ics23_commitment::merkle::MerkleProof,
    core::ics24_host::path::{
        AcksPath, ChannelEndsPath, ClientConsensusStatePath, ClientStatePath, CommitmentsPath,
        ConnectionsPath, ReceiptsPath, SeqRecvsPath,
    },
    core::{
        ics02_client::client_type::ClientType,
        ics03_connection::connection::{ConnectionEnd, IdentifiedConnectionEnd},
        ics04_channel::{
            channel::{ChannelEnd, IdentifiedChannelEnd},
            packet::{Receipt, Sequence},
        },
        ics23_commitment::commitment::CommitmentPrefix,
        ics24_host::identifier::{ChainId, ChannelId, ClientId, ConnectionId, PortId},
    },
    events::IbcEvent,
    signer::Signer,
    Height, Height as ICSHeight,
};
use near_account_id::AccountId;
use near_crypto::{InMemorySigner, KeyType};
use near_jsonrpc_client::JsonRpcClient;
use near_primitives::{
    types::{BlockId, Gas},
    views::FinalExecutionOutcomeView,
};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sp_core::sr25519;
use sp_core::{hexdisplay::HexDisplay, Pair, H256};
use std::{
    path::Path,
    thread,
    time::{Duration, SystemTime},
};
use tendermint::time::Time;
use tendermint_rpc::endpoint::broadcast::tx_sync::Response as TxResponse;
use tokio::runtime::Runtime as TokioRuntime;
use tracing::{debug, info, trace};

pub mod constants;
pub mod contract;
mod light_client;
pub mod rpc;

pub const REVISION_NUMBER: u64 = 0;

/// A struct used to start a Near chain instance in relayer
#[derive(Debug)]
pub struct NearChain {
    client: NearRpcClient,
    config: ChainConfig,
    keybase: KeyRing<Secp256k1KeyPair>,
    near_ibc_contract: AccountId,
    rt: Arc<TokioRuntime>,
}

impl NearIbcContract for NearChain {
    fn get_contract_id(&self) -> AccountId {
        self.near_ibc_contract.clone()
    }

    fn get_client(&self) -> &NearRpcClient {
        &self.client
    }

    fn get_rt(&self) -> &Arc<TokioRuntime> {
        &self.rt
    }
}

impl NearChain {
    pub fn config(&self) -> &ChainConfig {
        &self.config
    }

    /// Run a future to completion on the Tokio runtime.
    fn block_on<F: Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }

    /// Subscribe Events
    /// todo near don't have events subscription
    fn subscribe_ibc_events(&self) -> Result<Vec<IbcEvent>> {
        tracing::trace!("in near: [subscribe_ibc_events]");
        todo!() //Bob
    }

    /// get packet receipt by port_id, channel_id and sequence
    fn _get_packet_receipt(
        &self,
        port_id: &PortId,
        channel_id: &ChannelId,
        seq: &Sequence,
    ) -> Result<Receipt> {
        tracing::trace!("in near: [get_packet_receipt]");

        let port_id = serde_json::to_string(port_id).unwrap();
        let channel_id = serde_json::to_string(channel_id).unwrap();
        let seq = serde_json::to_string(seq).unwrap();

        // self.block_on(self.client.view(
        //     self.near_ibc_contract.clone(),
        //     "get_packet_receipt".to_string(),
        //     json!({"port_id": port_id, "channel_id": channel_id, "seq": seq}).to_string().into_bytes()
        // )).expect("Failed to get_packet_receipt.").json()

        todo!() // todo the receipt can't deserialize
    }

    // fn get_clients(&self) -> Result<Vec<IdentifiedAnyClientState>> {
    //
    //     tracing::trace!("in near: [get_clients]");
    //     self.block_on(self.client.view(
    //         self.near_ibc_contract.clone(),
    //         "get_clients".to_string(),
    //         json!({}).to_string().into_bytes()
    //     )).expect("Failed to get_clients.").json()
    // }

    /// The function to submit IBC request to a Near chain
    /// This function handles most of the IBC reqeusts to Near, except the MMR root update
    fn deliver(&self, messages: Vec<Any>) -> Result<FinalExecutionOutcomeView> {
        info!("in near: [deliver]");
        let msg = serde_json::to_string(&messages).unwrap();

        let mut home_dir = dirs::home_dir().expect("Impossible to get your home dir!");
        home_dir.push(".near-credentials/testnet/my-account.testnet.json");
        let signer = InMemorySigner::from_file(home_dir.as_path()).unwrap();

        self.block_on(self.client.call(
            &signer,
            &self.near_ibc_contract,
            "deliver".to_string(),
            json!({ "messages": messages }).to_string().into_bytes(),
            300000000000000,
            0,
        ))
    }

    fn raw_transfer(&self, messages: Vec<Any>) -> Result<FinalExecutionOutcomeView> {
        tracing::trace!("in near: [raw_transfer]");
        let msg = serde_json::to_string(&messages).unwrap();

        let signer = InMemorySigner::from_random("bob.testnet".parse().unwrap(), KeyType::ED25519);
        self.block_on(self.client.call(
            &signer,
            &self.near_ibc_contract,
            "deliver".to_string(),
            json!({ "messages": messages }).to_string().into_bytes(),
            300000000000000,
            1,
        ))
    }

    /// Retrieve the storage proof according to storage keys
    /// And convert the proof to IBC compatible type
    fn generate_storage_proof<'a>(
        &self,
        storage_entry: impl IntoIterator<Item = &'a [u8]>,
        height: &Height,
        _storage_name: &str,
    ) -> Result<MerkleProof, Error> {
        Ok(MerkleProof::default())
    }

    fn get_solomachine_pubkey(&self) -> PublicKey {
        PublicKey(
            tendermint::PublicKey::from_raw_secp256k1(
                &self
                    .keybase
                    .get_key(&self.config.key_name)
                    .unwrap()
                    .public_key
                    .serialize_uncompressed(),
            )
            .unwrap(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NearLightBlock {}

impl ChainEndpoint for NearChain {
    type LightBlock = NearLightBlock; // Todo: Import from Near light client //CS
    type Header = SmHeader; // Todo: Import from Near light client //CS
    type ConsensusState = SmConsensusState; // Todo: Import from Near light client //CS
    type ClientState = SmClientState; // Todo: Import from Near light client //CS
    type SigningKeyPair = Secp256k1KeyPair;

    fn id(&self) -> &ChainId {
        &self.config().id
    }

    fn config(&self) -> &ChainConfig {
        self.config()
    }

    // todo init NearChain
    fn bootstrap(config: ChainConfig, rt: Arc<TokioRuntime>) -> Result<Self, Error> {
        tracing::info!("in near: [bootstrap function], read config: {:?}", config);
        // Initialize key store and load key
        let keybase = KeyRing::new(config.key_store_type, &config.account_prefix, &config.id)
            .map_err(Error::key_base)?;
        Ok(NearChain {
            client: NearRpcClient::new(config.rpc_addr.to_string().as_str()),
            config,
            keybase,
            near_ibc_contract: AccountId::from_str("nearibc.testnet").unwrap(),
            rt,
        })
    }

    // fn init_event_monitor(
    //     &self,
    //     rt: Arc<TokioRuntime>,
    // ) -> Result<(EventReceiver, TxMonitorCmd), Error> {
    //     debug!(
    //         "in near: [init_event_mointor] >> websocket addr: {:?}",
    //         self.config.websocket_addr.clone()
    //     );
    //     todo!()
    // }

    fn shutdown(self) -> Result<(), Error> {
        Ok(())
    }

    fn health_check(&self) -> Result<HealthCheck, Error> {
        Ok(HealthCheck::Healthy)
    }

    fn keybase(&self) -> &KeyRing<Self::SigningKeyPair> {
        &self.keybase
    }

    fn keybase_mut(&mut self) -> &mut KeyRing<Self::SigningKeyPair> {
        &mut self.keybase
    }

    fn get_signer(&self) -> Result<Signer, Error> {
        trace!("In near: [get signer]");
        // crate::time!("get_signer");
        // // Todo: get Near Signer //CS
        // /// Public key type for Runtime
        // pub type PublicFor<P> = <P as Pair>::Public;
        //
        // /// formats public key as accountId as hex
        // fn format_account_id<P: Pair>(public_key: PublicFor<P>) -> String
        // where
        //     PublicFor<P>: Into<MultiSigner>,
        // {
        //     format!(
        //         "0x{}",
        //         HexDisplay::from(&public_key.into().into_account().as_ref())
        //     )
        // }
        //
        // // Get the key from key seed file
        // let key = self
        //     .keybase()
        //     .get_key(&self.config.key_name)
        //     .map_err(|e| Error::key_not_found(self.config.key_name.clone(), e))?;
        //
        // let private_seed = key.private_key.private_key;
        //
        // let pair = sr25519::Pair::from_seed_slice(private_seed.as_ref())
        // .map_err(|e| Error::report_error(format!("{:?}", e)))?;
        // let public_key = pair.public();
        //
        // let account_id = format_account_id::<sr25519::Pair>(public_key);
        //
        // Ok(Signer::from_str(&account_id).unwrap())
        Ok(Signer::from_str("xsb").unwrap())
    }

    fn get_key(&mut self) -> Result<Self::SigningKeyPair, Error> {
        tracing::trace!("in near: [get_key]");
        crate::time!("get_key");

        // Get the key from key seed file
        let key = self
            .keybase()
            .get_key(&self.config.key_name)
            .map_err(|e| Error::key_not_found(self.config.key_name.clone(), e))?;

        Ok(key)
    }

    fn add_key(&mut self, key_name: &str, key_pair: Self::SigningKeyPair) -> Result<(), Error> {
        self.keybase_mut()
            .add_key(key_name, key_pair)
            .map_err(Error::key_base)?;

        Ok(())
    }

    // versioning
    fn ibc_version(&self) -> Result<Option<Version>, Error> {
        // todo(bob)
        Ok(None)
    }

    // send transactions

    fn send_messages_and_wait_commit(
        &mut self,
        proto_msgs: TrackedMsgs,
    ) -> Result<Vec<IbcEventWithHeight>, Error> {
        info!(
            "in near: [send_messages_and_wait_commit], proto_msgs={:?}",
            proto_msgs
        );

        let result = match proto_msgs.tracking_id {
            TrackingId::Uuid(_) => {
                let result = self.deliver(proto_msgs.messages().to_vec()).map_err(|e| {
                    Error::report_error(format!("deliever error ({:?})", e.to_string()))
                })?;
                // result.transaction_outcome
                debug!(
                    "in near: [send_messages_and_wait_commit] >> extrics_hash  : {:?}",
                    result
                );
                result
            }
            TrackingId::Static(value) => match value {
                "ft-transfer" => {
                    todo!() // wait for near-ibc ics20
                }
                _ => {
                    let result = self.deliver(proto_msgs.messages().to_vec()).map_err(|e| {
                        Error::report_error(format!("deliever error ({:?})", e.to_string()))
                    })?;

                    debug!(
                        "in near: [send_messages_and_wait_commit] >> extrics_hash  : {:?}",
                        result
                    );
                    result
                }
            },
            TrackingId::ClearedUuid(_) => {
                todo!()
            }
        };

        Ok(collect_ibc_event_by_outcome(result))
        // Ok(ibc_event_with_height)
    }

    fn send_messages_and_wait_check_tx(
        &mut self,
        proto_msgs: TrackedMsgs,
    ) -> Result<Vec<TxResponse>, Error> {
        debug!(
            "in near: [send_messages_and_wait_check_tx], proto_msgs={:?}",
            proto_msgs.tracking_id
        );

        match proto_msgs.tracking_id {
            TrackingId::Uuid(_) => {
                let result = self.deliver(proto_msgs.messages().to_vec()).map_err(|e| {
                    Error::report_error(format!("deliever error ({:?})", e.to_string()))
                })?;
                debug!(
                    "in near: [send_messages_and_wait_commit] >> extrics_hash  : {:?}",
                    result
                );
            }
            TrackingId::Static(value) => match value {
                "ft-transfer" => {
                    let result = self
                        .raw_transfer(proto_msgs.messages().to_vec())
                        .map_err(|_| Error::report_error("ics20_transfer".to_string()))?;

                    debug!(
                        "in near: [send_messages_and_wait_commit] >> extrics_hash  : {:?}",
                        result
                    );
                }
                _ => {
                    let result = self.deliver(proto_msgs.messages().to_vec()).map_err(|e| {
                        Error::report_error(format!("deliever error ({:?})", e.to_string()))
                    })?;
                    debug!(
                        "in near: [send_messages_and_wait_commit] >> extrics_hash  : {:?}",
                        result
                    );
                }
            },
            TrackingId::ClearedUuid(_) => {}
        }

        Ok(vec![])
    }

    // Light client

    /// Fetch a header from the chain at the given height and verify it.
    fn verify_header(
        &mut self,
        _trusted: ICSHeight,
        _target: ICSHeight,
        _client_state: &AnyClientState,
    ) -> Result<Self::LightBlock, Error> {
        Ok(Self::LightBlock {})
    }

    /// Given a client update event that includes the header used in a client update,
    /// look for misbehaviour by fetching a header at same or latest height.
    fn check_misbehaviour(
        &mut self,
        _update: &UpdateClient,
        _client_state: &AnyClientState,
    ) -> Result<Option<MisbehaviourEvidence>, Error> {
        todo!()
    }

    // Queries

    fn query_balance(
        &self,
        _key_name: Option<&str>,
        _denom: Option<&str>,
    ) -> std::result::Result<Balance, Error> {
        Ok(Balance {
            amount: String::default(),
            denom: String::default(),
        })
    }

    fn query_all_balances(&self, _key_name: Option<&str>) -> Result<Vec<Balance>, Error> {
        todo!()
    }

    fn query_denom_trace(&self, _hash: String) -> std::result::Result<DenomTrace, Error> {
        // todo(daviarin) add mock denom trace
        Ok(DenomTrace {
            /// The chain of port/channel identifiers used for tracing the source of the coin.
            path: String::default(),
            /// The base denomination for that coin
            base_denom: String::default(),
        })
    }

    fn query_commitment_prefix(&self) -> Result<CommitmentPrefix, Error> {
        tracing::trace!("in near: [get_commitment_prefix]");
        self.get_commitment_prefix()
            .map_err(|e| Error::report_error("invalid_commitment_prefix".to_string()))

        // self.block_on(self.client.view(
        //     self.near_ibc_contract.clone(),
        //     "get_commitment_prefix".to_string(),
        //     json!({}).to_string().into_bytes()
        // )).and_then(|e| e.json())

        // TODO - do a real chain query
        // CommitmentPrefix::try_from(self.config().store_prefix.as_bytes().to_vec())
        //     .map_err(|_| Error::report_error("invalid_commitment_prefix".to_string()))
    }

    fn query_application_status(&self) -> Result<ChainStatus, Error> {
        tracing::trace!("in near: [query_status]");

        let latest_height = self
            .get_latest_height()
            .map_err(|_| Error::report_error("get_latest_height".to_string()))?;

        Ok(ChainStatus {
            height: latest_height,
            timestamp: Default::default(),
        })
    }

    fn query_clients(
        &self,
        _request: QueryClientStatesRequest,
    ) -> Result<Vec<IdentifiedAnyClientState>, Error> {
        tracing::trace!("in near: [query_clients]");

        let result = self
            .get_clients(_request)
            .map_err(|_| Error::report_error("get_clients".to_string()))?;

        Ok(result)
    }

    fn query_client_state(
        &self,
        request: QueryClientStateRequest,
        include_proof: IncludeProof,
    ) -> Result<(AnyClientState, Option<MerkleProof>), Error> {
        tracing::trace!("in near: [query_client_state]");

        let QueryClientStateRequest { client_id, height } = request;

        let query_height = match height {
            QueryHeight::Latest => {
                let height = self
                    .get_latest_height()
                    .map_err(|_| Error::report_error("query_latest_height".to_string()))?;
                height
            }
            QueryHeight::Specific(value) => value,
        };

        let result = self
            .get_client_state(&client_id)
            .map_err(|_| Error::report_error("query_client_state".to_string()))?;
        let client_state = AnyClientState::decode_vec(&result).unwrap();

        match include_proof {
            IncludeProof::Yes => Ok((client_state, Some(MerkleProof::default()))),
            IncludeProof::No => Ok((client_state, None)),
        }
    }

    fn query_consensus_state(
        &self,
        request: QueryConsensusStateRequest,
        include_proof: IncludeProof,
    ) -> Result<(AnyConsensusState, Option<MerkleProof>), Error> {
        tracing::trace!("in near: [query_consensus_state]");

        // query_height to amit to search chain height
        let QueryConsensusStateRequest {
            client_id,
            consensus_height,
            query_height: _,
        } = request;

        let result = self
            .get_client_consensus(&client_id, &consensus_height)
            .map_err(|_| Error::report_error("query_client_consensus".to_string()))?;
        let consensus_state = AnyConsensusState::decode_vec(&result).unwrap();

        match include_proof {
            IncludeProof::Yes => Ok((consensus_state, Some(MerkleProof::default()))),
            IncludeProof::No => Ok((consensus_state, None)),
        }
    }

    // fn query_consensus_states(
    //     &self,
    //     request: QueryConsensusStatesRequest,
    // ) -> Result<Vec<AnyConsensusStateWithHeight>, Error> {
    //     tracing::trace!("in near: [query_consensus_states]");
    //
    //     let request_client_id = ClientId::from_str(request.client_id.as_str())
    //         .map_err(|_| Error::report_error("identifier".to_string()))?;
    //
    //     let result = self
    //         .get_consensus_state_with_height(&request_client_id)
    //         .map_err(|_| Error::report_error("get_consensus_state_with_height".to_string()))?;
    //
    //     let consensus_state: Vec<(Height, AnyConsensusState)> = result;
    //
    //     let mut any_consensus_state_with_height = vec![];
    //     for (height, consensus_state) in consensus_state.into_iter() {
    //         let tmp = AnyConsensusStateWithHeight {
    //             height,
    //             consensus_state,
    //         };
    //         any_consensus_state_with_height.push(tmp.clone());
    //
    //         tracing::trace!(
    //             "in near: [query_consensus_state] >> any_consensus_state_with_height: {:?}",
    //             tmp
    //         );
    //     }
    //
    //     any_consensus_state_with_height.sort_by(|a, b| a.height.cmp(&b.height));
    //
    //     Ok(any_consensus_state_with_height)
    // }

    fn query_upgraded_client_state(
        &self,
        _request: QueryUpgradedClientStateRequest,
    ) -> Result<(AnyClientState, MerkleProof), Error> {
        tracing::trace!("in near: [query_upgraded_client_state]");

        todo!()
    }

    fn query_upgraded_consensus_state(
        &self,
        _request: QueryUpgradedConsensusStateRequest,
    ) -> Result<(AnyConsensusState, MerkleProof), Error> {
        tracing::trace!("in near: [query_upgraded_consensus_state]");

        todo!()
    }

    fn query_connections(
        &self,
        _request: QueryConnectionsRequest,
    ) -> Result<Vec<IdentifiedConnectionEnd>, Error> {
        tracing::trace!("in near: [query_connections]");

        let result = self
            .get_connections(_request)
            .map_err(|_| Error::report_error("get_connections".to_string()))?;

        Ok(result)
    }

    fn query_client_connections(
        &self,
        request: QueryClientConnectionsRequest,
    ) -> Result<Vec<ConnectionId>, Error> {
        tracing::trace!("in near: [query_client_connections]");
        todo!() //Bob
    }

    fn query_connection(
        &self,
        request: QueryConnectionRequest,
        include_proof: IncludeProof,
    ) -> Result<(ConnectionEnd, Option<MerkleProof>), Error> {
        tracing::trace!("in near: [query_connection]");

        let QueryConnectionRequest {
            connection_id,
            height,
        } = request;

        let connection_end = self
            .get_connection_end(&connection_id)
            .map_err(|_| Error::report_error("query_connection_end".to_string()))?;

        // update ConnectionsPath key
        let connections_path = ConnectionsPath(connection_id.clone()).to_string();

        Ok((connection_end, Some(MerkleProof::default())))

        // match include_proof {
        //     IncludeProof::Yes => {
        //         let query_height = match height {
        //             QueryHeight::Latest => {
        //                 let height = self
        //                     .get_latest_height()
        //                     .map_err(|_| Error::report_error("query_latest_height".to_string()))?;
        //                 height
        //             }
        //             QueryHeight::Specific(value) => value,
        //         };
        //
        //         Ok((
        //             connection_end,
        //             Some(self.generate_storage_proof(
        //                     vec![connections_path.as_bytes()],
        //                 &query_height,
        //                 "Connections",
        //             )?),
        //         ))
        //     }
        //     IncludeProof::No => Ok((connection_end, None)),
        // }
    }

    fn query_connection_channels(
        &self,
        request: QueryConnectionChannelsRequest,
    ) -> Result<Vec<IdentifiedChannelEnd>, Error> {
        tracing::trace!("in near: [query_connection_channels] ");

        let result = self
            .get_connection_channels(&request.connection_id)
            .map_err(|_| Error::report_error("get_connection_channels".to_string()))?;

        Ok(result)
    }

    fn query_channels(
        &self,
        _request: QueryChannelsRequest,
    ) -> Result<Vec<IdentifiedChannelEnd>, Error> {
        tracing::trace!("in near: [query_channels]");

        let result = self
            .get_channels(_request)
            .map_err(|_| Error::report_error("get_channels".to_string()))?;

        Ok(result)
    }

    fn query_channel(
        &self,
        request: QueryChannelRequest,
        include_proof: IncludeProof,
    ) -> Result<(ChannelEnd, Option<MerkleProof>), Error> {
        tracing::trace!("in near: [query_channel]");

        let QueryChannelRequest {
            port_id,
            channel_id,
            height,
        } = request;

        let channel_end = self
            .get_channel_end(&port_id, &channel_id)
            .map_err(|_| Error::report_error("query_channel_end".to_string()))?;

        // use channel_end path as key
        let channel_end_path = ChannelEndsPath(port_id.clone(), channel_id.clone()).to_string();

        match include_proof {
            IncludeProof::Yes => {
                let query_height = match height {
                    QueryHeight::Latest => {
                        let height = self
                            .get_latest_height()
                            .map_err(|_| Error::report_error("query_latest_height".to_string()))?;
                        height
                    }
                    QueryHeight::Specific(value) => value,
                };

                Ok((channel_end, Some(MerkleProof::default())))
            }
            IncludeProof::No => Ok((channel_end, None)),
        }
    }

    fn query_channel_client_state(
        &self,
        _request: QueryChannelClientStateRequest,
    ) -> Result<Option<IdentifiedAnyClientState>, Error> {
        tracing::trace!("in near: [query_channel_client_state]");

        todo!()
    }

    fn query_packet_commitment(
        &self,
        request: QueryPacketCommitmentRequest,
        include_proof: IncludeProof,
    ) -> Result<(Vec<u8>, Option<MerkleProof>), Error> {
        let QueryPacketCommitmentRequest {
            port_id,
            channel_id,
            sequence,
            height,
        } = request;

        let packet_commit = self
            .get_packet_commitment(&port_id, &channel_id, &sequence)
            .map_err(|_| Error::report_error("query_packet_commitment".to_string()))?;

        let packet_commits_path = CommitmentsPath {
            port_id: port_id.clone(),
            channel_id: channel_id.clone(),
            sequence: sequence.clone(),
        }
        .to_string();

        match include_proof {
            IncludeProof::Yes => {
                let query_height = match height {
                    QueryHeight::Latest => {
                        let height = self
                            .get_latest_height()
                            .map_err(|_| Error::report_error("query_latest_height".to_string()))?;
                        height
                    }
                    QueryHeight::Specific(value) => value,
                };
                Ok((packet_commit, Some(MerkleProof::default())))
            }
            IncludeProof::No => Ok((packet_commit, None)),
        }
    }

    fn query_packet_commitments(
        &self,
        _request: QueryPacketCommitmentsRequest,
    ) -> Result<(Vec<Sequence>, ICSHeight), Error> {
        tracing::trace!("in near: [query_packet_commitments]");

        let packet_commitments = self
            .get_commitment_packet_state(_request)
            .map_err(|_| Error::report_error("get_commitment_packet_state".to_string()))?
            .into_iter()
            .map(|value| Sequence::from(value.sequence))
            .collect();

        let latest_height = self
            .get_latest_height()
            .map_err(|_| Error::report_error("get_latest_height_error".to_string()))?;

        Ok((packet_commitments, latest_height))
    }

    fn query_packet_receipt(
        &self,
        request: QueryPacketReceiptRequest,
        include_proof: IncludeProof,
    ) -> Result<(Vec<u8>, Option<MerkleProof>), Error> {
        let QueryPacketReceiptRequest {
            port_id,
            channel_id,
            sequence,
            height,
        } = request;

        let packet_receipt = self
            .get_packet_receipt(&port_id, &channel_id, &sequence)
            .map_err(|_| Error::report_error("query_packet_receipt".to_string()))?;

        let packet_receipt_path = ReceiptsPath {
            port_id: port_id.clone(),
            channel_id: channel_id.clone(),
            sequence: sequence.clone(),
        }
        .to_string();

        match include_proof {
            IncludeProof::Yes => {
                let query_height = match height {
                    QueryHeight::Latest => {
                        let height = self
                            .get_latest_height()
                            .map_err(|_| Error::report_error("query_latest_height".to_string()))?;
                        height
                    }
                    QueryHeight::Specific(value) => value,
                };
                Ok((
                    packet_receipt,
                    Some(self.generate_storage_proof(
                        vec![packet_receipt_path.as_bytes()],
                        &query_height,
                        "PacketReceipt",
                    )?),
                ))
            }
            IncludeProof::No => Ok((packet_receipt, None)),
        }
    }

    fn query_unreceived_packets(
        &self,
        request: QueryUnreceivedPacketsRequest,
    ) -> Result<Vec<Sequence>, Error> {
        tracing::trace!("in near: [query_unreceived_packets]");

        let port_id = PortId::from_str(request.port_id.as_str())
            .map_err(|_| Error::report_error("identifier".to_string()))?;
        let channel_id = ChannelId::from_str(request.channel_id.as_str())
            .map_err(|_| Error::report_error("identifier".to_string()))?;
        let sequences = request
            .packet_commitment_sequences
            .into_iter()
            .map(Sequence::from)
            .collect::<Vec<_>>();

        let result = self
            .get_unreceipt_packet(&port_id, &channel_id, &sequences)
            .map_err(|_| Error::report_error("get_unreceipt_packet".to_string()))?
            .into_iter()
            .map(|value| Sequence::from(value))
            .collect();

        Ok(result)
    }

    fn query_packet_acknowledgement(
        &self,
        request: QueryPacketAcknowledgementRequest,
        include_proof: IncludeProof,
    ) -> Result<(Vec<u8>, Option<MerkleProof>), Error> {
        let QueryPacketAcknowledgementRequest {
            port_id,
            channel_id,
            sequence,
            height,
        } = request;

        let packet_acknowledgement = self
            .get_packet_acknowledgement(&port_id, &channel_id, &sequence)
            .map_err(|_| Error::report_error("query_packet_acknowledgement".to_string()))?;

        let packet_acknowledgement_path = AcksPath {
            port_id: port_id.clone(),
            channel_id: channel_id.clone(),
            sequence: sequence.clone(),
        }
        .to_string();

        match include_proof {
            IncludeProof::Yes => {
                let query_height = match height {
                    QueryHeight::Latest => {
                        let height = self
                            .get_latest_height()
                            .map_err(|_| Error::report_error("query_latest_height".to_string()))?;
                        height
                    }
                    QueryHeight::Specific(value) => value,
                };
                Ok((
                    packet_acknowledgement,
                    Some(self.generate_storage_proof(
                        vec![packet_acknowledgement_path.as_bytes()],
                        &query_height,
                        "Acknowledgements",
                    )?),
                ))
            }
            IncludeProof::No => Ok((packet_acknowledgement, None)),
        }
    }

    fn query_packet_acknowledgements(
        &self,
        _request: QueryPacketAcknowledgementsRequest,
    ) -> Result<(Vec<Sequence>, ICSHeight), Error> {
        tracing::trace!("in near: [query_packet_acknowledgements]");

        let packet_acknowledgements = self
            .get_acknowledge_packet_state()
            .map_err(|_| Error::report_error("get_acknowledge_packet_state".to_string()))?
            .into_iter()
            .map(|value| Sequence::from(value.sequence))
            .collect();

        let latest_height = self
            .get_latest_height()
            .map_err(|_| Error::report_error("get_latest_height".to_string()))?;

        Ok((packet_acknowledgements, latest_height))
    }

    fn query_unreceived_acknowledgements(
        &self,
        request: QueryUnreceivedAcksRequest,
    ) -> Result<Vec<Sequence>, Error> {
        tracing::trace!("in near: [query_unreceived_acknowledgements] ");

        let port_id = PortId::from_str(request.port_id.as_str())
            .map_err(|_| Error::report_error("identifier".to_string()))?;
        let channel_id = ChannelId::from_str(request.channel_id.as_str())
            .map_err(|_| Error::report_error("identifier".to_string()))?;
        let sequences = request
            .packet_ack_sequences
            .into_iter()
            .map(Sequence::from)
            .collect::<Vec<_>>();

        let mut unreceived_seqs = vec![];

        for seq in sequences {
            let cmt = self.get_packet_commitment(&port_id, &channel_id, &seq);

            // if packet commitment still exists on the original sending chain, then packet ack is unreceived
            // since processing the ack will delete the packet commitment
            if let Ok(_) = cmt {
                unreceived_seqs.push(seq);
            }
        }

        Ok(unreceived_seqs)
    }

    fn query_next_sequence_receive(
        &self,
        request: QueryNextSequenceReceiveRequest,
        include_proof: IncludeProof,
    ) -> Result<(Sequence, Option<MerkleProof>), Error> {
        tracing::trace!("in near: [query_next_sequence_receive] ");

        let QueryNextSequenceReceiveRequest {
            port_id,
            channel_id,
            height,
        } = request;

        let next_sequence_receive = self
            .get_next_sequence_receive(&port_id, &channel_id)
            .map_err(|_| Error::report_error("query_next_sequence_receive".to_string()))?;

        let next_sequence_receive_path =
            SeqRecvsPath(port_id.clone(), channel_id.clone()).to_string();

        match include_proof {
            IncludeProof::Yes => {
                let query_height = match height {
                    QueryHeight::Latest => {
                        let height = self
                            .get_latest_height()
                            .map_err(|_| Error::report_error("query_latest_height".to_string()))?;
                        height
                    }
                    QueryHeight::Specific(value) => value,
                };
                Ok((
                    next_sequence_receive,
                    Some(self.generate_storage_proof(
                        vec![next_sequence_receive_path.as_bytes()],
                        &query_height,
                        "NextSequenceRecv",
                    )?),
                ))
            }
            IncludeProof::No => Ok((next_sequence_receive, None)),
        }
    }

    fn query_txs(&self, request: QueryTxRequest) -> Result<Vec<IbcEventWithHeight>, Error> {
        tracing::trace!("in near: [query_txs]");

        match request {
            QueryTxRequest::Client(request) => {
                use ibc_relayer_types::core::ics02_client::events::Attributes;
                // Todo: the client event below is mock
                // replace it with real client event replied from a near chain
                // todo(davirian)
                let result: Vec<IbcEventWithHeight> = vec![IbcEventWithHeight {
                    event: IbcEvent::UpdateClient(
                        ibc_relayer_types::core::ics02_client::events::UpdateClient::from(
                            Attributes {
                                client_id: request.client_id,
                                client_type: ClientType::Near,
                                consensus_height: request.consensus_height,
                            },
                        ),
                    ),
                    height: Height::new(0, 9).unwrap(),
                }];

                Ok(result)
            }

            QueryTxRequest::Transaction(_tx) => {
                // Todo: https://github.com/octopus-network/ibc-rs/issues/98
                let result: Vec<IbcEventWithHeight> = vec![];
                Ok(result)
            }
        }
    }

    fn query_packet_events(
        &self,
        _request: QueryPacketEventDataRequest,
    ) -> Result<Vec<IbcEventWithHeight>, Error> {
        todo!()
    }

    fn query_host_consensus_state(
        &self,
        _request: QueryHostConsensusStateRequest,
    ) -> Result<Self::ConsensusState, Error> {
        tracing::trace!("in near: [query_host_consensus_state]");

        Ok(Self::ConsensusState::default())
    }

    fn build_client_state(
        &self,
        height: ICSHeight,
        _dst_config: ClientSettings,
    ) -> Result<Self::ClientState, Error> {
        tracing::trace!("in near: [build_client_state]");
        let pk = self.get_solomachine_pubkey();
        let duration_since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let timestamp_nanos = duration_since_epoch.as_nanos(); // u128
        Ok(SmClientState {
            sequence: height.revision_height(),
            frozen_sequence: 0,
            consensus_state: SmConsensusState {
                public_key: pk,
                diversifier: "oct".to_string(),
                timestamp: timestamp_nanos as u64,
                root: CommitmentRoot::from_bytes(&pk.to_bytes()),
            },
            allow_update_after_proposal: false,
        })
    }

    fn build_consensus_state(
        &self,
        light_block: Self::LightBlock,
    ) -> Result<Self::ConsensusState, Error> {
        tracing::trace!(
            "in near: [build_consensus_state] light_block:{:?}",
            light_block
        );
        let pk = PublicKey(
            tendermint::PublicKey::from_raw_secp256k1(&hex_literal::hex!(
                "02c88aca653727db28e0ade87497c1f03b551143dedfd4db8de71689ad5e38421c"
            ))
            .unwrap(),
        );
        let duration_since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let timestamp_nanos = duration_since_epoch.as_nanos(); // u128

        Ok(SmConsensusState {
            public_key: pk,
            diversifier: "oct".to_string(),
            // timestamp: timestamp_nanos as u64,
            timestamp: 9999,
            root: CommitmentRoot::from_bytes(&pk.to_bytes()),
        })
    }

    fn build_header(
        &mut self,
        trusted_height: ICSHeight,
        target_height: ICSHeight,
        client_state: &AnyClientState,
    ) -> Result<(Self::Header, Vec<Self::Header>), Error> {
        tracing::trace!("in near [build_header]");

        let pk = PublicKey(
            tendermint::PublicKey::from_raw_secp256k1(&hex_literal::hex!(
                "02c88aca653727db28e0ade87497c1f03b551143dedfd4db8de71689ad5e38421c"
            ))
            .unwrap(),
        );
        let duration_since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let timestamp_nanos = duration_since_epoch
            .checked_sub(Duration::from_secs(5))
            .unwrap()
            .as_nanos() as u64; // u128
        let data = SmHeaderData {
            new_pub_key: Some(pk),
            new_diversifier: "oct".to_string(),
        };

        let bytes = SignBytes {
            sequence: client_state.latest_height().revision_height(),
            timestamp: timestamp_nanos,
            diversifier: "oct".to_string(),
            data_type: 9,
            // data: Protobuf::<SmHeaderData>::encode_vec(&data).expect("encoding to `Any` from `HeaderData`"),
            data: data.encode_vec().unwrap(),
        };

        let encoded_bytes = bytes.encode_vec().unwrap();
        let standard = StandardHDPath::from_str("m/44'/60'/0'/0/0").unwrap();

        // m/44'/60'/0'/0/0
        // 0xd73E35f53b8180b241E70C0e9040173dd8D0e2A0
        // 0x02c88aca653727db28e0ade87497c1f03b551143dedfd4db8de71689ad5e38421c
        // 0x281afd44d50ffd0bab6502cbb9bc58a7f9b53813c862db01836d46a27b51168c

        let key_pair = Secp256k1KeyPair::from_mnemonic("captain walk infant web eye return ahead once face sunny usage devote cotton car old check symbol antique derive wire kid solve forest fish", &standard, &AddressType::Cosmos, "oct").unwrap();
        let signature = key_pair.sign(&encoded_bytes).unwrap();

        let header = SmHeader {
            sequence: client_state.latest_height().revision_height(),
            timestamp: timestamp_nanos,
            signature,
            new_public_key: Some(pk),
            new_diversifier: "oct".to_string(),
        };
        Ok((header, vec![]))
    }

    fn maybe_register_counterparty_payee(
        &mut self,
        _channel_id: &ChannelId,
        _port_id: &PortId,
        _counterparty_payee: &Signer,
    ) -> Result<(), Error> {
        todo!()
    }

    fn subscribe(&mut self) -> std::result::Result<Subscription, Error> {
        todo!()
    }

    fn query_consensus_state_heights(
        &self,
        request: QueryConsensusStateHeightsRequest,
    ) -> std::result::Result<Vec<Height>, Error> {
        todo!()
    }

    fn cross_chain_query(
        &self,
        requests: Vec<CrossChainQueryRequest>,
    ) -> std::result::Result<Vec<CrossChainQueryResponse>, Error> {
        todo!()
    }
}

fn collect_ibc_event_by_outcome(outcome: FinalExecutionOutcomeView) -> Vec<IbcEventWithHeight> {
    let mut ibc_events = vec![];
    for receipt_outcome in outcome.receipts_outcome {
        for log in receipt_outcome.outcome.logs {
            if log.starts_with("EVENT_JSON:") {
                // serde_json::value::Value::from_str()
                // serde_json::to_value()
                let event = log.replace("EVENT_JSON:", "");
                let event_value = serde_json::value::Value::from_str(event.as_str()).unwrap();
                if event_value["standard"].eq("near-ibc") {
                    let ibc_event: ibc::events::IbcEvent =
                        serde_json::from_value(event_value["raw-ibc-event"].clone()).unwrap();
                    let block_height = u64::from_str(
                        event_value["block_height"]
                            .as_str()
                            .expect("Failed to get block_height field."),
                    )
                    .expect("Failed to parse block_height field.");
                    let epoch_height = u64::from_str(
                        event_value["epoch_height"]
                            .as_str()
                            .expect("Failed to get epoch_height field."),
                    )
                    .expect("Failed to parse epoch_height field.");
                    ibc_events.push(IbcEventWithHeight {
                        event: convert_ibc_event_to_hermes_ibc_event(ibc_event),
                        height: Height::new(epoch_height, block_height).unwrap(),
                    })
                }
            }
        }
    }
    ibc_events
}
