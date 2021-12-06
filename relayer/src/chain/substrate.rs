use super::{ChainEndpoint, HealthCheck};
use crate::config::ChainConfig;
use crate::error::Error;
// use crate::event::monitor::{EventMonitor, EventReceiver, TxMonitorCmd};
use crate::event::substrate_mointor::{EventMonitor, EventReceiver, TxMonitorCmd};
use crate::keyring::{KeyEntry, KeyRing, Store};
use crate::light_client::LightClient;
use ibc::events::IbcEvent;
use ibc::ics02_client::client_consensus::{AnyConsensusState, AnyConsensusStateWithHeight};
use ibc::ics02_client::client_state::{AnyClientState, IdentifiedAnyClientState};
use ibc::ics03_connection::connection::{ConnectionEnd, IdentifiedConnectionEnd, Counterparty};
use ibc::ics04_channel::channel::{ChannelEnd, IdentifiedChannelEnd};
use ibc::ics04_channel::error::Error as Ics04Error;
use ibc::ics04_channel::packet::{PacketMsgType, Sequence, Packet, Receipt};
use ibc::ics10_grandpa::client_state::ClientState as GPClientState;
use ibc::ics10_grandpa::consensus_state::ConsensusState as GPConsensusState;
use ibc::ics10_grandpa::header::Header as GPHeader;
use ibc::ics23_commitment::commitment::{CommitmentPrefix, CommitmentRoot};
use ibc::ics24_host::identifier::{ChainId, ChannelId, ClientId, ConnectionId, PortId};
use ibc::query::QueryTxRequest;
use ibc::signer::Signer;
use ibc::Height;
use ibc::Height as ICSHeight;
use ibc_proto::ibc::core::channel::v1::{
    PacketState, QueryChannelClientStateRequest, QueryChannelsRequest,
    QueryConnectionChannelsRequest, QueryNextSequenceReceiveRequest,
    QueryPacketAcknowledgementsRequest, QueryPacketCommitmentsRequest, QueryUnreceivedAcksRequest,
    QueryUnreceivedPacketsRequest,
};
use ibc_proto::ibc::core::client::v1::{QueryClientStatesRequest, QueryConsensusStatesRequest};
use ibc_proto::ibc::core::commitment::v1::MerkleProof;
use ibc_proto::ibc::core::connection::v1::{
    QueryClientConnectionsRequest, QueryConnectionsRequest,
};
use prost_types::Any;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::runtime::Runtime as TokioRuntime;
use crate::light_client::grandpa::LightClient as GPLightClient;
use std::thread;
use tendermint::account::Id as AccountId;
use bitcoin::hashes::hex::ToHex;
use std::str::FromStr;
use bech32::{ToBase32, Variant};
use std::future::Future;
use substrate_subxt::{ClientBuilder, PairSigner, Client, EventSubscription, system::ExtrinsicSuccessEvent};
use calls::{ibc::DeliverCallExt, NodeRuntime};
use sp_keyring::AccountKeyring;
use calls::ibc::{
    NewBlockEvent,
    CreateClientEvent, OpenInitConnectionEvent, UpdateClientEvent, ClientMisbehaviourEvent,
    OpenTryConnectionEvent, OpenAckConnectionEvent, OpenConfirmConnectionEvent,
    OpenInitChannelEvent, OpenTryChannelEvent, OpenAckChannelEvent,
    OpenConfirmChannelEvent,  CloseInitChannelEvent, CloseConfirmChannelEvent,
    SendPacketEvent, ReceivePacketEvent, WriteAcknowledgementEvent,
    AcknowledgePacketEvent, TimeoutPacketEvent, TimeoutOnClosePacketEvent,
    EmptyEvent, ChainErrorEvent, PacketReceiptStore,
};
use calls::ibc::{ClientStatesStoreExt, ConnectionsStoreExt, ConsensusStatesStoreExt, ChannelsStoreExt,
                 ConnectionClientStoreExt, ChannelsConnectionStoreExt, PacketReceiptStoreExt, SendPacketEventStoreExt,
                 PacketCommitmentStoreExt
};
use codec::{Decode, Encode};
use substrate_subxt::sp_runtime::traits::BlakeTwo256;
use substrate_subxt::sp_runtime::generic::Header;
use tendermint_proto::Protobuf;
use std::thread::sleep;
use std::time::Duration;
use std::sync::mpsc::channel;
use ibc::ics02_client::client_type::ClientType;
use tendermint_rpc::endpoint::broadcast::tx_sync::Response as TxResponse;
use tendermint::abci::{Code, Log};
use tendermint::abci::transaction::Hash;
use chrono::offset::Utc;
use tokio::task;


#[derive(Debug)]
pub struct SubstrateChain {
    config: ChainConfig,
    websocket_url: String,
    rt: Arc<TokioRuntime>,
}

impl SubstrateChain {
    pub fn config(&self) -> &ChainConfig {
        &self.config
    }

    /// Run a future to completion on the Tokio runtime.
    fn block_on<F: Future>(&self, f: F) -> F::Output {
        crate::time!("block_on");
        self.rt.block_on(f)
    }

    /// Subscribe Events
    async fn subscribe_events(
        &self,
        client: Client<NodeRuntime>,
    ) -> Result<Vec<IbcEvent>, Box<dyn std::error::Error>> {
        const COUNTER_SYSTEM_EVENT: i32 = 10;
        tracing::info!("In substrate: [subscribe_events]");

        let sub = client.subscribe_finalized_events().await?;
        let decoder = client.events_decoder();
        let mut sub = EventSubscription::<NodeRuntime>::new(sub, decoder);

        let mut events = Vec::new();
        let mut counter_system_event = 0;
        while let Some(raw_event) = sub.next().await {
            if let Err(err) = raw_event {
                println!("In substrate: [subscribe_events] >> raw_event error: {:?}", err);
                continue;
            }
            let raw_event = raw_event.unwrap();
            // tracing::info!("In substrate: [subscribe_events] >> raw Event: {:?}", raw_event);
            let variant = raw_event.variant;
            // tracing::info!("In substrate: [subscribe_events] >> variant: {:?}", variant);
            match variant.as_str() {
                "CreateClient" => {
                    let event = CreateClientEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> CreateClient Event");

                    let height = event.height;
                    let client_id = event.client_id;
                    let client_type = event.client_type;
                    let consensus_height = event.consensus_height;
                    use ibc::ics02_client::events::Attributes;
                    events.push(IbcEvent::CreateClient(ibc::ics02_client::events::CreateClient(Attributes {
                        height: height.to_ibc_height(),
                        client_id: client_id.to_ibc_client_id(),
                        client_type: client_type.to_ibc_client_type(),
                        consensus_height: consensus_height.to_ibc_height()
                    })));
                    break;
                },
                "UpdateClient" => {
                    let event = UpdateClientEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> UpdateClient Event");

                    let height = event.height;
                    let client_id = event.client_id;
                    let client_type = event.client_type;
                    let consensus_height = event.consensus_height;
                    use ibc::ics02_client::events::Attributes;
                    events.push(IbcEvent::UpdateClient(ibc::ics02_client::events::UpdateClient{
                        common: Attributes {
                            height: height.to_ibc_height(),
                            client_id: client_id.to_ibc_client_id(),
                            client_type: client_type.to_ibc_client_type(),
                            consensus_height: consensus_height.to_ibc_height(),
                        },
                        header: None,
                    }));
                    // break;
                },
                "ClientMisbehaviour" => {
                    let event = ClientMisbehaviourEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> ClientMisbehaviour Event");

                    let height = event.height;
                    let client_id = event.client_id;
                    let client_type = event.client_type;
                    let consensus_height = event.consensus_height;
                    use ibc::ics02_client::events::Attributes;
                    events.push(IbcEvent::ClientMisbehaviour(ibc::ics02_client::events::ClientMisbehaviour(
                        Attributes {
                            height: height.to_ibc_height(),
                            client_id: client_id.to_ibc_client_id(),
                            client_type: client_type.to_ibc_client_type(),
                            consensus_height: consensus_height.to_ibc_height(),
                        }
                    )));
                    // break;
                },
                "OpenInitConnection" => {
                    let event = OpenInitConnectionEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> OpenInitConnection Event");

                    let height = event.height;
                    let connection_id = event.connection_id.map(|val| val.to_ibc_connection_id());
                    let client_id = event.client_id;
                    let counterparty_connection_id = event.counterparty_connection_id.map(|val| val.to_ibc_connection_id());
                    let counterparty_client_id = event.counterparty_client_id;
                    use ibc::ics03_connection::events::Attributes;
                    events.push(IbcEvent::OpenInitConnection(ibc::ics03_connection::events::OpenInit(Attributes {
                        height: height.to_ibc_height(),
                        connection_id,
                        client_id: client_id.to_ibc_client_id(),
                        counterparty_connection_id,
                        counterparty_client_id: counterparty_client_id.to_ibc_client_id(),
                    })));
                    sleep(Duration::from_secs(10));
                    break;
                },
                "OpenTryConnection" => {
                    let event = OpenTryConnectionEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> OpenTryConnection Event");

                    let height = event.height;
                    let connection_id = event.connection_id.map(|val| val.to_ibc_connection_id());
                    let client_id = event.client_id;
                    let counterparty_connection_id = event.counterparty_connection_id.map(|val| val.to_ibc_connection_id());
                    let counterparty_client_id = event.counterparty_client_id;
                    use ibc::ics03_connection::events::Attributes;
                    events.push(IbcEvent::OpenTryConnection(ibc::ics03_connection::events::OpenTry(Attributes {
                        height: height.to_ibc_height(),
                        connection_id,
                        client_id: client_id.to_ibc_client_id(),
                        counterparty_connection_id,
                        counterparty_client_id: counterparty_client_id.to_ibc_client_id(),
                    })));
                    sleep(Duration::from_secs(10));
                    break;
                },
                "OpenAckConnection" => {
                    let event = OpenAckConnectionEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> OpenAckConnection Event");

                    let height = event.height;
                    let connection_id = event.connection_id.map(|val| val.to_ibc_connection_id());
                    let client_id = event.client_id;
                    let counterparty_connection_id = event.counterparty_connection_id.map(|val| val.to_ibc_connection_id());
                    let counterparty_client_id = event.counterparty_client_id;
                    use ibc::ics03_connection::events::Attributes;
                    events.push(IbcEvent::OpenAckConnection(ibc::ics03_connection::events::OpenAck(Attributes {
                        height: height.to_ibc_height(),
                        connection_id,
                        client_id: client_id.to_ibc_client_id(),
                        counterparty_connection_id,
                        counterparty_client_id: counterparty_client_id.to_ibc_client_id(),
                    })));
                    sleep(Duration::from_secs(10));
                    break;
                },
                "OpenConfirmConnection" => {
                    let event = OpenConfirmConnectionEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> OpenConfirmConnection Event");

                    let height = event.height;
                    let connection_id = event.connection_id.map(|val| val.to_ibc_connection_id());
                    let client_id = event.client_id;
                    let counterparty_connection_id = event.counterparty_connection_id.map(|val| val.to_ibc_connection_id());
                    let counterparty_client_id = event.counterparty_client_id;
                    use ibc::ics03_connection::events::Attributes;
                    events.push(IbcEvent::OpenConfirmConnection(ibc::ics03_connection::events::OpenConfirm(Attributes {
                        height: height.to_ibc_height(),
                        connection_id,
                        client_id: client_id.to_ibc_client_id(),
                        counterparty_connection_id,
                        counterparty_client_id: counterparty_client_id.to_ibc_client_id(),
                    })));
                    sleep(Duration::from_secs(10));
                    break;
                }

                "OpenInitChannel" => {
                    let event = OpenInitChannelEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> OpenInitChannel Event");

                    let height = event.height;
                    let port_id = event.port_id;
                    let channel_id = event.channel_id.map(|val| val.to_ibc_channel_id());
                    let connection_id = event.connection_id;
                    let counterparty_port_id = event.counterparty_port_id;
                    let counterparty_channel_id = event.counterparty_channel_id.map(|val| val.to_ibc_channel_id());
                    use ibc::ics04_channel::events::Attributes;
                    events.push(IbcEvent::OpenInitChannel(ibc::ics04_channel::events::OpenInit(Attributes{
                        height: height.to_ibc_height(),
                        port_id: port_id.to_ibc_port_id(),
                        channel_id: channel_id,
                        connection_id: connection_id.to_ibc_connection_id(),
                        counterparty_port_id: counterparty_port_id.to_ibc_port_id(),
                        counterparty_channel_id: counterparty_channel_id,
                    })));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "OpenTryChannel" => {
                    let event = OpenTryChannelEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> OpenTryChannel Event");

                    let height = event.height;
                    let port_id = event.port_id;
                    let channel_id = event.channel_id.map(|val| val.to_ibc_channel_id());
                    let connection_id = event.connection_id;
                    let counterparty_port_id = event.counterparty_port_id;
                    let counterparty_channel_id = event.counterparty_channel_id.map(|val| val.to_ibc_channel_id());
                    use ibc::ics04_channel::events::Attributes;
                    events.push(IbcEvent::OpenTryChannel(ibc::ics04_channel::events::OpenTry(Attributes{
                        height: height.to_ibc_height(),
                        port_id: port_id.to_ibc_port_id(),
                        channel_id: channel_id,
                        connection_id: connection_id.to_ibc_connection_id(),
                        counterparty_port_id: counterparty_port_id.to_ibc_port_id(),
                        counterparty_channel_id: counterparty_channel_id,
                    })));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "OpenAckChannel" => {
                    let event = OpenAckChannelEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> OpenAckChannel Event");

                    let height = event.height;
                    let port_id = event.port_id;
                    let channel_id = event.channel_id.map(|val| val.to_ibc_channel_id());
                    let connection_id = event.connection_id;
                    let counterparty_port_id = event.counterparty_port_id;
                    let counterparty_channel_id = event.counterparty_channel_id.map(|val| val.to_ibc_channel_id());
                    use ibc::ics04_channel::events::Attributes;
                    events.push(IbcEvent::OpenAckChannel(ibc::ics04_channel::events::OpenAck(Attributes{
                        height: height.to_ibc_height(),
                        port_id: port_id.to_ibc_port_id(),
                        channel_id: channel_id,
                        connection_id: connection_id.to_ibc_connection_id(),
                        counterparty_port_id: counterparty_port_id.to_ibc_port_id(),
                        counterparty_channel_id: counterparty_channel_id,
                    })));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "OpenConfirmChannel" => {
                    let event = OpenConfirmChannelEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> OpenConfirmChannel Event");

                    let height = event.height;
                    let port_id = event.port_id;
                    let channel_id = event.channel_id.map(|val| val.to_ibc_channel_id());
                    let connection_id = event.connection_id;
                    let counterparty_port_id = event.counterparty_port_id;
                    let counterparty_channel_id = event.counterparty_channel_id.map(|val| val.to_ibc_channel_id());
                    use ibc::ics04_channel::events::Attributes;
                    events.push(IbcEvent::OpenConfirmChannel(ibc::ics04_channel::events::OpenConfirm(Attributes{
                        height: height.to_ibc_height(),
                        port_id: port_id.to_ibc_port_id(),
                        channel_id: channel_id,
                        connection_id: connection_id.to_ibc_connection_id(),
                        counterparty_port_id: counterparty_port_id.to_ibc_port_id(),
                        counterparty_channel_id: counterparty_channel_id,
                    })));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "CloseInitChannel" => {
                    let event = CloseInitChannelEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> CloseInitChannel Event");

                    let height = event.height;
                    let port_id = event.port_id;
                    let channel_id = event.channel_id.map(|val| val.to_ibc_channel_id());
                    let connection_id = event.connection_id;
                    let counterparty_port_id = event.counterparty_port_id;
                    let counterparty_channel_id = event.counterparty_channel_id.map(|val| val.to_ibc_channel_id());
                    use ibc::ics04_channel::events::Attributes;
                    events.push(IbcEvent::CloseInitChannel(ibc::ics04_channel::events::CloseInit(Attributes{
                        height: height.to_ibc_height(),
                        port_id: port_id.to_ibc_port_id(),
                        channel_id: channel_id,
                        connection_id: connection_id.to_ibc_connection_id(),
                        counterparty_port_id: counterparty_port_id.to_ibc_port_id(),
                        counterparty_channel_id: counterparty_channel_id,
                    })));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "CloseConfirmChannel" => {
                    let event = CloseConfirmChannelEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> CloseConfirmChannel Event");

                    let height = event.height;
                    let port_id = event.port_id;
                    let channel_id = event.channel_id.map(|val| val.to_ibc_channel_id());
                    let connection_id = event.connection_id;
                    let counterparty_port_id = event.counterparty_port_id;
                    let counterparty_channel_id = event.counterparty_channel_id.map(|val| val.to_ibc_channel_id());
                    use ibc::ics04_channel::events::Attributes;
                    events.push(IbcEvent::CloseConfirmChannel(ibc::ics04_channel::events::CloseConfirm(Attributes{
                        height: height.to_ibc_height(),
                        port_id: port_id.to_ibc_port_id(),
                        channel_id: channel_id,
                        connection_id: connection_id.to_ibc_connection_id(),
                        counterparty_port_id: counterparty_port_id.to_ibc_port_id(),
                        counterparty_channel_id: counterparty_channel_id,
                    })));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "SendPacket" => {
                    let event = SendPacketEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [substrate_events] >> SendPacket Event");

                    let height = event.height;
                    let packet = event.packet;
                    events.push(IbcEvent::SendPacket(ibc::ics04_channel::events::SendPacket{
                        height: height.to_ibc_height(),
                        packet: packet.to_ibc_packet(),
                    }));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "ReceivePacket" => {
                    let event = ReceivePacketEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [substrate_events] >> ReceivePacket Event");

                    let height = event.height;
                    let packet = event.packet;
                    events.push(IbcEvent::ReceivePacket(ibc::ics04_channel::events::ReceivePacket{
                        height: height.to_ibc_height(),
                        packet: packet.to_ibc_packet(),
                    }));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "WriteAcknowledgement" => {
                    let event = WriteAcknowledgementEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [substrate_events] >> WriteAcknowledgement Event");

                    let height = event.height;
                    let packet = event.packet;
                    events.push(IbcEvent::WriteAcknowledgement(ibc::ics04_channel::events::WriteAcknowledgement{
                        height: height.to_ibc_height(),
                        packet: packet.to_ibc_packet(),
                        ack: event.ack,
                    }));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "AcknowledgePacket" => {
                    let event = AcknowledgePacketEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [substrate_events] >> AcknowledgePacket Event");

                    let height = event.height;
                    let packet = event.packet;
                    events.push(IbcEvent::AcknowledgePacket(ibc::ics04_channel::events::AcknowledgePacket{
                        height: height.to_ibc_height(),
                        packet: packet.to_ibc_packet(),
                    }));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "TimeoutPacket" => {
                    let event = TimeoutPacketEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [substrate_events] >> TimeoutPacket Event");

                    let height = event.height;
                    let packet = event.packet;
                    events.push(IbcEvent::TimeoutPacket(ibc::ics04_channel::events::TimeoutPacket{
                        height: height.to_ibc_height(),
                        packet: packet.to_ibc_packet(),
                    }));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "TimeoutOnClosePacket" => {
                    let event = TimeoutOnClosePacketEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [substrate_events] >> TimeoutOnClosePacket Event");

                    let height = event.height;
                    let packet = event.packet;
                    events.push(IbcEvent::TimeoutOnClosePacket(ibc::ics04_channel::events::TimeoutOnClosePacket{
                        height: height.to_ibc_height(),
                        packet: packet.to_ibc_packet(),
                    }));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "Empty" => {
                    let event =  EmptyEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("in substrate: [substrate_events] >> Empty Event");

                    let data = String::from_utf8(event.data).unwrap();
                    events.push(IbcEvent::Empty(data));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "ChainError" => {
                    let event =  ChainErrorEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("in substrate: [substrate_events] >> ChainError Event");

                    let data = String::from_utf8(event.data).unwrap();
                    events.push(IbcEvent::Empty(data));
                    sleep(Duration::from_secs(10));
                    break;
                }
                "ExtrinsicSuccess" => {
                    let event = ExtrinsicSuccessEvent::<NodeRuntime>::decode(&mut &raw_event.data[..]).unwrap();
                    tracing::info!("In substrate: [subscribe_events] >> SystemEvent ");
                    if counter_system_event < COUNTER_SYSTEM_EVENT {
                        tracing::info!("In substrate: [subscribe_events] >> counter_system_event: {:?}", counter_system_event);
                        counter_system_event += 1;
                    } else {
                        tracing::info!("In substrate: [subscribe_events] >> counter_system_event: {:?}", counter_system_event);
                        break;
                    }
                }
                _ =>  {
                    tracing::info!("In substrate: [subscribe_events] >> Unknown event");
                }
            }
        }
        Ok(events)
    }

    /// get latest height used by subscribe_blocks
    async fn get_latest_height(&self, client: Client<NodeRuntime>) -> Result<u64, Box<dyn std::error::Error>> {
        tracing::info!("In Substrate: [get_latest_height]");
        let mut blocks = client.subscribe_finalized_blocks().await?;
        // let mut blocks = client.subscribe_blocks().await?;
        let height= match blocks.next().await {
            Ok(Some(header)) => {
                header.number as u64
            },
            Ok(None) => {
                tracing::info!("In Substrate: [get_latest_height] >> None");
                0
            },
            Err(err) =>  {
              tracing::info!(" In substrate: [get_latest_height] >> error: {:?} ", err);
                0
            },
        };
        tracing::info!("In Substrate: [get_latest_height] >> height: {:?}", height);
        Ok(height)
    }

    /// get connectionEnd according by connection_identifier and read Connections StorageMaps
    async fn get_connectionend(&self, connection_identifier: &ConnectionId, client: Client<NodeRuntime>)
        -> Result<ConnectionEnd, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_connectionend]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();
        tracing::info!("In substrate: [get_connectionend] >> block_hash: {:?}", block_hash);

        let data = client.connections(connection_identifier.as_bytes().to_vec(), Some(block_hash)).await?;
        let connection_end = ConnectionEnd::decode_vec(&*data).unwrap();

        Ok(connection_end)
    }

    /// get channelEnd according by port_identifier, channel_identifier and read Channles StorageMaps
    async fn get_channelend(&self, port_id: &PortId, channel_id: &ChannelId, client: Client<NodeRuntime>)
        -> Result<ChannelEnd, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_channelend]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();
        tracing::info!("In substrate: [get_channelend] >> block_hash: {:?}", block_hash);

        let data = client.channels((port_id.as_bytes().to_vec(), channel_id.as_bytes().to_vec()), Some(block_hash)).await?;
        let channel_end = ChannelEnd::decode_vec(&*data).unwrap();

        Ok(channel_end)
    }

    /// get packet receipt by port_id, channel_id and sequence
    async fn get_packet_receipt(&self, port_id: &PortId, channel_id: &ChannelId, seq: &Sequence, client: Client<NodeRuntime>)
                                -> Result<Receipt, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_packet_receipt]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();
        tracing::info!("In substrate: [get_packet_receipt] >> block_hash: {:?}", block_hash);

        let _seq = u64::from(*seq).encode();
        let data = client.packet_receipt((port_id.as_bytes().to_vec(), channel_id.as_bytes().to_vec(), _seq), Some(block_hash)).await?;
        let _data = String::from_utf8(data).unwrap();
        if _data.eq("Ok") {
            Ok(Receipt::Ok)
        } else {
            Err(format!("unrecognized packet receipt: {:?}", _data).into())
        }
    }

    /// get send packet event by port_id, channel_id and sequence
    async fn get_send_packet_event(&self, port_id: &PortId, channel_id: &ChannelId, seq: &Sequence, client: Client<NodeRuntime>)
                                   -> Result<Packet, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_send_packet_event]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();
        tracing::info!("In substrate: [get_send_packet_event] >> block_hash: {:?}", block_hash);

        let data = client.send_packet_event((
                                                port_id.as_bytes().to_vec(), channel_id.as_bytes().to_vec(), u64::from(*seq)),
                                            Some(block_hash)
        ).await?;
        let packet = Packet::decode_vec(&*data).unwrap();
        Ok(packet)
    }

    /// get client_state according by client_id, and read ClientStates StoraageMap
    async fn get_client_state(&self, client_id:  &ClientId, client: Client<NodeRuntime>)
        -> Result<AnyClientState, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_client_state]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();

        let data : Vec<u8> = client
            .client_states(
                client_id.as_bytes().to_vec(),
                Some(block_hash),
            )
            .await?;
        tracing::info!("in substrate [get_client_state]: client_state: {:?}",data);


        let client_state = AnyClientState::decode_vec(&*data).unwrap();
        tracing::info!("in substrate [get_client_state]: any_client_state : {:?}", client_state);
        // let client_state = match client_state {
        //     AnyClientState::Grandpa(client_state) => client_state,
        //     // AnyClientState::Tendermint(client_state) => client_state,
        //     _ => panic!("wrong client state type"),
        // };

        Ok(client_state)
    }

    /// get appoint height consensus_state according by client_identifier and height
    /// and read ConsensusStates StoreageMap
    async fn get_client_consensus(&self, client_id:  &ClientId, height: ICSHeight, client: Client<NodeRuntime>)
        -> Result<AnyConsensusState, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_client_consensus]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();
        tracing::info!("In substrate: [get_client_consensus] >> block_hash: {:?}", block_hash);

        let data = client
            .consensus_states(
                client_id.as_bytes().to_vec(),
                Some(block_hash),
            )
            .await?;

        // get the height consensus_state
        let mut consensus_state = vec![];
        for item in data.iter() {
            if item.0 == height.encode_vec().unwrap() {
                consensus_state = item.1.clone();
            }
        }

        let consensus_state = AnyConsensusState::decode_vec(&*consensus_state).unwrap();
        // let consensus_state = match consensus_state {
        //     AnyConsensusState::Grandpa(consensus_state) => consensus_state,
        //     _ => panic!("wrong consensus_state type"),
        // };

        Ok(consensus_state)
    }


    async fn get_consensus_state_with_height(&self, client_id: &ClientId, client: Client<NodeRuntime>)
        -> Result<Vec<(Height, AnyConsensusState)>, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_consensus_state_with_height]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();
        tracing::info!("In substrate: [get_client_consensus] >> block_hash: {:?}", block_hash);

        // vector<height, consensus_state>
        let ret : Vec<(Vec<u8>, Vec<u8>)> = client
            .consensus_states(
                client_id.as_bytes().to_vec(),
                Some(block_hash),
            ).await?;

        let mut result = vec![];
        for (height, consensus_state) in ret.iter() {
            let height = Height::decode_vec(&*height).unwrap();
            let consensus_state = AnyConsensusState::decode_vec(&*consensus_state).unwrap();
            // let consensus_state = match consensus_state {
            //     AnyConsensusState::Grandpa(consensus_state) => consensus_state,
            //     _ => panic!("wrong consensus_state type"),
            // };
            result.push((height, consensus_state));
        }

        Ok(result)
    }

    async fn get_unreceipt_packet(&self, port_id:  &PortId, channel_id: &ChannelId, seqs: Vec<u64>, client: Client<NodeRuntime>)
         -> Result<Vec<u64>, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_unreceipt_packet]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();
        tracing::info!("In substrate: [get_unreceipt_packet] >> block_hash: {:?}", block_hash);

        let mut result = Vec::new();

        let pair = seqs
            .into_iter()
            .map(|seq| (port_id.clone().as_bytes().to_vec(), channel_id.clone().as_bytes().to_vec(), (seq.encode(), seq)))
            .collect::<Vec<_>>();
        for (port_id, channel_id, (seq_u8, seq)) in pair.into_iter() {
            let ret : Vec<u8> = client
                .packet_receipt(
                    (port_id, channel_id, seq_u8),
                    Some(block_hash.clone()),
                ).await?;
            if ret.is_empty() {
                result.push(seq);
            }
        }

        Ok(result)
    }

    /// get key-value pair (client_identifier, client_state) construct IdentifieredAnyClientstate
    async fn get_clients(&self, client: Client<NodeRuntime>)
        -> Result<Vec<IdentifiedAnyClientState>, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_clients]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();

        let rpc_client = client.rpc_client();

        let ret: Vec<(Vec<u8>, Vec<u8>)> = rpc_client.request("get_identified_any_client_state", &[]).await?;

        let mut result = vec![];

        for (client_id, client_state) in ret.iter() {
            let client_id_str = String::from_utf8(client_id.clone()).unwrap();
            let client_id = ClientId::from_str(client_id_str.as_str()).unwrap();

            let client_state = AnyClientState::decode_vec(&*client_state).unwrap();

            result.push(IdentifiedAnyClientState::new(client_id, client_state));
        }

        Ok(result)
    }

    /// get key-value pair (connection_id, connection_end) construct IdentifiedConnectionEnd
    async fn get_connctions(&self, client: Client<NodeRuntime>)
                         -> Result<Vec<IdentifiedConnectionEnd>, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_connctions]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();

        let rpc_client = client.rpc_client();

        let ret: Vec<(Vec<u8>, Vec<u8>)> = rpc_client.request("get_idenfitied_connection_end", &[]).await?;

        let mut result = vec![];

        for (connection_id, connection_end) in ret.iter() {
            let connection_id_str = String::from_utf8(connection_id.clone()).unwrap();
            let connection_id = ConnectionId::from_str(connection_id_str.as_str()).unwrap();

            let connnection_end = ConnectionEnd::decode_vec(&*connection_end).unwrap();

            result.push(IdentifiedConnectionEnd::new(connection_id, connnection_end));
        }

        Ok(result)
    }


    /// get key-value pair (connection_id, connection_end) construct IdentifiedConnectionEnd
    async fn get_channels(&self, client: Client<NodeRuntime>)
        -> Result<Vec<IdentifiedChannelEnd>, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_channels]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();

        let rpc_client = client.rpc_client();

        let ret: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = rpc_client.request("get_idenfitied_channel_end", &[]).await?;

        let mut result = vec![];

        for (port_id, channel_id, channel_end) in ret.iter() {
            let port_id_str = String::from_utf8(port_id.clone()).unwrap();
            let port_id = PortId::from_str(port_id_str.as_str()).unwrap();

            let channel_id_str = String::from_utf8(channel_id.clone()).unwrap();
            let channel_id = ChannelId::from_str(channel_id_str.as_str()).unwrap();

            let channel_end = ChannelEnd::decode_vec(&*channel_end).unwrap();

            result.push(IdentifiedChannelEnd::new(port_id, channel_id, channel_end));
        }

        Ok(result)
    }

    // get get_commitment_packet_state
    async fn get_commitment_packet_state(&self, client: Client<NodeRuntime>)
        -> Result<Vec<PacketState>, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_commitment_packet_state]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();

        let rpc_client = client.rpc_client();

        let ret: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)> = rpc_client.request("get_packet_commitment_state", &[]).await?;

        let mut result = vec![];

        for (port_id, channel_id, seq, data) in ret.into_iter() {
            let port_id = String::from_utf8(port_id).unwrap();
            let channel_id = String::from_utf8(channel_id).unwrap();
            let mut seq: &[u8] = &seq;
            let seq = u64::decode(&mut seq).unwrap();
            let packet_state = PacketState {
                port_id: port_id,
                channel_id: channel_id,
                sequence: seq,
                data,
            };
            result.push(packet_state);
        }

        Ok(result)
    }

    /// get packet commitment by port_id, channel_id and sequence to verify if the ack has been received by the sending chain
    async fn get_packet_commitment(&self, port_id: &PortId, channel_id: &ChannelId, seq: u64, client: Client<NodeRuntime>)
                                   -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_packet_commitment]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();
        tracing::info!("In substrate: [get_packet_commitment] >> block_hash: {:?}", block_hash);

        let _seq = seq.encode();
        let data = client.packet_commitment((port_id.as_bytes().to_vec(), channel_id.as_bytes().to_vec(), _seq),
                                            Some(block_hash)
        ).await?;

        if data.is_empty() {
            Err(Box::new(Ics04Error::packet_commitment_not_found(Sequence(seq))))
        } else {
            Ok(data)
        }
    }

    // get get_commitment_packet_state
    async fn get_acknowledge_packet_state(&self, client: Client<NodeRuntime>)
        -> Result<Vec<PacketState>, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_acknowledge_packet_state]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();

        let rpc_client = client.rpc_client();

        let ret: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)> = rpc_client.request("get_packet_acknowledge_state", &[]).await?;

        let mut result = vec![];

        for (port_id, channel_id, seq, data) in ret.into_iter() {
            let port_id = String::from_utf8(port_id).unwrap();
            let channel_id = String::from_utf8(channel_id).unwrap();
            let mut seq: &[u8] = &seq;
            let seq = u64::decode(&mut seq).unwrap();
            let packet_state = PacketState {
                port_id: port_id,
                channel_id: channel_id,
                sequence: seq,
                data,
            };
            result.push(packet_state);
        }

        Ok(result)
    }

    /// get connection_identifier vector according by client_identifier
    async fn get_client_connections(&self, client_id: ClientId, client: Client<NodeRuntime>)
        -> Result<Vec<ConnectionId>, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_client_connections]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();
        tracing::info!("In substrate: [get_client_connections] >> block_hash: {:?}", block_hash);


        // client_id <-> connection_id
        let connection_id : Vec<u8> = client
            .connection_client(
                client_id.as_bytes().to_vec(),
                Some(block_hash),
            ).await?;
        if connection_id.is_empty() {
            return Ok(Vec::new());
        }

        let mut result = vec![];

        let connection_id_str = String::from_utf8(connection_id.clone()).unwrap();
        let connection_id = ConnectionId::from_str(connection_id_str.as_str()).unwrap();

        result.push(connection_id);

        Ok(result)
    }


    async fn get_connection_channels(&self, connection_id: ConnectionId, client: Client<NodeRuntime>)
        -> Result<Vec<IdentifiedChannelEnd>, Box<dyn std::error::Error>> {
        tracing::info!("in Substrate: [get_connection_channels]");

        let mut block = client.subscribe_finalized_blocks().await?;
        let block_header = block.next().await.unwrap().unwrap();

        let block_hash = block_header.hash();
        tracing::info!("In substrate: [get_client_connections] >> block_hash: {:?}", block_hash);


        // connection_id <-> Ve<(port_id, channel_id)>
        let channel_id_and_port_id : Vec<(Vec<u8>, Vec<u8>)> = client
            .channels_connection(
                connection_id.as_bytes().to_vec(),
                Some(block_hash),
            ).await?;

        let mut result = vec![];

        for (port_id, channel_id) in channel_id_and_port_id.iter() {
            // get port_id
            let port_id = String::from_utf8(port_id.clone()).unwrap();
            let port_id = PortId::from_str(port_id.as_str()).unwrap();

            // get channel_id
            let channel_id = String::from_utf8(channel_id.clone()).unwrap();
            let channel_id = ChannelId::from_str(channel_id.as_str()).unwrap();

            // get channel_end
            let channel_end = self.
                get_channelend(&port_id,&channel_id, client.clone()).await?;

            result.push(IdentifiedChannelEnd::new(port_id, channel_id, channel_end));
        }

        Ok(result)
    }

}

impl ChainEndpoint for SubstrateChain {
    type LightBlock = ();
    type Header = GPHeader;
    type ConsensusState = AnyConsensusState;
    type ClientState = AnyClientState;
    type LightClient = GPLightClient;

    fn bootstrap(config: ChainConfig, rt: Arc<TokioRuntime>) -> Result<Self, Error> {
        tracing::info!("in Substrate: [bootstrap function]");

        let websocket_url = format!("{}", config.websocket_addr.clone());
        tracing::info!("in Substrate: [bootstrap] websocket_url = {:?}", websocket_url);

        let chain = Self {
            config,
            websocket_url,
            rt,
        };

        Ok(chain)
    }

    fn init_light_client(&self) -> Result<Self::LightClient, Error> {
        tracing::info!("In Substrate: [init_light_client]");

        let light_client = GPLightClient::new();

        Ok(light_client)
    }

    fn init_event_monitor(
        &self,
        rt: Arc<TokioRuntime>,
    ) -> Result<(EventReceiver, TxMonitorCmd), Error> {
        tracing::info!("in Substrate: [init_event_mointor]");

        tracing::info!("In Substrate: [init_event_mointor] >> websocket addr: {:?}", self.config.websocket_addr.clone());

        let (mut event_monitor, event_receiver, monitor_tx) = EventMonitor::new(
            self.config.id.clone(),
            self.config.websocket_addr.clone(),
            rt,
        )
            .map_err(Error::event_monitor)?;

        event_monitor.subscribe().map_err(Error::event_monitor)?;

        thread::spawn(move || event_monitor.run());

        Ok((event_receiver, monitor_tx))
    }

    fn shutdown(self) -> Result<(), Error> {
        tracing::info!("in Substrate: [shutdown]");

        Ok(())
    }

    fn health_check(&self) -> Result<HealthCheck, Error> {

        Ok(HealthCheck::Healthy)
    }

    fn id(&self) -> &ChainId {
        tracing::info!("in Substrate: [id]");

        &self.config().id
    }

    fn keybase(&self) -> &KeyRing {
        tracing::info!("in Substrate: [keybase]");

        todo!()
    }

    fn keybase_mut(&mut self) -> &mut KeyRing {
        tracing::info!("in Substrate: [keybase_mut]");

        todo!()
    }

    fn send_messages_and_wait_commit(&mut self, proto_msgs: Vec<Any>) -> Result<Vec<IbcEvent>, Error> {
        tracing::info!("in Substrate: [send_messages_and_wait_commit]");

        let msg : Vec<pallet_ibc::Any> = proto_msgs.into_iter().map(|val| val.into()).collect();
        let signer = PairSigner::new(AccountKeyring::Bob.pair());
        let client = async {
                sleep(Duration::from_secs(3));

                let client = ClientBuilder::<NodeRuntime>::new()
                    .set_url(&self.websocket_url.clone())
                    .build().await.unwrap();

                let result = client.deliver(&signer, msg, 0).await;

                tracing::info!("in Substrate: [send_messages_and_wait_commit] >> result no unwrap: {:?}", result);

                let result = result.unwrap();
                tracing::info!("in Substrate: [send_messages_and_wait_commit] >> result : {:?}", result);

                result
        };

        let _ = self.block_on(client);

        let get_ibc_event = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let result = self.subscribe_events(client).await.unwrap();
            for event in result.iter() {
                tracing::info!("In Substrate: [send_messages_and_wait_commit] >> get_ibc_event: {:?}", event);
            }

            result
        };

        let ret = self.block_on(get_ibc_event);
        Ok(ret)
    }


    fn send_messages_and_wait_check_tx(
        &mut self,
        proto_msgs: Vec<Any>,
    ) -> Result<Vec<TxResponse>, Error> {
        tracing::info!("in Substrate: [send_messages_and_wait_check_tx]");
        tracing::debug!("in Substrate: [send_messages_and_wait_check_tx], raw msg to send {:?}", proto_msgs);
        let msg : Vec<pallet_ibc::Any> = proto_msgs.into_iter().map(|val| val.into()).collect();

        let signer = PairSigner::new(AccountKeyring::Bob.pair());
        tracing::debug!("in Substrate: [send_messages_and_wait_check_tx] >> signer: {:?}", "Bob");

        let client = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();

            let result = client.deliver(&signer, msg, 0).await;
            sleep(Duration::from_secs(10));  // For avoiding transaction low priority error, Todo:
            let result = result.unwrap();
            tracing::debug!("in Substrate: [send_messages_and_wait_check_tx] >> result : {:?}", result);
            result
        };
        let _result = self.block_on(client);

        use tendermint::abci::transaction;  // Todo:
        let json = "\"ChYKFGNvbm5lY3Rpb25fb3Blbl9pbml0\"";
        let txRe = TxResponse {
            code: Code::default(),
            data: serde_json::from_str(json).unwrap(),
            log: Log::from("testtest"),
            hash: transaction::Hash::new(*_result.as_fixed_bytes())
        };

        Ok(vec![txRe])
    }

    fn get_signer(&mut self) -> Result<Signer, Error> {
        tracing::info!("in Substrate: [get_signer]");
        tracing::info!("In Substraet: [get signer] >> key_name: {:?}", self.config.key_name.clone());

        fn get_dummy_account_id_raw() -> String {
            "0CDA3F47EF3C4906693B170EF650EB968C5F4B2C".to_string()
        }

        pub fn get_dummy_account_id() -> AccountId {
            AccountId::from_str(&get_dummy_account_id_raw()).unwrap()
        }

        let signer = Signer::new(get_dummy_account_id().to_string());
        tracing::info!("in Substrate: [get_signer] >>  signer {:?}", signer);

        Ok(signer)
    }

    fn get_key(&mut self) -> Result<KeyEntry, Error> {
        tracing::info!("in Substraet: [get_key]");

        todo!()
    }

    fn query_commitment_prefix(&self) -> Result<CommitmentPrefix, Error> {
        tracing::info!("in Substrate: [query_commitment_prefix]");

        // TODO - do a real chain query
        Ok(CommitmentPrefix::from(
            self.config().store_prefix.as_bytes().to_vec(),
        ))
    }

    fn query_latest_height(&self) -> Result<ICSHeight, Error> {
        tracing::info!("in Substrate: [query_latest_height]");

        let latest_height = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let height = self.get_latest_height(client).await.unwrap();

            tracing::info!("In Substrate: [query_latest_height] >> height: {:?}", height);

            height
        };

        let revision_height =  self.block_on(latest_height);
        let latest_height = Height::new(0, revision_height);
        Ok(latest_height)
    }

    fn query_clients(
        &self,
        request: QueryClientStatesRequest,
    ) -> Result<Vec<IdentifiedAnyClientState>, Error> {
        tracing::info!("in Substrate: [query_clients]");

        let clients = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();

            let clients = self.get_clients(client).await.unwrap();

            clients
        };

        let clients = self.block_on(clients);

        tracing::info!("in Substrate: [query_clients] >> clients: {:?}", clients);

        Ok(clients)
    }

    fn query_client_state(
        &self,
        client_id: &ClientId,
        height: ICSHeight,
    ) -> Result<Self::ClientState, Error> {
        tracing::info!("in Substrate: [query_client_state]");
        tracing::info!("in Substrate: [query_client_state] >> height: {:?}", height);

        let client_state = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let client_state = self
                .get_client_state(client_id, client)
                .await.unwrap();

            client_state
        };

        let client_state =  self.block_on(client_state);
        tracing::info!("in Substrate: [query_client_state] >> client_state: {:?}", client_state);

        Ok(client_state)
    }

    fn query_consensus_states(
        &self,
        request: QueryConsensusStatesRequest,
    ) -> Result<Vec<AnyConsensusStateWithHeight>, Error> {
        tracing::info!("in Substrate: [query_consensus_states]");
        let request_client_id = ClientId::from_str(request.client_id.as_str()).unwrap();

        let consensus_state = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let consensus_state = self
                .get_consensus_state_with_height(&request_client_id, client).await.unwrap();

            consensus_state
        };

        let consensus_state: Vec<(Height, AnyConsensusState)> =  self.block_on(consensus_state);

        let mut any_consensus_state_with_height = vec![];
        for (height, consensus_state) in consensus_state.into_iter() {
            // let consensus_state = AnyConsensusState::Grandpa(consensus_state);
            let tmp = AnyConsensusStateWithHeight {
                height: height,
                consensus_state,
            };
            any_consensus_state_with_height.push(tmp.clone());

            tracing::info!("In Substrate: [query_consensus_state] >> any_consensus_state_with_height: {:?}", tmp);
        }

        any_consensus_state_with_height.sort_by(|a, b| a.height.cmp(&b.height));

        Ok(any_consensus_state_with_height)
    }

    fn query_consensus_state(
        &self,
        client_id: ClientId,
        consensus_height: ICSHeight,
        query_height: ICSHeight,
    ) -> Result<AnyConsensusState, Error> {
        tracing::info!("in Substrate: [query_consensus_state]");

        let consensus_state = self
            .proven_client_consensus(&client_id, consensus_height, query_height)?
            .0;
        // Ok(AnyConsensusStateonsensusState::Grandpa(consensus_state))
        Ok(consensus_state)
    }

    fn query_upgraded_client_state(
        &self,
        height: ICSHeight,
    ) -> Result<(Self::ClientState, MerkleProof), Error> {
        tracing::info!("in Substrate: [query_upgraded_client_state]");

        todo!()
    }

    fn query_upgraded_consensus_state(
        &self,
        height: ICSHeight,
    ) -> Result<(Self::ConsensusState, MerkleProof), Error> {
        tracing::info!("in Substrate: [query_upgraded_consensus_state]");

        todo!()
    }

    fn query_connections(
        &self,
        request: QueryConnectionsRequest,
    ) -> Result<Vec<IdentifiedConnectionEnd>, Error> {
        tracing::info!("in Substrate: [query_connections]");

        let connections = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();

            let connections = self.get_connctions(client).await.unwrap();

            connections
        };

        let connections = self.block_on(connections);

        tracing::info!("in Substrate: [query_connections] >> clients: {:?}", connections);

        Ok(connections)
    }

    fn query_client_connections(
        &self,
        request: QueryClientConnectionsRequest,
    ) -> Result<Vec<ConnectionId>, Error> {
        tracing::info!("in substrate: [query_client_connections]");

        let client_id = ClientId::from_str(request.client_id.as_str()).unwrap();

        let client_connections = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();

            let client_connections = self.get_client_connections(client_id, client)
                .await.unwrap();

            tracing::info!("In substrate: [query_client_connections] >> client_connections: {:#?}",
                client_connections
            );

            client_connections
        };

        let client_connections = self.block_on(client_connections);

        Ok(client_connections)
    }

    fn query_connection(
        &self,
        connection_id: &ConnectionId,
        height: ICSHeight,
    ) -> Result<ConnectionEnd, Error> {
        tracing::info!("in Substrate: [query_connection]");

        let connection_end = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let connection_end = self
                .get_connectionend(connection_id, client)
                .await.unwrap();
            tracing::info!("In Substrate: [query_connection] \
                >> connection_id: {:?}, connection_end: {:?}", connection_id, connection_end);

            connection_end
        };

        let connection_end =  self.block_on(connection_end);

        Ok(connection_end)
    }

    fn query_connection_channels(
        &self,
        request: QueryConnectionChannelsRequest,
    ) -> Result<Vec<IdentifiedChannelEnd>, Error> {
        tracing::info!("in substrate: [query_connection_channels]");

        let connection_id = request.connection;
        let connection_id = ConnectionId::from_str(connection_id.as_str()).unwrap();

        let connection_channels = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();

            let connection_channels = self.get_connection_channels(connection_id, client)
                .await.unwrap();

            tracing::info!("In substrate: [query_connection_channels] >> connection_channels: {:?}", connection_channels);
            connection_channels
        };

        let connection_channels = self.block_on(connection_channels);

        Ok(connection_channels)
    }

    fn query_channels(
        &self,
        request: QueryChannelsRequest,
    ) -> Result<Vec<IdentifiedChannelEnd>, Error> {
        tracing::info!("in Substrate: [query_channels]");

        let channels = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();

            let channels = self.get_channels(client).await.unwrap();

            channels
        };

        let channels = self.block_on(channels);

        tracing::info!("in Substrate: [query_connections] >> clients: {:?}", channels);

        Ok(channels)
    }

    fn query_channel(
        &self,
        port_id: &PortId,
        channel_id: &ChannelId,
        height: ICSHeight,
    ) -> Result<ChannelEnd, Error> {
        tracing::info!("in Substrate: [query_channel]");

        let channel_end = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let channel_end = self
                .get_channelend(port_id,  channel_id,client).await.unwrap();
            tracing::info!("In Substrate: [query_channel] \
                >> port_id: {:?}, channel_id: {:?}, channel_end: {:?}",
                port_id, channel_id, channel_end);

            channel_end
        };

        let channel_end =  self.block_on(channel_end);

        Ok(channel_end)
    }

    fn query_channel_client_state(
        &self,
        request: QueryChannelClientStateRequest,
    ) -> Result<Option<IdentifiedAnyClientState>, Error> {
        tracing::info!("in Substrate: [query_channel_client_state]");

        todo!()
    }

    fn query_packet_commitments(
        &self,
        request: QueryPacketCommitmentsRequest,
    ) -> Result<(Vec<PacketState>, ICSHeight), Error> {
        tracing::info!("in Substrate: [query_packet_commitments]");

        let packet_commitments = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let packet_commitments = self
                .get_commitment_packet_state(client).await.unwrap();

            packet_commitments
        };

        let packet_commitments =  self.block_on(packet_commitments);

        let last_height = self.query_latest_height().unwrap();

        Ok((packet_commitments, last_height))

    }

    fn query_unreceived_packets(
        &self,
        request: QueryUnreceivedPacketsRequest,
    ) -> Result<Vec<u64>, Error> {
        tracing::info!("in Substrate: [query_unreceived_packets]");
        let port_id = PortId::from_str(request.port_id.as_str()).unwrap();
        let channel_id = ChannelId::from_str(request.channel_id.as_str()).unwrap();
        let seqs = request.packet_commitment_sequences.clone();

        let unreceived_packets = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let unreceived_packets = self
                .get_unreceipt_packet(&port_id, &channel_id, seqs, client).await.unwrap();

            unreceived_packets
        };

        let result =  self.block_on(unreceived_packets);

        Ok(result)
    }

    fn query_packet_acknowledgements(
        &self,
        request: QueryPacketAcknowledgementsRequest,
    ) -> Result<(Vec<PacketState>, ICSHeight), Error> {
        tracing::info!("in Substrate: [query_packet_acknowledegements]");

        let packet_acknowledgements = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();

            let packet_acknowledgements = self
                .get_acknowledge_packet_state(client).await.unwrap();

            packet_acknowledgements
        };

        let packet_acknowledgements =  self.block_on(packet_acknowledgements);

        let last_height = self.query_latest_height().unwrap();

        Ok((packet_acknowledgements, last_height))
    }

    fn query_unreceived_acknowledgements(
        &self,
        request: QueryUnreceivedAcksRequest,
    ) -> Result<Vec<u64>, Error> {
        tracing::info!("in Substraete: [query_unreceived_acknowledegements]");
        let port_id = PortId::from_str(request.port_id.as_str()).unwrap();
        let channel_id = ChannelId::from_str(request.channel_id.as_str()).unwrap();
        let seqs = request.packet_ack_sequences.clone();

        let unreceived_seqs = async {
            let mut unreceived_seqs: Vec<u64> = vec![];

            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();

            for _seq in seqs {
                let _cmt = self
                    .get_packet_commitment(&port_id, &channel_id, _seq, client.clone()).await;

                // if packet commitment still exists on the original sending chain, then packet ack is unreceived
                // since processing the ack will delete the packet commitment
                match _cmt {
                    Ok(_) => { unreceived_seqs.push(_seq); }
                    Err(_) => {}
                }
            }

            unreceived_seqs
        };

        let result =  self.block_on(unreceived_seqs);
        Ok(result)
    }

    fn query_next_sequence_receive(
        &self,
        request: QueryNextSequenceReceiveRequest,
    ) -> Result<Sequence, Error> {
        tracing::info!("in Substrate: [query_next_sequence_receiven]");

        todo!()
    }

    fn query_txs(&self, request: QueryTxRequest) -> Result<Vec<IbcEvent>, Error> {
        tracing::info!("in Substrate: [query_txs]");
        tracing::info!("in Substrate: [query_txs] >> request: {:?}", request);

        match request {
            QueryTxRequest::Packet(request) => {
                crate::time!("in Substrate: [query_txs]: query packet events");

                let mut result: Vec<IbcEvent> = vec![];
                if request.sequences.is_empty() {
                    return Ok(result);
                }

                tracing::info!("in Substrate: [query_txs]: query packet events request: {:?}", request);
                tracing::debug!("in Substrate: [query_txs]: packet >> sequence :{:?}", request.sequences);

                return Ok(result)

                // Todo: Related to https://github.com/octopus-network/ibc-rs/issues/88
                // To query to event by event type, sequence number and block height
                // use ibc::ics02_client::events::Attributes;
/*                use ibc::ics04_channel::events::Attributes;
                use ibc::ics02_client::header::AnyHeader;
                use core::ops::Add;

                result.push(IbcEvent::SendPacket(ibc::ics04_channel::events::SendPacket{
                    height: request.height,
                    packet: Packet{
                        sequence: request.sequences[0],
                        source_port: request.source_port_id,
                        source_channel: request.source_channel_id,
                        destination_port: request.destination_port_id,
                        destination_channel: request.destination_channel_id,
                        data: vec![1,3,5],  //Todo
                        timeout_height: ibc::Height::zero().add( 9999999), //Todo
                        timeout_timestamp: ibc::timestamp::Timestamp::from_nanoseconds(Utc::now().timestamp_nanos() as u64)
                            .unwrap().add(Duration::from_secs(99999)).unwrap() //Todo
                    }
                }));

                Ok(result)*/
            }

            QueryTxRequest::Client(request) => {

                crate::time!("in Substrate: [query_txs]: single client update event");
                tracing::info!("in Substrate: [query_txs]: single client update event: request:{:?}", request);


                // query the first Tx that includes the event matching the client request
                // Note: it is possible to have multiple Tx-es for same client and consensus height.
                // In this case it must be true that the client updates were performed with tha
                // same header as the first one, otherwise a subsequent transaction would have
                // failed on chain. Therefore only one Tx is of interest and current API returns
                // the first one.
                // let mut response = self
                //     .block_on(self.rpc_client.tx_search(
                //         header_query(&request),
                //         false,
                //         1,
                //         1, // get only the first Tx matching the query
                //         Order::Ascending,
                //     ))
                //     .map_err(|e| Error::rpc(self.config.rpc_addr.clone(), e))?;
                //
                // if response.txs.is_empty() {
                //     return Ok(vec![]);
                // }
                //
                // // the response must include a single Tx as specified in the query.
                // assert!(
                //     response.txs.len() <= 1,
                //     "packet_from_tx_search_response: unexpected number of txs"
                // );
                //
                // let tx = response.txs.remove(0);
                // let event = update_client_from_tx_search_response(self.id(), &request, tx);
                use ibc::ics02_client::events::Attributes;
                use ibc::ics02_client::header::AnyHeader;

                let mut result: Vec<IbcEvent> = vec![];

                result.push(IbcEvent::UpdateClient(ibc::ics02_client::events::UpdateClient{
                    common: Attributes {
                        height: request.height,
                        client_id: request.client_id,
                        client_type: ClientType::Grandpa,
                        consensus_height: request.consensus_height,
                    },
                    header: Some(AnyHeader::Grandpa(ibc::ics10_grandpa::header::Header::new(request.height.revision_height))),
                }));

                Ok(result)
                // Ok(event.into_iter().collect())
            }

            QueryTxRequest::Transaction(tx) => {
                crate::time!("in Substrate: [query_txs]: Transaction");
                tracing::info!("in Substrate: [query_txs]: Transaction: {:?}", tx);

                // let mut response = self
                //     .block_on(self.rpc_client.tx_search(
                //         tx_hash_query(&tx),
                //         false,
                //         1,
                //         1, // get only the first Tx matching the query
                //         Order::Ascending,
                //     ))
                //     .map_err(|e| Error::rpc(self.config.rpc_addr.clone(), e))?;
                //
                // if response.txs.is_empty() {
                //     Ok(vec![])
                // } else {
                //     let tx = response.txs.remove(0);
                //     Ok(all_ibc_events_from_tx_search_response(self.id(), tx))
                // }
                let mut result: Vec<IbcEvent> = vec![];

                Ok(result)
            }
        }
    }

    fn proven_client_state(
        &self,
        client_id: &ClientId,
        height: ICSHeight,
    ) -> Result<(Self::ClientState, MerkleProof), Error> {
        tracing::info!("in Substrate: [proven_client_state]");

        let client_state = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let client_state = self
                .get_client_state(client_id, client).await.unwrap();
            tracing::info!("In Substrate: [proven_client_state] \
                >> client_state : {:#?}", client_state);

            client_state
        };

        let client_state =  self.block_on(client_state);


        Ok((client_state, get_dummy_merkle_proof()))
    }

    fn proven_connection(
        &self,
        connection_id: &ConnectionId,
        height: ICSHeight,
    ) -> Result<(ConnectionEnd, MerkleProof), Error> {
        tracing::info !("in Substrate: [proven_connection]");

        let connection_end = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let connection_end = self
                .get_connectionend(connection_id, client)
                .await.unwrap();
            tracing::info!("In Substrate: [proven_connection] \
                >> connection_end: {:?}", connection_end);

            connection_end
        };

        let connection_end =  self.block_on(connection_end);

        let mut new_connection_end;

        if connection_end.counterparty().clone().connection_id.is_none() {

            // 构造 Counterparty
            let client_id = connection_end.counterparty().client_id().clone();
            let prefix = connection_end.counterparty().prefix().clone();
            let temp_connection_id = Some(connection_id.clone());

            let counterparty = Counterparty::new(client_id, temp_connection_id, prefix);
            let state = connection_end.state;
            let client_id = connection_end.client_id().clone();
            let versions = connection_end.versions().clone();
            let delay_period = connection_end.delay_period().clone();

            new_connection_end = ConnectionEnd::new(state, client_id, counterparty, versions, delay_period);
        } else {
            new_connection_end = connection_end;
        }

        Ok((new_connection_end, get_dummy_merkle_proof()))
    }

    fn proven_client_consensus(
        &self,
        client_id: &ClientId,
        consensus_height: ICSHeight,
        height: ICSHeight,
    ) -> Result<(Self::ConsensusState, MerkleProof), Error> {
        tracing::info!("in Substrate: [proven_client_consensus]");
        tracing::info!("in Substrate: [prove_client_consensus]: \
            client_id: {:?}, consensus_height: {:?}", client_id, consensus_height);

        let consensus_state = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let consensus_state = self
                .get_client_consensus(client_id, consensus_height, client)
                .await.unwrap();
            tracing::info!("In Substrate: [proven_client_consensus] \
                >> consensus_state : {:?}", consensus_state);

            consensus_state
        };

        let consensus_state =  self.block_on(consensus_state);

        Ok((consensus_state, get_dummy_merkle_proof()))
    }

    fn proven_channel(
        &self,
        port_id: &PortId,
        channel_id: &ChannelId,
        height: ICSHeight,
    ) -> Result<(ChannelEnd, MerkleProof), Error> {
        tracing::info!("in Substrate: [proven_channel]");

        let channel_end = async {
            let client = ClientBuilder::<NodeRuntime>::new()
                .set_url(&self.websocket_url.clone())
                .build().await.unwrap();
            let channel_end = self
                .get_channelend(port_id,  channel_id,client).await.unwrap();
            tracing::info!("In Substrate: [query_channel] \
                >> port_id: {:?}, channel_id: {:?}, channel_end: {:?}",
                port_id, channel_id, channel_end);

            channel_end
        };

        let channel_end =  self.block_on(channel_end);

        Ok((channel_end, get_dummy_merkle_proof()))
    }

    fn proven_packet(
        &self,
        packet_type: PacketMsgType,
        port_id: PortId,
        channel_id: ChannelId,
        sequence: Sequence,
        height: ICSHeight,
    ) -> Result<(Vec<u8>, MerkleProof), Error> {
        tracing::info!("in Substrate: [proven_packet]");

        // TODO This is Mock
        Ok((vec![0], get_dummy_merkle_proof()))
    }

    fn build_client_state(&self, height: ICSHeight) -> Result<Self::ClientState, Error> {
        // TODO this is mock
        tracing::info!("in Substrate: [build_client_state]");

        let chain_id = self.id().clone();
        tracing::info!("in Substrate: [build_client_state] >> chain_id = {:?}", chain_id);

        let frozen_height = Height::zero();
        tracing::info!("in Substrate: [build_client_state] >> frozen_height = {:?}", frozen_height);

        use ibc::ics02_client::client_state::AnyClientState;
        use ibc::ics10_grandpa::client_state::ClientState as GRANDPAClientState;

        // Create mock grandpa client state
        let client_state = GRANDPAClientState::new(chain_id, height, frozen_height)
            .unwrap();
        let any_client_state = AnyClientState::Grandpa(client_state);

        tracing::info!("in Substrate: [build_client_state] >> client_state: {:?}", any_client_state);

        Ok(any_client_state)
    }

    fn build_consensus_state(
        &self,
        light_block: Self::LightBlock,
    ) -> Result<Self::ConsensusState, Error> {
        // TODO this is mock
        tracing::info!("in Substrate: [build_consensus_state]");

        // Create mock grandpa consensus state
        use ibc::ics10_grandpa::consensus_state::ConsensusState as GRANDPAConsensusState;

        let consensus_state = GRANDPAConsensusState::new(CommitmentRoot::from(vec![1, 2, 3, 4]));

        Ok(AnyConsensusState::Grandpa(consensus_state))
    }

    fn build_header(
        &self,
        trusted_height: ICSHeight,
        target_height: ICSHeight,
        client_state: &AnyClientState,
        light_client: &mut Self::LightClient,
    ) -> Result<(Self::Header, Vec<Self::Header>), Error> {
        // TODO this is mock
        tracing::info!("in Substrate: [build_header]");
        tracing::info!("in Substrate: [build_header] >> Trusted_height: {:?}, Target_height: {:?}, client_state: {:?}",
            trusted_height, target_height, client_state);
        tracing::info!("in Substrate: [build_header] >> GPHEADER: {:?}", GPHeader::new(target_height.revision_height));

        Ok((GPHeader::new(target_height.revision_height), vec![GPHeader::new(trusted_height.revision_height)]))
    }
}

/// Returns a dummy `MerkleProof`, for testing only!
pub fn get_dummy_merkle_proof() -> MerkleProof {
    tracing::info!("in substrate: [get_dummy_merk_proof]");

    let parsed = ibc_proto::ics23::CommitmentProof { proof: None };
    let mproofs: Vec<ibc_proto::ics23::CommitmentProof> = vec![parsed];
    MerkleProof { proofs: mproofs }
}