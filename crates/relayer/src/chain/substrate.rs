use alloc::sync::Arc;
use ibc_proto::protobuf::Protobuf;
use ibc_relayer_types::applications::ics31_icq::response::CrossChainQueryResponse;
use ibc_relayer_types::core::ics02_client::error::Error as ClientError;
use ibc_relayer_types::core::ics02_client::events::UpdateClient;
use ibc_relayer_types::core::ics03_connection::connection::{
    ConnectionEnd, IdentifiedConnectionEnd,
};
use ibc_relayer_types::core::ics04_channel::channel::{ChannelEnd, IdentifiedChannelEnd};
use ibc_relayer_types::core::ics04_channel::packet::Sequence;
use ibc_relayer_types::core::ics23_commitment::commitment::CommitmentPrefix;
use ibc_relayer_types::core::ics23_commitment::merkle::{apply_prefix, MerkleProof};
use ibc_relayer_types::core::ics24_host::identifier::{ChainId, ChannelId, ConnectionId, PortId};
use ibc_relayer_types::signer::Signer;
use ibc_relayer_types::Height as ICSHeight;
use tendermint_rpc::endpoint::broadcast::tx_sync::Response;
use tokio::runtime::Runtime as TokioRuntime;

use crate::account::Balance;
use crate::chain::client::ClientSettings;
use crate::chain::endpoint::{ChainEndpoint, ChainStatus, HealthCheck};
use crate::chain::handle::Subscription;
use crate::chain::requests::*;
use crate::chain::tracking::TrackedMsgs;
use crate::client_state::{AnyClientState, IdentifiedAnyClientState};
use crate::config::ChainConfig;
use crate::consensus_state::{AnyConsensusState, AnyConsensusStateWithHeight};
use crate::denom::DenomTrace;
use crate::error::Error;
use crate::event::monitor::{EventMonitor, TxMonitorCmd};
use crate::event::IbcEventWithHeight;
use ibc_relayer_types::events::IbcEvent;

use crate::keyring::{KeyRing, Secp256k1KeyPair, SigningKeyPair};
use crate::misbehaviour::MisbehaviourEvidence;

use crate::config::AddressType;
use crate::connection::ConnectionMsgType;
use core::str::FromStr;
use hdpath::StandardHDPath;
use ibc_relayer_types::clients::ics06_solomachine::client_state::ClientState as SmClientState;
use ibc_relayer_types::clients::ics06_solomachine::consensus_state::ConsensusState as SmConsensusState;
use ibc_relayer_types::clients::ics06_solomachine::consensus_state::PublicKey;
use ibc_relayer_types::clients::ics06_solomachine::header::HeaderData as SmHeaderData;
use ibc_relayer_types::clients::ics06_solomachine::header::{Header as SmHeader, HeaderData};

use ibc_relayer_types::core::ics03_connection::connection::State;
use ibc_relayer_types::core::ics23_commitment::commitment::CommitmentProofBytes;
use ibc_relayer_types::core::ics23_commitment::commitment::CommitmentRoot;
use ibc_relayer_types::core::ics24_host::identifier::ClientId;
use ibc_relayer_types::core::ics24_host::path::{ChannelEndsPath, ConnectionsPath, Path};

use ibc_proto::ibc::lightclients::solomachine::v2::{
    ClientStateData, ConnectionStateData, ConsensusStateData, DataType, TimestampedSignatureData,
};
use ibc_relayer_types::proofs::{ConsensusProof, Proofs};
use ibc_relayer_types::timestamp::Timestamp;
use jsonrpsee::rpc_params;
use prost::Message;
use serde::{Deserialize, Serialize};
use sp_core::{Bytes, H256};
use sp_keyring::AccountKeyring;
use std::time::{Duration, SystemTime};
use subxt::{
    self,
    rpc::{BlockNumber, NumberOrHex},
    storage::StorageKey,
};
use subxt::{tx::PairSigner, OnlineClient, SubstrateConfig};
use tracing::info;
use ibc_relayer_types::clients::ics06_solomachine::{Header as SmHeader, SignBytes, HeaderData};
use ibc_relayer_types::clients::ics06_solomachine::HeaderData as SmHeaderData;
use hdpath::StandardHDPath;
use crate::config::AddressType;




#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubLightBlock {}

#[subxt::subxt(runtime_metadata_path = "./metadata.scale")]
pub mod substrate {}

pub mod subxt_ibc_event {
    use core::time::Duration;

    use super::substrate::runtime_types::ibc::core::ics02_client::client_type::ClientType as SubxtClientType;
    use super::substrate::runtime_types::ibc::core::ics02_client::height::Height as SubxtHeight;
    use super::substrate::runtime_types::ibc::core::ics03_connection::connection::{
        sealed::ConnectionEnd as SubxtConnectionEnd, Counterparty as SubxtConnectionCounterparty,
        State as SubxtConnectionState,
    };
    use super::substrate::runtime_types::ibc::core::ics03_connection::version::Version as SubxtConnectionVersion;
    use super::substrate::runtime_types::ibc::core::ics04_channel::channel::{
        ChannelEnd as SubxtChannelEnd, Counterparty as SubxtChannelCounterparty,
        Order as SubxtChannelOrder, State as SubxtChannelState,
    };
    use super::substrate::runtime_types::ibc::core::ics04_channel::commitment::{
        AcknowledgementCommitment as SubxtAcknowledgementCommitment,
        PacketCommitment as SubxtPacketCommitment,
    };
    use super::substrate::runtime_types::ibc::core::ics04_channel::msgs::acknowledgement::Acknowledgement as SubxtAcknowledgement;
    use super::substrate::runtime_types::ibc::core::ics04_channel::packet::{
        Receipt as SubxtReceipt, Sequence as SubxtSequence,
    };
    use super::substrate::runtime_types::ibc::core::ics04_channel::timeout::TimeoutHeight as SubxtTimeoutHeight;
    use super::substrate::runtime_types::ibc::core::ics04_channel::version::Version as SubxtChannelVersion;
    use super::substrate::runtime_types::ibc::core::ics23_commitment::commitment::CommitmentPrefix as SubxtCommitmentPrefix;
    use super::substrate::runtime_types::ibc::core::ics24_host::identifier::{
        ChannelId as SubxtChannelId, ClientId as SubxtClientId, ConnectionId as SubxtConnectionId,
        PortId as SubxtPortId,
    };
    use super::substrate::runtime_types::ibc::core::ics26_routing::context::ModuleId as SubxtModuleId;
    use super::substrate::runtime_types::ibc::core::{
        ics02_client::events::{
            ClientMisbehaviour as SubxtClientMisbehaviour, CreateClient as SubxtCreateClient,
            UpdateClient as SubxtUpdateClient, UpgradeClient as SubxtUpgradeClient,
        },
        ics03_connection::events::{
            OpenAck as SubxtConnectionOpenAck, OpenConfirm as SubxtConnectionOpenConfirm,
            OpenInit as SubxtConnectionOpenInit, OpenTry as SubxtConnectionOpenTry,
        },
        ics04_channel::events::{
            AcknowledgePacket as SubxtAcknowledgePacket,
            // ChannelClosed as SubxtChannelClosed, // todo channelClose
            CloseConfirm as SubxtChannelCloseConfirm,
            CloseInit as SubxtChannelCloseInit,
            OpenAck as SubxtChannelOpenAck,
            OpenConfirm as SubxtChannelOpenConfirm,
            OpenInit as SubxtChannelOpenInit,
            OpenTry as SubxtChannelOpenTry,
            ReceivePacket as SubxtReceivePacket,
            SendPacket as SubxtSendPacket,
            TimeoutPacket as SubxtTimeoutPacket,
            WriteAcknowledgement as SubxtWriteAcknowledgement,
        },
    };
    use super::substrate::runtime_types::ibc::events::{
        ModuleEvent as SubxtModuleEvent, ModuleEventAttribute as SubxtModuleEventAttribute,
    };
    use super::substrate::runtime_types::ibc::timestamp::Timestamp as SubxtTimestamp;
    use super::substrate::runtime_types::{self, ibc::events::IbcEvent as SubxtIbcEvent};
    use ibc_relayer_types::core::ics02_client::client_type::ClientType;
    use ibc_relayer_types::core::ics02_client::events::Attributes as ClientAttributes;
    use ibc_relayer_types::core::ics03_connection::connection::{
        ConnectionEnd, Counterparty as ConnectionCounterparty, State as ConnectionState,
    };
    use ibc_relayer_types::core::ics03_connection::events::Attributes as ConnectionAttributes;
    use ibc_relayer_types::core::ics03_connection::version::Version as ConnectionVersion;
    use ibc_relayer_types::core::ics04_channel::channel::{
        ChannelEnd, Counterparty as ChannelCounterparty, Order as ChannelOrder,
        State as ChannelState,
    };
    use ibc_relayer_types::core::ics04_channel::commitment::{
        AcknowledgementCommitment, PacketCommitment,
    };
    use ibc_relayer_types::core::ics04_channel::msgs::acknowledgement::Acknowledgement;
    use ibc_relayer_types::core::ics04_channel::packet::{Packet, Receipt, Sequence};
    use ibc_relayer_types::core::ics04_channel::timeout::TimeoutHeight;
    use ibc_relayer_types::core::ics04_channel::version::Version as ChannelVersion;
    use ibc_relayer_types::core::ics23_commitment::commitment::CommitmentPrefix;
    use ibc_relayer_types::core::ics24_host::identifier::{
        ChannelId, ClientId, ConnectionId, PortId,
    };
    use ibc_relayer_types::core::{
        ics02_client::events::{ClientMisbehaviour, CreateClient, UpdateClient, UpgradeClient},
        ics03_connection::events::{
            OpenAck as ConnectionOpenAck, OpenConfirm as ConnectionOpenConfirm,
            OpenInit as ConnectionOpenInit, OpenTry as ConnectionOpenTry,
        },
        ics04_channel::events::{
            AcknowledgePacket, CloseConfirm as ChannelCloseConfirm, CloseInit as ChannelCloseInit,
            OpenAck as ChannelOpenAck, OpenConfirm as ChannelOpenConfirm,
            OpenInit as ChannelOpenInit, OpenTry as ChannelOpenTry, ReceivePacket, SendPacket,
            TimeoutPacket, WriteAcknowledgement,
        },
    };
    use ibc_relayer_types::events::{self, IbcEvent, ModuleEventAttribute, ModuleId};
    use ibc_relayer_types::timestamp::Timestamp;
    use ibc_relayer_types::Height;
    use std::str::FromStr;

    // -------ics 02 client
    // client type
    impl From<SubxtClientType> for ClientType {
        fn from(value: SubxtClientType) -> Self {
            match value.0.as_ref() {
                "07-tendermint" => ClientType::Tendermint,
                "06-solomachine" => ClientType::Solomachine,
                _ => panic!("Unknown client type: {:?}", value),
            }
        }
    }
    // CreateClient
    impl From<SubxtCreateClient> for CreateClient {
        fn from(value: SubxtCreateClient) -> Self {
            let client_id = value.client_id.client_id;
            let client_type = value.client_type.client_type;
            let consensus_height = value.consensus_height.consensus_height;
            Self(ClientAttributes {
                client_id: client_id.into(),
                client_type: client_type.into(),
                consensus_height: consensus_height.into(),
            })
        }
    }

    // UpdateClient
    impl From<SubxtUpdateClient> for UpdateClient {
        fn from(value: SubxtUpdateClient) -> Self {
            let client_id = value.client_id.client_id;
            let client_type = value.client_type.client_type;
            let consensus_height = value.consensus_height.consensus_height;
            let _header = value.header.header;
            Self {
                common: ClientAttributes {
                    client_id: client_id.into(),
                    client_type: client_type.into(),
                    consensus_height: consensus_height.into(),
                },
                // todo, this type is `Option<Box<dyn Header>>` but get Any. So At Present We set None.
                header: None,
            }
        }
    }

    // UpgradeClient
    impl From<SubxtUpgradeClient> for UpgradeClient {
        fn from(value: SubxtUpgradeClient) -> Self {
            let client_id = value.client_id.client_id;
            let client_type = value.client_type.client_type;
            let consensus_height = value.consensus_height.consensus_height;
            Self(ClientAttributes {
                client_id: client_id.into(),
                client_type: client_type.into(),
                consensus_height: consensus_height.into(),
            })
        }
    }

    impl From<SubxtClientMisbehaviour> for ClientMisbehaviour {
        fn from(value: SubxtClientMisbehaviour) -> Self {
            let client_id = value.client_id.client_id;
            let client_type = value.client_type.client_type;
            // NOTICE, in ibc-rs  ClientMisbehaviour don't have consensus_height.
            let consensus_height = Height::new(0, 1).unwrap();
            Self(ClientAttributes {
                client_id: client_id.into(),
                client_type: client_type.into(),
                consensus_height,
            })
        }
    }

    // Height
    impl From<SubxtHeight> for Height {
        fn from(height: SubxtHeight) -> Self {
            Self::new(height.revision_number, height.revision_height)
                .expect("convert height: Never failed")
        }
    }

    // ------------ ics03 connection
    // ConnectionEnd
    impl From<SubxtConnectionEnd> for ConnectionEnd {
        fn from(value: SubxtConnectionEnd) -> Self {
            Self::new(
                value.state.into(),
                value.client_id.into(),
                value.counterparty.into(),
                value.versions.into_iter().map(|v| v.into()).collect(),
                Duration::new(value.delay_period_secs, value.delay_period_nanos),
            )
        }
    }

    // Counterparty
    impl From<SubxtConnectionCounterparty> for ConnectionCounterparty {
        fn from(value: SubxtConnectionCounterparty) -> Self {
            Self::new(
                value.client_id.into(),
                value.connection_id.map(|v| v.into()),
                value
                    .prefix
                    .bytes
                    .try_into()
                    .expect("never failed convert prefix from vec<u8>"),
            )
        }
    }

    // State
    impl From<SubxtConnectionState> for ConnectionState {
        fn from(value: SubxtConnectionState) -> Self {
            match value {
                SubxtConnectionState::Uninitialized => Self::Uninitialized,
                SubxtConnectionState::Init => Self::Init,
                SubxtConnectionState::TryOpen => Self::TryOpen,
                SubxtConnectionState::Open => Self::Open,
            }
        }
    }

    // OpenAck
    impl From<SubxtConnectionOpenAck> for ConnectionOpenAck {
        fn from(value: SubxtConnectionOpenAck) -> Self {
            let connection_id = value.0.connection_id;
            let client_id = value.0.client_id;
            let counterparty_connection_id = value.0.counterparty_connection_id;
            let counterparty_client_id = value.0.counterparty_client_id;
            Self(ConnectionAttributes {
                connection_id: Some(connection_id.into()),
                client_id: client_id.into(),
                counterparty_connection_id: counterparty_connection_id.map(|v| v.into()),
                counterparty_client_id: counterparty_client_id.into(),
            })
        }
    }

    // OpenConfirm
    impl From<SubxtConnectionOpenConfirm> for ConnectionOpenConfirm {
        fn from(value: SubxtConnectionOpenConfirm) -> Self {
            let connection_id = value.0.connection_id;
            let client_id = value.0.client_id;
            let counterparty_connection_id = value.0.counterparty_connection_id;
            let counterparty_client_id = value.0.counterparty_client_id;
            Self(ConnectionAttributes {
                connection_id: Some(connection_id.into()),
                client_id: client_id.into(),
                counterparty_connection_id: counterparty_connection_id.map(|v| v.into()),
                counterparty_client_id: counterparty_client_id.into(),
            })
        }
    }

    // OpenInit
    impl From<SubxtConnectionOpenInit> for ConnectionOpenInit {
        fn from(value: SubxtConnectionOpenInit) -> Self {
            let connection_id = value.0.connection_id;
            let client_id = value.0.client_id;
            let counterparty_connection_id = value.0.counterparty_connection_id;
            let counterparty_client_id = value.0.counterparty_client_id;
            Self(ConnectionAttributes {
                connection_id: Some(connection_id.into()),
                client_id: client_id.into(),
                counterparty_connection_id: counterparty_connection_id.map(|v| v.into()),
                counterparty_client_id: counterparty_client_id.into(),
            })
        }
    }

    // OpenTry
    impl From<SubxtConnectionOpenTry> for ConnectionOpenTry {
        fn from(value: SubxtConnectionOpenTry) -> Self {
            let connection_id = value.0.connection_id;
            let client_id = value.0.client_id;
            let counterparty_connection_id = value.0.counterparty_connection_id;
            let counterparty_client_id = value.0.counterparty_client_id;
            Self(ConnectionAttributes {
                connection_id: Some(connection_id.into()),
                client_id: client_id.into(),
                counterparty_connection_id: counterparty_connection_id.map(|v| v.into()),
                counterparty_client_id: counterparty_client_id.into(),
            })
        }
    }

    // Version
    impl From<SubxtConnectionVersion> for ConnectionVersion {
        fn from(value: SubxtConnectionVersion) -> Self {
            Self {
                identifier: value.identifier,
                features: value.features,
            }
        }
    }

    // ------------ ibc04 channel
    // channelEnd
    impl From<SubxtChannelEnd> for ChannelEnd {
        fn from(value: SubxtChannelEnd) -> Self {
            Self {
                state: value.state.into(),
                ordering: value.ordering.into(),
                remote: value.remote.into(),
                connection_hops: value
                    .connection_hops
                    .into_iter()
                    .map(|v| v.into())
                    .collect(),
                version: value.version.into(),
            }
        }
    }
    // Counterparty
    impl From<SubxtChannelCounterparty> for ChannelCounterparty {
        fn from(value: SubxtChannelCounterparty) -> Self {
            Self {
                port_id: value.port_id.into(),
                channel_id: value.channel_id.map(|v| v.into()),
            }
        }
    }
    // Order
    impl From<SubxtChannelOrder> for ChannelOrder {
        fn from(value: SubxtChannelOrder) -> Self {
            match value {
                SubxtChannelOrder::None => Self::None,
                SubxtChannelOrder::Unordered => Self::Unordered,
                SubxtChannelOrder::Ordered => Self::Ordered,
            }
        }
    }
    // state
    impl From<SubxtChannelState> for ChannelState {
        fn from(value: SubxtChannelState) -> Self {
            match value {
                SubxtChannelState::Uninitialized => Self::Uninitialized,
                SubxtChannelState::Init => Self::Init,
                SubxtChannelState::TryOpen => Self::TryOpen,
                SubxtChannelState::Open => Self::Open,
                SubxtChannelState::Closed => Self::Closed,
            }
        }
    }
    // AcknowledgementCommitment
    impl From<SubxtAcknowledgementCommitment> for AcknowledgementCommitment {
        fn from(value: SubxtAcknowledgementCommitment) -> Self {
            Self::from(value.0)
        }
    }
    // packetCommitment todo
    impl From<SubxtPacketCommitment> for PacketCommitment {
        fn from(value: SubxtPacketCommitment) -> Self {
            Self::from(value.0)
        }
    }
    // AcknowledgePacket
    impl From<SubxtAcknowledgePacket> for AcknowledgePacket {
        fn from(value: SubxtAcknowledgePacket) -> Self {
            let timeout_height = value.timeout_height.timeout_height;
            let timeout_timestamp = value.timeout_timestamp.timeout_timestamp;
            let sequence = value.sequence.sequence;
            let src_port_id = value.src_port_id.src_port_id;
            let src_channel_id = value.src_channel_id.src_channel_id;
            let dst_port_id = value.dst_port_id.dst_port_id;
            let dst_channel_id = value.dst_channel_id.dst_channel_id;
            let _channel_ordering = value.channel_ordering.order;
            let _src_connection_id = value.src_connection_id.connection_id;
            Self {
                packet: Packet {
                    sequence: sequence.into(),
                    source_port: src_port_id.into(),
                    source_channel: src_channel_id.into(),
                    destination_port: dst_port_id.into(),
                    destination_channel: dst_channel_id.into(),
                    data: b"ack".to_vec(),
                    timeout_height: timeout_height.into(),
                    timeout_timestamp: timeout_timestamp.into(),
                },
            }
        }
    }
    // ChannelClosed (todo in ibc-rs have this data struct but in ibc-relayer-type have not this)
    // CloseConfirm
    impl From<SubxtChannelCloseConfirm> for ChannelCloseConfirm {
        fn from(value: SubxtChannelCloseConfirm) -> Self {
            let channel_id = value.channel_id.channel_id;
            let port_id = value.port_id.port_id;
            let connection_id = value.connection_id.connection_id;
            let counterparty_port_id = value.counterparty_port_id.counterparty_port_id;
            let counterparty_channel_id = value.counterparty_channel_id.counterparty_channel_id;
            Self {
                channel_id: Some(channel_id.into()),
                port_id: port_id.into(),
                connection_id: connection_id.into(),
                counterparty_port_id: counterparty_port_id.into(),
                counterparty_channel_id: Some(counterparty_channel_id.into()),
            }
        }
    }
    // CloseInit
    impl From<SubxtChannelCloseInit> for ChannelCloseInit {
        fn from(value: SubxtChannelCloseInit) -> Self {
            let channel_id = value.channel_id.channel_id;
            let port_id = value.port_id.port_id;
            let connection_id = value.connection_id.connection_id;
            let counterparty_port_id = value.counterparty_port_id.counterparty_port_id;
            let counterparty_channel_id = value.counterparty_channel_id.counterparty_channel_id;
            Self {
                channel_id: channel_id.into(),
                port_id: port_id.into(),
                connection_id: connection_id.into(),
                counterparty_port_id: counterparty_port_id.into(),
                counterparty_channel_id: Some(counterparty_channel_id.into()),
            }
        }
    }
    // OpenAck
    impl From<SubxtChannelOpenAck> for ChannelOpenAck {
        fn from(value: SubxtChannelOpenAck) -> Self {
            let channel_id = value.channel_id.channel_id;
            let port_id = value.port_id.port_id;
            let connection_id = value.connection_id.connection_id;
            let counterparty_port_id = value.counterparty_port_id.counterparty_port_id;
            let counterparty_channel_id = value.counterparty_channel_id.counterparty_channel_id;
            Self {
                channel_id: Some(channel_id.into()),
                port_id: port_id.into(),
                connection_id: connection_id.into(),
                counterparty_port_id: counterparty_port_id.into(),
                counterparty_channel_id: Some(counterparty_channel_id.into()),
            }
        }
    }
    // OpenConfirm
    impl From<SubxtChannelOpenConfirm> for ChannelOpenConfirm {
        fn from(value: SubxtChannelOpenConfirm) -> Self {
            let channel_id = value.channel_id.channel_id;
            let port_id = value.port_id.port_id;
            let connection_id = value.connection_id.connection_id;
            let counterparty_port_id = value.counterparty_port_id.counterparty_port_id;
            let counterparty_channel_id = value.counterparty_channel_id.counterparty_channel_id;
            Self {
                channel_id: Some(channel_id.into()),
                port_id: port_id.into(),
                connection_id: connection_id.into(),
                counterparty_port_id: counterparty_port_id.into(),
                counterparty_channel_id: Some(counterparty_channel_id.into()),
            }
        }
    }
    // OpenInit
    impl From<SubxtChannelOpenInit> for ChannelOpenInit {
        fn from(value: SubxtChannelOpenInit) -> Self {
            let channel_id = value.channel_id.channel_id;
            let port_id = value.port_id.port_id;
            let connection_id = value.connection_id.connection_id;
            let counterparty_port_id = value.counterparty_port_id.counterparty_port_id;
            let _version = value.version.version;
            Self {
                channel_id: Some(channel_id.into()),
                port_id: port_id.into(),
                connection_id: connection_id.into(),
                counterparty_port_id: counterparty_port_id.into(),
                counterparty_channel_id: None,
            }
        }
    }
    // OpenTry
    impl From<SubxtChannelOpenTry> for ChannelOpenTry {
        fn from(value: SubxtChannelOpenTry) -> Self {
            let channel_id = value.channel_id.channel_id;
            let port_id = value.port_id.port_id;
            let connection_id = value.connection_id.connection_id;
            let counterparty_port_id = value.counterparty_port_id.counterparty_port_id;
            let counterparty_channel_id = value.counterparty_channel_id.counterparty_channel_id;
            Self {
                channel_id: Some(channel_id.into()),
                port_id: port_id.into(),
                connection_id: connection_id.into(),
                counterparty_port_id: counterparty_port_id.into(),
                counterparty_channel_id: Some(counterparty_channel_id.into()),
            }
        }
    }
    // ReceivePacket
    impl From<SubxtReceivePacket> for ReceivePacket {
        fn from(value: SubxtReceivePacket) -> Self {
            let packet_data = value.packet_data.packet_data;
            let timeout_height = value.timeout_height.timeout_height;
            let timeout_timestamp = value.timeout_timestamp.timeout_timestamp;
            let sequence = value.sequence.sequence;
            let src_port_id = value.src_port_id.src_port_id;
            let src_channel_id = value.src_channel_id.src_channel_id;
            let dst_port_id = value.dst_port_id.dst_port_id;
            let dst_channel_id = value.dst_channel_id.dst_channel_id;
            let _channel_ordering = value.channel_ordering.order;
            let _src_connection_id = value.dst_connection_id.connection_id;
            Self {
                packet: Packet {
                    sequence: sequence.into(),
                    source_port: src_port_id.into(),
                    source_channel: src_channel_id.into(),
                    destination_port: dst_port_id.into(),
                    destination_channel: dst_channel_id.into(),
                    data: packet_data,
                    timeout_height: timeout_height.into(),
                    timeout_timestamp: timeout_timestamp.into(),
                },
            }
        }
    }
    // SendPacket
    impl From<SubxtSendPacket> for SendPacket {
        fn from(value: SubxtSendPacket) -> Self {
            let packet_data = value.packet_data.packet_data;
            let timeout_height = value.timeout_height.timeout_height;
            let timeout_timestamp = value.timeout_timestamp.timeout_timestamp;
            let sequence = value.sequence.sequence;
            let src_port_id = value.src_port_id.src_port_id;
            let src_channel_id = value.src_channel_id.src_channel_id;
            let dst_port_id = value.dst_port_id.dst_port_id;
            let dst_channel_id = value.dst_channel_id.dst_channel_id;
            let _channel_ordering = value.channel_ordering.order;
            let _src_connection_id = value.src_connection_id.connection_id;
            Self {
                packet: Packet {
                    sequence: sequence.into(),
                    source_port: src_port_id.into(),
                    source_channel: src_channel_id.into(),
                    destination_port: dst_port_id.into(),
                    destination_channel: dst_channel_id.into(),
                    data: packet_data,
                    timeout_height: timeout_height.into(),
                    timeout_timestamp: timeout_timestamp.into(),
                },
            }
        }
    }
    // TimeoutPacket
    impl From<SubxtTimeoutPacket> for TimeoutPacket {
        fn from(value: SubxtTimeoutPacket) -> Self {
            let timeout_height = value.timeout_height.timeout_height;
            let timeout_timestamp = value.timeout_timestamp.timeout_timestamp;
            let sequence = value.sequence.sequence;
            let src_port_id = value.src_port_id.src_port_id;
            let src_channel_id = value.src_channel_id.src_channel_id;
            let dst_port_id = value.dst_port_id.dst_port_id;
            let dst_channel_id = value.dst_channel_id.dst_channel_id;
            let _channel_ordering = value.channel_ordering.order;

            Self {
                packet: Packet {
                    sequence: sequence.into(),
                    source_port: src_port_id.into(),
                    source_channel: src_channel_id.into(),
                    destination_port: dst_port_id.into(),
                    destination_channel: dst_channel_id.into(),
                    data: b"timeout".to_vec(),
                    timeout_height: timeout_height.into(),
                    timeout_timestamp: timeout_timestamp.into(),
                },
            }
        }
    }
    // WriteAcknowledgement
    impl From<SubxtWriteAcknowledgement> for WriteAcknowledgement {
        fn from(value: SubxtWriteAcknowledgement) -> Self {
            let packet_data = value.packet_data.packet_data;
            let timeout_height = value.timeout_height.timeout_height;
            let timeout_timestamp = value.timeout_timestamp.timeout_timestamp;
            let sequence = value.sequence.sequence;
            let src_port_id = value.src_port_id.src_port_id;
            let src_channel_id = value.src_channel_id.src_channel_id;
            let dst_port_id = value.dst_port_id.dst_port_id;
            let dst_channel_id = value.dst_channel_id.dst_channel_id;
            let _src_connection_id = value.dst_connection_id.connection_id;
            let acknowledgement = value.acknowledgement.acknowledgement.0;
            Self {
                packet: Packet {
                    sequence: sequence.into(),
                    source_port: src_port_id.into(),
                    source_channel: src_channel_id.into(),
                    destination_port: dst_port_id.into(),
                    destination_channel: dst_channel_id.into(),
                    data: packet_data,
                    timeout_height: timeout_height.into(),
                    timeout_timestamp: timeout_timestamp.into(),
                },
                ack: acknowledgement,
            }
        }
    }
    // Acknowledgement todo
    impl From<SubxtAcknowledgement> for Acknowledgement {
        fn from(ack: SubxtAcknowledgement) -> Self {
            Self::from(ack.0)
        }
    }
    // Receipt todo
    impl From<SubxtReceipt> for Receipt {
        fn from(receipt: SubxtReceipt) -> Self {
            match receipt {
                SubxtReceipt::Ok => Self::Ok,
            }
        }
    }
    // Sequence
    impl From<SubxtSequence> for Sequence {
        fn from(value: SubxtSequence) -> Self {
            Sequence::from(value.0)
        }
    }
    // TimeoutHeight
    impl From<SubxtTimeoutHeight> for TimeoutHeight {
        fn from(value: SubxtTimeoutHeight) -> Self {
            match value {
                SubxtTimeoutHeight::Never => Self::Never,
                SubxtTimeoutHeight::At(v) => Self::At(v.into()),
            }
        }
    }
    // Version  todo
    impl From<SubxtChannelVersion> for ChannelVersion {
        fn from(value: SubxtChannelVersion) -> Self {
            Self(value.0)
        }
    }

    // ------- ics23 commitment
    // CommitmentPrefix todo
    impl From<SubxtCommitmentPrefix> for CommitmentPrefix {
        fn from(value: SubxtCommitmentPrefix) -> Self {
            CommitmentPrefix::try_from(value.bytes)
                .expect("converty failed because subxt commitment Prefix is empty")
        }
    }

    // -------ics24 host
    // ChannelId
    impl From<SubxtChannelId> for ChannelId {
        fn from(value: SubxtChannelId) -> Self {
            ChannelId::from_str(value.0.as_ref()).expect("convert channelId: Never failed")
        }
    }
    // clientId
    impl From<SubxtClientId> for ClientId {
        fn from(value: SubxtClientId) -> Self {
            ClientId::from_str(value.0.as_ref()).expect("convert clientId: Never failed")
        }
    }
    // connectionId
    impl From<SubxtConnectionId> for ConnectionId {
        fn from(value: SubxtConnectionId) -> Self {
            ConnectionId::from_str(value.0.as_ref()).expect("convert connectionid: Never failed")
        }
    }
    // PortId
    impl From<SubxtPortId> for PortId {
        fn from(value: SubxtPortId) -> Self {
            PortId::from_str(value.0.as_ref()).expect("convert PortId: Never failed")
        }
    }

    // -------- ics26 routing
    // ModuleId
    impl From<SubxtModuleId> for ModuleId {
        fn from(value: SubxtModuleId) -> Self {
            ModuleId::from_str(value.0.as_ref()).expect("conver moudleid: never failed ")
        }
    }

    // --- events
    impl From<SubxtIbcEvent> for IbcEvent {
        fn from(value: SubxtIbcEvent) -> Self {
            match value {
                SubxtIbcEvent::CreateClient(value) => IbcEvent::CreateClient(value.into()),
                SubxtIbcEvent::UpdateClient(value) => IbcEvent::UpdateClient(value.into()),
                SubxtIbcEvent::UpgradeClient(value) => IbcEvent::UpgradeClient(value.into()),
                SubxtIbcEvent::ClientMisbehaviour(value) => {
                    IbcEvent::ClientMisbehaviour(value.into())
                }
                SubxtIbcEvent::OpenInitConnection(value) => {
                    IbcEvent::OpenInitConnection(value.into())
                }
                SubxtIbcEvent::OpenTryConnection(value) => {
                    IbcEvent::OpenTryConnection(value.into())
                }
                SubxtIbcEvent::OpenAckConnection(value) => {
                    IbcEvent::OpenAckConnection(value.into())
                }
                SubxtIbcEvent::OpenConfirmConnection(value) => {
                    IbcEvent::OpenConfirmConnection(value.into())
                }
                SubxtIbcEvent::OpenInitChannel(value) => IbcEvent::OpenInitChannel(value.into()),
                SubxtIbcEvent::OpenTryChannel(value) => IbcEvent::OpenTryChannel(value.into()),
                SubxtIbcEvent::OpenAckChannel(value) => IbcEvent::OpenAckChannel(value.into()),
                SubxtIbcEvent::OpenConfirmChannel(value) => {
                    IbcEvent::OpenConfirmChannel(value.into())
                }
                SubxtIbcEvent::CloseInitChannel(value) => IbcEvent::CloseInitChannel(value.into()),
                SubxtIbcEvent::CloseConfirmChannel(value) => {
                    IbcEvent::CloseConfirmChannel(value.into())
                }
                SubxtIbcEvent::SendPacket(value) => IbcEvent::SendPacket(value.into()),
                SubxtIbcEvent::ReceivePacket(value) => IbcEvent::ReceivePacket(value.into()),
                SubxtIbcEvent::WriteAcknowledgement(value) => {
                    IbcEvent::WriteAcknowledgement(value.into())
                }
                SubxtIbcEvent::AcknowledgePacket(value) => {
                    IbcEvent::AcknowledgePacket(value.into())
                }
                SubxtIbcEvent::TimeoutPacket(value) => IbcEvent::TimeoutPacket(value.into()),
                SubxtIbcEvent::ChannelClosed(value) => IbcEvent::ChainError(format!("{:?}", value)), // todo ibc_relayer_type::events don't have ChannelClosed variant.
                SubxtIbcEvent::AppModule(value) => IbcEvent::AppModule(value.into()),
            }
        }
    }

    impl From<SubxtModuleEvent> for events::ModuleEvent {
        fn from(value: SubxtModuleEvent) -> Self {
            let kind = value.kind;
            let module_name = value.module_name;
            let attributes = value.attributes.into_iter().map(|v| v.into()).collect();
            Self {
                kind,
                module_name: module_name.into(),
                attributes,
            }
        }
    }

    impl From<SubxtModuleEventAttribute> for ModuleEventAttribute {
        fn from(value: SubxtModuleEventAttribute) -> Self {
            Self {
                key: value.key,
                value: value.value,
            }
        }
    }

    // ------------------- timestamp
    impl From<SubxtTimestamp> for Timestamp {
        fn from(value: SubxtTimestamp) -> Self {
            if let Some(v) = value.time {
                // todo unwrap need hanele
                Timestamp::from_nanoseconds(v as u64).ok().unwrap()
            } else {
                Timestamp::none()
            }
        }
    }
}

pub struct SubstrateChain {
    config: ChainConfig,
    rpc_client: OnlineClient<SubstrateConfig>,
    rt: Arc<TokioRuntime>,
}

impl SubstrateChain {
    /// Retrieve the storage proof according to storage keys
    /// And convert the proof to IBC compatible type
    fn generate_storage_proof(&self, key_addr: &[u8], height: u64) -> Result<MerkleProof, Error> {
        let block_hash: H256 = self
            .rt
            .block_on(
                self.rpc_client
                    .rpc()
                    .block_hash(Some(BlockNumber::from(height))),
            )
            .unwrap()
            .unwrap();
        // Fetch at most 10 keys from below the prefix XcmPallet' VersionNotifiers.
        let keys = self
            .rt
            .block_on(
                self.rpc_client
                    .storage()
                    .fetch_keys(key_addr, 1, None, None),
            )
            .unwrap();
        let keys: Vec<&[u8]> = keys.iter().map(|key| key.as_ref()).collect();

        let storage_proof = self
            .rt
            .block_on(self.rpc_client.rpc().read_proof(keys, Some(block_hash)))
            .unwrap();

        println!(
            "in substrate: [generate_storage_proof] >> storage_proof_ : {:?}",
            storage_proof
        );

        let proof = compose_ibc_merkle_proof(storage_proof);
        Ok(proof)
    }
}

impl ChainEndpoint for SubstrateChain {
    type LightBlock = SubLightBlock;
    type Header = SmHeader;
    type ConsensusState = SmConsensusState;
    type ClientState = SmClientState;
    type SigningKeyPair = Secp256k1KeyPair;

    fn bootstrap(config: ChainConfig, rt: Arc<TokioRuntime>) -> Result<Self, Error> {
        let rpc_client = rt.block_on(OnlineClient::<SubstrateConfig>::new()).unwrap();

        Ok(Self {
            config,
            rpc_client,
            rt,
        })
    }

    fn shutdown(self) -> Result<(), Error> {
        unimplemented!();
    }

    fn keybase(&self) -> &KeyRing<Self::SigningKeyPair> {
        unimplemented!();
    }

    fn keybase_mut(&mut self) -> &mut KeyRing<Self::SigningKeyPair> {
        unimplemented!();
    }

    fn subscribe(&mut self) -> Result<Subscription, Error> {
        unimplemented!();
    }

    /// Does multiple RPC calls to the full node, to check for
    /// reachability and some basic APIs are available.
    ///
    /// Currently this checks that:
    ///     - the node responds OK to `/health` RPC call;
    ///     - the node has transaction indexing enabled;
    ///     - the SDK & IBC versions are supported;
    ///
    /// Emits a log warning in case anything is amiss.
    /// Exits early if any health check fails, without doing any
    /// further checks.
    fn health_check(&self) -> Result<HealthCheck, Error> {
        unimplemented!();
    }

    /// Fetch a header from the chain at the given height and verify it.
    fn verify_header(
        &mut self,
        _trusted: ICSHeight,
        _target: ICSHeight,
        _client_state: &AnyClientState,
    ) -> Result<Self::LightBlock, Error> {
        println!(
            "trusted: {:?}, target: {:?}, client_state: {:?}",
            _trusted, _target, _client_state
        );
        Ok(SubLightBlock {})
    }

    /// Given a client update event that includes the header used in a client update,
    /// look for misbehaviour by fetching a header at same or latest height.
    fn check_misbehaviour(
        &mut self,
        _update: &UpdateClient,
        _client_state: &AnyClientState,
    ) -> Result<Option<MisbehaviourEvidence>, Error> {
        unimplemented!();
    }

    // Queries

    /// Send one or more transactions that include all the specified messages.
    /// The `proto_msgs` are split in transactions such they don't exceed the configured maximum
    /// number of messages per transaction and the maximum transaction size.
    /// Then `send_tx()` is called with each Tx. `send_tx()` determines the fee based on the
    /// on-chain simulation and if this exceeds the maximum gas specified in the configuration file
    /// then it returns error.
    /// TODO - more work is required here for a smarter split maybe iteratively accumulating/ evaluating
    /// msgs in a Tx until any of the max size, max num msgs, max fee are exceeded.
    fn send_messages_and_wait_commit(
        &mut self,
        tracked_msgs: TrackedMsgs,
    ) -> Result<Vec<IbcEventWithHeight>, Error> {
        let proto_msgs = tracked_msgs.msgs;
        let log_msg = proto_msgs
            .clone()
            .into_iter()
            .map(|msg| {
                ibc_relayer_types::core::ics26_routing::msgs::Ics26Envelope::try_from(msg).unwrap()
            })
            .collect::<Vec<_>>();
        for msg in log_msg.into_iter() {
            match msg {
                ibc_relayer_types::core::ics26_routing::msgs::Ics26Envelope::Ics2Msg(value) => {
                    match value {
                        ibc_relayer_types::core::ics02_client::msgs::ClientMsg::CreateClient(
                            value,
                        ) => {
                            println!("create client msg -------------------------- start");
                            match value.client_state.type_url.as_str() {
                                ibc_relayer_types::clients::ics06_solomachine::SOLOMACHINE_CLIENT_STATE_TYPE_URL => {
                                    let v = ibc_relayer_types::clients::ics06_solomachine::client_state::ClientState::try_from(value.client_state.clone()).unwrap();
                                    println!("ics06 client state: {:?}", v);
                                }
                                ibc_relayer_types::clients::ics07_tendermint::client_state::TENDERMINT_CLIENT_STATE_TYPE_URL => {
                                    let v = ibc_relayer_types::clients::ics07_tendermint::client_state::ClientState::try_from(value.client_state.clone());
                                    println!("ics07 client state: {:?}", v);
                                }
                                _ => println!("unknown client state"),
                            }
                            match value.consensus_state.type_url.as_str() {
                                ibc_relayer_types::clients::ics06_solomachine::SOLOMACHINE_CONSENSUS_STATE_TYPE_URL => {
                                    let v = ibc_relayer_types::clients::ics06_solomachine::consensus_state::ConsensusState::try_from(value.consensus_state.clone()).unwrap();
                                    println!("ics06 client state: {:?}", v);
                                }
                                ibc_relayer_types::clients::ics07_tendermint::consensus_state::TENDERMINT_CONSENSUS_STATE_TYPE_URL => {
                                    let v = ibc_relayer_types::clients::ics07_tendermint::consensus_state::ConsensusState::try_from(value.consensus_state.clone());
                                    println!("ics07 client state: {:?}", v);
                                }
                                _ => println!("unknown consensus state"),
                            }
                            println!("signer: {:?}", value.signer);
                            println!("create client msg -------------------------- start");
                        }
                        ibc_relayer_types::core::ics02_client::msgs::ClientMsg::UpdateClient(
                            value,
                        ) => {
                            // pub client_id: ClientId,
                            println!("update client msg -------------------------- start");
                            println!("client id: {}", value.client_id);
                            // pub header: Any,
                            match value.header.type_url.as_str() {
                                ibc_relayer_types::clients::ics06_solomachine::SOLOMACHINE_HEADER_TYPE_URL => {
                                    let v = ibc_relayer_types::clients::ics06_solomachine::header::Header::try_from(value.header.clone()).unwrap();
                                    println!("ics06 header : {}", v);
                                }
                                ibc_relayer_types::clients::ics07_tendermint::header::TENDERMINT_HEADER_TYPE_URL => {
                                    let v = ibc_relayer_types::clients::ics07_tendermint::header::Header::try_from(value.header.clone()).unwrap();
                                    println!("ics07 header : {}", v);
                                }
                                _ => println!("unknown header "),
                            }
                            // pub signer: Signer,
                            println!("signer: {}", value.signer);
                            println!("update client msg -------------------------- end");
                        }
                        ibc_relayer_types::core::ics02_client::msgs::ClientMsg::Misbehaviour(
                            value,
                        ) => {
                            println!("misbehaviour: {value:?}");
                        }
                        ibc_relayer_types::core::ics02_client::msgs::ClientMsg::UpgradeClient(
                            value,
                        ) => {
                            println!("upgrade client: {value:?}");
                        }
                    }
                }
                ibc_relayer_types::core::ics26_routing::msgs::Ics26Envelope::Ics3Msg(value) => {
                    match value {
                        ibc_relayer_types::core::ics03_connection::msgs::ConnectionMsg::ConnectionOpenInit(
                            value,
                        ) => {println!("ConnectionOpenInit: {value:?}");}
                        ibc_relayer_types::core::ics03_connection::msgs::ConnectionMsg::ConnectionOpenTry(
                            value,
                        ) => {
                            println!("ConnectionOpenTry msg -------------------------- end");
                            // pub previous_connection_id: Option<ConnectionId>,
                            println!("previous_connection_id: {:?}", value.previous_connection_id);
                            // pub client_id: ClientId,
                            println!("client_id: {:?}", value.client_id);
                            // pub client_state: Option<Any>,
                            if let Some(value) = value.client_state {
                                match value.type_url.as_str() {
                                    ibc_relayer_types::clients::ics06_solomachine::SOLOMACHINE_CLIENT_STATE_TYPE_URL => {
                                        let v = ibc_relayer_types::clients::ics06_solomachine::client_state::ClientState::try_from(value.clone()).unwrap();
                                        println!("ics06 client state: {:?}", v);
                                    }
                                    ibc_relayer_types::clients::ics07_tendermint::client_state::TENDERMINT_CLIENT_STATE_TYPE_URL => {
                                        let v = ibc_relayer_types::clients::ics07_tendermint::client_state::ClientState::try_from(value.clone());
                                        println!("ics07 client state: {:?}", v);
                                    }
                                    _ => println!("unknown client state"),
                                }
                            } else {
                                println!("client state is None");
                            }
                            // pub counterparty: Counterparty,
                            println!("counterparty: {:?}", value.counterparty);
                            // pub counterparty_versions: Vec<Version>,
                            println!("counterparty_versions: {:?}", value.counterparty_versions);
                            // pub proofs: Proofs,
                            println!("proofs: {:?}", value.proofs);
                            // pub delay_period: Duration,
                            println!("delay_period: {:?}", value.delay_period);
                            // pub signer: Signer,
                            println!("signer: {:?}", value.signer);
                            println!("ConnectionOpenTry msg -------------------------- end");
                        }
                        ibc_relayer_types::core::ics03_connection::msgs::ConnectionMsg::ConnectionOpenAck(
                            value,
                        ) => {
                            println!("ConnectionOpenAck msg -------------------------- end");
                            // pub connection_id: ConnectionId,
                            println!("connection_id: {:?}", value.connection_id);
                            // pub counterparty_connection_id: ConnectionId,
                            println!("counterparty_connection_id: {:?}", value.counterparty_connection_id);
                            // pub client_state: Option<Any>,
                            if let Some(value) = value.client_state {
                                match value.type_url.as_str() {
                                    ibc_relayer_types::clients::ics06_solomachine::SOLOMACHINE_CLIENT_STATE_TYPE_URL => {
                                        let v = ibc_relayer_types::clients::ics06_solomachine::client_state::ClientState::try_from(value.clone()).unwrap();
                                        println!("ics06 client state: {:?}", v);
                                    }
                                    ibc_relayer_types::clients::ics07_tendermint::client_state::TENDERMINT_CLIENT_STATE_TYPE_URL => {
                                        let v = ibc_relayer_types::clients::ics07_tendermint::client_state::ClientState::try_from(value.clone());
                                        println!("ics07 client state: {:?}", v);
                                    }
                                    _ => println!("unknown client state"),
                                }
                            } else {
                                println!("client state is None");
                            }
                            // pub proofs: Proofs,
                            println!("proofs: {:?}", value.proofs);
                            // pub version: Version,
                            println!("version: {:?}", value.version);
                            // pub signer: Signer,
                            println!("signer: {}", value.signer);
                            println!("ConnectionOpenAck msg -------------------------- end");
                        }
                        ibc_relayer_types::core::ics03_connection::msgs::ConnectionMsg::ConnectionOpenConfirm(
                            value,
                        ) => {println!("ConnectionOpenConfirm: {value:?}");}
                    }
                }
                ibc_relayer_types::core::ics26_routing::msgs::Ics26Envelope::Ics4ChannelMsg(
                    value,
                ) => println!("ics04 channel msg: {value:?}"),
                ibc_relayer_types::core::ics26_routing::msgs::Ics26Envelope::Ics4PacketMsg(
                    value,
                ) => println!("ics04 packet msg: {value:?}"),
            }
        }

        let msg: Vec<substrate::runtime_types::ibc_proto::google::protobuf::Any> = proto_msgs
            .iter()
            .map(
                |m| substrate::runtime_types::ibc_proto::google::protobuf::Any {
                    type_url: m.type_url.clone(),
                    value: m.value.clone(),
                },
            )
            .collect();
        let signer: PairSigner<SubstrateConfig, subxt::ext::sp_core::sr25519::Pair> =
            PairSigner::new(AccountKeyring::Alice.pair());

        let binding = self.rpc_client.tx();
        let tx = substrate::tx().ibc().deliver(msg);

        let runtime = self.rt.clone();
        let deliver = binding.sign_and_submit_then_watch_default(&tx, &signer);
        let result = runtime.block_on(deliver);
        // println!("send_messages_and_wait_commit result: {:?}", result);
        let events = runtime.block_on(result.unwrap().wait_for_finalized_success());

        let ibc_events = events
            .unwrap()
            .find_first::<substrate::ibc::events::IbcEvents>()
            .unwrap()
            .unwrap();
        let es: Vec<IbcEventWithHeight> = ibc_events
            .events
            .into_iter()
            .map(|e| match e {
                _ => IbcEventWithHeight {
                    event: IbcEvent::from(e),
                    height: ICSHeight::new(0, 10).unwrap(),
                },
            })
            .collect();

        Ok(es)
    }

    fn send_messages_and_wait_check_tx(
        &mut self,
        _tracked_msgs: TrackedMsgs,
    ) -> Result<Vec<Response>, Error> {
        unimplemented!();
    }

    /// Get the account for the signer
    fn get_signer(&self) -> Result<Signer, Error> {
        // $ subkey inspect //Alice
        // Secret Key URI `//Alice` is account:
        // Network ID:        substrate
        // Secret seed:       0xe5be9a5092b81bca64be81d212e7f2f9eba183bb7a90954f7b76361f6edb5c0a
        // Public key (hex):  0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d
        // Account ID:        0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d
        // Public key (SS58): 5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY
        // SS58 Address:      5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY
        Ok(
            Signer::from_str("0xe5be9a5092b81bca64be81d212e7f2f9eba183bb7a90954f7b76361f6edb5c0a")
                .unwrap(),
        )
    }

    /// Get the chain configuration
    fn config(&self) -> &ChainConfig {
        &self.config
    }

    fn ibc_version(&self) -> Result<Option<semver::Version>, Error> {
        unimplemented!();
    }

    fn query_balance(
        &self,
        _key_name: Option<&str>,
        _denom: Option<&str>,
    ) -> Result<Balance, Error> {
        unimplemented!();
    }

    fn query_all_balances(&self, _key_name: Option<&str>) -> Result<Vec<Balance>, Error> {
        unimplemented!();
    }

    fn query_denom_trace(&self, _hash: String) -> Result<DenomTrace, Error> {
        unimplemented!();
    }

    fn query_commitment_prefix(&self) -> Result<CommitmentPrefix, Error> {
        crate::time!("query_commitment_prefix");
        crate::telemetry!(query, self.id(), "query_commitment_prefix");

        // TODO - do a real chain query
        CommitmentPrefix::try_from(self.config.store_prefix.as_bytes().to_vec())
            .map_err(|_| Error::ics02(ClientError::empty_prefix()))
    }

    /// Query the application status
    fn query_application_status(&self) -> Result<ChainStatus, Error> {
        crate::time!("query_application_status");
        crate::telemetry!(query, self.id(), "query_application_status");

        let finalized_head = self
            .rt
            .block_on(self.rpc_client.rpc().finalized_head())
            .unwrap();
        info!("finalized_header '{}'", finalized_head);
        let block = self
            .rt
            .block_on(self.rpc_client.rpc().block(Some(finalized_head)))
            .unwrap();
        // info!("extrinsics[0] '{:?}'", block.unwrap().block.extrinsics[0]);
        // let extrinsic = block.unwrap().block.extrinsics[0];
        // let b = parity_scale_codec::Decode::decode::<substrate::timestamp::calls::TransactionApi>(&mut extrinsic.0[..]);
        // let b = parity_scale_codec::Decode::decode::<substrate::runtime_types::kitchensink_runtime::RuntimeCall>(&mut extrinsic.0[..]);

        // let a = substrate::runtime_types::kitchensink_runtime::RuntimeCall::Timestamp.decode(&mut extrinsic);
        // let a = parity_scale_codec::Decode::decode::<substrate::timestamp::calls::Set>(&mut block.unwrap().block.extrinsics[0]);

        Ok(ChainStatus {
           height: ICSHeight::new(0, u64::from(block.unwrap().block.header.number)).unwrap(),
           timestamp: Timestamp::default(),
        })
    }

    fn query_clients(
        &self,
        _request: QueryClientStatesRequest,
    ) -> Result<Vec<IdentifiedAnyClientState>, Error> {
        unimplemented!();
    }

    fn query_client_state(
        &self,
        request: QueryClientStateRequest,
        include_proof: IncludeProof,
    ) -> Result<(AnyClientState, Option<MerkleProof>), Error> {
        println!("query_client_state: request: {:?}", request);
        println!("query_client_state: include_proof: {:?}", include_proof);

        let client_id = substrate::runtime_types::ibc::core::ics24_host::identifier::ClientId(
            request.client_id.to_string(),
        );
        let storage = substrate::storage().ibc().client_states(client_id);

        let states = self
            .rt
            .block_on(self.rpc_client.storage().fetch(&storage, None))
            .unwrap();

        let client_state = AnyClientState::decode_vec(&states.unwrap()).map_err(Error::decode)?;

        println!("states: {:?}", client_state);
        Ok((client_state, None))
    }

    fn query_upgraded_client_state(
        &self,
        _request: QueryUpgradedClientStateRequest,
    ) -> Result<(AnyClientState, MerkleProof), Error> {
        unimplemented!();
    }

    fn query_upgraded_consensus_state(
        &self,
        _request: QueryUpgradedConsensusStateRequest,
    ) -> Result<(AnyConsensusState, MerkleProof), Error> {
        unimplemented!();
    }

    fn query_consensus_state(
        &self,
        request: QueryConsensusStateRequest,
        include_proof: IncludeProof,
    ) -> Result<(AnyConsensusState, Option<MerkleProof>), Error> {
        let client_id = substrate::runtime_types::ibc::core::ics24_host::identifier::ClientId(
            request.client_id.to_string(),
        );

        let height = substrate::runtime_types::ibc::core::ics02_client::height::Height {
            revision_number: request.consensus_height.revision_number(),
            revision_height: request.consensus_height.revision_height(),
        };
        let storage = substrate::storage()
            .ibc()
            .consensus_states(client_id, height);

        let consensus_states = self
            .rt
            .block_on(self.rpc_client.storage().fetch(&storage, None))
            .unwrap();

        let consensus_state =
            AnyConsensusState::decode_vec(&consensus_states.unwrap()).map_err(Error::decode)?;

        println!("consensus_state: {:?}", consensus_state);
        Ok((consensus_state, None))
    }

    /// Query the heights of every consensus state for a given client.
    fn query_consensus_state_heights(
        &self,
        _request: QueryConsensusStateHeightsRequest,
    ) -> Result<Vec<ICSHeight>, Error> {
        unimplemented!();
    }

    fn query_client_connections(
        &self,
        _request: QueryClientConnectionsRequest,
    ) -> Result<Vec<ConnectionId>, Error> {
        unimplemented!();
    }

    fn query_connections(
        &self,
        _request: QueryConnectionsRequest,
    ) -> Result<Vec<IdentifiedConnectionEnd>, Error> {
        unimplemented!();
    }

    fn query_connection(
        &self,
        request: QueryConnectionRequest,
        include_proof: IncludeProof,
    ) -> Result<(ConnectionEnd, Option<MerkleProof>), Error> {
        let connection_id =
            substrate::runtime_types::ibc::core::ics24_host::identifier::ConnectionId(
                request.connection_id.to_string(),
            );
        let storage = substrate::storage().ibc().connections(connection_id);
        let connection = self
            .rt
            .block_on(self.rpc_client.storage().fetch(&storage, None))
            .unwrap();

        let conn = connection.unwrap();

        println!("connection: {:?}", conn); // update ConnectionsPath key
        Ok((conn.into(), None))
    }

    fn query_connection_channels(
        &self,
        _request: QueryConnectionChannelsRequest,
    ) -> Result<Vec<IdentifiedChannelEnd>, Error> {
        unimplemented!();
    }

    fn query_channels(
        &self,
        _request: QueryChannelsRequest,
    ) -> Result<Vec<IdentifiedChannelEnd>, Error> {
        unimplemented!();
    }

    fn query_channel(
        &self,
        _request: QueryChannelRequest,
        _include_proof: IncludeProof,
    ) -> Result<(ChannelEnd, Option<MerkleProof>), Error> {
        unimplemented!();
    }

    fn query_channel_client_state(
        &self,
        request: QueryChannelClientStateRequest,
    ) -> Result<Option<IdentifiedAnyClientState>, Error> {
        unimplemented!();
    }

    fn query_packet_commitment(
        &self,
        _request: QueryPacketCommitmentRequest,
        _include_proof: IncludeProof,
    ) -> Result<(Vec<u8>, Option<MerkleProof>), Error> {
        unimplemented!();
    }

    /// Queries the packet commitment hashes associated with a channel.
    fn query_packet_commitments(
        &self,
        _request: QueryPacketCommitmentsRequest,
    ) -> Result<(Vec<Sequence>, ICSHeight), Error> {
        unimplemented!();
    }

    fn query_packet_receipt(
        &self,
        _request: QueryPacketReceiptRequest,
        _include_proof: IncludeProof,
    ) -> Result<(Vec<u8>, Option<MerkleProof>), Error> {
        unimplemented!();
    }

    /// Queries the unreceived packet sequences associated with a channel.
    fn query_unreceived_packets(
        &self,
        _request: QueryUnreceivedPacketsRequest,
    ) -> Result<Vec<Sequence>, Error> {
        unimplemented!();
    }

    fn query_packet_acknowledgement(
        &self,
        _request: QueryPacketAcknowledgementRequest,
        _include_proof: IncludeProof,
    ) -> Result<(Vec<u8>, Option<MerkleProof>), Error> {
        unimplemented!();
    }

    /// Queries the packet acknowledgment hashes associated with a channel.
    fn query_packet_acknowledgements(
        &self,
        _request: QueryPacketAcknowledgementsRequest,
    ) -> Result<(Vec<Sequence>, ICSHeight), Error> {
        unimplemented!();
    }

    /// Queries the unreceived acknowledgements sequences associated with a channel.
    fn query_unreceived_acknowledgements(
        &self,
        _request: QueryUnreceivedAcksRequest,
    ) -> Result<Vec<Sequence>, Error> {
        unimplemented!();
    }

    fn query_next_sequence_receive(
        &self,
        _request: QueryNextSequenceReceiveRequest,
        _include_proof: IncludeProof,
    ) -> Result<(Sequence, Option<MerkleProof>), Error> {
        unimplemented!();
    }

    /// This function queries transactions for events matching certain criteria.
    /// 1. Client Update request - returns a vector with at most one update client event
    /// 2. Transaction event request - returns all IBC events resulted from a Tx execution
    fn query_txs(&self, _request: QueryTxRequest) -> Result<Vec<IbcEventWithHeight>, Error> {
        unimplemented!();
    }

    /// This function queries transactions for packet events matching certain criteria.
    /// It returns at most one packet event for each sequence specified in the request.
    ///    Note - there is no way to format the packet query such that it asks for Tx-es with either
    ///    sequence (the query conditions can only be AND-ed).
    ///    There is a possibility to include "<=" and ">=" conditions but it doesn't work with
    ///    string attributes (sequence is emmitted as a string).
    ///    Therefore, for packets we perform one tx_search for each sequence.
    ///    Alternatively, a single query for all packets could be performed but it would return all
    ///    packets ever sent.
    fn query_packet_events(
        &self,
        mut _request: QueryPacketEventDataRequest,
    ) -> Result<Vec<IbcEventWithHeight>, Error> {
        unimplemented!();
    }

    fn query_host_consensus_state(
        &self,
        _request: QueryHostConsensusStateRequest,
    ) -> Result<Self::ConsensusState, Error> {
        unimplemented!();
    }

    fn build_client_state(
        &self,
        height: ICSHeight,
        _settings: ClientSettings,
    ) -> Result<Self::ClientState, Error> {
        println!("ys-debug: in build_client_state");

        // Build the client state.
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
        Ok(SmClientState {
            sequence: height.revision_height(),
            is_frozen: false,
            consensus_state: SmConsensusState {
                public_key: pk,
                diversifier: "oct".to_string(),
                // timestamp: timestamp_nanos as u64,
                timestamp: 9999,
                root: CommitmentRoot::from_bytes(&pk.to_bytes()),
            },
            allow_update_after_proposal: false,
        })
    }

    fn build_consensus_state(
        &self,
        _light_block: Self::LightBlock,
    ) -> Result<Self::ConsensusState, Error> {
        println!("ys-debug: in build_consensus_state");
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
        println!(
                "ys-debug: in build_header, trusted_height: {:?}, target_height: {:?}, client_state: {:?}",
            trusted_height, target_height, client_state
        );
        if trusted_height.revision_height() >= target_height.revision_height() {
            return Err(Error::ics02(ClientError::invalid_height()));
        }
        let cs = if let AnyClientState::Solomachine(cs) = client_state {
            cs
        } else {
            todo!()
        };
        let mut timestamp = cs.consensus_state.timestamp;
        let mut h: Self::Header = SmHeader {
            sequence: 0,
            timestamp: 0,
            signature: vec![],
            new_public_key: None,
            new_diversifier: "oct".to_string(),
        };
        let mut hs: Vec<Self::Header> = Vec::new();
        let start = if trusted_height.revision_height() > cs.sequence {
            trusted_height.revision_height()
        } else {
            cs.sequence
        };
        let end = if target_height.revision_height() > cs.sequence {
            target_height.revision_height()
        } else {
            cs.sequence + 1
        };

        for seq in start..end {
            let pk = PublicKey(
                tendermint::PublicKey::from_raw_secp256k1(&hex_literal::hex!(
                    "02c88aca653727db28e0ade87497c1f03b551143dedfd4db8de71689ad5e38421c"
                ))
                .unwrap(),
            );
            println!("pk: {:?}", pk);
            let duration_since_epoch = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            let timestamp_nanos = duration_since_epoch
                // .checked_sub(Duration::from_secs(5))
                // .unwrap()
                .as_nanos() as u64; // u128
            let data = HeaderData {
                new_pub_key: Some(pk),
                new_diversifier: "oct".to_string(),
            };
            timestamp = timestamp + 1;

            let sig_data = alice_sign_sign_bytes(
                seq,
                timestamp,
                DataType::Header.into(),
                data.encode_vec().unwrap(),
            );

            let header = SmHeader {
                sequence: seq,
                timestamp,
                signature: sig_data,
                new_public_key: Some(pk),
                new_diversifier: "oct".to_string(),
            };

            if seq == end - 1 {
                h = header;
            } else {
                hs.push(header);
            }
        }

        Ok((h, hs))
    }

    fn maybe_register_counterparty_payee(
        &mut self,
        _channel_id: &ChannelId,
        _port_id: &PortId,
        _counterparty_payee: &Signer,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn cross_chain_query(
        &self,
        _requests: Vec<CrossChainQueryRequest>,
    ) -> Result<Vec<CrossChainQueryResponse>, Error> {
        unimplemented!();
    }

    /// Builds the required proofs and the client state for connection handshake messages.
    /// The proofs and client state must be obtained from queries at same height.
    fn build_connection_proofs_and_client_state(
        &self,
        message_type: ConnectionMsgType,
        connection_id: &ConnectionId,
        client_id: &ClientId,
        height: ICSHeight,
    ) -> Result<(Option<AnyClientState>, Proofs), Error> {
        let (connection_end, maybe_connection_proof) = self.query_connection(
            QueryConnectionRequest {
                connection_id: connection_id.clone(),
                height: QueryHeight::Specific(height),
            },
            IncludeProof::No,
        )?;

        // let prefix = CommitmentPrefix::from("ibc");
        // let path = apply_prefix(&prefix, vec!["connections".to_string(), connection_id.to_string()]);

        let mut buf = Vec::new();
        let data = ConnectionStateData {
            path: ("/ibc/connections%2F".to_string() + connection_id.as_str()).into(),
            connection: Some(connection_end.clone().into()),
        };
        println!("ys-debug: ConnectionStateData: {:?}", data);
        Message::encode(&data, &mut buf).unwrap();

        let duration_since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let timestamp_nanos = duration_since_epoch.as_nanos() as u64; // u128

        let sig_data = alice_sign_sign_bytes(
            height.revision_height() + 1,
            timestamp_nanos,
            DataType::ConnectionState.into(),
            buf.to_vec(),
        );

        let timestamped = TimestampedSignatureData {
            signature_data: sig_data,
            timestamp: timestamp_nanos,
        };
        let mut proof_init = Vec::new();
        Message::encode(&timestamped, &mut proof_init).unwrap();

        // Check that the connection state is compatible with the message
        match message_type {
            ConnectionMsgType::OpenTry => {
                if !connection_end.state_matches(&State::Init)
                    && !connection_end.state_matches(&State::TryOpen)
                {
                    return Err(Error::bad_connection_state());
                }
            }
            ConnectionMsgType::OpenAck => {
                if !connection_end.state_matches(&State::TryOpen)
                    && !connection_end.state_matches(&State::Open)
                {
                    return Err(Error::bad_connection_state());
                }
            }
            ConnectionMsgType::OpenConfirm => {
                if !connection_end.state_matches(&State::Open) {
                    return Err(Error::bad_connection_state());
                }
            }
        }

        let mut client_state = None;
        let mut client_proof = None;
        let mut consensus_proof = None;

        match message_type {
            ConnectionMsgType::OpenTry | ConnectionMsgType::OpenAck => {
                let (client_state_value, maybe_client_state_proof) = self.query_client_state(
                    QueryClientStateRequest {
                        client_id: client_id.clone(),
                        height: QueryHeight::Specific(height),
                    },
                    IncludeProof::No,
                )?;

                let mut buf = Vec::new();
                let data = ClientStateData {
                    path: ("/ibc/clients%2F".to_string()
                        + client_id.as_str()
                        + &"%2FclientState".to_string())
                        .into(),
                    client_state: Some(client_state_value.clone().into()),
                };
                println!("ys-debug: ClientStateData: {:?}", data);
                Message::encode(&data, &mut buf).unwrap();

                let duration_since_epoch = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap();
                let timestamp_nanos = duration_since_epoch.as_nanos() as u64; // u128

                let sig_data = alice_sign_sign_bytes(
                    height.revision_height() + 2,
                    timestamp_nanos,
                    DataType::ClientState.into(),
                    buf.to_vec(),
                );

                let timestamped = TimestampedSignatureData {
                    signature_data: sig_data,
                    timestamp: timestamp_nanos,
                };
                let mut proof_client = Vec::new();
                Message::encode(&timestamped, &mut proof_client).unwrap();

                client_proof = Some(
                    CommitmentProofBytes::try_from(proof_client).map_err(Error::malformed_proof)?,
                );

                let (consensus_state_value, maybe_consensus_state_proof) = self
                    .query_consensus_state(
                        QueryConsensusStateRequest {
                            client_id: client_id.clone(),
                            consensus_height: client_state_value.latest_height(),
                            query_height: QueryHeight::Specific(height),
                        },
                        IncludeProof::No,
                    )?;

                let mut buf = Vec::new();
                let data = ConsensusStateData {
                    path: ("/ibc/clients%2F".to_string()
                        + client_id.as_str()
                        + &"%2FconsensusStates%2F0-".to_string()
                        + &client_state_value
                            .latest_height()
                            .revision_height()
                            .to_string())
                        .into(),
                    consensus_state: Some(consensus_state_value.clone().into()),
                };
                println!("ys-debug: ConsensusStateData: {:?}", data);
                Message::encode(&data, &mut buf).unwrap();

                let duration_since_epoch = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap();
                let timestamp_nanos = duration_since_epoch.as_nanos() as u64; // u128

                let sig_data = alice_sign_sign_bytes(
                    height.revision_height() + 3,
                    timestamp_nanos,
                    DataType::ConsensusState.into(),
                    buf.to_vec(),
                );

                let timestamped = TimestampedSignatureData {
                    signature_data: sig_data,
                    timestamp: timestamp_nanos,
                };
                let mut consensus_state_proof = Vec::new();
                Message::encode(&timestamped, &mut consensus_state_proof).unwrap();

                consensus_proof = Option::from(
                    ConsensusProof::new(
                        CommitmentProofBytes::try_from(consensus_state_proof)
                            .map_err(Error::malformed_proof)?,
                        client_state_value.latest_height(),
                    )
                    .map_err(Error::consensus_proof)?,
                );

                client_state = Some(client_state_value);
            }
            _ => {}
        }

        Ok((
            client_state,
            Proofs::new(
                CommitmentProofBytes::try_from(proof_init.to_vec())
                    .map_err(Error::malformed_proof)?,
                client_proof,
                consensus_proof,
                None,
                height.increment(),
            )
            .map_err(Error::malformed_proof)?,
        ))
    }
}

// Todo: to create a new type in `commitment_proof::Proof`
/// Compose merkle proof according to ibc proto
pub fn compose_ibc_merkle_proof(proof: subxt::rpc::ReadProof<H256>) -> MerkleProof {
    use ics23::{commitment_proof, ExistenceProof, InnerOp};
    tracing::trace!("in substrate: [compose_ibc_merkle_proof]");

    let proofs = proof
        .proof
        .iter()
        .map(|p| ics23::CommitmentProof {
            proof: Some(commitment_proof::Proof::Exist(ExistenceProof {
                key: vec![0],
                value: p.to_vec(),
                leaf: None,
                path: vec![InnerOp {
                    hash: 0,
                    prefix: vec![0],
                    suffix: vec![0],
                }],
            })),
        })
        .collect::<Vec<ics23::CommitmentProof>>();

    MerkleProof { proofs }
}

fn alice_sign_sign_bytes(sequence: u64, timestamp: u64, data_type: i32, data: Vec<u8>) -> Vec<u8> {
    use ibc_proto::cosmos::tx::signing::v1beta1::signature_descriptor::{
        data::{Single, Sum},
        Data,
    };
    use ibc_proto::ibc::lightclients::solomachine::v2::SignBytes;

    println!(
        "ys-debug: sequence {:?}, timestamp: {:?}, data_type: {:?}, data: {:?}",
        sequence, timestamp, data_type, data
    );
    let bytes = SignBytes {
        sequence,
        timestamp,
        diversifier: "oct".to_string(),
        data_type,
        data,
    };
    let mut buf = Vec::new();
    Message::encode(&bytes, &mut buf).unwrap();
    println!("encoded_bytes: {:?}", buf);
    let standard = StandardHDPath::from_str("m/44'/60'/0'/0/0").unwrap();

    // m/44'/60'/0'/0/0
    // 0xd73E35f53b8180b241E70C0e9040173dd8D0e2A0
    // 0x02c88aca653727db28e0ade87497c1f03b551143dedfd4db8de71689ad5e38421c
    // 0x281afd44d50ffd0bab6502cbb9bc58a7f9b53813c862db01836d46a27b51168c

    let key_pair = Secp256k1KeyPair::from_mnemonic("captain walk infant web eye return ahead once face sunny usage devote cotton car old check symbol antique derive wire kid solve forest fish", &standard, &AddressType::Cosmos, "oct").unwrap();
    let signature = key_pair.sign(&buf).unwrap();
    println!("signature: {:?}", signature);
    let sig = Data {
        sum: Some(Sum::Single(Single { mode: 1, signature })),
    };
    buf = Vec::new();
    Message::encode(&sig, &mut buf).unwrap();

    println!("ys-debug: sig_data: {:?}", buf);
    buf
}

#[cfg(test)]
mod tests {}
