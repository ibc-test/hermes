use core::borrow::Borrow;
use core::str::FromStr;
use near_primitives::views::StateItem;
use std::collections::HashMap;
use ibc_relayer_types::events::{IbcEvent as HermesIbcEvent, ModuleEventAttribute, ModuleId};
use ibc::events::IbcEvent;
use ibc_relayer_types::core::ics02_client::client_type::ClientType;
use ibc_relayer_types::core::ics02_client::height::Height;
use ibc_relayer_types::core::ics04_channel::packet::Packet;
use ibc_relayer_types::core::ics04_channel::timeout::TimeoutHeight;
use ibc_relayer_types::core::ics24_host::identifier::{ChannelId, ClientId, ConnectionId, PortId};
use ibc_relayer_types::timestamp::Timestamp;

/// Convert `StateItem`s over to a Map<data_key, value_bytes> representation.
/// Assumes key and value are base64 encoded, so this also decodes them.
pub(crate) fn into_state_map(
    state_items: &[StateItem],
) -> anyhow::Result<HashMap<Vec<u8>, Vec<u8>>> {
    let decode = |s: &StateItem| Ok((base64::decode(&s.key)?, base64::decode(&s.value)?));

    state_items.iter().map(decode).collect()
}

/// Convert IbcEvent define in ibc::events::IbcEvent to IbcEvent in crates/relayer-types/src/events.rs
pub fn convert_ibc_event_to_hermes_ibc_event(ibc_event: IbcEvent) -> HermesIbcEvent {
    match ibc_event {
        IbcEvent::CreateClient(create_client) => {
            use ibc_relayer_types::core::ics02_client::events::Attributes;

            HermesIbcEvent::CreateClient(
                ibc_relayer_types::core::ics02_client::events::CreateClient::from(Attributes {
                    client_id: ClientId::from_str(create_client.client_id().as_str()).unwrap(),
                    client_type: ClientType::from_str(create_client.client_type().as_str()).unwrap(),
                    consensus_height: Height::new(
                        create_client.consensus_height().revision_number(),
                        create_client.consensus_height().revision_height(),
                    ).unwrap(),
                })
            )
        }
        IbcEvent::UpdateClient(update_client) => {
            use ibc_relayer_types::core::ics02_client::events::Attributes;

            HermesIbcEvent::UpdateClient(
                ibc_relayer_types::core::ics02_client::events::UpdateClient::from(Attributes {
                    client_id: ClientId::from_str(update_client.client_id().as_str()).unwrap(),
                    client_type: ClientType::from_str(update_client.client_type().as_str()).unwrap(),
                    consensus_height: Height::new(
                        update_client.consensus_height().revision_number(),
                        update_client.consensus_height().revision_height(),
                    ).unwrap(),
                })
            )
        }
        IbcEvent::UpgradeClient(upgrade_client) => {
            use ibc_relayer_types::core::ics02_client::events::Attributes;

            HermesIbcEvent::UpgradeClient(
                ibc_relayer_types::core::ics02_client::events::UpgradeClient::from(Attributes {
                    client_id: ClientId::from_str(upgrade_client.client_id().as_str()).unwrap(),
                    client_type: ClientType::from_str(upgrade_client.client_type().as_str()).unwrap(),
                    consensus_height: Height::new(
                        upgrade_client.consensus_height().revision_number(),
                        upgrade_client.consensus_height().revision_height(),
                    ).unwrap(),
                })
            )
        }
        IbcEvent::ClientMisbehaviour(client_misbehaviour) => {
            use ibc_relayer_types::core::ics02_client::events::Attributes;
            HermesIbcEvent::ClientMisbehaviour(
                ibc_relayer_types::core::ics02_client::events::ClientMisbehaviour::from(Attributes {
                    client_id: ClientId::from_str(client_misbehaviour.client_id().as_str()).unwrap(),
                    client_type: ClientType::from_str(client_misbehaviour.client_type().as_str()).unwrap(),
                    consensus_height: todo!(), //todo in ibc-rs(latest) have not this variant
                })
            )

        }
        IbcEvent::OpenInitConnection(open_init_connection) => {
            use ibc_relayer_types::core::ics03_connection::events::Attributes;
            HermesIbcEvent::OpenInitConnection(
                ibc_relayer_types::core::ics03_connection::events::OpenInit::from(Attributes {
                    connection_id: Some(ConnectionId::from_str(open_init_connection.connection_id().as_str()).unwrap()),
                    client_id: ClientId::from_str(open_init_connection.client_id().as_str()).unwrap(),
                    counterparty_connection_id: open_init_connection
                        .counterparty_connection_id()
                        .map(|e| {
                            ConnectionId::from_str(e.as_str()).unwrap()
                        }),
                    counterparty_client_id: Default::default()
                })
            )
        }
        IbcEvent::OpenTryConnection(open_try_connection) => {
            use ibc_relayer_types::core::ics03_connection::events::Attributes;
            HermesIbcEvent::OpenTryConnection(
                ibc_relayer_types::core::ics03_connection::events::OpenTry::from(Attributes {
                    connection_id: Some(ConnectionId::from_str(open_try_connection.connection_id().as_str()).unwrap()),
                    client_id: ClientId::from_str(open_try_connection.client_id().as_str()).unwrap(),
                    counterparty_connection_id: open_try_connection
                        .counterparty_connection_id()
                        .map(|e| {
                            ConnectionId::from_str(e.as_str()).unwrap()
                        }),
                    counterparty_client_id: Default::default()
                })
            )
        }
        IbcEvent::OpenAckConnection(open_ack_connection) => {
            use ibc_relayer_types::core::ics03_connection::events::Attributes;
            HermesIbcEvent::OpenAckConnection(
                ibc_relayer_types::core::ics03_connection::events::OpenAck::from(Attributes {
                    connection_id: Some(ConnectionId::from_str(open_ack_connection.connection_id().as_str()).unwrap()),
                    client_id: ClientId::from_str(open_ack_connection.client_id().as_str()).unwrap(),
                    counterparty_connection_id: open_ack_connection
                        .counterparty_connection_id()
                        .map(|e| {
                            ConnectionId::from_str(e.as_str()).unwrap()
                        }),
                    counterparty_client_id: Default::default()
                })
            )
        }
        IbcEvent::OpenConfirmConnection(open_confirm_connection) => {
            use ibc_relayer_types::core::ics03_connection::events::Attributes;
            HermesIbcEvent::OpenConfirmConnection(
                ibc_relayer_types::core::ics03_connection::events::OpenConfirm::from(Attributes {
                    connection_id: Some(ConnectionId::from_str(open_confirm_connection.connection_id().as_str()).unwrap()),
                    client_id: ClientId::from_str(open_confirm_connection.client_id().as_str()).unwrap(),
                    counterparty_connection_id: open_confirm_connection
                        .counterparty_connection_id()
                        .map(|e| {
                            ConnectionId::from_str(e.as_str()).unwrap()
                        }),
                    counterparty_client_id: Default::default()
                })
            )
        }
        IbcEvent::OpenInitChannel(open_init_channel) => {
            HermesIbcEvent::OpenInitChannel(
                ibc_relayer_types::core::ics04_channel::events::OpenInit {
                    port_id: PortId::from_str(open_init_channel.port_id().as_str()).unwrap(),
                    channel_id: Some(ChannelId::from_str(open_init_channel.channel_id().as_str()).unwrap()),
                    connection_id: ConnectionId::from_str(open_init_channel.connection_id().as_str()).unwrap(),
                    counterparty_port_id: PortId::from_str(open_init_channel.counterparty_port_id().as_str()).unwrap(),
                    counterparty_channel_id: None
                } 
            ) 
        }
        IbcEvent::OpenTryChannel(open_try_channel) => {
            HermesIbcEvent::OpenTryChannel(
                ibc_relayer_types::core::ics04_channel::events::OpenTry {
                    port_id: PortId::from_str(open_try_channel.port_id().as_str()).unwrap(),
                    channel_id: Some(ChannelId::from_str(open_try_channel.channel_id().as_str()).unwrap()),
                    connection_id: ConnectionId::from_str(open_try_channel.connection_id().as_str()).unwrap(),
                    counterparty_port_id: PortId::from_str(open_try_channel.counterparty_port_id().as_str()).unwrap(),
                    counterparty_channel_id: None
                }
            )
        }
        IbcEvent::OpenAckChannel(open_ack_channel) => {
            HermesIbcEvent::OpenAckChannel(
                ibc_relayer_types::core::ics04_channel::events::OpenAck {
                    port_id: PortId::from_str(open_ack_channel.port_id().as_str()).unwrap(),
                    channel_id: Some(ChannelId::from_str(open_ack_channel.channel_id().as_str()).unwrap()),
                    connection_id: ConnectionId::from_str(open_ack_channel.connection_id().as_str()).unwrap(),
                    counterparty_port_id: PortId::from_str(open_ack_channel.counterparty_port_id().as_str()).unwrap(),
                    counterparty_channel_id: None
                } 
            )
            
        }
        IbcEvent::OpenConfirmChannel(open_confirm_channel) => {
            HermesIbcEvent::OpenConfirmChannel(
                ibc_relayer_types::core::ics04_channel::events::OpenConfirm {
                    port_id: PortId::from_str(open_confirm_channel.port_id().as_str()).unwrap(),
                    channel_id: Some(ChannelId::from_str(open_confirm_channel.channel_id().as_str()).unwrap()),
                    connection_id: ConnectionId::from_str(open_confirm_channel.connection_id().as_str()).unwrap(),
                    counterparty_port_id: PortId::from_str(open_confirm_channel.counterparty_port_id().as_str()).unwrap(),
                    counterparty_channel_id: None
                } 
            )
        }
        IbcEvent::CloseInitChannel(close_init_channel) => {
            HermesIbcEvent::CloseInitChannel(
                ibc_relayer_types::core::ics04_channel::events::CloseInit {
                    port_id: PortId::from_str(close_init_channel.port_id().as_str()).unwrap(),
                    channel_id: ChannelId::from_str(close_init_channel.channel_id().as_str()).unwrap(),
                    connection_id: ConnectionId::from_str(close_init_channel.connection_id().as_str()).unwrap(),
                    counterparty_port_id: PortId::from_str(close_init_channel.counterparty_port_id().as_str()).unwrap(),
                    counterparty_channel_id: None
                } 
            )
            
        }
        IbcEvent::CloseConfirmChannel(close_confirm_channel) => {
            HermesIbcEvent::CloseConfirmChannel(
                ibc_relayer_types::core::ics04_channel::events::CloseConfirm {
                    channel_id: Some(ChannelId::from_str(close_confirm_channel.channel_id().as_str()).unwrap()),
                    port_id: PortId::from_str(close_confirm_channel.port_id().as_str()).unwrap(),
                    connection_id: ConnectionId::from_str(close_confirm_channel.connection_id().as_str()).unwrap(),
                    counterparty_port_id: PortId::from_str(close_confirm_channel.counterparty_port_id().as_str()).unwrap(),
                    counterparty_channel_id: None
                } 
            )
        }
        IbcEvent::SendPacket(send_packet) => {
            HermesIbcEvent::SendPacket(
                ibc_relayer_types::core::ics04_channel::events::SendPacket {
                    packet: Packet {
                        sequence: u64::from(*send_packet.sequence()).into(),
                        source_port: PortId::from_str(send_packet.src_port_id().as_str()).unwrap(),
                        source_channel: ChannelId::from_str(send_packet.src_channel_id().as_str()).unwrap(),
                        destination_port: PortId::from_str(send_packet.dst_port_id().as_str()).unwrap() ,
                        destination_channel: ChannelId::from_str(send_packet.dst_channel_id().as_str()).unwrap(),
                        data: send_packet.packet_data().to_vec(),
                        timeout_height: convert_timeout_height(send_packet.timeout_height().clone()),
                        timeout_timestamp: Timestamp::from_nanoseconds(send_packet.timeout_timestamp().nanoseconds()).unwrap()
                    }  
                }
            )
        }
        IbcEvent::ReceivePacket(receive_packet) => {
            HermesIbcEvent::ReceivePacket(
                ibc_relayer_types::core::ics04_channel::events::ReceivePacket {
                    packet: Packet {
                        sequence: u64::from(*receive_packet.sequence()).into(),
                        source_port: PortId::from_str(receive_packet.src_port_id().as_str()).unwrap(),
                        source_channel: ChannelId::from_str(receive_packet.src_channel_id().as_str()).unwrap(),
                        destination_port: PortId::from_str(receive_packet.dst_port_id().as_str()).unwrap() ,
                        destination_channel: ChannelId::from_str(receive_packet.dst_channel_id().as_str()).unwrap(),
                        data: receive_packet.packet_data().to_vec(),
                        timeout_height: convert_timeout_height(receive_packet.timeout_height().clone()),
                        timeout_timestamp: Timestamp::from_nanoseconds(receive_packet.timeout_timestamp().nanoseconds()).unwrap()
                    }
                }
            )
        }
        IbcEvent::WriteAcknowledgement(write_acknowledgement) => {
            HermesIbcEvent::WriteAcknowledgement(
                ibc_relayer_types::core::ics04_channel::events::WriteAcknowledgement {
                    packet: Packet {
                        sequence: u64::from(*write_acknowledgement.sequence()).into(),
                        source_port: PortId::from_str(write_acknowledgement.src_port_id().as_str()).unwrap(),
                        source_channel: ChannelId::from_str(write_acknowledgement.src_channel_id().as_str()).unwrap(),
                        destination_port: PortId::from_str(write_acknowledgement.dst_port_id().as_str()).unwrap() ,
                        destination_channel: ChannelId::from_str(write_acknowledgement.dst_channel_id().as_str()).unwrap(),
                        data: write_acknowledgement.packet_data().to_vec(),
                        timeout_height: convert_timeout_height(write_acknowledgement.timeout_height().clone()),
                        timeout_timestamp: Timestamp::from_nanoseconds(write_acknowledgement.timeout_timestamp().nanoseconds()).unwrap()
                    },
                    ack: write_acknowledgement.acknowledgement().as_bytes().to_vec()
                }
            )
        }
        IbcEvent::AcknowledgePacket(acknowledge_packet) => {
            HermesIbcEvent::AcknowledgePacket(
                ibc_relayer_types::core::ics04_channel::events::AcknowledgePacket {
                    packet: Packet {
                        sequence: u64::from(*acknowledge_packet.sequence()).into(),
                        source_port: PortId::from_str(acknowledge_packet.src_port_id().as_str()).unwrap(),
                        source_channel: ChannelId::from_str(acknowledge_packet.src_channel_id().as_str()).unwrap(),
                        destination_port: PortId::from_str(acknowledge_packet.dst_port_id().as_str()).unwrap() ,
                        destination_channel: ChannelId::from_str(acknowledge_packet.dst_channel_id().as_str()).unwrap(),
                        data: vec![],
                        timeout_height: convert_timeout_height(acknowledge_packet.timeout_height().clone()),
                        timeout_timestamp: Timestamp::from_nanoseconds(acknowledge_packet.timeout_timestamp().nanoseconds()).unwrap()
                    },
                }
            )
        }
        IbcEvent::TimeoutPacket(timeout_packet) => {
            HermesIbcEvent::TimeoutPacket(
                ibc_relayer_types::core::ics04_channel::events::TimeoutPacket {
                    packet: Packet {
                        sequence: u64::from(*timeout_packet.sequence()).into(),
                        source_port: PortId::from_str(timeout_packet.src_port_id().as_str()).unwrap(),
                        source_channel: ChannelId::from_str(timeout_packet.src_channel_id().as_str()).unwrap(),
                        destination_port: PortId::from_str(timeout_packet.dst_port_id().as_str()).unwrap() ,
                        destination_channel: ChannelId::from_str(timeout_packet.dst_channel_id().as_str()).unwrap(),
                        data: vec![],
                        timeout_height: convert_timeout_height(timeout_packet.timeout_height().clone()),
                        timeout_timestamp: Timestamp::from_nanoseconds(timeout_packet.timeout_timestamp().nanoseconds()).unwrap()
                    },
                }
            )
        }
        IbcEvent::ChannelClosed(channel_closed) => {
            todo!()
        }
        IbcEvent::AppModule(app_module) => {
            HermesIbcEvent::AppModule(
                ibc_relayer_types::events::ModuleEvent {
                    kind: app_module.kind,
                    module_name: ModuleId::from_str(app_module.module_name.borrow()).unwrap(),
                    attributes: app_module.attributes.iter().map(|attr| {
                        ModuleEventAttribute {
                            key: attr.key.clone(),
                            value: attr.value.clone()
                        }
                    }).collect()
                }
            )
        }
    }
}


fn convert_timeout_height(timeout_height: ibc::core::ics04_channel::timeout::TimeoutHeight)-> TimeoutHeight {
    match timeout_height {
        ibc::core::ics04_channel::timeout::TimeoutHeight::Never => {
            TimeoutHeight::Never
        }
        ibc::core::ics04_channel::timeout::TimeoutHeight::At(height) => {
            TimeoutHeight::At(Height::new(
                height.revision_number(),
                height.revision_height()).unwrap())
        }
    }
}
