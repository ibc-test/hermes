use core::convert::{TryFrom, TryInto};
use core::time::Duration;

use prost::Message;
use serde::{Deserialize, Serialize};

use ibc_proto::google::protobuf::Any;
use ibc_proto::protobuf::Protobuf;

use crate::clients::ics06_solomachine::error::Error;
use crate::core::ics02_client::client_state::{
    ClientState as Ics2ClientState, UpgradeOptions as CoreUpgradeOptions,
};
use crate::core::ics02_client::client_type::ClientType;
use crate::core::ics02_client::error::Error as Ics02Error;
use crate::core::ics24_host::identifier::ChainId;
use crate::prelude::*;
use crate::Height;

use crate::clients::ics06_solomachine::consensus_state::ConsensusState;
use ibc_proto::ibc::lightclients::solomachine::v2::ClientState as RawSmClientState;

pub const SOLOMACHINE_CLIENT_STATE_TYPE_URL: &str = "/ibc.lightclients.solomachine.v2.ClientState";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientState {
    pub sequence: u64,
    pub is_frozen: bool,
    pub consensus_state: ConsensusState,
    pub allow_update_after_proposal: bool,
}

impl Ics2ClientState for ClientState {
    fn chain_id(&self) -> ChainId {
        if self.allow_update_after_proposal {
            ChainId::new("ibc".to_string(), 1)
        } else {
            ChainId::new("ibc".to_string(), 0)
        }
    }

    fn client_type(&self) -> ClientType {
        ClientType::Solomachine
    }

    fn latest_height(&self) -> Height {
        Height::new(0, self.sequence).unwrap()
    }

    fn frozen_height(&self) -> Option<Height> {
        None
    }

    fn upgrade(
        &mut self,
        _upgrade_height: Height,
        _upgrade_options: &dyn CoreUpgradeOptions,
        _chain_id: ChainId,
    ) {
    }

    fn expired(&self, _elapsed: Duration) -> bool {
        false
    }
}

impl Protobuf<RawSmClientState> for ClientState {}

impl TryFrom<RawSmClientState> for ClientState {
    type Error = Error;

    fn try_from(raw: RawSmClientState) -> Result<Self, Self::Error> {
        Ok(Self {
            sequence: raw.sequence,
            is_frozen: raw.is_frozen,
            consensus_state: raw.consensus_state.unwrap().try_into().unwrap(),
            allow_update_after_proposal: raw.allow_update_after_proposal,
        })
    }
}

impl From<ClientState> for RawSmClientState {
    fn from(value: ClientState) -> Self {
        Self {
            sequence: value.sequence,
            is_frozen: value.is_frozen,
            consensus_state: Some(value.consensus_state.into()),
            allow_update_after_proposal: value.allow_update_after_proposal,
        }
    }
}

impl Protobuf<Any> for ClientState {}

impl TryFrom<Any> for ClientState {
    type Error = Ics02Error;

    fn try_from(raw: Any) -> Result<Self, Self::Error> {
        use bytes::Buf;
        use core::ops::Deref;

        fn decode_client_state<B: Buf>(buf: B) -> Result<ClientState, Error> {
            RawSmClientState::decode(buf)
                .map_err(Error::decode)?
                .try_into()
        }

        match raw.type_url.as_str() {
            SOLOMACHINE_CLIENT_STATE_TYPE_URL => {
                decode_client_state(raw.value.deref()).map_err(Into::into)
            }
            _ => Err(Ics02Error::unknown_client_state_type(raw.type_url)),
        }
    }
}

impl From<ClientState> for Any {
    fn from(client_state: ClientState) -> Self {
        Any {
            type_url: SOLOMACHINE_CLIENT_STATE_TYPE_URL.to_string(),
            value: Protobuf::<RawSmClientState>::encode_vec(&client_state),
        }
    }
}
