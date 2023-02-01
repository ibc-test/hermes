use alloc::vec::Vec;
use std::time::Duration;
use std::vec;
use serde::{Deserialize, Serialize};
use crate::core::ics02_client::height::Height;
use crate::core::ics02_client::client_state::{ClientState as Ics2ClientState, UpgradeOptions};
use crate::core::ics02_client::client_type::ClientType;
use crate::core::ics24_host::identifier::ChainId;
use ibc_proto::google::protobuf::Any;
use ibc_proto::protobuf::Protobuf;
use crate::clients::ics12_near::consensus_state::NEAR_CONSENSUS_STATE_TYPE_URL;
use crate::core::ics02_client::error::Error;
use core::str::FromStr;
use std::prelude::rust_2015::ToString;
use bytes::Buf;

pub const NEAR_CLIENT_STATE_TYPE_URL: &str = "/ibc.lightclients.near.v1.ClientState";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClientState {
    pub chain_id: ChainId,
    pub trusting_period: u64,
    pub latest_height: u64,
    pub latest_timestamp: u64,
    pub frozen_height: Option<u64>,
    pub upgrade_commitment_prefix: Vec<u8>,
    pub upgrade_key: Vec<u8>
}

impl ClientState {
    /// Get the refresh time to ensure the state does not expire
    pub fn refresh_time(&self) -> Option<Duration> {
        //TODO
        Some(Duration::new(3, 0))
    }
}

impl Ics2ClientState for ClientState {
    fn chain_id(&self) -> ChainId {
        self.chain_id.clone()
    }

    fn client_type(&self) -> ClientType {
        ClientType::Near
    }

    fn latest_height(&self) -> Height {
        Height::new(0, self.latest_height+1).unwrap()
    }

    fn frozen_height(&self) -> Option<crate::Height> {
        self.frozen_height.map(|frozen_height| {
            Height::new(0, frozen_height+1).unwrap()
        })
    }

    fn expired(&self, _elapsed: Duration) -> bool {
        false
    }

    fn upgrade(&mut self, _upgrade_height: crate::Height, _upgrade_options: &dyn UpgradeOptions, _chain_id: ChainId) {
    }
}

impl Protobuf<Any> for ClientState {}

impl From<ClientState> for Any {
    fn from(client_state: ClientState) -> Self {
        Any {
            type_url: NEAR_CONSENSUS_STATE_TYPE_URL.to_string(),
            value: vec![],
        }
    }
}

impl TryFrom<Any> for ClientState {
    type Error = Error;

    fn try_from(raw: Any) -> Result<Self, Error> {
        use core::ops::Deref;

        fn decode_header<B: Buf>(buf: B) -> Result<ClientState, Error> {
            Ok(ClientState::default())
        }

        match raw.type_url.as_str() {
            NEAR_CONSENSUS_STATE_TYPE_URL => decode_header(raw.value.deref()).map_err(Into::into),
            _ => Err(Error::unknown_header_type(raw.type_url)),
        }
    }
}