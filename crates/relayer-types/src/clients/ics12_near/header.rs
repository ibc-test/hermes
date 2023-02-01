use alloc::string::ToString;
use alloc::vec::Vec;
use bytes::Buf;
use ibc_proto::google::protobuf::Any;
use ibc_proto::protobuf::Protobuf;
use crate::clients::ics12_near::near_types::hash::CryptoHash;
use crate::clients::ics12_near::near_types::LightClientBlockView;
use crate::core::ics02_client::client_type::ClientType;
use crate::Height;
use crate::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use crate::clients::ics12_near::consensus_state::NEAR_CONSENSUS_STATE_TYPE_URL;
use crate::core::ics02_client::error::Error;
use core::fmt::{Display, Error as FmtError, Formatter};
use std::vec;

pub const NEAR_HEADER_TYPE_URL: &str = "/ibc.lightclients.near.v1.Header";

#[derive(Default, Clone,Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub light_client_block_view: LightClientBlockView,
    pub prev_state_root_of_chunks: Vec<CryptoHash>,
}

impl crate::core::ics02_client::header::Header for Header {
    fn client_type(&self) -> ClientType {
        ClientType::Near
    }

    fn height(&self) -> Height {
        Height::new(
            0,
            self.light_client_block_view.inner_lite.height)
            .unwrap()
    }

    fn timestamp(&self) -> Timestamp {
        Timestamp::from_nanoseconds(self.light_client_block_view.inner_lite.timestamp_nanosec)
            .unwrap()
    }
}


impl Protobuf<Any> for Header {}

impl TryFrom<Any> for Header {
    type Error = Error;

    fn try_from(raw: Any) -> Result<Self, Error> {
        use core::ops::Deref;

        fn decode_header<B: Buf>(buf: B) -> Result<Header, Error> {
            Ok(Header::default())
        }

        match raw.type_url.as_str() {
            NEAR_CONSENSUS_STATE_TYPE_URL => decode_header(raw.value.deref()).map_err(Into::into),
            _ => Err(Error::unknown_header_type(raw.type_url)),
        }
    }
}

impl From<Header> for Any {
    fn from(header: Header) -> Self {
        Any {
            type_url: NEAR_CONSENSUS_STATE_TYPE_URL.to_string(),
            value: vec![],
        }
    }
}