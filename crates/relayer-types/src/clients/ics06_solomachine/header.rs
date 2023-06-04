use crate::prelude::*;
use alloc::string::ToString;
use core::fmt::{Error as FmtError, Formatter};

use bytes::Buf;
use ibc_proto::google::protobuf::Any;
use ibc_proto::protobuf::Protobuf;
use prost::Message;
use serde_derive::{Deserialize, Serialize};

use crate::clients::ics06_solomachine::error::Error;
use crate::core::ics02_client::client_type::ClientType;
use crate::core::ics02_client::error::Error as Ics02Error;
use crate::timestamp::Timestamp;
use crate::Height;

use crate::clients::ics06_solomachine::consensus_state::PublicKey;
use ibc_proto::ibc::lightclients::solomachine::v2::Header as RawHeader;

pub const SOLOMACHINE_HEADER_TYPE_URL: &str = "/ibc.lightclients.solomachine.v2.Header";

/// Tendermint consensus header
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Header {
    pub sequence: u64,
    pub timestamp: u64,
    pub signature: Vec<u8>,
    pub new_public_key: PublicKey,
    pub new_diversifier: String,
}

impl core::fmt::Debug for Header {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        write!(f, " Header {{...}}")
    }
}

// impl Display for Header {
//     fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
//         write!(f, "Header {{ signed_header: {}, validator_set: {}, trusted_height: {}, trusted_validator_set: {} }}", PrettySignedHeader(&self.signed_header), PrettyValidatorSet(&self.validator_set), self.trusted_height, PrettyValidatorSet(&self.trusted_validator_set))
//     }
// }

impl Header {
    pub fn height(&self) -> Height {
        Height::new(0, u64::from(self.sequence))
            .expect("malformed tendermint header domain type has an illegal height of 0")
    }
}

impl crate::core::ics02_client::header::Header for Header {
    fn client_type(&self) -> ClientType {
        ClientType::Solomachine
    }

    fn height(&self) -> Height {
        self.height()
    }

    fn timestamp(&self) -> Timestamp {
        Timestamp::from_nanoseconds(self.timestamp).unwrap()
    }
}

impl Protobuf<RawHeader> for Header {}

impl TryFrom<RawHeader> for Header {
    type Error = Error;

    fn try_from(raw: RawHeader) -> Result<Self, Self::Error> {
        let pk: PublicKey = raw.new_public_key.unwrap().try_into().unwrap();
        let header = Self {
            sequence: raw.sequence,
            timestamp: raw.timestamp,
            signature: raw.signature,
            new_public_key: pk,
            new_diversifier: raw.new_diversifier,
        };

        Ok(header)
    }
}

impl Protobuf<Any> for Header {}

impl TryFrom<Any> for Header {
    type Error = Ics02Error;

    fn try_from(raw: Any) -> Result<Self, Ics02Error> {
        use core::ops::Deref;

        fn decode_header<B: Buf>(buf: B) -> Result<Header, Error> {
            RawHeader::decode(buf).map_err(Error::decode)?.try_into()
        }

        match raw.type_url.as_str() {
            SOLOMACHINE_HEADER_TYPE_URL => decode_header(raw.value.deref()).map_err(Into::into),
            _ => Err(Ics02Error::unknown_header_type(raw.type_url)),
        }
    }
}

impl From<Header> for Any {
    fn from(header: Header) -> Self {
        Any {
            type_url: SOLOMACHINE_HEADER_TYPE_URL.to_string(),
            value: Protobuf::<RawHeader>::encode_vec(&header),
        }
    }
}

pub fn decode_header<B: Buf>(buf: B) -> Result<Header, Error> {
    RawHeader::decode(buf).map_err(Error::decode)?.try_into()
}

impl From<Header> for RawHeader {
    fn from(value: Header) -> Self {
        RawHeader {
            sequence: value.sequence,
            timestamp: value.timestamp,
            signature: value.signature,
            new_public_key: Some(value.new_public_key.into()),
            new_diversifier: value.new_diversifier,
        }
    }
}
