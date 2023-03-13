use crate::prelude::*;
use serde::{Deserialize, Serialize};
use core::time::Duration;
use ed25519_dalek::PUBLIC_KEY_LENGTH;
use crate::core::ics02_client::client_state::{
    ClientState as Ics2ClientState, UpgradeOptions as CoreUpgradeOptions,
};
use crate::Height;
use crate::core::ics24_host::identifier::ChainId;
use crate::core::ics02_client::client_type::ClientType;
use ibc_proto::ibc::lightclients::solomachine::v1::ClientState as RawSmClientState;
use ibc_proto::ibc::lightclients::solomachine::v1::ConsensusState as RawSmConsesusState;
use ibc_proto::protobuf::Protobuf;
use ibc_proto::google::protobuf::Any;
use flex_error::{define_error, TraceError};
use crate::core::ics02_client::error::Error as Ics02Error;
// use prost::Message;
use crate::core::ics23_commitment::commitment::CommitmentRoot;
use crate::timestamp::Timestamp;
use ibc_proto::ibc::lightclients::solomachine::v1::ConsensusState as RawConsensusState;
use ibc_proto::ibc::lightclients::solomachine::v1::HeaderData as RawHeaderData;
use ibc_proto::ibc::lightclients::solomachine::v1::SignBytes as RawSignBytes;


// use secp256k1::{PublicKey, Secp256k1, SecretKey};
use eyre::Result;
use cosmos_sdk_proto::{
        self,
        traits::{Message, MessageExt},
    };
use ibc_proto::ibc::lightclients::solomachine::v1::Header as RawHeader;
use bytes::Buf;
use core::fmt::{Display, Error as FmtError, Formatter};



pub const SOLOMACHINE_CLIENT_STATE_TYPE_URL: &str = "/ibc.lightclients.solomachine.v2.ClientState";
pub const SOLOMACHINE_CONSENSUS_STATE_TYPE_URL: &str =
    "/ibc.lightclients.solomachine.v2.ConsensusState";
pub const SOLOMACHINE_HEADER_TYPE_URL: &str = "/ibc.lightclients.solomachine.v2.Header";



define_error! {
    #[derive(Debug, PartialEq, Eq)]
    Error {
        Solomachine
            |_| { "solomachine" },
        Decode
            [ TraceError<prost::DecodeError> ]
            | _ | { "decode error" },
    }
}

impl From<Error> for Ics02Error {
    fn from(e: Error) -> Self {
        Self::client_specific(e.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientState {
    pub sequence: u64,
    pub frozen_sequence: u64,
    pub consensus_state: ConsensusState,
    pub allow_update_after_proposal: bool,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            sequence: 1,
            frozen_sequence: 0,
            consensus_state: Default::default(),
            allow_update_after_proposal: false
        }
    }
}

impl Ics2ClientState for ClientState {
    fn chain_id(&self) -> ChainId {
        ChainId::new("ibc".to_string(), 1)
    }

    fn client_type(&self) -> ClientType {
        ClientType::Solomachine
    }

    fn latest_height(&self) -> Height {
        Height::new(0, self.sequence).unwrap()
    }

    fn frozen_height(&self) -> Option<Height> {
        // Some(Height::new(0, self.frozen_sequence).unwrap())
        None
    }

    fn upgrade(
            &mut self,
            upgrade_height: Height,
            upgrade_options: &dyn CoreUpgradeOptions,
            chain_id: ChainId,
            ) {
    }

    fn expired(&self, elapsed: Duration) -> bool {
        false
    }
}

impl Protobuf<RawSmClientState> for ClientState {}

impl TryFrom<RawSmClientState> for ClientState {
    type Error = Error;

    fn try_from(raw: RawSmClientState) -> Result<Self, Self::Error> {
        let cs = raw.consensus_state.unwrap();
        let pk = cs.public_key.unwrap().try_into().unwrap();
        Ok(Self {
            sequence: raw.sequence,
            frozen_sequence: raw.frozen_sequence,
            consensus_state: ConsensusState { public_key: pk, diversifier: cs.diversifier, timestamp: cs.timestamp, root: CommitmentRoot::from_bytes(&pk.to_bytes()) },
            allow_update_after_proposal: raw.allow_update_after_proposal,
        })
    }
}

impl From<ClientState> for RawSmClientState {
    fn from(value: ClientState) -> Self {
        Self {
            sequence: value.sequence,
            frozen_sequence: value.frozen_sequence,
            consensus_state: Some(RawSmConsesusState{
                public_key: Some(value.consensus_state.public_key.into()),
                diversifier: value.consensus_state.diversifier,
                timestamp: value.consensus_state.timestamp,
            }),
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
            value: Protobuf::<RawSmClientState>::encode_vec(&client_state)
                .expect("encoding to `Any` from `SmClientState`"),
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicKey(pub tendermint::PublicKey);

impl Default for PublicKey {
    fn default() -> Self {
        PublicKey(tendermint::PublicKey::Ed25519(ed25519_dalek::PublicKey::from_bytes(&[0u8; PUBLIC_KEY_LENGTH]).unwrap()))
    }
}

impl PublicKey {
    /// Protobuf [`Any`] type URL for Ed25519 public keys
    pub const ED25519_TYPE_URL: &'static str = "/cosmos.crypto.ed25519.PubKey";

    /// Protobuf [`Any`] type URL for secp256k1 public keys
    pub const SECP256K1_TYPE_URL: &'static str = "/cosmos.crypto.secp256k1.PubKey";

    /// Get the type URL for this [`PublicKey`].
    pub fn type_url(&self) -> &'static str {
        match &self.0 {
            tendermint::PublicKey::Ed25519(_) => Self::ED25519_TYPE_URL,
            tendermint::PublicKey::Secp256k1(_) => Self::SECP256K1_TYPE_URL,
            // `tendermint::PublicKey` is `non_exhaustive`
            _ => unreachable!("unknown pubic key type"),
        }
    }

    /// Convert this [`PublicKey`] to a Protobuf [`Any`] type.
    pub fn to_any(&self) -> Result<Any> {
        let value = match self.0 {
            tendermint::PublicKey::Ed25519(_) => cosmos_sdk_proto::cosmos::crypto::secp256k1::PubKey {
                key: self.to_bytes(),
            }
            .to_bytes()?,
            tendermint::PublicKey::Secp256k1(_) => cosmos_sdk_proto::cosmos::crypto::secp256k1::PubKey {
                key: self.to_bytes(),
            }
            .to_bytes()?,
            _ => return Err(Error::solomachine().into()),
        };

        Ok(Any {
            type_url: self.type_url().to_owned(),
            value,
        })
    }

    /// Serialize this [`PublicKey`] as a byte vector.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }
}

impl TryFrom<Any> for PublicKey {
    type Error = eyre::Report;

    fn try_from(any: Any) -> Result<PublicKey> {
        PublicKey::try_from(&any)
    }
}

impl TryFrom<&Any> for PublicKey {
    type Error = eyre::Report;

    fn try_from(any: &Any) -> Result<PublicKey> {
        match any.type_url.as_str() {
            Self::ED25519_TYPE_URL => {
                cosmos_sdk_proto::cosmos::crypto::ed25519::PubKey::decode(&*any.value)?.try_into()
            }
            Self::SECP256K1_TYPE_URL => {
                cosmos_sdk_proto::cosmos::crypto::secp256k1::PubKey::decode(&*any.value)?.try_into()
            }
            other => Err(Error::solomachine().into()),
        }
    }
}

impl TryFrom<cosmos_sdk_proto::cosmos::crypto::ed25519::PubKey> for PublicKey {
    type Error = eyre::Report;

    fn try_from(public_key: cosmos_sdk_proto::cosmos::crypto::ed25519::PubKey) -> Result<PublicKey> {
        tendermint::public_key::PublicKey::from_raw_ed25519(&public_key.key)
            .map(Into::into)
            .ok_or_else(|| Error::solomachine().into())
    }
}

impl TryFrom<cosmos_sdk_proto::cosmos::crypto::secp256k1::PubKey> for PublicKey {
    type Error = eyre::Report;

    fn try_from(public_key: cosmos_sdk_proto::cosmos::crypto::secp256k1::PubKey) -> Result<PublicKey> {
        tendermint::public_key::PublicKey::from_raw_secp256k1(&public_key.key)
            .map(Into::into)
            .ok_or_else(|| Error::solomachine().into())
    }
}

impl From<PublicKey> for Any {
    fn from(public_key: PublicKey) -> Any {
        // This is largely a workaround for `tendermint::PublicKey` being
        // marked `non_exhaustive`.
        public_key.to_any().expect("unsupported algorithm")
    }
}

impl From<tendermint::PublicKey> for PublicKey {
    fn from(pk: tendermint::PublicKey) -> PublicKey {
        PublicKey(pk)
    }
}

impl From<PublicKey> for tendermint::PublicKey {
    fn from(pk: PublicKey) -> tendermint::PublicKey {
        pk.0
    }
}


#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusState {
    pub public_key: PublicKey,
    pub diversifier: String,
    pub timestamp: u64,
    pub root: CommitmentRoot,
}

impl Default for ConsensusState {
    fn default() -> Self {
        Self {
            public_key: Default::default(),
            diversifier: "".to_string(),
            timestamp: 1,
            root: Default::default()
        }
    }
}

impl ConsensusState {
    pub fn new(public_key: PublicKey, diversifier: String, timestamp: u64) -> Self {

        Self {
            public_key,
            diversifier,
            timestamp,
            root: CommitmentRoot::from_bytes(&public_key.to_bytes()),
        }
    }
}

impl crate::core::ics02_client::consensus_state::ConsensusState for ConsensusState {
    fn client_type(&self) -> ClientType {
        ClientType::Solomachine
    }

    fn root(&self) -> &CommitmentRoot {
        &self.root
    }

    fn timestamp(&self) -> Timestamp {
        Timestamp::from_nanoseconds(self.timestamp).unwrap()
    }
}

impl Protobuf<RawConsensusState> for ConsensusState {}

impl TryFrom<RawConsensusState> for ConsensusState {
    type Error = Error;

    fn try_from(raw: RawConsensusState) -> Result<Self, Self::Error> {
        let pk = raw.public_key.unwrap().try_into().unwrap();
        Ok(Self {
            public_key: pk,
            diversifier: raw.diversifier,
            timestamp: raw.timestamp,
            root: CommitmentRoot::from_bytes(&pk.to_bytes())
        })
    }
}

impl From<ConsensusState> for RawConsensusState {
    fn from(value: ConsensusState) -> Self {

        RawConsensusState {
            public_key: Some(value.public_key.into()),
            diversifier: value.diversifier,
            timestamp: value.timestamp,
        }
    }
}

impl Protobuf<Any> for ConsensusState {}

impl TryFrom<Any> for ConsensusState {
    type Error = Ics02Error;

    fn try_from(raw: Any) -> Result<Self, Self::Error> {
        use bytes::Buf;
        use core::ops::Deref;
        use prost::Message;

        fn decode_consensus_state<B: Buf>(buf: B) -> Result<ConsensusState, Error> {
            RawConsensusState::decode(buf)
                .map_err(Error::decode)?
                .try_into()
        }

        match raw.type_url.as_str() {
            SOLOMACHINE_CONSENSUS_STATE_TYPE_URL => {
                decode_consensus_state(raw.value.deref()).map_err(Into::into)
            }
            _ => Err(Ics02Error::unknown_consensus_state_type(raw.type_url)),
        }
    }
}

impl From<ConsensusState> for Any {
    fn from(consensus_state: ConsensusState) -> Self {
        Any {
            type_url: SOLOMACHINE_CONSENSUS_STATE_TYPE_URL.to_string(),
            value: Protobuf::<RawConsensusState>::encode_vec(&consensus_state)
                .expect("encoding to `Any` from `SmConsensusState`"),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Header {
    pub sequence: u64,
    pub timestamp: u64,
    pub signature: Vec<u8>,
    pub new_public_key: Option<PublicKey>,
    pub new_diversifier: String,
}

// impl Default for Header {
//     fn default() -> Self {
//         Self {
//             sequence: 1,
//             timestamp: 1,
//             signature: vec![1,2,3],
//             new_public_key: Some(PublicKey::default()),
//             new_diversifier: "".to_string()
//         }
//     }
// }

impl crate::core::ics02_client::header::Header for Header {
    fn client_type(&self) -> ClientType {
        ClientType::Solomachine
    }

    fn height(&self) -> Height {
        Height::new(0, self.sequence).unwrap()
    }

    fn timestamp(&self) -> Timestamp {
        Timestamp::from_nanoseconds(self.timestamp).unwrap()
    }
}

impl core::fmt::Debug for Header {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        write!(f, " Header {{...}}")
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
            new_public_key: Some(pk),
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
            value: Protobuf::<RawHeader>::encode_vec(&header)
                .expect("encoding to `Any` from `SmHeader`"),
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
            new_public_key: value.new_public_key.map(|public_key| public_key.to_any().unwrap()),
            new_diversifier: value.new_diversifier,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SignBytes {
    pub sequence: u64,
    pub timestamp: u64,
    pub diversifier: String,
    /// type of the data used
    pub data_type: i32,
    /// marshaled data
    pub data: Vec<u8>,
}

impl Protobuf<RawSignBytes> for SignBytes {}

impl TryFrom<RawSignBytes> for SignBytes {
    type Error = Error;

    fn try_from(raw: RawSignBytes) -> Result<Self, Self::Error> {
        Ok(Self {
            sequence: raw.sequence,
            timestamp: raw.timestamp,
            diversifier: raw.diversifier,
            data_type: raw.data_type,
            data: raw.data,
        })
    }
}

impl From<SignBytes> for RawSignBytes {
    fn from(value: SignBytes) -> Self {
        RawSignBytes {
            sequence: value.sequence,
            timestamp: value.timestamp,
            diversifier: value.diversifier,
            data_type: value.data_type,
            data: value.data,
        }
    }
}


/// HeaderData returns the SignBytes data for update verification.
#[derive(Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct HeaderData {
    /// header public key
    pub new_pub_key: Option<PublicKey>,
    /// header diversifier
    pub new_diversifier: String,
}

impl Protobuf<RawHeaderData> for HeaderData {}

impl TryFrom<RawHeaderData> for HeaderData {
    type Error = Error;

    fn try_from(raw: RawHeaderData) -> Result<Self, Self::Error> {
        let pk: PublicKey = raw.new_pub_key.unwrap().try_into().unwrap();

        Ok(Self {
            new_pub_key: Some(pk),
            new_diversifier: raw.new_diversifier,
        })
    }
}

impl From<HeaderData> for RawHeaderData {
    fn from(value: HeaderData) -> Self {
        RawHeaderData {
            new_pub_key: Some(value.new_pub_key.unwrap().to_any().unwrap()),
            new_diversifier: value.new_diversifier,
        }
    }
}
