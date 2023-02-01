use alloc::string::ToString;
use alloc::vec::Vec;
use std::vec;
use ibc_proto::google::protobuf::Any;
use ibc_proto::protobuf::Protobuf;
use crate::clients::ics12_near::header::Header;
use crate::clients::ics12_near::near_types::{AccountId, Balance};
use crate::clients::ics12_near::near_types::signature::PublicKey;
use crate::core::ics02_client::client_type::ClientType;
use crate::core::ics23_commitment::commitment::CommitmentRoot;
use crate::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use crate::core::ics02_client::error::Error;

pub const NEAR_CONSENSUS_STATE_TYPE_URL: &str = "/ibc.lightclients.near.v1.ConsensusState";

/// The consensus state of NEAR light client.
#[derive(Default, Clone,Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusState {
    /// Block producers of current epoch
    pub current_bps: Vec<ValidatorStakeView>,
    /// Header data
    pub header: Header,

    /// todo
    pub commitment_root: CommitmentRoot
}

#[derive(Clone,Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorStakeViewV1 {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    pub stake: Balance,
}

#[derive(Clone,Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidatorStakeView {
    V1(ValidatorStakeViewV1),
}

impl crate::core::ics02_client::consensus_state::ConsensusState for ConsensusState {
    fn client_type(&self) -> ClientType {
        ClientType::Near
    }

    fn root(&self) -> &CommitmentRoot {
        &self.commitment_root
    }

    fn timestamp(&self) -> Timestamp {
        Timestamp::from_nanoseconds(
            self
                .header
                .light_client_block_view
                .inner_lite
                .timestamp_nanosec
        ).unwrap()
    }
}


impl Protobuf<Any> for ConsensusState {}

impl TryFrom<Any> for ConsensusState {
    type Error = Error;

    fn try_from(raw: Any) -> Result<Self, Self::Error> {
        use bytes::Buf;
        use core::ops::Deref;
        use prost::Message;

        fn decode_consensus_state<B: Buf>(buf: B) -> Result<ConsensusState, Error> {
            Ok(ConsensusState::default())
        }

        match raw.type_url.as_str() {
            NEAR_CONSENSUS_STATE_TYPE_URL => {
                decode_consensus_state(raw.value.deref()).map_err(Into::into)
            }
            _ => Err(Error::unknown_consensus_state_type(raw.type_url)),
        }
    }
}

impl From<ConsensusState> for Any {
    fn from(consensus_state: ConsensusState) -> Self {
        Any {
            type_url: NEAR_CONSENSUS_STATE_TYPE_URL.to_string(),
            value: vec![],
        }
    }
}



