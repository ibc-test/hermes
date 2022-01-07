use alloc::vec;
use alloc::vec::Vec;
use core::convert::Infallible;
use core::convert::{TryFrom, TryInto};

use serde::Serialize;

// use tendermint mock as grandpa
use ibc_proto::ibc::lightclients::grandpa::v1::ConsensusState as RawConsensusState;

use super::help::Commitment;
use crate::ics02_client::client_consensus::AnyConsensusState;
use crate::ics02_client::client_type::ClientType;
use crate::ics10_grandpa::error::Error;
use crate::ics10_grandpa::header::Header;
use crate::ics23_commitment::commitment::CommitmentRoot;
use tendermint_proto::Protobuf;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConsensusState {
    pub root: CommitmentRoot,
}

impl ConsensusState {
    pub fn new(root: CommitmentRoot) -> Self {
        Self { root }
    }

    pub fn from_commit(root_commit: Commitment) -> Self {
        let encode_root_commit = serde_json::to_string(&root_commit)
            .unwrap()
            .as_bytes()
            .to_vec();

        Self {
            root: CommitmentRoot::from_bytes(&encode_root_commit),
        }
    }
}

impl Protobuf<RawConsensusState> for ConsensusState {}

impl crate::ics02_client::client_consensus::ConsensusState for ConsensusState {
    type Error = Infallible;

    fn client_type(&self) -> ClientType {
        ClientType::Grandpa
    }

    fn root(&self) -> &CommitmentRoot {
        &self.root
    }

    fn validate_basic(&self) -> Result<(), Self::Error> {
        unimplemented!()
    }

    fn wrap_any(self) -> AnyConsensusState {
        AnyConsensusState::Grandpa(self)
    }
}

impl TryFrom<RawConsensusState> for ConsensusState {
    type Error = Error;

    fn try_from(raw: RawConsensusState) -> Result<Self, Self::Error> {
        Ok(ConsensusState {
            root: raw
                .root
                .ok_or_else(|| {
                    Error::invalid_raw_consensus_state("missing commitment root".into())
                })?
                .hash
                .into(),
        })
    }
}

impl From<ConsensusState> for RawConsensusState {
    fn from(value: ConsensusState) -> Self {
        Self {
            root: Some(ibc_proto::ibc::core::commitment::v1::MerkleRoot {
                hash: value.root.into_vec(),
            }),
        }
    }
}


impl From<Header> for ConsensusState {
    fn from(header: Header) -> Self {
        let commitment_root = header.signed_commitment.commitment.unwrap();

        let encode_commitment_root = serde_json::to_string(&commitment_root)
            .unwrap()
            .as_bytes()
            .to_vec();

        Self {
            root: CommitmentRoot::from_bytes(&encode_commitment_root),
        }
    }
}
