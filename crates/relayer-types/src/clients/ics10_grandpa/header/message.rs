// Nested message and enum types in `Header`.
use crate::clients::ics10_grandpa::error::Error;
use crate::prelude::*;
use alloc::collections::BTreeMap;
use ibc_proto::ibc::lightclients::grandpa::v1::header::Message as RawMessage;
use ibc_proto::ibc::lightclients::grandpa::v1::{
    ParachainHeader as RawParachainHeader, ParachainHeaderMap as RawParachainHeaderMap,
    StateProof as RawStateProof, SubchainHeader as RawSubchainHeader,
    SubchainHeaderMap as RawSubchainHeaderMap,
};
use ibc_proto::protobuf::Protobuf;
use serde::{Deserialize, Serialize};

/// only one header
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// solochain headers and their proofs
    SubchainHeaderMap(SubchainHeaderMap),
    /// parachain headers and their proofs
    ParachainHeaderMap(ParachainHeaderMap),
}

// todo fix ibc-proto-rs RawMessage
// impl Protobuf<RawMessage> for Message {}

// impl TryFrom<RawMessage> for Message {
//     type Error = Error;

//     fn try_from(raw: RawMessage) -> Result<Self, Self::Error> {
//         todo!()
//     }
// }

// impl From<Message> for RawMessage {
//     fn from(value: Message) -> Self {
//         todo!()
//     }
// }

/// substrate chain header map
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubchainHeaderMap {
    /// LatestMMR latest_mmr = 1;
    /// map<blocknumber,scale-encoded blockheader>
    ///
    /// map<uint32,Timestamp> timestamp_map=2;
    pub subchain_header_map: BTreeMap<u32, SubchainHeader>,
}

impl Protobuf<RawSubchainHeaderMap> for SubchainHeaderMap {}

impl TryFrom<RawSubchainHeaderMap> for SubchainHeaderMap {
    type Error = Error;

    fn try_from(raw: RawSubchainHeaderMap) -> Result<Self, Self::Error> {
        let subchain_header_map_result: Result<BTreeMap<u32, SubchainHeader>, Self::Error> = raw
            .subchain_header_map
            .into_iter()
            .map(|(k, v)| SubchainHeader::try_from(v).map(|header| (k, header)))
            .collect();

        Ok(Self {
            subchain_header_map: subchain_header_map_result?,
        })
    }
}

impl From<SubchainHeaderMap> for RawSubchainHeaderMap {
    fn from(value: SubchainHeaderMap) -> Self {
        Self {
            subchain_header_map: value
                .subchain_header_map
                .into_iter()
                .map(|(k, v)| (k, RawSubchainHeader::from(v)))
                .collect::<BTreeMap<u32, RawSubchainHeader>>(),
        }
    }
}
/// solochain header
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubchainHeader {
    /// scale-encoded solochain header bytes
    pub block_header: Vec<u8>,
    /// timestamp and proof
    pub timestamp: Option<StateProof>,
}

impl Protobuf<RawSubchainHeader> for SubchainHeader {}

impl TryFrom<RawSubchainHeader> for SubchainHeader {
    type Error = Error;

    fn try_from(raw: RawSubchainHeader) -> Result<Self, Self::Error> {
        Ok(Self {
            block_header: raw.block_header,
            timestamp: raw
                .timestamp
                .map(TryInto::try_into)
                .map_or(Ok(None), |r| r.map(Some))?,
        })
    }
}

impl From<SubchainHeader> for RawSubchainHeader {
    fn from(value: SubchainHeader) -> Self {
        Self {
            block_header: value.block_header,
            timestamp: value.timestamp.map(Into::into),
        }
    }
}

/// / Parachain headers and their merkle proofs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParachainHeaderMap {
    /// map<blocknumber,ParachainHeader>
    ///
    ///   map<uint32,Timestamp> timestamp_map=2;
    pub parachain_header_map: BTreeMap<u32, ParachainHeader>,
}

impl Protobuf<RawParachainHeaderMap> for ParachainHeaderMap {}

impl TryFrom<RawParachainHeaderMap> for ParachainHeaderMap {
    type Error = Error;

    fn try_from(raw: RawParachainHeaderMap) -> Result<Self, Self::Error> {
        let parachain_header_map_result: Result<BTreeMap<u32, ParachainHeader>, Self::Error> = raw
            .parachain_header_map
            .into_iter()
            .map(|(k, v)| ParachainHeader::try_from(v).map(|header| (k, header)))
            .collect();

        Ok(Self {
            parachain_header_map: parachain_header_map_result?,
        })
    }
}

impl From<ParachainHeaderMap> for RawParachainHeaderMap {
    fn from(value: ParachainHeaderMap) -> Self {
        Self {
            parachain_header_map: value
                .parachain_header_map
                .into_iter()
                .map(|(k, v)| (k, RawParachainHeader::from(v)))
                .collect::<BTreeMap<u32, RawParachainHeader>>(),
        }
    }
}
/// data needed to prove parachain header inclusion in mmr
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParachainHeader {
    /// para id
    pub parachain_id: u32,
    /// scale-encoded parachain header bytes
    pub block_header: Vec<u8>,
    /// proofs for parachain header in the mmr_leaf.parachain_heads
    pub proofs: Vec<Vec<u8>>,
    /// merkle leaf index for parachain heads proof
    pub header_index: u32,
    /// total number of para heads in parachain_heads_root
    pub header_count: u32,
    /// timestamp and proof
    pub timestamp: Option<StateProof>,
}

impl Protobuf<RawParachainHeader> for ParachainHeader {}

impl TryFrom<RawParachainHeader> for ParachainHeader {
    type Error = Error;

    fn try_from(raw: RawParachainHeader) -> Result<Self, Self::Error> {
        Ok(Self {
            parachain_id: raw.parachain_id,
            block_header: raw.block_header,
            proofs: raw.proofs,
            header_index: raw.header_index,
            header_count: raw.header_count,
            timestamp: raw
                .timestamp
                .map(TryInto::try_into)
                .map_or(Ok(None), |r| r.map(Some))?,
        })
    }
}

impl From<ParachainHeader> for RawParachainHeader {
    fn from(value: ParachainHeader) -> Self {
        Self {
            parachain_id: value.parachain_id,
            block_header: value.block_header,
            proofs: value.proofs,
            header_index: value.header_index,
            header_count: value.header_count,
            timestamp: value.timestamp.map(Into::into),
        }
    }
}

/// state value and proof
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateProof {
    /// state key
    pub key: Vec<u8>,
    /// the state value
    pub value: Vec<u8>,
    /// these proof gets from parachain by rpc methord:state_getReadProof
    pub proofs: Vec<Vec<u8>>,
}

impl Protobuf<RawStateProof> for StateProof {}

impl TryFrom<RawStateProof> for StateProof {
    type Error = Error;

    fn try_from(raw: RawStateProof) -> Result<Self, Self::Error> {
        Ok(Self {
            key: raw.key,
            value: raw.value,
            proofs: raw.proofs,
        })
    }
}

impl From<StateProof> for RawStateProof {
    fn from(value: StateProof) -> Self {
        Self {
            key: value.key,
            value: value.value,
            proofs: value.proofs,
        }
    }
}
