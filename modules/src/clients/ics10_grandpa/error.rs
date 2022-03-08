use alloc::string::String;

use crate::core::ics24_host::error::ValidationError;
use flex_error::{define_error, DisplayOnly, TraceError};

define_error! {
     #[derive(Debug, PartialEq, Eq)]
    Error{
        Dummy
            |_| { format_args!("dummy error") },

        Decode
            [ TraceError<prost::DecodeError> ]
            | _ | { "decode error" },

        MissingLatestHeight
            | _ | { "missing latest height" },

        MissingHeight
            | _ | { "missing height" },

        InvalidChainIdentifier
            [ ValidationError ]
            | _ | { "invalid chain identifier" },

        MissingFrozenHeight
            | _ | { "missing frozen height" },

        InvalidRawConsensusState
            { reason: String }
            | _ | { "invalid raw client consensus state" },

        InvalidRawMisbehaviour
            { reason: String }
            | _ | { "invalid raw misbehaviour" },

        Encode
            [ TraceError<prost::EncodeError> ]
            | _ | { "encode error" },

        EmptyCommitment
            | _ | { "empty commitment"},

        InvalidSignedCommitment
            | _ | { "invalid Signed Commitment" },

        InvalidValidatorMerkleProof
            | _ | { "invalid Validator Merkle Proof" },

        InvalidMmrLeaf
            | _ | { "invalid Mmr Leaf" },

        InvalidMmrLeafProof
            | _ | { "invalid Mmr Lead Proof" },

        InvalidCommitment
            | _ | { "Invalid commitment"},

        InvalidStorageProof
            | _ | { "invalid storage Proof" },

        GetStorageByProofErr
            {
                e: String,
            }
            | e | {
                format_args!("failed to get storage by proof: {0}",e)
            },
    }
}