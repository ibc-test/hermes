use alloc::string::String;
use alloc::vec::Vec;
use alloc::vec;
use core::convert::TryInto;
use codec::{Encode, Decode};

use ibc_proto::ibc::core::commitment::v1::MerkleProof;

use crate::ics02_client::client_consensus::AnyConsensusState;
use crate::ics02_client::client_def::ClientDef;
use crate::ics02_client::client_state::AnyClientState;
use crate::ics02_client::error::Error;
use crate::ics03_connection::connection::ConnectionEnd;
use crate::ics04_channel::channel::ChannelEnd;
use crate::ics04_channel::packet::Sequence;
use crate::ics10_grandpa::client_state::ClientState;
use crate::ics10_grandpa::consensus_state::ConsensusState;
use crate::ics10_grandpa::header::Header;
use crate::ics23_commitment::commitment::{CommitmentPrefix, CommitmentProofBytes, CommitmentRoot};
use crate::ics24_host::identifier::ConnectionId;
use crate::ics24_host::identifier::{ChannelId, ClientId, PortId};
use crate::Height;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrandpaClient;

impl ClientDef for GrandpaClient {
    type Header = Header;
    type ClientState = ClientState;
    type ConsensusState = ConsensusState;

    fn check_header_and_update_state(
        &self,
        client_state: Self::ClientState,
        header: Self::Header,
    ) -> Result<(Self::ClientState, Self::ConsensusState), Error> {
        // if client_state.latest_height() >= header.height() {
        //     return Err(Error::low_header_height(
        //         header.height(),
        //         client_state.latest_height(),
        //     ));
        // }

        // tracing::info!("in ics10 client_def [check_header_and_update_state] >> header = {:?}", header);
        // // destruct header
        // let Header {
        //     block_header,
        //     mmr_leaf,
        //     mmr_leaf_proof,
        // } = header;
        //
        //
        // if client_state.latest_commitment.is_none() {
        //     let new_client_state = ClientState {
        //         chain_id: client_state.chain_id,
        //         block_number: signed_commitment.clone().commitment.unwrap().block_number,
        //         frozen_height: client_state.frozen_height,
        //         latest_commitment: signed_commitment.clone().commitment,
        //         validator_set: mmr_leaf.beefy_next_authority_set
        //     };
        //
        //     let new_consensus_state = ConsensusState::from_commit(signed_commitment.commitment.unwrap());
        //
        //     return Ok((new_client_state, new_consensus_state));
        // }
        //
        // let mut beefy_light_client = beefy_light_client::LightClient {
        //     latest_commitment: Some(client_state.latest_commitment.unwrap().into()),
        //     validator_set: client_state.validator_set.unwrap().into(),
        //     in_process_state: None
        // };
        //
        // let encode_signed_commitment = signed_commitment.encode();
        // let validator_proofs = vec![validator_merkle_proof.into()];
        // let encode_mmr_leaf = mmr_leaf.encode();
        // let encode_mmr_leaf_proof = mmr_leaf_proof.encode();
        //
        // beefy_light_client.update_state(&encode_signed_commitment, &validator_proofs, &encode_mmr_leaf,&encode_mmr_leaf_proof);
        //
        // tracing::info!("in ics10 client_def [check_header_and_update_state] >> beefy_light_client = {:?}", beefy_light_client);
        //
        //
        // let new_client_state = ClientState {
        //     chain_id: client_state.chain_id,
        //     // TODO Need later to fix
        //     // block_number: beefy_light_client.latest_commitment.as_ref().unwrap().block_number,
        //     block_number: signed_commitment.commitment.as_ref().unwrap().block_number,
        //     frozen_height: client_state.frozen_height,
        //     latest_commitment: Some(beefy_light_client.latest_commitment.clone().unwrap().into()),
        //     validator_set: Some(beefy_light_client.validator_set.into()),
        // };
        //
        // let new_consensus_state = ConsensusState::from_commit(beefy_light_client.latest_commitment.unwrap().into());
        //
        // tracing::info!("in ics10 client_def [check_header_and_update_state] >> client_state = {:?}", new_client_state);
        // tracing::info!("in ics10 client_def [check_header_and_update_state] >> new_consensus_state = {:?}", new_consensus_state);

        // Ok((new_client_state ,new_consensus_state))
        Ok((client_state.with_header(header.clone()), ConsensusState::from(header)))
    }

    fn verify_client_consensus_state(
        &self,
        _client_state: &Self::ClientState,
        _height: Height,
        _prefix: &CommitmentPrefix,
        _proof: &CommitmentProofBytes,
        _client_id: &ClientId,
        _consensus_height: Height,
        _expected_consensus_state: &AnyConsensusState,
    ) -> Result<(), Error> {
        Self::extract_verify_beefy_proof(_client_state, _height, _proof)
    }

    fn verify_connection_state(
        &self,
        _client_state: &Self::ClientState,
        _height: Height,
        _prefix: &CommitmentPrefix,
        _proof: &CommitmentProofBytes,
        _connection_id: Option<&ConnectionId>,
        _expected_connection_end: &ConnectionEnd,
    ) -> Result<(), Error> {
        // Self::extract_verify_beefy_proof(_client_state, _height, _proof)
        use core::time::Duration;
        use sp_core::{storage::StorageKey, Bytes};
        use  alloc::vec;
        // use subxt::sp_core::H256;

/*        while _client_state.block_number < (_height.revision_height as u32) {
            let sleep_duration = Duration::from_micros(500);
            // wasm_timer::sleep(sleep_duration);
        }*/

        use serde::{Deserialize, Serialize};
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct ReadProof_ {
            pub at: String,
            pub proof: Vec<Bytes>,
        }

        use ibc_proto::ibc::core::commitment::v1::MerkleProof as RawMerkleProof;
        use core::convert::TryFrom;
        use ibc_proto::ics23::commitment_proof::Proof::Exist;
        use beefy_merkle_tree::Keccak256;
        use codec::Decode;

        // The latest height was increased here: https://github.com/octopus-network/ibc-rs/blob/b98094a57620d0b3d9f8d2caced09abfc14ab00f/relayer/src/chain.rs?_pjax=%23js-repo-pjax-container%2C%20div%5Bitemtype%3D%22http%3A%2F%2Fschema.org%2FSoftwareSourceCode%22%5D%20main%2C%20%5Bdata-pjax-container%5D#L438
        // Call decrement() to restore the latest height
        let _height = _height.decrement();

        let merkel_proof = RawMerkleProof::try_from(_proof.clone()).unwrap();
        let _merkel_proof = merkel_proof.proofs[0].proof.clone().unwrap();
        let leaf_proof = match _merkel_proof {
            Exist(_exist_proof) => {
                let _proof_str = String::from_utf8(_exist_proof.value).unwrap();
                // tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> _proof_str: {:?}", _proof_str);
                let leaf_proof: ReadProof_ = serde_json::from_str(&_proof_str).unwrap();
                tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> leaf_proof: {:?}", leaf_proof);
                leaf_proof
            }
            _ => unimplemented!()
        };

        let storage_key = (vec![_connection_id.unwrap().as_bytes().to_vec()]).iter();

/*        tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> _client_state: {:?}", _client_state);
        let mmr_root: [u8; 32] = _client_state.
            latest_commitment.as_ref().unwrap().payload.as_slice().try_into().map_err(|_| Error::cant_decode_mmr_root())?;
        tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> mmr_root: {:?}", mmr_root);

        let mmr_leaf: Vec<u8> =
            Decode::decode(&mut &leaf_proof.leaf[..]).map_err(|_| Error::cant_decode_mmr_leaf())?;
        tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> mmr_leaf: {:?}", mmr_leaf);
        let mmr_leaf_hash = Keccak256::hash(&mmr_leaf[..]);
        tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> mmr_leaf_hash: {:?}", mmr_leaf_hash);

        let mmr_leaf_proof = leaf_proof.proof;
        let mmr_proof = beefy_light_client::mmr::MmrLeafProof::decode(&mut &mmr_leaf_proof[..])
            .map_err(|_| Error::cant_decode_mmr_proof())?;
        tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> mmr_proof: {:?}", mmr_proof);

        let result = beefy_light_client::mmr::verify_leaf_proof(mmr_root, mmr_leaf_hash, mmr_proof).unwrap();
        if !result {
            return Err(Error::failed_to_verify_mmr_proof());
        }*/

        Ok(())
    }

    fn verify_channel_state(
        &self,
        _client_state: &Self::ClientState,
        _height: Height,
        _prefix: &CommitmentPrefix,
        _proof: &CommitmentProofBytes,
        _port_id: &PortId,
        _channel_id: &ChannelId,
        _expected_channel_end: &ChannelEnd,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn verify_client_full_state(
        &self,
        _client_state: &Self::ClientState,
        _height: Height,
        _root: &CommitmentRoot,
        _prefix: &CommitmentPrefix,
        _client_id: &ClientId,
        _proof: &CommitmentProofBytes,
        _expected_client_state: &AnyClientState,
    ) -> Result<(), Error> {
        Self::extract_verify_beefy_proof(_client_state, _height, _proof)
    }

    fn verify_packet_data(
        &self,
        _client_state: &Self::ClientState,
        _height: Height,
        _proof: &CommitmentProofBytes,
        _port_id: &PortId,
        _channel_id: &ChannelId,
        _seq: &Sequence,
        _data: String,
    ) -> Result<(), Error> {
        Ok(()) // Todo:
    }

    fn verify_packet_acknowledgement(
        &self,
        _client_state: &Self::ClientState,
        _height: Height,
        _proof: &CommitmentProofBytes,
        _port_id: &PortId,
        _channel_id: &ChannelId,
        _seq: &Sequence,
        _data: Vec<u8>,
    ) -> Result<(), Error> {
        Ok(()) // todo!()
    }

    fn verify_next_sequence_recv(
        &self,
        _client_state: &Self::ClientState,
        _height: Height,
        _proof: &CommitmentProofBytes,
        _port_id: &PortId,
        _channel_id: &ChannelId,
        _seq: &Sequence,
    ) -> Result<(), Error> {
        Ok(()) // todo!()
    }

    fn verify_packet_receipt_absence(
        &self,
        _client_state: &Self::ClientState,
        _height: Height,
        _proof: &CommitmentProofBytes,
        _port_id: &PortId,
        _channel_id: &ChannelId,
        _seq: &Sequence,
    ) -> Result<(), Error> {
        Ok(()) // todo:
    }

    fn verify_upgrade_and_update_state(
        &self,
        client_state: &Self::ClientState,
        consensus_state: &Self::ConsensusState,
        _proof_upgrade_client: MerkleProof,
        _proof_upgrade_consensus_state: MerkleProof,
    ) -> Result<(Self::ClientState, Self::ConsensusState), Error> {
        // TODO
        Ok((client_state.clone(), consensus_state.clone()))
    }
}

impl GrandpaClient {
    /// Extract `LeafProof_` and verify its validity
    fn extract_verify_beefy_proof(_client_state: &ClientState, _height: Height, _proof: &CommitmentProofBytes) -> Result<(), Error> {
        use core::time::Duration;
        use sp_core::{storage::StorageKey, Bytes};
        // use subxt::sp_core::H256;

        while _client_state.block_number < (_height.revision_height as u32) {
            let sleep_duration = Duration::from_micros(500);
            // wasm_timer::sleep(sleep_duration);
        }

/*        use serde::{Deserialize, Serialize};
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct ReadProof_ {
            pub at: String,
            pub proof: Vec<Bytes>,
        }*/

        use ibc_proto::ibc::core::commitment::v1::MerkleProof as RawMerkleProof;
        use core::convert::TryFrom;
        use ibc_proto::ics23::commitment_proof::Proof::Exist;
        use beefy_merkle_tree::Keccak256;
        use codec::Decode;

        use serde::{Deserialize, Serialize};
        #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
        #[serde(rename_all = "camelCase")]
        pub struct LeafProof_ {
            pub block_hash: String,
            pub leaf: Vec<u8>,
            pub proof: Vec<u8>,
        }

        // The latest height was increased here: https://github.com/octopus-network/ibc-rs/blob/b98094a57620d0b3d9f8d2caced09abfc14ab00f/relayer/src/chain.rs?_pjax=%23js-repo-pjax-container%2C%20div%5Bitemtype%3D%22http%3A%2F%2Fschema.org%2FSoftwareSourceCode%22%5D%20main%2C%20%5Bdata-pjax-container%5D#L438
        // Call decrement() to restore the latest height
        let _height = _height.decrement();
        let merkel_proof = RawMerkleProof::try_from(_proof.clone()).unwrap();
        let _merkel_proof = merkel_proof.proofs[0].proof.clone().unwrap();
        let leaf_proof = match _merkel_proof {
            Exist(_exist_proof) => {
                let _proof_str = String::from_utf8(_exist_proof.value).unwrap();
                // tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> _proof_str: {:?}", _proof_str);
                let leaf_proof: LeafProof_ = serde_json::from_str(&_proof_str).unwrap();
                tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> leaf_proof: {:?}", leaf_proof);
                leaf_proof
            }
            _ => unimplemented!()
        };

        tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> _client_state: {:?}", _client_state);
        let mmr_root: [u8; 32] = _client_state.
            latest_commitment.payload.as_slice().try_into().map_err(|_| Error::cant_decode_mmr_root())?;
        tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> mmr_root: {:?}", mmr_root);

        let mmr_leaf: Vec<u8> =
            Decode::decode(&mut &leaf_proof.leaf[..]).map_err(|_| Error::cant_decode_mmr_leaf())?;
        tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> mmr_leaf: {:?}", mmr_leaf);
        let mmr_leaf_hash = Keccak256::hash(&mmr_leaf[..]);
        tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> mmr_leaf_hash: {:?}", mmr_leaf_hash);

        let mmr_leaf_proof = leaf_proof.proof;
        let mmr_proof = beefy_light_client::mmr::MmrLeafProof::decode(&mut &mmr_leaf_proof[..])
            .map_err(|_| Error::cant_decode_mmr_proof())?;
        tracing::info!("In ics10-client_def.rs: [verify_connection_state] >> mmr_proof: {:?}", mmr_proof);

        let result = beefy_light_client::mmr::verify_leaf_proof(mmr_root, mmr_leaf_hash, mmr_proof).unwrap();
        if !result {
            return Err(Error::failed_to_verify_mmr_proof());
        }

        Ok(())
    }
}
