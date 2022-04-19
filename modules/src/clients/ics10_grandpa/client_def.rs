use alloc::format;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use codec::{Decode, Encode};
use core::convert::From;
use core::convert::TryInto;
use tendermint_proto::Protobuf;

use ibc_proto::ibc::core::commitment::v1::MerkleProof;

use crate::clients::ics10_grandpa::client_state::ClientState;
use crate::clients::ics10_grandpa::consensus_state::ConsensusState as GpConsensusState;
use crate::clients::ics10_grandpa::header::Header;
use crate::core::ics02_client::client_consensus::AnyConsensusState;
use crate::core::ics02_client::client_def::ClientDef;
use crate::core::ics02_client::client_state::AnyClientState;
use crate::core::ics02_client::context::ClientReader;
use crate::core::ics02_client::error::Error;
use crate::core::ics03_connection::connection::ConnectionEnd;
use crate::core::ics04_channel::channel::ChannelEnd;
use crate::core::ics04_channel::context::ChannelReader;
use crate::core::ics04_channel::msgs::acknowledgement::Acknowledgement;
use crate::core::ics04_channel::packet::Sequence;
use crate::core::ics23_commitment::commitment::{
    CommitmentPrefix, CommitmentProofBytes, CommitmentRoot,
};
use crate::core::ics24_host::identifier::ConnectionId;
use crate::core::ics24_host::identifier::{ChannelId, ClientId, PortId};
use crate::Height;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrandpaClient;

impl ClientDef for GrandpaClient {
    type Header = Header;
    type ClientState = ClientState;
    type ConsensusState = GpConsensusState;

    /// Verify if the block header is valid via MMR root
    /// * ctx: interface to read on-chain storage
    /// * client_id: not used
    /// * client_state: the current client's state containing MMR root
    /// * header: the counterpart's block header to be verified
    fn check_header_and_update_state(
        &self,
        ctx: &dyn ClientReader,
        client_id: ClientId,
        client_state: Self::ClientState,
        header: Self::Header,
    ) -> Result<(Self::ClientState, Self::ConsensusState), Error> {
        tracing::trace!(
            "in client_def: [check_header_and_update_state] >> client_state = {:?}, header = {:?}",
            client_state,
            header
        );

        if header.block_header.block_number > client_state.latest_commitment.block_number {
            return Err(Error::invalid_mmr_root_height(
                client_state.latest_commitment.block_number,
                header.block_header.block_number,
            ));
        }

        if client_state.latest_commitment.payload.is_empty() {
            return Err(Error::empty_mmr_root());
        }

        let mut mmr_root = [0u8; 32];

        // Fetch the desired mmr root from storage if it's different from the mmr root in client_state
        if header.mmr_leaf_proof.leaf_count != client_state.latest_commitment.block_number as u64 {
            tracing::trace!(
                    "in client_def: [check_header_and_update_state] >> header.mmr_leaf_proof.leaf_count = {:?}, client_state.latest_commitment.block_number = {:?}",
                    header.mmr_leaf_proof.leaf_count, client_state.latest_commitment.block_number
                );
            let height = Height::new(0, header.mmr_leaf_proof.leaf_count);
            // todo unwrap()
            let any_consensus_state = ctx.consensus_state(&client_id, height).unwrap();
            let consensus_state = match any_consensus_state {
                AnyConsensusState::Grandpa(_v) => _v,
                _ => unimplemented!(),
            };
            tracing::trace!(
                "in client_def: [check_header_and_update_state] >> consensus_state queried = {:?}",
                consensus_state
            );
            mmr_root.copy_from_slice(&consensus_state.digest);
        } else {
            mmr_root.copy_from_slice(&client_state.latest_commitment.payload);
        }

        let mmr_proof = header.clone().mmr_leaf_proof;
        let mmr_proof = beefy_light_client::mmr::MmrLeafProof::from(mmr_proof);

        let mmr_leaf_encode =
            beefy_light_client::mmr::MmrLeaf::from(header.clone().mmr_leaf).encode();
        let mmr_leaf_hash = beefy_merkle_tree::Keccak256::hash(&mmr_leaf_encode[..]);
        let mmr_leaf = beefy_light_client::mmr::MmrLeaf::from(header.clone().mmr_leaf);

        let header_hash = header.hash();

        if mmr_leaf.parent_number_and_hash.1.is_empty() {
            return Err(Error::empty_mmr_leaf_parent_hash_mmr_root());
        }
        tracing::trace!(
            "ics1 client_def :[check_header_and_update_state] >> header_hash = {:?}",
            header_hash
        );
        tracing::trace!(
            "ics1 client_def :[check_header_and_update_state] >> parent_mmr_root = {:?}",
            mmr_leaf.parent_number_and_hash.1
        );

        // todo 
        /*            if header_hash != mmr_leaf.parent_number_and_hash.1 {
            return Err(Error::header_hash_not_match());
        }*/
        // Todo: Is this comparism needed?

        tracing::trace!(
            "in client_def: [check_header_and_update_state] >> mmr_root = {:?}",
            mmr_root.clone()
        );
        tracing::trace!(
            "in client_def: [check_header_and_update_state] >> mmr_leaf_hash = {:?}",
            mmr_leaf_hash.clone()
        );
        tracing::trace!(
            "in client_def: [check_header_and_update_state] >> mmr_proof = {:?}",
            mmr_proof.clone()
        );
        let result = beefy_light_client::mmr::verify_leaf_proof(mmr_root, mmr_leaf_hash, mmr_proof)
            .map_err(|_| Error::invalid_mmr_leaf_proof())?;

        if !result {
            return Err(Error::invalid_mmr_leaf_proof());
        }

        let client_state = ClientState {
            block_header: header.clone().block_header,
            block_number: header.clone().block_header.block_number,
            ..client_state
        };

        tracing::trace!(
            "in client_def: [check_header_and_update_state] >> client_state = {:?}",
            client_state
        );

        tracing::trace!(
            "in client_def: [check_header_and_update_state] >> consensus_state = {:?}",
            GpConsensusState::from(header.clone())
        );

        // grandpa consensus_state update from substrate-ibc
        Ok((client_state, GpConsensusState::from(header.clone())))
    }

    /// TODO
    fn verify_upgrade_and_update_state(
        &self,
        client_state: &Self::ClientState,
        consensus_state: &Self::ConsensusState,
        proof_upgrade_client: MerkleProof,
        proof_upgrade_consensus_state: MerkleProof,
    ) -> Result<(Self::ClientState, Self::ConsensusState), Error> {
        Ok((client_state.clone(), consensus_state.clone()))
    }

    /// Verification functions as specified in:
    /// <https://github.com/cosmos/ibc/tree/master/spec/ics-002-client-semantics>
    ///
    /// Verify a `proof` that the consensus state of a given client (at height `consensus_height`)
    /// matches the input `consensus_state`. The parameter `counterparty_height` represent the
    /// height of the counterparty chain that this proof assumes (i.e., the height at which this
    /// proof was computed).
    #[allow(clippy::too_many_arguments)]
    fn verify_client_consensus_state(
        &self,
        client_state: &Self::ClientState,
        height: Height,
        prefix: &CommitmentPrefix,
        proof: &CommitmentProofBytes,
        root: &CommitmentRoot,
        client_id: &ClientId,
        consensus_height: Height,
        expected_consensus_state: &AnyConsensusState,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Verify a `proof` that a connection state reconstructed from storage proof, storage key and state root matches
    /// the `expected_connection_end` of the counterparty chain.
    #[allow(clippy::too_many_arguments)]
    fn verify_connection_state(
        &self,
        client_state: &Self::ClientState,
        height: Height,
        prefix: &CommitmentPrefix,
        proof: &CommitmentProofBytes,
        root: &CommitmentRoot,
        connection_id: &ConnectionId,
        expected_connection_end: &ConnectionEnd,
    ) -> Result<(), Error> {
        let keys: Vec<Vec<u8>> = vec![connection_id.as_bytes().to_vec()];
        // todo unwrap
        let storage_result =
            Self::get_storage_via_proof(client_state, height, proof, keys, "Connections").unwrap();
        // todo unwrap
        let connection_end = ConnectionEnd::decode_vec(&storage_result).unwrap();
        tracing::trace!(
            "In ics10-client_def.rs: [verify_connection_state] >> connection_end: {:?}",
            connection_end
        );

        // todo unwrap
        if !(connection_end.encode_vec().unwrap() == expected_connection_end.encode_vec().unwrap())
        {
            return Err(Error::invalid_connection_state());
        }
        Ok(())
    }

    /// Verify a `proof` that a channel state reconstructed from storage proof, storage key and state root matches that of
    /// the `expected_channel_end` of the counterparty chain.
    #[allow(clippy::too_many_arguments)]
    fn verify_channel_state(
        &self,
        client_state: &Self::ClientState,
        height: Height,
        prefix: &CommitmentPrefix,
        proof: &CommitmentProofBytes,
        root: &CommitmentRoot,
        port_id: &PortId,
        channel_id: &ChannelId,
        expected_channel_end: &ChannelEnd,
    ) -> Result<(), Error> {
        let keys: Vec<Vec<u8>> = vec![port_id.as_bytes().to_vec(), channel_id.as_bytes().to_vec()];
        // todo unwrap
        let storage_result =
            Self::get_storage_via_proof(client_state, height, proof, keys, "Channels").unwrap();
        // todo unwrap
        let channel_end = ChannelEnd::decode_vec(&storage_result).unwrap();
        tracing::trace!(
            "In ics10-client_def.rs: [verify_connection_state] >> channel_end: {:?}",
            channel_end
        );

        // todo unwrap()
        if !(channel_end.encode_vec().unwrap() == expected_channel_end.encode_vec().unwrap()) {
            return Err(Error::invalid_connection_state());
        }
        Ok(())
    }

    /// Verify a `proof` that a client state reconstructed from storage proof, storage key and state root matches that of
    /// the `expected_client_state` of the counterparty chain.
    #[allow(clippy::too_many_arguments)]
    fn verify_client_full_state(
        &self,
        client_state: &Self::ClientState,
        height: Height,
        prefix: &CommitmentPrefix,
        proof: &CommitmentProofBytes,
        root: &CommitmentRoot,
        client_id: &ClientId,
        expected_client_state: &AnyClientState,
    ) -> Result<(), Error> {
        let keys: Vec<Vec<u8>> = vec![client_id.as_bytes().to_vec()];
        // todo unwrap
        let storage_result =
            Self::get_storage_via_proof(client_state, height, proof, keys, "ClientStates").unwrap();
        // todo unwrap
        let any_client_state = AnyClientState::decode_vec(&storage_result).unwrap();
        tracing::trace!(
            "In ics10-client_def.rs: [verify_client_full_state] >> decoded client_state: {:?}",
            any_client_state
        );
        tracing::trace!(
            "In ics10-client_def.rs: [verify_client_full_state] >>  _expected_client_state: {:?}",
            expected_client_state
        );

        // todo unwrap
        if !(any_client_state.encode_vec().unwrap() == expected_client_state.encode_vec().unwrap())
        {
            return Err(Error::invalid_client_state());
        }
        Ok(())
    }

    /// Verify a `proof` that a packet reconstructed from storage proof, storage key and state root matches that of
    /// the packet stored the counterparty chain.
    #[allow(clippy::too_many_arguments)]
    fn verify_packet_data(
        &self,
        ctx: &dyn ChannelReader,
        client_state: &Self::ClientState,
        height: Height,
        connection_end: &ConnectionEnd,
        proof: &CommitmentProofBytes,
        root: &CommitmentRoot,
        port_id: &PortId,
        channel_id: &ChannelId,
        sequence: Sequence,
        commitment: String,
    ) -> Result<(), Error> {
        let keys: Vec<Vec<u8>> = vec![
            port_id.as_bytes().to_vec(),
            channel_id.as_bytes().to_vec(),
            u64::from(sequence).encode(),
        ];
        //  todo unwrap
        let storage_result =
            Self::get_storage_via_proof(client_state, height, proof, keys, "PacketCommitment")
                .unwrap();

        // todo unwrap()
        tracing::trace!(
            "In ics10-client_def.rs: [verify_packet_data] >> decoded packet_commitment: {:?}",
            String::from_utf8(storage_result.clone()).unwrap()
        );
        tracing::trace!(
            "In ics10-client_def.rs: [verify_packet_data] >>  expected packet_commitment: {:?}",
            commitment.clone()
        );

        if !(storage_result == commitment.encode()) {
            return Err(Error::invalid_packet_commitment(sequence));
        }
        Ok(())
    }

    /// Verify a `proof` that a packet reconstructed from storage proof, storage key and state root matches that of
    /// the packet stored the counterparty chain.
    #[allow(clippy::too_many_arguments)]
    fn verify_packet_acknowledgement(
        &self,
        ctx: &dyn ChannelReader,
        client_state: &Self::ClientState,
        height: Height,
        connection_end: &ConnectionEnd,
        proof: &CommitmentProofBytes,
        root: &CommitmentRoot,
        port_id: &PortId,
        channel_id: &ChannelId,
        sequence: Sequence,
        ack: Acknowledgement,
    ) -> Result<(), Error> {
        let keys: Vec<Vec<u8>> = vec![
            port_id.as_bytes().to_vec(),
            channel_id.as_bytes().to_vec(),
            u64::from(sequence).encode(),
        ];
        // todo unwrap
        let storage_result =
            Self::get_storage_via_proof(client_state, height, proof, keys, "Acknowledgements")
                .unwrap();
        tracing::trace!(
            "In ics10-client_def.rs: [verify_packet_acknowledgement] >> encoded ack: {:?}",
            storage_result
        );
        tracing::trace!(
            "In ics10-client_def.rs: [verify_packet_acknowledgement] >>  raw ack: {:?}",
            ack
        );

        let ack = format!("{:?}", ack.into_bytes());
        if !(storage_result == Self::hash(ack).encode()) {
            return Err(Error::invalid_packet_ack(sequence));
        }
        Ok(())
    }

    /// Verify a `proof` that of the next_seq_received.
    #[allow(clippy::too_many_arguments)]
    fn verify_next_sequence_recv(
        &self,
        ctx: &dyn ChannelReader,
        client_state: &Self::ClientState,
        height: Height,
        connection_end: &ConnectionEnd,
        proof: &CommitmentProofBytes,
        root: &CommitmentRoot,
        port_id: &PortId,
        channel_id: &ChannelId,
        sequence: Sequence,
    ) -> Result<(), Error> {
        let keys: Vec<Vec<u8>> = vec![port_id.as_bytes().to_vec(), channel_id.as_bytes().to_vec()];
        // todo unwrap
        let storage_result =
            Self::get_storage_via_proof(client_state, height, proof, keys, "NextSequenceRecv")
                .unwrap();
        tracing::trace!(
            "In ics10-client_def: [verify_next_sequence_recv] >> storage_result: {:?}",
            storage_result
        );

        // todo unwrap
        let sequence_restored: u64 = u64::decode(&mut &storage_result[..]).unwrap();
        if sequence_restored > u64::from(sequence) {
            return Err(Error::invalid_next_sequence_recv(
                sequence_restored,
                u64::from(sequence),
            ));
        }

        Ok(())
    }

    /// Verify a `proof` that a packet has not been received.
    #[allow(clippy::too_many_arguments)]
    fn verify_packet_receipt_absence(
        &self,
        ctx: &dyn ChannelReader,
        client_state: &Self::ClientState,
        height: Height,
        connection_end: &ConnectionEnd,
        proof: &CommitmentProofBytes,
        root: &CommitmentRoot,
        port_id: &PortId,
        channel_id: &ChannelId,
        sequence: Sequence,
    ) -> Result<(), Error> {
        Ok(())
    }
}

impl GrandpaClient {
    /// Reconstruct on-chain storage value by proof, key(path), and state root
    fn get_storage_via_proof(
        _client_state: &ClientState,
        _height: Height,
        _proof: &CommitmentProofBytes,
        _keys: Vec<Vec<u8>>,
        _storage_name: &str,
    ) -> Result<Vec<u8>, Error> {
        tracing::trace!("In ics10-client_def.rs: [extract_verify_beefy_proof] >> _client_state: {:?}, _height: {:?}, _keys: {:?}, _storage_name: {:?}",
            _client_state, _height, _keys, _storage_name);

        use crate::clients::ics10_grandpa::state_machine::read_proof_check;
        use core::convert::TryFrom;
        use ibc_proto::ibc::core::commitment::v1::MerkleProof as RawMerkleProof;
        use ibc_proto::ics23::commitment_proof::Proof::Exist;
        use sp_runtime::traits::BlakeTwo256;
        use sp_trie::StorageProof;

        use serde::{Deserialize, Serialize};
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct ReadProofU8 {
            pub at: String,
            pub proof: Vec<Vec<u8>>,
        }

        // todo unwrap()
        let merkel_proof = RawMerkleProof::try_from(_proof.clone()).unwrap();
        // todo unwrap
        let _merkel_proof = merkel_proof.proofs[0].proof.clone().unwrap(); // TODO have error
        let storage_proof = match _merkel_proof {
            Exist(_exist_proof) => {
                // todo unwrap
                let _proof_str = String::from_utf8(_exist_proof.value).unwrap();
                tracing::trace!(
                    "In ics10-client_def.rs: [extract_verify_beefy_proof] >> _proof_str: {:?}",
                    _proof_str
                );
                // todo unwrap
                let _storage_proof: ReadProofU8 = serde_json::from_str(&_proof_str).unwrap();
                _storage_proof
            }
            _ => unimplemented!(),
        };

        let _storage_keys = Self::storage_map_final_key(_keys, _storage_name)?;
        tracing::trace!(
            "In ics10-client_def.rs: [extract_verify_beefy_proof] >> storage_keys: {:?}",
            _storage_keys
        );
        let state_root = _client_state.clone().block_header.state_root;
        let state_root = vector_to_array::<u8, 32>(state_root);
        tracing::trace!(
            "In ics10-client_def.rs: [extract_verify_beefy_proof] >> state_root: {:?}",
            state_root
        );

        // todo unwrap
        let storage_result = read_proof_check::<BlakeTwo256>(
            sp_core::H256::from(state_root),
            StorageProof::new(storage_proof.proof),
            &_storage_keys,
        )
        .unwrap()
        .unwrap();
        tracing::trace!(
            "In ics10-client_def.rs: [extract_verify_beefy_proof] >> {:?}-storage_result: {:?}",
            _storage_name,
            storage_result
        );

        // todo unwrap
        let storage_result = <Vec<u8>>::decode(&mut &storage_result[..]).unwrap();
        tracing::trace!("In ics10-client_def.rs: [extract_verify_beefy_proof] >> storage_result truncated: {:?}", storage_result);

        Ok(storage_result)
    }

    /// Calculate the storage's final key
    fn storage_map_final_key(_keys: Vec<Vec<u8>>, _storage_name: &str) -> Result<Vec<u8>, Error> {
        use frame_support::storage::storage_prefix;
        use frame_support::storage::types::EncodeLikeTuple;
        use frame_support::storage::types::KeyGenerator;
        use frame_support::storage::types::TupleToEncodedIter;
        use frame_support::storage::Key;
        use frame_support::{Blake2_128Concat, StorageHasher};

        // Todo: To justify different types of keys by an enum like below, instead of _keys.len()
        /*
            enum StorageMapKeys<KArg> where KArg: EncodeLikeTuple<Vec<u8>> + TupleToEncodedIter
            {
                HashMapKey([Vec<u8>; 1]),
                DoubleHashMapKey([Vec<u8>; 2]),
                NHashMapKey(KArg)
            }
        */

        // Migrate from: https://github.com/paritytech/substrate/blob/32b71896df8a832e7c139a842e46710e4d3f70cd/frame/support/src/storage/generator/map.rs?_pjax=%23js-repo-pjax-container%2C%20div%5Bitemtype%3D%22http%3A%2F%2Fschema.org%2FSoftwareSourceCode%22%5D%20main%2C%20%5Bdata-pjax-container%5D#L66
        if _keys.len() == 1 {
            let key_hashed: &[u8] = &Blake2_128Concat::hash(&_keys[0].encode());
            let storage_prefix = storage_prefix("Ibc".as_bytes(), _storage_name.as_bytes());
            let mut final_key =
                Vec::with_capacity(storage_prefix.len() + key_hashed.as_ref().len());
            final_key.extend_from_slice(&storage_prefix);
            final_key.extend_from_slice(key_hashed.as_ref());
            return Ok(final_key);
        }

        // Migrate from: https://github.com/paritytech/substrate/blob/32b71896df8a832e7c139a842e46710e4d3f70cd/frame/support/src/storage/generator/double_map.rs#L92
        if _keys.len() == 2 {
            let key1_hashed: &[u8] = &Blake2_128Concat::hash(&_keys[0].encode());
            let key2_hashed: &[u8] = &Blake2_128Concat::hash(&_keys[1].encode());
            let storage_prefix = storage_prefix("Ibc".as_bytes(), _storage_name.as_bytes());
            let mut final_key = Vec::with_capacity(
                storage_prefix.len() + key1_hashed.as_ref().len() + key2_hashed.as_ref().len(),
            );
            final_key.extend_from_slice(&storage_prefix);
            final_key.extend_from_slice(key1_hashed.as_ref());
            final_key.extend_from_slice(key2_hashed.as_ref());
            return Ok(final_key);
        }

        // Todo: expand the capability of the code to handle key length of more than 3
        // Migrate from: https://github.com/paritytech/substrate/blob/32b71896df8a832e7c139a842e46710e4d3f70cd/frame/support/src/storage/generator/nmap.rs#L100
        if _keys.len() == 3 {
            let result_keys = (_keys[0].clone(), _keys[1].clone(), _keys[2].clone());
            let storage_prefix = storage_prefix("Ibc".as_bytes(), _storage_name.as_bytes());
            let key_hashed = <(
                Key<Blake2_128Concat, Vec<u8>>,
                Key<Blake2_128Concat, Vec<u8>>,
                Key<Blake2_128Concat, Vec<u8>>,
            ) as KeyGenerator>::final_key(result_keys);

            let mut final_key = Vec::with_capacity(storage_prefix.len() + key_hashed.len());
            final_key.extend_from_slice(&storage_prefix);
            final_key.extend_from_slice(key_hashed.as_ref());

            return Ok(final_key);
        }

        // todo unwrap
        return Err(Error::wrong_key_number(_keys.len().try_into().unwrap()));
    }

    /// A hashing function for packet commitments
    fn hash(value: String) -> String {
        tracing::trace!("in client_def: [hash] >> value = {:?}", value.clone());

        let r = sp_io::hashing::sha2_256(value.as_bytes());

        let mut tmp = String::new();
        for item in r.iter() {
            tmp.push_str(&format!("{:02x}", item));
        }
        tracing::trace!("in client_def: [hash] >> result = {:?}", tmp.clone());
        tmp
    }
}

fn vector_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into()
        .unwrap_or_else(|v: Vec<T>| panic!("Expected a Vec of length {} but it was {}", N, v.len()))
}

#[cfg(test)]
mod tests {
    use super::GrandpaClient;
    use crate::clients::ics10_grandpa::client_state::ClientState;
    use crate::clients::ics10_grandpa::header::Header;
    use crate::clients::ics10_grandpa::help::{
        BlockHeader, Commitment, MmrLeaf, MmrLeafProof, ValidatorSet,
    };
    use crate::core::ics02_client::client_consensus::AnyConsensusState;
    use crate::core::ics02_client::client_def::ClientDef;
    use crate::core::ics02_client::client_state::AnyClientState;
    use crate::core::ics02_client::client_type::ClientType;
    use crate::core::ics02_client::context::ClientReader;
    use crate::core::ics02_client::error::Error;
    use crate::core::ics02_client::height::Height;
    use crate::core::ics24_host::identifier::ChainId;
    use crate::core::ics24_host::identifier::ClientId;
    use alloc::boxed::Box;
    use beefy_merkle_tree::Keccak256;
    use codec::{Decode, Encode};
    use hex_literal::hex;
    use octopusxt::ibc_node;
    use octopusxt::subscribe_beefy;
    use prost::Message;
    use std::format;
    use std::string::ToString;
    use std::vec::Vec;
    use std::{println, vec};
    use subxt::sp_core::hexdisplay::HexDisplay;
    use subxt::BlockNumber;
    use subxt::ClientBuilder;

    struct GrandpaClientReader;

    impl ClientReader for GrandpaClientReader {
        fn client_type(&self, client_id: &ClientId) -> Result<ClientType, Error> {
            unimplemented!()
        }

        fn client_state(&self, client_id: &ClientId) -> Result<AnyClientState, Error> {
            unimplemented!()
        }

        fn consensus_state(
            &self,
            client_id: &ClientId,
            height: Height,
        ) -> Result<AnyConsensusState, Error> {
            unimplemented!()
        }

        fn next_consensus_state(
            &self,
            client_id: &ClientId,
            height: Height,
        ) -> Result<Option<AnyConsensusState>, Error> {
            unimplemented!()
        }

        fn prev_consensus_state(
            &self,
            client_id: &ClientId,
            height: Height,
        ) -> Result<Option<AnyConsensusState>, Error> {
            unimplemented!()
        }

        fn host_height(&self) -> Height {
            unimplemented!()
        }

        fn host_consensus_state(&self, height: Height) -> Result<AnyConsensusState, Error> {
            unimplemented!()
        }

        fn pending_host_consensus_state(&self) -> Result<AnyConsensusState, Error> {
            unimplemented!()
        }

        fn client_counter(&self) -> Result<u64, Error> {
            unimplemented!()
        }
    }

    #[test]
    fn test_check_header_and_update_state_by_test_case_from_beefy_light_client() {
        let client = GrandpaClient;

        let commitment = beefy_light_client::commitment::Commitment {
            payload: hex!("7fe1460305e05d0937df34aa47a251811b0f83032fd153a64ebb8812cb252ee2"),
            block_number: 89,
            validator_set_id: 0,
        };

        let ics10_commitment = Commitment::from(commitment);

        let mut ics10_client_state = ClientState::default();
        ics10_client_state.latest_commitment = ics10_commitment;

        let encoded_header = vec![
            10, 13, 22, 200, 67, 234, 70, 53, 53, 35, 181, 174, 39, 195, 107, 232, 128, 49, 144, 0,
            46, 49, 133, 110, 254, 85, 186, 83, 203, 199, 197, 6, 69, 1, 144, 163, 197, 173, 189,
            82, 34, 223, 212, 9, 231, 160, 19, 228, 191, 132, 66, 233, 82, 181, 164, 11, 244, 139,
            67, 151, 196, 198, 210, 20, 105, 63, 105, 3, 166, 96, 244, 224, 235, 128, 247, 251,
            169, 168, 144, 60, 51, 9, 243, 15, 221, 196, 212, 16, 234, 164, 29, 199, 205, 36, 112,
            165, 9, 62, 20, 6, 66, 65, 66, 69, 52, 2, 0, 0, 0, 0, 159, 96, 136, 32, 0, 0, 0, 0, 4,
            66, 69, 69, 70, 132, 3, 4, 27, 102, 51, 199, 84, 23, 10, 207, 202, 104, 184, 2, 235,
            159, 61, 6, 10, 40, 223, 155, 198, 15, 56, 24, 158, 249, 244, 126, 70, 119, 186, 4, 66,
            65, 66, 69, 169, 3, 1, 20, 212, 53, 147, 199, 21, 253, 211, 28, 97, 20, 26, 189, 4,
            169, 159, 214, 130, 44, 133, 88, 133, 76, 205, 227, 154, 86, 132, 231, 165, 109, 162,
            125, 1, 0, 0, 0, 0, 0, 0, 0, 142, 175, 4, 21, 22, 135, 115, 99, 38, 201, 254, 161, 126,
            37, 252, 82, 135, 97, 54, 147, 201, 18, 144, 156, 178, 38, 170, 71, 148, 242, 106, 72,
            1, 0, 0, 0, 0, 0, 0, 0, 144, 181, 171, 32, 92, 105, 116, 201, 234, 132, 27, 230, 136,
            134, 70, 51, 220, 156, 168, 163, 87, 132, 62, 234, 207, 35, 20, 100, 153, 101, 254, 34,
            1, 0, 0, 0, 0, 0, 0, 0, 48, 103, 33, 33, 29, 84, 4, 189, 157, 168, 142, 2, 4, 54, 10,
            26, 154, 184, 184, 124, 102, 193, 188, 47, 205, 211, 127, 60, 34, 34, 204, 32, 1, 0, 0,
            0, 0, 0, 0, 0, 230, 89, 167, 161, 98, 140, 221, 147, 254, 188, 4, 164, 224, 100, 110,
            162, 14, 159, 95, 12, 224, 151, 217, 160, 82, 144, 212, 169, 224, 84, 223, 78, 1, 0, 0,
            0, 0, 0, 0, 0, 37, 247, 211, 55, 231, 96, 163, 185, 188, 26, 127, 33, 131, 57, 43, 42,
            10, 32, 114, 255, 223, 190, 21, 179, 20, 120, 184, 196, 24, 104, 65, 222, 0, 128, 99,
            229, 155, 248, 17, 21, 89, 124, 79, 189, 134, 73, 152, 214, 16, 205, 166, 187, 227, 44,
            110, 19, 25, 72, 143, 62, 170, 60, 59, 165, 150, 110, 5, 66, 65, 66, 69, 1, 1, 176, 82,
            55, 247, 244, 160, 12, 115, 166, 169, 63, 233, 237, 9, 141, 45, 194, 186, 67, 39, 32,
            222, 11, 20, 122, 50, 3, 97, 121, 104, 223, 9, 80, 154, 189, 211, 112, 187, 167, 113,
            224, 8, 134, 78, 168, 215, 202, 1, 228, 214, 23, 143, 125, 11, 211, 149, 154, 171, 25,
            134, 44, 183, 166, 137,
        ];

        let header: beefy_light_client::header::Header =
            Decode::decode(&mut &encoded_header[..]).unwrap();
        let header_1: beefy_light_client::header::Header =
            Decode::decode(&mut &encoded_header[..]).unwrap();
        println!("beefy_light_client header = {:?}", header);
        println!("beefy_light_client header hash = {:?}", header.hash());

        let ics10_header = crate::clients::ics10_grandpa::help::BlockHeader::from(header_1);
        println!("grandpa_header  = {:?}", ics10_header);
        println!("ics10_header hash = {:?}", ics10_header.hash());

        // Query mmr leaf with leaf index 81 (NOTE: not 81-1) at block 89
        // {
        //     blockHash: 0xd0d7c0b309926a2c64ed82f9a8ab8e2b037feb48fb3b783989bba30b041b1315
        //     leaf: 0xc5010051000000f728a8e3b29fb62b3234be2ba31e6beffd00bb571a978962ff9c26ea8dcc20ab010000000000000005000000304803fa5a91d9852caafe04b4b867a4ed27a07a5bee3d1507b4b187a68777a20000000000000000000000000000000000000000000000000000000000000000
        //     proof: 0x5100000000000000590000000000000018bddfdcc0399d0ce1be41f1126f63053ecb26ee19c107c0f96013f216b7b21933f8611a08a46cd74fd96d54d2eb19898dbd743b019bf7ba32b17b9a193f0e65b8c231bab606963f6a5a05071bea9af2a30f22adc43224affe87b3f90d1a07d0db4b6a7c61c56d1174067b6e816970631b8727f6dfe3ebd3923581472d45f47ad3940e1f16782fd635f4789d7f5674d2cbf12d1bbd7823c6ee37c807ad34424d48f0e3888f05a1d6183d9dbf8a91d3400ea2047b5e19d498968011e63b91058fbd
        // }

        let  encoded_mmr_leaf = hex!("c5010051000000f728a8e3b29fb62b3234be2ba31e6beffd00bb571a978962ff9c26ea8dcc20ab010000000000000005000000304803fa5a91d9852caafe04b4b867a4ed27a07a5bee3d1507b4b187a68777a20000000000000000000000000000000000000000000000000000000000000000");

        let leaf: Vec<u8> = Decode::decode(&mut &encoded_mmr_leaf[..]).unwrap();
        let mmr_leaf: beefy_light_client::mmr::MmrLeaf = Decode::decode(&mut &*leaf).unwrap();
        println!("mmr_leaf: {:?}", mmr_leaf);
        println!(
            "mmr_leaf parent_number_and_hash : {:?}",
            mmr_leaf.parent_number_and_hash.1
        );
        assert_eq!(
            header.hash(),
            mmr_leaf.parent_number_and_hash.1,
            "beefy_light_client header hash not equal mmr_leaf parent_hash"
        );

        let encoded_mmr_proof =  hex!("5100000000000000590000000000000018bddfdcc0399d0ce1be41f1126f63053ecb26ee19c107c0f96013f216b7b21933f8611a08a46cd74fd96d54d2eb19898dbd743b019bf7ba32b17b9a193f0e65b8c231bab606963f6a5a05071bea9af2a30f22adc43224affe87b3f90d1a07d0db4b6a7c61c56d1174067b6e816970631b8727f6dfe3ebd3923581472d45f47ad3940e1f16782fd635f4789d7f5674d2cbf12d1bbd7823c6ee37c807ad34424d48f0e3888f05a1d6183d9dbf8a91d3400ea2047b5e19d498968011e63b91058fbd");
        let mmr_proof =
            beefy_light_client::mmr::MmrLeafProof::decode(&mut &encoded_mmr_proof[..]).unwrap();
        // println!("mmr_proof: {:?}", mmr_proof);

        let ics10_header = Header {
            block_header: ics10_header,
            mmr_leaf: MmrLeaf::from(mmr_leaf),
            mmr_leaf_proof: MmrLeafProof::from(mmr_proof),
        };
        //
        println!(">> ics10_header = {:?}", ics10_header);

        // let result = client
        //     .check_header_and_update_state(ics10_client_state, ics10_header)
        //     .unwrap();
        let grandpa_client = GrandpaClientReader;
        let result = client
            .check_header_and_update_state(
                &grandpa_client,
                ClientId::default(),
                ics10_client_state,
                ics10_header,
            )
            .unwrap();

        // println!(" >> client_state = {:?} ", result.0);
        // println!(" >> consensus_state = {:?}", result.1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_check_header_and_update_state_by_test_case_from_substrate() {
        let client = ClientBuilder::new()
            .set_url("ws://localhost:9944")
            .build::<ibc_node::DefaultConfig>()
            .await
            .unwrap();

        // subscribe beefy justification
        let signed_commitment_raw = subscribe_beefy(client.clone()).await.unwrap().0;

        let signed_commitment = beefy_light_client::commitment::SignedCommitment::decode(
            &mut &signed_commitment_raw.clone()[..],
        )
        .unwrap();

        println!("signed_commitment = {:?}", signed_commitment);

        let beefy_light_client::commitment::Commitment {
            payload,
            block_number,
            validator_set_id,
        } = signed_commitment.commitment;

        // let commitment = beefy_light_client::commitment::Commitment {
        //     payload: hex!("7fe1460305e05d0937df34aa47a251811b0f83032fd153a64ebb8812cb252ee2"),
        //     block_number: 89,
        //     validator_set_id: 0,
        // };

        let grandpa_client = GrandpaClient;
        let commitment = signed_commitment.commitment.clone();
        let temp_block_number = block_number - 10;
        println!("commitment = {:?}", commitment);
        println!("block_number = {:?}", temp_block_number);

        let ics10_commitment = Commitment::from(commitment);

        let mut ics10_client_state = ClientState::default();
        ics10_client_state.latest_commitment = ics10_commitment;

        let client = ClientBuilder::new()
            .set_url("ws://localhost:9944")
            .build::<ibc_node::DefaultConfig>()
            .await
            .unwrap();

        // get block header
        let ics10_header = octopusxt::call_ibc::get_header_by_block_number(
            client.clone(),
            Some(BlockNumber::from(temp_block_number - 1)),
        )
        .await
        .unwrap();
        println!("block_header = {:?}", ics10_header);
        println!("block_header hash = {:?}", ics10_header.hash());

        let api = client
            .clone()
            .to_runtime_api::<ibc_node::RuntimeApi<ibc_node::DefaultConfig>>();

        // block hash by block number
        let block_hash: Option<sp_core::H256> = api
            .client
            .rpc()
            .block_hash(Some(BlockNumber::from(block_number)))
            .await
            .unwrap();

        // get mmr_leaf and mmr_leaf_proof
        let mmr_leaf_and_mmr_leaf_proof = octopusxt::call_ibc::get_mmr_leaf_and_mmr_proof(
            (temp_block_number - 1) as u64,
            block_hash,
            client,
        )
        .await
        .unwrap();
        println!(
            "mmr_leaf_and_mmr_leaf_proof = {:?}",
            mmr_leaf_and_mmr_leaf_proof
        );

        let encoded_mmr_leaf = mmr_leaf_and_mmr_leaf_proof.1;
        let encode_mmr_leaf_proof = mmr_leaf_and_mmr_leaf_proof.2;

        let leaf: Vec<u8> = Decode::decode(&mut &encoded_mmr_leaf[..]).unwrap();
        let mmr_leaf: beefy_light_client::mmr::MmrLeaf = Decode::decode(&mut &*leaf).unwrap();
        println!("mmr_leaf = {:?}", mmr_leaf);

        let mmr_leaf_proof =
            beefy_light_client::mmr::MmrLeafProof::decode(&mut &encode_mmr_leaf_proof[..]).unwrap();
        println!("mmr_leaf_proof = {:?}", mmr_leaf_proof);

        let ics10_header = Header {
            block_header: BlockHeader {
                parent_hash: ics10_header.parent_hash,
                block_number: ics10_header.block_number,
                state_root: ics10_header.state_root,
                extrinsics_root: ics10_header.extrinsics_root,
                digest: ics10_header.digest,
            },
            mmr_leaf: MmrLeaf::from(mmr_leaf),
            mmr_leaf_proof: MmrLeafProof::from(mmr_leaf_proof),
        };

        println!(">> ics10_header = {:?}", ics10_header);

        // let result = grandpa_client
        //     .check_header_and_update_state(ics10_client_state, ics10_header)
        //     .unwrap();
        let grandpa_client_1 = GrandpaClientReader;

        let result = grandpa_client
            .check_header_and_update_state(
                &grandpa_client_1,
                ClientId::default(),
                ics10_client_state,
                ics10_header,
            )
            .unwrap();
        // println!(" >> client_state = {:?} ", result.0);
        // println!(" >> consensus_state = {:?}", result.1);
    }

    #[tokio::test]
    #[ignore]
    async fn verify_leaf_proof_works_2() -> Result<(), Box<dyn std::error::Error>> {
        let client = ClientBuilder::new()
            .set_url("ws://localhost:9944")
            .build::<ibc_node::DefaultConfig>()
            .await?;

        // subscribe beefy justification
        let signed_commitment_raw = subscribe_beefy(client.clone()).await.unwrap().0 .0;
        println!(
            "signed_commitment = {:?}",
            HexDisplay::from(&signed_commitment_raw)
        );
        // decode signed_commitment
        let signed_commitment = beefy_light_client::commitment::SignedCommitment::decode(
            &mut &signed_commitment_raw.clone()[..],
        )
        .unwrap();
        println!("signed_commitment = {:?}", signed_commitment);

        let beefy_light_client::commitment::Commitment {
            payload,
            block_number,
            validator_set_id,
        } = signed_commitment.commitment;

        // get mmr root
        let mmr_root = payload;
        println!(
            "root_hash(signed commitment payload) : {:?}
signed commitment block_number : {}
signed commitment validator_set_id : {}",
            format!("{}", HexDisplay::from(&mmr_root)),
            block_number,
            validator_set_id
        );

        let api = client
            .clone()
            .to_runtime_api::<ibc_node::RuntimeApi<ibc_node::DefaultConfig>>();

        //get block hash by block_number
        let block_hash: sp_core::H256 = api
            .client
            .rpc()
            .block_hash(Some(BlockNumber::from(block_number)))
            .await?
            .unwrap();
        println!(
            "block number : {} -> block hash : {:?}",
            block_number, block_hash
        );

        //get mmr leaf and proof
        // Note: target height=block_number - 1
        let target_height = (block_number - 1) as u64;
        let (block_hash, mmr_leaf, mmr_leaf_proof) =
            octopusxt::call_ibc::get_mmr_leaf_and_mmr_proof(
                target_height,
                Some(block_hash),
                client.clone(),
            )
            .await?;
        println!("generate_proof block hash : {:?}", block_hash);

        // mmr leaf proof
        println!(
            "generated the mmr leaf proof = {:?}",
            format!("{}", HexDisplay::from(&mmr_leaf_proof))
        );
        let decode_mmr_proof =
            beefy_light_client::mmr::MmrLeafProof::decode(&mut &mmr_leaf_proof[..]).unwrap();
        println!("decode the mmr leaf proof = {:?}", decode_mmr_proof);

        // mmr leaf
        println!(
            "generated the mmr leaf  = {:?}",
            format!("{}", HexDisplay::from(&mmr_leaf))
        );

        let mmr_leaf: Vec<u8> = Decode::decode(&mut &mmr_leaf[..]).unwrap();
        println!(
            "decode the mmr leaf vec<u8> = {:?}",
            format!("{}", HexDisplay::from(&mmr_leaf))
        );

        let mmr_leaf_hash = Keccak256::hash(&mmr_leaf[..]);
        println!(
            "the mmr leaf hash = {:?}",
            format!("{}", HexDisplay::from(&mmr_leaf_hash))
        );

        let mmr_leaf_2: beefy_light_client::mmr::MmrLeaf = Decode::decode(&mut &*mmr_leaf).unwrap();
        println!("decode the mmr leaf  = {:?}", mmr_leaf_2);
        println!("parent_number  = {}", mmr_leaf_2.parent_number_and_hash.0);
        println!(
            "parent_hash  = {:?}",
            HexDisplay::from(&mmr_leaf_2.parent_number_and_hash.1)
        );

        let result =
            beefy_light_client::mmr::verify_leaf_proof(mmr_root, mmr_leaf_hash, decode_mmr_proof);

        match result {
            Ok(b) => {
                if !b {
                    println!("mmr::verify_leaf_proof failure:InvalidMmrLeafProof! ");
                } else {
                    println!("mmr::verify_leaf_proof succees! ");
                }
            }

            Err(e) => println!("mr::verify_leaf_proof error! : {:?}", e),
        }

        println!(
            "********************************************************************************"
        );

        let grandpa_client = GrandpaClient;
        let commitment = signed_commitment.commitment.clone();
        let temp_block_number = block_number - 1;
        println!("commitment = {:?}", commitment);
        println!("block_number = {:?}", temp_block_number);

        let ics10_commitment = Commitment::from(commitment);

        let mut ics10_client_state = ClientState::default();
        ics10_client_state.latest_commitment = ics10_commitment;

        let client = ClientBuilder::new()
            .set_url("ws://localhost:9944")
            .build::<ibc_node::DefaultConfig>()
            .await
            .unwrap();

        // get block header
        let ics10_header = octopusxt::call_ibc::get_header_by_block_number(
            client.clone(),
            Some(BlockNumber::from(temp_block_number - 1)),
        )
        .await
        .unwrap();
        println!("block_header = {:?}", ics10_header);
        println!("block_header hash = {:?}", ics10_header.hash());

        let api = client
            .clone()
            .to_runtime_api::<ibc_node::RuntimeApi<ibc_node::DefaultConfig>>();

        // block hash by block number
        let block_hash: Option<sp_core::H256> = api
            .client
            .rpc()
            .block_hash(Some(BlockNumber::from(block_number)))
            .await
            .unwrap();

        // get mmr_leaf and mmr_leaf_proof
        let mmr_leaf_and_mmr_leaf_proof = octopusxt::call_ibc::get_mmr_leaf_and_mmr_proof(
            (temp_block_number - 1) as u64,
            block_hash,
            client,
        )
        .await
        .unwrap();

        println!(
            "mmr_leaf_and_mmr_leaf_proof = {:?}",
            mmr_leaf_and_mmr_leaf_proof
        );

        let encoded_mmr_leaf = mmr_leaf_and_mmr_leaf_proof.1;
        let encode_mmr_leaf_proof = mmr_leaf_and_mmr_leaf_proof.2;

        let leaf: Vec<u8> = Decode::decode(&mut &encoded_mmr_leaf[..]).unwrap();
        let mmr_leaf: beefy_light_client::mmr::MmrLeaf = Decode::decode(&mut &*leaf).unwrap();
        println!("mmr_leaf = {:?}", mmr_leaf);

        let mmr_leaf_proof =
            beefy_light_client::mmr::MmrLeafProof::decode(&mut &encode_mmr_leaf_proof[..]).unwrap();
        println!("mmr_leaf_proof = {:?}", mmr_leaf_proof);

        let ics10_header = Header {
            block_header: BlockHeader {
                parent_hash: ics10_header.parent_hash,
                block_number: ics10_header.block_number,
                state_root: ics10_header.state_root,
                extrinsics_root: ics10_header.extrinsics_root,
                digest: ics10_header.digest,
            },
            mmr_leaf: MmrLeaf::from(mmr_leaf),
            mmr_leaf_proof: MmrLeafProof::from(mmr_leaf_proof),
        };

        // println!(">> ics10_header = {:?}", ics10_header);

        // let result = grandpa_client
        //     .check_header_and_update_state(ics10_client_state, ics10_header)
        //     .unwrap();
        let grandpa_client_1 = GrandpaClientReader;

        let result = grandpa_client
            .check_header_and_update_state(
                &grandpa_client_1,
                ClientId::default(),
                ics10_client_state,
                ics10_header,
            )
            .unwrap();

        // println!(" >> client_state = {:?} ", result.0);
        // println!(" >> consensus_state = {:?}", result.1);

        Ok(())
    }
}