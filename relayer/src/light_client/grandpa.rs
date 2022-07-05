use crate::chain::substrate::SubstrateChain;
use crate::error::Error;

use octopusxt::{MyConfig, OctopusxtClient};
use std::future::Future;
use std::sync::Arc;
use subxt::{BlockNumber, ClientBuilder};
use tokio::runtime::Runtime as TokioRuntime;

use crate::config::ChainConfig;
use crate::light_client::Verified;
use ibc::clients::ics10_grandpa::header::Header as GPHeader;
use ibc::clients::ics10_grandpa::help::BlockHeader;
use ibc::core::ics02_client::client_state::AnyClientState;
use ibc::core::ics02_client::events::UpdateClient;

use ibc::core::ics02_client::misbehaviour::MisbehaviourEvidence;
use ibc::core::ics24_host::identifier::ChainId;

use ibc::Height;

pub struct LightClient {
    chain_id: ChainId,
    websocket_url: String,
    rt: Arc<TokioRuntime>,
    // beefy_light_client: beefy_light_client::LightClient,
}

impl LightClient {
    pub fn from_config(
        config: &ChainConfig,
        websocket_url: String,
        rt: Arc<TokioRuntime>,
        _initial_public_keys: Vec<String>,
    ) -> Self {
        let chain_id = config.id.clone();
        // let beefy_light_client = beefy_light_client::new(initial_public_keys);
        Self {
            chain_id,
            websocket_url,
            rt,
            // beefy_light_client,
        }
    }

    /// Run a future to completion on the Tokio runtime.
    pub fn block_on<F: Future>(&self, f: F) -> F::Output {
        crate::time!("block_on");
        self.rt.block_on(f)
    }
}

impl super::LightClient<SubstrateChain> for LightClient {
    fn header_and_minimal_set(
        &mut self,
        trusted: Height,
        target: Height,
        _client_state: &AnyClientState,
    ) -> Result<Verified<GPHeader>, Error> {
        tracing::info!("In grandpa: [header_and_minimal_set]");

        Ok(Verified {
            target: GPHeader {
                block_header: BlockHeader {
                    parent_hash: vec![],
                    block_number: target.revision_height as u32,
                    state_root: vec![],
                    extrinsics_root: vec![],
                    digest: vec![],
                },
                mmr_leaf: Default::default(),
                mmr_leaf_proof: Default::default(),
            },
            supporting: vec![GPHeader {
                block_header: BlockHeader {
                    parent_hash: vec![],
                    block_number: trusted.revision_height as u32,
                    state_root: vec![],
                    extrinsics_root: vec![],
                    digest: vec![],
                },
                mmr_leaf: Default::default(),
                mmr_leaf_proof: Default::default(),
            }],
        })
    }

    fn verify(
        &mut self,
        _trusted: Height,
        target: Height,
        _client_state: &AnyClientState,
    ) -> Result<Verified<GPHeader>, Error> {
        tracing::info!("In grandpa: [verify]");

        let block_header = async {
            let client = ClientBuilder::new()
                .set_url(&self.websocket_url.clone())
                .build::<MyConfig>()
                .await
                .map_err(|_| Error::substrate_client_builder_error())?;

            let client = OctopusxtClient::new(client);

            // get block header
            let block_header = client
                .query_header_by_block_number(Some(BlockNumber::from(
                    target.revision_height as u32,
                )))
                .await
                .map_err(|_| Error::get_header_by_block_number_error())?;

            Ok(block_header)
        };

        let result = self.block_on(block_header)?;

        Ok(Verified {
            target: GPHeader {
                block_header: result,
                mmr_leaf: Default::default(),
                mmr_leaf_proof: Default::default(),
            },
            supporting: Vec::new(),
        })
    }

    fn check_misbehaviour(
        &mut self,
        _update: UpdateClient,
        _client_state: &AnyClientState,
    ) -> Result<Option<MisbehaviourEvidence>, Error> {
        tracing::info!("in grandpa: [check_misbehaviour]");

        Ok(None) // Todo: May need to implement the same logic of check_misbehaviour in tendermint.rs
    }

    fn fetch(&mut self, _height: Height) -> Result<GPHeader, Error> {
        tracing::info!("in grandpa: [fetch]");

        Ok(GPHeader::default())
    }
}
