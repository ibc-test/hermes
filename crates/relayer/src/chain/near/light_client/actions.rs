use near_light_client::BasicNearLightClient;
use near_primitives::types::BlockId;
use near_primitives::views::BlockView;

use crate::info_with_time;

use super::near_rpc_client_wrapper::NearRpcClientWrapper;
use super::utils::produce_light_client_block;
use super::LightClient;

pub async fn start_light_client(
    rpc_addr: String,
    state_data_folder: String,
    max_cached_heights: u64,
) {
    let rpc_client = NearRpcClientWrapper::new(rpc_addr.as_str());
    let mut light_client = LightClient::new(state_data_folder.clone());
    //
    // Keep updating state and save state to file
    //
    let latest_height = match light_client.latest_height() {
        0 => None,
        height => Some(height),
    };
    let mut block_view = get_block(&rpc_client, &latest_height).await;
    let mut should_break = false;
    while !should_break {
        let light_client_block_view = rpc_client
            .get_next_light_client_block(&block_view.header.hash)
            .await
            .expect("Failed to get next light client block.");
        block_view = get_block(
            &rpc_client,
            &Some(light_client_block_view.inner_lite.height),
        )
        .await;
        let header = produce_light_client_block(&light_client_block_view, &block_view);
        let current_cs = light_client.get_consensus_state(&light_client.latest_height());
        let current_bps = match current_cs {
            Some(cs) => cs.get_block_producers_of(&header.epoch_id()),
            None => None,
        };
        if current_bps.is_some() {
            if let Err(err) = light_client.verify_header(&header) {
                tracing::error!(
                    "Failed to verify header at height {}: {:?}",
                    header.height(),
                    err
                );
                should_break = true;
            } else {
                info_with_time!(
                    "Successfully verified header at height {}.",
                    header.height()
                );
            }
        } else {
            info_with_time!("Skip verifying header at height {}.", header.height());
        }
        light_client.update_state(header);
        //
        while light_client.cached_heights().len() > max_cached_heights as usize {
            light_client.remove_oldest_head();
        }
    }
}

async fn get_block(rpc_client: &NearRpcClientWrapper, height: &Option<u64>) -> BlockView {
    rpc_client
        .view_block(&height.map(|height| BlockId::Height(height)))
        .await
        .expect(format!("Failed to get block at height {:?}.", height).as_str())
}
