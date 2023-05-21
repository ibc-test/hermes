use super::{monitor::Error, monitor::EventBatch, IbcEventWithHeight};
use crate::chain::{
    handle::Subscription,
    near::{
        contract::NearIbcContract,
        rpc::{client::NearRpcClient, tool::convert_ibc_event_to_hermes_ibc_event},
        CONTRACT_ACCOUNT_ID,
    },
    tracking::TrackingId,
};
use alloc::sync::Arc;
use crossbeam_channel as channel;
use ibc_relayer_types::{
    core::ics02_client::height::Height, core::ics24_host::identifier::ChainId,
};
use serde_json::json;
use std::str::FromStr;
use tokio::runtime::Runtime as TokioRuntime;
use tracing::{error, info, instrument, log::warn};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct TxMonitorCmd(channel::Sender<MonitorCmd>);

impl TxMonitorCmd {
    pub fn shutdown(&self) -> Result<()> {
        self.0
            .send(MonitorCmd::Shutdown)
            .map_err(|_| Error::channel_send_failed())
    }

    pub fn subscribe(&self) -> Result<Subscription> {
        let (tx, rx) = crossbeam_channel::bounded(1);

        self.0
            .send(MonitorCmd::Subscribe(tx))
            .map_err(|_| Error::channel_send_failed())?;

        let subscription = rx.recv().map_err(|_| Error::channel_recv_failed())?;
        Ok(subscription)
    }
}

#[derive(Debug)]
pub enum MonitorCmd {
    Shutdown,
    Subscribe(crossbeam_channel::Sender<Subscription>),
}

pub enum Next {
    Abort,
    Continue,
}

/// Connect to a Tendermint node, subscribe to a set of queries,
/// receive push events over a websocket, and filter them for the
/// event handler.
///
/// The default events that are queried are:
/// - [`EventType::NewBlock`](tendermint_rpc::query::EventType::NewBlock)
/// - [`EventType::Tx`](tendermint_rpc::query::EventType::Tx)
pub struct NearEventMonitor {
    chain_id: ChainId,
    /// The NEAR rpc client to collect IBC events
    client: NearRpcClient,
    /// Channel where to send EventBatch
    event_tx: Option<crossbeam_channel::Sender<Arc<Result<EventBatch>>>>,
    /// Channel where to receive commands
    rx_cmd: channel::Receiver<MonitorCmd>,
    /// Tokio runtime
    rt: Arc<TokioRuntime>,
    /// The heights that have already been checked for IBC events.
    checked_heights: Vec<u64>,
}

impl NearIbcContract for NearEventMonitor {
    fn get_contract_id(&self) -> near_account_id::AccountId {
        near_account_id::AccountId::from_str(CONTRACT_ACCOUNT_ID).unwrap()
    }

    fn get_client(&self) -> &NearRpcClient {
        &self.client
    }

    fn get_rt(&self) -> &Arc<TokioRuntime> {
        &self.rt
    }
}

impl NearEventMonitor {
    /// Create an event monitor, and connect to a node
    #[instrument(
        name = "near_event_monitor.create",
        level = "error",
        skip_all,
        fields(chain = %chain_id, rpc_client = %rpc_addr)
    )]
    pub fn new(
        chain_id: ChainId,
        rpc_addr: String,
        rt: Arc<TokioRuntime>,
    ) -> Result<(Self, TxMonitorCmd)> {
        let (tx_cmd, rx_cmd) = channel::unbounded();

        let client = NearRpcClient::new(rpc_addr.as_str());

        let monitor = Self {
            rt,
            chain_id,
            client,
            event_tx: None,
            rx_cmd,
            checked_heights: vec![],
        };

        Ok((monitor, TxMonitorCmd(tx_cmd)))
    }

    /// Event monitor loop
    #[allow(clippy::while_let_loop)]
    #[instrument(
        name = "event_monitor",
        level = "error",
        skip_all,
        fields(chain = %self.chain_id)
    )]
    pub fn run(mut self) {
        info!("starting event monitor");

        // Continuously run the event loop, so that when it aborts
        // because of WebSocket client restart, we pick up the work again.
        loop {
            match self.run_loop() {
                Next::Continue => continue,
                Next::Abort => break,
            }
        }

        info!("event monitor has successfully shut down");
    }

    fn run_loop(&mut self) -> Next {
        loop {
            info!("checking monitor commands");
            if let Ok(cmd) = self.rx_cmd.try_recv() {
                match cmd {
                    MonitorCmd::Shutdown => return Next::Abort,
                    MonitorCmd::Subscribe(tx) => {
                        let (event_tx, event_rx) = crossbeam_channel::unbounded();
                        self.event_tx = Some(event_tx);
                        if let Err(e) = tx.send(event_rx) {
                            error!("failed to send back subscription: {e}");
                        }
                    }
                }
            }
            if self.event_tx.is_some() {
                let heights = self.get_ibc_events_heights();
                let unchecked_heights = heights
                    .iter()
                    .filter(|h| !self.checked_heights.contains(h))
                    .map(|h| *h)
                    .collect::<Vec<u64>>();
                if unchecked_heights.len() > 0 {
                    let height = unchecked_heights[0];
                    info!("querying ibc events at height: {}", height);
                    let event_tx = self.event_tx.as_ref().unwrap();
                    match self.query_events_at_height(&Height::new(0, height).unwrap()) {
                        Ok(batch) => {
                            if batch.events.len() > 0 {
                                if let Err(e) = event_tx.send(Arc::new(Ok(batch))) {
                                    error!("failed to send event batch: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            if let Err(e) = event_tx.send(Arc::new(Err(e))) {
                                error!("failed to send error in fetching event batch: {}", e);
                            }
                        }
                    }
                    self.checked_heights.push(height);
                }
            }
        }
    }

    fn get_ibc_events_heights(&self) -> Vec<u64> {
        self.get_rt()
            .block_on(self.get_client().view(
                self.get_contract_id().clone(),
                "get_ibc_events_heights".to_string(),
                json!({}).to_string().into_bytes(),
            ))
            .map_or_else(|_| vec![], |result| result.json().unwrap())
    }

    fn query_events_at_height(&self, height: &Height) -> Result<EventBatch> {
        self.get_rt()
            .block_on(
                self.get_client().view(
                    self.get_contract_id().clone(),
                    "get_ibc_events_at".to_string(),
                    json!({ "height": height.revision_height() })
                        .to_string()
                        .into_bytes(),
                ),
            )
            .map_or_else(
                |_| {
                    Ok(EventBatch {
                        height: height.clone(),
                        events: vec![],
                        chain_id: self.chain_id.clone(),
                        tracking_id: TrackingId::new_uuid(),
                    })
                },
                |result| {
                    let ibc_events: Vec<ibc::events::IbcEvent> = result.json().unwrap();
                    Ok(EventBatch {
                        height: height.clone(),
                        events: ibc_events
                            .iter()
                            .map(|event| IbcEventWithHeight {
                                height: height.clone(),
                                event: convert_ibc_event_to_hermes_ibc_event(event),
                            })
                            .collect(),
                        chain_id: self.chain_id.clone(),
                        tracking_id: TrackingId::new_uuid(),
                    })
                },
            )
    }
}
