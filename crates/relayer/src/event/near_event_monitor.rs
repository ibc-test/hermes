use alloc::sync::Arc;
use core::cmp::Ordering;
use near_jsonrpc_primitives::types::transactions::TransactionInfo;
use near_primitives::{
    types::{BlockId, StateChangeCause},
    views::StateChangeCauseView,
};
use std::str::FromStr;

use crate::{
    chain::{
        handle::Subscription,
        near::{
            collect_ibc_event_by_outcome, contract::NearIbcContract, rpc::client::NearRpcClient,
            CONTRACT_ACCOUNT_ID, SIGNER_ACCOUNT_TESTNET,
        },
        tracking::TrackingId,
    },
    telemetry,
    util::{
        retry::{retry_with_index, RetryResult},
        stream::try_group_while,
    },
};
use crossbeam_channel as channel;
use futures::{
    pin_mut,
    stream::{self, select_all, StreamExt},
    Stream, TryStreamExt,
};
use ibc_relayer_types::{
    core::ics02_client::height::Height, core::ics24_host::identifier::ChainId, events::IbcEvent,
};
use tokio::{
    task::JoinHandle,
    {runtime::Runtime as TokioRuntime, sync::mpsc},
};
use tracing::{debug, error, info, instrument, log::warn, trace};

use super::{bus::EventBus, monitor::Error, monitor::EventBatch, IbcEventWithHeight};

pub type Result<T> = core::result::Result<T, Error>;

mod retry_strategy {
    use crate::util::retry::clamp_total;
    use core::time::Duration;
    use retry::delay::Fibonacci;

    // Default parameters for the retrying mechanism
    const MAX_DELAY: Duration = Duration::from_secs(60); // 1 minute
    const MAX_TOTAL_DELAY: Duration = Duration::from_secs(10 * 60); // 10 minutes
    const INITIAL_DELAY: Duration = Duration::from_secs(1); // 1 second

    pub fn default() -> impl Iterator<Item = Duration> {
        clamp_total(Fibonacci::from(INITIAL_DELAY), MAX_DELAY, MAX_TOTAL_DELAY)
    }
}

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
    /// The latest height while waiting for ibc events
    waiting_on_height: Option<Height>,
    /// The account id that used by relayer to sign transactions on NEAR protocol
    signer_account_id: near_account_id::AccountId,
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
        signer_account_id: &str,
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
            waiting_on_height: None,
            signer_account_id: near_account_id::AccountId::from_str(signer_account_id).unwrap(),
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
                        match self.get_latest_height() {
                            Ok(height) => {
                                self.waiting_on_height = Some(height);
                            }
                            Err(e) => error!("Failed to get the latest height: {}", e),
                        };
                        let (event_tx, event_rx) = crossbeam_channel::unbounded();
                        self.event_tx = Some(event_tx);
                        if let Err(e) = tx.send(event_rx) {
                            error!("failed to send back subscription: {e}");
                        }
                    }
                }
            }
            if let Some(height) = &self.waiting_on_height {
                info!("querying ibc events");
                let event_tx = self.event_tx.as_ref().unwrap();
                match self.query_events_at_height(height) {
                    Ok(batch) => {
                        if batch.events.len() > 0 {
                            if let Err(e) = event_tx.send(Arc::new(Ok(batch))) {
                                error!("failed to send event batch: {}", e);
                            }
                            self.waiting_on_height = None;
                            self.event_tx = None;
                        } else {
                            self.waiting_on_height = Some(height.increment());
                        }
                    }
                    Err(e) => {
                        if let Err(e) = event_tx.send(Arc::new(Err(e))) {
                            error!("failed to send error in fetching event batch: {}", e);
                        }
                        self.waiting_on_height = None;
                        self.event_tx = None;
                    }
                }
            }
        }
    }

    fn query_events_at_height(&self, height: &Height) -> Result<EventBatch> {
        if let Ok(state_changes) = self.get_rt().block_on(self.client.get_state_changes_of(
            self.get_contract_id(),
            Some(BlockId::Height(height.revision_height())),
        )) {
            let mut ibc_events = vec![];
            for state_change in state_changes {
                match state_change.cause {
                    StateChangeCauseView::TransactionProcessing { tx_hash } => {
                        let final_execution_outcome_view = self
                            .get_rt()
                            .block_on(self.client.view_tx(TransactionInfo::TransactionId {
                                hash: tx_hash,
                                account_id: self.signer_account_id.clone(),
                            }))
                            .expect("Failed to get transaction info");
                        ibc_events.extend(
                            collect_ibc_event_by_outcome(final_execution_outcome_view).drain(..),
                        );
                    }
                    _ => (),
                }
            }
            Ok(EventBatch {
                height: height.clone(),
                events: ibc_events,
                chain_id: self.chain_id.clone(),
                tracking_id: TrackingId::new_uuid(),
            })
        } else {
            warn!("Failed to get state changes at height {}, skip it.", height);
            Ok(EventBatch {
                height: height.clone(),
                events: vec![],
                chain_id: self.chain_id.clone(),
                tracking_id: TrackingId::new_uuid(),
            })
        }
    }
}
