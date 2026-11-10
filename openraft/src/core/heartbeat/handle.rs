use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use openraft_macros::since;
use tracing::Instrument;
use tracing::Level;
use tracing::Span;

use crate::Config;
use crate::OptionalSend;
use crate::RaftNetworkFactory;
use crate::RaftTypeConfig;
use crate::async_runtime::AsyncRuntime;
use crate::async_runtime::Charge;
use crate::async_runtime::Oneshot;
use crate::async_runtime::watch::Watch;
use crate::async_runtime::watch::WatchSender;
use crate::core::heartbeat::event::HeartbeatEvent;
use crate::core::heartbeat::worker::HeartbeatWorker;
use crate::core::notification::Notification;
use crate::engine::TargetProgress;
use crate::errors::Unfunded;
use crate::network::NetStreamAppend;
use crate::progress::stream_id::StreamId;
use crate::proposer::funding::Funding;
use crate::proposer::funding::TaskCharge;
use crate::proposer::funding::tree_bytes;
use crate::type_config::alias::AsyncRuntimeOf;
use crate::type_config::alias::CommittedVoteOf;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::MpscSenderOf;
use crate::type_config::alias::OneshotOf;
use crate::type_config::alias::OneshotReceiverOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::WatchOf;
use crate::type_config::alias::WatchSenderOf;

/// Handle for a single heartbeat worker task.
pub(crate) struct WorkerHandle<C>
where C: RaftTypeConfig
{
    /// Channel to send heartbeat events to the worker.
    event_tx: WatchSenderOf<C, Option<HeartbeatEvent<C>>>,

    #[allow(dead_code)]
    stream_id: StreamId,

    /// Channel to signal shutdown to the worker.
    ///
    /// When this sender is dropped, the worker's receiver will detect it and shutdown.
    _shutdown_tx: OneshotSenderOf<C, ()>,

    /// Join handle for the worker task.
    _join_handle: JoinHandleOf<C, ()>,
}

/// The bytes a Leader's map of its heartbeat workers is charged, for `members` other members:
/// the tree's nodes, charged before it is built and held until they are freed. A node run with a
/// funder holds two such maps charged at once while it rebuilds its workers, the one it builds
/// and the one it replaces.
#[since(version = "0.10.0")]
pub const fn heartbeat_map_bytes<C: RaftTypeConfig>(members: usize) -> usize {
    tree_bytes::<C::NodeId, WorkerHandle<C>>(members)
}

/// The bytes a Leader charges for each heartbeat worker it runs for another member, sending
/// through clients of `NF`, before any of the worker is made: its task, everything the runtime
/// allocates for it, and the channels that stop it and send it heartbeats, each held until it is
/// freed. The client is not among them: it funds its own allocations. A node run with a funder
/// holds two workers charged at once per other member while it rebuilds its workers, as a
/// replaced worker's task may still run until it sees it is stopped.
///
/// `None` if the runtime cannot fund its tasks or channels now, as
/// [`AsyncRuntime::spawn_bytes`](crate::async_runtime::AsyncRuntime::spawn_bytes) tells: a funded
/// Leader runs no heartbeat worker then.
#[since(version = "0.10.0")]
pub fn heartbeat_worker_bytes<C, NF>() -> Option<usize>
where
    C: RaftTypeConfig,
    NF: RaftNetworkFactory<C>,
{
    worker_task_bytes(worker_task::<C, NF::Network>)?.checked_add(worker_parts_bytes::<C>()?)
}

/// The future a heartbeat worker's task runs: the worker's loop until `rx_shutdown`, in `span`.
fn worker_task<C, N>(
    worker: HeartbeatWorker<C, N>,
    rx_shutdown: OneshotReceiverOf<C, ()>,
    span: Span,
) -> impl Future<Output = ()>
where
    C: RaftTypeConfig,
    N: NetStreamAppend<C>,
{
    worker.run(rx_shutdown).instrument(span)
}

/// What the runtime charges a task running the future `task` makes, as
/// [`TaskCharge::take`] asks it.
fn worker_task_bytes<C, N, T>(
    _task: impl FnOnce(HeartbeatWorker<C, N>, OneshotReceiverOf<C, ()>, Span) -> T,
) -> Option<usize>
where
    C: RaftTypeConfig,
    N: NetStreamAppend<C>,
    T: Future + OptionalSend + 'static,
    T::Output: OptionalSend + 'static,
{
    AsyncRuntimeOf::<C>::spawn_bytes::<T>()
}

/// The blocks of the channels a heartbeat worker holds: the one that stops it and the one it is
/// sent heartbeats on.
fn worker_parts_bytes<C: RaftTypeConfig>() -> Option<usize> {
    let stop = OneshotOf::<C>::block_bytes::<()>()?;
    let events = WatchOf::<C>::block_bytes::<Option<HeartbeatEvent<C>>>()?;
    stop.checked_add(events)
}

pub(crate) struct HeartbeatWorkersHandle<C>
where C: RaftTypeConfig
{
    pub(crate) id: C::NodeId,

    pub(crate) config: Arc<Config>,

    pub(crate) workers: BTreeMap<C::NodeId, WorkerHandle<C>>,

    /// The nodes of `workers`, charged before the map is built and held, declared after it, until
    /// they are freed.
    workers_funding: Option<Charge>,

    /// The targets of the last [`spawn_workers`](Self::spawn_workers), kept while any of them is
    /// left without a worker because its charge was refused: spawned again before each heartbeat
    /// is sent, until each has its worker. What the caller rebuilt them with, kept, not copied.
    unspawned: Option<Vec<TargetProgress<C>>>,
}

impl<C> HeartbeatWorkersHandle<C>
where C: RaftTypeConfig
{
    pub(crate) fn new(id: C::NodeId, config: Arc<Config>) -> Self {
        Self {
            id,
            config,
            workers: Default::default(),
            workers_funding: None,
            unspawned: None,
        }
    }

    pub(crate) fn broadcast(&self, events: impl IntoIterator<Item = (C::NodeId, HeartbeatEvent<C>)>) {
        for (target, event) in events {
            tracing::debug!("id={} target={} send_heartbeat {}", self.id, target, event);
            // A target whose worker was refused its funding has none to send to until it is spawned
            // again, before a later heartbeat: see `spawn_unspawned()`.
            if let Some(worker) = self.workers.get(&target) {
                worker.event_tx.send(Some(event)).ok();
            }
        }
    }

    pub(crate) fn close_workers(&mut self) {
        self.unspawned = None;
        self.workers = Default::default();
        // Only after the map's nodes are freed.
        self.workers_funding = None;
    }

    /// Whether the last [`spawn_workers`](Self::spawn_workers) left a target without its worker.
    pub(crate) fn has_unspawned(&self) -> bool {
        self.unspawned.is_some()
    }

    /// Spawns again, each charged to `funding` as [`spawn_workers`](Self::spawn_workers) charges
    /// it, the workers the last spawn left their targets without, keeping the workers they have;
    /// those refused again are kept to be spawned before the next heartbeat.
    pub(crate) async fn spawn_unspawned<NF>(
        &mut self,
        leader_vote: CommittedVoteOf<C>,
        network_factory: &mut NF,
        tx_notification: &MpscSenderOf<C, Notification<C>>,
        funding: &Funding,
    ) where
        NF: RaftNetworkFactory<C>,
    {
        let Some(targets) = self.unspawned.take() else {
            return;
        };
        if !self.spawn_workers(leader_vote, network_factory, tx_notification, &targets, false, funding).await {
            self.keep_unspawned(targets);
        }
    }

    /// Keeps `targets`, which [`spawn_workers`](Self::spawn_workers) was just given and left
    /// some of without a worker, to be spawned again before the next heartbeat.
    pub(crate) fn keep_unspawned(&mut self, targets: Vec<TargetProgress<C>>) {
        self.unspawned = Some(targets);
    }

    /// Spawns a worker for each of `progresses` that has none, or each if `close_old`, every part
    /// of a worker charged to `funding` before any of it is made: its task, the channels that
    /// stop it and send it heartbeats, and the network client it sends them with, which funds its
    /// own copy of the target. A worker refused its charge is not made; the map holding the
    /// workers is charged before it is built, refused, no worker is spawned, and the old ones are
    /// kept unless `close_old`. Returns whether every target has its worker: if not, the caller
    /// keeps `progresses` with [`keep_unspawned`](Self::keep_unspawned), so those left without are
    /// spawned again before the next heartbeat is sent.
    pub(crate) async fn spawn_workers<NF>(
        &mut self,
        leader_vote: CommittedVoteOf<C>,
        network_factory: &mut NF,
        tx_notification: &MpscSenderOf<C, Notification<C>>,
        progresses: &[TargetProgress<C>],
        close_old: bool,
        funding: &Funding,
    ) -> bool
    where
        NF: RaftNetworkFactory<C>,
    {
        // Whatever an earlier spawn left unspawned is replaced by what this one leaves.
        self.unspawned = None;

        let Ok(workers_funding) = funding.charge(heartbeat_map_bytes::<C>(progresses.len())) else {
            tracing::warn!("id={}: no HeartbeatWorkers: their map is unfunded", self.id);
            // Workers asked to be replaced must not go on sending for what they were made for.
            if close_old {
                self.close_workers();
            }
            return false;
        };

        let mut new_workers = BTreeMap::new();
        let mut complete = true;

        for prog in progresses {
            tracing::debug!("id={} spawn HeartbeatWorker target={}", self.id, prog.target);

            let removed = self.workers.remove(&prog.target);

            let handle = removed.filter(|_removed| !close_old);

            let handle = if let Some(handle) = handle {
                handle
            } else {
                let spawned = Self::spawn_worker(
                    self.id.clone(),
                    self.config.clone(),
                    leader_vote.clone(),
                    network_factory,
                    tx_notification,
                    prog,
                    funding,
                )
                .await;
                let Some(handle) = spawned else {
                    complete = false;
                    continue;
                };
                handle
            };

            new_workers.insert(prog.target.clone(), handle);
        }

        // All the left are dropped and closed, their map's nodes freed before its charge goes
        // back.
        let left = std::mem::replace(&mut self.workers, new_workers);
        let left_funding = std::mem::replace(&mut self.workers_funding, workers_funding);
        drop(left);
        drop(left_funding);
        complete
    }

    /// A worker of `id`'s sending heartbeats to `prog`'s target, every part of it charged before
    /// any is made: none, nothing made, if the charge is refused. It borrows nothing of the
    /// handle, whose workers need not be shared with other threads, across its awaits.
    async fn spawn_worker<NF>(
        id: C::NodeId,
        config: Arc<Config>,
        leader_vote: CommittedVoteOf<C>,
        network_factory: &mut NF,
        tx_notification: &MpscSenderOf<C, Notification<C>>,
        prog: &TargetProgress<C>,
        funding: &Funding,
    ) -> Option<WorkerHandle<C>>
    where
        NF: RaftNetworkFactory<C>,
    {
        let mut task = match TaskCharge::<C, _>::take(funding, worker_parts_bytes::<C>) {
            Ok(task) => task,
            Err(Unfunded) => {
                tracing::warn!("id={} target={}: no HeartbeatWorker: it is unfunded", id, prog.target);
                return None;
            }
        };

        // Each part from its share of the charge taken for all of them: none is refused.
        let (tx_shutdown, rx_shutdown) = task.oneshot::<()>().ok()?;
        let (tx, rx) = task.watch(None).ok()?;

        let network = network_factory.new_heartbeat_client(prog.target.clone(), &prog.target_node).await;

        let span = tracing::span!(parent: &Span::current(), Level::DEBUG, "heartbeat", id=display(&id), target=display(&prog.target));

        let worker = HeartbeatWorker {
            id,
            leader_vote,
            stream_id: prog.progress.data.stream_id,
            rx,
            network,
            target: prog.target.clone(),
            config,
            tx_notification: tx_notification.clone(),
        };

        let worker_handle = task.spawn(worker_task(worker, rx_shutdown, span)).ok()?;

        Some(WorkerHandle {
            event_tx: tx,
            stream_id: prog.progress.data.stream_id,
            _shutdown_tx: tx_shutdown,
            _join_handle: worker_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::Poll;

    use futures_util::Stream;
    use futures_util::StreamExt;

    use super::HeartbeatWorkersHandle;
    use crate::Config;
    use crate::OptionalSend;
    use crate::RaftNetworkFactory;
    use crate::RaftNetworkV2;
    use crate::async_runtime::MpscReceiver;
    use crate::base::BoxFuture;
    use crate::base::BoxStream;
    use crate::core::heartbeat::event::HeartbeatEvent;
    use crate::core::notification::Notification;
    use crate::engine::TargetProgress;
    use crate::engine::testing::UTConfig;
    use crate::engine::testing::UTLeaderId;
    use crate::errors::RPCError;
    use crate::errors::ReplicationClosed;
    use crate::errors::StreamingError;
    use crate::impls::Vote;
    use crate::network::RPCOption;
    use crate::progress::entry::ProgressEntry;
    use crate::progress::stream_id::StreamId;
    use crate::proposer::SendStamp;
    use crate::proposer::funding::Funding;
    use crate::proposer::funding::testing::Limit;
    use crate::raft::AppendEntriesRequest;
    use crate::raft::AppendEntriesResponse;
    use crate::raft::SnapshotResponse;
    use crate::raft::StreamAppendResult;
    use crate::raft::VoteRequest;
    use crate::raft::VoteResponse;
    use crate::type_config::TypeConfigExt;
    use crate::type_config::alias::SnapshotOf;
    use crate::type_config::alias::VoteOf;
    use crate::vote::raft_vote::RaftVoteExt;

    type C = UTConfig;

    /// A follower answering every heartbeat it is sent.
    struct Follower;

    impl RaftNetworkV2<C> for Follower {
        type SnapshotData = ();

        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<C>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
            unreachable!("heartbeats are streamed")
        }

        fn stream_append<'s, S>(
            &'s mut self,
            mut input: S,
            _option: RPCOption,
        ) -> BoxFuture<'s, Result<BoxStream<'s, Result<StreamAppendResult<C>, RPCError<C>>>, RPCError<C>>>
        where
            S: Stream<Item = AppendEntriesRequest<C>> + OptionalSend + Unpin + 'static,
        {
            let output = futures_util::stream::poll_fn(move |cx| match input.poll_next_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Some(_heartbeat)) => Poll::Ready(Some(Ok(Ok(None)))),
                Poll::Ready(None) => Poll::Ready(None),
            });
            let output: BoxStream<'s, _> = Box::pin(output);
            Box::pin(async move { Ok(output) })
        }

        async fn vote(&mut self, _rpc: VoteRequest<C>, _option: RPCOption) -> Result<VoteResponse<C>, RPCError<C>> {
            unreachable!("a heartbeat worker does not vote")
        }

        async fn full_snapshot(
            &mut self,
            _vote: VoteOf<C>,
            _snapshot: SnapshotOf<C, ()>,
            _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
            _option: RPCOption,
        ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
            unreachable!("a heartbeat worker sends no snapshot")
        }
    }

    struct Followers;

    impl RaftNetworkFactory<C> for Followers {
        type Network = Follower;

        async fn new_client(&mut self, _target: u64, _node: &()) -> Follower {
            Follower
        }
    }

    /// A target whose heartbeat worker is refused its charge has no worker, and is kept to be
    /// spawned again before a later heartbeat: refused again, it is still kept; once its funding
    /// holds, its worker is spawned and the heartbeat broadcast next reaches it.
    #[test]
    fn test_a_refused_worker_is_spawned_again_and_reached_by_the_next_heartbeat() {
        C::run(async {
            let (tx_notification, mut rx_notification) = C::mpsc(16);
            let mut handle = HeartbeatWorkersHandle::<C>::new(1, Arc::new(Config::default()));
            let leader_vote = Vote::<UTLeaderId>::new(1, 1).to_committed();
            let targets = vec![TargetProgress {
                target: 2,
                target_node: (),
                progress: ProgressEntry::empty(2, StreamId::new(1), 0),
            }];
            let heartbeat = || {
                [(2, HeartbeatEvent {
                    time: SendStamp::new(C::now(), 1),
                    matching: None,
                    cluster_committed: None,
                })]
            };

            let refusing = Limit::new(0);
            let complete = handle
                .spawn_workers(
                    leader_vote.clone(),
                    &mut Followers,
                    &tx_notification,
                    &targets,
                    false,
                    &refusing.funding(),
                )
                .await;
            assert!(!complete);
            assert_eq!(0, refusing.held(), "a refused worker is charged nothing");
            handle.keep_unspawned(targets);
            assert!(handle.workers.is_empty());

            handle
                .spawn_unspawned(
                    leader_vote.clone(),
                    &mut Followers,
                    &tx_notification,
                    &refusing.funding(),
                )
                .await;
            assert!(handle.has_unspawned(), "refused again, it is kept");
            handle.broadcast(heartbeat());
            assert!(rx_notification.try_recv().is_err(), "no worker reaches it");

            // A node whose funder holds what the worker takes; the test runtime funds no task, so
            // the worker is made as on a node run unfunded.
            handle.spawn_unspawned(leader_vote, &mut Followers, &tx_notification, &Funding::default()).await;
            assert!(!handle.has_unspawned());
            handle.broadcast(heartbeat());
            let reached = rx_notification.recv().await.unwrap();
            assert!(
                matches!(reached, Notification::HeartbeatProgress { target: 2, .. }),
                "the next heartbeat reaches it"
            );
            handle.close_workers();
        });
    }
}
