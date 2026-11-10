use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use display_more::DisplayOptionExt;
use display_more::DisplaySliceExt;
use tracing::Instrument;
use tracing::Level;
use tracing::Span;

use crate::ChangeMembers;
use crate::Instant;
use crate::Membership;
use crate::OptionalSend;
use crate::RaftState;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::async_runtime::Charge;
use crate::async_runtime::MpscReceiver;
use crate::async_runtime::OneshotSender;
use crate::async_runtime::Select2;
use crate::async_runtime::Select4;
use crate::async_runtime::TryRecvError;
use crate::async_runtime::mpsc::PeekableReceiver;
use crate::async_runtime::watch::WatchSender;
use crate::batch::Batch;
use crate::config::Config;
use crate::config::RuntimeConfig;
use crate::core::ClientResponderQueue;
use crate::core::IoBroadcast;
use crate::core::MetricsChannels;
use crate::core::PendingRead;
use crate::core::PendingReadDeadlineNotifier;
use crate::core::PendingReadQueue;
use crate::core::ServerState;
use crate::core::SharedReplicateBatch;
use crate::core::balancer::Balancer;
use crate::core::core_state::CoreState;
use crate::core::heartbeat::event::HeartbeatEvent;
use crate::core::heartbeat::handle::HeartbeatWorkersHandle;
use crate::core::heartbeat::handle::Spawning;
use crate::core::merged_raft_msg_receiver::BatchRaftMsgReceiver;
use crate::core::notification::Notification;
use crate::core::raft_msg::AppendEntriesTx;
use crate::core::raft_msg::ClientReadTx;
use crate::core::raft_msg::RaftMsg;
use crate::core::raft_msg::RaftMsgName;
use crate::core::raft_msg::ResultSender;
use crate::core::raft_msg::VoteTx;
use crate::core::raft_msg::external_command::ExternalCommand;
use crate::core::raft_msg::install_full_snapshot_request::InstallFullSnapshotRequest;
use crate::core::raft_msg::membership_payloads::MembershipPayloads;
use crate::core::runtime_stats::Recorder;
use crate::core::sm;
use crate::core::stage::Stage;
use crate::display_ext::DisplayInstantExt;
use crate::engine::Command;
use crate::engine::Condition;
use crate::engine::Engine;
use crate::engine::Respond;
use crate::engine::TargetProgress;
use crate::engine::event_commands;
use crate::engine::handler::leader_handler::LeaderHandler;
use crate::engine::leader_log_ids::LeaderLogIds;
use crate::entry::RaftEntry;
use crate::entry::RaftPayload;
use crate::errors::AllowNextRevertError;
use crate::errors::ClientWriteError;
use crate::errors::Fatal;
use crate::errors::ForwardReason;
use crate::errors::ForwardToLeader;
use crate::errors::Infallible;
use crate::errors::InitializeError;
use crate::errors::LinearizableReadError;
use crate::errors::MembershipRefused;
use crate::errors::PreconditionFailed;
use crate::errors::StorageIOResult;
use crate::errors::Timeout;
use crate::errors::UncommittedLeaderLog;
use crate::errors::Unfunded;
use crate::impls::ProgressResponder;
use crate::log_id::option_raft_log_id_ext::OptionRaftLogIdExt;
use crate::metrics::HeartbeatMetrics;
use crate::metrics::MetricsRecorder;
use crate::metrics::RaftDataMetrics;
use crate::metrics::RaftMetrics;
use crate::metrics::RaftServerMetrics;
use crate::metrics::ReplicationMetrics;
use crate::metrics::SerdeInstant;
use crate::network::NetSnapshot;
use crate::network::NetTransferLeader;
use crate::network::NetVote;
use crate::network::RPCOption;
use crate::network::RPCTypes;
use crate::network::RaftNetworkFactory;
use crate::network::clients_bytes;
use crate::progress::VecProgressEntry;
use crate::progress::inflight_id::InflightId;
use crate::progress::stream_id::StreamId;
use crate::proposer::Leader;
use crate::proposer::LeaderQuorumSet;
use crate::proposer::SendStamp;
use crate::proposer::funding::Elapsed;
use crate::proposer::funding::Funding;
use crate::proposer::funding::Prepaid;
use crate::proposer::funding::Shared;
use crate::proposer::funding::TaskCharge;
use crate::proposer::funding::within;
use crate::raft::AppendEntriesRequest;
use crate::raft::ClientWriteResult;
use crate::raft::LogSegment;
use crate::raft::Precondition;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::raft::linearizable_read::Linearizer;
use crate::raft::linearizable_read::LinearizerOption;
use crate::raft::message::TransferLeaderRequest;
use crate::raft::responder::Responder;
use crate::raft::responder::core_responder::CoreResponder;
use crate::raft_state::LogStateReader;
use crate::raft_state::io_state::io_id::IOId;
use crate::raft_state::io_state::log_io_id::LogIOId;
use crate::replication::ReplicationCore;
use crate::replication::ReplicationSessionId;
use crate::replication::event_watcher::EventWatcher;
use crate::replication::replicate::Replicate;
use crate::replication::replication_context::ReplicationContext;
use crate::replication::replication_handle::ReplicationHandle;
use crate::replication::replication_handle::replication_map_bytes;
use crate::replication::replication_progress;
use crate::replication::snapshot_transmitter::SnapshotTransmitter;
use crate::replication::snapshot_transmitter::snapshot_transmitter_parts_bytes;
use crate::replication::snapshot_transmitter_handle::SnapshotTransmitterHandle;
use crate::replication::stream_parts_bytes;
use crate::runtime::RaftRuntime;
use crate::storage::IOFlushed;
use crate::storage::RaftLogStorage;
use crate::storage::RaftStateMachine;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::BatchOf;
use crate::type_config::alias::ChangeMembershipErrorOf;
use crate::type_config::alias::CommittedLeaderIdOf;
use crate::type_config::alias::CommittedVoteOf;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::MembershipStateOf;
use crate::type_config::alias::MpscReceiverOf;
use crate::type_config::alias::MpscSenderOf;
use crate::type_config::alias::OneshotReceiverOf;
use crate::type_config::alias::PayloadOf;
use crate::type_config::alias::VoteOf;
use crate::type_config::alias::WatchReceiverOf;
use crate::type_config::async_runtime::mpsc::MpscSender;
use crate::vote::RaftLeaderId;
use crate::vote::RaftVote;
use crate::vote::raft_vote::RaftVoteExt;
use crate::vote::vote_status::VoteStatus;

/// The result of applying log entries to state machine.
pub(crate) struct ApplyResult<C: RaftTypeConfig> {
    pub(crate) since: u64,
    pub(crate) end: u64,
    pub(crate) last_applied: LogIdOf<C>,
}

impl<C: RaftTypeConfig> Debug for ApplyResult<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplyResult")
            .field("since", &self.since)
            .field("end", &self.end)
            .field("last_applied", &self.last_applied)
            .finish()
    }
}

impl<C: RaftTypeConfig> fmt::Display for ApplyResult<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ApplyResult([{}, {}), last_applied={})",
            self.since, self.end, self.last_applied,
        )
    }
}

fn apply_membership_to_payload<C>(
    membership_state: &MembershipStateOf<C>,
    changes: ChangeMembers<C::NodeId, C::Node>,
    retain: bool,
    payloads: MembershipPayloads<C>,
) -> Result<C::Payload, ChangeMembershipErrorOf<C>>
where
    C: RaftTypeConfig,
{
    let membership = membership_state.next_membership(changes, retain)?;
    let payload = payloads.select(membership)?;
    Ok(payload)
}

/// The core type implementing the Raft protocol.
pub struct RaftCore<C, NF, LS, SM>
where
    C: RaftTypeConfig,
    NF: RaftNetworkFactory<C>,
    NF::Network: NetSnapshot<C, SnapshotData = SM::SnapshotData>,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
{
    /// This node's ID.
    pub(crate) id: C::NodeId,

    /// This node's runtime config.
    pub(crate) config: Arc<Config>,

    pub(crate) runtime_config: Shared<RuntimeConfig<C>>,

    /// Additional state that does not directly affect the consensus.
    pub(crate) core_state: CoreState<C>,

    /// The `RaftNetworkFactory` implementation, used only by this core: held inline, it is
    /// borrowed while a client is made, and allocates no lock or block of its own.
    pub(crate) network_factory: NF,

    /// The [`RaftLogStorage`] implementation.
    pub(crate) log_store: LS,

    /// A controlling handle to the [`RaftStateMachine`] worker.
    ///
    /// [`RaftStateMachine`]: `crate::storage::RaftStateMachine`
    pub(crate) sm_handle: sm::handle::Handle<C, SM>,

    pub(crate) engine: Engine<C, SM>,

    /// Responders to send result back to client when logs are applied.
    pub(crate) client_responders: ClientResponderQueue<CoreResponder<C>>,

    /// Linearizable reads waiting for the quorum acknowledgement clock to exceed their thresholds.
    pub(crate) pending_reads: PendingReadQueue<C>,

    /// Wakes this core when a pending linearizable read reaches its deadline.
    pub(crate) pending_read_deadline_notifier: PendingReadDeadlineNotifier<C>,

    /// A mapping of node IDs the replication state of the target node.
    pub(crate) replications: BTreeMap<C::NodeId, ReplicationHandle<C>>,

    /// The nodes of `replications`, charged before the map is built for at most
    /// `replications_room` streams and held, declared after it, until they are freed.
    pub(crate) replications_funding: Option<Charge>,

    /// How many streams `replications_funding` was charged for.
    pub(crate) replications_room: usize,

    pub(crate) heartbeat_handle: HeartbeatWorkersHandle<C>,

    #[allow(dead_code)]
    pub(crate) tx_api: MpscSenderOf<C, RaftMsg<C>>,
    pub(crate) rx_api: BatchRaftMsgReceiver<C>,

    /// A self-held keepalive sender for the dedicated install-snapshot channel.
    ///
    /// Mirrors [`Self::tx_api`]: it keeps `rx_install_snapshot` open after the application drops
    /// its last `Raft` handle, so the core shuts down only via `rx_shutdown` instead of
    /// truncating an in-flight `stream_append`.
    #[allow(dead_code)]
    pub(crate) tx_install_snapshot: MpscSenderOf<C, InstallFullSnapshotRequest<C, SM>>,

    /// Receiver of the dedicated channel that delivers a full snapshot to install.
    ///
    /// The snapshot data type is defined by the state machine, thus it does not go through
    /// `rx_api`, which is independent of the state machine type.
    pub(crate) rx_install_snapshot: MpscReceiverOf<C, InstallFullSnapshotRequest<C, SM>>,

    /// A Sender to send callback by other components to [`RaftCore`], when an action is finished,
    /// such as flushing log to disk, or applying log entries to state machine.
    pub(crate) tx_notification: MpscSenderOf<C, Notification<C>>,

    /// A Receiver to receive callback from other components.
    pub(crate) rx_notification: PeekableReceiver<MpscReceiverOf<C, Notification<C>>, Notification<C>>,

    /// The watch channels that broadcast local IO progress to storage callbacks and replication.
    pub(crate) io_broadcast: IoBroadcast<C>,

    /// The watch channels that publish this node's state for observers.
    pub(crate) metrics: MetricsChannels<C>,

    /// Runtime statistics for Raft operations, recorded only if the `runtime-stats` feature can
    /// read them.
    ///
    /// Owned directly by RaftCore for lock-free access to most stats.
    /// Only `replicate_batch` is shared with replication tasks via `shared_replicate_batch`.
    pub(crate) runtime_stats: Recorder<C>,

    /// Shared histogram for replication batch sizes.
    ///
    /// This is the only stats field that needs to be shared with replication tasks.
    /// All other stats are updated only by RaftCore.
    pub(crate) shared_replicate_batch: SharedReplicateBatch,

    /// External metrics recorder for exporting metrics to custom backends.
    ///
    /// Defaults to `None`. Applications can install a custom recorder
    /// via [`Raft::set_metrics_recorder`] to collect metrics.
    ///
    /// [`Raft::set_metrics_recorder`]: crate::Raft::set_metrics_recorder
    pub(crate) metrics_recorder: Option<Arc<dyn MetricsRecorder>>,

    pub(crate) span: Span,
}

/// Selects whether [`RaftCore::spawn_parallel_vote_requests`] sends real Vote or Pre-Vote RPCs.
#[derive(Clone, Copy)]
enum VoteRequestKind {
    Vote,
    PreVote,
}

impl VoteRequestKind {
    /// Lowercase label used in log messages: `"vote"` or `"pre-vote"`.
    fn as_str(self) -> &'static str {
        match self {
            VoteRequestKind::Vote => "vote",
            VoteRequestKind::PreVote => "pre-vote",
        }
    }
}

impl<C, NF, LS, SM> RaftCore<C, NF, LS, SM>
where
    C: RaftTypeConfig,
    NF: RaftNetworkFactory<C>,
    NF::Network: NetSnapshot<C, SnapshotData = SM::SnapshotData>,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
{
    /// The main loop of the Raft protocol.
    pub(crate) async fn main(
        mut self: Box<Self>,
        rx_shutdown: OneshotReceiverOf<C, ()>,
    ) -> Result<Infallible, Fatal<C>> {
        let span = tracing::span!(parent: &self.span, Level::DEBUG, "main");
        let res = self.do_main(rx_shutdown).instrument(span).await;

        // Flush buffered metrics
        self.flush_metrics();

        // Safe unwrap: res is Result<Infallible, _>
        let err = res.unwrap_err();
        match err {
            Fatal::Stopped => { /* Normal quit */ }
            _ => {
                tracing::error!("RaftCore::main error: {}", err);
            }
        }

        tracing::debug!("update metrics for shutdown");
        {
            let mut curr = self.metrics.all.borrow_watched().clone();
            curr.state = ServerState::Shutdown;
            curr.running_state = Err(err.clone());

            self.metrics.all.send(curr).ok();
        }

        tracing::info!("RaftCore shutdown complete");

        Err(err)
    }

    #[tracing::instrument(level = "trace", skip_all, fields(id=display(&self.id), cluster=%self.config.cluster_name
    ))]
    async fn do_main(&mut self, mut rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<Infallible, Fatal<C>> {
        tracing::debug!("raft node is initializing");

        self.wait_for_event_room(&mut rx_shutdown).await?;
        self.engine.startup();
        // It may not finish running all the commands, if there is a command waiting for a callback.
        self.run_engine_commands().await?;

        // Initialize metrics.
        self.flush_metrics();

        self.runtime_loop(rx_shutdown).await
    }

    fn ensure_preconditions_satisfied(
        &self,
        preconditions: BatchOf<C, Precondition<C>>,
    ) -> Result<(), PreconditionFailed<C>> {
        for c in preconditions {
            c.ensure_satisfied(&self.engine.state)?;
        }

        Ok(())
    }

    /// Handle `is_leader` requests.
    ///
    /// Send heartbeat to all voters. We respond once we have
    /// a quorum of agreement.
    ///
    /// Why:
    /// To ensure linearizability, a read request proposed at time `T1` confirms this node's
    /// leadership to guarantee that all the committed entries proposed before `T1` are present in
    /// this node.
    ///
    /// Both the fast path and the queue require `now < acked + age`; an acknowledgement at the
    /// exact threshold is not fresh enough. The queue also accepts an acknowledgement at the
    /// threshold time when it is for a heartbeat round started after the read arrived; see
    /// [`SendStamp`].
    ///
    /// Broadcasting a heartbeat per read does not amplify RPCs: heartbeat events reach each target
    /// through a watch channel, so a burst coalesces to the latest event, and a heartbeat sent
    /// later also satisfies the thresholds of the reads queued before it.
    #[tracing::instrument(level = "trace", skip(self, tx))]
    pub(super) fn handle_get_linearizer(&mut self, linearizer_option: LinearizerOption, tx: ClientReadTx<C>) {
        // Setup sentinel values to track when we've received majority confirmation of leadership.

        let now = C::now();
        let leader_lease = self.engine.config.timer_config.leader_lease;
        let max_quorum_ack_age = linearizer_option.effective_max_quorum_ack_age(leader_lease);
        let applied = self.engine.state.io_applied().cloned();
        let id = self.id.clone();

        let mut lh = match self.ensure_leader_handler() {
            Ok(leading_handler) => leading_handler,
            Err(forward) => {
                tx.send(Err(forward.into())).ok();
                return;
            }
        };

        let read_log_id = lh.get_read_log_id();

        let linearizer = Linearizer::new(id, read_log_id, applied);

        if lh.leader.is_self_quorum() {
            tx.send(Ok(linearizer)).ok();
            return;
        }

        let last_quorum_acked_at = lh.leader.last_quorum_acked_time();

        // This comparison must remain strict. A zero `max_quorum_ack_age` selects ReadIndex,
        // which requires a fresh heartbeat round to be acknowledged by a quorum. It must not reuse
        // a recorded acknowledgement, even when its timestamp equals `now`; using `<=` would take
        // this fast path and skip the new quorum confirmation.
        if last_quorum_acked_at.is_some_and(|last_quorum_acked_at| now < last_quorum_acked_at + max_quorum_ack_age) {
            tx.send(Ok(linearizer)).ok();
            return;
        }

        // A heartbeat broadcast after this read has a higher round, so its acknowledgement exceeds
        // this threshold even if the clock has not moved since `now`.
        let heartbeat_round = lh.leader.heartbeat_round();
        let min_quorum_acked_at = SendStamp::new(now - max_quorum_ack_age, heartbeat_round);
        let wait_timeout = linearizer_option.effective_wait_timeout(leader_lease);

        // A read that will not wait cannot benefit from a newer quorum acknowledgement.
        if wait_timeout.is_zero() {
            let quorum_not_enough = lh.leader.clock_quorum_not_enough(min_quorum_acked_at);
            let err = LinearizableReadError::QuorumNotEnough(quorum_not_enough);
            tx.send(Err(err)).ok();
            return;
        }

        // What the read takes of the queue is charged before anything is done for it: refused, it
        // is answered `Unfunded`, neither queued nor heartbeat sent for.
        let funding = match PendingReadQueue::<C>::charge(&lh.config.funding) {
            Ok(funding) => funding,
            Err(unfunded) => {
                tx.send(Err(LinearizableReadError::Unfunded(unfunded))).ok();
                return;
            }
        };

        if linearizer_option.heartbeat_if_quorum_ack_stale {
            lh.send_heartbeat(true);
        }

        let deadline = now + wait_timeout;
        let pending_read = PendingRead::new(deadline, linearizer, tx, funding);
        self.pending_reads.push(min_quorum_acked_at, pending_read);
        self.reschedule_pending_read_check();
    }

    /// Submit change-membership by writing a Membership log entry.
    ///
    /// If `retain` is `true`, removed `voter` will becomes `learner`. Otherwise they will
    /// be just removed.
    ///
    /// Changing membership includes changing voters config or adding/removing learners:
    ///
    /// - To change voters config, it will build a new **joint** config. If it already a joint
    ///   config, it returns the final uniform config.
    /// - Adding a learner does not affect election, thus it does not need to enter joint consensus.
    ///   But it still has to wait for the previous membership to commit. Otherwise a second
    ///   proposed membership implies the previous one is committed.
    // ---
    // TODO: This limit can be removed if membership_state is replaced by a list of membership logs.
    //       Because allowing this requires the engine to be able to store more than 2
    //       membership logs. And it does not need to wait for the previous membership log to commit
    //       to propose the new membership log.
    #[tracing::instrument(level = "debug", skip(self, payloads, tx, preconditions))]
    pub(super) fn change_membership(
        &mut self,
        changes: ChangeMembers<C::NodeId, C::Node>,
        retain: bool,
        preconditions: BatchOf<C, Precondition<C>>,
        payloads: MembershipPayloads<C>,
        tx: ProgressResponder<C, ClientWriteResult<C>>,
    ) {
        if let Err(e) = self.ensure_leader_handler() {
            tx.on_complete(Err(ClientWriteError::ForwardToLeader(e)));
            return;
        }

        if let Err(e) = self.ensure_preconditions_satisfied(preconditions) {
            tx.on_complete(Err(e.into()));
            return;
        }

        let res = apply_membership_to_payload::<C>(&self.engine.state.membership_state, changes, retain, payloads);
        let payload = match res {
            Ok(x) => x,
            Err(e) => {
                tx.on_complete(Err(ClientWriteError::ChangeMembershipError(e)));
                return;
            }
        };

        self.write_entries(
            Batch::of([payload]),
            Batch::of([Some(CoreResponder::Progress(tx))]),
            #[cfg(feature = "runtime-stats")]
            C::now(),
        );
    }

    /// Append a caller-built membership as one log entry, with no intermediate joint membership.
    ///
    /// `payload` already carries the proposed membership, bound by
    /// [`RaftPayload::bind_membership()`]. That payload is the only source of truth here: the
    /// membership this method validates is read back out of it, and the entry written to the log
    /// is that same payload.
    ///
    /// [`RaftPayload::bind_membership()`]: crate::entry::RaftPayload::bind_membership
    #[tracing::instrument(level = "debug", skip(self, payload, tx, preconditions))]
    pub(super) fn append_membership(
        &mut self,
        payload: C::Payload,
        preconditions: BatchOf<C, Precondition<C>>,
        tx: ProgressResponder<C, ClientWriteResult<C>>,
    ) {
        let lh = match Self::writable_leader_handler(&mut self.engine) {
            Ok(lh) => lh,
            Err(forward) => {
                tx.on_complete(Err(ClientWriteError::ForwardToLeader(forward)));
                return;
            }
        };

        // Read the barrier while the leader handler is still borrowed, but report it only after
        // the preconditions: every `Precondition` is checked before anything else is validated,
        // so a caller holding a stale membership log id learns that instead of being told to
        // retry.
        let leader_log_committed = Self::ensure_leader_log_committed(lh.state, lh.leader);

        if let Err(e) = self.ensure_preconditions_satisfied(preconditions) {
            tx.on_complete(Err(e.into()));
            return;
        }

        if let Err(e) = leader_log_committed {
            tx.on_complete(Err(ClientWriteError::ChangeMembershipError(e.into())));
            return;
        }

        // Safe unwrap(): the caller bound `membership` into this payload with
        // `RaftPayload::bind_membership()`, whose contract is that `get_membership()` returns it.
        let membership = payload.get_membership().unwrap();

        let membership_state = &self.engine.state.membership_state;
        if let Err(e) = membership_state.validate_append_membership(&membership) {
            tx.on_complete(Err(ClientWriteError::ChangeMembershipError(e)));
            return;
        }

        self.write_entries(
            Batch::of([payload]),
            Batch::of([Some(CoreResponder::Progress(tx))]),
            #[cfg(feature = "runtime-stats")]
            C::now(),
        );
    }

    /// Ensure the leader has committed a log entry proposed in its own term.
    ///
    /// A direct membership append needs this barrier on top of a valid leader lease. Without it,
    /// two configurations proposed in different terms from the same committed parent can both
    /// become committed, even when their quorums do not intersect. See [`UncommittedLeaderLog`]
    /// for a run that loses a committed membership this way.
    ///
    /// A valid lease does not imply the barrier. The lease reads the quorum acknowledgement clock,
    /// which the heartbeat worker advances even when a follower reports a log conflict, so the
    /// lease proves a quorum still sees this leader but not that a quorum stored the blank log.
    ///
    /// [`Raft::change_membership()`] deliberately does not take this barrier: its two proposals
    /// share an exact constituent voter set, so every pair of quorums intersects through the two
    /// strict majorities of that set.
    ///
    /// [`Raft::change_membership()`]: crate::raft::Raft::change_membership
    fn ensure_leader_log_committed(
        state: &RaftState<C>,
        leader: &Leader<C, LeaderQuorumSet<C>>,
    ) -> Result<(), UncommittedLeaderLog<CommittedLeaderIdOf<C>>> {
        let noop_log_id = leader.noop_log_id();
        let cluster_committed = state.cluster_committed();

        if cluster_committed >= Some(noop_log_id) {
            return Ok(());
        }

        Err(UncommittedLeaderLog {
            committed: cluster_committed.cloned(),
            leader_log_id: noop_log_id.clone(),
        })
    }

    /// Ensure this node is the leader and is not transferring leadership.
    ///
    /// Returns `Err(ForwardToLeader)` if:
    /// - This node is not the leader
    /// - The leader is transferring leadership to another node
    fn ensure_leader_handler(&mut self) -> Result<LeaderHandler<'_, C, SM>, ForwardToLeader<C>> {
        Self::leader_handler_of(&mut self.engine)
    }

    /// [`ensure_leader_handler`](Self::ensure_leader_handler), borrowing only `engine`.
    fn leader_handler_of(engine: &mut Engine<C, SM>) -> Result<LeaderHandler<'_, C, SM>, ForwardToLeader<C>> {
        let lh = engine.try_leader_handler()?;

        // If the leader is transferring leadership, forward requests to the new leader.
        if let Some(to) = lh.leader.get_transfer_to() {
            let forward = lh.state.new_forward_to_leader(to.clone(), &lh.config.funding);
            return Err(forward.with_reason(ForwardReason::LeadershipTransfer));
        }

        Ok(lh)
    }

    /// Ensure this node has a valid leader lease and may accept new writes. Borrows only
    /// `engine`, so that the core's other state stays free beside the handler.
    fn writable_leader_handler(engine: &mut Engine<C, SM>) -> Result<LeaderHandler<'_, C, SM>, ForwardToLeader<C>> {
        let lh = Self::leader_handler_of(engine)?;

        if !lh.is_lease_valid() {
            return Err(ForwardToLeader::empty().with_reason(ForwardReason::LeaseExpired));
        }

        Ok(lh)
    }

    /// Write log entries to the cluster through raft protocol.
    ///
    /// I.e.: append the log entries to local store, forward them to a quorum(including the
    /// leader), waiting for them to be committed and applied.
    ///
    /// Returns the log IDs assigned to the entries, or `None` if the entries could not be
    /// written (e.g., this node is not the leader).
    ///
    /// The result of applying each entry to state machine is sent to its corresponding responder,
    /// if provided. The calling side may not receive a result if raft is shut down.
    ///
    /// The responder is either Responder type of [`RaftTypeConfig::Responder`]
    /// (application-defined) or [`ProgressResponder`] (general-purpose); the former is for
    /// application-defined entries like user data, the latter is for membership configuration
    /// changes.
    #[tracing::instrument(level = "debug", skip_all, fields(id = display(&self.id)))]
    pub fn write_entries(
        &mut self,
        payloads: BatchOf<C, PayloadOf<C>>,
        responders: BatchOf<C, Option<CoreResponder<C>>>,
        #[cfg(feature = "runtime-stats")] proposed_at: InstantOf<C>,
    ) -> Option<LeaderLogIds<CommittedLeaderIdOf<C>>> {
        debug_assert_eq!(
            payloads.len(),
            responders.len(),
            "payloads and responders must have same length"
        );

        tracing::debug!("write {} entries", payloads.len());

        let mut lh = match Self::writable_leader_handler(&mut self.engine) {
            Ok(lh) => lh,
            Err(forward_err) => {
                let err = ClientWriteError::ForwardToLeader(forward_err);
                for tx in responders.into_iter().flatten() {
                    tx.on_complete(Err(err.clone()))
                }
                return None;
            }
        };

        // Charged before the entries are appended: refused, none of them is written.
        let funded = match lh.fund_append(payloads.as_ref()) {
            Ok(funded) => funded,
            Err(Unfunded) => {
                let err = ClientWriteError::ChangeMembershipError(MembershipRefused.into());
                for tx in responders.into_iter().flatten() {
                    tx.on_complete(Err(err.clone()))
                }
                return None;
            }
        };

        // Room for the responders, charged before any entry is appended: refused, none is.
        let waiting = responders.as_ref().iter().filter(|tx| tx.is_some()).count();
        if let Err(unfunded) = self.client_responders.reserve(waiting, &lh.config.funding) {
            for tx in responders.into_iter().flatten() {
                tx.on_complete(Err(ClientWriteError::Unfunded(unfunded)))
            }
            return None;
        }

        let entry_count = payloads.len() as u64;
        let log_ids = lh.leader_append_entries(payloads, funded)?;

        #[cfg(feature = "runtime-stats")]
        {
            let right = log_ids.last_ref().index() + 1;
            self.runtime_stats.record_log_stage(Stage::Proposed, right, proposed_at);
            self.runtime_stats.record_log_stage_now(Stage::Received, right);
        }

        // Record write batch size to external metrics recorder
        if let Some(r) = &self.metrics_recorder {
            r.record_write_batch(entry_count);
        }

        for (log_id, resp_tx) in log_ids.clone().into_iter().zip(responders) {
            if let Some(tx) = resp_tx {
                let index = log_id.index();
                tracing::debug!("write entries: push tx to responders, log_id: {}", log_id);
                self.client_responders.push(index, tx);
            }
        }

        Some(log_ids)
    }

    /// Send a heartbeat message to every follower/learners.
    #[tracing::instrument(level = "debug", skip_all, fields(id = display(&self.id)))]
    pub(crate) fn send_heartbeat(&mut self, emitter: impl fmt::Display) -> bool {
        tracing::debug!("send heartbeat, now: {}", C::now().display());

        let Some(mut lh) = self.engine.try_leader_handler().ok() else {
            tracing::debug!(
                "{} failed to send heartbeat, not a Leader: now: {}",
                emitter,
                C::now().display()
            );
            return false;
        };

        if lh.leader.get_transfer_to().is_some() {
            tracing::debug!(
                "{} is transferring leadership, skip sending heartbeat: now: {}",
                emitter,
                C::now().display()
            );
            return false;
        }

        if !lh.send_heartbeat(false) {
            return false;
        }

        // Record heartbeat to external metrics recorder
        if let Some(r) = &self.metrics_recorder {
            r.increment_heartbeat();
        }

        tracing::debug!("{} triggered sending heartbeat", emitter);
        true
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn flush_metrics(&mut self) {
        let io_state = self.engine.state.io_state();
        self.metrics.progress.send_log_progress(io_state.log_progress.flushed().cloned());
        self.metrics.progress.send_commit_progress(io_state.apply_progress.accepted().cloned());
        self.metrics.progress.send_apply_progress(io_state.apply_progress.flushed().cloned());
        self.metrics.progress.send_snapshot_progress(io_state.snapshot.flushed().cloned());

        let (replication, heartbeat) = if let Some(leader) = self.engine.leader.as_ref() {
            let replication_prog = &leader.progress;
            let replication = Some(replication_prog.collect_mapped(|item| item.id_progress_owned()));

            let clock_prog = &leader.clock_progress;
            let heartbeat = Some(clock_prog.collect_mapped(|item| {
                let sending_time = item.val.map(|stamp| stamp.time);
                (item.id.clone(), sending_time.map(SerdeInstant::new))
            }));

            (replication, heartbeat)
        } else {
            (None, None)
        };

        self.report_metrics(replication, heartbeat);
    }

    /// Report a metrics payload on the current state of the Raft node.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn report_metrics(
        &mut self,
        replication: Option<ReplicationMetrics<C>>,
        heartbeat: Option<HeartbeatMetrics<C>>,
    ) {
        let last_quorum_acked = self.last_quorum_acked_time();
        let is_self_quorum = self.engine.leader.as_ref().is_some_and(|leader| leader.is_self_quorum());
        let millis_since_quorum_ack = if is_self_quorum {
            Some(0)
        } else {
            last_quorum_acked.map(|t| t.elapsed().as_millis() as u64)
        };

        let st = &self.engine.state;

        // Get the last flushed vote, or use initial vote (term=0, node_id=self.id)
        // if no IO has been flushed yet (e.g., during startup).
        let vote = st
            .log_progress()
            .flushed()
            .map(|io_id| io_id.to_app_vote())
            .unwrap_or_else(|| VoteOf::<C>::new_with_default_term(self.id.clone()));

        #[allow(deprecated)]
        let data_metrics = RaftDataMetrics {
            last_log: st.last_log_id().cloned(),
            local_committed: st.local_committed().cloned(),
            committed: st.local_committed().cloned(),
            cluster_committed: st.cluster_committed().cloned(),
            last_applied: st.io_applied().cloned(),
            snapshot: st.io_snapshot_last_log_id().cloned(),
            purged: st.io_purged().cloned(),

            #[cfg(feature = "metrics-logids")]
            log_id_list: st.log_ids.clone(),

            millis_since_quorum_ack,
            last_quorum_acked: last_quorum_acked.map(SerdeInstant::new),
            replication,
            heartbeat,
        };

        let server_metrics = RaftServerMetrics::<C> {
            id: self.id.clone(),
            vote,
            state: st.server_state,
            current_leader: self.current_leader(),
            membership_config: st.membership_state.effective().clone(),
            committed_membership_config: st.membership_state.committed().clone(),
        };

        // `RaftMetrics` is the union of the two above, plus the running state and the term.
        // It is assembled from them rather than re-read from the state, so that every field has
        // exactly one place where it is derived.
        #[allow(deprecated)]
        let m = RaftMetrics {
            running_state: Ok(()),
            id: server_metrics.id.clone(),

            // --- data ---
            current_term: st.vote_ref().term(),
            vote: server_metrics.vote.clone(),
            last_log_index: data_metrics.last_log.index(),
            local_committed: data_metrics.local_committed.clone(),
            committed: data_metrics.committed.clone(),
            cluster_committed: data_metrics.cluster_committed.clone(),
            last_applied: data_metrics.last_applied.clone(),
            snapshot: data_metrics.snapshot.clone(),
            purged: data_metrics.purged.clone(),

            #[cfg(feature = "metrics-logids")]
            log_id_list: data_metrics.log_id_list.clone(),

            // --- cluster ---
            state: server_metrics.state,
            current_leader: server_metrics.current_leader.clone(),
            millis_since_quorum_ack: data_metrics.millis_since_quorum_ack,
            last_quorum_acked: data_metrics.last_quorum_acked,
            membership_config: server_metrics.membership_config.clone(),
            committed_membership_config: server_metrics.committed_membership_config.clone(),
            heartbeat: data_metrics.heartbeat.clone(),

            // --- replication ---
            replication: data_metrics.replication.clone(),
        };

        // Record to external metrics recorder
        if let Some(r) = &self.metrics_recorder {
            crate::metrics::forward_metrics(&m, r.as_ref());
        }

        // Start to send metrics
        // `RaftMetrics` is sent last, because `Wait` only examines `RaftMetrics`
        // but not `RaftDataMetrics` and `RaftServerMetrics`.
        // Thus if `RaftMetrics` change is perceived, the other two should have been updated.

        self.metrics.data.send_if_modified(|metrics| {
            if data_metrics.ne(metrics) {
                *metrics = data_metrics.clone();
                return true;
            }
            false
        });

        self.metrics.server.send_if_modified(|metrics| {
            if server_metrics.ne(metrics) {
                *metrics = server_metrics.clone();
                return true;
            }
            false
        });

        tracing::debug!("report metrics: {}", m);
        let res = self.metrics.all.send(m);

        if let Err(err) = res {
            tracing::error!("failed to report metrics, error: {}, id: {}", err, &self.id);
        }
    }

    /// Handle the admin command `initialize`.
    ///
    /// It is allowed to initialize only when `last_log_id.is_none()` and `vote==(0,0)`.
    /// See: [Conditions for initialization][precondition]
    ///
    /// [precondition]: crate::docs::cluster_control::cluster_formation#preconditions-for-initialization
    #[tracing::instrument(level = "debug", skip(self, tx))]
    pub(crate) fn handle_initialize(
        &mut self,
        member_nodes: BTreeMap<C::NodeId, C::Node>,
        payload: C::Payload,
        tx: ResultSender<C, (), InitializeError<C>>,
    ) {
        tracing::debug!("{}: member_nodes: {:?}", func_name!(), member_nodes);

        let membership = Membership::from(member_nodes);

        let res = self.engine.initialize(payload, membership);

        let has_error = res.is_err();

        // If there is an error, respond at once.
        // Otherwise, wait for the initialization log to be applied to state machine.
        let condition = if has_error {
            None
        } else {
            // Wait for the initialization log to be flushed, not applied.
            //
            // Because committing a log entry requires a leader, the first leader may or may not be able to
            // established, for example, there are already other nodes in the cluster with more logs.
            //
            // Thus, initialization should never wait for to apply of the initialization log.
            //
            // When adding new learners after initialization, we should not wait for the log to be applied.
            // But this introduces an issue, if the client send a change membership at once after
            // initialization, it may receive a InProgress error:
            // `"InProgress": { "committed": null, "membership_log_id": { "leader_id": { "term": 0,
            //             "node_id": 0 }, "index": 0 } }`
            // TODO: change-membership should check leadership or wait for leader to establish?

            // Wait for the generated IO to be flushed before respond.
            let accepted = self.engine.state.io_state().log_progress.accepted().cloned();
            accepted.map(|io_id| Condition::IOFlushed { io_id })
        };
        self.engine.output.push_command(Command::Respond {
            when: condition,
            resp: Respond::new(res, tx),
        });

        if !has_error {
            // With the new config, start to elect to become leader
            self.engine.elect();
        }
    }

    /// Trigger a snapshot building(log compaction) job if there is no pending building job.
    ///
    /// Returns `true` if a build was queued, `false` if one is already in progress and the request
    /// was dropped.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn trigger_snapshot(&mut self) -> bool {
        tracing::debug!("{}", func_name!());
        self.engine.snapshot_handler().trigger_snapshot()
    }

    /// Reset the backoff of the replication streams to `to`, or of every stream if `to` is empty.
    pub(crate) fn trigger_reset_backoff(&mut self, to: BatchOf<C, C::NodeId>) {
        if to.is_empty() {
            for (_node_id, repl_handle) in self.replications.iter() {
                repl_handle.reset_backoff();
            }
            return;
        }

        for node_id in to {
            if let Some(repl_handle) = self.replications.get(&node_id) {
                repl_handle.reset_backoff();
            }
        }
    }

    /// Trigger routine actions that need to be checked after processing messages.
    ///
    /// This is called in the main event loop after processing messages and running engine commands.
    /// It performs routine checks and triggers corresponding actions:
    /// - Snapshot building based on `SnapshotPolicy`
    /// - Initiate replication if the replication stream is idle (for leader)
    ///
    /// Unlike tick-based triggers, this runs after every message batch, making it independent of
    /// the tick configuration and more responsive to state changes.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn trigger_routine_actions(&mut self) {
        // Check snapshot policy and trigger snapshot if needed
        if let Some(at) = self
            .config
            .snapshot_policy
            .should_snapshot(&self.engine.state, self.core_state.snapshot_tried_at.as_ref())
        {
            tracing::debug!("snapshot policy triggered at: {}", at);
            // Only record the attempt if a build was actually queued. A trigger dropped because a
            // build is already in flight must not advance `snapshot_tried_at`; otherwise the phantom
            // attempt suppresses `should_snapshot` and the snapshot never re-arms once the in-flight
            // build completes. See https://github.com/databendlabs/openraft/issues/1829
            if self.trigger_snapshot() {
                self.core_state.snapshot_tried_at = Some(at);
            }
        }

        // Keep replicating to a target if the replication stream to it is idle
        if let Ok(mut lh) = self.engine.try_leader_handler() {
            lh.replication_handler().initiate_replication();
        }

        // Broadcast I/O progress so replication tasks can read submitted logs.
        if let Some(submitted) = self.engine.state.log_progress().submitted().cloned() {
            self.io_broadcast.submitted.send_if_greater(submitted);
        }

        self.process_pending_reads();
    }

    fn process_pending_reads(&mut self) {
        let now = C::now();
        let applied = self.engine.state.io_applied().cloned();

        let lh_res = self.engine.try_leader_handler();
        match lh_res {
            Ok(lh) => {
                // Quorum satisfaction takes precedence over a delayed timeout wake-up.
                let quorum_acked_at = lh.leader.last_quorum_acked_stamp();
                if let Some(quorum_acked_at) = quorum_acked_at {
                    self.pending_reads.drain_satisfied(quorum_acked_at, applied);
                }

                let leader = &*lh.leader;
                let make_error = |min_quorum_acked_at| {
                    let quorum_not_enough = leader.clock_quorum_not_enough(min_quorum_acked_at);
                    LinearizableReadError::QuorumNotEnough(quorum_not_enough)
                };

                self.pending_reads.drain_expired(now, make_error);
            }
            Err(forward) => {
                let err = LinearizableReadError::ForwardToLeader(forward);
                self.pending_reads.drain_all_with_error(err);
            }
        }

        self.reschedule_pending_read_check();
    }

    fn fail_pending_reads(&mut self) {
        let forward = self.engine.state.forward_to_leader(&self.engine.config.funding);
        let err = LinearizableReadError::ForwardToLeader(forward);
        self.pending_reads.drain_all_with_error(err);
        self.reschedule_pending_read_check();
    }

    fn reschedule_pending_read_check(&self) {
        let deadline = self.pending_reads.earliest_deadline();
        self.pending_read_deadline_notifier.set_deadline(deadline);
    }

    /// Return the current leader node ID based on the committed vote.
    ///
    /// In OpenRaft, a leader does not have to be a voter — it can be a learner
    /// or even a node outside the membership. Leadership is determined solely by
    /// a committed vote (i.e., a vote granted by a quorum), following Paxos
    /// semantics. Therefore, this method does not check voter or membership
    /// status.
    ///
    /// Currently, this situation arises when a membership change removes the
    /// leader from the voter set (or from the membership entirely). The leader
    /// continues to operate and commit logs until it steps down or a new leader
    /// is elected. In the future, OpenRaft will also allow a node that was never
    /// in the membership to become a leader.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) fn current_leader(&self) -> Option<C::NodeId> {
        tracing::debug!(
            "get current_leader: self_id: {}, vote: {}",
            self.id,
            self.engine.state.vote_ref()
        );

        let vote = self.engine.state.vote_ref();

        if !vote.is_committed() {
            return None;
        }

        Some(vote.to_leader_id().node_id().clone())
    }

    /// Retrieves the most recent timestamp that is acknowledged by a quorum.
    ///
    /// This function returns the latest known time at which the leader received acknowledgment
    /// from a quorum of followers, indicating its leadership is current and recognized.
    /// If the node is not a leader or no acknowledgment has been received, `None` is returned.
    fn last_quorum_acked_time(&self) -> Option<InstantOf<C>> {
        let leading = self.engine.leader.as_ref();
        leading.and_then(|l| l.last_quorum_acked_time())
    }

    /// The node of `leader_id`, if it is a member. A funded node names the leader by id only,
    /// and copies no node: the copy would own heap no charge was taken for.
    pub(crate) fn get_leader_node(&self, leader_id: Option<C::NodeId>) -> Option<C::Node> {
        let leader_id = leader_id?;
        if self.engine.config.funding.is_funded() {
            return None;
        }

        self.engine.state.membership_state.effective().get_node(&leader_id).cloned()
    }

    /// Apply log entries to the state machine, from the `first`(inclusive) to `last`(inclusive).
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn apply_to_state_machine(
        &mut self,
        first: LogIdOf<C>,
        last: LogIdOf<C>,
    ) -> Result<(), StorageError<C>> {
        tracing::debug!("{}: {}..={}", func_name!(), first, last);

        debug_assert!(
            first.index() <= last.index(),
            "first.index {} should <= last.index {}",
            first.index(),
            last.index()
        );

        #[cfg(debug_assertions)]
        if let Some(first_idx) = self.client_responders.first_index() {
            debug_assert!(
                first.index() <= first_idx,
                "first.index {} should <= client_resp_channels.first index {}",
                first.index(),
                first_idx,
            );
        }

        // Drain responders up to last.index, with the charge for the vector they are drained
        // into, which the state machine worker holds until that vector is freed.
        let (mut responders, slots) = self.client_responders.drain_upto(last.index());

        let entry_count = last.index() + 1 - first.index();
        self.runtime_stats.apply_batch.record(entry_count);

        // Record to external metrics recorder
        if let Some(r) = &self.metrics_recorder {
            r.record_apply_batch(entry_count);
        }

        // Call on_commit on each responder
        for (index, responder) in responders.iter_mut() {
            let log_id = self.engine.state.get_log_id(*index).unwrap();
            responder.on_commit(log_id);
        }

        let cmd = sm::Command::apply(first, last.clone(), responders, slots);
        self.sm_handle
            .send(cmd)
            .await
            .map_err(|_e| StorageError::apply(last, C::err_from_string("cannot send to sm::Worker")))?;

        Ok(())
    }

    /// A replication stream to `target` for `leader_vote`, the stream `stream_id` of its
    /// progress, replicating from `matching`, every part of it charged before any is made, as
    /// [`replication_stream_bytes`](crate::replication_stream_bytes) sizes it: its task, the
    /// channels it holds and the state its task holds, and what the network client it replicates
    /// with allocates ([`RaftNetworkFactory::new_client_bytes`]), held with its task. None, nothing
    /// made, if the charge is refused or `target` is no member of the effective membership.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn spawn_replication_stream(
        &mut self,
        leader_vote: CommittedVoteOf<C>,
        target: &C::NodeId,
        stream_id: StreamId,
        matching: Option<LogIdOf<C>>,
    ) -> Option<ReplicationHandle<C>> {
        let Some(node) = self.engine.state.membership_state.effective().get_node(target) else {
            tracing::debug!(
                "id={} target={}: no replication stream: no longer a member",
                self.id,
                target
            );
            return None;
        };

        let funding = &self.engine.config.funding;
        let factory = &self.network_factory;
        let (backoff, network_backoff) = Self::client_backoff(funding, factory, target, node);
        let parts = || {
            stream_parts_bytes::<C, NF, LS>()?
                .checked_add(factory.new_client_bytes(target, node)?)?
                .checked_add(backoff)
        };
        let mut task = match TaskCharge::<C, _>::take(funding, parts) {
            Ok(task) => task,
            Err(Unfunded) => {
                tracing::warn!(
                    "id={} target={}: no replication stream: it is unfunded",
                    self.id,
                    target
                );
                return None;
            }
        };

        // Each channel from its share of the charge taken for all of them: none is refused. The
        // rest is held with the task, and funds what the task holds.
        let (replicate_tx, replicate_rx) = task.watch(Replicate::default()).ok()?;
        let (backoff_reset_tx, backoff_reset_rx) = task.watch(()).ok()?;
        let (cancel_tx, cancel_rx) = task.watch(()).ok()?;

        let network = self.network_factory.new_client(target.clone(), node).await;

        let event_watcher = self.new_event_watcher(replicate_rx);
        let replication_context =
            self.new_replication_context(leader_vote, target, stream_id, cancel_rx, network_backoff);

        let progress = replication_progress::ReplicationProgress {
            local_committed: self.engine.state.local_committed().cloned(),
            remote_matched: matching,
        };

        let core = ReplicationCore::<C, NF, LS>::new(
            replication_context,
            progress,
            network,
            self.log_store.get_log_reader().await,
            event_watcher,
            backoff_reset_rx,
        );

        let span = tracing::span!(parent: &self.span, Level::DEBUG, "replication", id=display(&self.id), target=display(target));

        let join_handle = match task.spawn(core.main().instrument(span)) {
            Ok(join_handle) => join_handle,
            Err(Unfunded) => {
                tracing::warn!(
                    "id={} target={}: no replication stream: its task is unfunded",
                    self.id,
                    target
                );
                return None;
            }
        };

        Some(ReplicationHandle::new(
            stream_id,
            replicate_tx,
            cancel_tx,
            backoff_reset_tx,
            join_handle,
        ))
    }

    /// What a replication task to `target` at `node` is charged for the backoff its client gives,
    /// and whether it asks its client for one: on a node run with a funder, only if the factory
    /// tells what one allocates ([`RaftNetworkFactory::backoff_bytes`]), charged with the client;
    /// if not, it backs off by the config's policy, which allocates nothing.
    fn client_backoff(funding: &Funding, factory: &NF, target: &C::NodeId, node: &C::Node) -> (usize, bool) {
        if !funding.is_funded() {
            return (0, true);
        }
        match factory.backoff_bytes(target, node) {
            Some(bytes) => (bytes, true),
            None => (0, false),
        }
    }

    /// The context of a replication task to `target` for `leader_vote`, of the stream `stream_id`,
    /// stopped by `cancel_rx`, asking its client for a backoff if `network_backoff`.
    fn new_replication_context(
        &self,
        leader_vote: CommittedVoteOf<C>,
        target: &C::NodeId,
        stream_id: StreamId,
        cancel_rx: WatchReceiverOf<C, ()>,
        network_backoff: bool,
    ) -> ReplicationContext<C> {
        ReplicationContext {
            id: self.id.clone(),
            target: target.clone(),
            leader_vote,
            stream_id,
            config: self.config.clone(),
            tx_notify: self.tx_notification.clone(),
            cancel_rx,
            replicate_batch: self.shared_replicate_batch.clone(),
            network_backoff,
        }
    }

    fn new_event_watcher(&self, replicate_rx: WatchReceiverOf<C, Replicate<C>>) -> EventWatcher<C> {
        EventWatcher {
            replicate_rx,
            committed_rx: self.io_broadcast.committed.subscribe(),
            io_accepted_rx: self.io_broadcast.accepted.subscribe(),
            io_submitted_rx: self.io_broadcast.submitted.subscribe(),
        }
    }

    /// Run as many commands as possible.
    ///
    /// If there is a command that waits for a callback, just return and wait for
    /// next RaftMsg.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn run_engine_commands(&mut self) -> Result<(), StorageError<C>> {
        if tracing::enabled!(Level::DEBUG) {
            tracing::debug!("queued commands: start...");
            for c in self.engine.output.iter_commands() {
                tracing::debug!("queued commands: {:?}", c);
            }
            tracing::debug!("queued commands: end...");
        }

        self.send_satisfied_responds();

        loop {
            // Batch commands for better I/O performance (e.g., merge consecutive AppendEntries).
            // Not on a funded node: merging grows one command's entries into a buffer nothing
            // was charged for, so each command is appended with the entries it was funded with.
            if !self.engine.config.funding.is_funded() {
                self.engine.output.sched_commands(&self.config);
            }

            let Some(cmd) = self.engine.output.pop_command() else {
                break;
            };

            let res = self.run_command(cmd).await?;

            let Some(cmd) = res else {
                // cmd executed. Process next
                continue;
            };

            // cmd is returned, means it can not be executed now, postpone it.

            tracing::debug!(
                "RAFT_stats id={:<2}    cmd: postpone command: {}, pending: {}",
                self.id,
                cmd,
                self.engine.output.len()
            );

            if self.engine.output.postpone_command(cmd, &self.engine.config.funding).is_ok() {
                continue;
            }

            // cmd is put back to the front of the queue. quit the loop

            if tracing::enabled!(Level::DEBUG) {
                for c in self.engine.output.iter_commands().take(8) {
                    tracing::debug!("postponed, first 8 queued commands: {:?}", c);
                }
            }

            // A command must be postponed, but progress driven command may be ready to run.
            // Thus, we do not return, but find progress driven commands to run.
            break;
        }

        // Progress driven commands run at last because some command may generate progress changes.
        self.run_progress_driven_command().await?;

        // The queues these commands drained give back the charge of the slots they emptied.
        self.engine.shrink_queues();
        self.client_responders.shrink(&self.engine.config.funding);

        Ok(())
    }

    /// Run all commands that are automatically generated by progress changes.
    async fn run_progress_driven_command(&mut self) -> Result<(), StorageError<C>> {
        while let Some(cmd) = self.engine.next_progress_driven_command() {
            tracing::debug!("RAFT_event id={:<2}    progress_driven cmd: {}", self.id, cmd);

            // IO progress generated command is always ready to run. no need to postpone.
            let res: Option<Command<C, SM>> = self.run_command(cmd).await?;
            debug_assert!(res.is_none(), "progress driven command should always be executed");
        }

        Ok(())
    }

    /// Send responds whose waiting conditions are satisfied.
    ///
    /// Responds are queued when their waiting conditions (log flushed, applied, snapshot built)
    /// are not yet met. This method drains all responds whose conditions are now satisfied.
    pub(crate) fn send_satisfied_responds(&mut self) {
        let io_state = self.engine.state.io_state();

        tracing::debug!(
            "RAFT_stats id={:<2}    cmd: try send satisfied responds: log_io: {}, apply: {}, snapshot: {}",
            self.id,
            io_state.log_progress.flushed().display(),
            io_state.apply_progress.flushed().display(),
            io_state.snapshot.flushed().display(),
        );

        for (phase, respond) in self.engine.output.pending_responds.drain_satisfied(io_state) {
            tracing::debug!(
                "RAFT_stats id={:<2}    cmd: send respond waiting for {}: {}",
                self.id,
                phase,
                respond
            );
            respond.send();
        }
    }

    /// Run an event handling loop
    ///
    /// It always returns a [`Fatal`] error upon returning.
    #[tracing::instrument(level = "debug", skip_all, fields(id=display(&self.id)))]
    async fn runtime_loop(&mut self, mut rx_shutdown: OneshotReceiverOf<C, ()>) -> Result<Infallible, Fatal<C>> {
        // Ratio control the ratio of number of RaftMsg to process to number of Notification to process.
        let mut balancer = Balancer::new(10_000);

        loop {
            self.flush_metrics();

            tracing::debug!(
                "RAFT_stats id={:<2} log_io: {}",
                self.id,
                self.engine.state.log_progress()
            );

            // In each loop, it does not have to check rx_shutdown and flush metrics for every RaftMsg
            // processed.
            // In each loop, the first step is blocking waiting for any message from any channel.
            // Then if there is any message, process as many as possible to maximize throughput.

            // The event selected below is handled before any other is taken: its room is charged
            // first.
            self.wait_for_event_room(&mut rx_shutdown).await?;

            // Check shutdown first so that a message flood in `tx_api` won't block shutting down.
            let selected = C::select4(
                &mut rx_shutdown,
                self.rx_notification.recv(),
                self.rx_install_snapshot.recv(),
                self.rx_api.ensure_buffered(),
            )
            .await;
            match selected {
                Select4::First(_) => {
                    tracing::info!("recv from rx_shutdown");
                    return Err(Fatal::Stopped);
                }

                Select4::Second(notify_res) => {
                    match notify_res {
                        Some(notify) => self.handle_notification(notify)?,
                        None => {
                            tracing::error!("all rx_notify senders are dropped");
                            return Err(Fatal::Stopped);
                        }
                    };
                }

                Select4::Third(install_res) => {
                    match install_res {
                        Some(req) => self.handle_install_full_snapshot_request(req),
                        None => {
                            tracing::error!("all rx_install_snapshot senders are dropped");
                            return Err(Fatal::Stopped);
                        }
                    };
                }

                Select4::Fourth(msg_res) => {
                    msg_res?;
                }
            };

            self.run_engine_commands().await?;

            // There is a message waking up the loop, process channels one by one.

            let raft_msg_processed = self.process_raft_msg(balancer.raft_msg()).await?;
            let notify_processed = self.process_notification(balancer.notification()).await?;

            // If one of the channel consumed all its budget, re-balance the budget ratio.

            #[allow(clippy::collapsible_else_if)]
            if notify_processed == balancer.notification() {
                tracing::info!("there may be more Notification to process, increase Notification ratio");
                balancer.increase_notification();
            } else {
                if raft_msg_processed == balancer.raft_msg() {
                    tracing::info!("there may be more RaftMsg to process, increase RaftMsg ratio");
                    balancer.increase_raft_msg();
                }
            }

            // Trigger routine actions after processing all messages; refused their room, they are
            // tried again after the next event.
            if self.reserve_event_room().is_ok() {
                self.trigger_routine_actions();
            }

            self.run_engine_commands().await?;
        }
    }

    /// Room in the Engine's command queue for the commands one event queues
    /// ([`event_commands`]), charged before the event is taken: refused as [`Unfunded`], nothing
    /// is allocated or charged, and the event is left where it is.
    fn reserve_event_room(&mut self) -> Result<(), Unfunded> {
        let targets = self.engine.state.membership_state.effective().nodes().count();
        self.engine.output.reserve(&self.engine.config.funding, event_commands(targets))
    }

    /// Wait until room for one event's commands is charged ([`Self::reserve_event_room`]),
    /// trying again every tick, or until the core is shut down.
    async fn wait_for_event_room(&mut self, rx_shutdown: &mut OneshotReceiverOf<C, ()>) -> Result<(), Fatal<C>> {
        let tick = Duration::from_millis(self.config.tick_interval());
        while self.reserve_event_room().is_err() {
            tracing::debug!("room for an event's commands refused, trying again in {:?}", tick);
            if let Select2::First(_) = C::select2(&mut *rx_shutdown, C::sleep(tick)).await {
                tracing::info!("recv from rx_shutdown");
                return Err(Fatal::Stopped);
            }
        }
        Ok(())
    }

    /// Process RaftMsg up to the budget, yielding to ready notifications in eager mode.
    ///
    /// It returns the number of processed message.
    /// If the input channel is closed, it returns `Fatal::Stopped`.
    async fn process_raft_msg(&mut self, at_most: u64) -> Result<u64, Fatal<C>> {
        self.runtime_stats.raft_msg_budget.record(at_most);

        let mut processed = 0u64;
        let mut total = 0u64;
        // Being 0 disabled batch msg processing.
        // TODO: make it configurable
        let run_command_threshold = 0;
        let mut last_log_index = 0;

        for _i in 0..at_most {
            // Process at least one RaftMsg so notifications cannot starve API traffic.
            if total > 0 && self.config.broadcast_submitted_on_append() {
                match self.rx_notification.peek() {
                    Ok(_) => break,
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {
                        tracing::error!("rx_notify is disconnected, quit");
                        return Err(Fatal::Stopped);
                    }
                }
            }

            // Refused room for its commands, a message is left in the channel until room is charged.
            if self.reserve_event_room().is_err() {
                break;
            }

            let res = self.rx_api.try_recv().await?;
            let Some(msg) = res else {
                break;
            };

            self.handle_api_msg(msg);
            processed += 1;
            total += 1;

            let index = self.engine.state.last_log_id().next_index();

            if index.saturating_sub(last_log_index) >= run_command_threshold {
                // After handling all the inputs, batch run all the commands for better performance
                self.runtime_stats.raft_msg_per_run.record(processed);
                self.runtime_stats.raft_msg_usage_permille.record(processed * 1000 / at_most);
                self.run_engine_commands().await?;

                last_log_index = index;
                processed = 0;
            }
        }

        // After handling all the inputs, batch run all the commands for better performance
        self.runtime_stats.raft_msg_per_run.record(processed);
        self.runtime_stats.raft_msg_usage_permille.record(processed * 1000 / at_most);
        self.run_engine_commands().await?;

        if total == at_most {
            tracing::debug!("at_most({}) reached, there are more queued RaftMsg to process", at_most);
        }

        Ok(total)
    }

    /// Process Notification as many as possible.
    ///
    /// It returns the number of processed notifications.
    /// If the input channel is closed, it returns `Fatal::Stopped`.
    async fn process_notification(&mut self, at_most: u64) -> Result<u64, Fatal<C>> {
        self.runtime_stats.notification_budget.record(at_most);

        let mut processed = 0u64;

        for _i in 0..at_most {
            // Refused room for its commands, a notification is left in the channel until room is
            // charged.
            if self.reserve_event_room().is_err() {
                break;
            }

            let res = self.rx_notification.try_recv();
            let notify = match res {
                Ok(msg) => msg,
                Err(e) => match e {
                    TryRecvError::Empty => {
                        tracing::debug!("all Notification are processed, wait for more");
                        break;
                    }
                    TryRecvError::Disconnected => {
                        tracing::error!("rx_notify is disconnected, quit");
                        return Err(Fatal::Stopped);
                    }
                },
            };

            self.handle_notification(notify)?;
            processed += 1;
        }

        if processed > 0 {
            self.run_engine_commands().await?;
        }

        self.runtime_stats.notification_usage_permille.record(processed * 1000 / at_most);

        if processed == at_most {
            tracing::debug!(
                "at_most({}) reached, there are more queued Notification to process",
                at_most
            );
        }

        Ok(processed)
    }

    /// Spawn parallel Vote or Pre-Vote requests to all other cluster members, selected by `kind`.
    ///
    /// For a Pre-Vote, only an affirmative `Ok(granted)` response counts toward the quorum: a
    /// transport error — including [`Unreachable`](crate::error::Unreachable) from a genuinely
    /// partitioned peer — is **not** a grant, otherwise a fully isolated node would synthesize a
    /// quorum and inflate its term. A network that has not implemented `pre_vote` returns
    /// `Ok(granted)` from the default impl, keeping Pre-Vote a no-op during a rolling upgrade.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn spawn_parallel_vote_requests(&mut self, vote_req: &VoteRequest<C>, kind: VoteRequestKind) {
        let vote = vote_req.vote.clone();
        let id = self.id.clone();
        let tx = self.tx_notification.clone();
        let ttl = Duration::from_millis(self.config.election_timeout_min);

        self.broadcast_to_voters(ttl, |target, mut client, option| {
            let req = vote_req.clone();
            let vote = vote.clone();
            let id = id.clone();
            let tx = tx.clone();

            let span = match kind {
                VoteRequestKind::Vote => {
                    tracing::debug_span!(parent: &Span::current(), "send_vote_req", target = display(&target))
                }
                VoteRequestKind::PreVote => {
                    tracing::debug_span!(parent: &Span::current(), "send_pre_vote_req", target = display(&target))
                }
            };

            async move {
                let deadline = C::now() + ttl;
                let tm_res = match kind {
                    VoteRequestKind::Vote => within::<C, _>(deadline, client.vote(req, option)).await,
                    VoteRequestKind::PreVote => within::<C, _>(deadline, client.pre_vote(req, option)).await,
                };
                let res = match tm_res {
                    Ok(res) => res,

                    Err(Elapsed) => {
                        let timeout_err = Timeout::<C> {
                            action: RPCTypes::Vote,
                            id,
                            target: target.clone(),
                            timeout: ttl,
                        };
                        tracing::warn!("timeout while requesting {}: {}", kind.as_str(), timeout_err);
                        return;
                    }
                };

                match res {
                    Ok(resp) => {
                        let candidate_vote = vote.to_non_committed();
                        let notification = match kind {
                            VoteRequestKind::Vote => Notification::VoteResponse {
                                target,
                                resp,
                                candidate_vote,
                            },
                            VoteRequestKind::PreVote => Notification::PreVoteResponse {
                                target,
                                resp,
                                candidate_vote,
                            },
                        };
                        tx.send(notification).await.ok();
                    }
                    // A transport failure is not a grant: a partitioned peer must not count
                    // toward the (Pre-)Vote quorum. A network that has not implemented
                    // `pre_vote` returns `Ok(granted)` from the default impl, so Pre-Vote
                    // degrades to a no-op rather than relying on this.
                    Err(err) => {
                        tracing::warn!("while requesting {}, error: {}, target: {}", kind.as_str(), err, target)
                    }
                }
            }
            .instrument(span)
        })
        .await;
    }

    /// Tell every other voter to accept a new leader.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn broadcast_transfer_leader(&mut self, req: TransferLeaderRequest<C>) {
        let ttl = Duration::from_millis(self.config.election_timeout_min);

        self.broadcast_to_voters(ttl, |target, mut client, option| {
            let r = req.clone();

            let span = tracing::debug_span!(
                parent: &Span::current(),
                "send_transfer_leader",
                target = display(&target)
            );

            async move {
                let tm_res = within::<C, _>(C::now() + ttl, client.transfer_leader(r, option)).await;
                let res = match tm_res {
                    Ok(res) => res,
                    Err(elapsed) => {
                        tracing::warn!(
                            "timeout sending transfer_leader: {} after {:?}, target: {}",
                            elapsed,
                            ttl,
                            target
                        );
                        return;
                    }
                };

                match res {
                    Err(e) => {
                        tracing::warn!("error sending transfer_leader: {}, target: {}", e, target);
                    }
                    Ok(resp) => {
                        tracing::info!("Done transfer_leader sent to {}, resp: {:?}", target, resp);
                    }
                }
            }
            .instrument(span)
        })
        .await;
    }

    /// Send an RPC to every voter except this node, each in its own detached task.
    ///
    /// `make_rpc` is handed a fresh client for one target and the shared [`RPCOption`], and returns
    /// the future that talks to it. Nothing is joined, so each future has to report its own
    /// outcome; `ttl` is the timeout the caller is expected to enforce inside that future, by
    /// [`within`], on the one sleep each task is charged.
    ///
    /// On a funded node every task, the sleep it holds and what its client allocates
    /// ([`RaftNetworkFactory::new_client_bytes`]) are charged before any of them is made: refused,
    /// no request is sent, nothing allocated for any. The voters are read where the membership
    /// holds them, not collected.
    async fn broadcast_to_voters<F, Fut>(&mut self, ttl: Duration, make_rpc: F)
    where
        F: Fn(C::NodeId, NF::Network, RPCOption) -> Fut,
        Fut: Future<Output = ()> + OptionalSend + 'static,
    {
        // The fields used, borrowed apart: `engine` only shared, `network_factory` only mutably,
        // so the membership and the voter it yields stay borrowed from the engine while the
        // factory is awaited making a client. Held across those awaits, they are `Send` as
        // `Membership` is `Sync`: its ids, nodes and owner are.
        let Self {
            id,
            engine,
            network_factory,
            ..
        } = self;
        let id: &C::NodeId = id;
        let funding: &Funding = &engine.config.funding;
        let membership: &Membership<C::NodeId, C::Node> = engine.state.membership_state.effective().membership();

        let charge = match Self::charge_broadcast::<Fut>(funding, network_factory, id, membership) {
            Ok(charge) => charge,
            Err(Unfunded) => {
                tracing::warn!("id={}: no broadcast to the voters: it is unfunded", id);
                return;
            }
        };
        let mut prepaid: Prepaid<C, Unfunded> = Prepaid::new(charge, std::convert::identity);

        for (target, node) in membership.voters() {
            if target == id {
                continue;
            }
            // As charged for it; only a node without a funder, which charged nothing, is not told.
            let client_bytes = network_factory.new_client_bytes(target, node).unwrap_or(0);

            let client = network_factory.new_client(target.clone(), node).await;

            let fut: Fut = make_rpc(target.clone(), client, RPCOption::new(ttl));

            // From its share of the charge taken for all of them, held by the task with the client
            // its future holds: refused only if the runtime cannot make the task.
            let spawned = match prepaid.sleeping(client_bytes) {
                Ok(held) => prepaid.spawn(fut, held),
                Err(unfunded) => Err(unfunded),
            };
            if let Err(Unfunded) = spawned {
                tracing::warn!(
                    "id={} target={}: no broadcast request: its task is unfunded",
                    id,
                    target
                );
            }
        }
    }

    /// What a broadcast of `Fut`s to the voters of `membership` other than `id` is charged before
    /// any is made, in one charge: each task, the sleep it holds, and what its client allocates.
    /// Refused, nothing charged, if the funder refuses it or the factory cannot tell what a client
    /// allocates; `None` on a node without a funder.
    fn charge_broadcast<Fut>(
        funding: &Funding,
        network_factory: &NF,
        id: &C::NodeId,
        membership: &Membership<C::NodeId, C::Node>,
    ) -> Result<Option<Charge>, Unfunded>
    where
        Fut: Future<Output = ()> + OptionalSend + 'static,
    {
        if !funding.is_funded() {
            return Ok(None);
        }
        let others = || membership.voters().filter(move |&(target, _)| target != id);
        let clients = clients_bytes::<C, NF, _>(network_factory, others()).ok_or(Unfunded)?;
        funding.sleeping_tasks::<C, Fut>(others().count(), clients)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) fn handle_vote_request(&mut self, req: VoteRequest<C>, tx: VoteTx<C>) {
        tracing::info!("{}: req: {}", func_name!(), req);

        let resp = self.engine.handle_vote_req(req);

        // Record vote to external metrics recorder
        if let Some(r) = &self.metrics_recorder {
            r.increment_vote();
        }

        let condition = Some(Condition::IOFlushed {
            io_id: IOId::new(self.engine.state.vote_ref()),
        });
        self.engine.output.push_command(Command::Respond {
            when: condition,
            resp: Respond::new(resp, tx),
        });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) fn handle_pre_vote_request(&mut self, req: VoteRequest<C>, tx: VoteTx<C>) {
        tracing::info!("{}: req: {}", func_name!(), req);

        let resp = self.engine.handle_pre_vote_req(req);

        // A Pre-Vote persists nothing, so there is no vote IO to wait for: respond at once.
        self.engine.output.push_command(Command::Respond {
            when: None,
            resp: Respond::new(resp, tx),
        });
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) fn handle_append_entries_request(&mut self, req: AppendEntriesRequest<C>, tx: AppendEntriesTx<C>) {
        tracing::debug!("{}: req: {}", func_name!(), req);

        // The entries stay with their owner, if they have one, until the log store took them.
        let segment = LogSegment::new(req.prev_log_id, req.entries);
        self.engine.handle_append_entries(&req.vote, segment, tx);

        // Record append entries to external metrics recorder
        if let Some(r) = &self.metrics_recorder {
            r.increment_append();
        }

        let committed = LogIOId::new(req.vote.to_committed(), req.leader_commit);
        self.engine.state.update_committed(committed);
    }

    /// Handle a full snapshot received from the dedicated install-snapshot channel.
    #[tracing::instrument(level = "debug", skip(self, req), fields(state = debug(self.engine.state.server_state), id=display(&self.id)
    ))]
    pub(crate) fn handle_install_full_snapshot_request(&mut self, req: InstallFullSnapshotRequest<C, SM>) {
        tracing::debug!("RAFT_event id={:<2}  input: {}", self.id, req);

        self.runtime_stats.record_raft_msg(RaftMsgName::InstallSnapshot);

        self.engine.handle_install_full_snapshot(req.vote, req.snapshot, req.tx);
    }

    #[tracing::instrument(level = "debug", skip(self, msg), fields(state = debug(self.engine.state.server_state), id=display(&self.id)
    ))]
    pub(crate) fn handle_api_msg(&mut self, msg: RaftMsg<C>) {
        tracing::debug!("RAFT_event id={:<2}  input: {}", self.id, msg);

        self.runtime_stats.record_raft_msg(msg.name());

        match msg {
            RaftMsg::AppendEntries { rpc, tx } => {
                self.handle_append_entries_request(rpc, tx);
            }
            RaftMsg::RequestVote { rpc, tx } => {
                let now = C::now();
                tracing::info!(
                    "received RaftMsg::RequestVote: {}, now: {}, vote_request: {}",
                    func_name!(),
                    now.display(),
                    rpc
                );

                self.handle_vote_request(rpc, tx);
            }
            RaftMsg::RequestPreVote { rpc, tx } => {
                tracing::info!("received RaftMsg::RequestPreVote: vote_request: {}", rpc);

                self.handle_pre_vote_request(rpc, tx);
            }
            RaftMsg::GetLinearizer { linearizer_option, tx } => {
                self.handle_get_linearizer(linearizer_option, tx);
            }
            RaftMsg::ClientWrite {
                payloads,
                responders,
                expected_leader,
                #[cfg(feature = "runtime-stats")]
                proposed_at,
            } => {
                // Check if expected leader matches current leader
                if let Some(expected) = expected_leader {
                    let vote = self.engine.state.vote_ref();

                    let committed_leader_id = vote.try_to_committed_leader_id();

                    if committed_leader_id.as_ref() != Some(&expected) {
                        // Leader has changed, return ForwardToLeader error to all responders
                        let forward_err = self.engine.state.forward_to_leader(&self.engine.config.funding);
                        for r in responders.into_iter().flatten() {
                            let err = ClientWriteError::ForwardToLeader(forward_err.clone());
                            r.on_complete(Err(err));
                        }
                        return;
                    }
                }
                self.runtime_stats.write_batch.record(payloads.len() as u64);
                self.write_entries(
                    payloads,
                    responders,
                    #[cfg(feature = "runtime-stats")]
                    proposed_at,
                );
            }
            RaftMsg::Initialize { members, payload, tx } => {
                tracing::info!("received RaftMsg::Initialize: {}, members: {:?}", func_name!(), members);

                self.handle_initialize(members, payload, tx);
            }
            RaftMsg::ChangeMembership {
                changes,
                payloads,
                retain,
                preconditions,
                held,
                tx,
            } => {
                tracing::info!(
                    "received RaftMsg::ChangeMembership: {}, members: {:?}, retain: {:?}, preconditions: {}",
                    func_name!(),
                    changes,
                    retain,
                    preconditions.as_ref().display()
                );

                self.change_membership(changes, retain, preconditions, payloads, tx);
                // The preconditions were freed as they were checked.
                drop(held);
            }
            RaftMsg::AppendMembership {
                payload,
                preconditions,
                tx,
            } => {
                tracing::info!(
                    "received RaftMsg::AppendMembership: {}, payload: {}, preconditions: {}",
                    func_name!(),
                    payload,
                    preconditions.as_ref().display()
                );

                self.append_membership(payload, preconditions, tx);
            }
            RaftMsg::WithRaftState { req, boxed } => {
                req(&self.engine.state);
                // Running `req` freed its box.
                drop(boxed);
            }
            RaftMsg::HandleTransferLeader {
                from: current_leader_vote,
                to,
                last_log_id,
            } => {
                if self.engine.state.vote_ref() == &current_leader_vote {
                    tracing::info!("Transfer Leader from: {}, to {}", current_leader_vote, to);

                    self.engine.state.vote.disable_lease();
                    if self.id == to {
                        if last_log_id.as_ref() > self.engine.state.last_log_id() {
                            tracing::info!(
                                "ignore transfer Leader: local log is not up to date; expected: {}, local: {}",
                                last_log_id.display(),
                                self.engine.state.last_log_id().display()
                            );
                            return;
                        }

                        self.engine.elect_by_leadership_transfer();
                    }
                }
            }
            RaftMsg::ExternalCommand { cmd } => {
                tracing::info!("{}: received RaftMsg::ExternalCommand, cmd: {:?}", func_name!(), cmd);

                self.handle_external_command(cmd);
            }
            #[cfg(feature = "runtime-stats")]
            RaftMsg::GetRuntimeStats { tx } => {
                // Copy runtime_stats and sync the shared replicate_batch
                let mut stats = self.runtime_stats.clone();
                stats.replicate_batch = self.shared_replicate_batch.snapshot();

                stats.build_log_stage_histograms();

                tx.send(stats).ok();
            }
        };
    }

    /// Handle an [`ExternalCommand`], a request from the application that bypasses the Raft
    /// protocol, such as triggering an election or a snapshot.
    fn handle_external_command(&mut self, cmd: ExternalCommand<C>) {
        match cmd {
            ExternalCommand::Elect { pre_vote } => {
                if self.engine.leader.is_some() {
                    // A Leader can not win a campaign it starts: its own heartbeats keep
                    // refreshing the voters' leader lease, and a lease that has not expired
                    // rejects the vote request. Leave the established leadership alone.
                    tracing::info!("ExternalCommand: already a Leader, ignore election trigger");
                } else {
                    if self.engine.state.membership_state.is_voter_in_effective_or_committed(&self.id) {
                        if pre_vote {
                            self.engine.pre_elect();
                        } else {
                            self.engine.elect();
                        }
                        tracing::debug!("ExternalCommand: triggered election, pre_vote: {}", pre_vote);
                    } else {
                        // Node is switched to learner.
                    }
                }
            }
            ExternalCommand::Heartbeat => {
                self.send_heartbeat("ExternalCommand");
            }
            ExternalCommand::Snapshot => {
                self.trigger_snapshot();
            }
            ExternalCommand::PurgeLog { upto } => {
                self.engine.trigger_purge_log(upto);
            }
            ExternalCommand::TriggerTransferLeader { to } => {
                if self.config.get_reset_backoff_on_transfer_leader() {
                    let targets = BatchOf::<C, _>::of([to.clone()]);
                    self.trigger_reset_backoff(targets);
                }
                self.engine.trigger_transfer_leader(to);
            }
            ExternalCommand::ResetBackoff { to } => {
                self.trigger_reset_backoff(to);
            }
            ExternalCommand::AllowNextRevert { to, allow, tx } => {
                //
                let res = match self.engine.try_leader_handler() {
                    Ok(mut l) => {
                        let res = l.replication_handler().allow_next_revert(to, allow);
                        res.map_err(AllowNextRevertError::from)
                    }
                    Err(e) => {
                        tracing::warn!("AllowNextRevert: current node is not a Leader");
                        Err(AllowNextRevertError::from(e))
                    }
                };
                tx.send(res).ok();
            }
            ExternalCommand::SetMetricsRecorder { recorder } => {
                tracing::info!("setting metrics recorder");
                self.metrics_recorder = recorder;
            }
            ExternalCommand::RefreshServerState {
                vote,
                membership_log_id,
            } => {
                // The condition to refresh, e.g., the membership config that removes the
                // Leader being committed, is checked by the sender. Refresh only if the
                // vote and the effective membership config log id still match what the
                // sender observed, so that a delayed command can not cause an unexpected
                // server state refresh. A `None` skips the corresponding check.
                let st = &self.engine.state;
                let vote_unchanged = vote.as_ref().is_none_or(|v| st.vote_ref() == v);
                let membership_unchanged = membership_log_id
                    .as_ref()
                    .is_none_or(|log_id| st.membership_state.effective().log_id().as_ref() == Some(log_id));

                if vote_unchanged && membership_unchanged {
                    self.engine.refresh_server_state();
                } else {
                    tracing::info!(
                        "RefreshServerState is dropped: expected vote: {}, membership log id: {}; current vote: {}, membership log id: {}",
                        vote.display(),
                        membership_log_id.display(),
                        self.engine.state.vote_ref(),
                        self.engine.state.membership_state.effective().log_id().display(),
                    );
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(state = debug(self.engine.state.server_state), id=display(&self.id)
    ))]
    pub(crate) fn handle_notification(&mut self, notify: Notification<C>) -> Result<(), Fatal<C>> {
        tracing::debug!("RAFT_event id={:<2} notify: {}", self.id, notify);

        self.runtime_stats.record_notification(notify.name());

        match notify {
            Notification::VoteResponse {
                target,
                resp,
                candidate_vote,
            } => {
                let now = C::now();

                tracing::info!(
                    "received Notification::VoteResponse: {}, now: {}, resp: {}",
                    func_name!(),
                    now.display(),
                    resp
                );

                #[allow(clippy::collapsible_if)]
                if self.engine.candidate.is_some() {
                    let my_vote = self.engine.candidate_ref().map(|x| x.vote_ref());
                    if Self::does_vote_match("Candidate", &candidate_vote, my_vote, "VoteResponse") {
                        self.engine.handle_vote_resp(target, resp);
                    }
                }
            }

            Notification::PreVoteResponse {
                target,
                resp,
                candidate_vote,
            } => {
                tracing::info!(
                    "received Notification::PreVoteResponse: target: {}, resp: {}",
                    target,
                    resp
                );

                #[allow(clippy::collapsible_if)]
                if self.engine.pre_candidate.is_some() {
                    let my_vote = self.engine.pre_candidate_ref().map(|x| x.vote_ref());
                    if Self::does_vote_match("Pre-Candidate", &candidate_vote, my_vote, "PreVoteResponse") {
                        self.engine.handle_pre_vote_resp(target, resp);
                    }
                }
            }

            Notification::HigherVote {
                target,
                higher,
                leader_vote,
            } => {
                tracing::info!(
                    "{}: received Notification::HigherVote, target: {}, higher_vote: {}, sending_vote: {}",
                    func_name!(),
                    target,
                    higher,
                    leader_vote
                );

                let my_vote = self.engine.leader.as_ref().map(|x| &x.committed_vote);
                if Self::does_vote_match("Leader", &leader_vote, my_vote, "HigherVote") {
                    // Rejected vote change is ok.
                    self.engine.vote_handler().update_vote(&higher).ok();
                }
            }

            Notification::Tick { i } => self.handle_tick(i),
            Notification::StorageError { error } => {
                tracing::error!("RaftCore received Notification::StorageError: {}", error);
                return Err(Fatal::StorageError(error));
            }

            Notification::LocalIO { io_id } => self.handle_local_io(io_id),
            Notification::ReplicationProgress {
                stream_id,
                progress,
                inflight_id,
            } => {
                tracing::debug!("recv Notification::ReplicationProgress: progress: {}", progress);

                if let Some(mut rh) = self.engine.try_replication_handler() {
                    rh.update_progress(progress.target, stream_id, progress.result, inflight_id);
                }
            }

            Notification::HeartbeatProgress {
                stream_id,
                sending_time,
                target,
            } => {
                if let Some(mut rh) = self.engine.try_replication_handler() {
                    rh.try_update_leader_clock(stream_id, target, sending_time);
                }
            }

            Notification::StateMachine { command_result } => self.handle_state_machine_result(command_result)?,
            Notification::PendingReadDeadlineReached => self.process_pending_reads(),
        };
        Ok(())
    }

    /// Handle [`Notification::Tick`]: check the election timer and, if leading, the heartbeat
    /// timer.
    fn handle_tick(&mut self, i: u64) {
        // check every timer

        let now = C::now();
        tracing::debug!("received tick: {}, now: {}", i, now.display());

        self.handle_tick_election();

        // Leader send heartbeat
        let heartbeat_at = self.engine.leader_ref().map(|l| l.next_heartbeat);
        if let Some(t) = heartbeat_at
            && now >= t
        {
            if self.runtime_config.enable_heartbeat.load(Ordering::Relaxed) {
                self.send_heartbeat("tick");
            }

            // Install next heartbeat
            if let Some(l) = self.engine.leader_mut() {
                l.next_heartbeat = C::now() + Duration::from_millis(self.config.heartbeat_interval);
            }
        }
    }

    /// Handle [`Notification::LocalIO`]: a local log or vote write reached the disk.
    fn handle_local_io(&mut self, io_id: IOId<C>) {
        self.engine.state.log_progress_mut().try_flush(io_id.clone());

        match io_id {
            IOId::Log(log_io_id) => {
                if let Some(ref log_id) = log_io_id.log_id {
                    self.runtime_stats.record_log_stage_now(Stage::Persisted, log_id.index() + 1);
                }

                // No need to check against membership change,
                // because not like removing-then-adding a remote node,
                // local log wont revert when membership changes.
                #[allow(clippy::collapsible_if)]
                if self.engine.leader.is_some() {
                    let my_vote = self.engine.leader.as_ref().map(|x| &x.committed_vote);
                    if Self::does_vote_match("Leader", &log_io_id.committed_vote, my_vote, "LocalIO Notification") {
                        self.engine.replication_handler().update_local_progress(log_io_id.log_id);
                    }
                }
            }
            IOId::Vote(_vote) => {
                // nothing to do
            }
        }
    }

    /// Handle [`Notification::StateMachine`]: the state machine worker finished a command.
    fn handle_state_machine_result(&mut self, command_result: sm::CommandResult<C>) -> Result<(), Fatal<C>> {
        tracing::debug!("sm::StateMachine command result: {:?}", command_result);

        let res = command_result.result?;

        match res {
            sm::Response::BuildSnapshotDone(meta) => {
                tracing::info!(
                    "sm::StateMachine command done: BuildSnapshotDone: {}: {}",
                    meta.display(),
                    func_name!()
                );

                self.engine.on_building_snapshot_done(meta);
            }
            sm::Response::InstallSnapshot((log_io_id, meta)) => {
                tracing::info!(
                    "sm::StateMachine command done: InstallSnapshot: {}, log_io_id: {}: {}",
                    meta.display(),
                    log_io_id,
                    func_name!()
                );

                self.engine.state.log_progress_mut().try_flush(IOId::Log(log_io_id));

                if let Some(meta) = meta {
                    let st = self.engine.state.io_state_mut();
                    if let Some(last) = &meta.last_log_id {
                        st.apply_progress.try_flush(last.clone());
                        st.snapshot.try_flush(last.clone());
                    }
                }
            }
            sm::Response::Apply(res) => {
                self.runtime_stats.record_log_stage_now(Stage::Applied, res.last_applied.index() + 1);
                self.engine.state.apply_progress_mut().try_flush(res.last_applied);
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn handle_tick_election(&mut self) {
        let now = C::now();

        tracing::debug!("try to trigger election, now: {}", now.display());

        if self.engine.state.server_state == ServerState::Leader {
            tracing::debug!("skip election, already a leader");
            return;
        }

        if !self.engine.state.membership_state.is_voter_in_effective_or_committed(&self.id) {
            tracing::debug!("skip election, not a voter in effective or committed membership");
            return;
        }

        if !self.runtime_config.enable_elect.load(Ordering::Relaxed) {
            tracing::debug!("skip election, election disabled");
            return;
        }

        let mut election_timeout = self.engine.config.timer_config.election_timeout;
        if self.engine.is_there_greater_log() {
            election_timeout += self.engine.config.timer_config.smaller_log_timeout;
        }

        let effective = self.engine.state.membership_state.effective();
        // Whether this node is the only one that will start an election.
        // There is no need to check `committed` membership.
        // Because there won't be a leader established without contacting this node, if effective membership
        // contains only this node.
        let is_single_campaigner = effective.is_voter(&self.id) && effective.voter_ids().count() == 1;

        if is_single_campaigner {
            // When a node restart, it may stay in any state but the in progress election(engine.candidate) is
            // empty.
            if self.engine.candidate_ref().is_some() {
                tracing::debug!("skip election, single voter already has an active election in progress");
                return;
            }
            tracing::debug!("single voter, elect immediately");
        } else {
            tracing::debug!("remote votes required, check election timeout");

            let local_vote = &self.engine.state.vote;
            tracing::debug!("local vote: {}, election_timeout: {:?}", local_vote, election_timeout,);

            if local_vote.is_expired(now, election_timeout) {
                tracing::info!("election timeout expired, triggering election");
            } else {
                tracing::debug!("election timeout not yet expired");
                return;
            }
        }

        // Only a node that is itself the sole effective voter can skip Pre-Vote. A removed
        // committed voter still needs a remote quorum, even if there is only one effective voter.
        let pre_vote = self.runtime_config.enable_pre_vote.load(Ordering::Relaxed) && !is_single_campaigner;

        if pre_vote {
            // A Pre-Vote does not advance `vote.last_update_time`, so without this guard a node
            // would re-issue a Pre-Vote on every tick. Skip while a fresh round is still in flight;
            // restart once it has been pending longer than `election_timeout`.
            if let Some(started) = self.engine.pre_candidate_ref().map(|pc| pc.starting_time())
                && now < started + election_timeout
            {
                tracing::debug!("skip pre-vote, a pre-vote round is already in flight");
                return;
            }
        }

        // Every time elect, reset this flag.
        self.engine.reset_greater_log();

        if pre_vote {
            tracing::info!("trigger pre-vote");
            self.engine.pre_elect();
        } else {
            tracing::info!("trigger election");
            self.engine.elect();
        }
    }

    /// If a message is sent under a vote that this node no longer holds, it is a stale message
    /// and should be just ignored.
    ///
    /// `role` names the role the vote belongs to, for the log message, e.g. `"Candidate"`.
    /// `my_vote` is the vote this node currently holds in that role, `None` if it left the role
    /// (a Candidate that finished voting has no vote).
    ///
    /// The two votes may have different types: the vote a message was sent under is an
    /// `UncommittedVote`/`CommittedVote`, while the vote this node holds may be the
    /// application's `C::Vote`. They are therefore compared by leader id.
    fn does_vote_match<V, W>(role: &str, sent_vote: &V, my_vote: Option<&W>, msg: impl fmt::Display) -> bool
    where
        V: RaftVote,
        W: RaftVote<LeaderId = V::LeaderId>,
    {
        let Some(my_vote) = my_vote else {
            tracing::warn!(
                "A message will be ignored because this node is no longer {}: \
                 msg sent by vote: {}; when ({})",
                role,
                sent_vote,
                msg
            );
            return false;
        };

        if sent_vote.leader_id() != my_vote.leader_id() {
            tracing::warn!(
                "A message will be ignored because {} vote changed: \
                msg sent by vote: {}; current my vote: {}; when ({})",
                role,
                sent_vote,
                my_vote,
                msg
            );
            return false;
        }

        true
    }

    /// Broadcast heartbeat to all followers with per-follower matching log ids.
    ///
    /// This method validates the session and sends heartbeat events only if the current
    /// session matches the requested session (no leader change or membership change).
    ///
    /// The heartbeat workers the last rebuild was refused the funding of are spawned again first,
    /// so a target left without its worker by a refusal is sent this heartbeat once funding is
    /// restored.
    async fn broadcast_heartbeat(&mut self, session_id: ReplicationSessionId<C>, bypass_min_interval: bool) {
        // Lazy get the progress data for heartbeat. If the leader changes or replication
        // config changes, no need to send heartbeat.
        let Ok(lh) = self.engine.try_leader_handler() else {
            // No longer a leader
            return;
        };

        let committed_vote = lh.leader.committed_vote.clone();
        let membership_log_id = lh.state.membership_state.effective().log_id();
        let current_session_id = ReplicationSessionId::new(committed_vote.clone(), membership_log_id.clone());

        if current_session_id != session_id {
            // Session changed, skip heartbeat
            return;
        }

        if self.heartbeat_handle.has_unspawned() {
            let factory = &mut self.network_factory;
            let spawning = Spawning {
                leader_vote: committed_vote,
                tx_notification: &self.tx_notification,
                membership: self.engine.state.membership_state.effective().membership(),
                funding: &self.engine.config.funding,
            };
            self.heartbeat_handle.spawn_unspawned::<NF>(spawning, factory).await;
        }

        let Ok(lh) = self.engine.try_leader_handler() else {
            return;
        };

        let cluster_committed = lh.state.cluster_committed().cloned();
        let now = C::now();
        let round = lh.leader.next_heartbeat_round();
        let min_interval = Duration::from_millis(self.config.heartbeat_min_interval());
        let leader = &*lh.leader;
        let events = leader
            .progress
            .iter()
            .filter(|progress_entry| progress_entry.id != self.id)
            .filter(|progress_entry| {
                bypass_min_interval || leader.need_heartbeat(&progress_entry.id, now, min_interval)
            })
            .map(|progress_entry| {
                (progress_entry.id.clone(), HeartbeatEvent {
                    time: SendStamp::new(now, round),
                    matching: progress_entry.matching.clone(),
                    cluster_committed: cluster_committed.clone(),
                })
            });

        self.heartbeat_handle.broadcast(events);
    }

    /// Stops the replication stream `s` to `target`: its channels are dropped, and its task ends
    /// as it sees it is stopped. Its task is not joined, so nothing is spawned to join it.
    fn close_replication(target: &C::NodeId, s: ReplicationHandle<C>) {
        tracing::debug!("closing removed replication: {}", target);
        drop(s);
    }

    /// Run [`Command::UpdateIOProgress`].
    async fn run_update_io_progress(&mut self, io_id: IOId<C>) {
        // Notify that I/O is about to be submitted.
        self.io_broadcast.accepted.send_if_greater(io_id.clone());

        self.engine.state.log_progress_mut().submit(io_id.clone());

        let notify = Notification::LocalIO { io_id: io_id.clone() };

        self.tx_notification.send(notify).await.ok();
    }

    /// Run [`Command::AppendEntries`].
    async fn run_append_entries(
        &mut self,
        committed_vote: CommittedVoteOf<C>,
        entries: BatchOf<C, C::Entry>,
    ) -> Result<(), StorageError<C>> {
        let last_log_id = entries.last().unwrap().log_id();
        let last_log_index = last_log_id.index();
        tracing::debug!("AppendEntries: {}", entries.as_ref().display_n(10));

        let entry_count = entries.len() as u64;

        // Record to internal histogram
        self.runtime_stats.append_batch.record(entry_count);

        // Record to external metrics recorder
        if let Some(r) = &self.metrics_recorder {
            r.record_append_batch(entry_count);
        }

        let io_id = IOId::new_log_io(committed_vote, Some(last_log_id));
        let callback = IOFlushed::new(io_id.clone(), self.io_broadcast.completed.clone());

        // Notify that I/O is about to be submitted.
        self.io_broadcast.accepted.send_if_greater(io_id.clone());

        // Mark this IO request as submitted,
        // other commands relying on it can then be processed.
        // For example,
        // `Replicate` command cannot run until this IO request is submitted(no need to be flushed),
        // because it needs to read the log entry from the log store.
        //
        // The `submit` state must be updated before calling `append()`,
        // because `append()` may call the callback before returning.
        self.engine.state.log_progress_mut().submit(io_id.clone());

        self.runtime_stats.record_log_stage_now(Stage::Submitted, last_log_index + 1);

        // Submit IO request, do not wait for the response.
        self.log_store.append(entries, callback).await.sto_write_logs()?;

        if self.config.broadcast_submitted_on_append() {
            self.io_broadcast.submitted.send_if_greater(io_id);
        }

        Ok(())
    }

    /// Run [`Command::SaveVote`].
    async fn run_save_vote(&mut self, vote: VoteOf<C>) -> Result<(), StorageError<C>> {
        let io_id = IOId::new(&vote);

        // Notify that vote I/O is about to be submitted.
        self.io_broadcast.accepted.send_if_greater(io_id.clone());

        self.engine.state.log_progress_mut().submit(io_id.clone());
        self.log_store.save_vote(&vote).await.sto_write_vote()?;

        self.tx_notification
            .send(Notification::LocalIO {
                io_id: IOId::new(&vote),
            })
            .await
            .ok();

        // If a non-committed vote is saved,
        // there may be a candidate waiting for the response.
        if let VoteStatus::Pending(non_committed) = vote.clone().into_vote_status() {
            self.tx_notification
                .send(Notification::VoteResponse {
                    target: self.id.clone(),
                    // last_log_id is not used when sending VoteRequest to local node
                    resp: VoteResponse::new(vote, None, true),
                    candidate_vote: non_committed,
                })
                .await
                .ok();
        }

        Ok(())
    }

    /// Run [`Command::PurgeLog`].
    async fn run_purge_log(&mut self, upto: LogIdOf<C>) -> Result<(), StorageError<C>> {
        self.log_store.purge(upto.clone()).await.sto_write_logs()?;

        // A responder may still be pending for a log covered by this purge, e.g. a former
        // leader's log superseded by a snapshot install. Its outcome is unknown because another
        // node may have committed it. Complete the responder instead of leaving it stranded below
        // the purge boundary (which would later panic in `apply_to_state_machine`).
        let leader_id = self.current_leader();
        let leader_node = self.get_leader_node(leader_id.clone());
        let (purged, slots) = self.client_responders.drain_upto(upto.index());
        for (log_index, tx) in purged {
            tx.on_complete(Err(ClientWriteError::LogEntryDiscarded(ForwardToLeader {
                leader_id: leader_id.clone(),
                leader_node: leader_node.clone(),
                reason: ForwardReason::NotLeader,
            })));
            tracing::debug!("sent LogEntryDiscarded for purged log_index: {}", log_index);
        }
        // Credited back once the vector the purged responders were drained into is freed.
        drop(slots);

        self.engine.state.io_state_mut().update_purged(Some(upto));

        Ok(())
    }

    /// Run [`Command::TruncateLog`].
    async fn run_truncate_log(&mut self, after: Option<LogIdOf<C>>) -> Result<(), StorageError<C>> {
        self.log_store.truncate_after(after.clone()).await.sto_write_logs()?;

        // Inform clients waiting for logs to be applied.
        let leader_id = self.current_leader();
        let leader_node = self.get_leader_node(leader_id.clone());

        for (log_index, tx) in self.client_responders.drain_from(after.next_index()) {
            tx.on_complete(Err(ClientWriteError::LogEntryDiscarded(ForwardToLeader {
                leader_id: leader_id.clone(),
                leader_node: leader_node.clone(),
                reason: ForwardReason::NotLeader,
            })));

            tracing::debug!("sent LogEntryDiscarded for truncated log_index: {}", log_index);
        }

        Ok(())
    }

    /// Run [`Command::SaveCommittedAndApply`].
    async fn run_save_committed_and_apply(
        &mut self,
        already_applied: Option<LogIdOf<C>>,
        upto: LogIdOf<C>,
    ) -> Result<(), StorageError<C>> {
        self.runtime_stats.record_log_stage_now(Stage::Committed, upto.index() + 1);

        self.engine.state.apply_progress_mut().submit(upto.clone());

        self.log_store.save_committed(Some(upto.clone())).await.sto_write()?;

        let first = self.engine.state.get_log_id(already_applied.next_index()).unwrap();
        self.apply_to_state_machine(first, upto).await?;

        Ok(())
    }

    /// Run [`Command::ReplicateSnapshot`]: a snapshot transmitter to `target`, every part of it
    /// charged before any is made, as
    /// [`snapshot_transmitter_bytes`](crate::snapshot_transmitter_bytes) sizes it: its task, the
    /// channel that stops it and the sleep it backs off on, and what the network client it sends
    /// with allocates ([`RaftNetworkFactory::new_client_bytes`]). Refused its charge, or its
    /// replication stream, nothing is made, and the snapshot inflight is reset to be sent again.
    async fn run_replicate_snapshot(
        &mut self,
        leader_vote: CommittedVoteOf<C>,
        target: C::NodeId,
        inflight_id: InflightId,
    ) {
        let Some(stream_id) = self.replication_stream_id(&target).await else {
            self.reset_inflight(target, inflight_id);
            return;
        };

        // A target no longer a member is sent nothing.
        let Some(node) = self.engine.state.membership_state.effective().get_node(&target) else {
            self.reset_inflight(target, inflight_id);
            return;
        };

        // The channel that cancels it, the one sleep at a time it waits out its backoff on, and
        // what the client it sends with allocates.
        let funding = &self.engine.config.funding;
        let factory = &self.network_factory;
        let (backoff, network_backoff) = Self::client_backoff(funding, factory, &target, node);
        let parts = || {
            snapshot_transmitter_parts_bytes::<C>()?
                .checked_add(factory.new_client_bytes(&target, node)?)?
                .checked_add(backoff)
        };
        let mut task = match TaskCharge::<C, _>::take(funding, parts) {
            Ok(task) => task,
            Err(Unfunded) => {
                tracing::warn!(
                    "id={} target={}: no snapshot transmitter: it is unfunded",
                    self.id,
                    target
                );
                self.reset_inflight(target, inflight_id);
                return;
            }
        };

        // From its share of the charge taken for it: not refused.
        let Ok((cancel_tx, cancel_rx)) = task.watch(()) else {
            self.reset_inflight(target, inflight_id);
            return;
        };

        let snapshot_network = self.network_factory.new_snapshot_client(target.clone(), node).await;

        let snapshot_reader = self.sm_handle.new_snapshot_reader(self.engine.config.funding.clone());
        let replication_task_context =
            self.new_replication_context(leader_vote, &target, stream_id, cancel_rx, network_backoff);

        let transmitter = SnapshotTransmitter::<C, NF, SM>::new(
            replication_task_context,
            snapshot_network,
            snapshot_reader,
            inflight_id,
        );

        let join_handle = match task.spawn(transmitter.stream_snapshot()) {
            Ok(join_handle) => join_handle,
            Err(Unfunded) => {
                tracing::warn!(
                    "id={} target={}: no snapshot transmitter: its task is unfunded",
                    self.id,
                    target
                );
                self.reset_inflight(target, inflight_id);
                return;
            }
        };

        let handle = SnapshotTransmitterHandle {
            _join_handle: join_handle,
            _tx_cancel: cancel_tx,
        };

        let node = self.replications.get_mut(&target).expect("replication to target node exists");
        // TODO: it is not cleaned when snapshot transmission is done.
        node.snapshot_transmit_handle = Some(handle);
    }

    /// The stream id of the replication stream to `target`, which is spawned now, for the
    /// Leader's progress of `target`, if it has none: its earlier spawn was refused its charge. A
    /// map of more streams than its nodes were charged for is charged again for them before the
    /// stream is inserted. None, nothing made, if refused, or no Leader tracks `target`.
    async fn replication_stream_id(&mut self, target: &C::NodeId) -> Option<StreamId> {
        if let Some(handle) = self.replications.get(target) {
            return Some(handle.stream_id);
        }

        let leader = self.engine.leader.as_ref()?;
        let entry = leader.progress.try_get(target)?;
        let leader_vote = leader.committed_vote.clone();
        let stream_id = entry.data.stream_id;
        let matching = entry.matching.clone();

        let room = self.replications.len() + 1;
        let grown = if room > self.replications_room {
            let Ok(grown) = self.engine.config.funding.charge(replication_map_bytes::<C>(room)) else {
                tracing::warn!(
                    "id={} target={}: no replication stream: its map is unfunded",
                    self.id,
                    target
                );
                return None;
            };
            Some(grown)
        } else {
            None
        };

        let handle = self.spawn_replication_stream(leader_vote, target, stream_id, matching).await?;
        self.replications.insert(target.clone(), handle);

        // The map is now bounded by what it was charged again for: the old charge goes back.
        if let Some(grown) = grown {
            self.replications_funding = grown;
            self.replications_room = room;
        }
        Some(stream_id)
    }

    /// Resets the inflight `inflight_id` to `target`, which was not sent: the Leader sends it
    /// again, as it does after a replication error.
    fn reset_inflight(&mut self, target: C::NodeId, inflight_id: InflightId) {
        let Some(stream_id) = self
            .engine
            .leader
            .as_ref()
            .and_then(|leader| leader.progress.try_get(&target))
            .map(|entry| entry.data.stream_id)
        else {
            return;
        };
        if let Some(mut rh) = self.engine.try_replication_handler() {
            // An empty error allocates nothing.
            rh.update_progress(target, stream_id, Err(String::new()), Some(inflight_id));
        }
    }

    /// Run [`Command::CloseReplicationStreams`].
    fn run_close_replication_streams(&mut self) {
        self.heartbeat_handle.close_workers();
        self.close_replication_streams();
    }

    /// Stops every replication stream, the nodes of their map freed before its charge goes back.
    fn close_replication_streams(&mut self) {
        let left = std::mem::take(&mut self.replications);
        for (target, s) in left {
            Self::close_replication(&target, s);
        }
        self.replications_funding = None;
        self.replications_room = 0;
    }

    /// Run [`Command::RebuildReplicationStreams`]: the heartbeat workers and replication streams
    /// for `targets`, which `funding` was charged for before they were built and is held with
    /// until they are freed.
    ///
    /// The map of the streams is charged before it is built, as
    /// [`replication_map_bytes`](crate::replication_map_bytes) sizes it: refused, no stream is
    /// spawned, and the old ones are kept unless `close_old_streams`. A stream refused its charge
    /// is left out, and spawned when it is next replicated to: see
    /// [`replication_stream_id`](Self::replication_stream_id).
    async fn run_rebuild_replication_streams(
        &mut self,
        leader_vote: CommittedVoteOf<C>,
        targets: Vec<TargetProgress<C>>,
        close_old_streams: bool,
        funding: Option<Charge>,
    ) {
        let heartbeats_complete = {
            let factory = &mut self.network_factory;
            let spawning = Spawning {
                leader_vote: leader_vote.clone(),
                tx_notification: &self.tx_notification,
                membership: self.engine.state.membership_state.effective().membership(),
                funding: &self.engine.config.funding,
            };
            self.heartbeat_handle.spawn_workers::<NF>(spawning, factory, &targets, close_old_streams).await
        };

        self.rebuild_replications(leader_vote, &targets, close_old_streams).await;

        // Kept, not copied, for the heartbeat workers refused their funding to be spawned again;
        // otherwise freed, then their charge goes back.
        if heartbeats_complete {
            drop(targets);
            drop(funding);
        } else {
            self.heartbeat_handle.keep_unspawned(targets, funding);
        }
    }

    /// The replication streams of
    /// [`run_rebuild_replication_streams`](Self::run_rebuild_replication_streams).
    async fn rebuild_replications(
        &mut self,
        leader_vote: CommittedVoteOf<C>,
        targets: &[TargetProgress<C>],
        close_old_streams: bool,
    ) {
        let Ok(map_funding) = self.engine.config.funding.charge(replication_map_bytes::<C>(targets.len())) else {
            tracing::warn!("id={}: no replication streams rebuilt: their map is unfunded", self.id);
            // Streams asked to be replaced must not go on replicating for what they were made for.
            if close_old_streams {
                self.close_replication_streams();
            }
            return;
        };

        let mut new_replications = BTreeMap::new();

        for prog in targets {
            let removed = self.replications.remove(&prog.target);

            let handle = match removed {
                Some(removed) if close_old_streams => {
                    Self::close_replication(&prog.target, removed);
                    None
                }
                removed => removed,
            };

            let handle = match handle {
                Some(handle) => Some(handle),
                None => {
                    let stream_id = prog.progress.data.stream_id;
                    let matching = prog.progress.matching.clone();
                    self.spawn_replication_stream(leader_vote.clone(), &prog.target, stream_id, matching).await
                }
            };

            if let Some(handle) = handle {
                new_replications.insert(prog.target.clone(), handle);
            }
        }

        tracing::debug!("removing unused replications");

        let left = std::mem::replace(&mut self.replications, new_replications);
        let left_funding = std::mem::replace(&mut self.replications_funding, map_funding);
        self.replications_room = targets.len();

        for (target, s) in left {
            Self::close_replication(&target, s);
        }
        // Only after the old map's nodes are freed.
        drop(left_funding);
    }

    /// Run [`Command::StateMachine`], forwarding the inner command to the state machine worker.
    async fn run_state_machine(&mut self, command: sm::Command<C, SM>) -> Result<(), StorageError<C>> {
        let io_id = command.get_log_progress();

        if let Some(io_id) = io_id {
            self.engine.state.log_progress_mut().submit(io_id);
        }

        // If this command update the last-applied log id, mark it as submitted(to state machine).
        if let Some(log_id) = command.get_apply_progress() {
            self.engine.state.apply_progress_mut().submit(log_id);
        }

        if let Some(log_id) = command.get_snapshot_progress() {
            self.engine.state.snapshot_progress_mut().submit(log_id);
        }

        // Just forward a state machine command to the worker.
        self.sm_handle
            .send(command)
            .await
            .map_err(|_e| StorageError::write_state_machine(C::err_from_string("cannot send to sm::Worker")))?;

        Ok(())
    }
}

impl<C, N, LS, SM> RaftRuntime<C, SM> for RaftCore<C, N, LS, SM>
where
    C: RaftTypeConfig,
    N: RaftNetworkFactory<C>,
    N::Network: NetSnapshot<C, SnapshotData = SM::SnapshotData>,
    LS: RaftLogStorage<C>,
    SM: RaftStateMachine<C>,
{
    async fn run_command(&mut self, cmd: Command<C, SM>) -> Result<Option<Command<C, SM>>, StorageError<C>> {
        // tracing::debug!("RAFT_event id={:<2} trycmd: {}", self.id, cmd);

        let condition = cmd.condition();
        tracing::debug!("condition: {:?}", condition);

        if let Some(condition) = condition {
            if condition.is_met(&self.engine.state.io_state) {
                // continue run the command
            } else {
                tracing::debug!("{} is not yet met, postpone cmd: {}", condition, cmd);
                return Ok(Some(cmd));
            }
        }

        tracing::debug!("RAFT_event id={:<2}    cmd: {}", self.id, cmd);

        // Record command execution
        self.runtime_stats.record_command(cmd.name());

        match cmd {
            Command::UpdateIOProgress { io_id, .. } => self.run_update_io_progress(io_id).await,
            Command::AppendEntries {
                committed_vote,
                entries,
                owner,
            } => {
                self.run_append_entries(committed_vote, entries).await?;
                // The log store took the entries and returned: what accounted for them goes.
                drop(owner);
            }
            Command::SaveVote { vote } => self.run_save_vote(vote).await?,
            Command::PurgeLog { upto } => self.run_purge_log(upto).await?,
            Command::TruncateLog { after } => self.run_truncate_log(after).await?,
            Command::SendVote { vote_req } => {
                self.spawn_parallel_vote_requests(&vote_req, VoteRequestKind::Vote).await;
            }
            Command::SendPreVote { vote_req } => {
                self.spawn_parallel_vote_requests(&vote_req, VoteRequestKind::PreVote).await;
            }
            Command::ReplicateCommitted { committed } => {
                self.io_broadcast.committed.send_if_greater(committed);
            }
            Command::BroadcastHeartbeat {
                session_id,
                bypass_min_interval,
            } => self.broadcast_heartbeat(session_id, bypass_min_interval).await,
            Command::SaveCommittedAndApply { already_applied, upto } => {
                self.run_save_committed_and_apply(already_applied, upto).await?
            }
            Command::Replicate { req, target } => {
                // A stream refused its charge is spawned now: refused again, the request is not
                // sent, and its inflight is reset to be sent again.
                if self.replication_stream_id(&target).await.is_none() {
                    self.reset_inflight(target, req.inflight_id);
                    return Ok(None);
                }
                let node = self.replications.get(&target).expect("replication to target node exists");
                node.replicate_tx.send(req).ok();
            }
            Command::ReplicateSnapshot {
                leader_vote,
                target,
                inflight_id,
            } => self.run_replicate_snapshot(leader_vote, target, inflight_id).await,
            Command::BroadcastTransferLeader { req } => self.broadcast_transfer_leader(req).await,
            Command::CloseReplicationStreams => self.run_close_replication_streams(),
            Command::FailPendingReads => self.fail_pending_reads(),
            Command::RebuildReplicationStreams {
                leader_vote,
                targets,
                close_old_streams,
                funding,
            } => {
                self.run_rebuild_replication_streams(leader_vote, targets, close_old_streams, funding).await;
            }
            Command::StateMachine { command } => self.run_state_machine(command).await?,
            Command::Respond { resp, .. } => resp.send(),
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use maplit::btreemap;
    use maplit::btreeset;
    use openraft_rt_tokio::TokioRuntime;

    use super::apply_membership_to_payload;
    use crate::ChangeMembers;
    use crate::Membership;
    use crate::core::raft_msg::membership_payloads::MembershipPayloads;
    use crate::entry::RaftPayload;
    use crate::type_config::TypeConfigExt;
    use crate::type_config::alias::MembershipStateOf;
    use crate::type_config::alias::StoredMembershipOf;

    #[derive(Debug, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
    struct TestPayload {
        normal: Option<u64>,
        membership: Option<Membership<u64, ()>>,
    }

    impl fmt::Display for TestPayload {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "normal={:?}, membership={:?}", self.normal, self.membership)
        }
    }

    impl RaftPayload for TestPayload {
        type D = u64;
        type NodeId = u64;
        type Node = ();

        fn blank() -> Self {
            Self {
                normal: None,
                membership: None,
            }
        }

        fn with_normal(mut self, data: u64) -> Self {
            self.normal = Some(data);
            self
        }

        fn with_membership(mut self, membership: Membership<u64, ()>) -> Self {
            self.membership = Some(membership);
            self
        }

        fn get_membership(&self) -> Option<Membership<u64, ()>> {
            self.membership.clone()
        }
    }

    crate::declare_raft_types!(
        TestConfig:
            D = u64,
            R = (),
            Node = (),
            Payload = TestPayload,
            AsyncRuntime = TokioRuntime,
    );

    #[test]
    fn membership_change_replaces_payload_membership() {
        let current = Membership::new_with_defaults(vec![btreeset! {1}], []);
        let stored = StoredMembershipOf::<TestConfig>::new(None, current);
        let membership_state = MembershipStateOf::<TestConfig>::new(stored.clone(), stored);

        let input_membership = Membership::new_with_defaults(vec![btreeset! {9}], []);
        let payload = TestPayload {
            normal: Some(7),
            membership: Some(input_membership),
        };

        let payloads = MembershipPayloads::<TestConfig>::Uniform(payload);
        let changes = ChangeMembers::AddNodes(btreemap! {2 => ()});
        let result = apply_membership_to_payload::<TestConfig>(&membership_state, changes, false, payloads);
        let actual = result.unwrap();

        let expected_membership = Membership::new_with_defaults(vec![btreeset! {1}], btreeset! {2});
        let expected = TestPayload {
            normal: Some(7),
            membership: Some(expected_membership),
        };
        assert_eq!(expected, actual);
    }

    #[test]
    fn membership_change_from_blank_payload_creates_membership_payload() {
        let current = Membership::new_with_defaults(vec![btreeset! {1}], []);
        let stored = StoredMembershipOf::<TestConfig>::new(None, current);
        let membership_state = MembershipStateOf::<TestConfig>::new(stored.clone(), stored);

        let payloads = MembershipPayloads::<TestConfig>::Uniform(TestPayload::blank());
        let changes = ChangeMembers::AddNodes(btreemap! {2 => ()});
        let result = apply_membership_to_payload::<TestConfig>(&membership_state, changes, false, payloads);
        let actual = result.unwrap();

        let expected_membership = Membership::new_with_defaults(vec![btreeset! {1}], btreeset! {2});
        let expected = TestPayload {
            normal: None,
            membership: Some(expected_membership),
        };
        assert_eq!(expected, actual);
    }

    /// Replacing the voters computes a joint membership, so the joint payload carries it and the
    /// uniform payload goes back to the caller.
    #[test]
    fn joint_membership_selects_joint_payload() {
        let current = Membership::new_with_defaults(vec![btreeset! {1, 2, 3}], btreeset! {4, 5});
        let stored = StoredMembershipOf::<TestConfig>::new(None, current);
        let membership_state = MembershipStateOf::<TestConfig>::new(stored.clone(), stored);

        let (unused_tx, mut unused_rx) = TestConfig::oneshot();
        let payloads = MembershipPayloads::<TestConfig>::JointOrUniform {
            joint: TestPayload::normal(1),
            uniform: TestPayload::normal(2),
            unused_tx,
        };

        let changes = ChangeMembers::ReplaceAllVoters(btreeset! {3, 4, 5});
        let result = apply_membership_to_payload::<TestConfig>(&membership_state, changes, false, payloads);
        let actual = result.unwrap();

        let joint_membership = Membership::new_with_defaults(vec![btreeset! {1, 2, 3}, btreeset! {3, 4, 5}], []);
        let expected = TestPayload {
            normal: Some(1),
            membership: Some(joint_membership),
        };
        assert_eq!(expected, actual);

        let returned = unused_rx.try_recv().unwrap();
        assert_eq!(TestPayload::normal(2), returned);
    }

    /// Adding a node moves no voter, so the computed membership is uniform and the uniform
    /// payload carries it, sending the joint payload back to the caller.
    #[test]
    fn uniform_membership_selects_uniform_payload() {
        let current = Membership::new_with_defaults(vec![btreeset! {1}], []);
        let stored = StoredMembershipOf::<TestConfig>::new(None, current);
        let membership_state = MembershipStateOf::<TestConfig>::new(stored.clone(), stored);

        let (unused_tx, mut unused_rx) = TestConfig::oneshot();
        let payloads = MembershipPayloads::<TestConfig>::JointOrUniform {
            joint: TestPayload::normal(1),
            uniform: TestPayload::normal(2),
            unused_tx,
        };

        let changes = ChangeMembers::AddNodes(btreemap! {2 => ()});
        let result = apply_membership_to_payload::<TestConfig>(&membership_state, changes, false, payloads);
        let actual = result.unwrap();

        let expected_membership = Membership::new_with_defaults(vec![btreeset! {1}], btreeset! {2});
        let expected = TestPayload {
            normal: Some(2),
            membership: Some(expected_membership),
        };
        assert_eq!(expected, actual);

        let returned = unused_rx.try_recv().unwrap();
        assert_eq!(TestPayload::normal(1), returned);
    }
}
