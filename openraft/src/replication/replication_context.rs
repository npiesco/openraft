use std::fmt;
use std::sync::Arc;

use crate::Config;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::async_runtime::MpscSender;
use crate::core::SharedReplicateBatch;
use crate::core::notification::Notification;
use crate::network::Backoff;
use crate::network::NetBackoff;
use crate::progress::inflight_id::InflightId;
use crate::progress::stream_id::StreamId;
use crate::proposer::SendStamp;
use crate::raft_state::IOId;
use crate::replication::response::Progress;
use crate::replication::response::ReplicationResult;
use crate::type_config::alias::CommittedVoteOf;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::MpscSenderOf;
use crate::type_config::alias::WatchReceiverOf;
use crate::vote::raft_vote::RaftVote;

/// Shared context for replication tasks.
///
/// Contains the common state needed by both log replication (`ReplicationCore`)
/// and snapshot transmission (`SnapshotTransmitter`) tasks, including node identifiers,
/// session information, configuration, and the notification channel back to `RaftCore`.
#[derive(Clone)]
pub(crate) struct ReplicationContext<C>
where C: RaftTypeConfig
{
    /// This node id
    #[allow(dead_code)]
    pub(crate) id: C::NodeId,

    /// The ID of the target Raft node which replication events are to be sent to.
    pub(crate) target: C::NodeId,

    /// The leader this replication works for
    pub(crate) leader_vote: CommittedVoteOf<C>,

    /// Identifies which session this replication belongs to.
    pub(crate) stream_id: StreamId,

    /// The Raft's runtime config.
    pub(crate) config: Arc<Config>,

    /// A channel for sending events to the RaftCore.
    #[allow(clippy::type_complexity)]
    pub(crate) tx_notify: MpscSenderOf<C, Notification<C>>,

    /// Watch channel receiver for cancellation signal.
    ///
    /// When the sender is dropped, this signals that replication should stop.
    pub(crate) cancel_rx: WatchReceiverOf<C, ()>,

    /// Shared histogram for recording replication batch sizes.
    pub(crate) replicate_batch: SharedReplicateBatch,

    /// Whether the network client is asked for its own backoff: on a node run with a funder,
    /// only if its factory told what one allocates
    /// ([`RaftNetworkFactory::backoff_bytes`](crate::RaftNetworkFactory::backoff_bytes)), and it
    /// was charged with the client.
    pub(crate) network_backoff: bool,
}

/// The backoff to wait out after `network` failed: the one it gives, if `ask`, as
/// [`ReplicationContext::network_backoff`] tells, else, or if it gives none, that of `config`'s
/// policy, which allocates nothing.
pub(crate) fn backoff_of<C, N>(network: &N, ask: bool, config: &Arc<Config>) -> Backoff
where
    C: RaftTypeConfig,
    N: NetBackoff<C> + ?Sized,
{
    let given = if ask { network.backoff() } else { None };
    given.unwrap_or_else(|| config.build_backoff())
}

impl<C> ReplicationContext<C>
where C: RaftTypeConfig
{
    /// Whether the leader this replication works for has been superseded.
    ///
    /// A replication task is bound to the leader that spawned it. Once local IO is accepted under
    /// a different leader, anything this task still sends would carry a stale vote, so the caller
    /// must stop rather than finish what it started.
    pub(crate) fn leader_changed(&self, accepted_io: &IOId<C>) -> bool {
        let current_leader = accepted_io.leader_id();
        let belonging_leader = self.leader_vote.leader_id();

        if current_leader == belonging_leader {
            return false;
        }

        tracing::info!(
            "{}: Leader changed from {} to {}, quit replication",
            self,
            belonging_leader,
            current_leader
        );
        true
    }

    /// Report a storage failure to [`RaftCore`].
    ///
    /// [`RaftCore`]: crate::core::RaftCore
    pub(crate) async fn notify_storage_error(&self, error: StorageError<C>) {
        self.tx_notify.send(Notification::StorageError { error }).await.ok();
    }

    /// Report that the target answered a request sent at `sending_time`.
    ///
    /// Any successful exchange with the target also proves the leader is still reachable, so this
    /// doubles as a heartbeat acknowledgement. A replication request belongs to no heartbeat round,
    /// so its stamp has round `0`.
    pub(crate) async fn notify_heartbeat_progress(&self, sending_time: InstantOf<C>) {
        self.tx_notify
            .send(Notification::HeartbeatProgress {
                stream_id: self.stream_id,
                target: self.target.clone(),
                sending_time: SendStamp::new(sending_time, 0),
            })
            .await
            .ok();
    }

    /// Report the outcome of the payload identified by `inflight_id`.
    ///
    /// `inflight_id` is `None` when nothing was sent that [`RaftCore`] is waiting on, such as a
    /// bare committed-index update.
    ///
    /// [`RaftCore`]: crate::core::RaftCore
    pub(crate) async fn notify_progress(
        &self,
        result: Result<ReplicationResult<C>, String>,
        inflight_id: Option<InflightId>,
    ) {
        self.tx_notify
            .send(Notification::ReplicationProgress {
                stream_id: self.stream_id,
                progress: Progress {
                    target: self.target.clone(),
                    result,
                },
                inflight_id,
            })
            .await
            .ok();
    }
}

impl<C> fmt::Display for ReplicationContext<C>
where C: RaftTypeConfig
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{id: {}, target: {}, {}}}", self.id, self.target, self.stream_id)
    }
}

impl<C> fmt::Debug for ReplicationContext<C>
where C: RaftTypeConfig
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicationContext")
            .field("id", &self.id)
            .field("target", &self.target)
            .field("session_id", &self.stream_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::iter::Repeat;
    use std::sync::Arc;
    use std::time::Duration;

    use super::backoff_of;
    use crate::Config;
    use crate::engine::testing::UTConfig;
    use crate::metered::metered;
    use crate::network::Backoff;
    use crate::network::NetBackoff;

    /// A network that gives its own backoff, boxed.
    struct Giving;

    impl NetBackoff<UTConfig> for Giving {
        fn backoff(&self) -> Option<Backoff> {
            Some(Backoff::new(std::iter::repeat(Duration::from_secs(7))))
        }
    }

    /// Asked, the network's backoff is taken, allocating what `Backoff::new_bytes` tells, as its
    /// factory charges it; not asked, as a funded node whose factory cannot tell, the network is
    /// not asked and the config's policy is read in place, allocating nothing.
    #[test]
    fn test_network_backoff_only_if_asked() {
        let config = Arc::new(Config {
            backoff: "100ms 200ms".to_string(),
            ..Default::default()
        });

        let (first, meter) = metered(|| backoff_of::<UTConfig, _>(&Giving, true, &config).next());
        assert_eq!(Some(Duration::from_secs(7)), first);
        assert_eq!(Backoff::new_bytes::<Repeat<Duration>>(), meter.allocated);

        let (first, meter) = metered(|| backoff_of::<UTConfig, _>(&Giving, false, &config).next());
        assert_eq!(Some(Duration::from_millis(100)), first);
        assert_eq!(0, meter.allocated, "the policy's backoff allocates nothing");
    }
}
