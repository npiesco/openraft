use openraft_macros::since;
use rt::WatchSender;

use crate::RaftTypeConfig;
use crate::errors::ReplicationClosed;
use crate::progress::stream_id::StreamId;
use crate::proposer::funding::tree_bytes;
use crate::replication::replicate::Replicate;
use crate::replication::snapshot_transmitter_handle::SnapshotTransmitterHandle;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::WatchSenderOf;

/// The bytes a Leader's map of its replication streams is charged, for `members` other members:
/// the tree's nodes, charged before it is built and held until they are freed. A node run with a
/// funder holds two such maps charged at once while it rebuilds its streams, the one it builds and
/// the one it replaces.
#[since(version = "0.10.0")]
pub const fn replication_map_bytes<C: RaftTypeConfig>(members: usize) -> usize {
    tree_bytes::<C::NodeId, ReplicationHandle<C>>(members)
}

/// The handle to a spawned replication stream.
pub(crate) struct ReplicationHandle<C>
where C: RaftTypeConfig
{
    /// Identifies this replication session (leader vote + target node).
    pub(crate) stream_id: StreamId,

    /// The channel used for communicating with the replication task.
    pub(crate) replicate_tx: WatchSenderOf<C, Replicate<C>>,

    /// Sender for the cancellation signal, held only to be dropped: dropping it stops
    /// replication, as the task's `cancel_rx` sees the channel closed.
    _cancel_tx: WatchSenderOf<C, ()>,

    /// Sender for the backoff reset signal; sending on it ends the active backoff of replication.
    pub(crate) backoff_reset_tx: WatchSenderOf<C, ()>,

    /// The spawn handle of the `ReplicationCore` task: dropped, the task runs on until it sees it
    /// is stopped, and is not joined.
    pub(crate) _join_handle: JoinHandleOf<C, Result<(), ReplicationClosed>>,

    /// Handle to the snapshot transmitter task, if one is running.
    pub(crate) snapshot_transmit_handle: Option<SnapshotTransmitterHandle<C>>,
}

impl<C> ReplicationHandle<C>
where C: RaftTypeConfig
{
    pub(crate) fn new(
        stream_id: StreamId,
        replicate_tx: WatchSenderOf<C, Replicate<C>>,
        cancel_tx: WatchSenderOf<C, ()>,
        backoff_reset_tx: WatchSenderOf<C, ()>,
        join_handle: JoinHandleOf<C, Result<(), ReplicationClosed>>,
    ) -> Self {
        Self {
            stream_id,
            _join_handle: join_handle,
            replicate_tx,
            snapshot_transmit_handle: None,
            _cancel_tx: cancel_tx,
            backoff_reset_tx,
        }
    }

    /// Signal the replication task to end its active backoff.
    pub(crate) fn reset_backoff(&self) {
        let send_res = self.backoff_reset_tx.send(());
        if send_res.is_err() {
            tracing::warn!(
                "Failed to reset backoff for replication stream {}: channel closed",
                self.stream_id
            );
        }
    }
}
