use std::sync::Arc;

use crate::RaftTypeConfig;
use crate::base::slot_lock::SlotLock;
use crate::errors::ReplicationClosed;
use crate::replication::inflight_append_queue::InflightAppendQueue;
use crate::replication::stream_state::StreamState;
use crate::storage::RaftLogStorage;

/// Context passed through the AppendEntries request stream.
///
/// This struct is used with `futures_util::stream::unfold` to generate
/// AppendEntries requests. It holds both the mutable state for reading
/// log entries and a queue for tracking in-flight requests.
pub(crate) struct StreamContext<C, LS>
where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
{
    /// Shared state for generating the next request.
    pub(crate) stream_state: Arc<SlotLock<StreamState<C, LS>>>,

    /// Tracks in-flight requests for RTT measurement.
    pub(crate) inflight_append_queue: InflightAppendQueue<C>,

    /// Fatal error found while generating the request stream.
    pub(crate) fatal_error: Arc<SlotLock<Option<ReplicationClosed>>>,
}
