use crate::RaftTypeConfig;
use crate::async_runtime::Charge;
use crate::core::raft_msg::ClientReadTx;
use crate::raft::linearizable_read::Linearizer;
use crate::type_config::alias::InstantOf;

/// A linearizable read waiting for the quorum-acknowledgement clock to satisfy its threshold.
pub(crate) struct PendingRead<C>
where C: RaftTypeConfig
{
    /// The deadline for responding to this read request.
    pub(super) deadline: InstantOf<C>,

    /// The Linearizer to send back to the client.
    pub(super) linearizer: Linearizer<C>,

    /// The channel to send the linearizer back to the client.
    pub(super) response_tx: ClientReadTx<C>,

    /// What its queue charged for the nodes it takes in both of the queue's maps, before it was
    /// queued: given back by the queue only once those nodes are freed. `None` on a node run
    /// unfunded.
    pub(super) funding: Option<Charge>,
}

impl<C> PendingRead<C>
where C: RaftTypeConfig
{
    /// A read to be queued, its share of the queue's nodes charged before as `funding`: see
    /// [`PendingReadQueue::charge`](super::pending_read_queue::PendingReadQueue::charge).
    pub(crate) fn new(
        deadline: InstantOf<C>,
        linearizer: Linearizer<C>,
        response_tx: ClientReadTx<C>,
        funding: Option<Charge>,
    ) -> Self {
        Self {
            deadline,
            linearizer,
            response_tx,
            funding,
        }
    }
}
