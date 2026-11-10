use openraft_macros::since;

use crate::RaftTypeConfig;
use crate::TryAsRef;
use crate::errors::ForwardToLeader;
use crate::errors::QuorumNotEnough;
use crate::errors::Unfunded;

/// An error related to an is_leader request.
#[since(version = "0.10.0", change = "added `Unfunded`")]
#[derive(Debug, Clone, thiserror::Error, derive_more::TryInto)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub enum LinearizableReadError<C>
where C: RaftTypeConfig
{
    /// This node is not the leader; request should be forwarded to the leader.
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader<C>),

    /// Cannot finish a request, such as elect or replicate, because a quorum is not available.
    #[error(transparent)]
    QuorumNotEnough(#[from] QuorumNotEnough<C>),

    /// The node's funder refused what the read takes while it waits for a quorum to acknowledge
    /// this node's leadership, so it was not queued; it can be retried once the funder has room.
    #[since(version = "0.10.0")]
    #[error(transparent)]
    Unfunded(Unfunded),
}

impl<C> TryAsRef<ForwardToLeader<C>> for LinearizableReadError<C>
where C: RaftTypeConfig
{
    fn try_as_ref(&self) -> Option<&ForwardToLeader<C>> {
        match self {
            Self::ForwardToLeader(f) => Some(f),
            _ => None,
        }
    }
}
