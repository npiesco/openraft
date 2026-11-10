use openraft_macros::since;

/// The node's [`Funder`] refused to charge what a call would allocate, so nothing was allocated
/// and the call was not made.
///
/// A node run with [`Raft::run_funded()`] charges the channel each call waits on before making
/// it; the call can be retried once the funder has room.
///
/// [`Funder`]: crate::async_runtime::Funder
/// [`Raft::run_funded()`]: crate::Raft::run_funded
#[since(version = "0.10.0")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("the node's funder refused what the call allocates")]
pub struct Unfunded;
