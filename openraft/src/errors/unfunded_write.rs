use openraft_macros::since;

/// The node's [`Funder`] refused what queueing a write's answer takes, so nothing of the write
/// was appended.
///
/// A node run with [`Raft::run_funded()`] charges the room its core keeps a write's answer in,
/// while the write waits to be applied, before the write is appended: the answer's slot, and the
/// queue's grown buffer while the old one still lives. [`bytes()`](Self::bytes) is exactly what
/// was asked of the funder and refused; the write can be retried once the funder has that room.
///
/// [`Funder`]: crate::async_runtime::Funder
/// [`Raft::run_funded()`]: crate::Raft::run_funded
#[since(version = "0.10.0")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[error("the node's funder refused the {bytes} bytes queueing the write's answer takes")]
pub struct UnfundedWrite {
    bytes: usize,
}

impl UnfundedWrite {
    pub(crate) fn new(bytes: usize) -> Self {
        Self { bytes }
    }

    /// The bytes the funder was asked for and refused.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}
