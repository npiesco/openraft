//! Defines the [`NetSnapshot`] trait for snapshot transmission.

use std::future::Future;

use openraft_macros::add_async_trait;
use openraft_macros::since;

use crate::OptionalSend;
use crate::OptionalSync;
use crate::RaftTypeConfig;
use crate::errors::ReplicationClosed;
use crate::errors::StreamingError;
use crate::network::RPCOption;
use crate::raft::SnapshotResponse;
use crate::type_config::alias::SnapshotOf;
use crate::type_config::alias::VoteOf;

/// Sends full snapshots to a target node.
///
/// **For most applications, implement [`RaftNetworkV2`] instead.** This trait is
/// automatically derived from `RaftNetworkV2` via blanket implementation.
///
/// Direct implementation is an advanced option for fine-grained control.
///
/// [`RaftNetworkV2`]: crate::network::RaftNetworkV2
#[add_async_trait]
pub trait NetSnapshot<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    /// Snapshot data this network implementation can transmit.
    #[since(version = "0.10.0", change = "moved SnapshotData from RaftTypeConfig to NetSnapshot")]
    type SnapshotData: OptionalSend + 'static;

    /// Send a complete Snapshot to the target.
    ///
    /// This method is responsible for fragmenting the snapshot and sending it to the target node.
    /// Before returning from this method, the snapshot should be completely transmitted and
    /// installed on the target node or rejected because of `vote` being smaller than the
    /// remote one.
    ///
    /// The `vote` is the leader vote used to check if the leader is still valid by a
    /// follower.
    /// When the follower finished receiving the snapshot, it calls
    /// [`Raft::install_full_snapshot()`] with this vote.
    ///
    /// `cancel` gets `Ready` when the caller decides to cancel this snapshot transmission.
    /// The network implementation is also responsible for enforcing `option.soft_ttl()`.
    ///
    /// [`Raft::install_full_snapshot()`]: crate::raft::Raft::install_full_snapshot
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, Self::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>>;

    /// Admit a snapshot transmission before its snapshot is read.
    ///
    /// Openraft calls this before it asks the state machine for the snapshot it then sends with
    /// [`full_snapshot()`](Self::full_snapshot), so the network can reserve what sending it
    /// holds, the snapshot's metadata among it, before the state machine makes that metadata.
    /// An error is handled as one returned by `full_snapshot()`. The default admits at once.
    #[since(version = "0.10.0")]
    async fn admit_snapshot(&mut self) -> Result<(), StreamingError<C>> {
        Ok(())
    }
}
