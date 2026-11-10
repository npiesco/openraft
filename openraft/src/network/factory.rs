use openraft_macros::add_async_trait;
use openraft_macros::since;

use crate::OptionalSend;
use crate::OptionalSync;
use crate::RaftTypeConfig;
use crate::network::NetBackoff;
use crate::network::NetSnapshot;
use crate::network::NetStreamAppend;
use crate::network::NetTransferLeader;
use crate::network::NetVote;

/// A trait defining the interface for a Raft network factory to create connections between cluster
/// members.
///
/// See the [network chapter of the guide](crate::docs::getting_started#4-implement-raftnetwork)
/// for details and discussion on this trait and how to implement it.
///
/// ## Related Types
///
/// - [`RaftLogStorage`](crate::storage::RaftLogStorage) - For log storage operations
/// - [`RaftStateMachine`](crate::storage::RaftStateMachine) - For state machine operations
/// - [`Config`](crate::config::Config) - For configuration options
///
/// Typically, the network implementation as such will be hidden behind a `Box<T>` or `Arc<T>` and
/// this interface implemented on the `Box<T>` or `Arc<T>`.
#[add_async_trait]
pub trait RaftNetworkFactory<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    /// Actual type of the network handling a single connection.
    type Network: NetBackoff<C> + NetStreamAppend<C> + NetVote<C> + NetSnapshot<C> + NetTransferLeader<C>;

    /// Create a new network instance sending RPCs to the target node.
    ///
    /// This function should **not** create a connection but rather a client that will connect when
    /// required. Therefore, there is a chance it will build a client that is unable to send out
    /// anything, e.g., in case the Node network address is configured incorrectly. But this method
    /// does not return an error because openraft can only ignore it.
    ///
    /// The method is intentionally async to give the implementation a chance to use asynchronous
    /// sync primitives to serialize access to the common internal object, if needed.
    async fn new_client(&mut self, target: C::NodeId, node: &C::Node) -> Self::Network;

    /// The most bytes [`new_client()`](Self::new_client),
    /// [`new_heartbeat_client()`](Self::new_heartbeat_client) or
    /// [`new_snapshot_client()`](Self::new_snapshot_client) allocates for a client to `target` at
    /// `node`, and the client holds until it is dropped: what it copies of the node, say. The
    /// client's own size is not counted: the task that holds it is charged for it.
    ///
    /// A node [`run`](crate::Raft::run) with a funder charges these bytes to the funding the
    /// client is made for before it asks for the client, and holds the charge until the client
    /// is dropped; refused, it asks for none. `None`, the default, tells that the factory cannot
    /// say: a funded node then makes no client. A factory whose clients allocate nothing, or fund
    /// what they allocate themselves, returns `Some(0)`.
    #[since(version = "0.10.0")]
    fn new_client_bytes(&self, target: &C::NodeId, node: &C::Node) -> Option<usize> {
        let _ = (target, node);
        None
    }

    /// The most bytes a [`Backoff`](crate::network::Backoff) given by
    /// [`backoff()`](crate::network::NetBackoff::backoff) of a client to `target` at `node`
    /// allocates, the box [`Backoff::new`](crate::network::Backoff::new) puts it in
    /// ([`Backoff::new_bytes`](crate::network::Backoff::new_bytes)) and what its iterator holds.
    /// A replication stream or snapshot transmitter holds one such backoff at a time.
    ///
    /// A node [`run`](crate::Raft::run) with a funder charges these bytes with the client, before
    /// it is made, and holds them as long as the client. `None`, the default, tells that the
    /// factory cannot say: a funded node then does not ask the client for a backoff, and backs off
    /// by [`Config::build_backoff`](crate::Config::build_backoff), which allocates nothing.
    #[since(version = "0.10.0")]
    fn backoff_bytes(&self, target: &C::NodeId, node: &C::Node) -> Option<usize> {
        let _ = (target, node);
        None
    }

    /// Create a network client dedicated to sending heartbeat RPCs to the target node.
    ///
    /// This client carries the Leader's liveness probes: the periodic heartbeat that maintains the
    /// Leader lease and the lease-confirmation probe issued for linearizable reads. These must not
    /// be delayed by replication backpressure. On a transport that cannot multiplex a single
    /// connection (e.g., HTTP/1.1), sharing the replication client lets a large `AppendEntries`
    /// batch or a snapshot transfer stall heartbeats through head-of-line blocking. Override this
    /// method to isolate these probes on an independent connection when that matters.
    ///
    /// The default implementation delegates to [`new_client()`](Self::new_client), so heartbeats
    /// share the replication path unless overridden.
    #[since(version = "0.10.0")]
    async fn new_heartbeat_client(&mut self, target: C::NodeId, node: &C::Node) -> Self::Network {
        self.new_client(target, node).await
    }

    /// Create a network client dedicated to transmitting snapshots to the target node.
    ///
    /// Snapshot transfers can be large and long-running. On a transport that cannot multiplex a
    /// single connection, sharing the replication client lets an in-flight snapshot block ordinary
    /// log replication. Override this method to isolate snapshot transfer on its own connection.
    ///
    /// The default implementation delegates to [`new_client()`](Self::new_client), so snapshot
    /// transfer shares the replication path unless overridden.
    #[since(version = "0.10.0")]
    async fn new_snapshot_client(&mut self, target: C::NodeId, node: &C::Node) -> Self::Network {
        self.new_client(target, node).await
    }
}

/// What clients `factory` makes to `targets` allocate between them, as
/// [`RaftNetworkFactory::new_client_bytes`] tells for each: `None` if it cannot tell for any, or
/// the sum overflows.
pub(crate) fn clients_bytes<'a, C, NF, I>(factory: &NF, targets: I) -> Option<usize>
where
    C: RaftTypeConfig,
    NF: RaftNetworkFactory<C>,
    I: Iterator<Item = (&'a C::NodeId, &'a C::Node)>,
{
    let mut bytes = 0usize;
    for (target, node) in targets {
        bytes = bytes.checked_add(factory.new_client_bytes(target, node)?)?;
    }
    Some(bytes)
}
