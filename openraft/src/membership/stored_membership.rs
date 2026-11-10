use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use display_more::DisplayOptionExt;
use openraft_macros::since;

use crate::Membership;
use crate::log_id::LogId;
use crate::membership::membership::arc_block_bytes;
use crate::node::Node;
use crate::node::NodeId;
use crate::quorum::QuorumSet;
use crate::vote::RaftCommittedLeaderId;

/// This struct represents information about a membership config that has already been stored in the
/// raft logs.
///
/// It includes log id and a membership config. Such a record is used in the state machine or
/// snapshot to track the last membership and its log id. And it is also used as a return value for
/// functions that return membership and its log position.
///
/// It derives `Default` for building an uninitialized membership state, e.g., when a raft-node is
/// just created.
#[since(
    version = "0.10.0",
    change = "from `StoredMembership<C>` to `StoredMembership<CLID, NID, N>`"
)]
#[since(version = "0.8.0")]
#[derive(Clone, Debug)]
#[derive(PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct StoredMembership<CLID, NID, N>
where
    CLID: RaftCommittedLeaderId,
    NID: NodeId,
    N: Node,
{
    /// The id of the log that stores this membership config
    log_id: Option<LogId<CLID>>,

    /// Membership config
    membership: Membership<NID, N>,
}

impl<CLID, NID, N> Default for StoredMembership<CLID, NID, N>
where
    CLID: RaftCommittedLeaderId,
    NID: NodeId,
    N: Node,
{
    fn default() -> Self {
        Self {
            log_id: None,
            membership: Membership::default(),
        }
    }
}

impl<CLID, NID, N> StoredMembership<CLID, NID, N>
where
    CLID: RaftCommittedLeaderId,
    NID: NodeId,
    N: Node,
{
    /// Create a new StoredMembership with the given log ID and membership configuration.
    pub fn new(log_id: Option<LogId<CLID>>, membership: Membership<NID, N>) -> Self {
        Self { log_id, membership }
    }

    pub(crate) fn new_arc(log_id: Option<LogId<CLID>>, membership: Membership<NID, N>) -> Arc<Self> {
        Arc::new(Self::new(log_id, membership))
    }

    /// The bytes of the `Arc` blocks OpenRaft allocates around one owned membership
    /// ([`Membership::with_owner`]) while it holds a clone of it, which that membership's owner
    /// accounts for besides [`Membership::OWNED_BLOCK_BYTES`].
    ///
    /// Each block holds a clone that shares the owner, so it is freed just after the owner is
    /// released, when its last clone drops. At most this many are held at once for one owner:
    ///
    /// - Two `Arc<StoredMembership>`: the membership state's committed and effective membership.
    ///   The effective one is built by `FollowingHandler::append_membership` from an entry,
    ///   `FollowingHandler::membership_install_snapshot`, `ReplicationHandler::append_membership`
    ///   (`StoredMembership::new_arc`) or `StorageHelper::get_membership`, which wraps one
    ///   membership twice when no log entry holds a later one. Committing shares the effective
    ///   block.
    /// - Five `Arc<Membership>` quorum sets: one each for a pre-candidate and a candidate
    ///   (`Engine::new_pre_candidate` and `Engine::new_candidate`), two for a leader's progress
    ///   and clock progress (`ReplicationHandler::rebuild_progresses`, or one both share, built
    ///   by `RaftState::new_leader` or inherited from the candidate), and one more for the
    ///   block any of these replaces, which is freed only after its replacement is built.
    ///
    /// The membership state's default blocks and the metrics' blocks are either empty, holding
    /// no owner, or clones of the blocks above, which allocate nothing.
    pub const OWNED_ARC_BYTES: usize = 2 * arc_block_bytes::<Self>() + 5 * arc_block_bytes::<Membership<NID, N>>();

    /// Get the log ID at which this membership was stored.
    pub fn log_id(&self) -> &Option<LogId<CLID>> {
        &self.log_id
    }

    /// Get the membership configuration.
    pub fn membership(&self) -> &Membership<NID, N> {
        &self.membership
    }

    #[allow(dead_code)]
    pub(crate) fn is_voter(&self, nid: &NID) -> bool {
        self.membership.is_voter(nid)
    }

    /// Get an iterator over the voter node IDs.
    pub fn voter_ids(&self) -> impl Iterator<Item = NID> {
        self.membership.voter_ids()
    }

    /// Returns an Iterator of all learner node ids. Voters are not included.
    pub(crate) fn learner_ids(&self) -> impl Iterator<Item = NID> + '_ {
        self.membership.learner_ids()
    }

    /// Get the node (either voter or learner) by node id.
    #[since(version = "0.10.0")]
    pub fn get_node(&self, node_id: &NID) -> Option<&N> {
        self.membership.get_node(node_id)
    }

    /// Get an iterator over all nodes (ID and node information).
    pub fn nodes(&self) -> impl Iterator<Item = (&NID, &N)> {
        self.membership.nodes()
    }

    /// Returns reference to the joint config.
    ///
    /// Membership is defined by a joint of multiple configs.
    /// Each config is a set of node-id.
    #[since(version = "0.10.0")]
    pub fn get_joint_config(&self) -> &Vec<BTreeSet<NID>> {
        self.membership.get_joint_config()
    }
}

impl<CLID, NID, N> fmt::Display for StoredMembership<CLID, NID, N>
where
    CLID: RaftCommittedLeaderId,
    NID: NodeId,
    N: Node,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{log_id:{}, {}}}", self.log_id.display(), self.membership)
    }
}

/// Implement node-id joint quorum set.
impl<CLID, NID, N> QuorumSet for StoredMembership<CLID, NID, N>
where
    CLID: RaftCommittedLeaderId,
    NID: NodeId,
    N: Node,
{
    type Id = NID;
    type Iter = std::collections::btree_set::IntoIter<NID>;

    fn is_quorum<'a, I: Iterator<Item = &'a NID> + Clone>(&self, ids: I) -> bool {
        self.membership.is_quorum(ids)
    }

    fn ids(&self) -> Self::Iter {
        self.membership.ids()
    }
}
