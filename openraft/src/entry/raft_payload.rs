use std::fmt::Debug;
use std::fmt::Display;

use openraft_macros::since;

use crate::AppData;
use crate::Membership;
use crate::base::OptionalFeatures;
use crate::errors::MembershipRefused;
use crate::node::Node;
use crate::node::NodeId;

/// Defines operations for constructing and inspecting an entry payload.
#[since(
    version = "0.10.0",
    change = "replaced generic parameters with associated types and added construction methods"
)]
#[since(
    version = "0.10.0",
    change = "replaced `C: RaftTypeConfig` with `NID: NodeId, N: Node`"
)]
pub trait RaftPayload
where Self: OptionalFeatures + Debug + Display + Sized + 'static
{
    /// Application-specific data stored in the payload.
    #[since(version = "0.10.0")]
    type D: AppData;

    /// The node ID type used in memberships.
    #[since(version = "0.10.0")]
    type NodeId: NodeId;

    /// The node type used in memberships.
    #[since(version = "0.10.0")]
    type Node: Node;

    /// Create a blank payload.
    ///
    /// OpenRaft uses a new blank payload as the base of a membership-change entry when the
    /// application does not supply a payload.
    #[since(version = "0.10.0")]
    fn blank() -> Self;

    /// Replace the normal application data in this payload.
    #[since(version = "0.10.0")]
    fn with_normal(self, data: Self::D) -> Self;

    /// Replace the membership in this payload.
    ///
    /// OpenRaft calls this method for every physical membership-change entry. An implementation
    /// may preserve other application fields.
    #[since(version = "0.10.0")]
    fn with_membership(self, membership: Membership<Self::NodeId, Self::Node>) -> Self;

    /// Bind a membership OpenRaft proposes into this payload, or refuse it.
    ///
    /// OpenRaft binds every membership it proposes through this method: the one a membership
    /// change computes, the one a direct membership append is given and the one initialization
    /// builds. A refusal answers that call with [`MembershipRefused`], and no entry is written.
    /// The default binds it with [`with_membership()`](Self::with_membership) and never refuses.
    #[since(version = "0.10.0")]
    fn bind_membership(self, membership: Membership<Self::NodeId, Self::Node>) -> Result<Self, MembershipRefused> {
        Ok(self.with_membership(membership))
    }

    /// Return `Some(Membership)` if the entry payload contains a membership payload.
    #[since(version = "0.10.0", change = "use associated node types")]
    fn get_membership(&self) -> Option<Membership<Self::NodeId, Self::Node>>;

    /// Create a payload containing normal application data.
    #[since(version = "0.10.0")]
    fn normal(data: Self::D) -> Self {
        let payload = Self::blank();
        payload.with_normal(data)
    }

    /// Create a payload containing a membership.
    #[since(version = "0.10.0")]
    fn membership(membership: Membership<Self::NodeId, Self::Node>) -> Self {
        let payload = Self::blank();
        payload.with_membership(membership)
    }
}
