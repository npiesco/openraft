use crate::OptionalSend;
use crate::OptionalSync;

/// What accounts for the memory of a [`Membership`] a storage implementation returns, for
/// example a reservation charged before the membership was decoded.
///
/// A membership given an owner ([`Membership::with_owner`]) keeps its members in one block every
/// clone shares, so the copies openraft keeps of it, in its membership state, its metrics, a
/// snapshot meta or a response, allocate nothing. When the last clone is dropped, the block is
/// freed, then the members' memory, and only then is the owner released: what it accounts for is
/// given back only once nothing it accounts for is still allocated.
///
/// The owner is boxed by the implementation that charged for it, so the box is its memory too:
/// [`MembershipOwner::release`] frees the box before giving anything back.
///
/// [`Membership`]: crate::Membership
/// [`Membership::with_owner`]: crate::Membership::with_owner
pub trait MembershipOwner: OptionalSend + OptionalSync + 'static {
    /// Gives back what the membership's memory was charged to. Called once, when the members and
    /// the block they shared are freed; the implementation frees `self`'s box before it gives
    /// anything back.
    fn release(self: Box<Self>);
}
