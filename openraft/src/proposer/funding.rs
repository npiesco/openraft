//! What a node's proposer state allocates, charged to the node's funder before it is.
//!
//! A pre-candidate's and a candidate's vote tracker, a Leader's replication and clock progress,
//! and the rebuild of that progress for an appended membership are each charged, sized from the
//! membership they track, before any of them is allocated, and credited back only once they are
//! freed: the charge is held by the state it funds, declared after what it funds. A node
//! [`run`](crate::Raft::run) without a funder charges nothing.

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use openraft_macros::since;

use crate::Membership;
use crate::OptionalSend;
use crate::OptionalSync;
use crate::RaftTypeConfig;
use crate::async_runtime::AsyncRuntime;
use crate::async_runtime::Charge;
use crate::async_runtime::Funder;
use crate::async_runtime::Oneshot;
use crate::async_runtime::watch::Watch;
use crate::errors::Unfunded;
use crate::node::Node;
use crate::node::NodeId;
use crate::progress::IdVal;
use crate::progress::entry::ProgressEntry;
use crate::proposer::SendStamp;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::AsyncRuntimeOf;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::OneshotOf;
use crate::type_config::alias::OneshotReceiverOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::WatchOf;
use crate::type_config::alias::WatchReceiverOf;
use crate::type_config::alias::WatchSenderOf;

/// The funder a node's proposer state is charged to, if the node was run with one.
#[derive(Clone, Default)]
pub(crate) struct Funding(Option<Arc<dyn Funder>>);

impl Funding {
    pub(crate) fn new(funder: Option<Arc<dyn Funder>>) -> Self {
        Self(funder)
    }

    /// `bytes` charged before what they fund is allocated, or refused, charging nothing. A node
    /// without a funder charges nothing: `Ok(None)`.
    pub(crate) fn charge(&self, bytes: usize) -> Result<Option<Charge>, Unfunded> {
        match &self.0 {
            None => Ok(None),
            Some(funder) => Charge::take(funder, bytes).map(Some).ok_or(Unfunded),
        }
    }

    /// Whether the node has a funder its allocations are charged to.
    pub(crate) fn is_funded(&self) -> bool {
        self.0.is_some()
    }

    /// A oneshot channel carrying `T`, its block charged before it is made and held until both
    /// halves are dropped and the block is freed: refused as [`Unfunded`], nothing allocated, as
    /// [`OneshotCharge::take`] is.
    pub(crate) fn oneshot<C, T>(&self) -> Result<(OneshotSenderOf<C, T>, OneshotReceiverOf<C, T>), Unfunded>
    where
        C: RaftTypeConfig,
        T: OptionalSend,
    {
        OneshotCharge::<C, T>::take(self.0.as_ref(), 1)?.channel()
    }
}

/// `more` joined to the charge `held` holds, credited back with it.
pub(crate) fn merge(held: &mut Option<Charge>, more: Option<Charge>) {
    let Some(more) = more else {
        return;
    };
    if let Some(charge) = held.as_mut() {
        charge.merge(more);
    } else {
        *held = Some(more);
    }
}

impl PartialEq for Funding {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for Funding {}

impl fmt::Debug for Funding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Funding").field(&self.0.is_some()).finish()
    }
}

/// The charge, taken from a node's funder before any of them is made, for oneshot channels
/// carrying `T` that are yet to be made; for none, uncharged, on a node run unfunded. Dropped,
/// what it still holds is credited back.
pub(crate) struct OneshotCharge<C, T> {
    /// The charge, and the bytes of each channel's block it funds.
    charge: Option<(Charge, usize)>,
    _channel: PhantomData<fn() -> (C, T)>,
}

impl<C, T> OneshotCharge<C, T>
where
    C: RaftTypeConfig,
    T: OptionalSend,
{
    /// The blocks of `count` channels charged to `funder`, all of them before any is made: refused
    /// as [`Unfunded`], nothing charged, if `funder` refuses them or the runtime cannot fund
    /// channels. Without a funder, nothing is charged and the channels are made uncharged.
    pub(crate) fn take(funder: Option<&Arc<dyn Funder>>, count: usize) -> Result<Self, Unfunded> {
        let charge = match funder {
            Some(funder) => {
                let block = OneshotOf::<C>::block_bytes::<T>().ok_or(Unfunded)?;
                let bytes = block.checked_mul(count).ok_or(Unfunded)?;
                Some((Charge::take(funder, bytes).ok_or(Unfunded)?, block))
            }
            None => None,
        };
        Ok(Self {
            charge,
            _channel: PhantomData,
        })
    }

    /// One of the channels it was taken for, its block funded by its share of the charge, held
    /// until both halves are dropped and the block is freed; refused as [`Unfunded`], nothing
    /// allocated, once every channel it was taken for is made.
    pub(crate) fn channel(&mut self) -> Result<(OneshotSenderOf<C, T>, OneshotReceiverOf<C, T>), Unfunded> {
        match &mut self.charge {
            Some((charge, block)) => {
                let block = charge.split(*block).ok_or(Unfunded)?;
                OneshotOf::<C>::charged_channel(block).map_err(|_| Unfunded)
            }
            None => Ok(C::oneshot()),
        }
    }
}

/// A task running `T` yet to be spawned, and what it holds, charged to a node's funder before any
/// of it is made; uncharged on a node run unfunded. Each part is made from its share of the
/// charge, which is held by the part until it is freed; dropped, what it still holds is credited
/// back.
pub(crate) struct TaskCharge<C, T> {
    charge: Option<Charge>,
    _task: PhantomData<fn() -> (C, T)>,
}

impl<C, T> TaskCharge<C, T>
where
    C: RaftTypeConfig,
    T: Future + OptionalSend + 'static,
    T::Output: OptionalSend + 'static,
{
    /// The task's own allocations and `more` bytes of what it holds, charged to `funding`'s
    /// funder before any of them is made: refused as [`Unfunded`], nothing charged, if the funder
    /// refuses them, or the runtime cannot fund tasks or a part `more` sizes, which it tells by
    /// `None`. `more` is asked only of a funded node.
    pub(crate) fn take(funding: &Funding, more: impl FnOnce() -> Option<usize>) -> Result<Self, Unfunded> {
        let charge = match &funding.0 {
            Some(funder) => {
                let task = AsyncRuntimeOf::<C>::spawn_bytes::<T>().ok_or(Unfunded)?;
                let bytes = task.checked_add(more().ok_or(Unfunded)?).ok_or(Unfunded)?;
                Some(Charge::take(funder, bytes).ok_or(Unfunded)?)
            }
            None => None,
        };
        Ok(Self {
            charge,
            _task: PhantomData,
        })
    }

    /// `bytes` of the charge split off for a part the task holds, to be held with that part
    /// until it is freed: refused as [`Unfunded`] if the charge holds fewer; `None` unfunded.
    pub(crate) fn split(&mut self, bytes: usize) -> Result<Option<Charge>, Unfunded> {
        match &mut self.charge {
            Some(charge) => charge.split(bytes).map(Some).ok_or(Unfunded),
            None => Ok(None),
        }
    }

    /// A oneshot channel the task holds, its block funded by its share of the charge, held until
    /// both halves are dropped and the block is freed: refused as [`Unfunded`], nothing made, if
    /// the charge holds too few.
    pub(crate) fn oneshot<U>(&mut self) -> Result<(OneshotSenderOf<C, U>, OneshotReceiverOf<C, U>), Unfunded>
    where U: OptionalSend {
        if self.charge.is_none() {
            return Ok(C::oneshot());
        }
        let block = OneshotOf::<C>::block_bytes::<U>().ok_or(Unfunded)?;
        let charge = self.split(block)?.ok_or(Unfunded)?;
        OneshotOf::<C>::charged_channel(charge).map_err(|_| Unfunded)
    }

    /// A watch channel the task holds, starting at `init`, its block funded by its share of the
    /// charge, held until every handle is dropped and the block is freed: refused as
    /// [`Unfunded`], nothing made, if the charge holds too few.
    pub(crate) fn watch<U>(&mut self, init: U) -> Result<(WatchSenderOf<C, U>, WatchReceiverOf<C, U>), Unfunded>
    where U: OptionalSend + OptionalSync {
        if self.charge.is_none() {
            return Ok(C::watch_channel(init));
        }
        let block = WatchOf::<C>::block_bytes::<U>().ok_or(Unfunded)?;
        let charge = self.split(block)?.ok_or(Unfunded)?;
        WatchOf::<C>::charged_channel(charge, init).map_err(|_| Unfunded)
    }

    /// Spawns `future`, everything the runtime allocates for the task funded by what is left of
    /// the charge and held until that is freed: refused as [`Unfunded`], `future` dropped and
    /// nothing allocated, if what is left holds too few.
    pub(crate) fn spawn(self, future: T) -> Result<JoinHandleOf<C, T::Output>, Unfunded> {
        match self.charge {
            Some(charge) => AsyncRuntimeOf::<C>::spawn_charged(charge, future).map_err(|_| Unfunded),
            None => Ok(C::spawn(future)),
        }
    }
}

const fn larger(a: usize, b: usize) -> usize {
    if a > b { a } else { b }
}

/// Every buffer a vector of at most `len` slots of `T` is given while it is built by collects,
/// pushes and extends, summed as if none were freed: std grows a vector to at least twice its
/// capacity, from at least eight slots, so the buffers it outgrew add up to less than its last
/// one, which is less than twice its length.
const fn vec_bytes<T>(len: usize) -> usize {
    4 * larger(len, 8) * size_of::<T>()
}

/// The scratch std's stable sort allocates to sort `len` items of `T`: never more than the larger
/// of `len` and its smallest heap scratch, 48 items.
const fn sort_bytes<T>(len: usize) -> usize {
    larger(len, 48) * size_of::<T>()
}

/// Every node a B-tree map of at most `len` entries of `K` and `V` (a set has `V = ()`) holds,
/// whether built by inserts or in bulk from sorted entries: every node but the root holds at
/// least five entries, so there are at most `len / 5 + 1` leaves and no more internal nodes than
/// leaves, each charged as [`tree_node_bytes`].
pub(crate) const fn tree_bytes<K, V>(len: usize) -> usize {
    2 * (len / 5 + 1) * tree_node_bytes::<K, V>()
}

/// One node of a B-tree map of `K` and `V`, charged as an internal node, the larger: a parent
/// link, its index and length, eleven keys and eleven values each padded to its alignment, and
/// twelve child links, padded to the largest alignment. A map of one entry or more holds no more
/// nodes than entries, as every node holds at least one; an emptied map keeps its root until it
/// is replaced.
pub(crate) const fn tree_node_bytes<K, V>() -> usize {
    let link = size_of::<usize>();
    link + 2 * size_of::<u16>()
        + 11 * (size_of::<K>() + size_of::<V>())
        + align_of::<K>()
        + align_of::<V>()
        + 12 * link
        + larger(link, larger(align_of::<K>(), align_of::<V>()))
}

/// What collecting at most `len` items into a `BTreeSet<T>` allocates: std collects them into a
/// vector, sorts it and builds the tree from it.
const fn set_bytes<T>(len: usize) -> usize {
    vec_bytes::<T>(len) + sort_bytes::<T>(len) + tree_bytes::<T, ()>(len)
}

/// What collecting at most `len` pairs into a `BTreeMap<K, V>` allocates, as [`set_bytes`].
const fn map_bytes<K, V>(len: usize) -> usize {
    vec_bytes::<(K, V)>(len) + sort_bytes::<(K, V)>(len) + tree_bytes::<K, V>(len)
}

/// What building a progress tracker of `E` entries for `voters` and `learners` allocates: the
/// set of voter ids its membership quorum set yields, the voter and learner sets it collects
/// them into, its entries and the sort of its voters.
const fn progress_bytes<Id, E>(voters: usize, learners: usize) -> usize {
    tree_bytes::<Id, ()>(voters)
        + set_bytes::<Id>(voters)
        + set_bytes::<Id>(learners)
        + vec_bytes::<E>(voters + learners)
        + sort_bytes::<E>(voters)
}

/// The clock progress entry a Leader tracks per node.
type ClockEntry<C> = IdVal<<C as RaftTypeConfig>::NodeId, Option<SendStamp<InstantOf<C>>>>;

/// What building a Candidate for `voters` and `learners` allocates: its learner ids and its vote
/// progress.
const fn candidate_bytes<C: RaftTypeConfig>(voters: usize, learners: usize) -> usize {
    vec_bytes::<C::NodeId>(learners) + progress_bytes::<C::NodeId, IdVal<C::NodeId, bool>>(voters, 0)
}

/// What building a Leader for `voters` and `learners` allocates: the learner ids it collects, its
/// replication progress and its clock progress.
const fn leader_bytes<C: RaftTypeConfig>(voters: usize, learners: usize) -> usize {
    vec_bytes::<C::NodeId>(learners)
        + progress_bytes::<C::NodeId, ProgressEntry<C>>(voters, learners)
        + progress_bytes::<C::NodeId, ClockEntry<C>>(voters, learners)
}

/// What rebuilding a Leader's progress of `tracked` entries for `voters` and `learners`
/// allocates, the rebuilt progress included: the new learner ids and their copy, and per tracker
/// a copy of the old one, the map its entries are moved into and the new tracker.
const fn rebuild_bytes<C: RaftTypeConfig>(tracked: usize, voters: usize, learners: usize) -> usize {
    2 * vec_bytes::<C::NodeId>(learners)
        + vec_bytes::<ProgressEntry<C>>(tracked)
        + map_bytes::<C::NodeId, ProgressEntry<C>>(tracked)
        + progress_bytes::<C::NodeId, ProgressEntry<C>>(voters, learners)
        + vec_bytes::<ClockEntry<C>>(tracked)
        + map_bytes::<C::NodeId, ClockEntry<C>>(tracked)
        + progress_bytes::<C::NodeId, ClockEntry<C>>(voters, learners)
}

/// At most how many voters and learners a proposer built from `membership` tracks: its distinct
/// voters, no more than the ids of all its configs nor than its nodes, every voter being one,
/// and its nodes.
fn tracked<NID: NodeId, N: Node>(membership: &Membership<NID, N>) -> (usize, usize) {
    let nodes = membership.nodes().count();
    let voters = membership.get_joint_config().iter().map(|config| config.len()).sum::<usize>();
    (voters.min(nodes), nodes)
}

/// What a Candidate built from `membership` is charged.
pub(crate) fn candidate_state_bytes<C: RaftTypeConfig>(membership: &Membership<C::NodeId, C::Node>) -> usize {
    let (voters, learners) = tracked(membership);
    candidate_bytes::<C>(voters, learners)
}

/// What a Leader built from `membership` is charged, by an election or without one.
pub(crate) fn leader_state_bytes<C: RaftTypeConfig>(membership: &Membership<C::NodeId, C::Node>) -> usize {
    let (voters, learners) = tracked(membership);
    leader_bytes::<C>(voters, learners)
}

/// What appending `membership` to a Leader whose progress tracks `tracked_now` nodes is charged:
/// the rebuild of its progress, of which the Leader keeps [`leader_state_bytes`] for the rebuilt
/// progress in place of its own charge.
pub(crate) fn rebuild_state_bytes<C: RaftTypeConfig>(
    tracked_now: usize,
    membership: &Membership<C::NodeId, C::Node>,
) -> usize {
    let (voters, learners) = tracked(membership);
    rebuild_bytes::<C>(tracked_now, voters, learners)
}

/// The most bytes a node run with a funder holds charged at once for its proposer state, for
/// memberships of at most `members` nodes: a Pre-Vote round's Candidate, an election's Candidate
/// with the Leader it is charged for in advance, two Leaders (one built without an election
/// while an older one is still held), and the rebuild of a Leader's progress for an appended
/// membership. Every voter is a node, and a progress tracks no more entries than nodes, so each
/// is sized as `members` voters, `members` learners and `members` tracked entries.
#[since(version = "0.10.0")]
pub const fn proposer_state_bytes<C: RaftTypeConfig>(members: usize) -> usize {
    let candidate = candidate_bytes::<C>(members, members);
    let leader = leader_bytes::<C>(members, members);
    2 * candidate + 3 * leader + rebuild_bytes::<C>(members, members, members)
}

#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::Funding;
    use crate::async_runtime::Funder;

    /// A funder granting at most `limit` bytes held at once, counting what it holds.
    pub(crate) struct Limit {
        limit: usize,
        held: AtomicUsize,
    }

    impl Limit {
        pub(crate) fn new(limit: usize) -> Arc<Self> {
            Arc::new(Self {
                limit,
                held: AtomicUsize::new(0),
            })
        }

        pub(crate) fn held(&self) -> usize {
            self.held.load(Ordering::SeqCst)
        }

        /// Proposer state charged to this funder.
        pub(crate) fn funding(self: &Arc<Self>) -> Funding {
            let funder: Arc<dyn Funder> = self.clone();
            Funding::new(Some(funder))
        }
    }

    impl Funder for Limit {
        fn charge(&self, bytes: usize) -> bool {
            self.held
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                    held.checked_add(bytes).filter(|held| *held <= self.limit)
                })
                .is_ok()
        }

        fn release(&self, bytes: usize) {
            self.held.fetch_sub(bytes, Ordering::SeqCst);
        }
    }
}
