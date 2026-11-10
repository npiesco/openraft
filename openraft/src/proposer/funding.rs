//! What a node's proposer state allocates, charged to the node's funder before it is.
//!
//! A pre-candidate's and a candidate's vote tracker, a Leader's replication and clock progress,
//! and the rebuild of that progress for an appended membership are each charged, sized from the
//! membership they track, before any of them is allocated, and credited back only once they are
//! freed: the charge is held by the state it funds, declared after what it funds. A node
//! [`run`](crate::Raft::run) without a funder charges nothing.

use std::collections::TryReserveError;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::Arc;

use openraft_macros::since;

use crate::Membership;
use crate::OptionalSend;
use crate::OptionalSync;
use crate::RaftTypeConfig;
use crate::async_runtime::AsyncRuntime;
use crate::async_runtime::Charge;
use crate::async_runtime::Charged;
use crate::async_runtime::Funder;
use crate::async_runtime::Mpsc;
use crate::async_runtime::Oneshot;
use crate::async_runtime::Select2;
use crate::async_runtime::watch::Watch;
use crate::engine::TargetProgress;
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
use crate::type_config::alias::MpscOf;
use crate::type_config::alias::MpscReceiverOf;
use crate::type_config::alias::MpscSenderOf;
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

    /// `count` tasks running `T`, each holding the one sleep it makes, and `held` bytes more the
    /// tasks hold between them, charged together before any of them is made, to be spawned from a
    /// [`Prepaid`] of the charge, each by [`Prepaid::sleeping`] and [`Prepaid::spawn`]: refused,
    /// charging nothing, if the funder refuses them or the runtime cannot fund a task or a sleep.
    /// A node without a funder charges nothing: `Ok(None)`.
    pub(crate) fn sleeping_tasks<C, T>(&self, count: usize, held: usize) -> Result<Option<Charge>, Unfunded>
    where
        C: RaftTypeConfig,
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        if !self.is_funded() {
            return Ok(None);
        }
        let task = spawned_bytes::<C, T>().ok_or(Unfunded)?;
        let each = task.checked_add(sleep_bytes::<C>().ok_or(Unfunded)?).ok_or(Unfunded)?;
        self.charge(each.checked_mul(count).and_then(|tasks| tasks.checked_add(held)).ok_or(Unfunded)?)
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

    /// Room in `queue` for `room` more items. On a node with a funder, the whole capacity it
    /// grows to is charged before it is reserved and held in `held` in place of the charge for
    /// the capacity it had, credited back only once the buffer that capacity was is freed; refused
    /// as [`Unfunded`], nothing is allocated or charged. An unfunded node's queue grows as it is
    /// pushed to.
    pub(crate) fn reserve<B: Buffer>(
        &self,
        queue: &mut B,
        held: &mut Option<Charge>,
        room: usize,
    ) -> Result<(), Unfunded> {
        let needed = queue.items().saturating_add(room);
        if !self.is_funded() || queue.slots() >= needed {
            return Ok(());
        }
        let capacity = needed.max(queue.slots().saturating_mul(2));
        let charge = self.charge(capacity.saturating_mul(size_of::<B::Item>()))?;
        queue.grow_exact(capacity - queue.items()).map_err(|_| Unfunded)?;
        *held = charge;
        Ok(())
    }

    /// Gives back what the capacity of `queue` is charged beyond what its items need: once it
    /// holds a quarter of its slots or fewer, and has more than [`KEPT_SLOTS`], it is shrunk to
    /// twice its items, at least [`KEPT_SLOTS`]. The smaller buffer is charged before it is
    /// allocated and held in `held` in place of the larger one's charge, credited back only once
    /// the larger buffer is freed. Left as it is if it holds no charge, or the smaller buffer is
    /// refused.
    pub(crate) fn shrink<B: Buffer>(&self, queue: &mut B, held: &mut Option<Charge>) {
        let slot = size_of::<B::Item>();
        let slots = queue.slots();
        if held.is_none() || slot == 0 || slots <= KEPT_SLOTS || queue.items().saturating_mul(4) > slots {
            return;
        }
        let capacity = queue.items().saturating_mul(2).max(KEPT_SLOTS);
        let Ok(charge) = self.charge(capacity * slot) else {
            return;
        };
        queue.shrink_exact(capacity);
        debug_assert!(queue.slots() <= capacity, "shrunk to the capacity charged");
        *held = charge;
    }
}

/// The slots below which [`Funding::shrink`] keeps a queue as it is, and to which at least it
/// shrinks one: a queue drained each turn is not freed and grown again each turn.
pub(crate) const KEPT_SLOTS: usize = 32;

/// A buffer of contiguous slots, grown by [`Funding::reserve`] once the capacity it grows to is
/// charged.
pub(crate) trait Buffer {
    type Item;

    /// The items it holds.
    fn items(&self) -> usize;

    /// The items it has room for without growing.
    fn slots(&self) -> usize;

    /// Room for `more` items than it holds, exactly, or the allocator's refusal.
    fn grow_exact(&mut self, more: usize) -> Result<(), TryReserveError>;

    /// Its slots shrunk to `capacity`, or to the items it holds if they are more.
    fn shrink_exact(&mut self, capacity: usize);
}

impl<T> Buffer for VecDeque<T> {
    type Item = T;

    fn items(&self) -> usize {
        self.len()
    }

    fn slots(&self) -> usize {
        self.capacity()
    }

    fn grow_exact(&mut self, more: usize) -> Result<(), TryReserveError> {
        self.try_reserve_exact(more)
    }

    fn shrink_exact(&mut self, capacity: usize) {
        self.shrink_to(capacity)
    }
}

impl<T> Buffer for Vec<T> {
    type Item = T;

    fn items(&self) -> usize {
        self.len()
    }

    fn slots(&self) -> usize {
        self.capacity()
    }

    fn grow_exact(&mut self, more: usize) -> Result<(), TryReserveError> {
        self.try_reserve_exact(more)
    }

    fn shrink_exact(&mut self, capacity: usize) {
        self.shrink_to(capacity)
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

/// The charge for parts of a node yet to be made, taken from its funder before any of them is;
/// uncharged on a node without a funder. Each part is made from its share of the charge, which
/// the part holds until it is freed; dropped, what is left is credited back. `R` is what a part
/// is refused with, made from [`Unfunded`]; a node made without a funder makes its parts uncharged
/// and never refuses one.
pub(crate) struct Prepaid<C, R = Unfunded> {
    /// The charge, and how a part it holds too few for, or the runtime cannot make, is refused.
    charge: Option<(Charge, fn(Unfunded) -> R)>,
    _parts: PhantomData<fn() -> C>,
}

impl<C, R> Prepaid<C, R> {
    /// Nothing charged: every part is made uncharged, and none is refused.
    pub(crate) fn unfunded() -> Self {
        Self {
            charge: None,
            _parts: PhantomData,
        }
    }

    /// `charge` for the parts to be made, a part it holds too few for refused as `refuse` makes
    /// [`Unfunded`]; without a charge, they are made uncharged.
    pub(crate) fn new(charge: Option<Charge>, refuse: fn(Unfunded) -> R) -> Self {
        Self {
            charge: charge.map(|charge| (charge, refuse)),
            _parts: PhantomData,
        }
    }
}

impl<C, R> Prepaid<C, R>
where C: RaftTypeConfig
{
    /// What is left, to make parts refused as [`Unfunded`].
    pub(crate) fn rest(self) -> Prepaid<C> {
        Prepaid::new(self.charge.map(|(charge, _)| charge), std::convert::identity)
    }

    /// `held` bytes and those of one sleep, as [`sleep_bytes`] tells, for a task that sleeps to
    /// hold: refused if the runtime cannot fund its timers; just `held`, uncharged.
    pub(crate) fn sleeping(&self, held: usize) -> Result<usize, R> {
        match &self.charge {
            Some((_, refuse)) => {
                sleep_bytes::<C>().and_then(|sleep| sleep.checked_add(held)).ok_or_else(|| refuse(Unfunded))
            }
            None => Ok(held),
        }
    }

    /// `bytes` of the charge split off for a part, to be held with that part until it is freed:
    /// refused if the charge holds fewer; `None` uncharged.
    pub(crate) fn split(&mut self, bytes: usize) -> Result<Option<Charge>, R> {
        match &mut self.charge {
            Some((charge, refuse)) => charge.split(bytes).map(Some).ok_or_else(|| refuse(Unfunded)),
            None => Ok(None),
        }
    }

    /// A oneshot channel, its block funded by its share of the charge, held until both halves are
    /// dropped and the block is freed: refused, nothing made, if the charge holds too few.
    pub(crate) fn oneshot<U>(&mut self) -> Result<(OneshotSenderOf<C, U>, OneshotReceiverOf<C, U>), R>
    where U: OptionalSend {
        let Some((_, refuse)) = &self.charge else {
            return Ok(C::oneshot());
        };
        let refuse = *refuse;
        let block = OneshotOf::<C>::block_bytes::<U>().ok_or_else(|| refuse(Unfunded))?;
        let charge = self.split(block)?.ok_or_else(|| refuse(Unfunded))?;
        OneshotOf::<C>::charged_channel(charge).map_err(|_| refuse(Unfunded))
    }

    /// A watch channel starting at `init`, its block funded by its share of the charge, held
    /// until every handle is dropped and the block is freed: refused, nothing made, if the charge
    /// holds too few.
    pub(crate) fn watch<U>(&mut self, init: U) -> Result<(WatchSenderOf<C, U>, WatchReceiverOf<C, U>), R>
    where U: OptionalSend + OptionalSync {
        let Some((_, refuse)) = &self.charge else {
            return Ok(C::watch_channel(init));
        };
        let refuse = *refuse;
        let block = WatchOf::<C>::block_bytes::<U>().ok_or_else(|| refuse(Unfunded))?;
        let charge = self.split(block)?.ok_or_else(|| refuse(Unfunded))?;
        WatchOf::<C>::charged_channel(charge, init).map_err(|_| refuse(Unfunded))
    }

    /// A bounded channel of `buffer` values, everything it allocates in its life made now and
    /// funded by its share of the charge, held until every handle is dropped and all of it is
    /// freed: refused, nothing made, if the charge holds too few.
    pub(crate) fn mpsc<U>(&mut self, buffer: usize) -> Result<(MpscSenderOf<C, U>, MpscReceiverOf<C, U>), R>
    where U: OptionalSend {
        let Some((_, refuse)) = &self.charge else {
            return Ok(C::mpsc(buffer));
        };
        let refuse = *refuse;
        let block = MpscOf::<C>::block_bytes::<U>(buffer).ok_or_else(|| refuse(Unfunded))?;
        let charge = self.split(block)?.ok_or_else(|| refuse(Unfunded))?;
        MpscOf::<C>::charged_channel(charge, buffer).map_err(|_| refuse(Unfunded))
    }

    /// `value` in a block its handles share, the block funded by its share of the charge, held
    /// until the last handle frees the block and `value`: refused, `value` dropped and nothing
    /// made, if the charge holds too few.
    pub(crate) fn shared<T>(&mut self, value: T) -> Result<Shared<T>, R> {
        let charge = self.split(Shared::<T>::block_bytes())?;
        Ok(Shared::new(value, charge))
    }

    /// Spawns `future`, everything the runtime allocates for the task funded by its share of the
    /// charge and held until that is freed, and `held` bytes more held by the task's future,
    /// after `future`, to fund what `future` holds until that is freed: refused, `future` dropped
    /// before any of its share goes back and nothing allocated, if the charge holds too few or
    /// the runtime cannot make the task.
    pub(crate) fn spawn<T>(&mut self, future: T, held: usize) -> Result<JoinHandleOf<C, T::Output>, R>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        self.spawn_with(held, || future)
    }

    /// [`Self::spawn`] of the future `make` makes, made only once both shares are split off, so
    /// what it allocates for its `held` bytes is funded before it is: refused, nothing made, if
    /// the charge holds too few.
    pub(crate) fn spawn_with<T>(
        &mut self,
        held: usize,
        make: impl FnOnce() -> T,
    ) -> Result<JoinHandleOf<C, T::Output>, R>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        let Some((charge, refuse)) = &mut self.charge else {
            return Ok(C::spawn(make()));
        };
        let refuse = *refuse;
        let Some(task) = spawned_bytes::<C, T>().and_then(|bytes| charge.split(bytes)) else {
            return Err(refuse(Unfunded));
        };
        let Some(held) = charge.split(held) else {
            drop(make);
            drop(task);
            return Err(refuse(Unfunded));
        };
        AsyncRuntimeOf::<C>::spawn_charged(task, Charged::new(make(), held)).map_err(|_| refuse(Unfunded))
    }
}

/// A value in a block shared by its handles, as an `Arc` is, the block charged before it was made:
/// the last handle frees the block, then the value, and only then credits the charge back.
pub(crate) struct Shared<T> {
    block: ManuallyDrop<Arc<Funded<T>>>,
}

/// The block a [`Shared`] value's handles share.
struct Funded<T> {
    value: T,
    charge: Option<Charge>,
}

impl<T> Shared<T> {
    /// What the block a [`Shared`] `T` is made in allocates.
    pub(crate) const fn block_bytes() -> usize {
        arc_block_bytes::<Funded<T>>()
    }

    /// `value` moved into a block of [`Self::block_bytes`] funded by `charge`, uncharged without
    /// one.
    pub(crate) fn new(value: T, charge: Option<Charge>) -> Self {
        Self {
            block: ManuallyDrop::new(Arc::new(Funded { value, charge })),
        }
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self {
            block: ManuallyDrop::new(Arc::clone(&self.block)),
        }
    }
}

impl<T> Deref for Shared<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.block.value
    }
}

impl<T> Drop for Shared<T> {
    /// The last handle frees the block, then the value, and only then credits the charge back.
    fn drop(&mut self) {
        // SAFETY: `block` is taken once, here, and the handle is never used again.
        let block = unsafe { ManuallyDrop::take(&mut self.block) };
        if let Some(Funded { value, charge }) = Arc::into_inner(block) {
            drop(value);
            drop(charge);
        }
    }
}

/// A task running `T` yet to be spawned, and what it holds, charged to a node's funder before any
/// of it is made; uncharged on a node run unfunded. Each part is made from its share of the
/// charge, which is held by the part until it is freed; dropped, what it still holds is credited
/// back.
pub(crate) struct TaskCharge<C, T> {
    parts: Prepaid<C>,
    _task: PhantomData<fn() -> T>,
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
        Self::take_from(funding.0.as_ref(), more)
    }

    /// As [`Self::take`], charged to `funder`; uncharged without one.
    pub(crate) fn take_from(
        funder: Option<&Arc<dyn Funder>>,
        more: impl FnOnce() -> Option<usize>,
    ) -> Result<Self, Unfunded> {
        let charge = match funder {
            Some(funder) => {
                let task = spawned_bytes::<C, T>().ok_or(Unfunded)?;
                let bytes = task.checked_add(more().ok_or(Unfunded)?).ok_or(Unfunded)?;
                Some(Charge::take(funder, bytes).ok_or(Unfunded)?)
            }
            None => None,
        };
        Ok(Self {
            parts: Prepaid::new(charge, std::convert::identity),
            _task: PhantomData,
        })
    }

    /// `bytes` of the charge split off for a part the task holds, to be held with that part
    /// until it is freed: refused as [`Unfunded`] if the charge holds fewer; `None` unfunded.
    pub(crate) fn split(&mut self, bytes: usize) -> Result<Option<Charge>, Unfunded> {
        self.parts.split(bytes)
    }

    /// A oneshot channel the task holds, as [`Prepaid::oneshot`] makes it.
    pub(crate) fn oneshot<U>(&mut self) -> Result<(OneshotSenderOf<C, U>, OneshotReceiverOf<C, U>), Unfunded>
    where U: OptionalSend {
        self.parts.oneshot()
    }

    /// A watch channel the task holds, as [`Prepaid::watch`] makes it.
    pub(crate) fn watch<U>(&mut self, init: U) -> Result<(WatchSenderOf<C, U>, WatchReceiverOf<C, U>), Unfunded>
    where U: OptionalSend + OptionalSync {
        self.parts.watch(init)
    }

    /// A bounded channel the task holds, as [`Prepaid::mpsc`] makes it.
    pub(crate) fn mpsc<U>(&mut self, buffer: usize) -> Result<(MpscSenderOf<C, U>, MpscReceiverOf<C, U>), Unfunded>
    where U: OptionalSend {
        self.parts.mpsc(buffer)
    }

    /// Spawns `future`, everything the runtime allocates for the task funded by its share of the
    /// charge and held until that is freed, and what is left held by the task's future, after
    /// `future`, to fund what `future` holds until that is freed: refused as [`Unfunded`],
    /// `future` dropped before any of the charge goes back and nothing allocated, if what is left
    /// holds too few.
    pub(crate) fn spawn(mut self, future: T) -> Result<JoinHandleOf<C, T::Output>, Unfunded> {
        let held = match &self.parts.charge {
            Some((charge, _)) => {
                let Some(task) = spawned_bytes::<C, T>() else {
                    drop(future);
                    return Err(Unfunded);
                };
                charge.bytes().saturating_sub(task)
            }
            None => 0,
        };
        self.parts.spawn(future, held)
    }
}

/// What the runtime charges a task running the future `task` makes, as [`TaskCharge::take`] asks
/// it: the future's type named by the function that makes it.
pub(crate) fn task_bytes<C, A, T>(_task: impl FnOnce(A) -> T) -> Option<usize>
where
    C: RaftTypeConfig,
    T: Future + OptionalSend + 'static,
    T::Output: OptionalSend + 'static,
{
    spawned_bytes::<C, T>()
}

/// The bytes the box of a stream `futures_util::stream::unfold(init, step)` makes allocates,
/// its state of type `T`: the stream's type named by the function `step`.
pub(crate) fn unfold_bytes<T, F, Fut>(_step: F) -> usize
where
    F: FnMut(T) -> Fut,
    Fut: Future,
{
    size_of::<futures_util::stream::Unfold<T, F, Fut>>()
}

/// What the runtime charges a task running `T` as [`TaskCharge::spawn`] spawns it, holding what
/// is left of its charge after it.
pub(crate) fn spawned_bytes<C, T>() -> Option<usize>
where
    C: RaftTypeConfig,
    T: Future + OptionalSend + 'static,
    T::Output: OptionalSend + 'static,
{
    AsyncRuntimeOf::<C>::spawn_bytes::<Charged<T>>()
}

/// What one sleep a funded task makes allocates until it is dropped, its timer's registration
/// included, as [`AsyncRuntime::sleep_bytes`] tells: held by a task that sleeps, which makes one
/// sleep at a time, or by one that runs [`within`]. `None` if the runtime cannot fund its timers.
pub(crate) fn sleep_bytes<C: RaftTypeConfig>() -> Option<usize> {
    AsyncRuntimeOf::<C>::sleep_bytes()
}

/// The deadline passed before the future [`within`] raced it against completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline elapsed")
    }
}

/// The output of `future`, or [`Elapsed`] if `deadline` passes first: a timeout that allocates
/// one sleep, which [`sleep_bytes`] charges, whatever `future` is, where the runtime's own timeout
/// may box `future`. `future` is polled first, as the runtime's timeout polls it.
pub(crate) async fn within<C, F>(deadline: InstantOf<C>, future: F) -> Result<F::Output, Elapsed>
where
    C: RaftTypeConfig,
    F: Future + OptionalSend,
{
    match C::select2(future, C::sleep_until(deadline)).await {
        Select2::First(output) => Ok(output),
        Select2::Second(()) => Err(Elapsed),
    }
}

/// The bytes an `Arc<T>` allocates for its block: its strong and weak counts, then the value,
/// padded to the block's alignment.
pub(crate) const fn arc_block_bytes<T>() -> usize {
    let value = align_of::<T>();
    let align = if value > align_of::<usize>() {
        value
    } else {
        align_of::<usize>()
    };
    let offset = (2 * size_of::<usize>()).next_multiple_of(value);
    (offset + size_of::<T>()).next_multiple_of(align)
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

/// One node of a B-tree map of `K` and `V`, charged as an internal node, the larger, as
/// [`openraft_rt::funding::tree_node_bytes`] tells. A map of one entry or more holds no more
/// nodes than entries, as every node holds at least one; an emptied map keeps its root until it
/// is replaced.
pub(crate) const fn tree_node_bytes<K, V>() -> usize {
    openraft_rt::funding::tree_node_bytes::<K, V>()
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
/// replication progress and its clock progress; and the targets it rebuilds its replication
/// streams and heartbeat workers for, split from its charge as it rebuilds them.
const fn leader_bytes<C: RaftTypeConfig>(voters: usize, learners: usize) -> usize {
    vec_bytes::<C::NodeId>(learners)
        + progress_bytes::<C::NodeId, ProgressEntry<C>>(voters, learners)
        + progress_bytes::<C::NodeId, ClockEntry<C>>(voters, learners)
        + targets_bytes::<C>(learners)
}

/// The targets a Leader rebuilds its replication streams and heartbeat workers for, `targets` of
/// them, each reserved exactly before any is pushed.
pub(crate) const fn targets_bytes<C: RaftTypeConfig>(targets: usize) -> usize {
    targets * size_of::<TargetProgress<C>>()
}

/// What rebuilding a Leader's progress of `tracked` entries for `voters` and `learners`
/// allocates, the rebuilt progress included: the new learner ids and their copy, per tracker a
/// copy of the old one, the map its entries are moved into and the new tracker, and the targets
/// of the replication streams rebuilt for it.
const fn rebuild_bytes<C: RaftTypeConfig>(tracked: usize, voters: usize, learners: usize) -> usize {
    2 * vec_bytes::<C::NodeId>(learners)
        + vec_bytes::<ProgressEntry<C>>(tracked)
        + map_bytes::<C::NodeId, ProgressEntry<C>>(tracked)
        + progress_bytes::<C::NodeId, ProgressEntry<C>>(voters, learners)
        + vec_bytes::<ClockEntry<C>>(tracked)
        + map_bytes::<C::NodeId, ClockEntry<C>>(tracked)
        + progress_bytes::<C::NodeId, ClockEntry<C>>(voters, learners)
        + targets_bytes::<C>(learners)
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::convert::identity;
    use std::future::Future;
    use std::future::pending;
    use std::future::ready;
    use std::task::Context;
    use std::task::Waker;
    use std::time::Duration;

    use super::Elapsed;
    use super::Funding;
    use super::KEPT_SLOTS;
    use super::Prepaid;
    use super::Shared;
    use super::testing::Limit;
    use super::within;
    use crate::engine::testing::UTConfig;
    use crate::errors::Unfunded;
    use crate::metered::metered;
    use crate::type_config::TypeConfigExt;

    type C = UTConfig;

    /// A queue drained to a quarter of its slots or fewer is shrunk, and its charge drops to
    /// what the smaller buffer holds; it is kept at [`KEPT_SLOTS`] at least.
    #[test]
    fn test_shrink_gives_back_the_charge_of_the_slots_it_frees() {
        let slot = size_of::<u64>();
        let limit = Limit::new(usize::MAX);
        let funding = limit.funding();
        let mut queue = VecDeque::<u64>::new();
        let mut held = None;

        funding.reserve(&mut queue, &mut held, 1024).unwrap();
        queue.extend(0..1024);
        assert_eq!(queue.capacity() * slot, limit.held());

        queue.drain(..1000).for_each(drop);
        funding.shrink(&mut queue, &mut held);
        assert_eq!(48, queue.capacity(), "shrunk to twice its items");
        assert_eq!(48 * slot, limit.held(), "charged only the smaller buffer");

        queue.clear();
        funding.shrink(&mut queue, &mut held);
        assert_eq!(
            KEPT_SLOTS,
            queue.capacity(),
            "kept at the slots below which it is not shrunk"
        );
        assert_eq!(KEPT_SLOTS * slot, limit.held());

        funding.shrink(&mut queue, &mut held);
        assert_eq!(KEPT_SLOTS, queue.capacity());

        drop(queue);
        drop(held);
        assert_eq!(0, limit.held());
    }

    /// A shrink the funder refuses the smaller buffer for leaves the queue and its charge as they
    /// are, allocating nothing.
    #[test]
    fn test_a_refused_shrink_allocates_nothing() {
        let slot = size_of::<u64>();
        let limit = Limit::new(1024 * slot);
        let funding = limit.funding();
        let mut queue = VecDeque::<u64>::new();
        let mut held = None;
        funding.reserve(&mut queue, &mut held, 1024).unwrap();
        queue.extend(0..1024);
        queue.drain(..1000).for_each(drop);
        let (capacity, charged) = (queue.capacity(), limit.held());

        let ((), meter) = metered(|| funding.shrink(&mut queue, &mut held));

        assert_eq!(0, meter.allocated, "refused, nothing allocated");
        assert_eq!(capacity, queue.capacity());
        assert_eq!(charged, limit.held());
    }

    /// A funded block held across a stall, by a future pending forever, is charged as long as
    /// it is live; the stall cannot wedge the pool: an admission beyond what is left is refused at
    /// once, not waited for, and once the stalled future is dropped and the block freed, the
    /// charge is back and the same admission is granted.
    #[test]
    fn test_custody_stall_holds_its_charge_and_cannot_wedge_the_pool() {
        let block = Shared::<[u8; 64]>::block_bytes();
        let limit = Limit::new(block);
        let funding = limit.funding();

        let mut prepaid: Prepaid<C> = Prepaid::new(funding.charge(block).unwrap(), identity);
        let (shared, meter) = metered(|| prepaid.shared([7u8; 64]).unwrap());
        drop(prepaid);
        assert_eq!(
            block,
            meter.kept.unsigned_abs(),
            "the block is charged what it allocates"
        );
        assert_eq!(block, limit.held());

        let mut stalled = Box::pin(async move {
            let _custody = shared;
            pending::<()>().await
        });
        let mut cx = Context::from_waker(Waker::noop());
        assert!(stalled.as_mut().poll(&mut cx).is_pending());
        assert!(stalled.as_mut().poll(&mut cx).is_pending());
        assert_eq!(block, limit.held(), "charged while the stall holds it");

        let (refused, meter) = metered(|| funding.charge(1));
        assert_eq!(Some(Unfunded), refused.err(), "refused at once, not waited for");
        assert_eq!(0, meter.allocated);
        assert_eq!(block, limit.held());

        drop(stalled);
        assert_eq!(0, limit.held(), "freed, its charge is back");
        let granted = funding.charge(block).unwrap();
        assert!(granted.is_some());
        assert_eq!(block, limit.held());
    }

    /// A funded task that sleeps is refused before anything is made for it if the runtime cannot
    /// fund a sleep, as the test runtime cannot; an unfunded one holds just what it holds.
    #[test]
    fn test_sleeping_is_refused_if_the_runtime_cannot_fund_a_sleep() {
        let limit = Limit::new(usize::MAX);
        let prepaid: Prepaid<C> = Prepaid::new(limit.funding().charge(64).unwrap(), identity);
        assert_eq!(Err(Unfunded), prepaid.sleeping(8));
        drop(prepaid);
        assert_eq!(0, limit.held());

        let unfunded: Prepaid<C> = Prepaid::unfunded();
        assert_eq!(Ok(8), unfunded.sleeping(8));
    }

    /// Tasks that sleep, charged together, are refused, charging nothing, if the runtime cannot
    /// fund a task or a sleep; uncharged on a node without a funder.
    #[test]
    fn test_sleeping_tasks_are_charged_together_or_refused() {
        let limit = Limit::new(usize::MAX);
        let refused = limit.funding().sleeping_tasks::<C, std::future::Ready<()>>(3, 64);
        assert!(matches!(refused, Err(Unfunded)));
        assert_eq!(0, limit.held());

        let unfunded = Funding::default().sleeping_tasks::<C, std::future::Ready<()>>(3, 64);
        assert!(matches!(unfunded, Ok(None)));
    }

    /// [`within`] yields the future's output if it completes first, and [`Elapsed`] once the
    /// deadline passes before it does.
    #[test]
    fn test_within() {
        C::run(async {
            let done = within::<C, _>(C::now() + Duration::from_secs(5), ready(3)).await;
            assert_eq!(Ok(3), done);

            let elapsed = within::<C, _>(C::now() + Duration::from_millis(10), pending::<()>()).await;
            assert_eq!(Err(Elapsed), elapsed);

            let first = within::<C, _>(C::now(), ready(4)).await;
            assert_eq!(Ok(4), first, "a ready future is polled before the deadline");
        });
    }
}
