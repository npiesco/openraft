//! Queue of client responders indexed by log index, optimized for sequential
//! insertion and range extraction.

use std::collections::VecDeque;

use crate::async_runtime::Charge;
use crate::errors::UnfundedWrite;
use crate::proposer::funding;
use crate::proposer::funding::Funding;

/// Queue of client responders indexed by log index.
///
/// Responders must be pushed in monotonically increasing order.
/// In debug builds, non-monotonic pushes panic.
///
/// On a funded node, room for responders is [`reserve`](Self::reserve)d before they are pushed:
/// the queue's buffer grown for them, and each one's slot in the vector it is later drained into
/// to be applied, charged before either is allocated and credited back once it is freed.
///
/// # Performance
///
/// - `push()`: O(1) amortized
/// - `extend()`: O(k) where k = items added
/// - `drain_upto()`: O(k) where k = drained count
/// - `drain_from()`: O(log n + k) where n = total responders, k = drained count
/// - `first_index()`, `len()`, `is_empty()`: O(1)
pub(crate) struct ClientResponderQueue<T> {
    /// Responders stored as (log_index, responder) pairs in sorted order
    responders: VecDeque<(u64, T)>,

    /// What the buffer of `responders` is charged, declared after it so that it is credited back
    /// only once the buffer is freed.
    buffer: Option<Charge>,

    /// The slots of the queued responders in the vectors they are drained into, charged when
    /// they were reserved and split off with them by [`drain_upto`](Self::drain_upto).
    slots: Option<Charge>,
}

impl<T> ClientResponderQueue<T> {
    /// What one responder takes, in the queue's buffer and in the vector it is drained into.
    const SLOT: usize = size_of::<(u64, T)>();

    /// Creates a new empty responder queue.
    pub(crate) fn new() -> Self {
        Self {
            responders: VecDeque::new(),
            buffer: None,
            slots: None,
        }
    }

    /// Room for `count` more responders, charged to `funding` in one charge before any of it is
    /// allocated: the queue's buffer grown to hold them, at least doubled, and their slots in the
    /// vector they are drained into. Refused, nothing is charged and nothing allocated, and the
    /// refusal names exactly the bytes asked for.
    pub(crate) fn reserve(&mut self, count: usize, funding: &Funding) -> Result<(), UnfundedWrite> {
        if count == 0 {
            return Ok(());
        }
        let len = self.responders.len();
        let need = len + count;
        let capacity = self.responders.capacity();
        let grown = if need > capacity { need.max(2 * capacity) } else { 0 };

        let bytes = (count + grown) * Self::SLOT;
        let mut slots = funding.charge(bytes).map_err(|_| UnfundedWrite::new(bytes))?;
        if grown > 0 {
            let buffer = slots.as_mut().and_then(|slots| slots.split(grown * Self::SLOT));
            // The old buffer stays charged until the grown one replaced it and it was freed.
            self.responders.reserve_exact(grown - len);
            self.buffer = buffer;
        }
        funding::merge(&mut self.slots, slots);
        Ok(())
    }

    /// Gives back the charge of the buffer's slots once few of them hold responders, as
    /// [`Funding::shrink`] does.
    pub(crate) fn shrink(&mut self, funding: &Funding) {
        funding.shrink(&mut self.responders, &mut self.buffer)
    }

    /// Pushes a responder to the end of the queue.
    ///
    /// # Panics
    ///
    /// In debug builds, panics if `index <= last_index`.
    pub(crate) fn push(&mut self, index: u64, responder: T) {
        #[cfg(debug_assertions)]
        if let Some((last_index, _)) = self.responders.back() {
            debug_assert!(
                index > *last_index,
                "Responder indices must be monotonically increasing: tried to push {} after {}",
                index,
                last_index
            );
        }

        self.responders.push_back((index, responder));
    }

    /// Extends the queue with multiple responders.
    ///
    /// # Panics
    ///
    /// In debug builds, panics if indices are not monotonically increasing.
    #[allow(dead_code)]
    pub(crate) fn extend(&mut self, iter: impl IntoIterator<Item = (u64, T)>) {
        for (index, responder) in iter {
            self.push(index, responder);
        }
    }

    /// Drains responders from the beginning up to and including `last_index`.
    ///
    /// Returns matching responders and removes them from the queue.
    /// Not all indices in the range may have responders.
    ///
    /// The vector is exactly as long as the responders drained, and comes with the charge
    /// [`reserve`](Self::reserve) took for their slots: its holder drops it only once the vector
    /// is freed.
    pub(crate) fn drain_upto(&mut self, last_index: u64) -> (Vec<(u64, T)>, Option<Charge>) {
        let end_pos = self.responders.partition_point(|(index, _)| *index <= last_index);
        let charge = self.split_slots(end_pos);
        let mut drained = Vec::with_capacity(end_pos);
        drained.extend(self.responders.drain(0..end_pos));
        (drained, charge)
    }

    /// The charge for `count` queued responders' slots, split off what is held for all of them.
    fn split_slots(&mut self, count: usize) -> Option<Charge> {
        let bytes = count * Self::SLOT;
        let slots = self.slots.as_mut()?;
        // Responders pushed without a reservation have no charged slot.
        slots.split(bytes.min(slots.bytes()))
    }

    /// Drains all responders from the specified index onwards.
    ///
    /// Returns an iterator over responders with `log_index >= from_index` and removes them.
    /// Used when truncating logs to notify affected responders.
    pub(crate) fn drain_from(&mut self, from_index: u64) -> impl Iterator<Item = (u64, T)> + '_ {
        let start_pos =
            self.responders.binary_search_by_key(&from_index, |(index, _)| *index).unwrap_or_else(|pos| pos);

        // Drained here and never into a vector: their slots' charge funds nothing and returns.
        drop(self.split_slots(self.responders.len() - start_pos));
        self.responders.drain(start_pos..)
    }

    /// Returns the first (smallest) log index in the queue, if any.
    #[allow(dead_code)]
    pub(crate) fn first_index(&self) -> Option<u64> {
        self.responders.front().map(|(index, _)| *index)
    }

    /// Returns the number of responders in the queue.
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.responders.len()
    }

    /// Returns true if the queue is empty.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.responders.is_empty()
    }
}

impl<T> Default for ClientResponderQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proposer::funding::KEPT_SLOTS;
    use crate::proposer::funding::testing::Limit;

    type TestQueue = ClientResponderQueue<u64>;

    #[test]
    fn test_new() {
        let queue = TestQueue::new();
        assert!(queue.is_empty());
        assert_eq!(queue.first_index(), None);
    }

    #[test]
    fn test_push() {
        let mut queue = TestQueue::new();
        queue.push(10, 100);
        queue.push(20, 200);
        queue.push(30, 300);

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.first_index(), Some(10));
        assert_eq!(queue.drain_upto(30).0, vec![(10, 100), (20, 200), (30, 300)]);
    }

    #[test]
    fn test_extend() {
        let mut queue = TestQueue::new();
        queue.extend(vec![(10, 100), (20, 200), (30, 300)]);
        assert_eq!(queue.drain_upto(30).0, vec![(10, 100), (20, 200), (30, 300)]);

        queue.push(40, 400);
        queue.extend(vec![(50, 500), (60, 600)]);
        assert_eq!(queue.drain_upto(60).0, vec![(40, 400), (50, 500), (60, 600)]);
    }

    #[test]
    fn test_drain_upto() {
        let mut queue = TestQueue::new();
        queue.push(10, 100);
        queue.push(20, 200);
        queue.push(30, 300);
        queue.push(40, 400);

        // Partial drain
        assert_eq!(queue.drain_upto(25).0, vec![(10, 100), (20, 200)]);
        assert_eq!(queue.first_index(), Some(30));

        // Drain with gaps
        queue.push(60, 600);
        assert_eq!(queue.drain_upto(50).0, vec![(30, 300), (40, 400)]);
        assert_eq!(queue.first_index(), Some(60));

        // Drain all
        assert_eq!(queue.drain_upto(100).0, vec![(60, 600)]);
        assert!(queue.is_empty());

        // Drain empty
        assert_eq!(queue.drain_upto(100).0, vec![]);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "Responder indices must be monotonically increasing")]
    fn test_push_panics_non_monotonic() {
        let mut queue = TestQueue::new();
        queue.push(20, 200);
        queue.push(10, 100);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "Responder indices must be monotonically increasing")]
    fn test_push_panics_duplicate() {
        let mut queue = TestQueue::new();
        queue.push(20, 200);
        queue.push(20, 200);
    }

    #[test]
    fn test_large_scale() {
        let mut queue = TestQueue::new();
        for i in 0..1000 {
            queue.push(i * 10, i * 10);
        }

        assert_eq!(queue.drain_upto(4990).0.len(), 500);
        assert_eq!(queue.drain_upto(9990).0.len(), 500);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_reserve_refused_charges_and_allocates_nothing() {
        let slot = size_of::<(u64, u64)>();
        let limit = Limit::new(3 * slot);
        let mut queue = TestQueue::new();

        // Two responders need 2 slots and a buffer of 2: a slot more than the limit allows.
        assert_eq!(
            queue.reserve(2, &limit.funding()),
            Err(UnfundedWrite::new(4 * slot)),
            "refused, exactly what was asked for is named"
        );
        assert_eq!(limit.held(), 0, "refused, nothing is charged");
        assert_eq!(queue.responders.capacity(), 0, "and nothing is allocated");
    }

    #[test]
    fn test_reserve_charges_until_drained_and_freed() {
        let slot = size_of::<(u64, u64)>();
        let limit = Limit::new(usize::MAX);
        let funding = limit.funding();
        let mut queue = TestQueue::new();

        queue.reserve(3, &funding).unwrap();
        assert_eq!(queue.responders.capacity(), 3);
        assert_eq!(limit.held(), 3 * slot + 3 * slot, "its buffer and its drained slots");
        queue.push(10, 100);
        queue.push(20, 200);
        queue.push(30, 300);

        // Grown, at least doubled: the old buffer's charge goes once the new one replaced it.
        queue.reserve(1, &funding).unwrap();
        assert_eq!(queue.responders.capacity(), 6);
        assert_eq!(limit.held(), 6 * slot + 4 * slot);
        queue.push(40, 400);

        let (drained, charge) = queue.drain_upto(20);
        assert_eq!(drained, vec![(10, 100), (20, 200)]);
        assert_eq!(drained.capacity(), 2);
        assert_eq!(
            charge.as_ref().map(Charge::bytes),
            Some(2 * slot),
            "the drained vector's slots"
        );
        drop(drained);
        drop(charge);
        assert_eq!(limit.held(), 6 * slot + 2 * slot);

        // Truncated responders are never drained into a vector: their slots return at once.
        assert_eq!(queue.drain_from(30).count(), 2);
        assert_eq!(limit.held(), 6 * slot);

        drop(queue);
        assert_eq!(limit.held(), 0, "the buffer's charge returns once it is freed");
    }

    /// Drained, the queue gives back the charge of the slots it emptied: its buffer shrinks to
    /// twice what it still holds, at least `KEPT_SLOTS`, charged before the larger one is freed.
    #[test]
    fn test_shrink_gives_back_the_charge_of_drained_slots() {
        let slot = size_of::<(u64, u64)>();
        let limit = Limit::new(usize::MAX);
        let funding = limit.funding();
        let mut queue = TestQueue::new();

        let count = 4 * KEPT_SLOTS as u64;
        queue.reserve(4 * KEPT_SLOTS, &funding).unwrap();
        queue.extend((1..=count).map(|i| (i, i)));
        queue.shrink(&funding);
        assert_eq!(queue.responders.capacity(), 4 * KEPT_SLOTS, "full, it is kept");

        let (drained, charge) = queue.drain_upto(count - 1);
        drop((drained, charge));
        assert_eq!(limit.held(), 4 * KEPT_SLOTS * slot + slot);

        queue.shrink(&funding);
        assert_eq!(queue.responders.capacity(), KEPT_SLOTS);
        assert_eq!(
            limit.held(),
            KEPT_SLOTS * slot + slot,
            "the emptied slots' charge is given back"
        );

        drop(queue);
        assert_eq!(limit.held(), 0);
    }

    #[test]
    fn test_drain_from() {
        let mut queue = TestQueue::new();
        queue.push(10, 100);
        queue.push(20, 200);
        queue.push(30, 300);
        queue.push(40, 400);

        // Drain from middle
        let drained: Vec<_> = queue.drain_from(25).collect();
        assert_eq!(drained, vec![(30, 300), (40, 400)]);
        assert_eq!(queue.first_index(), Some(10));

        // Drain from exact match
        let drained: Vec<_> = queue.drain_from(10).collect();
        assert_eq!(drained, vec![(10, 100), (20, 200)]);
        assert!(queue.is_empty());

        // Drain from empty
        let drained: Vec<_> = queue.drain_from(50).collect();
        assert_eq!(drained, vec![]);

        // Early break
        queue.extend(vec![(10, 100), (20, 200), (30, 300)]);
        let mut result = Vec::new();
        for (log_index, value) in queue.drain_from(20) {
            result.push((log_index, value));
            if log_index == 20 {
                break;
            }
        }
        assert_eq!(result, vec![(20, 200)]);
        assert_eq!(queue.first_index(), Some(10));
    }
}
