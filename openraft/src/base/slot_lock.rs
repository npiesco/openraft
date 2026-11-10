use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

/// How many tasks wait on a [`SlotLock`] by name; any more re-poll themselves until a slot frees.
pub(crate) const WAIT_SLOTS: usize = 4;

/// An async lock whose waiters wait in `N` slots it holds inline, so contending for it allocates
/// nothing: an async `Mutex` that queues its waiters in a slab, as `futures::lock::Mutex` does,
/// grows it, unfunded, as they contend.
///
/// A waiter that finds every slot taken wakes itself at once and tries again when next polled.
pub(crate) struct SlotLock<T, const N: usize = WAIT_SLOTS> {
    state: Mutex<Waiting<N>>,
    value: UnsafeCell<T>,
}

// SAFETY: the value is reached only through a guard, which `locked` makes the only one.
unsafe impl<T: Send, const N: usize> Send for SlotLock<T, N> {}
// SAFETY: as above: a shared `SlotLock` hands its value to one guard at a time.
unsafe impl<T: Send, const N: usize> Sync for SlotLock<T, N> {}

struct Waiting<const N: usize> {
    locked: bool,
    slots: [Slot; N],
}

enum Slot {
    Free,
    Waiting(Waker),
    /// Woken by an unlock; it passes the wake on if it stops waiting without taking the lock.
    Woken,
}

impl<T, const N: usize> SlotLock<T, N> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            state: Mutex::new(Waiting {
                locked: false,
                slots: [const { Slot::Free }; N],
            }),
            value: UnsafeCell::new(value),
        }
    }

    pub(crate) fn lock(&self) -> Lock<'_, T, N> {
        Lock { lock: self, slot: None }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, Waiting<N>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<const N: usize> Waiting<N> {
    fn wake_one(&mut self) {
        for slot in self.slots.iter_mut() {
            if !matches!(slot, Slot::Waiting(_)) {
                continue;
            }
            if let Slot::Waiting(waker) = std::mem::replace(slot, Slot::Woken) {
                waker.wake();
            }
            return;
        }
    }
}

/// Waiting for a [`SlotLock`].
pub(crate) struct Lock<'a, T, const N: usize> {
    lock: &'a SlotLock<T, N>,
    slot: Option<usize>,
}

impl<'a, T, const N: usize> Future for Lock<'a, T, N> {
    type Output = SlotGuard<'a, T, N>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this.lock.state();

        if !state.locked {
            state.locked = true;
            if let Some(i) = this.slot.take() {
                state.slots[i] = Slot::Free;
            }
            return Poll::Ready(SlotGuard {
                lock: this.lock,
                _value: PhantomData,
            });
        }

        // Its own slot, kept from an earlier poll, or the first free one.
        let free = state.slots.iter().position(|s| matches!(s, Slot::Free));
        match this.slot.or(free) {
            Some(i) => {
                match &mut state.slots[i] {
                    Slot::Waiting(waker) => waker.clone_from(cx.waker()),
                    slot => *slot = Slot::Waiting(cx.waker().clone()),
                }
                this.slot = Some(i);
            }
            None => {
                drop(state);
                cx.waker().wake_by_ref();
            }
        }
        Poll::Pending
    }
}

impl<T, const N: usize> Drop for Lock<'_, T, N> {
    fn drop(&mut self) {
        let Some(i) = self.slot else {
            return;
        };
        let mut state = self.lock.state();
        let woken = matches!(state.slots[i], Slot::Woken);
        state.slots[i] = Slot::Free;
        if woken && !state.locked {
            state.wake_one();
        }
    }
}

/// The value of a locked [`SlotLock`]; dropping it unlocks it and wakes one waiter.
pub(crate) struct SlotGuard<'a, T, const N: usize> {
    lock: &'a SlotLock<T, N>,
    /// Shared across threads only as `&mut T` would be.
    _value: PhantomData<&'a mut T>,
}

impl<T, const N: usize> Deref for SlotGuard<'_, T, N> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: this guard is the only one: `locked` is set until it drops.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T, const N: usize> DerefMut for SlotGuard<'_, T, N> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as in `deref`, and the guard is borrowed mutably.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T, const N: usize> Drop for SlotGuard<'_, T, N> {
    fn drop(&mut self) {
        let mut state = self.lock.state();
        state.locked = false;
        state.wake_one();
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::SlotLock;
    use crate::metered::metered;

    /// The lock `futures` offers queues its waiters in a slab that grows as they contend: an
    /// allocation nothing funds. A `SlotLock` makes none, with more waiters than slots too.
    #[test]
    fn test_contention_allocates_nothing() {
        let mut cx = Context::from_waker(Waker::noop());

        let slab = futures_util::lock::Mutex::new(0u64);
        let held = slab.try_lock().unwrap();
        let mut waiter = pin!(slab.lock());
        let ((), meter) = metered(|| assert!(waiter.as_mut().poll(&mut cx).is_pending()));
        assert!(meter.allocated > 0, "the futures lock grows its waiter slab");
        drop(held);

        let lock = SlotLock::<u64, 2>::new(0);
        let ((), meter) = metered(|| {
            let mut held = pin!(lock.lock());
            let Poll::Ready(mut held) = held.as_mut().poll(&mut cx) else {
                panic!("an unlocked lock is taken at once");
            };
            *held += 1;

            let mut first = pin!(lock.lock());
            let mut second = pin!(lock.lock());
            let mut third = pin!(lock.lock());
            assert!(first.as_mut().poll(&mut cx).is_pending());
            assert!(second.as_mut().poll(&mut cx).is_pending());
            assert!(
                third.as_mut().poll(&mut cx).is_pending(),
                "with every slot taken it waits by re-polling"
            );

            drop(held);
            let Poll::Ready(mut taken) = first.as_mut().poll(&mut cx) else {
                panic!("an unlock lets a waiter take it");
            };
            *taken += 1;
            assert!(second.as_mut().poll(&mut cx).is_pending());
            assert!(third.as_mut().poll(&mut cx).is_pending());
        });
        assert_eq!(0, meter.allocated, "waiters wait in slots the lock holds");

        let mut last = pin!(lock.lock());
        let Poll::Ready(value) = last.as_mut().poll(&mut cx) else {
            panic!("every guard and waiter is dropped");
        };
        assert_eq!(2, *value);
    }

    /// A waiter woken by an unlock that stops waiting passes the wake on, so the lock is not left
    /// free with others still asleep in their slots.
    #[test]
    fn test_a_dropped_woken_waiter_passes_the_wake_on() {
        let mut cx = Context::from_waker(Waker::noop());
        let lock = SlotLock::<(), 2>::new(());

        let held = lock.try_lock_for_test();
        let mut first = Box::pin(lock.lock());
        let mut second = pin!(lock.lock());
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());

        drop(held);
        assert!(lock.is_woken(0) && lock.is_waiting(1));

        drop(first);
        assert!(lock.is_woken(1), "the wake passed on to the other waiter");
        assert!(second.as_mut().poll(&mut cx).is_ready());
    }

    impl<T, const N: usize> SlotLock<T, N> {
        fn try_lock_for_test(&self) -> super::SlotGuard<'_, T, N> {
            let mut cx = Context::from_waker(Waker::noop());
            let mut lock = pin!(self.lock());
            match lock.as_mut().poll(&mut cx) {
                Poll::Ready(guard) => guard,
                Poll::Pending => panic!("locked"),
            }
        }

        fn is_woken(&self, i: usize) -> bool {
            matches!(self.state().slots[i], super::Slot::Woken)
        }

        fn is_waiting(&self, i: usize) -> bool {
            matches!(self.state().slots[i], super::Slot::Waiting(_))
        }
    }
}
