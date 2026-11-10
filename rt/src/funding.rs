//! Funding for the memory a Raft node allocates on behalf of its owner.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use pin_project_lite::pin_project;

/// What a Raft node charges, before it makes each, the allocations it makes for its owner: the
/// channels its callers wait on and its leader's state. A charge refused allocates nothing and
/// is answered with an error; a charge granted is credited back only once what it funded is
/// freed.
pub trait Funder: Send + Sync + 'static {
    /// Charges `bytes`, or refuses them; refusing charges nothing.
    fn charge(&self, bytes: usize) -> bool;

    /// Credits back `bytes` an earlier [`charge`](Self::charge) took, once what they funded is
    /// freed.
    fn release(&self, bytes: usize);
}

/// `bytes` charged to a [`Funder`], credited back when this is dropped. Whatever holds it beside
/// what it funds drops it after that: as a field, it is declared after the allocation it funds.
pub struct Charge {
    bytes: usize,
    funder: Arc<dyn Funder>,
}

impl Charge {
    /// `bytes` charged to `funder`, if it grants them. Another handle on `funder` is taken, which
    /// allocates nothing.
    pub fn take(funder: &Arc<dyn Funder>, bytes: usize) -> Option<Charge> {
        if !funder.charge(bytes) {
            return None;
        }
        Some(Charge {
            bytes,
            funder: funder.clone(),
        })
    }

    /// `bytes` its holder already charged to `funder`, as part of a larger charge it waited for,
    /// handed over to a charge of their own: credited back to `funder` when this is dropped, as
    /// a charge [`take`](Self::take)n is, and no longer by what held them. Another handle on
    /// `funder` is taken, which allocates nothing.
    pub fn prepaid(funder: &Arc<dyn Funder>, bytes: usize) -> Charge {
        Charge {
            bytes,
            funder: funder.clone(),
        }
    }

    /// The bytes it holds charged.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// `bytes` of this charge split off into a charge of their own, credited back when that is
    /// dropped and no longer when this is; `None`, this charge unchanged, if it holds fewer.
    /// Allocates nothing.
    pub fn split(&mut self, bytes: usize) -> Option<Charge> {
        self.bytes = self.bytes.checked_sub(bytes)?;
        Some(Charge {
            bytes,
            funder: self.funder.clone(),
        })
    }

    /// `other`, charged to the same funder, joined to this charge: its bytes are credited back
    /// when this is dropped, and no longer when `other` is. Allocates nothing.
    pub fn merge(&mut self, mut other: Charge) {
        debug_assert!(
            Arc::ptr_eq(&self.funder, &other.funder),
            "merged a charge from another funder"
        );
        self.bytes += std::mem::take(&mut other.bytes);
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.funder.release(self.bytes);
        }
    }
}

impl fmt::Debug for Charge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Charge").field("bytes", &self.bytes).finish()
    }
}

pin_project! {
    /// A future run holding the charge that funds what it holds: declared after the future, the
    /// charge is credited back only once the future, and every allocation it holds, is freed,
    /// whether it completed, was cancelled, or was given back unspawned.
    pub struct Charged<F> {
        #[pin]
        future: F,
        _charge: Charge,
    }
}

impl<F> Charged<F> {
    /// `future`, holding `charge` until it is freed.
    pub fn new(future: F, charge: Charge) -> Self {
        Self {
            future,
            _charge: charge,
        }
    }
}

impl<F: Future> Future for Charged<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.project().future.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;

    use super::Charge;
    use super::Charged;
    use super::Funder;

    /// A funder that records whether a charge came back before what it funded was freed.
    #[derive(Default)]
    struct Ledger {
        held: AtomicUsize,
        freed: AtomicBool,
        early: AtomicBool,
    }

    impl Funder for Ledger {
        fn charge(&self, bytes: usize) -> bool {
            self.held.fetch_add(bytes, Ordering::SeqCst);
            true
        }

        fn release(&self, bytes: usize) {
            if !self.freed.load(Ordering::SeqCst) {
                self.early.store(true, Ordering::SeqCst);
            }
            self.held.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    /// A future holding what its charge funds, freed when it is dropped.
    struct Holding(Arc<Ledger>);

    impl Future for Holding {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }

    impl Drop for Holding {
        fn drop(&mut self) {
            self.0.freed.store(true, Ordering::SeqCst);
        }
    }

    /// A charged future given back unspawned is dropped before its charge is credited back:
    /// what the future held is freed first.
    #[test]
    fn a_charged_future_is_freed_before_its_charge_goes_back() {
        let ledger = Arc::new(Ledger::default());
        let funder: Arc<dyn Funder> = ledger.clone();
        let charge = Charge::take(&funder, 64).expect("granted");
        let charged = Charged::new(Holding(ledger.clone()), charge);
        assert_eq!(64, ledger.held.load(Ordering::SeqCst));

        drop(charged);

        assert!(ledger.freed.load(Ordering::SeqCst));
        assert!(
            !ledger.early.load(Ordering::SeqCst),
            "credited back only after the future is freed"
        );
        assert_eq!(0, ledger.held.load(Ordering::SeqCst));
    }
}
