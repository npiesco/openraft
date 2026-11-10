//! Funding for the memory a Raft node allocates on behalf of its owner.

use std::fmt;
use std::sync::Arc;

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
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.funder.release(self.bytes);
    }
}

impl fmt::Debug for Charge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Charge").field("bytes", &self.bytes).finish()
    }
}
