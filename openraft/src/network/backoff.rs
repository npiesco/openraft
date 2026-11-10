use std::sync::Arc;
use std::time::Duration;

use crate::Config;
use crate::OptionalSend;
use crate::base::BoxIterator;
use crate::network::policy_backoff::PolicyBackoff;

/// A backoff instance that is an infinite iterator of durations to sleep before next retry, when a
/// [`Unreachable`](`crate::error::Unreachable`) occurs.
pub struct Backoff {
    inner: Inner,
}

/// The durations a [`Backoff`] yields: an application's, in a box, or those of the config's
/// policy, read in place.
enum Inner {
    Boxed(BoxIterator<'static, Duration>),
    Policy(PolicyBackoff),
}

impl Backoff {
    /// Create a new Backoff from an iterator of durations, boxed: it allocates
    /// [`new_bytes::<I>()`](Self::new_bytes), and whatever the iterator allocates.
    pub fn new(iter: impl Iterator<Item = Duration> + OptionalSend + 'static) -> Self {
        Self {
            inner: Inner::Boxed(Box::new(iter)),
        }
    }

    /// The bytes [`Backoff::new`] allocates for an iterator `I`: the box it puts it in, nothing
    /// for an iterator of no size.
    pub const fn new_bytes<I>() -> usize
    where I: Iterator<Item = Duration> + OptionalSend + 'static {
        size_of::<I>()
    }

    /// The backoff of `config`'s [`backoff`](Config::backoff) policy, made without allocating:
    /// the policy is read where `config` holds it. `None` if the policy is invalid, as only a
    /// config never validated holds.
    pub(crate) fn of_policy(config: &Arc<Config>) -> Option<Self> {
        let policy = PolicyBackoff::new(config)?;
        Some(Self {
            inner: Inner::Policy(policy),
        })
    }
}

impl Iterator for Backoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            Inner::Boxed(iter) => iter.next(),
            Inner::Policy(policy) => policy.next(),
        }
    }
}
