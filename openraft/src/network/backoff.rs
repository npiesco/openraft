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
    /// Create a new Backoff from an iterator of durations.
    pub fn new(iter: impl Iterator<Item = Duration> + OptionalSend + 'static) -> Self {
        Self {
            inner: Inner::Boxed(Box::new(iter)),
        }
    }

    /// The backoff of `config`'s [`backoff`](Config::backoff) policy, as
    /// [`Config::build_backoff`] builds it, made without allocating: the policy is read where
    /// `config` holds it. Built by [`Config::build_backoff`], which panics, if the policy is
    /// invalid, as only a config never validated holds.
    pub(crate) fn of_policy(config: &Arc<Config>) -> Self {
        match PolicyBackoff::new(config) {
            Some(policy) => Self {
                inner: Inner::Policy(policy),
            },
            None => config.build_backoff(),
        }
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
