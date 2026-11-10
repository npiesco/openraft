//! A boxed error wrapper for smaller error type sizes.

use std::error::Error;
use std::fmt;

use anyerror::AnyError;

use crate::errors::ErrorSource;

/// A boxed wrapper around [`AnyError`] for smaller error type sizes.
///
/// This type stores `AnyError` in a `Box`, reducing the size of error types
/// that contain it (like `StorageError`). This is the default `ErrorSource`
/// implementation used by [`declare_raft_types!`](crate::declare_raft_types).
///
/// A fixed reason ([`ErrorSource::from_static`], or [`ErrorSource::from_args`] with nothing to
/// format) is kept as the `&'static str` it is: making it allocates nothing. Any other error is
/// written into a boxed [`AnyError`], which allocates.
///
/// Use [`AnyError`] directly if you prefer inline storage and don't mind
/// larger error types.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(from = "Wire", into = "Wire"))]
pub struct BoxedErrorSource {
    inner: Inner,
}

#[derive(Clone)]
enum Inner {
    /// A fixed reason, allocating nothing.
    Static(&'static str),
    Boxed(Box<AnyError>),
}

/// What a [`BoxedErrorSource`] is (de)serialized as: a boxed [`AnyError`], as it always was.
#[cfg(feature = "serde")]
#[derive(serde::Deserialize, serde::Serialize)]
struct Wire {
    inner: Box<AnyError>,
}

#[cfg(feature = "serde")]
impl From<Wire> for BoxedErrorSource {
    fn from(wire: Wire) -> Self {
        Self {
            inner: Inner::Boxed(wire.inner),
        }
    }
}

#[cfg(feature = "serde")]
impl From<BoxedErrorSource> for Wire {
    fn from(source: BoxedErrorSource) -> Self {
        let inner = match source.inner {
            Inner::Static(reason) => Box::new(AnyError::error(reason)),
            Inner::Boxed(inner) => inner,
        };
        Wire { inner }
    }
}

/// A fixed reason equals the [`AnyError`] made of its text alone, as it is once serialized.
impl PartialEq for BoxedErrorSource {
    fn eq(&self, other: &Self) -> bool {
        match (&self.inner, &other.inner) {
            (Inner::Static(a), Inner::Static(b)) => a == b,
            (Inner::Boxed(a), Inner::Boxed(b)) => a == b,
            (Inner::Static(reason), Inner::Boxed(any)) | (Inner::Boxed(any), Inner::Static(reason)) => {
                **any == AnyError::error(reason)
            }
        }
    }
}

impl Eq for BoxedErrorSource {}

/// Debugs as it did when it was always boxed: a fixed reason as the [`AnyError`] made of it would.
impl fmt::Debug for BoxedErrorSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("BoxedErrorSource");
        match &self.inner {
            Inner::Static(reason) => debug.field("inner", &format_args!("{reason}")),
            Inner::Boxed(inner) => debug.field("inner", inner),
        };
        debug.finish()
    }
}

impl fmt::Display for BoxedErrorSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Inner::Static(reason) => f.write_str(reason),
            Inner::Boxed(inner) => inner.fmt(f),
        }
    }
}

impl Error for BoxedErrorSource {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.inner {
            Inner::Static(_) => None,
            Inner::Boxed(inner) => inner.source(),
        }
    }
}

impl ErrorSource for BoxedErrorSource {
    fn from_error<E: Error + 'static>(error: &E) -> Self {
        Self {
            inner: Inner::Boxed(Box::new(AnyError::new(error))),
        }
    }

    fn from_string(msg: impl ToString) -> Self {
        Self {
            inner: Inner::Boxed(Box::new(AnyError::error(msg))),
        }
    }

    fn from_static(reason: &'static str) -> Self {
        Self {
            inner: Inner::Static(reason),
        }
    }

    fn has_backtrace(&self) -> bool {
        anyerror::backtrace_str().is_some()
    }

    fn fmt_backtrace(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(bt) = anyerror::backtrace_str() {
            write!(f, "{}", bt)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use anyerror::AnyError;

    use super::BoxedErrorSource;
    use crate::errors::ErrorSource;
    use crate::metered::metered;

    /// A fixed reason, given as is or as arguments with nothing to format, allocates nothing,
    /// and displays and compares as the text it is.
    #[test]
    fn test_a_fixed_reason_allocates_nothing() {
        let (fixed, meter) = metered(|| BoxedErrorSource::from_static("cannot send to sm::Worker"));
        assert_eq!(0, meter.allocated);
        let (args, meter) = metered(|| BoxedErrorSource::from_args(format_args!("log entry not found")));
        assert_eq!(0, meter.allocated);

        assert_eq!("cannot send to sm::Worker", fixed.to_string());
        assert_eq!("log entry not found", args.to_string());
        assert_eq!(BoxedErrorSource::from_string("log entry not found"), args);
        assert_ne!(
            BoxedErrorSource::from_error(&AnyError::error("x").with_type(Some("t"))),
            BoxedErrorSource::from_static("x")
        );
    }

    /// Arguments with something to format are written into a boxed error.
    #[test]
    fn test_formatted_arguments_are_written() {
        let formatted = BoxedErrorSource::from_args(format_args!("need logs from index {}", 3));
        assert_eq!("need logs from index 3", formatted.to_string());
    }

    /// A fixed reason is serialized as the boxed error it always was.
    #[cfg(feature = "serde")]
    #[test]
    fn test_a_fixed_reason_serializes_as_a_boxed_error() {
        let fixed = BoxedErrorSource::from_static("snapshot not found");
        let json = serde_json::to_string(&fixed).unwrap();
        assert_eq!(
            serde_json::to_string(&BoxedErrorSource::from_string("snapshot not found")).unwrap(),
            json
        );
        let back: BoxedErrorSource = serde_json::from_str(&json).unwrap();
        assert_eq!(fixed, back);
    }
}
