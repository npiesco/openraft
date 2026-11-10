//! The name of the enclosing function, as `func_name!` tells it to a log line, without allocating.

use std::fmt;

/// A function's path, as [`std::any::type_name`] tells it, shown without its `::{{closure}}`
/// segments. It borrows the static name and writes it a segment at a time, so naming a function
/// in a log line allocates nothing.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DisplayFuncName(&'static str);

impl DisplayFuncName {
    /// The name of the function enclosing `f`, as `type_name` tells it for a fn item `f` declared
    /// in it: its path less the trailing `::f`.
    pub(crate) fn of_item(type_name: &'static str) -> Self {
        Self(type_name.strip_suffix("::f").unwrap_or(type_name))
    }
}

impl fmt::Display for DisplayFuncName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for segment in self.0.split("::{{closure}}") {
            f.write_str(segment)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Waker;

    use crate::metered::metered;

    /// A fixed buffer that counts what it is written, never growing.
    struct Fixed {
        buf: [u8; 128],
        len: usize,
    }

    impl Write for Fixed {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            let end = self.len.checked_add(s.len()).filter(|end| *end <= self.buf.len()).ok_or(std::fmt::Error)?;
            self.buf[self.len..end].copy_from_slice(s.as_bytes());
            self.len = end;
            Ok(())
        }
    }

    impl Fixed {
        fn as_str(&self) -> &str {
            std::str::from_utf8(&self.buf[..self.len]).unwrap()
        }
    }

    /// `func_name!` names the function it is in, closures and async blocks dropped, writing it
    /// allocates nothing.
    #[test]
    fn test_func_name_is_written_without_allocating() {
        let in_closure = Some(()).map(|()| func_name!()).unwrap();
        let mut fixed = Fixed { buf: [0; 128], len: 0 };
        let (written, meter) = metered(|| write!(fixed, "{}", in_closure));
        assert_eq!(Ok(()), written);
        assert_eq!(0, meter.allocated);
        assert_eq!(
            "openraft::display_ext::display_func_name::tests::test_func_name_is_written_without_allocating",
            fixed.as_str()
        );

        // Named by its path, not imported: `func_name!` names it by `$crate::display_ext`, so an
        // import here would make that path a redundant qualification.
        let raw = "a::b::{{closure}}::c::{{closure}}::f";
        assert_eq!("a::b::c", super::DisplayFuncName::of_item(raw).to_string());
    }

    /// With tracing disabled, as it is with no subscriber, a span, an instrumented future and an
    /// event evaluate none of their fields and allocate nothing; nor is an instrumented future
    /// boxed.
    #[test]
    fn test_disabled_tracing_evaluates_no_field_and_allocates_nothing() {
        static EVALUATED: AtomicBool = AtomicBool::new(false);

        fn costly() -> String {
            EVALUATED.store(true, Ordering::Relaxed);
            "costly".to_string()
        }

        #[tracing::instrument(level = "debug", skip_all, fields(costly = %costly()))]
        async fn traced(x: u8) -> u8 {
            tracing::debug!(costly = %costly(), "{}: {}", func_name!(), x);
            x + 1
        }

        let ((), meter) = metered(|| {
            let span = tracing::debug_span!(parent: &tracing::Span::current(), "span", costly = %costly());
            let _entered = span.enter();
            tracing::info!("{}: {}", func_name!(), costly());

            let mut cx = Context::from_waker(Waker::noop());
            let mut future = pin!(traced(1));
            assert_eq!(std::task::Poll::Ready(2), future.as_mut().poll(&mut cx));
        });
        assert!(!EVALUATED.load(Ordering::Relaxed), "no field is evaluated");
        assert_eq!(0, meter.allocated);
    }
}
