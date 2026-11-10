//! The unit tests' allocator: the system's, metering what this thread allocates while a test
//! runs a call under [`metered`]. Other threads are not metered.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;

thread_local! {
    static METERING: Cell<bool> = const { Cell::new(false) };
    /// Every byte requested while metering.
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
    /// What was allocated less what was freed while metering, and its high-water.
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

/// What a metered call allocated on its thread.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Meter {
    /// Every byte it requested, whether or not it freed them.
    pub(crate) allocated: usize,
    /// The most it held at once beyond what was live as it began.
    pub(crate) peak: usize,
    /// What it still held as it ended beyond what was live as it began; what it freed of what
    /// was live before counts against it.
    pub(crate) kept: isize,
}

fn metering() -> bool {
    METERING.try_with(Cell::get).unwrap_or(false)
}

fn allocated(bytes: usize) {
    let _ = ALLOCATED.try_with(|allocated| allocated.set(allocated.get().saturating_add(bytes)));
    grown(bytes.cast_signed());
}

fn grown(bytes: isize) {
    let _ = LIVE.try_with(|live| {
        let now = live.get().saturating_add(bytes);
        live.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

fn begin() {
    let _ = ALLOCATED.try_with(|allocated| allocated.set(0));
    let _ = LIVE.try_with(|live| live.set(0));
    let _ = PEAK.try_with(|peak| peak.set(0));
}

fn meter(on: bool) {
    let _ = METERING.try_with(|metering| metering.set(on));
}

fn read() -> Meter {
    Meter {
        allocated: ALLOCATED.try_with(Cell::get).unwrap_or(0),
        peak: PEAK.try_with(Cell::get).unwrap_or(0).unsigned_abs(),
        kept: LIVE.try_with(Cell::get).unwrap_or(0),
    }
}

/// `run`, with what it allocated on this thread.
pub(crate) fn metered<T>(run: impl FnOnce() -> T) -> (T, Meter) {
    begin();
    meter(true);
    let out = run();
    meter(false);
    (out, read())
}

struct Metered;

#[global_allocator]
static ALLOCATOR: Metered = Metered;

// SAFETY: every call is forwarded to System unchanged; the metering touches only constant
// thread-locals without destructors, so it allocates nothing.
unsafe impl GlobalAlloc for Metered {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && metering() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() && metering() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let grown_ptr = unsafe { System.realloc(ptr, layout, size) };
        if !grown_ptr.is_null() && metering() {
            allocated(size);
            grown(-layout.size().cast_signed());
        }
        grown_ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if metering() {
            grown(-layout.size().cast_signed());
        }
        unsafe { System.dealloc(ptr, layout) };
    }
}
