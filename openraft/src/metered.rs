//! The unit tests' allocator: the system's, metering what this thread allocates while a test
//! runs a call under [`metered`], and refusing every allocation this thread asks for while it
//! runs a call under [`refusing`]. Other threads are neither metered nor refused.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;

thread_local! {
    static METERING: Cell<bool> = const { Cell::new(false) };
    /// Every byte requested while metering.
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
    /// What was allocated less what was freed while metering.
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static REFUSING: Cell<bool> = const { Cell::new(false) };
    /// Every allocation refused while refusing.
    static REFUSED: Cell<usize> = const { Cell::new(0) };
}

/// What a metered call allocated on its thread.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Meter {
    /// Every byte it requested, whether or not it freed them.
    pub(crate) allocated: usize,
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
    let _ = LIVE.try_with(|live| live.set(live.get().saturating_add(bytes)));
}

fn begin() {
    let _ = ALLOCATED.try_with(|allocated| allocated.set(0));
    let _ = LIVE.try_with(|live| live.set(0));
}

fn meter(on: bool) {
    let _ = METERING.try_with(|metering| metering.set(on));
}

fn read() -> Meter {
    Meter {
        allocated: ALLOCATED.try_with(Cell::get).unwrap_or(0),
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

/// `run` with every allocation it asks for on this thread refused, with how many it asked for.
pub(crate) fn refusing<T>(run: impl FnOnce() -> T) -> (T, usize) {
    let _ = REFUSED.try_with(|refused| refused.set(0));
    let _ = REFUSING.try_with(|refusing| refusing.set(true));
    let out = run();
    let _ = REFUSING.try_with(|refusing| refusing.set(false));
    (out, REFUSED.try_with(Cell::get).unwrap_or(0))
}

/// Whether to refuse this allocation, counting it if so.
fn refused() -> bool {
    let refuse = REFUSING.try_with(Cell::get).unwrap_or(false);
    if refuse {
        let _ = REFUSED.try_with(|refused| refused.set(refused.get().saturating_add(1)));
    }
    refuse
}

struct Metered;

#[global_allocator]
static ALLOCATOR: Metered = Metered;

// SAFETY: every call is forwarded to System unchanged, or refused with a null pointer, which
// the contract allows for any allocation; the metering and refusing touch only constant
// thread-locals without destructors, so they allocate nothing.
unsafe impl GlobalAlloc for Metered {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if refused() {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && metering() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if refused() {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() && metering() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if refused() {
            return std::ptr::null_mut();
        }
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
