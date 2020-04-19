//! Segment Queue => skeleton from msqueue + test from segqueue source

use core::mem::{ManuallyDrop, MaybeUninit};
use core::ptr;
use core::sync::atomic::Ordering;

use crossbeam_epoch::{self, unprotected, Atomic, Guard, Owned};

use std::cell::UnsafeCell;
use std::cmp;
use std::sync::atomic::{AtomicBool, AtomicUsize};

pub(crate) const SEG_SIZE: usize = 32;

#[derive(Debug, Default)]
/// Segment Queue structure
pub struct SegQueue<T> {
    pub(crate) head: Atomic<Segment<T>>,
    pub(crate) tail: Atomic<Segment<T>>,
}

#[derive(Debug)]
pub struct Segment<T> {
    pub(crate) low: AtomicUsize,
    pub(crate) high: AtomicUsize,
    pub(crate) data: ManuallyDrop<[UnsafeCell<(T, AtomicBool)>; SEG_SIZE]>,
    pub(crate) next: Atomic<Segment<T>>,
}

unsafe impl<T: Send> Sync for Segment<T> {}
impl<T> Segment<T> {
    fn new() -> Segment<T> {
        let s = Segment {
            low: AtomicUsize::new(0),
            high: AtomicUsize::new(0),
            data: unsafe { MaybeUninit::uninit().assume_init() },
            next: Atomic::null(),
        };
        for cell in s.data.iter() {
            unsafe {
                (*cell.get()).1 = AtomicBool::new(false);
            }
        }
        s
    }
}

impl<T> SegQueue<T> {
    /// Create a new, empty queue.
    pub fn new() -> SegQueue<T> {
        let q = SegQueue {
            head: Atomic::null(),
            tail: Atomic::null(),
        };
        let sentinel = Owned::new(Segment::new());
        unsafe {
            let sentinel = sentinel.into_shared(&unprotected());
            q.head.store(sentinel, Ordering::Relaxed);
            q.tail.store(sentinel, Ordering::Relaxed);
        }
        q
    }

    /// Push
    pub fn push(&self, t: T, guard: &Guard) {
        loop {
            //load acquire tail
            let tail = unsafe { self.tail.load(Ordering::Acquire, guard).deref() };
            //check if this is real tail
            if tail.high.load(Ordering::Relaxed) >= SEG_SIZE {
                continue;
            }

            let cur_high = tail.high.fetch_add(1, Ordering::Relaxed);
            unsafe {
                if cur_high >= SEG_SIZE {
                    continue;
                }
                let cell = (*tail).data.get_unchecked(cur_high).get();
                ptr::write(&mut (*cell).0, t);
                (*cell).1.store(true, Ordering::Release);
                if cur_high + 1 == SEG_SIZE {
                    let new_tail = Owned::new(Segment::new()).into_shared(guard);
                    tail.next.store(new_tail, Ordering::Release);
                    self.tail.store(new_tail, Ordering::Release);
                }
                return;
            }
        }
    }

    /// Pop
    pub fn try_pop(&self, guard: &Guard) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Acquire, guard);
            let head_ref = unsafe { head.as_ref() }.unwrap();

            loop {
                let low = head_ref.low.load(Ordering::Relaxed);
                if low >= cmp::min(head_ref.high.load(Ordering::Relaxed), SEG_SIZE) {
                    break;
                }
                if head_ref
                    .low
                    .compare_and_swap(low, low + 1, Ordering::Relaxed)
                    == low
                {
                    let cell = unsafe { (*head_ref).data.get_unchecked(low).get() };
                    loop {
                        if unsafe { (*cell).1.load(Ordering::Acquire) } {
                            break;
                        }
                    }
                    // check end of segment
                    if low + 1 == SEG_SIZE {
                        loop {
                            // load next segment
                            let next_head = head_ref.next.load(Ordering::Acquire, guard);
                            if unsafe { next_head.as_ref() }.is_some()
                                && self
                                    .head
                                    .compare_and_set(head, next_head, Ordering::Release, guard)
                                    .is_ok()
                            {
                                unsafe { guard.defer_destroy(head) };
                                break;
                            }
                        }
                    }
                    return unsafe { Some(ptr::read(&(*cell).0)) };
                }
            }
            // check empty queue
            if head_ref.next.load(Ordering::Relaxed, guard).is_null() {
                return None;
            }
        }
    }
}

impl<T> Drop for SegQueue<T> {
    fn drop(&mut self) {
        unsafe {
            while self.try_pop(unprotected()).is_some() {}
            // Destroy the remaining sentinel node.
            let sentinel = self.head.load(Ordering::Relaxed, unprotected());
            drop(sentinel.into_owned());
        }
    }
}
