//! Segment Queue => skeleton from msqueue + test from segqueue source

use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::Ordering;

use crossbeam_epoch::{pin, unprotected, Atomic, Owned, Shared};

use crate::Queue;
use std::cmp;
use std::sync::atomic::{AtomicBool, AtomicUsize};
const SEG_SIZE: usize = 32;

#[derive(Debug, Default)]
/// Segment Queue structure
pub struct SegQueue<T> {
    head: Atomic<Segment<T>>,
    tail: Atomic<Segment<T>>,
}

pub struct Segment<T> {
    low: AtomicUsize,
    high: AtomicUsize,
    data: [(MaybeUninit<T>, AtomicBool); SEG_SIZE],
    next: Atomic<Segment<T>>,
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
        for cell in &s.data {
            cell.1.store(false, Ordering::Relaxed);
        }
        s
    }
}

impl<T> Queue<T> for SegQueue<T> {
    /// Create a new, empty queue.
    fn new() -> Self
    where
        T: Sized,
    {
        let q = SegQueue {
            head: Atomic::null(),
            tail: Atomic::null(),
        };

        let sentinel = Owned::new(Segment::new());
        let sentinel = unsafe { sentinel.into_shared(&unprotected()) };
        q.head.store(sentinel, Ordering::Relaxed);
        q.tail.store(sentinel, Ordering::Relaxed);

        q
    }

    /// Push
    fn push(&self, t: T) {
        let guard = &pin();

        loop {
            //load acquire tail
            let tail = unsafe { self.tail.load(Ordering::Acquire, guard).deref_mut() };
            //check if this is real tail
            if tail.high.load(Ordering::Relaxed) >= SEG_SIZE {
                continue;
            }

            let cur_high = tail.high.fetch_add(1, Ordering::Relaxed);
            if cur_high >= SEG_SIZE {
                continue;
            }

            let cell;
            unsafe {
                cell = (*tail).data.get_unchecked_mut(cur_high);
                cell.0.as_mut_ptr().write(t);
            }
            (*cell).1.store(true, Ordering::Release);
            if cur_high + 1 == SEG_SIZE {
                let new_tail = Owned::new(Segment::new()).into_shared(guard);
                tail.next.store(new_tail, Ordering::Release);
                self.tail.store(new_tail, Ordering::Release);
            }
            return;
        }
    }

    /// Pop
    fn try_pop(&self) -> Option<T> {
        let guard = &pin();

        loop {
            let head = self.head.load(Ordering::Acquire, guard);
            let head_ref = unsafe { head.as_ref() }.unwrap();

            loop {
                let low = head_ref.low.load(Ordering::Relaxed);
                if low >= cmp::min(head_ref.high.load(Ordering::Relaxed), SEG_SIZE) {
                    // Need to use 'min' because, high might be bigger than SEG_SIZE if there is case like cur_high >= SEG_SIZE in push() function(line 91).
                    break;
                }
                if head_ref
                    .low
                    .compare_exchange(low, low + 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    let cell = unsafe { (*head_ref).data.get_unchecked(low) };
                    loop {
                        if (*cell).1.load(Ordering::Acquire) {
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
                    return unsafe { Some(ptr::read((*cell).0.as_ptr())) };
                }
            }
            // check empty queue
            if head_ref.next.load(Ordering::Relaxed, guard).is_null() {
                return None;
            }
        }
    }

    fn is_empty(&self) -> bool {
        let guard = &pin();
        let head = self.head.load(Ordering::Acquire, guard);
        let tail = self.tail.load(Ordering::Acquire, guard);
        if head != tail {
            return false;
        }
        let h = unsafe { head.deref() };
        if h.low.load(Ordering::Relaxed) >= cmp::min(h.high.load(Ordering::Relaxed), SEG_SIZE) {
            return true;
        }
        false
    }

    fn pop(&self) -> T {
        loop {
            if let Some(t) = self.try_pop() {
                return t;
            }
        }
    }
}

impl<T> Drop for SegQueue<T> {
    fn drop(&mut self) {
        while self.try_pop().is_some() {}
        unsafe {
            // Destroy the remaining sentinel node.
            let sentinel = self.head.load(Ordering::Relaxed, unprotected());
            drop(sentinel.into_owned());
        }
    }
}

mod tests {
    use super::*;
    use crate::queue::*;
    use crossbeam_utils::thread::scope;

    const CONC_COUNT: i64 = 1_000_000;

    #[test]
    fn is_empty_dont_pop() {
        let q = Box::new(SegQueue::new());
        test_is_empty_dont_pop(q);
    }

    #[test]
    fn push_try_pop_1() {
        let q = Box::new(SegQueue::new());
        test_push_try_pop_1(q);
    }

    #[test]
    fn push_try_pop_2() {
        let q = Box::new(SegQueue::new());
        test_push_try_pop_2(q);
    }

    #[test]
    fn push_try_pop_many_seq() {
        let q = Box::new(SegQueue::new());
        test_push_try_pop_many_seq(q);
    }

    #[test]
    fn push_pop_1() {
        let q = Box::new(SegQueue::new());
        test_push_pop_1(q);
    }

    #[test]
    fn push_pop_2() {
        let q = Box::new(SegQueue::new());
        test_push_pop_2(q);
    }

    #[test]
    fn push_pop_empty_check() {
        let q = Box::new(SegQueue::new());
        test_push_pop_empty_check(q);
    }

    #[test]
    fn push_pop_many_seq() {
        let q = Box::new(SegQueue::new());
        test_push_pop_many_seq(q);
    }

    #[test]
    fn test_push_pop_many_spsc() {
        let q = SegQueue::new();
        scope(|scope| {
            scope.spawn(|_| {
                let mut next = 0;

                while next < CONC_COUNT {
                    if let Some(elem) = q.try_pop() {
                        assert_eq!(elem, next);
                        next += 1;
                    }
                }
            });

            for i in 0..CONC_COUNT {
                q.push(i)
            }
        })
        .unwrap();
    }

    #[test]
    fn test_push_try_pop_many_spmc() {
        fn recv_seg(_t: i32, q: &SegQueue<i64>) {
            let mut cur = -1;
            for _i in 0..CONC_COUNT {
                if let Some(elem) = q.try_pop() {
                    assert!(elem > cur);
                    cur = elem;

                    if cur == CONC_COUNT - 1 {
                        break;
                    }
                }
            }
        }
        let q = SegQueue::new();
        assert!(q.is_empty());
        scope(|scope| {
            for i in 0..3 {
                let q = &q;
                scope.spawn(move |_| recv_seg(i, q));
            }

            scope.spawn(|_| {
                for i in 0..CONC_COUNT {
                    q.push(i);
                }
            });
        })
        .unwrap();
    }

    #[test]
    fn test_push_try_pop_many_mpmc() {
        enum LR {
            Left(i64),
            Right(i64),
        }
        let q = SegQueue::new();
        assert!(q.is_empty());

        scope(|scope| {
            for _t in 0..2 {
                scope.spawn(|_| {
                    for i in CONC_COUNT - 1..CONC_COUNT {
                        q.push(LR::Left(i))
                    }
                });
                scope.spawn(|_| {
                    for i in CONC_COUNT - 1..CONC_COUNT {
                        q.push(LR::Right(i))
                    }
                });
                scope.spawn(|_| {
                    let mut vl = vec![];
                    let mut vr = vec![];
                    for _i in 0..CONC_COUNT {
                        match q.try_pop() {
                            Some(LR::Left(x)) => vl.push(x),
                            Some(LR::Right(x)) => vr.push(x),
                            _ => {}
                        }
                    }

                    let mut vl2 = vl.clone();
                    let mut vr2 = vr.clone();
                    vl2.sort();
                    vr2.sort();

                    assert_eq!(vl, vl2);
                    assert_eq!(vr, vr2);
                });
            }
        })
        .unwrap();
    }
}
