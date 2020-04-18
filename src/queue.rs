//! Segment Queue => skeleton from msqueue + test from segqueue source

use core::mem::{self, ManuallyDrop};
use core::ptr;
use core::sync::atomic::Ordering;

use crossbeam_epoch::{self, unprotected, Atomic, Guard, Owned};

use std::cell::UnsafeCell;
use std::cmp;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;

const SEG_SIZE: usize = 32;

#[derive(Debug, Default)]
pub struct Queue<T> {
    head: Atomic<Segment<T>>,
    tail: Atomic<Segment<T>>,
}

#[derive(Debug)]
struct Segment<T> {
    low: AtomicUsize,
    high: AtomicUsize,
    data: ManuallyDrop<[UnsafeCell<(T, AtomicBool)>; SEG_SIZE]>,
    next: Atomic<Segment<T>>,
}

unsafe impl<T: Send> Sync for Segment<T> {}
impl<T> Segment<T> {
    fn new() -> Segment<T> {
        let s = Segment {
            low: AtomicUsize::new(0),
            high: AtomicUsize::new(0),
            data: unsafe { ManuallyDrop::new(mem::uninitialized()) },
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

impl<T> Queue<T> {
    /// Create a new, empty queue.
    pub fn new() -> Queue<T> {
        let q = Queue {
            head: Atomic::null(),
            tail: Atomic::null(),
        };
        let sentinel = Owned::new(Segment::new());
        unsafe {
            let guard = &unprotected();
            let sentinel = sentinel.into_shared(&guard);
            q.head.store(sentinel, Ordering::Relaxed);
            q.tail.store(sentinel, Ordering::Relaxed);
        }
        q
    }

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
            //check empty queue
            if head_ref.next.load(Ordering::Relaxed, guard).is_null() {
                return None;
            }
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            let guard = unprotected();
            while self.try_pop(guard).is_some() {}
            // Destroy the remaining sentinel node.
            let sentinel = self.head.load(Ordering::Relaxed, guard);
            drop(sentinel.into_owned());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crossbeam_epoch::pin;
    use crossbeam_utils::thread::scope;

    struct Queue<T> {
        queue: super::Queue<T>,
    }

    impl<T> Queue<T> {
        pub fn new() -> Queue<T> {
            Queue {
                queue: super::Queue::new(),
            }
        }

        pub fn push(&self, t: T) {
            let guard = &pin();
            self.queue.push(t, guard);
        }

        pub fn is_empty(&self) -> bool {
            let guard = &pin();
            let head = self.queue.head.load(Ordering::Acquire, guard);
            let tail = self.queue.tail.load(Ordering::Acquire, guard);
            if head != tail {
                return false;
            }
            let h = unsafe { head.deref() };
            if h.low.load(Ordering::Relaxed) >= cmp::min(h.high.load(Ordering::Relaxed), SEG_SIZE) {
                return true;
            }
            return false;
            //h.next.load(Ordering::Acquire, guard).is_null()
        }

        pub fn try_pop(&self) -> Option<T> {
            let guard = &pin();
            self.queue.try_pop(guard)
        }

        pub fn pop(&self) -> T {
            loop {
                match self.try_pop() {
                    None => continue,
                    Some(t) => return t,
                }
            }
        }
    }

    const CONC_COUNT: i64 = 1000000;

    #[test]
    fn push_try_pop_1() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        q.push(37);
        assert!(!q.is_empty());
        assert_eq!(q.try_pop(), Some(37));
        assert!(q.is_empty());
    }

    #[test]
    fn push_try_pop_2() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        q.push(37);
        q.push(48);
        assert_eq!(q.try_pop(), Some(37));
        assert!(!q.is_empty());
        assert_eq!(q.try_pop(), Some(48));
        assert!(q.is_empty());
    }

    #[test]
    fn push_try_pop_many_seq() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        for i in 0..200 {
            q.push(i)
        }
        assert!(!q.is_empty());
        for i in 0..200 {
            assert_eq!(q.try_pop(), Some(i));
        }
        assert!(q.is_empty());
    }

    #[test]
    fn push_pop_1() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        q.push(37);
        assert!(!q.is_empty());
        assert_eq!(q.pop(), 37);
        assert!(q.is_empty());
    }

    #[test]
    fn push_pop_2() {
        let q: Queue<i64> = Queue::new();
        q.push(37);
        q.push(48);
        assert_eq!(q.pop(), 37);
        assert_eq!(q.pop(), 48);
    }
    #[test]
    fn push_pop_empty_check() {
        let q: Queue<i64> = Queue::new();
        assert_eq!(q.is_empty(), true);
        q.push(42);
        assert_eq!(q.is_empty(), false);
        assert_eq!(q.try_pop(), Some(42));
        assert_eq!(q.is_empty(), true);
    }
    #[test]
    fn push_pop_many_seq() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        for i in 0..200 {
            q.push(i)
        }
        assert!(!q.is_empty());
        for i in 0..200 {
            assert_eq!(q.pop(), i);
        }
        assert!(q.is_empty());
    }

    #[test]
    fn push_pop_many_spsc() {
        let q: Queue<i64> = Queue::new();

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
    fn push_pop_many_mpmc() {
        enum LR {
            Left(i64),
            Right(i64),
        }

        let q: Queue<LR> = Queue::new();

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

    #[test]
    fn is_empty_dont_pop() {
        let q: Queue<i64> = Queue::new();
        q.push(20);
        q.push(20);
        assert!(!q.is_empty());
        assert!(!q.is_empty());
        assert!(q.try_pop().is_some());
    }
}
