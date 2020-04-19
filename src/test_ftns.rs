//! Tests for segqueue and msqueue

use crossbeam_epoch::pin;
use crossbeam_utils::thread::scope;

use crate::segqueue::SEG_SIZE;
use crate::{msqueue, segqueue};
use core::sync::atomic::Ordering;
use std::cmp;

const CONC_COUNT: i64 = 1_000_000;

struct SegQueue<T> {
    queue: segqueue::SegQueue<T>,
}

impl<T> SegQueue<T> {
    pub fn new() -> SegQueue<T> {
        SegQueue {
            queue: segqueue::SegQueue::new(),
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

struct MSQueue<T> {
    queue: msqueue::MSQueue<T>,
}

impl<T> MSQueue<T> {
    pub fn new() -> MSQueue<T> {
        MSQueue {
            queue: msqueue::MSQueue::new(),
        }
    }

    pub fn push(&self, t: T) {
        self.queue.push(t, &pin());
    }

    pub fn is_empty(&self) -> bool {
        let guard = &pin();
        let head = self.queue.head.load(Ordering::Acquire, guard);
        let h = unsafe { head.deref() };
        h.next.load(Ordering::Acquire, guard).is_null()
    }

    pub fn try_pop(&self) -> Option<T> {
        //let guard = &pin();
        self.queue.try_pop(&pin())
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

mod test {
    use super::*;
    macro_rules! push_try_pop_1 {
        ($struct:expr) => {
            let q = $struct;
            assert!(q.is_empty());
            q.push(37);
            assert!(!q.is_empty());
            assert_eq!(q.try_pop(), Some(37));
            assert!(q.is_empty());
        };
    }
    #[test]
    fn test_push_try_pop_1() {
        push_try_pop_1!(SegQueue::new());
        push_try_pop_1!(MSQueue::new());
    }

    macro_rules! push_try_pop_2 {
        ($struct:expr) => {
            let q = $struct;
            assert!(q.is_empty());
            q.push(37);
            q.push(48);
            assert_eq!(q.try_pop(), Some(37));
            assert!(!q.is_empty());
            assert_eq!(q.try_pop(), Some(48));
            assert!(q.is_empty());
        };
    }
    #[test]
    fn test_push_try_pop_2() {
        push_try_pop_2!(SegQueue::new());
        push_try_pop_2!(MSQueue::new());
    }

    macro_rules! push_try_pop_many_seq {
        ($struct:expr) => {
            let q = $struct;
            assert!(q.is_empty());
            for i in 0..200 {
                q.push(i)
            }
            assert!(!q.is_empty());
            for i in 0..200 {
                assert_eq!(q.try_pop(), Some(i));
            }
            assert!(q.is_empty());
        };
    }
    #[test]
    fn test_push_try_pop_many_seq() {
        push_try_pop_many_seq!(SegQueue::new());
        push_try_pop_many_seq!(MSQueue::new());
    }

    macro_rules! push_pop_1 {
        ($struct:expr) => {
            let q = $struct;
            assert!(q.is_empty());
            q.push(37);
            assert!(!q.is_empty());
            assert_eq!(q.pop(), 37);
            assert!(q.is_empty());
        };
    }
    #[test]
    fn test_push_pop_1() {
        push_pop_1!(SegQueue::new());
        push_pop_1!(MSQueue::new());
    }

    macro_rules! push_pop_2 {
        ($struct:expr) => {
            let q = $struct;
            q.push(37);
            q.push(48);
            assert_eq!(q.pop(), 37);
            assert_eq!(q.pop(), 48);
        };
    }
    #[test]
    fn test_push_pop_2() {
        push_pop_2!(SegQueue::new());
        push_pop_2!(MSQueue::new());
    }

    macro_rules! push_pop_empty_check {
        ($struct:expr) => {
            let q = $struct;
            assert_eq!(q.is_empty(), true);
            q.push(42);
            assert_eq!(q.is_empty(), false);
            assert_eq!(q.try_pop(), Some(42));
            assert_eq!(q.is_empty(), true);
        };
    }
    #[test]
    fn test_push_pop_empty_check() {
        push_pop_empty_check!(SegQueue::new());
        push_pop_empty_check!(MSQueue::new());
    }

    macro_rules! push_pop_many_seq {
        ($struct:expr) => {
            let q = $struct;
            assert!(q.is_empty());
            for i in 0..200 {
                q.push(i)
            }
            assert!(!q.is_empty());
            for i in 0..200 {
                assert_eq!(q.pop(), i);
            }
            assert!(q.is_empty());
        };
    }
    #[test]
    fn test_push_pop_many_seq() {
        push_pop_many_seq!(SegQueue::new());
        push_pop_many_seq!(MSQueue::new());
    }

    macro_rules! push_pop_many_spsc {
        ($struct:expr) => {
            let q = $struct;
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
        };
    }
    #[test]
    fn test_push_pop_many_spsc() {
        push_pop_many_spsc!(SegQueue::new());
        push_pop_many_spsc!(MSQueue::new());
    }

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
    fn recv_ms(_t: i32, q: &MSQueue<i64>) {
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
    macro_rules! push_try_pop_many_spmc {
        ($struct:expr, $type:expr) => {
            let q = $struct;
            assert!(q.is_empty());
            scope(|scope| {
                for i in 0..3 {
                    let q = &q;
                    scope.spawn(move |_| $type(i, q));
                }

                scope.spawn(|_| {
                    for i in 0..CONC_COUNT {
                        q.push(i);
                    }
                });
            })
            .unwrap();
        };
    }
    #[test]
    fn test_push_try_pop_many_spmc() {
        push_try_pop_many_spmc!(SegQueue::new(), recv_seg);
        push_try_pop_many_spmc!(MSQueue::new(), recv_ms);
    }

    enum LR {
        Left(i64),
        Right(i64),
    }
    macro_rules! push_try_pop_many_mpmc {
        ($struct:expr) => {
            let q = $struct;
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
        };
    }
    #[test]
    fn test_push_try_pop_many_mpmc() {
        push_try_pop_many_mpmc!(SegQueue::new());
        push_try_pop_many_mpmc!(MSQueue::new());
    }

    macro_rules! is_empty_dont_pop {
        ($struct:expr) => {
            let q = $struct;
            q.push(20);
            q.push(20);
            assert!(!q.is_empty());
            assert!(!q.is_empty());
            assert!(q.try_pop().is_some());
        };
    }
    #[test]
    fn test_is_empty_dont_pop() {
        is_empty_dont_pop!(SegQueue::new());
        is_empty_dont_pop!(MSQueue::new());
    }
}
