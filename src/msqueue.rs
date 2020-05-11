//! msqueue => skeleton from cs492-concur-master/lockfree/queue

use core::mem::{ManuallyDrop, MaybeUninit};
use core::ptr;
use core::sync::atomic::Ordering;

use crossbeam_epoch::{pin, unprotected, Atomic, Owned, Shared};
use crossbeam_utils::CachePadded;

use crate::Queue;

#[derive(Debug, Default)]
/// Michael-Scott queue structure
pub struct MSQueue<T> {
    pub(crate) head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
}

#[derive(Debug)]
pub struct Node<T> {
    data: ManuallyDrop<T>,
    pub(crate) next: Atomic<Node<T>>,
}

unsafe impl<T: Send> Sync for MSQueue<T> {}

impl<T> Queue<T> for MSQueue<T> {
    /// Create a new, empty queue.
    fn new() -> Self
    where
        Self: Sized,
    {
        let q = MSQueue {
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
        };
        let sentinel = Owned::new(Node {
            data: unsafe { MaybeUninit::uninit().assume_init() },
            next: Atomic::null(),
        });
        unsafe {
            let sentinel = sentinel.into_shared(&unprotected());
            q.head.store(sentinel, Ordering::Relaxed);
            q.tail.store(sentinel, Ordering::Relaxed);
        }
        q
    }

    /// Adds `t` to the back of the queue.
    fn push(&self, t: T) {
        let guard = &pin();
        let node = Owned::new(Node {
            data: ManuallyDrop::new(t),
            next: Atomic::null(),
        });
        let node = node.into_shared(guard);
        loop {
            // load acquire tail
            let tail = self.tail.load(Ordering::Acquire, guard);

            // add new node to the queue
            if unsafe { tail.deref() }
                .next
                .compare_and_set(Shared::null(), node, Ordering::Release, guard)
                .is_ok()
            {
                let _ = self
                    .tail
                    .compare_and_set(tail, node, Ordering::Release, guard);
                break;
            } else {
                // if tail is not real tail, move tail pointer forward
                let _ = self.tail.compare_and_set(
                    tail,
                    unsafe { tail.deref() }.next.load(Ordering::Relaxed, guard),
                    Ordering::Release,
                    guard,
                );
            }
        }
    }

    /// Attempts to dequeue from the front.
    /// Returns `None` if the queue is observed to be empty.
    fn try_pop(&self) -> Option<T> {
        let guard = &pin();
        loop {
            let head = self.head.load(Ordering::Acquire, guard);
            let next = unsafe { head.deref() }.next.load(Ordering::Acquire, guard);
            let nextref = some_or!(unsafe { next.as_ref() }, return None);

            // Update tail pointer if it is pointing dummy node.
            let tail = self.tail.load(Ordering::Acquire, guard);
            if tail == head {
                let _ = self
                    .tail
                    .compare_and_set(tail, next, Ordering::Release, guard);
            }

            // Update head pointer to new head and return 't'.
            if self
                .head
                .compare_and_set(head, next, Ordering::Release, guard)
                .is_ok()
            {
                unsafe {
                    guard.defer_destroy(head);
                    return Some(ManuallyDrop::into_inner(ptr::read(&nextref.data)));
                }
            }
        }
    }

    /// Check queue is empty or not.
    fn is_empty(&self) -> bool {
        let guard = &pin();
        let head = self.head.load(Ordering::Acquire, guard);
        let h = unsafe { head.deref() };
        h.next.load(Ordering::Acquire, guard).is_null()
    }

    /// Dequeue from the front.
    fn pop(&self) -> T {
        loop {
            if let Some(t) = self.try_pop() {
                return t;
            }
        }
    }
}

impl<T> Drop for MSQueue<T> {
    fn drop(&mut self) {
        unsafe {
            while let Some(_) = self.try_pop() {}

            // Destroy the remaining sentinel node.
            let sentinel = self.head.load(Ordering::Relaxed, unprotected());
            drop(sentinel.into_owned());
        }
    }
}

mod test {
    use super::*;
    use crate::queue::*;
    use crossbeam_utils::thread::scope;

    const CONC_COUNT: i64 = 1_000_000;

    #[test]
    fn is_empty_dont_pop() {
        let q = Box::new(MSQueue::new());
        test_is_empty_dont_pop(q);
    }

    #[test]
    fn push_try_pop_1() {
        let q = Box::new(MSQueue::new());
        test_push_try_pop_1(q);
    }

    #[test]
    fn push_try_pop_2() {
        let q = Box::new(MSQueue::new());
        test_push_try_pop_2(q);
    }

    #[test]
    fn push_try_pop_many_seq() {
        let q = Box::new(MSQueue::new());
        test_push_try_pop_many_seq(q);
    }

    #[test]
    fn push_pop_1() {
        let q = Box::new(MSQueue::new());
        test_push_pop_1(q);
    }

    #[test]
    fn push_pop_2() {
        let q = Box::new(MSQueue::new());
        test_push_pop_2(q);
    }

    #[test]
    fn push_pop_empty_check() {
        let q = Box::new(MSQueue::new());
        test_push_pop_empty_check(q);
    }

    #[test]
    fn push_pop_many_seq() {
        let q = Box::new(MSQueue::new());
        test_push_pop_many_seq(q);
    }

    #[test]
    fn test_push_pop_many_spsc() {
        //qq: Box<dyn Queue<i64>>) {
        let q = MSQueue::new();
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
        let q = MSQueue::new();
        assert!(q.is_empty());
        scope(|scope| {
            for i in 0..3 {
                let q = &q;
                scope.spawn(move |_| recv_ms(i, q));
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
        let q = MSQueue::new();
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
