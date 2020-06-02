//! msqueue => skeleton from cs492-concur-master/lockfree/queue

use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::Ordering;

use crossbeam_epoch::{pin, unprotected, Atomic, Owned, Shared};
use crossbeam_utils::CachePadded;

use crate::Queue;

#[derive(Debug, Default)]
/// Michael-Scott queue structure
pub struct MSQueue<T> {
    head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
}

pub struct Node<T> {
    data: MaybeUninit<T>,
    next: Atomic<Node<T>>,
}

unsafe impl<T: Send> Sync for MSQueue<T> {}

impl<T> Queue<T> for MSQueue<T> {
    fn new() -> Self
    where
        T: Sized,
    {
        let q = MSQueue {
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
        };
        let sentinel = Owned::new(Node {
            data: unsafe { MaybeUninit::uninit().assume_init() },
            next: Atomic::null(),
        });

        let sentinel = unsafe { sentinel.into_shared(&unprotected()) };
        q.head.store(sentinel, Ordering::Relaxed);
        q.tail.store(sentinel, Ordering::Relaxed);

        q
    }

    fn push(&self, t: T) {
        let guard = &pin();
        let node = Owned::new(Node {
            data: MaybeUninit::new(t),
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
            }

            // if tail is not real tail, move tail pointer forward
            let _ = self.tail.compare_and_set(
                tail,
                unsafe { tail.deref() }.next.load(Ordering::Relaxed, guard),
                Ordering::Release,
                guard,
            );
        }
    }

    fn try_pop(&self) -> Option<T> {
        let guard = &pin();
        loop {
            let head = self.head.load(Ordering::Acquire, guard);
            let next = unsafe { head.deref() }.next.load(Ordering::Acquire, guard);
            let nextref = unsafe { next.as_ref() }?;

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
                    return Some(ptr::read(&nextref.data).assume_init());
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        let guard = &pin();
        let head = self.head.load(Ordering::Acquire, guard);
        let h = unsafe { head.deref() };
        h.next.load(Ordering::Acquire, guard).is_null()
    }

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
        while let Some(_) = self.try_pop() {}
        unsafe {
            // Destroy the remaining sentinel node.
            let sentinel = self.head.load(Ordering::Relaxed, unprotected());
            drop(sentinel.into_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_test::*;

    #[test]
    fn push_try_pop_1() {
        let q = MSQueue::new();
        test_push_try_pop_1(&q);
    }

    #[test]
    fn push_try_pop_2() {
        let q = MSQueue::new();
        test_push_try_pop_2(&q);
    }

    #[test]
    fn push_try_pop_many_seq() {
        let q = MSQueue::new();
        test_push_try_pop_many_seq(&q);
    }

    #[test]
    fn push_pop_1() {
        let q = MSQueue::new();
        test_push_pop_1(&q);
    }

    #[test]
    fn push_pop_2() {
        let q = MSQueue::new();
        test_push_pop_2(&q);
    }

    #[test]
    fn push_pop_empty_check() {
        let q = MSQueue::new();
        test_push_pop_empty_check(&q);
    }

    #[test]
    fn push_pop_many_seq() {
        let q = MSQueue::new();
        test_push_pop_many_seq(&q);
    }

    #[test]
    fn is_empty_dont_pop() {
        let q = MSQueue::new();
        test_is_empty_dont_pop(&q);
    }

    #[test]
    fn push_pop_many_spec() {
        let q = MSQueue::new();
        test_push_pop_many_spsc(&q);
    }

    #[test]
    fn push_try_pop_many_spmc() {
        let q = MSQueue::new();
        test_push_try_pop_many_spmc(&q);
    }

    #[test]
    fn push_try_pop_many_mpmc() {
        let q = MSQueue::new();
        test_push_try_pop_many_mpmc(&q);
    }
}
