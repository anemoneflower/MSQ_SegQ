//! msqueue => skeleton from cs492-concur-master/lockfree/queue

use core::mem::{ManuallyDrop, MaybeUninit};
use core::ptr;
use core::sync::atomic::Ordering;

use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Shared};
use crossbeam_utils::CachePadded;

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
impl<T> MSQueue<T> {
    /// Create a new, empty queue.
    pub fn new() -> MSQueue<T> {
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

    /// Adds `t` to the back of the queue, possibly waking up threads blocked on `pop`.
    pub fn push(&self, t: T, guard: &Guard) {
        let node = Owned::new(Node {
            data: ManuallyDrop::new(t),
            next: Atomic::null(),
        });
        let node = node.into_shared(guard);
        loop {
            // load acquire tail
            let tail = self.tail.load(Ordering::Acquire, guard);

            // check if this is real tail
            let tail_ref = unsafe { tail.deref() };
            let next = tail_ref.next.load(Ordering::Acquire, guard);
            if !next.is_null() {
                // move tail pointer forward
                let _ = self
                    .tail
                    .compare_and_set(tail, next, Ordering::Release, guard);
                continue;
            }

            if tail_ref
                .next
                .compare_and_set(Shared::null(), node, Ordering::Release, guard)
                .is_ok()
            {
                let _ = self
                    .tail
                    .compare_and_set(tail, node, Ordering::Release, guard);
                break;
            }
        }
    }

    /// Attempts to dequeue from the front.
    /// Returns `None` if the queue is observed to be empty.
    pub fn try_pop(&self, guard: &Guard) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Acquire, guard);
            let next = unsafe { head.deref() }.next.load(Ordering::Acquire, guard);
            let nextref = some_or!(unsafe { next.as_ref() }, return None);

            // Move tail
            let tail = self.tail.load(Ordering::Acquire, guard);
            if tail == head {
                let _ = self
                    .tail
                    .compare_and_set(tail, next, Ordering::Release, guard);
            }

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
}

impl<T> Drop for MSQueue<T> {
    fn drop(&mut self) {
        unsafe {
            while let Some(_) = self.try_pop(unprotected()) {}

            // Destroy the remaining sentinel node.
            let sentinel = self.head.load(Ordering::Relaxed, unprotected());
            drop(sentinel.into_owned());
        }
    }
}
