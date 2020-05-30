//! Lock-free data structures.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

extern crate crossbeam_epoch;
extern crate crossbeam_utils;

#[macro_use]
mod utils;
mod msqueue;
mod segqueue;

#[cfg(test)]
mod queue_test;

/// Queue trait to implement MSQueue and SegQueue
pub trait Queue<T> {
    /// Create a new, empty queue.
    fn new() -> Self;

    /// Adds `t` to the back of the queue.
    fn push(&self, t: T);

    /// Attempts to dequeue from the front.
    /// Returns `None` if the queue is observed to be empty.
    fn try_pop(&self) -> Option<T>;

    /// Check queue is empty or not.
    fn is_empty(&self) -> bool;

    /// Dequeue from the front.
    fn pop(&self) -> T;
}

pub use msqueue::MSQueue;
pub use segqueue::SegQueue;
