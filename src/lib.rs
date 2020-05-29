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

/// TODO
pub trait Queue<T> {
    /// TODO
    fn new() -> Self;

    /// TODO
    fn push(&self, t: T);

    /// TODO
    fn try_pop(&self) -> Option<T>;

    /// TODO
    fn is_empty(&self) -> bool;

    /// TODO
    fn pop(&self) -> T;
}

pub use msqueue::MSQueue;
pub use segqueue::SegQueue;
