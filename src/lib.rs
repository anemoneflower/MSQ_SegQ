//! Lock-free data structures.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

extern crate crossbeam_epoch;
extern crate crossbeam_utils;

#[macro_use]
mod utils;
mod msqueue;
mod queue; //segqueue

pub use msqueue::MSQueue;
pub use queue::Queue;
