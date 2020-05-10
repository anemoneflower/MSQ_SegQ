use std::ops::Deref;

pub trait Queue<T> {
    fn new() -> Self
    where
        Self: Sized;
    fn push(&self, t: T);
    fn try_pop(&self) -> Option<T>;
    fn is_empty(&self) -> bool;
    fn pop(&self) -> T;
}

const CONC_COUNT: i64 = 1_000_000;
use crossbeam_utils::thread::scope;

pub fn test_push_try_pop_1(qq: Box<dyn Queue<i64>>) {
    let q = qq.deref();
    assert!(q.is_empty());
    q.push(37);
    assert!(!q.is_empty());
    assert_eq!(q.try_pop(), Some(37));
    assert!(q.is_empty());
}
pub fn test_push_try_pop_2(qq: Box<dyn Queue<i64>>) {
    let q = qq.deref();
    assert!(q.is_empty());
    q.push(37);
    q.push(48);
    assert_eq!(q.try_pop(), Some(37));
    assert!(!q.is_empty());
    assert_eq!(q.try_pop(), Some(48));
    assert!(q.is_empty());
}

pub fn test_push_try_pop_many_seq(qq: Box<dyn Queue<i64>>) {
    let q = qq.deref();
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

pub fn test_push_pop_1(qq: Box<dyn Queue<i64>>) {
    let q = qq.deref();
    assert!(q.is_empty());
    q.push(37);
    assert!(!q.is_empty());
    assert_eq!(q.pop(), 37);
    assert!(q.is_empty());
}

pub fn test_push_pop_2(qq: Box<dyn Queue<i64>>) {
    let q = qq.deref();
    q.push(37);
    q.push(48);
    assert_eq!(q.pop(), 37);
    assert_eq!(q.pop(), 48);
}

pub fn test_push_pop_empty_check(qq: Box<dyn Queue<i64>>) {
    let q = qq.deref();
    assert_eq!(q.is_empty(), true);
    q.push(42);
    assert_eq!(q.is_empty(), false);
    assert_eq!(q.try_pop(), Some(42));
    assert_eq!(q.is_empty(), true);
}

pub fn test_push_pop_many_seq(qq: Box<dyn Queue<i64>>) {
    let q = qq.deref();
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

pub fn test_is_empty_dont_pop(qq: Box<dyn Queue<i64>>) {
    let q = qq.deref();
    q.push(20);
    q.push(20);
    assert!(!q.is_empty());
    assert!(!q.is_empty());
    assert!(q.try_pop().is_some());
}
