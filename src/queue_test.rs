use std::ops::Deref;

use crate::Queue;

pub fn test_push_try_pop_1<Q: Queue<i64>>(qq: &Q) {
    let q = qq.deref();
    assert!(q.is_empty());
    q.push(37);
    assert!(!q.is_empty());
    assert_eq!(q.try_pop(), Some(37));
    assert!(q.is_empty());
}
pub fn test_push_try_pop_2<Q: Queue<i64>>(qq: &Q) {
    let q = qq.deref();
    assert!(q.is_empty());
    q.push(37);
    q.push(48);
    assert_eq!(q.try_pop(), Some(37));
    assert!(!q.is_empty());
    assert_eq!(q.try_pop(), Some(48));
    assert!(q.is_empty());
}

pub fn test_push_try_pop_many_seq<Q: Queue<i64>>(qq: &Q) {
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

pub fn test_push_pop_1<Q: Queue<i64>>(qq: &Q) {
    let q = qq.deref();
    assert!(q.is_empty());
    q.push(37);
    assert!(!q.is_empty());
    assert_eq!(q.pop(), 37);
    assert!(q.is_empty());
}

pub fn test_push_pop_2<Q: Queue<i64>>(qq: &Q) {
    let q = qq.deref();
    q.push(37);
    q.push(48);
    assert_eq!(q.pop(), 37);
    assert_eq!(q.pop(), 48);
}

pub fn test_push_pop_empty_check<Q: Queue<i64>>(qq: &Q) {
    let q = qq.deref();
    assert_eq!(q.is_empty(), true);
    q.push(42);
    assert_eq!(q.is_empty(), false);
    assert_eq!(q.try_pop(), Some(42));
    assert_eq!(q.is_empty(), true);
}

pub fn test_push_pop_many_seq<Q: Queue<i64>>(qq: &Q) {
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

pub fn test_is_empty_dont_pop<Q: Queue<i64>>(qq: &Q) {
    let q = qq.deref();
    q.push(20);
    q.push(20);
    assert!(!q.is_empty());
    assert!(!q.is_empty());
    assert!(q.try_pop().is_some());
}
