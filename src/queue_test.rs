use crossbeam_utils::thread::scope;
use std::ops::Deref;

use crate::Queue;

const CONC_COUNT: i64 = 1_000_000;

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

pub fn test_push_pop_many_spsc<Q: Queue<i64> + std::marker::Sync>(qq: &Q) {
    let q = qq.deref();
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

fn recv<Q: Queue<i64>>(_t: i32, q: &&Q) {
    let q = q.deref();
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
pub fn test_push_try_pop_many_spmc<Q: Queue<i64> + std::marker::Sync>(qq: &Q) {
    let q = qq.deref();
    assert!(q.is_empty());
    scope(|scope| {
        for i in 0..3 {
            let q = &q;
            scope.spawn(move |_| recv(i, q));
        }

        scope.spawn(|_| {
            for i in 0..CONC_COUNT {
                q.push(i);
            }
        });
    })
    .unwrap();
}

pub enum LR {
    Left(i64),
    Right(i64),
}

pub fn test_push_try_pop_many_mpmc<Q: Queue<LR> + std::marker::Sync>(qq: &Q) {
    let q = qq.deref();
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
