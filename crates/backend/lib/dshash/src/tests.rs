use super::*;
use core::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicU8, Ordering};
use std::sync::Once;

const N_PROCS: usize = 256;

static LW_WAITING: [AtomicU8; N_PROCS] = [const { AtomicU8::new(0) }; N_PROCS];
static LW_WAIT_MODE: [AtomicU8; N_PROCS] = [const { AtomicU8::new(0) }; N_PROCS];
static LW_WAIT_LINK: [AtomicU64; N_PROCS] = [const { AtomicU64::new(0) }; N_PROCS];
static SEMA: [AtomicI32; N_PROCS] = [const { AtomicI32::new(0) }; N_PROCS];
static NEXT_PROC: AtomicI32 = AtomicI32::new(0);

fn pack(node: lmgr_proc_seams::proclist_node) -> u64 {
    ((node.next as u32 as u64) << 32) | node.prev as u32 as u64
}

fn unpack(v: u64) -> lmgr_proc_seams::proclist_node {
    lmgr_proc_seams::proclist_node {
        next: (v >> 32) as u32 as i32,
        prev: v as u32 as i32,
    }
}

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        lmgr_proc_seams::proc_lw_waiting::set(|p| LW_WAITING[p as usize].load(Ordering::Acquire));
        lmgr_proc_seams::set_proc_lw_waiting::set(|p, s| {
            LW_WAITING[p as usize].store(s, Ordering::Release)
        });
        lmgr_proc_seams::proc_lw_wait_mode::set(|p| {
            LW_WAIT_MODE[p as usize].load(Ordering::Acquire)
        });
        lmgr_proc_seams::set_proc_lw_wait_mode::set(|p, m| {
            LW_WAIT_MODE[p as usize].store(m, Ordering::Release)
        });
        lmgr_proc_seams::proc_lw_wait_link::set(|p| {
            unpack(LW_WAIT_LINK[p as usize].load(Ordering::Acquire))
        });
        lmgr_proc_seams::set_proc_lw_wait_link::set(|p, n| {
            LW_WAIT_LINK[p as usize].store(pack(n), Ordering::Release)
        });
        lmgr_proc_seams::pg_semaphore_lock::set(|p| {
            let sema = &SEMA[p as usize];
            loop {
                let c = sema.load(Ordering::Acquire);
                if c > 0
                    && sema
                        .compare_exchange(c, c - 1, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                {
                    return;
                }
                std::thread::yield_now();
            }
        });
        lmgr_proc_seams::pg_semaphore_unlock::set(|p| {
            SEMA[p as usize].fetch_add(1, Ordering::AcqRel);
        });
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
    });
}

fn claim_proc() {
    install_seams();
    let n = NEXT_PROC.fetch_add(1, Ordering::AcqRel);
    assert!((n as usize) < N_PROCS);
    globals::SetMyProcNumber(n);
}

struct TestParams;

struct TestEntry {
    key: u64,
    value: u64,
}

impl DshashParams for TestParams {
    type Key = u64;
    type Entry = TestEntry;

    fn hash(&self, key: &u64) -> DshashHash {
        dshash_memhash(&key.to_ne_bytes())
    }

    fn keys_equal(&self, a: &u64, b: &u64) -> bool {
        a == b
    }

    fn entry_key<'e>(&self, entry: &'e TestEntry) -> &'e u64 {
        &entry.key
    }

    fn new_entry(&self, key: &u64) -> TestEntry {
        TestEntry {
            key: *key,
            value: 0,
        }
    }
}

fn new_table() -> DshashTable<TestParams> {
    claim_proc();
    DshashTable::create(TestParams, 1)
}

#[test]
fn find_missing_returns_none() {
    let t = new_table();
    assert!(t.find_shared(&42).unwrap().is_none());
    assert!(t.find_exclusive(&42).unwrap().is_none());
    assert!(!t.delete_key(&42).unwrap());
}

#[test]
fn insert_find_delete_cycle() {
    let t = new_table();

    let (mut e, found) = t.find_or_insert(&7).unwrap();
    assert!(!found);
    assert_eq!(e.key, 7);
    e.value = 99;
    drop(e);

    let (e, found) = t.find_or_insert(&7).unwrap();
    assert!(found);
    assert_eq!(e.value, 99);
    drop(e);

    let e = t.find_shared(&7).unwrap().unwrap();
    assert_eq!(e.value, 99);
    drop(e);

    let mut e = t.find_exclusive(&7).unwrap().unwrap();
    e.value = 100;
    drop(e);
    assert_eq!(t.find_shared(&7).unwrap().unwrap().value, 100);

    assert!(t.delete_key(&7).unwrap());
    assert!(!t.delete_key(&7).unwrap());
    assert!(t.find_shared(&7).unwrap().is_none());
}

#[test]
fn delete_entry_via_guard() {
    let t = new_table();
    let (_, found) = t.find_or_insert(&1).unwrap();
    assert!(!found);
    let e = t.find_exclusive(&1).unwrap().unwrap();
    e.delete();
    assert!(t.find_shared(&1).unwrap().is_none());
}

#[test]
fn grow_across_partition_splits() {
    let t = new_table();
    let n: u64 = 4096;
    for k in 0..n {
        let (e, found) = t.find_or_insert(&k).unwrap();
        assert!(!found, "key {k} double-inserted");
        assert_eq!(e.key, k);
    }
    assert!(t.size_log2.load(Relaxed) > DSHASH_NUM_PARTITIONS_LOG2);

    for k in 0..n {
        assert_eq!(t.find_shared(&k).unwrap().unwrap().key, k);
    }

    let mut seen = vec![false; n as usize];
    let mut scan = t.seq_scan(false);
    while let Some(e) = scan.next().unwrap() {
        assert!(!seen[e.key as usize], "key {} scanned twice", e.key);
        seen[e.key as usize] = true;
    }
    drop(scan);
    assert!(seen.iter().all(|s| *s));
}

#[test]
fn seqscan_delete_current() {
    let t = new_table();
    for k in 0..100u64 {
        t.find_or_insert(&k).unwrap();
    }

    let mut scan = t.seq_scan(true);
    while let Some(e) = scan.next_mut().unwrap() {
        if e.key % 2 == 0 {
            scan.delete_current();
        }
    }
    drop(scan);

    let mut count = 0;
    let mut scan = t.seq_scan(false);
    while let Some(e) = scan.next().unwrap() {
        assert_eq!(e.key % 2, 1);
        count += 1;
    }
    drop(scan);
    assert_eq!(count, 50);

    for k in 0..100u64 {
        assert_eq!(t.find_shared(&k).unwrap().is_some(), k % 2 == 1);
    }
}

#[test]
fn exclusive_guard_blocks_readers() {
    let t = std::sync::Arc::new(new_table());
    static RELEASED: AtomicBool = AtomicBool::new(false);

    let (mut e, _) = t.find_or_insert(&5).unwrap();
    e.value = 1;

    let t2 = t.clone();
    let reader = std::thread::spawn(move || {
        claim_proc();
        let e = t2.find_shared(&5).unwrap().unwrap();
        assert!(RELEASED.load(Ordering::Acquire), "reader got in under the exclusive guard");
        e.value
    });

    for _ in 0..50 {
        std::thread::yield_now();
    }
    e.value = 42;
    RELEASED.store(true, Ordering::Release);
    drop(e);

    assert_eq!(reader.join().unwrap(), 42);
}

#[test]
fn stress_increment_no_lost_updates() {
    const THREADS: usize = if cfg!(miri) { 3 } else { 8 };
    const OPS: u64 = if cfg!(miri) { 60 } else { 2000 };
    const KEYS: u64 = 64;

    let t = std::sync::Arc::new(new_table());
    let handles: Vec<_> = (0..THREADS)
        .map(|ti| {
            let t = t.clone();
            std::thread::spawn(move || {
                claim_proc();
                let mut tally = [0u64; KEYS as usize];
                let mut rng = 0x9e3779b97f4a7c15u64.wrapping_mul(ti as u64 + 1);
                for _ in 0..OPS {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    let key = (rng >> 33) % KEYS;
                    let (mut e, _) = t.find_or_insert(&key).unwrap();
                    e.value += 1;
                    tally[key as usize] += 1;
                }
                tally
            })
        })
        .collect();

    let mut expected = [0u64; KEYS as usize];
    for h in handles {
        for (k, n) in h.join().unwrap().iter().enumerate() {
            expected[k] += n;
        }
    }

    let mut total = 0;
    for k in 0..KEYS {
        let got = t.find_shared(&k).unwrap().map(|e| e.value).unwrap_or(0);
        assert_eq!(got, expected[k as usize], "lost updates on key {k}");
        total += got;
    }
    assert_eq!(total, (THREADS as u64) * OPS);
}

#[test]
fn stress_insert_delete_churn() {
    const THREADS: usize = if cfg!(miri) { 3 } else { 8 };
    const OPS: usize = if cfg!(miri) { 60 } else { 4000 };
    const KEYS: usize = 128;

    let t = std::sync::Arc::new(new_table());
    let inserts_won: std::sync::Arc<Vec<AtomicU64>> =
        std::sync::Arc::new((0..KEYS).map(|_| AtomicU64::new(0)).collect());
    let deletes_won: std::sync::Arc<Vec<AtomicU64>> =
        std::sync::Arc::new((0..KEYS).map(|_| AtomicU64::new(0)).collect());

    let handles: Vec<_> = (0..THREADS)
        .map(|ti| {
            let t = t.clone();
            let iw = inserts_won.clone();
            let dw = deletes_won.clone();
            std::thread::spawn(move || {
                claim_proc();
                let mut rng = 0xdeadbeefcafef00du64.wrapping_mul(ti as u64 + 3);
                for _ in 0..OPS {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    let key = ((rng >> 33) as usize) % KEYS;
                    if rng & 1 == 0 {
                        let (_, found) = t.find_or_insert(&(key as u64)).unwrap();
                        if !found {
                            iw[key].fetch_add(1, Ordering::AcqRel);
                        }
                    } else if t.delete_key(&(key as u64)).unwrap() {
                        dw[key].fetch_add(1, Ordering::AcqRel);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let mut expected_present = 0u64;
    for k in 0..KEYS {
        let net = inserts_won[k].load(Ordering::Acquire) - deletes_won[k].load(Ordering::Acquire);
        assert!(net <= 1, "key {k} net {net}");
        assert_eq!(
            t.find_shared(&(k as u64)).unwrap().is_some(),
            net == 1,
            "presence mismatch on key {k}"
        );
        expected_present += net;
    }

    let mut scanned = 0u64;
    let mut scan = t.seq_scan(false);
    while scan.next().unwrap().is_some() {
        scanned += 1;
    }
    drop(scan);
    assert_eq!(scanned, expected_present);
}

#[test]
fn stress_grow_under_concurrency() {
    const THREADS: u64 = if cfg!(miri) { 3 } else { 8 };
    const PER_THREAD: u64 = if cfg!(miri) { 80 } else { 2048 };

    let t = std::sync::Arc::new(new_table());
    let handles: Vec<_> = (0..THREADS)
        .map(|ti| {
            let t = t.clone();
            std::thread::spawn(move || {
                claim_proc();
                for k in (ti * PER_THREAD)..((ti + 1) * PER_THREAD) {
                    let (_, found) = t.find_or_insert(&k).unwrap();
                    assert!(!found);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    for k in 0..(THREADS * PER_THREAD) {
        assert!(t.find_shared(&k).unwrap().is_some(), "key {k} missing");
    }
    let mut scanned = 0u64;
    let mut scan = t.seq_scan(false);
    while scan.next().unwrap().is_some() {
        scanned += 1;
    }
    drop(scan);
    assert_eq!(scanned, THREADS * PER_THREAD);
}

#[test]
fn drop_frees_all_entries() {
    let t = new_table();
    for k in 0..1000u64 {
        t.find_or_insert(&k).unwrap();
    }
    drop(t);
}

#[test]
fn hash_helpers_match_hashfn() {
    assert_eq!(dshash_memhash(b"abcd"), hashfn::tag_hash(b"abcd", 4));
    assert_eq!(dshash_strhash(b"ab\0cd", 5), hashfn::string_hash(b"ab\0cd", 5));
}
