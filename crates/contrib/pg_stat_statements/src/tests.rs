use rustc_hash::FxBuildHasher;
use std::collections::HashMap;

use crate::store::{entry_alloc, entry_dealloc};
use crate::{
    Counters, PgssGlobalStats, PgssHashKey, PgssShared, ASSUMED_LENGTH_INIT,
    ASSUMED_MEDIAN_INIT, PGSS_EXEC, USAGE_INIT,
};

fn shared(max: usize) -> PgssShared {
    PgssShared {
        hash: HashMap::with_hasher(FxBuildHasher),
        max,
        cur_median_usage: ASSUMED_MEDIAN_INIT,
        mean_query_len: ASSUMED_LENGTH_INIT,
        stats: PgssGlobalStats::default(),
    }
}

fn key(q: i64) -> PgssHashKey {
    PgssHashKey { userid: 10, dbid: 5, queryid: q, toplevel: true }
}

#[test]
fn sticky_entries_evicted_first() {
    let mut s = shared(100);
    for q in 0..100 {
        entry_alloc(&mut s, key(q), "q", 6, false);
        let e = s.hash.get_mut(&key(q)).unwrap();
        e.counters.calls[PGSS_EXEC] = 1;
        e.counters.usage = 100.0 + q as f64;
    }
    // Next alloc must evict max(10, 5%) = 10 lowest-usage entries.
    entry_alloc(&mut s, key(1000), "new", 6, false);
    assert_eq!(s.hash.len(), 91);
    assert_eq!(s.stats.dealloc, 1);
    for q in 0..10 {
        assert!(!s.hash.contains_key(&key(q)), "lowest-usage entry {q} survived");
    }
    assert!(s.hash.contains_key(&key(1000)));
}

#[test]
fn sticky_starts_at_median_and_unsticks() {
    let mut s = shared(10);
    s.cur_median_usage = 42.0;
    entry_alloc(&mut s, key(1), "select $1", 6, true);
    let e = s.hash.get(&key(1)).unwrap();
    assert!(e.counters.is_sticky());
    assert_eq!(e.counters.usage, 42.0);
    entry_alloc(&mut s, key(2), "select 2", 6, false);
    assert_eq!(s.hash.get(&key(2)).unwrap().counters.usage, USAGE_INIT);
}

#[test]
fn dealloc_decays_usage_and_tracks_mean_len() {
    let mut s = shared(100);
    entry_alloc(&mut s, key(1), &"a".repeat(99), 6, false);
    let e = s.hash.get_mut(&key(1)).unwrap();
    e.counters.calls[PGSS_EXEC] = 1;
    e.counters.usage = 10.0;
    entry_dealloc(&mut s);
    // Zapped (only entry, nvictims >= 10 clamps to n) but decay + stats ran.
    assert_eq!(s.stats.dealloc, 1);
    assert_eq!(s.mean_query_len, 100);
    assert_eq!(s.cur_median_usage, 10.0 * crate::USAGE_DECREASE_FACTOR);
}

#[test]
fn counters_dump_roundtrip() {
    let mut c = Counters { usage: 3.5, ..Counters::default() };
    c.calls[0] = 7;
    c.calls[1] = 9;
    c.total_time[1] = 1.25;
    c.min_time[1] = 0.5;
    c.max_time[1] = 2.0;
    c.mean_time[1] = 1.0;
    c.sum_var_time[1] = 0.25;
    c.rows = 1234;
    c.shared_blks_hit = 5;
    c.temp_blk_write_time = 0.125;
    c.wal_records = 3;
    c.wal_bytes = u64::MAX - 17;
    c.parallel_workers_launched = 2;
    let words = crate::store::counters_to_words(&c);
    let arr: [u64; crate::store::COUNTER_WORDS] = words.try_into().unwrap();
    let back = crate::store::counters_from_words(&arr);
    assert_eq!(crate::store::counters_to_words(&back), crate::store::counters_to_words(&c));
    assert_eq!(back.calls, c.calls);
    assert_eq!(back.wal_bytes, c.wal_bytes);
    assert_eq!(back.rows, c.rows);
}
