use rustc_hash::FxBuildHasher;
use std::collections::HashMap;

use crate::store::{entry_alloc, entry_dealloc, read_dump, write_dump, PGSS_ENTRY_SIZE};
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
        entry_alloc(&mut s, key(q), b"q", 6, false);
        let e = s.hash.get_mut(&key(q)).unwrap();
        e.counters.calls[PGSS_EXEC] = 1;
        e.counters.usage = 100.0 + q as f64;
    }
    // Next alloc must evict max(10, 5%) = 10 lowest-usage entries.
    entry_alloc(&mut s, key(1000), b"new", 6, false);
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
    entry_alloc(&mut s, key(1), b"select $1", 6, true);
    let e = s.hash.get(&key(1)).unwrap();
    assert!(e.counters.is_sticky());
    assert_eq!(e.counters.usage, 42.0);
    entry_alloc(&mut s, key(2), b"select 2", 6, false);
    assert_eq!(s.hash.get(&key(2)).unwrap().counters.usage, USAGE_INIT);
}

#[test]
fn dealloc_decays_usage_and_tracks_mean_len() {
    let mut s = shared(100);
    entry_alloc(&mut s, key(1), "a".repeat(99).as_bytes(), 6, false);
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

fn dump_of(s: &PgssShared) -> Vec<u8> {
    let mut out = Vec::new();
    write_dump(&mut out, s).unwrap();
    out
}

/// pg_stat_statements.c:780 writes each entry as the raw 432-byte pgssEntry
/// (LP64 layout) followed by the NUL-terminated text, after a 12-byte
/// header and before the 16-byte pgssGlobalStats.
#[test]
fn dump_record_is_the_c_struct_layout() {
    let mut s = shared(10);
    s.stats.dealloc = 3;
    s.stats.stats_reset = 0x0102_0304_0506_0708;
    let k = PgssHashKey { userid: 10, dbid: 5, queryid: -77, toplevel: true };
    entry_alloc(&mut s, k, b"select $1 as dumpprobe", 6, false);
    let e = s.hash.get_mut(&k).unwrap();
    e.counters.calls[PGSS_EXEC] = 9;
    e.counters.rows = 1234;
    e.counters.usage = 3.5;
    e.counters.wal_bytes = u64::MAX - 17;
    e.counters.parallel_workers_launched = 2;
    e.stats_since = 0x1111_2222_3333_4444;
    e.minmax_stats_since = 0x5555_6666_7777_8888;
    let text_len = 22;

    let d = dump_of(&s);
    assert_eq!(d.len(), 12 + PGSS_ENTRY_SIZE + text_len + 1 + 16);
    assert_eq!(&d[0..4], &0x2022_0408u32.to_ne_bytes());
    assert_eq!(&d[4..8], &1800u32.to_ne_bytes());
    assert_eq!(&d[8..12], &1i32.to_ne_bytes());
    let r = &d[12..12 + PGSS_ENTRY_SIZE];
    // pgssHashKey: Oid userid @0, Oid dbid @4, int64 queryid @8, bool toplevel @16, 7 pad.
    assert_eq!(&r[0..4], &10u32.to_ne_bytes());
    assert_eq!(&r[4..8], &5u32.to_ne_bytes());
    assert_eq!(&r[8..16], &(-77i64).to_ne_bytes());
    assert_eq!(r[16], 1);
    assert_eq!(&r[17..24], &[0u8; 7]);
    // Counters @24: calls[PGSS_EXEC] is the second int64; rows is word 12;
    // usage word 29; wal_bytes word 32; parallel_workers_launched word 45.
    let word = |i: usize| u64::from_ne_bytes(r[24 + 8 * i..32 + 8 * i].try_into().unwrap());
    assert_eq!(word(PGSS_EXEC), 9);
    assert_eq!(word(12), 1234);
    assert_eq!(f64::from_bits(word(29)), 3.5);
    assert_eq!(word(32), u64::MAX - 17);
    assert_eq!(word(45), 2);
    // Size query_offset @392 (zero), int query_len @400, int encoding @404,
    // stats_since @408, minmax_stats_since @416, slock_t + pad @424..432.
    assert_eq!(&r[392..400], &[0u8; 8]);
    assert_eq!(&r[400..404], &(text_len as i32).to_ne_bytes());
    assert_eq!(&r[404..408], &6i32.to_ne_bytes());
    assert_eq!(&r[408..416], &0x1111_2222_3333_4444i64.to_ne_bytes());
    assert_eq!(&r[416..424], &0x5555_6666_7777_8888i64.to_ne_bytes());
    assert_eq!(&r[424..432], &[0u8; 8]);
    let t = &d[12 + PGSS_ENTRY_SIZE..];
    assert_eq!(&t[..text_len], b"select $1 as dumpprobe");
    assert_eq!(t[text_len], 0);
    assert_eq!(&t[text_len + 1..text_len + 9], &3i64.to_ne_bytes());
    assert_eq!(&t[text_len + 9..], &0x0102_0304_0506_0708i64.to_ne_bytes());
}

#[test]
fn dump_roundtrip_keeps_stats_and_skips_sticky() {
    let mut s = shared(10);
    s.stats.dealloc = 2;
    s.stats.stats_reset = 99;
    entry_alloc(&mut s, key(1), b"select $1", 6, false);
    let e = s.hash.get_mut(&key(1)).unwrap();
    e.counters.calls[PGSS_EXEC] = 4;
    e.counters.total_time[PGSS_EXEC] = 1.25;
    e.counters.usage = 7.0;
    e.stats_since = 1000;
    e.minmax_stats_since = 2000;
    // Non-UTF8 bytes in a LATIN1 (8) entry load verbatim, as C's fread does.
    let k2 = PgssHashKey { userid: 10, dbid: 7, queryid: 2, toplevel: false };
    entry_alloc(&mut s, k2, b"select $1 as caf\xe9", 8, false);
    s.hash.get_mut(&k2).unwrap().counters.calls[PGSS_EXEC] = 1;
    // Sticky (never executed): written, skipped on load.
    entry_alloc(&mut s, key(3), b"select $1, $2", 6, true);

    let d = dump_of(&s);
    let mut back = shared(10);
    assert!(read_dump(&mut &d[..], &mut back).unwrap());
    assert_eq!(back.hash.len(), 2);
    let e = back.hash.get(&key(1)).unwrap();
    assert_eq!(e.counters.calls[PGSS_EXEC], 4);
    assert_eq!(e.counters.total_time[PGSS_EXEC], 1.25);
    assert_eq!(e.counters.usage, 7.0);
    assert_eq!((e.stats_since, e.minmax_stats_since), (1000, 2000));
    assert_eq!(e.query_text, b"select $1");
    let e2 = back.hash.get(&k2).unwrap();
    assert_eq!(e2.encoding, 8);
    assert_eq!(e2.query_text, b"select $1 as caf\xe9");
    assert!(!back.hash.contains_key(&key(3)));
    assert_eq!((back.stats.dealloc, back.stats.stats_reset), (2, 99));
}

#[test]
fn dump_rejects_bad_header_version_encoding_and_short_file() {
    let mut s = shared(10);
    entry_alloc(&mut s, key(1), b"select $1", 6, false);
    s.hash.get_mut(&key(1)).unwrap().counters.calls[PGSS_EXEC] = 1;
    let d = dump_of(&s);

    let mut bad = d.clone();
    bad[0] ^= 1;
    assert!(!read_dump(&mut &bad[..], &mut shared(10)).unwrap());
    let mut bad = d.clone();
    bad[4..8].copy_from_slice(&1700u32.to_ne_bytes());
    assert!(!read_dump(&mut &bad[..], &mut shared(10)).unwrap());
    // Encoding is the only entry field C sanity-checks (PG_VALID_BE_ENCODING).
    let mut bad = d.clone();
    bad[12 + 404..12 + 408].copy_from_slice(&999i32.to_ne_bytes());
    assert!(!read_dump(&mut &bad[..], &mut shared(10)).unwrap());
    // Truncated: C's fread fails -> read_error.
    let short = &d[..d.len() - 1];
    assert!(read_dump(&mut &short[..], &mut shared(10)).is_err());
}
