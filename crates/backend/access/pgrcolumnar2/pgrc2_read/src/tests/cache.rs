//! Part cache gates (§5 M3-F): equal identity ⇒ identical bytes (shared
//! entries), pin + LRU janitor under budget pressure, no eviction of pinned
//! parts, eviction safety for in-flight holders.

use std::sync::Arc;

use pgrc2_format::abi::{ByteArena, DecodeOut};

use crate::cursor::{reference_binding_leaked, StreamCursor};
use crate::io::PartIo;
use crate::openpart::PartExpect;
use crate::registry::{PartPin, PartRegistry};
use crate::testpart::{build_part, seq_i64_col, BuiltPart, PartSpec};

use super::ArenaBuf;

fn small_part() -> BuiltPart {
    build_part(&PartSpec::new(9_000, vec![seq_i64_col(1, 9_000)]))
}

fn opener(b: &BuiltPart, dev: u64, ino: u64) -> impl FnOnce() -> crate::ReadResult<Box<dyn PartIo>> {
    let io = b.mem_io(dev, ino);
    move || Ok(Box::new(io) as Box<dyn PartIo>)
}

#[test]
fn equal_identity_shares_one_entry() {
    let b = small_part();
    let reg = PartRegistry::new(u64::MAX);
    let p1 = reg
        .open_pinned(&PartExpect::none(), opener(&b, 1, 10))
        .expect("open 1");
    let p2 = reg
        .open_pinned(&PartExpect::none(), opener(&b, 1, 10))
        .expect("open 2");
    assert!(
        Arc::ptr_eq(p1.part(), p2.part()),
        "equal identity must share one parsed entry"
    );
    assert_eq!(reg.len(), 1);
    let c = reg.counters();
    assert_eq!((c.hits, c.misses), (1, 1), "second open is a stat-only hit");
}

#[test]
fn copied_part_gets_a_new_identity() {
    let b = small_part();
    let reg = PartRegistry::new(u64::MAX);
    let p1 = reg
        .open_pinned(&PartExpect::none(), opener(&b, 1, 10))
        .expect("open");
    // Same bytes, different ino: a COPY — spec §11: new identity, own entry.
    let p2 = reg
        .open_pinned(&PartExpect::none(), opener(&b, 1, 11))
        .expect("open copy");
    assert!(!Arc::ptr_eq(p1.part(), p2.part()));
    assert_ne!(p1.part().uuid(), p2.part().uuid());
    assert_eq!(reg.len(), 2);
}

#[test]
fn janitor_evicts_lru_unpinned_and_never_pinned() {
    let b = small_part();
    let reg = PartRegistry::new(u64::MAX);
    let p1 = reg
        .open_pinned(&PartExpect::none(), opener(&b, 1, 21))
        .expect("p1");
    let p2 = reg
        .open_pinned(&PartExpect::none(), opener(&b, 1, 22))
        .expect("p2");
    let p3 = reg
        .open_pinned(&PartExpect::none(), opener(&b, 1, 23))
        .expect("p3");
    for p in [&p1, &p2, &p3] {
        p.part().prefault_all_sections().expect("warm");
    }
    assert!(reg.resident_bytes() > 0);
    // Unpin p1 and p2; touch p1 so p2 is the LRU victim.
    let key1 = {
        let i = p1.part().ident();
        (i.dev, i.ino, i.len)
    };
    let key2 = {
        let i = p2.part().ident();
        (i.dev, i.ino, i.len)
    };
    let key3 = {
        let i = p3.part().ident();
        (i.dev, i.ino, i.len)
    };
    drop(p1);
    drop(p2);
    reg.get(key1).expect("touch p1");
    // Budget forces eviction of ALL unpinned; pinned p3 must survive.
    reg.set_budget(0);
    reg.maintain();
    assert!(reg.get(key2).is_none(), "LRU unpinned p2 evicted");
    assert!(reg.get(key1).is_none(), "then p1");
    assert!(reg.get(key3).is_some(), "pinned p3 NEVER evicted");
    let c = reg.counters();
    assert_eq!(c.evictions, 2);
    assert!(
        c.over_budget_events >= 1,
        "running over budget under pin pressure is witnessed"
    );
    // After the pin drops, the janitor may reclaim it.
    drop(p3);
    reg.maintain();
    assert!(reg.get(key3).is_none(), "unpinned p3 reclaimed");
    assert_eq!(reg.len(), 0);
}

#[test]
fn partial_budget_keeps_recently_used() {
    let b = small_part();
    let reg = PartRegistry::new(u64::MAX);
    let pins: Vec<PartPin> = (0..3)
        .map(|i| {
            let p = reg
                .open_pinned(&PartExpect::none(), opener(&b, 2, 30 + i))
                .expect("open");
            p.part().prefault_all_sections().expect("warm");
            p
        })
        .collect();
    let per_part = reg.resident_bytes() / 3;
    let keys: Vec<_> = pins
        .iter()
        .map(|p| {
            let i = p.part().ident();
            (i.dev, i.ino, i.len)
        })
        .collect();
    drop(pins);
    // Budget for two parts: exactly one (the LRU) goes.
    reg.set_budget(per_part * 2 + per_part / 2);
    reg.get(keys[1]).expect("touch");
    reg.get(keys[2]).expect("touch");
    reg.maintain();
    assert!(reg.get(keys[0]).is_none(), "oldest evicted");
    assert!(reg.get(keys[1]).is_some());
    assert!(reg.get(keys[2]).is_some());
    assert_eq!(reg.counters().evictions, 1);
}

#[test]
fn eviction_is_safe_for_inflight_holders() {
    // Eviction removes the map entry, never the bytes: an in-flight holder
    // keeps decoding, and its section pointers stay generation-stable.
    let b = small_part();
    let reg = PartRegistry::new(u64::MAX);
    let pin = reg
        .open_pinned(&PartExpect::none(), opener(&b, 3, 40))
        .expect("open");
    let part = pin.part().clone();
    let binding = reference_binding_leaked();
    let mut cur = StreamCursor::open(part.clone(), binding, 1, 0).expect("cursor");
    let key = {
        let i = part.ident();
        (i.dev, i.ino, i.len)
    };
    drop(pin);
    reg.set_budget(0);
    reg.maintain();
    assert!(reg.get(key).is_none(), "entry evicted");
    // The held Arc still decodes correctly.
    let mut d = vec![0u64; 8192];
    let mut ab = ArenaBuf::new(1 << 20);
    let mut out = DecodeOut {
        datums: &mut d,
        arena: ByteArena::new(ab.bytes_mut()),
    };
    let n = cur.decode_full(0, &mut out).expect("decode after eviction");
    assert_eq!(n, 8192);
}

#[test]
fn entry_count_cap_bounds_zero_resident_entries() {
    // Regression: entries that are opened but never decoded have
    // resident()==0, so the byte-budget janitor never reclaims them even
    // though each pins a live kernel fd. A stream of such opens (e.g. a
    // self-scan of an aborted publish, or a zero-row scan) must not grow the
    // registry without bound: the entry-count cap reclaims LRU-unpinned
    // entries regardless of resident bytes.
    let b = small_part();
    let reg = PartRegistry::new(u64::MAX); // byte budget can never bite
    reg.set_max_entries(2);
    assert_eq!(reg.resident_bytes(), 0, "no part decoded: zero resident");
    // Open many distinct identities, dropping each pin so it is reclaimable.
    for ino in 0..64u64 {
        let pin = reg
            .open_pinned(&PartExpect::none(), opener(&b, 7, 1000 + ino))
            .expect("open");
        drop(pin);
        assert!(
            reg.len() <= 2,
            "entry count stays bounded by the cap ({} entries)",
            reg.len()
        );
    }
    assert_eq!(reg.resident_bytes(), 0, "still nothing decoded");
    assert!(
        reg.counters().evictions >= 62,
        "the cap drove count-based eviction of zero-resident entries"
    );
}

#[test]
fn invalidate_and_clear() {
    let b = small_part();
    let reg = PartRegistry::new(u64::MAX);
    let pin = reg
        .open_pinned(&PartExpect::none(), opener(&b, 4, 50))
        .expect("open");
    let key = {
        let i = pin.part().ident();
        (i.dev, i.ino, i.len)
    };
    drop(pin);
    assert!(reg.invalidate(key), "invalidate drops the entry");
    assert!(reg.get(key).is_none());
    let _ = reg
        .open_pinned(&PartExpect::none(), opener(&b, 4, 50))
        .expect("reopen");
    assert_eq!(reg.counters().misses, 2, "reopen after invalidate is a miss");
    reg.clear();
    assert!(reg.is_empty());
}
