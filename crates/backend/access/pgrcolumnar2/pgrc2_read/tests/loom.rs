//! Loom models for the reader's process-shared states (§5 M3-F: "loom
//! models for the shared segment-map/registry states"; M2 claim-channel
//! laws apply — production faces, production fences, never mirror models).
//!
//! Build/run:
//!   RUSTFLAGS="--cfg loom" cargo test -p pgrc2_read --test loom --release
//!
//! Models (each drives the REAL types over real built parts):
//!   1. `concurrent_section_fault_is_write_once` — two threads fault the
//!      same section of one [`OpenPart`]: both see identical bytes, the
//!      fault log records the region ONCE, and the resident counter counts
//!      it once (the write-once segment-map law: racing faulters both read,
//!      first insert wins, the loser's buffer drops).
//!   2. `registry_racing_opens_share_one_entry` — two threads
//!      `open_pinned` the same identity concurrently: the registry ends
//!      with ONE entry and both pins hold the same `Arc` (equal identity ⇒
//!      identical bytes makes the losing open a benign duplicate).
//!   3. `registry_janitor_never_evicts_a_racing_pinned_part` — one thread
//!      holds a pin while another forces the janitor at budget zero: the
//!      pinned entry survives every interleaving (pins are law).
//!   4. `dict_lazy_region_publishes_once` — two threads resolve the same
//!      dict entry concurrently: the payload region publishes exactly once,
//!      so both observe the SAME image pointer (the generation-stability
//!      mechanism under its founding race).

#![cfg(loom)]

use std::sync::Arc;

use loom::thread;

use pgrc2_read::testpart::{build_part, seq_i64_col, text_col, BuiltPart, DictSpec, PartSpec};
use pgrc2_read::{DictHandle, OpenPart, PartExpect, PartIo, PartRegistry};

fn small_part() -> BuiltPart {
    build_part(&PartSpec::new(100, vec![seq_i64_col(1, 100)]))
}

fn open(b: &BuiltPart, dev: u64, ino: u64) -> Arc<OpenPart> {
    Arc::new(
        OpenPart::open(Box::new(b.mem_io(dev, ino)), &PartExpect::none()).expect("open"),
    )
}

/// Model 1: the segment map is write-once under a faulting race.
#[test]
fn concurrent_section_fault_is_write_once() {
    let b = small_part();
    loom::model(move || {
        let part = open(&b, 1, 1);
        // The one values stream section is section-table index 0.
        let p1 = part.clone();
        let t1 = thread::spawn(move || p1.section_bytes(0).expect("fault").bytes().to_vec());
        let bytes2 = part.section_bytes(0).expect("fault").bytes().to_vec();
        let bytes1 = t1.join().expect("join");
        assert_eq!(bytes1, bytes2, "racing faulters see identical bytes");
        let faults = part.faults();
        let section_faults = faults
            .iter()
            .filter(|f| !matches!(
                f.tag,
                pgrc2_read::FaultTag::Tail
                    | pgrc2_read::FaultTag::Footer
                    | pgrc2_read::FaultTag::SectionTable
                    | pgrc2_read::FaultTag::Header
            ))
            .count();
        assert_eq!(section_faults, 1, "one region, one fault log entry");
        assert_eq!(
            part.resident(),
            bytes1.len() as u64,
            "the loser's buffer never counts"
        );
    });
}

/// Model 2: racing opens of one identity share one registry entry.
#[test]
fn registry_racing_opens_share_one_entry() {
    let b = small_part();
    loom::model(move || {
        let reg = Arc::new(PartRegistry::new(u64::MAX));
        let (b1, b2) = (b.mem_io(2, 7), b.mem_io(2, 7));
        let r1 = reg.clone();
        let t1 = thread::spawn(move || {
            r1.open_pinned(&PartExpect::none(), move || {
                Ok(Box::new(b1) as Box<dyn PartIo>)
            })
            .expect("open")
        });
        let p2 = reg
            .open_pinned(&PartExpect::none(), move || {
                Ok(Box::new(b2) as Box<dyn PartIo>)
            })
            .expect("open");
        let p1 = t1.join().expect("join");
        assert!(
            Arc::ptr_eq(p1.part(), p2.part()),
            "equal identity must converge on one shared entry"
        );
        assert_eq!(reg.len(), 1);
    });
}

/// Model 3: the janitor never evicts a pinned part, under any interleaving
/// of pin-drop and maintain.
#[test]
fn registry_janitor_never_evicts_a_racing_pinned_part() {
    let b = small_part();
    loom::model(move || {
        let reg = Arc::new(PartRegistry::new(u64::MAX));
        let io = b.mem_io(3, 9);
        let pin = reg
            .open_pinned(&PartExpect::none(), move || {
                Ok(Box::new(io) as Box<dyn PartIo>)
            })
            .expect("open");
        pin.part().section_bytes(0).expect("resident bytes");
        let key = {
            let i = pin.part().ident();
            (i.dev, i.ino, i.len)
        };
        reg.set_budget(0);
        let r1 = reg.clone();
        let t1 = thread::spawn(move || r1.maintain());
        // While the pin lives, no interleaving may evict the entry.
        assert!(
            reg.get(key).is_some(),
            "pinned part evicted under a racing janitor"
        );
        t1.join().expect("join");
        assert!(reg.get(key).is_some(), "still resident after maintain");
        drop(pin);
    });
}

/// Model 4: the dict lazy region publishes exactly once — both racers see
/// one pointer (generation stability under its founding race).
#[test]
fn dict_lazy_region_publishes_once() {
    let mut col = text_col(1, 100, None);
    col.dict = Some(DictSpec {
        entries: (0..8u32).map(|i| format!("d-{i:03}").into_bytes()).collect(),
    });
    let b = build_part(&PartSpec::new(100, vec![col]));
    loom::model(move || {
        let part = open(&b, 4, 11);
        let h = Arc::new(DictHandle::open(part, None, &[], 1, 0).expect("handle"));
        let h1 = h.clone();
        let t1 = thread::spawn(move || {
            let e = h1.entry(3).expect("entry");
            (e.image.as_ptr() as usize, e.bytes.to_vec())
        });
        let e2 = h.entry(3).expect("entry");
        let (ptr2, bytes2) = (e2.image.as_ptr() as usize, e2.bytes.to_vec());
        let (ptr1, bytes1) = t1.join().expect("join");
        assert_eq!(ptr1, ptr2, "one publication, one pointer");
        assert_eq!(bytes1, b"d-003");
        assert_eq!(bytes2, b"d-003");
    });
}
