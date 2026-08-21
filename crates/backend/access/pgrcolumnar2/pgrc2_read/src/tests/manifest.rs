//! Effective-manifest walk gates (spec §13): the clog fence walks past
//! uncommitted generations, absent CURRENT means empty table, and every
//! structural violation of the publish-ordering law refuses typed.

use std::collections::BTreeSet;

use pgrc2_format::dirlayout::{manifest_file_name, CURRENT_FILE_NAME};

use crate::io::MemTableDir;
use crate::manifest_walk::{resolve_effective, CommitCheck, TableExpect};
use crate::openpart::{OpenPart, PartExpect};
use crate::testpart::{
    build_current, build_manifest, build_part, part_record, seq_i64_col, ManifestSpec, PartSpec,
};
use crate::ReadError;

struct Committed(BTreeSet<u64>);

impl CommitCheck for Committed {
    fn committed(&self, fxid: u64) -> bool {
        self.0.contains(&fxid)
    }
}

fn mspec(gen: u64, prev: u64, fxid: u64) -> ManifestSpec {
    ManifestSpec {
        gen,
        prev_gen: prev,
        publisher_fxid: fxid,
        parts: Vec::new(),
        relfilenumber: 4242,
        spc: 1663,
        db: 5,
        schema_fingerprint: 0xF17E_0001_D00D_BEEF,
        next_part_no: 100,
    }
}

fn expect() -> TableExpect {
    TableExpect {
        relfilenumber: Some(4242),
        spc_db: Some((1663, 5)),
        schema_fingerprint: Some(0xF17E_0001_D00D_BEEF),
    }
}

#[test]
fn absent_current_means_empty_table() {
    let dir = MemTableDir::new();
    let r = resolve_effective(&dir, &Committed(BTreeSet::new()), &expect()).expect("resolve");
    assert!(r.is_none(), "no CURRENT ⇒ no committed publish ⇒ empty");
}

#[test]
fn committed_candidate_is_effective() {
    let mut dir = MemTableDir::new();
    let m1 = build_manifest(&mspec(1, 0, 900));
    dir.put(CURRENT_FILE_NAME, build_current(&m1, 1));
    dir.put(&manifest_file_name(1), m1);
    let r = resolve_effective(&dir, &Committed(BTreeSet::from([900])), &expect())
        .expect("resolve")
        .expect("effective");
    assert_eq!(r.manifest.header.gen, 1);
    assert_eq!(r.walked_past, 0);
}

#[test]
fn clog_fence_walks_past_uncommitted_generations() {
    // gen3 (aborted) → gen2 (crashed-before-commit) → gen1 (committed).
    let mut dir = MemTableDir::new();
    let m1 = build_manifest(&mspec(1, 0, 900));
    let m2 = build_manifest(&mspec(2, 1, 901));
    let m3 = build_manifest(&mspec(3, 2, 902));
    dir.put(CURRENT_FILE_NAME, build_current(&m3, 3));
    dir.put(&manifest_file_name(1), m1);
    dir.put(&manifest_file_name(2), m2);
    dir.put(&manifest_file_name(3), m3);
    let r = resolve_effective(&dir, &Committed(BTreeSet::from([900])), &expect())
        .expect("resolve")
        .expect("effective");
    assert_eq!(r.manifest.header.gen, 1, "the fence lands on gen 1");
    assert_eq!(r.walked_past, 2);
    // A recycled plain xid CANNOT resurrect gen 3: fxids are epoch-qualified
    // and the check is exact-match on the 64-bit value (#254 shape).
    let r = resolve_effective(&dir, &Committed(BTreeSet::from([902 + (1 << 32)])), &expect())
        .expect("resolve");
    assert!(r.is_none() || r.expect("some").manifest.header.gen != 3);
}

#[test]
fn all_uncommitted_is_empty_not_an_error() {
    let mut dir = MemTableDir::new();
    let m1 = build_manifest(&mspec(1, 0, 900));
    dir.put(CURRENT_FILE_NAME, build_current(&m1, 1));
    dir.put(&manifest_file_name(1), m1);
    let r = resolve_effective(&dir, &Committed(BTreeSet::new()), &expect()).expect("resolve");
    assert!(r.is_none());
}

#[test]
fn missing_chained_manifest_is_typed() {
    let mut dir = MemTableDir::new();
    let m2 = build_manifest(&mspec(2, 1, 901));
    dir.put(CURRENT_FILE_NAME, build_current(&m2, 2));
    dir.put(&manifest_file_name(2), m2);
    // gen 1 file absent while the chain demands it.
    let e = resolve_effective(&dir, &Committed(BTreeSet::new()), &expect()).expect_err("missing");
    assert!(matches!(e, ReadError::ManifestMissing { gen: 1 }), "{e}");
    // The candidate itself missing is the same refusal.
    let mut dir = MemTableDir::new();
    let m1 = build_manifest(&mspec(1, 0, 900));
    dir.put(CURRENT_FILE_NAME, build_current(&m1, 1));
    let e = resolve_effective(&dir, &Committed(BTreeSet::new()), &expect()).expect_err("missing");
    assert!(matches!(e, ReadError::ManifestMissing { gen: 1 }));
}

#[test]
fn corrupt_current_and_crc_echo_refuse_typed() {
    let mut dir = MemTableDir::new();
    let m1 = build_manifest(&mspec(1, 0, 900));
    let mut cur = build_current(&m1, 1);
    cur[3] ^= 0xA5;
    dir.put(CURRENT_FILE_NAME, cur);
    dir.put(&manifest_file_name(1), m1.clone());
    let e = resolve_effective(&dir, &Committed(BTreeSet::from([900])), &expect())
        .expect_err("corrupt CURRENT");
    assert!(matches!(e, ReadError::Format(_)), "{e}");

    // manifest_len echo.
    let mut dir = MemTableDir::new();
    let mut long = m1.clone();
    long.push(0);
    dir.put(CURRENT_FILE_NAME, build_current(&m1, 1));
    dir.put(&manifest_file_name(1), long);
    let e = resolve_effective(&dir, &Committed(BTreeSet::from([900])), &expect())
        .expect_err("manifest_len echo");
    assert!(matches!(e, ReadError::Format(_)), "{e}");

    // Manifest body corruption under a valid pointer: internal crc refuses.
    let mut dir = MemTableDir::new();
    let mut bad = m1.clone();
    bad[10] ^= 0xA5;
    dir.put(CURRENT_FILE_NAME, build_current(&m1, 1));
    dir.put(&manifest_file_name(1), bad);
    let e = resolve_effective(&dir, &Committed(BTreeSet::from([900])), &expect())
        .expect_err("manifest crc");
    assert!(matches!(e, ReadError::Format(_)), "{e}");
}

#[test]
fn table_expectations_are_enforced() {
    let mut dir = MemTableDir::new();
    let m1 = build_manifest(&mspec(1, 0, 900));
    dir.put(CURRENT_FILE_NAME, build_current(&m1, 1));
    dir.put(&manifest_file_name(1), m1);
    for bad in [
        TableExpect {
            relfilenumber: Some(9999),
            ..expect()
        },
        TableExpect {
            spc_db: Some((1663, 6)),
            ..expect()
        },
        TableExpect {
            schema_fingerprint: Some(1),
            ..expect()
        },
    ] {
        let e = resolve_effective(&dir, &Committed(BTreeSet::from([900])), &bad)
            .expect_err("expect mismatch");
        assert!(matches!(e, ReadError::OpenMismatch { .. }), "{e}");
    }
}

#[test]
fn end_to_end_manifest_record_gates_the_part_open() {
    // The full readability gate (spec §5): resolve the manifest, build the
    // PartExpect from its record, open the part under those facts.
    let built = build_part(&PartSpec::new(9_000, vec![seq_i64_col(1, 9_000)]));
    let rec = part_record(&built);
    let mut ms = mspec(1, 0, 900);
    ms.parts = vec![rec];
    let m1 = build_manifest(&ms);
    let mut dir = MemTableDir::new();
    dir.put(CURRENT_FILE_NAME, build_current(&m1, 1));
    dir.put(&manifest_file_name(1), m1);
    let eff = resolve_effective(&dir, &Committed(BTreeSet::from([900])), &expect())
        .expect("resolve")
        .expect("effective");
    let rec = eff.manifest.parts[0];
    let pe = PartExpect::from_manifest(
        &rec,
        eff.manifest.header.schema_fingerprint,
        eff.manifest.header.relfilenumber,
        eff.manifest.header.spc,
        eff.manifest.header.db,
    );
    OpenPart::open(Box::new(built.mem_io(10, 10)), &pe).expect("manifest-gated open");
    // And the gate has teeth: a record for a DIFFERENT part refuses.
    let mut wrong = rec;
    wrong.rows += 1;
    wrong.granule_count = pgrc2_format::geom::granule_count(wrong.rows);
    wrong.band_count = pgrc2_format::geom::band_count(wrong.rows);
    let pe = PartExpect::from_manifest(&wrong, rec.footer_off, 4242, 1663, 5);
    assert!(OpenPart::open(Box::new(built.mem_io(10, 10)), &pe).is_err());
}
