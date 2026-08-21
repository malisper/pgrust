//! Scan-driver gates: parallel-native byte identity under dynamic claiming
//! (the L2 dirsha discipline applied to reads), claim-pin law witnesses
//! (IN-4 at read grain, both arms), the OD-9 guard decision, and the
//! verdict-plane consult with census identity at every DOP.

use std::collections::BTreeMap;
use std::sync::Arc;

use pgrc2_format::dirlayout::part_file_name;
use pgrc2_meta::profile::MetaProfile;
use pgrc2_qa::adapters::{full_binding, Probe};
use pgrc2_qa::corpus::{
    append_rows_with_sources, build_fixture, finish_with_sources, int8_fixture, open_part_bytes,
    open_writer, standard_corpus, BuiltFixture, Fixture, OracleVal,
};
use pgrc2_read::OpenPart;
use pgrc2_write::dict::TextSemantics;
use pgrc2_write::elect::{CandidateSource, CodecCandidates, ColumnPosture, DictPolicy, ReferenceCandidates};
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::writer::PartCutPolicy;
use pgrc2_write::wvfs::WriteVfs;

/// A REAL sealed dict-text fixture (the standard corpus carries no
/// dict-electing source; the scan's dict-lane paths need one): low-NDV
/// no-null text sealed under a dict-capable candidate source — DICT_CODES
/// values stream, exec flag, byte-rank dictionary.
fn dict_text_bf(rows: u64, vocab: u64, relf: u64) -> BuiltFixture {
    let oracle: Vec<Option<OracleVal>> = (0..rows)
        .map(|i| {
            let v = (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 17) % vocab;
            Some(OracleVal::Bytes(format!("dict-entry-{v:06}").into_bytes()))
        })
        .collect();
    let fx = Fixture {
        name: "dict_text_scan",
        dir: format!("/qa/t{relf}"),
        spc: 1663,
        db: 5,
        relfilenumber: relf,
        schema: vec![pgrc2_write::testkit::text_col(1)],
        oracle: vec![oracle],
        plans: vec![],
        policy: PartCutPolicy::default(),
    };
    let mut vfs = pgrc2_qa::simvfs::SimVfs::new();
    vfs.mkdir_path(&fx.dir).expect("mkdir");
    let fxid = 900 + relf;
    let mut probe = Probe::new(TxnVerdict::Aborted);
    probe.mark(fxid, TxnVerdict::InProgress);
    let cands = CodecCandidates::new(ColumnPosture {
        cold: true,
        dict: Some(DictPolicy {
            ndv_cap: 1_000_000,
            exec_ok: true,
            sem: TextSemantics::BytesOnly,
        }),
        ..Default::default()
    });
    let reference = ReferenceCandidates;
    let sources: [&dyn CandidateSource; 2] = [&cands, &reference];
    let mut w = open_writer(&fx, fxid).expect("open");
    append_rows_with_sources(&mut vfs, &mut w, &fx, 0, fx.rows(), &sources).expect("append");
    finish_with_sources(&mut vfs, &mut w, &sources).expect("finish");
    w.publish(&mut vfs, &probe).expect("publish");
    probe.mark(fxid, TxnVerdict::Committed);
    let files = vfs.snapshot_dir(&fx.dir);
    let manifest = pgrc2_write::publish::effective_manifest(&mut vfs, &fx.dir, &probe)
        .expect("effective")
        .expect("committed");
    BuiltFixture {
        fx,
        files,
        manifest,
    }
}

use crate::meta::{ScanConst, ScanPredicate};
use crate::scan::{code_bound_guard, ScanColumn, ScanOptions, TableScan};

fn open_parts(bf: &BuiltFixture) -> Vec<Arc<OpenPart>> {
    bf.manifest
        .parts
        .iter()
        .enumerate()
        .map(|(i, rec)| {
            let name = part_file_name(rec.part_no);
            let bytes = bf.files.get(&name).expect("part file present");
            open_part_bytes(bytes, 500 + i as u64).expect("open part")
        })
        .collect()
}

fn scan_of(bf: &BuiltFixture, opts: ScanOptions) -> TableScan {
    let binding = full_binding();
    let parts = open_parts(bf);
    // Root-scannable columns only (structural ArrayDual/JsonbShred arms
    // decode via child lanes — the recorded carve, as in the harness).
    let structural = [
        pgrc2_format::enc::EncodingId::ArrayDual.as_u16(),
        pgrc2_format::enc::EncodingId::JsonbShred.as_u16(),
    ];
    let dir = parts[0].stream_directory().expect("dir");
    let columns: Vec<ScanColumn> = bf
        .fx
        .schema
        .iter()
        .filter(|s| {
            dir.lookup(s.attno, 0, pgrc2_format::part::StreamRole::Values)
                .map(|v| !structural.contains(&v.entry.encoding))
                .unwrap_or(false)
        })
        .map(|s| ScanColumn::root(*s, true))
        .collect();
    TableScan {
        parts,
        binding,
        unwrappers: binding.unwrappers,
        columns,
        predicate: None,
        strview_cols: vec![],
            code_key_cols: vec![],
        deletes: Vec::new(),
        opts,
    }
}

/// The L3 dirsha discipline: dirsha(serial) == dirsha(DOP-4) under dynamic
/// claiming WITH a worker-speed skew (claim-order independence WITNESSED),
/// plus folded-census identity at every DOP (PC-6.3) and the claim-pin
/// balance (PC-2.4: every claim pin released at end_claim).
#[test]
fn serial_eq_dop4_skew_byte_identity_and_census_identity() {
    let mut saw_dict_lane = false;
    let mut saw_strview = false;
    let mut fixtures: Vec<BuiltFixture> =
        standard_corpus().into_iter().map(build_fixture).collect();
    // The dict-lane arm (the standard corpus has no dict-electing source).
    fixtures.push(dict_text_bf(20_000, 3_000, 7301));
    for bf in fixtures {
        let scan = scan_of(&bf, ScanOptions::default());
        if scan.columns.is_empty() {
            continue; // structural-only fixture (child-lane decode)
        }
        let serial = scan.run(1, None).expect("serial scan");
        let par = scan.run(4, Some(1)).expect("dop4 skew scan");
        assert_eq!(
            serial.canon, par.canon,
            "{}: serial vs DOP-4-skew byte identity",
            bf.fx.name
        );
        assert_eq!(
            serial.census, par.census,
            "{}: folded census identity at every DOP (PC-6.3)",
            bf.fx.name
        );
        assert_eq!(par.pins.pins_taken, par.pins.pins_released, "pin balance");
        assert!(serial.census.rows_emitted > 0, "non-vacuous scan");
        saw_dict_lane |= serial.census.dict_lanes_published > 0;
        saw_strview |= serial.census.strview_lanes_built > 0;
    }
    // Non-vacuity across the corpus: the dict-lane + StrView paths ran.
    assert!(saw_dict_lane, "no fixture published a dict lane");
    assert!(saw_strview, "no fixture built StrView cells");
}

/// IN-4 at read grain, both arms: release-on-advance bounds the pinned
/// peak by the worker count; the kill-switch arm (the v3 whole-scan-hold
/// posture) grows it toward the part count — the born-RED direction.
#[test]
fn pin_witness_release_on_advance_vs_kill_arm() {
    // A multi-part int8 table (small cut policy forces several parts).
    let fx = int8_fixture(
        "int8_multipart_pins",
        7101,
        40_000,
        vec![],
        PartCutPolicy {
            max_rows: 6_000,
            ..PartCutPolicy::default()
        },
        |i| Some((i % 977) as i64),
    );
    let bf = build_fixture(fx);
    let nparts = bf.manifest.parts.len() as u64;
    assert!(nparts >= 3, "fixture must cut multiple parts, got {nparts}");

    let mut released = scan_of(&bf, ScanOptions::default());
    released.opts.release_on_advance = true;
    let r = released.run(1, None).expect("release arm");

    let mut held = scan_of(&bf, ScanOptions::default());
    held.opts.release_on_advance = false;
    let h = held.run(1, None).expect("kill arm");

    assert_eq!(r.canon, h.canon, "pin posture never changes bytes");
    assert_eq!(r.pins.pins_taken, r.pins.pins_released, "release arm balance");
    assert_eq!(h.pins.pins_taken, h.pins.pins_released, "kill arm balance at drain");
    // The witness direction: serial release arm never holds more than the
    // claim pin + nothing cached beyond the current part (peak <= 2:
    // claim pin + a moment of overlap at advance); the kill arm's peak
    // reaches parts + the claim pin.
    assert!(
        r.pins.parts_pinned_peak <= 2,
        "release arm peak {} must stay claim-bounded",
        r.pins.parts_pinned_peak
    );
    assert!(
        h.pins.parts_pinned_peak >= nparts,
        "kill arm peak {} must reach the part count {nparts} (the v3 posture)",
        h.pins.parts_pinned_peak
    );
}

/// OD-9: the per-batch max-code guard decision (the born-RED tooth for the
/// trusted-gather license).
#[test]
fn code_bound_guard_fires_exactly_on_out_of_domain_codes() {
    assert!(!code_bound_guard(&[], 5));
    assert!(!code_bound_guard(&[0, 1, 4, 4, 2], 5));
    assert!(code_bound_guard(&[0, 1, 5, 2], 5), "== ncodes is out of domain");
    assert!(code_bound_guard(&[u32::MAX], 5), "escape/corrupt code demotes");
    assert!(!code_bound_guard(&[0], 1));
}

/// Verdict-plane consumers: a selective equality probe on a sealed int8
/// table — zone pruning fires (granules erased), the census attributes,
/// and serial==parallel census identity holds WITH the verdict plane on.
#[test]
fn zone_prune_fires_and_census_is_dop_identical() {
    // Values ascend: granule g holds [g*8192, (g+1)*8192) * 3 — a probe
    // for one value prunes every other granule by zone keys.
    let fx = int8_fixture(
        "int8_zoneprune",
        7102,
        40_000,
        vec![],
        PartCutPolicy::default(),
        |i| Some((i * 3) as i64),
    );
    let bf = build_fixture(fx);
    let mut scan = scan_of(&bf, ScanOptions::default());
    let schema = bf.fx.schema[0];
    let profile = MetaProfile::derive(schema.class, schema.collation_class, schema.semantics)
        .expect("profile");
    scan.predicate = Some((
        ScanPredicate {
            attno: 1,
            // 30000*3 lies in granule 3 (row 30000 of 40000).
            eq: ScanConst::Word((30_000u64 * 3) as u64),
        },
        profile,
    ));
    let serial = scan.run(1, None).expect("serial");
    assert!(
        serial.census.granules_zone_pruned > 0,
        "zone pruning must fire on the selective probe: {:?}",
        serial.census
    );
    assert!(
        serial.census.granules_scanned < serial.census.granules_zone_pruned + serial.census.granules_scanned,
        "sanity"
    );
    let par = scan.run(4, Some(2)).expect("dop4");
    assert_eq!(serial.census, par.census, "verdict census identity at DOP 4");
    assert_eq!(serial.canon, par.canon, "verdict-plane byte identity");
    // Pruned granules carry the deterministic marker.
    let pruned: usize = serial
        .canon
        .values()
        .filter(|v| v.as_slice() == b"P")
        .count();
    assert_eq!(pruned as u64, serial.census.granules_zone_pruned);
}

/// The frame-lazy residency term surfaces through the scan (SB-7): a
/// dict-bearing fixture reports bounded resident dict bytes.
#[test]
fn dict_residency_witness_reports_through_the_scan() {
    let bf = dict_text_bf(20_000, 3_000, 7302);
    let scan = scan_of(&bf, ScanOptions::default());
    let r = scan.run(1, None).expect("scan");
    assert!(
        r.census.dict_lanes_published > 0,
        "dict fixture must publish lanes: {:?}",
        r.census
    );
    assert!(
        r.census.strview_lanes_built > 0,
        "StrView gather must engage on the dict lane"
    );
    assert!(
        r.pins.dict_resident_bytes_peak > 0,
        "dict lanes published but no residency witness"
    );
}

/// Canonical outputs are stable across two runs of the same scan (the
/// determinism reference the dirsha probes lean on).
#[test]
fn canon_is_stable_across_runs() {
    let fx = int8_fixture(
        "int8_stable",
        7103,
        12_000,
        vec![],
        PartCutPolicy::default(),
        |i| if i % 13 == 7 { None } else { Some(i as i64) },
    );
    let bf = build_fixture(fx);
    let scan = scan_of(&bf, ScanOptions::default());
    let a = scan.run(2, None).expect("a");
    let b = scan.run(2, Some(0)).expect("b");
    let am: &BTreeMap<u64, Vec<u8>> = &a.canon;
    assert_eq!(am, &b.canon);
    assert_eq!(a.claimed_units, b.claimed_units);
}

/// The digest comparison currency (the L4 MANIFEST's above-1m fold): the
/// folded canon preserves serial==DOP identity, is a pure per-granule
/// function of the byte canon (digest(bytes) == fold-at-insert), and
/// stays 24 bytes/granule.
#[test]
fn canon_digest_currency_preserves_identity() {
    let bf = dict_text_bf(20_000, 3_000, 7303);
    let scan_bytes = scan_of(&bf, ScanOptions::default());
    let scan_digest = scan_of(
        &bf,
        ScanOptions {
            canon_digest: true,
            ..ScanOptions::default()
        },
    );
    let serial = scan_digest.run(1, None).expect("serial digest");
    let par = scan_digest.run(4, Some(1)).expect("dop4 digest");
    assert_eq!(serial.canon, par.canon, "digest currency serial==DOP-4-skew");
    assert_eq!(serial.census, par.census, "census unchanged by currency");
    assert!(serial.canon.values().all(|v| v.len() == 24), "24B/granule");
    // Pure function of the byte canon: fold the byte-currency canon and
    // compare against the folded-at-insert one.
    let bytes = scan_bytes.run(1, None).expect("byte canon");
    assert_eq!(bytes.canon.len(), serial.canon.len());
    for (unit, b) in &bytes.canon {
        let mut h1: u64 = 0xcbf29ce484222325;
        let mut h2: u64 = 0x84222325cbf29ce4 ^ 0x9e3779b97f4a7c15;
        for &x in b.iter() {
            h1 ^= x as u64;
            h1 = h1.wrapping_mul(0x100000001b3);
            h2 ^= x as u64;
            h2 = h2.wrapping_mul(0x100000001b3);
            h2 = h2.rotate_left(29);
        }
        let mut want = Vec::with_capacity(24);
        want.extend_from_slice(&(b.len() as u64).to_le_bytes());
        want.extend_from_slice(&h1.to_le_bytes());
        want.extend_from_slice(&h2.to_le_bytes());
        assert_eq!(&want, serial.canon.get(unit).expect("unit"), "pure fold");
    }
}

// ---------------------------------------------------------------------------
// DM-2: delete-vector application at the verdict layer (M4-S3b)
// ---------------------------------------------------------------------------

/// DM-2 born-RED: a deletion-bearing part scanned WITH its Dv produces
/// exactly the surviving rows (counts, canon bytes, census), serial ==
/// DOP-4-skew; the Dv-BLIND arm of the SAME part resurrects the deleted
/// rows — the tooth proves it can detect the loss shape (the S4 CRITICAL's
/// landmine, now dead by construction).
#[test]
fn dv_application_born_red_counts_and_byte_identity() {
    use crate::dv::PartDeletes;
    use pgrc2_format::dml::{encode_dv, DvBlockKind};

    let rows = 20_000u64;
    let fx = int8_fixture(
        "int8_dv",
        7401,
        rows,
        vec![],
        PartCutPolicy::default(),
        |i| Some(i as i64),
    );
    let bf = build_fixture(fx);
    assert_eq!(bf.manifest.parts.len(), 1, "single-part fixture");
    let rec = bf.manifest.parts[0];
    assert!(rec.granule_count >= 3, "multi-granule part");

    // Granule 0: a LIST block straddling window boundaries (batch_rows =
    // 1024 — 1023/1024 and 2047/2048 sit on adjacent windows) plus the
    // granule's first rows. Granule 1: a BITMAP block (every third row).
    let g0: Vec<u16> = vec![0, 1, 1023, 1024, 2047, 2048];
    let g1: Vec<u16> = (0..pgrc2_format::geom::GRANULE_ROWS as u16)
        .step_by(3)
        .collect();
    let deleted = (g0.len() + g1.len()) as u64;
    let payload = encode_dv(
        rec.part_no,
        7,
        &[(0, DvBlockKind::List, &g0), (1, DvBlockKind::Bitmap, &g1)],
    )
    .expect("encode_dv");
    let pd = Arc::new(
        PartDeletes::from_dv_payload(&payload, rec.part_no, 7, rec.granule_count)
            .expect("decode dv"),
    );
    assert_eq!(pd.deleted_rows, deleted);

    let mut dv_scan = scan_of(&bf, ScanOptions::default());
    dv_scan.deletes = vec![Some(Arc::clone(&pd))];
    let serial = dv_scan.run(1, None).expect("dv serial");
    let par = dv_scan.run(4, Some(1)).expect("dv dop4 skew");
    assert_eq!(serial.canon, par.canon, "Dv-applied dirsha identity (PC-3.4)");
    assert_eq!(serial.census, par.census, "Dv census identity at every DOP");
    assert_eq!(serial.census.rows_deleted_skipped, deleted);
    assert_eq!(serial.census.rows_emitted, rows - deleted);

    // The born-RED direction: the Dv-blind arm counts the dead rows.
    let blind = scan_of(&bf, ScanOptions::default())
        .run(1, None)
        .expect("blind arm");
    assert_eq!(blind.census.rows_emitted, rows, "blind arm resurrects");
    assert_eq!(blind.census.rows_deleted_skipped, 0);
    assert_ne!(
        blind.canon, serial.canon,
        "canon bytes must witness the deletions (AB-3.3 selection currency)"
    );
}

/// DM-2 refusal teeth: every payload-vs-manifest mismatch is a typed
/// refusal, never a silent Dv-free (or wrong-Dv) scan.
#[test]
fn dv_payload_refusals_are_typed() {
    use crate::dv::PartDeletes;
    use pgrc2_format::dml::{encode_dv, DvBlockKind};

    let payload = encode_dv(5, 3, &[(0, DvBlockKind::List, &[1u16, 5, 9])]).expect("encode");
    // Identity binds: wrong part, wrong generation.
    assert!(PartDeletes::from_dv_payload(&payload, 6, 3, 4).is_err(), "part_no");
    assert!(PartDeletes::from_dv_payload(&payload, 5, 4, 4).is_err(), "dv_gen");
    // Geometry binds: block granule beyond the part's granule_count.
    assert!(PartDeletes::from_dv_payload(&payload, 5, 3, 0).is_err(), "granule bound");
    // Corruption binds (crc trip inside the frozen DvReader).
    let mut bad = payload.clone();
    let n = bad.len();
    bad[n - 1] ^= 1;
    assert!(PartDeletes::from_dv_payload(&bad, 5, 3, 4).is_err(), "crc");
    // The good arm decodes to exactly the encoded membership.
    let pd = PartDeletes::from_dv_payload(&payload, 5, 3, 4).expect("good");
    assert_eq!(pd.deleted_rows, 3);
    let mask = pd.granule_mask(0).expect("granule 0 mask");
    for r in [1u32, 5, 9] {
        assert!(PartDeletes::is_deleted(mask, r), "row {r} deleted");
    }
    for r in [0u32, 2, 4, 6, 8, 10, 8191] {
        assert!(!PartDeletes::is_deleted(mask, r), "row {r} live");
    }
    assert!(pd.granule_mask(1).is_none(), "untouched granule stays None");
}

/// DM-2 composition: the Dv verdict composes with zone pruning (a pruned
/// granule never consults its mask; an unpruned deletion-bearing granule
/// still applies it) — serial==parallel identity holds with BOTH verdict
/// planes engaged.
#[test]
fn dv_composes_with_zone_pruning() {
    use crate::dv::PartDeletes;
    use pgrc2_format::dml::{encode_dv, DvBlockKind};

    let fx = int8_fixture(
        "int8_dv_zone",
        7402,
        40_000,
        vec![],
        PartCutPolicy::default(),
        |i| Some((i * 3) as i64),
    );
    let bf = build_fixture(fx);
    let rec = bf.manifest.parts[0];
    // The probe value lives in granule 3 (row 30000); delete two rows
    // there — one of them the probed row itself — plus rows in granule 0
    // (which the zone plane prunes: its mask must never matter).
    let g0: Vec<u16> = vec![0, 100];
    let g3: Vec<u16> = vec![(30_000 - 3 * 8192) as u16, (30_001 - 3 * 8192) as u16];
    let payload = encode_dv(
        rec.part_no,
        2,
        &[(0, DvBlockKind::List, &g0), (3, DvBlockKind::List, &g3)],
    )
    .expect("encode");
    let pd = Arc::new(
        PartDeletes::from_dv_payload(&payload, rec.part_no, 2, rec.granule_count).expect("dv"),
    );

    let schema = bf.fx.schema[0];
    let profile = MetaProfile::derive(schema.class, schema.collation_class, schema.semantics)
        .expect("profile");
    let mut scan = scan_of(&bf, ScanOptions::default());
    scan.predicate = Some((
        ScanPredicate {
            attno: 1,
            eq: ScanConst::Word((30_000u64 * 3) as u64),
        },
        profile,
    ));
    scan.deletes = vec![Some(pd)];
    let serial = scan.run(1, None).expect("serial");
    let par = scan.run(4, Some(2)).expect("dop4");
    assert_eq!(serial.canon, par.canon, "zone+Dv byte identity");
    assert_eq!(serial.census, par.census, "zone+Dv census identity");
    assert!(serial.census.granules_zone_pruned > 0, "zone plane engaged");
    // Only the SCANNED deletion-bearing granule's rows are skipped: the
    // zone-pruned granule 0 contributes nothing to the Dv census.
    assert_eq!(serial.census.rows_deleted_skipped, g3.len() as u64);
}

// ---------------------------------------------------------------------------
// M4.code-batch-probe: the dict-code currency through the frozen batch ABI
// ---------------------------------------------------------------------------

use std::collections::BTreeSet;

use pgrc2_batch::{Batch, ColRep, DictEpochKey, GuardWord, GUARD_CODE_BOUND};
use pgrc2_claim::{ClaimCursor, ClaimOutcome, NoObserver};
use pgrc2_read::ReadResult;

use crate::scan::{push_varlena_canon, ScanCensus, ScanWorker, SharedCounters};
use crate::spans::GranuleSpans;

/// The probe's stub consumer set (one per worker; fields fold
/// order-independently). Everything stored is OWNED bytes — the
/// claim-scope law: `space()`/`entry_datum` results alias dict-owned
/// storage, so every byte kept beyond the visit callback is copied here;
/// no `Datum` and no `&dyn DictSpace` ever outlives a callback.
#[derive(Default)]
struct ProbeStubs {
    /// group stub: value canon bytes -> survivor count.
    group: BTreeMap<Vec<u8>, u64>,
    /// sort stub, CODES currency: unit -> (epoch, survivor codes).
    sort_codes: BTreeMap<u64, (u64, Vec<u32>)>,
    /// sort stub, HYDRATED currency: unit -> per-survivor canon chunks.
    sort_raw: BTreeMap<u64, Vec<Vec<u8>>>,
    /// emit stub: unit -> survivor canon bytes in window order.
    emit: BTreeMap<u64, Vec<u8>>,
    /// codes arm: per-epoch code -> canon chunk, hydrated LAZILY (first
    /// sight of a code only) — the currency witness.
    memo: BTreeMap<u64, BTreeMap<u32, Vec<u8>>>,
    rows_consumed: u64,
    batches_seen: u64,
    code_lane_batches: u64,
    hydrations: u64,
    epochs: BTreeSet<u64>,
    epoch_keys: BTreeMap<u64, DictEpochKey>,
    narrowed_batches: u64,
    /// Window-order witness: last win_start seen per unit.
    last_win: BTreeMap<u64, u32>,
}

/// The folded probe output the arms compare on.
#[derive(Default)]
struct ProbeProducts {
    group: BTreeMap<Vec<u8>, u64>,
    sort: BTreeMap<u64, Vec<u8>>,
    emit: BTreeMap<u64, Vec<u8>>,
    rows_consumed: u64,
    batches_seen: u64,
    code_lane_batches: u64,
    hydrations: u64,
    epochs: BTreeSet<u64>,
    epoch_keys: BTreeMap<u64, DictEpochKey>,
    narrowed_batches: u64,
}

impl ProbeStubs {
    /// Consume one staged ABI batch (the visit callback body). The dict
    /// column is spec column 0 (the RowId lane rides last). Survivors are
    /// the SELECTION's rows (AB-3.3) — never `0..nrows`.
    fn consume(
        &mut self,
        batch: &Batch,
        gw: GuardWord,
        unit: u64,
        win_start: u32,
        per_row_hydrate: bool,
    ) {
        self.batches_seen += 1;
        let nrows = batch.nrows as usize;
        if batch.sel.len() < nrows {
            self.narrowed_batches += 1;
        }
        let prev = self.last_win.insert(unit, win_start);
        assert!(
            prev.is_none_or(|p| p < win_start),
            "windows arrive in ascending order within a unit"
        );
        let col = &batch.cols[0];
        match col.rep {
            ColRep::DictCodes(lane) => {
                self.code_lane_batches += 1;
                assert!(
                    !gw.has(GUARD_CODE_BOUND),
                    "a published code lane is guard-clean (OD-9)"
                );
                let epoch = lane.epoch().0;
                self.epochs.insert(epoch);
                let key = self.epoch_keys.entry(epoch).or_insert_with(|| lane.epoch_key());
                assert_eq!(
                    *key,
                    lane.epoch_key(),
                    "every batch of an epoch carries ONE epoch_key (AB-2.2)"
                );
                // SAFETY: claim-scoped consume of the published lane (R1):
                // the codes slice and the dict space alias producer-/dict-
                // owned storage and never outlive this callback — every
                // byte kept is copied into owned Vecs below.
                let codes = unsafe { lane.codes(nrows) };
                let space = unsafe { lane.dict().space() };
                assert!(
                    space.code_order_is_value_order(),
                    "byte-rank dictionary: code order embeds value order (C6)"
                );
                let emit = self.emit.entry(unit).or_default();
                if per_row_hydrate {
                    // HYDRATED currency: dict entry -> owned canon bytes
                    // per surviving ROW — the baseline the codes arm must
                    // match byte-for-byte.
                    let raw = self.sort_raw.entry(unit).or_default();
                    for &pos in batch.sel.as_slice() {
                        let r = pos as usize;
                        assert!(col.validity.is_valid(r), "dict lanes are all-valid (C2)");
                        let mut chunk = Vec::new();
                        push_varlena_canon(space.entry_datum(codes[r]).as_u64(), &mut chunk);
                        self.hydrations += 1;
                        emit.extend_from_slice(&chunk);
                        raw.push(chunk.clone());
                        *self.group.entry(chunk).or_insert(0) += 1;
                        self.rows_consumed += 1;
                    }
                } else {
                    // CODES currency: group/sort on the u32 code; value
                    // bytes only through the lazy per-epoch memo.
                    let memo = self.memo.entry(epoch).or_default();
                    let sc = self
                        .sort_codes
                        .entry(unit)
                        .or_insert_with(|| (epoch, Vec::new()));
                    assert_eq!(sc.0, epoch, "one dict epoch per granule unit (Law A)");
                    for &pos in batch.sel.as_slice() {
                        let r = pos as usize;
                        assert!(col.validity.is_valid(r), "dict lanes are all-valid (C2)");
                        let code = codes[r];
                        if !memo.contains_key(&code) {
                            // First sight of the code: the ONLY hydration.
                            let mut c = Vec::new();
                            push_varlena_canon(space.entry_datum(code).as_u64(), &mut c);
                            memo.insert(code, c);
                            self.hydrations += 1;
                        }
                        let chunk = memo.get(&code).expect("just memoized");
                        *self.group.entry(chunk.clone()).or_insert(0) += 1;
                        emit.extend_from_slice(chunk);
                        sc.1.push(code);
                        self.rows_consumed += 1;
                    }
                }
            }
            ColRep::Varlena { .. } => {
                // The OD-9 demotion (checked hydrated form): the datum
                // lane is authoritative — consume it per-row. Counted, but
                // this fixture never takes it; the code-lane non-vacuity
                // assert is the witness.
                assert!(
                    gw.has(GUARD_CODE_BOUND),
                    "an eager varlena batch on a dict fixture must be the OD-9 demotion"
                );
                let emit = self.emit.entry(unit).or_default();
                let raw = self.sort_raw.entry(unit).or_default();
                for &pos in batch.sel.as_slice() {
                    let r = pos as usize;
                    assert!(col.validity.is_valid(r), "demoted gather stays all-valid");
                    let mut chunk = Vec::new();
                    push_varlena_canon(col.datums[r].as_u64(), &mut chunk);
                    self.hydrations += 1;
                    emit.extend_from_slice(&chunk);
                    raw.push(chunk.clone());
                    *self.group.entry(chunk).or_insert(0) += 1;
                    self.rows_consumed += 1;
                }
            }
            other => panic!("unexpected rep for the dict column: {other:?}"),
        }
    }

    /// Finalize the sort stub (per-worker, at drain: every unit this
    /// worker consumed is complete — units never split across workers).
    fn finish(self) -> ProbeProducts {
        let ProbeStubs {
            group,
            sort_codes,
            mut sort_raw,
            emit,
            memo,
            rows_consumed,
            batches_seen,
            code_lane_batches,
            hydrations,
            epochs,
            epoch_keys,
            narrowed_batches,
            last_win: _,
        } = self;
        let mut sort: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
        for (unit, (epoch, mut codes)) in sort_codes {
            // The CODES-currency sort: order by code (code order == value
            // order, asserted at consume), then map to bytes via the memo.
            codes.sort_unstable();
            let epoch_memo = memo.get(&epoch).expect("memo carries every consumed epoch");
            let mut chunks: Vec<Vec<u8>> = codes
                .iter()
                .map(|c| epoch_memo.get(c).expect("memoized at first sight").clone())
                .collect();
            if let Some(raw) = sort_raw.remove(&unit) {
                // Demoted windows inside a codes-arm unit (never on this
                // fixture): merge and fall back to value-byte order —
                // identical to code order by the byte-rank law.
                chunks.extend(raw);
                chunks.sort_by(|a, b| a[4..].cmp(&b[4..]));
            }
            sort.insert(unit, chunks.concat());
        }
        for (unit, mut chunks) in sort_raw {
            // The HYDRATED-currency sort: value-byte order over owned
            // chunks (len prefix stripped: value order is payload memcmp
            // order, the C-collation law).
            chunks.sort_by(|a, b| a[4..].cmp(&b[4..]));
            let prev = sort.insert(unit, chunks.concat());
            assert!(prev.is_none(), "a unit sorts under exactly one currency");
        }
        ProbeProducts {
            group,
            sort,
            emit,
            rows_consumed,
            batches_seen,
            code_lane_batches,
            hydrations,
            epochs,
            epoch_keys,
            narrowed_batches,
        }
    }
}

/// Drive `scan` through [`ScanWorker`]s at `dop` — the `TableScan::run`
/// claim-loop pattern (GranuleSpans over per-part granule counts, one
/// ClaimCursor, SharedCounters, scoped workers with the worker-speed
/// skew), the consumer visitor in place of the canon sink. One stub set
/// per worker, folded after join (PC-6.1 for the census; the products
/// fold order-independently because units never split across workers).
fn drive_code_probe(
    scan: &TableScan,
    dop: usize,
    skew_worker: Option<usize>,
    per_row_hydrate: bool,
) -> (ProbeProducts, ScanCensus) {
    let granule_counts: Vec<u32> = scan
        .parts
        .iter()
        .map(|p| p.footer().granule_count)
        .collect();
    let spans = GranuleSpans::new(&granule_counts);
    let cursor = ClaimCursor::new();
    let shared = SharedCounters::new();
    let workers: Vec<(ProbeStubs, ScanCensus)> = std::thread::scope(|s| {
        let mut handles = Vec::new();
        for w in 0..dop {
            let spans = &spans;
            let cursor = &cursor;
            let shared = &shared;
            let slow = skew_worker == Some(w);
            handles.push(s.spawn(move || {
                let mut worker = ScanWorker::new(scan);
                let mut stubs = ProbeStubs::default();
                let observer = NoObserver;
                loop {
                    if slow {
                        std::thread::sleep(std::time::Duration::from_micros(200));
                    }
                    let mut guard = match cursor.begin_claim(spans, w, &observer) {
                        Ok(g) => g,
                        Err(ClaimOutcome::Drained) => break,
                        Err(_) => {
                            std::thread::yield_now();
                            continue;
                        }
                    };
                    let mut visit = |batch: &Batch,
                                     gw: GuardWord,
                                     unit: u64,
                                     win_start: u32|
                     -> ReadResult<()> {
                        stubs.consume(batch, gw, unit, win_start, per_row_hydrate);
                        Ok(())
                    };
                    worker
                        .drive_claim(&mut guard, spans, shared, &mut visit)
                        .expect("drive_claim");
                    // guard drops HERE: end_claim releases the claim pin
                    // (PC-2.4, by construction).
                }
                (stubs, worker.finish(shared))
            }));
        }
        handles
            .into_iter()
            .map(|h| h.join().expect("worker"))
            .collect()
    });
    let mut census = ScanCensus::default();
    let mut out = ProbeProducts::default();
    for (stubs, c) in workers {
        census.fold(&c);
        let p = stubs.finish();
        for (k, v) in p.group {
            *out.group.entry(k).or_insert(0) += v;
        }
        for (u, b) in p.sort {
            assert!(out.sort.insert(u, b).is_none(), "one worker per unit");
        }
        for (u, b) in p.emit {
            assert!(out.emit.insert(u, b).is_none(), "one worker per unit");
        }
        out.rows_consumed += p.rows_consumed;
        out.batches_seen += p.batches_seen;
        out.code_lane_batches += p.code_lane_batches;
        out.hydrations += p.hydrations;
        out.epochs.extend(p.epochs);
        for (e, k) in p.epoch_keys {
            let prev = out.epoch_keys.entry(e).or_insert(k);
            assert_eq!(*prev, k, "one epoch_key per epoch across workers (AB-2.2)");
        }
        out.narrowed_batches += p.narrowed_batches;
    }
    (out, census)
}

/// M4.code-batch-probe / Amendment 2: "the dict-code currency crosses a
/// probed seam two milestones before M5d's first full crossing" — the
/// scan substrate emits SURVIVOR dict codes + epoch through the frozen
/// batch ABI (`ColRep::DictCodes`: the u32 code lane, `DictEpoch`,
/// `DictEpochKey`) into stub group/sort/emit consumers, and the
/// code-currency products are BYTE-IDENTICAL to a hydrated arm's, serial
/// AND DOP-4-skew, over a REAL Dv-narrowed selection (AB-3.3: survivors
/// != all rows). The currency witness: the codes arm hydrates each code
/// at most once (lazy memo, <= vocab), STRICTLY fewer hydrations than the
/// per-row hydrated arm (== survivor rows) — the codes were the currency,
/// never a hidden hydration.
#[test]
fn code_batch_probe_codes_and_epoch_cross_the_abi_into_stub_consumers() {
    use crate::dv::PartDeletes;
    use pgrc2_format::dml::{encode_dv, DvBlockKind};

    let rows = 20_000u64;
    let vocab = 3_000u64;
    let bf = dict_text_bf(rows, vocab, 7501);
    assert_eq!(bf.manifest.parts.len(), 1, "single-part fixture");
    let rec = bf.manifest.parts[0];
    assert!(rec.granule_count >= 3, "multi-granule part");

    // The dv test's deletion pattern: granule 0 a LIST block straddling
    // window boundaries, granule 1 a BITMAP block (every 5th row) — so
    // the batch selection carries real survivors.
    let g0: Vec<u16> = vec![0, 1, 1023, 1024, 2047, 2048];
    let g1: Vec<u16> = (0..pgrc2_format::geom::GRANULE_ROWS as u16)
        .step_by(5)
        .collect();
    let deleted = (g0.len() + g1.len()) as u64;
    let payload = encode_dv(
        rec.part_no,
        7,
        &[(0, DvBlockKind::List, &g0), (1, DvBlockKind::Bitmap, &g1)],
    )
    .expect("encode_dv");
    let pd = Arc::new(
        PartDeletes::from_dv_payload(&payload, rec.part_no, 7, rec.granule_count)
            .expect("decode dv"),
    );
    assert_eq!(pd.deleted_rows, deleted);
    let survivors = rows - deleted;

    // ONE TableScan: both arms consume the SAME drive output and differ
    // only in consumption currency. StrView cell build is OFF so the dict
    // column crosses the seam as ColRep::DictCodes — the default
    // build_strviews_from_dict flips the rep to the cell lane, a different
    // currency; this cell probes the CODE lane.
    let mut scan = scan_of(
        &bf,
        ScanOptions {
            build_strviews: false,
            ..ScanOptions::default()
        },
    );
    assert_eq!(scan.columns.len(), 1, "one dict text column");
    scan.deletes = vec![Some(Arc::clone(&pd))];

    let (c1, c1_census) = drive_code_probe(&scan, 1, None, false);
    let (c4, c4_census) = drive_code_probe(&scan, 4, Some(1), false);
    let (h1, h1_census) = drive_code_probe(&scan, 1, None, true);
    let (h4, h4_census) = drive_code_probe(&scan, 4, Some(1), true);

    // The cell's clause: every product byte-identical across the currency
    // seam AND across DOPs.
    let pairs = [
        ("dop1 codes vs hydrated", &c1, &h1),
        ("dop4-skew codes vs hydrated", &c4, &h4),
        ("codes serial vs dop4-skew", &c1, &c4),
        ("hydrated serial vs dop4-skew", &h1, &h4),
    ];
    for (name, a, b) in pairs {
        assert_eq!(a.group, b.group, "{name}: group product");
        assert_eq!(a.sort, b.sort, "{name}: sort product");
        assert_eq!(a.emit, b.emit, "{name}: emit product");
        assert_eq!(a.epochs, b.epochs, "{name}: epochs seen");
        assert_eq!(a.epoch_keys, b.epoch_keys, "{name}: epoch keys");
    }
    // The drive census is a pure function of the scan, never the currency.
    assert_eq!(c1_census, c4_census, "codes-arm census identity (PC-6.3)");
    assert_eq!(h1_census, h4_census, "hydrated-arm census identity (PC-6.3)");
    assert_eq!(c1_census, h1_census, "currency never touches the census");
    assert!(c1_census.dict_lanes_published > 0, "dict lanes crossed");
    assert_eq!(c1_census.rows_deleted_skipped, deleted);
    assert_eq!(c1_census.rows_emitted, survivors);

    // Survivor accounting + non-vacuity, every arm.
    assert!(survivors > 0);
    for (name, p) in [("c1", &c1), ("c4", &c4), ("h1", &h1), ("h4", &h4)] {
        assert_eq!(p.rows_consumed, survivors, "{name}: survivor rows");
        assert!(p.code_lane_batches > 0, "{name}: code-lane batches crossed");
        assert_eq!(
            p.batches_seen, p.code_lane_batches,
            "{name}: every batch crossed as a code lane (no demotions)"
        );
        assert!(
            p.narrowed_batches > 0,
            "{name}: at least one batch narrowed by the Dv (selection < nrows)"
        );
        assert!(!p.epochs.is_empty(), "{name}: epochs seen");
        assert_eq!(
            p.group.values().sum::<u64>(),
            survivors,
            "{name}: group counts fold to the survivor count"
        );
        assert_eq!(
            p.emit.len(),
            rec.granule_count as usize,
            "{name}: every granule unit emitted"
        );
    }

    // The currency witness: lazy memo hydration vs per-row hydration.
    assert!(
        c1.hydrations > 0 && c1.hydrations <= vocab,
        "codes arm memo-bounded: {}",
        c1.hydrations
    );
    assert!(
        c4.hydrations > 0 && c4.hydrations <= vocab,
        "codes arm memo-bounded: {}",
        c4.hydrations
    );
    assert_eq!(h1.hydrations, survivors, "hydrated arm hydrates per survivor row");
    assert_eq!(h4.hydrations, survivors, "hydrated arm hydrates per survivor row");
    assert!(c1.hydrations < h1.hydrations, "the codes were the currency (dop1)");
    assert!(c4.hydrations < h4.hydrations, "the codes were the currency (dop4)");
}
