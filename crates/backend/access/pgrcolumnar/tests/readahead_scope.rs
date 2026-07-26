// Claim-time readahead scope guard (parallelism-redesign §2.8, M1 lane B;
// serial default flipped 2026-07-15 — cold-readahead lane, ratified):
// over a REAL multi-row-group part written by the production writer,
// (a) a SERIAL scan drive advises at the default through the CLAIM channel
//     (rgs_claim_readahead) and NEVER through the legacy parallel channel
//     (rgs_readahead == 0 stays structural);
//     PGRUST_CBSTORE_READAHEAD_SERIAL=0 restores the historical
//     prefetch-free serial arm;
// (b) a PARALLEL scan drive advises exactly the next unclaimed row group
//     per claim (nrgs - 1 in-range advises for a single-worker drive) and
//     stages byte-identical row counts to the serial drive;
// (c) PGRUST_CBSTORE_READAHEAD=0 kills the parallel advises too;
// (d) Part::advise_willneed computes in-bounds extents for every RG and
//     column subset without panicking (footer-metadata-only contract).
use std::rc::Rc;
use std::sync::Mutex;

use pgrcolumnar::scan::CbScanDescData;
use pgrcolumnar::writer::open_writer_at;
use pgrcolumnar::ColType;
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use types_rel::{FormData_pg_class, LockInfoData, LockRelId, Relation, RELKIND_RELATION};
use types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};

// The env gate is read at scan construction and the process env is global:
// serialize the tests so the kill-switch test cannot race the others.
static ENV_LOCK: Mutex<()> = Mutex::new(());

// Inline 4B-U text image; `keep` owns the bytes for the datum's lifetime.
fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
    let mut v = Vec::with_capacity(4 + s.len());
    v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + s.len()));
    v.extend_from_slice(s);
    keep.push(v);
    Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
}

fn tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    // (int8, text) — the scan takes coltypes explicitly (new_with_part), so
    // the tupdesc only shapes the relation shell.
    let atts = [
        FormData_pg_attribute {
            attnum: 1,
            attlen: 8,
            attbyval: true,
            attalign: ::types_tuple::TYPALIGN_DOUBLE,
            attstorage: ::types_tuple::TYPSTORAGE_PLAIN,
            ..Default::default()
        },
        FormData_pg_attribute {
            attnum: 2,
            attlen: -1,
            attbyval: false,
            attalign: ::types_tuple::TYPALIGN_INT,
            attstorage: ::types_tuple::TYPSTORAGE_EXTENDED,
            ..Default::default()
        },
    ];
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for att in atts {
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: 2,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn test_relation<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("cbreadahead");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: 0, // AM dispatch is bypassed by new_with_part
        relfilenode: oid,
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relisshared: false,
        relpersistence: RELPERSISTENCE_PERMANENT,
        relkind: RELKIND_RELATION,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: b'd',
        relispartition: false,
        relfrozenxid: 3,
        relminmxid: 1,
    };
    let data = ::types_rel::RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: std::cell::Cell::new(true),
        rd_createSubid: std::cell::Cell::new(0),
        rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_droppedSubid: std::cell::Cell::new(0),
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: oid, dbId: 5 } },
        rd_rel,
        rd_att: tupdesc(mcx),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
        pgstat_enabled: std::cell::Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
    };
    Relation::open(data, None)
}

fn scan_base<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    parallel: Option<std::ptr::NonNull<::tableam_vocab::ParallelBlockTableScanDescData>>,
) -> ::tableam_vocab::TableScanDescData<'mcx> {
    ::tableam_vocab::TableScanDescData {
        rs_rd: test_relation(mcx, oid),
        rs_snapshot: None,
        rs_nkeys: 0,
        rs_key: PgVec::new_in(mcx),
        rs_mintid: Default::default(),
        rs_maxtid: Default::default(),
        rs_flags: 0,
        rs_parallel: parallel,
        rs_am: ::tableam_vocab::TableAm::Pgrcolumnar,
    }
}

// Write a 3-row-group (int8, text) part and return (path, nrows).
fn write_part(tag: &str) -> (String, usize) {
    let path = std::env::temp_dir()
        .join(format!("cbreadahead-{tag}-{}.cb", std::process::id()))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&path);
    std::fs::write(&path, []).unwrap();
    // 3 row groups: 2 full RGs (RG_ROWS = 65536) + a partial tail.
    let n_rows = 2 * 65_536 + 12_345;
    let vocab: [&[u8]; 6] = [b"alpha", b"beta", b"gamma", b"delta", b"eps", b"zeta"];
    let mut w =
        open_writer_at(&path, vec![ColType::I64, ColType::Text]).unwrap();
    let mut keep = Vec::new();
    for i in 0..n_rows {
        let vals =
            [Datum::from_i64((i as i64 * 13) % 1000), text_datum(vocab[i % 6], &mut keep)];
        w.append_row(&vals, &[false, false]).unwrap();
        if keep.len() > 512 {
            keep.clear();
        }
    }
    w.finish().unwrap();
    (path, n_rows)
}

fn open_part(path: &str) -> std::sync::Arc<pgrcolumnar::reader::Part> {
    std::sync::Arc::new(pgrcolumnar::reader::Part::open(path, 2).unwrap().expect("part exists"))
}

// Drive next_window to exhaustion; returns total staged rows.
fn drive(scan: &mut CbScanDescData<'_>) -> usize {
    let mut staged = 0usize;
    loop {
        let n = scan.next_window().unwrap();
        if n == 0 {
            return staged;
        }
        staged += n as usize;
    }
}

#[test]
fn serial_scan_advises_claim_channel_only() {
    let _g = ENV_LOCK.lock().unwrap();
    let (path, n_rows) = write_part("serial");
    let part = open_part(&path);
    assert!(part.rgs.len() >= 3, "fixture must span row groups");
    let ctx = MemoryContext::new("cbreadahead-serial");
    let mcx = ctx.mcx();
    // Defaults (2026-07-15 flip): the serial drive advises through the
    // CLAIM channel; the legacy parallel-arm channel stays structurally 0.
    let mut scan = CbScanDescData::new_with_part(
        scan_base(mcx, 41011, None),
        Some(part),
        vec![ColType::I64, ColType::Text],
    );
    assert_eq!(drive(&mut scan), n_rows);
    assert!(scan.rgs_claim_readahead > 0, "serial default advises (claim channel)");
    assert_eq!(scan.rgs_readahead, 0, "legacy parallel channel stays 0 on serial");
    // Rescan advises again (fresh physical pass).
    let before = scan.rgs_claim_readahead;
    scan.reset_position();
    assert_eq!(drive(&mut scan), n_rows);
    assert!(scan.rgs_claim_readahead > before, "serial rescan advises again");
    assert_eq!(scan.rgs_readahead, 0);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn serial_opt_out_restores_prefetch_free_arm() {
    let _g = ENV_LOCK.lock().unwrap();
    let (path, n_rows) = write_part("serialoptout");
    let part = open_part(&path);
    let ctx = MemoryContext::new("cbreadahead-serialoptout");
    let mcx = ctx.mcx();
    std::env::set_var("PGRUST_CBSTORE_READAHEAD_SERIAL", "0");
    let mut scan = CbScanDescData::new_with_part(
        scan_base(mcx, 41018, None),
        Some(part),
        vec![ColType::I64, ColType::Text],
    );
    std::env::remove_var("PGRUST_CBSTORE_READAHEAD_SERIAL");
    assert_eq!(drive(&mut scan), n_rows);
    assert_eq!(scan.rgs_claim_readahead, 0, "SERIAL=0 restores the prefetch-free arm");
    assert_eq!(scan.rgs_readahead, 0);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn parallel_claims_advise_next_rg() {
    let _g = ENV_LOCK.lock().unwrap();
    let (path, n_rows) = write_part("parallel");
    let part = open_part(&path);
    let nrgs = part.rgs.len();
    assert!(nrgs >= 3, "fixture must span row groups");
    let ctx = MemoryContext::new("cbreadahead-parallel");
    let mcx = ctx.mcx();
    let mut pdesc =
        Box::new(::tableam_vocab::ParallelBlockTableScanDescData::default());
    let pptr = std::ptr::NonNull::from(pdesc.as_mut());
    let mut scan = CbScanDescData::new_with_part(
        scan_base(mcx, 41012, Some(pptr)),
        Some(part),
        vec![ColType::I64, ColType::Text],
    );
    // Single-worker parallel drive: stages every row exactly once and
    // advises each claim's successor while it is still unclaimed — the
    // out-of-range successors of the last claims are skipped, so exactly
    // nrgs - 1 advises land.
    assert_eq!(drive(&mut scan), n_rows);
    assert_eq!(scan.rgs_readahead, (nrgs - 1) as u64);
    drop(scan);
    drop(pdesc);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn parallel_kill_switch_disables_advises() {
    let _g = ENV_LOCK.lock().unwrap();
    let (path, n_rows) = write_part("killswitch");
    let part = open_part(&path);
    let ctx = MemoryContext::new("cbreadahead-kill");
    let mcx = ctx.mcx();
    let mut pdesc =
        Box::new(::tableam_vocab::ParallelBlockTableScanDescData::default());
    let pptr = std::ptr::NonNull::from(pdesc.as_mut());
    std::env::set_var("PGRUST_CBSTORE_READAHEAD", "0");
    let mut scan = CbScanDescData::new_with_part(
        scan_base(mcx, 41013, Some(pptr)),
        Some(part),
        vec![ColType::I64, ColType::Text],
    );
    std::env::remove_var("PGRUST_CBSTORE_READAHEAD");
    assert_eq!(drive(&mut scan), n_rows);
    assert_eq!(scan.rgs_readahead, 0, "PGRUST_CBSTORE_READAHEAD=0 must kill advises");
    drop(scan);
    drop(pdesc);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn advise_extents_cover_every_rg_and_column_subset() {
    let _g = ENV_LOCK.lock().unwrap();
    let (path, _n_rows) = write_part("extents");
    let part = open_part(&path);
    // Footer-metadata-only extents: every RG x column-subset combination
    // must compute in-bounds extents and issue (unix) without touching the
    // mapping's contents. Includes the last RG's last column (whose extent
    // ends at the data region's end, ahead of stitch blobs/footer).
    for rg in 0..part.rgs.len() {
        for cols in [&[0u16][..], &[1u16][..], &[0u16, 1u16][..]] {
            assert!(part.advise_willneed(rg, cols), "rg {rg} cols {cols:?}");
        }
    }
    // Out-of-range RG and empty column set: refused, no panic.
    assert!(!part.advise_willneed(part.rgs.len(), &[0, 1]));
    assert!(!part.advise_willneed(0, &[]));
    std::fs::remove_file(&path).unwrap();
}

// ---------------------------------------------------------------------------
// Claim-drive / serial readahead (cold-readahead lane): the runtime morsel
// drive's set_granule_range hook and the OPT-IN serial knob, counted in
// rgs_claim_readahead so the legacy serial guard above stays exact.
// ---------------------------------------------------------------------------

// Drive the scan claim-by-claim through set_granule_range (whole-RG claims,
// the runtime drive's whole_boundary_claims shape); returns staged rows.
fn drive_claims(scan: &mut CbScanDescData<'_>) -> usize {
    let (_total, starts) = scan.granule_geometry().unwrap();
    let mut staged = 0usize;
    for w in starts.windows(2) {
        scan.set_granule_range(w[0], w[1]).unwrap();
        loop {
            let n = scan.next_window().unwrap();
            if n == 0 {
                break;
            }
            staged += n as usize;
        }
    }
    staged
}

#[test]
fn claim_drive_advises_own_and_next_rg() {
    let _g = ENV_LOCK.lock().unwrap();
    let (path, n_rows) = write_part("claimdrive");
    let part = open_part(&path);
    let nrgs = part.rgs.len();
    assert!(nrgs >= 3, "fixture must span row groups");
    let ctx = MemoryContext::new("cbreadahead-claim");
    let mcx = ctx.mcx();
    // Defaults: readahead ON, claim depth 1, serial knob OFF.
    let mut scan = CbScanDescData::new_with_part(
        scan_base(mcx, 41014, None),
        Some(part),
        vec![ColType::I64, ColType::Text],
    );
    assert_eq!(drive_claims(&mut scan), n_rows);
    // Per RG switch: own RG + 1 ahead, out-of-range successor of the last
    // RG skipped => 2*(nrgs-1) + 1.
    assert_eq!(scan.rgs_claim_readahead, (2 * (nrgs - 1) + 1) as u64);
    assert_eq!(scan.rgs_readahead, 0, "legacy counter untouched by the claim drive");
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn claim_drive_kill_switches() {
    let _g = ENV_LOCK.lock().unwrap();
    let (path, n_rows) = write_part("claimkill");
    let part = open_part(&path);
    let ctx = MemoryContext::new("cbreadahead-claimkill");
    let mcx = ctx.mcx();
    // Global kill switch silences the claim drive too.
    std::env::set_var("PGRUST_CBSTORE_READAHEAD", "0");
    let mut scan = CbScanDescData::new_with_part(
        scan_base(mcx, 41015, None),
        Some(part.clone()),
        vec![ColType::I64, ColType::Text],
    );
    std::env::remove_var("PGRUST_CBSTORE_READAHEAD");
    assert_eq!(drive_claims(&mut scan), n_rows);
    assert_eq!(scan.rgs_claim_readahead, 0, "PGRUST_CBSTORE_READAHEAD=0 must kill claim advises");
    drop(scan);
    // Hook-scoped switch: PGRUST_CBSTORE_READAHEAD_CLAIMS=off.
    let ctx2 = MemoryContext::new("cbreadahead-claimoff");
    let mcx2 = ctx2.mcx();
    std::env::set_var("PGRUST_CBSTORE_READAHEAD_CLAIMS", "off");
    let mut scan = CbScanDescData::new_with_part(
        scan_base(mcx2, 41016, None),
        Some(part),
        vec![ColType::I64, ColType::Text],
    );
    std::env::remove_var("PGRUST_CBSTORE_READAHEAD_CLAIMS");
    assert_eq!(drive_claims(&mut scan), n_rows);
    assert_eq!(scan.rgs_claim_readahead, 0, "CLAIMS=off must disable the claim hook");
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn serial_kill_switch_covers_claim_channel() {
    let _g = ENV_LOCK.lock().unwrap();
    let (path, n_rows) = write_part("serialkill");
    let part = open_part(&path);
    let ctx = MemoryContext::new("cbreadahead-serialkill");
    let mcx = ctx.mcx();
    std::env::set_var("PGRUST_CBSTORE_READAHEAD", "0");
    let mut scan = CbScanDescData::new_with_part(
        scan_base(mcx, 41017, None),
        Some(part),
        vec![ColType::I64, ColType::Text],
    );
    std::env::remove_var("PGRUST_CBSTORE_READAHEAD");
    assert_eq!(drive(&mut scan), n_rows);
    assert_eq!(scan.rgs_claim_readahead, 0, "global kill switch silences the serial drive");
    assert_eq!(scan.rgs_readahead, 0);
    std::fs::remove_file(&path).unwrap();
}

// ---------------------------------------------------------------------------
// GL-Q4142: the morsel-range tripwire's cbstore leg.
//
// `heapam::heap_set_block_range` refuses block-range positioning on a scan
// that carries a shared parallel scan descriptor: a private range drive
// abandons the shared `phs_nallocated` cursor, so every participant would
// walk the WHOLE relation and each partial aggregate would be the global
// answer — a silent result inflated by the participant count. The cbstore
// leg of the same dispatch (`table_scan_set_morsel_range`) carried no such
// check, so the columnar side of that tripwire fail-OPENED where the heap
// side fail-CLOSES. Release-effective (an `Err`, not a `debug_assert`) —
// the check has to hold in the profile the fleet actually runs.
#[test]
fn granule_range_refuses_a_parallel_scan() {
    let _g = ENV_LOCK.lock().unwrap();
    let (path, _n_rows) = write_part("rangeparallel");
    let part = open_part(&path);
    assert!(part.rgs.len() >= 3, "fixture must span row groups");
    let ctx = MemoryContext::new("cbrange-parallel");
    let mcx = ctx.mcx();

    // Serial control: the same claim positions fine (the runtime morsel
    // drive's ordinary path — the refusal must be about the SHARED cursor,
    // not about range positioning as such).
    let mut serial = CbScanDescData::new_with_part(
        scan_base(mcx, 41030, None),
        Some(part.clone()),
        vec![ColType::I64, ColType::Text],
    );
    serial
        .set_granule_range(0, 1)
        .expect("serial granule-range positioning stays admitted");
    drop(serial);

    // Parallel: the scan rides a shared descriptor, so range positioning
    // must be refused outright.
    let mut pdesc = Box::new(::tableam_vocab::ParallelBlockTableScanDescData::default());
    let pptr = std::ptr::NonNull::from(pdesc.as_mut());
    let mut par = CbScanDescData::new_with_part(
        scan_base(mcx, 41031, Some(pptr)),
        Some(part),
        vec![ColType::I64, ColType::Text],
    );
    let err = par
        .set_granule_range(0, 1)
        .expect_err("granule-range positioning on a parallel scan must be refused");
    assert!(
        format!("{err:?}").contains("parallel"),
        "the refusal must name the parallel scan: {err:?}"
    );
    drop(par);
    drop(pdesc);
    std::fs::remove_file(&path).unwrap();
}

// GL-Q4142 granule-sum witness: the invariant the tripwire above protects.
//
// Two participants sharing ONE parallel scan descriptor claim row groups
// through `phs_nallocated`, so their staged rows SUM to the part exactly —
// every row once, no gaps, no overlaps. That is why a classic-parallel
// partial aggregate is a true partial.
//
// The broken mode is the arithmetic complement of this test: give each
// participant a PRIVATE part-global granule map instead and each one stages
// the whole part, so the sum is participants x n_rows and every partial
// aggregate is the GLOBAL answer — which the finalize then sums, returning
// count x participants. This test fails the moment the shared cursor stops
// dividing the work, whatever the cause.
#[test]
fn shared_cursor_partitions_participants_exactly_once() {
    let _g = ENV_LOCK.lock().unwrap();
    let (path, n_rows) = write_part("sharedcursor");
    let part = open_part(&path);
    let nrgs = part.rgs.len();
    assert!(nrgs >= 3, "fixture must span row groups");
    let ctx = MemoryContext::new("cbshared-cursor");
    let mcx = ctx.mcx();

    // ONE shared descriptor, three participants (leader + two helpers).
    let mut pdesc = Box::new(::tableam_vocab::ParallelBlockTableScanDescData::default());
    let pptr = std::ptr::NonNull::from(pdesc.as_mut());
    let mut scans: Vec<CbScanDescData<'_>> = [41040u32, 41041, 41042]
        .into_iter()
        .map(|oid| {
            CbScanDescData::new_with_part(
                scan_base(mcx, Oid::from(oid), Some(pptr)),
                Some(part.clone()),
                vec![ColType::I64, ColType::Text],
            )
        })
        .collect();
    // ROUND-ROBIN, one window each: a sequential drive would let the first
    // participant drain the cursor before the others ever claimed, which is a
    // property of the harness and not of the mechanism under test.
    let mut staged_each = vec![0usize; scans.len()];
    let mut live = vec![true; scans.len()];
    while live.iter().any(|&l| l) {
        for (i, scan) in scans.iter_mut().enumerate() {
            if !live[i] {
                continue;
            }
            let n = scan.next_window().unwrap();
            if n == 0 {
                live[i] = false;
            } else {
                staged_each[i] += n as usize;
            }
        }
    }
    drop(scans);
    let total: usize = staged_each.iter().sum();
    assert_eq!(
        total, n_rows,
        "participants sharing one cursor must stage the part EXACTLY once \
         (got {staged_each:?} = {total}, part is {n_rows}); a per-participant \
         total of {n_rows} each would be the (participants)x inflation"
    );
    // And the division must be real: no single participant swallowed
    // everything (that would make the sum right only by luck of a
    // zero-staging sibling, and it is the shape the private-map bug takes).
    assert!(
        staged_each.iter().all(|&n| n < n_rows),
        "no participant may stage the whole part: {staged_each:?}"
    );
    drop(pdesc);
    std::fs::remove_file(&path).unwrap();
}
