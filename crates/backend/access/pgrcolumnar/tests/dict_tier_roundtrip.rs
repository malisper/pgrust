// Dict-tier round trip over a REAL pgrcolumnar part (pgrcolumnar-v2 Stage 1.4
// acceptance): write rows through the production writer (dict-encoded text
// column), stage windows through the harvested scan drive (next_window /
// batch_deform / staged_dict_lane -> SoaDictLane publish), translate an
// int + dict-LIKE qual, evaluate through the dict-memo tier into the
// selection bitmap, and compare every staged row against a per-row oracle
// computed from the written source arrays. No executor wiring: this is the
// test-only driver the wiring tranche will replace with PgrcolumnarSource.
use std::rc::Rc;

use pgrcolumnar::scan::CbScanDescData;
use pgrcolumnar::writer::open_writer_at;
use pgrcolumnar::ColType;
use datum::Datum;
use exectuples::{SoaBatch, SOA_BM_WORDS};
use laneexec::shape::{LaneClause, LaneCmpClause, LaneCmpRhs, LaneQualShape, LaneSuffix};
use laneexec::{eval_lane_qual, translate_scan_qual};
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use types_rel::{FormData_pg_class, LockInfoData, LockRelId, Relation, RELKIND_RELATION};
use types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};

const F_INT8GT: u32 = 470;
const F_TEXTLIKE: u32 = 850;
const C_COLLATION_OID: Oid = 950;

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
    relname.namestrcpy("cbdict");
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

fn scan_base<'mcx>(mcx: Mcx<'mcx>) -> ::tableam_vocab::TableScanDescData<'mcx> {
    ::tableam_vocab::TableScanDescData {
        rs_rd: test_relation(mcx, 41007),
        rs_snapshot: None,
        rs_nkeys: 0,
        rs_key: PgVec::new_in(mcx),
        rs_mintid: Default::default(),
        rs_maxtid: Default::default(),
        rs_flags: 0,
        rs_parallel: None,
        rs_am: ::tableam_vocab::TableAm::Pgrcolumnar,
    }
}

fn bm_contains(sel: &[u64; SOA_BM_WORDS], i: usize) -> bool {
    sel[i / 64] & (1u64 << (i % 64)) != 0
}

// Stage every window of the part, deform into the SoA batch (dict columns
// publish zero-decode SoaDictLane), evaluate `q` into the bitmap, and check
// row-for-row against `oracle(global_row)`. Returns (rows staged, rows
// selected, windows with a dict lane up).
fn drive_scan(
    scan: &mut CbScanDescData<'_>,
    soa: &mut SoaBatch<'_>,
    lq: &mut laneexec::LaneQualProg,
    ncols: usize,
    oracle: &dyn Fn(usize) -> bool,
) -> (usize, usize, usize) {
    let (mut staged, mut selected, mut dict_windows) = (0usize, 0usize, 0usize);
    loop {
        let n = scan.next_window().unwrap();
        if n == 0 {
            break;
        }
        let (rg, base) = scan.window_ref().expect("window staged");
        assert_eq!(rg, 0, "single-RG fixture");
        scan.batch_deform(ncols, soa, None, None);
        if let Some(lane) = soa.dict_lane(1) {
            dict_windows += 1;
            // v7 stitch rides the lane: a fresh all-dict part always
            // publishes one, and over a single-RG part the byte-rank global
            // codes are exactly the (byte-sorted) local codes.
            let t = lane.table;
            assert!(t.has_stitch(), "fresh all-dict part must publish a stitch");
            assert!(t.gepoch != 0 && t.gndv == t.ndict);
            for c in 0..t.ndict {
                assert_eq!(t.global_code(c), c, "single-RG stitch is the identity");
            }
        }
        let mut sel = [0u64; SOA_BM_WORDS];
        eval_lane_qual(lq, soa, n, &mut sel).unwrap();
        for i in 0..n as usize {
            let want = oracle(base as usize + i);
            assert_eq!(bm_contains(&sel, i), want, "rg-row {}", base as usize + i);
            selected += want as usize;
        }
        staged += n as usize;
    }
    (staged, selected, dict_windows)
}

#[test]
fn dict_tier_roundtrip_over_real_part() {
    let path = std::env::temp_dir()
        .join(format!("cbdicttier-{}.cb", std::process::id()))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&path);
    std::fs::write(&path, []).unwrap();

    // Source data: 20k rows (3 granules), 8-string vocabulary (duplicates
    // force the dict encoding), ints cycling through a small range.
    let vocab: [&[u8]; 8] =
        [b"alpha", b"drab", b"beta", b"crab", b"delta", b"ab", b"zz", b"gamma"];
    let n_rows = 20_000usize;
    let ints: Vec<i64> = (0..n_rows as i64).map(|i| (i * 7) % 100).collect();
    let codes: Vec<usize> = (0..n_rows).map(|i| (i * 11 + i / 13) % vocab.len()).collect();

    let coltypes = vec![ColType::I64, ColType::Text];
    let mut w = open_writer_at(&path, coltypes.clone()).unwrap();
    let mut keep = Vec::new();
    for i in 0..n_rows {
        let vals = [Datum::from_i64(ints[i]), text_datum(vocab[codes[i]], &mut keep)];
        w.append_row(&vals, &[false, false]).unwrap();
        if keep.len() > 512 {
            // append_row copies the payload; the images need not outlive it.
            keep.clear();
        }
    }
    w.finish().unwrap();

    let part = std::sync::Arc::new(pgrcolumnar::reader::Part::open(&path, 2).unwrap().expect("part exists"));
    assert_eq!(part.total_rows(), n_rows as u64);

    let ctx = MemoryContext::new("cbdicttier");
    let mcx = ctx.mcx();

    // v0 > 42 AND t LIKE '%ab%': int clause + dict clause, both in the
    // vectorizable prefix (no requal tail).
    let qual = LaneQualShape {
        clauses: vec![
            LaneClause::Cmp(LaneCmpClause {
                col: 0,
                fn_oid: F_INT8GT,
                commuted: false,
                collation: 0,
                rhs: LaneCmpRhs::Const(Datum::from_i64(42)),
            }),
            LaneClause::Cmp(LaneCmpClause {
                col: 1,
                fn_oid: F_TEXTLIKE,
                commuted: false,
                collation: C_COLLATION_OID,
                rhs: LaneCmpRhs::Const(text_datum(b"%ab%", &mut keep)),
            }),
        ],
        max_attnum: 1,
        suffix: LaneSuffix::None,
    };
    let mut lq = translate_scan_qual(&qual, true).expect("dict qual translates");
    assert_eq!(lq.ndict(), 1);
    assert!(!lq.requal);
    // The staged drive would fold this zone src per granule; assert the
    // translate side of the contract here.
    let zone_srcs: Vec<_> = (0..lq.nstaged()).filter_map(|k| lq.staged_zone_src(k)).collect();
    assert_eq!(zone_srcs.len(), 1);
    assert_eq!(zone_srcs[0].col, 0);

    let mut scan = CbScanDescData::new_with_part(scan_base(mcx), Some(part), coltypes);
    let mut soa = SoaBatch::new_in(mcx, 2);
    for c in lq.dict_cols() {
        soa.set_dict_want(c);
    }

    let matches_ab = |s: &[u8]| s.windows(2).any(|w| w == b"ab");
    let oracle = |row: usize| ints[row] > 42 && matches_ab(vocab[codes[row]]);
    let (staged, selected, dict_windows) =
        drive_scan(&mut scan, &mut soa, &mut lq, 2, &oracle);
    assert_eq!(staged, n_rows, "no zone quals: every row stages");
    let want_total = (0..n_rows).filter(|&i| oracle(i)).count();
    assert_eq!(selected, want_total);
    assert!(want_total > 0 && want_total < n_rows, "fixture must discriminate");
    // The text chunk dict-encoded and the zero-decode SoaDictLane publish
    // engaged on every window (the whole point of the tier).
    assert!(dict_windows > 0, "text column must dict-encode + publish lanes");

    // Rescan (epoch stability: memo keyed on rg index survives; results
    // must be identical).
    scan.reset_position();
    let (staged2, selected2, _) = drive_scan(&mut scan, &mut soa, &mut lq, 2, &oracle);
    assert_eq!((staged2, selected2), (staged, selected));

    // Same part, dict_want NOT armed: batch_deform gathers dict[code] into
    // Raw cells and the dict tier's per-row fallback must agree.
    let mut lq2 = translate_scan_qual(&qual, true).unwrap();
    let mut scan2 = CbScanDescData::new_with_part(
        scan_base(mcx),
        Some(std::sync::Arc::new(pgrcolumnar::reader::Part::open(&path, 2).unwrap().unwrap())),
        vec![ColType::I64, ColType::Text],
    );
    let mut soa2 = SoaBatch::new_in(mcx, 2);
    let (staged3, selected3, dict_windows3) =
        drive_scan(&mut scan2, &mut soa2, &mut lq2, 2, &oracle);
    assert_eq!((staged3, selected3), (staged, selected));
    assert_eq!(dict_windows3, 0, "no dict_want: the fill gathers to Raw");

    std::fs::remove_file(&path).unwrap();
}
