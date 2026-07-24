// Translate + dict-tier tests over hand-built qual SHAPES (the ExprState
// walker producing LaneQualShape lands with the executor-wiring tranche;
// until then shapes are constructed directly — the whitelist, the hybrid
// gate, the staged ordering and the dict tier are all downstream of the
// shape and fully exercised here). The evaluation oracles are scalar
// per-row re-implementations over the same source arrays.
use datum::{Datum, NullableDatum};
use exectuples::{SoaBatch, SoaDictLane, SoaDictTable, SOA_BM_WORDS};
use laneexec::interp::CmpOp as LaneCmp;
use laneexec::shape::{LaneClause, LaneCmpClause, LaneCmpRhs, LaneQualShape, LaneSuffix};
use laneexec::{eval_lane_qual, lane_cmp_for_fn_oid, translate_scan_qual};
use mcx::MemoryContext;
use types_core::catalog::C_COLLATION_OID;

const F_INT4EQ: u32 = 65;
const F_INT4GT: u32 = 147;
const F_INT4LT: u32 = 66;
const F_INT8LT: u32 = 469;
const F_INT48GT: u32 = 855;
const F_INT84GT: u32 = 477;
const F_INT42GT: u32 = 163;
const F_INT24LT: u32 = 160;
const F_TEXTLIKE: u32 = 850;
const F_TEXTNLIKE: u32 = 851;
const F_TEXTEQ: u32 = 67;
const F_TEXTNE: u32 = 157;
const F_TEXTICLIKE: u32 = 1633;

fn cmp_clause(col: u16, fn_oid: u32, commuted: bool, rhs: LaneCmpRhs) -> LaneClause {
    LaneClause::Cmp(LaneCmpClause { col, fn_oid, commuted, collation: 0, rhs })
}

fn text_clause(col: u16, fn_oid: u32, commuted: bool, pat: &[u8]) -> LaneClause {
    LaneClause::Cmp(LaneCmpClause {
        col,
        fn_oid,
        commuted,
        collation: C_COLLATION_OID,
        rhs: LaneCmpRhs::Const(text_datum(pat)),
    })
}

fn shape(clauses: Vec<LaneClause>, suffix: LaneSuffix) -> LaneQualShape {
    let max_attnum = clauses
        .iter()
        .map(|c| match c {
            LaneClause::Cmp(c) => match c.rhs {
                LaneCmpRhs::Col(o) => c.col.max(o),
                LaneCmpRhs::Const(_) => c.col,
            },
            LaneClause::NullTest { col, .. }
            | LaneClause::BoolVar { col }
            | LaneClause::BoolTest { col, .. }
            | LaneClause::InList { col, .. } => *col,
        })
        .max()
        .unwrap_or(0);
    LaneQualShape { clauses, max_attnum, suffix }
}

// Leaked inline 4B-U text image (test fixture lifetime).
fn text_datum(bytes: &[u8]) -> Datum {
    let total = 4 + bytes.len();
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&datum::varlena::set_varsize_4b(total));
    img.extend_from_slice(bytes);
    let d = Datum::from_usize(img.as_ptr() as usize);
    std::mem::forget(img);
    d
}

fn bm_contains(sel: &[u64; SOA_BM_WORDS], i: usize) -> bool {
    sel[i / 64] & (1u64 << (i % 64)) != 0
}

// ---------------------------------------------------------------- whitelist

#[test]
fn comparator_whitelist_families() {
    use LaneCmp::*;
    // (oid, uncommuted, commuted): the commuted map mirrors the relation AND
    // crosses width families.
    let cases: &[(u32, LaneCmp, LaneCmp)] = &[
        (65, Int4Eq, Int4Eq),
        (66, Int4Lt, Int4Gt),
        (147, Int4Gt, Int4Lt),
        (467, Int8Eq, Int8Eq),
        (471, Int8Le, Int8Ge),
        (64, Int2Lt, Int2Gt),
        (477, Int84Gt, Int48Lt),
        (855, Int48Gt, Int84Lt),
        (163, Int42Gt, Int24Lt),
        (160, Int24Lt, Int42Gt),
        // date = int4 compare; timestamp/timestamptz = int8 compare.
        (1087, Int4Lt, Int4Gt),
        (2054, Int8Lt, Int8Gt),
        (1154, Int8Lt, Int8Gt),
        // oid unsigned family.
        (716, OidLt, OidGt),
        (184, OidEq, OidEq),
        // floats incl. cross-width mirrors.
        (289, Float4Lt, Float4Gt),
        (297, Float8Gt, Float8Lt),
        (303, Float48Gt, Float84Lt),
        (307, Float84Lt, Float48Gt),
    ];
    for &(oid, plain, commuted) in cases {
        assert_eq!(lane_cmp_for_fn_oid(oid, false), Some(plain), "oid {oid}");
        assert_eq!(lane_cmp_for_fn_oid(oid, true), Some(commuted), "oid {oid} commuted");
    }
    // Not whitelisted: textlike, int4pl, unknown.
    assert_eq!(lane_cmp_for_fn_oid(850, false), None);
    assert_eq!(lane_cmp_for_fn_oid(177, false), None);
    assert_eq!(lane_cmp_for_fn_oid(999_999, false), None);
}

// Census conformance (ne-admission audit): the translate whitelist must admit
// every comparator the central lanereg registry censuses (both operand
// orders), and every Ne family in particular — a census/whitelist drift here
// is exactly the class of gap that parks a `col <> const` scan qual on the
// per-row drive. Direction matters: translate ⊇ census (translate may carry
// extra type-alias oids only when they are censused too — the date/timestamp
// aliases joined the census with this branch).
#[test]
fn translate_whitelist_covers_lanereg_census() {
    let mut censused = 0u32;
    for oid in 0u32..=3000 {
        if let Some(shape) = lanereg::aot_qual_cmp(oid) {
            censused += 1;
            assert!(
                lane_cmp_for_fn_oid(oid, false).is_some(),
                "censused comparator oid {oid} ({:?}) refused by translate",
                shape
            );
            assert!(
                lane_cmp_for_fn_oid(oid, true).is_some(),
                "censused comparator oid {oid} ({:?}) refused commuted",
                shape
            );
            // The whitelist's predicate must agree with the census shape for
            // the Ne rows (the audited class).
            if shape.pred == lanereg::CmpPred::Ne {
                let op = lane_cmp_for_fn_oid(oid, false).unwrap();
                assert!(
                    format!("{op:?}").ends_with("Ne"),
                    "oid {oid}: census says Ne, translate maps to {op:?}"
                );
            }
        }
    }
    assert_eq!(censused, 90, "census size drifted; re-audit the whitelist");
}

// Ne admission end-to-end at the translate layer: `a <> 5 AND b8 <> 0`
// engages without a requal tail, Ne clauses cost-class as range (2), fold a
// zone src (evaluated, never wrongly pruned — the driver decides
// eligibility), and evaluate with strict-NULL parity against the oracle.
#[test]
fn ne_translates_and_evals_with_nulls() {
    const F_INT4NE: u32 = 144;
    const F_INT8NE: u32 = 468;
    let ctx = MemoryContext::new("lane-ne-eval");
    let mcx = ctx.mcx();
    let s = shape(
        vec![
            cmp_clause(0, F_INT4NE, false, LaneCmpRhs::Const(Datum::from_i32(5))),
            cmp_clause(1, F_INT8NE, false, LaneCmpRhs::Const(Datum::from_i64(0))),
        ],
        LaneSuffix::None,
    );
    let mut lq = translate_scan_qual(&s, false).expect("ne qual engages");
    assert!(!lq.requal);
    assert_eq!(lq.nclauses, 2);
    assert_eq!(lq.nstaged(), 2);
    for k in 0..2 {
        let zs = lq.staged_zone_src(k).expect("single-col ne folds a zone src");
        assert!(zs.fn_oid == F_INT4NE || zs.fn_oid == F_INT8NE);
    }
    let mut soa = SoaBatch::new_in(mcx, 2);
    let n = 64usize;
    soa.begin(n as u32);
    let mut rows = Vec::new();
    for i in 0..n {
        let a = (i as i32 % 9) - 2; // hits 5 periodically
        let a_null = i % 7 == 0;
        let b = (i as i64 % 4) - 1; // hits 0 periodically
        let b_null = i % 11 == 0;
        soa.col_values_mut(0)[i] = Datum::from_i32(a);
        soa.col_isnull_mut(0)[i] = a_null;
        soa.col_values_mut(1)[i] = Datum::from_i64(b);
        soa.col_isnull_mut(1)[i] = b_null;
        rows.push((a, a_null, b, b_null));
    }
    let mut sel = [0u64; SOA_BM_WORDS];
    eval_lane_qual(&mut lq, &soa, n as u32, &mut sel).unwrap();
    for (i, &(a, a_null, b, b_null)) in rows.iter().enumerate() {
        // Strict comparators: a NULL operand yields NULL, which fails the
        // qual exactly like false.
        let want = !a_null && a != 5 && !b_null && b != 0;
        assert_eq!(bm_contains(&sel, i), want, "row {i}: {rows:?}");
    }
}

// ------------------------------------------------------------ translation

#[test]
fn leading_unknown_oid_refuses() {
    let s = shape(
        vec![cmp_clause(0, 177, false, LaneCmpRhs::Const(Datum::from_i32(1)))],
        LaneSuffix::None,
    );
    assert!(translate_scan_qual(&s, false).is_err());
}

#[test]
fn hybrid_gate_one_clause_opaque_tail_refuses() {
    // 1-clause prefix + opaque (assumed-volatile) tail: below the gate.
    let s = shape(
        vec![cmp_clause(0, F_INT4GT, false, LaneCmpRhs::Const(Datum::from_i32(5)))],
        LaneSuffix::Opaque,
    );
    assert!(translate_scan_qual(&s, false).is_err());
    // Calls tail without a pg_proc seam: unknown volatility = volatile.
    let s = shape(
        vec![cmp_clause(0, F_INT4GT, false, LaneCmpRhs::Const(Datum::from_i32(5)))],
        LaneSuffix::Calls(vec![177]),
    );
    assert!(translate_scan_qual(&s, false).is_err());
}

#[test]
fn hybrid_gate_two_clause_prefix_engages_with_requal() {
    let s = shape(
        vec![
            cmp_clause(0, F_INT4GT, false, LaneCmpRhs::Const(Datum::from_i32(5))),
            cmp_clause(1, F_INT4LT, false, LaneCmpRhs::Const(Datum::from_i32(3))),
        ],
        LaneSuffix::Opaque,
    );
    let lq = translate_scan_qual(&s, false).expect("2-clause prefix engages");
    assert!(lq.requal);
    assert_eq!(lq.nclauses, 2);
}

#[test]
fn refused_mid_clause_splits_prefix() {
    // Clause 2's oid (textlike) is refused with dict_lanes=false: clauses
    // 0..2 vectorize, the rest requals.
    let s = shape(
        vec![
            cmp_clause(0, F_INT4GT, false, LaneCmpRhs::Const(Datum::from_i32(5))),
            cmp_clause(1, F_INT4LT, false, LaneCmpRhs::Const(Datum::from_i32(3))),
            text_clause(2, F_TEXTLIKE, false, b"%x%"),
        ],
        LaneSuffix::None,
    );
    let lq = translate_scan_qual(&s, false).expect("int prefix engages");
    assert!(lq.requal);
    assert_eq!(lq.nclauses, 2);
    assert_eq!(lq.ndict(), 0);
}

#[test]
fn no_suffix_no_requal() {
    let s = shape(
        vec![
            cmp_clause(0, F_INT4GT, false, LaneCmpRhs::Const(Datum::from_i32(5))),
            LaneClause::NullTest { col: 1, want_null: false },
        ],
        LaneSuffix::None,
    );
    let lq = translate_scan_qual(&s, false).expect("must translate");
    assert!(!lq.requal);
    assert_eq!(lq.nclauses, 2);
    assert_eq!(lq.max_attnum, 1);
}

// ------------------------------------------------------------- evaluation

// a > 5 AND b8 < a (int48 commuted spelled var-var) AND a IN (2,7,9)
// AND c IS NOT NULL AND d (bool var): full vocabulary parity vs a scalar
// oracle over 300 pseudo-random rows in two windows.
#[test]
fn eval_parity_full_vocabulary() {
    let ctx = MemoryContext::new("lane-eval-parity");
    let mcx = ctx.mcx();
    let elems: Vec<NullableDatum> = [2i32, 7, 9]
        .iter()
        .map(|&v| NullableDatum { value: Datum::from_i32(v), isnull: false })
        .collect();
    let s = shape(
        vec![
            cmp_clause(0, F_INT4GT, false, LaneCmpRhs::Const(Datum::from_i32(5))),
            // b8 < a spelled as int48gt commuted? Use var-var: a int48gt b8.
            cmp_clause(0, F_INT48GT, false, LaneCmpRhs::Col(1)),
            LaneClause::InList { col: 0, fn_oid: F_INT4EQ, elems },
            LaneClause::NullTest { col: 2, want_null: false },
            LaneClause::BoolVar { col: 3 },
        ],
        LaneSuffix::None,
    );
    let mut lq = translate_scan_qual(&s, false).expect("must translate");
    assert_eq!(lq.nclauses, 5);
    assert_eq!(lq.nstaged(), 5);

    let mut soa = SoaBatch::new_in(mcx, 4);
    let mut state = 0x9e37_79b9_u64;
    let mut rnd = move || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (state >> 33) as i64
    };
    for win in 0..2 {
        let n = if win == 0 { 256 } else { 44 };
        soa.begin(n as u32);
        let mut rows = Vec::new();
        for i in 0..n {
            let a = (rnd() % 12) as i32;
            let b8 = rnd() % 12;
            let c_null = rnd() % 3 == 0;
            let d_null = rnd() % 4 == 0;
            let d = rnd() % 2 == 0;
            soa.col_values_mut(0)[i] = Datum::from_i32(a);
            soa.col_isnull_mut(0)[i] = false;
            soa.col_values_mut(1)[i] = Datum::from_i64(b8);
            soa.col_isnull_mut(1)[i] = false;
            soa.col_values_mut(2)[i] = Datum::from_i32(0);
            soa.col_isnull_mut(2)[i] = c_null;
            soa.col_values_mut(3)[i] = Datum::from_bool(d);
            soa.col_isnull_mut(3)[i] = d_null;
            rows.push((a, b8, c_null, d_null, d));
        }
        let mut sel = [0u64; SOA_BM_WORDS];
        eval_lane_qual(&mut lq, &soa, n as u32, &mut sel).unwrap();
        for (i, &(a, b8, c_null, d_null, d)) in rows.iter().enumerate() {
            let want = a > 5
                && (a as i64) > b8
                && [2, 7, 9].contains(&a)
                && !c_null
                && (!d_null && d);
            assert_eq!(bm_contains(&sel, i), want, "win {win} row {i}: {rows:?}");
        }
    }
}

// -------------------------------------------------------------- staged/PREWHERE

#[test]
fn staged_order_and_zone_srcs() {
    // Written order: LIKE (dict, class 5), a < 3 (class 2), b = 7 (class 1),
    // c IS NULL (class 0), a IN (1,2) (class 3). Static cost order must be
    // null < eq < range < in-list < dict LIKE, ties by original position.
    let elems: Vec<NullableDatum> = [1i32, 2]
        .iter()
        .map(|&v| NullableDatum { value: Datum::from_i32(v), isnull: false })
        .collect();
    let s = shape(
        vec![
            text_clause(4, F_TEXTLIKE, false, b"%ab%"),
            cmp_clause(0, F_INT4LT, false, LaneCmpRhs::Const(Datum::from_i32(3))),
            cmp_clause(1, F_INT4EQ, false, LaneCmpRhs::Const(Datum::from_i32(7))),
            LaneClause::NullTest { col: 2, want_null: true },
            LaneClause::InList { col: 0, fn_oid: F_INT4EQ, elems },
        ],
        LaneSuffix::None,
    );
    let lq = translate_scan_qual(&s, true).expect("dict qual engages");
    assert_eq!(lq.ndict(), 1);
    assert_eq!(lq.nstaged(), 5);
    let staged_first_cols: Vec<u16> =
        (0..lq.nstaged()).map(|k| lq.staged_cols(k)[0]).collect();
    // null(c=2), eq(c=1), lt(c=0), inlist(c=0), dict like(c=4).
    assert_eq!(staged_first_cols, vec![2, 1, 0, 0, 4]);
    // Zone srcs: only the single-column Var CMP Const int clauses fold.
    assert!(lq.staged_zone_src(0).is_none(), "null test never folds");
    let eq_src = lq.staged_zone_src(1).expect("eq clause folds");
    assert_eq!((eq_src.col, eq_src.fn_oid, eq_src.commuted), (1, F_INT4EQ, false));
    assert_eq!(eq_src.konst.as_i32(), 7);
    let lt_src = lq.staged_zone_src(2).expect("lt clause folds");
    assert_eq!((lt_src.col, lt_src.fn_oid), (0, F_INT4LT));
    assert!(lq.staged_zone_src(3).is_none(), "IN-list never folds");
    assert!(lq.staged_zone_src(4).is_none(), "dict clause never folds");
    // read_cols covers every prefix lane incl. the dict column.
    let mut cols: Vec<u16> = lq.read_cols().collect();
    cols.sort_unstable();
    cols.dedup();
    assert_eq!(cols, vec![0, 1, 2, 4]);
    // dict_cols drives set_dict_want arming.
    assert_eq!(lq.dict_cols().collect::<Vec<_>>(), vec![4]);
}

#[test]
fn order_staged_selectivity_reorders_equal_classes() {
    let s = shape(
        vec![
            cmp_clause(0, F_INT4EQ, false, LaneCmpRhs::Const(Datum::from_i32(1))),
            cmp_clause(1, F_INT4EQ, false, LaneCmpRhs::Const(Datum::from_i32(2))),
        ],
        LaneSuffix::None,
    );
    let mut lq = translate_scan_qual(&s, false).unwrap();
    assert_eq!(lq.staged_cols(0)[0], 0, "static order = original position");
    // Column 1's eq is far more selective: it must move first.
    lq.order_staged(&|col, class| {
        assert_eq!(class, 1);
        Some(if col == 1 { 0.001 } else { 0.9 })
    });
    assert_eq!(lq.staged_cols(0)[0], 1);
    assert_eq!(lq.staged_cols(1)[0], 0);
}

#[test]
fn eval_staged_conjunction_matches_eval_lane_qual() {
    let ctx = MemoryContext::new("lane-staged-parity");
    let mcx = ctx.mcx();
    let s = shape(
        vec![
            cmp_clause(0, F_INT4GT, false, LaneCmpRhs::Const(Datum::from_i32(2))),
            cmp_clause(1, F_INT8LT, false, LaneCmpRhs::Const(Datum::from_i64(50))),
            LaneClause::NullTest { col: 2, want_null: false },
        ],
        LaneSuffix::None,
    );
    let mut whole = translate_scan_qual(&s, false).unwrap();
    let mut staged = translate_scan_qual(&s, false).unwrap();
    let n = 200usize;
    let mut soa = SoaBatch::new_in(mcx, 3);
    soa.begin(n as u32);
    for i in 0..n {
        soa.col_values_mut(0)[i] = Datum::from_i32((i % 7) as i32);
        soa.col_isnull_mut(0)[i] = false;
        soa.col_values_mut(1)[i] = Datum::from_i64((i as i64 * 13) % 101);
        soa.col_isnull_mut(1)[i] = false;
        soa.col_values_mut(2)[i] = Datum::from_i32(0);
        soa.col_isnull_mut(2)[i] = i % 5 == 0;
    }
    let mut want = [0u64; SOA_BM_WORDS];
    eval_lane_qual(&mut whole, &soa, n as u32, &mut want).unwrap();
    // Staged drive: all-ones live mask, clauses in staged order.
    let mut got = [0u64; SOA_BM_WORDS];
    let full = n / 64;
    got[..full].fill(!0u64);
    if n % 64 != 0 {
        got[full] = (1u64 << (n % 64)) - 1;
    }
    for k in 0..staged.nstaged() {
        staged.eval_staged(k, &soa, n as u32, &mut got).unwrap();
    }
    assert_eq!(&want[..], &got[..]);
}

// ------------------------------------------------------------------ dict tier

struct DictFixture {
    _images: Vec<Vec<u8>>,
    datums: Vec<Datum>,
    codes: Vec<u32>,
}

impl DictFixture {
    fn new(entries: &[&[u8]], codes: &[u32]) -> Self {
        let images: Vec<Vec<u8>> = entries
            .iter()
            .map(|e| {
                let mut v = Vec::with_capacity(4 + e.len());
                v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + e.len()));
                v.extend_from_slice(e);
                v
            })
            .collect();
        let datums = images.iter().map(|i| Datum::from_usize(i.as_ptr() as usize)).collect();
        DictFixture { _images: images, datums, codes: codes.to_vec() }
    }

    fn lane(&self, epoch: u64, sorted: bool) -> SoaDictLane {
        SoaDictLane {
            codes: self.codes.as_ptr(),
            table: SoaDictTable {
                dict: self.datums.as_ptr(),
                ndict: self.datums.len() as u32,
                epoch,
                sorted,
                stitch: std::ptr::null(),
                gndv: 0,
                gepoch: 0,
                lazy: core::ptr::null(),
                lazy_ensure: None,
                lazy_ensure_all: None,
                // Separate per-image Vec allocations: NO whole-span
                // readability witness (F-R1-1).
                contig: false,
            },
        }
    }
}

#[test]
fn dict_like_joins_prefix_and_memo_parity() {
    let ctx = MemoryContext::new("lane-dict-parity");
    let mcx = ctx.mcx();
    // a > 0 AND t LIKE '%ab%' — the dict clause joins the prefix (no requal).
    let s = shape(
        vec![
            cmp_clause(0, F_INT4GT, false, LaneCmpRhs::Const(Datum::from_i32(0))),
            text_clause(5, F_TEXTLIKE, false, b"%ab%"),
        ],
        LaneSuffix::None,
    );
    let mut lq = translate_scan_qual(&s, true).expect("dict clause must join");
    assert_eq!(lq.ndict(), 1);
    assert_eq!(lq.nclauses, 2);
    assert!(!lq.requal);

    let strings: [&[u8]; 4] = [b"xaby", b"noop", b"ab", b"cab"];
    let n = 200usize;
    let codes: Vec<u32> = (0..n).map(|i| (i % strings.len()) as u32).collect();
    let fx = DictFixture::new(&strings, &codes);
    let mut soa = SoaBatch::new_in(mcx, 6);
    soa.set_dict_want(5);
    soa.begin(n as u32);
    for i in 0..n {
        soa.col_values_mut(0)[i] = Datum::from_i32((i as i32 % 7) - 3);
        soa.col_isnull_mut(0)[i] = false;
    }
    soa.set_dict_lane(5, fx.lane(3, false));
    let mut sel = [0u64; SOA_BM_WORDS];
    eval_lane_qual(&mut lq, &soa, n as u32, &mut sel).unwrap();
    let contains_ab = [true, false, true, true];
    for i in 0..n {
        let want = ((i as i32 % 7) - 3) > 0 && contains_ab[i % 4];
        assert_eq!(bm_contains(&sel, i), want, "row {i}");
    }

    // Second window, same epoch: memo reused; then a NEW epoch with a
    // different dictionary — the memo must invalidate.
    let strings2: [&[u8]; 2] = [b"ab", b"zz"];
    let codes2: Vec<u32> = (0..n).map(|i| (i % 2) as u32).collect();
    let fx2 = DictFixture::new(&strings2, &codes2);
    soa.begin(n as u32);
    for i in 0..n {
        soa.col_values_mut(0)[i] = Datum::from_i32(1);
        soa.col_isnull_mut(0)[i] = false;
    }
    soa.set_dict_lane(5, fx2.lane(4, false));
    let mut sel2 = [0u64; SOA_BM_WORDS];
    eval_lane_qual(&mut lq, &soa, n as u32, &mut sel2).unwrap();
    for i in 0..n {
        assert_eq!(bm_contains(&sel2, i), i % 2 == 0, "epoch-2 row {i}");
    }
}

#[test]
fn dict_sorted_prefix_uses_code_range() {
    let ctx = MemoryContext::new("lane-dict-range");
    let mcx = ctx.mcx();
    let s = shape(vec![text_clause(0, F_TEXTLIKE, false, b"ab%")], LaneSuffix::None);
    let mut lq = translate_scan_qual(&s, true).expect("leading dict clause");
    // Byte-sorted dictionary; matching prefix run is [ab, abz].
    let strings: [&[u8]; 5] = [b"aa", b"ab", b"abz", b"ac", b"b"];
    let codes: Vec<u32> = vec![0, 1, 2, 3, 4, 1, 2, 0];
    let fx = DictFixture::new(&strings, &codes);
    let n = codes.len();
    let mut soa = SoaBatch::new_in(mcx, 1);
    soa.set_dict_want(0);
    soa.begin(n as u32);
    soa.set_dict_lane(0, fx.lane(9, true));
    let mut sel = [0u64; SOA_BM_WORDS];
    eval_lane_qual(&mut lq, &soa, n as u32, &mut sel).unwrap();
    let want = [false, true, true, false, false, true, true, false];
    for (i, &w) in want.iter().enumerate() {
        assert_eq!(bm_contains(&sel, i), w, "row {i}");
    }
}

#[test]
fn dict_raw_window_fallback_and_negations() {
    let ctx = MemoryContext::new("lane-dict-raw");
    let mcx = ctx.mcx();
    // NOT LIKE + texteq + textne over a RAW (non-dict) window: the per-row
    // fallback path over filled Datum cells, NULLs fail every clause.
    let s = shape(
        vec![
            text_clause(0, F_TEXTNLIKE, false, b"%b%"),
            text_clause(0, F_TEXTEQ, false, b"aa"),
            text_clause(0, F_TEXTNE, true, b"zz"),
        ],
        LaneSuffix::None,
    );
    let mut lq = translate_scan_qual(&s, true).expect("all three join the dict tier");
    assert_eq!(lq.ndict(), 3);
    let rows: [(&[u8], bool); 4] = [(b"aa", false), (b"ab", false), (b"aa", true), (b"zz", false)];
    let mut soa = SoaBatch::new_in(mcx, 1);
    soa.set_dict_want(0);
    soa.begin(rows.len() as u32);
    let mut keep = Vec::new();
    for (i, (sbytes, isnull)) in rows.iter().enumerate() {
        let mut v = Vec::with_capacity(4 + sbytes.len());
        v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + sbytes.len()));
        v.extend_from_slice(sbytes);
        soa.col_values_mut(0)[i] = Datum::from_usize(v.as_ptr() as usize);
        soa.col_isnull_mut(0)[i] = *isnull;
        keep.push(v);
    }
    // No set_dict_lane call: the window is Raw.
    let mut sel = [0u64; SOA_BM_WORDS];
    eval_lane_qual(&mut lq, &soa, rows.len() as u32, &mut sel).unwrap();
    // aa: no 'b', == aa, != zz -> pass; ab: contains b -> fail;
    // NULL -> fail; zz: != aa -> fail.
    let want = [true, false, false, false];
    for (i, &w) in want.iter().enumerate() {
        assert_eq!(bm_contains(&sel, i), w, "row {i}");
    }
}

#[test]
fn dict_refusals_fail_closed() {
    // Commuted LIKE (column is the pattern): leading clause -> hard refusal.
    let s = shape(vec![text_clause(0, F_TEXTLIKE, true, b"abc")], LaneSuffix::None);
    assert!(translate_scan_qual(&s, true).is_err());

    // Pattern ending in an escape: production errors per row; refuse.
    let s = shape(vec![text_clause(0, F_TEXTLIKE, false, b"%ab\\")], LaneSuffix::None);
    assert!(translate_scan_qual(&s, true).is_err());

    // Invalid collation (0): generic_match_text errors; refuse.
    let cl = LaneClause::Cmp(LaneCmpClause {
        col: 0,
        fn_oid: F_TEXTLIKE,
        commuted: false,
        collation: 0,
        rhs: LaneCmpRhs::Const(text_datum(b"%ab%")),
    });
    let s = shape(vec![cl], LaneSuffix::None);
    assert!(translate_scan_qual(&s, true).is_err());

    // ILIKE under the C collation single-byte arm... requires encoding
    // probes; a non-constant pattern rhs (Var) always refuses.
    let cl = LaneClause::Cmp(LaneCmpClause {
        col: 0,
        fn_oid: F_TEXTICLIKE,
        commuted: false,
        collation: C_COLLATION_OID,
        rhs: LaneCmpRhs::Col(1),
    });
    let s = shape(vec![cl], LaneSuffix::None);
    assert!(translate_scan_qual(&s, true).is_err());

    // Heap scans (dict_lanes=false) never admit text clauses.
    let s = shape(vec![text_clause(0, F_TEXTLIKE, false, b"%ab%")], LaneSuffix::None);
    assert!(translate_scan_qual(&s, false).is_err());
}

#[test]
fn underscore_pattern_admits_without_kernel() {
    let ctx = MemoryContext::new("lane-dict-underscore");
    let mcx = ctx.mcx();
    // 'a_c' classifies no byte kernel but stays dict-memoizable through the
    // production match_text per distinct code.
    let s = shape(vec![text_clause(0, F_TEXTLIKE, false, b"a_c")], LaneSuffix::None);
    let mut lq = translate_scan_qual(&s, true).expect("scalar-predicate dict clause");
    let strings: [&[u8]; 3] = [b"abc", b"ac", b"axc"];
    let codes = vec![0u32, 1, 2, 0];
    let fx = DictFixture::new(&strings, &codes);
    let mut soa = SoaBatch::new_in(mcx, 1);
    soa.set_dict_want(0);
    soa.begin(codes.len() as u32);
    soa.set_dict_lane(0, fx.lane(1, false));
    let mut sel = [0u64; SOA_BM_WORDS];
    eval_lane_qual(&mut lq, &soa, codes.len() as u32, &mut sel).unwrap();
    let want = [true, false, true, true];
    for (i, &w) in want.iter().enumerate() {
        assert_eq!(bm_contains(&sel, i), w, "row {i}");
    }
}

// ------------------------------------------------------------- inline const

#[test]
fn inline_const_probe() {
    // Inline images answer the walker-side Const admission probe.
    let d = text_datum(b"xyz");
    assert!(laneexec::inline_const_ok(d));
}

// ===========================================================================
// Condition-cache fingerprints (translate::fingerprint_prefix through
// LaneQualProg.fingerprint): deterministic per identical prefix, sensitive
// to every keyed component (column, operator, commutation, collation,
// constant VALUE), and insensitive to the requal tail (cached bits are
// prefix verdicts only).
// ===========================================================================

#[test]
fn condcache_fingerprint_deterministic_and_value_sensitive() {
    let fp = |clauses: Vec<LaneClause>| {
        translate_scan_qual(&shape(clauses, LaneSuffix::None), true)
            .expect("translates")
            .fingerprint
            .expect("cacheable prefix")
    };
    let base = || {
        vec![
            cmp_clause(0, F_INT4EQ, false, LaneCmpRhs::Const(Datum::from_i32(100))),
            text_clause(2, F_TEXTLIKE, false, b"%google%"),
        ]
    };
    // Deterministic across independent translations.
    assert_eq!(fp(base()), fp(base()));
    // Every keyed component moves the fingerprint.
    let variants = [
        vec![
            cmp_clause(1, F_INT4EQ, false, LaneCmpRhs::Const(Datum::from_i32(100))),
            text_clause(2, F_TEXTLIKE, false, b"%google%"),
        ],
        vec![
            cmp_clause(0, F_INT4GT, false, LaneCmpRhs::Const(Datum::from_i32(100))),
            text_clause(2, F_TEXTLIKE, false, b"%google%"),
        ],
        vec![
            cmp_clause(0, F_INT4EQ, true, LaneCmpRhs::Const(Datum::from_i32(100))),
            text_clause(2, F_TEXTLIKE, false, b"%google%"),
        ],
        vec![
            cmp_clause(0, F_INT4EQ, false, LaneCmpRhs::Const(Datum::from_i32(101))),
            text_clause(2, F_TEXTLIKE, false, b"%google%"),
        ],
        vec![
            cmp_clause(0, F_INT4EQ, false, LaneCmpRhs::Const(Datum::from_i32(100))),
            text_clause(2, F_TEXTLIKE, false, b"%googleX%"),
        ],
        vec![
            cmp_clause(0, F_INT4EQ, false, LaneCmpRhs::Const(Datum::from_i32(100))),
            text_clause(2, F_TEXTNLIKE, false, b"%google%"),
        ],
    ];
    let b = fp(base());
    for (i, v) in variants.into_iter().enumerate() {
        assert_ne!(b, fp(v), "variant {i} must not share the fingerprint");
    }
    // Clause order is keyed too (original order is the canonical spelling).
    let swapped = vec![
        text_clause(2, F_TEXTLIKE, false, b"%google%"),
        cmp_clause(0, F_INT4EQ, false, LaneCmpRhs::Const(Datum::from_i32(100))),
    ];
    assert_ne!(b, fp(swapped));
}

#[test]
fn condcache_fingerprint_ignores_requal_tail() {
    // Same 2-clause prefix; one shape carries a non-volatile per-row tail
    // (int4eq is provably non-volatile only with a syscache — use a second
    // undecodable-but-parsed clause via LaneSuffix::None vs a trailing
    // parsed clause is walker territory; here: identical prefixes across
    // two translations, one truncated one not, must match because the
    // fingerprint hashes prefix clauses only). The tail-bearing shape needs
    // the hybrid gate: prefix >= 2 clauses.
    let prefix = || {
        vec![
            cmp_clause(0, F_INT4EQ, false, LaneCmpRhs::Const(Datum::from_i32(7))),
            cmp_clause(1, F_INT4LT, false, LaneCmpRhs::Const(Datum::from_i32(9))),
        ]
    };
    let plain = translate_scan_qual(&shape(prefix(), LaneSuffix::None), true)
        .expect("translates");
    let tailed = translate_scan_qual(&shape(prefix(), LaneSuffix::Opaque), true)
        .expect("hybrid engages at prefix >= 2");
    assert!(tailed.requal && !plain.requal);
    assert_eq!(plain.fingerprint, tailed.fingerprint);
}
