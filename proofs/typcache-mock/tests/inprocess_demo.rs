//! Demonstrates the typcache-mock unlock: `excluded(typcache)` builtins run
//! IN-PROCESS (no server), with the real `typcache::lookup_type_cache` filling
//! entries from the generated catalog tables.
//!
//! Three layers of evidence:
//! 1. `typcache_entries_match_live_server`: entry fields (opfamilies,
//!    operators, procs, resolved finfos) equal the values the live
//!    malisper/pgrust:v0.2 server derives for the same types. The literals
//!    below were read from that server's pg_opclass/pg_amop/pg_amproc/pg_range
//!    on 2026-07-30 (queries in the comments).
//! 2. `default_opclass_matches_server`: the transcribed GetDefaultOpClass —
//!    including the binary-coercion legs (varchar->text, _int4->anyarray,
//!    int4range->anyrange) — picks the same opclass oids the server holds.
//! 3. `array_*_differential`: array_append (oid 378), btarraycmp (382) and
//!    hash_array (626) — all `excluded(typcache)` ledger rows — executed
//!    in-process and (when TYPCACHE_ORACLE_CONTAINER is set) diffed against
//!    the live server over randomized inputs.

use datum::Datum;
use types_core::{Oid, INT4OID};
use types_fmgr::{FmgrInfo, LocalFcinfo};

const TEXTOID: Oid = 25;
const NUMERICOID: Oid = 1700;
const VARCHAROID: Oid = 1043;
const INT4ARRAYOID: Oid = 1007;
const INT4RANGEOID: Oid = 3904;
const INT4MULTIRANGEOID: Oid = 4451;

use typcache::{
    lookup_type_cache, TYPECACHE_BTREE_OPFAMILY, TYPECACHE_CMP_PROC, TYPECACHE_CMP_PROC_FINFO,
    TYPECACHE_EQ_OPR, TYPECACHE_EQ_OPR_FINFO, TYPECACHE_GT_OPR, TYPECACHE_HASH_EXTENDED_PROC,
    TYPECACHE_HASH_OPFAMILY, TYPECACHE_HASH_PROC, TYPECACHE_HASH_PROC_FINFO, TYPECACHE_LT_OPR,
    TYPECACHE_MULTIRANGE_INFO, TYPECACHE_RANGE_INFO,
};

const ALL_SCALAR: i32 = TYPECACHE_EQ_OPR
    | TYPECACHE_LT_OPR
    | TYPECACHE_GT_OPR
    | TYPECACHE_CMP_PROC
    | TYPECACHE_HASH_PROC
    | TYPECACHE_HASH_EXTENDED_PROC
    | TYPECACHE_EQ_OPR_FINFO
    | TYPECACHE_CMP_PROC_FINFO
    | TYPECACHE_HASH_PROC_FINFO
    | TYPECACHE_BTREE_OPFAMILY
    | TYPECACHE_HASH_OPFAMILY;

#[test]
fn typcache_entries_match_live_server() {
    typcache_mock::install();

    // int4: server: btree opc 1978 fam 1976; hash opc 10020 fam 1977;
    // amop(1976,23,23): 1<97 3=96 5>521; amproc(1976)=351; amproc(1977)=450,425.
    let e = lookup_type_cache(INT4OID, ALL_SCALAR).unwrap();
    assert_eq!(e.btree_opf(), 1976);
    assert_eq!(e.hash_opf(), 1977);
    assert_eq!((e.eq_opr(), e.lt_opr(), e.gt_opr()), (96, 97, 521));
    assert_eq!(e.cmp_proc(), 351);
    assert_eq!((e.hash_proc(), e.hash_extended_proc()), (450, 425));
    assert_eq!(e.eq_opr_finfo().fn_oid, 65); // int4eq (oprcode of 96)
    assert_eq!(e.cmp_proc_finfo().fn_oid, 351);
    assert_eq!(e.hash_proc_finfo().fn_oid, 450);
    assert_eq!(e.typlen(), 4);
    assert!(e.typbyval());

    // text: fam 1994/1995; ops 664/98/666; cmp 360; hash 400,448.
    let e = lookup_type_cache(TEXTOID, ALL_SCALAR).unwrap();
    assert_eq!((e.btree_opf(), e.hash_opf()), (1994, 1995));
    assert_eq!((e.eq_opr(), e.lt_opr(), e.gt_opr()), (98, 664, 666));
    assert_eq!(e.cmp_proc(), 360);
    assert_eq!((e.hash_proc(), e.hash_extended_proc()), (400, 448));
    assert_eq!(e.typlen(), -1);
    assert_eq!(e.typcollation(), 100); // DEFAULT_COLLATION_OID

    // numeric: fam 1988/1998; ops 1754/1752/1756; cmp 1769; hash 432,780.
    let e = lookup_type_cache(NUMERICOID, ALL_SCALAR).unwrap();
    assert_eq!((e.btree_opf(), e.hash_opf()), (1988, 1998));
    assert_eq!((e.eq_opr(), e.lt_opr(), e.gt_opr()), (1752, 1754, 1756));
    assert_eq!(e.cmp_proc(), 1769);
    assert_eq!((e.hash_proc(), e.hash_extended_proc()), (432, 780));

    // int4[]: default opclasses resolve via anyarray coercion (server opc
    // 10000 fam 397 btree, opc 10001 fam 627 hash); array ops survive the
    // element-capability check because int4 has eq/cmp/hash:
    // amop(397): 1<1072 3=1070 5>1073; amproc(397)=382; amproc(627)=626,782.
    let e = lookup_type_cache(INT4ARRAYOID, ALL_SCALAR).unwrap();
    assert_eq!((e.btree_opf(), e.hash_opf()), (397, 627));
    assert_eq!((e.eq_opr(), e.lt_opr(), e.gt_opr()), (1070, 1072, 1073));
    assert_eq!(e.cmp_proc(), 382);
    assert_eq!((e.hash_proc(), e.hash_extended_proc()), (626, 782));
    assert_eq!(e.typelem(), INT4OID);

    // int4range: RANGE_INFO from pg_range row
    // (3904|23|4451|0|1978|3914|3922) — subtype int4, canonical 3914,
    // subdiff 3922, opclass 1978 -> fam 1976 cmp 351.
    let e = lookup_type_cache(INT4RANGEOID, TYPECACHE_RANGE_INFO | ALL_SCALAR).unwrap();
    let elem = e.rngelemtype().expect("rngelemtype");
    assert_eq!(elem.type_id, INT4OID);
    assert_eq!(e.rng_opfamily(), 1976);
    assert_eq!(e.rng_collation(), 0);
    assert_eq!(e.rng_cmp_proc_finfo().fn_oid, 351);
    assert_eq!(e.rng_canonical_finfo().fn_oid, 3914);
    assert_eq!(e.rng_subdiff_finfo().fn_oid, 3922);
    // scalar lanes ride anyrange coercion: fam 3901/3903, eq 3882, cmp 3870,
    // hash 3902/3417 (hash survives because int4 elems hash).
    assert_eq!((e.btree_opf(), e.hash_opf()), (3901, 3903));
    assert_eq!(e.eq_opr(), 3882);
    assert_eq!(e.cmp_proc(), 3870);
    assert_eq!((e.hash_proc(), e.hash_extended_proc()), (3902, 3417));

    // int4multirange links its range type.
    let e = lookup_type_cache(INT4MULTIRANGEOID, TYPECACHE_MULTIRANGE_INFO).unwrap();
    assert_eq!(e.rngtype().expect("rngtype").type_id, INT4RANGEOID);

    // varchar has no opclasses of its own; binary coercion to text gives it
    // text's families/operators, exactly as the server does.
    let e = lookup_type_cache(VARCHAROID, ALL_SCALAR).unwrap();
    assert_eq!((e.btree_opf(), e.hash_opf()), (1994, 1995));
    assert_eq!((e.eq_opr(), e.cmp_proc()), (98, 360));
}

#[test]
fn default_opclass_matches_server() {
    // Literals = live-server pg_opclass oids (opcdefault rows / coercion
    // targets), 2026-07-30.
    typcache_mock::install();
    let cases: &[(Oid, Oid, Oid)] = &[
        (INT4OID, 403, 1978),
        (INT4OID, 405, 10020),
        (TEXTOID, 403, 3126),
        (TEXTOID, 405, 10037),
        (NUMERICOID, 403, 3125),
        (NUMERICOID, 405, 10030),
        (1082, 403, 3122),          // date
        (1082, 405, 10011),
        (INT4ARRAYOID, 403, 10000), // -> anyarray array_ops
        (INT4ARRAYOID, 405, 10001),
        (INT4RANGEOID, 403, 10076), // -> anyrange range_ops
        (INT4RANGEOID, 405, 10077),
        (VARCHAROID, 403, 3126),    // -> text (preferred-type tiebreak)
        (VARCHAROID, 405, 10037),
    ];
    for &(t, am, want) in cases {
        assert_eq!(
            typcache_mock::get_default_opclass(t, am).unwrap(),
            want,
            "type {t} am {am}"
        );
    }
}

// ---- in-process execution of excluded(typcache) builtins -------------------

fn int4_array<'m>(mcx: mcx::Mcx<'m>, elems: &[Option<i32>]) -> mcx::PgVec<'m, u8> {
    let dv: Vec<Datum> = elems.iter().map(|e| Datum::from_i32(e.unwrap_or(0))).collect();
    let nulls: Vec<bool> = elems.iter().map(|e| e.is_none()).collect();
    let dims = [elems.len() as i32];
    arrayfuncs::construct::construct_md_array(
        mcx, &dv, Some(&nulls), 1, &dims, &[1], INT4OID, 4, true, b'i',
    )
    .unwrap()
}

fn datum_of(image: &[u8]) -> Datum {
    Datum::from_usize(image.as_ptr() as usize)
}

fn decode_int4_array(mcx: mcx::Mcx<'_>, d: Datum) -> Vec<Option<i32>> {
    let p = d.as_usize() as *const u8;
    let len = arrayfuncs::foundation::varsize_any(p);
    let image = unsafe { std::slice::from_raw_parts(p, len) };
    let (elems, nulls) =
        arrayfuncs::construct::deconstruct_array(mcx, image, 4, true, b'i', true).unwrap();
    elems
        .iter()
        .zip(nulls.iter())
        .map(|(d, &n)| if n { None } else { Some(d.as_i32()) })
        .collect()
}

fn pg_literal(elems: &[Option<i32>]) -> String {
    let inner: Vec<String> =
        elems.iter().map(|e| e.map_or("NULL".into(), |v| v.to_string())).collect();
    format!("{{{}}}", inner.join(","))
}

fn oracle(sql: &str) -> Option<String> {
    let container = std::env::var("TYPCACHE_ORACLE_CONTAINER").ok()?;
    let out = std::process::Command::new("docker")
        .args(["exec", &container, "psql", "-U", "postgres", "-Atc", sql])
        .output()
        .expect("docker exec psql");
    assert!(out.status.success(), "oracle query failed: {sql}: {}",
        String::from_utf8_lossy(&out.stderr));
    Some(String::from_utf8(out.stdout).unwrap().trim_end().to_string())
}

// xorshift — deterministic case generation, no rand dep.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn rand_elems(rng: &mut Rng, max_len: u64) -> Vec<Option<i32>> {
    let n = rng.below(max_len + 1) as usize;
    (0..n)
        .map(|_| {
            if rng.below(5) == 0 {
                None
            } else {
                Some((rng.next() as i32) % 100)
            }
        })
        .collect()
}

/// array_append (fmgr oid 378, `excluded(typcache)`): run the SHIPPED fc
/// wrapper in-process over the mock; diff against the live server when
/// TYPCACHE_ORACLE_CONTAINER is set.
#[test]
fn array_append_inprocess_differential() {
    typcache_mock::install();
    let ctx = mcx::MemoryContext::new("demo");
    let mcx = ctx.mcx();
    let mut flinfo = FmgrInfo::new(array_userfuncs::builtins::fc_array_append, 378, 2, false, false);

    let mut rng = Rng(0x74797063_6d6f636b);
    let mut checked_oracle = 0;
    for case in 0..500u32 {
        let elems = rand_elems(&mut rng, 6);
        let elem: Option<i32> =
            if rng.below(5) == 0 { None } else { Some((rng.next() as i32) % 100) };
        let arr = int4_array(mcx, &elems);

        let mut fc = LocalFcinfo::<2>::fresh(0);
        unsafe { fc.set_result_mcx(mcx) };
        fc.set_arg(0, datum_of(&arr));
        match elem {
            Some(v) => fc.set_arg(1, Datum::from_i32(v)),
            None => fc.set_arg_null(1),
        }
        let out = array_userfuncs::builtins::fc_array_append(Some(&mut flinfo), &mut fc).unwrap();
        let got = decode_int4_array(mcx, out);

        // Self-check: append semantics.
        let mut want = elems.clone();
        want.push(elem);
        assert_eq!(got, want, "case {case}");

        // Server diff on a subsample (oracle round-trips are the slow part —
        // that asymmetry is the point of the in-process unlock).
        if case % 25 == 0 {
            if let Some(srv) = oracle(&format!(
                "select array_append('{}'::int4[], {})::text",
                pg_literal(&elems),
                elem.map_or("NULL".into(), |v| v.to_string())
            )) {
                assert_eq!(pg_literal(&got), srv, "case {case} vs oracle");
                checked_oracle += 1;
            }
        }
    }
    eprintln!("array_append: 500 in-process cases, {checked_oracle} oracle-diffed");
}

/// btarraycmp (382) + hash_array (626), both `excluded(typcache)`: the
/// typcache lanes exercised here are the array element-capability walk
/// (eq/cmp/hash of int4) and *_FINFO resolution — all served by the mock.
#[test]
fn array_cmp_and_hash_inprocess_differential() {
    typcache_mock::install();
    let ctx = mcx::MemoryContext::new("demo2");
    let mcx = ctx.mcx();

    let mut cmp_flinfo = FmgrInfo::new(arrayfuncs::ops::fc_btarraycmp, 382, 2, true, false);
    let mut hash_flinfo = FmgrInfo::new(arrayfuncs::ops::fc_hash_array, 626, 1, true, false);

    let mut rng = Rng(0xa11a_5eed);
    let mut checked_oracle = 0;
    for case in 0..500u32 {
        let a = rand_elems(&mut rng, 5);
        let b = if rng.below(3) == 0 { a.clone() } else { rand_elems(&mut rng, 5) };
        // btarraycmp is C-strict on NULL args; element NULLs are fine, and
        // hash_array errors on element NULLs like the server — keep both
        // sides null-free for cmp+hash (append covers element NULLs).
        let a: Vec<Option<i32>> = a.into_iter().map(|e| e.or(Some(7))).collect();
        let b: Vec<Option<i32>> = b.into_iter().map(|e| e.or(Some(7))).collect();
        let ai = int4_array(mcx, &a);
        let bi = int4_array(mcx, &b);

        let mut fc = LocalFcinfo::<2>::fresh(0);
        unsafe { fc.set_result_mcx(mcx) };
        fc.set_arg(0, datum_of(&ai));
        fc.set_arg(1, datum_of(&bi));
        let cmp = arrayfuncs::ops::fc_btarraycmp(Some(&mut cmp_flinfo), &mut fc)
            .unwrap()
            .as_i32();

        let mut fh = LocalFcinfo::<2>::fresh(0);
        unsafe { fh.set_result_mcx(mcx) };
        fh.set_arg(0, datum_of(&ai));
        let hash = arrayfuncs::ops::fc_hash_array(Some(&mut hash_flinfo), &mut fh)
            .unwrap()
            .as_i32();

        if case % 25 == 0 {
            if let Some(srv) = oracle(&format!(
                "select btarraycmp('{}'::int4[], '{}'::int4[]), hash_array('{}'::int4[])",
                pg_literal(&a),
                pg_literal(&b),
                pg_literal(&a)
            )) {
                let mut it = srv.split('|');
                let scmp: i32 = it.next().unwrap().parse().unwrap();
                let shash: i32 = it.next().unwrap().parse().unwrap();
                assert_eq!(cmp, scmp, "cmp case {case}: {a:?} vs {b:?}");
                assert_eq!(hash, shash, "hash case {case}: {a:?}");
                checked_oracle += 1;
            }
        }
        // Local coherence: cmp==0 iff equal contents (no NULLs present).
        assert_eq!(cmp == 0, a == b, "case {case}");
    }
    eprintln!("btarraycmp+hash_array: 500 in-process cases, {checked_oracle} oracle-diffed");
}

/// The typcache-inst Kani lane stubs per-type entries by hand (int4: typlen 4
/// byval align 'i', eq_opr_finfo=int4eq(65), cmp=btint4cmp(351)). This pins
/// those hand-typed instantiation constants to the generated catalog through
/// the REAL lookup_type_cache fill — the proof stub can no longer rot silently.
#[test]
fn typcache_inst_instantiation_constants_are_catalog_true() {
    typcache_mock::install();
    for (oid, typlen, byval, align, eq_fn, cmp_proc) in [
        (INT4OID, 4i16, true, b'i' as i8, 65u32, 351u32), // int4
        (20, 8, true, b'd' as i8, 467, 842),              // int8: int8eq, btint8cmp
        (1082, 4, true, b'i' as i8, 1086, 1092),          // date: date_eq, date_cmp
        (2950, 16, false, b'c' as i8, 2956, 2960),        // uuid: uuid_eq, uuid_cmp
        (TEXTOID, -1, false, b'i' as i8, 67, 360),        // text: texteq, bttextcmp
    ] {
        let e = lookup_type_cache(
            oid,
            TYPECACHE_EQ_OPR_FINFO | TYPECACHE_CMP_PROC_FINFO,
        )
        .unwrap();
        assert_eq!(e.typlen(), typlen, "type {oid}");
        assert_eq!(e.typbyval(), byval, "type {oid}");
        assert_eq!(e.typalign(), align, "type {oid}");
        assert_eq!(e.eq_opr_finfo().fn_oid, eq_fn, "type {oid}");
        assert_eq!(e.cmp_proc_finfo().fn_oid, cmp_proc, "type {oid}");
    }
}

/// Throughput smoke: the reason to be in-process at all.
#[test]
fn inprocess_throughput_smoke() {
    typcache_mock::install();
    let ctx = mcx::MemoryContext::new_bump("bench");
    let mcx = ctx.mcx();
    let mut flinfo = FmgrInfo::new(array_userfuncs::builtins::fc_array_append, 378, 2, false, false);
    let arr = int4_array(mcx, &[Some(1), Some(2), Some(3)]);
    let n = 200_000u32;
    let t0 = std::time::Instant::now();
    let mut sink = 0usize;
    for i in 0..n {
        let scratch = mcx::MemoryContext::new_bump("iter");
        let smcx = scratch.mcx();
        let mut fc = LocalFcinfo::<2>::fresh(0);
        unsafe { fc.set_result_mcx(smcx) };
        fc.set_arg(0, datum_of(&arr));
        fc.set_arg(1, Datum::from_i32(i as i32));
        sink ^= array_userfuncs::builtins::fc_array_append(Some(&mut flinfo), &mut fc)
            .unwrap()
            .as_usize();
    }
    let dt = t0.elapsed();
    eprintln!(
        "array_append in-process: {n} calls in {dt:?} ({:.0} calls/sec) sink={sink:x}",
        n as f64 / dt.as_secs_f64()
    );
}

/// array_in (750) / array_out (751), both `excluded(typcache)`: the io lane
/// rides the pg_type_io_shape projection (element in/out fns resolved through
/// fmgr). Round-trips in-process; text form diffed against the server.
#[test]
fn array_io_inprocess_differential() {
    typcache_mock::install();
    let ctx = mcx::MemoryContext::new("io");
    let mcx = ctx.mcx();
    let mut out_flinfo = FmgrInfo::new(arrayfuncs::builtins::fc_array_out, 751, 1, true, false);
    let mut in_flinfo = FmgrInfo::new(arrayfuncs::builtins::fc_array_in, 750, 3, true, false);

    let mut rng = Rng(0x1057_10f0);
    let mut checked_oracle = 0;
    for case in 0..200u32 {
        let elems = rand_elems(&mut rng, 6);
        let arr = int4_array(mcx, &elems);

        // array_out
        let mut fo = LocalFcinfo::<2>::fresh(0);
        unsafe { fo.set_result_mcx(mcx) };
        fo.set_arg(0, datum_of(&arr));
        let d = arrayfuncs::builtins::fc_array_out(Some(&mut out_flinfo), &mut fo).unwrap();
        let cstr = unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const std::ffi::c_char) };
        let text = cstr.to_str().unwrap().to_string();
        assert_eq!(text, pg_literal(&elems), "array_out case {case}");

        // array_in round-trip
        let lit = std::ffi::CString::new(text.clone()).unwrap();
        let mut fi = LocalFcinfo::<3>::fresh(0);
        unsafe { fi.set_result_mcx(mcx) };
        fi.set_arg(0, Datum::from_usize(lit.as_ptr() as usize));
        fi.set_arg(1, Datum::from_oid(INT4OID));
        fi.set_arg(2, Datum::from_i32(-1));
        let back = arrayfuncs::builtins::fc_array_in(Some(&mut in_flinfo), &mut fi).unwrap();
        assert_eq!(decode_int4_array(mcx, back), elems, "array_in case {case}");

        if case % 25 == 0 {
            if let Some(srv) = oracle(&format!("select '{}'::int4[]::text", pg_literal(&elems))) {
                assert_eq!(text, srv, "case {case} vs oracle");
                checked_oracle += 1;
            }
        }
    }
    eprintln!("array_out/array_in: 200 in-process round-trips, {checked_oracle} oracle-diffed");
}
