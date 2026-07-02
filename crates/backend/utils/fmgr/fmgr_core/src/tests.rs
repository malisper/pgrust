use ::datum::Datum;
use ::fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo, TRACK_FUNC_ALL};
use ::types_core::{primitive::InvalidOid, Oid};
use ::types_error::PgResult;

use crate::*;

fn int4pl_body(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    Ok(Datum::from_i32(fcinfo.arg_i32(0) + fcinfo.arg_i32(1)))
}

#[test]
fn table_matches_canonical_and_is_sorted() {
    assert_eq!(FMGR_BUILTINS.len(), CANONICAL.len());
    assert_eq!(FMGR_BUILTINS.len(), 3102);
    assert_eq!(FMGR_BUILTINS[FMGR_BUILTINS.len() - 1].foid, FMGR_LAST_BUILTIN_OID);
    for (i, (b, c)) in FMGR_BUILTINS.iter().zip(CANONICAL.iter()).enumerate() {
        assert_eq!((b.foid, b.name, b.nargs, b.strict, b.retset), *c);
        if i > 0 {
            assert!(FMGR_BUILTINS[i - 1].foid < b.foid);
        }
    }
}

#[test]
fn oid_index_round_trips_every_entry() {
    for b in FMGR_BUILTINS.iter() {
        let hit = fmgr_isbuiltin(b.foid).unwrap();
        assert!(core::ptr::eq(hit, b));
    }
}

#[test]
fn isbuiltin_misses_match_c() {
    assert!(fmgr_isbuiltin(InvalidOid).is_none());
    assert!(fmgr_isbuiltin(58).is_none());
    assert!(fmgr_isbuiltin(FMGR_LAST_BUILTIN_OID + 1).is_none());
    assert!(fmgr_isbuiltin(u32::MAX).is_none());
    assert!(fmgr_isbuiltin(6411).is_none());
}

#[test]
fn known_builtin_metadata() {
    let b = fmgr_isbuiltin(177).unwrap();
    assert_eq!((b.name, b.nargs, b.strict, b.retset), ("int4pl", 2, true, false));
    let b = fmgr_isbuiltin(6430).unwrap();
    assert_eq!(b.name, "uuidv7_interval");
    let b = fmgr_isbuiltin(3).unwrap();
    assert_eq!(b.name, "heap_tableam_handler");
    let b = fmgr_isbuiltin(6401).unwrap();
    assert!(b.retset && b.strict && b.nargs == 0);
}

#[test]
fn fmgr_info_builtin_fast_path() {
    let f = fmgr_info(177).unwrap();
    assert_eq!(f.fn_oid, 177);
    assert_eq!(f.fn_nargs, 2);
    assert!(f.fn_strict);
    assert!(!f.fn_retset);
    assert_eq!(f.fn_stats, TRACK_FUNC_ALL);
    assert!(f.fn_extra.is_none());
    assert!(f.fn_expr.is_none());
}

#[test]
fn fmgr_info_into_refills_carrier() {
    let mut f = FmgrInfo::unresolved();
    fmgr_info_into(177, &mut f).unwrap();
    assert_eq!((f.fn_oid, f.fn_nargs, f.fn_strict, f.fn_retset), (177, 2, true, false));
    f.set_fn_extra(41i32);
    fmgr_info_into(65, &mut f).unwrap();
    assert_eq!((f.fn_oid, f.fn_nargs), (65, 2));
    assert!(f.fn_extra.is_none());
    assert!(f.fn_expr.is_none());
    assert_eq!(f.fn_stats, TRACK_FUNC_ALL);
}

#[test]
#[should_panic(expected = "not a builtin")]
fn fmgr_info_non_builtin_panics() {
    let _ = fmgr_info(16384);
}

#[test]
#[should_panic(expected = "not ported")]
fn unported_builtin_invocation_panics() {
    let mut f = fmgr_info(177).unwrap();
    let mut fci = LocalFcinfo::<2>::new(InvalidOid);
    fci.set_arg(0, Datum::from_i32(1));
    fci.set_arg(1, Datum::from_i32(2));
    let _ = f.invoke(&mut fci);
}

#[test]
fn internal_function_lookup() {
    assert_eq!(fmgr_internal_function("int4pl"), 177);
    assert_eq!(fmgr_internal_function("uuidv7"), 6429);
    assert_eq!(fmgr_internal_function("no_such_function"), InvalidOid);
    assert_eq!(fmgr_internal_function(""), InvalidOid);
}

#[test]
#[should_panic(expected = "not a builtin")]
fn oid_function_call_non_builtin_panics() {
    let _ = oid_function_call2_coll(16385, InvalidOid, Datum::from_i32(1), Datum::from_i32(2));
}

const TEST_ENTRIES: &[FmgrBuiltin] = &[
    FmgrBuiltin { foid: 65, name: "int4eq", nargs: 2, strict: true, retset: false, func: int4pl_body },
    FmgrBuiltin { foid: 177, name: "int4pl", nargs: 2, strict: true, retset: false, func: int4pl_body },
];
const TEST_INDEX: BuiltinOidIndex<FMGR_OID_INDEX_SIZE> = BuiltinOidIndex::build(TEST_ENTRIES);

#[test]
fn generic_table_resolve_and_call() {
    let fbp = TEST_INDEX.lookup(TEST_ENTRIES, 177).unwrap();
    let mut flinfo = fmgr_info_from_builtin(fbp, 177);
    let r = function_call2_coll(&mut flinfo, InvalidOid, Datum::from_i32(40), Datum::from_i32(2));
    assert_eq!(r.unwrap().as_i32(), 42);
    assert!(TEST_INDEX.lookup(TEST_ENTRIES, 66).is_none());
    assert!(TEST_INDEX.lookup(TEST_ENTRIES, 0).is_none());
}

#[test]
fn resolve_once_carrier_reuse() {
    let fbp = TEST_INDEX.lookup(TEST_ENTRIES, 65).unwrap();
    let mut flinfo = fmgr_info_from_builtin(fbp, 65);
    for i in 0..100i32 {
        let r = function_call2_coll(&mut flinfo, InvalidOid, Datum::from_i32(i), Datum::from_i32(1));
        assert_eq!(r.unwrap().as_i32(), i + 1);
    }
    assert_eq!(flinfo.fn_oid, 65);
}

#[test]
fn ported_overlay_is_subset_of_canonical() {
    for (oid, _) in ported::PORTED.iter() {
        assert!(fmgr_isbuiltin(*oid).is_some());
    }
}
