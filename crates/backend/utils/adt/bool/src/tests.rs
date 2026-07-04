use mcx::MemoryContext;
use types_fmgr::LocalFcinfo;

use super::builtins::*;
use super::*;

use ::datum::Datum;

#[test]
fn parse_bool_spellings_match_c() {
    for (s, v) in [
        ("true", true),
        ("TRUE", true),
        ("t", true),
        ("tr", true),
        ("yes", true),
        ("y", true),
        ("on", true),
        ("ON", true),
        ("1", true),
        ("false", false),
        ("f", false),
        ("FaLsE", false),
        ("no", false),
        ("n", false),
        ("off", false),
        ("of", false),
        ("0", false),
    ] {
        assert_eq!(parse_bool(s), Some(v), "{s}");
    }
    for s in [
        "", "o", "O", "truex", "truee", "yess", "offf", "11", "00", "2", "tru e", " t", "-",
        "\u{e9}",
    ] {
        assert_eq!(parse_bool(s), None, "{s}");
    }
}

#[test]
fn boolin_trims_and_errors_like_c() {
    assert!(boolin("  true  ", None).unwrap());
    assert!(boolin("\t\n on \r", None).unwrap());
    assert!(!boolin("false", None).unwrap());
    assert!(!boolin(" of ", None).unwrap());

    let err = boolin("o", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(err.message(), "invalid input syntax for type boolean: \"o\"");
    let err = boolin(" junk ", None).unwrap_err();
    assert_eq!(
        err.message(),
        "invalid input syntax for type boolean: \" junk \""
    );

    let mut soft = SoftErrorContext::new(true);
    assert!(!boolin("nope!", Some(&mut soft)).unwrap());
    assert!(soft.error_occurred());
}

#[test]
fn out_text_wire_forms() {
    assert_eq!(boolout(true), b't');
    assert_eq!(boolout(false), b'f');

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(booltext(mcx, true).unwrap().data(), b"true");
    assert_eq!(booltext(mcx, false).unwrap().data(), b"false");

    assert_eq!(boolsend(mcx, true).unwrap().data(), &[1]);
    assert_eq!(boolsend(mcx, false).unwrap().data(), &[0]);

    let mut si = stringinfo::StringInfo::new_in(mcx).unwrap();
    si.append_bytes(&[7]).unwrap();
    assert!(boolrecv(&mut si).unwrap());
    let mut si = stringinfo::StringInfo::new_in(mcx).unwrap();
    si.append_bytes(&[0]).unwrap();
    assert!(!boolrecv(&mut si).unwrap());
}

#[test]
fn comparisons_and_hashes() {
    for a in [false, true] {
        for b in [false, true] {
            assert_eq!(booleq(a, b), a == b);
            assert_eq!(boolne(a, b), a != b);
            assert_eq!(boollt(a, b), !a & b);
            assert_eq!(boolgt(a, b), a & !b);
            assert_eq!(boolle(a, b), a <= b);
            assert_eq!(boolge(a, b), a >= b);
            assert_eq!(booland_statefunc(a, b), a && b);
            assert_eq!(boolor_statefunc(a, b), a || b);
        }
    }
    assert_eq!(hashbool(true), hashfn::hash_bytes_uint32(1));
    assert_eq!(hashbool(false), hashfn::hash_bytes_uint32(0));
    assert_eq!(
        hashboolextended(true, 42),
        hashfn::hash_bytes_uint32_extended(1, 42)
    );
}

#[test]
fn bool_agg_state_machine() {
    let s = bool_accum(None, Some(true));
    let s = bool_accum(Some(s), Some(false));
    let s = bool_accum(Some(s), None);
    assert_eq!(s, BoolAggState { aggcount: 2, aggtrue: 1 });
    assert_eq!(bool_alltrue(Some(&s)), Some(false));
    assert_eq!(bool_anytrue(Some(&s)), Some(true));

    let s = bool_accum_inv(Some(s), Some(false)).unwrap();
    assert_eq!(bool_alltrue(Some(&s)), Some(true));

    let empty = bool_accum(None, None);
    assert_eq!(bool_alltrue(Some(&empty)), None);
    assert_eq!(bool_anytrue(Some(&empty)), None);
    assert_eq!(bool_alltrue(None), None);

    let err = bool_accum_inv(None, Some(true)).unwrap_err();
    assert_eq!(err.message(), "bool_accum_inv called with NULL state");
}

fn call2(f: types_fmgr::PGFunction, a: Datum, b: Datum) -> Datum {
    let mut fcinfo = LocalFcinfo::<2>::new(0);
    fcinfo.set_arg(0, a);
    fcinfo.set_arg(1, b);
    f(None, &mut fcinfo).unwrap()
}

#[test]
fn fc_wrappers_and_registry() {
    assert!(call2(fc_booleq, Datum::from_bool(true), Datum::from_bool(true)).as_bool());
    assert!(call2(fc_boollt, Datum::from_bool(false), Datum::from_bool(true)).as_bool());
    assert!(!call2(fc_boolge, Datum::from_bool(false), Datum::from_bool(true)).as_bool());
    assert_eq!(
        call2(fc_hashboolextended, Datum::from_bool(true), Datum::from_i64(7)).as_u64(),
        hashboolextended(true, 7)
    );

    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_bool(true));
    let out = fc_boolout(None, &mut fcinfo).unwrap();
    // SAFETY: fc_boolout returns a NUL-terminated cstring datum.
    let s = unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) };
    assert_eq!(s.to_bytes(), b"t");

    let mut oids: Vec<u32> = BOOL_BUILTINS.iter().map(|r| r.foid).collect();
    let sorted = {
        let mut v = oids.clone();
        v.sort_unstable();
        v
    };
    assert_eq!(oids, sorted);
    oids.dedup();
    assert_eq!(oids.len(), BOOL_BUILTINS.len());
    // bool_accum/bool_accum_inv (3496/3497) are the catalog's two
    // non-strict rows (NULL transvalue on first call).
    assert!(BOOL_BUILTINS
        .iter()
        .all(|r| (r.strict || r.foid == 3496 || r.foid == 3497) && !r.retset));
}

#[test]
fn parse_bool_seam_installed() {
    init_seams();
    assert_eq!(scalar_seams::parse_bool::call("yes"), Some(true));
    assert_eq!(scalar_seams::parse_bool::call("nyet"), None);
}

#[test]
fn fc_booltext_result_mcx() {
    let ctx = mcx::MemoryContext::new_bump("t");
    for (b, want) in [(true, &b"true"[..]), (false, &b"false"[..])] {
        let d = types_fmgr::direct_function_call1_coll_in(
            crate::builtins::fc_booltext,
            0,
            ctx.mcx(),
            datum::Datum::from_bool(b),
        )
        .unwrap();
        // SAFETY: booltext result is a live 4B-header varlena in ctx.
        let r = unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
        assert_eq!(r.data(), want);
    }
}
