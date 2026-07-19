use std::sync::Once;

use datum::Datum;
use mcx::MemoryContext;
use types_core::{C_COLLATION_OID, POSIX_COLLATION_OID};
use types_fmgr::LocalFcinfo;

use super::builtins::*;
use super::*;

static SETUP: Once = Once::new();

fn setup() {
    SETUP.call_once(|| {
        // UTF-8 boundary clip, enough for the test inputs.
        mbutils_seams::pg_mbcliplen::set(|s, len, limit| {
            let mut l = (limit as usize).min(len as usize);
            while l > 0 && s[l] & 0xC0 == 0x80 {
                l -= 1;
            }
            l as i32
        });
        mbutils_seams::pg_server_to_client::set(|_mcx, _s| Ok(None));
    });
}

fn name(s: &str) -> NameData {
    let mut n = NameData::default();
    n.namestrcpy(s);
    n
}

#[test]
fn namein_pads_and_truncates() {
    setup();
    let n = namein(b"pg_class");
    assert_eq!(n.name_str(), b"pg_class");
    assert_eq!(&n.data[8..], &[0u8; 56]);

    let exact = [b'a'; 63];
    assert_eq!(namein(&exact).name_str(), &exact[..]);

    let long = [b'x'; 100];
    let n = namein(&long);
    assert_eq!(n.name_str().len(), 63);
    assert_eq!(n.data[63], 0);

    // Multibyte truncation lands on a char boundary.
    let s = "é".repeat(40);
    let n = namein(s.as_bytes());
    assert_eq!(n.name_str().len(), 62);
    assert!(std::str::from_utf8(n.name_str()).is_ok());
}

#[test]
fn nameout_roundtrip() {
    setup();
    let ctx = MemoryContext::new("t");
    let n = name("pg_type");
    let out = nameout(ctx.mcx(), &n).unwrap();
    assert_eq!(&out[..], b"pg_type");

    let mut buf = Vec::new();
    nameout_into(&n, &mut buf);
    assert_eq!(&buf, b"pg_type\0");
}

#[test]
fn recv_send_roundtrip() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let bytea = namesend(mcx, &name("some_ident")).unwrap();
    assert_eq!(bytea.data(), b"some_ident");

    let mut si = stringinfo::StringInfo::new_in(mcx).unwrap();
    si.append_bytes(b"some_ident").unwrap();
    let n = namerecv(mcx, &mut si).unwrap();
    assert_eq!(n.name_str(), b"some_ident");

    let mut si = stringinfo::StringInfo::new_in(mcx).unwrap();
    si.append_bytes(&[b'z'; 64]).unwrap();
    let err = namerecv(mcx, &mut si).unwrap_err();
    assert!(err.to_string().contains("identifier too long"));
}

#[test]
fn comparisons_c_collation() {
    setup();
    for collid in [C_COLLATION_OID, POSIX_COLLATION_OID] {
        let (a, b) = (name("abc"), name("abd"));
        assert!(nameeq(&a, &a, collid).unwrap());
        assert!(namene(&a, &b, collid).unwrap());
        assert!(namelt(&a, &b, collid).unwrap());
        assert!(namele(&a, &a, collid).unwrap());
        assert!(namegt(&b, &a, collid).unwrap());
        assert!(namege(&b, &b, collid).unwrap());
        assert_eq!(btnamecmp(&a, &b, collid).unwrap(), -1);
        assert_eq!(btnamecmp(&b, &a, collid).unwrap(), 1);
        assert_eq!(btnamecmp(&a, &a, collid).unwrap(), 0);
    }
    // Prefix sorts first; bytes compare unsigned. Raw strncmp magnitudes
    // (names are NUL-padded, so the prefix case compares NUL vs the next
    // byte). C 18.3: btnamecmp('ab','abc') → -99; 0xC3 vs 'z' → 73.
    assert_eq!(btnamecmp(&name("ab"), &name("abc"), C_COLLATION_OID).unwrap(), -99);
    assert_eq!(
        btnamecmp(&name("a\u{ff}"), &name("az"), C_COLLATION_OID).unwrap(),
        0xC3 - b'z' as i32
    );
}

#[test]
fn namestrcmp_null_and_padding() {
    setup();
    assert_eq!(namestrcmp(None, None), 0);
    assert_eq!(namestrcmp(None, Some(b"x")), -1);
    assert_eq!(namestrcmp(Some(&name("x")), None), 1);
    assert_eq!(namestrcmp(Some(&name("pg_am")), Some(b"pg_am")), 0);
    assert!(namestrcmp(Some(&name("pg_am")), Some(b"pg_am2")) < 0);
    assert!(namestrcmp(Some(&name("pg_am2")), Some(b"pg_am")) > 0);
    // A 63-byte name equals its exact source; a longer C string wins at the
    // NUL (C strncmp semantics, raw byte difference: 0 - 'a' = -97).
    assert_eq!(namestrcmp(Some(&namein(&[b'a'; 63])), Some(&[b'a'; 63])), 0);
    assert_eq!(namestrcmp(Some(&namein(&[b'a'; 100])), Some(&[b'a'; 80])), -97);
}

#[test]
fn nameconcatoid_appends_and_truncates() {
    setup();
    let n = nameconcatoid(&name("func"), 1234);
    assert_eq!(n.name_str(), b"func_1234");

    let long = name(&"n".repeat(63));
    let n = nameconcatoid(&long, 4294967295);
    assert_eq!(n.name_str().len(), 63);
    assert!(n.name_str().ends_with(b"_4294967295"));
    assert_eq!(n.data[63], 0);
}

#[test]
fn fc_wrappers_and_table() {
    setup();
    let a = name("alpha");
    let b = name("beta");
    let mut fci = LocalFcinfo::<2>::new(C_COLLATION_OID);
    fci.set_arg(0, Datum::from_usize(a.data.as_ptr() as usize));
    fci.set_arg(1, Datum::from_usize(b.data.as_ptr() as usize));
    assert!(!fc_nameeq(None, &mut fci).unwrap().as_bool());
    assert!(fc_namene(None, &mut fci).unwrap().as_bool());
    assert!(fc_namelt(None, &mut fci).unwrap().as_bool());
    assert!(fc_namele(None, &mut fci).unwrap().as_bool());
    assert!(!fc_namegt(None, &mut fci).unwrap().as_bool());
    assert!(!fc_namege(None, &mut fci).unwrap().as_bool());
    assert_eq!(fc_btnamecmp(None, &mut fci).unwrap().as_i32(), -1);

    let mut fci = LocalFcinfo::<1>::new(0);
    fci.set_arg(0, Datum::from_usize(a.data.as_ptr() as usize));
    let d = fc_nameout(None, &mut fci).unwrap();
    let s = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    assert_eq!(s.to_bytes(), b"alpha");

    let mut prev = 0;
    for b in NAME_BUILTINS {
        assert!(b.foid > prev);
        prev = b.foid;
        assert!(b.strict && !b.retset);
    }
}

fn text_datum(mcx: mcx::Mcx<'_>, s: &str) -> Datum {
    let mut v = mcx::vec_with_capacity_in(mcx, 4 + s.len()).unwrap();
    mcx::vec_append_bytes(&mut v, &datum::varlena::set_varsize_4b(4 + s.len())).unwrap();
    mcx::vec_append_bytes(&mut v, s.as_bytes()).unwrap();
    let d = Datum::from_usize(v.as_ptr() as usize);
    core::mem::forget(v);
    d
}

// text_name (OID 407) and its varchar overload (OID 1400, same prosrc): the
// oracle-starving gap was 1400 missing from NAME_BUILTINS entirely.
#[test]
fn fc_text_name_truncates_and_is_registered_for_both_oids() {
    setup();
    let ctx = MemoryContext::new("t");
    let mut fci = LocalFcinfo::<1>::new(0);
    unsafe { fci.set_result_mcx(ctx.mcx()) };
    fci.set_arg(0, text_datum(ctx.mcx(), "pg_class"));
    let d = fc_text_name(None, &mut fci).unwrap();
    let n = unsafe { &*(d.as_usize() as *const NameData) };
    assert_eq!(n.name_str(), b"pg_class");

    // Oversize input truncates on a multibyte boundary, matching namein.
    let long = "é".repeat(40);
    let mut fci = LocalFcinfo::<1>::new(0);
    unsafe { fci.set_result_mcx(ctx.mcx()) };
    fci.set_arg(0, text_datum(ctx.mcx(), &long));
    let d = fc_text_name(None, &mut fci).unwrap();
    let n = unsafe { &*(d.as_usize() as *const NameData) };
    assert_eq!(n.name_str().len(), 62);
    assert!(std::str::from_utf8(n.name_str()).is_ok());

    assert!(NAME_BUILTINS.iter().any(|b| b.foid == 407 && b.func as *const () == fc_text_name as *const ()));
    assert!(NAME_BUILTINS.iter().any(|b| b.foid == 1400 && b.func as *const () == fc_text_name as *const ()));
}

// fnconf batch-1 cmp-magnitude family: C's btnamecmp is raw strncmp — the
// byte difference at the first mismatch is byte-visible at the SQL level.
// C 18.3: btnamecmp('a','c') → -2. Red at base: pgrust returned -1.
#[test]
fn btnamecmp_returns_raw_strncmp_magnitude() {
    let a = name("a");
    let c = name("c");
    assert_eq!(btnamecmp(&a, &c, C_COLLATION_OID).unwrap(), -2);
    assert_eq!(btnamecmp(&c, &a, C_COLLATION_OID).unwrap(), 2);
    assert_eq!(btnamecmp(&a, &a, C_COLLATION_OID).unwrap(), 0);
}
