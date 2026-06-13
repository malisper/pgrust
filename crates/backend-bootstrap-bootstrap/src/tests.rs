//! Unit tests for the bootstrap-mode catalog loader.
//!
//! These exercise the in-crate algorithm (the `TypInfo[]` table, the `gettype`
//! / `boot_get_type_io_data` lookups against the hard-wired array, the C-string
//! helpers, and the faithful `getopt` parser) without installing any seam,
//! since the tested paths take the `Typ == NIL` branch and the pure-logic
//! helpers. The seam-reaching paths (relcache, heap insert, fmgr) are covered
//! at wiring time when their owners land.

use super::*;
use mcx::MemoryContext;

/// A process-lifetime context for the `Mcx<'static>`-taking entry points. The
/// tested paths never allocate through it.
fn test_mcx() -> Mcx<'static> {
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("bootstrap-test")));
    ctx.mcx()
}

/// The `TypInfo[]` table must have the same 25 entries as `bootstrap.c`.
#[test]
fn typinfo_table_has_all_boot_types() {
    assert_eq!(n_types(), 25);
    assert_eq!(TYP_INFO[0].name, "bool");
    assert_eq!(TYP_INFO[0].oid, BOOLOID);
    assert_eq!(TYP_INFO[0].inproc, F_BOOLIN);
    assert_eq!(TYP_INFO[0].outproc, F_BOOLOUT);
    let last = &TYP_INFO[n_types() - 1];
    assert_eq!(last.name, "_aclitem");
    assert_eq!(last.oid, 1034);
    assert_eq!(last.elem, ACLITEMOID);
    assert_eq!(last.inproc, F_ARRAY_IN);
}

#[test]
fn typinfo_name_entry_is_faithful() {
    let name = TYP_INFO.iter().find(|t| t.name == "name").unwrap();
    assert_eq!(name.oid, NAMEOID);
    assert_eq!(name.elem, CHAROID);
    assert_eq!(name.len, NAMEDATALEN as i16);
    assert!(!name.byval);
    assert_eq!(name.align, TYPALIGN_CHAR);
    assert_eq!(name.collation, C_COLLATION_OID);
}

/// With `Typ == NIL`, `gettype` returns the *index* into `TypInfo[]` (the C
/// "ugly" contract), not an OID.
#[test]
fn gettype_returns_typinfo_index_when_typ_nil() {
    let mcx = test_mcx();
    set_typ(None);
    assert_eq!(gettype(mcx, "bool").unwrap(), 0);
    assert_eq!(gettype(mcx, "int4").unwrap(), 4);
    assert_eq!(gettype(mcx, "_aclitem").unwrap(), (n_types() - 1) as Oid);
}

/// With `Typ == NIL`, `boot_get_type_io_data` reads the hard-wired `TypInfo`
/// array.
#[test]
fn boot_get_type_io_data_uses_typinfo_when_typ_nil() {
    let mcx = test_mcx();
    set_typ(None);

    let io = boot_get_type_io_data(mcx, INT4OID).unwrap();
    assert_eq!(io.typlen, 4);
    assert!(io.typbyval);
    assert_eq!(io.typalign, TYPALIGN_INT);
    assert_eq!(io.typdelim, b',' as i8);
    assert_eq!(io.typioparam, INT4OID);
    assert_eq!(io.typinput, F_INT4IN);
    assert_eq!(io.typoutput, F_INT4OUT);

    let io = boot_get_type_io_data(mcx, INT4ARRAYOID).unwrap();
    assert_eq!(io.typlen, -1);
    assert!(!io.typbyval);
    assert_eq!(io.typioparam, INT4OID);
    assert_eq!(io.typinput, F_ARRAY_IN);
    assert_eq!(io.typoutput, F_ARRAY_OUT);

    let io = boot_get_type_io_data(mcx, NAMEOID).unwrap();
    assert_eq!(io.typioparam, CHAROID);
}

#[test]
fn boot_get_type_io_data_errors_on_unknown_oid_when_typ_nil() {
    let mcx = test_mcx();
    set_typ(None);
    let err = boot_get_type_io_data(mcx, 999_999).unwrap_err();
    assert!(err.message().contains("not found in TypInfo"));
}

#[test]
fn gettype_known_names_resolve() {
    let mcx = test_mcx();
    set_typ(None);
    assert_eq!(gettype(mcx, "regclass").unwrap(), 7);
    assert_eq!(gettype(mcx, "oidvector").unwrap(), 19);
}

#[test]
fn namestrcpy_pads_and_truncates() {
    let mut nd = NameData::default();
    namestrcpy(&mut nd, "pg_class");
    assert_eq!(namestr(&nd), "pg_class");
    let long = "x".repeat(100);
    namestrcpy(&mut nd, &long);
    assert_eq!(namestr(&nd).len(), NAMEDATALEN - 1);
}

#[test]
fn strncmp_matches_c_semantics() {
    assert!(strncmp_str("int4", "int4", NAMEDATALEN));
    assert!(!strncmp_str("int4", "int8", NAMEDATALEN));
    let mut nd = NameData::default();
    namestrcpy(&mut nd, "bool");
    assert!(strncmp_name(&nd, "bool", NAMEDATALEN));
    assert!(!strncmp_name(&nd, "boolean", NAMEDATALEN));
}

#[test]
fn oid_is_valid_is_oidisvalid() {
    assert!(!oid_is_valid(InvalidOid));
    assert!(oid_is_valid(1));
}

#[test]
fn getopt_parses_bootstrap_forms() {
    let argv = vec![
        String::from("postgres"),
        String::from("--boot"),
        String::from("-B"),
        String::from("16MB"),
        String::from("-F"),
        String::from("-k"),
        String::from("-X"),
        String::from("64"),
    ];
    let mut g = Getopt::new(&argv[1..], "B:c:d:D:Fkr:X:-:");
    assert_eq!(g.next(), Some('B'));
    assert_eq!(g.optarg.as_deref(), Some("16MB"));
    assert_eq!(g.next(), Some('F'));
    assert!(g.optarg.is_none());
    assert_eq!(g.next(), Some('k'));
    assert_eq!(g.next(), Some('X'));
    assert_eq!(g.optarg.as_deref(), Some("64"));
    assert_eq!(g.next(), None);
    assert_eq!(g.argc(), g.optind);
}

/// makeRangeVar faithful defaults.
#[test]
fn make_range_var_defaults() {
    let rv = make_range_var("pg_class");
    assert_eq!(rv.relname, "pg_class");
    assert!(rv.catalogname.is_none());
    assert!(rv.schemaname.is_none());
    assert!(rv.inh);
    assert_eq!(rv.relpersistence, RELPERSISTENCE_PERMANENT);
    assert_eq!(rv.location, -1);
}
