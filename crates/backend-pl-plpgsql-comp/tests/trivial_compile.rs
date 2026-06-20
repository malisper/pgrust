//! Smoke: a trivial `BEGIN RETURN 1; END` function drives scanner -> grammar ->
//! compiler through the comp-seams. End-to-end compilation additionally needs
//! the syscache `pg_type_form` owner (installed by `seams-init::init_all()` in
//! the real backend); in this unit test only this crate's seams are installed,
//! so the compile reaches the syscache boundary (building the magic FOUND var's
//! BOOL datatype) and stops there — proving every comp-seam the grammar fires
//! (curr_compile_*, build_variable/datatype, ns resolution, datum bookkeeping)
//! is wired. The cold-path driver is used (not the `DO` inline path) so the test
//! does not depend on the GUC table the inline path's `check_function_bodies`
//! read needs (that table is installed at backend boot, not in this unit test).
use std::panic::{catch_unwind, AssertUnwindSafe};

use backend_pl_plpgsql_comp::ProcCompileFacts;
use types_plpgsql::PLpgSQL_trigtype;

#[test]
fn trivial_block_reaches_catalog_boundary() {
    backend_pl_plpgsql_comp::init_seams();

    let facts = ProcCompileFacts {
        proname: "trivial".to_string(),
        fn_oid: 0,
        fn_input_collation: 0,
        prosrc: "BEGIN RETURN 1; END".to_string(),
        prorettype: 23, // int4
        proretset: false,
        prokind: b'f',
        provolatile: b'v',
        pronargs: 0,
        argtypes: Vec::new(),
        argnames: Vec::new(),
        argmodes: Vec::new(),
        fn_is_trigger: PLpgSQL_trigtype::PLPGSQL_NOT_TRIGGER,
        for_validator: false,
        resolved_rettype: 0,
        resolved_argtypes: Vec::new(),
    };

    let res = catch_unwind(AssertUnwindSafe(|| {
        backend_pl_plpgsql_comp::plpgsql_compile_from_source(&facts)
    }));

    let err = res.expect_err("without the syscache owner, the compile stops at pg_type_form");
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        msg.contains("pg_type_form"),
        "expected to reach the (uninstalled-in-unit-test) syscache pg_type_form \
         owner AFTER driving the grammar + comp-seams; got: {msg:?}"
    );
}
