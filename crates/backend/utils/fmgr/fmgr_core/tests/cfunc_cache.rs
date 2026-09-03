// fmgr.c CFuncHash protocol: an external C function is resolved through
// dfmgr once per pg_proc tuple; a replaced tuple (xmin/TID) resolves again.
// Own process: the seams below are set-once.
use core::sync::atomic::{AtomicU16, AtomicUsize, Ordering};

use datum::Datum;
use fmgr::{FmgrInfo, FunctionCallInfoBaseData, PGFunction};
use types_error::PgResult;
use types_tuple::ItemPointerData;

const C_FUNC_OID: u32 = 90002;
static LOOKUPS: AtomicUsize = AtomicUsize::new(0);
static TID_POS: AtomicU16 = AtomicU16::new(1);

fn tfn(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i32(7))
}

#[allow(function_casts_as_integer)] // fn address used as identity; the cast is intentional
fn resolve() -> usize {
    let flinfo = fmgr_core::fmgr_info(C_FUNC_OID).unwrap();
    assert_eq!(flinfo.fn_addr as usize, tfn as usize);
    assert_eq!(flinfo.fn_stats, fmgr::TRACK_FUNC_PL);
    LOOKUPS.load(Ordering::Relaxed)
}

#[test]
fn external_c_function_resolves_once_per_tuple() {
    syscache_seams::lookup_pg_proc_fmgr::set(|funcid| {
        assert_eq!(funcid, C_FUNC_OID);
        Ok(Some(syscache_seams::PgProcFmgrShape {
            prolang: fmgr_core::C_LANGUAGE_ID,
            prorettype: 23,
            pronargs: 0,
            proisstrict: false,
            proretset: false,
            prosecdef: false,
            proconfig_isnull: true,
            xmin: 1234,
            tid: ItemPointerData::new(0, TID_POS.load(Ordering::Relaxed)),
        }))
    });
    syscache_seams::lookup_pg_proc_prosrc::set(|mcx, _| {
        Ok(Some(mcx::PgString::from_str_in("tfn", mcx)?))
    });
    syscache_seams::lookup_pg_proc_probin::set(|mcx, _| {
        Ok(Some(mcx::PgString::from_str_in("$libdir/tlib", mcx)?))
    });
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: "tlib",
        lookup: |name| {
            LOOKUPS.fetch_add(1, Ordering::Relaxed);
            (name == "tfn").then_some(tfn as PGFunction)
        },
        pg_init: None,
    });
    let mut buf = [0u8; types_core::MAXPGPATH];
    let pkglib = format!("/nonexistent-pkglib-{}", std::process::id());
    buf[..pkglib.len()].copy_from_slice(pkglib.as_bytes());
    init_small::globals::set_pkglib_path(buf);
    dfmgr::dynamic_library_path_set(Some("$libdir".to_owned()));

    assert_eq!(resolve(), 1);
    assert_eq!(resolve(), 1, "second fmgr_info must hit CFuncHash");
    // CREATE OR REPLACE: a new pg_proc tuple invalidates the entry.
    TID_POS.store(2, Ordering::Relaxed);
    assert_eq!(resolve(), 2);
    assert_eq!(resolve(), 2);
}
