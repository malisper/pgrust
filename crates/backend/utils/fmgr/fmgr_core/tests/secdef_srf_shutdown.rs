// fmgr_security_definer + rsinfo.srf_shutdown: C registers ShutdownSQLFunction
// with the fcache pointer (functions.c) and fmgr_security_definer owns
// fcache->flinfo; the port's hook re-derives the fcache from the FmgrInfo it
// is handed, and the SRF node hands it the OUTER FmgrInfo (whose fn_extra is
// the wrapper's cache). The wrapper must route the hook to the inner
// FmgrInfo. Own process: the seams below are set-once.
use core::sync::atomic::{AtomicUsize, Ordering};

use datum::Datum;
use fmgr::rsinfo::ExprDoneCond;
use fmgr::{FmgrInfo, FunctionCallInfoBaseData, PGFunction, ReturnSetInfo};
use types_error::PgResult;
use types_tuple::ItemPointerData;

const SRF_OID: u32 = 90003;
static SHUTDOWNS: AtomicUsize = AtomicUsize::new(0);

// The inner function's per-FmgrInfo state (stands in for SqlFcacheGuard).
struct InnerState {
    calls: usize,
}

fn inner_shutdown(flinfo: &mut FmgrInfo) -> PgResult<()> {
    // Panics with "fmgr fn_extra: downcast ... failed" if handed the outer
    // FmgrInfo (its fn_extra is the SecurityDefinerCache).
    let st = flinfo.fn_extra_mut::<InnerState>().expect("inner state planted");
    assert_eq!(st.calls, 2);
    SHUTDOWNS.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn srf(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let flinfo = flinfo.expect("flinfo");
    if flinfo.fn_extra_ref::<InnerState>().is_none() {
        flinfo.set_fn_extra(InnerState { calls: 0 });
    }
    let st = flinfo.fn_extra_mut::<InnerState>().unwrap();
    st.calls += 1;
    let calls = st.calls;
    let rsi = fcinfo.rsinfo_mut().expect("rsinfo armed");
    // The wrapper must have hidden its own hook from us.
    assert!(rsi.srf_shutdown.is_none(), "inner call sees a planted hook");
    if calls == 1 {
        // Lazy suspension: plant the hook (functions.c RegisterExprContextCallback).
        rsi.isDone = ExprDoneCond::ExprMultipleResult;
        rsi.srf_shutdown = Some(inner_shutdown);
    } else {
        // Later calls do not re-register; the callback stays registered.
        rsi.isDone = ExprDoneCond::ExprMultipleResult;
    }
    Ok(Datum::from_i32(calls as i32))
}

#[test]
fn security_definer_routes_srf_shutdown_to_inner_flinfo() {
    syscache_seams::lookup_pg_proc_fmgr::set(|funcid| {
        assert_eq!(funcid, SRF_OID);
        Ok(Some(syscache_seams::PgProcFmgrShape {
            prolang: fmgr_core::C_LANGUAGE_ID,
            prorettype: 23,
            pronargs: 0,
            proisstrict: false,
            proretset: true,
            prosecdef: true,
            proconfig_isnull: true,
            xmin: 1234,
            tid: ItemPointerData::new(0, 1),
        }))
    });
    syscache_seams::lookup_pg_proc_prosrc::set(|mcx, _| {
        Ok(Some(mcx::PgString::from_str_in("srf", mcx)?))
    });
    syscache_seams::lookup_pg_proc_probin::set(|mcx, _| {
        Ok(Some(mcx::PgString::from_str_in("$libdir/tsrf", mcx)?))
    });
    syscache_seams::lookup_pg_proc_secdef::set(|_| {
        Ok(Some(syscache_seams::PgProcSecdefShape {
            proowner: 42,
            prosecdef: true,
            proconfig: None,
        }))
    });
    miscinit_seams::get_user_id_and_sec_context::set(|| (10, 0));
    miscinit_seams::set_user_id_and_sec_context::set(|_, _| {});
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: "tsrf",
        lookup: |name| (name == "srf").then_some(srf as PGFunction),
        pg_init: None,
    });
    let mut buf = [0u8; types_core::MAXPGPATH];
    let pkglib = format!("/nonexistent-pkglib-{}", std::process::id());
    buf[..pkglib.len()].copy_from_slice(pkglib.as_bytes());
    init_small::globals::set_pkglib_path(buf);
    dfmgr::dynamic_library_path_set(Some("$libdir".to_owned()));

    let mut outer = fmgr_core::fmgr_info(SRF_OID).unwrap();
    assert!(outer.is_security_definer_wrapper());

    let mut rsi = ReturnSetInfo::new(fmgr::rsinfo::SFRM_ValuePerCall);
    // Call 1 suspends and plants the hook: the wrapper interposes its own.
    let mut fci = fmgr::LocalFcinfo::<0>::fresh(0);
    fci.resultinfo = rsi.as_fmnode_ptr();
    assert_eq!(outer.invoke(&mut fci).unwrap().as_i32(), 1);
    assert!(rsi.srf_shutdown.is_some(), "hook surfaces on the outer rsinfo");
    let hook1 = rsi.srf_shutdown.unwrap();

    // Call 2 plants nothing: the interposed hook stays registered.
    let mut fci = fmgr::LocalFcinfo::<0>::fresh(0);
    fci.resultinfo = rsi.as_fmnode_ptr();
    assert_eq!(outer.invoke(&mut fci).unwrap().as_i32(), 2);
    let hook2 = rsi.srf_shutdown.take().expect("hook kept across calls");
    assert!(core::ptr::fn_addr_eq(hook1, hook2));

    // The SRF node fires it with the OUTER FmgrInfo (nodeprojectset's
    // shutdown_srf_elems); it must reach the inner FmgrInfo's state.
    hook2(&mut outer).unwrap();
    assert_eq!(SHUTDOWNS.load(Ordering::Relaxed), 1);
    // Fired once; a second fire is a no-op (the callback was consumed).
    hook2(&mut outer).unwrap();
    assert_eq!(SHUTDOWNS.load(Ordering::Relaxed), 1);
}
