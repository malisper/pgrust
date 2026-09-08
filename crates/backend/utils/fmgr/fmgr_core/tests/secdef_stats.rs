use std::sync::Once;

use datum::Datum;
use fmgr::{
    FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo, TRACK_FUNC_ALL, TRACK_FUNC_OFF,
    TRACK_FUNC_PL,
};
use types_error::PgResult;

const PL_LANG_OID: u32 = 13570;
const PL_CALL_HANDLER_OID: u32 = 13569;
const SECDEF_PL_OID: u32 = 90011;
const SECDEF_INTERNAL_OID: u32 = 90012;
const NESTED_OID: u32 = 90013;
const ERROR_OID: u32 = 90014;
const SRF_OID: u32 = 90015;
const SQL_CONFIG_OID: u32 = 90016;

fn fake_pl_call_handler(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    match flinfo.unwrap().fn_oid {
        NESTED_OID => {
            let mut inner = fmgr_core::fmgr_info(SECDEF_PL_OID)?;
            let mut args = LocalFcinfo::<0>::new(0);
            inner.invoke(&mut args)?;
        }
        ERROR_OID => return Err(Box::new(types_error::PgError::error("test error"))),
        SRF_OID => {
            let rs = fcinfo.rsinfo_mut().unwrap();
            rs.isDone = if rs.isDone == fmgr::ExprDoneCond::ExprMultipleResult {
                fmgr::ExprDoneCond::ExprEndResult
            } else {
                fmgr::ExprDoneCond::ExprMultipleResult
            };
        }
        _ => {}
    }
    Ok(Datum::from_i32(7))
}

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_proc_fmgr::set(|funcid| {
            let (prolang, pronargs) = match funcid {
                SECDEF_PL_OID | NESTED_OID | ERROR_OID | SRF_OID => (PL_LANG_OID, 0),
                SQL_CONFIG_OID => (14, 0),
                PL_CALL_HANDLER_OID => (14, 0),
                SECDEF_INTERNAL_OID => (12, 2),
                other => panic!("unexpected pg_proc lookup for {other}"),
            };
            Ok(Some(syscache_seams::PgProcFmgrShape {
                prolang,
                prorettype: 23,
                pronargs,
                proisstrict: false,
                proretset: funcid == SRF_OID,
                prosecdef: funcid != SQL_CONFIG_OID,
                proconfig_isnull: funcid != SQL_CONFIG_OID,
                xmin: 0,
                tid: Default::default(),
            }))
        });
        syscache_seams::lookup_pg_proc_prosrc::set(|mcx, funcid| {
            let prosrc = match funcid {
                PL_CALL_HANDLER_OID => "plpgsql_call_handler",
                SECDEF_INTERNAL_OID => "int4pl",
                other => panic!("unexpected prosrc lookup for {other}"),
            };
            Ok(Some(mcx::PgString::from_str_in(prosrc, mcx)?))
        });
        syscache_seams::lookup_pg_language_fmgr::set(|lang| {
            assert_eq!(lang, PL_LANG_OID);
            Ok(Some(syscache_seams::PgLanguageFmgrShape {
                lanplcallfoid: PL_CALL_HANDLER_OID,
                laninline: 0,
                lanvalidator: 0,
            }))
        });
        syscache_seams::lookup_pg_proc_secdef::set(|funcid| {
            Ok(Some(syscache_seams::PgProcSecdefShape {
                proowner: 42,
                prosecdef: funcid != SQL_CONFIG_OID,
                proconfig: (funcid == SQL_CONFIG_OID).then(|| vec!["work_mem=64MB".to_owned()]),
            }))
        });
        syscache_seams::search_syscache_exists_procoid::set(|_| Ok(true));
        inval_seams::accept_invalidation_messages::set(|| Ok(()));
        miscinit_seams::get_user_id_and_sec_context::set(|| (10, 0));
        miscinit_seams::set_user_id_and_sec_context::set(|_, _| {});
        guc_seams::new_guc_nest_level::set(|| 1);
        guc_seams::process_guc_array_secdef::set(|_| Ok(()));
        guc_seams::at_eoxact_guc::set(|_, _| Ok(()));
        fmgr_core::register_sql_language_handler(fake_pl_call_handler);
    });
}

fn numcalls(funcid: u32) -> Option<i64> {
    pgstat::find_funcstat_entry(funcid).map(|c| c.numcalls)
}

#[test]
fn security_definer_pl_function_is_counted_under_track_functions_pl() {
    install_seams();
    pgstat::function::set_pgstat_track_functions(i32::from(TRACK_FUNC_PL));

    let mut flinfo = fmgr_core::fmgr_info(SECDEF_PL_OID).unwrap();
    assert!(flinfo.is_security_definer_wrapper());
    assert_eq!(flinfo.fn_stats, TRACK_FUNC_ALL);
    assert_eq!(numcalls(SECDEF_PL_OID), None);

    let mut fci = LocalFcinfo::<0>::new(0);
    assert_eq!(flinfo.invoke(&mut fci).unwrap().as_i32(), 7);
    assert_eq!(
        numcalls(SECDEF_PL_OID),
        Some(1),
        "SECURITY DEFINER PL function invisible to track_functions = pl"
    );

    let mut fci = LocalFcinfo::<0>::new(0);
    assert_eq!(flinfo.invoke(&mut fci).unwrap().as_i32(), 7);
    assert_eq!(numcalls(SECDEF_PL_OID), Some(2));
}

#[test]
fn security_definer_internal_function_is_never_counted() {
    install_seams();
    pgstat::function::set_pgstat_track_functions(i32::from(TRACK_FUNC_ALL));

    let mut flinfo = fmgr_core::fmgr_info(SECDEF_INTERNAL_OID).unwrap();
    assert!(flinfo.is_security_definer_wrapper());
    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i32(40));
    fci.set_arg(1, Datum::from_i32(2));
    assert_eq!(flinfo.invoke(&mut fci).unwrap().as_i32(), 42);
    assert_eq!(numcalls(SECDEF_INTERNAL_OID), None);
}

#[test]
fn security_definer_pl_function_is_not_counted_when_tracking_is_off() {
    install_seams();
    pgstat::function::set_pgstat_track_functions(i32::from(TRACK_FUNC_OFF));

    let mut flinfo = fmgr_core::fmgr_info(SECDEF_PL_OID).unwrap();
    let mut fci = LocalFcinfo::<0>::new(0);
    assert_eq!(flinfo.invoke(&mut fci).unwrap().as_i32(), 7);
    assert_eq!(numcalls(SECDEF_PL_OID), None);
}

#[test]
fn nested_security_definer_counts_each_function_once() {
    install_seams();
    pgstat::function::set_pgstat_track_functions(i32::from(TRACK_FUNC_PL));
    let before = numcalls(SECDEF_PL_OID).unwrap_or(0);
    let mut outer = fmgr_core::fmgr_info(NESTED_OID).unwrap();
    outer.invoke(&mut LocalFcinfo::<0>::new(0)).unwrap();
    assert_eq!(numcalls(NESTED_OID), Some(1));
    assert_eq!(numcalls(SECDEF_PL_OID), Some(before + 1));
    let counts = pgstat::find_funcstat_entry(NESTED_OID).unwrap();
    assert!(counts.total_time >= counts.self_time);
}

#[test]
fn failed_call_does_not_finalize_usage() {
    install_seams();
    pgstat::function::set_pgstat_track_functions(i32::from(TRACK_FUNC_PL));
    let mut flinfo = fmgr_core::fmgr_info(ERROR_OID).unwrap();
    assert!(flinfo.invoke(&mut LocalFcinfo::<0>::new(0)).is_err());
    assert_eq!(numcalls(ERROR_OID), Some(0));
    let counts = pgstat::find_funcstat_entry(ERROR_OID).unwrap();
    assert_eq!(counts.total_time, 0);
    assert_eq!(counts.self_time, 0);
}

#[test]
fn set_returning_call_finalizes_only_at_end() {
    install_seams();
    pgstat::function::set_pgstat_track_functions(i32::from(TRACK_FUNC_PL));
    let mut flinfo = fmgr_core::fmgr_info(SRF_OID).unwrap();
    let mut fci = LocalFcinfo::<0>::new(0);
    let mut rs = fmgr::ReturnSetInfo::new(fmgr::SFRM_ValuePerCall);
    fci.resultinfo = rs.as_fmnode_ptr();
    flinfo.invoke(&mut fci).unwrap();
    assert_eq!(numcalls(SRF_OID), Some(0));
    assert_eq!(rs.isDone, fmgr::ExprDoneCond::ExprMultipleResult);
    fci.resultinfo = rs.as_fmnode_ptr();
    flinfo.invoke(&mut fci).unwrap();
    assert_eq!(numcalls(SRF_OID), Some(1));
}

#[test]
fn proconfig_sql_function_requires_track_functions_all() {
    install_seams();
    let mut flinfo = fmgr_core::fmgr_info(SQL_CONFIG_OID).unwrap();
    assert!(flinfo.is_security_definer_wrapper());
    pgstat::function::set_pgstat_track_functions(i32::from(TRACK_FUNC_PL));
    flinfo.invoke(&mut LocalFcinfo::<0>::new(0)).unwrap();
    assert_eq!(numcalls(SQL_CONFIG_OID), None);
    pgstat::function::set_pgstat_track_functions(i32::from(TRACK_FUNC_ALL));
    flinfo.invoke(&mut LocalFcinfo::<0>::new(0)).unwrap();
    assert_eq!(numcalls(SQL_CONFIG_OID), Some(1));
}
