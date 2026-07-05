// fmgr_security_definer wrapper protocol (fmgr.c:632): userid switch under
// SECURITY_LOCAL_USERID_CHANGE, proconfig GUC push at a fresh nest level,
// pop + userid restore after the inner call. Own process: the seams below
// are set-once.
use std::sync::Mutex;

use datum::Datum;
use fmgr::{LocalFcinfo, TRACK_FUNC_ALL};

static EVENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());

const SECDEF_OID: u32 = 90001;

#[test]
fn security_definer_wrapper_switches_user_and_gucs() {
    syscache_seams::lookup_pg_proc_fmgr::set(|funcid| {
        assert_eq!(funcid, SECDEF_OID);
        Ok(Some(syscache_seams::PgProcFmgrShape {
            prolang: 12,
            prorettype: 23,
            pronargs: 2,
            proisstrict: true,
            proretset: false,
            prosecdef: true,
            proconfig_isnull: false,
        }))
    });
    syscache_seams::lookup_pg_proc_prosrc::set(|mcx, _| {
        Ok(Some(mcx::PgString::from_str_in("int4pl", mcx)?))
    });
    syscache_seams::lookup_pg_proc_secdef::set(|_| {
        Ok(Some(syscache_seams::PgProcSecdefShape {
            proowner: 42,
            prosecdef: true,
            proconfig: Some(vec!["work_mem=64MB".to_string()]),
        }))
    });
    miscinit_seams::get_user_id_and_sec_context::set(|| (10, 0));
    miscinit_seams::set_user_id_and_sec_context::set(|userid, ctx| {
        EVENTS.lock().unwrap().push(format!("setuser({userid},{ctx})"));
    });
    guc_seams::new_guc_nest_level::set(|| 7);
    guc_seams::process_guc_array_secdef::set(|array| {
        EVENTS.lock().unwrap().push(format!("guc({})", array.join(",")));
        Ok(())
    });
    guc_seams::at_eoxact_guc::set(|is_commit, nest| {
        EVENTS.lock().unwrap().push(format!("pop({is_commit},{nest})"));
        Ok(())
    });

    let mut flinfo = fmgr_core::fmgr_info(SECDEF_OID).unwrap();
    assert_eq!(flinfo.fn_addr as usize, fmgr_core::fmgr_security_definer as usize);
    assert_eq!(flinfo.fn_stats, TRACK_FUNC_ALL);
    assert!(flinfo.fn_strict);
    assert_eq!(flinfo.fn_nargs, 2);

    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i32(40));
    fci.set_arg(1, Datum::from_i32(2));
    assert_eq!(flinfo.invoke(&mut fci).unwrap().as_i32(), 42);

    assert_eq!(
        EVENTS.lock().unwrap().clone(),
        vec![
            "setuser(42,1)".to_string(), // owner | SECURITY_LOCAL_USERID_CHANGE
            "guc(work_mem=64MB)".to_string(),
            "pop(true,7)".to_string(),
            "setuser(10,0)".to_string(),
        ]
    );

    // Repeat call rides the fn_extra cache; same push/pop protocol.
    EVENTS.lock().unwrap().clear();
    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i32(1));
    fci.set_arg(1, Datum::from_i32(2));
    assert_eq!(flinfo.invoke(&mut fci).unwrap().as_i32(), 3);
    assert_eq!(EVENTS.lock().unwrap().len(), 4);
}
