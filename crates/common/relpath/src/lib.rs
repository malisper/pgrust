#![allow(non_snake_case)]

use mcx::{Mcx, PgString};
use types_core::{ForkNumber, Oid, ProcNumber, INVALID_PROC_NUMBER, MAIN_FORKNUM};
use types_error::PgResult;
use types_storage::{RelFileLocator, PG_TBLSPC_DIR, TABLESPACE_VERSION_DIRECTORY};

pub const DEFAULTTABLESPACE_OID: Oid = 1663;
pub const GLOBALTABLESPACE_OID: Oid = 1664;

pub fn forkname_chars(fork: ForkNumber) -> &'static str {
    match fork {
        ForkNumber::MAIN_FORKNUM => "main",
        ForkNumber::FSM_FORKNUM => "fsm",
        ForkNumber::VISIBILITYMAP_FORKNUM => "vm",
        ForkNumber::INIT_FORKNUM => "init",
        ForkNumber::InvalidForkNumber => panic!("forkname_chars: invalid fork number"),
    }
}

pub fn GetDatabasePath<'mcx>(mcx: Mcx<'mcx>, dbOid: Oid, spcOid: Oid) -> PgResult<PgString<'mcx>> {
    let s = if spcOid == GLOBALTABLESPACE_OID {
        debug_assert_eq!(dbOid, 0);
        "global".to_string()
    } else if spcOid == DEFAULTTABLESPACE_OID {
        format!("base/{dbOid}")
    } else {
        format!("{PG_TBLSPC_DIR}/{spcOid}/{TABLESPACE_VERSION_DIRECTORY}/{dbOid}")
    };
    Ok(PgString::from_str_in(&s, mcx)?)
}

pub fn GetRelationPath(
    rlocator: RelFileLocator,
    procNumber: ProcNumber,
    forkNumber: ForkNumber,
) -> String {
    use core::fmt::Write;
    let RelFileLocator { spcOid, dbOid, relNumber } = rlocator;
    let mut rp = String::with_capacity(64);
    if spcOid == GLOBALTABLESPACE_OID {
        debug_assert_eq!(dbOid, 0);
        debug_assert_eq!(procNumber, INVALID_PROC_NUMBER);
        rp.push_str("global/");
    } else if spcOid == DEFAULTTABLESPACE_OID {
        write!(rp, "base/{dbOid}/").unwrap();
    } else {
        write!(rp, "{PG_TBLSPC_DIR}/{spcOid}/{TABLESPACE_VERSION_DIRECTORY}/{dbOid}/").unwrap();
    }
    if procNumber != INVALID_PROC_NUMBER {
        write!(rp, "t{procNumber}_").unwrap();
    }
    write!(rp, "{relNumber}").unwrap();
    if forkNumber != MAIN_FORKNUM {
        rp.push('_');
        rp.push_str(forkname_chars(forkNumber));
    }
    rp
}

fn relpathbackend(rlocator: RelFileLocator, backend: ProcNumber, forknum: ForkNumber) -> String {
    GetRelationPath(rlocator, backend, forknum)
}

fn relpathperm(rlocator: RelFileLocator, forknum: ForkNumber) -> String {
    GetRelationPath(rlocator, INVALID_PROC_NUMBER, forknum)
}

pub fn init_seams() {
    relpath_seams::get_database_path::set(GetDatabasePath);
    relpath_seams::relpathbackend::set(relpathbackend);
    relpath_seams::relpathperm::set(relpathperm);
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_core::ForkNumber::*;

    #[test]
    fn relation_paths_match_c() {
        let shared = RelFileLocator::new(GLOBALTABLESPACE_OID, 0, 1262);
        assert_eq!(GetRelationPath(shared, INVALID_PROC_NUMBER, MAIN_FORKNUM), "global/1262");
        assert_eq!(
            GetRelationPath(shared, INVALID_PROC_NUMBER, VISIBILITYMAP_FORKNUM),
            "global/1262_vm"
        );

        let base = RelFileLocator::new(DEFAULTTABLESPACE_OID, 5, 16384);
        assert_eq!(GetRelationPath(base, INVALID_PROC_NUMBER, MAIN_FORKNUM), "base/5/16384");
        assert_eq!(GetRelationPath(base, INVALID_PROC_NUMBER, FSM_FORKNUM), "base/5/16384_fsm");
        assert_eq!(GetRelationPath(base, 7, MAIN_FORKNUM), "base/5/t7_16384");
        assert_eq!(GetRelationPath(base, 7, INIT_FORKNUM), "base/5/t7_16384_init");

        let spc = RelFileLocator::new(16385, 5, 16400);
        assert_eq!(
            GetRelationPath(spc, INVALID_PROC_NUMBER, MAIN_FORKNUM),
            format!("pg_tblspc/16385/{TABLESPACE_VERSION_DIRECTORY}/5/16400")
        );
        assert_eq!(
            GetRelationPath(spc, 3, FSM_FORKNUM),
            format!("pg_tblspc/16385/{TABLESPACE_VERSION_DIRECTORY}/5/t3_16400_fsm")
        );
    }

    #[test]
    fn database_paths_match_c() {
        let ctx = mcx::MemoryContext::new("t");
        let m = ctx.mcx();
        assert_eq!(GetDatabasePath(m, 0, GLOBALTABLESPACE_OID).unwrap().as_str(), "global");
        assert_eq!(GetDatabasePath(m, 5, DEFAULTTABLESPACE_OID).unwrap().as_str(), "base/5");
        assert_eq!(
            GetDatabasePath(m, 5, 16385).unwrap().as_str(),
            format!("pg_tblspc/16385/{TABLESPACE_VERSION_DIRECTORY}/5")
        );
    }
}
