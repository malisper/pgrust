//! Vocabulary tests for `pg_db_role_setting`.
//!
//! `AlterSetting` / `DropSetting` / `ApplySetting` carry their decision logic
//! inline against the real genam `systable_*` iterator + the indexing/guc
//! catalog seams (the same boundary `pg_depend` uses), so there is no handle to
//! fake; what these tests pin is the genbki-assigned OID / attribute-number /
//! scan-key vocabulary the scan-key construction relies on.

use types_catalog::catalog::{
    DB_ROLE_SETTING_DATID_ROLID_INDEX_ID, DB_ROLE_SETTING_RELATION_ID,
};
use types_catalog::pg_db_role_setting::{
    Anum_pg_db_role_setting_setconfig, Anum_pg_db_role_setting_setdatabase,
    Anum_pg_db_role_setting_setrole, Natts_pg_db_role_setting,
};
use types_core::fmgr::F_OIDEQ;
use types_scan::scankey::BTEqualStrategyNumber;
use types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock};

#[test]
fn catalog_oids_match_postgres() {
    /* genbki-assigned OIDs the scan-key / relation-open construction relies on */
    assert_eq!(DB_ROLE_SETTING_RELATION_ID, 2964);
    assert_eq!(DB_ROLE_SETTING_DATID_ROLID_INDEX_ID, 2965);
}

#[test]
fn attribute_numbers_match_postgres() {
    /* pg_db_role_setting attribute numbers follow the CATALOG field order */
    assert_eq!(Anum_pg_db_role_setting_setdatabase, 1);
    assert_eq!(Anum_pg_db_role_setting_setrole, 2);
    assert_eq!(Anum_pg_db_role_setting_setconfig, 3);
    assert_eq!(Natts_pg_db_role_setting, 3);
}

#[test]
fn scan_key_and_lock_vocabulary_matches_postgres() {
    /* ScanKeyInit arguments (stratnum.h, pg_proc.dat) */
    assert_eq!(BTEqualStrategyNumber, 3);
    assert_eq!(F_OIDEQ, 184);
    /* RowExclusiveLock for AlterSetting/DropSetting, AccessShareLock for the
     * process_settings read, NoLock to keep AlterSetting's lock till commit */
    assert_eq!(AccessShareLock, 1);
    assert_eq!(RowExclusiveLock, 3);
    assert_eq!(NoLock, 0);
}
