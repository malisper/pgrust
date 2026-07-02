use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::AccessShareLock;

use crate::schemapg::{
    ACCESS_METHOD_PROCEDURE_INDEX_ID, ACCESS_METHOD_PROCEDURE_RELATION_ID,
    ATTRIBUTE_RELID_NUM_INDEX_ID, AUTH_ID_OID_INDEX_ID, AUTH_ID_ROLNAME_INDEX_ID,
    AUTH_MEM_MEM_ROLE_INDEX_ID, CAT_PG_SHSECLABEL, CLASS_OID_INDEX_ID, DATABASE_NAME_INDEX_ID,
    DATABASE_OID_INDEX_ID, INDEX_RELID_INDEX_ID, LOCAL_BOOTSTRAP_CATALOGS,
    OPCLASS_OID_INDEX_ID, OPERATOR_CLASS_RELATION_ID, REWRITE_RELATION_ID,
    REWRITE_REL_RULENAME_INDEX_ID, SHARED_BOOTSTRAP_CATALOGS, SHARED_SEC_LABEL_OBJECT_INDEX_ID,
    TRIGGER_RELATION_ID, TRIGGER_RELID_NAME_INDEX_ID,
};
use crate::{build, with_state};

pub const RELCACHE_INIT_FILENAME: &str = "pg_internal.init";
pub const RELCACHE_INIT_FILEMAGIC: i32 = 0x573266;
const TABLESPACE_VERSION_DIRECTORY: &str = "PG_18_202506291";
const PG_TBLSPC_DIR: &str = "pg_tblspc";
// BUILTIN_TRANCHE_NAMES[16] == "RelCacheInit"; pinned by a test.
const RELCACHE_INIT_LOCK_OFFSET: usize = 16;

pub fn RelationCacheInitialize() {
    // The hash table is created eagerly by the state cell (INITRELCACHESIZE).
    with_state(|_| ());
    relmapper_seams::relation_map_initialize::call();
}

pub fn RelationCacheInitializePhase2() -> PgResult<()> {
    relmapper_seams::relation_map_initialize_phase2::call()?;
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }
    if !load_relcache_init_file(true)? {
        for cat in SHARED_BOOTSTRAP_CATALOGS {
            build::formrdesc(cat)?;
        }
    }
    Ok(())
}

pub fn RelationCacheInitializePhase3() -> PgResult<()> {
    let mut need_new_cache_file = !with_state(|st| st.critical_shared_relcaches_built);
    relmapper_seams::relation_map_initialize_phase3::call()?;

    let bootstrap = miscinit_seams::is_bootstrap_processing_mode::call();
    if bootstrap || !load_relcache_init_file(false)? {
        need_new_cache_file = true;
        for cat in LOCAL_BOOTSTRAP_CATALOGS {
            build::formrdesc(cat)?;
        }
    }
    if bootstrap {
        return Ok(());
    }

    // Critical indexes break the relcache-load recursion: until they're
    // nailed, ScanPgRelation heapscans (criticalRelcachesBuilt gates index_ok).
    if !crate::criticalRelcachesBuilt() {
        load_critical_index(CLASS_OID_INDEX_ID, types_core::RELATION_RELATION_ID)?;
        load_critical_index(ATTRIBUTE_RELID_NUM_INDEX_ID, types_core::ATTRIBUTE_RELATION_ID)?;
        load_critical_index(INDEX_RELID_INDEX_ID, types_core::INDEX_RELATION_ID)?;
        load_critical_index(OPCLASS_OID_INDEX_ID, OPERATOR_CLASS_RELATION_ID)?;
        load_critical_index(ACCESS_METHOD_PROCEDURE_INDEX_ID, ACCESS_METHOD_PROCEDURE_RELATION_ID)?;
        load_critical_index(REWRITE_REL_RULENAME_INDEX_ID, REWRITE_RELATION_ID)?;
        load_critical_index(TRIGGER_RELID_NAME_INDEX_ID, TRIGGER_RELATION_ID)?;
        with_state(|st| st.critical_relcaches_built = true);
    }

    if !crate::criticalSharedRelcachesBuilt() {
        load_critical_index(DATABASE_NAME_INDEX_ID, types_core::DATABASE_RELATION_ID)?;
        load_critical_index(DATABASE_OID_INDEX_ID, types_core::DATABASE_RELATION_ID)?;
        load_critical_index(AUTH_ID_ROLNAME_INDEX_ID, types_core::AUTH_ID_RELATION_ID)?;
        load_critical_index(AUTH_ID_OID_INDEX_ID, types_core::AUTH_ID_RELATION_ID)?;
        load_critical_index(AUTH_MEM_MEM_ROLE_INDEX_ID, types_core::AUTH_MEM_RELATION_ID)?;
        load_critical_index(SHARED_SEC_LABEL_OBJECT_INDEX_ID, CAT_PG_SHSECLABEL.relid)?;
        with_state(|st| st.critical_shared_relcaches_built = true);
    }

    finish_relcache_entries()?;

    if need_new_cache_file {
        // C: InitCatalogCachePhase2() + write_relcache_init_file(true/false).
        write_relcache_init_file(true)?;
        write_relcache_init_file(false)?;
    }
    Ok(())
}

// Replace formrdesc stubs (relowner == InvalidOid) with the real pg_class
// row. C also refreshes rules/triggers/RLS/tableam here; those fields live
// with later units. Restart-from-scratch scan shape as in C.
fn finish_relcache_entries() -> PgResult<()> {
    loop {
        let target = with_state(|st| {
            st.id_cache
                .iter()
                .find(|(_, e)| e.rel.rd_rel.relowner == types_core::InvalidOid)
                .map(|(k, e)| (*k, std::rc::Rc::clone(&e.rel)))
        });
        let Some((relid, rel)) = target else {
            return Ok(());
        };
        let index_ok = crate::criticalRelcachesBuilt();
        let scanned = relcache_build_seams::scan_pg_relation::call(relid, index_ok, false)?
            .ok_or_else(|| cache_lookup_failed(relid))?;
        debug_assert_eq!(rel.rd_att.tdtypeid, scanned.form.reltype);
        debug_assert_eq!(rel.rd_att.tdtypmod, -1);
        if scanned.form.relowner == types_core::InvalidOid {
            return Err(Box::new(
                PgError::error(format!(
                    "invalid relowner in pg_class entry for \"{}\"",
                    rel.name()
                ))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
            ));
        }
        // formrdesc set up rd_att correctly by construction (C asserts, never
        // copies it: catcache entries may already share it).
        let newrel = std::rc::Rc::new(types_rel::RelationData {
            rd_id: relid,
            rd_backend: rel.rd_backend,
            rd_islocaltemp: rel.rd_islocaltemp,
            rd_isvalid: core::cell::Cell::new(true),
            rd_createSubid: core::cell::Cell::new(types_core::InvalidSubTransactionId),
            rd_newRelfilelocatorSubid: core::cell::Cell::new(types_core::InvalidSubTransactionId),
            rd_firstRelfilelocatorSubid: core::cell::Cell::new(types_core::InvalidSubTransactionId),
            rd_droppedSubid: core::cell::Cell::new(types_core::InvalidSubTransactionId),
            rd_lockInfo: lmgr::RelationInitLockInfo(relid, scanned.form.relisshared),
            rd_rel: scanned.form,
            rd_att: std::rc::Rc::clone(&rel.rd_att),
            rd_index: None,
            rd_opcintype: mcx::PgVec::new_in(crate::cache_mcx()),
            rd_opfamily: mcx::PgVec::new_in(crate::cache_mcx()),
            rd_indoption: mcx::PgVec::new_in(crate::cache_mcx()),
            rd_indcollation: mcx::PgVec::new_in(crate::cache_mcx()),
            rd_options: scanned.options,
            pgstat_enabled: core::cell::Cell::new(rel.pgstat_enabled.get()),
            rd_amcache: Default::default(),
            rd_supportinfo: Default::default(),
        });
        with_state(|st| {
            if let Some(ent) = st.id_cache.get_mut(&relid) {
                ent.rel = std::rc::Rc::clone(&newrel);
            }
        });
    }
}

#[cold]
#[inline(never)]
fn cache_lookup_failed(relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cache lookup failed for relation {relid}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

fn load_critical_index(indexoid: Oid, heapoid: Oid) -> PgResult<()> {
    // Catalog before index, or deadlock against exclusive lockers.
    lmgr::LockRelationOid(heapoid, AccessShareLock)?;
    lmgr::LockRelationOid(indexoid, AccessShareLock)?;
    let ird = build::RelationBuildDesc(indexoid, true)?;
    if ird.is_none() {
        return Err(Box::new(
            PgError::error(format!("could not open critical system index {indexoid}"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        ));
    }
    // C: rd_isnailed = true, rd_refcnt = 1 (the nail is the flag here).
    with_state(|st| {
        if let Some(ent) = st.id_cache.get_mut(&indexoid) {
            ent.nailed = true;
        }
    });
    lmgr::UnlockRelationOid(indexoid, AccessShareLock)?;
    lmgr::UnlockRelationOid(heapoid, AccessShareLock)?;
    // C also pre-warms RelationGetIndexAttOptions (derived-data unit).
    Ok(())
}

fn init_file_path(shared: bool) -> Option<PathBuf> {
    if shared {
        Some(Path::new("global").join(RELCACHE_INIT_FILENAME))
    } else {
        init_small::globals::DatabasePath()
            .map(|p| Path::new(p).join(RELCACHE_INIT_FILENAME))
    }
}

// Shape of load_relcache_init_file: locate + magic-validate. The per-entry
// decode is a named deferred surface (needs the entry codec); any file,
// including a valid one, takes C's read_failed recovery — rebuild from the
// catalogs. Startup-only cost, never a correctness divergence.
fn load_relcache_init_file(shared: bool) -> PgResult<bool> {
    let Some(path) = init_file_path(shared) else {
        return Ok(false);
    };
    let Ok(mut file) = fs::File::open(&path) else {
        return Ok(false);
    };
    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_err() || i32::from_ne_bytes(magic) != RELCACHE_INIT_FILEMAGIC
    {
        return Ok(false);
    }
    Ok(false)
}

// Shape of write_relcache_init_file: C's skip guard; the entry codec (and the
// InitCatalogCachePhase2 pre-warm) is a named deferred surface, so no file is
// ever produced and load always rebuilds.
fn write_relcache_init_file(shared: bool) -> PgResult<()> {
    let skip = with_state(|st| st.invals_received != 0);
    if skip {
        return Ok(());
    }
    let _ = init_file_path(shared);
    Ok(())
}

pub fn RelationIdIsInInitFile(relationId: Oid) -> bool {
    if relationId == CAT_PG_SHSECLABEL.relid
        || relationId == TRIGGER_RELID_NAME_INDEX_ID
        || relationId == DATABASE_NAME_INDEX_ID
        || relationId == SHARED_SEC_LABEL_OBJECT_INDEX_ID
    {
        // Init-file members without syscache support (C asserts the same).
        debug_assert!(!syscache_seams::relation_has_sys_cache::call(relationId));
        return true;
    }
    syscache_seams::relation_has_sys_cache::call(relationId)
}

fn unlink_initfile(path: &Path, error_level: bool) -> PgResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) if error_level => Err(Box::new(
            PgError::error(format!("could not remove cache file \"{}\": {e}", path.display()))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        )),
        Err(_) => Ok(()),
    }
}

// Serializes against write_relcache_init_file via RelCacheInitLock: unlink
// under the lock, send SI between Pre and Post, release in Post.
pub fn RelationCacheInitFilePreInvalidate() -> PgResult<()> {
    let lock = lwlock::main_lock(RELCACHE_INIT_LOCK_OFFSET);
    lwlock::LWLockAcquire(lock, lwlock::LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    if let Some(db) = init_small::globals::DatabasePath() {
        unlink_initfile(&Path::new(db).join(RELCACHE_INIT_FILENAME), true)?;
    }
    unlink_initfile(&Path::new("global").join(RELCACHE_INIT_FILENAME), true)
}

pub fn RelationCacheInitFilePostInvalidate() -> PgResult<()> {
    lwlock::LWLockRelease(lwlock::main_lock(RELCACHE_INIT_LOCK_OFFSET))
}

// Startup removal: init files may be stale after crash recovery / PITR.
pub fn RelationCacheInitFileRemove() {
    let _ = unlink_initfile(&Path::new("global").join(RELCACHE_INIT_FILENAME), false);
    remove_in_dir(Path::new("base"));
    if let Ok(entries) = fs::read_dir(PG_TBLSPC_DIR) {
        for de in entries.flatten() {
            if de.file_name().to_string_lossy().bytes().all(|b| b.is_ascii_digit()) {
                remove_in_dir(&de.path().join(TABLESPACE_VERSION_DIRECTORY));
            }
        }
    }
}

fn remove_in_dir(tblspc: &Path) {
    let Ok(entries) = fs::read_dir(tblspc) else {
        return;
    };
    for de in entries.flatten() {
        if de.file_name().to_string_lossy().bytes().all(|b| b.is_ascii_digit()) {
            let _ = unlink_initfile(&de.path().join(RELCACHE_INIT_FILENAME), false);
        }
    }
}
