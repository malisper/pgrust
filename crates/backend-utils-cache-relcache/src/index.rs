//! index family — index/table access-method initialization (OWN logic).
//!
//! Faithful port of relcache.c's index-AM-init tree over the REAL owned entry
//! store (`crate::core_entry_store::entry::RelationData`). Only genuine
//! cross-unit catalog primitives are routed through their owner seams (panic
//! until the owner lands): the `SearchSysCache1` reads (INDEXRELID/AMOID), the
//! index/table AM handler calls (`GetIndexAmRoutine`/`GetTableAmRoutine`), the
//! `RelationMapOidToFilenumber` relation-map read, and the ambient-global reads
//! (`HistoricSnapshotActive`/`IsTransactionState`/`IsCatalogRelation`/
//! `wal_level`/`IsParallelWorker`/`MyDatabaseId`/`MyDatabaseTableSpace`/
//! `RelFileLocatorSkippingWAL`). The `pg_opclass`/`pg_amproc` scans
//! `LookupOpclassInfo` runs cross the syscache owner; the per-opclass cache it
//! maintains (`OpClassCache`) is relcache's OWN in-crate dynahash.

use std::cell::RefCell;

use backend_utils_error::{ereport, PgResult};
use types_error::ERROR;

use backend_utils_hash_dynahash::{hash_create, hash_search};
use types_hash::hsearch::{HASHACTION, HASHCTL, HASH_BLOBS, HASH_ELEM, HTAB};

use mcx::MemoryContext;
use types_core::primitive::{Oid, RegProcedure};
use types_core::{InvalidOid, InvalidSubTransactionId, TopSubTransactionId};
use types_wal::xlog_consts::{WAL_LEVEL_LOGICAL, WAL_LEVEL_REPLICA};

use backend_access_index_amapi_seams as amapi_seam;
use backend_access_index_genam_seams as genam_seam;
use backend_access_table_tableam_seams as tableam_seam;
use backend_access_transam_parallel_seams as parallel_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_catalog_catalog_seams as catalog_seam;
use backend_catalog_storage_seams as storage_seam;
use backend_utils_cache_relmapper_seams as relmapper_seam;
use backend_utils_cache_syscache_seams as syscache_seam;
use backend_utils_init_small_seams as init_seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;

use crate::core_entry_store::entry::RelationData;

/* ==========================================================================
 * Catalog OIDs / relkind constants the index family branches on. Values
 * verified against the c2rust translation of relcache.c.
 * ======================================================================== */

/// `ClassOidIndexId` — `pg_class_oid_index`.
const CLASS_OID_INDEX_ID: Oid = 2662;
/// `AttributeRelidNumIndexId` — `pg_attribute_relid_attnum_index`.
#[allow(dead_code)]
const ATTRIBUTE_RELID_NUM_INDEX_ID: Oid = 2659;
/// `OID_BTREE_OPS_OID` / `INT2_BTREE_OPS_OID` (`pg_opclass`).
const OID_BTREE_OPS_OID: Oid = 1981;
const INT2_BTREE_OPS_OID: Oid = 1979;
/// `HEAP_TABLE_AM_OID` / `F_HEAP_TABLEAM_HANDLER`.
const HEAP_TABLE_AM_OID: Oid = 2;
const F_HEAP_TABLEAM_HANDLER: Oid = 3;
/// `GLOBALTABLESPACE_OID` (`pg_tablespace`).
const GLOBALTABLESPACE_OID: Oid = 1664;

const RELKIND_RELATION: i8 = b'r' as i8;
const RELKIND_INDEX: i8 = b'i' as i8;
const RELKIND_SEQUENCE: i8 = b'S' as i8;
const RELKIND_TOASTVALUE: i8 = b't' as i8;
const RELKIND_MATVIEW: i8 = b'm' as i8;
const RELKIND_PARTITIONED_INDEX: i8 = b'I' as i8;
const RELPERSISTENCE_PERMANENT: i8 = b'p' as i8;

/// `RELKIND_HAS_STORAGE(relkind)` (`pg_class.h`).
#[inline]
fn relkind_has_storage(relkind: i8) -> bool {
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}

/* ==========================================================================
 * `OpClassCache` — relcache's OWN per-(opclass) support-proc cache.
 *
 * The C `OpClassCacheEnt` is `{ Oid opclassoid; StrategyNumber numSupport;
 * bool valid; Oid opcfamily; Oid opcintype; RegProcedure *supportProcs; }`
 * held in a `HASH_BLOBS` dynahash keyed by `opclassoid`. We mirror it
 * `#[repr(C)]` so the dynahash byte-copy of the key lands on `opclassoid` at
 * offset 0, and carry `supportProcs` as a raw pointer into a leaked, never-
 * freed `Box<[RegProcedure]>` (the C array is `MemoryContextAllocZero`d in
 * `CacheMemoryContext` and never freed — there is no cache flush, per the C
 * comment).
 * ======================================================================== */

/// `OpClassCacheEnt` (relcache.c): the per-opclass cached support-proc OID
/// array + opfamily/opcintype + a valid mask. Held in the relcache-owned
/// `OpClassCache` dynahash. **Own logic.**
#[repr(C)]
pub struct OpClassCacheEnt {
    /// `Oid opclassoid` — the hash key (offset 0; `keysize == sizeof(Oid)`).
    pub opclassoid: Oid,
    /// `StrategyNumber numSupport`.
    pub numSupport: u16,
    /// `bool valid`.
    pub valid: bool,
    /// `Oid opcfamily`.
    pub opcfamily: Oid,
    /// `Oid opcintype`.
    pub opcintype: Oid,
    /// `RegProcedure *supportProcs` — `numSupport` entries, or null.
    pub supportProcs: *mut RegProcedure,
}

thread_local! {
    /// `OpClassCache` (relcache.c file-static): null until first use, then a
    /// `HASH_BLOBS` dynahash keyed by opclass OID.
    static OPCLASS_CACHE: RefCell<*mut HTAB> = const { RefCell::new(std::ptr::null_mut()) };
}

fn blank_hashctl() -> HASHCTL {
    HASHCTL {
        num_partitions: 0,
        ssize: 0,
        dsize: 0,
        max_dsize: 0,
        keysize: 0,
        entrysize: 0,
        hash: None,
        match_: None,
        keycopy: None,
        alloc: None,
        hcxt: std::ptr::null_mut(),
        hctl: std::ptr::null_mut(),
    }
}

/* ==========================================================================
 * RelationInitPhysicalAddr (relcache.c)
 * ======================================================================== */

/// `RelationInitPhysicalAddr(relation)` (relcache.c): compute `rd_locator`
/// from `pg_class.reltablespace`/`relfilenode` (or the relation map for mapped
/// relations). **Own logic** (relation-map read + historic-decoding rewrite
/// fixup are seamed).
pub fn RelationInitPhysicalAddr(rd: &mut RelationData) -> PgResult<()> {
    let oldnumber = rd.rd_locator.relNumber;

    // these relations kinds never have storage
    if !relkind_has_storage(rd.rd_rel.relkind) {
        return Ok(());
    }

    if rd.rd_rel.reltablespace != InvalidOid {
        rd.rd_locator.spcOid = rd.rd_rel.reltablespace;
    } else {
        rd.rd_locator.spcOid = init_seam::my_database_table_space::call();
    }
    if rd.rd_locator.spcOid == GLOBALTABLESPACE_OID {
        rd.rd_locator.dbOid = InvalidOid;
    } else {
        rd.rd_locator.dbOid = init_seam::my_database_id::call();
    }

    if rd.rd_rel.relfilenode != InvalidOid {
        // RelationIsAccessibleInLogicalDecoding(relation):
        //   XLogLogicalInfoActive() && RelationNeedsWAL(relation) &&
        //   (IsCatalogRelation(relation) || RelationIsUsedAsCatalogTable(relation))
        // expanded exactly as the C macros (see utils/rel.h).
        let wal = backend_access_transam_xlog_seams::wal_level::call();
        let xlog_logical_info_active = wal >= WAL_LEVEL_LOGICAL;
        let relation_needs_wal = rd.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT
            && (wal >= WAL_LEVEL_REPLICA
                || (rd.rd_createSubid == InvalidSubTransactionId
                    && rd.rd_firstRelfilelocatorSubid == InvalidSubTransactionId));
        let used_as_catalog_table = rd.rd_options.as_ref().is_some_and(|o| {
            (rd.rd_rel.relkind == RELKIND_RELATION || rd.rd_rel.relkind == RELKIND_MATVIEW)
                && o.user_catalog_table
        });
        let accessible_in_logical_decoding = xlog_logical_info_active
            && relation_needs_wal
            && (catalog_seam::is_catalog_relation_oid::call(rd.rd_id) || used_as_catalog_table);

        if snapmgr_seam::historic_snapshot_active::call()
            && accessible_in_logical_decoding
            && xact_seam::is_transaction_state::call()
        {
            // Re-read pg_class to pick up the current relfilenode for a
            // logical-decoding catalog rewrite. The pg_class scan
            // (`ScanPgRelation`) is the build family's seamed catalog primitive.
            let indexok = rd.rd_id != CLASS_OID_INDEX_ID;
            let phys = crate::build::ScanPgRelation(rd.rd_id, indexok, true)?;
            match phys {
                Some((physrel, _reloptions)) => {
                    rd.rd_rel.reltablespace = physrel.reltablespace;
                    rd.rd_rel.relfilenode = physrel.relfilenode;
                }
                None => {
                    return Err(ereport(ERROR)
                        .errmsg_internal(format!(
                            "could not find pg_class entry for {}",
                            rd.rd_id
                        ))
                        .into_error());
                }
            }
        }

        rd.rd_locator.relNumber = rd.rd_rel.relfilenode;
    } else {
        // Consult the relation mapper
        rd.rd_locator.relNumber =
            relmapper_seam::relation_map_oid_to_filenumber::call(rd.rd_id, rd.rd_rel.relisshared)?;
        if rd.rd_locator.relNumber == InvalidOid {
            return Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "could not find relation mapping for relation \"{}\", OID {}",
                    rd.rd_rel.relname, rd.rd_id
                ))
                .into_error());
        }
    }

    // For RelationNeedsWAL() to answer correctly on parallel workers, restore
    // rd_firstRelfilelocatorSubid.
    if parallel_seam::is_parallel_worker::call() && oldnumber != rd.rd_locator.relNumber {
        if storage_seam::rel_file_locator_skipping_wal::call(rd.rd_locator) {
            rd.rd_firstRelfilelocatorSubid = TopSubTransactionId;
        } else {
            rd.rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
        }
    }

    Ok(())
}

/* ==========================================================================
 * InitIndexAmRoutine / RelationInitIndexAccessInfo / IndexSupportInitialize
 * ======================================================================== */

/// `InitIndexAmRoutine(relation)` (relcache.c): resolve and cache the index
/// AM's `IndexAmRoutine` vtable into `rd_indam`. `relation->rd_amhandler` must
/// be valid already. The handler call is the amapi owner's seam.
pub fn InitIndexAmRoutine(rd: &mut RelationData) -> PgResult<()> {
    let routine = amapi_seam::get_index_am_routine::call(rd.rd_amhandler)?;
    rd.rd_indam = Some(routine);
    Ok(())
}

/// `RelationInitIndexAccessInfo(relation)` (relcache.c): set up an index
/// relation's `rd_index`/`rd_amhandler`/`rd_indam`/`rd_opfamily`/`rd_opcintype`
/// /`rd_support`/`rd_indoption`/`rd_indcollation` from `pg_index`/`pg_opclass`/
/// `pg_am`.
pub fn RelationInitIndexAccessInfo(rd: &mut RelationData) -> PgResult<()> {
    // A scratch context for the seam copies; the entry stores owned mirrors,
    // so this context is dropped on return (the C `rd_indexcxt` lifetime is
    // the entry's own owned `Vec`s here).
    let scratch = MemoryContext::new("index info");
    let mcx = scratch.mcx();

    // Make a copy of the pg_index entry for the index (SearchSysCache1 +
    // heap_copytuple, projected with the variable-length arrays the C reads
    // off rd_indextuple with fastgetattr).
    let idxinfo = match syscache_seam::search_pg_index_info::call(mcx, rd.rd_id)? {
        Some(info) => info,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for index {}", rd.rd_id))
                .into_error());
        }
    };

    // Fill rd_index from the projected pg_index row.
    rd.rd_index = Some(crate::core_entry_store::entry::FormPgIndex {
        indexrelid: idxinfo.indexrelid,
        indrelid: idxinfo.indrelid,
        indnatts: idxinfo.indnatts,
        indnkeyatts: idxinfo.indnkeyatts,
        indisunique: idxinfo.indisunique,
        indnullsnotdistinct: idxinfo.indnullsnotdistinct,
        indisprimary: idxinfo.indisprimary,
        indisexclusion: idxinfo.indisexclusion,
        indimmediate: idxinfo.indimmediate,
        indisclustered: idxinfo.indisclustered,
        indisvalid: idxinfo.indisvalid,
        indcheckxmin: idxinfo.indcheckxmin,
        indisready: idxinfo.indisready,
        indislive: idxinfo.indislive,
        indisreplident: idxinfo.indisreplident,
        indkey: idxinfo.indkey.to_vec(),
    });

    // Look up the index's access method, save the OID of its handler function.
    debug_assert!(rd.rd_rel.relam != InvalidOid);
    rd.rd_amhandler = match syscache_seam::search_am_handler::call(rd.rd_rel.relam)? {
        Some(h) => h,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "cache lookup failed for access method {}",
                    rd.rd_rel.relam
                ))
                .into_error());
        }
    };

    let indnatts = rd.rd_att.natts;
    if indnatts != idxinfo.indnatts as i32 {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("relnatts disagrees with indnatts for index {}", rd.rd_id))
            .into_error());
    }
    let indnkeyatts = idxinfo.indnkeyatts as i32;

    // Now we can fetch the index AM's API struct.
    InitIndexAmRoutine(rd)?;

    // Allocate arrays to hold data. Opclasses are not used for included
    // columns, so allocate them for indnkeyatts only.
    rd.rd_opfamily = vec![InvalidOid; indnkeyatts as usize];
    rd.rd_opcintype = vec![InvalidOid; indnkeyatts as usize];

    let amsupport = rd.rd_indam.as_ref().expect("rd_indam set above").amsupport;
    if amsupport > 0 {
        let nsupport = indnatts * amsupport as i32;
        rd.rd_support = vec![InvalidOid; nsupport as usize];
        rd.rd_supportinfo = (0..nsupport).map(|_| types_core::fmgr::FmgrInfo::default()).collect();
    } else {
        rd.rd_support = Vec::new();
        rd.rd_supportinfo = Vec::new();
    }

    rd.rd_indcollation = vec![InvalidOid; indnkeyatts as usize];
    rd.rd_indoption = vec![0i16; indnkeyatts as usize];

    // indcollation: copy the first indnkeyatts entries from the projected
    // oidvector (the C `memcpy(rd_indcollation, indcoll->values, ...)`).
    for i in 0..(indnkeyatts as usize) {
        rd.rd_indcollation[i] = idxinfo.indcollation[i];
    }

    // Fill the support procedure OID array + opfamily/opcintype info from the
    // opclass cache.
    IndexSupportInitialize(
        &idxinfo.indclass,
        &mut rd.rd_support,
        &mut rd.rd_opfamily,
        &mut rd.rd_opcintype,
        amsupport,
        indnkeyatts as i16,
    )?;

    // Similarly extract indoption and copy it to the cache entry.
    for i in 0..(indnkeyatts as usize) {
        rd.rd_indoption[i] = idxinfo.indoption[i];
    }

    // Force population of the AM/opclass per-column options cache (own logic
    // in the derived family; the result is discarded as in C's `(void)`).
    crate::derived::RelationGetIndexAttOptions(rd, false)?;

    // expressions, predicate, exclusion caches will be filled later
    rd.rd_exclops = Vec::new();
    rd.rd_exclprocs = Vec::new();
    rd.rd_exclstrats = Vec::new();

    Ok(())
}

/// `IndexSupportInitialize(indclass, indexSupport, opFamily, opcInType,
/// maxSupportNumber, maxAttributeNumber)` (relcache.c): fill the support-proc
/// OID array + opfamily/opcintype info from the opclass cache.
pub fn IndexSupportInitialize(
    indclass: &[Oid],
    index_support: &mut [RegProcedure],
    op_family: &mut [Oid],
    opc_intype: &mut [Oid],
    max_support_number: u16,
    max_attribute_number: i16,
) -> PgResult<()> {
    for att_index in 0..(max_attribute_number as usize) {
        if indclass[att_index] == InvalidOid {
            return Err(ereport(ERROR).errmsg_internal("bogus pg_index tuple").into_error());
        }

        // look up the info for this opclass, using a cache
        let opcentry = LookupOpclassInfo(indclass[att_index], max_support_number)?;

        // copy cached data into relcache entry
        // SAFETY: `opcentry` is a live `OpClassCache` element; its fields are
        // read in place exactly as the C dereferences the returned pointer.
        #[allow(unsafe_code)]
        let ent = unsafe { &*opcentry };
        op_family[att_index] = ent.opcfamily;
        opc_intype[att_index] = ent.opcintype;
        if max_support_number > 0 {
            let n = max_support_number as usize;
            // SAFETY: `supportProcs` points at `numSupport` initialized
            // `RegProcedure`s (allocated/zeroed when the entry was filled).
            #[allow(unsafe_code)]
            let procs = unsafe { std::slice::from_raw_parts(ent.supportProcs, n) };
            index_support[att_index * n..att_index * n + n].copy_from_slice(procs);
        }
    }
    Ok(())
}

/// `LookupOpclassInfo(operatorClassOid, numSupport)` (relcache.c): the
/// `OpClassCache` lookup/build for an opclass's default support procs. Returns
/// the cached entry pointer (stable for the backend lifetime). The
/// `pg_opclass`/`pg_amproc` reads cross the syscache owner; the cache itself is
/// relcache's OWN in-crate dynahash.
#[allow(unsafe_code)]
pub fn LookupOpclassInfo(
    operator_class_oid: Oid,
    num_support: u16,
) -> PgResult<*mut OpClassCacheEnt> {
    // First time through: initialize the opclass cache.
    let cache = OPCLASS_CACHE.with(|c| *c.borrow());
    let cache = if cache.is_null() {
        let mut ctl = blank_hashctl();
        ctl.keysize = std::mem::size_of::<Oid>();
        ctl.entrysize = std::mem::size_of::<OpClassCacheEnt>();
        let htab = hash_create("Operator class cache", 64, &ctl, HASH_ELEM | HASH_BLOBS)?;
        OPCLASS_CACHE.with(|c| *c.borrow_mut() = htab);
        htab
    } else {
        cache
    };

    let key = operator_class_oid.to_ne_bytes();
    let (entry_ptr, found) = hash_search(cache, key.as_ptr(), HASHACTION::HASH_ENTER)?;
    // SAFETY: `entry_ptr` is the dynahash element buffer sized
    // `sizeof(OpClassCacheEnt)`; we read/write the payload in place.
    let opcentry = unsafe { &mut *(entry_ptr as *mut OpClassCacheEnt) };

    if !found {
        // Initialize new entry.
        opcentry.opclassoid = operator_class_oid;
        opcentry.valid = false; // until known OK
        opcentry.numSupport = num_support;
        opcentry.supportProcs = std::ptr::null_mut(); // filled below
        opcentry.opcfamily = InvalidOid;
        opcentry.opcintype = InvalidOid;
    } else {
        debug_assert_eq!(num_support, opcentry.numSupport);
    }

    if opcentry.valid {
        return Ok(opcentry);
    }

    // Need to fill in new entry. Allocate the support-proc array unless a
    // previous attempt already did. The C array lives in CacheMemoryContext
    // and is never freed; we leak a `Box<[RegProcedure]>` to match.
    if opcentry.supportProcs.is_null() && num_support > 0 {
        let buf: Box<[RegProcedure]> = vec![InvalidOid; num_support as usize].into_boxed_slice();
        opcentry.supportProcs = Box::into_raw(buf) as *mut RegProcedure;
    }

    // To avoid infinite recursion during startup, force heap scans if we're
    // looking up info for the opclasses used by the indexes we would like to
    // reference here. (relcache.c LookupOpclassInfo: indexOK = criticalRel-
    // cachesBuilt || (operatorClassOid != OID_BTREE_OPS_OID && ...)).
    //
    // We must NOT go through the CLAOID/AMPROCNUM syscache here: that opens
    // OpclassOidIndexId/AccessMethodProcedureIndexId, whose relcache build would
    // re-enter LookupOpclassInfo and recurse during early startup. C does a
    // direct systable_beginscan, threading this gate to a heap scan until the
    // critical relcaches are nailed; we do the same via the genam scan seams.
    let index_ok = crate::core_entry_store::with_state(|st| st.critical_relcaches_built)
        || (operator_class_oid != OID_BTREE_OPS_OID && operator_class_oid != INT2_BTREE_OPS_OID);

    // We have to fetch the pg_opclass row to determine its opfamily and
    // opcintype, which are needed to look up related operators and functions.
    let opclassform = match genam_seam::scan_pg_opclass::call(operator_class_oid, index_ok)? {
        Some(f) => f,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("could not find tuple for opclass {}", operator_class_oid))
                .into_error());
        }
    };
    opcentry.opcfamily = opclassform.opcfamily;
    opcentry.opcintype = opclassform.opcintype;

    // Scan pg_amproc to obtain support procs for the opclass. The genam scan
    // already keys on amproclefttype = amprocrighttype = opcintype, so it
    // returns only the default support procs.
    if num_support > 0 {
        let members =
            genam_seam::scan_pg_amproc::call(opcentry.opcfamily, opcentry.opcintype, index_ok)?;
        // SAFETY: supportProcs points at num_support initialized entries.
        let procs =
            unsafe { std::slice::from_raw_parts_mut(opcentry.supportProcs, num_support as usize) };
        for am in members.iter() {
            if am.amprocnum <= 0 || am.amprocnum as u16 > num_support {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!(
                        "invalid amproc number {} for opclass {}",
                        am.amprocnum, operator_class_oid
                    ))
                    .into_error());
            }
            procs[(am.amprocnum - 1) as usize] = am.amproc;
        }
    }

    opcentry.valid = true;
    Ok(opcentry)
}

/* ==========================================================================
 * InitTableAmRoutine / RelationInitTableAccessMethod
 * ======================================================================== */

/// `InitTableAmRoutine(relation)` (relcache.c): resolve and cache the table
/// AM's `TableAmRoutine` vtable into `rd_tableam`. `relation->rd_amhandler`
/// must be valid already.
pub fn InitTableAmRoutine(rd: &mut RelationData) -> PgResult<()> {
    rd.rd_tableam = Some(tableam_seam::get_table_am_routine::call(rd.rd_amhandler)?);
    Ok(())
}

/// `RelationInitTableAccessMethod(relation)` (relcache.c): set the relation's
/// `rd_amhandler`/`rd_tableam` for a table-like relation.
pub fn RelationInitTableAccessMethod(rd: &mut RelationData) -> PgResult<()> {
    if rd.rd_rel.relkind == RELKIND_SEQUENCE {
        // Sequences are accessed like heap tables, but it's not shown in the
        // catalog; overwrite here.
        debug_assert_eq!(rd.rd_rel.relam, InvalidOid);
        rd.rd_amhandler = F_HEAP_TABLEAM_HANDLER;
    } else if catalog_seam::is_catalog_relation_oid::call(rd.rd_id) {
        // Avoid a syscache lookup for catalog tables.
        debug_assert_eq!(rd.rd_rel.relam, HEAP_TABLE_AM_OID);
        rd.rd_amhandler = F_HEAP_TABLEAM_HANDLER;
    } else {
        // Look up the table access method, save the OID of its handler.
        debug_assert!(rd.rd_rel.relam != InvalidOid);
        rd.rd_amhandler = match syscache_seam::search_am_handler::call(rd.rd_rel.relam)? {
            Some(h) => h,
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!(
                        "cache lookup failed for access method {}",
                        rd.rd_rel.relam
                    ))
                    .into_error());
            }
        };
    }

    // Now we can fetch the table AM's API struct.
    InitTableAmRoutine(rd)
}

/* ==========================================================================
 * RelationReloadIndexInfo / RelationReloadNailed (in-place rebuild refresh)
 * ======================================================================== */

/// `RelationReloadIndexInfo(relation)` (relcache.c): refresh a non-nailed
/// index entry's `pg_class`/`pg_index` fields in place during rebuild.
pub fn RelationReloadIndexInfo(rd: &mut RelationData) -> PgResult<()> {
    // Should be called only for invalidated, live indexes.
    debug_assert!(
        (rd.rd_rel.relkind == RELKIND_INDEX || rd.rd_rel.relkind == RELKIND_PARTITIONED_INDEX)
            && !rd.rd_isvalid
            && rd.rd_droppedSubid == InvalidSubTransactionId
    );

    // If it's a shared index, we might be called before backend startup has
    // finished selecting a database. A shared index can never have schema
    // updates, so just refresh the physical relfilenumber, mark valid, return.
    let critical_built = crate::core_entry_store::with_state(|st| st.critical_relcaches_built);
    if rd.rd_rel.relisshared && !critical_built {
        RelationInitPhysicalAddr(rd)?;
        rd.rd_isvalid = true;
        return Ok(());
    }

    // Read the pg_class row. Don't try to use an indexscan of
    // pg_class_oid_index to reload the info for pg_class_oid_index.
    let index_ok = rd.rd_id != CLASS_OID_INDEX_ID;
    let (pg_class, reloptions) = match crate::build::ScanPgRelation(rd.rd_id, index_ok, false)? {
        Some(pair) => pair,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("could not find pg_class tuple for index {}", rd.rd_id))
                .into_error());
        }
    };
    // memcpy(relation->rd_rel, relp, CLASS_TUPLE_SIZE)
    rd.rd_rel = pg_class;
    // Reload reloptions in case they changed (RelationParseRelOptions is build
    // family own logic).
    crate::build::RelationParseRelOptions(rd, reloptions.as_deref())?;
    // We must recalculate physical address in case it changed.
    RelationInitPhysicalAddr(rd)?;

    // For a non-system index, re-read the bool fields of pg_index.
    if !is_system_relation(rd) {
        let scratch = MemoryContext::new("index reload");
        let mcx = scratch.mcx();
        let idxinfo = match syscache_seam::search_pg_index_info::call(mcx, rd.rd_id)? {
            Some(info) => info,
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("cache lookup failed for index {}", rd.rd_id))
                    .into_error());
            }
        };
        if let Some(idx) = rd.rd_index.as_mut() {
            // Copy all the bool fields; none of the array fields may change.
            idx.indisunique = idxinfo.indisunique;
            idx.indnullsnotdistinct = idxinfo.indnullsnotdistinct;
            idx.indisprimary = idxinfo.indisprimary;
            idx.indisexclusion = idxinfo.indisexclusion;
            idx.indimmediate = idxinfo.indimmediate;
            idx.indisclustered = idxinfo.indisclustered;
            idx.indisvalid = idxinfo.indisvalid;
            idx.indcheckxmin = idxinfo.indcheckxmin;
            idx.indisready = idxinfo.indisready;
            idx.indislive = idxinfo.indislive;
            idx.indisreplident = idxinfo.indisreplident;
            // The C copies t_data's xmin too (for indcheckxmin); the owned
            // entry doesn't carry the raw heap-tuple header, so the xmin lives
            // with the projected tuple, not separately tracked here.
        }
    }

    // Okay, now it's valid again.
    rd.rd_isvalid = true;
    Ok(())
}

/// `RelationReloadNailed(relation)` (relcache.c): refresh a nailed entry's
/// `pg_class` fields in place during rebuild.
pub fn RelationReloadNailed(rd: &mut RelationData) -> PgResult<()> {
    // Should be called only for invalidated, nailed, non-index relations.
    debug_assert!(!rd.rd_isvalid);
    debug_assert!(rd.rd_isnailed);
    debug_assert_eq!(rd.rd_rel.relkind, RELKIND_RELATION);

    // Redo RelationInitPhysicalAddr in case it is a mapped relation whose
    // mapping changed.
    RelationInitPhysicalAddr(rd)?;

    // Reload a non-index entry. We can't easily do so if relcaches aren't yet
    // built; in that case leave it invalid but usable.
    let critical_built = crate::core_entry_store::with_state(|st| st.critical_relcaches_built);
    if critical_built {
        // Mark valid before scanning, to avoid self-recursion when re-building
        // pg_class.
        rd.rd_isvalid = true;

        let (pg_class, _reloptions) = match crate::build::ScanPgRelation(rd.rd_id, true, false)? {
            Some(pair) => pair,
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("could not find pg_class tuple for {}", rd.rd_id))
                    .into_error());
            }
        };
        rd.rd_rel = pg_class;

        // Again mark valid, to protect against concurrently arriving
        // invalidations.
        rd.rd_isvalid = true;
    }

    Ok(())
}

/// `IsSystemRelation(relation)` (catalog.c, mirrored here as the index family
/// needs it for `RelationReloadIndexInfo`): a relation is "system" if it is in
/// `pg_catalog` or is a `toast` rel or a shared/bootstrap catalog. The owner
/// (`backend-catalog-catalog`) decides; we route through its `is_catalog_*`
/// seam keyed by OID (the C `IsSystemRelation` ORs `IsCatalogRelation` with the
/// toast-namespace check, both subsumed by the owner's projection).
fn is_system_relation(rd: &RelationData) -> bool {
    catalog_seam::is_catalog_relation_oid::call(rd.rd_id)
}
