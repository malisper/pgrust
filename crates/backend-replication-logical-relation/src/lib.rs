//! `relation.c` — PostgreSQL logical replication relation mapping cache.
//!
//! Faithful port of `src/backend/replication/logical/relation.c` (PG 18.3).
//!
//! Routines here map the properties of local replication target relations to
//! the properties of their remote counterpart.
//!
//! # The cache model
//!
//! C keeps two process-global `HTAB`s, `LogicalRepRelMap` (keyed by
//! `LogicalRepRelId`) and `LogicalRepPartMap` (keyed by partition `Oid`), each
//! rooted in an `AllocSetContext` under `CacheMemoryContext` so the entries
//! live for the life of the process. We model each as a `thread_local!`
//! `RefCell<Option<HashMap<..>>>` (the per-backend "global" rule, cf.
//! `backend-bootstrap-bootstrap`). Entries are `'static` because C pallocs them
//! in `CacheMemoryContext`; we obtain the `'static` cache arena through the
//! `top_memory_context()` seam — C uses `CacheMemoryContext`, and this is the
//! repo's closest-available `'static` cache arena.
//!
//! C's accessors return `LogicalRepRelMapEntry *`, a pointer into the
//! persistent table that the apply worker reads and later closes. The owned
//! model keeps the entry resident in the map (so the relcache-invalidation
//! callback can still reach it) and hands the caller an *alias* of it
//! ([`clone_entry_static`]) — the structural analog of C returning the
//! pointer. [`logicalrep_rel_close`] closes the returned alias's `localrel` and
//! clears the cached entry's `localrel` by key, mirroring C's
//! `rel->localrel = NULL`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::collections::HashMap;

use mcx::{Mcx, PgBox, PgVec};
use types_core::primitive::{
    AttrNumber, InvalidOid, Oid, OidIsValid, XLogRecPtr,
};
use types_storage::lock::LOCKMODE;
use types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};

use types_nodes::Bitmapset;
use types_rel::Relation;
use types_tuple::attmap::AttrMap;

use backend_replication_logical_proto::{LogicalRepRelation, LogicalRepRelId};

// Merged dependencies (called directly).
use backend_nodes_core::bitmapset as bms;
use backend_nodes_core::makefuncs::make_range_var;
use backend_access_table_table::{table_close, table_open, try_table_open};
use backend_access_index_indexam::{index_close, index_open};
use backend_utils_cache_relcache::derived::{
    IndexAttrBitmapKind, RelationGetIndexAttrBitmap, RelationGetIndexList,
    RelationGetPrimaryKeyIndex, RelationGetReplicaIndex,
};
use backend_utils_cache_lsyscache::opclass::get_opclass_family;

// Outward seams into not-yet-ported owners.
use backend_access_common_next_seams as next_sx;
use backend_catalog_pg_subscription_seams as sub_sx;
use backend_catalog_namespace_seams as nsp_sx;
use backend_utils_cache_syscache_seams as syscache_sx;
use backend_utils_cache_typcache_seams as typcache_sx;
use backend_access_index_amapi_seams as amapi_sx;
use backend_utils_cache_inval_seams as inval_sx;
use backend_utils_mmgr_mcxt_seams as cachectx_sx;
use backend_replication_logical_worker_seams as worker_sx;

// ---------------------------------------------------------------------------
// Constants mirrored from PG headers (used only here).
// ---------------------------------------------------------------------------

/// `SUBREL_STATE_READY` (`catalog/pg_subscription_rel.h`): ready, `sublsn` set.
const SUBREL_STATE_READY: u8 = b'r';

/// `REPLICA_IDENTITY_FULL` (`catalog/pg_class.h`).
const REPLICA_IDENTITY_FULL: u8 = b'f';

/// `RELKIND_PARTITIONED_TABLE` (`catalog/pg_class.h`).
const RELKIND_PARTITIONED_TABLE: u8 = b'p';

/// `AccessShareLock` / `NoLock` (`storage/lockdefs.h`).
const AccessShareLock: LOCKMODE = 1;
const NoLock: LOCKMODE = 0;

/// `FirstLowInvalidHeapAttributeNumber` (`access/sysattr.h`) = -7.
const FirstLowInvalidHeapAttributeNumber: AttrNumber = -7;

/// `COMPARE_EQ` (`access/cmptype.h`).
const COMPARE_EQ: i32 = 3;

/// `InvalidStrategy` (`access/stratnum.h`).
const InvalidStrategy: i16 = 0;

/// `AttrNumberIsForUserDefinedAttr(attnum)` — `((attnum) > 0)`.
#[inline]
fn AttrNumberIsForUserDefinedAttr(attnum: i32) -> bool {
    attnum > 0
}

/// `AttrNumberGetAttrOffset(attnum)` — `((attnum) - 1)`.
#[inline]
fn AttrNumberGetAttrOffset(attnum: i32) -> i32 {
    attnum - 1
}

/// `AttributeNumberIsValid(attno)` — `((bool) ((attno) != InvalidAttrNumber))`,
/// `InvalidAttrNumber == 0`.
#[inline]
fn AttributeNumberIsValid(attno: AttrNumber) -> bool {
    attno != 0
}

// ---------------------------------------------------------------------------
// Cache entry types (replication/logicalrelation.h).
// ---------------------------------------------------------------------------

/// `LogicalRepRelMapEntry` (`replication/logicalrelation.h`): one mapped
/// relation, the key being `remoterel.remoteid`.
pub struct LogicalRepRelMapEntry<'mcx> {
    /// `LogicalRepRelation remoterel` — key is `remoterel.remoteid`.
    pub remoterel: LogicalRepRelation<'mcx>,

    /// Validity flag — when false, revalidate all derived info at the next
    /// `logicalrep_rel_open`.
    pub localrelvalid: bool,

    /// `Oid localreloid` — local relation id.
    pub localreloid: Oid,
    /// `Relation localrel` — relcache entry (`None` when closed).
    pub localrel: Option<Relation<'mcx>>,
    /// `AttrMap *attrmap` — map of local attributes to remote ones.
    pub attrmap: Option<AttrMap<'mcx>>,
    /// `bool updatable` — can apply updates/deletes?
    pub updatable: bool,
    /// `Oid localindexoid` — which index to use, or `InvalidOid` if none.
    pub localindexoid: Oid,

    /// `char state` — sync state.
    pub state: u8,
    /// `XLogRecPtr statelsn`.
    pub statelsn: XLogRecPtr,
}

/// `LogicalRepPartMapEntry` (relation.c): partition-keyed wrapper.
struct LogicalRepPartMapEntry<'mcx> {
    /// `Oid partoid` — `LogicalRepPartMap`'s key.
    #[allow(dead_code)]
    partoid: Oid,
    /// `LogicalRepRelMapEntry relmapentry`.
    relmapentry: LogicalRepRelMapEntry<'mcx>,
}

// ---------------------------------------------------------------------------
// Process-global caches.
//
// C: `static HTAB *LogicalRepRelMap` / `static HTAB *LogicalRepPartMap`,
// rooted in `LogicalRepRelMapContext` / `LogicalRepPartMapContext` under
// `CacheMemoryContext`. Modeled as per-backend thread-local maps holding the
// `'static` cache-arena entries.
// ---------------------------------------------------------------------------

thread_local! {
    /// `LogicalRepRelMap` (keyed by `LogicalRepRelId`). `None` == C `NULL`
    /// (uninitialized).
    static LOGICAL_REP_REL_MAP: RefCell<Option<HashMap<LogicalRepRelId, LogicalRepRelMapEntry<'static>>>> =
        const { RefCell::new(None) };

    /// `LogicalRepPartMap` (keyed by partition `Oid`). `None` == C `NULL`.
    static LOGICAL_REP_PART_MAP: RefCell<Option<HashMap<Oid, LogicalRepPartMapEntry<'static>>>> =
        const { RefCell::new(None) };
}

/// The `'static` cache arena (C: `CacheMemoryContext`). Entries pallocked here
/// live for the life of the process, like the C `HTAB` contents.
fn cache_mcx() -> Mcx<'static> {
    cachectx_sx::top_memory_context::call()
}

// ---------------------------------------------------------------------------
// Helpers: deep copies into the cache arena.
// ---------------------------------------------------------------------------

/// Copy a `&str` (the C `char *` payload) into a fresh `PgVec<u8>` in `mcx`,
/// mirroring `pstrdup` for the bytes the protocol stored.
fn pstrdup_bytes<'m>(mcx: Mcx<'m>, src: &[u8]) -> PgResult<PgVec<'m, u8>> {
    mcx::slice_in(mcx, src)
}

/// Copy a `LogicalRepRelation` into the cache arena (`pstrdup` of names,
/// `palloc` of the attname/atttyp arrays, `bms_copy` of attkeys), mirroring the
/// `MemoryContextSwitchTo(LogicalRepRelMapContext)` block of
/// `logicalrep_relmap_update`.
fn copy_remoterel<'m>(
    mcx: Mcx<'m>,
    src: &LogicalRepRelation<'_>,
) -> PgResult<LogicalRepRelation<'m>> {
    let natts = src.natts;

    let nspname = pstrdup_bytes(mcx, src.nspname.as_slice())?;
    let relname = pstrdup_bytes(mcx, src.relname.as_slice())?;

    let mut attnames: PgVec<'m, PgVec<'m, u8>> =
        mcx::vec_with_capacity_in(mcx, natts as usize)?;
    let mut atttyps: PgVec<'m, Oid> = mcx::vec_with_capacity_in(mcx, natts as usize)?;
    for i in 0..natts as usize {
        attnames.push(pstrdup_bytes(mcx, src.attnames[i].as_slice())?);
        atttyps.push(src.atttyps[i]);
    }

    let attkeys = bms::bms_copy(mcx, src.attkeys.as_deref())?;

    Ok(LogicalRepRelation {
        remoteid: src.remoteid,
        nspname,
        relname,
        natts,
        attnames,
        atttyps,
        replident: src.replident,
        relkind: src.relkind,
        attkeys,
    })
}

/// Deep-copy an [`AttrMap`] into `mcx` (mirrors the `make_attrmap` +
/// `memcpy(attnums)` "hard way" in `logicalrep_partition_open`).
fn copy_attrmap<'m>(mcx: Mcx<'m>, src: &AttrMap<'_>) -> PgResult<AttrMap<'m>> {
    Ok(AttrMap {
        attnums: mcx::slice_in(mcx, src.attnums.as_slice())?,
    })
}

/// Produce the alias of a cached entry that the C functions return as the
/// `LogicalRepRelMapEntry *` into the persistent table. The localrel is aliased
/// (a second live `RelationData *`, as C aliasing bumps `rd_refcnt`); the rest
/// is deep-copied out of the `'static` arena.
fn clone_entry_static(
    mcx: Mcx<'static>,
    e: &LogicalRepRelMapEntry<'static>,
) -> PgResult<LogicalRepRelMapEntry<'static>> {
    Ok(LogicalRepRelMapEntry {
        remoterel: copy_remoterel(mcx, &e.remoterel)?,
        localrelvalid: e.localrelvalid,
        localreloid: e.localreloid,
        localrel: e.localrel.as_ref().map(|r| r.alias()),
        attrmap: match &e.attrmap {
            Some(m) => Some(copy_attrmap(mcx, m)?),
            None => None,
        },
        updatable: e.updatable,
        localindexoid: e.localindexoid,
        state: e.state,
        statelsn: e.statelsn,
    })
}

/// Clone a `'static` cache entry into an arbitrary (shorter) lifetime `'m`,
/// WITHOUT carrying its `localrel` (the partition path overlays `partrel`).
/// `mcx` is the `'static` cache arena, whose allocations outlive `'m`.
fn clone_entry_to<'m>(
    mcx: Mcx<'static>,
    e: &LogicalRepRelMapEntry<'static>,
) -> PgResult<LogicalRepRelMapEntry<'m>> {
    Ok(LogicalRepRelMapEntry {
        remoterel: copy_remoterel(mcx, &e.remoterel)?,
        localrelvalid: e.localrelvalid,
        localreloid: e.localreloid,
        localrel: None,
        attrmap: match &e.attrmap {
            Some(m) => Some(copy_attrmap(mcx, m)?),
            None => None,
        },
        updatable: e.updatable,
        localindexoid: e.localindexoid,
        state: e.state,
        statelsn: e.statelsn,
    })
}

// ---------------------------------------------------------------------------
// Relcache invalidation callbacks.
// ---------------------------------------------------------------------------

/// `logicalrep_relmap_invalidate_cb(arg, reloid)` (relation.c): the relcache
/// invalidation callback for the relation map cache.
fn logicalrep_relmap_invalidate_cb(_arg: types_datum::Datum, reloid: Oid) {
    LOGICAL_REP_REL_MAP.with(|cell| {
        let mut guard = cell.borrow_mut();
        /* Just to be sure. */
        let map = match guard.as_mut() {
            Some(m) => m,
            None => return,
        };

        if reloid != InvalidOid {
            /* TODO, use inverse lookup hashtable? */
            for entry in map.values_mut() {
                if entry.localreloid == reloid {
                    entry.localrelvalid = false;
                    break;
                }
            }
        } else {
            /* invalidate all cache entries */
            for entry in map.values_mut() {
                entry.localrelvalid = false;
            }
        }
    });
}

/// `logicalrep_partmap_invalidate_cb(arg, reloid)` (relation.c).
fn logicalrep_partmap_invalidate_cb(_arg: types_datum::Datum, reloid: Oid) {
    LOGICAL_REP_PART_MAP.with(|cell| {
        let mut guard = cell.borrow_mut();
        /* Just to be sure. */
        let map = match guard.as_mut() {
            Some(m) => m,
            None => return,
        };

        if reloid != InvalidOid {
            /* TODO, use inverse lookup hashtable? */
            for entry in map.values_mut() {
                if entry.relmapentry.localreloid == reloid {
                    entry.relmapentry.localrelvalid = false;
                    break;
                }
            }
        } else {
            /* invalidate all cache entries */
            for entry in map.values_mut() {
                entry.relmapentry.localrelvalid = false;
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Cache init.
// ---------------------------------------------------------------------------

/// `logicalrep_relmap_init(void)` (relation.c): initialize the relation map
/// cache and register the relcache invalidation callback.
fn logicalrep_relmap_init() -> PgResult<()> {
    /*
     * C creates LogicalRepRelMapContext under CacheMemoryContext and the HTAB
     * inside it. The owned model installs the thread-local map; entries are
     * pallocked in the `'static` cache arena (cache_mcx).
     */
    LOGICAL_REP_REL_MAP.with(|cell| {
        let mut guard = cell.borrow_mut();
        if guard.is_none() {
            *guard = Some(HashMap::new());
        }
    });

    /* Watch for invalidation events. */
    inval_sx::cache_register_relcache_callback::call(
        logicalrep_relmap_invalidate_cb,
        types_datum::Datum::null(),
    )?;

    Ok(())
}

/// `logicalrep_partmap_init(void)` (relation.c): initialize the partition map
/// cache and register its relcache invalidation callback.
fn logicalrep_partmap_init() -> PgResult<()> {
    LOGICAL_REP_PART_MAP.with(|cell| {
        let mut guard = cell.borrow_mut();
        if guard.is_none() {
            *guard = Some(HashMap::new());
        }
    });

    /* Watch for invalidation events. */
    inval_sx::cache_register_relcache_callback::call(
        logicalrep_partmap_invalidate_cb,
        types_datum::Datum::null(),
    )?;

    Ok(())
}

/// `true` if `LogicalRepRelMap == NULL`.
fn rel_map_is_null() -> bool {
    LOGICAL_REP_REL_MAP.with(|c| c.borrow().is_none())
}

/// `true` if `LogicalRepPartMap == NULL`.
fn part_map_is_null() -> bool {
    LOGICAL_REP_PART_MAP.with(|c| c.borrow().is_none())
}

// ---------------------------------------------------------------------------
// logicalrep_relmap_update.
// ---------------------------------------------------------------------------

/// `logicalrep_relmap_update(remoterel)` (relation.c): add a new entry or
/// update an existing entry in the relation map cache.
///
/// Called when a new relation mapping is sent by the publisher.
pub fn logicalrep_relmap_update(remoterel: &LogicalRepRelation<'_>) -> PgResult<()> {
    if rel_map_is_null() {
        logicalrep_relmap_init()?;
    }

    let mcx = cache_mcx();

    /*
     * HASH_ENTER returns the existing entry if present or creates a new one. If
     * found, the old entry is freed; the owned model just overwrites (its
     * storage drops, mirroring logicalrep_relmap_free_entry's pfree/bms_free).
     *
     * Make cached copy of the data in LogicalRepRelMapContext (cache_mcx).
     */
    let new_remoterel = copy_remoterel(mcx, remoterel)?;

    LOGICAL_REP_REL_MAP.with(|cell| -> PgResult<()> {
        let mut guard = cell.borrow_mut();
        let map = guard.as_mut().expect("relmap initialized above");

        let entry = LogicalRepRelMapEntry {
            remoterel: new_remoterel,
            localrelvalid: false,
            localreloid: InvalidOid,
            localrel: None,
            attrmap: None,
            updatable: false,
            localindexoid: InvalidOid,
            state: 0,
            statelsn: 0,
        };
        map.insert(remoterel.remoteid, entry);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------

/// `logicalrep_rel_att_by_name(remoterel, attname)` (relation.c): find the
/// attribute index in the remote relation by name, or `-1` if not found.
fn logicalrep_rel_att_by_name(remoterel: &LogicalRepRelation<'_>, attname: &[u8]) -> i32 {
    for i in 0..remoterel.natts as usize {
        if remoterel.attnames[i].as_slice() == attname {
            return i as i32;
        }
    }
    -1
}

/// `logicalrep_get_attrs_str(remoterel, atts)` (relation.c): a comma-separated
/// string of quoted attribute names for the bitmap `atts`.
fn logicalrep_get_attrs_str(
    remoterel: &LogicalRepRelation<'_>,
    atts: Option<&Bitmapset<'_>>,
) -> String {
    debug_assert!(!bms::bms_is_empty(atts));

    let mut attsbuf = String::new();
    let mut attcnt = 0;
    let mut i: i32 = -1;

    loop {
        i = bms::bms_next_member(atts, i);
        if i < 0 {
            break;
        }
        attcnt += 1;
        if attcnt > 1 {
            attsbuf.push_str(", ");
        }
        let name = String::from_utf8_lossy(remoterel.attnames[i as usize].as_slice());
        attsbuf.push('"');
        attsbuf.push_str(&name);
        attsbuf.push('"');
    }

    attsbuf
}

/// `logicalrep_report_missing_or_gen_attrs(remoterel, missingatts,
/// generatedatts)` (relation.c): raise an error if we'd replicate missing or
/// generated columns; missing is prioritized.
fn logicalrep_report_missing_or_gen_attrs(
    remoterel: &LogicalRepRelation<'_>,
    missingatts: Option<&Bitmapset<'_>>,
    generatedatts: Option<&Bitmapset<'_>>,
) -> PgResult<()> {
    let nspname = String::from_utf8_lossy(remoterel.nspname.as_slice());
    let relname = String::from_utf8_lossy(remoterel.relname.as_slice());

    if !bms::bms_is_empty(missingatts) {
        let n = bms::bms_num_members(missingatts);
        let cols = logicalrep_get_attrs_str(remoterel, missingatts);
        let word = if n == 1 { "column" } else { "columns" };
        return Err(PgError::error(format!(
            "logical replication target relation \"{nspname}.{relname}\" is missing replicated {word}: {cols}"
        ))
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    if !bms::bms_is_empty(generatedatts) {
        let n = bms::bms_num_members(generatedatts);
        let cols = logicalrep_get_attrs_str(remoterel, generatedatts);
        let word = if n == 1 { "column" } else { "columns" };
        return Err(PgError::error(format!(
            "logical replication target relation \"{nspname}.{relname}\" has incompatible generated {word}: {cols}"
        ))
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    Ok(())
}

/// `logicalrep_rel_mark_updatable(entry)` (relation.c): check whether the local
/// replica identity is sufficient to apply updates/deletes, marking the
/// entry's `updatable` flag.
///
/// `localrelid` is `entry->localrel`'s OID (`RelationGetRelid`). It is passed
/// explicitly because the partition-cache path holds the partition `Relation`
/// outside the `'static` cache slot (C stores the borrowed `partrel` pointer
/// there; the owned model keeps it in the caller-returned entry only).
fn logicalrep_rel_mark_updatable(
    entry: &mut LogicalRepRelMapEntry<'_>,
    localrelid: Oid,
) -> PgResult<()> {
    entry.updatable = true;

    /* The identity-key attribute bitmap; fall back to PK if no replica id. */
    let mut idkey = RelationGetIndexAttrBitmap(localrelid, IndexAttrBitmapKind::Identity)?;
    /* fallback to PK if no replica identity */
    if idkey.is_empty() {
        idkey = RelationGetIndexAttrBitmap(localrelid, IndexAttrBitmapKind::PrimaryKey)?;

        /*
         * If no replica identity index and no PK, the published table must have
         * replica identity FULL.
         */
        if idkey.is_empty() && entry.remoterel.replident != REPLICA_IDENTITY_FULL {
            entry.updatable = false;
        }
    }

    /*
     * RelationGetIndexAttrBitmap returns the (offset-shifted) attribute numbers
     * as a Vec; iterate it as the C `bms_next_member` loop does.
     */
    let attrmap = entry.attrmap.as_ref().expect("attrmap built in rel_open");
    for &bit in &idkey {
        let i = bit; // offset-shifted attribute number, as stored in the bitmap
        let attnum = i + FirstLowInvalidHeapAttributeNumber as i32;

        if !AttrNumberIsForUserDefinedAttr(attnum) {
            let nspname = String::from_utf8_lossy(entry.remoterel.nspname.as_slice());
            let relname = String::from_utf8_lossy(entry.remoterel.relname.as_slice());
            return Err(PgError::error(format!(
                "logical replication target relation \"{nspname}.{relname}\" uses system columns in REPLICA IDENTITY index"
            ))
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
        }

        let attoff = AttrNumberGetAttrOffset(attnum) as usize;

        let mapped = attrmap.attnums[attoff] as i32;
        if mapped < 0 || !bms::bms_is_member(mapped, entry.remoterel.attkeys.as_deref()) {
            entry.updatable = false;
            break;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// logicalrep_rel_open / logicalrep_rel_close.
// ---------------------------------------------------------------------------

/// `logicalrep_rel_open(remoteid, lockmode)` (relation.c): open the local
/// relation associated with the remote one, rebuilding the relcache mapping if
/// it was invalidated by local DDL.
pub fn logicalrep_rel_open(
    remoteid: LogicalRepRelId,
    lockmode: LOCKMODE,
) -> PgResult<LogicalRepRelMapEntry<'static>> {
    if rel_map_is_null() {
        logicalrep_relmap_init()?;
    }

    /* Search for existing entry (must exist). */
    let found = LOGICAL_REP_REL_MAP.with(|c| {
        c.borrow()
            .as_ref()
            .map(|m| m.contains_key(&remoteid))
            .unwrap_or(false)
    });
    if !found {
        return Err(PgError::error(format!(
            "no relation map entry for remote relation ID {remoteid}"
        )));
    }

    // Refresh the cached entry in place, then return its alias. All the
    // open/rebuild work below operates on the resident entry so the
    // invalidation callback can still reach it.
    rel_open_refresh(remoteid, lockmode)?;

    let mcx = cache_mcx();
    LOGICAL_REP_REL_MAP.with(|c| {
        let guard = c.borrow();
        let e = guard
            .as_ref()
            .and_then(|m| m.get(&remoteid))
            .expect("entry present after refresh");
        clone_entry_static(mcx, e)
    })
}

/// The body of `logicalrep_rel_open` that mutates the resident entry.
fn rel_open_refresh(
    remoteid: LogicalRepRelId,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let mcx = cache_mcx();

    // --- snapshot the few scalars/names we need without holding a borrow ---
    let (mut localrelvalid, localreloid, already_open, nspname, relname) =
        LOGICAL_REP_REL_MAP.with(|c| {
            let guard = c.borrow();
            let e = guard.as_ref().unwrap().get(&remoteid).unwrap();
            (
                e.localrelvalid,
                e.localreloid,
                e.localrel.is_some(),
                e.remoterel.nspname.as_slice().to_vec(),
                e.remoterel.relname.as_slice().to_vec(),
            )
        });

    /* Ensure we don't leak a relcache refcount. */
    if already_open {
        return Err(PgError::error(format!(
            "remote relation ID {remoteid} is already open"
        )));
    }

    /*
     * When opening and locking a relation, pending invalidation messages are
     * processed which can invalidate the relation. Hence, if the entry is
     * currently considered valid, try to open the local relation by OID and see
     * if invalidation ensues.
     */
    if localrelvalid {
        let opened = try_table_open(mcx, localreloid, lockmode)?;
        match opened {
            None => {
                /* Table was renamed or dropped. */
                set_localrelvalid(remoteid, false);
                localrelvalid = false;
            }
            Some(rel) => {
                // Re-read localrelvalid: opening may have run invalidation.
                let still_valid = LOGICAL_REP_REL_MAP
                    .with(|c| c.borrow().as_ref().unwrap().get(&remoteid).unwrap().localrelvalid);
                if !still_valid {
                    /* Note we release the no-longer-useful lock here. */
                    table_close(rel, lockmode)?;
                    set_localrel(remoteid, None);
                    localrelvalid = false;
                } else {
                    set_localrel(remoteid, Some(rel));
                }
            }
        }
    }

    /*
     * If the entry has been marked invalid since we last had lock on it,
     * re-open the local relation by name and rebuild all derived data.
     */
    if !localrelvalid {
        /* Release the no-longer-useful attrmap, if any. */
        let old_map = take_attrmap(remoteid);
        if let Some(m) = old_map {
            next_sx::free_attrmap::call(m);
        }

        /* Try to find and lock the relation by name. */
        let rv = make_range_var(
            Some(String::from_utf8_lossy(&nspname).into_owned()),
            String::from_utf8_lossy(&relname).into_owned(),
            -1,
        );
        let relid = nsp_sx::range_var_get_relid::call(mcx, &rv, lockmode, true)?;
        if !OidIsValid(relid) {
            let n = String::from_utf8_lossy(&nspname);
            let r = String::from_utf8_lossy(&relname);
            return Err(PgError::error(format!(
                "logical replication target relation \"{n}.{r}\" does not exist"
            ))
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
        }
        let localrel = table_open(mcx, relid, NoLock)?;

        // Read relkind + tuple-descriptor data out of the relcache cell (copy
        // scalars OUT; do not hold a borrow across relcache-touching calls).
        let relkind = localrel.rd_rel.relkind;
        let natts = localrel.rd_att.natts;
        // Per-attribute (isdropped, generated, name).
        let mut attrs: Vec<(bool, bool, Vec<u8>)> = Vec::with_capacity(natts as usize);
        for i in 0..natts as usize {
            let a = localrel.rd_att.attr(i);
            attrs.push((
                a.attisdropped,
                a.attgenerated != 0,
                a.attname.name_str().to_vec(),
            ));
        }

        set_localrel(remoteid, Some(localrel));
        set_localreloid(remoteid, relid);

        /* Check for supported relkind. */
        {
            let n = String::from_utf8_lossy(&nspname);
            let r = String::from_utf8_lossy(&relname);
            sub_sx::check_subscription_relkind::call(relkind, &n, &r)?;
        }

        /*
         * Build the mapping of local attribute numbers to remote attribute
         * numbers and validate that we don't miss any replicated columns.
         */
        let mut attrmap = next_sx::make_attrmap::call(mcx, natts)?;

        // Read remote-side data we need (natts + attnames) once.
        let (remote_natts, remote_attnames) = LOGICAL_REP_REL_MAP.with(|c| {
            let guard = c.borrow();
            let e = guard.as_ref().unwrap().get(&remoteid).unwrap();
            let names: Vec<Vec<u8>> = (0..e.remoterel.natts as usize)
                .map(|i| e.remoterel.attnames[i].as_slice().to_vec())
                .collect();
            (e.remoterel.natts, names)
        });

        /* check and report missing attrs, if any */
        let mut missingatts: Option<PgBox<'static, Bitmapset<'static>>> =
            bms::bms_add_range(mcx, None, 0, remote_natts - 1)?;
        let mut generatedattrs: Option<PgBox<'static, Bitmapset<'static>>> = None;

        for i in 0..natts as usize {
            let (isdropped, isgenerated, name) = &attrs[i];

            if *isdropped {
                attrmap.attnums[i] = -1;
                continue;
            }

            // logicalrep_rel_att_by_name over the snapshotted remote names.
            let mut attnum: i32 = -1;
            for (ri, rn) in remote_attnames.iter().enumerate() {
                if rn.as_slice() == name.as_slice() {
                    attnum = ri as i32;
                    break;
                }
            }

            attrmap.attnums[i] = attnum as AttrNumber;
            if attnum >= 0 {
                /* Remember which subscriber columns are generated. */
                if *isgenerated {
                    generatedattrs = Some(bms::bms_add_member(mcx, generatedattrs, attnum)?);
                }
                missingatts = bms::bms_del_member(missingatts, attnum);
            }
        }

        // Install the attrmap before report/mark (rel_mark_updatable reads it).
        set_attrmap(remoteid, Some(attrmap));

        // report missing/generated attrs
        LOGICAL_REP_REL_MAP.with(|c| -> PgResult<()> {
            let guard = c.borrow();
            let e = guard.as_ref().unwrap().get(&remoteid).unwrap();
            logicalrep_report_missing_or_gen_attrs(
                &e.remoterel,
                missingatts.as_deref(),
                generatedattrs.as_deref(),
            )
        })?;

        /* be tidy */
        bms::bms_free(generatedattrs);
        bms::bms_free(missingatts);

        /*
         * Set if the table's replica identity is enough to apply update/delete.
         */
        let localrelid = with_entry(remoteid, |e| e.localrel.as_ref().unwrap().rd_id);
        with_entry_mut(remoteid, |e| logicalrep_rel_mark_updatable(e, localrelid))?;

        /*
         * Finding a usable index is an infrequent task.
         */
        let localindexoid = with_entry(remoteid, |e| {
            let localrel = e.localrel.as_ref().unwrap();
            let attrmap = e.attrmap.as_ref().unwrap();
            FindLogicalRepLocalIndex(localrel, &e.remoterel, attrmap)
        })?;
        set_localindexoid(remoteid, localindexoid);

        set_localrelvalid(remoteid, true);
    }

    /* Refresh sync state if not already READY. */
    let state = LOGICAL_REP_REL_MAP.with(|c| c.borrow().as_ref().unwrap().get(&remoteid).unwrap().state);
    if state != SUBREL_STATE_READY {
        let localreloid =
            LOGICAL_REP_REL_MAP.with(|c| c.borrow().as_ref().unwrap().get(&remoteid).unwrap().localreloid);
        let subid = worker_sx::my_subscription_oid::call();
        let (newstate, statelsn) = sub_sx::get_subscription_rel_state::call(subid, localreloid)?;
        set_state_and_lsn(remoteid, newstate, statelsn);
    }

    Ok(())
}

/// `logicalrep_rel_close(rel, lockmode)` (relation.c): close a previously
/// opened logical relation.
pub fn logicalrep_rel_close(
    rel: &mut LogicalRepRelMapEntry<'_>,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let remoteid = rel.remoterel.remoteid;
    // Close the alias the caller holds.
    if let Some(localrel) = rel.localrel.take() {
        table_close(localrel, lockmode)?;
    }
    // Mirror `rel->localrel = NULL` on the cached entry too.
    set_localrel_if_present(remoteid, None);
    Ok(())
}

// ---------------------------------------------------------------------------
// logicalrep_partmap_reset_relmap / logicalrep_partition_open.
// ---------------------------------------------------------------------------

/// `logicalrep_partmap_reset_relmap(remoterel)` (relation.c): reset the entries
/// in the partition map that refer to `remoterel`.
pub fn logicalrep_partmap_reset_relmap(remoterel: &LogicalRepRelation<'_>) -> PgResult<()> {
    if part_map_is_null() {
        return Ok(());
    }

    let target = remoterel.remoteid;

    LOGICAL_REP_PART_MAP.with(|c| {
        let mut guard = c.borrow_mut();
        let map = guard.as_mut().unwrap();
        for part_entry in map.values_mut() {
            let entry = &mut part_entry.relmapentry;
            if entry.remoterel.remoteid != target {
                continue;
            }

            // logicalrep_relmap_free_entry + memset(entry, 0, ...): reset the
            // entry to the zeroed shape (its old storage drops).
            entry.remoterel = empty_remoterel();
            entry.localrelvalid = false;
            entry.localreloid = InvalidOid;
            entry.localrel = None;
            entry.attrmap = None;
            entry.updatable = false;
            entry.localindexoid = InvalidOid;
            entry.state = 0;
            entry.statelsn = 0;
        }
    });

    Ok(())
}

/// A zeroed `LogicalRepRelation` mirror of `memset(entry, 0, ...)` for the
/// `remoterel` sub-struct (its `remoteid == 0` is the "uninitialized" sentinel
/// `logicalrep_partition_open` checks).
fn empty_remoterel() -> LogicalRepRelation<'static> {
    let mcx = cache_mcx();
    LogicalRepRelation {
        remoteid: 0,
        nspname: PgVec::new_in(mcx),
        relname: PgVec::new_in(mcx),
        natts: 0,
        attnames: PgVec::new_in(mcx),
        atttyps: PgVec::new_in(mcx),
        replident: 0,
        relkind: 0,
        attkeys: None,
    }
}

/// `logicalrep_partition_open(root, partrel, map)` (relation.c): look up (or
/// build) the partition's `LogicalRepRelMapEntry`, reusing most of the root
/// table's entry save the attribute map.
pub fn logicalrep_partition_open<'mcx>(
    root: &LogicalRepRelMapEntry<'_>,
    partrel: &Relation<'mcx>,
    map: Option<&AttrMap<'_>>,
) -> PgResult<LogicalRepRelMapEntry<'mcx>> {
    let part_oid = partrel.rd_id;

    if part_map_is_null() {
        logicalrep_partmap_init()?;
    }

    /* Search for existing entry. */
    let found = LOGICAL_REP_PART_MAP.with(|c| {
        c.borrow().as_ref().map(|m| m.contains_key(&part_oid)).unwrap_or(false)
    });

    /*
     * We must always overwrite entry->localrel with the latest partition
     * Relation pointer. If found and valid, just refresh localrel and return.
     */
    if found {
        let valid =
            LOGICAL_REP_PART_MAP.with(|c| c.borrow().as_ref().unwrap().get(&part_oid).unwrap().relmapentry.localrelvalid);
        if valid {
            // entry->localrel = partrel; return entry;
            //
            // C stores the borrowed `partrel` pointer into the cache slot; the
            // owned model cannot put a non-`'static` `Relation` in the `'static`
            // cache, so the returned entry (the alias the caller holds) carries
            // `partrel`, while the cache slot stays `None`. `localrelvalid`
            // (set by the invalidation callback) remains the source of truth.
            let mcx = cache_mcx();
            return LOGICAL_REP_PART_MAP.with(|c| {
                let guard = c.borrow();
                let e = &guard.as_ref().unwrap().get(&part_oid).unwrap().relmapentry;
                let mut out = clone_entry_to(mcx, e)?;
                out.localrel = Some(partrel.alias());
                Ok(out)
            });
        }
    }

    let mcx = cache_mcx();

    /* Switch to longer-lived context (cache_mcx) for all allocations below. */

    // Ensure the entry exists (memset for the !found case).
    LOGICAL_REP_PART_MAP.with(|c| {
        let mut guard = c.borrow_mut();
        let pm = guard.as_mut().unwrap();
        if !found {
            pm.insert(
                part_oid,
                LogicalRepPartMapEntry {
                    partoid: part_oid,
                    relmapentry: LogicalRepRelMapEntry {
                        remoterel: empty_remoterel(),
                        localrelvalid: false,
                        localreloid: InvalidOid,
                        localrel: None,
                        attrmap: None,
                        updatable: false,
                        localindexoid: InvalidOid,
                        state: 0,
                        statelsn: 0,
                    },
                },
            );
        }
    });

    /* Release the no-longer-useful attrmap, if any. */
    let old = take_part_attrmap(part_oid);
    if let Some(m) = old {
        next_sx::free_attrmap::call(m);
    }

    // if (!entry->remoterel.remoteid) -> copy remoterel from root.
    let has_remoteid =
        LOGICAL_REP_PART_MAP.with(|c| c.borrow().as_ref().unwrap().get(&part_oid).unwrap().relmapentry.remoterel.remoteid != 0);
    if !has_remoteid {
        let new_remoterel = copy_remoterel(mcx, &root.remoterel)?;
        set_part_remoterel(part_oid, new_remoterel);
    }

    // C: entry->localrel = partrel (borrowed pointer). The owned model keeps
    // partrel out of the `'static` cache; it is overlaid onto the returned
    // entry below. localreloid is the partition OID.
    set_part_localreloid(part_oid, part_oid);

    /*
     * Build the partition's attrmap. 'map' carries 1-based parent attribute
     * numbers; the entry map carries 0-based remote attribute numbers.
     */
    let root_attrmap = root.attrmap.as_ref().expect("root attrmap present");
    let new_attrmap = match map {
        Some(map) => {
            let maplen = map.attnums.len();
            let mut em = next_sx::make_attrmap::call(mcx, maplen as i32)?;
            for attno in 0..maplen {
                let root_attno = map.attnums[attno];
                /* 0 means it's a dropped attribute. See comments atop AttrMap. */
                if root_attno == 0 {
                    em.attnums[attno] = -1;
                } else {
                    em.attnums[attno] = root_attrmap.attnums[(root_attno - 1) as usize];
                }
            }
            em
        }
        None => {
            /* Lacking copy_attmap, do this the hard way. */
            let maplen = root_attrmap.attnums.len();
            let mut em = next_sx::make_attrmap::call(mcx, maplen as i32)?;
            for i in 0..maplen {
                em.attnums[i] = root_attrmap.attnums[i];
            }
            em
        }
    };
    set_part_attrmap(part_oid, Some(new_attrmap));

    /* Set if the table's replica identity is enough to apply update/delete. */
    // C reads entry->localrel (== partrel); we pass partrel's OID directly.
    with_part_entry_mut(part_oid, |e| {
        logicalrep_rel_mark_updatable(e, partrel.rd_id)
    })?;

    /* state and statelsn are left set to 0. */

    /*
     * Finding a usable index is an infrequent task.
     */
    // C uses entry->localrel (== partrel); we use partrel directly.
    let localindexoid = with_part_entry(part_oid, |e| {
        let attrmap = e.attrmap.as_ref().unwrap();
        FindLogicalRepLocalIndex(partrel, &e.remoterel, attrmap)
    })?;
    set_part_localindexoid(part_oid, localindexoid);

    set_part_localrelvalid(part_oid, true);

    LOGICAL_REP_PART_MAP.with(|c| {
        let guard = c.borrow();
        let e = &guard.as_ref().unwrap().get(&part_oid).unwrap().relmapentry;
        let mut out = clone_entry_to(mcx, e)?;
        out.localrel = Some(partrel.alias());
        Ok(out)
    })
}

// ---------------------------------------------------------------------------
// Index selection.
// ---------------------------------------------------------------------------

/// `FindUsableIndexForReplicaIdentityFull(localrel, attrmap)` (relation.c):
/// return the OID of an index usable by the apply worker, or `InvalidOid`.
fn FindUsableIndexForReplicaIdentityFull(
    localrel: &Relation<'_>,
    attrmap: &AttrMap<'_>,
) -> PgResult<Oid> {
    let mcx = cache_mcx();
    let idxlist = RelationGetIndexList(localrel.rd_id)?;

    for idxoid in idxlist {
        let idx_rel = index_open(mcx, idxoid, AccessShareLock)?;
        let is_usable_idx = IsIndexUsableForReplicaIdentityFull(&idx_rel, attrmap)?;
        index_close(idx_rel, AccessShareLock)?;

        /* Return the first eligible index found */
        if is_usable_idx {
            return Ok(idxoid);
        }
    }

    Ok(InvalidOid)
}

/// `IsIndexUsableForReplicaIdentityFull(idxrel, attrmap)` (relation.c): whether
/// the index can be used for replica identity full.
pub fn IsIndexUsableForReplicaIdentityFull(
    idxrel: &Relation<'_>,
    attrmap: &AttrMap<'_>,
) -> PgResult<bool> {
    let mcx = cache_mcx();
    let idxoid = idxrel.rd_id;
    let relam = idxrel.rd_rel.relam;

    /* The index must not be a partial index. */
    let has_pred = syscache_sx::pg_index_has_predicate::call(idxoid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for index {idxoid}")))?;
    if has_pred {
        return Ok(false);
    }

    // The fixed scalars + the indkey/indclass vararrays, off rd_indextuple.
    let idxinfo = syscache_sx::search_pg_index_info::call(mcx, idxoid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for index {idxoid}")))?;

    debug_assert!(idxinfo.indnatts >= 1);

    let indclass = &idxinfo.indclass;

    /* Ensure the index has a valid equal strategy for each key column. */
    for i in 0..idxinfo.indnkeyatts as usize {
        let opfamily = get_opclass_family(indclass[i])?;
        let strat =
            amapi_sx::index_am_translate_cmptype::call(COMPARE_EQ, relam, opfamily, true)?;
        if strat == InvalidStrategy {
            return Ok(false);
        }
    }

    /*
     * For indexes other than PK and REPLICA IDENTITY we must match the local
     * and remote tuples; tuples_equal() needs an equality operator for each
     * column's type.
     */
    let natts = idxrel.rd_att.natts;
    for i in 0..natts as usize {
        let atttypid = idxrel.rd_att.attr(i).atttypid;
        let eq_fn_oid = typcache_sx::lookup_element_eq_opr::call(atttypid)?;
        if !OidIsValid(eq_fn_oid) {
            return Ok(false);
        }
    }

    /* The leftmost index field must not be an expression. */
    let keycol = idxinfo.indkey[0];
    if !AttributeNumberIsValid(keycol) {
        return Ok(false);
    }

    /*
     * And the leftmost index field must reference the remote relation column.
     */
    let keyoff = AttrNumberGetAttrOffset(keycol as i32);
    if (attrmap.attnums.len() as i32) <= keyoff || (attrmap.attnums[keyoff as usize] as i32) < 0 {
        return Ok(false);
    }

    /*
     * The given index AM must implement "amgettuple", used later to fetch the
     * tuples. See RelationFindReplTupleByIndex().
     */
    if !amapi_sx::index_am_has_gettuple::call(relam)? {
        return Ok(false);
    }

    Ok(true)
}

/// `GetRelationIdentityOrPK(rel)` (relation.c): the OID of the replica-identity
/// index if defined, else the OID of a non-deferrable PK, else `InvalidOid`.
///
/// NOTE: in PG18.3 this lives in relation.c (not execReplication.c); we expose
/// it as a normal `pub fn` here.
pub fn GetRelationIdentityOrPK(rel: &Relation<'_>) -> PgResult<Oid> {
    let mut idxoid = RelationGetReplicaIndex(rel.rd_id)?;

    if !OidIsValid(idxoid) {
        idxoid = RelationGetPrimaryKeyIndex(rel.rd_id, false)?;
    }

    Ok(idxoid)
}

/// `FindLogicalRepLocalIndex(localrel, remoterel, attrMap)` (relation.c):
/// return the index OID usable for the subscriber, or `InvalidOid`.
fn FindLogicalRepLocalIndex(
    localrel: &Relation<'_>,
    remoterel: &LogicalRepRelation<'_>,
    attr_map: &AttrMap<'_>,
) -> PgResult<Oid> {
    /*
     * We never need an index oid for partitioned tables; rely on leaf
     * partition's index.
     */
    if localrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Ok(InvalidOid);
    }

    /* Simple case: we already have a PK or replica identity index. */
    let idxoid = GetRelationIdentityOrPK(localrel)?;
    if OidIsValid(idxoid) {
        return Ok(idxoid);
    }

    if remoterel.replident == REPLICA_IDENTITY_FULL {
        /*
         * Look for one more opportunity for using an index. If any indexes are
         * defined on the local relation, try to pick a suitable index.
         */
        return FindUsableIndexForReplicaIdentityFull(localrel, attr_map);
    }

    Ok(InvalidOid)
}

// ---------------------------------------------------------------------------
// Resident-entry field mutators (RelMap).
// ---------------------------------------------------------------------------

fn with_entry<R>(remoteid: LogicalRepRelId, f: impl FnOnce(&LogicalRepRelMapEntry<'static>) -> R) -> R {
    LOGICAL_REP_REL_MAP.with(|c| {
        let guard = c.borrow();
        f(guard.as_ref().unwrap().get(&remoteid).unwrap())
    })
}

fn with_entry_mut<R>(
    remoteid: LogicalRepRelId,
    f: impl FnOnce(&mut LogicalRepRelMapEntry<'static>) -> R,
) -> R {
    LOGICAL_REP_REL_MAP.with(|c| {
        let mut guard = c.borrow_mut();
        f(guard.as_mut().unwrap().get_mut(&remoteid).unwrap())
    })
}

fn set_localrelvalid(remoteid: LogicalRepRelId, v: bool) {
    with_entry_mut(remoteid, |e| e.localrelvalid = v);
}
fn set_localrel(remoteid: LogicalRepRelId, v: Option<Relation<'static>>) {
    with_entry_mut(remoteid, |e| e.localrel = v);
}
fn set_localrel_if_present(remoteid: LogicalRepRelId, v: Option<Relation<'static>>) {
    LOGICAL_REP_REL_MAP.with(|c| {
        let mut guard = c.borrow_mut();
        if let Some(e) = guard.as_mut().and_then(|m| m.get_mut(&remoteid)) {
            e.localrel = v;
        }
    });
}
fn set_localreloid(remoteid: LogicalRepRelId, v: Oid) {
    with_entry_mut(remoteid, |e| e.localreloid = v);
}
fn set_attrmap(remoteid: LogicalRepRelId, v: Option<AttrMap<'static>>) {
    with_entry_mut(remoteid, |e| e.attrmap = v);
}
fn take_attrmap(remoteid: LogicalRepRelId) -> Option<AttrMap<'static>> {
    with_entry_mut(remoteid, |e| e.attrmap.take())
}
fn set_localindexoid(remoteid: LogicalRepRelId, v: Oid) {
    with_entry_mut(remoteid, |e| e.localindexoid = v);
}
fn set_state_and_lsn(remoteid: LogicalRepRelId, state: u8, lsn: XLogRecPtr) {
    with_entry_mut(remoteid, |e| {
        e.state = state;
        e.statelsn = lsn;
    });
}

// ---------------------------------------------------------------------------
// Resident-entry field mutators (PartMap).
// ---------------------------------------------------------------------------

fn with_part_entry<R>(part_oid: Oid, f: impl FnOnce(&LogicalRepRelMapEntry<'static>) -> R) -> R {
    LOGICAL_REP_PART_MAP.with(|c| {
        let guard = c.borrow();
        f(&guard.as_ref().unwrap().get(&part_oid).unwrap().relmapentry)
    })
}
fn with_part_entry_mut<R>(
    part_oid: Oid,
    f: impl FnOnce(&mut LogicalRepRelMapEntry<'static>) -> R,
) -> R {
    LOGICAL_REP_PART_MAP.with(|c| {
        let mut guard = c.borrow_mut();
        f(&mut guard.as_mut().unwrap().get_mut(&part_oid).unwrap().relmapentry)
    })
}
fn set_part_localreloid(part_oid: Oid, v: Oid) {
    with_part_entry_mut(part_oid, |e| e.localreloid = v);
}
fn set_part_remoterel(part_oid: Oid, v: LogicalRepRelation<'static>) {
    with_part_entry_mut(part_oid, |e| e.remoterel = v);
}
fn set_part_attrmap(part_oid: Oid, v: Option<AttrMap<'static>>) {
    with_part_entry_mut(part_oid, |e| e.attrmap = v);
}
fn take_part_attrmap(part_oid: Oid) -> Option<AttrMap<'static>> {
    with_part_entry_mut(part_oid, |e| e.attrmap.take())
}
fn set_part_localindexoid(part_oid: Oid, v: Oid) {
    with_part_entry_mut(part_oid, |e| e.localindexoid = v);
}
fn set_part_localrelvalid(part_oid: Oid, v: bool) {
    with_part_entry_mut(part_oid, |e| e.localrelvalid = v);
}

// ---------------------------------------------------------------------------
// init_seams — this crate owns no inward seam (cf. functioncmds): nothing to
// install. (Kept as a no-op for symmetry / future inward seams.)
// ---------------------------------------------------------------------------

/// No inward seams are owned by this crate. (Confirmed via the seam guard.)
pub fn init_seams() {}
