#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! `backend/utils/cache/relmapper.c` — the catalog-to-filenumber relation map.
//!
//! For most tables the physical file is named by `pg_class.relfilenode`, but
//! that cannot work for `pg_class` itself, the other nailed catalogs, or shared
//! catalogs. For these "mapped" catalogs PostgreSQL keeps a separate map file
//! (`pg_filenode.map`) per database plus one for the shared catalogs, mapping
//! catalog OIDs to filenumbers. Rewriting the map file is effectively the commit
//! of a mapped catalog relocation.
//!
//! ## State model
//!
//! relmapper.c keeps six file-static `RelMapFile` variables: the per-backend
//! cached `shared_map`/`local_map` (reloaded whenever a sinval message arrives)
//! plus the `active_*`/`pending_*` uncommitted-update maps. relmapper.c does
//! **no** `ShmemInitStruct` of its own — these are ordinary per-backend process
//! state, so they live here in a [`thread_local!`] (every backend is one thread;
//! a `SET`/reload in one session must not touch another's). The genuinely
//! cross-backend state is the on-disk map file plus `RelationMappingLock`,
//! reached through the owner seam crates.
//!
//! The CRC/magic validation, the merge/apply rules, and the control flow of
//! `read_relmap_file` / `write_relmap_file` / `perform_relmap_update` /
//! `relmap_redo` stay in this crate; only the genuinely-external primitives
//! (lock, file load/store, WAL, sinval, storage, CRC32C, process globals) go
//! through seams.

use std::cell::RefCell;

use types_core::{InvalidOid, InvalidRelFileNumber, Oid, RelFileNumber};
use types_error::{
    ErrorLocation, PgError, PgResult, SqlState, ERRCODE_DATA_CORRUPTED,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERROR, FATAL, PANIC,
};
use types_pgstat::wait_event::{
    WAIT_EVENT_RELATION_MAP_READ, WAIT_EVENT_RELATION_MAP_REPLACE, WAIT_EVENT_RELATION_MAP_WRITE,
};
use types_storage::RelFileLocator;
use types_wal::{RM_RELMAP_ID, XLR_INFO_MASK};

use backend_access_transam_xact_seams as xact_seams;
use backend_access_transam_xlog_seams as xlog_seams;
use backend_access_transam_xloginsert_seams as xloginsert_seams;
use backend_catalog_catalog_seams as catalog_seams;
use backend_catalog_storage_seams as storage_seams;
use backend_storage_file_fd_seams::{self as fd_seams, RelmapReadOutcome, RelmapWriteOutcome};
use backend_storage_lmgr_lwlock_seams as lwlock_seams;
use backend_utils_cache_inval_seams as inval_seams;
use backend_utils_error_seams as error_seams;
use backend_utils_init_miscinit_seams as miscinit_seams;
use backend_utils_init_small_seams as init_small_seams;
use backend_utils_misc_guc_seams as guc_seams;
use port_crc32c_seams as crc32c_seams;

/* ---------------------------------------------------------------------------
 * Compile-time constants from relmapper.c / pg_tablespace.h.
 * ------------------------------------------------------------------------- */

/// `RELMAPPER_FILENAME`.
const RELMAPPER_FILENAME: &str = "pg_filenode.map";

/// `RELMAPPER_TEMP_FILENAME`.
const RELMAPPER_TEMP_FILENAME: &str = "pg_filenode.map.tmp";

/// `RELMAPPER_FILEMAGIC` — version ID value.
const RELMAPPER_FILEMAGIC: i32 = 0x0059_2717;

/// `MAX_MAPPINGS`.
const MAX_MAPPINGS: usize = 64;

/// `XLOG_RELMAP_UPDATE` (utils/relmapper.h).
const XLOG_RELMAP_UPDATE: u8 = 0x00;

/// `GLOBALTABLESPACE_OID` (catalog/pg_tablespace.dat) — pg_global.
const GLOBALTABLESPACE_OID: Oid = 1664;

/// `ENOSPC` substituted when a short write left errno unset (28 on Linux/macOS).
const ENOSPC: i32 = 28;

/* ---------------------------------------------------------------------------
 * On-disk map structures (relmapper.c-internal; not shared vocabulary).
 * ------------------------------------------------------------------------- */

/// `RelMapping` — one catalog-OID → relfilenumber entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RelMapping {
    /// `mapoid` — OID of a catalog.
    mapoid: Oid,
    /// `mapfilenumber` — its rel file number.
    mapfilenumber: RelFileNumber,
}

/// `RelMapFile` — the exact on-disk image: magic, count, fixed-size mapping
/// array, and a trailing CRC over everything before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RelMapFile {
    magic: i32,
    num_mappings: i32,
    mappings: [RelMapping; MAX_MAPPINGS],
    crc: u32,
}

const fn empty_relmap_file() -> RelMapFile {
    RelMapFile {
        magic: 0,
        num_mappings: 0,
        mappings: [RelMapping {
            mapoid: 0,
            mapfilenumber: 0,
        }; MAX_MAPPINGS],
        crc: 0,
    }
}

/// `SerializedActiveRelMaps` — the active shared+local update maps passed to
/// parallel workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SerializedActiveRelMaps {
    active_shared_updates: RelMapFile,
    active_local_updates: RelMapFile,
}

/* ---------------------------------------------------------------------------
 * Per-backend process state — the six file-static RelMapFile variables.
 * ------------------------------------------------------------------------- */

struct RelMapperState {
    shared_map: RelMapFile,
    local_map: RelMapFile,
    active_shared_updates: RelMapFile,
    active_local_updates: RelMapFile,
    pending_shared_updates: RelMapFile,
    pending_local_updates: RelMapFile,
}

impl RelMapperState {
    const fn new() -> Self {
        Self {
            shared_map: empty_relmap_file(),
            local_map: empty_relmap_file(),
            active_shared_updates: empty_relmap_file(),
            active_local_updates: empty_relmap_file(),
            pending_shared_updates: empty_relmap_file(),
            pending_local_updates: empty_relmap_file(),
        }
    }
}

thread_local! {
    static STATE: RefCell<RelMapperState> = const { RefCell::new(RelMapperState::new()) };
}

fn with_state<R>(f: impl FnOnce(&mut RelMapperState) -> R) -> R {
    STATE.with(|st| f(&mut st.borrow_mut()))
}

/* ---------------------------------------------------------------------------
 * Byte-image helpers.
 *
 * The on-disk image is an exact `sizeof(RelMapFile)` blob; the CRC is computed
 * over `offsetof(RelMapFile, crc)` leading bytes. All fields are 4-byte
 * integers and `RelMapping` is `{u32,u32}`, so the C struct has no padding and
 * the native-endian field-by-field serialization reproduces the image C
 * read()/write()/memcpy/CRC use byte-for-byte.
 * ------------------------------------------------------------------------- */

/// `sizeof(RelMapFile)` = magic(4) + num_mappings(4) + mappings(64*8) + crc(4).
const SIZEOF_RELMAPFILE: usize = 4 + 4 + (MAX_MAPPINGS * 8) + 4;

/// `offsetof(RelMapFile, crc)`.
const OFFSETOF_RELMAPFILE_CRC: usize = SIZEOF_RELMAPFILE - 4;

fn encode_relmapfile(map: &RelMapFile) -> [u8; SIZEOF_RELMAPFILE] {
    let mut out = [0u8; SIZEOF_RELMAPFILE];
    out[0..4].copy_from_slice(&map.magic.to_ne_bytes());
    out[4..8].copy_from_slice(&map.num_mappings.to_ne_bytes());
    let mut off = 8;
    for m in &map.mappings {
        out[off..off + 4].copy_from_slice(&m.mapoid.to_ne_bytes());
        out[off + 4..off + 8].copy_from_slice(&m.mapfilenumber.to_ne_bytes());
        off += 8;
    }
    out[off..off + 4].copy_from_slice(&map.crc.to_ne_bytes());
    out
}

fn decode_relmapfile(bytes: &[u8]) -> RelMapFile {
    debug_assert_eq!(bytes.len(), SIZEOF_RELMAPFILE);
    let mut map = empty_relmap_file();
    map.magic = i32::from_ne_bytes(bytes[0..4].try_into().unwrap());
    map.num_mappings = i32::from_ne_bytes(bytes[4..8].try_into().unwrap());
    let mut off = 8;
    for m in &mut map.mappings {
        m.mapoid = u32::from_ne_bytes(bytes[off..off + 4].try_into().unwrap());
        m.mapfilenumber = u32::from_ne_bytes(bytes[off + 4..off + 8].try_into().unwrap());
        off += 8;
    }
    map.crc = u32::from_ne_bytes(bytes[off..off + 4].try_into().unwrap());
    map
}

/* ---------------------------------------------------------------------------
 * Filename helper. relmapper.c builds these into fixed `char[MAXPGPATH]` stack
 * buffers (non-allocating); the Rust analog is a plain owned `String`.
 * ------------------------------------------------------------------------- */

fn relmap_filename(dbpath: &str, name: &str) -> String {
    format!("{dbpath}/{name}")
}

/* ---------------------------------------------------------------------------
 * CRC helpers — INIT/FIN/EQ are trivial inline macros; COMP is the seamed
 * (table/hardware) primitive.
 * ------------------------------------------------------------------------- */

const fn init_crc32c() -> u32 {
    0xffff_ffff
}

const fn fin_crc32c(crc: u32) -> u32 {
    crc ^ 0xffff_ffff
}

const fn eq_crc32c(c1: u32, c2: u32) -> bool {
    c1 == c2
}

/// `INIT/COMP/FIN_CRC32C(crc, map, offsetof(RelMapFile, crc))`.
fn relmapfile_crc(image: &[u8; SIZEOF_RELMAPFILE]) -> u32 {
    let mut crc = init_crc32c();
    crc = crc32c_seams::comp_crc32c::call(crc, &image[..OFFSETOF_RELMAPFILE_CRC]);
    fin_crc32c(crc)
}

/* ---------------------------------------------------------------------------
 * Lookups.
 * ------------------------------------------------------------------------- */

fn lookup_oid(map: &RelMapFile, relationId: Oid) -> Option<RelFileNumber> {
    for i in 0..map.num_mappings as usize {
        if relationId == map.mappings[i].mapoid {
            return Some(map.mappings[i].mapfilenumber);
        }
    }
    None
}

fn lookup_filenumber(map: &RelMapFile, filenumber: RelFileNumber) -> Option<Oid> {
    for i in 0..map.num_mappings as usize {
        if filenumber == map.mappings[i].mapfilenumber {
            return Some(map.mappings[i].mapoid);
        }
    }
    None
}

/// `RelationMapOidToFilenumber(relationId, shared)` — given a relation OID, look
/// up its filenumber. Returns `InvalidRelFileNumber` if not known.
pub fn RelationMapOidToFilenumber(relationId: Oid, shared: bool) -> RelFileNumber {
    with_state(|st| {
        // If there are active updates, believe those over the main maps.
        if shared {
            if let Some(n) = lookup_oid(&st.active_shared_updates, relationId) {
                return n;
            }
            if let Some(n) = lookup_oid(&st.shared_map, relationId) {
                return n;
            }
        } else {
            if let Some(n) = lookup_oid(&st.active_local_updates, relationId) {
                return n;
            }
            if let Some(n) = lookup_oid(&st.local_map, relationId) {
                return n;
            }
        }
        InvalidRelFileNumber
    })
}

/// `RelationMapFilenumberToOid(filenumber, shared)` — reverse mapping, for
/// information purposes only. Returns `InvalidOid` if not known.
pub fn RelationMapFilenumberToOid(filenumber: RelFileNumber, shared: bool) -> Oid {
    with_state(|st| {
        if shared {
            if let Some(o) = lookup_filenumber(&st.active_shared_updates, filenumber) {
                return o;
            }
            if let Some(o) = lookup_filenumber(&st.shared_map, filenumber) {
                return o;
            }
        } else {
            if let Some(o) = lookup_filenumber(&st.active_local_updates, filenumber) {
                return o;
            }
            if let Some(o) = lookup_filenumber(&st.local_map, filenumber) {
                return o;
            }
        }
        InvalidOid
    })
}

/// `RelationMapOidToFilenumberForDatabase(dbpath, relationId)` — like
/// `RelationMapOidToFilenumber`, but reads the mapping from `dbpath`.
pub fn RelationMapOidToFilenumberForDatabase(
    dbpath: &str,
    relationId: Oid,
) -> PgResult<RelFileNumber> {
    let mut map = empty_relmap_file();
    read_relmap_file(&mut map, dbpath, false, ERROR.0)?;

    for i in 0..map.num_mappings as usize {
        if relationId == map.mappings[i].mapoid {
            return Ok(map.mappings[i].mapfilenumber);
        }
    }
    Ok(InvalidRelFileNumber)
}

/// `RelationMapCopy(dbid, tsid, srcdbpath, dstdbpath)` — copy the relmap file
/// from the source db path to the destination and WAL-log it (creating a new
/// database's relmap file, not replacing an existing one).
pub fn RelationMapCopy(dbid: Oid, tsid: Oid, srcdbpath: &str, dstdbpath: &str) -> PgResult<()> {
    let mut map = empty_relmap_file();
    read_relmap_file(&mut map, srcdbpath, false, ERROR.0)?;

    // No sinval is needed (no one is connected to the destination yet) and no
    // point preserving files (the new database isn't usable yet).
    lwlock_seams::lock_relation_mapping::call(true)?;
    let res = write_relmap_file(&mut map, true, false, false, dbid, tsid, dstdbpath);
    lwlock_seams::unlock_relation_mapping::call()?;
    res
}

/* ---------------------------------------------------------------------------
 * RelationMapUpdateMap / apply_map_update / merge_map_updates
 * ------------------------------------------------------------------------- */

#[derive(Clone, Copy)]
enum MapSlot {
    SharedMap,
    LocalMap,
    ActiveShared,
    ActiveLocal,
    PendingShared,
    PendingLocal,
}

fn slot_mut(st: &mut RelMapperState, slot: MapSlot) -> &mut RelMapFile {
    match slot {
        MapSlot::SharedMap => &mut st.shared_map,
        MapSlot::LocalMap => &mut st.local_map,
        MapSlot::ActiveShared => &mut st.active_shared_updates,
        MapSlot::ActiveLocal => &mut st.active_local_updates,
        MapSlot::PendingShared => &mut st.pending_shared_updates,
        MapSlot::PendingLocal => &mut st.pending_local_updates,
    }
}

/// `RelationMapUpdateMap(relationId, fileNumber, shared, immediate)` — install a
/// new relfilenumber mapping. If `immediate` (or bootstrapping) it is activated
/// immediately; otherwise it is made pending until CCI.
pub fn RelationMapUpdateMap(
    relationId: Oid,
    fileNumber: RelFileNumber,
    shared: bool,
    immediate: bool,
) -> PgResult<()> {
    let slot;

    if miscinit_seams::is_bootstrap_processing_mode::call() {
        // In bootstrap mode, the mapping gets installed in the permanent map.
        slot = if shared {
            MapSlot::SharedMap
        } else {
            MapSlot::LocalMap
        };
    } else {
        // We don't currently support map changes within subtransactions, or
        // when in parallel mode.
        if xact_seams::get_current_transaction_nest_level::call() > 1 {
            return elog_error("cannot change relation mapping within subtransaction");
        }
        if xact_seams::is_in_parallel_mode::call() {
            return elog_error("cannot change relation mapping in parallel mode");
        }

        if immediate {
            slot = if shared {
                MapSlot::ActiveShared
            } else {
                MapSlot::ActiveLocal
            };
        } else {
            slot = if shared {
                MapSlot::PendingShared
            } else {
                MapSlot::PendingLocal
            };
        }
    }

    with_state(|st| apply_map_update(slot_mut(st, slot), relationId, fileNumber, true))
}

/// `apply_map_update(map, relationId, fileNumber, add_okay)` — insert a mapping,
/// replacing any existing one for the same relation. If `!add_okay` and none is
/// found, errors.
fn apply_map_update(
    map: &mut RelMapFile,
    relationId: Oid,
    fileNumber: RelFileNumber,
    add_okay: bool,
) -> PgResult<()> {
    for i in 0..map.num_mappings as usize {
        if relationId == map.mappings[i].mapoid {
            map.mappings[i].mapfilenumber = fileNumber;
            return Ok(());
        }
    }

    if !add_okay {
        return elog_error(format!(
            "attempt to apply a mapping to unmapped relation {relationId}"
        ));
    }
    if map.num_mappings as usize >= MAX_MAPPINGS {
        return elog_error("ran out of space in relation map");
    }
    let n = map.num_mappings as usize;
    map.mappings[n].mapoid = relationId;
    map.mappings[n].mapfilenumber = fileNumber;
    map.num_mappings += 1;
    Ok(())
}

/// `merge_map_updates(map, updates, add_okay)` — bulk `apply_map_update`.
fn merge_map_updates(map: &mut RelMapFile, updates: &RelMapFile, add_okay: bool) -> PgResult<()> {
    for i in 0..updates.num_mappings as usize {
        apply_map_update(
            map,
            updates.mappings[i].mapoid,
            updates.mappings[i].mapfilenumber,
            add_okay,
        )?;
    }
    Ok(())
}

/// `RelationMapRemoveMapping(relationId)` — remove a relation's entry from the
/// active-local-updates map (VACUUM FULL/CLUSTER transient-target backout).
pub fn RelationMapRemoveMapping(relationId: Oid) -> PgResult<()> {
    with_state(|st| {
        let map = &mut st.active_local_updates;
        for i in 0..map.num_mappings as usize {
            if relationId == map.mappings[i].mapoid {
                // Found it, collapse it out.
                let last = (map.num_mappings - 1) as usize;
                map.mappings[i] = map.mappings[last];
                map.num_mappings -= 1;
                return Ok(());
            }
        }
        elog_error(format!(
            "could not find temporary mapping for relation {relationId}"
        ))
    })
}

/* ---------------------------------------------------------------------------
 * Invalidation.
 * ------------------------------------------------------------------------- */

/// `RelationMapInvalidate(shared)` — SI cache flush handler: re-read the
/// indicated map file, but only if currently valid (loaded).
pub fn RelationMapInvalidate(shared: bool) -> PgResult<()> {
    let need_reload = with_state(|st| {
        if shared {
            st.shared_map.magic == RELMAPPER_FILEMAGIC
        } else {
            st.local_map.magic == RELMAPPER_FILEMAGIC
        }
    });
    if need_reload {
        load_relmap_file(shared, false)?;
    }
    Ok(())
}

/// `RelationMapInvalidateAll(void)` — reload all currently-valid map files (SI
/// buffer overflow recovery).
pub fn RelationMapInvalidateAll() -> PgResult<()> {
    let (reload_shared, reload_local) = with_state(|st| {
        (
            st.shared_map.magic == RELMAPPER_FILEMAGIC,
            st.local_map.magic == RELMAPPER_FILEMAGIC,
        )
    });
    if reload_shared {
        load_relmap_file(true, false)?;
    }
    if reload_local {
        load_relmap_file(false, false)?;
    }
    Ok(())
}

/* ---------------------------------------------------------------------------
 * Transaction-boundary hooks.
 * ------------------------------------------------------------------------- */

/// `AtCCI_RelationMap(void)` — activate pending relation map updates at CCI.
pub fn AtCCI_RelationMap() -> PgResult<()> {
    with_state(|st| {
        if st.pending_shared_updates.num_mappings != 0 {
            let pending = st.pending_shared_updates;
            merge_map_updates(&mut st.active_shared_updates, &pending, true)?;
            st.pending_shared_updates.num_mappings = 0;
        }
        if st.pending_local_updates.num_mappings != 0 {
            let pending = st.pending_local_updates;
            merge_map_updates(&mut st.active_local_updates, &pending, true)?;
            st.pending_local_updates.num_mappings = 0;
        }
        Ok(())
    })
}

/// `AtEOXact_RelationMap(isCommit, isParallelWorker)` — relation mapping at
/// main-transaction commit or abort.
pub fn AtEOXact_RelationMap(isCommit: bool, isParallelWorker: bool) -> PgResult<()> {
    if isCommit && !isParallelWorker {
        // We should not get here with any "pending" updates.
        debug_assert_eq!(with_state(|st| st.pending_shared_updates.num_mappings), 0);
        debug_assert_eq!(with_state(|st| st.pending_local_updates.num_mappings), 0);

        // Write any active updates to the actual map files, then reset them.
        let have_shared = with_state(|st| st.active_shared_updates.num_mappings != 0);
        if have_shared {
            let updates = with_state(|st| st.active_shared_updates);
            perform_relmap_update(true, &updates)?;
            with_state(|st| st.active_shared_updates.num_mappings = 0);
        }
        let have_local = with_state(|st| st.active_local_updates.num_mappings != 0);
        if have_local {
            let updates = with_state(|st| st.active_local_updates);
            perform_relmap_update(false, &updates)?;
            with_state(|st| st.active_local_updates.num_mappings = 0);
        }
    } else {
        // Abort or parallel worker --- drop all local and pending updates.
        debug_assert!(
            !isParallelWorker || with_state(|st| st.pending_shared_updates.num_mappings) == 0
        );
        debug_assert!(
            !isParallelWorker || with_state(|st| st.pending_local_updates.num_mappings) == 0
        );

        with_state(|st| {
            st.active_shared_updates.num_mappings = 0;
            st.active_local_updates.num_mappings = 0;
            st.pending_shared_updates.num_mappings = 0;
            st.pending_local_updates.num_mappings = 0;
        });
    }
    Ok(())
}

/// `AtPrepare_RelationMap(void)` — error out if the transaction changed the map
/// (not supported under 2PC).
pub fn AtPrepare_RelationMap() -> PgResult<()> {
    let modified = with_state(|st| {
        st.active_shared_updates.num_mappings != 0
            || st.active_local_updates.num_mappings != 0
            || st.pending_shared_updates.num_mappings != 0
            || st.pending_local_updates.num_mappings != 0
    });
    if modified {
        return Err(PgError::new(
            ERROR,
            "cannot PREPARE a transaction that modified relation mapping",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_error_location(here()));
    }
    Ok(())
}

/// `CheckPointRelationMap(void)` — flush WAL-logged map updates by taking and
/// releasing the lock (write_relmap_file fsyncs before releasing it).
pub fn CheckPointRelationMap() -> PgResult<()> {
    lwlock_seams::lock_relation_mapping::call(false)?;
    lwlock_seams::unlock_relation_mapping::call()
}

/* ---------------------------------------------------------------------------
 * Bootstrap / startup.
 * ------------------------------------------------------------------------- */

/// `RelationMapFinishBootstrap(void)` — write out the initial map files.
pub fn RelationMapFinishBootstrap() -> PgResult<()> {
    debug_assert!(miscinit_seams::is_bootstrap_processing_mode::call());

    // Shouldn't be anything "pending" ...
    debug_assert_eq!(
        with_state(|st| (
            st.active_shared_updates.num_mappings,
            st.active_local_updates.num_mappings,
            st.pending_shared_updates.num_mappings,
            st.pending_local_updates.num_mappings,
        )),
        (0, 0, 0, 0)
    );

    // Write the files; no WAL or sinval needed.
    lwlock_seams::lock_relation_mapping::call(true)?;
    let database_path = match init_small_seams::database_path::call() {
        Ok(p) => p,
        Err(e) => {
            let _ = lwlock_seams::unlock_relation_mapping::call();
            return Err(e);
        }
    };
    let my_database_id = init_small_seams::my_database_id::call();
    let my_database_table_space = init_small_seams::my_database_table_space::call();

    let res = (|| {
        let mut shared = with_state(|st| st.shared_map);
        write_relmap_file(
            &mut shared,
            false,
            false,
            false,
            InvalidOid,
            GLOBALTABLESPACE_OID,
            "global",
        )?;
        with_state(|st| st.shared_map = shared);

        let mut local = with_state(|st| st.local_map);
        write_relmap_file(
            &mut local,
            false,
            false,
            false,
            my_database_id,
            my_database_table_space,
            &database_path,
        )?;
        with_state(|st| st.local_map = local);
        Ok(())
    })();

    lwlock_seams::unlock_relation_mapping::call()?;
    res
}

/// `RelationMapInitialize(void)` — empty the maps at process startup.
pub fn RelationMapInitialize() {
    with_state(|st| {
        st.shared_map.magic = 0; // mark it not loaded
        st.local_map.magic = 0;
        st.shared_map.num_mappings = 0;
        st.local_map.num_mappings = 0;
        st.active_shared_updates.num_mappings = 0;
        st.active_local_updates.num_mappings = 0;
        st.pending_shared_updates.num_mappings = 0;
        st.pending_local_updates.num_mappings = 0;
    });
}

/// `RelationMapInitializePhase2(void)` — load the shared map file.
pub fn RelationMapInitializePhase2() -> PgResult<()> {
    // In bootstrap mode, the map file isn't there yet, so do nothing.
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }
    load_relmap_file(true, false)
}

/// `RelationMapInitializePhase3(void)` — load the local map file.
pub fn RelationMapInitializePhase3() -> PgResult<()> {
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }
    load_relmap_file(false, false)
}

/* ---------------------------------------------------------------------------
 * Parallel-worker serialize/restore.
 * ------------------------------------------------------------------------- */

/// `EstimateRelationMapSpace(void)` — `sizeof(SerializedActiveRelMaps)`.
pub fn EstimateRelationMapSpace() -> usize {
    2 * SIZEOF_RELMAPFILE
}

/// `SerializeRelationMap(void)` — serialize active shared and local relmap state
/// for parallel workers. The C version writes into a caller-provided shared
/// buffer; we return the owned value for the DSM-marshaling layer to place.
pub fn SerializeRelationMap() -> SerializedActiveRelMaps {
    with_state(|st| SerializedActiveRelMaps {
        active_shared_updates: st.active_shared_updates,
        active_local_updates: st.active_local_updates,
    })
}

/// `RestoreRelationMap(startAddress)` — restore active shared and local relmap
/// state within a parallel worker.
pub fn RestoreRelationMap(relmaps: &SerializedActiveRelMaps) -> PgResult<()> {
    let existing = with_state(|st| {
        st.active_shared_updates.num_mappings != 0
            || st.active_local_updates.num_mappings != 0
            || st.pending_shared_updates.num_mappings != 0
            || st.pending_local_updates.num_mappings != 0
    });
    if existing {
        return elog_error("parallel worker has existing mappings");
    }

    with_state(|st| {
        st.active_shared_updates = relmaps.active_shared_updates;
        st.active_local_updates = relmaps.active_local_updates;
    });
    Ok(())
}

/* ---------------------------------------------------------------------------
 * load_relmap_file / read_relmap_file
 * ------------------------------------------------------------------------- */

/// `load_relmap_file(shared, lock_held)` — load the shared or local map file.
/// Failure is fatal (these files are essential for catalog access).
fn load_relmap_file(shared: bool, lock_held: bool) -> PgResult<()> {
    if shared {
        let mut shared_map = with_state(|st| st.shared_map);
        read_relmap_file(&mut shared_map, "global", lock_held, FATAL.0)?;
        with_state(|st| st.shared_map = shared_map);
    } else {
        let database_path = init_small_seams::database_path::call()?;
        let mut local_map = with_state(|st| st.local_map);
        read_relmap_file(&mut local_map, &database_path, lock_held, FATAL.0)?;
        with_state(|st| st.local_map = local_map);
    }
    Ok(())
}

/// `read_relmap_file(map, dbpath, lock_held, elevel)` — load data from any
/// relation mapper file. Errors are reported at `elevel` (at least ERROR).
fn read_relmap_file(
    map: &mut RelMapFile,
    dbpath: &str,
    lock_held: bool,
    elevel: i32,
) -> PgResult<()> {
    debug_assert!(elevel >= ERROR.0);

    // Grab the lock unless the caller already holds it. The fd seam opens after
    // the lock and closes before release (so write_relmap_file's exclusive lock
    // implies no one else has it open), mirroring C's Windows-rename ordering.
    if !lock_held {
        lwlock_seams::lock_relation_mapping::call(false)?;
    }

    let mapfilename = relmap_filename(dbpath, RELMAPPER_FILENAME);

    pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_READ);
    let outcome = match fd_seams::relmap_read_file::call(dbpath) {
        Ok(o) => o,
        Err(e) => {
            pgstat_report_wait_end();
            if !lock_held {
                let _ = lwlock_seams::unlock_relation_mapping::call();
            }
            return Err(e);
        }
    };
    pgstat_report_wait_end();

    if !lock_held {
        lwlock_seams::unlock_relation_mapping::call()?;
    }

    let bytes = match outcome {
        RelmapReadOutcome::OpenFailed { errno } => {
            return ereport_file_access(elevel, errno, &mapfilename, "could not open file");
        }
        RelmapReadOutcome::ReadFailed { errno } => {
            return ereport_file_access(elevel, errno, &mapfilename, "could not read file");
        }
        RelmapReadOutcome::ShortRead { got } => {
            return Err(PgError::new(
                level(elevel),
                format!(
                    "could not read file \"{mapfilename}\": read {got} of {SIZEOF_RELMAPFILE}"
                ),
            )
            .with_sqlstate(ERRCODE_DATA_CORRUPTED)
            .with_error_location(here()));
        }
        RelmapReadOutcome::CloseFailed { errno } => {
            return ereport_file_access(elevel, errno, &mapfilename, "could not close file");
        }
        RelmapReadOutcome::Ok { bytes } => bytes,
    };

    if bytes.len() != SIZEOF_RELMAPFILE {
        return Err(PgError::new(
            level(elevel),
            format!("relation mapping file \"{mapfilename}\" contains invalid data"),
        )
        .with_error_location(here()));
    }
    *map = decode_relmapfile(&bytes);

    // Check for correct magic number, etc.
    if map.magic != RELMAPPER_FILEMAGIC
        || map.num_mappings < 0
        || map.num_mappings as usize > MAX_MAPPINGS
    {
        return Err(PgError::new(
            level(elevel),
            format!("relation mapping file \"{mapfilename}\" contains invalid data"),
        )
        .with_error_location(here()));
    }

    // Verify the CRC.
    let image: [u8; SIZEOF_RELMAPFILE] = bytes.as_slice().try_into().unwrap();
    let crc = relmapfile_crc(&image);

    if !eq_crc32c(crc, map.crc) {
        return Err(PgError::new(
            level(elevel),
            format!("relation mapping file \"{mapfilename}\" contains incorrect checksum"),
        )
        .with_error_location(here()));
    }

    Ok(())
}

/* ---------------------------------------------------------------------------
 * write_relmap_file
 * ------------------------------------------------------------------------- */

/// `write_relmap_file(...)` — write out a new shared or local map file. The
/// magic number and CRC are filled into `*newmap`.
#[allow(clippy::too_many_arguments)]
fn write_relmap_file(
    newmap: &mut RelMapFile,
    write_wal: bool,
    send_sinval: bool,
    preserve_files: bool,
    dbid: Oid,
    tsid: Oid,
    dbpath: &str,
) -> PgResult<()> {
    // CheckPointRelationMap() relies on this locking.
    debug_assert!(lwlock_seams::relation_mapping_lock_held_by_me_exclusive::call());

    // Fill in the overhead fields and update CRC.
    newmap.magic = RELMAPPER_FILEMAGIC;
    if newmap.num_mappings < 0 || newmap.num_mappings as usize > MAX_MAPPINGS {
        return elog_error("attempt to write bogus relation mapping");
    }

    let mut image = encode_relmapfile(newmap);
    let crc = relmapfile_crc(&image);
    newmap.crc = crc;
    // Re-encode the crc field into the image handed to the file/WAL seams.
    image[OFFSETOF_RELMAPFILE_CRC..].copy_from_slice(&crc.to_ne_bytes());

    let maptempfilename = relmap_filename(dbpath, RELMAPPER_TEMP_FILENAME);

    pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_WRITE);
    let outcome = match fd_seams::relmap_write_temp::call(dbpath, &image) {
        Ok(o) => o,
        Err(e) => {
            pgstat_report_wait_end();
            return Err(e);
        }
    };
    pgstat_report_wait_end();

    match outcome {
        RelmapWriteOutcome::OpenFailed { errno } => {
            return ereport_file_access(ERROR.0, errno, &maptempfilename, "could not open file");
        }
        RelmapWriteOutcome::WriteFailed { errno } => {
            // If write didn't set errno, assume problem is no disk space.
            let errno = if errno == 0 { ENOSPC } else { errno };
            return ereport_file_access(ERROR.0, errno, &maptempfilename, "could not write file");
        }
        RelmapWriteOutcome::CloseFailed { errno } => {
            return ereport_file_access(ERROR.0, errno, &maptempfilename, "could not close file");
        }
        RelmapWriteOutcome::Ok => {}
    }

    if write_wal {
        // now errors are fatal ...
        miscinit_seams::start_crit_section::call();

        // Build the xl_relmap_update record and emit WAL, flushing the LSN.
        // (WAL must hit the disk before the data update does.)
        let xlrec = encode_xl_relmap_update(dbid, tsid, SIZEOF_RELMAPFILE as i32);
        let lsn = xloginsert_seams::xlog_insert::call(
            RM_RELMAP_ID,
            XLOG_RELMAP_UPDATE,
            0,
            &[&xlrec, &image],
        )?;
        xlog_seams::xlog_flush::call(lsn)?;
    }

    // durable_rename() crash-safely renames the temp file into place. Although
    // the rename uses ERROR, we're often in a critical section, so ERROR becomes
    // PANIC.
    pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_REPLACE);
    fd_seams::relmap_durable_rename::call(dbpath)?;
    pgstat_report_wait_end();

    // Now that the file is safely on disk, send sinval so other backends re-read
    // it. Inside the critical section: failing to send forces a PANIC, else
    // other backends could keep using stale mapping info.
    if send_sinval {
        inval_seams::cache_invalidate_relmap::call(dbid)?;
    }

    // Make sure the files in the map aren't deleted if the outer transaction
    // aborts (mapped files are assumed to be in pg_global or the database's
    // default tablespace).
    if preserve_files {
        for i in 0..newmap.num_mappings as usize {
            let rlocator = RelFileLocator {
                spcOid: tsid,
                dbOid: dbid,
                relNumber: newmap.mappings[i].mapfilenumber,
            };
            storage_seams::relation_preserve_storage::call(rlocator, false)?;
        }
    }

    if write_wal {
        miscinit_seams::end_crit_section::call();
    }

    Ok(())
}

/// Encode the fixed `xl_relmap_update` header (`MinSizeOfRelmapUpdate` == 12:
/// dbid@0, tsid@4, nbytes@8) the map image follows.
fn encode_xl_relmap_update(dbid: Oid, tsid: Oid, nbytes: i32) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[0..4].copy_from_slice(&dbid.to_ne_bytes());
    out[4..8].copy_from_slice(&tsid.to_ne_bytes());
    out[8..12].copy_from_slice(&nbytes.to_ne_bytes());
    out
}

/* ---------------------------------------------------------------------------
 * perform_relmap_update
 * ------------------------------------------------------------------------- */

/// `perform_relmap_update(shared, updates)` — merge the updates into the real
/// map and write out the changes (committing updates during normal operation).
fn perform_relmap_update(shared: bool, updates: &RelMapFile) -> PgResult<()> {
    // Acquire the lock, re-read the target file, apply the updates, and write
    // before releasing the lock.
    lwlock_seams::lock_relation_mapping::call(true)?;

    let res = (|| {
        // Be certain we see any other updates just made.
        load_relmap_file(shared, true)?;

        let mut newmap = if shared {
            with_state(|st| st.shared_map)
        } else {
            with_state(|st| st.local_map)
        };

        // Apply the updates. No new mappings should appear unless somebody is
        // adding indexes to system catalogs.
        let add_okay = guc_seams::allow_system_table_mods::call();
        merge_map_updates(&mut newmap, updates, add_okay)?;

        let (dbid, tsid, dbpath) = if shared {
            (InvalidOid, GLOBALTABLESPACE_OID, "global".to_string())
        } else {
            (
                init_small_seams::my_database_id::call(),
                init_small_seams::my_database_table_space::call(),
                init_small_seams::database_path::call()?,
            )
        };
        write_relmap_file(&mut newmap, true, true, true, dbid, tsid, &dbpath)?;

        // We successfully wrote the updated file, so it's now safe to rely on
        // the new values in this process too.
        if shared {
            with_state(|st| st.shared_map = newmap);
        } else {
            with_state(|st| st.local_map = newmap);
        }
        Ok(())
    })();

    lwlock_seams::unlock_relation_mapping::call()?;
    res
}

/* ---------------------------------------------------------------------------
 * RELMAP resource manager's redo routine.
 * ------------------------------------------------------------------------- */

/// `relmap_redo(record)` — WAL replay for relmap update records (the `rm_redo`
/// slot). The decoded record carries `XLogRecGetInfo` and `XLogRecGetData`.
pub fn relmap_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> PgResult<()> {
    let decoded = record
        .record
        .as_ref()
        .expect("relmap_redo: XLogReaderState has no decoded record");

    let info = decoded.info() & !XLR_INFO_MASK;
    let data = decoded.main_data();

    // Backup blocks are not used in relmap records.
    debug_assert!(decoded.blocks().iter().all(|b| !b.in_use()));

    if info == XLOG_RELMAP_UPDATE {
        // xl_relmap_update header: dbid@0, tsid@4, nbytes@8, then the map image.
        if data.len() < 12 {
            return elog_panic("relmap_redo: truncated relmap update record");
        }
        let dbid = u32::from_ne_bytes(data[0..4].try_into().unwrap());
        let tsid = u32::from_ne_bytes(data[4..8].try_into().unwrap());
        let nbytes = i32::from_ne_bytes(data[8..12].try_into().unwrap());

        if nbytes as usize != SIZEOF_RELMAPFILE {
            return elog_panic(format!(
                "relmap_redo: wrong size {nbytes} in relmap update record"
            ));
        }
        let image = &data[12..12 + SIZEOF_RELMAPFILE];
        let mut newmap = decode_relmapfile(image);

        // We need to construct the pathname for this database.
        let dbpath = catalog_seams::get_database_path::call(dbid, tsid)?;

        // Write out the new map and send sinval, but don't write a new WAL entry
        // (no surrounding transaction to preserve files). Grab the lock to
        // interlock against load_relmap_file().
        lwlock_seams::lock_relation_mapping::call(true)?;
        let res = write_relmap_file(&mut newmap, false, true, false, dbid, tsid, &dbpath);
        lwlock_seams::unlock_relation_mapping::call()?;
        res
        // (C pfree(dbpath) is automatic here — dbpath is dropped.)
    } else {
        elog_panic(format!("relmap_redo: unknown op code {info}"))
    }
}

/* ---------------------------------------------------------------------------
 * Small error/elevel helpers (mirror elog()/ereport(elevel, ...)).
 * ------------------------------------------------------------------------- */

fn level(elevel: i32) -> types_error::ErrorLevel {
    if elevel >= PANIC.0 {
        PANIC
    } else if elevel >= FATAL.0 {
        FATAL
    } else {
        ERROR
    }
}

fn here() -> ErrorLocation {
    ErrorLocation::new("relmapper.c", 0, "")
}

fn elog_error<T>(message: impl Into<String>) -> PgResult<T> {
    Err(PgError::new(ERROR, message).with_error_location(here()))
}

fn elog_panic<T>(message: impl Into<String>) -> PgResult<T> {
    Err(PgError::new(PANIC, message).with_error_location(here()))
}

/// `ereport(elevel, (errcode_for_file_access(), errmsg("<phrase> \"%s\": %m")))`.
/// The SQLSTATE comes from the errno switch (the owner's
/// `errcode_for_file_access`); `%m` expands from the saved errno later.
fn ereport_file_access<T>(elevel: i32, errno: i32, filename: &str, phrase: &str) -> PgResult<T> {
    let sqlstate: SqlState = error_seams::sqlstate_for_file_access::call(errno);
    Err(
        PgError::new(level(elevel), format!("{phrase} \"{filename}\": %m"))
            .with_sqlstate(sqlstate)
            .with_saved_errno(errno)
            .with_error_location(here()),
    )
}

/* ---------------------------------------------------------------------------
 * Wait-event reporting (raw WaitEventIO codes through the pgstat seam).
 * ------------------------------------------------------------------------- */

fn pgstat_report_wait_start(wait_event: u32) {
    backend_utils_activity_waitevent_seams::pgstat_report_wait_start::call(wait_event);
}

fn pgstat_report_wait_end() {
    backend_utils_activity_waitevent_seams::pgstat_report_wait_end::call();
}

/* ---------------------------------------------------------------------------
 * Seam installation.
 * ------------------------------------------------------------------------- */

/// Install every seam this crate owns (`backend-utils-cache-relmapper-seams`).
pub fn init_seams() {
    use backend_utils_cache_relmapper_seams as seams;
    seams::relation_map_filenumber_to_oid::set(RelationMapFilenumberToOid);
    seams::relmap_redo::set(relmap_redo);
    seams::at_cci_relation_map::set(AtCCI_RelationMap);
    seams::at_eoxact_relation_map::set(AtEOXact_RelationMap);
    seams::at_prepare_relation_map::set(AtPrepare_RelationMap);
    // Pure-wiring install (assemble/seam-wiring-guard): owner body matches.
    seams::relation_map_finish_bootstrap::set(RelationMapFinishBootstrap);
}

#[cfg(test)]
mod tests;
