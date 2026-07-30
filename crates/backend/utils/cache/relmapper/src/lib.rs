#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use elog::ereport;
use init_small::globals;
use types_core::{InvalidOid, InvalidRelFileNumber, Oid, RelFileNumber};
use types_error::{
    ErrorLevel, ErrorLocation, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERROR, FATAL, PANIC,
};
use types_storage::RelFileLocator;
use xlogreader_seams::XLogReaderState;

const RELMAPPER_FILENAME: &str = "pg_filenode.map";
const RELMAPPER_TEMP_FILENAME: &str = "pg_filenode.map.tmp";
const RELMAPPER_FILEMAGIC: i32 = 0x592717;
const MAX_MAPPINGS: usize = 64;
pub const XLOG_RELMAP_UPDATE: u8 = 0x00;
const MIN_SIZE_OF_RELMAP_UPDATE: usize = 12;
const GLOBALTABLESPACE_OID: Oid = 1664;
// lwlocklist.h PG_LWLOCK(25, RelationMapping)
const RELATION_MAPPING_LOCK: usize = 25;
const XLR_INFO_MASK: u8 = 0x0F;
// rmgrlist.h order (RmgrIds::RM_RELMAP_ID).
const RM_RELMAP_ID: u8 = 7;
const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_RELATION_MAP_READ: u32 = PG_WAIT_IO + 40;
const WAIT_EVENT_RELATION_MAP_REPLACE: u32 = PG_WAIT_IO + 41;
const WAIT_EVENT_RELATION_MAP_WRITE: u32 = PG_WAIT_IO + 42;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct RelMapping {
    mapoid: Oid,
    mapfilenumber: RelFileNumber,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RelMapFile {
    magic: i32,
    num_mappings: i32,
    mappings: [RelMapping; MAX_MAPPINGS],
    crc: u32,
}

const SIZEOF_RELMAPFILE: usize = std::mem::size_of::<RelMapFile>();
const OFFSETOF_CRC: usize = std::mem::offset_of!(RelMapFile, crc);
// All-4-byte fields: padding-free, so the struct IS the on-disk/WAL image.
const _: () = assert!(SIZEOF_RELMAPFILE == 524 && OFFSETOF_CRC == 520);

impl RelMapFile {
    const EMPTY: RelMapFile = RelMapFile {
        magic: 0,
        num_mappings: 0,
        mappings: [RelMapping {
            mapoid: 0,
            mapfilenumber: 0,
        }; MAX_MAPPINGS],
        crc: 0,
    };

    fn as_bytes(&self) -> &[u8; SIZEOF_RELMAPFILE] {
        // SAFETY: repr(C), size const-asserted, no padding, no invalid bit
        // patterns; shared borrow of POD reinterpreted as bytes.
        unsafe { &*(self as *const RelMapFile as *const [u8; SIZEOF_RELMAPFILE]) }
    }

    fn from_bytes(bytes: &[u8]) -> RelMapFile {
        assert_eq!(bytes.len(), SIZEOF_RELMAPFILE);
        // SAFETY: every bit pattern is a valid RelMapFile (integer fields
        // only); unaligned source handled by read_unaligned.
        unsafe { (bytes.as_ptr() as *const RelMapFile).read_unaligned() }
    }

    fn compute_crc(&self) -> u32 {
        let crc = crc32c::pg_comp_crc32c(0xFFFF_FFFF, &self.as_bytes()[..OFFSETOF_CRC]);
        crc ^ 0xFFFF_FFFF
    }
}

pub struct SerializedActiveRelMaps {
    active_shared_updates: [u8; SIZEOF_RELMAPFILE],
    active_local_updates: [u8; SIZEOF_RELMAPFILE],
}

struct State {
    shared_map: RelMapFile,
    local_map: RelMapFile,
    active_shared_updates: RelMapFile,
    active_local_updates: RelMapFile,
    pending_shared_updates: RelMapFile,
    pending_local_updates: RelMapFile,
}

thread_local! {
    // Borrows are scoped to in-memory map edits only, never held across the
    // lock/file/WAL/sinval calls (which can re-enter via inval callbacks).
    static STATE: RefCell<State> = const {
        RefCell::new(State {
            shared_map: RelMapFile::EMPTY,
            local_map: RelMapFile::EMPTY,
            active_shared_updates: RelMapFile::EMPTY,
            active_local_updates: RelMapFile::EMPTY,
            pending_shared_updates: RelMapFile::EMPTY,
            pending_local_updates: RelMapFile::EMPTY,
        })
    };
}

fn with_state<R>(f: impl FnOnce(&mut State) -> R) -> R {
    STATE.with(|st| f(&mut st.borrow_mut()))
}

fn map_lock() -> &'static lwlock::LWLock {
    lwlock::main_lock(RELATION_MAPPING_LOCK)
}

fn lock_map(mode: lwlock::LWLockMode) -> PgResult<()> {
    lwlock::LWLockAcquire(map_lock(), mode, globals::MyProcNumber())?;
    Ok(())
}

fn unlock_map() -> PgResult<()> {
    lwlock::LWLockRelease(map_lock())
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn database_path() -> &'static str {
    globals::DatabasePath().expect("relmapper: DatabasePath not set")
}

fn lookup_oid(map: &RelMapFile, relationId: Oid) -> Option<RelFileNumber> {
    map.mappings[..map.num_mappings as usize]
        .iter()
        .find(|m| m.mapoid == relationId)
        .map(|m| m.mapfilenumber)
}

fn lookup_filenumber(map: &RelMapFile, filenumber: RelFileNumber) -> Option<Oid> {
    map.mappings[..map.num_mappings as usize]
        .iter()
        .find(|m| m.mapfilenumber == filenumber)
        .map(|m| m.mapoid)
}

pub fn RelationMapOidToFilenumber(relationId: Oid, shared: bool) -> RelFileNumber {
    with_state(|st| {
        let (updates, main) = if shared {
            (&st.active_shared_updates, &st.shared_map)
        } else {
            (&st.active_local_updates, &st.local_map)
        };
        lookup_oid(updates, relationId)
            .or_else(|| lookup_oid(main, relationId))
            .unwrap_or(InvalidRelFileNumber)
    })
}

pub fn RelationMapFilenumberToOid(filenumber: RelFileNumber, shared: bool) -> Oid {
    with_state(|st| {
        let (updates, main) = if shared {
            (&st.active_shared_updates, &st.shared_map)
        } else {
            (&st.active_local_updates, &st.local_map)
        };
        lookup_filenumber(updates, filenumber)
            .or_else(|| lookup_filenumber(main, filenumber))
            .unwrap_or(InvalidOid)
    })
}

pub fn RelationMapOidToFilenumberForDatabase(
    dbpath: &str,
    relationId: Oid,
) -> PgResult<RelFileNumber> {
    let mut map = RelMapFile::EMPTY;
    read_relmap_file(&mut map, dbpath, false, ERROR)?;
    Ok(lookup_oid(&map, relationId).unwrap_or(InvalidRelFileNumber))
}

pub fn RelationMapCopy(dbid: Oid, tsid: Oid, srcdbpath: &str, dstdbpath: &str) -> PgResult<()> {
    let mut map = RelMapFile::EMPTY;
    read_relmap_file(&mut map, srcdbpath, false, ERROR)?;

    lock_map(lwlock::LW_EXCLUSIVE)?;
    let res = write_relmap_file(&mut map, true, false, false, dbid, tsid, dstdbpath);
    unlock_map()?;
    res
}

pub fn RelationMapUpdateMap(
    relationId: Oid,
    fileNumber: RelFileNumber,
    shared: bool,
    immediate: bool,
) -> PgResult<()> {
    let bootstrap = miscinit::IsBootstrapProcessingMode();
    if !bootstrap {
        if xact_seams::get_current_transaction_nest_level::call() > 1 {
            ereport(ERROR)
                .errmsg("cannot change relation mapping within subtransaction")
                .finish(loc("RelationMapUpdateMap"))?;
        }
        if xact_seams::is_in_parallel_mode::call() {
            ereport(ERROR)
                .errmsg("cannot change relation mapping in parallel mode")
                .finish(loc("RelationMapUpdateMap"))?;
        }
    }
    with_state(|st| {
        let map = if bootstrap {
            if shared {
                &mut st.shared_map
            } else {
                &mut st.local_map
            }
        } else if immediate {
            if shared {
                &mut st.active_shared_updates
            } else {
                &mut st.active_local_updates
            }
        } else if shared {
            &mut st.pending_shared_updates
        } else {
            &mut st.pending_local_updates
        };
        apply_map_update(map, relationId, fileNumber, true)
    })
}

fn apply_map_update(
    map: &mut RelMapFile,
    relationId: Oid,
    fileNumber: RelFileNumber,
    add_okay: bool,
) -> PgResult<()> {
    for m in map.mappings[..map.num_mappings as usize].iter_mut() {
        if m.mapoid == relationId {
            m.mapfilenumber = fileNumber;
            return Ok(());
        }
    }

    if !add_okay {
        ereport(ERROR)
            .errmsg(format!(
                "attempt to apply a mapping to unmapped relation {relationId}"
            ))
            .finish(loc("apply_map_update"))?;
    }
    if map.num_mappings as usize >= MAX_MAPPINGS {
        ereport(ERROR)
            .errmsg("ran out of space in relation map")
            .finish(loc("apply_map_update"))?;
    }
    map.mappings[map.num_mappings as usize] = RelMapping {
        mapoid: relationId,
        mapfilenumber: fileNumber,
    };
    map.num_mappings += 1;
    Ok(())
}

fn merge_map_updates(map: &mut RelMapFile, updates: &RelMapFile, add_okay: bool) -> PgResult<()> {
    for u in &updates.mappings[..updates.num_mappings as usize] {
        apply_map_update(map, u.mapoid, u.mapfilenumber, add_okay)?;
    }
    Ok(())
}

pub fn RelationMapRemoveMapping(relationId: Oid) -> PgResult<()> {
    let found = with_state(|st| {
        let map = &mut st.active_local_updates;
        for i in 0..map.num_mappings as usize {
            if map.mappings[i].mapoid == relationId {
                map.mappings[i] = map.mappings[map.num_mappings as usize - 1];
                map.num_mappings -= 1;
                return true;
            }
        }
        false
    });
    if !found {
        ereport(ERROR)
            .errmsg(format!(
                "could not find temporary mapping for relation {relationId}"
            ))
            .finish(loc("RelationMapRemoveMapping"))?;
    }
    Ok(())
}

pub fn RelationMapInvalidate(shared: bool) -> PgResult<()> {
    let loaded = with_state(|st| {
        if shared {
            st.shared_map.magic == RELMAPPER_FILEMAGIC
        } else {
            st.local_map.magic == RELMAPPER_FILEMAGIC
        }
    });
    if loaded {
        load_relmap_file(shared, false)?;
    }
    Ok(())
}

pub fn RelationMapInvalidateAll() -> PgResult<()> {
    let (shared_loaded, local_loaded) = with_state(|st| {
        (
            st.shared_map.magic == RELMAPPER_FILEMAGIC,
            st.local_map.magic == RELMAPPER_FILEMAGIC,
        )
    });
    if shared_loaded {
        load_relmap_file(true, false)?;
    }
    if local_loaded {
        load_relmap_file(false, false)?;
    }
    Ok(())
}

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

pub fn AtEOXact_RelationMap(isCommit: bool, isParallelWorker: bool) -> PgResult<()> {
    if isCommit && !isParallelWorker {
        debug_assert!(with_state(|st| {
            st.pending_shared_updates.num_mappings == 0
                && st.pending_local_updates.num_mappings == 0
        }));

        let shared_updates = with_state(|st| st.active_shared_updates);
        if shared_updates.num_mappings != 0 {
            perform_relmap_update(true, &shared_updates)?;
            with_state(|st| st.active_shared_updates.num_mappings = 0);
        }
        let local_updates = with_state(|st| st.active_local_updates);
        if local_updates.num_mappings != 0 {
            perform_relmap_update(false, &local_updates)?;
            with_state(|st| st.active_local_updates.num_mappings = 0);
        }
    } else {
        with_state(|st| {
            debug_assert!(
                !isParallelWorker
                    || (st.pending_shared_updates.num_mappings == 0
                        && st.pending_local_updates.num_mappings == 0)
            );
            st.active_shared_updates.num_mappings = 0;
            st.active_local_updates.num_mappings = 0;
            st.pending_shared_updates.num_mappings = 0;
            st.pending_local_updates.num_mappings = 0;
        });
    }
    Ok(())
}

pub fn AtPrepare_RelationMap() -> PgResult<()> {
    let modified = with_state(|st| {
        st.active_shared_updates.num_mappings != 0
            || st.active_local_updates.num_mappings != 0
            || st.pending_shared_updates.num_mappings != 0
            || st.pending_local_updates.num_mappings != 0
    });
    if modified {
        ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot PREPARE a transaction that modified relation mapping")
            .finish(loc("AtPrepare_RelationMap"))?;
    }
    Ok(())
}

pub fn CheckPointRelationMap() -> PgResult<()> {
    lock_map(lwlock::LW_SHARED)?;
    unlock_map()
}

pub fn RelationMapFinishBootstrap() -> PgResult<()> {
    debug_assert!(miscinit::IsBootstrapProcessingMode());
    debug_assert!(with_state(|st| {
        st.active_shared_updates.num_mappings == 0
            && st.active_local_updates.num_mappings == 0
            && st.pending_shared_updates.num_mappings == 0
            && st.pending_local_updates.num_mappings == 0
    }));

    lock_map(lwlock::LW_EXCLUSIVE)?;
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
            globals::MyDatabaseId(),
            globals::MyDatabaseTableSpace(),
            database_path(),
        )?;
        with_state(|st| st.local_map = local);
        Ok(())
    })();
    unlock_map()?;
    res
}

pub fn RelationMapInitialize() {
    with_state(|st| {
        st.shared_map.magic = 0;
        st.local_map.magic = 0;
        st.shared_map.num_mappings = 0;
        st.local_map.num_mappings = 0;
        st.active_shared_updates.num_mappings = 0;
        st.active_local_updates.num_mappings = 0;
        st.pending_shared_updates.num_mappings = 0;
        st.pending_local_updates.num_mappings = 0;
    });
}

pub fn RelationMapInitializePhase2() -> PgResult<()> {
    if miscinit::IsBootstrapProcessingMode() {
        return Ok(());
    }
    load_relmap_file(true, false)
}

pub fn RelationMapInitializePhase3() -> PgResult<()> {
    if miscinit::IsBootstrapProcessingMode() {
        return Ok(());
    }
    load_relmap_file(false, false)
}

pub fn EstimateRelationMapSpace() -> usize {
    2 * SIZEOF_RELMAPFILE
}

pub fn SerializeRelationMap() -> SerializedActiveRelMaps {
    with_state(|st| SerializedActiveRelMaps {
        active_shared_updates: *st.active_shared_updates.as_bytes(),
        active_local_updates: *st.active_local_updates.as_bytes(),
    })
}

pub fn RestoreRelationMap(relmaps: &SerializedActiveRelMaps) -> PgResult<()> {
    let existing = with_state(|st| {
        st.active_shared_updates.num_mappings != 0
            || st.active_local_updates.num_mappings != 0
            || st.pending_shared_updates.num_mappings != 0
            || st.pending_local_updates.num_mappings != 0
    });
    if existing {
        ereport(ERROR)
            .errmsg("parallel worker has existing mappings")
            .finish(loc("RestoreRelationMap"))?;
    }
    with_state(|st| {
        st.active_shared_updates = RelMapFile::from_bytes(&relmaps.active_shared_updates);
        st.active_local_updates = RelMapFile::from_bytes(&relmaps.active_local_updates);
    });
    Ok(())
}

fn load_relmap_file(shared: bool, lock_held: bool) -> PgResult<()> {
    if shared {
        let mut map = with_state(|st| st.shared_map);
        read_relmap_file(&mut map, "global", lock_held, FATAL)?;
        with_state(|st| st.shared_map = map);
    } else {
        let mut map = with_state(|st| st.local_map);
        read_relmap_file(&mut map, database_path(), lock_held, FATAL)?;
        with_state(|st| st.local_map = map);
    }
    Ok(())
}

fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

// C ereports mid-IO with lock/fd held and relies on abort cleanup; here the
// fd and lock are put back before the Err propagates (same net state).
fn read_relmap_file(
    map: &mut RelMapFile,
    dbpath: &str,
    lock_held: bool,
    elevel: ErrorLevel,
) -> PgResult<()> {
    debug_assert!(elevel.0 >= ERROR.0);

    if !lock_held {
        lock_map(lwlock::LW_SHARED)?;
    }

    // Open after acquiring and close before releasing, so write_relmap_file's
    // exclusive lock implies no concurrent open (C's Windows-rename ordering).
    let mapfilename = format!("{dbpath}/{RELMAPPER_FILENAME}");
    enum IoFail {
        Open(i32),
        Read(i32),
        ShortRead(isize),
        Close(i32),
    }
    let io: PgResult<Result<(), IoFail>> = (|| {
        let fd = fd::desc::OpenTransientFile(&mapfilename, libc::O_RDONLY)?;
        if fd < 0 {
            return Ok(Err(IoFail::Open(errno())));
        }

        waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_RELATION_MAP_READ);
        // SAFETY: map is a padding-free POD of const-asserted size; the
        // pread writes at most SIZEOF_RELMAPFILE bytes into it. vfs-routed
        // at offset 0 (DST reroute: the whole file is one positioned read;
        // raw libc::read on a sim fd is EBADF at the kernel).
        let r = fd::pg_pread(
            fd,
            unsafe {
                std::slice::from_raw_parts_mut(map as *mut RelMapFile as *mut u8, SIZEOF_RELMAPFILE)
            },
            0,
        );
        let read_errno = errno();
        waitevent_seams::pgstat_report_wait_end::call();

        let read_fail = if r != SIZEOF_RELMAPFILE as isize {
            Some(if r < 0 {
                IoFail::Read(read_errno)
            } else {
                IoFail::ShortRead(r)
            })
        } else {
            None
        };

        if fd::desc::CloseTransientFile(fd) != 0 && read_fail.is_none() {
            return Ok(Err(IoFail::Close(errno())));
        }
        Ok(match read_fail {
            Some(f) => Err(f),
            None => Ok(()),
        })
    })();

    if !lock_held {
        unlock_map()?;
    }

    match io? {
        Ok(()) => {}
        Err(IoFail::Open(en)) => {
            return ereport(elevel)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{mapfilename}\": %m"))
                .finish(loc("read_relmap_file"))
        }
        Err(IoFail::Read(en)) => {
            return ereport(elevel)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{mapfilename}\": %m"))
                .finish(loc("read_relmap_file"))
        }
        Err(IoFail::ShortRead(r)) => {
            return ereport(elevel)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg(format!(
                    "could not read file \"{mapfilename}\": read {r} of {SIZEOF_RELMAPFILE}"
                ))
                .finish(loc("read_relmap_file"))
        }
        Err(IoFail::Close(en)) => {
            return ereport(elevel)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg(format!("could not close file \"{mapfilename}\": %m"))
                .finish(loc("read_relmap_file"))
        }
    }

    if map.magic != RELMAPPER_FILEMAGIC
        || map.num_mappings < 0
        || map.num_mappings > MAX_MAPPINGS as i32
    {
        return ereport(elevel)
            .errmsg(format!(
                "relation mapping file \"{mapfilename}\" contains invalid data"
            ))
            .finish(loc("read_relmap_file"));
    }

    if map.compute_crc() != map.crc {
        return ereport(elevel)
            .errmsg(format!(
                "relation mapping file \"{mapfilename}\" contains incorrect checksum"
            ))
            .finish(loc("read_relmap_file"));
    }

    Ok(())
}

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
    debug_assert!(lwlock::LWLockHeldByMeInMode(map_lock(), lwlock::LW_EXCLUSIVE));

    newmap.magic = RELMAPPER_FILEMAGIC;
    if newmap.num_mappings < 0 || newmap.num_mappings > MAX_MAPPINGS as i32 {
        ereport(ERROR)
            .errmsg("attempt to write bogus relation mapping")
            .finish(loc("write_relmap_file"))?;
    }
    newmap.crc = newmap.compute_crc();

    let mapfilename = format!("{dbpath}/{RELMAPPER_FILENAME}");
    let maptempfilename = format!("{dbpath}/{RELMAPPER_TEMP_FILENAME}");

    let fd = fd::desc::OpenTransientFile(
        &maptempfilename,
        libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
    )?;
    if fd < 0 {
        return ereport(ERROR)
            .with_saved_errno(errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{maptempfilename}\": %m"))
            .finish(loc("write_relmap_file"));
    }

    waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_RELATION_MAP_WRITE);
    // SAFETY: newmap is a padding-free POD; the pwrite reads exactly its
    // SIZEOF_RELMAPFILE bytes. vfs-routed at offset 0 (DST reroute: the
    // whole file is one positioned write on a freshly-created fd).
    let w = fd::pg_pwrite(
        fd,
        unsafe {
            std::slice::from_raw_parts(newmap as *const RelMapFile as *const u8, SIZEOF_RELMAPFILE)
        },
        0,
    );
    let write_errno = errno();
    waitevent_seams::pgstat_report_wait_end::call();
    if w != SIZEOF_RELMAPFILE as isize {
        // If write didn't set errno, assume the problem is no disk space.
        let en = if w >= 0 && write_errno == 0 {
            libc::ENOSPC
        } else {
            write_errno
        };
        fd::desc::CloseTransientFile(fd);
        return ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not write file \"{maptempfilename}\": %m"))
            .finish(loc("write_relmap_file"));
    }

    if fd::desc::CloseTransientFile(fd) != 0 {
        return ereport(ERROR)
            .with_saved_errno(errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{maptempfilename}\": %m"))
            .finish(loc("write_relmap_file"));
    }

    // Errors below are PANIC-promoted while the crit section is open, as in C.
    if write_wal {
        globals::StartCriticalSection();
        // xl_relmap_update: dbid@0, tsid@4, nbytes@8, then the map image.
        let mut xlrec = [0u8; MIN_SIZE_OF_RELMAP_UPDATE];
        xlrec[0..4].copy_from_slice(&dbid.to_ne_bytes());
        xlrec[4..8].copy_from_slice(&tsid.to_ne_bytes());
        xlrec[8..12].copy_from_slice(&(SIZEOF_RELMAPFILE as i32).to_ne_bytes());
        let lsn = xloginsert_seams::xlog_insert::call(
            RM_RELMAP_ID,
            XLOG_RELMAP_UPDATE,
            &[&xlrec, newmap.as_bytes()],
        )?;
        transam_xlog_seams::xlog_flush::call(lsn)?;
    }

    waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_RELATION_MAP_REPLACE);
    let rename = fd::durable_rename(&maptempfilename, &mapfilename, ERROR);
    waitevent_seams::pgstat_report_wait_end::call();
    rename?;

    if send_sinval {
        inval::invalidate::CacheInvalidateRelmap(dbid)?;
    }

    if preserve_files {
        for m in &newmap.mappings[..newmap.num_mappings as usize] {
            catalog_storage_seams::relation_preserve_storage::call(
                RelFileLocator {
                    spcOid: tsid,
                    dbOid: dbid,
                    relNumber: m.mapfilenumber,
                },
                false,
            );
        }
    }

    if write_wal {
        globals::EndCriticalSection();
    }
    Ok(())
}

fn perform_relmap_update(shared: bool, updates: &RelMapFile) -> PgResult<()> {
    lock_map(lwlock::LW_EXCLUSIVE)?;

    let res = (|| {
        load_relmap_file(shared, true)?;

        let mut newmap = with_state(|st| if shared { st.shared_map } else { st.local_map });
        merge_map_updates(&mut newmap, updates, globals::allowSystemTableMods())?;

        if shared {
            write_relmap_file(
                &mut newmap,
                true,
                true,
                true,
                InvalidOid,
                GLOBALTABLESPACE_OID,
                "global",
            )?;
            with_state(|st| st.shared_map = newmap);
        } else {
            write_relmap_file(
                &mut newmap,
                true,
                true,
                true,
                globals::MyDatabaseId(),
                globals::MyDatabaseTableSpace(),
                database_path(),
            )?;
            with_state(|st| st.local_map = newmap);
        }
        Ok(())
    })();

    unlock_map()?;
    res
}

pub fn relmap_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let decoded = record
        .record
        .as_ref()
        .expect("relmap_redo: no decoded record");
    let info = decoded.xl_info & !XLR_INFO_MASK;

    debug_assert!(decoded.blocks.iter().all(|b| !b.in_use));

    if info != XLOG_RELMAP_UPDATE {
        return ereport(PANIC)
            .errmsg(format!("relmap_redo: unknown op code {info}"))
            .finish(loc("relmap_redo"));
    }

    // SAFETY: main_data points at the decoded record's owned data buffer.
    let data = unsafe { decoded.main_data_bytes() };
    if data.len() < MIN_SIZE_OF_RELMAP_UPDATE {
        return ereport(PANIC)
            .errmsg("relmap_redo: truncated relmap update record")
            .finish(loc("relmap_redo"));
    }
    let dbid = Oid::from_ne_bytes(data[0..4].try_into().unwrap());
    let tsid = Oid::from_ne_bytes(data[4..8].try_into().unwrap());
    let nbytes = i32::from_ne_bytes(data[8..12].try_into().unwrap());
    if nbytes as usize != SIZEOF_RELMAPFILE || data.len() < 12 + SIZEOF_RELMAPFILE {
        return ereport(PANIC)
            .errmsg(format!(
                "relmap_redo: wrong size {nbytes} in relmap update record"
            ))
            .finish(loc("relmap_redo"));
    }
    let mut newmap = RelMapFile::from_bytes(&data[12..12 + SIZEOF_RELMAPFILE]);

    // Recovery-cold; per-call context for the db path is fine.
    let ctx = mcx::MemoryContext::new("relmap_redo");
    let dbpath = relpath_seams::get_database_path::call(ctx.mcx(), dbid, tsid)?;

    lock_map(lwlock::LW_EXCLUSIVE)?;
    let res = write_relmap_file(&mut newmap, false, true, false, dbid, tsid, &dbpath);
    unlock_map()?;
    res
}

pub fn init_seams() {
    use relmapper_seams as s;
    s::relation_map_invalidate::set(RelationMapInvalidate);
    s::relation_map_invalidate_all::set(RelationMapInvalidateAll);
    s::relation_map_initialize::set(RelationMapInitialize);
    s::relation_map_initialize_phase2::set(RelationMapInitializePhase2);
    s::relation_map_initialize_phase3::set(RelationMapInitializePhase3);
    s::relation_map_update_map::set(RelationMapUpdateMap);
    s::relation_map_oid_to_filenumber::set(RelationMapOidToFilenumber);
}

#[cfg(test)]
mod tests;
