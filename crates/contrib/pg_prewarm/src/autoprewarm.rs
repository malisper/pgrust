//! `contrib/pg_prewarm/autoprewarm.c` — periodically dump the identities of
//! the blocks in shared_buffers to `$PGDATA/autoprewarm.blocks` and reload
//! them after a restart: the "autoprewarm leader" background worker
//! (`autoprewarm_main`), the per-database load workers
//! (`autoprewarm_database_main`), the shared state, the dump/load file
//! logic and the two SQL entry points.
//!
//! Thread-model mapping (the pg_stat_statements / bgworker precedent): C's
//! `AutoPrewarmSharedState` lives in a named DSM segment guarded by its own
//! LWLock (`apw_init_shmem`, autoprewarm.c:887); every backend here is a
//! thread of one process, so the state is one process-global
//! `pgsync::Mutex<Option<..>>` — `None` is "segment not created yet", which
//! is exactly the `found` bit `GetNamedDSMSegment` reports. The
//! `block_info_handle` DSM segment the leader hands to each per-database
//! worker (autoprewarm.c:345/514) is an `Arc<Vec<BlockInfoRecord>>` in the
//! same state. `bgw_library_name`/`bgw_function_name` collapse to direct
//! `bgw_main` fns (bgworker crate). C 18's read stream over the sorted
//! block list has no surface here, so the per-fork prewarm is the
//! callback's exact walk (`apw_read_stream_next_block`, :452) feeding
//! `ReadBufferExtended` one block at a time — same blocks, same counts.
//! PIDs are this port's per-thread process ids (`MyProcPid`).

use std::io::{Read, Write};
use std::sync::Arc;

use datum::Datum;
use elog::{elog, ereport};
use init_small::globals as g;
use pgsync::Mutex;
use procsignal::ThreadSignalHandler::Simple;
use types_core::{
    BlockNumber, ForkNumber, InvalidOid, Oid, OidIsValid, RelFileNumber, InvalidForkNumber,
};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG1, ERRCODE_INSUFFICIENT_RESOURCES,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, LOG,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_guc::GucContext::PGC_SIGHUP;
use types_storage::buf::{BM_PERMANENT, BM_TAG_VALID};
use types_storage::storage::ReadBufferMode;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

const AUTOPREWARM_FILE: &str = "autoprewarm.blocks";

// miscadmin.h InvalidPid.
const INVALID_PID: i32 = -1;

// pgstat.h PG_WAIT_EXTENSION (the class has no name table; the janitor /
// launcher precedent declares it locally).
const PG_WAIT_EXTENSION: u32 = 0x0700_0000;

// Blocked-wait recheck cadence for the leader's latch sleeps (the
// WaitForBackgroundWorkerStartup / launcher idiom): the SIGTERM /
// SIGHUP wake for a thread-model worker is postmaster-routed, so an
// unbounded sleep is bounded by a re-poll of the pending flags. Invisible
// to users — the flags are re-read and the sleep resumes.
const APW_RECHECK_MS: i64 = 1000;

/// autoprewarm.c:55 BlockInfoRecord. `forknum` is C's `ForkNumber` enum,
/// i.e. an int in the file and in the sort (the load path validates it
/// before any smgr call, autoprewarm.c:597). The derived `Ord` is
/// `apw_compare_blockinfo` (:1008): database, tablespace, filenumber,
/// forknum, blocknum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BlockInfoRecord {
    database: Oid,
    tablespace: Oid,
    filenumber: RelFileNumber,
    forknum: i32,
    blocknum: BlockNumber,
}

/// autoprewarm.c:65 AutoPrewarmSharedState (minus the LWLock: the mutex
/// around the whole state is it).
struct AutoPrewarmSharedState {
    bgworker_pid: i32,
    pid_using_dumpfile: i32,
    // block_info_handle: None is DSM_HANDLE_INVALID.
    block_info: Option<Arc<Vec<BlockInfoRecord>>>,
    database: Oid,
    prewarm_start_idx: usize,
    prewarm_stop_idx: usize,
    prewarmed_blocks: i32,
}

/// The named DSM segment ("autoprewarm"): None until the first
/// apw_init_shmem creates it.
static APW_STATE: Mutex<Option<AutoPrewarmSharedState>> = Mutex::new(None);

/// apw_init_state (autoprewarm.c:873).
fn apw_init_state() -> AutoPrewarmSharedState {
    AutoPrewarmSharedState {
        bgworker_pid: INVALID_PID,
        pid_using_dumpfile: INVALID_PID,
        block_info: None,
        database: InvalidOid,
        prewarm_start_idx: 0,
        prewarm_stop_idx: 0,
        prewarmed_blocks: 0,
    }
}

/// apw_init_shmem (autoprewarm.c:888): create-or-attach; true if the
/// segment already existed.
fn apw_init_shmem() -> bool {
    let mut guard = pgsync::lock(&APW_STATE);
    let found = guard.is_some();
    if !found {
        *guard = Some(apw_init_state());
    }
    found
}

/// LWLockAcquire(&apw_state->lock, LW_EXCLUSIVE) .. LWLockRelease.
fn with_state<R>(f: impl FnOnce(&mut AutoPrewarmSharedState) -> R) -> R {
    let mut guard = pgsync::lock(&APW_STATE);
    // GetNamedDSMSegment semantics: attach creates when absent.
    f(guard.get_or_insert_with(apw_init_state))
}

fn here(function: &'static str) -> ErrorLocation {
    ErrorLocation::new(file!(), line!() as i32, function)
}

/// ereport(ERROR, (errcode_for_file_access(), errmsg("<verb> \"<name>\": %m"))).
#[cold]
fn file_error(verb: &str, name: &str, errno: i32) -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .with_saved_errno(errno)
            .errcode_for_file_access()
            .errmsg(format!("could not {verb} file \"{name}\": %m"))
            .into_error(),
    )
}

fn io_errno(err: &std::io::Error) -> i32 {
    err.raw_os_error().unwrap_or(0)
}

/// apw_detach_shmem (autoprewarm.c:905): clear our PID from the shared state.
fn apw_detach_shmem(_code: i32, _arg: Datum) -> PgResult<()> {
    let my_pid = g::MyProcPid();
    with_state(|s| {
        if s.pid_using_dumpfile == my_pid {
            s.pid_using_dumpfile = INVALID_PID;
        }
        if s.bgworker_pid == my_pid {
            s.bgworker_pid = INVALID_PID;
        }
    });
    Ok(())
}

/// autoprewarm_main (autoprewarm.c:167): the leader's bgw_main.
pub(crate) fn autoprewarm_main(_main_arg: u64) -> PgResult<()> {
    let mut first_time = true;
    let mut final_dump_allowed = true;
    let mut last_dump_time: types_core::TimestampTz = 0;

    // Establish signal handlers; once that's done, unblock signals.
    // (bgworker's defaults: SIGTERM = die, SIGHUP ignored — C's leader wants
    // the shutdown-request and config-reload flags instead.)
    procsignal::pqsignal_thread(
        procsignal::signums::SIGTERM,
        Simple(interrupt::SignalHandlerForShutdownRequest),
    );
    procsignal::pqsignal_thread(
        procsignal::signums::SIGHUP,
        Simple(interrupt::SignalHandlerForConfigReload),
    );
    procsignal::pqsignal_thread(
        procsignal::signums::SIGUSR1,
        Simple(procsignal::procsignal_sigusr1_handler),
    );
    bgworker::BackgroundWorkerUnblockSignals();

    // Create (if necessary) and attach to our shared memory area.
    if apw_init_shmem() {
        first_time = false;
    }

    // Set on-detach hook so that our PID will be cleared on exit.
    ipc::before_shmem_exit(apw_detach_shmem, Datum::from_i64(0))?;

    // Store our PID in the shared memory area --- unless there's already
    // another worker running, in which case just exit.
    let my_pid = g::MyProcPid();
    let other = with_state(|s| {
        if s.bgworker_pid != INVALID_PID {
            Some(s.bgworker_pid)
        } else {
            s.bgworker_pid = my_pid;
            None
        }
    });
    if let Some(pid) = other {
        let _ = elog(LOG, format!("autoprewarm worker is already running under PID {pid}"));
        return Ok(());
    }

    // Preload buffers from the dump file only if we just created the shared
    // memory region (autoprewarm.c:208-227).
    if first_time {
        apw_load_buffers()?;
        final_dump_allowed = !interrupt::ShutdownRequestPending();
        last_dump_time = adt_timestamp::GetCurrentTimestamp();
    }

    // Periodically dump buffers until terminated.
    while !interrupt::ShutdownRequestPending() {
        // In case of a SIGHUP, just reload the configuration.
        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            guc_file::ProcessConfigFile(PGC_SIGHUP)?;
        }

        let interval = crate::autoprewarm_interval();
        if interval <= 0 {
            // We're only dumping at shutdown, so just wait forever
            // (bounded re-poll, see APW_RECHECK_MS).
            latch::WaitLatch(
                g::MyLatch(),
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                APW_RECHECK_MS,
                PG_WAIT_EXTENSION,
            )?;
        } else {
            // Compute the next dump time (TimestampTzPlusMilliseconds:
            // timestamps are microseconds).
            let next_dump_time = last_dump_time + i64::from(interval) * 1000 * 1000;
            let delay_in_ms = adt_timestamp::TimestampDifferenceMilliseconds(
                adt_timestamp::GetCurrentTimestamp(),
                next_dump_time,
            );

            // Perform a dump if it's time.
            if delay_in_ms <= 0 {
                last_dump_time = adt_timestamp::GetCurrentTimestamp();
                apw_dump_now(true, false)?;
                continue;
            }

            // Sleep until the next dump time (bounded re-poll, see
            // APW_RECHECK_MS; the delay is recomputed on every pass).
            latch::WaitLatch(
                g::MyLatch(),
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                delay_in_ms.min(APW_RECHECK_MS),
                PG_WAIT_EXTENSION,
            )?;
        }

        // Reset the latch, loop.
        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }
    }

    // Dump one last time.
    if final_dump_allowed {
        apw_dump_now(true, true)?;
    }
    Ok(())
}

/// fscanf-style scanner over the dump file (autoprewarm.c:338 `<<%d>>\n`,
/// :353 `%u,%u,%u,%u,%u\n`): a literal matches itself, `%d`/`%u` skip
/// leading whitespace and take an optional sign plus digits, and the
/// format's `\n` matches any run of whitespace.
struct Scanner<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Scanner<'_> {
    fn skip_ws(&mut self) {
        while self.pos < self.buf.len() && self.buf[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn literal(&mut self, lit: &[u8]) -> bool {
        if self.buf[self.pos..].starts_with(lit) {
            self.pos += lit.len();
            true
        } else {
            false
        }
    }

    /// `%u` / `%d`: wrapping like strtoul/strtol into the C target width.
    fn number(&mut self) -> Option<u32> {
        self.skip_ws();
        let mut neg = false;
        if self.pos < self.buf.len() && (self.buf[self.pos] == b'+' || self.buf[self.pos] == b'-') {
            neg = self.buf[self.pos] == b'-';
            self.pos += 1;
        }
        let start = self.pos;
        let mut v: u32 = 0;
        while self.pos < self.buf.len() && self.buf[self.pos].is_ascii_digit() {
            v = v.wrapping_mul(10).wrapping_add(u32::from(self.buf[self.pos] - b'0'));
            self.pos += 1;
        }
        if self.pos == start {
            return None;
        }
        Some(if neg { v.wrapping_neg() } else { v })
    }

    fn record(&mut self) -> Option<BlockInfoRecord> {
        let database = self.number()?;
        self.literal(b",").then_some(())?;
        let tablespace = self.number()?;
        self.literal(b",").then_some(())?;
        let filenumber = self.number()?;
        self.literal(b",").then_some(())?;
        let forknum = self.number()?;
        self.literal(b",").then_some(())?;
        let blocknum = self.number()?;
        self.skip_ws();
        Some(BlockInfoRecord {
            database,
            tablespace,
            filenumber,
            forknum: forknum as i32,
            blocknum,
        })
    }
}

/// apw_load_buffers (autoprewarm.c:292): read the dump file and launch
/// per-database workers one at a time to prewarm the buffers found there.
fn apw_load_buffers() -> PgResult<()> {
    let my_pid = g::MyProcPid();

    // Skip the prewarm if the dump file is in use; otherwise, prevent any
    // other process from writing it while we're using it.
    let busy = with_state(|s| {
        if s.pid_using_dumpfile == INVALID_PID {
            s.pid_using_dumpfile = my_pid;
            None
        } else {
            Some(s.pid_using_dumpfile)
        }
    });
    if let Some(pid) = busy {
        let _ = elog(
            LOG,
            format!("skipping prewarm because block dump file is being written by PID {pid}"),
        );
        return Ok(());
    }

    // Open the block dump file.  Exit quietly if it doesn't exist, but report
    // any other error.
    let file = fd::AllocateFile(AUTOPREWARM_FILE, "r")?;
    if file < 0 {
        let errno = fd::get_errno();
        if std::io::Error::from_raw_os_error(errno).kind() == std::io::ErrorKind::NotFound {
            with_state(|s| s.pid_using_dumpfile = INVALID_PID);
            return Ok(()); // No file to load.
        }
        return Err(file_error("read", AUTOPREWARM_FILE, errno));
    }

    // The whole file (the fscanf reads below scan this image).
    let mut contents: Vec<u8> = Vec::new();
    let read = fd::with_allocated_stdio(file, |f| f.read_to_end(&mut contents));
    let read_errno = match read {
        Some(Ok(_)) => None,
        Some(Err(e)) => Some(io_errno(&e)),
        None => Some(0),
    };

    // First line of the file is a record count.
    let mut sc = Scanner { buf: &contents, pos: 0 };
    let header = (sc.literal(b"<<"), sc.number(), sc.literal(b">>"));
    let num_elements = match (read_errno, header) {
        (None, (true, Some(n), true)) => n as i32,
        (errno, _) => {
            return Err(file_error("read from", AUTOPREWARM_FILE, errno.unwrap_or(0)));
        }
    };
    sc.skip_ws();

    // Read records, one per line.
    let num_elements = num_elements.max(0) as usize;
    let mut blkinfo: Vec<BlockInfoRecord> = Vec::with_capacity(num_elements);
    for i in 0..num_elements {
        match sc.record() {
            Some(r) => blkinfo.push(r),
            None => {
                return Err(Box::new(PgError::error(format!(
                    "autoprewarm block dump file is corrupted at line {}",
                    i + 1
                ))))
            }
        }
    }
    drop(contents);
    fd::FreeFile(file)?;

    // Sort the blocks to be loaded (apw_compare_blockinfo).
    blkinfo.sort_unstable();

    // Populate shared memory state.
    let blkinfo = Arc::new(blkinfo);
    with_state(|s| {
        s.block_info = Some(Arc::clone(&blkinfo));
        s.prewarm_start_idx = 0;
        s.prewarm_stop_idx = 0;
        s.prewarmed_blocks = 0;
    });

    // Get the info position of the first block of the next database.
    loop {
        let start = with_state(|s| s.prewarm_start_idx);
        if start >= num_elements {
            break;
        }
        let mut j = start;
        let mut current_db = blkinfo[j].database;

        // Advance the prewarm_stop_idx to the first BlockInfoRecord that does
        // not belong to this database.
        j += 1;
        while j < num_elements {
            if current_db != blkinfo[j].database {
                // Combine BlockInfoRecords for global objects with those of
                // the database.
                if current_db != InvalidOid {
                    break;
                }
                current_db = blkinfo[j].database;
            }
            j += 1;
        }

        // If we reach this point with current_db == InvalidOid, then only
        // BlockInfoRecords belonging to global objects exist.  We can't
        // prewarm without a database connection, so just bail out.
        if current_db == InvalidOid {
            break;
        }

        // Configure stop point and database for next per-database worker.
        with_state(|s| {
            s.prewarm_stop_idx = j;
            s.database = current_db;
        });
        debug_assert!(start < j);

        // If we've run out of free buffers, don't launch another worker.
        if !bufmgr::have_free_buffer() {
            break;
        }

        // Likewise, don't launch if we've already been told to shut down.
        if interrupt::ShutdownRequestPending() {
            break;
        }

        // Start a per-database worker to load blocks for this database; this
        // function will return once the per-database worker exits.
        apw_start_database_worker()?;

        // Prepare for next database.
        with_state(|s| s.prewarm_start_idx = s.prewarm_stop_idx);
    }

    // Clean up.
    let prewarmed_blocks = with_state(|s| {
        s.block_info = None;
        s.pid_using_dumpfile = INVALID_PID;
        s.prewarmed_blocks
    });

    // Report our success, if we were able to finish.
    if !interrupt::ShutdownRequestPending() {
        let _ = elog(
            LOG,
            format!(
                "autoprewarm successfully prewarmed {prewarmed_blocks} of {num_elements} \
                 previously-loaded blocks"
            ),
        );
    }
    Ok(())
}

/// autoprewarm_database_main (autoprewarm.c:501): prewarm all blocks for one
/// database (and possibly also global objects, if those got grouped with
/// this database).
pub(crate) fn autoprewarm_database_main(_main_arg: u64) -> PgResult<()> {
    // SIGTERM = die: bgworker's default handler. Unblock signals.
    bgworker::BackgroundWorkerUnblockSignals();

    // Connect to correct database and get block information.
    apw_init_shmem();
    let (block_info, database, start_idx, stop_idx) = with_state(|s| {
        (s.block_info.clone(), s.database, s.prewarm_start_idx, s.prewarm_stop_idx)
    });
    let Some(block_info) = block_info else {
        return Err(Box::new(
            PgError::error("could not map dynamic shared memory segment")
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    };
    bgworker::BackgroundWorkerInitializeConnectionByOid(database, InvalidOid, 0)?;

    let cx = mcx::MemoryContext::new_bump("autoprewarm worker");
    let mcx = cx.mcx();
    let mut i = start_idx;

    // Loop until we run out of blocks to prewarm or until we run out of free
    // buffers.
    while i < stop_idx && bufmgr::have_free_buffer() {
        let blk = block_info[i];
        let tablespace = blk.tablespace;
        let filenumber = blk.filenumber;

        xact::StartTransactionCommand()?;

        let reloid = relfilenumbermap::RelidByRelfilenumber(tablespace, filenumber)?;
        let rel = if OidIsValid(reloid) {
            relation::try_relation_open(mcx, reloid, types_rel::AccessShareLock)?
        } else {
            None
        };
        let Some(rel) = rel else {
            // We failed to open the relation, so there is nothing to close.
            xact::CommitTransactionCommand()?;

            // Fast-forward to the next relation.
            while i < stop_idx {
                let b = block_info[i];
                if b.tablespace != tablespace || b.filenumber != filenumber {
                    break;
                }
                i += 1;
            }
            continue;
        };

        // We have a relation; now let's loop until we find a valid fork of
        // the relation or we run out of free buffers.
        while i < stop_idx && bufmgr::have_free_buffer() {
            let blk = block_info[i];

            // Stop when we reach a different relation.
            if blk.tablespace != tablespace || blk.filenumber != filenumber {
                break;
            }

            let forknum = blk.forknum;

            // smgrexists is not safe for illegal forknum, hence check whether
            // the passed forknum is valid before using it in smgrexists.
            let fork = match ForkNumber::from_i32(forknum) {
                Some(f) if f != InvalidForkNumber && smgr::smgrexists(crate::rel_smgr_key(&rel)?, f)? => {
                    Some(f)
                }
                _ => None,
            };
            let Some(fork) = fork else {
                // Fast-forward to the next fork.
                while i < stop_idx {
                    let b = block_info[i];
                    if b.tablespace != tablespace || b.filenumber != filenumber || b.forknum != forknum
                    {
                        break;
                    }
                    i += 1;
                }
                continue;
            };

            let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(&rel, fork)?;

            // apw_read_stream_next_block (:452) driving the per-block read:
            // the callback checks free buffers and the (tablespace,
            // filenumber, fork) key before every block, fast-forwards past
            // blocks beyond the fork's size, and leaves `i` at the next
            // relation or fork when the stream ends.
            while i < stop_idx {
                postgres_seams::check_for_interrupts::call()?;
                if !bufmgr::have_free_buffer() {
                    i = stop_idx;
                    break;
                }
                let b = block_info[i];
                if b.tablespace != tablespace || b.filenumber != filenumber || b.forknum != forknum {
                    break;
                }
                i += 1;
                if b.blocknum >= nblocks {
                    continue;
                }
                let buf =
                    bufmgr::ReadBufferExtended(&rel, fork, b.blocknum, ReadBufferMode::Normal, None)?;
                with_state(|s| s.prewarmed_blocks += 1);
                bufmgr::ReleaseBuffer(buf)?;
            }
        }

        rel.close(types_rel::AccessShareLock)?;
        xact::CommitTransactionCommand()?;
    }

    drop(block_info);
    Ok(())
}

/// apw_dump_now (autoprewarm.c:676): dump information on blocks in shared
/// buffers; returns the number of blocks dumped.
fn apw_dump_now(is_bgworker: bool, dump_unlogged: bool) -> PgResult<i32> {
    let my_pid = g::MyProcPid();
    let pid = with_state(|s| {
        let pid = s.pid_using_dumpfile;
        if pid == INVALID_PID {
            s.pid_using_dumpfile = my_pid;
        }
        pid
    });

    if pid != INVALID_PID {
        if !is_bgworker {
            return Err(Box::new(PgError::error(format!(
                "could not perform block dump because dump file is being used by PID {pid}"
            ))));
        }
        let _ = elog(
            LOG,
            format!("skipping block dump because it is already being performed by PID {pid}"),
        );
        return Ok(0);
    }

    let nbuffers = bufmgr::NBuffersInited();
    let mut block_info_array: Vec<BlockInfoRecord> = Vec::with_capacity(nbuffers.max(0) as usize);

    for i in 0..nbuffers {
        postgres_seams::check_for_interrupts::call()?;

        let buf_hdr = bufmgr::GetBufferDescriptor(i);

        // Lock each buffer header before inspecting.
        let buf_state = bufmgr::LockBufHdr(buf_hdr);

        // Unlogged tables will be automatically truncated after a crash or
        // unclean shutdown. In such cases we need not prewarm them. Dump them
        // only if requested by caller.
        if buf_state & BM_TAG_VALID != 0 && (buf_state & BM_PERMANENT != 0 || dump_unlogged) {
            let tag = buf_hdr.tag();
            block_info_array.push(BlockInfoRecord {
                database: tag.dbOid,
                tablespace: tag.spcOid,
                filenumber: tag.relNumber,
                forknum: tag.forkNum as i32,
                blocknum: tag.blockNum,
            });
        }

        bufmgr::UnlockBufHdr(buf_hdr, buf_state);
    }
    let num_blocks = block_info_array.len() as i32;

    let transient_dump_file_path = format!("{AUTOPREWARM_FILE}.tmp");
    let file = fd::AllocateFile(&transient_dump_file_path, "w")?;
    if file < 0 {
        return Err(file_error("open", &transient_dump_file_path, fd::get_errno()));
    }

    // The record count, then one record per line (stdio-buffered in C; the
    // whole image is assembled and written once here).
    let mut image = format!("<<{num_blocks}>>\n");
    for (i, r) in block_info_array.iter().enumerate() {
        if i % 4096 == 0 {
            postgres_seams::check_for_interrupts::call()?;
        }
        use std::fmt::Write as _;
        let _ = writeln!(
            image,
            "{},{},{},{},{}",
            r.database, r.tablespace, r.filenumber, r.forknum as u32, r.blocknum
        );
    }
    drop(block_info_array);

    let written = fd::with_allocated_stdio(file, |f| f.write_all(image.as_bytes()));
    drop(image);
    let write_errno = match written {
        Some(Ok(())) => None,
        Some(Err(e)) => Some(io_errno(&e)),
        None => Some(0),
    };
    if let Some(errno) = write_errno {
        fd::FreeFile(file)?;
        fd::pg_unlink(&transient_dump_file_path);
        return Err(file_error("write to", &transient_dump_file_path, errno));
    }

    // Rename transient_dump_file_path to AUTOPREWARM_FILE to make things
    // permanent.
    if fd::FreeFile(file)? != 0 {
        let errno = fd::get_errno();
        fd::pg_unlink(&transient_dump_file_path);
        return Err(file_error("close", &transient_dump_file_path, errno));
    }

    fd::durable_rename(&transient_dump_file_path, AUTOPREWARM_FILE, ERROR)?;
    with_state(|s| s.pid_using_dumpfile = INVALID_PID);

    let _ = ereport(DEBUG1)
        .errmsg_internal(format!("wrote block details for {num_blocks} blocks"))
        .finish(here("apw_dump_now"));
    Ok(num_blocks)
}

/// autoprewarm_start_worker (autoprewarm.c:825): SQL-callable function to
/// launch autoprewarm.
pub(crate) fn fc_autoprewarm_start_worker(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if !crate::autoprewarm() {
        return Err(Box::new(
            PgError::error("autoprewarm is disabled")
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }

    apw_init_shmem();
    let pid = with_state(|s| s.bgworker_pid);

    if pid != INVALID_PID {
        return Err(Box::new(
            PgError::error(format!("autoprewarm worker is already running under PID {pid}"))
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }

    apw_start_leader_worker()?;

    Ok(Datum::from_i32(0)) // PG_RETURN_VOID
}

/// autoprewarm_dump_now (autoprewarm.c:857): SQL-callable function to
/// perform an immediate block dump; int8 result.
pub(crate) fn fc_autoprewarm_dump_now(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    apw_init_shmem();

    // PG_ENSURE_ERROR_CLEANUP(apw_detach_shmem, 0) { ... } PG_END_ENSURE_ERROR_CLEANUP.
    let arg = Datum::from_i64(0);
    ipc::before_shmem_exit(apw_detach_shmem, arg)?;
    let result = apw_dump_now(false, true);
    ipc::cancel_before_shmem_exit(apw_detach_shmem, arg)?;
    let num_blocks = match result {
        Ok(n) => n,
        Err(e) => {
            apw_detach_shmem(0, arg)?;
            return Err(e);
        }
    };

    Ok(Datum::from_i64(i64::from(num_blocks)))
}

/// apw_start_leader_worker (autoprewarm.c:919).
pub(crate) fn apw_start_leader_worker() -> PgResult<()> {
    let mut worker = bgworker::BackgroundWorker {
        bgw_name: "autoprewarm leader".to_string(),
        bgw_type: "autoprewarm leader".to_string(),
        bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS,
        bgw_start_time: bgworker::BgWorkerStartTime::ConsistentState,
        // `BackgroundWorker worker = {0}`: restart without delay.
        bgw_restart_time: 0,
        bgw_main: autoprewarm_main,
        bgw_main_arg: 0,
        bgw_extra: [0; bgworker::BGW_EXTRALEN],
        bgw_notify_pid: 0,
    };

    if miscinit::process_shared_preload_libraries_in_progress() {
        bgworker::RegisterBackgroundWorker(&worker)?;
        return Ok(());
    }

    // must set notify PID to wait for startup
    worker.bgw_notify_pid = g::MyProcPid();

    let Some(handle) = bgworker::RegisterDynamicBackgroundWorker(worker)? else {
        return Err(Box::new(
            PgError::error("could not register background process")
                .with_sqlstate(ERRCODE_INSUFFICIENT_RESOURCES)
                .with_hint("You may need to increase \"max_worker_processes\"."),
        ));
    };

    let (status, _pid) = bgworker::WaitForBackgroundWorkerStartup(&handle)?;
    if status != bgworker::BgwHandleStatus::BGWH_STARTED {
        return Err(Box::new(
            PgError::error("could not start background process")
                .with_sqlstate(ERRCODE_INSUFFICIENT_RESOURCES)
                .with_hint("More details may be available in the server log."),
        ));
    }
    Ok(())
}

/// apw_start_database_worker (autoprewarm.c:960).
fn apw_start_database_worker() -> PgResult<()> {
    let worker = bgworker::BackgroundWorker {
        bgw_name: "autoprewarm worker".to_string(),
        bgw_type: "autoprewarm worker".to_string(),
        bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS | bgworker::BGWORKER_BACKEND_DATABASE_CONNECTION,
        bgw_start_time: bgworker::BgWorkerStartTime::ConsistentState,
        bgw_restart_time: bgworker::BGW_NEVER_RESTART,
        bgw_main: autoprewarm_database_main,
        bgw_main_arg: 0,
        bgw_extra: [0; bgworker::BGW_EXTRALEN],
        // must set notify PID to wait for shutdown
        bgw_notify_pid: g::MyProcPid(),
    };

    let Some(handle) = bgworker::RegisterDynamicBackgroundWorker(worker)? else {
        return Err(Box::new(
            PgError::error("registering dynamic bgworker autoprewarm failed")
                .with_sqlstate(ERRCODE_INSUFFICIENT_RESOURCES)
                .with_hint(
                    "Consider increasing the configuration parameter \"max_worker_processes\".",
                ),
        ));
    };

    // Ignore the status; if it fails, postmaster has died, but we have
    // checks for that elsewhere. (Interrupts raised while waiting propagate.)
    bgworker::WaitForBackgroundWorkerShutdown(&handle)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // apw_compare_blockinfo (autoprewarm.c:1008): database, tablespace,
    // filenumber, forknum, blocknum — the derived Ord.
    #[test]
    fn blockinfo_sort_is_apw_compare_blockinfo() {
        let r = |d, t, f, k, b| BlockInfoRecord {
            database: d,
            tablespace: t,
            filenumber: f,
            forknum: k,
            blocknum: b,
        };
        let mut v = vec![
            r(5, 1663, 16384, 0, 7),
            r(0, 1664, 1262, 0, 0),
            r(5, 1663, 16384, 0, 3),
            r(5, 1663, 16384, 1, 0),
            r(5, 1663, 1259, 0, 9),
            r(1, 1663, 1259, 0, 0),
        ];
        v.sort_unstable();
        assert_eq!(
            v,
            vec![
                r(0, 1664, 1262, 0, 0),
                r(1, 1663, 1259, 0, 0),
                r(5, 1663, 1259, 0, 9),
                r(5, 1663, 16384, 0, 3),
                r(5, 1663, 16384, 0, 7),
                r(5, 1663, 16384, 1, 0),
            ]
        );
    }

    // The dump file grammar (autoprewarm.c:338/:353 fscanf formats): header
    // count, one record per line; whitespace-tolerant like fscanf.
    #[test]
    fn scanner_parses_dump_file_grammar() {
        let text = b"<<3>>\n0,1664,1262,0,0\n5,1663,16384,0,3\n 5,1663,16384,1,0\n";
        let mut sc = Scanner { buf: text, pos: 0 };
        assert!(sc.literal(b"<<"));
        assert_eq!(sc.number(), Some(3));
        assert!(sc.literal(b">>"));
        sc.skip_ws();
        let a = sc.record().unwrap();
        assert_eq!((a.database, a.tablespace, a.filenumber, a.forknum, a.blocknum), (0, 1664, 1262, 0, 0));
        let b = sc.record().unwrap();
        assert_eq!(b.blocknum, 3);
        let c = sc.record().unwrap();
        assert_eq!(c.forknum, 1);
        assert!(sc.record().is_none(), "a short file is corrupt at the next line");

        let mut bad = Scanner { buf: b"<<x>>\n", pos: 0 };
        assert!(bad.literal(b"<<"));
        assert_eq!(bad.number(), None);

        // %u wraps a negative the way strtoul does; the load path rejects
        // the out-of-range fork before any smgr call.
        let mut neg = Scanner { buf: b"1,2,3,-1,4\n", pos: 0 };
        assert_eq!(neg.record().unwrap().forknum, -1);
    }

    // apw_init_shmem: first caller creates the state (found = false), every
    // later caller attaches (found = true); the initial PIDs are InvalidPid.
    #[test]
    fn init_shmem_reports_found() {
        let first = apw_init_shmem();
        let second = apw_init_shmem();
        assert!(second, "second apw_init_shmem must find the existing state");
        if !first {
            assert_eq!(with_state(|s| (s.bgworker_pid, s.pid_using_dumpfile)), (INVALID_PID, INVALID_PID));
        }
    }
}
