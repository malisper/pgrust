//! The external query-texts file (`qtext_store`/`load`/`fetch` + GC) and the
//! shutdown/startup dump/restore of `pg_stat_statements.stat`.
//!
//! Faithful port of the corresponding pg_stat_statements.c routines. The query
//! text file lives in the per-process scratch dir `pg_stat_tmp/`; the permanent
//! dump lives in `pg_stat/`. Both paths are relative to the backend's DataDir
//! cwd, exactly as the C `PG_STAT_TMP_DIR` / `PGSTAT_STAT_PERMANENT_DIRECTORY`
//! macros resolve.

use std::io::{Read, Seek, SeekFrom, Write};

use types_error::PgResult;

use types_tuple::heaptuple::Datum;

use crate::shmem;
use crate::{PgssEntry, ASSUMED_LENGTH_INIT, PGSS_FILE_HEADER};

/// `PGSS_TEXT_FILE` — the external query texts file.
const PGSS_TEXT_FILE: &str = "pg_stat_tmp/pgss_query_texts.stat";
/// `PGSS_DUMP_FILE` — the permanent stats dump (valid while server is down).
const PGSS_DUMP_FILE: &str = "pg_stat/pg_stat_statements.stat";

/// `PGSS_PG_MAJOR_VERSION` — PG18.
const PGSS_PG_MAJOR_VERSION: u32 = 18;

// ---------------------------------------------------------------------------
// qtext_store / load / fetch.
// ---------------------------------------------------------------------------

/// `qtext_store(query, query_len, &query_offset, &gc_count)`
/// (pg_stat_statements.c:2226). Append `query` (+ trailing NUL) to the external
/// file at the reserved offset. Returns `(true, offset)` on success.
pub(crate) fn qtext_store(query: &[u8], query_len: usize, gc_count: Option<&mut i32>) -> (bool, usize) {
    // SAFETY: pgss is live (callers hold at least shared lock; checked upstream).
    let pgss = unsafe { shmem::pgss_ref() };

    // Reserve file space under the shared-state spinlock.
    shmem::spin_lock_acquire(&pgss.mutex);
    let off = pgss.extent;
    pgss.extent += query_len + 1;
    pgss.n_writers += 1;
    if let Some(gc) = gc_count {
        *gc = pgss.gc_count;
    }
    shmem::spin_lock_release(&pgss.mutex);

    let result = (|| -> std::io::Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(PGSS_TEXT_FILE)?;
        f.seek(SeekFrom::Start(off as u64))?;
        f.write_all(&query[..query_len])?;
        f.write_all(b"\0")?;
        Ok(())
    })();

    // Mark our write complete.
    shmem::spin_lock_acquire(&pgss.mutex);
    pgss.n_writers -= 1;
    shmem::spin_lock_release(&pgss.mutex);

    match result {
        Ok(()) => (true, off),
        Err(_) => (false, off),
    }
}

/// `qtext_load_file(&buffer_size)` (pg_stat_statements.c:2306). Slurp the whole
/// external file into a buffer. `None` (no error) if unreadable.
pub(crate) fn qtext_load_file() -> Option<Vec<u8>> {
    match std::fs::read(PGSS_TEXT_FILE) {
        Ok(buf) => Some(buf),
        Err(_) => None,
    }
}

/// `qtext_fetch(query_offset, query_len, buffer, buffer_size)`
/// (pg_stat_statements.c:2399). Validate offset/len and return the text slice.
pub(crate) fn qtext_fetch(query_offset: usize, query_len: i32, buffer: &[u8]) -> Option<&[u8]> {
    if query_len < 0 {
        return None;
    }
    let end = query_offset.checked_add(query_len as usize)?;
    if end >= buffer.len() {
        return None;
    }
    // As a further sanity check, ensure a trailing NUL.
    if buffer[end] != 0 {
        return None;
    }
    Some(&buffer[query_offset..end])
}

// ---------------------------------------------------------------------------
// Garbage collection.
// ---------------------------------------------------------------------------

/// Write a new empty query file (the `entry_reset` "all entries removed" tail,
/// pg_stat_statements.c:2758). Best-effort; logs nothing.
pub(crate) fn reset_texts_file() {
    if let Ok(f) = std::fs::File::create(PGSS_TEXT_FILE) {
        let _ = f.set_len(0);
    }
}

/// `record_gc_qtexts()` macro (pg_stat_statements.c:307).
pub(crate) fn record_gc_qtexts() {
    let pgss = unsafe { shmem::pgss_ref() };
    shmem::spin_lock_acquire(&pgss.mutex);
    pgss.gc_count += 1;
    shmem::spin_lock_release(&pgss.mutex);
}

/// `need_gc_qtexts()` (pg_stat_statements.c:2422). Caller holds >= shared lock.
pub(crate) fn need_gc_qtexts() -> bool {
    let pgss = unsafe { shmem::pgss_ref() };
    shmem::spin_lock_acquire(&pgss.mutex);
    let extent = pgss.extent as u64;
    shmem::spin_lock_release(&pgss.mutex);

    let pgss_max = crate::pgss_max() as u64;
    // Don't proceed if file does not exceed 512 bytes per possible entry.
    if extent < 512 * pgss_max {
        return false;
    }
    // Don't proceed if file is less than about 50% bloat.
    if extent < pgss.mean_query_len as u64 * pgss_max * 2 {
        return false;
    }
    true
}

/// `gc_qtexts()` (pg_stat_statements.c:2471). Caller holds the exclusive lock.
pub(crate) fn gc_qtexts() {
    if !need_gc_qtexts() {
        return;
    }

    let pgss = unsafe { shmem::pgss_ref() };
    let pgss_hash = shmem::pgss_hash();

    let qbuffer = match qtext_load_file() {
        Some(b) => b,
        None => return gc_fail(),
    };

    // Overwrite the texts file in place.
    let mut qfile = match std::fs::File::create(PGSS_TEXT_FILE) {
        Ok(f) => f,
        Err(_) => return gc_fail(),
    };

    let mut extent: usize = 0;
    let mut nentries: i64 = 0;

    let mut hash_seq = hash::hsearch::HASH_SEQ_STATUS::new();
    dynahash::hash_seq_init(&mut hash_seq, pgss_hash);
    loop {
        let ptr = match dynahash::hash_seq_search(&mut hash_seq) {
            Ok(p) => p,
            Err(_) => break,
        };
        if ptr.is_null() {
            break;
        }
        // SAFETY: live entry held by the exclusive lock.
        let entry = unsafe { shmem::entry_ref(ptr) };
        let query_len = entry.query_len;
        let qry = qtext_fetch(entry.query_offset, query_len, &qbuffer);
        match qry {
            None => {
                // Trouble: drop the text.
                entry.query_offset = 0;
                entry.query_len = -1;
                continue;
            }
            Some(qry) => {
                let mut chunk = qry.to_vec();
                chunk.push(0);
                if qfile.write_all(&chunk).is_err() {
                    let _ = dynahash::hash_seq_term(&mut hash_seq);
                    return gc_fail();
                }
                entry.query_offset = extent;
                extent += query_len as usize + 1;
                nentries += 1;
            }
        }
    }

    // Truncate away any now-unused space.
    let _ = qfile.set_len(extent as u64);
    let _ = qfile.flush();
    drop(qfile);

    pgss.extent = extent;
    pgss.mean_query_len = if nentries > 0 {
        extent / nentries as usize
    } else {
        ASSUMED_LENGTH_INIT
    };

    record_gc_qtexts();
}

/// The `gc_fail:` cleanup branch of `gc_qtexts`.
fn gc_fail() {
    let pgss = unsafe { shmem::pgss_ref() };
    let pgss_hash = shmem::pgss_hash();

    // Mark all entries as having invalid texts.
    let mut hash_seq = hash::hsearch::HASH_SEQ_STATUS::new();
    dynahash::hash_seq_init(&mut hash_seq, pgss_hash);
    loop {
        let ptr = match dynahash::hash_seq_search(&mut hash_seq) {
            Ok(p) => p,
            Err(_) => break,
        };
        if ptr.is_null() {
            break;
        }
        let entry = unsafe { shmem::entry_ref(ptr) };
        entry.query_offset = 0;
        entry.query_len = -1;
    }

    // Destroy the text file and create a new empty one.
    let _ = std::fs::remove_file(PGSS_TEXT_FILE);
    let _ = std::fs::File::create(PGSS_TEXT_FILE);

    pgss.extent = 0;
    pgss.mean_query_len = ASSUMED_LENGTH_INIT;
    record_gc_qtexts();
}

// ---------------------------------------------------------------------------
// Startup load / shutdown dump.
// ---------------------------------------------------------------------------

/// The first-time-init tail of `pgss_shmem_startup` (pg_stat_statements.c:581).
/// Create the empty texts file, then (if `pgss_save`) restore the dump.
pub(crate) fn startup_load() -> PgResult<()> {
    // Unlink stale text file possibly left over from crash, then create fresh.
    let _ = std::fs::remove_file(PGSS_TEXT_FILE);
    let mut qfile = match std::fs::File::create(PGSS_TEXT_FILE) {
        Ok(f) => f,
        Err(_) => return Ok(()), // write_error: just press on
    };

    if !crate::pgss_save() {
        return Ok(());
    }

    // Attempt to load old statistics.
    let file = match std::fs::read(PGSS_DUMP_FILE) {
        Ok(b) => b,
        Err(_) => return Ok(()), // no dump → done
    };

    let pgss = unsafe { shmem::pgss_ref() };
    if let Err(()) = restore_dump(&file, &mut qfile, pgss) {
        // data_error / read_error: discard the bogus dump.
        let _ = std::fs::remove_file(PGSS_DUMP_FILE);
        return Ok(());
    }

    // Remove the persisted file so it's not included in backups.
    let _ = std::fs::remove_file(PGSS_DUMP_FILE);
    Ok(())
}

/// Parse the dump-file image and rebuild the hashtable + texts file. `Err(())`
/// on a malformed image (the C goto read_error / data_error).
fn restore_dump(
    file: &[u8],
    qfile: &mut std::fs::File,
    pgss: &mut crate::PgssSharedState,
) -> Result<(), ()> {
    let mut cur = std::io::Cursor::new(file);

    let header = read_u32(&mut cur)?;
    let pgver = read_u32(&mut cur)?;
    let num = read_i32(&mut cur)?;
    if header != PGSS_FILE_HEADER || pgver != PGSS_PG_MAJOR_VERSION {
        return Err(());
    }

    for _ in 0..num {
        let temp = read_entry(&mut cur)?;
        // Encoding sanity-check (best-effort: accept >= 0).
        if temp.encoding < 0 {
            return Err(());
        }
        let qlen = temp.query_len;
        if qlen < 0 {
            return Err(());
        }
        let mut text = vec![0u8; qlen as usize + 1];
        cur.read_exact(&mut text).map_err(|_| ())?;
        text[qlen as usize] = 0;

        // Skip loading sticky entries.
        if temp.counters.is_sticky() {
            continue;
        }

        let query_offset = pgss.extent;
        if qfile.write_all(&text).is_err() {
            return Err(());
        }
        pgss.extent += qlen as usize + 1;

        // Make the hashtable entry (discards old entries if too many).
        let entry = crate::store::entry_alloc(&temp.key, query_offset, qlen, temp.encoding, false);
        if !entry.is_null() {
            // Copy in the actual stats.
            // SAFETY: entry is a live PgssEntry from entry_alloc.
            let e = unsafe { shmem::entry_ref(entry) };
            e.counters = temp.counters;
            e.stats_since = temp.stats_since;
            e.minmax_stats_since = temp.minmax_stats_since;
        }
    }

    // Read global statistics.
    pgss.stats.dealloc = read_i64(&mut cur)?;
    pgss.stats.stats_reset = read_i64(&mut cur)?;
    Ok(())
}

/// `pgss_shmem_shutdown` (pg_stat_statements.c:736). Dump the hashtable into the
/// permanent file. Registered via `on_shmem_exit`.
fn pgss_shmem_shutdown(code: i32, _arg: Datum<'static>) -> PgResult<()> {
    // Don't try to dump during a crash.
    if code != 0 {
        return Ok(());
    }
    if !shmem::is_initialized() {
        return Ok(());
    }
    if !crate::pgss_save() {
        return Ok(());
    }

    let pgss = unsafe { shmem::pgss_ref() };
    let pgss_hash = shmem::pgss_hash();

    let tmp = format!("{PGSS_DUMP_FILE}.tmp");
    let mut file = match std::fs::File::create(&tmp) {
        Ok(f) => f,
        Err(_) => return Ok(()),
    };

    let num_entries = dynahash::hash_get_num_entries(pgss_hash) as i32;

    let write = (|| -> std::io::Result<()> {
        file.write_all(&PGSS_FILE_HEADER.to_ne_bytes())?;
        file.write_all(&PGSS_PG_MAJOR_VERSION.to_ne_bytes())?;
        file.write_all(&num_entries.to_ne_bytes())?;

        let qbuffer = qtext_load_file().unwrap_or_default();

        let mut hash_seq = hash::hsearch::HASH_SEQ_STATUS::new();
        dynahash::hash_seq_init(&mut hash_seq, pgss_hash);
        loop {
            let ptr = match dynahash::hash_seq_search(&mut hash_seq) {
                Ok(p) => p,
                Err(_) => break,
            };
            if ptr.is_null() {
                break;
            }
            // SAFETY: live entry at shutdown (no other processes running).
            let entry = unsafe { shmem::entry_ref(ptr) };
            let len = entry.query_len;
            let qstr = match qtext_fetch(entry.query_offset, len, &qbuffer) {
                Some(s) => s,
                None => continue, // Ignore entries with bogus texts.
            };
            file.write_all(entry_bytes(entry))?;
            file.write_all(qstr)?;
            file.write_all(b"\0")?;
        }

        // Dump global statistics.
        file.write_all(&pgss.stats.dealloc.to_ne_bytes())?;
        file.write_all(&pgss.stats.stats_reset.to_ne_bytes())?;
        file.flush()?;
        Ok(())
    })();

    if write.is_err() {
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(PGSS_TEXT_FILE);
        return Ok(());
    }
    drop(file);

    // Rename into place atomically.
    let _ = std::fs::rename(&tmp, PGSS_DUMP_FILE);
    // Unlink query-texts file; not needed while shut down.
    let _ = std::fs::remove_file(PGSS_TEXT_FILE);
    Ok(())
}

/// Register the shutdown dump via `on_shmem_exit` (the C `on_shmem_exit` in
/// pgss_shmem_startup when `!IsUnderPostmaster`).
pub(crate) fn register_shutdown_dump() {
    let _ = dsm_core::ipc::on_shmem_exit(
        pgss_shmem_shutdown,
        Datum::from_usize(0),
    );
}

// ---------------------------------------------------------------------------
// Raw entry serialization (the C `fwrite(entry, sizeof(pgssEntry), 1, file)` and
// `fread(&temp, sizeof(pgssEntry), 1, file)` — a byte-image copy of the struct).
// ---------------------------------------------------------------------------

/// The raw bytes of a `PgssEntry` (its on-disk image; C writes sizeof(pgssEntry)
/// bytes verbatim). We copy the whole struct image including the mutex word.
fn entry_bytes(entry: &PgssEntry) -> &[u8] {
    // SAFETY: PgssEntry is #[repr(C)] plain-old-data (the AtomicU32 mutex is a
    // u32 word); reading its bytes is sound.
    unsafe {
        core::slice::from_raw_parts(
            (entry as *const PgssEntry).cast::<u8>(),
            core::mem::size_of::<PgssEntry>(),
        )
    }
}

/// Read one `PgssEntry` image from the dump cursor.
fn read_entry(cur: &mut std::io::Cursor<&[u8]>) -> Result<PgssEntry, ()> {
    let mut buf = vec![0u8; core::mem::size_of::<PgssEntry>()];
    cur.read_exact(&mut buf).map_err(|_| ())?;
    // SAFETY: PgssEntry is #[repr(C)] POD; any byte pattern read back from a file
    // we wrote is a valid value (mirrors C's fread into a pgssEntry).
    let entry = unsafe { core::ptr::read_unaligned(buf.as_ptr().cast::<PgssEntry>()) };
    Ok(entry)
}

fn read_u32(cur: &mut std::io::Cursor<&[u8]>) -> Result<u32, ()> {
    let mut b = [0u8; 4];
    cur.read_exact(&mut b).map_err(|_| ())?;
    Ok(u32::from_ne_bytes(b))
}
fn read_i32(cur: &mut std::io::Cursor<&[u8]>) -> Result<i32, ()> {
    let mut b = [0u8; 4];
    cur.read_exact(&mut b).map_err(|_| ())?;
    Ok(i32::from_ne_bytes(b))
}
fn read_i64(cur: &mut std::io::Cursor<&[u8]>) -> Result<i64, ()> {
    let mut b = [0u8; 8];
    cur.read_exact(&mut b).map_err(|_| ())?;
    Ok(i64::from_ne_bytes(b))
}
