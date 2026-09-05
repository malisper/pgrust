//! Entry storage: pgss_store's counter accumulation (Welford variance),
//! LRU-by-usage eviction, reset, and the pgss_save dump file. The dump file
//! is byte-for-byte the C 18.6 file (header, major version, raw
//! sizeof(pgssEntry) records each followed by its NUL-terminated query text,
//! pgssGlobalStats), so a data directory moves between C and pgrust with
//! its statistics intact.

use std::io::{Read, Write};

use elog::ereport;
use types_core::instrument::{instr_time, BufferUsage, WalUsage};
use types_error::{ERRCODE_INVALID_PARAMETER_VALUE, LOG};

use crate::{
    gucs, nesting_level, Counters, PgssEntry, PgssHashKey, PgssShared,
    ASSUMED_LENGTH_INIT, PGSS, PGSS_DUMP_FILE, PGSS_EXEC, PGSS_FILE_HEADER, PGSS_NUMKIND,
    PGSS_PG_MAJOR_VERSION, PGSS_PLAN, STICKY_DECREASE_FACTOR, USAGE_DEALLOC_PERCENT,
    USAGE_DECREASE_FACTOR, USAGE_EXEC, USAGE_INIT,
};

fn ms(t: instr_time) -> f64 {
    t.ticks as f64 / 1_000_000.0
}

/// `pgss_store`. kind None == C's jstate!=NULL sticky-entry path.
#[allow(clippy::too_many_arguments)]
pub(crate) fn pgss_store(
    query: &str,
    query_id: i64,
    query_location: i32,
    query_len: i32,
    kind: Option<usize>,
    total_time: f64,
    rows: u64,
    bufusage: &BufferUsage,
    walusage: &WalUsage,
    jstate: Option<&queryjumble::JumbleState<'_>>,
    parallel_workers_to_launch: i64,
    parallel_workers_launched: i64,
) {
    // C: compute_query_id off and no module-side id => not tracked.
    if query_id == 0 {
        return;
    }

    let mut location = query_location;
    let mut len = query_len;
    let query = queryjumble::CleanQuerytext(query, &mut location, &mut len);
    let encoding = mbutils::GetDatabaseEncoding() as i32;

    let key = PgssHashKey {
        userid: miscinit::GetUserId(),
        dbid: init_small::globals::MyDatabaseId(),
        queryid: query_id,
        toplevel: nesting_level() == 0,
    };

    let mut guard = PGSS.lock().unwrap();
    let Some(shared) = guard.as_mut() else { return };

    if !shared.hash.contains_key(&key) {
        let norm_query;
        let text: &str = match jstate {
            Some(js) => {
                norm_query = crate::normalize::generate_normalized_query(js, query, location);
                &norm_query
            }
            None => query,
        };
        entry_alloc(shared, key, text.as_bytes(), encoding, jstate.is_some());
    }

    let Some(kind) = kind else { return };
    debug_assert!(jstate.is_none() && (kind == PGSS_PLAN || kind == PGSS_EXEC));
    let e = shared.hash.get_mut(&key).expect("entry created above");
    let c = &mut e.counters;

    if c.is_sticky() {
        c.usage = USAGE_INIT;
    }

    c.calls[kind] += 1;
    c.total_time[kind] += total_time;
    if c.calls[kind] == 1 {
        c.min_time[kind] = total_time;
        c.max_time[kind] = total_time;
        c.mean_time[kind] = total_time;
    } else {
        // Welford's method, as in C.
        let old_mean = c.mean_time[kind];
        c.mean_time[kind] += (total_time - old_mean) / c.calls[kind] as f64;
        c.sum_var_time[kind] += (total_time - old_mean) * (total_time - c.mean_time[kind]);
        // min==0 && max==0 means the min/max stats were reset.
        if c.min_time[kind] == 0.0 && c.max_time[kind] == 0.0 {
            c.min_time[kind] = total_time;
            c.max_time[kind] = total_time;
        } else {
            if c.min_time[kind] > total_time {
                c.min_time[kind] = total_time;
            }
            if c.max_time[kind] < total_time {
                c.max_time[kind] = total_time;
            }
        }
    }
    c.rows += rows as i64;
    c.shared_blks_hit += bufusage.shared_blks_hit;
    c.shared_blks_read += bufusage.shared_blks_read;
    c.shared_blks_dirtied += bufusage.shared_blks_dirtied;
    c.shared_blks_written += bufusage.shared_blks_written;
    c.local_blks_hit += bufusage.local_blks_hit;
    c.local_blks_read += bufusage.local_blks_read;
    c.local_blks_dirtied += bufusage.local_blks_dirtied;
    c.local_blks_written += bufusage.local_blks_written;
    c.temp_blks_read += bufusage.temp_blks_read;
    c.temp_blks_written += bufusage.temp_blks_written;
    c.shared_blk_read_time += ms(bufusage.shared_blk_read_time);
    c.shared_blk_write_time += ms(bufusage.shared_blk_write_time);
    c.local_blk_read_time += ms(bufusage.local_blk_read_time);
    c.local_blk_write_time += ms(bufusage.local_blk_write_time);
    c.temp_blk_read_time += ms(bufusage.temp_blk_read_time);
    c.temp_blk_write_time += ms(bufusage.temp_blk_write_time);
    c.usage += USAGE_EXEC;
    c.wal_records += walusage.wal_records;
    c.wal_fpi += walusage.wal_fpi;
    c.wal_bytes = c.wal_bytes.wrapping_add(walusage.wal_bytes);
    c.wal_buffers_full += walusage.wal_buffers_full;
    // No JIT in this engine: jit_* counters stay zero (C with jit disabled).
    c.parallel_workers_to_launch += parallel_workers_to_launch;
    c.parallel_workers_launched += parallel_workers_launched;
}

/// `entry_alloc`: evicts down to max first; sticky entries start at the
/// current median usage so they survive until execution completes.
pub(crate) fn entry_alloc(
    shared: &mut PgssShared,
    key: PgssHashKey,
    query_text: &[u8],
    encoding: i32,
    sticky: bool,
) {
    while shared.hash.len() >= shared.max {
        entry_dealloc(shared);
    }
    if shared.hash.contains_key(&key) {
        return;
    }
    let now = adt_timestamp::GetCurrentTimestamp();
    let usage = if sticky { shared.cur_median_usage } else { USAGE_INIT };
    shared.hash.insert(
        key,
        PgssEntry {
            counters: Counters { usage, ..Counters::default() },
            query_text: query_text.to_owned(),
            encoding,
            stats_since: now,
            minmax_stats_since: now,
        },
    );
}

/// `entry_dealloc`: decay usages, drop the lowest USAGE_DEALLOC_PERCENT.
pub(crate) fn entry_dealloc(shared: &mut PgssShared) {
    let mut tottextlen: usize = 0;
    let mut nvalidtexts: usize = 0;
    let mut usages: Vec<(PgssHashKey, f64)> = Vec::with_capacity(shared.hash.len());

    for (k, e) in shared.hash.iter_mut() {
        if e.counters.is_sticky() {
            e.counters.usage *= STICKY_DECREASE_FACTOR;
        } else {
            e.counters.usage *= USAGE_DECREASE_FACTOR;
        }
        usages.push((*k, e.counters.usage));
        tottextlen += e.query_text.len() + 1;
        nvalidtexts += 1;
    }

    usages.sort_by(|a, b| a.1.total_cmp(&b.1));

    let n = usages.len();
    if n > 0 {
        shared.cur_median_usage = usages[n / 2].1;
    }
    shared.mean_query_len =
        if nvalidtexts > 0 { tottextlen / nvalidtexts } else { ASSUMED_LENGTH_INIT };

    let nvictims = std::cmp::min(std::cmp::max(10, n * USAGE_DEALLOC_PERCENT / 100), n);
    for (k, _) in usages.iter().take(nvictims) {
        shared.hash.remove(k);
    }

    shared.stats.dealloc += 1;
}

/// `entry_reset`; returns the reset timestamp.
pub(crate) fn entry_reset(
    userid: types_core::Oid,
    dbid: types_core::Oid,
    queryid: i64,
    minmax_only: bool,
) -> types_error::PgResult<i64> {
    let mut guard = PGSS.lock().unwrap();
    let Some(shared) = guard.as_mut() else {
        return Err(crate::not_loaded_error());
    };

    let stats_reset = adt_timestamp::GetCurrentTimestamp();
    let num_entries = shared.hash.len();
    let mut num_remove = 0usize;

    let mut reset_one = |shared: &mut PgssShared, key: &PgssHashKey| {
        if minmax_only {
            if let Some(e) = shared.hash.get_mut(key) {
                for kind in 0..PGSS_NUMKIND {
                    e.counters.max_time[kind] = 0.0;
                    e.counters.min_time[kind] = 0.0;
                }
                e.minmax_stats_since = stats_reset;
            }
        } else if shared.hash.remove(key).is_some() {
            num_remove += 1;
        }
    };

    if userid != 0 && dbid != 0 && queryid != 0 {
        // All parameters given: probe the two (toplevel) slots directly.
        for toplevel in [false, true] {
            let key = PgssHashKey { userid, dbid, queryid, toplevel };
            if shared.hash.contains_key(&key) {
                reset_one(shared, &key);
            }
        }
    } else if userid != 0 || dbid != 0 || queryid != 0 {
        let keys: Vec<PgssHashKey> = shared
            .hash
            .keys()
            .filter(|k| {
                (userid == 0 || k.userid == userid)
                    && (dbid == 0 || k.dbid == dbid)
                    && (queryid == 0 || k.queryid == queryid)
            })
            .copied()
            .collect();
        for k in keys {
            reset_one(shared, &k);
        }
    } else {
        let keys: Vec<PgssHashKey> = shared.hash.keys().copied().collect();
        for k in keys {
            reset_one(shared, &k);
        }
    }

    // Only a full wipe resets the global stats.
    if num_entries == num_remove {
        shared.stats.dealloc = 0;
        shared.stats.stats_reset = stats_reset;
    }

    Ok(stats_reset)
}

fn dump_path() -> Option<std::path::PathBuf> {
    init_small::globals::DataDir().map(|d| std::path::Path::new(d).join(PGSS_DUMP_FILE))
}

fn here(function: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(file!(), line!() as i32, function)
}

/// C `ereport(LOG, (errcode_for_file_access(), errmsg("... \"%s\": %m", ...)))`
/// for a dump-file I/O failure: the OS reason follows the C-relative name.
fn log_file_error(verb: &str, name: &str, err: &std::io::Error, function: &'static str) {
    let _ = ereport(LOG)
        .with_saved_errno(err.raw_os_error().unwrap_or(0))
        .errcode_for_file_access()
        .errmsg(format!("could not {verb} file \"{name}\": %m"))
        .finish(here(function));
}

pub(crate) const COUNTER_WORDS: usize = 6 * PGSS_NUMKIND + 34;

/// C `sizeof(pgssEntry)` on LP64 (x86_64 and aarch64 alike): pgssHashKey 24
/// (Oid, Oid, int64, bool + 7 pad) + Counters 368 + Size query_offset 8 +
/// int query_len 4 + int encoding 4 + TimestampTz stats_since 8 +
/// TimestampTz minmax_stats_since 8 + slock_t mutex (1 or 4) padded to the
/// 8-byte struct alignment. The dump file is these raw structs
/// (pg_stat_statements.c:780 fwrite(entry, sizeof(pgssEntry)) /
/// :632 fread(&temp, sizeof(pgssEntry))), so the record layout is fixed.
pub(crate) const PGSS_ENTRY_SIZE: usize = 432;
const ENTRY_COUNTERS_OFF: usize = 24;
const ENTRY_QUERY_LEN_OFF: usize = 400;
const ENTRY_ENCODING_OFF: usize = 404;
const ENTRY_STATS_SINCE_OFF: usize = 408;
const ENTRY_MINMAX_SINCE_OFF: usize = 416;
/// C `sizeof(pgssGlobalStats)`: int64 dealloc + TimestampTz stats_reset.
const GLOBAL_STATS_SIZE: usize = 16;

/// The Counters struct as 64-bit words in C declaration order (every field
/// is an 8-byte int64/uint64/double, so the words ARE the struct image).
pub(crate) fn counters_to_words(c: &Counters) -> Vec<u64> {
    let mut w: Vec<u64> = Vec::with_capacity(COUNTER_WORDS);
    for k in 0..PGSS_NUMKIND {
        w.push(c.calls[k] as u64);
    }
    for arr in [&c.total_time, &c.min_time, &c.max_time, &c.mean_time, &c.sum_var_time] {
        for k in 0..PGSS_NUMKIND {
            w.push(arr[k].to_bits());
        }
    }
    for v in [
        c.rows,
        c.shared_blks_hit,
        c.shared_blks_read,
        c.shared_blks_dirtied,
        c.shared_blks_written,
        c.local_blks_hit,
        c.local_blks_read,
        c.local_blks_dirtied,
        c.local_blks_written,
        c.temp_blks_read,
        c.temp_blks_written,
    ] {
        w.push(v as u64);
    }
    for v in [
        c.shared_blk_read_time,
        c.shared_blk_write_time,
        c.local_blk_read_time,
        c.local_blk_write_time,
        c.temp_blk_read_time,
        c.temp_blk_write_time,
        c.usage,
    ] {
        w.push(v.to_bits());
    }
    w.push(c.wal_records as u64);
    w.push(c.wal_fpi as u64);
    w.push(c.wal_bytes);
    w.push(c.wal_buffers_full as u64);
    w.push(c.jit_functions as u64);
    w.push(c.jit_generation_time.to_bits());
    w.push(c.jit_inlining_count as u64);
    w.push(c.jit_deform_time.to_bits());
    w.push(c.jit_deform_count as u64);
    w.push(c.jit_inlining_time.to_bits());
    w.push(c.jit_optimization_count as u64);
    w.push(c.jit_optimization_time.to_bits());
    w.push(c.jit_emission_count as u64);
    w.push(c.jit_emission_time.to_bits());
    w.push(c.parallel_workers_to_launch as u64);
    w.push(c.parallel_workers_launched as u64);
    debug_assert_eq!(w.len(), COUNTER_WORDS);
    w
}

pub(crate) fn counters_from_words(w: &[u64; COUNTER_WORDS]) -> Counters {
    struct R<'a> {
        w: &'a [u64],
        i: usize,
    }
    impl R<'_> {
        fn int(&mut self) -> i64 {
            let v = self.w[self.i] as i64;
            self.i += 1;
            v
        }
        fn flt(&mut self) -> f64 {
            let v = f64::from_bits(self.w[self.i]);
            self.i += 1;
            v
        }
        fn raw(&mut self) -> u64 {
            let v = self.w[self.i];
            self.i += 1;
            v
        }
    }
    let mut r = R { w, i: 0 };
    let mut c = Counters::default();
    for k in 0..PGSS_NUMKIND {
        c.calls[k] = r.int();
    }
    for k in 0..PGSS_NUMKIND {
        c.total_time[k] = r.flt();
    }
    for k in 0..PGSS_NUMKIND {
        c.min_time[k] = r.flt();
    }
    for k in 0..PGSS_NUMKIND {
        c.max_time[k] = r.flt();
    }
    for k in 0..PGSS_NUMKIND {
        c.mean_time[k] = r.flt();
    }
    for k in 0..PGSS_NUMKIND {
        c.sum_var_time[k] = r.flt();
    }
    c.rows = r.int();
    c.shared_blks_hit = r.int();
    c.shared_blks_read = r.int();
    c.shared_blks_dirtied = r.int();
    c.shared_blks_written = r.int();
    c.local_blks_hit = r.int();
    c.local_blks_read = r.int();
    c.local_blks_dirtied = r.int();
    c.local_blks_written = r.int();
    c.temp_blks_read = r.int();
    c.temp_blks_written = r.int();
    c.shared_blk_read_time = r.flt();
    c.shared_blk_write_time = r.flt();
    c.local_blk_read_time = r.flt();
    c.local_blk_write_time = r.flt();
    c.temp_blk_read_time = r.flt();
    c.temp_blk_write_time = r.flt();
    c.usage = r.flt();
    c.wal_records = r.int();
    c.wal_fpi = r.int();
    c.wal_bytes = r.raw();
    c.wal_buffers_full = r.int();
    c.jit_functions = r.int();
    c.jit_generation_time = r.flt();
    c.jit_inlining_count = r.int();
    c.jit_deform_time = r.flt();
    c.jit_deform_count = r.int();
    c.jit_inlining_time = r.flt();
    c.jit_optimization_count = r.int();
    c.jit_optimization_time = r.flt();
    c.jit_emission_count = r.int();
    c.jit_emission_time = r.flt();
    c.parallel_workers_to_launch = r.int();
    c.parallel_workers_launched = r.int();
    debug_assert_eq!(r.i, COUNTER_WORDS);
    c
}

/// One raw `pgssEntry` image (native byte order, like C's fwrite of the
/// struct). query_offset (the offset into the query-text temp file, which C
/// never reads back), the spinlock and the padding are zero.
fn entry_record(key: &PgssHashKey, e: &PgssEntry) -> [u8; PGSS_ENTRY_SIZE] {
    let mut rec = [0u8; PGSS_ENTRY_SIZE];
    rec[0..4].copy_from_slice(&key.userid.to_ne_bytes());
    rec[4..8].copy_from_slice(&key.dbid.to_ne_bytes());
    rec[8..16].copy_from_slice(&key.queryid.to_ne_bytes());
    rec[16] = u8::from(key.toplevel);
    for (i, w) in counters_to_words(&e.counters).into_iter().enumerate() {
        let off = ENTRY_COUNTERS_OFF + 8 * i;
        rec[off..off + 8].copy_from_slice(&w.to_ne_bytes());
    }
    rec[ENTRY_QUERY_LEN_OFF..ENTRY_QUERY_LEN_OFF + 4]
        .copy_from_slice(&(e.query_text.len() as i32).to_ne_bytes());
    rec[ENTRY_ENCODING_OFF..ENTRY_ENCODING_OFF + 4].copy_from_slice(&e.encoding.to_ne_bytes());
    rec[ENTRY_STATS_SINCE_OFF..ENTRY_STATS_SINCE_OFF + 8]
        .copy_from_slice(&e.stats_since.to_ne_bytes());
    rec[ENTRY_MINMAX_SINCE_OFF..ENTRY_MINMAX_SINCE_OFF + 8]
        .copy_from_slice(&e.minmax_stats_since.to_ne_bytes());
    rec
}

fn ne_u32(b: &[u8]) -> u32 {
    u32::from_ne_bytes(b[..4].try_into().expect("4 bytes"))
}
fn ne_i32(b: &[u8]) -> i32 {
    i32::from_ne_bytes(b[..4].try_into().expect("4 bytes"))
}
fn ne_u64(b: &[u8]) -> u64 {
    u64::from_ne_bytes(b[..8].try_into().expect("8 bytes"))
}
fn ne_i64(b: &[u8]) -> i64 {
    i64::from_ne_bytes(b[..8].try_into().expect("8 bytes"))
}

/// The dump-file body `pgss_shmem_shutdown` writes (pg_stat_statements.c:
/// 762-800): PGSS_FILE_HEADER, PGSS_PG_MAJOR_VERSION, the entry count, then
/// each raw pgssEntry followed by its NUL-terminated query text, and the
/// pgssGlobalStats struct.
pub(crate) fn write_dump<W: Write>(w: &mut W, shared: &PgssShared) -> std::io::Result<()> {
    w.write_all(&PGSS_FILE_HEADER.to_ne_bytes())?;
    w.write_all(&PGSS_PG_MAJOR_VERSION.to_ne_bytes())?;
    w.write_all(&(shared.hash.len() as i32).to_ne_bytes())?;
    for (k, e) in shared.hash.iter() {
        w.write_all(&entry_record(k, e))?;
        w.write_all(&e.query_text)?;
        w.write_all(&[0u8])?;
    }
    w.write_all(&shared.stats.dealloc.to_ne_bytes())?;
    w.write_all(&shared.stats.stats_reset.to_ne_bytes())?;
    Ok(())
}

/// The load half of `pgss_shmem_startup` (pg_stat_statements.c:615-676).
/// `Ok(false)` is C's `data_error` (bad header/version, an encoding that is
/// not a valid backend encoding — "the only field we can easily
/// sanity-check" — or a negative query_len, which C would index at -1);
/// an `Err` is C's `read_error`.
pub(crate) fn read_dump<R: Read>(r: &mut R, shared: &mut PgssShared) -> std::io::Result<bool> {
    let mut hdr = [0u8; 12];
    r.read_exact(&mut hdr)?;
    let header = ne_u32(&hdr[0..4]);
    let pgver = ne_u32(&hdr[4..8]);
    let num = ne_i32(&hdr[8..12]);
    if header != PGSS_FILE_HEADER || pgver != PGSS_PG_MAJOR_VERSION {
        return Ok(false);
    }
    let mut rec = [0u8; PGSS_ENTRY_SIZE];
    for _ in 0..num.max(0) {
        r.read_exact(&mut rec)?;
        let encoding = ne_i32(&rec[ENTRY_ENCODING_OFF..]);
        if !wchar::pg_valid_be_encoding(encoding) {
            return Ok(false);
        }
        let query_len = ne_i32(&rec[ENTRY_QUERY_LEN_OFF..]);
        if query_len < 0 {
            return Ok(false);
        }
        // C reads query_len + 1 bytes and forces the trailing NUL.
        let mut query_text = vec![0u8; query_len as usize + 1];
        r.read_exact(&mut query_text)?;
        query_text.pop();

        let mut words = [0u64; COUNTER_WORDS];
        for (i, w) in words.iter_mut().enumerate() {
            *w = ne_u64(&rec[ENTRY_COUNTERS_OFF + 8 * i..]);
        }
        let counters = counters_from_words(&words);
        // C skips loading sticky entries.
        if counters.is_sticky() {
            continue;
        }
        let key = PgssHashKey {
            userid: ne_u32(&rec[0..4]),
            dbid: ne_u32(&rec[4..8]),
            queryid: ne_i64(&rec[8..16]),
            toplevel: rec[16] != 0,
        };
        entry_alloc(shared, key, &query_text, encoding, false);
        if let Some(e) = shared.hash.get_mut(&key) {
            e.counters = counters;
            e.stats_since = ne_i64(&rec[ENTRY_STATS_SINCE_OFF..]);
            e.minmax_stats_since = ne_i64(&rec[ENTRY_MINMAX_SINCE_OFF..]);
        }
    }
    let mut stats = [0u8; GLOBAL_STATS_SIZE];
    r.read_exact(&mut stats)?;
    shared.stats.dealloc = ne_i64(&stats[0..8]);
    shared.stats.stats_reset = ne_i64(&stats[8..16]);
    Ok(true)
}

/// `pgss_shmem_startup`'s dump-file load half.
pub(crate) fn load_dump_file(shared: &mut PgssShared) {
    let Some(path) = dump_path() else { return };
    let mut file = match std::fs::File::open(&path) {
        Ok(f) => f,
        // No existing persisted stats file, so we're done.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            log_file_error("read", PGSS_DUMP_FILE, &e, "pgss_shmem_startup");
            return;
        }
    };

    match read_dump(&mut file, shared) {
        // Remove the persisted file so backups/standbys don't inherit it;
        // a new one is written on next clean shutdown.
        Ok(true) => {}
        Ok(false) => {
            let _ = ereport(LOG)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("ignoring invalid data in file \"{PGSS_DUMP_FILE}\""))
                .finish(here("pgss_shmem_startup"));
        }
        Err(e) => log_file_error("read", PGSS_DUMP_FILE, &e, "pgss_shmem_startup"),
    }
    drop(file);
    // C: unlink(PGSS_DUMP_FILE) on every arm; errors ignored.
    let _ = std::fs::remove_file(&path);
}

/// `pgss_shmem_shutdown` (on_shmem_exit): dump to disk on clean shutdown.
pub(crate) fn pgss_shmem_shutdown(code: i32, _arg: usize) {
    if code != 0 {
        return;
    }
    if !gucs::pgss_save() {
        return;
    }
    let guard = PGSS.lock().unwrap();
    let Some(shared) = guard.as_ref() else { return };
    let Some(path) = dump_path() else { return };
    let tmp = path.with_extension("stat.tmp");
    let tmp_name = format!("{PGSS_DUMP_FILE}.tmp");

    let write = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        write_dump(&mut f, shared)?;
        // durable_rename's fsync of the old file before the rename.
        f.sync_all()?;
        Ok(())
    })();
    if let Err(e) = write {
        log_file_error("write", &tmp_name, &e, "pgss_shmem_shutdown");
        let _ = std::fs::remove_file(&tmp);
        return;
    }

    // Rename file into place, so we atomically replace any old one
    // (durable_rename(..., LOG): a failure is logged, the .tmp is left).
    if let Err(e) = std::fs::rename(&tmp, &path) {
        let _ = ereport(LOG)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!(
                "could not rename file \"{tmp_name}\" to \"{PGSS_DUMP_FILE}\": %m"
            ))
            .finish(here("pgss_shmem_shutdown"));
    }
}
