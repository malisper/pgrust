//! Entry storage: pgss_store's counter accumulation (Welford variance),
//! LRU-by-usage eviction, reset, and the pgss_save dump file. The C dump
//! layout (header, major version, entry records) is kept, but entry records
//! are written field-by-field little-endian rather than as raw C structs.

use std::io::{Read, Write};

use elog::elog;
use types_core::instrument::{instr_time, BufferUsage, WalUsage};
use types_error::LOG;

use crate::{
    gucs, nesting_level, Counters, PgssEntry, PgssGlobalStats, PgssHashKey, PgssShared,
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
        entry_alloc(shared, key, text, encoding, jstate.is_some());
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
    query_text: &str,
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

pub(crate) const COUNTER_WORDS: usize = 6 * PGSS_NUMKIND + 34;

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

/// `pgss_shmem_startup`'s dump-file load half.
pub(crate) fn load_dump_file(shared: &mut PgssShared) {
    let Some(path) = dump_path() else { return };
    let mut file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(_) => {
            let _ = elog(LOG, format!("could not read file \"{}\"", path.display()));
            return;
        }
    };

    let ok = (|| -> std::io::Result<bool> {
        let mut u32buf = [0u8; 4];
        file.read_exact(&mut u32buf)?;
        let header = u32::from_le_bytes(u32buf);
        file.read_exact(&mut u32buf)?;
        let pgver = u32::from_le_bytes(u32buf);
        file.read_exact(&mut u32buf)?;
        let num = i32::from_le_bytes(u32buf);
        if header != PGSS_FILE_HEADER || pgver != PGSS_PG_MAJOR_VERSION || num < 0 {
            return Ok(false);
        }
        let mut u64buf = [0u8; 8];
        for _ in 0..num {
            file.read_exact(&mut u32buf)?;
            let userid = u32::from_le_bytes(u32buf);
            file.read_exact(&mut u32buf)?;
            let dbid = u32::from_le_bytes(u32buf);
            file.read_exact(&mut u64buf)?;
            let queryid = i64::from_le_bytes(u64buf);
            file.read_exact(&mut u32buf)?;
            let toplevel = match u32::from_le_bytes(u32buf) {
                0 => false,
                1 => true,
                _ => return Ok(false),
            };
            let mut words = [0u64; COUNTER_WORDS];
            for w in words.iter_mut() {
                file.read_exact(&mut u64buf)?;
                *w = u64::from_le_bytes(u64buf);
            }
            file.read_exact(&mut u64buf)?;
            let stats_since = i64::from_le_bytes(u64buf);
            file.read_exact(&mut u64buf)?;
            let minmax_stats_since = i64::from_le_bytes(u64buf);
            file.read_exact(&mut u32buf)?;
            let encoding = i32::from_le_bytes(u32buf);
            file.read_exact(&mut u32buf)?;
            let qlen = u32::from_le_bytes(u32buf) as usize;
            if qlen > 1024 * 1024 * 1024 {
                return Ok(false);
            }
            let mut qbuf = vec![0u8; qlen];
            file.read_exact(&mut qbuf)?;
            let Ok(query_text) = String::from_utf8(qbuf) else { return Ok(false) };

            let counters = counters_from_words(&words);
            // C skips loading sticky entries.
            if counters.is_sticky() {
                continue;
            }
            let key = PgssHashKey { userid, dbid, queryid, toplevel };
            entry_alloc(shared, key, &query_text, encoding, false);
            if let Some(e) = shared.hash.get_mut(&key) {
                e.counters = counters;
                e.stats_since = stats_since;
                e.minmax_stats_since = minmax_stats_since;
            }
        }
        file.read_exact(&mut u64buf)?;
        shared.stats.dealloc = i64::from_le_bytes(u64buf);
        file.read_exact(&mut u64buf)?;
        shared.stats.stats_reset = i64::from_le_bytes(u64buf);
        Ok(true)
    })();

    match ok {
        Ok(true) => {
            // Remove the persisted file so backups/standbys don't inherit it;
            // a new one is written on next clean shutdown.
            let _ = std::fs::remove_file(&path);
        }
        Ok(false) => {
            let _ = elog(LOG, format!("ignoring invalid data in file \"{}\"", path.display()));
            let _ = std::fs::remove_file(&path);
        }
        Err(_) => {
            let _ = elog(LOG, format!("could not read file \"{}\"", path.display()));
            let _ = std::fs::remove_file(&path);
        }
    }
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

    let write = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&PGSS_FILE_HEADER.to_le_bytes())?;
        f.write_all(&PGSS_PG_MAJOR_VERSION.to_le_bytes())?;
        f.write_all(&(shared.hash.len() as i32).to_le_bytes())?;
        for (k, e) in shared.hash.iter() {
            f.write_all(&k.userid.to_le_bytes())?;
            f.write_all(&k.dbid.to_le_bytes())?;
            f.write_all(&k.queryid.to_le_bytes())?;
            f.write_all(&u32::from(k.toplevel).to_le_bytes())?;
            for w in counters_to_words(&e.counters) {
                f.write_all(&w.to_le_bytes())?;
            }
            f.write_all(&e.stats_since.to_le_bytes())?;
            f.write_all(&e.minmax_stats_since.to_le_bytes())?;
            f.write_all(&e.encoding.to_le_bytes())?;
            f.write_all(&(e.query_text.len() as u32).to_le_bytes())?;
            f.write_all(e.query_text.as_bytes())?;
        }
        f.write_all(&shared.stats.dealloc.to_le_bytes())?;
        f.write_all(&shared.stats.stats_reset.to_le_bytes())?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, &path)?;
        Ok(())
    })();

    if write.is_err() {
        let _ = elog(LOG, format!("could not write file \"{}\"", tmp.display()));
        let _ = std::fs::remove_file(&tmp);
    }
}
