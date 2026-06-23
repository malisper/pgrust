//! The set-returning `pg_stat_statements()` view function + the
//! `pg_stat_statements_info()` record function.
//!
//! pgss's SQL functions are resolved as bare `PGFunction`s through the
//! dynamic-loader builtin-library registry; a SETOF function reached this way
//! runs in **materialize mode** via the `::fmgr::mat_srf` sink (the
//! `dispatch_user_setof` path in execSRF). So the view function reads the
//! caller's expected descriptor (for the API-version column count) and appends
//! each row to the sink, rather than using `InitMaterializedSRF`.

use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use ::fmgr::mat_srf::{self, MatCell};
use ::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use crate::shmem::{self, entry_ref, pgss_hash, pgss_ref};
use crate::{PgssVersion, PGSS_EXEC, PGSS_NUMKIND};

// Column counts per API version (pg_stat_statements.c:1556).
const COLS_V1_0: usize = 14;
const COLS_V1_1: usize = 18;
const COLS_V1_2: usize = 19;
const COLS_V1_3: usize = 23;
const COLS_V1_8: usize = 32;
const COLS_V1_9: usize = 33;
const COLS_V1_10: usize = 43;
const COLS_V1_11: usize = 49;
const COLS_V1_12: usize = 52;

/// `ROLE_PG_READ_ALL_STATS` (pg_authid).
const ROLE_PG_READ_ALL_STATS: types_core::Oid = 3375;

// Type OIDs for the by-ref columns.
const TEXTOID: types_core::Oid = 25;
const NUMERICOID: types_core::Oid = 1700;

/// `pg_stat_statements_internal` (pg_stat_statements.c:1661).
pub(crate) fn pg_stat_statements_internal(
    fcinfo: &mut FunctionCallInfoBaseData,
    api_version: PgssVersion,
    showtext: bool,
) -> PgResult<()> {
    use ::utils_error::ereport;
    use ::types_error::{ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR};

    let userid = miscinit::GetUserId();
    let is_allowed_role =
        acl_seams::has_privs_of_role::call(userid, ROLE_PG_READ_ALL_STATS)?;

    if !shmem::is_initialized() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("pg_stat_statements must be loaded via \"shared_preload_libraries\"")
            .into_error());
    }

    // The expected descriptor's column count tells us the SQL declaration's API.
    let natts = mat_srf::with_top(|sink| sink.map(|s| s.expected_desc_cols.len()).unwrap_or(0));

    let api_version = resolve_api_version(api_version, natts)?;

    // The C `CStringGetTextDatum` / `numeric_in` palloc in the per-query context;
    // here a transient context backs the per-column varlena images, which are
    // copied (owned) into the materialize sink immediately, so it can be dropped
    // at function end.
    let scratch = mcx::MemoryContext::new("pg_stat_statements view");
    let mcx = scratch.mcx();
    let _ = fcinfo;

    let pgss = unsafe { pgss_ref() };
    let pgss_hash = pgss_hash();
    let lock = pgss.lock;

    // Load the query-texts file (best effort) before taking the lock.
    let mut qbuffer: Option<Vec<u8>> = None;
    if showtext {
        shmem::spin_lock_acquire(&pgss.mutex);
        let n_writers = pgss.n_writers;
        shmem::spin_lock_release(&pgss.mutex);
        if n_writers == 0 {
            qbuffer = crate::qtext::qtext_load_file();
        }
    }

    // SAFETY: lock addresses the shared LWLock array.
    let lock_ref = unsafe { &*lock };
    lwlock::LWLockAcquire(
        lock_ref,
        types_storage::storage::LWLockMode::LW_SHARED,
        init_small_seams::my_proc_number::call(),
    )?;
    if showtext && qbuffer.is_none() {
        qbuffer = crate::qtext::qtext_load_file();
    }

    let qbuf = qbuffer.unwrap_or_default();

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
        let entry = unsafe { entry_ref(ptr) };

        let mut row: Vec<MatCell> = Vec::with_capacity(natts);
        let queryid = entry.key.queryid;

        push_val(&mut row, Datum::from_oid(entry.key.userid));
        push_val(&mut row, Datum::from_oid(entry.key.dbid));
        if api_version >= PgssVersion::V1_9 {
            push_val(&mut row, Datum::from_bool(entry.key.toplevel));
        }

        if is_allowed_role || entry.key.userid == userid {
            if api_version >= PgssVersion::V1_2 {
                push_val(&mut row, Datum::from_i64(queryid));
            }
            if showtext {
                let qstr = crate::qtext::qtext_fetch(entry.query_offset, entry.query_len, &qbuf);
                match qstr {
                    Some(s) => {
                        // pg_any_to_server is the identity for the encodings we
                        // run under (UTF-8/SQL_ASCII); store the bytes as text.
                        let text = String::from_utf8_lossy(s).into_owned();
                        push_text(&mut row, mcx, &text)?;
                    }
                    None => push_null(&mut row),
                }
            } else {
                push_null(&mut row);
            }
        } else {
            if api_version >= PgssVersion::V1_2 {
                push_null(&mut row);
            }
            if showtext {
                push_text(&mut row, mcx, "<insufficient privilege>")?;
            } else {
                push_null(&mut row);
            }
        }

        // Copy counters under the entry spinlock.
        shmem::spin_lock_acquire(&entry.mutex);
        let tmp = entry.counters;
        shmem::spin_lock_release(&entry.mutex);

        let stats_since = entry.stats_since;
        let minmax_stats_since = entry.minmax_stats_since;

        // Skip pending sticky entries.
        if tmp.is_sticky() {
            continue;
        }

        for kind in 0..PGSS_NUMKIND {
            if kind == PGSS_EXEC || api_version >= PgssVersion::V1_8 {
                push_val(&mut row, Datum::from_i64(tmp.calls[kind]));
                push_val(&mut row, Datum::from_f64(tmp.total_time[kind]));
            }
            if (kind == PGSS_EXEC && api_version >= PgssVersion::V1_3)
                || api_version >= PgssVersion::V1_8
            {
                push_val(&mut row, Datum::from_f64(tmp.min_time[kind]));
                push_val(&mut row, Datum::from_f64(tmp.max_time[kind]));
                push_val(&mut row, Datum::from_f64(tmp.mean_time[kind]));
                let stddev = if tmp.calls[kind] > 1 {
                    (tmp.sum_var_time[kind] / tmp.calls[kind] as f64).sqrt()
                } else {
                    0.0
                };
                push_val(&mut row, Datum::from_f64(stddev));
            }
        }
        push_val(&mut row, Datum::from_i64(tmp.rows));
        push_val(&mut row, Datum::from_i64(tmp.shared_blks_hit));
        push_val(&mut row, Datum::from_i64(tmp.shared_blks_read));
        if api_version >= PgssVersion::V1_1 {
            push_val(&mut row, Datum::from_i64(tmp.shared_blks_dirtied));
        }
        push_val(&mut row, Datum::from_i64(tmp.shared_blks_written));
        push_val(&mut row, Datum::from_i64(tmp.local_blks_hit));
        push_val(&mut row, Datum::from_i64(tmp.local_blks_read));
        if api_version >= PgssVersion::V1_1 {
            push_val(&mut row, Datum::from_i64(tmp.local_blks_dirtied));
        }
        push_val(&mut row, Datum::from_i64(tmp.local_blks_written));
        push_val(&mut row, Datum::from_i64(tmp.temp_blks_read));
        push_val(&mut row, Datum::from_i64(tmp.temp_blks_written));
        if api_version >= PgssVersion::V1_1 {
            push_val(&mut row, Datum::from_f64(tmp.shared_blk_read_time));
            push_val(&mut row, Datum::from_f64(tmp.shared_blk_write_time));
        }
        if api_version >= PgssVersion::V1_11 {
            push_val(&mut row, Datum::from_f64(tmp.local_blk_read_time));
            push_val(&mut row, Datum::from_f64(tmp.local_blk_write_time));
        }
        if api_version >= PgssVersion::V1_10 {
            push_val(&mut row, Datum::from_f64(tmp.temp_blk_read_time));
            push_val(&mut row, Datum::from_f64(tmp.temp_blk_write_time));
        }
        if api_version >= PgssVersion::V1_8 {
            push_val(&mut row, Datum::from_i64(tmp.wal_records));
            push_val(&mut row, Datum::from_i64(tmp.wal_fpi));
            // wal_bytes as numeric.
            push_numeric(&mut row, mcx, &tmp.wal_bytes.to_string())?;
        }
        if api_version >= PgssVersion::V1_12 {
            push_val(&mut row, Datum::from_i64(tmp.wal_buffers_full));
        }
        if api_version >= PgssVersion::V1_10 {
            push_val(&mut row, Datum::from_i64(tmp.jit_functions));
            push_val(&mut row, Datum::from_f64(tmp.jit_generation_time));
            push_val(&mut row, Datum::from_i64(tmp.jit_inlining_count));
            push_val(&mut row, Datum::from_f64(tmp.jit_inlining_time));
            push_val(&mut row, Datum::from_i64(tmp.jit_optimization_count));
            push_val(&mut row, Datum::from_f64(tmp.jit_optimization_time));
            push_val(&mut row, Datum::from_i64(tmp.jit_emission_count));
            push_val(&mut row, Datum::from_f64(tmp.jit_emission_time));
        }
        if api_version >= PgssVersion::V1_11 {
            push_val(&mut row, Datum::from_i64(tmp.jit_deform_count));
            push_val(&mut row, Datum::from_f64(tmp.jit_deform_time));
        }
        if api_version >= PgssVersion::V1_12 {
            push_val(&mut row, Datum::from_i64(tmp.parallel_workers_to_launch));
            push_val(&mut row, Datum::from_i64(tmp.parallel_workers_launched));
        }
        if api_version >= PgssVersion::V1_11 {
            push_val(&mut row, Datum::from_i64(stats_since));
            push_val(&mut row, Datum::from_i64(minmax_stats_since));
        }

        mat_srf::with_top(|sink| {
            if let Some(sink) = sink {
                sink.materialized = true;
                sink.rows.push(row);
            }
        });
    }

    lwlock::LWLockRelease(lock_ref)?;
    Ok(())
}

/// Detect the API version from the SQL declaration's natts (the C switch in
/// pg_stat_statements_internal). Maps the 1.1 special-case.
fn resolve_api_version(requested: PgssVersion, natts: usize) -> PgResult<PgssVersion> {
    let v = match natts {
        COLS_V1_0 => Some(PgssVersion::V1_0),
        COLS_V1_1 => Some(PgssVersion::V1_1),
        COLS_V1_2 => Some(PgssVersion::V1_2),
        COLS_V1_3 => Some(PgssVersion::V1_3),
        COLS_V1_8 => Some(PgssVersion::V1_8),
        COLS_V1_9 => Some(PgssVersion::V1_9),
        COLS_V1_10 => Some(PgssVersion::V1_10),
        COLS_V1_11 => Some(PgssVersion::V1_11),
        COLS_V1_12 => Some(PgssVersion::V1_12),
        _ => None,
    };
    match v {
        // 1.1 should have been requested as 1.0 (legacy entry point).
        Some(PgssVersion::V1_1) if requested == PgssVersion::V1_0 => Ok(PgssVersion::V1_1),
        Some(ver) if ver == requested => Ok(ver),
        _ => Err(::types_error::PgError::error(
            "incorrect number of output arguments",
        )),
    }
}

fn push_val(row: &mut Vec<MatCell>, d: Datum) {
    row.push(MatCell {
        value: d.as_usize(),
        ref_payload: None,
        isnull: false,
    });
}

fn push_null(row: &mut Vec<MatCell>) {
    row.push(MatCell {
        value: 0,
        ref_payload: None,
        isnull: true,
    });
}

fn push_text(row: &mut Vec<MatCell>, mcx: mcx::Mcx<'_>, s: &str) -> PgResult<()> {
    let image = varlena::keystone::cstring_to_text(mcx, s.as_bytes())?;
    row.push(MatCell {
        value: 0,
        ref_payload: Some(RefPayload::Varlena(image.as_slice().to_vec())),
        isnull: false,
    });
    let _ = TEXTOID;
    Ok(())
}

fn push_numeric(row: &mut Vec<MatCell>, mcx: mcx::Mcx<'_>, s: &str) -> PgResult<()> {
    let image = small1_seams::numeric_in::call(mcx, s)?;
    row.push(MatCell {
        value: 0,
        ref_payload: Some(RefPayload::Varlena(image.as_slice().to_vec())),
        isnull: false,
    });
    let _ = NUMERICOID;
    Ok(())
}

/// `pg_stat_statements_info()` (pg_stat_statements.c:2018) — build a 2-col record
/// (dealloc, stats_reset) and return its self-contained composite varlena image
/// (the caller sets `fcinfo.ref_result`).
pub(crate) fn pg_stat_statements_info(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Vec<u8>> {
    use ::utils_error::ereport;
    use ::types_error::{ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR};

    if !shmem::is_initialized() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("pg_stat_statements must be loaded via \"shared_preload_libraries\"")
            .into_error());
    }

    let pgss = unsafe { pgss_ref() };
    shmem::spin_lock_acquire(&pgss.mutex);
    let dealloc = pgss.stats.dealloc;
    let stats_reset = pgss.stats.stats_reset;
    shmem::spin_lock_release(&pgss.mutex);

    let scratch = mcx::MemoryContext::new("pg_stat_statements_info");
    let mcx = scratch.mcx();
    let values = [Datum::from_i64(dealloc), Datum::from_i64(stats_reset)];
    let nulls = [false, false];
    let coltypes = [20, 1184];
    let rec = funcapi_seams::record_from_values::call(
        mcx, &coltypes, &values, &nulls,
    )?;
    // Copy the self-contained composite image out (owned) before the scratch
    // context drops.
    Ok(rec.as_ref_bytes().to_vec())
}
