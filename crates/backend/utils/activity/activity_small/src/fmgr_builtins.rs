//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the global
//! checkpointer / bgwriter SQL-callable accessors in
//! `src/backend/utils/adt/pgstatfuncs.c`.
//!
//! Each function takes no arguments and projects one field of the cluster-wide
//! `PgStat_CheckpointerStats` / `PgStat_BgWriterStats` snapshot, fetched via
//! [`crate::pgstat_checkpointer::pgstat_fetch_stat_checkpointer`] /
//! [`crate::pgstat_bgwriter::pgstat_fetch_stat_bgwriter`]. None can return NULL
//! (the C bodies are unconditional `PG_RETURN_*`).
//!
//! Checkpointer `write_time`/`sync_time` are already stored in milliseconds and
//! returned as `float8` (just an `(int64) → double` widening). Reset timestamps
//! are returned as `timestamptz` directly (no NULL-when-zero collapse — matching
//! the C `PG_RETURN_TIMESTAMPTZ`).
//!
//! OIDs / nargs (0) / strict (false; no args) / retset (false) are transcribed
//! exactly from `pg_proc.dat`.

use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::resolution::BuiltinFunction;
use ::fmgr::{FunctionCallInfoBaseData, PgFnNative};

use crate::pgstat_bgwriter::pgstat_fetch_stat_bgwriter;
use crate::pgstat_checkpointer::pgstat_fetch_stat_checkpointer;

// ---------------------------------------------------------------------------
// Checkpointer (INT64).
// ---------------------------------------------------------------------------

fn fc_ckpt_num_timed(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(pgstat_fetch_stat_checkpointer()?.num_timed))
}
fn fc_ckpt_num_requested(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(pgstat_fetch_stat_checkpointer()?.num_requested))
}
fn fc_ckpt_num_performed(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(pgstat_fetch_stat_checkpointer()?.num_performed))
}
fn fc_ckpt_restartpoints_timed(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(
        pgstat_fetch_stat_checkpointer()?.restartpoints_timed,
    ))
}
fn fc_ckpt_restartpoints_requested(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(
        pgstat_fetch_stat_checkpointer()?.restartpoints_requested,
    ))
}
fn fc_ckpt_restartpoints_performed(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(
        pgstat_fetch_stat_checkpointer()?.restartpoints_performed,
    ))
}
fn fc_ckpt_buffers_written(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(
        pgstat_fetch_stat_checkpointer()?.buffers_written,
    ))
}
fn fc_ckpt_slru_written(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(pgstat_fetch_stat_checkpointer()?.slru_written))
}

// Checkpointer (FLOAT8; time already in msec, widened to double).
fn fc_ckpt_write_time(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_f64(
        pgstat_fetch_stat_checkpointer()?.write_time as f64,
    ))
}
fn fc_ckpt_sync_time(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_f64(
        pgstat_fetch_stat_checkpointer()?.sync_time as f64,
    ))
}

// Checkpointer (TIMESTAMPTZ; returned directly, no NULL collapse).
fn fc_ckpt_stat_reset_time(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(
        pgstat_fetch_stat_checkpointer()?.stat_reset_timestamp,
    ))
}

// ---------------------------------------------------------------------------
// Bgwriter (INT64 + TIMESTAMPTZ + buf_alloc).
// ---------------------------------------------------------------------------

fn fc_bgwriter_buf_written_clean(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(
        pgstat_fetch_stat_bgwriter()?.buf_written_clean,
    ))
}
fn fc_bgwriter_maxwritten_clean(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(pgstat_fetch_stat_bgwriter()?.maxwritten_clean))
}
fn fc_bgwriter_stat_reset_time(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(
        pgstat_fetch_stat_bgwriter()?.stat_reset_timestamp,
    ))
}
fn fc_buf_alloc(_fc: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(Datum::from_i64(pgstat_fetch_stat_bgwriter()?.buf_alloc))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(foid: u32, name: &str, native: PgFnNative) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs: 0,
            // pg_proc.dat omits proisstrict for these 0-arg functions, so the
            // catalog default (`proisstrict => 't'`) applies.
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register every global checkpointer / bgwriter `pg_stat_get_*` builtin
/// (C: their `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
pub fn register_pgstat_checkpointer_bgwriter_builtins() {
    fmgr_core::register_builtins_native([
        // Checkpointer INT64
        builtin(2769, "pg_stat_get_checkpointer_num_timed", fc_ckpt_num_timed),
        builtin(
            2770,
            "pg_stat_get_checkpointer_num_requested",
            fc_ckpt_num_requested,
        ),
        builtin(
            6377,
            "pg_stat_get_checkpointer_num_performed",
            fc_ckpt_num_performed,
        ),
        builtin(
            6327,
            "pg_stat_get_checkpointer_restartpoints_timed",
            fc_ckpt_restartpoints_timed,
        ),
        builtin(
            6328,
            "pg_stat_get_checkpointer_restartpoints_requested",
            fc_ckpt_restartpoints_requested,
        ),
        builtin(
            6329,
            "pg_stat_get_checkpointer_restartpoints_performed",
            fc_ckpt_restartpoints_performed,
        ),
        builtin(
            2771,
            "pg_stat_get_checkpointer_buffers_written",
            fc_ckpt_buffers_written,
        ),
        builtin(
            6366,
            "pg_stat_get_checkpointer_slru_written",
            fc_ckpt_slru_written,
        ),
        // Checkpointer FLOAT8
        builtin(3160, "pg_stat_get_checkpointer_write_time", fc_ckpt_write_time),
        builtin(3161, "pg_stat_get_checkpointer_sync_time", fc_ckpt_sync_time),
        // Checkpointer TIMESTAMPTZ
        builtin(
            6314,
            "pg_stat_get_checkpointer_stat_reset_time",
            fc_ckpt_stat_reset_time,
        ),
        // Bgwriter
        builtin(
            2772,
            "pg_stat_get_bgwriter_buf_written_clean",
            fc_bgwriter_buf_written_clean,
        ),
        builtin(
            2773,
            "pg_stat_get_bgwriter_maxwritten_clean",
            fc_bgwriter_maxwritten_clean,
        ),
        builtin(
            3075,
            "pg_stat_get_bgwriter_stat_reset_time",
            fc_bgwriter_stat_reset_time,
        ),
        builtin(2859, "pg_stat_get_buf_alloc", fc_buf_alloc),
    ]);
}
