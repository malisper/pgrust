//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `xlogfuncs.c`
//! recovery-control SQL-callable functions whose argument/result types are
//! expressible at the current fmgr boundary (`bool`, `int4`, `text`).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result word / by-reference payload. [`register_xlogfuncs_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch (and the `fmgr_isbuiltin` fast path) resolves them. OIDs / nargs /
//! strict / retset are transcribed exactly from `pg_proc.dat`.
//!
//! The recovery-control functions (`pg_wal_replay_pause`/`pg_wal_replay_resume`
//! return `void`, `pg_is_wal_replay_paused`/`pg_is_in_recovery`/`pg_promote`
//! return `bool`, `pg_get_wal_replay_pause_state` returns `text`) are registered
//! here, along with the WAL-control / current-LSN functions whose `pg_lsn`
//! (`XLogRecPtr`, an 8-byte by-value word — `LSNGetDatum`/`Datum::from_u64`) or
//! `timestamptz` results ARE expressible at the boundary: `pg_current_wal_lsn`,
//! `pg_current_wal_insert_lsn`, `pg_current_wal_flush_lsn`, `pg_switch_wal`,
//! `pg_create_restore_point(text)`, `pg_last_wal_receive_lsn`,
//! `pg_last_wal_replay_lsn`, `pg_last_xact_replay_timestamp`, and
//! `pg_log_standby_snapshot`. The online base-backup control functions
//! `pg_backup_start(text, bool) -> pg_lsn` and `pg_backup_stop(bool) ->
//! record(lsn, labelfile, spcmapfile)` are also registered: the long-lived
//! `(BackupState, tablespace_map)` that C keeps in file-scope statics between the
//! two SQL calls is mirrored in this module's session thread-locals, and the
//! 3-column `pg_backup_stop` record crosses the by-reference `Composite` lane via
//! `funcapi::record_from_values` (the same record carrier `pg_create_*_replication_slot`
//! / `pg_get_object_address` use). The remaining `xlogfuncs.c` SQL functions are
//! NOT registered: the file-name / LSN-diff functions return `numeric` or
//! composite rows whose `Mcx`-built varlena/composite the fmgr boundary cannot
//! yet carry for those shapes.

use ::types_core::{Oid, TimestampTz, XLogRecPtr};
use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use ::types_tuple::Datum as DatumV;

/// C: `LSNOID` (`pg_type.h`) — the `pg_lsn` result-column type Oid.
const LSNOID: Oid = 3220;
/// C: `TEXTOID` (`pg_type.h`) — the `text` result-column type Oid.
const TEXTOID: Oid = 25;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_BOOL(i)` → `DatumGetBool`: any nonzero word reads back as `true`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .arg(i)
        .expect("xlogfuncs fn: missing arg")
        .value
        .as_bool()
}

/// `PG_GETARG_TEXT_PP(i)` → `text_to_cstring`: a `text` arg's `VARDATA_ANY`
/// payload bytes on the by-ref lane, decoded as UTF-8 (C:
/// `pg_create_restore_point` does `text_to_cstring(PG_GETARG_TEXT_PP(0))`).
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("xlogfuncs fn: text arg missing from by-ref lane");
    let bytes = vardata_any(image);
    core::str::from_utf8(bytes).expect("xlogfuncs fn: text arg not valid UTF-8")
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image:
/// skip ONE header byte for a short (1-byte, low-bit-set) header, else `VARHDRSZ`
/// (4). A small stored value arrives short-headed once `SHORT_VARLENA_PACKING` is
/// on; a fixed 4-byte strip would drop three payload bytes. No-op while off.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= 4 => &image[4..],
        _ => &[],
    }
}

/// `PG_RETURN_LSN(v)`: a `pg_lsn`/`XLogRecPtr` result word (C: `LSNGetDatum`
/// over the 8-byte by-value `XLogRecPtr`).
#[inline]
fn ret_lsn(v: XLogRecPtr) -> Datum {
    Datum::from_u64(v)
}

/// An `Option<XLogRecPtr>` result: `None` is SQL NULL (C: `PG_RETURN_NULL()`
/// when `GetXLogReplayRecPtr`/`GetWalRcvFlushRecPtr` returns `InvalidXLogRecPtr`).
#[inline]
fn ret_lsn_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<XLogRecPtr>) -> Datum {
    match v {
        Some(l) => ret_lsn(l),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// An `Option<TimestampTz>` result: `None` is SQL NULL (C: `PG_RETURN_NULL()`
/// when `GetLatestXTime` returns 0).
#[inline]
fn ret_timestamptz_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<TimestampTz>) -> Datum {
    match v {
        Some(t) => Datum::from_i64(t),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// `PG_GETARG_LSN(i)` → `DatumGetLSN`: a `pg_lsn`/`XLogRecPtr` 8-byte by-value
/// word.
#[inline]
fn arg_lsn(fcinfo: &FunctionCallInfoBaseData, i: usize) -> XLogRecPtr {
    fcinfo
        .arg(i)
        .expect("xlogfuncs fn: missing pg_lsn arg")
        .value
        .as_u64()
}

/// `PG_GETARG_INT32(i)` → `DatumGetInt32`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .expect("xlogfuncs fn: missing arg")
        .value
        .as_i32()
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Set a `text` (`PG_RETURN_TEXT_P`) result on the by-ref lane and return the
/// dummy word. `bytes` is the header-LESS payload the `cstring_to_text` core
/// returns (the keystone carrier is the bare payload); under the
/// header-ful-everywhere `RefPayload::Varlena` convention this stamps the 4-byte
/// uncompressed varlena length word (`SET_VARSIZE`) in front, symmetric with how
/// `arg_text` reads a `text` arg back (skipping the header).
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    const VARHDRSZ: usize = 4;
    let mut image = Vec::with_capacity(bytes.len() + VARHDRSZ);
    image.extend_from_slice(&::datum::varlena::set_varsize_4b(bytes.len() + VARHDRSZ));
    image.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// returned varlena bytes are copied out onto the by-ref lane before this
/// context is dropped, so the result outlives the scratch arena.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("xlogfuncs fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_is_wal_replay_paused()` (xlogfuncs.c:572).
fn fc_pg_is_wal_replay_paused(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::pg_is_wal_replay_paused()?))
}

/// `pg_get_wal_replay_pause_state()` (xlogfuncs.c:593) — a `text` result.
fn fc_pg_get_wal_replay_pause_state(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    // Copy the varlena image out of the scratch arena onto the by-ref lane
    // (C: PG_RETURN_TEXT_P over a palloc'd varlena) before the arena is dropped.
    let bytes: Vec<u8> = crate::pg_get_wal_replay_pause_state(m.mcx())?
        .as_slice()
        .to_vec();
    Ok(ret_text(fcinfo, bytes))
}

/// `pg_wal_replay_pause()` (xlogfuncs.c:518) — `void` result
/// (C: `PG_RETURN_VOID()` = Datum 0).
fn fc_pg_wal_replay_pause(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    crate::pg_wal_replay_pause()?;
    Ok(Datum::from_usize(0))
}

/// `pg_wal_replay_resume()` (xlogfuncs.c:548) — `void` result
/// (C: `PG_RETURN_VOID()` = Datum 0).
fn fc_pg_wal_replay_resume(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    crate::pg_wal_replay_resume()?;
    Ok(Datum::from_usize(0))
}

/// `pg_is_in_recovery()` (xlogfuncs.c:643) — infallible `bool`.
fn fc_pg_is_in_recovery(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::pg_is_in_recovery()))
}

/// `pg_promote(wait bool, wait_seconds int4)` (xlogfuncs.c:670).
fn fc_pg_promote(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let wait = arg_bool(fcinfo, 0);
    let wait_seconds = arg_int32(fcinfo, 1);
    Ok(ret_bool(crate::pg_promote(wait, wait_seconds)?))
}

/// `pg_current_wal_lsn()` (xlogfuncs.c) — `pg_lsn` result.
fn fc_pg_current_wal_lsn(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_lsn(crate::pg_current_wal_lsn()?))
}

/// `pg_current_wal_insert_lsn()` (xlogfuncs.c) — `pg_lsn` result.
fn fc_pg_current_wal_insert_lsn(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    Ok(ret_lsn(crate::pg_current_wal_insert_lsn()?))
}

/// `pg_current_wal_flush_lsn()` (xlogfuncs.c) — `pg_lsn` result.
fn fc_pg_current_wal_flush_lsn(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    Ok(ret_lsn(crate::pg_current_wal_flush_lsn()?))
}

/// `pg_switch_wal()` (xlogfuncs.c) — `pg_lsn` result.
fn fc_pg_switch_wal(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_lsn(crate::pg_switch_wal()?))
}

/// `pg_create_restore_point(text)` (xlogfuncs.c) — `pg_lsn` result.
fn fc_pg_create_restore_point(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let name = arg_text(fcinfo, 0);
    Ok(ret_lsn(crate::pg_create_restore_point(name)?))
}

/// `pg_last_wal_receive_lsn()` (xlogfuncs.c) — NULL-able `pg_lsn` result.
fn fc_pg_last_wal_receive_lsn(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    Ok(ret_lsn_opt(fcinfo, crate::pg_last_wal_receive_lsn()))
}

/// `pg_last_wal_replay_lsn()` (xlogfuncs.c) — NULL-able `pg_lsn` result.
fn fc_pg_last_wal_replay_lsn(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    Ok(ret_lsn_opt(fcinfo, crate::pg_last_wal_replay_lsn()))
}

/// `pg_last_xact_replay_timestamp()` (xlogfuncs.c) — NULL-able `timestamptz`.
fn fc_pg_last_xact_replay_timestamp(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    Ok(ret_timestamptz_opt(
        fcinfo,
        crate::pg_last_xact_replay_timestamp(),
    ))
}

/// `pg_log_standby_snapshot()` (xlogfuncs.c) — `pg_lsn` result.
fn fc_pg_log_standby_snapshot(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    Ok(ret_lsn(crate::pg_log_standby_snapshot()?))
}

/// `pg_walfile_name(lsn pg_lsn)` (xlogfuncs.c:438) — a `text` result. The core
/// builds the WAL file name varlena (`PG_RETURN_TEXT_P(cstring_to_text(...))`)
/// in the scratch arena; copy its image out onto the by-ref lane before the
/// arena drops.
fn fc_pg_walfile_name(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: locationpoint = PG_GETARG_LSN(0) — a by-value uint64.
    let locationpoint = arg_lsn(fcinfo, 0);
    let m = scratch_mcx();
    let bytes: Vec<u8> = crate::pg_walfile_name(m.mcx(), locationpoint)?
        .as_slice()
        .to_vec();
    Ok(ret_text(fcinfo, bytes))
}

// ---------------------------------------------------------------------------
// pg_backup_start / pg_backup_stop — session-persistent backup statics.
// ---------------------------------------------------------------------------

// C `xlogfuncs.c` keeps the long-lived `backup_state` / `tablespace_map` in
// file-scope statics (allocated in a dedicated TopMemoryContext child), so the
// `BackupState` and tablespace-map bytes survive from the `pg_backup_start()`
// SQL call to the matching `pg_backup_stop()` SQL call. The value-model cores
// (`crate::pg_backup_start` / `crate::pg_backup_stop`) thread that state through
// their return/argument, so the fmgr boundary owns the longevity here: this
// thread-local pair mirrors the two C statics. (Backend-private state — a
// backup is per-session, never crosses to a worker.)
std::thread_local! {
    /// C: `static BackupState *backup_state = NULL;`
    static FMGR_BACKUP_STATE: core::cell::Cell<Option<::wal::BackupState>> =
        const { core::cell::Cell::new(None) };
    /// C: `static StringInfo tablespace_map = NULL;` — the cstring body bytes.
    static FMGR_TABLESPACE_MAP: core::cell::RefCell<Option<Vec<u8>>> =
        const { core::cell::RefCell::new(None) };
}

/// `pg_backup_start(label text, fast bool)` (xlogfuncs.c:54) — `pg_lsn` result.
///
/// Mirrors the C wrapper: stash the long-lived `(backup_state, tablespace_map)`
/// returned by the value core into the session statics, then return the start
/// LSN. C resets the statics on a second start; `crate::pg_backup_start` itself
/// rejects a double-start (`SESSION_BACKUP_RUNNING`), so on success the previous
/// statics were already cleared by the matching stop / abort.
fn fc_pg_backup_start(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let backupid = arg_text(fcinfo, 0).to_string();
    let fast = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let (startpoint, backup_state, tablespace_map) =
        crate::pg_backup_start(m.mcx(), &backupid, fast)?;
    // Copy the long-lived state out of the scratch arena into the session
    // statics (C: the dedicated backupcontext child of TopMemoryContext).
    FMGR_BACKUP_STATE.with(|c| c.set(Some(backup_state)));
    FMGR_TABLESPACE_MAP.with(|c| *c.borrow_mut() = Some(tablespace_map.as_slice().to_vec()));
    Ok(ret_lsn(startpoint))
}

/// `pg_backup_stop(wait_for_archive bool DEFAULT true)` (xlogfuncs.c:121) —
/// `record(lsn pg_lsn, labelfile text, spcmapfile text)`.
///
/// Mirrors the C wrapper: read the session statics stashed by
/// `pg_backup_start()`, finish the backup, build the 3-column record, then clear
/// the statics (C: `MemoryContextDelete(backupcontext)`). If no backup is in
/// progress the statics are absent; the value core's status check still raises
/// "backup is not in progress" because `get_backup_status()` is the source of
/// truth, so we synthesize empty state only to reach that error path.
fn fc_pg_backup_stop(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let waitforarchive = arg_bool(fcinfo, 0);
    let backup_state = FMGR_BACKUP_STATE.with(|c| c.get());
    let tablespace_map = FMGR_TABLESPACE_MAP.with(|c| c.borrow().clone());

    let m = scratch_mcx();

    // C asserts the statics are non-NULL only after the SESSION_BACKUP_RUNNING
    // check passes. If they are absent the core's own status check raises
    // "backup is not in progress" (errhint "Did you call pg_backup_start()?");
    // supply a default state purely so we can reach that error branch.
    let state = backup_state.unwrap_or_else(::wal::BackupState::zeroed);
    let map = tablespace_map.unwrap_or_default();

    let res = crate::pg_backup_stop(m.mcx(), waitforarchive, state, &map)?;

    // Clear the session statics (C: clears backup_state/tablespace_map and
    // deletes backupcontext) now the backup has ended.
    FMGR_BACKUP_STATE.with(|c| c.set(None));
    FMGR_TABLESPACE_MAP.with(|c| *c.borrow_mut() = None);

    // values[0] = LSNGetDatum(stoppoint);
    // values[1] = CStringGetTextDatum(backup_label);
    // values[2] = CStringGetTextDatum(tablespace_map->data);
    let coltypes = [LSNOID, TEXTOID, TEXTOID];
    let values = [
        DatumV::from_u64(res.lsn),
        text_datum(m.mcx(), res.labelfile.as_slice())?,
        text_datum(m.mcx(), res.spcmapfile.as_slice())?,
    ];
    let nulls = [false, false, false];

    let rec = funcapi_seams::record_from_values::call(m.mcx(), &coltypes, &values, &nulls)?;
    Ok(ret_record(fcinfo, rec))
}

/// Build a `text` value `DatumV` (header-ful varlena image: 4-byte length word
/// + payload bytes) from a header-LESS `text` payload, as the
/// `CStringGetTextDatum` columns of the `pg_backup_stop` record require for
/// `heap_form_tuple`.
fn text_datum<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    payload: &[u8],
) -> types_error::PgResult<DatumV<'mcx>> {
    let total = payload.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&::datum::varlena::set_varsize_4b(total));
    img.extend_from_slice(payload);
    DatumV::from_byref_bytes_in(mcx, &img)
}

/// Carry a composite-record `Datum` (built by `record_from_values`) onto the
/// fmgr frame's by-reference `Composite` lane (C:
/// `PG_RETURN_DATUM(HeapTupleGetDatum(tuple))`).
fn ret_record(fcinfo: &mut FunctionCallInfoBaseData, built: DatumV<'_>) -> Datum {
    match built {
        DatumV::ByRef(bytes) => {
            fcinfo.set_ref_result(RefPayload::Composite(bytes.as_slice().to_vec()));
            Datum::from_usize(0)
        }
        DatumV::Composite(t) => {
            fcinfo.set_ref_result(RefPayload::Composite(t.to_datum_image()));
            Datum::from_usize(0)
        }
        _ => panic!("xlogfuncs fmgr: record_from_values produced a non-composite Datum"),
    }
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register the six expressible `xlogfuncs.c` recovery-control fmgr builtins
/// (C: their `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
///
/// OIDs / nargs / strict / retset transcribed from the generated
/// `fmgrtab.c` (Gen_fmgrtab over `pg_proc.dat`): all six are `provolatile => 'v'`
/// and inherit `proisstrict BKI_DEFAULT(t)` (none overrides it, so `strict =
/// true`), and none is `proretset` (so `retset = false`).
pub fn register_xlogfuncs_builtins() {
    fmgr_core::register_builtins_native([
        // pg_wal_replay_pause() -> void
        builtin(3071, "pg_wal_replay_pause", 0, true, false, fc_pg_wal_replay_pause),
        // pg_wal_replay_resume() -> void
        builtin(3072, "pg_wal_replay_resume", 0, true, false, fc_pg_wal_replay_resume),
        // pg_is_wal_replay_paused() -> bool
        builtin(3073, "pg_is_wal_replay_paused", 0, true, false, fc_pg_is_wal_replay_paused),
        // pg_get_wal_replay_pause_state() -> text
        builtin(
            1137,
            "pg_get_wal_replay_pause_state",
            0,
            true,
            false,
            fc_pg_get_wal_replay_pause_state,
        ),
        // pg_is_in_recovery() -> bool
        builtin(3810, "pg_is_in_recovery", 0, true, false, fc_pg_is_in_recovery),
        // pg_promote(bool, int4) -> bool
        builtin(3436, "pg_promote", 2, true, false, fc_pg_promote),
        // ---- WAL-control / LSN functions (pg_lsn / timestamptz results) ----
        // pg_current_wal_lsn() -> pg_lsn
        builtin(2849, "pg_current_wal_lsn", 0, true, false, fc_pg_current_wal_lsn),
        // pg_current_wal_insert_lsn() -> pg_lsn
        builtin(2852, "pg_current_wal_insert_lsn", 0, true, false, fc_pg_current_wal_insert_lsn),
        // pg_current_wal_flush_lsn() -> pg_lsn
        builtin(3330, "pg_current_wal_flush_lsn", 0, true, false, fc_pg_current_wal_flush_lsn),
        // pg_switch_wal() -> pg_lsn
        builtin(2848, "pg_switch_wal", 0, true, false, fc_pg_switch_wal),
        // pg_create_restore_point(text) -> pg_lsn
        builtin(3098, "pg_create_restore_point", 1, true, false, fc_pg_create_restore_point),
        // pg_last_wal_receive_lsn() -> pg_lsn (NULL-able)
        builtin(3820, "pg_last_wal_receive_lsn", 0, true, false, fc_pg_last_wal_receive_lsn),
        // pg_last_wal_replay_lsn() -> pg_lsn (NULL-able)
        builtin(3821, "pg_last_wal_replay_lsn", 0, true, false, fc_pg_last_wal_replay_lsn),
        // pg_last_xact_replay_timestamp() -> timestamptz (NULL-able)
        builtin(3830, "pg_last_xact_replay_timestamp", 0, true, false, fc_pg_last_xact_replay_timestamp),
        // pg_log_standby_snapshot() -> pg_lsn
        builtin(6305, "pg_log_standby_snapshot", 0, true, false, fc_pg_log_standby_snapshot),
        // pg_walfile_name(pg_lsn) -> text
        builtin(2851, "pg_walfile_name", 1, true, false, fc_pg_walfile_name),
        // ---- online base-backup control (composite / lsn results) ----
        // pg_backup_start(text, bool) -> pg_lsn
        builtin(2172, "pg_backup_start", 2, true, false, fc_pg_backup_start),
        // pg_backup_stop(bool) -> record(lsn, labelfile, spcmapfile)
        builtin(2739, "pg_backup_stop", 1, true, false, fc_pg_backup_stop),
    ]);
}
