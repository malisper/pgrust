//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `slotfuncs.c`
//! SQL-callable functions.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result. [`register_slotfuncs_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch (and the
//! `fmgr_isbuiltin` fast path) resolves them. OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat`.
//!
//! The `void`-returning administration functions (`pg_drop_replication_slot`,
//! `pg_sync_replication_slots`) and the composite `(slot_name name, lsn pg_lsn)`
//! / `(slot_name name, end_lsn pg_lsn)`-returning functions
//! (`pg_create_physical_replication_slot`, `pg_create_logical_replication_slot`,
//! `pg_copy_physical_replication_slot_{a,b}`,
//! `pg_copy_logical_replication_slot_{a,b,c}`, `pg_replication_slot_advance`)
//! are registered here. The composite returns cross the by-reference
//! `Composite` lane: the value core hands back the typed [`crate::SlotNameLsnRow`]
//! `(name, pg_lsn)` row, which [`ret_slot_name_lsn_row`] turns into a flat
//! `HeapTupleHeader` Datum via `funcapi::record_from_values` (C:
//! `get_call_result_type` + `heap_form_tuple` + `HeapTupleGetDatum` +
//! `PG_RETURN_DATUM`) — the same record carrier the genfile.c `pg_stat_file` and
//! objectaddress.c `pg_get_object_address` builtins already use. The 20-column
//! `pg_get_replication_slots` SRF stays off this scalar registry (it lives in
//! the executor-frame SRF home).

use types_core::{Oid, XLogRecPtr};
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use types_tuple::heaptuple::{NameData, NAMEOID};
use types_tuple::Datum as DatumV;

use crate::SlotNameLsnRow;

/// C: `LSNOID` / `PG_LSNOID` (`pg_type.h`) — the `pg_lsn` type Oid, the second
/// (`lsn` / `end_lsn`) result column's type.
const LSNOID: Oid = 3220;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_NAME(i)` → `NameStr(*name)`: a `name` value's fixed
/// `NAMEDATALEN` buffer on the by-ref lane, trimmed at the first NUL (C passes
/// the whole `NameData` by pointer).
#[inline]
fn arg_name<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("slotfuncs fn: name arg missing from by-ref lane");
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).expect("slotfuncs fn: name arg not valid UTF-8")
}

/// `PG_GETARG_BOOL(i)` → `DatumGetBool`: any nonzero word reads back as `true`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .arg(i)
        .expect("slotfuncs fn: missing bool arg")
        .value
        .as_bool()
}

/// `PG_GETARG_LSN(i)` → `DatumGetLSN`: a `pg_lsn`/`XLogRecPtr` 8-byte by-value
/// word.
#[inline]
fn arg_lsn(fcinfo: &FunctionCallInfoBaseData, i: usize) -> XLogRecPtr {
    fcinfo
        .arg(i)
        .expect("slotfuncs fn: missing pg_lsn arg")
        .value
        .as_u64()
}

/// `PG_RETURN_VOID()`: the dummy `(Datum) 0` a `void`-returning function yields.
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}

/// A scratch context for the cores' `Mcx<'_>` argument and the record builder.
/// The composite result is copied out onto the by-ref lane (as its flat
/// `HeapTupleHeader` Datum image) before this context is dropped, so the result
/// outlives the scratch arena.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("slotfuncs fmgr scratch")
}

/// `NameGetDatum(&name)` — a 64-byte NUL-padded `NameData` by-reference Datum
/// image (the C `Name` is a pointer to a fixed-length `NameData`), the form
/// `heap_form_tuple` reads a `name` (`NAMEOID`, fixed-length by-ref) column from.
fn name_datum<'mcx>(mcx: mcx::Mcx<'mcx>, nd: &NameData) -> types_error::PgResult<DatumV<'mcx>> {
    Ok(DatumV::ByRef(mcx::slice_in(mcx, &nd.data)?))
}

/// Build the `(slot_name name, lsn pg_lsn)` two-column composite Datum from a
/// [`SlotNameLsnRow`] value-core result and carry it onto the fmgr frame's
/// by-reference `Composite` lane (C: `values[0] = NameGetDatum(...); values[1] =
/// LSNGetDatum(...)` / `nulls[1] = true; ... heap_form_tuple(tupdesc, values,
/// nulls); PG_RETURN_DATUM(HeapTupleGetDatum(tuple))`).
///
/// `record_from_values` is the `CreateTemplateTupleDesc` + `TupleDescInitEntry`
/// + `BlessTupleDesc` + `heap_form_tuple` + `HeapTupleGetDatum` idiom; the
/// resulting flat `HeapTupleHeader` Datum image crosses verbatim onto the
/// `Composite` lane (the dispatch result mapper reads it back as a composite
/// value and routes it to the record output function), matching C exactly.
fn ret_slot_name_lsn_row(
    fcinfo: &mut FunctionCallInfoBaseData,
    row: SlotNameLsnRow,
) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    // values[0] = NameGetDatum(&data.name); values[1] = LSNGetDatum(lsn) /
    // nulls[1] = true.
    let coltypes = [NAMEOID, LSNOID];
    let values = [
        name_datum(m.mcx(), &row.slot_name)?,
        match row.lsn {
            Some(l) => DatumV::from_u64(l),
            None => DatumV::null(),
        },
    ];
    let nulls = [false, row.lsn.is_none()];

    let rec = backend_utils_fmgr_funcapi_seams::record_from_values::call(
        m.mcx(),
        &coltypes,
        &values,
        &nulls,
    )?;

    // HeapTupleGetDatum hands back the composite Datum as a `ByRef`/`Composite`
    // image; carry it onto the by-ref lane (the flattened `Composite` arm covers
    // a live formed tuple, if the builder returns one).
    match rec {
        DatumV::ByRef(bytes) => {
            fcinfo.set_ref_result(RefPayload::Composite(bytes.as_slice().to_vec()));
            Ok(Datum::from_usize(0))
        }
        DatumV::Composite(t) => {
            fcinfo.set_ref_result(RefPayload::Composite(t.to_datum_image()));
            Ok(Datum::from_usize(0))
        }
        _ => panic!("slotfuncs fmgr: record_from_values produced unexpected Datum arm"),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters — void administration functions.
// ---------------------------------------------------------------------------

/// `pg_drop_replication_slot(name)` (slotfuncs.c) — `void`.
fn fc_pg_drop_replication_slot(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let name = arg_name(fcinfo, 0).to_string();
    let m = scratch_mcx();
    crate::pg_drop_replication_slot(m.mcx(), &name)?;
    Ok(ret_void())
}

/// `pg_sync_replication_slots()` (slotfuncs.c) — `void`, no arguments.
fn fc_pg_sync_replication_slots(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    crate::pg_sync_replication_slots(m.mcx())?;
    Ok(ret_void())
}

// ---------------------------------------------------------------------------
// fc_ adapters — composite-returning create / copy / advance functions.
// ---------------------------------------------------------------------------

/// `pg_create_physical_replication_slot(slot_name name, immediately_reserve
/// bool, temporary bool)` — `record(slot_name name, lsn pg_lsn)`.
fn fc_pg_create_physical_replication_slot(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let name = arg_name(fcinfo, 0).to_string();
    let immediately_reserve = arg_bool(fcinfo, 1);
    let temporary = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    let row = crate::pg_create_physical_replication_slot(
        m.mcx(),
        &name,
        immediately_reserve,
        temporary,
    )?;
    ret_slot_name_lsn_row(fcinfo, row)
}

/// `pg_create_logical_replication_slot(slot_name name, plugin name, temporary
/// bool, twophase bool, failover bool)` — `record(slot_name name, lsn pg_lsn)`.
fn fc_pg_create_logical_replication_slot(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let name = arg_name(fcinfo, 0).to_string();
    let plugin = arg_name(fcinfo, 1).to_string();
    let temporary = arg_bool(fcinfo, 2);
    let two_phase = arg_bool(fcinfo, 3);
    let failover = arg_bool(fcinfo, 4);
    let m = scratch_mcx();
    let row = crate::pg_create_logical_replication_slot(
        m.mcx(),
        &name,
        &plugin,
        temporary,
        two_phase,
        failover,
    )?;
    ret_slot_name_lsn_row(fcinfo, row)
}

/// `pg_replication_slot_advance(slot_name name, upto_lsn pg_lsn)` —
/// `record(slot_name name, end_lsn pg_lsn)`.
fn fc_pg_replication_slot_advance(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let slotname = arg_name(fcinfo, 0).to_string();
    let moveto = arg_lsn(fcinfo, 1);
    let m = scratch_mcx();
    let row = crate::pg_replication_slot_advance(m.mcx(), &slotname, moveto)?;
    ret_slot_name_lsn_row(fcinfo, row)
}

/// `pg_copy_physical_replication_slot(src_slot_name name, dst_slot_name name,
/// temporary bool)` — 3-arg form (`pg_copy_physical_replication_slot_a`).
fn fc_pg_copy_physical_replication_slot_a(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let src = arg_name(fcinfo, 0).to_string();
    let dst = arg_name(fcinfo, 1).to_string();
    let temporary = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    // C `pg_copy_physical_replication_slot_a` (3-arg) → the pgrust value core
    // named by arg count: `pg_copy_physical_replication_slot_b` is the 3-arg
    // body (the pgrust `_a/_b` suffix numbers by ascending arg count; the C
    // `prosrc` suffix numbers descending — they invert).
    let row = crate::pg_copy_physical_replication_slot_b(m.mcx(), &src, &dst, temporary)?;
    ret_slot_name_lsn_row(fcinfo, row)
}

/// `pg_copy_physical_replication_slot(src_slot_name name, dst_slot_name name)`
/// — 2-arg form (`pg_copy_physical_replication_slot_b`).
fn fc_pg_copy_physical_replication_slot_b(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let src = arg_name(fcinfo, 0).to_string();
    let dst = arg_name(fcinfo, 1).to_string();
    let m = scratch_mcx();
    let row = crate::pg_copy_physical_replication_slot_a(m.mcx(), &src, &dst)?;
    ret_slot_name_lsn_row(fcinfo, row)
}

/// `pg_copy_logical_replication_slot(src_slot_name name, dst_slot_name name,
/// temporary bool, plugin name)` — 4-arg form
/// (`pg_copy_logical_replication_slot_a`).
fn fc_pg_copy_logical_replication_slot_a(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let src = arg_name(fcinfo, 0).to_string();
    let dst = arg_name(fcinfo, 1).to_string();
    let temporary = arg_bool(fcinfo, 2);
    let plugin = arg_name(fcinfo, 3).to_string();
    let m = scratch_mcx();
    let row =
        crate::pg_copy_logical_replication_slot_c(m.mcx(), &src, &dst, temporary, &plugin)?;
    ret_slot_name_lsn_row(fcinfo, row)
}

/// `pg_copy_logical_replication_slot(src_slot_name name, dst_slot_name name,
/// temporary bool)` — 3-arg form (`pg_copy_logical_replication_slot_b`).
fn fc_pg_copy_logical_replication_slot_b(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let src = arg_name(fcinfo, 0).to_string();
    let dst = arg_name(fcinfo, 1).to_string();
    let temporary = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    let row = crate::pg_copy_logical_replication_slot_b(m.mcx(), &src, &dst, temporary)?;
    ret_slot_name_lsn_row(fcinfo, row)
}

/// `pg_copy_logical_replication_slot(src_slot_name name, dst_slot_name name)` —
/// 2-arg form (`pg_copy_logical_replication_slot_c`).
fn fc_pg_copy_logical_replication_slot_c(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let src = arg_name(fcinfo, 0).to_string();
    let dst = arg_name(fcinfo, 1).to_string();
    let m = scratch_mcx();
    let row = crate::pg_copy_logical_replication_slot_a(m.mcx(), &src, &dst)?;
    ret_slot_name_lsn_row(fcinfo, row)
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

/// Register the `slotfuncs.c` fmgr builtins (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`.
///
/// OIDs / nargs / strict / retset transcribed from `pg_proc.dat`: every row
/// inherits `proisstrict BKI_DEFAULT(t)` (none overrides it, so `strict =
/// true`), and none is `proretset` (so `retset = false`). The composite
/// returners declare `prorettype => 'record'` but `proretset => 'f'`, so they
/// dispatch through this scalar registry exactly like `pg_stat_file`.
pub fn register_slotfuncs_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // pg_drop_replication_slot(name) -> void
        builtin(3780, "pg_drop_replication_slot", 1, true, false, fc_pg_drop_replication_slot),
        // pg_sync_replication_slots() -> void
        builtin(6344, "pg_sync_replication_slots", 0, true, false, fc_pg_sync_replication_slots),
        // pg_create_physical_replication_slot(name, bool, bool) -> record(name, pg_lsn)
        builtin(
            3779,
            "pg_create_physical_replication_slot",
            3,
            true,
            false,
            fc_pg_create_physical_replication_slot,
        ),
        // pg_create_logical_replication_slot(name, name, bool, bool, bool) -> record(name, pg_lsn)
        builtin(
            3786,
            "pg_create_logical_replication_slot",
            5,
            true,
            false,
            fc_pg_create_logical_replication_slot,
        ),
        // pg_replication_slot_advance(name, pg_lsn) -> record(name, pg_lsn)
        builtin(
            3878,
            "pg_replication_slot_advance",
            2,
            true,
            false,
            fc_pg_replication_slot_advance,
        ),
        // pg_copy_physical_replication_slot(name, name, bool) -> record(name, pg_lsn)
        builtin(
            4220,
            "pg_copy_physical_replication_slot_a",
            3,
            true,
            false,
            fc_pg_copy_physical_replication_slot_a,
        ),
        // pg_copy_physical_replication_slot(name, name) -> record(name, pg_lsn)
        builtin(
            4221,
            "pg_copy_physical_replication_slot_b",
            2,
            true,
            false,
            fc_pg_copy_physical_replication_slot_b,
        ),
        // pg_copy_logical_replication_slot(name, name, bool, name) -> record(name, pg_lsn)
        builtin(
            4222,
            "pg_copy_logical_replication_slot_a",
            4,
            true,
            false,
            fc_pg_copy_logical_replication_slot_a,
        ),
        // pg_copy_logical_replication_slot(name, name, bool) -> record(name, pg_lsn)
        builtin(
            4223,
            "pg_copy_logical_replication_slot_b",
            3,
            true,
            false,
            fc_pg_copy_logical_replication_slot_b,
        ),
        // pg_copy_logical_replication_slot(name, name) -> record(name, pg_lsn)
        builtin(
            4224,
            "pg_copy_logical_replication_slot_c",
            2,
            true,
            false,
            fc_pg_copy_logical_replication_slot_c,
        ),
    ]);
}
