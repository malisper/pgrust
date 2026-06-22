//! The single-composite-row (non-set-returning) members of `pgstatfuncs.c` that
//! are reachable through the **scalar fmgr call path** (target-list position,
//! e.g. `SELECT pg_stat_get_replication_slot(NULL)`), as opposed to the
//! FROM-clause / function-RTE SRF path (which `backend-executor-execSRF`'s
//! `pgstat_composite_srf` already serves):
//!
//!   * `pg_stat_get_replication_slot(text)`   (OID 6169) — 10-column replslot stats
//!   * `pg_stat_get_subscription_stats(oid)`  (OID 6231) — 11-column subscription stats
//!
//! Neither is `proretset` — each returns exactly one composite row. C builds its
//! own `CreateTemplateTupleDesc` + `TupleDescInitEntry` + `BlessTupleDesc`, fills
//! `values`/`nulls`, and returns
//! `HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls))`
//! (pgstatfuncs.c:2113 / 2184). The owned model builds the composite `Datum`
//! with `record_from_values` (the funcapi `BlessTupleDesc` + `heap_form_tuple` +
//! `HeapTupleGetDatum` pipeline) from the projected fetch struct, then carries it
//! onto the fmgr frame's by-reference `Composite` lane (read back as a
//! `Datum::Composite` row by the dispatch result mapper) — exactly the pattern
//! `backend-utils-misc-more`'s `pg_control_*` scalar builtins use.
//!
//! Without these scalar registrations the fmgr `Internal`-language resolution
//! fails with `internal function "..." is not in internal lookup table`.

extern crate alloc;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::boundary::RefPayload;
use types_fmgr::FunctionCallInfoBaseData;
use types_replication::conflict::CONFLICT_NUM_TYPES;
use types_tuple::heaptuple::NameData;
use types_tuple::Datum as DatumV;

use backend_utils_fmgr_funcapi_seams::record_from_values;

const INT8OID: Oid = 20;
const TEXTOID: Oid = 25;
const OIDOID: Oid = 26;
const TIMESTAMPTZOID: Oid = 1184;

/// A scratch context for the cores' transient allocations (C charges them to
/// `CurrentMemoryContext`). The result composite image is copied out onto the
/// fmgr frame's `Composite` lane before this context drops.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("pgstat composite fmgr scratch")
}

/// `CStringGetTextDatum(s)` → a `text` varlena `Datum`.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<DatumV<'mcx>> {
    backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// Carry a composite-record `Datum` (built by `record_from_values`) onto the
/// fmgr frame's by-reference `Composite` lane, returning the `(Datum) 0`
/// placeholder word. Mirrors `backend-utils-misc-more`'s `ret_record`.
fn ret_record(
    fcinfo: &mut FunctionCallInfoBaseData,
    built: PgResult<DatumV<'_>>,
) -> PgResult<Datum> {
    match built? {
        DatumV::ByRef(bytes) => {
            fcinfo.set_ref_result(RefPayload::Composite(bytes.as_slice().to_vec()));
            Ok(Datum::from_usize(0))
        }
        DatumV::Composite(t) => {
            fcinfo.set_ref_result(RefPayload::Composite(t.to_datum_image()));
            Ok(Datum::from_usize(0))
        }
        _ => panic!(
            "pgstat composite fmgr: record_from_values produced a non-composite Datum"
        ),
    }
}

/// `text_to_cstring(PG_GETARG_TEXT_P(i))`: a `text` arg's payload on the by-ref
/// lane (header-ful image; skip the 4-byte varlena header), decoded as UTF-8.
fn arg_text(fcinfo: &FunctionCallInfoBaseData, i: usize) -> alloc::string::String {
    use types_datum::varlena::VARHDRSZ;
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pgstat composite fmgr: text arg missing from by-ref lane");
    // `VARDATA_ANY`: skip ONE header byte for a short (1-byte, low-bit-set)
    // header, else `VARHDRSZ`. No-op while `SHORT_VARLENA_PACKING` is off.
    let bytes: &[u8] = match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    };
    core::str::from_utf8(bytes)
        .expect("pgstat composite fmgr: text arg not valid UTF-8")
        .into()
}

/// `PG_GETARG_OID(i)`.
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("pgstat composite fmgr: missing oid arg")
        .value
        .as_oid()
}

// ===========================================================================
//  pg_stat_get_replication_slot (pgstatfuncs.c:2113) — 10 cols.
// ===========================================================================

/// `pg_stat_get_replication_slot(text)` (OID 6169).
pub(crate) fn fc_pg_stat_get_replication_slot(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let ctx = scratch_mcx();
    let mcx = ctx.mcx();

    // C: namestrcpy(&slotname, text_to_cstring(PG_GETARG_TEXT_P(0)));
    let slotname_str = arg_text(fcinfo, 0);
    let mut slotname = NameData::default();
    slotname.namestrcpy(&slotname_str);

    // C: slotent = pgstat_fetch_replslot(slotname); if (!slotent) allzero.
    let slotent = backend_utils_activity_pgstat_replslot::pgstat_fetch_replslot(slotname)?
        .unwrap_or_default();

    // C: values[0] = CStringGetTextDatum(NameStr(slotname));
    let slot_name = core::str::from_utf8(slotname.name_str()).unwrap_or("");

    let coltypes = [
        TEXTOID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID,
        TIMESTAMPTZOID,
    ];
    let mut values: [DatumV; 10] = [
        text_datum(mcx, slot_name)?,
        DatumV::from_i64(slotent.spill_txns),
        DatumV::from_i64(slotent.spill_count),
        DatumV::from_i64(slotent.spill_bytes),
        DatumV::from_i64(slotent.stream_txns),
        DatumV::from_i64(slotent.stream_count),
        DatumV::from_i64(slotent.stream_bytes),
        DatumV::from_i64(slotent.total_txns),
        DatumV::from_i64(slotent.total_bytes),
        DatumV::null(),
    ];
    let mut nulls = [false; 10];
    if slotent.stat_reset_timestamp == 0 {
        nulls[9] = true;
    } else {
        values[9] = DatumV::from_i64(slotent.stat_reset_timestamp);
    }

    let built = record_from_values::call(mcx, &coltypes, &values, &nulls);
    ret_record(fcinfo, built)
}

// ===========================================================================
//  pg_stat_get_subscription_stats (pgstatfuncs.c:2184) — 11 cols.
// ===========================================================================

/// `pg_stat_get_subscription_stats(oid)` (OID 6231).
pub(crate) fn fc_pg_stat_get_subscription_stats(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let ctx = scratch_mcx();
    let mcx = ctx.mcx();

    let subid = arg_oid(fcinfo, 0);

    // C: subentry = pgstat_fetch_stat_subscription(subid); if (!subentry) allzero.
    let subentry =
        backend_utils_activity_pgstat_subscription::pgstat_fetch_stat_subscription(subid)?
            .unwrap_or_default();

    // 11 cols: subid, apply_error_count, sync_error_count, CONFLICT_NUM_TYPES (=7)
    // conflict counters, stats_reset.
    let mut coltypes: Vec<Oid> = Vec::with_capacity(11);
    coltypes.push(OIDOID);
    coltypes.push(INT8OID);
    coltypes.push(INT8OID);
    for _ in 0..CONFLICT_NUM_TYPES {
        coltypes.push(INT8OID);
    }
    coltypes.push(TIMESTAMPTZOID);

    let mut values: Vec<DatumV> = Vec::with_capacity(11);
    let mut nulls: Vec<bool> = Vec::with_capacity(11);

    // subid
    values.push(DatumV::from_oid(subid));
    nulls.push(false);
    // apply_error_count
    values.push(DatumV::from_i64(subentry.apply_error_count));
    nulls.push(false);
    // sync_error_count
    values.push(DatumV::from_i64(subentry.sync_error_count));
    nulls.push(false);
    // conflict counts
    for nconflict in 0..CONFLICT_NUM_TYPES {
        values.push(DatumV::from_i64(subentry.conflict_count[nconflict]));
        nulls.push(false);
    }
    // stats_reset
    if subentry.stat_reset_timestamp == 0 {
        values.push(DatumV::null());
        nulls.push(true);
    } else {
        values.push(DatumV::from_i64(subentry.stat_reset_timestamp));
        nulls.push(false);
    }

    debug_assert_eq!(values.len(), 11);
    let built = record_from_values::call(mcx, &coltypes, &values, &nulls);
    ret_record(fcinfo, built)
}
