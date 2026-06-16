//! `backend-utils-adt-enum` — port of PostgreSQL 18.3
//! `src/backend/utils/adt/enum.c`: the enum-type support functions (I/O,
//! comparison, and the `enum_first` / `enum_last` / `enum_range` programming
//! helpers).
//!
//! Control flow, branch order, error-message text and SQLSTATE mirror C. This
//! is an owned-value rewrite: where C returns a `Datum` the core returns the
//! decoded value (an [`Oid`] for an enum value, a `String` for a `cstring`, a
//! `PgVec<u8>` for a `bytea` body, or, for `enum_range`, the array varlena
//! image). Hard errors surface as [`PgError`] via `PgResult`; `enum_in`'s
//! soft-error path routes to a [`SoftErrorContext`] when supplied, mirroring
//! C's `ereturn`.
//!
//! Cross-subsystem work goes through seams / direct calls:
//!  * `pg_enum` syscache reads — `lookup_enum_by_oid` / `lookup_enum_by_typoid_name`
//!    (syscache-seams, `SearchSysCache1(ENUMOID)` / `SearchSysCache2(ENUMTYPOIDNAME)`).
//!  * the ordered `EnumTypIdSortOrderIndexId` scan — `scan_enum_typid_sorted`
//!    (pg-enum-seams).
//!  * `check_safe_enum_use`'s transaction-status primitives —
//!    `transaction_id_is_in_progress` (procarray-seams) /
//!    `transaction_id_did_commit` (transam-seams); the body's branch order is
//!    ported here. `EnumUncommitted` data is folded into the projected row's
//!    `xmin`/`xmin_committed` per C.
//!  * odd-OID comparison — `compare_values_of_enum` (typcache-seams).
//!  * `construct_array` — direct call into `backend-utils-adt-arrayfuncs`.
//!  * `format_type_be` — `format-type-seams` (diagnostic).
//!  * pqformat — direct calls into `backend-libpq-pqformat`.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use mcx::{Mcx, PgVec};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_core::TransactionId;
use types_error::{
    PgError, PgResult, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE, ERROR,
};
use backend_utils_error::ereport;
use types_catalog::pg_enum::EnumTupleData;
use types_core::fmgr::NAMEDATALEN;
use types_datum::datum::Datum;
use types_scan::sdir::ScanDirection;
use types_stringinfo::StringInfo;

use backend_catalog_pg_enum_seams as pg_enum_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_cache_typcache_seams as typcache_seams;
use backend_utils_adt_format_type_seams as format_type_seams;
use backend_storage_ipc_procarray_seams as procarray_seams;
use backend_access_transam_transam_seams as transam_seams;
use backend_libpq_pqformat as pqformat;
use backend_utils_adt_arrayfuncs::construct as arrayfuncs;

pub mod boundary;

/// `TYPALIGN_INT` (`catalog/pg_type.h`).
const TYPALIGN_INT: u8 = b'i';

// ===========================================================================
// check_safe_enum_use (enum.c:60)
// ===========================================================================

/// `check_safe_enum_use(enumval_tup)` — disallow use of an uncommitted
/// `pg_enum` tuple (enum.c:60). `transaction_xmin` is C's `TransactionXmin`
/// global, threaded explicitly (the `transaction_id_did_commit` seam reads it
/// when chasing a sub-committed xid's parent). `xmin`/`xmin_committed` are the
/// header facts the projected row carries (C `HeapTupleHeaderXminCommitted` /
/// `HeapTupleHeaderGetXmin`).
fn check_safe_enum_use(en: &EnumTupleData, transaction_xmin: TransactionId) -> PgResult<()> {
    // If the row is hinted as committed, it's surely safe.
    if en.xmin_committed {
        return Ok(());
    }

    // Usually, a row would get hinted as committed when it's read or loaded
    // into syscache; but just in case not, let's check the xmin directly.
    let xmin = en.xmin;
    if !procarray_seams::transaction_id_is_in_progress::call(xmin)?
        && transam_seams::transaction_id_did_commit::call(xmin, transaction_xmin)?
    {
        return Ok(());
    }

    // Check if the enum value is listed as uncommitted. If not, it's safe.
    if !pg_enum_seams::enum_uncommitted::call(en.oid) {
        return Ok(());
    }

    // There might well be other tests we could do here to narrow down the
    // unsafe conditions, but for now just raise an exception.
    Err(ereport(ERROR)
        .errcode(ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE)
        .errmsg(format!(
            "unsafe use of new value \"{}\" of enum type {}",
            label_to_string(&en.enumlabel),
            format_type_be(en.enumtypid)?
        ))
        .errhint("New enum values must be committed before they can be used.")
        .into_error())
}

// ===========================================================================
// Basic I/O support
// ===========================================================================

/// `enum_in` — parse a label into its enum OID (enum.c:107). On a soft error
/// (`escontext` present) C returns `(Datum) 0`; the port returns `Ok(None)`
/// after `errsave`, `Err` otherwise.
pub fn enum_in(
    name: &str,
    enumtypoid: Oid,
    transaction_xmin: TransactionId,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Oid>> {
    // must check length to prevent Assert failure within SearchSysCache
    if name.len() >= NAMEDATALEN as usize {
        return errsave(escontext, invalid_input_value(enumtypoid, name)?).map(|()| None);
    }

    let tup = syscache_seams::lookup_enum_by_typoid_name::call(enumtypoid, name)?;
    let Some(tup) = tup else {
        return errsave(escontext, invalid_input_value(enumtypoid, name)?).map(|()| None);
    };

    // Check it's safe to use in SQL (C raises this hard even with escontext).
    check_safe_enum_use(&tup, transaction_xmin)?;

    // This comes from pg_enum.oid and stores system oids in user tables.
    Ok(Some(tup.oid))
}

/// `enum_out` — render an enum OID as its label (enum.c:155).
pub fn enum_out(enumval: Oid) -> PgResult<String> {
    let tup = syscache_seams::lookup_enum_by_oid::call(enumval)?;
    let Some(en) = tup else {
        return Err(invalid_internal_value(enumval));
    };
    // result = pstrdup(NameStr(en->enumlabel));
    Ok(label_to_string(&en.enumlabel))
}

/// `enum_recv` — external binary format to enum OID (enum.c:179).
pub fn enum_recv(
    buf: &mut StringInfo<'_>,
    enumtypoid: Oid,
    transaction_xmin: TransactionId,
) -> PgResult<Oid> {
    // name = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    let mcx = buf.allocator();
    let rawbytes = buf.data.len().saturating_sub(buf.cursor);
    let name_bytes = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    // C treats `name` as a NUL-terminated C string: strlen + CStringGetDatum
    // both stop at the first embedded NUL.
    let name = cstring_str(&name_bytes);

    // must check length to prevent Assert failure within SearchSysCache
    if name.len() >= NAMEDATALEN as usize {
        return Err(invalid_input_value(enumtypoid, name)?);
    }

    let tup = syscache_seams::lookup_enum_by_typoid_name::call(enumtypoid, name)?;
    let Some(tup) = tup else {
        return Err(invalid_input_value(enumtypoid, name)?);
    };

    // check it's safe to use in SQL
    check_safe_enum_use(&tup, transaction_xmin)?;

    Ok(tup.oid)
}

/// `enum_send` — enum OID to external binary format (enum.c:221). Returns the
/// `bytea` varlena image.
pub fn enum_send<'mcx>(mcx: Mcx<'mcx>, enumval: Oid) -> PgResult<PgVec<'mcx, u8>> {
    let tup = syscache_seams::lookup_enum_by_oid::call(enumval)?;
    let Some(en) = tup else {
        return Err(invalid_internal_value(enumval));
    };

    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendtext(&mut buf, name_str_bytes(&en.enumlabel))?;
    Ok(pqformat::pq_endtypsend(buf).into_image())
}

// ===========================================================================
// Comparison functions and related (enum.c:264)
// ===========================================================================

/// `enum_cmp_internal(arg1, arg2)` — the common comparison engine (enum.c:264).
pub fn enum_cmp_internal(arg1: Oid, arg2: Oid) -> PgResult<i32> {
    // Equal OIDs are equal no matter what
    if arg1 == arg2 {
        return Ok(0);
    }

    // Fast path: even-numbered Oids are known to compare correctly
    if (arg1 & 1) == 0 && (arg2 & 1) == 0 {
        if arg1 < arg2 {
            return Ok(-1);
        } else {
            return Ok(1);
        }
    }

    // Get the OID of the enum type containing arg1.
    let enum_tup = syscache_seams::lookup_enum_by_oid::call(arg1)?;
    let Some(en) = enum_tup else {
        return Err(invalid_internal_value(arg1));
    };
    let typeoid = en.enumtypid;

    // The remaining comparison logic is in typcache.c.
    typcache_seams::compare_values_of_enum::call(typeoid, arg1, arg2)
}

/// `enum_lt` (enum.c:306).
pub fn enum_lt(a: Oid, b: Oid) -> PgResult<bool> {
    Ok(enum_cmp_internal(a, b)? < 0)
}

/// `enum_le` (enum.c:315).
pub fn enum_le(a: Oid, b: Oid) -> PgResult<bool> {
    Ok(enum_cmp_internal(a, b)? <= 0)
}

/// `enum_eq` (enum.c:324). OID equality only.
pub fn enum_eq(a: Oid, b: Oid) -> bool {
    a == b
}

/// `enum_ne` (enum.c:333). OID inequality only.
pub fn enum_ne(a: Oid, b: Oid) -> bool {
    a != b
}

/// `enum_ge` (enum.c:342).
pub fn enum_ge(a: Oid, b: Oid) -> PgResult<bool> {
    Ok(enum_cmp_internal(a, b)? >= 0)
}

/// `enum_gt` (enum.c:351).
pub fn enum_gt(a: Oid, b: Oid) -> PgResult<bool> {
    Ok(enum_cmp_internal(a, b)? > 0)
}

/// `enum_smaller` (enum.c:360).
pub fn enum_smaller(a: Oid, b: Oid) -> PgResult<Oid> {
    Ok(if enum_cmp_internal(a, b)? < 0 { a } else { b })
}

/// `enum_larger` (enum.c:369).
pub fn enum_larger(a: Oid, b: Oid) -> PgResult<Oid> {
    Ok(if enum_cmp_internal(a, b)? > 0 { a } else { b })
}

/// `enum_cmp` (enum.c:378).
pub fn enum_cmp(a: Oid, b: Oid) -> PgResult<i32> {
    enum_cmp_internal(a, b)
}

// ===========================================================================
// Enum programming support functions (enum.c:389)
// ===========================================================================

/// `enum_endpoint(enumtypoid, direction)` — common code for `enum_first` /
/// `enum_last` (enum.c:394). Walks `pg_enum_typid_sortorder_index` in
/// `direction`; `InvalidOid` for an empty enum.
fn enum_endpoint(
    mcx: Mcx<'_>,
    enumtypoid: Oid,
    direction: ScanDirection,
    transaction_xmin: TransactionId,
) -> PgResult<Oid> {
    let sorted = pg_enum_seams::scan_enum_typid_sorted::call(mcx, enumtypoid)?;

    let enum_tuple = match direction {
        ScanDirection::ForwardScanDirection => sorted.first(),
        ScanDirection::BackwardScanDirection => sorted.last(),
        ScanDirection::NoMovementScanDirection => None,
    };

    let minmax = if let Some(enum_tuple) = enum_tuple {
        // check it's safe to use in SQL
        check_safe_enum_use(enum_tuple, transaction_xmin)?;
        enum_tuple.oid
    } else {
        // should only happen with an empty enum
        InvalidOid
    };

    Ok(minmax)
}

/// `enum_first` — lowest-sorting label of an enum type (enum.c:437).
pub fn enum_first(mcx: Mcx<'_>, enumtypoid: Oid, transaction_xmin: TransactionId) -> PgResult<Oid> {
    if enumtypoid == InvalidOid {
        return Err(could_not_determine_enum_type());
    }

    let min = enum_endpoint(mcx, enumtypoid, ScanDirection::ForwardScanDirection, transaction_xmin)?;

    if !OidIsValid(min) {
        return Err(enum_contains_no_values(enumtypoid)?);
    }

    Ok(min)
}

/// `enum_last` — highest-sorting label of an enum type (enum.c:466).
pub fn enum_last(mcx: Mcx<'_>, enumtypoid: Oid, transaction_xmin: TransactionId) -> PgResult<Oid> {
    if enumtypoid == InvalidOid {
        return Err(could_not_determine_enum_type());
    }

    let max =
        enum_endpoint(mcx, enumtypoid, ScanDirection::BackwardScanDirection, transaction_xmin)?;

    if !OidIsValid(max) {
        return Err(enum_contains_no_values(enumtypoid)?);
    }

    Ok(max)
}

/// `enum_range_bounds` — 2-argument variant of `enum_range` (enum.c:496).
/// `lower`/`upper` are `Some(oid)` for a present argument, `None` for SQL NULL
/// (C `PG_ARGISNULL` → `InvalidOid`). Returns the array varlena image.
pub fn enum_range_bounds<'mcx>(
    mcx: Mcx<'mcx>,
    lower: Option<Oid>,
    upper: Option<Oid>,
    enumtypoid: Oid,
    transaction_xmin: TransactionId,
) -> PgResult<PgVec<'mcx, u8>> {
    let lower = lower.unwrap_or(InvalidOid);
    let upper = upper.unwrap_or(InvalidOid);

    if enumtypoid == InvalidOid {
        return Err(could_not_determine_enum_type());
    }

    enum_range_internal(mcx, enumtypoid, lower, upper, transaction_xmin)
}

/// `enum_range_all` — 1-argument variant of `enum_range` (enum.c:527).
pub fn enum_range_all<'mcx>(
    mcx: Mcx<'mcx>,
    enumtypoid: Oid,
    transaction_xmin: TransactionId,
) -> PgResult<PgVec<'mcx, u8>> {
    if enumtypoid == InvalidOid {
        return Err(could_not_determine_enum_type());
    }

    enum_range_internal(mcx, enumtypoid, InvalidOid, InvalidOid, transaction_xmin)
}

/// `enum_range_internal(enumtypoid, lower, upper)` — build the ordered array of
/// member OIDs in `[lower, upper]` (enum.c:553).
fn enum_range_internal<'mcx>(
    mcx: Mcx<'mcx>,
    enumtypoid: Oid,
    lower: Oid,
    upper: Oid,
    transaction_xmin: TransactionId,
) -> PgResult<PgVec<'mcx, u8>> {
    let sorted = pg_enum_seams::scan_enum_typid_sorted::call(mcx, enumtypoid)?;

    let mut elems: PgVec<Datum> = PgVec::new_in(mcx);
    let mut left_found = !OidIsValid(lower);

    for enum_tuple in sorted.iter() {
        let enum_oid = enum_tuple.oid;

        if !left_found && lower == enum_oid {
            left_found = true;
        }

        if left_found {
            // check it's safe to use in SQL
            check_safe_enum_use(enum_tuple, transaction_xmin)?;

            elems
                .try_reserve(1)
                .map_err(|_| mcx.oom(core::mem::size_of::<Datum>()))?;
            elems.push(Datum::from_oid(enum_oid));
        }

        if OidIsValid(upper) && upper == enum_oid {
            break;
        }
    }

    // and build the result array
    // note this hardwires some details about the representation of Oid
    arrayfuncs::construct_array(
        mcx,
        &elems,
        enumtypoid,
        core::mem::size_of::<Oid>() as i32,
        true,
        TYPALIGN_INT,
    )
}

// ===========================================================================
// Error / name helpers
// ===========================================================================

/// `format_type_be(type_oid)` via the seam, in a scratch context (the
/// diagnostic string is consumed into the owned `PgError` message immediately).
fn format_type_be(type_oid: Oid) -> PgResult<String> {
    let scratch = mcx::MemoryContext::new("enum format_type_be");
    let s = format_type_seams::format_type_be::call(scratch.mcx(), type_oid)?;
    Ok(s.as_str().to_string())
}

/// `errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input value
/// for enum %s: \"%s\"", format_type_be(enumtypoid), name)`.
fn invalid_input_value(enumtypoid: Oid, name: &str) -> PgResult<PgError> {
    Ok(PgError::error(format!(
        "invalid input value for enum {}: \"{}\"",
        format_type_be(enumtypoid)?,
        name
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION))
}

/// `errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("invalid internal
/// value for enum: %u", enumval)`.
fn invalid_internal_value(enumval: Oid) -> PgError {
    PgError::error(format!("invalid internal value for enum: {enumval}"))
        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION)
}

/// `errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("could not determine actual
/// enum type")`.
fn could_not_determine_enum_type() -> PgError {
    PgError::error("could not determine actual enum type")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// `errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("enum %s contains
/// no values", format_type_be(enumtypoid))`.
fn enum_contains_no_values(enumtypoid: Oid) -> PgResult<PgError> {
    Ok(PgError::error(format!(
        "enum {} contains no values",
        format_type_be(enumtypoid)?
    ))
    .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE))
}

/// C `errsave(escontext, (Datum) 0, ...)` — save a soft error to `escontext`
/// (returning `Ok`) or raise it hard when there is none.
fn errsave(escontext: Option<&mut SoftErrorContext>, err: PgError) -> PgResult<()> {
    types_error::ereturn(escontext, (), err)
}

/// `NameStr(en->enumlabel)` — the catalog `NameData` bytes up to the first NUL.
fn name_str_bytes(label: &[u8]) -> &[u8] {
    let end = label.iter().position(|&b| b == 0).unwrap_or(label.len());
    &label[..end]
}

/// The label text rendered as an owned `String` (`pstrdup(NameStr(...))`).
fn label_to_string(label: &[u8]) -> String {
    String::from_utf8_lossy(name_str_bytes(label)).into_owned()
}

/// A NUL-terminated wire byte buffer as a `&str` up to the first NUL (C
/// `CStringGetDatum` semantics), UTF-8-lossy for the catalog key.
fn cstring_str(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).unwrap_or("")
}

#[cfg(test)]
mod tests;
