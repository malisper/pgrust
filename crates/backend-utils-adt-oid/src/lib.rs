#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! Port of PostgreSQL 18.3 `src/backend/utils/adt/oid.c`: the built-in scalar
//! `Oid` type (and the `oidvector` composite, partially — see below).
//!
//! `Oid` is a pass-by-value, 32-bit unsigned type. Following the sibling adt
//! ports, the value cores are plain typed Rust functions over `Oid`/`&str`; the
//! fmgr/`Datum` boundary lives in [`fmgr_builtins`], where each SQL-callable
//! entry is a `fc_<name>` adapter registered into the fmgr-core builtin table
//! (C: `fmgr_builtins[]`) by [`fmgr_builtins::register_oid_builtins`]. Without
//! that registration `fmgr_isbuiltin(F_OIDEQ)` would miss and a catalog scankey
//! comparison (`oideq`, OID 184) would fall into a recursive `SearchSysCache`
//! lookup during early single-user boot.
//!
//! The `oidvector` I/O (`buildoidvector`, `oidvectorin`, `oidvectorout`) builds
//! and reads the on-disk `oidvector` image — a 1-D `ArrayType` of `OIDOID`,
//! lower bound 0, no NULLs — through the array subsystem's `construct_md_array`
//! / `oidvector_to_oids_bytes`, and is registered here. The binary
//! `oidvectorrecv`/`oidvectorsend` still need the `array_recv`/`array_send`
//! fcinfo-sharing path (they reuse the caller's `flinfo->fn_extra` cache) and so
//! remain unregistered; they will land with that array machinery rather than be
//! faked. The `oidvectoreq/ne/lt/le/ge/gt` operators (which delegate to
//! `btoidvectorcmp`) ARE registered: each decodes its two `oidvector` images and
//! calls the element-wise comparison core. [`check_valid_oidvector`] validates an already-decoded
//! array header (its seam takes the header fields, not the carrier), as
//! `hashoidvector` and `oidvectorout` consume it.
//!
//! No `extern "C"`, no `*mut`/`*const`, no `libc`; soft errors flow through
//! `backend-utils-error` / `types_error::SoftErrorContext`.

pub mod fmgr_builtins;

use types_core::{Oid, OIDOID};
use types_error::{PgError, PgResult, SoftErrorContext, ERRCODE_DATATYPE_MISMATCH};
use types_parsenodes::Node;

// ===========================================================================
// USER I/O ROUTINES
// ===========================================================================

/// `oidin` (oid.c:37): converts a `cstring` to an `Oid` via
/// `uint32in_subr(s, NULL, "oid", fcinfo->context)`. With `endloc == NULL` only
/// trailing whitespace is permitted after the number. A soft `escontext`
/// records the error and the parsed value is meaningless (C returns `0`); a
/// hard error propagates as `Err`.
pub fn oidin(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<Oid> {
    let (result, _rest) = backend_utils_adt_numutils::uint32in_subr(s, false, "oid", escontext)?;
    Ok(result as Oid)
}

/// `oidout` (oid.c:47): `snprintf(result, 12, "%u", o)` — decimal text.
pub fn oidout(o: Oid) -> String {
    format!("{o}")
}

/// `oidrecv` (oid.c:60): `pq_getmsgint(buf, sizeof(Oid))` — the 32-bit big-endian
/// wire form. The caller decodes the `Oid` off the message reader.
pub fn oidrecv(buf: &mut types_stringinfo::StringInfo<'_>) -> PgResult<Oid> {
    let v = backend_libpq_pqformat::pq_getmsgint(buf, core::mem::size_of::<Oid>() as i32)?;
    Ok(v as Oid)
}

/// `oidsend` (oid.c:71): `pq_begintypsend` + `pq_sendint32(arg1)` +
/// `pq_endtypsend` — the 32-bit big-endian wire form as a `bytea`.
pub fn oidsend<'mcx>(mcx: mcx::Mcx<'mcx>, arg1: Oid) -> PgResult<types_datum::Bytea<'mcx>> {
    let mut buf = backend_libpq_pqformat::pq_begintypsend(mcx)?;
    backend_libpq_pqformat::pq_sendint32(&mut buf, arg1)?;
    Ok(backend_libpq_pqformat::pq_endtypsend(buf))
}

/// `buildoidvector(oids, n)` (oid.c:84): build the `oidvector` on-disk image — a
/// 1-D `ArrayType` of `OIDOID` (4-byte pass-by-value, int-aligned, no NULLs)
/// whose index lower bound is 0 (not 1), matching the historical oidvector
/// layout. An empty input yields a zero-dimension array.
pub fn buildoidvector<'mcx>(mcx: mcx::Mcx<'mcx>, oids: &[Oid]) -> PgResult<mcx::PgVec<'mcx, u8>> {
    // construct_md_array(elems, NULL, 1, &dim1, &lbound0, OIDOID, sizeof(Oid),
    //                    true /* byval */, TYPALIGN_INT)
    let datums: Vec<types_datum::Datum> =
        oids.iter().map(|&o| types_datum::Datum::from_oid(o)).collect();
    let n = oids.len() as i32;
    backend_utils_adt_arrayfuncs::construct::construct_md_array(
        mcx,
        &datums,
        None,
        1,
        &[n],
        &[0], // lbound 0, per oidvector convention
        OIDOID,
        core::mem::size_of::<Oid>() as i32,
        true,
        b'i', // TYPALIGN_INT
    )
}

/// `oidvectorin` (oid.c:122): parse a whitespace-separated list of OIDs into an
/// `oidvector` image. A soft parse error (bad OID token) records into
/// `escontext` and returns `None` (C's `PG_RETURN_NULL`); a hard error
/// propagates as `Err`.
pub fn oidvectorin<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    input: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<mcx::PgVec<'mcx, u8>>> {
    let mut oids: Vec<Oid> = Vec::new();
    let mut rest = input;
    loop {
        // while (*oidString && isspace(*oidString)) oidString++;
        rest = rest.trim_start_matches(|c: char| c.is_ascii_whitespace());
        if rest.is_empty() {
            break;
        }
        // result->values[n] = uint32in_subr(oidString, &oidString, "oid", escontext);
        let (val, after) = backend_utils_adt_numutils::uint32in_subr(
            rest,
            true,
            "oid",
            escontext.as_deref_mut(),
        )?;
        // if (SOFT_ERROR_OCCURRED(escontext)) PG_RETURN_NULL();
        if escontext.as_deref().map(|e| e.error_occurred()).unwrap_or(false) {
            return Ok(None);
        }
        oids.push(val as Oid);
        rest = after;
    }
    Ok(Some(buildoidvector(mcx, &oids)?))
}

/// `oidvectorout` (oid.c:170): render an `oidvector` image as a
/// space-separated decimal OID list. The header is validated first
/// (`check_valid_oidvector`).
pub fn oidvectorout(ndim: i32, dataoffset: i32, elemtype: Oid, values: &[Oid]) -> PgResult<String> {
    check_valid_oidvector(ndim, dataoffset, elemtype)?;
    let mut out = String::new();
    for (i, v) in values.iter().enumerate() {
        if i != 0 {
            out.push(' ');
        }
        out.push_str(&format!("{v}"));
    }
    Ok(out)
}

/// `check_valid_oidvector` (oid.c:118): validate that an array object meets the
/// `oidvector` restrictions — `ndim == 1`, `dataoffset == 0` (no nulls), and
/// `elemtype == OIDOID`. A violation is
/// `ereport(ERROR, ERRCODE_DATATYPE_MISMATCH, "array is not a valid oidvector")`.
///
/// The array header is already decoded by the caller (the carrier lives in the
/// array subsystem), so this takes the three checked header fields.
pub fn check_valid_oidvector(ndim: i32, dataoffset: i32, elemtype: Oid) -> PgResult<()> {
    if ndim != 1 || dataoffset != 0 || elemtype != OIDOID {
        return Err(
            PgError::error("array is not a valid oidvector").with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
        );
    }
    Ok(())
}

/// `oidparse(node)` (oid.c:264): get an `Oid` from an `ICONST`/`FCONST` parser
/// value node. An `Integer` yields its `ival`; a `Float` (used by the lexer for
/// values too large for `int4`) is re-parsed via `oidin`. Any other node tag is
/// `elog(ERROR, "unrecognized node type: %d")`.
pub fn oidparse(node: &Node) -> PgResult<Oid> {
    if let Some(i) = node.as_integer() {
        // intVal(node) — the i32 ival, widened to Oid (matches C's
        // `return intVal(node);` assigned to an Oid).
        return Ok(i.ival as Oid);
    }
    if let Some(f) = node.as_float() {
        // uint32in_subr(castNode(Float, node)->fval, NULL, "oid", NULL) — a hard
        // parse (escontext = NULL).
        let fval = f.fval.as_deref().unwrap_or("");
        return oidin(fval, None);
    }
    Err(PgError::error(format!(
        "unrecognized node type: {}",
        node.node_tag_name()
    )))
}

/// `oid_cmp` (oid.c:287): qsort comparison for `Oid`s — `pg_cmp_u32(v1, v2)`.
pub fn oid_cmp(v1: Oid, v2: Oid) -> i32 {
    // pg_cmp_u32: (a > b) - (a < b).
    (v1 > v2) as i32 - (v1 < v2) as i32
}

// ===========================================================================
// PUBLIC ROUTINES (comparison operators)
// ===========================================================================

/// `oideq` (oid.c:301): `PG_RETURN_BOOL(arg1 == arg2)`.
pub fn oideq(arg1: Oid, arg2: Oid) -> bool {
    arg1 == arg2
}

/// `oidne` (oid.c:310): `PG_RETURN_BOOL(arg1 != arg2)`.
pub fn oidne(arg1: Oid, arg2: Oid) -> bool {
    arg1 != arg2
}

/// `oidlt` (oid.c:319): `PG_RETURN_BOOL(arg1 < arg2)`.
pub fn oidlt(arg1: Oid, arg2: Oid) -> bool {
    arg1 < arg2
}

/// `oidle` (oid.c:328): `PG_RETURN_BOOL(arg1 <= arg2)`.
pub fn oidle(arg1: Oid, arg2: Oid) -> bool {
    arg1 <= arg2
}

/// `oidge` (oid.c:337): `PG_RETURN_BOOL(arg1 >= arg2)`.
pub fn oidge(arg1: Oid, arg2: Oid) -> bool {
    arg1 >= arg2
}

/// `oidgt` (oid.c:346): `PG_RETURN_BOOL(arg1 > arg2)`.
pub fn oidgt(arg1: Oid, arg2: Oid) -> bool {
    arg1 > arg2
}

/// `btoidvectorcmp` (nbtcompare.c:522): the B-tree comparison support function
/// for `oidvector`. Each vector's header is validated (`check_valid_oidvector`)
/// before comparison; the caller decodes the header fields and element values.
/// We sort first by vector length (`a->dim1 - b->dim1`), then element-wise.
pub fn btoidvectorcmp(
    a_ndim: i32,
    a_dataoffset: i32,
    a_elemtype: Oid,
    a: &[Oid],
    b_ndim: i32,
    b_dataoffset: i32,
    b_elemtype: Oid,
    b: &[Oid],
) -> PgResult<i32> {
    check_valid_oidvector(a_ndim, a_dataoffset, a_elemtype)?;
    check_valid_oidvector(b_ndim, b_dataoffset, b_elemtype)?;

    // We arbitrarily choose to sort first by vector length.
    if a.len() != b.len() {
        return Ok(a.len() as i32 - b.len() as i32);
    }
    for i in 0..a.len() {
        if a[i] != b[i] {
            // A_GREATER_THAN_B = 1, A_LESS_THAN_B = -1.
            return Ok(if a[i] > b[i] { 1 } else { -1 });
        }
    }
    Ok(0)
}

/// `oidvectoreq` (oid.c:373): `btoidvectorcmp(...) == 0`.
pub fn oidvectoreq(cmp: i32) -> bool {
    cmp == 0
}
/// `oidvectorne` (oid.c:381): `cmp != 0`.
pub fn oidvectorne(cmp: i32) -> bool {
    cmp != 0
}
/// `oidvectorlt` (oid.c:389): `cmp < 0`.
pub fn oidvectorlt(cmp: i32) -> bool {
    cmp < 0
}
/// `oidvectorle` (oid.c:397): `cmp <= 0`.
pub fn oidvectorle(cmp: i32) -> bool {
    cmp <= 0
}
/// `oidvectorge` (oid.c:405): `cmp >= 0`.
pub fn oidvectorge(cmp: i32) -> bool {
    cmp >= 0
}
/// `oidvectorgt` (oid.c:413): `cmp > 0`.
pub fn oidvectorgt(cmp: i32) -> bool {
    cmp > 0
}

/// `oidlarger` (oid.c:355): `PG_RETURN_OID((arg1 > arg2) ? arg1 : arg2)`.
pub fn oidlarger(arg1: Oid, arg2: Oid) -> Oid {
    if arg1 > arg2 {
        arg1
    } else {
        arg2
    }
}

/// `oidsmaller` (oid.c:364): `PG_RETURN_OID((arg1 < arg2) ? arg1 : arg2)`.
pub fn oidsmaller(arg1: Oid, arg2: Oid) -> Oid {
    if arg1 < arg2 {
        arg1
    } else {
        arg2
    }
}

// ===========================================================================
// Seam install + fmgr builtin registration.
// ===========================================================================

/// Install the inward seams this unit owns and register the `oid.c` fmgr
/// builtins (so `fmgr_isbuiltin` resolves them on the fast path).
pub fn init_seams() {
    use backend_utils_adt_oid_seams as seam;

    // oidparse(node) -> Oid
    seam::oidparse::set(oidparse);

    // oidin(s, soft) -> Option<Oid>. `soft = true` models a soft ErrorSaveContext
    // (an out-of-range/malformed value is Ok(None)); `soft = false` propagates a
    // hard error on Err.
    seam::oidin::set(|s: &str, soft: bool| -> PgResult<Option<Oid>> {
        if soft {
            let mut escontext = SoftErrorContext::new(true);
            let result = oidin(s, Some(&mut escontext))?;
            if escontext.error_occurred() {
                Ok(None)
            } else {
                Ok(Some(result))
            }
        } else {
            Ok(Some(oidin(s, None)?))
        }
    });

    // check_valid_oidvector(ndim, dataoffset, elemtype) -> ()
    seam::check_valid_oidvector::set(check_valid_oidvector);

    fmgr_builtins::register_oid_builtins();
}

#[cfg(test)]
mod tests;
