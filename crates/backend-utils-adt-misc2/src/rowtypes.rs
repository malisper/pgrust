//! Family `rowtypes` — `src/backend/utils/adt/rowtypes.c`.
//!
//! Composite-type (RECORD / named row) I/O and operators: `record_in` /
//! `record_out` / `record_recv` / `record_send`, the comparison engine
//! (`record_cmp` + `record_eq` and the bt/lt/le/gt/ge/ne wrappers,
//! `record_larger` / `record_smaller`), the byte-image comparison family
//! (`record_image_cmp` / `record_image_eq` + wrappers, `btrecordimagecmp`),
//! and `hash_record` / `hash_record_extended`.
//!
//! These deconstruct/construct composite Datums and call per-column type I/O
//! and comparison functions, so they take `Mcx` and surface `ereport`s as
//! `PgResult`. A composite value crosses as the owned
//! [`FormedTuple`](types_tuple::backend_access_common_heaptuple::FormedTuple) —
//! the real `HeapTupleHeader`-backed struct C reaches through
//! `PG_GETARG_HEAPTUPLEHEADER` / returns through `PG_RETURN_HEAPTUPLEHEADER`,
//! not an opaque `Datum`. Independent of the keystone (does not touch expanded
//! records).
//!
//! The C functions are `PG_FUNCTION_ARGS` SQL functions that cache per-call I/O
//! metadata in `fcinfo->flinfo->fn_extra` (the `RecordIOData` /
//! `RecordCompareData` structs). The owned signatures carry no `fcinfo`, so the
//! cache is simply not retained between calls — every call re-looks-up the
//! column I/O / comparison / hash support functions, which is behaviour-
//! preserving (the cache is a pure optimisation guarded by an unchanged
//! record-type check). Each such per-column lookup+invoke is encapsulated in a
//! seam owned by the genuinely-unported owner of that machinery:
//! `lookup_type_cache` + `FunctionCallInvoke` -> typcache-seams
//! (`record_column_{cmp,eq,hash,hash_extended}`); `getType*Info` + the
//! input/output/receive/send fmgr calls -> fmgr-seams
//! (`record_column_{input,receive,output,send}`); the per-column byte-image
//! equality (`record_image_eq`) goes through datum.c's canonical
//! `datum_image_eq(Datum, Datum, typByVal, typLen)` -> datum-seams. The
//! per-column byte-image three-way comparison of `record_image_cmp` is inlined
//! here exactly as rowtypes.c does it (datum.c owns no `datum_image_cmp` — none
//! exists in PostgreSQL): a by-value word compare, a fixed-length `memcmp`, or a
//! varlena `VARDATA`/length compare over the already-detoasted column image.
//! `format_type_be` -> format-type-seams;
//! `check_stack_depth` -> stack-depth-seams. Tuple form/deform is the real
//! `backend-access-common-heaptuple` unit; the protocol message helpers are the
//! real `backend-libpq-pqformat` unit.

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::Oid;
use types_datum::varlena::VARHDRSZ;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERRCODE_INVALID_TEXT_REPRESENTATION,
};
use types_stringinfo::StringInfo;
use types_tuple::backend_access_common_heaptuple::{FormedTuple, Datum};
use types_tuple::heaptuple::{
    FormData_pg_attribute, HeapTupleHeaderGetTypMod, HeapTupleHeaderGetTypeId, TupleDescData,
};

use backend_access_common_heaptuple::{heap_deform_tuple, heap_form_tuple};
use backend_libpq_pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgbytes, pq_getmsgint, pq_sendbytes, pq_sendint32,
};
use backend_utils_adt_datum_seams::datum_image_eq_v;
use backend_utils_adt_format_type_seams::format_type_be;
use backend_utils_cache_typcache_seams::{
    lookup_rowtype_tupdesc, record_column_cmp, record_column_eq, record_column_hash,
    record_column_hash_extended,
};
use backend_utils_fmgr_fmgr_seams::{
    record_column_input, record_column_output, record_column_receive, record_column_send,
};
use backend_utils_misc_stack_depth_seams::check_stack_depth;

/// `RECORDOID` (`catalog/pg_type_d.h`): the pseudo-type OID of an anonymous
/// composite / RECORD value.
const RECORDOID: Oid = 2249;

/// `FirstGenbkiObjectId` (`access/transam.h`): OIDs below this are hand-assigned
/// built-in objects whose values are stable across installations; used by
/// `record_recv` to decide whether a column-type mismatch is worth complaining
/// about.
const FIRST_GENBKI_OBJECT_ID: Oid = 10000;

/// `format_type_be(oid)` as an owned `String` for an error message.
fn fmt_type(mcx: Mcx<'_>, oid: Oid) -> PgResult<alloc::string::String> {
    let s = format_type_be::call(mcx, oid)?;
    Ok(s.as_str().into())
}

/// `record_in(string, typioparam, typmod)` — input routine for any composite
/// type.
///
/// `tupioparam` is the composite type OID (C `PG_GETARG_OID(1)`). Returns the
/// formed composite value, or `None` (C `PG_RETURN_NULL`) when a soft error was
/// reported into `escontext`.
pub fn record_in<'mcx>(
    mcx: Mcx<'mcx>,
    string: Option<&str>,
    tupioparam: Oid,
    tup_typmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let string = string.expect("record_in: NULL cstring (strict function)");
    let tup_type = tupioparam;

    check_stack_depth::call()?; // recurses for record-type columns

    // Give a friendly error message if we did not get enough info to identify
    // the target record type.
    if tup_type == RECORDOID && tup_typmod < 0 {
        return ereturn(
            escontext.as_deref_mut(),
            None,
            PgError::error("input of anonymous composite types is not implemented")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        );
    }

    let tupdesc = lookup_rowtype_tupdesc::call(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;

    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(ncolumns);
    let mut nulls: Vec<bool> = Vec::with_capacity(ncolumns);
    for _ in 0..ncolumns {
        values.push(Datum::null());
        nulls.push(false);
    }

    // Scan the string. `buf` accumulates the de-quoted data for each column.
    let bytes = string.as_bytes();
    let mut ptr = 0usize;
    let mut buf: Vec<u8> = Vec::new();
    let mut need_comma = false;

    // Allow leading whitespace.
    while ptr < bytes.len() && bytes[ptr].is_ascii_whitespace() {
        ptr += 1;
    }
    if ptr >= bytes.len() || bytes[ptr] != b'(' {
        return ereturn(
            escontext.as_deref_mut(),
            None,
            PgError::error(format!("malformed record literal: \"{string}\""))
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .with_detail("Missing left parenthesis."),
        );
    }
    ptr += 1;

    for i in 0..ncolumns {
        let att = tupdesc.attr(i);
        let column_type = att.atttypid;

        // Ignore dropped columns in datatype, but fill with nulls.
        if att.attisdropped {
            values[i] = Datum::null();
            nulls[i] = true;
            continue;
        }

        if need_comma {
            // Skip comma that separates prior field from this one.
            if ptr < bytes.len() && bytes[ptr] == b',' {
                ptr += 1;
            } else {
                // *ptr must be ')'
                return ereturn(
            escontext.as_deref_mut(),
                    None,
                    PgError::error(format!("malformed record literal: \"{string}\""))
                        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                        .with_detail("Too few columns."),
                );
            }
        }

        // Check for null: completely empty input means null.
        let column_data: Option<alloc::string::String>;
        if ptr >= bytes.len() || bytes[ptr] == b',' || bytes[ptr] == b')' {
            column_data = None;
            nulls[i] = true;
        } else {
            // Extract string for this column.
            let mut inquote = false;
            buf.clear();
            while inquote || !(ptr >= bytes.len() || bytes[ptr] == b',' || bytes[ptr] == b')') {
                if ptr >= bytes.len() {
                    // ch == '\0' (end of input) inside the loop body in C.
                    return ereturn(
            escontext.as_deref_mut(),
                        None,
                        PgError::error(format!("malformed record literal: \"{string}\""))
                            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                            .with_detail("Unexpected end of input."),
                    );
                }
                let ch = bytes[ptr];
                ptr += 1;
                if ch == b'\\' {
                    if ptr >= bytes.len() {
                        return ereturn(
            escontext.as_deref_mut(),
                            None,
                            PgError::error(format!("malformed record literal: \"{string}\""))
                                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                                .with_detail("Unexpected end of input."),
                        );
                    }
                    buf.push(bytes[ptr]);
                    ptr += 1;
                } else if ch == b'"' {
                    if !inquote {
                        inquote = true;
                    } else if ptr < bytes.len() && bytes[ptr] == b'"' {
                        // doubled quote within quote sequence
                        buf.push(bytes[ptr]);
                        ptr += 1;
                    } else {
                        inquote = false;
                    }
                } else {
                    buf.push(ch);
                }
            }
            // The de-quoted column text. C feeds a NUL-terminated cstring; we
            // pass it as &str (the input function never sees the NUL).
            column_data = Some(
                alloc::string::String::from_utf8(buf.clone())
                    .unwrap_or_else(|e| unsafe {
                        alloc::string::String::from_utf8_unchecked(e.into_bytes())
                    }),
            );
            nulls[i] = false;
        }

        // Convert the column value.
        let conv = record_column_input::call(
            mcx,
            column_type,
            column_data.as_deref(),
            att.atttypmod,
            // C threads the same escontext through InputFunctionCallSafe. A
            // soft error there returns Ok(None); we then bail with a NULL
            // result, matching C's `goto fail` (ReleaseTupleDesc +
            // PG_RETURN_NULL after the inner errsave populated escontext).
            escontext.as_deref_mut(),
        )?;
        match conv {
            Some(v) => {
                values[i] = v;
                nulls[i] = false;
            }
            None => {
                // Soft error reported by the column input function: C's
                // `goto fail` -> ReleaseTupleDesc + PG_RETURN_NULL.
                return Ok(None);
            }
        }

        // Prep for next column.
        need_comma = true;
    }

    if ptr >= bytes.len() || bytes[ptr] != b')' {
        return ereturn(
            escontext.as_deref_mut(),
            None,
            PgError::error(format!("malformed record literal: \"{string}\""))
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .with_detail("Too many columns."),
        );
    }
    ptr += 1;
    // Allow trailing whitespace.
    while ptr < bytes.len() && bytes[ptr].is_ascii_whitespace() {
        ptr += 1;
    }
    if ptr < bytes.len() {
        return ereturn(
            escontext.as_deref_mut(),
            None,
            PgError::error(format!("malformed record literal: \"{string}\""))
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .with_detail("Junk after right parenthesis."),
        );
    }

    let tuple = form_tuple(mcx, &tupdesc, &values, &nulls)?;
    Ok(Some(tuple))
}

/// `record_out(record)` — output routine for any composite type.
///
/// Returns the textual representation as raw bytes (C's `cstring`, NUL
/// excluded).
pub fn record_out<'mcx>(mcx: Mcx<'mcx>, record: &FormedTuple<'_>) -> PgResult<mcx::PgVec<'mcx, u8>> {
    check_stack_depth::call()?; // recurses for record-type columns

    // Extract type info from the tuple itself.
    let rec = record
        .tuple
        .t_data
        .as_ref()
        .expect("record_out: composite datum has no header");
    let tup_type = HeapTupleHeaderGetTypeId(rec);
    let tup_typmod = HeapTupleHeaderGetTypMod(rec);
    let tupdesc = lookup_rowtype_tupdesc::call(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;

    // Break down the tuple into fields.
    let cols = heap_deform_tuple(mcx, &record.tuple, &tupdesc, &record.data)?;

    // Build the result string.
    let mut out: Vec<u8> = Vec::new();
    out.push(b'(');
    let mut need_comma = false;

    for i in 0..ncolumns {
        let att = tupdesc.attr(i);
        let column_type = att.atttypid;

        // Ignore dropped columns in datatype.
        if att.attisdropped {
            continue;
        }

        if need_comma {
            out.push(b',');
        }
        need_comma = true;

        let (value, isnull) = &cols[i];
        if *isnull {
            // emit nothing...
            continue;
        }

        // Convert the column value to text.
        let value_bytes = record_column_output::call(mcx, column_type, value)?;

        // Detect whether we need double quotes for this value.
        let mut nq = value_bytes.is_empty(); // force quotes for empty string
        if !nq {
            for &ch in value_bytes.iter() {
                if ch == b'"'
                    || ch == b'\\'
                    || ch == b'('
                    || ch == b')'
                    || ch == b','
                    || ch.is_ascii_whitespace()
                {
                    nq = true;
                    break;
                }
            }
        }

        // And emit the string.
        if nq {
            out.push(b'"');
        }
        for &ch in value_bytes.iter() {
            if ch == b'"' || ch == b'\\' {
                out.push(ch);
            }
            out.push(ch);
        }
        if nq {
            out.push(b'"');
        }
    }

    out.push(b')');

    mcx::slice_in(mcx, &out)
}

/// `record_recv(buf, typioparam, typmod)` — binary input routine for any
/// composite type.
pub fn record_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &[u8],
    tupioparam: Oid,
    tup_typmod: i32,
) -> PgResult<FormedTuple<'mcx>> {
    let tup_type = tupioparam;

    check_stack_depth::call()?; // recurses for record-type columns

    if tup_type == RECORDOID && tup_typmod < 0 {
        return Err(
            PgError::error("input of anonymous composite types is not implemented")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        );
    }

    let tupdesc = lookup_rowtype_tupdesc::call(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;

    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(ncolumns);
    let mut nulls: Vec<bool> = Vec::with_capacity(ncolumns);
    for _ in 0..ncolumns {
        values.push(Datum::null());
        nulls.push(false);
    }

    // Read the message via a StringInfo cursor over the supplied bytes.
    let mut msg = StringInfo::from_vec(slice_to_pgvec(mcx, buf)?);

    // Fetch number of columns user thinks it has.
    let usercols = pq_getmsgint(&mut msg, 4)? as i32;

    // Need to scan to count nondeleted columns.
    let mut validcols = 0i32;
    for i in 0..ncolumns {
        if !tupdesc.attr(i).attisdropped {
            validcols += 1;
        }
    }
    if usercols != validcols {
        return Err(PgError::error(format!(
            "wrong number of columns: {usercols}, expected {validcols}"
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    // Process each column.
    for i in 0..ncolumns {
        let att = tupdesc.attr(i);
        let column_type = att.atttypid;

        // Ignore dropped columns in datatype, but fill with nulls.
        if att.attisdropped {
            values[i] = Datum::null();
            nulls[i] = true;
            continue;
        }

        // Check column type recorded in the data.
        let coltypoid = pq_getmsgint(&mut msg, 4)?; // sizeof(Oid) == 4

        // Complain about a type mismatch only when both OIDs are built-in.
        if coltypoid != column_type
            && coltypoid < FIRST_GENBKI_OBJECT_ID
            && column_type < FIRST_GENBKI_OBJECT_ID
        {
            return Err(PgError::error(format!(
                "binary data has type {} ({}) instead of expected {} ({}) in record column {}",
                coltypoid,
                fmt_type(mcx, coltypoid)?,
                column_type,
                fmt_type(mcx, column_type)?,
                i + 1
            ))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
        }

        // Get and check the item length.
        let itemlen = pq_getmsgint(&mut msg, 4)? as i32;
        let remaining = (msg.len() - msg.cursor) as i64;
        if itemlen < -1 || (itemlen as i64) > remaining {
            return Err(PgError::error("insufficient data left in message")
                .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
        }

        if itemlen == -1 {
            // -1 length means NULL.
            values[i] = record_column_receive::call(
                mcx,
                column_type,
                None,
                att.atttypmod,
                (i + 1) as i32,
            )?;
            nulls[i] = true;
        } else {
            // Read the column's item bytes off the message cursor (the C
            // initReadOnlyStringInfo points into the buffer; here we hand the
            // exact slice to the receive seam, which verifies it was fully
            // consumed).
            let item = pq_getmsgbytes(&mut msg, itemlen as usize)?;
            let item_owned = slice_to_pgvec(mcx, item)?;
            values[i] = record_column_receive::call(
                mcx,
                column_type,
                Some(&item_owned),
                att.atttypmod,
                (i + 1) as i32,
            )?;
            nulls[i] = false;
        }
    }

    form_tuple(mcx, &tupdesc, &values, &nulls)
}

/// `record_send(record)` — binary output routine for any composite type.
///
/// Returns the `bytea` payload bytes (varlena header stripped).
pub fn record_send<'mcx>(
    mcx: Mcx<'mcx>,
    record: &FormedTuple<'_>,
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    check_stack_depth::call()?; // recurses for record-type columns

    let rec = record
        .tuple
        .t_data
        .as_ref()
        .expect("record_send: composite datum has no header");
    let tup_type = HeapTupleHeaderGetTypeId(rec);
    let tup_typmod = HeapTupleHeaderGetTypMod(rec);
    let tupdesc = lookup_rowtype_tupdesc::call(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;

    // Break down the tuple into fields.
    let cols = heap_deform_tuple(mcx, &record.tuple, &tupdesc, &record.data)?;

    // And build the result string.
    let mut buf = pq_begintypsend(mcx)?;

    // Need to scan to count nondeleted columns.
    let mut validcols = 0i32;
    for i in 0..ncolumns {
        if !tupdesc.attr(i).attisdropped {
            validcols += 1;
        }
    }
    pq_sendint32(&mut buf, validcols as u32)?;

    for i in 0..ncolumns {
        let att = tupdesc.attr(i);
        let column_type = att.atttypid;

        // Ignore dropped columns in datatype.
        if att.attisdropped {
            continue;
        }

        pq_sendint32(&mut buf, column_type)?;

        let (value, isnull) = &cols[i];
        if *isnull {
            // emit -1 data length to signify a NULL
            pq_sendint32(&mut buf, (-1i32) as u32)?;
            continue;
        }

        // Convert the column value to binary (payload bytes, header stripped).
        let outputbytes = record_column_send::call(mcx, column_type, value)?;
        pq_sendint32(&mut buf, outputbytes.len() as u32)?;
        pq_sendbytes(&mut buf, &outputbytes)?;
    }

    let bytea = pq_endtypsend(buf);
    // Cross out the bytea's payload bytes (header stripped), matching the
    // PG_RETURN_BYTEA_P contract callers see.
    let payload = bytea.data();
    mcx::slice_in(mcx, payload)
}

/// `record_cmp(record1, record2)` — internal three-way comparison engine shared
/// by the btree/equality wrappers. Returns -1, 0 or 1.
///
/// Do not assume the two inputs are exactly the same record type; compare as
/// long as they have the same number of non-dropped columns of the same types.
pub fn record_cmp<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<i32> {
    check_stack_depth::call()?; // recurses for record-type columns

    let (tupdesc1, ncolumns1, cols1) = deform_record(mcx, left)?;
    let (tupdesc2, ncolumns2, cols2) = deform_record(mcx, right)?;

    let mut result = 0i32;

    // Scan corresponding columns, allowing for dropped columns in different
    // places. i1 and i2 are physical column indexes, j is the logical index.
    let mut i1 = 0usize;
    let mut i2 = 0usize;
    let mut j = 0usize;
    while i1 < ncolumns1 || i2 < ncolumns2 {
        // Skip dropped columns.
        if i1 < ncolumns1 && tupdesc1.attr(i1).attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < ncolumns2 && tupdesc2.attr(i2).attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= ncolumns1 || i2 >= ncolumns2 {
            break; // we'll deal with mismatch below loop
        }

        let att1 = tupdesc1.attr(i1);
        let att2 = tupdesc2.attr(i2);

        let collation = matching_collation(mcx, att1, att2, j)?;

        // We consider two NULLs equal; NULL > not-NULL.
        let (v1, n1) = &cols1[i1];
        let (v2, n2) = &cols2[i2];
        if !*n1 || !*n2 {
            if *n1 {
                // arg1 is greater than arg2
                result = 1;
                break;
            }
            if *n2 {
                // arg1 is less than arg2
                result = -1;
                break;
            }

            // Compare the pair of elements.
            let cmpresult = record_column_cmp::call(att1.atttypid, collation, v1, v2)?;
            if cmpresult < 0 {
                result = -1;
                break;
            } else if cmpresult > 0 {
                result = 1;
                break;
            }
        }

        // equal, so continue to next column
        i1 += 1;
        i2 += 1;
        j += 1;
    }

    // If we didn't break out early, check for column count mismatch.
    if result == 0 && (i1 != ncolumns1 || i2 != ncolumns2) {
        return Err(PgError::error(
            "cannot compare record types with different numbers of columns",
        )
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    Ok(result)
}

/// `record_eq(record1, record2)` — compares two records for equality.
///
/// Note: this does not use [`record_cmp`], since equality may be meaningful in
/// datatypes that don't have a total ordering.
pub fn record_eq<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    check_stack_depth::call()?; // recurses for record-type columns

    let (tupdesc1, ncolumns1, cols1) = deform_record(mcx, left)?;
    let (tupdesc2, ncolumns2, cols2) = deform_record(mcx, right)?;

    let mut result = true;

    let mut i1 = 0usize;
    let mut i2 = 0usize;
    let mut j = 0usize;
    while i1 < ncolumns1 || i2 < ncolumns2 {
        // Skip dropped columns.
        if i1 < ncolumns1 && tupdesc1.attr(i1).attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < ncolumns2 && tupdesc2.attr(i2).attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= ncolumns1 || i2 >= ncolumns2 {
            break;
        }

        let att1 = tupdesc1.attr(i1);
        let att2 = tupdesc2.attr(i2);

        let collation = matching_collation(mcx, att1, att2, j)?;

        // We consider two NULLs equal; NULL > not-NULL.
        let (v1, n1) = &cols1[i1];
        let (v2, n2) = &cols2[i2];
        if !*n1 || !*n2 {
            if *n1 || *n2 {
                result = false;
                break;
            }

            // Compare the pair of elements; C treats a null operator result as
            // false, which the eq seam folds into its bool return.
            let oprresult = record_column_eq::call(att1.atttypid, collation, v1, v2)?;
            if !oprresult {
                result = false;
                break;
            }
        }

        i1 += 1;
        i2 += 1;
        j += 1;
    }

    if result && (i1 != ncolumns1 || i2 != ncolumns2) {
        return Err(PgError::error(
            "cannot compare record types with different numbers of columns",
        )
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    Ok(result)
}

/// `record_ne(record1, record2)`.
pub fn record_ne<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    Ok(!record_eq(mcx, left, right)?)
}

/// `record_lt(record1, record2)`.
pub fn record_lt<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    Ok(record_cmp(mcx, left, right)? < 0)
}

/// `record_gt(record1, record2)`.
pub fn record_gt<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    Ok(record_cmp(mcx, left, right)? > 0)
}

/// `record_le(record1, record2)`.
pub fn record_le<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    Ok(record_cmp(mcx, left, right)? <= 0)
}

/// `record_ge(record1, record2)`.
pub fn record_ge<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    Ok(record_cmp(mcx, left, right)? >= 0)
}

/// `btrecordcmp(record1, record2)`.
pub fn btrecordcmp<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<i32> {
    record_cmp(mcx, left, right)
}

/// `record_larger(record1, record2)`: returns whichever of the two compares
/// greater (arg1 on a tie, mirroring C `PG_RETURN_DATUM(PG_GETARG_DATUM(...))`).
pub fn record_larger<'mcx>(
    mcx: Mcx<'mcx>,
    left: FormedTuple<'mcx>,
    right: FormedTuple<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    if record_cmp(mcx, &left, &right)? > 0 {
        Ok(left)
    } else {
        Ok(right)
    }
}

/// `record_smaller(record1, record2)`.
pub fn record_smaller<'mcx>(
    mcx: Mcx<'mcx>,
    left: FormedTuple<'mcx>,
    right: FormedTuple<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    if record_cmp(mcx, &left, &right)? < 0 {
        Ok(left)
    } else {
        Ok(right)
    }
}

/// `record_image_cmp(record1, record2)` — internal byte-oriented comparison
/// engine. Returns -1, 0 or 1.
///
/// Note: different representations of values considered equal are not considered
/// identical (e.g. citext 'A' vs 'a').
pub fn record_image_cmp<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<i32> {
    let (tupdesc1, ncolumns1, cols1) = deform_record(mcx, left)?;
    let (tupdesc2, ncolumns2, cols2) = deform_record(mcx, right)?;

    let mut result = 0i32;

    let mut i1 = 0usize;
    let mut i2 = 0usize;
    let mut j = 0usize;
    while i1 < ncolumns1 || i2 < ncolumns2 {
        // Skip dropped columns.
        if i1 < ncolumns1 && tupdesc1.attr(i1).attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < ncolumns2 && tupdesc2.attr(i2).attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= ncolumns1 || i2 >= ncolumns2 {
            break;
        }

        let att1 = tupdesc1.attr(i1);
        let att2 = tupdesc2.attr(i2);

        // Have two matching columns, they must be same type.
        if att1.atttypid != att2.atttypid {
            return Err(PgError::error(format!(
                "cannot compare dissimilar column types {} and {} at record column {}",
                fmt_type(mcx, att1.atttypid)?,
                fmt_type(mcx, att2.atttypid)?,
                j + 1
            ))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
        }
        // The same type should have the same length (Assert in C).
        debug_assert_eq!(att1.attlen, att2.attlen);

        // We consider two NULLs equal; NULL > not-NULL.
        let (v1, n1) = &cols1[i1];
        let (v2, n2) = &cols2[i2];
        if !*n1 || !*n2 {
            if *n1 {
                result = 1;
                break;
            }
            if *n2 {
                result = -1;
                break;
            }

            // Compare the pair of elements byte-image-wise, inlined exactly as
            // rowtypes.c does (incl. the elog(ERROR, "unexpected attlen") for
            // any other length). Uses att1's storage, as C does.
            let cmpresult = column_image_cmp(v1, v2, att1.attbyval, att1.attlen)?;
            if cmpresult < 0 {
                result = -1;
                break;
            } else if cmpresult > 0 {
                result = 1;
                break;
            }
        }

        i1 += 1;
        i2 += 1;
        j += 1;
    }

    if result == 0 && (i1 != ncolumns1 || i2 != ncolumns2) {
        return Err(PgError::error(
            "cannot compare record types with different numbers of columns",
        )
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    Ok(result)
}

/// `record_image_eq(record1, record2)` — compares two records for identical
/// byte-image contents.
///
/// Note: this does not use [`record_image_cmp`], since unequal lengths can be
/// detected without de-toasting.
pub fn record_image_eq<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    let (tupdesc1, ncolumns1, cols1) = deform_record(mcx, left)?;
    let (tupdesc2, ncolumns2, cols2) = deform_record(mcx, right)?;

    let mut result = true;

    let mut i1 = 0usize;
    let mut i2 = 0usize;
    let mut j = 0usize;
    while i1 < ncolumns1 || i2 < ncolumns2 {
        // Skip dropped columns.
        if i1 < ncolumns1 && tupdesc1.attr(i1).attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < ncolumns2 && tupdesc2.attr(i2).attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= ncolumns1 || i2 >= ncolumns2 {
            break;
        }

        let att1 = tupdesc1.attr(i1);
        let att2 = tupdesc2.attr(i2);

        // Have two matching columns, they must be same type.
        if att1.atttypid != att2.atttypid {
            return Err(PgError::error(format!(
                "cannot compare dissimilar column types {} and {} at record column {}",
                fmt_type(mcx, att1.atttypid)?,
                fmt_type(mcx, att2.atttypid)?,
                j + 1
            ))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
        }

        // We consider two NULLs equal; NULL > not-NULL.
        let (v1, n1) = &cols1[i1];
        let (v2, n2) = &cols2[i2];
        if !*n1 || !*n2 {
            if *n1 || *n2 {
                result = false;
                break;
            }

            // Compare the pair of elements via datum.c's canonical
            // `datum_image_eq(value1, value2, typByVal, typLen)` over att1's
            // storage, exactly as rowtypes.c does. The unified-value `_v` seam
            // takes the columns as `&Datum<'mcx>` directly (by-value arms are
            // the scalar word, by-reference arms the detoasted image), so no
            // pointer-forge bridge is needed.
            result = datum_image_eq_v::call(
                v1,
                v2,
                att1.attbyval,
                att1.attlen,
            )?;
            if !result {
                break;
            }
        }

        i1 += 1;
        i2 += 1;
        j += 1;
    }

    if result && (i1 != ncolumns1 || i2 != ncolumns2) {
        return Err(PgError::error(
            "cannot compare record types with different numbers of columns",
        )
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }

    Ok(result)
}

/// `record_image_ne(record1, record2)`.
pub fn record_image_ne<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    Ok(!record_image_eq(mcx, left, right)?)
}

/// `record_image_lt(record1, record2)`.
pub fn record_image_lt<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    Ok(record_image_cmp(mcx, left, right)? < 0)
}

/// `record_image_gt(record1, record2)`.
pub fn record_image_gt<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    Ok(record_image_cmp(mcx, left, right)? > 0)
}

/// `record_image_le(record1, record2)`.
pub fn record_image_le<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    Ok(record_image_cmp(mcx, left, right)? <= 0)
}

/// `record_image_ge(record1, record2)`.
pub fn record_image_ge<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<bool> {
    Ok(record_image_cmp(mcx, left, right)? >= 0)
}

/// `btrecordimagecmp(record1, record2)`.
pub fn btrecordimagecmp<'mcx>(
    mcx: Mcx<'mcx>,
    left: &FormedTuple<'_>,
    right: &FormedTuple<'_>,
) -> PgResult<i32> {
    record_image_cmp(mcx, left, right)
}

/// `hash_record(record)`.
pub fn hash_record<'mcx>(mcx: Mcx<'mcx>, record: &FormedTuple<'_>) -> PgResult<u32> {
    check_stack_depth::call()?; // recurses for record-type columns

    let (tupdesc, ncolumns, cols) = deform_record(mcx, record)?;

    let mut result: u32 = 0;
    for i in 0..ncolumns {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            continue;
        }

        // Compute hash of element.
        let element_hash = if cols[i].1 {
            0u32
        } else {
            record_column_hash::call(att.atttypid, att.attcollation, &cols[i].0)?
        };

        // see hash_array()
        result = (result << 5).wrapping_sub(result).wrapping_add(element_hash);
    }

    Ok(result)
}

/// `hash_record_extended(record, seed)`.
pub fn hash_record_extended<'mcx>(
    mcx: Mcx<'mcx>,
    record: &FormedTuple<'_>,
    seed: u64,
) -> PgResult<u64> {
    check_stack_depth::call()?; // recurses for record-type columns

    let (tupdesc, ncolumns, cols) = deform_record(mcx, record)?;

    let mut result: u64 = 0;
    for i in 0..ncolumns {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            continue;
        }

        // Compute hash of element.
        let element_hash = if cols[i].1 {
            0u64
        } else {
            record_column_hash_extended::call(att.atttypid, att.attcollation, &cols[i].0, seed)?
        };

        // see hash_array_extended()
        result = (result << 5).wrapping_sub(result).wrapping_add(element_hash);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Helpers shared by the family (the per-function `RecordIOData` /
// `RecordCompareData` boilerplate collapses to these once fn_extra caching is
// dropped).
// ---------------------------------------------------------------------------

/// Look up the record's tuple descriptor (from its composite-datum header) and
/// deform it into `(tupdesc, ncolumns, columns)`. Mirrors the per-function
/// "extract type info from the tuple + heap_deform_tuple" preamble.
type DeformedRecord<'mcx> = (
    mcx::PgBox<'mcx, TupleDescData<'mcx>>,
    usize,
    mcx::PgVec<'mcx, (Datum<'mcx>, bool)>,
);

fn deform_record<'mcx>(mcx: Mcx<'mcx>, record: &FormedTuple<'_>) -> PgResult<DeformedRecord<'mcx>> {
    let rec = record
        .tuple
        .t_data
        .as_ref()
        .expect("rowtypes: composite datum has no header");
    let tup_type = HeapTupleHeaderGetTypeId(rec);
    let tup_typmod = HeapTupleHeaderGetTypMod(rec);
    let tupdesc = lookup_rowtype_tupdesc::call(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;
    let cols = heap_deform_tuple(mcx, &record.tuple, &tupdesc, &record.data)?;
    Ok((tupdesc, ncolumns, cols))
}

/// The two matching columns must be the same type; their effective collation is
/// the first column's collation, or `InvalidOid` (0) when they disagree.
/// `j` is the 0-based logical column index used in the error message.
fn matching_collation(
    mcx: Mcx<'_>,
    att1: &FormData_pg_attribute,
    att2: &FormData_pg_attribute,
    j: usize,
) -> PgResult<Oid> {
    if att1.atttypid != att2.atttypid {
        return Err(PgError::error(format!(
            "cannot compare dissimilar column types {} and {} at record column {}",
            fmt_type(mcx, att1.atttypid)?,
            fmt_type(mcx, att2.atttypid)?,
            j + 1
        ))
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
    }
    // If they're not the same collation, we don't complain here, but the
    // comparison function might.
    let mut collation = att1.attcollation;
    if collation != att2.attcollation {
        collation = 0; // InvalidOid
    }
    Ok(collation)
}

/// `heap_form_tuple` + the composite-datum "copy into a fresh palloc chunk"
/// step of `record_in`/`record_recv` (C copies `tuple->t_data` so the caller
/// can `pfree` the result). In the owned model the formed tuple is already its
/// own allocation in `mcx`, so this is just `heap_form_tuple` mapped to
/// `PgResult`.
fn form_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
    values: &[Datum<'_>],
    nulls: &[bool],
) -> PgResult<FormedTuple<'mcx>> {
    // C `heap_form_tuple` raises ereport(ERROR) for too-many-columns; the owned
    // `HeapTupleError` carries the identical PgError via `From`.
    heap_form_tuple(mcx, tupdesc, values, nulls).map_err(PgError::from)
}

/// Copy a borrowed byte slice into an `mcx`-allocated `PgVec`.
fn slice_to_pgvec<'mcx>(mcx: Mcx<'mcx>, src: &[u8]) -> PgResult<mcx::PgVec<'mcx, u8>> {
    mcx::slice_in(mcx, src)
}

/// The byte-image three-way comparison of two non-null column values, inlined
/// exactly as `record_image_cmp` (rowtypes.c) does it. datum.c owns no
/// `datum_image_cmp` (PostgreSQL has none), so the per-type dispatch lives here:
///
/// * pass-by-value (`attbyval`): compare the raw `Datum` words as unsigned
///   integers, as C does (`values1[i1] != values2[i2]` then `<`).
/// * fixed-length pass-by-reference (`attlen > 0`): `memcmp` of `attlen` bytes.
/// * varlena (`attlen == -1`): compare `VARDATA` over `Min(payload lengths)`,
///   breaking ties by total length. The column image is already detoasted
///   (`Datum::ByRef`), so this reads it directly.
/// * any other `attlen`: the C `elog(ERROR, "unexpected attlen: %d")`.
fn column_image_cmp(
    v1: &Datum<'_>,
    v2: &Datum<'_>,
    attbyval: bool,
    attlen: i16,
) -> PgResult<i32> {
    if attbyval {
        let (w1, w2) = match (v1, v2) {
            (Datum::ByVal(a), Datum::ByVal(b)) => (*a, *b),
            _ => panic!("record_image_cmp: by-value attribute deformed as by-reference"),
        };
        Ok(match w1.cmp(&w2) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        })
    } else if attlen > 0 {
        let b1 = v1.as_ref_bytes();
        let b2 = v2.as_ref_bytes();
        let n = attlen as usize;
        Ok(memcmp_sign(&b1[..n], &b2[..n]))
    } else if attlen == -1 {
        // Both images are already-detoasted in-line varlenas (no external TOAST
        // pointers / compression). They may carry a 1-byte ("short") or 4-byte
        // header, exactly as `heap_deform_tuple` produced them, so read the
        // payload via `VARDATA_ANY` / the logical payload length via
        // `VARSIZE_ANY_EXHDR` (C: `toast_raw_datum_size` minus `VARHDRSZ`).
        let (data1, len1) = varlena_payload(v1.as_ref_bytes());
        let (data2, len2) = varlena_payload(v2.as_ref_bytes());
        let min = core::cmp::min(len1, len2);
        let mut cmpresult = memcmp_sign(&data1[..min], &data2[..min]);
        if cmpresult == 0 && len1 != len2 {
            // Tie-break on logical payload length (C: `len1 < len2`).
            cmpresult = if len1 < len2 { -1 } else { 1 };
        }
        Ok(cmpresult)
    } else {
        Err(PgError::error(format!("unexpected attlen: {attlen}")))
    }
}

/// `memcmp` returning a normalized sign (-1/0/1), as the C comparison sites use.
fn memcmp_sign(a: &[u8], b: &[u8]) -> i32 {
    match a.cmp(b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// `(VARDATA_ANY(ptr), VARSIZE_ANY_EXHDR(ptr))` (varatt.h) for an in-line,
/// already-detoasted varlena image: the payload slice and its length, handling
/// both the 1-byte ("short", `VARATT_IS_1B`) and 4-byte header forms. The total
/// size comes from the shared `varsize_any` (`backend-access-common-heaptuple`);
/// the header width is `VARHDRSZ_SHORT` (1) for a short header, else `VARHDRSZ`
/// (4). `ByRef` never carries an external TOAST pointer (it is detoasted), so
/// only these two forms occur.
fn varlena_payload(b: &[u8]) -> (&[u8], usize) {
    const VARHDRSZ_SHORT: usize = 1;
    // `VARATT_IS_1B(PTR)` (varatt.h, little-endian): low bit of the first byte.
    let hdr = if (b[0] & 0x01) == 0x01 {
        VARHDRSZ_SHORT
    } else {
        VARHDRSZ
    };
    let total = backend_access_common_heaptuple::varsize_any(b);
    (&b[hdr..total], total - hdr)
}
