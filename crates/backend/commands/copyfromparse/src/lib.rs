//! Port of PostgreSQL `src/backend/commands/copyfromparse.c` — the COPY FROM
//! input parser: parse CSV / text / binary format and return the next input
//! line as Datums.
//!
//! The byte-exact codec — the raw-buffer fill / line reading state machine
//! (`CopyReadLineText`), the text / CSV field tokenizers
//! (`CopyReadAttributesText` / `CopyReadAttributesCSV`), the binary readers
//! (`CopyReadBinaryData` / `CopyGetInt16` / `CopyGetInt32` /
//! `CopyReadBinaryAttribute`), and the buffer state machine (`CopyConvertBuf` /
//! `CopyLoadRawBuf` / `CopyLoadInputBuf`) — is implemented entirely in-crate,
//! operating on the owned byte buffers of [`CopyParseState`].
//!
//! The C "[data source] → raw_buf → input_buf → line_buf → attribute_buf"
//! pipeline is reproduced over `Vec<u8>` buffers. The "input_buf points at
//! raw_buf when no transcoding" shortcut is modeled by the `input_is_raw` flag:
//! the codec then reads from / verifies `raw_buf` in place and tracks the
//! verified prefix through `input_buf_len`, exactly as the C does over the
//! single shared buffer.
//!
//! Genuine externals — pulling bytes from the source, the encoding verify /
//! convert routines, the `attnumlist` / `TupleDesc` accessors, the fmgr value
//! layer (`InputFunctionCallSafe` / `ReceiveFunctionCall` / `ExecEvalExpr`), and
//! the libpq frontend (`ReceiveCopyBegin`) — cross the seams in
//! `backend_commands_copyfromparse_seams`, each panicking loudly until a runtime
//! is installed.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use utils_error::ereport;
use types_copy::{
    AttrValue, CopyHeaderChoice, CopyLogVerbosityChoice, CopyOnErrorChoice, CopyParseState,
    CopySource, EolType, FieldRange, BINARY_SIGNATURE, INPUT_BUF_SIZE, MAX_CONVERSION_INPUT_LENGTH,
    RAW_BUF_SIZE,
};
#[allow(unused_imports)]
use types_core::primitive::Oid;
use types_tuple::heaptuple::Datum;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_BAD_COPY_FILE_FORMAT, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERROR,
};

use copyfrom_seams as s;
use mbutils_seams as mb;

#[cfg(test)]
mod tests;

mod init;
pub use init::init_seams;

/* ---------------------------------------------------------------------------
 * Macros / small helpers (copyfromparse.c:78-79).
 * ------------------------------------------------------------------------- */

/// `#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))`.
#[inline]
fn ISOCTAL(c: u8) -> bool {
    (b'0'..=b'7').contains(&c)
}

/// `#define OCTVALUE(c) ((c) - '0')`.
#[inline]
fn OCTVALUE(c: u8) -> i32 {
    (c - b'0') as i32
}

/// `#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch) & HIGHBIT)` (HIGHBIT = 0x80).
#[inline]
fn IS_HIGHBIT_SET(c: u8) -> bool {
    c & 0x80 != 0
}

/// `RAW_BUF_BYTES(cstate)` (copyfrom_internal.h):
/// `((cstate)->raw_buf_len - (cstate)->raw_buf_index)`.
#[inline]
fn RAW_BUF_BYTES(cstate: &CopyParseState) -> i32 {
    cstate.raw_buf_len - cstate.raw_buf_index
}

/// `INPUT_BUF_BYTES(cstate)` (copyfrom_internal.h):
/// `((cstate)->input_buf_len - (cstate)->input_buf_index)`.
#[inline]
fn INPUT_BUF_BYTES(cstate: &CopyParseState) -> i32 {
    cstate.input_buf_len - cstate.input_buf_index
}

/// `ErrorLocation` for `ereport(...).finish(...)`.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("copyfromparse.c", 0, funcname)
}

/// `log_verbosity == COPY_LOG_VERBOSITY_VERBOSE`.
#[inline]
fn is_verbose(v: CopyLogVerbosityChoice) -> bool {
    matches!(v, CopyLogVerbosityChoice::COPY_LOG_VERBOSITY_VERBOSE)
}

/// The physical buffer the C `input_buf` refers to (== `raw_buf` when aliased).
#[inline]
fn input_byte(cstate: &CopyParseState, i: usize) -> u8 {
    if cstate.input_is_raw {
        cstate.raw_buf[i]
    } else {
        cstate.input_buf[i]
    }
}

/* ===========================================================================
 * ReceiveCopyBegin / ReceiveCopyBinaryHeader (copyfromparse.c:169-229).
 * =========================================================================== */

/// `ReceiveCopyBegin(cstate)` (copyfromparse.c:169) — the libpq side of starting
/// a COPY IN: send the `CopyInResponse` (overall + per-column formats), switch
/// the source to `COPY_FRONTEND`, allocate `fe_msgbuf`, and flush.
pub fn ReceiveCopyBegin(cstate: &mut CopyParseState) -> PgResult<()> {
    // int natts = list_length(cstate->attnumlist);
    let natts = cstate.attnumlist.len() as i32;
    // int16 format = (cstate->opts.binary ? 1 : 0);
    let binary = cstate.opts.binary;
    s::receive_copy_begin::call(cstate, natts, binary)
}

/// `ReceiveCopyBinaryHeader(cstate)` (copyfromparse.c:189) — read and verify the
/// 11-byte binary signature, the flags field, and skip the header extension.
pub fn ReceiveCopyBinaryHeader(cstate: &mut CopyParseState) -> PgResult<()> {
    // char readSig[11]; int32 tmp;
    let mut read_sig = [0u8; 11];

    /* Signature */
    if CopyReadBinaryData(cstate, &mut read_sig, 11)? != 11 || read_sig != BINARY_SIGNATURE {
        return ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg("COPY file signature not recognized")
            .finish(here("ReceiveCopyBinaryHeader"));
    }

    /* Flags field */
    let mut tmp = 0i32;
    if !CopyGetInt32(cstate, &mut tmp)? {
        return ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg("invalid COPY file header (missing flags)")
            .finish(here("ReceiveCopyBinaryHeader"));
    }
    if (tmp & (1 << 16)) != 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg("invalid COPY file header (WITH OIDS)")
            .finish(here("ReceiveCopyBinaryHeader"));
    }
    tmp &= !(1 << 16);
    if (tmp >> 16) != 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg("unrecognized critical flags in COPY file header")
            .finish(here("ReceiveCopyBinaryHeader"));
    }

    /* Header extension length */
    if !CopyGetInt32(cstate, &mut tmp)? || tmp < 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg("invalid COPY file header (missing length)")
            .finish(here("ReceiveCopyBinaryHeader"));
    }
    /* Skip extension header, if present */
    while tmp > 0 {
        tmp -= 1;
        if CopyReadBinaryData(cstate, &mut read_sig, 1)? != 1 {
            return ereport(ERROR)
                .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                .errmsg("invalid COPY file header (wrong length)")
                .finish(here("ReceiveCopyBinaryHeader"));
        }
    }
    Ok(())
}

/* ===========================================================================
 * CopyGetData (copyfromparse.c:244) — the data-source read.
 * =========================================================================== */

/// `CopyGetData(cstate, databuf, minread, maxread)` (copyfromparse.c:244) — read
/// at least `minread`, at most `maxread`, bytes from the source into `dest`.
/// Returns the number of bytes read; fewer than `minread` indicates EOF.
fn CopyGetData(
    cstate: &mut CopyParseState,
    dest: &mut [u8],
    minread: i32,
    maxread: i32,
) -> PgResult<i32> {
    // switch (cstate->copy_src)
    let result = match cstate.copy_src {
        CopySource::COPY_FILE => s::copy_get_data_file::call(cstate, maxread)?,
        CopySource::COPY_FRONTEND => s::copy_get_data_frontend::call(cstate, minread, maxread)?,
        CopySource::COPY_CALLBACK => s::copy_get_data_callback::call(cstate, minread, maxread)?,
    };
    // The source legs own the raw_reached_eof bookkeeping that the C does inline
    // (COPY_FILE: bytesread==0; COPY_FRONTEND: CopyDone); they report it back.
    if result.reached_eof {
        cstate.raw_reached_eof = true;
    }
    let bytesread = result.data.len() as i32;
    debug_assert!(bytesread <= maxread);
    debug_assert!(result.data.len() <= dest.len());
    dest[..result.data.len()].copy_from_slice(&result.data);
    Ok(bytesread)
}

/// `CopyGetInt32(cstate, val)` (copyfromparse.c:361) — read a network-byte-order
/// int32. Returns `true` if OK, `false` if EOF.
fn CopyGetInt32(cstate: &mut CopyParseState, val: &mut i32) -> PgResult<bool> {
    let mut buf = [0u8; 4];
    if CopyReadBinaryData(cstate, &mut buf, 4)? != 4 {
        *val = 0;
        return Ok(false);
    }
    // *val = (int32) pg_ntoh32(buf);
    *val = i32::from_be_bytes(buf);
    Ok(true)
}

/// `CopyGetInt16(cstate, val)` (copyfromparse.c:378) — read a network-byte-order
/// int16.
fn CopyGetInt16(cstate: &mut CopyParseState, val: &mut i16) -> PgResult<bool> {
    let mut buf = [0u8; 2];
    if CopyReadBinaryData(cstate, &mut buf, 2)? != 2 {
        *val = 0;
        return Ok(false);
    }
    // *val = (int16) pg_ntoh16(buf);
    *val = i16::from_be_bytes(buf);
    Ok(true)
}

/* ===========================================================================
 * CopyConvertBuf / CopyConversionError (copyfromparse.c:399-581).
 * =========================================================================== */

/// `CopyConvertBuf(cstate)` (copyfromparse.c:399) — perform encoding conversion
/// on data in `raw_buf`, writing the converted data into `input_buf`.
fn CopyConvertBuf(cstate: &mut CopyParseState) -> PgResult<()> {
    // if (!cstate->need_transcoding)
    if !cstate.need_transcoding {
        let preverifiedlen = cstate.input_buf_len;
        let unverifiedlen = cstate.raw_buf_len - cstate.input_buf_len;

        if unverifiedlen == 0 {
            if cstate.raw_reached_eof {
                cstate.input_reached_eof = true;
            }
            return Ok(());
        }

        /* Verify the new data, including residual unverified bytes. */
        let start = preverifiedlen as usize;
        let end = start + unverifiedlen as usize;
        let nverified =
            s::pg_encoding_verifymbstr::call(cstate.file_encoding, &cstate.raw_buf[start..end]);
        if nverified == 0 {
            if cstate.raw_reached_eof
                || unverifiedlen >= s::pg_encoding_max_length::call(cstate.file_encoding)
            {
                cstate.input_reached_error = true;
            }
            return Ok(());
        }
        cstate.input_buf_len += nverified;
    } else {
        /* Encoding conversion is needed. */
        if RAW_BUF_BYTES(cstate) == 0 {
            if cstate.raw_reached_eof {
                cstate.input_reached_eof = true;
            }
            return Ok(());
        }

        /* First, copy down any unprocessed data. */
        let nbytes = INPUT_BUF_BYTES(cstate);
        if nbytes > 0 && cstate.input_buf_index > 0 {
            let src = cstate.input_buf_index as usize;
            cstate.input_buf.copy_within(src..src + nbytes as usize, 0);
        }
        cstate.input_buf_index = 0;
        cstate.input_buf_len = nbytes;
        cstate.input_buf[nbytes as usize] = b'\0';

        let src_start = cstate.raw_buf_index as usize;
        let srclen = cstate.raw_buf_len - cstate.raw_buf_index;
        let dstlen = INPUT_BUF_SIZE - cstate.input_buf_len + 1;

        let database_encoding = mb::get_database_encoding::call();
        let conv = s::pg_do_encoding_conversion_buf::call(
            cstate.conversion_proc,
            cstate.file_encoding,
            database_encoding,
            &cstate.raw_buf[src_start..src_start + srclen as usize],
            dstlen,
        )?;
        if conv.converted_src_len == 0 {
            if cstate.raw_reached_eof || srclen >= MAX_CONVERSION_INPUT_LENGTH {
                cstate.input_reached_error = true;
            }
            return Ok(());
        }
        let dst_start = cstate.input_buf_len as usize;
        let conv_len = conv.converted.len();
        cstate.input_buf[dst_start..dst_start + conv_len].copy_from_slice(&conv.converted);
        cstate.input_buf[dst_start + conv_len] = b'\0';
        cstate.raw_buf_index += conv.converted_src_len;
        cstate.input_buf_len += conv_len as i32;
    }
    Ok(())
}

/// `CopyConversionError(cstate)` (copyfromparse.c:532) — report an encoding or
/// conversion error. Always raises.
fn CopyConversionError(cstate: &mut CopyParseState) -> PgResult<()> {
    debug_assert!(cstate.raw_buf_len > 0);
    debug_assert!(cstate.input_reached_error);

    if !cstate.need_transcoding {
        let start = cstate.input_buf_len as usize;
        let end = cstate.raw_buf_len as usize;
        s::report_invalid_encoding::call(cstate.file_encoding, &cstate.raw_buf[start..end])?;
    } else {
        let src_start = cstate.raw_buf_index as usize;
        let srclen = cstate.raw_buf_len - cstate.raw_buf_index;
        let dstlen = INPUT_BUF_SIZE - cstate.input_buf_len + 1;
        let database_encoding = mb::get_database_encoding::call();
        s::conversion_error_raise::call(
            cstate.conversion_proc,
            cstate.file_encoding,
            database_encoding,
            &cstate.raw_buf[src_start..src_start + srclen as usize],
            dstlen,
        )?;
    }
    // Not reached: elog(ERROR, "encoding conversion failed without error");
    ereport(ERROR)
        .errmsg_internal("encoding conversion failed without error")
        .finish(here("CopyConversionError"))
}

/// `CopyLoadRawBuf(cstate)` (copyfromparse.c:589) — load more data from the data
/// source into `raw_buf`.
fn CopyLoadRawBuf(cstate: &mut CopyParseState) -> PgResult<()> {
    if cstate.input_is_raw {
        debug_assert!(!cstate.need_transcoding);
        debug_assert_eq!(cstate.raw_buf_index, cstate.input_buf_index);
        debug_assert!(cstate.input_buf_len <= cstate.raw_buf_len);
    }

    /* Copy down the unprocessed data if any. */
    let mut nbytes = RAW_BUF_BYTES(cstate);
    if nbytes > 0 && cstate.raw_buf_index > 0 {
        let src = cstate.raw_buf_index as usize;
        cstate.raw_buf.copy_within(src..src + nbytes as usize, 0);
    }
    cstate.raw_buf_len -= cstate.raw_buf_index;
    cstate.raw_buf_index = 0;

    if cstate.input_is_raw {
        cstate.input_buf_len -= cstate.input_buf_index;
        cstate.input_buf_index = 0;
    }

    /* Load more data */
    let off = cstate.raw_buf_len as usize;
    let want = RAW_BUF_SIZE - cstate.raw_buf_len;
    let mut scratch = vec![0u8; want.max(0) as usize];
    let inbytes = CopyGetData(cstate, &mut scratch, 1, want)?;
    cstate.raw_buf[off..off + inbytes as usize].copy_from_slice(&scratch[..inbytes as usize]);
    nbytes += inbytes;
    cstate.raw_buf[nbytes as usize] = b'\0';
    cstate.raw_buf_len = nbytes;

    cstate.bytes_processed += inbytes as u64;
    // pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, cstate->bytes_processed);
    s::pgstat_progress_update_bytes_processed::call(cstate.bytes_processed as i64)?;

    if inbytes == 0 {
        cstate.raw_reached_eof = true;
    }
    Ok(())
}

/// `CopyLoadInputBuf(cstate)` (copyfromparse.c:649) — load some more data into
/// `input_buf`. On return, at least one more input character is loaded into
/// `input_buf`, or `input_reached_eof` is set.
fn CopyLoadInputBuf(cstate: &mut CopyParseState) -> PgResult<()> {
    let nbytes = INPUT_BUF_BYTES(cstate);

    if cstate.input_is_raw {
        debug_assert!(!cstate.need_transcoding);
        debug_assert!(cstate.input_buf_index >= cstate.raw_buf_index);
        cstate.raw_buf_index = cstate.input_buf_index;
    }

    loop {
        /* If we now have some unconverted data, try to convert it */
        CopyConvertBuf(cstate)?;

        /* If we now have some more input bytes ready, return them */
        if INPUT_BUF_BYTES(cstate) > nbytes {
            return Ok(());
        }

        if cstate.input_reached_error {
            CopyConversionError(cstate)?;
        }

        /* no more input, and everything has been converted */
        if cstate.input_reached_eof {
            break;
        }

        /* Try to load more raw data */
        debug_assert!(!cstate.raw_reached_eof);
        CopyLoadRawBuf(cstate)?;
    }
    Ok(())
}

/// `CopyReadBinaryData(cstate, dest, nbytes)` (copyfromparse.c:700) — read up to
/// `nbytes` bytes from the source via `raw_buf` and write them to `dest`.
/// Returns the number of bytes read (< `nbytes` only at EOF).
fn CopyReadBinaryData(cstate: &mut CopyParseState, dest: &mut [u8], nbytes: i32) -> PgResult<i32> {
    let mut copied_bytes = 0i32;

    if RAW_BUF_BYTES(cstate) >= nbytes {
        /* Enough bytes are present in the buffer. */
        let src = cstate.raw_buf_index as usize;
        dest[..nbytes as usize].copy_from_slice(&cstate.raw_buf[src..src + nbytes as usize]);
        cstate.raw_buf_index += nbytes;
        copied_bytes = nbytes;
    } else {
        /* Not enough bytes; must read from the file. Loop since nbytes could be
         * larger than the buffer size. */
        loop {
            /* Load more data if buffer is empty. */
            if RAW_BUF_BYTES(cstate) == 0 {
                CopyLoadRawBuf(cstate)?;
                if cstate.raw_reached_eof {
                    break; /* EOF */
                }
            }

            /* Transfer some bytes. */
            let copy_bytes = core::cmp::min(nbytes - copied_bytes, RAW_BUF_BYTES(cstate));
            let src = cstate.raw_buf_index as usize;
            let dst = copied_bytes as usize;
            dest[dst..dst + copy_bytes as usize]
                .copy_from_slice(&cstate.raw_buf[src..src + copy_bytes as usize]);
            cstate.raw_buf_index += copy_bytes;
            copied_bytes += copy_bytes;

            if copied_bytes >= nbytes {
                break;
            }
        }
    }

    Ok(copied_bytes)
}

/* ===========================================================================
 * NextCopyFromRawFields / NextCopyFromRawFieldsInternal (copyfromparse.c:746).
 * =========================================================================== */

/// `NextCopyFromRawFields(cstate, fields, nfields)` (copyfromparse.c:746) — read
/// the raw fields of the next line for text/csv mode. Returns the field count on
/// a successful read, or `None` at EOF; field ranges land in `cstate.raw_fields`.
pub fn NextCopyFromRawFields(cstate: &mut CopyParseState) -> PgResult<Option<i32>> {
    // return NextCopyFromRawFieldsInternal(cstate, fields, nfields, cstate->opts.csv_mode);
    NextCopyFromRawFieldsInternal(cstate, cstate.opts.csv_mode)
}

/// `NextCopyFromRawFieldsInternal(cstate, fields, nfields, is_csv)`
/// (copyfromparse.c:770).
fn NextCopyFromRawFieldsInternal(cstate: &mut CopyParseState, is_csv: bool) -> PgResult<Option<i32>> {
    debug_assert!(!cstate.opts.binary);

    /* on input check that the header line is correct if needed */
    if cstate.cur_lineno == 0 && cstate.opts.header_line != CopyHeaderChoice::COPY_HEADER_FALSE {
        cstate.cur_lineno += 1;
        let done = CopyReadLine(cstate, is_csv)?;

        if cstate.opts.header_line == CopyHeaderChoice::COPY_HEADER_MATCH {
            let fldct = if is_csv {
                CopyReadAttributesCSV(cstate)?
            } else {
                CopyReadAttributesText(cstate)?
            };

            let attlen = cstate.attnumlist.len() as i32;
            if fldct != attlen {
                return ereport(ERROR)
                    .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                    .errmsg(format!(
                        "wrong number of fields in header line: got {fldct}, expected {attlen}"
                    ))
                    .finish(here("NextCopyFromRawFields"))
                    .map(|()| None);
            }

            let attnums: Vec<i32> = cstate.attnumlist.iter().map(|&a| a as i32).collect();
            let mut fldnum = 0i32;
            for &attnum in &attnums {
                let attr = s::attr_info::call(&cstate.rel, attnum - 1)?;
                debug_assert!(fldnum < cstate.max_fields);

                let col_field = cstate.raw_fields[fldnum as usize];
                fldnum += 1;
                let col_name = match col_field {
                    None => {
                        return ereport(ERROR)
                            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                            .errmsg(format!(
                                "column name mismatch in header line field {fldnum}: got null value (\"{}\"), expected \"{}\"",
                                cstate.opts.null_print, attr.attname
                            ))
                            .finish(here("NextCopyFromRawFields"))
                            .map(|()| None);
                    }
                    Some(range) => field_str(cstate, range),
                };

                if s::namestrcmp_attr::call(&cstate.rel, attnum - 1, &col_name)? != 0 {
                    return ereport(ERROR)
                        .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                        .errmsg(format!(
                            "column name mismatch in header line field {fldnum}: got \"{col_name}\", expected \"{}\"",
                            attr.attname
                        ))
                        .finish(here("NextCopyFromRawFields"))
                        .map(|()| None);
                }
            }
        }

        if done {
            return Ok(None);
        }
    }

    cstate.cur_lineno += 1;

    /* Actually read the line into memory here */
    let done = CopyReadLine(cstate, is_csv)?;

    if done && cstate.line_buf.is_empty() {
        return Ok(None);
    }

    /* Parse the line into de-escaped field values */
    let fldct = if is_csv {
        CopyReadAttributesCSV(cstate)?
    } else {
        CopyReadAttributesText(cstate)?
    };

    Ok(Some(fldct))
}

/* ===========================================================================
 * NextCopyFrom + the per-row callbacks (copyfromparse.c:870-1148).
 * =========================================================================== */

/// `NextCopyFrom(cstate, econtext, values, nulls)` (copyfromparse.c:870) — read
/// the next tuple from the source. Returns the per-physical-attribute
/// `(Datum, isnull)` pairs on a successful read, or `None` if no more tuples.
pub fn NextCopyFrom<'mcx>(
    cstate: &mut CopyParseState<'mcx>,
) -> PgResult<Option<Vec<AttrValue<'mcx>>>> {
    // tupDesc = RelationGetDescr(cstate->rel); num_phys_attrs = tupDesc->natts;
    let num_phys_attrs = s::relation_natts::call(&cstate.rel)?;
    let num_defaults = cstate.num_defaults;

    /* Initialize all values for row to NULL */
    let mut values: Vec<AttrValue<'mcx>> = vec![
        AttrValue {
            datum: Datum::ByVal(0),
            isnull: true,
        };
        num_phys_attrs as usize
    ];
    // MemSet(cstate->defaults, false, num_phys_attrs * sizeof(bool));
    for d in cstate.defaults.iter_mut() {
        *d = false;
    }

    /* Get one row from source */
    if !copy_from_one_row(cstate, &mut values)? {
        return Ok(None);
    }

    /* Now compute and insert any defaults available for the columns not
     * provided by the input data. */
    for i in 0..num_defaults as usize {
        let m = cstate.defmap[i] as usize;
        debug_assert!(cstate.defexprs[m].is_some(), "defmap entry has a defexpr");
        // values[m] = ExecEvalExpr(defexprs[m], econtext, &nulls[m]);
        values[m] = s::exec_eval_expr::call(cstate, m as i32)?;
    }

    Ok(Some(values))
}

/// Dispatch `cstate->routine->CopyFromOneRow` to the in-crate format workhorse.
fn copy_from_one_row<'mcx>(
    cstate: &mut CopyParseState<'mcx>,
    values: &mut [AttrValue<'mcx>],
) -> PgResult<bool> {
    if cstate.opts.binary {
        CopyFromBinaryOneRow(cstate, values)
    } else {
        CopyFromTextLikeOneRow(cstate, values, cstate.opts.csv_mode)
    }
}

/// `CopyFromTextOneRow(cstate, econtext, values, nulls)` (copyfromparse.c:915).
pub fn CopyFromTextOneRow<'mcx>(
    cstate: &mut CopyParseState<'mcx>,
    values: &mut [AttrValue<'mcx>],
) -> PgResult<bool> {
    CopyFromTextLikeOneRow(cstate, values, false)
}

/// `CopyFromCSVOneRow(cstate, econtext, values, nulls)` (copyfromparse.c:923).
pub fn CopyFromCSVOneRow<'mcx>(
    cstate: &mut CopyParseState<'mcx>,
    values: &mut [AttrValue<'mcx>],
) -> PgResult<bool> {
    CopyFromTextLikeOneRow(cstate, values, true)
}

/// `CopyFromTextLikeOneRow(cstate, econtext, values, nulls, is_csv)`
/// (copyfromparse.c:936) — the workhorse for text and CSV per-row reads. The C
/// `econtext` argument is read from `cstate.econtext` by the `exec_eval_expr`
/// seam.
fn CopyFromTextLikeOneRow<'mcx>(
    cstate: &mut CopyParseState<'mcx>,
    values: &mut [AttrValue<'mcx>],
    is_csv: bool,
) -> PgResult<bool> {
    let attr_count = cstate.attnumlist.len() as i32;

    /* read raw fields in the next line */
    let fldct = match NextCopyFromRawFieldsInternal(cstate, is_csv)? {
        None => return Ok(false),
        Some(fldct) => fldct,
    };

    /* check for overflowing fields */
    if attr_count > 0 && fldct > attr_count {
        return ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg("extra data after last expected column")
            .finish(here("CopyFromTextLikeOneRow"))
            .map(|()| false);
    }

    let mut fieldno = 0i32;

    /* Loop to read the user attributes on the line. */
    let attnums: Vec<i32> = cstate.attnumlist.iter().map(|&a| a as i32).collect();
    for &attnum in &attnums {
        let m = (attnum - 1) as usize;
        let att = s::attr_info::call(&cstate.rel, attnum - 1)?;

        if fieldno >= fldct {
            return ereport(ERROR)
                .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                .errmsg(format!("missing data for column \"{}\"", att.attname))
                .finish(here("CopyFromTextLikeOneRow"))
                .map(|()| false);
        }
        // string = field_strings[fieldno++];
        let field_range = cstate.raw_fields[fieldno as usize];
        fieldno += 1;
        // Idiomatic: a None range is a NULL field.
        let mut string: Option<String> = field_range.map(|r| field_str(cstate, r));

        // if (cstate->convert_select_flags && !cstate->convert_select_flags[m])
        if let Some(flags) = &cstate.convert_select_flags {
            if !flags[m] {
                /* ignore input field, leaving column as NULL */
                continue;
            }
        }

        if is_csv {
            if string.is_none() && cstate.force_notnull_flags[m] {
                // string = cstate->opts.null_print;
                string = Some(cstate.opts.null_print.clone());
            } else if let Some(sv) = &string {
                if cstate.force_null_flags[m] && *sv == cstate.opts.null_print {
                    string = None;
                }
            }
        }

        cstate.cur_attname = Some(att.attname.clone());
        cstate.cur_attval = string.clone();

        if string.is_some() {
            values[m].isnull = false;
        }

        if cstate.defaults[m] {
            // values[m] = ExecEvalExpr(defexprs[m], econtext, &nulls[m]);
            debug_assert!(cstate.defexprs[m].is_some(), "defaults[m] => defexpr set");
            values[m] = s::exec_eval_expr::call(cstate, m as i32)?;
        } else {
            // values[m] = InputFunctionCallSafe(&in_functions[m], string,
            //                 typioparams[m], att->atttypmod, escontext, ...);
            let call_result =
                s::input_function_call_safe::call(cstate, m as i32, string.as_deref(), att.atttypmod)?;
            match call_result {
                Some(datum) => {
                    values[m].datum = datum;
                }
                None => {
                    debug_assert_ne!(cstate.opts.on_error, CopyOnErrorChoice::COPY_ON_ERROR_STOP);
                    cstate.num_errors += 1;

                    if is_verbose(cstate.opts.log_verbosity) {
                        debug_assert!(!cstate.relname_only);
                        cstate.relname_only = true;

                        let relname = cstate.rel.rd_rel.relname.as_str().to_string();
                        let attname = cstate.cur_attname.clone().unwrap_or_default();
                        if let Some(attval) = &cstate.cur_attval {
                            let attval = CopyLimitPrintoutLength(attval);
                            s::notice_skipping_row::call(&relname, cstate.cur_lineno, &attname, Some(&attval))?;
                        } else {
                            s::notice_skipping_row::call(&relname, cstate.cur_lineno, &attname, None)?;
                        }

                        cstate.relname_only = false;
                    }

                    return Ok(true);
                }
            }
        }

        cstate.cur_attname = None;
        cstate.cur_attval = None;
    }

    debug_assert_eq!(fieldno, attr_count);

    Ok(true)
}

/// `CopyFromBinaryOneRow(cstate, econtext, values, nulls)` (copyfromparse.c:1085).
pub fn CopyFromBinaryOneRow<'mcx>(
    cstate: &mut CopyParseState<'mcx>,
    values: &mut [AttrValue<'mcx>],
) -> PgResult<bool> {
    let attr_count = cstate.attnumlist.len() as i32;

    cstate.cur_lineno += 1;

    // if (!CopyGetInt16(cstate, &fld_count)) return false;  /* EOF */
    let mut fld_count: i16 = 0;
    if !CopyGetInt16(cstate, &mut fld_count)? {
        return Ok(false);
    }

    if fld_count == -1 {
        /* Received EOF marker. Wait for the protocol-level EOF. */
        let mut dummy = [0u8; 1];
        if CopyReadBinaryData(cstate, &mut dummy, 1)? > 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                .errmsg("received copy data after EOF marker")
                .finish(here("CopyFromBinaryOneRow"))
                .map(|()| false);
        }
        return Ok(false);
    }

    if fld_count as i32 != attr_count {
        return ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg(format!(
                "row field count is {}, expected {attr_count}",
                fld_count as i32
            ))
            .finish(here("CopyFromBinaryOneRow"))
            .map(|()| false);
    }

    let attnums: Vec<i32> = cstate.attnumlist.iter().map(|&a| a as i32).collect();
    for &attnum in &attnums {
        let m = (attnum - 1) as usize;
        let att = s::attr_info::call(&cstate.rel, attnum - 1)?;

        cstate.cur_attname = Some(att.attname.clone());
        let (datum, isnull) = CopyReadBinaryAttribute(cstate, m as i32, att.atttypmod)?;
        values[m] = AttrValue { datum, isnull };
        cstate.cur_attname = None;
    }

    Ok(true)
}

/* ===========================================================================
 * CopyReadLine / CopyReadLineText (copyfromparse.c:1157-1530).
 * =========================================================================== */

/// `CopyReadLine(cstate, is_csv)` (copyfromparse.c:1157) — read the next input
/// line and stash it in `line_buf`. Returns `true` if terminated by EOF, `false`
/// if by newline. The terminating newline / EOF marker is not included.
fn CopyReadLine(cstate: &mut CopyParseState, is_csv: bool) -> PgResult<bool> {
    cstate.line_buf.clear();
    cstate.line_buf_valid = false;

    /* Parse data and transfer into line_buf */
    let result = CopyReadLineText(cstate, is_csv)?;

    if result {
        /* Reached EOF. In v3 protocol, ignore anything after \. up to EOF. */
        if cstate.copy_src == CopySource::COPY_FRONTEND {
            let mut scratch = vec![0u8; INPUT_BUF_SIZE as usize];
            loop {
                let inbytes = CopyGetData(cstate, &mut scratch, 1, INPUT_BUF_SIZE)?;
                if inbytes <= 0 {
                    break;
                }
            }
            cstate.input_buf_index = 0;
            cstate.input_buf_len = 0;
            cstate.raw_buf_index = 0;
            cstate.raw_buf_len = 0;
        }
    } else {
        /* We transferred the EOL marker to line_buf; get rid of it. */
        match cstate.eol_type {
            EolType::EOL_NL => {
                debug_assert!(!cstate.line_buf.is_empty());
                debug_assert_eq!(*cstate.line_buf.last().unwrap(), b'\n');
                cstate.line_buf.pop();
            }
            EolType::EOL_CR => {
                debug_assert!(!cstate.line_buf.is_empty());
                debug_assert_eq!(*cstate.line_buf.last().unwrap(), b'\r');
                cstate.line_buf.pop();
            }
            EolType::EOL_CRNL => {
                debug_assert!(cstate.line_buf.len() >= 2);
                debug_assert_eq!(cstate.line_buf[cstate.line_buf.len() - 2], b'\r');
                debug_assert_eq!(cstate.line_buf[cstate.line_buf.len() - 1], b'\n');
                cstate.line_buf.pop();
                cstate.line_buf.pop();
            }
            EolType::EOL_UNKNOWN => {
                /* shouldn't get here */
                debug_assert!(false);
            }
        }
    }

    /* Now it's safe to use the buffer in error messages */
    cstate.line_buf_valid = true;

    Ok(result)
}

/// `CopyReadLineText(cstate, is_csv)` (copyfromparse.c:1233) — inner loop of
/// `CopyReadLine` for text mode.
///
/// The C uses the `IF_NEED_REFILL_*` / `REFILL_LINEBUF` macros that do
/// continue/break control over the local cursor. This port reproduces them as
/// inline blocks; `REFILL_LINEBUF` appends `input_buf[input_buf_index ..
/// input_buf_ptr]` to `line_buf`.
fn CopyReadLineText(cstate: &mut CopyParseState, is_csv: bool) -> PgResult<bool> {
    let mut need_data = false;
    let mut hit_eof = false;
    let mut result = false;

    /* CSV variables */
    let mut in_quote = false;
    let mut last_was_esc = false;
    let mut quotec = b'\0';
    let mut escapec = b'\0';

    if is_csv {
        quotec = cstate.opts.quote;
        escapec = cstate.opts.escape;
        if quotec == escapec {
            escapec = b'\0';
        }
    }

    let mut input_buf_ptr = cstate.input_buf_index;
    let mut copy_buf_len = cstate.input_buf_len;

    'outer: loop {
        /* Load more data if needed. */
        if input_buf_ptr >= copy_buf_len || need_data {
            // REFILL_LINEBUF;
            refill_linebuf(cstate, &mut input_buf_ptr);

            CopyLoadInputBuf(cstate)?;
            hit_eof = cstate.input_reached_eof;
            input_buf_ptr = cstate.input_buf_index;
            copy_buf_len = cstate.input_buf_len;

            if INPUT_BUF_BYTES(cstate) <= 0 {
                result = true;
                break;
            }
            need_data = false;
        }

        /* OK to fetch a character */
        let prev_raw_ptr = input_buf_ptr;
        let mut c = input_byte(cstate, input_buf_ptr as usize);
        input_buf_ptr += 1;

        if is_csv {
            // if (c == '\r') { IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0); }
            if c == b'\r'
                && need_refill_and_not_eof(input_buf_ptr, 0, copy_buf_len, hit_eof, prev_raw_ptr)
            {
                input_buf_ptr = prev_raw_ptr; /* undo fetch */
                need_data = true;
                continue;
            }

            if in_quote && c == escapec {
                last_was_esc = !last_was_esc;
            }
            if c == quotec && !last_was_esc {
                in_quote = !in_quote;
            }
            if c != escapec {
                last_was_esc = false;
            }

            // if (in_quote && c == (eol_type == EOL_NL ? '\n' : '\r')) cstate->cur_lineno++;
            let eol_char = if cstate.eol_type == EolType::EOL_NL {
                b'\n'
            } else {
                b'\r'
            };
            if in_quote && c == eol_char {
                cstate.cur_lineno += 1;
            }
        }

        /* Process \r */
        if c == b'\r' && (!is_csv || !in_quote) {
            if cstate.eol_type == EolType::EOL_UNKNOWN || cstate.eol_type == EolType::EOL_CRNL {
                // IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
                if need_refill_and_not_eof(input_buf_ptr, 0, copy_buf_len, hit_eof, prev_raw_ptr) {
                    input_buf_ptr = prev_raw_ptr;
                    need_data = true;
                    continue;
                }

                /* get next char */
                c = input_byte(cstate, input_buf_ptr as usize);

                if c == b'\n' {
                    input_buf_ptr += 1;
                    cstate.eol_type = EolType::EOL_CRNL;
                } else {
                    /* found \r, but no \n */
                    if cstate.eol_type == EolType::EOL_CRNL {
                        return cr_error(is_csv, "CopyReadLineText").map(|()| false);
                    }
                    /* eol_type = EOL_CR;  (don't consume the peeked character) */
                    cstate.eol_type = EolType::EOL_CR;
                }
            } else if cstate.eol_type == EolType::EOL_NL {
                return cr_error(is_csv, "CopyReadLineText").map(|()| false);
            }
            /* If reach here, we have found the line terminator */
            break;
        }

        /* Process \n */
        if c == b'\n' && (!is_csv || !in_quote) {
            if cstate.eol_type == EolType::EOL_CR || cstate.eol_type == EolType::EOL_CRNL {
                return nl_error(is_csv, "CopyReadLineText").map(|()| false);
            }
            cstate.eol_type = EolType::EOL_NL;
            /* found the line terminator */
            break;
        }

        /* Process backslash, except in CSV mode. */
        if c == b'\\' && !is_csv {
            // IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
            if need_refill_and_not_eof(input_buf_ptr, 0, copy_buf_len, hit_eof, prev_raw_ptr) {
                input_buf_ptr = prev_raw_ptr;
                need_data = true;
                continue;
            }
            // IF_NEED_REFILL_AND_EOF_BREAK(0);
            if need_refill_and_eof(input_buf_ptr, 0, copy_buf_len, hit_eof) {
                result = true;
                break;
            }

            let mut c2 = input_byte(cstate, input_buf_ptr as usize);

            if c2 == b'.' {
                input_buf_ptr += 1; /* consume the '.' */
                if cstate.eol_type == EolType::EOL_CRNL {
                    // IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
                    if need_refill_and_not_eof(input_buf_ptr, 0, copy_buf_len, hit_eof, prev_raw_ptr) {
                        input_buf_ptr = prev_raw_ptr;
                        need_data = true;
                        continue;
                    }
                    c2 = input_byte(cstate, input_buf_ptr as usize);
                    input_buf_ptr += 1;

                    if c2 == b'\n' {
                        return ereport(ERROR)
                            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                            .errmsg("end-of-copy marker does not match previous newline style")
                            .finish(here("CopyReadLineText"))
                            .map(|()| false);
                    } else if c2 != b'\r' {
                        return ereport(ERROR)
                            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                            .errmsg("end-of-copy marker is not alone on its line")
                            .finish(here("CopyReadLineText"))
                            .map(|()| false);
                    }
                }

                /* Get the next character */
                // IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
                if need_refill_and_not_eof(input_buf_ptr, 0, copy_buf_len, hit_eof, prev_raw_ptr) {
                    input_buf_ptr = prev_raw_ptr;
                    need_data = true;
                    continue;
                }
                c2 = input_byte(cstate, input_buf_ptr as usize);
                input_buf_ptr += 1;

                if c2 != b'\r' && c2 != b'\n' {
                    return ereport(ERROR)
                        .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                        .errmsg("end-of-copy marker is not alone on its line")
                        .finish(here("CopyReadLineText"))
                        .map(|()| false);
                }

                if (cstate.eol_type == EolType::EOL_NL && c2 != b'\n')
                    || (cstate.eol_type == EolType::EOL_CRNL && c2 != b'\n')
                    || (cstate.eol_type == EolType::EOL_CR && c2 != b'\r')
                {
                    return ereport(ERROR)
                        .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                        .errmsg("end-of-copy marker does not match previous newline style")
                        .finish(here("CopyReadLineText"))
                        .map(|()| false);
                }

                /* If there is any data on this line before the \., complain. */
                if !cstate.line_buf.is_empty() || prev_raw_ptr > cstate.input_buf_index {
                    return ereport(ERROR)
                        .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                        .errmsg("end-of-copy marker is not alone on its line")
                        .finish(here("CopyReadLineText"))
                        .map(|()| false);
                }

                /* Discard the \. and newline, then report EOF. */
                cstate.input_buf_index = input_buf_ptr;
                result = true;
                break 'outer;
            } else {
                /* backslash followed by something other than period; skip the
                 * second character too. */
                input_buf_ptr += 1;
            }
        }
    } /* end of outer loop */

    /* Transfer any still-uncopied data to line_buf. */
    // REFILL_LINEBUF;
    refill_linebuf(cstate, &mut input_buf_ptr);

    Ok(result)
}

/// `REFILL_LINEBUF` (copyfromparse.c:126) — transfer approved data to `line_buf`.
#[inline]
fn refill_linebuf(cstate: &mut CopyParseState, input_buf_ptr: &mut i32) {
    if *input_buf_ptr > cstate.input_buf_index {
        let start = cstate.input_buf_index as usize;
        let end = *input_buf_ptr as usize;
        if cstate.input_is_raw {
            cstate.line_buf.extend_from_slice(&cstate.raw_buf[start..end]);
        } else {
            cstate.line_buf.extend_from_slice(&cstate.input_buf[start..end]);
        }
        cstate.input_buf_index = *input_buf_ptr;
    }
}

/// `IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(extralen)` (copyfromparse.c:97) — test
/// arm (the macro's `continue` is handled at the call site).
#[inline]
fn need_refill_and_not_eof(
    input_buf_ptr: i32,
    extralen: i32,
    copy_buf_len: i32,
    hit_eof: bool,
    _prev_raw_ptr: i32,
) -> bool {
    // if (input_buf_ptr + (extralen) >= copy_buf_len && !hit_eof)
    input_buf_ptr + extralen >= copy_buf_len && !hit_eof
}

/// `IF_NEED_REFILL_AND_EOF_BREAK(extralen)` (copyfromparse.c:109) — test arm
/// (the macro's `break` is handled at the call site). With `extralen == 0` there
/// is no partial-character consume.
#[inline]
fn need_refill_and_eof(input_buf_ptr: i32, extralen: i32, copy_buf_len: i32, hit_eof: bool) -> bool {
    // if (input_buf_ptr + (extralen) >= copy_buf_len && hit_eof)
    input_buf_ptr + extralen >= copy_buf_len && hit_eof
}

/// The `\r`-found error (copyfromparse.c:1391 / 1408) with the text/CSV-specific
/// message + hint.
fn cr_error(is_csv: bool, func: &'static str) -> PgResult<()> {
    let (msg, hint) = if !is_csv {
        (
            "literal carriage return found in data",
            "Use \"\\r\" to represent carriage return.",
        )
    } else {
        (
            "unquoted carriage return found in data",
            "Use quoted CSV field to represent carriage return.",
        )
    };
    ereport(ERROR)
        .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
        .errmsg(msg)
        .errhint(hint)
        .finish(here(func))
}

/// The `\n`-found error (copyfromparse.c:1424) with the text/CSV-specific
/// message + hint.
fn nl_error(is_csv: bool, func: &'static str) -> PgResult<()> {
    let (msg, hint) = if !is_csv {
        (
            "literal newline found in data",
            "Use \"\\n\" to represent newline.",
        )
    } else {
        (
            "unquoted newline found in data",
            "Use quoted CSV field to represent newline.",
        )
    };
    ereport(ERROR)
        .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
        .errmsg(msg)
        .errhint(hint)
        .finish(here(func))
}

/* ===========================================================================
 * GetDecimalFromHex / CopyReadAttributesText / CopyReadAttributesCSV
 * (copyfromparse.c:1535-2006).
 * =========================================================================== */

/// `GetDecimalFromHex(hex)` (copyfromparse.c:1535) — decimal value of a hex
/// digit.
fn GetDecimalFromHex(hex: u8) -> i32 {
    if hex.is_ascii_digit() {
        (hex - b'0') as i32
    } else {
        (hex.to_ascii_lowercase() - b'a') as i32 + 10
    }
}

/// `CopyReadAttributesText(cstate)` (copyfromparse.c:1563) — parse the current
/// line into separate attributes (fields), de-escaping as needed. The de-escaped
/// field bytes are written into `attribute_buf`; `raw_fields[k]` is set to the
/// k'th field's byte range (or `None` for the NULL marker). Returns the field
/// count.
fn CopyReadAttributesText(cstate: &mut CopyParseState) -> PgResult<i32> {
    let delimc = cstate.opts.delim;

    /* zero-column tables: check the line is empty, return. */
    if cstate.max_fields <= 0 {
        if !cstate.line_buf.is_empty() {
            return ereport(ERROR)
                .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                .errmsg("extra data after last expected column")
                .finish(here("CopyReadAttributesText"))
                .map(|()| 0);
        }
        return Ok(0);
    }

    cstate.attribute_buf.clear();

    /* Force attribute_buf large enough; pointers stored into raw_fields stay
     * valid because we never reallocate mid-stream.  Idiomatically we record
     * (start,end) ranges, so this is just a capacity reserve. */
    let line_len = cstate.line_buf.len();
    cstate.attribute_buf.reserve(line_len + 1);

    let mut cur_ptr = 0usize;
    let line_end_ptr = line_len;

    /* Outer loop iterates over fields */
    let mut fieldno = 0i32;
    loop {
        let mut found_delim = false;
        let mut saw_non_ascii = false;

        /* Make sure there is enough space for the next value */
        if fieldno >= cstate.max_fields {
            cstate.max_fields *= 2;
            cstate.raw_fields.resize(cstate.max_fields as usize, None);
        }

        /* Remember start of field on both input and output sides */
        let start_ptr = cur_ptr;
        let field_out_start = cstate.attribute_buf.len();
        cstate.raw_fields[fieldno as usize] = Some(FieldRange {
            start: field_out_start,
            end: field_out_start,
        });

        let mut end_ptr;

        /* Scan data for field. */
        loop {
            end_ptr = cur_ptr;
            if cur_ptr >= line_end_ptr {
                break;
            }
            let mut c = cstate.line_buf[cur_ptr];
            cur_ptr += 1;
            if c == delimc {
                found_delim = true;
                break;
            }
            if c == b'\\' {
                if cur_ptr >= line_end_ptr {
                    break;
                }
                c = cstate.line_buf[cur_ptr];
                cur_ptr += 1;
                match c {
                    b'0'..=b'7' => {
                        /* handle \013 */
                        let mut val = OCTVALUE(c);
                        if cur_ptr < line_end_ptr {
                            c = cstate.line_buf[cur_ptr];
                            if ISOCTAL(c) {
                                cur_ptr += 1;
                                val = (val << 3) + OCTVALUE(c);
                                if cur_ptr < line_end_ptr {
                                    c = cstate.line_buf[cur_ptr];
                                    if ISOCTAL(c) {
                                        cur_ptr += 1;
                                        val = (val << 3) + OCTVALUE(c);
                                    }
                                }
                            }
                        }
                        c = (val & 0o377) as u8;
                        if c == b'\0' || IS_HIGHBIT_SET(c) {
                            saw_non_ascii = true;
                        }
                    }
                    b'x' => {
                        /* Handle \x3F */
                        if cur_ptr < line_end_ptr {
                            let hexchar = cstate.line_buf[cur_ptr];
                            if hexchar.is_ascii_hexdigit() {
                                let mut val = GetDecimalFromHex(hexchar);
                                cur_ptr += 1;
                                if cur_ptr < line_end_ptr {
                                    let hexchar2 = cstate.line_buf[cur_ptr];
                                    if hexchar2.is_ascii_hexdigit() {
                                        cur_ptr += 1;
                                        val = (val << 4) + GetDecimalFromHex(hexchar2);
                                    }
                                }
                                c = (val & 0xff) as u8;
                                if c == b'\0' || IS_HIGHBIT_SET(c) {
                                    saw_non_ascii = true;
                                }
                            }
                        }
                    }
                    b'b' => c = 0x08, /* '\b' */
                    b'f' => c = 0x0c, /* '\f' */
                    b'n' => c = b'\n',
                    b'r' => c = b'\r',
                    b't' => c = b'\t',
                    b'v' => c = 0x0b, /* '\v' */
                    // default: take the char after '\' literally (c unchanged)
                    _ => {}
                }
            }

            cstate.attribute_buf.push(c);
        }

        let content_end = cstate.attribute_buf.len();

        /* Check whether raw input matched null marker */
        let input_len = end_ptr - start_ptr;
        if input_len == cstate.opts.null_print_len as usize
            && cstate.line_buf[start_ptr..start_ptr + input_len] == *cstate.opts.null_print.as_bytes()
        {
            cstate.raw_fields[fieldno as usize] = None;
        } else if matched_default_marker(cstate, fieldno, start_ptr, input_len)? {
            handle_default_marker(cstate, fieldno, "CopyReadAttributesText")?;
            finalize_field_range(cstate, fieldno, content_end);
        } else {
            /* the field is supposed to contain data. */
            if saw_non_ascii {
                let r =
                    cstate.raw_fields[fieldno as usize].expect("data field has a recorded range");
                let bytes = cstate.attribute_buf[r.start..content_end].to_vec();
                s::pg_verifymbstr::call(&bytes)?;
            }
            finalize_field_range(cstate, fieldno, content_end);
        }

        /* Terminate attribute value in output area */
        cstate.attribute_buf.push(b'\0');

        fieldno += 1;
        /* Done if we hit EOL instead of a delim */
        if !found_delim {
            break;
        }
    }

    /* Clean up state of attribute_buf */
    debug_assert_eq!(*cstate.attribute_buf.last().unwrap(), b'\0');
    cstate.attribute_buf.pop(); // drop the trailing extra terminator (len = output_ptr - data)

    Ok(fieldno)
}

/// `CopyReadAttributesCSV(cstate)` (copyfromparse.c:1817) — like
/// `CopyReadAttributesText`, but parses according to standard CSV usage.
// The C sets `end_ptr = cur_ptr` at the top of *both* the not-quote and the
// in-quote inner loops; the in-quote assignment is only ever superseded by the
// not-quote one before `endfield` reads it, so it appears dead to rustc. The
// assignment is kept for 1:1 fidelity with the C control flow.
#[allow(unused_assignments)]
fn CopyReadAttributesCSV(cstate: &mut CopyParseState) -> PgResult<i32> {
    let delimc = cstate.opts.delim;
    let quotec = cstate.opts.quote;
    let escapec = cstate.opts.escape;

    /* zero-column tables. */
    if cstate.max_fields <= 0 {
        if !cstate.line_buf.is_empty() {
            return ereport(ERROR)
                .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                .errmsg("extra data after last expected column")
                .finish(here("CopyReadAttributesCSV"))
                .map(|()| 0);
        }
        return Ok(0);
    }

    cstate.attribute_buf.clear();
    let line_len = cstate.line_buf.len();
    cstate.attribute_buf.reserve(line_len + 1);

    let mut cur_ptr = 0usize;
    let line_end_ptr = line_len;

    let mut fieldno = 0i32;
    loop {
        let mut found_delim = false;
        let mut saw_quote = false;

        /* Make sure there is enough space for the next value */
        if fieldno >= cstate.max_fields {
            cstate.max_fields *= 2;
            cstate.raw_fields.resize(cstate.max_fields as usize, None);
        }

        /* Remember start of field on both input and output sides */
        let start_ptr = cur_ptr;
        let field_out_start = cstate.attribute_buf.len();
        cstate.raw_fields[fieldno as usize] = Some(FieldRange {
            start: field_out_start,
            end: field_out_start,
        });

        let mut end_ptr;

        /* Scan data for field. The loop starts in "not quote" mode. */
        'field: loop {
            // Not in quote
            loop {
                end_ptr = cur_ptr;
                if cur_ptr >= line_end_ptr {
                    break 'field;
                }
                let c = cstate.line_buf[cur_ptr];
                cur_ptr += 1;
                if c == delimc {
                    found_delim = true;
                    break 'field;
                }
                if c == quotec {
                    saw_quote = true;
                    break;
                }
                cstate.attribute_buf.push(c);
            }

            // In quote
            loop {
                end_ptr = cur_ptr;
                if cur_ptr >= line_end_ptr {
                    return ereport(ERROR)
                        .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
                        .errmsg("unterminated CSV quoted field")
                        .finish(here("CopyReadAttributesCSV"))
                        .map(|()| 0);
                }
                let c = cstate.line_buf[cur_ptr];
                cur_ptr += 1;

                if c == escapec {
                    if cur_ptr < line_end_ptr {
                        let nextc = cstate.line_buf[cur_ptr];
                        if nextc == escapec || nextc == quotec {
                            cstate.attribute_buf.push(nextc);
                            cur_ptr += 1;
                            continue;
                        }
                    }
                }

                if c == quotec {
                    break;
                }

                cstate.attribute_buf.push(c);
            }
        }
        /* endfield: */

        let content_end = cstate.attribute_buf.len();

        /* Terminate attribute value in output area */
        cstate.attribute_buf.push(b'\0');

        /* Check whether raw input matched null marker */
        let input_len = end_ptr - start_ptr;
        if !saw_quote
            && input_len == cstate.opts.null_print_len as usize
            && cstate.line_buf[start_ptr..start_ptr + input_len] == *cstate.opts.null_print.as_bytes()
        {
            cstate.raw_fields[fieldno as usize] = None;
        } else if matched_default_marker(cstate, fieldno, start_ptr, input_len)? {
            handle_default_marker(cstate, fieldno, "CopyReadAttributesCSV")?;
            finalize_field_range(cstate, fieldno, content_end);
        } else {
            finalize_field_range(cstate, fieldno, content_end);
        }

        fieldno += 1;
        /* Done if we hit EOL instead of a delim */
        if !found_delim {
            break;
        }
    }

    /* Clean up state of attribute_buf */
    debug_assert_eq!(*cstate.attribute_buf.last().unwrap(), b'\0');
    cstate.attribute_buf.pop();

    Ok(fieldno)
}

/// Set the recorded field range's `end` to `content_end` (the de-escaped field
/// is `attribute_buf[start..content_end]`, excluding the trailing NUL). No-op if
/// the field was already nulled.
#[inline]
fn finalize_field_range(cstate: &mut CopyParseState, fieldno: i32, content_end: usize) {
    if let Some(r) = &mut cstate.raw_fields[fieldno as usize] {
        r.end = content_end;
    }
}

/// The default-marker test shared by text/CSV (copyfromparse.c:1753-1756 /
/// 1968-1971): `fieldno < list_length(attnumlist) && default_print &&
/// input_len == default_print_len && strncmp(start_ptr, default_print) == 0`.
fn matched_default_marker(
    cstate: &CopyParseState,
    fieldno: i32,
    start_ptr: usize,
    input_len: usize,
) -> PgResult<bool> {
    // `default_print` is a pure NULL-pointer check and `list_length` is a
    // side-effect-free accessor, so testing `default_print` first preserves the
    // boolean result while letting the byte codec run without the list seam when
    // no DEFAULT marker is configured.
    let dp = match &cstate.opts.default_print {
        None => return Ok(false),
        Some(dp) => dp,
    };
    let attlen = cstate.attnumlist.len() as i32;
    Ok(fieldno < attlen
        && input_len == cstate.opts.default_print_len as usize
        && cstate.line_buf[start_ptr..start_ptr + input_len] == *dp.as_bytes())
}

/// The default-marker handler shared by text/CSV (copyfromparse.c:1757-1776 /
/// 1972-1991): set `cstate->defaults[m]` if there is a default expr, else raise.
fn handle_default_marker(
    cstate: &mut CopyParseState,
    fieldno: i32,
    func: &'static str,
) -> PgResult<()> {
    // int m = list_nth_int(cstate->attnumlist, fieldno) - 1;
    let m = cstate.attnumlist[fieldno as usize] as i32 - 1;
    if cstate.defexprs[m as usize].is_some() {
        cstate.defaults[m as usize] = true;
        Ok(())
    } else {
        let att = s::attr_info::call(&cstate.rel, m)?;
        ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg("unexpected default marker in COPY data")
            .errdetail(format!("Column \"{}\" has no default value.", att.attname))
            .finish(here(func))
    }
}

/// `CopyReadBinaryAttribute(cstate, flinfo, typioparam, typmod, isnull)`
/// (copyfromparse.c:2012) — read a binary attribute. Returns `(datum, isnull)`.
fn CopyReadBinaryAttribute<'mcx>(
    cstate: &mut CopyParseState<'mcx>,
    m: i32,
    typmod: i32,
) -> PgResult<(Datum<'mcx>, bool)> {
    let mut fld_size = 0i32;
    if !CopyGetInt32(cstate, &mut fld_size)? {
        return ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg("unexpected EOF in COPY data")
            .finish(here("CopyReadBinaryAttribute"))
            .map(|()| (Datum::ByVal(0), true));
    }
    // if (fld_size == -1) { *isnull = true; return ReceiveFunctionCall(flinfo, NULL, ...); }
    if fld_size == -1 {
        let datum = s::receive_function_call::call(cstate, m, None, typmod)?;
        return Ok((datum, true));
    }
    if fld_size < 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg("invalid field size")
            .finish(here("CopyReadBinaryAttribute"))
            .map(|()| (Datum::ByVal(0), true));
    }

    /* reset attribute_buf to empty, and load raw data in it */
    cstate.attribute_buf.clear();
    cstate.attribute_buf.resize(fld_size as usize, 0);
    let mut buf = vec![0u8; fld_size as usize];
    if CopyReadBinaryData(cstate, &mut buf, fld_size)? != fld_size {
        return ereport(ERROR)
            .errcode(ERRCODE_BAD_COPY_FILE_FORMAT)
            .errmsg("unexpected EOF in COPY data")
            .finish(here("CopyReadBinaryAttribute"))
            .map(|()| (Datum::ByVal(0), true));
    }
    cstate.attribute_buf.copy_from_slice(&buf);
    // attribute_buf.len = fld_size; attribute_buf.data[fld_size] = '\0';
    // (idiomatic: the buf slice IS the data; the NUL terminator is implicit)
    cstate.attribute_cursor = 0;

    /* Call the column type's binary input converter */
    let datum = s::receive_function_call::call(cstate, m, Some(&buf), typmod)?;

    /* Trouble if it didn't eat the whole buffer */
    if cstate.attribute_cursor != fld_size {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_BINARY_REPRESENTATION)
            .errmsg("incorrect binary data format")
            .finish(here("CopyReadBinaryAttribute"))
            .map(|()| (Datum::ByVal(0), true));
    }

    Ok((datum, false))
}

/* ===========================================================================
 * Helpers tying the owned buffers to the C semantics.
 * =========================================================================== */

/// The de-escaped field bytes for `range`, decoded as a `String` (the C hands
/// back a `char *` into `attribute_buf`; the value layer treats it as a
/// NUL-terminated server-encoded string).
fn field_str(cstate: &CopyParseState, range: FieldRange) -> String {
    String::from_utf8_lossy(&cstate.attribute_buf[range.start..range.end]).into_owned()
}

/// `CopyLimitPrintoutLength(str)` (copyfrom.c:333, used by the ON_ERROR verbose
/// NOTICE) — make sure we don't print an unreasonable amount of COPY data. The
/// encoding-aware variant lives in `copyfrom.c`; here we mirror the byte-length
/// fast path plus a conservative truncation (the NOTICE seam owns the final
/// rendering).
fn CopyLimitPrintoutLength(s_in: &str) -> String {
    /// `#define MAX_COPY_DATA_DISPLAY 100`.
    const MAX_COPY_DATA_DISPLAY: usize = 100;
    if s_in.len() <= MAX_COPY_DATA_DISPLAY {
        return s_in.to_string();
    }
    let mut end = MAX_COPY_DATA_DISPLAY;
    while end > 0 && !s_in.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = s_in[..end].to_string();
    out.push_str("...");
    out
}
