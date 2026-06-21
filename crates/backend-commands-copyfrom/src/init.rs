//! Seam installation for `backend-commands-copyfrom` (the COPY FROM driver).
//!
//! This crate owns `copyfrom.c`; the parser (`copyfromparse.c`,
//! `backend-commands-copyfromparse`) reaches the cross-subsystem boundary
//! points (data-source reads, the fmgr value layer, encoding verify/convert,
//! pgstat progress, the libpq frontend) through the seams declared in
//! `backend-commands-copyfrom-seams`. The driver — which holds the per-query
//! `Mcx`, the resolved `FmgrInfo`s, the `EState` and the open data source —
//! installs every one of them here.
//!
//! Seams that reach genuinely-unwired machinery (binary receive, encoding
//! transcoding, the frontend `CopyInResponse`, default-expression evaluation)
//! install a body that raises a clear `ereport(ERROR)` rather than the latent
//! panic; the no-transcoding text path the driver exercises end-to-end never
//! reaches them.

use mcx::{Mcx, MemoryContext};
use types_copy::{
    AttrInfo, AttrValue, CopyGetDataResult, CopyParseState, EncodingConversionResult,
};
use types_core::primitive::Oid;
use types_tuple::backend_access_common_heaptuple::Datum as RichDatum;
use types_error::{PgError, PgResult};
use types_rel::Relation;

use backend_commands_copyfrom_seams as s;

/// `pg_encoding_max_length(encoding)` — the longest valid multibyte sequence
/// for `encoding` (mb/wchar.c `pg_wchar_table[encoding].maxmblen`). Inlined for
/// the encodings COPY FROM supports on the no-transcoding path; an unknown
/// encoding falls back to the conservative `MAX_CONVERSION_INPUT_LENGTH` (4).
fn pg_encoding_max_length(encoding: i32) -> i32 {
    const PG_SQL_ASCII: i32 = 0;
    const PG_UTF8: i32 = 6;
    match encoding {
        PG_SQL_ASCII => 1,
        PG_UTF8 => 4,
        // LATIN1..LATIN10 (8..17) and the other single-byte server encodings are
        // 1; anything else we don't transcode, so the conservative max is fine.
        8..=17 => 1,
        _ => 4,
    }
}

/// Install every `backend-commands-copyfrom-seams` seam.
pub fn init_seams() {
    // -- data source reads --
    s::copy_get_data_file::set(|cstate: &CopyParseState<'_>, maxread: i32| {
        crate::copy_get_data_file_impl(cstate, maxread)
    });
    s::copy_get_data_frontend::set(
        |cstate: &CopyParseState<'_>, _minread: i32, maxread: i32| -> PgResult<CopyGetDataResult> {
            crate::copy_get_data_frontend_impl(cstate, maxread)
        },
    );
    s::copy_get_data_callback::set(
        |cstate: &CopyParseState<'_>, minread: i32, maxread: i32| -> PgResult<CopyGetDataResult> {
            crate::copy_get_data_callback_impl(cstate, minread, maxread)
        },
    );

    // -- encoding verify / convert (no-transcoding path consults verifymbstr) --
    s::pg_encoding_verifymbstr::set(|encoding: i32, mbstr: &[u8]| -> i32 {
        // pg_encoding_verifymbstr returns the number of leading bytes that form
        // valid characters in `encoding`. We delegate to pg_verifymbstr (whole-
        // buffer verify) for the supported encodings: a fully-valid buffer
        // returns its length; otherwise 0 (conservative — the codec then waits
        // for more bytes / raises).
        match backend_utils_mb_mbutils_seams::pg_verifymbstr::call(mbstr, true) {
            Ok(true) => mbstr.len() as i32,
            _ => 0,
        }
    });
    s::pg_encoding_max_length::set(pg_encoding_max_length);
    s::pg_do_encoding_conversion_buf::set(
        |proc: Oid, se: i32, de: i32, src: &[u8], cap: i32| -> PgResult<EncodingConversionResult> {
            // copyfromparse.c:504 — convert as much of `src` as fits a `cap`-byte
            // destination buffer with noError=true (stop short on a bad sequence).
            // The seam carries no Mcx; the converted bytes are copied out into the
            // owned result, so run the conversion in a scratch context.
            let ctx = MemoryContext::new("CopyConvertBuf");
            let (converted_src_len, converted) =
                backend_utils_mb_mbutils_seams::pg_do_encoding_conversion_buf::call(
                    ctx.mcx(), proc, se, de, src, cap, true,
                )?;
            Ok(EncodingConversionResult {
                converted_src_len,
                converted: converted.to_vec(),
            })
        },
    );
    s::report_invalid_encoding::set(|encoding: i32, mbstr: &[u8]| -> PgResult<()> {
        // report_invalid_encoding (mbutils.c) — formats the leading bad-char hex
        // dump ("0xe3 0x81") and raises ERRCODE_CHARACTER_NOT_IN_REPERTOIRE.
        backend_utils_mb_mbutils_seams::report_invalid_encoding::call(encoding, mbstr)
    });
    s::pg_verifymbstr::set(|mbstr: &[u8]| -> PgResult<()> {
        backend_utils_mb_mbutils_seams::pg_verifymbstr::call(mbstr, false).map(|_| ())
    });
    s::conversion_error_raise::set(
        |proc: Oid, se: i32, de: i32, src: &[u8], cap: i32| -> PgResult<()> {
            // copyfromparse.c:568 — re-run the conversion with noError=false so the
            // conversion routine itself raises the specific error.
            let ctx = MemoryContext::new("CopyConversionError");
            backend_utils_mb_mbutils_seams::pg_do_encoding_conversion_buf::call(
                ctx.mcx(), proc, se, de, src, cap, false,
            )?;
            // The conversion routine should have raised; if it returned, mirror
            // C's `elog(ERROR, "encoding conversion failed without error")`.
            Err(PgError::error("encoding conversion failed without error"))
        },
    );

    // -- pgstat progress (advisory; no-op) --
    s::pgstat_progress_update_bytes_processed::set(|_value: i64| -> PgResult<()> { Ok(()) });

    // -- tuple-descriptor / relcache accessors --
    s::relation_natts::set(|rel: &Relation<'_>| -> PgResult<i32> { Ok(rel.rd_att.natts) });
    s::attr_info::set(|rel: &Relation<'_>, m: i32| -> PgResult<AttrInfo> {
        let att = &rel.rd_att.attrs[m as usize];
        Ok(AttrInfo {
            attname: String::from_utf8_lossy(att.attname.name_str()).into_owned(),
            atttypmod: att.atttypmod,
        })
    });
    s::namestrcmp_attr::set(|rel: &Relation<'_>, m: i32, col_name: &str| -> PgResult<i32> {
        let att = &rel.rd_att.attrs[m as usize];
        let name = String::from_utf8_lossy(att.attname.name_str());
        Ok(match name.as_ref().cmp(col_name) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        })
    });

    // -- fmgr / Datum value layer --
    s::input_function_call_safe::set(
        |cstate: &mut CopyParseState<'_>, m: i32, string: Option<&str>, typmod: i32| -> PgResult<Option<RichDatum<'_>>> {
            // Resolve the per-query Mcx from the cstate's own allocator (the
            // attnumlist PgVec lives in the query context).
            let mcx: Mcx<'_> = *cstate.attnumlist.allocator();
            crate::input_function_call_safe_impl(mcx, cstate, m, string, typmod)
        },
    );
    s::receive_function_call::set(
        |cstate: &mut CopyParseState<'_>, m: i32, buf: Option<&[u8]>, typmod: i32| -> PgResult<RichDatum<'_>> {
            // The receive-function output is allocated in the per-query context;
            // reach it through the cstate's own allocator, as the text input leg
            // does.
            let mcx: Mcx<'_> = *cstate.attnumlist.allocator();
            crate::receive_function_call_impl(mcx, cstate, m, buf, typmod)
        },
    );
    s::exec_eval_expr::set(
        |cstate: &mut CopyParseState<'_>, m: i32| -> PgResult<AttrValue> {
            // values[m] = ExecEvalExpr(cstate->defexprs[m], cstate->econtext,
            //                          &nulls[m]);   (copyfromparse.c:1640)
            //
            // The compiled `ExprState` lives on `cstate.defexprs[m]`; the
            // per-tuple `ExprContext` and the owning `EState` are reached through
            // the back-link wired by `CopyFrom` before the row loop. The EState
            // lives on `CopyFrom`'s stack, in distinct memory from `cstate`, so
            // re-deriving `&mut EState` from the link does not alias the `&mut
            // cstate` we hold.
            let econtext = cstate
                .econtext
                .expect("CopyFrom wires cstate.econtext before any default eval");
            let estate = cstate
                .estate
                .as_mut()
                .expect("CopyFrom wires cstate.estate before any default eval")
                .get_mut();
            let state = cstate.defexprs[m as usize]
                .as_mut()
                .expect("defmap entry implies a compiled defexpr");
            let (datum, isnull) =
                backend_executor_execExpr_seams::exec_eval_expr_switch_context::call(
                    state, econtext, estate,
                )?;
            Ok(AttrValue { datum, isnull })
        },
    );
    s::notice_skipping_row::set(
        |_lineno: u64, _attname: &str, _attval: Option<&str>| -> PgResult<()> { Ok(()) },
    );

    // -- libpq frontend --
    s::receive_copy_begin::set(
        |cstate: &mut CopyParseState<'_>, natts: i32, binary: bool| -> PgResult<()> {
            // The `CopyInResponse` is built in the per-query context; reach it
            // through the cstate's own allocator (the attnumlist PgVec lives in
            // the copy context), exactly as the other allocating seam bodies do.
            let mcx: Mcx<'_> = *cstate.attnumlist.allocator();
            crate::receive_copy_begin_impl(mcx, natts, binary)
        },
    );
}
