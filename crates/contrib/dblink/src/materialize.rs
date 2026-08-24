// Copy a remote result into the current SRF's tuplestore (dblink.c
// materializeResult / storeRow). Remote column text -> local type input fn,
// via a per-column resolved FmgrInfo carrier (C's AttInMetadata), resolved
// once against the FROM-clause rowtype and reused for every row.
use std::ffi::CString;

use datum::Datum;
use pgclient::{ExecStatus, QueryResult, RowSink};
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, SetFunctionReturnMode};
use types_tuple::TupleDescData;

struct AttInMeta {
    natts: usize,
    dropped: Vec<bool>,
    in_funcs: Vec<FmgrInfo>,
    typioparams: Vec<types_core::Oid>,
    typmods: Vec<i32>,
}

impl AttInMeta {
    fn build(tupdesc: &TupleDescData<'_>) -> PgResult<AttInMeta> {
        let natts = tupdesc.natts as usize;
        let mut dropped = Vec::with_capacity(natts);
        let mut in_funcs = Vec::with_capacity(natts);
        let mut typioparams = Vec::with_capacity(natts);
        let mut typmods = Vec::with_capacity(natts);
        for i in 0..natts {
            let att = tupdesc.attr(i);
            if att.attisdropped {
                dropped.push(true);
                in_funcs.push(FmgrInfo::unresolved());
                typioparams.push(types_core::InvalidOid);
                typmods.push(-1);
                continue;
            }
            let (infunc, typioparam) = lsyscache::getTypeInputInfo(att.atttypid)?;
            dropped.push(false);
            in_funcs.push(fmgr_core::fmgr_info(infunc)?);
            typioparams.push(typioparam);
            typmods.push(att.atttypmod);
        }
        Ok(AttInMeta { natts, dropped, in_funcs, typioparams, typmods })
    }
}

// applyRemoteGucs: mirror the remote's DateStyle/IntervalStyle (from
// ParameterStatus) into a local GUC nest level so datatype input parses the
// remote's output formats. Captured as values because the connection lives
// inside the thread-local registry. -1 == no nest level opened.
pub(crate) struct RemoteIoGucs {
    datestyle: Option<String>,
    intervalstyle: Option<String>,
}

impl RemoteIoGucs {
    pub(crate) fn capture(conn: &pgclient::PgConn) -> RemoteIoGucs {
        RemoteIoGucs {
            datestyle: conn.parameter_status("DateStyle").map(str::to_string),
            intervalstyle: conn.parameter_status("IntervalStyle").map(str::to_string),
        }
    }

    pub(crate) fn apply(&self) -> PgResult<i32> {
        let mut nestlevel = -1i32;
        for (name, remote) in [
            ("DateStyle", &self.datestyle),
            ("IntervalStyle", &self.intervalstyle),
        ] {
            let Some(remote) = remote else {
                continue;
            };
            let local = guc::GetConfigOption(name, false, false)?
                .expect("IO GUCs always have a value");
            if *remote == local {
                continue;
            }
            if nestlevel < 0 {
                nestlevel = guc::NewGUCNestLevel();
            }
            guc::set_config_option(
                name,
                Some(remote),
                types_guc::PGC_USERSET,
                types_guc::PGC_S_SESSION,
                guc::GUC_ACTION_SAVE,
                true,
                types_error::ErrorLevel(0),
                false,
            )?;
        }
        Ok(nestlevel)
    }
}

// restoreLocalGucs: success path only; error unwinds leave it to the
// transaction's GUC machinery, as C does.
pub(crate) fn restore_local_gucs(nestlevel: i32) {
    if nestlevel > 0 {
        guc::AtEOXact_GUC(true, nestlevel);
    }
}

fn rowtype_mismatch() -> Box<PgError> {
    Box::new(
        PgError::error(
            "remote query result rowtype does not match the specified FROM clause rowtype",
        )
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
    )
}

fn build_row(
    srf: &mut funcapi::MaterializedSRF<'_>,
    attinmeta: &mut AttInMeta,
    scratch: mcx::Mcx<'_>,
    cols: &[Option<&[u8]>],
) -> PgResult<()> {
    let natts = attinmeta.natts;
    let mut values = vec![Datum::null(); natts];
    let mut isnull = vec![true; natts];
    for i in 0..natts {
        if attinmeta.dropped[i] {
            continue;
        }
        if let Some(bytes) = cols.get(i).copied().flatten() {
            // dblink pins the remote client_encoding to the local database
            // encoding at connect (lib.rs), but honoring that is entirely up to
            // the remote: a hostile/compromised server, an honest SQL_ASCII
            // server, or a MITM on a non-TLS link can return text-format bytes
            // that are invalid in the local encoding. C's dblink tolerates the
            // resulting mojibake because it is byte-oriented, but this port has
            // from_utf8_unchecked sinks (e.g. jsonpath_exec over stored jsonb)
            // that rely on the engine invariant that stored strings are valid
            // in the database encoding; feeding them invalid bytes is UB.
            // Verify against the database encoding — as C's pg_client_to_server
            // would when client_encoding == database encoding — before the type
            // input function runs and the value can become a stored datum.
            // SQL_ASCII accepts every byte, so valid behavior is preserved.
            // This covers both the streaming TupleSink::row path and
            // materialize_result, which funnel every remote column through here.
            mbutils::pg_verifymbstr(bytes, false)?;
            let cstr = CString::new(bytes)
                .map_err(|_| Box::new(PgError::error("remote value contains embedded NUL byte")))?;
            values[i] = types_fmgr::input_function_call(
                &mut attinmeta.in_funcs[i],
                Some(&cstr),
                attinmeta.typioparams[i],
                attinmeta.typmods[i],
                scratch,
            )?;
            isnull[i] = false;
        }
    }
    srf.putvalues(&values, &isnull)
}

// The RowSink for exec_streaming (dblink's synchronous, single-row-mode path).
// One InitMaterializedSRF per resultset — a re-`result_start` discards the
// previous store (C's storeRow throw-away-all-but-last).
// The per-row scratch is a BUMP context: C's tmpcontext is reset after every
// row with the row's palloc'd datums still live (storeRow), and the bump
// backend is the one whose reset releases charges wholesale — an
// exact-accounting context debug-asserts "reset with N bytes still charged"
// on exactly this pattern (caught by the CI cluster gate at 0bbbb7970f).
pub struct TupleSink<'m, 'f> {
    mcx: mcx::Mcx<'m>,
    flinfo: &'f mut FmgrInfo,
    fcinfo_ptr: *mut Fcinfo,
    srf: Option<funcapi::MaterializedSRF<'m>>,
    attinmeta: Option<AttInMeta>,
    scratch: mcx::MemoryContext,
    guc_nestlevel: i32,
}

impl<'m, 'f> TupleSink<'m, 'f> {
    // SAFETY: fcinfo must outlive the sink and stay unaliased for its life
    // (single-threaded fmgr call). The sink re-derives &mut from the pointer
    // only inside result_start, between the caller's own accesses.
    pub unsafe fn new(mcx: mcx::Mcx<'m>, flinfo: &'f mut FmgrInfo, fcinfo: *mut Fcinfo) -> Self {
        TupleSink {
            mcx,
            flinfo,
            fcinfo_ptr: fcinfo,
            srf: None,
            attinmeta: None,
            scratch: mcx::MemoryContext::new_bump("dblink temporary context"),
            guc_nestlevel: -1,
        }
    }

    pub fn finish(self, fcinfo: &mut Fcinfo) -> Datum {
        restore_local_gucs(self.guc_nestlevel);
        match self.srf {
            Some(srf) => srf.finish(fcinfo),
            None => Datum::from_usize(0),
        }
    }
}

impl RowSink for TupleSink<'_, '_> {
    fn result_start(&mut self, conn: &pgclient::PgConn, nfields: usize) -> PgResult<()> {
        if self.guc_nestlevel < 0 {
            self.guc_nestlevel = RemoteIoGucs::capture(conn).apply()?;
        }
        // SAFETY: constructor contract; no other live borrow of fcinfo here.
        let fcinfo = unsafe { &mut *self.fcinfo_ptr };
        let srf = funcapi::InitMaterializedSRF(self.mcx, self.flinfo, fcinfo, 0)?;
        let attinmeta = AttInMeta::build(&srf.tupdesc)?;
        if nfields != attinmeta.natts {
            return Err(rowtype_mismatch());
        }
        self.srf = Some(srf);
        self.attinmeta = Some(attinmeta);
        Ok(())
    }

    fn row(&mut self, cols: &[Option<&[u8]>]) -> PgResult<()> {
        let srf = self.srf.as_mut().expect("result_start precedes row");
        let attinmeta = self.attinmeta.as_mut().expect("result_start precedes row");
        let r = build_row(srf, attinmeta, self.scratch.mcx(), cols);
        self.scratch.reset();
        r
    }
}

// materializeResult: a whole buffered result (dblink_fetch, async
// dblink_get_result). COMMAND_OK returns the status string as one TEXT column.
pub fn materialize_result(
    mcx: mcx::Mcx<'_>,
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    gucs: &RemoteIoGucs,
    res: &QueryResult,
) -> PgResult<Datum> {
    if res.status == ExecStatus::CommandOk {
        return materialize_command_status(mcx, fcinfo, &res.cmd_tag);
    }
    let fcinfo_ptr: *mut Fcinfo = fcinfo;
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    let mut attinmeta = AttInMeta::build(&srf.tupdesc)?;
    if res.nfields != attinmeta.natts {
        return Err(rowtype_mismatch());
    }
    let nestlevel = if res.rows.is_empty() { -1 } else { gucs.apply()? };
    let mut scratch = mcx::MemoryContext::new_bump("dblink temporary context");
    let mut cols: Vec<Option<&[u8]>> = Vec::with_capacity(res.nfields);
    for row in &res.rows {
        cols.clear();
        cols.extend(row.iter().map(|c| c.as_deref()));
        build_row(&mut srf, &mut attinmeta, scratch.mcx(), &cols)?;
        scratch.reset();
    }
    restore_local_gucs(nestlevel);
    // SAFETY: fcinfo_ptr is the same borrow InitMaterializedSRF released.
    Ok(srf.finish(unsafe { &mut *fcinfo_ptr }))
}

// One TEXT column named "status" holding the CommandComplete tag.
pub(crate) fn materialize_command_status(
    mcx: mcx::Mcx<'_>,
    fcinfo: &mut Fcinfo,
    tag: &str,
) -> PgResult<Datum> {
    let tupdesc = crate::single_text_tupdesc(mcx, "status")?;
    let mut store =
        tuplestore::Tuplestore::begin_heap(false, false, init_small::globals::work_mem());
    let d = types_fmgr::varlena_result(varlena::cstring_to_text(mcx, tag.as_bytes())?);
    store.putvalues(&tupdesc, &[d], &[false])?;
    // C materializeResult: `rsinfo->setDesc = tupdesc` publishes the exact
    // descriptor the rows were formed under — here a single TEXT "status"
    // column. The executor's tupledesc_match check runs only when setDesc is
    // set; leaving it None lets a FROM clause naming N != 1 columns deform this
    // 1-column tuple under the N-column query descriptor (out-of-bounds read).
    // Persist the descriptor in the per-query result context (as C does: it
    // dies with that context, never freed explicitly) so the pointer stays
    // valid after we return and the executor can validate it.
    let desc = mcx::alloc_in(mcx, tupdesc)?;
    let set_desc = core::ptr::NonNull::from(&*desc).cast::<core::ffi::c_void>();
    core::mem::forget(desc);
    match fcinfo.rsinfo_mut() {
        Some(rsi) => {
            rsi.returnMode = SetFunctionReturnMode::Materialize;
            rsi.setResult = Some(Box::new(store));
            rsi.setDesc = Some(set_desc);
        }
        None => return Err(Box::new(PgError::error("materialize mode required"))),
    }
    Ok(Datum::from_usize(0))
}

#[cfg(test)]
mod tests {
    // build_row now runs pg_verifymbstr against the local database encoding
    // before handing remote text-format column bytes to the type input function
    // (idx 84). build_row itself needs a live backend (mcx, fmgr, catalogs, an
    // SRF), so guard the exact predicate it relies on: under a UTF-8 database,
    // invalid multibyte sequences must produce a catchable error rather than
    // flow through to from_utf8_unchecked sinks, while valid text passes.
    // SQL_ASCII (byte-transparent) is left to mbutils' own coverage.
    #[test]
    fn remote_column_bytes_are_encoding_verified() {
        use wchar::PG_UTF8;

        // Valid UTF-8 (ASCII and a multibyte codepoint) is accepted.
        assert!(mbutils::pg_verify_mbstr(PG_UTF8, b"hello", false).unwrap());
        assert!(mbutils::pg_verify_mbstr(PG_UTF8, "ol\u{00e9}".as_bytes(), false).unwrap());

        // A lone 0xFF and a truncated multibyte sequence are rejected with a
        // catchable error instead of becoming a poisoned text datum.
        let err = mbutils::pg_verify_mbstr(PG_UTF8, b"bad\xff", false)
            .err()
            .unwrap();
        assert_eq!(
            err.sqlstate(),
            types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
        );
        let err = mbutils::pg_verify_mbstr(PG_UTF8, b"bad\xe2\x82", false)
            .err()
            .unwrap();
        assert_eq!(
            err.sqlstate(),
            types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
        );
    }
}
