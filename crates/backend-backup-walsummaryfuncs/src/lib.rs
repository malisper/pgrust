//! Port of PostgreSQL `src/backend/backup/walsummaryfuncs.c` — the
//! SQL-callable functions for accessing WAL summary data.
//!
//! Every function in `walsummaryfuncs.c` is ported here with its full logic:
//!
//! * `pg_available_wal_summaries`  (SRF: list the summary files)
//! * `pg_wal_summary_contents`     (SRF: the block refs in one summary file)
//! * `pg_get_wal_summarizer_state` (the WAL summarizer process state)
//!
//! # Set-returning functions (`pg_available_wal_summaries`,
//! `pg_wal_summary_contents`)
//!
//! Both are materialize-mode SRFs. They take the owned fmgr call frame
//! (`FunctionCallInfoBaseData`), run `InitMaterializedSRF`, and append each
//! row through `materialized_srf_putvalues` (the funcapi-owned
//! `heap_form_tuple` + `tuplestore_puttuple` against `rsi->setDesc` /
//! `rsi->setResult`). The row values are built on the canonical
//! `types_tuple::Datum` (the C `Int64GetDatum` / `LSNGetDatum` /
//! `ObjectIdGetDatum` / `Int16GetDatum` / `BoolGetDatum` word builders). The
//! `(Datum) 0` return is the null word.
//!
//! # Composite-returning function (`pg_get_wal_summarizer_state`)
//!
//! Following the project model for record-returning builtins (cf.
//! `backend-access-transam-xlogfuncs`), the four output columns are returned
//! as a typed Rust struct ([`WalSummarizerStateResult`]) — one field per
//! column, with the nullable `summarizer_pid` as `Option<i32>`. The C
//! `get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE`
//! assertion + `heap_form_tuple` + `HeapTupleGetDatum` have no value-boundary
//! analogue at this owned interface; the caller-side fmgr glue forms the
//! tuple from the struct.
//!
//! # Seamed dependencies
//!
//! * `GetWalSummaries` — `backend-backup-walsummary` (landed seam).
//! * `OpenWalSummaryFile` + `CreateBlockRefTableReader` (with the
//!   `ReadWalSummary` read callback and `ReportWalSummaryError` error
//!   callback) — bundled into `wal_summary_create_reader`, plus the
//!   `FileClose` teardown `wal_summary_reader_file_close`
//!   (`backend-backup-walsummary`, owner not yet ported: loud seam-and-panic).
//! * `BlockRefTableReaderNextRelation` / `BlockRefTableReaderGetBlocks` /
//!   `DestroyBlockRefTableReader` — `common/blkreftable.c` reader side
//!   (owner not yet ported: loud seam-and-panic).
//! * `GetWalSummarizerState` — `backend-postmaster-walsummarizer` (landed,
//!   called directly).
//! * `InitMaterializedSRF` / `materialized_srf_putvalues` / the
//!   `PG_GETARG_INT64` / `PG_GETARG_LSN` argument reads — `funcapi` (landed).
//! * `CHECK_FOR_INTERRUPTS` — tcop (landed seam).

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use mcx::Mcx;

use backend_backup_walsummary_seams as walsummary;
use backend_tcop_postgres_seams as tcop;
use backend_utils_fmgr_funcapi_seams as funcapi;
use common_blkreftable_seams as blkreftable;

use types_core::{TimeLineID, XLogRecPtr};
use types_error::error::ERRCODE_INVALID_PARAMETER_VALUE;
use types_error::{PgError, PgResult};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::Datum;
use types_walsummarizer::WalSummaryFile;

/// `NUM_WS_ATTS` — output columns of `pg_available_wal_summaries`.
const NUM_WS_ATTS: usize = 3;
/// `NUM_SUMMARY_ATTS` — output columns of `pg_wal_summary_contents`.
const NUM_SUMMARY_ATTS: usize = 6;
/// `MAX_BLOCKS_PER_CALL` — block numbers fetched per
/// `BlockRefTableReaderGetBlocks` call.
const MAX_BLOCKS_PER_CALL: usize = 256;
/// `PG_INT32_MAX` (`c.h`) — the timeline-range guard bound.
const PG_INT32_MAX: i64 = i32::MAX as i64;

/// `pg_available_wal_summaries(PG_FUNCTION_ARGS)` (walsummaryfuncs.c) — list
/// the WAL summary files available in `pg_wal/summaries`. Returns the SRF null
/// word `(Datum) 0`.
pub fn pg_available_wal_summaries<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // ReturnSetInfo *rsi;
    // Datum values[NUM_WS_ATTS]; bool nulls[NUM_WS_ATTS];

    // InitMaterializedSRF(fcinfo, 0);
    // rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;

    // memset(nulls, 0, sizeof(nulls));
    let nulls: [bool; NUM_WS_ATTS] = [false; NUM_WS_ATTS];

    // wslist = GetWalSummaries(0, InvalidXLogRecPtr, InvalidXLogRecPtr);
    let wslist = walsummary::get_wal_summaries::call(
        mcx,
        0,                            // tli = 0 (any)
        types_core::InvalidXLogRecPtr, // start_lsn (unbounded)
        types_core::InvalidXLogRecPtr, // end_lsn (unbounded)
    )?;

    // foreach(lc, wslist) { WalSummaryFile *ws = lfirst(lc); ... }
    for ws in wslist.iter() {
        // CHECK_FOR_INTERRUPTS();
        tcop::check_for_interrupts::call()?;

        // values[0] = Int64GetDatum((int64) ws->tli);
        // values[1] = LSNGetDatum(ws->start_lsn);
        // values[2] = LSNGetDatum(ws->end_lsn);
        let values: [Datum<'mcx>; NUM_WS_ATTS] = [
            Datum::from_i64(ws.tli as i64),
            Datum::from_u64(ws.start_lsn),
            Datum::from_u64(ws.end_lsn),
        ];

        // tuple = heap_form_tuple(rsi->setDesc, values, nulls);
        // tuplestore_puttuple(rsi->setResult, tuple);
        let rsi = fcinfo
            .resultinfo
            .as_mut()
            .expect("InitMaterializedSRF set fcinfo->resultinfo");
        funcapi::materialized_srf_putvalues::call(rsi, &values, &nulls)?;
    }

    // return (Datum) 0;
    Ok(Datum::null())
}

/// `pg_wal_summary_contents(PG_FUNCTION_ARGS)` (walsummaryfuncs.c) — list the
/// contents of a WAL summary file identified by TLI, start LSN, and end LSN.
/// Returns the SRF null word `(Datum) 0`.
pub fn pg_wal_summary_contents<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // Datum values[NUM_SUMMARY_ATTS]; bool nulls[NUM_SUMMARY_ATTS];
    // WalSummaryFile ws; WalSummaryIO io; BlockRefTableReader *reader; ...

    // InitMaterializedSRF(fcinfo, 0);
    // rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;
    // memset(nulls, 0, sizeof(nulls));
    let nulls: [bool; NUM_SUMMARY_ATTS] = [false; NUM_SUMMARY_ATTS];

    // raw_tli = PG_GETARG_INT64(0);
    let raw_tli: i64 = funcapi::srf_arg_int64::call(fcinfo, 0);
    // if (raw_tli < 1 || raw_tli > PG_INT32_MAX)
    //     ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE),
    //             errmsg("invalid timeline %lld", raw_tli));
    if raw_tli < 1 || raw_tli > PG_INT32_MAX {
        return Err(
            PgError::error(alloc::format!("invalid timeline {raw_tli}"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }

    // ws.tli = (TimeLineID) raw_tli;
    // ws.start_lsn = PG_GETARG_LSN(1);
    // ws.end_lsn = PG_GETARG_LSN(2);
    let ws = WalSummaryFile {
        tli: raw_tli as TimeLineID,
        start_lsn: funcapi::srf_arg_lsn::call(fcinfo, 1),
        end_lsn: funcapi::srf_arg_lsn::call(fcinfo, 2),
    };

    // io.filepos = 0;
    // io.file = OpenWalSummaryFile(&ws, false);
    // reader = CreateBlockRefTableReader(ReadWalSummary, &io,
    //                                    FilePathName(io.file),
    //                                    ReportWalSummaryError, NULL);
    let reader = walsummary::wal_summary_create_reader::call(mcx, ws)?;

    // while (BlockRefTableReaderNextRelation(reader, &rlocator, &forknum,
    //                                        &limit_block)) { ... }
    while let Some((rlocator, forknum, limit_block)) =
        blkreftable::block_ref_table_reader_next_relation::call(reader)?
    {
        // CHECK_FOR_INTERRUPTS();
        tcop::check_for_interrupts::call()?;

        // values[0] = ObjectIdGetDatum(rlocator.relNumber);
        // values[1] = ObjectIdGetDatum(rlocator.spcOid);
        // values[2] = ObjectIdGetDatum(rlocator.dbOid);
        // values[3] = Int16GetDatum((int16) forknum);
        let mut values: [Datum<'mcx>; NUM_SUMMARY_ATTS] =
            core::array::from_fn(|_| Datum::null());
        values[0] = Datum::from_oid(rlocator.relNumber);
        values[1] = Datum::from_oid(rlocator.spcOid);
        values[2] = Datum::from_oid(rlocator.dbOid);
        values[3] = Datum::from_i16(forknum as i32 as i16);

        // If the limit block is not InvalidBlockNumber, emit an extra row with
        // that block number and limit_block = true.
        //   if (BlockNumberIsValid(limit_block)) { ... }
        if limit_block != types_core::InvalidBlockNumber {
            // values[4] = Int64GetDatum((int64) limit_block);
            // values[5] = BoolGetDatum(true);
            values[4] = Datum::from_i64(limit_block as i64);
            values[5] = Datum::from_bool(true);

            // tuple = heap_form_tuple(rsi->setDesc, values, nulls);
            // tuplestore_puttuple(rsi->setResult, tuple);
            let rsi = fcinfo
                .resultinfo
                .as_mut()
                .expect("InitMaterializedSRF set fcinfo->resultinfo");
            funcapi::materialized_srf_putvalues::call(rsi, &values, &nulls)?;
        }

        // Loop over blocks within the current relation fork.
        loop {
            // CHECK_FOR_INTERRUPTS();
            tcop::check_for_interrupts::call()?;

            // nblocks = BlockRefTableReaderGetBlocks(reader, blocks,
            //                                        MAX_BLOCKS_PER_CALL);
            // if (nblocks == 0) break;
            let blocks = blkreftable::block_ref_table_reader_get_blocks::call(
                mcx,
                reader,
                MAX_BLOCKS_PER_CALL,
            )?;
            if blocks.is_empty() {
                break;
            }

            // For each known-modified block, emit a row with that block number
            // and limit_block = false.
            //   values[5] = BoolGetDatum(false);
            values[5] = Datum::from_bool(false);
            for &blk in blocks.iter() {
                // values[4] = Int64GetDatum((int64) blocks[i]);
                values[4] = Datum::from_i64(blk as i64);

                // tuple = heap_form_tuple(rsi->setDesc, values, nulls);
                // tuplestore_puttuple(rsi->setResult, tuple);
                let rsi = fcinfo
                    .resultinfo
                    .as_mut()
                    .expect("InitMaterializedSRF set fcinfo->resultinfo");
                funcapi::materialized_srf_putvalues::call(rsi, &values, &nulls)?;
            }
        }
    }

    // Cleanup
    //   DestroyBlockRefTableReader(reader);
    //   FileClose(io.file);
    blkreftable::destroy_block_ref_table_reader::call(reader);
    walsummary::wal_summary_reader_file_close::call(reader);

    // return (Datum) 0;
    Ok(Datum::null())
}

/// The `pg_get_wal_summarizer_state()` result row (`NUM_STATE_ATTS == 4`).
///
/// One field per output column, mirroring the C `heap_form_tuple` over the
/// state values; the nullable `summarizer_pid` column is `Option<i32>`
/// (`nulls[3] = true` when the C `summarizer_pid < 0`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalSummarizerStateResult {
    /// `summarized_tli` (`int8`) — `Int64GetDatum((int64) summarized_tli)`.
    pub summarized_tli: TimeLineID,
    /// `summarized_lsn` (`pg_lsn`) — `LSNGetDatum(summarized_lsn)`.
    pub summarized_lsn: XLogRecPtr,
    /// `pending_lsn` (`pg_lsn`) — `LSNGetDatum(pending_lsn)`.
    pub pending_lsn: XLogRecPtr,
    /// `summarizer_pid` (`int4`) — `Int32GetDatum(summarizer_pid)`, or NULL
    /// (`None`) when `summarizer_pid < 0`.
    pub summarizer_pid: Option<i32>,
}

/// `pg_get_wal_summarizer_state(PG_FUNCTION_ARGS)` (walsummaryfuncs.c) —
/// returns information about the state of the WAL summarizer process.
pub fn pg_get_wal_summarizer_state() -> PgResult<WalSummarizerStateResult> {
    // GetWalSummarizerState(&summarized_tli, &summarized_lsn, &pending_lsn,
    //                       &summarizer_pid);
    let state = backend_postmaster_walsummarizer::GetWalSummarizerState()?;

    // if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    //     elog(ERROR, "return type must be a row type");
    // (Composite-result classification is driven by the caller-side fmgr glue
    // in the value model; cf. backend-access-transam-xlogfuncs.)

    // memset(nulls, 0, sizeof(nulls));
    // values[0] = Int64GetDatum((int64) summarized_tli);
    // values[1] = LSNGetDatum(summarized_lsn);
    // values[2] = LSNGetDatum(pending_lsn);
    // if (summarizer_pid < 0) nulls[3] = true;
    // else values[3] = Int32GetDatum(summarizer_pid);
    let summarizer_pid = if state.summarizer_pid < 0 {
        None
    } else {
        Some(state.summarizer_pid)
    };

    // htup = heap_form_tuple(tupdesc, values, nulls);
    // PG_RETURN_DATUM(HeapTupleGetDatum(htup));
    Ok(WalSummarizerStateResult {
        summarized_tli: state.summarized_tli,
        summarized_lsn: state.summarized_lsn,
        pending_lsn: state.pending_lsn,
        summarizer_pid,
    })
}

/// This unit installs no inward seams — `walsummaryfuncs.c` is a leaf of
/// SQL-callable functions that no other unit calls. The fmgr dispatch table
/// reaches these `pub fn` entry points directly. Present for the workspace
/// `init_seams()` convention.
pub fn init_seams() {}
