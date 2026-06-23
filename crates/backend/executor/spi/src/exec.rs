//! The SPI cursor-driver leg (`ts_rewrite`'s SPI cursor walk).
//!
//! This is the `ts_rewrite(query, text)` driver (`tsquery_rewrite_query`),
//! which walks an SPI **cursor** rather than a single execute:
//! `SPI_connect()` → `SPI_prepare(command, 0, NULL)` →
//! `SPI_cursor_open(NULL, plan, NULL, NULL, true)` →
//! `SPI_cursor_fetch(portal, true, 100)` loop reading `SPI_tuptable` /
//! `SPI_processed` and the per-column `SPI_getbinval(..., &isnull)` →
//! `SPI_cursor_close` / `SPI_freeplan` / `SPI_finish`.
//!
//! `SPI_cursor_open` opens a [`Portal`] over the prepared `SpiPlanPtr`'s single
//! `CachedPlanSource` (`SPI_cursor_open_internal`: `GetCachedPlan` +
//! `PortalDefineQuery` + `PortalStart`), and the fetch loop drives the portal
//! through `PortalRunFetch` into a DestSPI receiver — the same forward-fetch
//! machinery `cursor.rs` uses for an externally-declared cursor, but here the
//! portal is created from the prepared plan instead of looked up by name.

use ::utils_error::ereport;
use ::mcx::MemoryContext;
use types_error::{PgResult, ERROR};
use portal::{
    CommandTag, FetchDirection, Portal, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL,
};
use ::types_resowner::ResourceOwner;

use spi_seams::{TsRewriteResult, TsRewriteRow};

use crate::backbone::{SPI_connect, SPI_finish};
use crate::dest_spi::{create_spi_dest_receiver, take_spi_raw_result};
use crate::prepare;

use pquery as pquery;
use cache_plancache as plancache;
use plancache_seams as plancache_seams;
use portalmem_seams as portalmem;
use snapmgr_seams as snapmgr;
use ::nodes::parsestmt::{CachedPlanHandle as NodeCachedPlanHandle, CachedPlanSourceHandle};

/// The fixed batch size of `SPI_cursor_fetch(portal, true, 100)` in
/// `tsquery_rewrite_query`.
const FETCH_COUNT: i64 = 100;

/// `VARHDRSZ` — the 4-byte varlena length word.
const VARHDRSZ: usize = 4;

/// Un-pack a short (1-byte header) stored varlena image to the canonical 4-byte-
/// header form, mirroring the short arm of `detoast_attr` — exactly what C's
/// `DatumGetTSVector` / `DatumGetTSQuery` (`PG_DETOAST_DATUM`) does to the raw
/// `SPI_getbinval` material before it reaches `ts_accum` / the `tsquery_rewrite`
/// row loop. Those cores read the size word at the FIXED offset 4 and the
/// `WordEntry`/`QueryItem` array at offset 8, so a 4-byte-header base is required.
///
/// A `tsvector`/`tsquery` column is toastable (typlen == -1, typstorage 'x'), so
/// under `SHORT_VARLENA_PACKING` a small stored value arrives here with a 1-byte
/// header and a fixed offset-4 read would land 3 bytes into the payload (or panic
/// on a tiny image). A 4-byte / external / compressed image (external/compressed
/// columns are already detoasted by the executor) passes through verbatim. With
/// the flag OFF every stored varlena is 4-byte, so this is a no-op
/// (behavior-preserving).
fn detoast_short_spi_varlena(image: Vec<u8>) -> Vec<u8> {
    // VARATT_IS_1B && !VARATT_IS_1B_E (a genuine short inline header: low bit set,
    // but not the lone `0x01` external/expanded tag byte).
    if image.first().is_some_and(|&b| b != 0x01 && (b & 0x01) == 0x01) {
        const VARHDRSZ_SHORT: usize = 1;
        let data_size = ((image[0] >> 1) & 0x7f) as usize - VARHDRSZ_SHORT;
        let new_size = data_size + VARHDRSZ;
        let mut out = Vec::with_capacity(new_size);
        out.extend_from_slice(&((new_size as u32) << 2).to_ne_bytes());
        out.extend_from_slice(&image[VARHDRSZ_SHORT..VARHDRSZ_SHORT + data_size]);
        out
    } else {
        image
    }
}

/// `tsquery_rewrite_run(command)` seam body — the `ts_rewrite(query, text)` SPI
/// cursor driver (`tsquery_rewrite_query`, tsquery_rewrite.c). Runs `command`
/// through an SPI cursor and returns the fetched `(target, substitute)` tsquery
/// pairs plus the result descriptor shape for the caller's two-`tsquery`-column
/// type check.
pub(crate) fn tsquery_rewrite_run_seam(command: String) -> PgResult<TsRewriteResult> {
    // SPI_connect();
    SPI_connect()?;
    // Run the cursor body; on error or success we must still SPI_finish() (the C
    // path has no PG_TRY, but a thrown error unwinds through AtEOXact_SPI which
    // tears the connection down — here we finish on the success path and let the
    // error propagate, mirroring C's "error aborts the (sub)transaction").
    let res = run_cursor(&command);
    // SPI_finish();
    SPI_finish()?;
    res
}

/// `TSVECTOROID` (pg_type.dat).
const TSVECTOROID: types_core::Oid = 3614;

/// `exec_stat_query(sql)` seam body — the `ts_stat(query[, weights])` SPI cursor
/// driver (`ts_stat_sql`, tsvector_op.c:2574). Runs `sql` through a read-only SPI
/// cursor, validates it returns exactly one `tsvector`-coercible column, and
/// returns each non-null result tsvector datum's verbatim varlena image (the
/// `SPI_getbinval(..., 1, &isnull)` material `ts_accum` consumes).
pub(crate) fn exec_stat_query_seam(sql: &[u8]) -> PgResult<Vec<Vec<u8>>> {
    let query = String::from_utf8(sql.to_vec())
        .map_err(|_| ereport(ERROR).errmsg("ts_stat: query is not valid UTF-8").into_error())?;
    // SPI_connect();
    SPI_connect()?;
    let res = run_stat_cursor(&query);
    // SPI_finish();
    SPI_finish()?;
    res
}

fn run_stat_cursor(command: &str) -> PgResult<Vec<Vec<u8>>> {
    // if ((plan = SPI_prepare(query, 0, NULL)) == NULL)
    //     elog(ERROR, "SPI_prepare(\"%s\") failed", query);
    let plan = match prepare::spi_prepare(command.as_bytes(), &[])? {
        Some(p) => p,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("SPI_prepare(\"{command}\") failed"))
                .into_error());
        }
    };

    // if ((portal = SPI_cursor_open(NULL, plan, NULL, NULL, true)) == NULL)
    let portal = spi_cursor_open(plan).map_err(|e| {
        let _ = prepare::spi_freeplan(plan);
        e
    })?;

    // SPI_cursor_fetch(portal, true, 100); then the one-tsvector-column type
    // check on SPI_tuptable->tupdesc (== portal->tupDesc for a one-select portal).
    let (natts, col1_type, _col2_type) = portal_result_shape(&portal);

    let mut rows: Vec<Vec<u8>> = Vec::new();
    let fetch_result = (|| -> PgResult<()> {
        // if (SPI_tuptable == NULL || tupdesc->natts != 1 ||
        //     !IsBinaryCoercible(SPI_gettypeid(tupdesc, 1), TSVECTOROID))
        //     ereport(ERROR, "ts_stat query must return one tsvector column");
        let coercible = natts == 1
            && coerce_seams::is_binary_coercible::call(col1_type, TSVECTOROID)?;
        if !coercible {
            return Err(ereport(ERROR)
                .errcode(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("ts_stat query must return one tsvector column")
                .into_error());
        }

        // while (SPI_processed > 0) { for each row: ts_accum(getbinval(.,1)); fetch }
        loop {
            let batch = stat_cursor_fetch_rows(&portal)?;
            let n = batch.len();
            for col in batch {
                if let Some(bytes) = col {
                    rows.push(bytes);
                }
            }
            if (n as i64) < FETCH_COUNT {
                break;
            }
        }
        Ok(())
    })();

    // SPI_cursor_close(portal); SPI_freeplan(plan);
    let _ = portalmem::portal_drop::call(&portal, false);
    let _ = prepare::spi_freeplan(plan);

    fetch_result?;
    Ok(rows)
}

/// `SPI_cursor_fetch(portal, true, 100)` reading the single `tsvector` column of
/// each fetched row (`SPI_getbinval(vals[i], tupdesc, 1, &isnull)`): `None` for a
/// SQL NULL column (C: `if (!isnull) ts_accum(...)`), else the verbatim varlena
/// image.
fn stat_cursor_fetch_rows(portal: &Portal) -> PgResult<Vec<Option<Vec<u8>>>> {
    let receiver = create_spi_dest_receiver();
    let _nfetched =
        pquery::portal_run_fetch(portal, FetchDirection::FETCH_FORWARD, FETCH_COUNT, receiver)?;
    let (_columns, raw_rows) = take_spi_raw_result(receiver);

    let mut rows: Vec<Option<Vec<u8>>> = Vec::with_capacity(raw_rows.len());
    for raw in &raw_rows {
        // C: ts_accum(stat, DatumGetTSVector(SPI_getbinval(..., 1, &isnull))).
        // DatumGetTSVector (PG_DETOAST_DATUM) un-packs a short-headed stored
        // tsvector to 4-byte form before `ts_accum`'s offset-4 `tsv_size` read.
        let col = match raw.first() {
            Some(c) if !c.isnull => c.byref.clone().map(detoast_short_spi_varlena),
            _ => None,
        };
        rows.push(col);
    }
    Ok(rows)
}

fn run_cursor(command: &str) -> PgResult<TsRewriteResult> {
    // if ((plan = SPI_prepare(buf, 0, NULL)) == NULL)
    //     elog(ERROR, "SPI_prepare(\"%s\") failed", buf);
    let plan = match prepare::spi_prepare(command.as_bytes(), &[])? {
        Some(p) => p,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("SPI_prepare(\"{command}\") failed"))
                .into_error());
        }
    };

    // if ((portal = SPI_cursor_open(NULL, plan, NULL, NULL, true)) == NULL)
    //     elog(ERROR, "SPI_cursor_open(\"%s\") failed", buf);
    let portal = spi_cursor_open(plan).map_err(|e| {
        // Best-effort free of the prepared plan on the error path (C leaks it to
        // (sub)xact abort; we drop it explicitly since our carrier is a
        // thread-local registry, not the aborting SPI memory context).
        let _ = prepare::spi_freeplan(plan);
        e
    })?;

    // The type check reads SPI_tuptable->tupdesc, which for a PORTAL_ONE_SELECT
    // portal is portal->tupDesc (set by PortalStart). C performs the check after
    // the first fetch regardless of SPI_processed; the descriptor is the same
    // value, so we read it off the portal up front.
    let (natts, col1_type, col2_type) = portal_result_shape(&portal);

    // while (SPI_processed > 0) { SPI_cursor_fetch(portal, true, 100); ... }
    let mut batches: Vec<Vec<TsRewriteRow>> = Vec::new();
    let fetch_result = (|| -> PgResult<()> {
        loop {
            let batch = cursor_fetch_rows(&portal)?;
            let n = batch.len();
            batches.push(batch);
            if (n as i64) < FETCH_COUNT {
                // A short (or empty) batch means the cursor is exhausted — the
                // next fetch would report SPI_processed == 0, ending C's loop.
                break;
            }
        }
        Ok(())
    })();

    // SPI_cursor_close(portal); — PortalDrop. Always runs (success or fetch err).
    let _ = portalmem::portal_drop::call(&portal, false);
    // SPI_freeplan(plan);
    let _ = prepare::spi_freeplan(plan);

    fetch_result?;

    Ok(TsRewriteResult {
        natts,
        col1_type,
        col2_type,
        batches,
    })
}

/// `SPI_cursor_open(NULL, plan, NULL, NULL, true)` → `SPI_cursor_open_internal`,
/// specialized to the parameter-less read-only `ts_rewrite` call: open an
/// unnamed [`Portal`] over the prepared plan's single `CachedPlanSource` and
/// start its execution.
fn spi_cursor_open(plan: types_ri_triggers::SpiPlanPtr) -> PgResult<Portal> {
    // Assert(list_length(plan->plancache_list) == 1);
    // plansource = (CachedPlanSource *) linitial(plan->plancache_list);
    let (sources, saved) = prepare::plan_sources(plan)?;
    if sources.len() != 1 {
        // ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
        //         errmsg("cannot open multi-query plan as cursor")));
        return Err(ereport(ERROR)
            .errcode(::types_error::ERRCODE_INVALID_CURSOR_DEFINITION)
            .errmsg("cannot open multi-query plan as cursor")
            .into_error());
    }
    let source: prepare::SourceHandle = sources[0];

    // if (!SPI_is_cursor_plan(plan)) — a plan that returns no tuples cannot be a
    // cursor. SPI_is_cursor_plan: plansource->resultDesc != NULL.
    let returns_tuples =
        plancache_seams::plansource_has_result_desc::call(CachedPlanSourceHandle(source))?;
    if !returns_tuples {
        let cmdtag = plancache_seams::plansource_command_tag::call(CachedPlanSourceHandle(source))?;
        return Err(ereport(ERROR)
            .errcode(::types_error::ERRCODE_INVALID_CURSOR_DEFINITION)
            .errmsg(format!(
                "cannot open {} query as cursor",
                command_tag_name(cmdtag.0)
            ))
            .into_error());
    }

    // portal = CreateNewPortal();  (name == NULL)
    let portal = portalmem::create_new_portal::call()?;

    // query_string = MemoryContextStrdup(portal->portalContext,
    //                                    plansource->query_string);
    let cxt = MemoryContext::new("SPI cursor open");
    let mcx = cxt.mcx();
    let query_string =
        plancache_seams::plansource_query_string::call(mcx, CachedPlanSourceHandle(source))?;
    let command_tag = plancache_seams::plansource_command_tag::call(CachedPlanSourceHandle(source))?.0;

    // cplan = GetCachedPlan(plansource, NULL, NULL, NULL);  (no bound params)
    let cplan = plancache::GetCachedPlan(source, None, ResourceOwner::NULL, None)?;
    // stmt_list = cplan->stmt_list;
    let stmt_list = plancache_seams::cached_plan_stmt_list::call(mcx, NodeCachedPlanHandle(cplan))?;

    // PortalDefineQuery(portal, NULL, query_string, plansource->commandTag,
    //                   stmt_list, cplan);
    //
    // For an unsaved plan (ts_rewrite never SPI_keepplan's), C copies stmt_list
    // into the portal context and releases the cplan refcount
    // (cplan = NULL): the portal must not depend on a transient
    // CachedPlanSource. portal_define_query_list with CachedPlanHandle::NULL
    // does exactly that (copies the stmts into the portal context). A saved plan
    // would record the cplan handle; ts_rewrite's path is always unsaved, but
    // honor `saved` for faithfulness.
    let portal_cplan = if saved {
        ::portal::CachedPlanHandle(cplan)
    } else {
        ::portal::CachedPlanHandle::NULL
    };
    portalmem::portal_define_query_list::call(
        &portal,
        None,
        query_string.as_str(),
        command_tag,
        stmt_list.as_slice(),
        portal_cplan,
    )?;
    if !saved {
        // ReleaseCachedPlan(cplan, NULL); cplan = NULL;
        let _ = plancache::ReleaseCachedPlan(cplan, ResourceOwner::NULL);
    }

    // portal->cursorOptions = plan->cursor_options; (CURSOR_OPT_PARALLEL_OK)
    // then, since neither SCROLL nor NO_SCROLL was requested, choose NO_SCROLL
    // for the multi-statement-or-FOR-UPDATE-safe default. ts_rewrite's single
    // read-only SELECT does not need a scrollable cursor; force NO_SCROLL (the
    // safe default — matches PerformCursorOpen when backward scan is not needed).
    {
        let mut p = portal.borrow_mut();
        p.cursorOptions = ::nodes::copy_query::CURSOR_OPT_PARALLEL_OK;
        if (p.cursorOptions & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL)) == 0 {
            p.cursorOptions |= CURSOR_OPT_NO_SCROLL;
        }
    }

    // If told to be read-only, check for read-only queries. This can't be done
    // earlier because we need to look at the finished, planned queries. (In
    // particular, not between GetCachedPlan and PortalDefineQuery, since an error
    // there would leak the plancache refcount.) ts_rewrite passes
    // read_only = true.
    //
    //   foreach(lc, stmt_list)
    //     if (!CommandIsReadOnly(pstmt))
    //       ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
    //               errmsg("%s is not allowed in a non-volatile function",
    //                      CreateCommandName((Node *) pstmt))));
    for pstmt in stmt_list.iter() {
        if !utility_seams::command_is_read_only::call(pstmt)? {
            let tag = utility_seams::planned_stmt_command_tag::call(pstmt)?;
            return Err(ereport(ERROR)
                .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "{} is not allowed in a non-volatile function",
                    command_tag_name(tag.0)
                ))
                .into_error());
        }
    }

    // if (read_only) snapshot = GetActiveSnapshot();
    let snapshot = snapmgr::get_active_snapshot::call()?;

    // PortalStart(portal, paramLI, 0, snapshot);  (paramLI == NULL)
    pquery::portal_start(&portal, None, 0, snapshot)?;

    Ok(portal)
}

/// `SPI_cursor_fetch(portal, true, 100)` + the per-row `SPI_getbinval` reads:
/// fetch up to [`FETCH_COUNT`] rows forward into a DestSPI receiver and return
/// the raw `(target, substitute)` tsquery datum bytes of each fetched row.
fn cursor_fetch_rows(portal: &Portal) -> PgResult<Vec<TsRewriteRow>> {
    // CreateDestReceiver(DestSPI).
    let receiver = create_spi_dest_receiver();

    // _SPI_cursor_operation: PortalRunFetch(portal, FETCH_FORWARD, count, dest).
    let _nfetched =
        pquery::portal_run_fetch(portal, FetchDirection::FETCH_FORWARD, FETCH_COUNT, receiver)?;

    // Read the raw bare-word / by-ref datums the receiver collected — the
    // SPI_getbinval(tuptab->vals[i], tupdesc, n, &isnull) material. Each tsquery
    // column is pass-by-reference, so its raw value is the verbatim varlena image
    // (RawCol.byref); SQL NULL is RawCol.isnull.
    let (_columns, raw_rows) = take_spi_raw_result(receiver);

    let mut rows: Vec<TsRewriteRow> = Vec::with_capacity(raw_rows.len());
    for raw in &raw_rows {
        // Column 1 (target) and column 2 (substitute). The caller already
        // verified natts == 2; a row with fewer columns cannot occur, but guard
        // against it by treating a missing column as SQL NULL.
        let col1 = raw_col_tsquery(raw.first());
        let col2 = raw_col_tsquery(raw.get(1));
        rows.push((col1, col2));
    }
    Ok(rows)
}

/// Extract a pass-by-reference `tsquery` datum's verbatim varlena bytes from a
/// collected [`crate::dest_spi::RawCol`]: `None` for SQL NULL (the C
/// `if (isnull) continue;` / `if (!isnull)` guards), else the
/// `DatumGetTSQuery(...)` varlena image the consumer re-parses with `QT2QTN`.
fn raw_col_tsquery(col: Option<&crate::dest_spi::RawCol>) -> Option<Vec<u8>> {
    match col {
        // C: qtex = DatumGetTSQuery(SPI_getbinval(..., 1, &isnull)). DatumGetTSQuery
        // (PG_DETOAST_DATUM) un-packs a short-headed stored tsquery to 4-byte form
        // before tsquery_rewrite_query's offset-4 `tsq_size` / offset-8 GETQUERY
        // reads.
        Some(c) if !c.isnull => c.byref.clone().map(detoast_short_spi_varlena),
        _ => None,
    }
}

/// Read the result tuple-descriptor shape the consumer's two-`tsquery`-column
/// type check inspects: `(natts, SPI_gettypeid(tupdesc, 1),
/// SPI_gettypeid(tupdesc, 2))`. For a `PORTAL_ONE_SELECT` portal the descriptor
/// is `portal->tupDesc`, set by `PortalStart`. A descriptor with fewer than two
/// attributes reports `InvalidOid` for the missing column type so the caller's
/// `!= TSQUERYOID` check fires the expected error.
fn portal_result_shape(portal: &Portal) -> (i32, types_core::Oid, types_core::Oid) {
    let p = portal.borrow();
    match p.tupDesc.as_ref() {
        Some(td) => {
            let natts = td.natts;
            let t1 = if natts >= 1 {
                td.attr(0).atttypid
            } else {
                types_core::InvalidOid
            };
            let t2 = if natts >= 2 {
                td.attr(1).atttypid
            } else {
                types_core::InvalidOid
            };
            (natts, t1, t2)
        }
        // No result descriptor — SPI_is_cursor_plan already rejected a
        // tuple-less plan, so this is unreachable for an opened cursor; report
        // natts == 0 so the caller raises the two-column error.
        None => (0, types_core::InvalidOid, types_core::InvalidOid),
    }
}

/// The "cannot open %s query as cursor" command-tag name
/// (`SPI_cursor_open_internal`): C uses `"SELECT INTO"` when the tag is
/// `CMDTAG_SELECT` (a SELECT that reached here must be `SELECT INTO`, since a
/// plain SELECT has a `resultDesc` and passes `SPI_is_cursor_plan`), else
/// `GetCommandTagName(commandTag)`.
fn command_tag_name(tag: CommandTag) -> &'static str {
    if tag == ::portal::CMDTAG_SELECT {
        "SELECT INTO"
    } else {
        cmdtag::get_command_tag_name(tag)
    }
}
