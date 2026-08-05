use core::cell::RefCell;

use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;

use crate::{
    current_exec_mcx, set_spi_result, with_current, _SPI_begin_call, _SPI_end_call,
    SPI_ERROR_ARGUMENT,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SpiPlanPtr(pub u64);

impl SpiPlanPtr {
    pub const NULL: SpiPlanPtr = SpiPlanPtr(0);

    pub fn is_null(self) -> bool {
        self.0 == 0
    }
}

// C _SPI_plan; sources index their statement within the source string so the
// oneshot path can re-locate its RawStmt at completion time.
pub(crate) struct SpiPlanState {
    pub sources: Vec<(plancache::CachedPlanSourceHandle, usize)>,
    pub oneshot: bool,
    pub saved: bool,
    pub cursor_options: i32,
    pub argtypes: Vec<Oid>,
}

thread_local! {
    static PLANS: RefCell<Vec<Option<SpiPlanState>>> = const { RefCell::new(Vec::new()) };
}

fn encode(idx: usize) -> SpiPlanPtr {
    SpiPlanPtr(idx as u64 + 1)
}

fn with_plan<R>(ptr: SpiPlanPtr, f: impl FnOnce(&mut SpiPlanState) -> R) -> Option<R> {
    if ptr.is_null() {
        return None;
    }
    PLANS.with(|p| p.borrow_mut().get_mut(ptr.0 as usize - 1).and_then(|s| s.as_mut()).map(f))
}

fn analyze_and_rewrite(
    qmcx: Mcx<'static>,
    raw: &types_nodes::rawnodes::RawStmt<'static>,
    src: &str,
    argtypes: &[Oid],
    query_env: types_portal::QueryEnvHandle,
) -> PgResult<PgVec<'static, Query<'static>>> {
    let query = analyze_seams::parse_analyze_fixedparams::call(
        qmcx,
        raw,
        src,
        argtypes,
        query_env,
    )?;
    if query.commandType == CmdType::CMD_UTILITY {
        let mut v = PgVec::new_in(qmcx);
        v.try_reserve_exact(1).map_err(|_| qmcx.oom(1))?;
        v.push(query);
        Ok(v)
    } else {
        rewrite_handler_seams::query_rewrite::call(qmcx, query)
    }
}

// One-shot completion (C's in-_SPI_execute_plan parse-analysis arm); analysis
// scribbles query-arena pointers into its input, so the plansource's retained
// raw tree is copied into the query arena (no re-lex: a second lex re-emits
// scanner warnings C doesn't).
pub(crate) fn complete_source(
    psrc: plancache::CachedPlanSourceHandle,
    stmt_index: usize,
    argtypes: &[Oid],
    cursor_options: i32,
) -> PgResult<()> {
    let src = plancache::CachedPlanQueryString(psrc);
    let qmcx = plancache::SourceQueryMcx(psrc);
    let raw = plancache::CachedPlanRawParseTreeCopy(qmcx, psrc)?.expect("created with a raw tree");
    let query_list = analyze_and_rewrite(qmcx, raw, src, argtypes, crate::current_query_env())
        .map_err(|e| spi_error_transpose(src, e))?;
    plancache::CompleteCachedPlan(psrc, query_list, argtypes, cursor_options, false)?;
    plancache::SetCachedPlanReanalyze(psrc, reanalyze_spi_source, stmt_index as i32);
    Ok(())
}

// C revalidates fixed-param SPI sources via pg_analyze_and_rewrite_fixedparams
// on the retained raw tree (plancache.c:810-814), under _SPI_error_callback;
// the transpose is the callback, `arg` (the statement index) is unused.
fn reanalyze_spi_source(
    _h: plancache::CachedPlanSourceHandle,
    qmcx: Mcx<'static>,
    raw: &'static types_nodes::rawnodes::RawStmt<'static>,
    query_string: &'static str,
    param_types: &'static [Oid],
    query_env: types_portal::QueryEnvHandle,
    _arg: i32,
) -> PgResult<PgVec<'static, Query<'static>>> {
    analyze_and_rewrite(qmcx, raw, query_string, param_types, query_env)
        .map_err(|e| spi_error_transpose(query_string, e))
}


// _SPI_error_callback (spi.c): a parse-phase syntax error position converts
// to an internal error against the SPI query text; otherwise the query rides
// the context stack.
pub(crate) fn spi_error_transpose(
    query: &str,
    mut e: Box<types_error::PgError>,
) -> Box<types_error::PgError> {
    match e.cursor_position {
        Some(pos) if pos > 0 => {
            e.cursor_position = None;
            e.internal_position = Some(pos);
            e.internal_query = Some(query.to_owned());
        }
        _ => {
            let line = format!("SQL statement \"{query}\"");
            e.context = Some(match e.context.take() {
                Some(c) => format!("{c}\n{line}"),
                None => line,
            });
        }
    }
    e
}

fn create_sources(
    src: &str,
    oneshot: bool,
    argtypes: &[Oid],
    cursor_options: i32,
) -> PgResult<Vec<(plancache::CachedPlanSourceHandle, usize)>> {
    let mcx = current_exec_mcx();
    let raw_list =
        parser_seams::raw_parser::call(mcx, src, parser_seams::RawParseMode::RAW_PARSE_DEFAULT)?;
    let mut sources = Vec::with_capacity(raw_list.len());
    let outcome = (|| -> PgResult<()> {
        for (i, raw) in raw_list.iter().enumerate() {
            let stmt = raw.stmt.expect("RawStmt has a stmt");
            let tag = utility_seams::create_command_tag::call(stmt);
            let psrc = plancache::CreateCachedPlan(Some(raw), src, tag)?;
            sources.push((psrc, i));
            if !oneshot {
                complete_source(psrc, i, argtypes, cursor_options)?;
            }
        }
        Ok(())
    })();
    if let Err(e) = outcome {
        // C leaks these into the SPI exec context; the registry needs the
        // explicit drop (extended_query precedent).
        for (psrc, _) in sources {
            plancache::DropCachedPlan(psrc);
        }
        return Err(e);
    }
    Ok(sources)
}

pub(crate) fn prepare_oneshot(src: &str, cursor_options: i32) -> PgResult<SpiPlanState> {
    prepare_oneshot_args(src, cursor_options, &[])
}

pub(crate) fn prepare_oneshot_args(
    src: &str,
    cursor_options: i32,
    argtypes: &[Oid],
) -> PgResult<SpiPlanState> {
    Ok(SpiPlanState {
        sources: create_sources(src, true, argtypes, cursor_options)
            .map_err(|e| spi_error_transpose(src, e))?,
        oneshot: true,
        saved: false,
        cursor_options,
        argtypes: argtypes.to_vec(),
    })
}

pub(crate) fn drop_state_sources(state: &mut SpiPlanState) {
    for (psrc, _) in state.sources.drain(..) {
        plancache::DropCachedPlan(psrc);
    }
}

pub fn SPI_prepare(src: &str, argtypes: &[Oid]) -> PgResult<SpiPlanPtr> {
    SPI_prepare_cursor(src, argtypes, 0)
}

pub fn SPI_prepare_cursor(src: &str, argtypes: &[Oid], cursor_options: i32) -> PgResult<SpiPlanPtr> {
    // C rejects only src == NULL; an empty string is a legal empty plan.
    let res = _SPI_begin_call(true);
    if res < 0 {
        set_spi_result(res);
        return Ok(SpiPlanPtr::NULL);
    }

    let state = SpiPlanState {
        sources: create_sources(src, false, argtypes, cursor_options)
            .map_err(|e| spi_error_transpose(src, e))?,
        oneshot: false,
        saved: false,
        cursor_options,
        argtypes: argtypes.to_vec(),
    };

    let ptr = PLANS.with(|p| {
        let mut plans = p.borrow_mut();
        match plans.iter().position(Option::is_none) {
            Some(i) => {
                plans[i] = Some(state);
                encode(i)
            }
            None => {
                plans.push(Some(state));
                encode(plans.len() - 1)
            }
        }
    });
    // C parents the unsaved plan under procCxt; the connection's plan list is
    // that parentage (freed at SPI_finish unless kept).
    with_current(|conn| conn.plans.push(ptr));

    _SPI_end_call(true);
    set_spi_result(0);
    Ok(ptr)
}

// SPI_prepare_extended's plpgsql leg (spi.c): raw-parse under the plpgsql
// parse mode, analyze with the flattened var-resolution hooks. Plan argtypes
// cover every hook-referenceable datum slot (paramid = dno+1; InvalidOid
// holes for datums a plan never references).
pub fn SPI_prepare_plpgsql(
    src: &str,
    parse_mode: parser_seams::RawParseMode,
    hooks: &parser_small1::PlpgsqlHookState<'_>,
    cursor_options: i32,
) -> PgResult<SpiPlanPtr> {
    let res = _SPI_begin_call(true);
    if res < 0 {
        set_spi_result(res);
        return Ok(SpiPlanPtr::NULL);
    }

    let mut argtypes: Vec<Oid> = Vec::with_capacity(hooks.params_by_dno.len());
    for slot in hooks.params_by_dno {
        argtypes.push(match slot {
            Some((t, _, _)) => *t,
            None => InvalidOid,
        });
    }

    let outcome = (|| -> PgResult<Vec<(plancache::CachedPlanSourceHandle, usize)>> {
        let mcx = current_exec_mcx();
        let raw_list = parser_seams::raw_parser::call(mcx, src, parse_mode)?;
        let mut sources = Vec::with_capacity(raw_list.len());
        let inner = (|| -> PgResult<()> {
            for (i, raw) in raw_list.iter().enumerate() {
                let stmt = raw.stmt.expect("RawStmt has a stmt");
                let tag = utility_seams::create_command_tag::call(stmt);
                let psrc = plancache::CreateCachedPlan(Some(raw), src, tag)?;
                sources.push((psrc, i));
                let qmcx = plancache::SourceQueryMcx(psrc);
                let qsrc = plancache::CachedPlanQueryString(psrc);
                // Retained-tree copy, not a re-parse: a second lex re-emits
                // scanner warnings C doesn't (plpgsql strtest).
                let qraw = plancache::CachedPlanRawParseTreeCopy(qmcx, psrc)?
                    .expect("created with a raw tree");
                let query = analyze_seams::parse_analyze_plpgsql::call(
                    qmcx,
                    qraw,
                    qsrc,
                    hooks,
                    crate::current_query_env(),
                )?;
                let query_list = if query.commandType == CmdType::CMD_UTILITY {
                    let mut v = PgVec::new_in(qmcx);
                    v.try_reserve_exact(1).map_err(|_| qmcx.oom(1))?;
                    v.push(query);
                    v
                } else {
                    rewrite_handler_seams::query_rewrite::call(qmcx, query)?
                };
                plancache::CompleteCachedPlan(psrc, query_list, &argtypes, cursor_options, false)?;
            }
            Ok(())
        })();
        if let Err(e) = inner {
            for (psrc, _) in sources {
                plancache::DropCachedPlan(psrc);
            }
            return Err(e);
        }
        Ok(sources)
    })();
    let sources = outcome?;

    let state = SpiPlanState {
        sources,
        oneshot: false,
        saved: false,
        cursor_options,
        argtypes,
    };

    let ptr = PLANS.with(|p| {
        let mut plans = p.borrow_mut();
        match plans.iter().position(Option::is_none) {
            Some(i) => {
                plans[i] = Some(state);
                encode(i)
            }
            None => {
                plans.push(Some(state));
                encode(plans.len() - 1)
            }
        }
    });
    with_current(|conn| conn.plans.push(ptr));

    _SPI_end_call(true);
    set_spi_result(0);
    Ok(ptr)
}

/// SPI_plan_get_cached_plan's single-source precondition (SPI_is_cursor_plan
/// shape): the one plansource of a single-statement plan.
pub fn SPI_plan_single_source(ptr: SpiPlanPtr) -> Option<(plancache::CachedPlanSourceHandle, i32)> {
    single_source(ptr)
}

pub(crate) fn single_source(ptr: SpiPlanPtr) -> Option<(plancache::CachedPlanSourceHandle, i32)> {
    with_plan(ptr, |p| {
        if p.sources.len() == 1 {
            Some((p.sources[0].0, p.cursor_options))
        } else {
            None
        }
    })
    .flatten()
}

pub(crate) fn state_snapshot(ptr: SpiPlanPtr) -> Option<SpiPlanState> {
    with_plan(ptr, |p| SpiPlanState {
        sources: p.sources.clone(),
        oneshot: p.oneshot,
        saved: p.saved,
        cursor_options: p.cursor_options,
        argtypes: p.argtypes.clone(),
    })
}

fn unlink_from_connections(ptr: SpiPlanPtr) {
    crate::SPI_STACK.with(|s| {
        for conn in s.borrow_mut().iter_mut() {
            conn.plans.retain(|&p| p != ptr);
        }
    });
}

pub fn SPI_keepplan(ptr: SpiPlanPtr) -> i32 {
    let ok = with_plan(ptr, |p| {
        if p.saved || p.oneshot {
            return None;
        }
        p.saved = true;
        Some(p.sources.clone())
    });
    match ok {
        Some(Some(sources)) => {
            for (psrc, _) in sources {
                if plancache::SaveCachedPlan(psrc).is_err() {
                    panic!("SPI_keepplan: SaveCachedPlan failed");
                }
            }
            unlink_from_connections(ptr);
            0
        }
        _ => SPI_ERROR_ARGUMENT,
    }
}

pub fn SPI_freeplan(ptr: SpiPlanPtr) -> i32 {
    if ptr.is_null() {
        return SPI_ERROR_ARGUMENT;
    }
    let taken = PLANS.with(|p| {
        p.borrow_mut().get_mut(ptr.0 as usize - 1).and_then(Option::take)
    });
    match taken {
        Some(mut state) => {
            drop_state_sources(&mut state);
            unlink_from_connections(ptr);
            0
        }
        None => SPI_ERROR_ARGUMENT,
    }
}

pub(crate) fn free_connection_plans(plans: &[SpiPlanPtr]) {
    for &ptr in plans {
        let taken = PLANS.with(|p| {
            p.borrow_mut().get_mut(ptr.0 as usize - 1).and_then(Option::take)
        });
        if let Some(mut state) = taken {
            debug_assert!(!state.saved, "saved plan still on a connection plan list");
            drop_state_sources(&mut state);
        }
    }
}

/// Command tags of the plan's sources (plpgsql mod_stmt detection).
pub fn SPI_plan_command_tags(ptr: SpiPlanPtr) -> Vec<types_core::CommandTag> {
    with_plan(ptr, |p| {
        p.sources.iter().map(|&(s, _)| plancache::CachedPlanCommandTag(s)).collect()
    })
    .unwrap_or_default()
}

pub fn SPI_getargcount(ptr: SpiPlanPtr) -> i32 {
    match with_plan(ptr, |p| p.argtypes.len() as i32) {
        Some(n) => n,
        None => {
            set_spi_result(SPI_ERROR_ARGUMENT);
            -1
        }
    }
}

pub fn SPI_getargtypeid(ptr: SpiPlanPtr, arg_index: i32) -> Oid {
    let got = with_plan(ptr, |p| p.argtypes.get(arg_index as usize).copied());
    match got {
        Some(Some(t)) => t,
        _ => {
            set_spi_result(SPI_ERROR_ARGUMENT);
            InvalidOid
        }
    }
}

pub fn SPI_plan_is_valid(ptr: SpiPlanPtr) -> bool {
    with_plan(ptr, |p| p.sources.iter().all(|&(s, _)| plancache::CachedPlanIsValid(s)))
        .unwrap_or(false)
}

pub(crate) fn debug_live_plans() -> usize {
    PLANS.with(|p| p.borrow().iter().flatten().count())
}
