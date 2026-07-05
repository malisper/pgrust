// pl_exec.c. Statement set: block (incl. EXCEPTION sections), assign,
// if, loop/while/fori/fors, exit/continue, return, raise, assert, execsql
// (incl. INTO [STRICT]), dynexecute, perform, getdiag(row_count).
// Expressions ride SPI
// plans (saved; plancache invalidation is loud per repo discipline) with the
// simple-expression fast path over execexpr.
//
// Documented divergences from C:
// - Simple-expr ExprStates and cast ExprStates are cached per invocation
//   (estate), not per transaction: fast execexpr bakes the param-slot
//   address at compile, so a state cannot outlive its estate's param
//   buffer. Loops (the hot case) reuse within the invocation.
// - Old var values are not freed on reassignment; they live until the
//   invocation's datum context is dropped at function exit (C pfrees).
//
// Std collections justified as in ast.rs (invocation-lifetime bookkeeping,
// never per row on a steady path).
use std::collections::HashMap;

type FxHashMap<K, V> = HashMap<K, V, rustc_hash::FxBuildHasher>;

use datum::Datum;
use mcx::{Mcx, MemoryContext};
use spi::{
    SPI_cursor_close, SPI_cursor_fetch, SPI_cursor_open, SpiCursor, SpiPlanPtr, TuptabHandle,
};
use types_core::{Oid, OidIsValid};
use types_error::{PgError, PgResult, SqlState, ERROR};
use types_portal::params::ParamExternData;

use crate::ast::*;
use crate::errcodes::EXCEPTION_LABEL_MAP;

pub const RC_OK: i32 = 0;
pub const RC_EXIT: i32 = 1;
pub const RC_RETURN: i32 = 2;
pub const RC_CONTINUE: i32 = 3;

const BOOLOID: Oid = 16;
const INT8OID: Oid = 20;
const TEXTOID: Oid = 25;
const UNKNOWNOID: Oid = 705;
const RECORDOID: Oid = 2249;
const TYPTYPE_DOMAIN: i8 = b'd' as i8;

const CURSOR_OPT_PARALLEL_OK: i32 = 0x0800;

pub(crate) struct Ctx(*mut MemoryContext);

impl Ctx {
    pub fn new(name: &'static str) -> Ctx {
        Ctx(Box::into_raw(Box::new(MemoryContext::new_bump(name))))
    }
    pub fn mcx(&self) -> Mcx<'static> {
        // SAFETY: reclaimed only in Drop; handles do not outlive the estate.
        unsafe { (*self.0).mcx() }
    }
    pub fn reset(&self) {
        // SAFETY: as above; no live borrows at reset points (allocations are
        // raw datums, invalidated by contract like C's context reset).
        unsafe { (*self.0).reset() }
    }
}

impl Drop for Ctx {
    fn drop(&mut self) {
        // SAFETY: Box::into_raw provenance.
        drop(unsafe { Box::from_raw(self.0) });
    }
}

#[derive(Clone)]
pub struct RecDesc {
    pub names: Vec<String>,
    pub types: Vec<Oid>,
    pub typmods: Vec<i32>,
    pub typlens: Vec<i16>,
    pub typbyvals: Vec<bool>,
    pub dropped: Vec<bool>,
}

impl RecDesc {
    pub fn from_tupdesc(td: &types_tuple::TupleDescData<'_>) -> RecDesc {
        let natts = td.attrs.len();
        let mut d = RecDesc {
            names: Vec::with_capacity(natts),
            types: Vec::with_capacity(natts),
            typmods: Vec::with_capacity(natts),
            typlens: Vec::with_capacity(natts),
            typbyvals: Vec::with_capacity(natts),
            dropped: Vec::with_capacity(natts),
        };
        for a in td.attrs.iter() {
            d.names
                .push(String::from_utf8_lossy(a.attname.name_str()).to_ascii_lowercase());
            d.types.push(a.atttypid);
            d.typmods.push(a.atttypmod);
            d.typlens.push(a.attlen);
            d.typbyvals.push(a.attbyval);
            d.dropped.push(a.attisdropped);
        }
        d
    }
}

// A deconstructed expanded record (values always deconstructed; src_desc
// keeps the physical tupdesc so the record can re-materialize as a
// composite Datum, dropped columns included).
#[derive(Clone)]
pub struct RecValue {
    pub desc: RecDesc,
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
    pub src_desc: Option<std::rc::Rc<types_tuple::TupleDescData<'static>>>,
    /// C ExpandedRecordIsEmpty: shape known, no row stored — reads as SQL
    /// NULL as a whole, fields read NULL, assignment makes it non-empty.
    pub empty: bool,
}

pub enum DatumVal {
    Var { value: Datum, isnull: bool },
    Rec(Option<RecValue>),
    None,
}

struct PlanEntry {
    plan: SpiPlanPtr,
    paramnos: Vec<Dno>,
    argtypes: Vec<Oid>,
}

std::thread_local! {
    // expr_id -> saved SPI plan (C stores expr->plan in the function AST;
    // the side table keeps the shared AST immutable). Entries die with the
    // compiled function (free_function_plans).
    static EXPR_PLANS: core::cell::RefCell<FxHashMap<u32, PlanEntry>> =
        core::cell::RefCell::new(FxHashMap::default());
    // expr_id -> CALL OUT-arg row varnos (C stmt->target, cached in fn_cxt).
    static CALL_TARGETS: core::cell::RefCell<FxHashMap<u32, Vec<Dno>>> =
        core::cell::RefCell::new(FxHashMap::default());
}

pub fn free_function_plans(expr_ids: &[u32]) {
    EXPR_PLANS.with(|t| {
        let mut t = t.borrow_mut();
        for id in expr_ids {
            if let Some(e) = t.remove(id) {
                spi::SPI_freeplan(e.plan);
            }
        }
    });
    CALL_TARGETS.with(|t| {
        let mut t = t.borrow_mut();
        for id in expr_ids {
            t.remove(id);
        }
    });
}

struct SimpleExpr {
    state: mcx::PgBox<'static, execexpr::ExprState<'static>>,
    cplan: types_portal::CachedPlanHandle,
    psrc: plancache::CachedPlanSourceHandle,
    rettype: Oid,
    rettypmod: i32,
}

struct CastEntry {
    // None = no-op relabeling.
    state: Option<mcx::PgBox<'static, execexpr::ExprState<'static>>>,
    // Stable slot the compiled Param step points into.
    param: Box<[ParamExternData; 1]>,
    // PlanCacheInvalCounter at build; any bump forces a rebuild (coarse
    // stand-in for C's per-CachedExpression is_valid, pl_exec.c:7982).
    inval_gen: u64,
}

#[derive(Clone, Copy)]
pub struct RsiSnapshot {
    pub allowed_modes: u32,
    pub expected_desc: Option<core::ptr::NonNull<core::ffi::c_void>>,
}

pub struct Estate<'a> {
    pub func: &'a PlFunction,
    pub datums: Vec<DatumVal>,
    pub retval: Datum,
    pub retisnull: bool,
    pub rettype: Oid,
    /// RETURN of a record variable (trigger tuple return protocol).
    pub ret_rec: Option<RecValue>,
    pub rsi: Option<RsiSnapshot>,
    pub tuple_store: Option<tuplestore::Tuplestore>,
    tuple_store_desc: Option<types_tuple::TupleDescData<'static>>,
    pub cur_error: Option<Box<PgError>>,
    pub eval_processed: u64,
    eval_tuptable: Option<TuptabHandle>,
    pub exitlabel: Option<String>,
    pub readonly_func: bool,
    pub atomic: bool,
    // Stable param image for compiled expressions (address baked into
    // cached ExprStates; the Box never moves while cached states live).
    param_buf: Box<[ParamExternData]>,
    simple_cache: FxHashMap<u32, Option<SimpleExpr>>,
    cast_cache: FxHashMap<(Oid, i32, Oid, i32), CastEntry>,
    // Invocation-lifetime var values (C's "procedure" context).
    datum_ctx: Ctx,
    // Per-evaluation scratch (C's eval_mcontext); reset by exec_eval_cleanup.
    eval_ctx: Ctx,
    // Execution-copy datatype changes (C mutates its per-estate datum copies;
    // the shared AST here is immutable). Consulted wherever a Var's declared
    // type feeds plan/param typing.
    var_type_overrides: FxHashMap<Dno, PlType>,
    pub frame: std::rc::Rc<FrameShared>,
}

// C's plpgsql_exec_error_callback arg, shared with the thread-local context
// stack so GET DIAGNOSTICS PG_CONTEXT can render every live frame without
// aliasing the estates.
pub struct FrameShared {
    pub sig: String,
    pub stmt: core::cell::Cell<Option<(i32, &'static str)>>,
    pub text: core::cell::Cell<Option<&'static str>>,
}

enum CtxFrame {
    Pl(std::rc::Rc<FrameShared>),
    Spi(String),
}

std::thread_local! {
    static CONTEXT_FRAMES: core::cell::RefCell<Vec<CtxFrame>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

pub struct FrameGuard(u64);

impl FrameGuard {
    pub fn push_pl(estate: &Estate<'_>) -> FrameGuard {
        let f = estate.frame.clone();
        // C's errfinish walks error_context_stack for every elevel: notices
        // and warnings emitted while this frame is live carry its context
        // line on the wire (the ERROR path attaches on propagation instead).
        let cb = {
            let f = f.clone();
            elog::push_emit_context_callback(Box::new(move |e| {
                e.add_context_line(frame_context_line_of(&f));
            }))
        };
        CONTEXT_FRAMES.with(|s| s.borrow_mut().push(CtxFrame::Pl(f)));
        FrameGuard(cb)
    }

    fn push_spi(query: &str, mode: parser_seams::RawParseMode) -> FrameGuard {
        let line = spi_context_line(query, mode);
        let cb = {
            let line = line.clone();
            elog::push_emit_context_callback(Box::new(move |e| {
                e.add_context_line(line.clone());
            }))
        };
        CONTEXT_FRAMES.with(|s| s.borrow_mut().push(CtxFrame::Spi(line)));
        FrameGuard(cb)
    }
}

impl Drop for FrameGuard {
    fn drop(&mut self) {
        elog::pop_emit_context_callback(self.0);
        CONTEXT_FRAMES.with(|s| {
            s.borrow_mut().pop();
        });
    }
}

// GetErrorContextStack (elog.c): innermost frame first.
fn get_error_context_stack() -> String {
    CONTEXT_FRAMES.with(|s| {
        let s = s.borrow();
        let mut out = String::new();
        for frame in s.iter().rev() {
            if !out.is_empty() {
                out.push('\n');
            }
            match frame {
                CtxFrame::Pl(f) => out.push_str(&frame_context_line_of(f)),
                CtxFrame::Spi(line) => out.push_str(line),
            }
        }
        out
    })
}

// _SPI_error_callback (spi.c): position becomes internal query/position;
// otherwise a parse-mode-shaped context line.
#[cold]
fn spi_ctx_err(
    mut e: Box<PgError>,
    query: &str,
    mode: parser_seams::RawParseMode,
) -> Box<PgError> {
    if let Some(p) = e.cursor_position.filter(|&p| p > 0) {
        e.cursor_position = None;
        e.internal_position = Some(p);
        e.internal_query = Some(query.to_string());
        return e;
    }
    // The spi crate's _SPI_error_callback port may already have handled this
    // query (transpose or context line); C runs the callback once per level.
    if e.internal_query.as_deref() == Some(query) {
        return e;
    }
    let line = spi_context_line(query, mode);
    if e.context.as_deref().is_some_and(|c| c.contains(line.as_str())) {
        return e;
    }
    match e.context.take() {
        Some(prev) => e.context = Some(format!("{prev}\n{line}")),
        None => e.context = Some(line),
    }
    e
}

fn spi_context_line(query: &str, mode: parser_seams::RawParseMode) -> String {
    use parser_seams::RawParseMode as M;
    match mode {
        M::RAW_PARSE_PLPGSQL_EXPR => format!("PL/pgSQL expression \"{query}\""),
        M::RAW_PARSE_PLPGSQL_ASSIGN1
        | M::RAW_PARSE_PLPGSQL_ASSIGN2
        | M::RAW_PARSE_PLPGSQL_ASSIGN3 => format!("PL/pgSQL assignment \"{query}\""),
        _ => format!("SQL statement \"{query}\""),
    }
}

#[cold]
pub(crate) fn exec_err(code: SqlState, msg: String) -> Box<PgError> {
    Box::new(elog::ereport(ERROR).errcode(code).errmsg(msg).into_error())
}

impl Drop for Estate<'_> {
    fn drop(&mut self) {
        for (_, e) in self.simple_cache.drain() {
            if let Some(se) = e {
                plancache::ReleaseCachedPlan(se.cplan);
            }
        }
    }
}

impl<'a> Estate<'a> {
    pub fn new(func: &'a PlFunction, readonly_func: bool, atomic: bool) -> Estate<'a> {
        let mut datums = Vec::with_capacity(func.datums.len());
        let mut param_buf = Vec::with_capacity(func.datums.len());
        for d in &func.datums {
            datums.push(match d {
                PlDatum::Var(_) => DatumVal::Var { value: Datum::null(), isnull: true },
                PlDatum::Rec(_) => DatumVal::Rec(None),
                _ => DatumVal::None,
            });
            param_buf.push(ParamExternData {
                value: Datum::null(),
                isnull: true,
                pflags: 0,
                ptype: types_core::InvalidOid,
            });
        }
        Estate {
            func,
            datums,
            retval: Datum::null(),
            retisnull: true,
            rettype: types_core::InvalidOid,
            ret_rec: None,
            rsi: None,
            tuple_store: None,
            tuple_store_desc: None,
            cur_error: None,
            eval_processed: 0,
            eval_tuptable: None,
            exitlabel: None,
            readonly_func,
            atomic,
            param_buf: param_buf.into_boxed_slice(),
            simple_cache: FxHashMap::default(),
            cast_cache: FxHashMap::default(),
            datum_ctx: Ctx::new("PLpgSQL per-invocation values"),
            eval_ctx: Ctx::new("PLpgSQL eval scratch"),
            var_type_overrides: FxHashMap::default(),
            frame: std::rc::Rc::new(FrameShared {
                sig: func.fn_signature.clone(),
                stmt: core::cell::Cell::new(None),
                text: core::cell::Cell::new(None),
            }),
        }
    }

    fn var_type(&self, dno: Dno) -> &PlType {
        if let Some(t) = self.var_type_overrides.get(&dno) {
            return t;
        }
        match &self.func.datums[dno as usize] {
            PlDatum::Var(v) => &v.datatype,
            _ => panic!("plpgsql: datum {dno} is not a Var"),
        }
    }

    pub fn set_var(&mut self, dno: Dno, value: Datum, isnull: bool) {
        match &mut self.datums[dno as usize] {
            DatumVal::Var { value: v, isnull: n } => {
                *v = value;
                *n = isnull;
            }
            _ => panic!("plpgsql: assign to non-Var datum {dno}"),
        }
    }

    pub fn get_var(&self, dno: Dno) -> (Datum, bool) {
        match &self.datums[dno as usize] {
            DatumVal::Var { value, isnull } => (*value, *isnull),
            _ => panic!("plpgsql: read of non-Var datum {dno}"),
        }
    }

    fn exec_set_found(&mut self, state: bool) {
        let dno = self.func.found_varno;
        self.set_var(dno, Datum::from_bool(state), false);
    }

    // exec_eval_cleanup.
    fn exec_eval_cleanup(&mut self) {
        if let Some(t) = self.eval_tuptable.take() {
            let _ = spi::SPI_freetuptable(t);
        }
        self.eval_ctx.reset();
    }

    pub(crate) fn datum_mcx(&self) -> Mcx<'static> {
        self.datum_ctx.mcx()
    }

    // datumCopy into the invocation context (by-ref survives statements).
    pub(crate) fn copy_to_datum_ctx(&self, value: Datum, isnull: bool, typlen: i16, typbyval: bool) -> PgResult<Datum> {
        if isnull || typbyval {
            return Ok(value);
        }
        // SAFETY: value is a live by-ref datum of typlen discipline.
        unsafe { execexpr::agg_datum_copy(self.datum_ctx.mcx(), value, typlen) }
    }

    // ------------------------------------------------------------------
    // Expression evaluation
    // ------------------------------------------------------------------

    fn ensure_plan(&mut self, expr: &PlExpr, cursor_options: i32) -> PgResult<()> {
        let cached = EXPR_PLANS.with(|t| {
            t.borrow()
                .get(&expr.expr_id)
                .map(|e| spi::SPI_plan_single_source(e.plan).map(|(psrc, _)| psrc))
        });
        // Catalog-probing revalidation runs outside the EXPR_PLANS borrow.
        let stale = match cached {
            None => None,
            Some(None) => Some(false),
            Some(Some(psrc)) => Some(plancache::CachedPlanSourceRequiresReanalysis(psrc)?),
        };
        match stale {
            Some(false) => return Ok(()),
            // C's RevalidateCachedQuery re-analyzes the retained raw tree in
            // place; this port re-prepares from the retained query text (same
            // text, fresh analysis) whenever the source would need re-analysis
            // (invalidation, search_path mismatch, RLS environment change).
            Some(true) => {
                // Release this estate's simple-expr pin before the source is
                // dropped (handles are generation-checked; a stale probe
                // panics). Other in-flight estates of the same function would
                // still hold stale pins — loud, not silent, if ever hit.
                if let Some(Some(se)) = self.simple_cache.remove(&expr.expr_id) {
                    plancache::ReleaseCachedPlan(se.cplan);
                }
                if let Some(e) =
                    EXPR_PLANS.with(|t| t.borrow_mut().remove(&expr.expr_id))
                {
                    let _ = spi::SPI_freeplan(e.plan);
                }
            }
            None => {}
        }
        let (hooks_names, params_by_dno, recs, valueless) = self
            .build_hook_tables(expr)
            .map_err(|e| spi_ctx_err(e, &expr.query, expr.parse_mode))?;
        let used = core::cell::RefCell::new(Vec::new());
        let name_entries: Vec<parser_small1::PlpgsqlNameEntry> = hooks_names
            .iter()
            .map(|(key, dno, t, m, c)| parser_small1::PlpgsqlNameEntry {
                key,
                dno: *dno,
                typoid: *t,
                typmod: *m,
                collation: *c,
            })
            .collect();
        let rec_names: Vec<&str> = recs.iter().map(|s| s.as_str()).collect();
        let valueless_names: Vec<&str> = valueless.iter().map(|s| s.as_str()).collect();
        let hooks = parser_small1::PlpgsqlHookState {
            names: &name_entries,
            params_by_dno: &params_by_dno,
            arg_dnos: &self.func.fn_argvarnos,
            recs: &rec_names,
            valueless_recs: &valueless_names,
            resolve_option: match self.func.resolve_option {
                crate::comp::PLPGSQL_RESOLVE_VARIABLE => {
                    parser_small1::PlpgsqlResolveOption::Variable
                }
                crate::comp::PLPGSQL_RESOLVE_COLUMN => parser_small1::PlpgsqlResolveOption::Column,
                _ => parser_small1::PlpgsqlResolveOption::Error,
            },
            used: &used,
        };
        let plan = spi::SPI_prepare_plpgsql(&expr.query, expr.parse_mode, &hooks, cursor_options)
            .map_err(|e| spi_ctx_err(e, &expr.query, expr.parse_mode))?;
        if spi::SPI_keepplan(plan) != 0 {
            panic!("plpgsql exec_prepare_plan: SPI_keepplan failed");
        }
        let mut paramnos = used.into_inner();
        paramnos.sort_unstable();
        let argtypes: Vec<Oid> = params_by_dno
            .iter()
            .map(|s| s.map(|(t, _, _)| t).unwrap_or(types_core::InvalidOid))
            .collect();
        EXPR_PLANS.with(|t| {
            t.borrow_mut().insert(expr.expr_id, PlanEntry { plan, paramnos, argtypes })
        });
        Ok(())
    }

    // plpgsql_parser_setup's resolution tables, flattened from the expr's
    // namespace chain (most-local binding wins; label-qualified aliases per
    // level; rec fields resolve against the rec's CURRENT tupdesc).
    #[allow(clippy::type_complexity)]
    fn build_hook_tables(
        &mut self,
        expr: &PlExpr,
    ) -> PgResult<(
        Vec<(String, Dno, Oid, i32, Oid)>,
        Vec<Option<(Oid, i32, Oid)>>,
        Vec<String>,
        Vec<String>,
    )> {
        let func = self.func;
        let mut names: Vec<(String, Dno, Oid, i32, Oid)> = Vec::new();
        let mut recs: Vec<String> = Vec::new();
        let mut valueless: Vec<String> = Vec::new();
        let mut pending_valueless: Vec<String> = Vec::new();
        let have = |names: &Vec<(String, Dno, Oid, i32, Oid)>, k: &str| {
            names.iter().any(|(n, ..)| n == k)
        };

        let mut params_by_dno: Vec<Option<(Oid, i32, Oid)>> = Vec::new();
        for d in &func.datums {
            params_by_dno.push(match d {
                PlDatum::Var(v) => {
                    let t = self.var_type(v.dno);
                    Some((t.typoid, t.atttypmod, t.collation))
                }
                PlDatum::RecField(f) => self.recfield_type(f)?,
                PlDatum::Rec(r) => Some((self.rec_param_type(r.dno), -1, types_core::InvalidOid)),
                _ => None,
            });
        }

        let mut cur = expr.ns;
        let mut pending: Vec<(String, Dno, Oid, i32, Oid)> = Vec::new();
        let mut pending_recs: Vec<String> = Vec::new();
        while cur >= 0 {
            let item = &func.ns[cur as usize];
            match item.itemtype {
                NsType::Var => {
                    if let PlDatum::Var(v) = &func.datums[item.itemno as usize] {
                        let key = item.name.to_ascii_lowercase();
                        let t = self.var_type(v.dno);
                        let info = (key.clone(), v.dno, t.typoid, t.atttypmod, t.collation);
                        if !have(&names, &key) {
                            names.push(info.clone());
                        }
                        pending.push(info);
                    }
                }
                NsType::Rec => {
                    let recname = item.name.to_ascii_lowercase();
                    let recno = item.itemno;
                    let marker = (
                        recname.clone(),
                        recno,
                        self.rec_param_type(recno),
                        -1,
                        types_core::InvalidOid,
                    );
                    if !have(&names, &recname) {
                        names.push(marker.clone());
                    }
                    pending.push(marker);
                    for d in &func.datums {
                        if let PlDatum::RecField(f) = d {
                            if f.recparentno == recno {
                                if let Some((t, m, c)) = self.recfield_type(f)? {
                                    let key = format!(
                                        "{recname}.{}",
                                        f.fieldname.to_ascii_lowercase()
                                    );
                                    let info = (key.clone(), f.dno, t, m, c);
                                    if !have(&names, &key) {
                                        names.push(info.clone());
                                    }
                                    pending.push(info);
                                }
                            }
                        }
                    }
                    if !recs.contains(&recname) {
                        recs.push(recname.clone());
                    }
                    if matches!(&self.datums[recno as usize], DatumVal::Rec(None))
                        && self.rec_meta(recno).rectypeid == RECORDOID
                    {
                        if !valueless.contains(&recname) {
                            valueless.push(recname.clone());
                        }
                        pending_valueless.push(recname.clone());
                    }
                    pending_recs.push(recname);
                }
                NsType::Row => {}
                NsType::Label => {
                    if !item.name.is_empty() {
                        let label = item.name.to_ascii_lowercase();
                        for (k, dno, t, m, c) in pending.drain(..) {
                            let lk = format!("{label}.{k}");
                            if !have(&names, &lk) {
                                names.push((lk, dno, t, m, c));
                            }
                        }
                        for r in pending_recs.drain(..) {
                            let lr = format!("{label}.{r}");
                            if !recs.contains(&lr) {
                                recs.push(lr);
                            }
                        }
                        for r in pending_valueless.drain(..) {
                            let lr = format!("{label}.{r}");
                            if !valueless.contains(&lr) {
                                valueless.push(lr);
                            }
                        }
                    } else {
                        pending.clear();
                        pending_recs.clear();
                        pending_valueless.clear();
                    }
                }
            }
            cur = item.prev;
        }
        Ok((names, params_by_dno, recs, valueless))
    }

    // exec_get_datum_type_info REC arm: the declared rectypeid, typmod -1.
    fn rec_param_type(&self, recno: Dno) -> Oid {
        self.rec_meta(recno).rectypeid
    }

    // exec_eval_datum REC arm: materialize the record as a composite Datum in
    // the eval scratch (valueless/empty record is a plain NULL).
    fn rec_as_composite_datum(&mut self, recno: Dno) -> PgResult<(Datum, bool)> {
        let rectypeid = self.rec_meta(recno).rectypeid;
        let DatumVal::Rec(Some(rv)) = &self.datums[recno as usize] else {
            return Ok((Datum::null(), true));
        };
        if rv.empty {
            return Ok((Datum::null(), true));
        }
        let src = rv
            .src_desc
            .clone()
            .expect("RecValue carries its source tupdesc");
        let values = rv.values.clone();
        let nulls = rv.nulls.clone();
        let mcx = self.eval_ctx.mcx();
        let mut td = tupdesc::CreateTupleDescCopy(mcx, &src)?;
        if rectypeid != RECORDOID {
            td.tdtypeid = rectypeid;
            td.tdtypmod = -1;
        } else {
            td.tdtypeid = RECORDOID;
            if td.tdtypmod < 0 {
                typcache::assign_record_type_typmod(&mut td)?;
            }
        }
        let tup = heaptuple::heap_form_tuple(mcx, &td, &values, &nulls)?;
        let img = tup.header_ptr();
        core::mem::forget(tup);
        Ok((Datum::from_usize(img as usize), false))
    }

    fn rec_meta(&self, recno: Dno) -> &PlRec {
        match &self.func.datums[recno as usize] {
            PlDatum::Rec(r) => r,
            _ => panic!("plpgsql: datum {recno} is not a Rec"),
        }
    }

    // instantiate_empty_record_variable (pl_exec.c:7810).
    pub(crate) fn instantiate_empty_rec(&mut self, recno: Dno) -> PgResult<()> {
        let rec = self.rec_meta(recno);
        if rec.rectypeid == RECORDOID {
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!("record \"{}\" is not assigned yet", rec.refname))
                    .errdetail(
                        "The tuple structure of a not-yet-assigned record is indeterminate.",
                    )
                    .into_error(),
            ));
        }
        let rectypeid = rec.rectypeid;
        let td = typcache::lookup_rowtype_tupdesc_copy(self.datum_ctx.mcx(), rectypeid, -1)?;
        let desc = RecDesc::from_tupdesc(&td);
        let n = desc.types.len();
        self.datums[recno as usize] = DatumVal::Rec(Some(RecValue {
            desc,
            values: vec![Datum::null(); n],
            nulls: vec![true; n],
            src_desc: Some(std::rc::Rc::new(td)),
            empty: true,
        }));
        Ok(())
    }

    // exec_get_datum_type-ish for RECFIELD: type from the rec's live value;
    // a valueless named-composite rec is instantiated first (C
    // exec_get_datum_type_info). Valueless RECORD recs stay None — the 55000
    // fires only if the SQL expression actually references a field
    // (resolve_column_ref's valueless_recs arm).
    fn recfield_type(&mut self, f: &PlRecField) -> PgResult<Option<(Oid, i32, Oid)>> {
        if matches!(&self.datums[f.recparentno as usize], DatumVal::Rec(None))
            && self.rec_meta(f.recparentno).rectypeid != RECORDOID
        {
            self.instantiate_empty_rec(f.recparentno)?;
        }
        if let DatumVal::Rec(Some(rv)) = &self.datums[f.recparentno as usize] {
            let want = f.fieldname.to_ascii_lowercase();
            for (i, n) in rv.desc.names.iter().enumerate() {
                if !rv.desc.dropped[i] && *n == want {
                    let t = rv.desc.types[i];
                    let coll = lsyscache::typ::get_typcollation(t)?;
                    return Ok(Some((t, rv.desc.typmods[i], coll)));
                }
            }
        }
        Ok(None)
    }

    // setup_param_list: write current datum values for the plan's paramnos
    // into the stable buffer, returning (values, nulls) views for SPI.
    fn setup_params(&mut self, entry_paramnos: &[Dno], argtypes: &[Oid]) -> PgResult<(Vec<Datum>, Vec<bool>)> {
        let n = argtypes.len();
        let mut values = vec![Datum::null(); n];
        let mut nulls = vec![true; n];
        for &dno in entry_paramnos {
            let (v, isnull) = self.datum_as_param(dno)?;
            values[dno as usize] = v;
            nulls[dno as usize] = isnull;
            let slot = &mut self.param_buf[dno as usize];
            slot.value = v;
            slot.isnull = isnull;
            slot.ptype = argtypes[dno as usize];
            slot.pflags = types_portal::params::PARAM_FLAG_CONST;
        }
        Ok((values, nulls))
    }

    fn datum_as_param(&mut self, dno: Dno) -> PgResult<(Datum, bool)> {
        let func = self.func;
        match &func.datums[dno as usize] {
            PlDatum::Var(_) => Ok(self.get_var(dno)),
            PlDatum::Rec(_) => self.rec_as_composite_datum(dno),
            PlDatum::RecField(f) => {
                if matches!(&self.datums[f.recparentno as usize], DatumVal::Rec(None))
                    && self.rec_meta(f.recparentno).rectypeid != RECORDOID
                {
                    self.instantiate_empty_rec(f.recparentno)?;
                }
                if let DatumVal::Rec(Some(rv)) = &self.datums[f.recparentno as usize] {
                    let want = f.fieldname.to_ascii_lowercase();
                    for (i, n) in rv.desc.names.iter().enumerate() {
                        if !rv.desc.dropped[i] && *n == want {
                            return Ok((rv.values[i], rv.nulls[i]));
                        }
                    }
                    let recname = match &self.func.datums[f.recparentno as usize] {
                        PlDatum::Rec(r) => r.refname.clone(),
                        _ => String::new(),
                    };
                    return Err(exec_err(
                        types_error::ERRCODE_UNDEFINED_COLUMN,
                        format!("record \"{recname}\" has no field \"{}\"", f.fieldname),
                    ));
                }
                let recname = match &self.func.datums[f.recparentno as usize] {
                    PlDatum::Rec(r) => r.refname.clone(),
                    _ => String::new(),
                };
                Err(Box::new(
                    elog::ereport(ERROR)
                        .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg(format!("record \"{recname}\" is not assigned yet"))
                        .errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")
                        .into_error(),
                ))
            }
            _ => panic!("plpgsql: datum {dno} cannot be a parameter"),
        }
    }

    // exec_eval_expr: returns (value, isnull, rettype, rettypmod). Caller
    // must exec_eval_cleanup when done with a by-ref result.
    pub fn exec_eval_expr(&mut self, expr: &PlExpr) -> PgResult<(Datum, bool, Oid, i32)> {
        self.ensure_plan(expr, CURSOR_OPT_PARALLEL_OK)?;

        if let Some(r) = self.exec_eval_simple_expr(expr)? {
            return Ok(r);
        }

        let rc = self.exec_run_select(expr, 0)?;
        if rc != spi::SPI_OK_SELECT {
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg("query did not return data")
                    .errcontext_msg(format!("query: {}", expr.query))
                    .into_error(),
            ));
        }
        let tuptab = self.eval_tuptable.expect("exec_run_select stored tuptable");
        let (natts, rettype, rettypmod) = spi::tuptable_with(tuptab, |t| {
            let n = t.tupdesc.attrs.len();
            if n >= 1 {
                (n, t.tupdesc.attrs[0].atttypid, t.tupdesc.attrs[0].atttypmod)
            } else {
                (n, types_core::InvalidOid, -1)
            }
        });
        if natts != 1 {
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(types_error::ERRCODE_SYNTAX_ERROR)
                    .errmsg_plural(
                        format!("query returned {natts} column"),
                        format!("query returned {natts} columns"),
                        natts as u64,
                    )
                    .errcontext_msg(format!("query: {}", expr.query))
                    .into_error(),
            ));
        }
        if self.eval_processed == 0 {
            return Ok((Datum::null(), true, rettype, rettypmod));
        }
        if self.eval_processed != 1 {
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(types_error::ERRCODE_CARDINALITY_VIOLATION)
                    .errmsg("query returned more than one row")
                    .errcontext_msg(format!("query: {}", expr.query))
                    .into_error(),
            ));
        }
        let (v, isnull) =
            spi::tuptable_with(tuptab, |t| spi::SPI_getbinval(&t.vals[0], &t.tupdesc, 1));
        Ok((v, isnull, rettype, rettypmod))
    }

    // exec_eval_simple_expr; Ok(None) = not simple, take the SPI path.
    fn exec_eval_simple_expr(
        &mut self,
        expr: &PlExpr,
    ) -> PgResult<Option<(Datum, bool, Oid, i32)>> {
        let (psrc, paramnos, argtypes) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&expr.expr_id).expect("plan ensured");
            (spi::SPI_plan_single_source(e.plan), e.paramnos.clone(), e.argtypes.clone())
        });
        let Some((psrc, _)) = psrc else {
            self.simple_cache.insert(expr.expr_id, None);
            return Ok(None);
        };

        // Per-eval revalidation (C CachedPlanIsSimplyValid): source valid,
        // plan is the source's current gplan, plan valid, search_path match.
        if let Some(entry) = self.simple_cache.get(&expr.expr_id) {
            match entry {
                None => return Ok(None),
                Some(se) => {
                    if !plancache::CachedPlanIsSimplyValid(se.psrc, se.cplan)? {
                        if let Some(Some(se)) = self.simple_cache.remove(&expr.expr_id) {
                            plancache::ReleaseCachedPlan(se.cplan);
                        }
                    }
                }
            }
        }

        if !self.simple_cache.contains_key(&expr.expr_id) {
            // Write param types BEFORE compile: exec_init_expr reads the
            // slot types and bakes slot addresses.
            let cplan = plancache::GetCachedPlan(
                psrc,
                types_portal::ParamListHandle::NULL,
                None,
                types_portal::QueryEnvHandle::NULL,
            )?;
            let built = (|| -> PgResult<Option<SimpleExpr>> {
                let stmts = plancache::CachedPlanStmtList(cplan);
                if stmts.len() != 1 {
                    return Ok(None);
                }
                let stmt = &stmts[0];
                if stmt.commandType != types_nodes::nodes_enums::CmdType::CMD_SELECT
                    || stmt.utilityStmt.is_some()
                    || !stmt.rowMarks.is_nil()
                {
                    return Ok(None);
                }
                let Some(plan) = simple_result_expr(stmt) else {
                    return Ok(None);
                };
                for &dno in &paramnos {
                    let slot = &mut self.param_buf[dno as usize];
                    slot.ptype = argtypes[dno as usize];
                    slot.pflags = types_portal::params::PARAM_FLAG_CONST;
                }
                let bind = types_portal::params::ParamBind {
                    extern_params: Some(
                        // SAFETY: param_buf is a stable Box'd slice living as
                        // long as the cached state (both die with the estate).
                        unsafe {
                            core::slice::from_raw_parts(
                                self.param_buf.as_ptr(),
                                self.param_buf.len(),
                            )
                        },
                    ),
                    exec_vals: None,
                    n_exec: 0,
                };
                // Compile into the invocation context (survives per-eval
                // resets); results land in the eval scratch.
                let mcx = self.datum_ctx.mcx();
                let Some(mut state) = execexpr::exec_init_expr(mcx, Some(plan.0), bind)? else {
                    return Ok(None);
                };
                state.arm_result_mcx(self.eval_ctx.mcx());
                Ok(Some(SimpleExpr {
                    state,
                    cplan,
                    psrc,
                    rettype: plan.1,
                    rettypmod: plan.2,
                }))
            })();
            match built {
                Ok(Some(se)) => {
                    self.simple_cache.insert(expr.expr_id, Some(se));
                }
                Ok(None) => {
                    plancache::ReleaseCachedPlan(cplan);
                    self.simple_cache.insert(expr.expr_id, None);
                    return Ok(None);
                }
                Err(e) => {
                    plancache::ReleaseCachedPlan(cplan);
                    return Err(e);
                }
            }
        }

        // Write current param values into the stable buffer.
        for &dno in &paramnos {
            let (v, isnull) = self.datum_as_param(dno)?;
            let slot = &mut self.param_buf[dno as usize];
            slot.value = v;
            slot.isnull = isnull;
        }

        let mut pushed = false;
        if !self.readonly_func {
            xact::CommandCounterIncrement()?;
            let snap = snapmgr::GetTransactionSnapshot()?;
            snapmgr::PushActiveSnapshot(&snap)?;
            pushed = true;
        }
        let result = (|| {
            let se = self
                .simple_cache
                .get_mut(&expr.expr_id)
                .and_then(|e| e.as_mut())
                .expect("inserted above");
            let mut slots = execexpr::EvalSlots { scan: None, inner: None, outer: None };
            let r = execexpr::exec_eval_expr(&mut se.state, &mut slots)?;
            Ok((r.value, r.isnull, se.rettype, se.rettypmod))
        })();
        if pushed {
            let popped = snapmgr::PopActiveSnapshot();
            if result.is_ok() {
                popped?;
            }
        }
        result.map(Some)
    }

    // exec_run_select (portal-less arm): SPI_execute_plan.
    fn exec_run_select(&mut self, expr: &PlExpr, maxtuples: i64) -> PgResult<i32> {
        let (plan, paramnos, argtypes) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&expr.expr_id).expect("plan ensured");
            (e.plan, e.paramnos.clone(), e.argtypes.clone())
        });
        let (values, nulls) = self.setup_params(&paramnos, &argtypes)?;
        let _frame = FrameGuard::push_spi(&expr.query, expr.parse_mode);
        let rc = spi::SPI_execute_plan(plan, &values, &nulls, self.readonly_func, maxtuples)
            .map_err(|e| spi_ctx_err(e, &expr.query, expr.parse_mode))?;
        self.eval_processed = spi::SPI_processed();
        if let Some(t) = self.eval_tuptable.take() {
            let _ = spi::SPI_freetuptable(t);
        }
        self.eval_tuptable = spi::SPI_tuptable();
        Ok(rc)
    }

    fn exec_eval_boolean(&mut self, expr: &PlExpr) -> PgResult<(bool, bool)> {
        let (v, mut isnull, t, m) = self.exec_eval_expr(expr)?;
        let v = self.exec_cast_value(v, &mut isnull, t, m, BOOLOID, -1)?;
        Ok((v.as_bool(), isnull))
    }

    // ------------------------------------------------------------------
    // Casts (get_cast_hashentry over a Param placeholder instead of C's
    // CaseTestExpr — identical coercion tree, supported by execexpr).
    // ------------------------------------------------------------------

    pub fn exec_cast_value(
        &mut self,
        value: Datum,
        isnull: &mut bool,
        valtype: Oid,
        valtypmod: i32,
        reqtype: Oid,
        reqtypmod: i32,
    ) -> PgResult<Datum> {
        if valtype == reqtype && (valtypmod == reqtypmod || reqtypmod == -1) {
            return Ok(value);
        }
        self.do_cast_value(value, isnull, valtype, valtypmod, reqtype, reqtypmod)
    }

    #[inline(never)]
    fn do_cast_value(
        &mut self,
        value: Datum,
        isnull: &mut bool,
        valtype: Oid,
        valtypmod: i32,
        reqtype: Oid,
        reqtypmod: i32,
    ) -> PgResult<Datum> {
        let key = (valtype, valtypmod, reqtype, reqtypmod);
        let cur_gen = plancache::PlanCacheInvalCounter();
        let stale = match self.cast_cache.get(&key) {
            Some(e) => e.inval_gen != cur_gen,
            None => true,
        };
        if stale {
            let entry = self.build_cast_entry(valtype, valtypmod, reqtype, reqtypmod)?;
            self.cast_cache.insert(key, entry);
        }
        let entry = self.cast_cache.get_mut(&key).expect("inserted");
        let Some(state) = entry.state.as_mut() else {
            return Ok(value);
        };
        entry.param[0].value = value;
        entry.param[0].isnull = *isnull;
        let mut slots = execexpr::EvalSlots { scan: None, inner: None, outer: None };
        let r = execexpr::exec_eval_expr(state, &mut slots)?;
        *isnull = r.isnull;
        Ok(r.value)
    }

    fn build_cast_entry(
        &mut self,
        srctype: Oid,
        srctypmod: i32,
        dsttype: Oid,
        dsttypmod: i32,
    ) -> PgResult<CastEntry> {
        use types_nodes::primnodes::{CoercionForm, Param, ParamKind};

        // Read before catalog lookups: an inval firing mid-build leaves this
        // entry stamped old, so the next use rebuilds.
        let inval_gen = plancache::PlanCacheInvalCounter();
        let param: Box<[ParamExternData; 1]> = Box::new([ParamExternData {
            value: Datum::null(),
            isnull: true,
            pflags: 0,
            ptype: srctype,
        }]);

        // The coercion tree is built and compiled in the invocation context
        // (it lives as long as the cache entry).
        let mcx = self.datum_ctx.mcx();
        let placeholder = types_nodes::Node::mk(
            mcx,
            Param {
                paramkind: ParamKind::PARAM_EXTERN,
                paramid: 1,
                paramtype: srctype,
                paramtypmod: srctypmod,
                paramcollid: lsyscache::typ::get_typcollation(srctype)?,
                location: -1,
            },
        )?;

        let pstate = parser_small1::make_parsestate(mcx, None);
        let cast_expr = if srctype == UNKNOWNOID || srctype == RECORDOID {
            None
        } else {
            coerce::coerce_to_target_type(
                mcx,
                &pstate,
                placeholder,
                srctype,
                dsttype,
                dsttypmod,
                coerce::CoercionContext::COERCION_PLPGSQL,
                CoercionForm::COERCE_IMPLICIT_CAST,
                -1,
            )?
        };
        let cast_expr = match cast_expr {
            Some(e) => Some(e),
            None => {
                let io = types_nodes::Node::mk(
                    mcx,
                    types_nodes::primnodes::CoerceViaIO {
                        arg: placeholder,
                        resulttype: dsttype,
                        resultcollid: types_core::InvalidOid,
                        coerceformat: CoercionForm::COERCE_IMPLICIT_CAST,
                        location: -1,
                    },
                )?;
                if dsttypmod != -1 {
                    coerce::coerce_to_target_type(
                        mcx,
                        &pstate,
                        io,
                        dsttype,
                        dsttype,
                        dsttypmod,
                        coerce::CoercionContext::COERCION_ASSIGNMENT,
                        CoercionForm::COERCE_IMPLICIT_CAST,
                        -1,
                    )?
                } else {
                    Some(io)
                }
            }
        };
        parser_small1::free_parsestate(pstate)?;

        let Some(cast_expr) = cast_expr else {
            return Ok(CastEntry { state: None, param, inval_gen });
        };
        // No-op relabeling of the bare placeholder: skip evaluation.
        if let Some(r) = cast_expr.as_relabel_type() {
            if r.arg.as_variant::<Param>().is_some() {
                return Ok(CastEntry { state: None, param, inval_gen });
            }
        }

        let bind = types_portal::params::ParamBind {
            // SAFETY: `param` is a stable Box living in the cache entry
            // alongside the compiled state.
            extern_params: Some(unsafe {
                core::slice::from_raw_parts(param.as_ptr(), 1)
            }),
            exec_vals: None,
            n_exec: 0,
        };
        let Some(mut state) = execexpr::exec_init_expr(mcx, Some(cast_expr), bind)? else {
            return Ok(CastEntry { state: None, param, inval_gen });
        };
        state.arm_result_mcx(self.eval_ctx.mcx());
        Ok(CastEntry { state: Some(state), param, inval_gen })
    }

    // convert_value_to_string: type output function in eval scratch.
    fn convert_value_to_string(&mut self, value: Datum, valtype: Oid) -> PgResult<String> {
        let (foutoid, _) = lsyscache::typ::getTypeOutputInfo(valtype)?;
        let mut finfo = fmgr_core::fmgr_info(foutoid)?;
        let out = fmgr::function_call1_coll_in(
            &mut finfo,
            types_core::InvalidOid,
            self.eval_ctx.mcx(),
            value,
        )?;
        // SAFETY: type output functions return a NUL-terminated cstring.
        let s = unsafe {
            core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char)
        };
        Ok(s.to_string_lossy().into_owned())
    }

    // appendStringInfoStringQuoted(-1) (stringinfo_mb.c): single-quote the
    // whole value, doubling embedded quotes.
    fn append_quoted(out: &mut String, s: &str) {
        out.push('\'');
        for ch in s.chars() {
            if ch == '\'' {
                out.push('\'');
            }
            out.push(ch);
        }
        out.push('\'');
    }

    // format_expr_params (pl_exec.c); None when the expr takes no parameters.
    fn format_expr_params(&mut self, expr: &PlExpr) -> PgResult<Option<String>> {
        if !self.func.print_strict_params {
            return Ok(None);
        }
        let (paramnos, argtypes) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&expr.expr_id).expect("plan ensured");
            (e.paramnos.clone(), e.argtypes.clone())
        });
        if paramnos.is_empty() {
            return Ok(None);
        }
        let mut out = String::new();
        for (i, &dno) in paramnos.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let refname = match &self.func.datums[dno as usize] {
                PlDatum::Var(v) => v.refname.clone(),
                PlDatum::Row(r) => r.refname.clone(),
                PlDatum::Rec(r) => r.refname.clone(),
                // C reads ->refname through a PLpgSQL_var cast; a recfield's
                // same-offset member is its fieldname.
                PlDatum::RecField(f) => f.fieldname.clone(),
            };
            out.push_str(&refname);
            out.push_str(" = ");
            let (v, isnull) = self.datum_as_param(dno)?;
            if isnull {
                out.push_str("NULL");
            } else {
                let sv = self.convert_value_to_string(v, argtypes[dno as usize])?;
                Self::append_quoted(&mut out, &sv);
            }
        }
        Ok(Some(out))
    }

    // format_preparedparamsdata (pl_exec.c) over EXECUTE USING values.
    fn format_prepared_params(
        &mut self,
        ptypes: &[Oid],
        pvalues: &[Datum],
        pnulls: &[bool],
    ) -> PgResult<Option<String>> {
        if !self.func.print_strict_params || ptypes.is_empty() {
            return Ok(None);
        }
        let mut out = String::new();
        for i in 0..ptypes.len() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&format!("${} = ", i + 1));
            if pnulls[i] {
                out.push_str("NULL");
            } else {
                let sv = self.convert_value_to_string(pvalues[i], ptypes[i])?;
                Self::append_quoted(&mut out, &sv);
            }
        }
        Ok(Some(out))
    }
}

// exec_simple_check_plan's Result-node test on the built plan; returns the
// single tlist expr and its type.
fn simple_result_expr(
    stmt: &types_nodes::plannodes::PlannedStmt<'static>,
) -> Option<(types_nodes::Node<'static>, Oid, i32)> {
    let plan = stmt.planTree?;
    let result = plan.as_variant::<types_nodes::plannodes::Result>()?;
    if result.resconstantqual.is_some() {
        return None;
    }
    let base = &result.plan;
    if base.lefttree.is_some() || !base.initPlan.is_nil() || !base.qual.is_nil() {
        return None;
    }
    let tlist = &base.targetlist;
    if tlist.len() != 1 {
        return None;
    }
    let te = tlist.first()?.as_variant::<types_nodes::primnodes::TargetEntry>()?;
    let expr = te.expr;
    let t = nodes_core::node_funcs::expr_type(expr);
    let m = nodes_core::node_funcs::expr_typmod(expr);
    Some((expr, t, m))
}

impl<'a> Estate<'a> {
    // ------------------------------------------------------------------
    // Statement machine
    // ------------------------------------------------------------------

    pub fn exec_toplevel_block(&mut self, block: &'a PlBlock) -> PgResult<i32> {
        self.frame.stmt.set(Some((block.lineno, "statement block")));
        let rc = self.exec_stmt_block(block)?;
        self.frame.stmt.set(None);
        Ok(rc)
    }

    fn exec_stmts(&mut self, stmts: &'a [PlStmt]) -> PgResult<i32> {
        let save = self.frame.stmt.get();
        for s in stmts {
            self.frame.stmt.set(Some((stmt_lineno(s), stmt_typename(s))));
            let rc = self.exec_stmt(s)?;
            if rc != RC_OK {
                self.frame.stmt.set(save);
                return Ok(rc);
            }
        }
        self.frame.stmt.set(save);
        Ok(RC_OK)
    }

    fn exec_stmt(&mut self, stmt: &'a PlStmt) -> PgResult<i32> {
        match stmt {
            PlStmt::Block(b) => self.exec_stmt_block(b),
            PlStmt::Assign { varno, expr, .. } => {
                self.exec_assign_expr(*varno, expr)?;
                Ok(RC_OK)
            }
            PlStmt::If { cond, then_body, elsifs, else_body, .. } => {
                let (value, isnull) = self.exec_eval_boolean(cond)?;
                self.exec_eval_cleanup();
                if !isnull && value {
                    return self.exec_stmts(then_body);
                }
                for (c, body) in elsifs {
                    let (value, isnull) = self.exec_eval_boolean(c)?;
                    self.exec_eval_cleanup();
                    if !isnull && value {
                        return self.exec_stmts(body);
                    }
                }
                if let Some(body) = else_body {
                    return self.exec_stmts(body);
                }
                Ok(RC_OK)
            }
            PlStmt::Loop { label, body, .. } => loop {
                let rc = self.exec_stmts(body)?;
                if let Some(rc) = self.loop_rc(label.as_deref(), rc) {
                    return Ok(rc);
                }
            },
            PlStmt::While { label, cond, body, .. } => loop {
                let (value, isnull) = self.exec_eval_boolean(cond)?;
                self.exec_eval_cleanup();
                if isnull || !value {
                    return Ok(RC_OK);
                }
                let rc = self.exec_stmts(body)?;
                if let Some(rc) = self.loop_rc(label.as_deref(), rc) {
                    return Ok(rc);
                }
            },
            PlStmt::ForI { label, var, lower, upper, step, reverse, body, .. } => {
                self.exec_stmt_fori(label.as_deref(), *var, lower, upper, step.as_ref(), *reverse, body)
            }
            PlStmt::ForS { label, var, query, body, .. } => {
                self.exec_stmt_fors(label.as_deref(), *var, query, body)
            }
            PlStmt::ExitContinue { is_exit, label, cond, .. } => {
                if let Some(c) = cond {
                    let (value, isnull) = self.exec_eval_boolean(c)?;
                    self.exec_eval_cleanup();
                    if isnull || !value {
                        return Ok(RC_OK);
                    }
                }
                self.exitlabel = label.clone();
                Ok(if *is_exit { RC_EXIT } else { RC_CONTINUE })
            }
            PlStmt::Return { expr, retvarno, .. } => {
                self.exec_stmt_return(expr.as_ref(), *retvarno)?;
                Ok(RC_RETURN)
            }
            PlStmt::Raise { .. } => self.exec_stmt_raise(stmt),
            PlStmt::Assert { cond, message, .. } => self.exec_stmt_assert(cond, message.as_ref()),
            PlStmt::ExecSql { sqlstmt, into, strict, target, .. } => {
                self.exec_stmt_execsql(sqlstmt, *into, *strict, *target)
            }
            PlStmt::Perform { expr, .. } => {
                self.ensure_plan(expr, CURSOR_OPT_PARALLEL_OK)?;
                let _ = self.exec_run_select(expr, 0)?;
                let found = self.eval_processed != 0;
                self.exec_set_found(found);
                self.exec_eval_cleanup();
                Ok(RC_OK)
            }
            PlStmt::Call { expr, is_call, .. } => self.exec_stmt_call(expr, *is_call),
            PlStmt::Commit { chain, .. } => self.exec_stmt_commit_rollback(true, *chain),
            PlStmt::Rollback { chain, .. } => self.exec_stmt_commit_rollback(false, *chain),
            PlStmt::DynExecute { query, into, strict, target, params, .. } => {
                self.exec_stmt_dynexecute(query, *into, *strict, *target, params)
            }
            PlStmt::GetDiag { is_stacked, items, .. } => {
                self.exec_stmt_getdiag(*is_stacked, items)
            }
            PlStmt::Case { t_expr, t_varno, whens, have_else, else_stmts, .. } => self
                .exec_stmt_case(
                    t_expr.as_ref(),
                    *t_varno,
                    whens,
                    *have_else,
                    else_stmts,
                ),
            PlStmt::ForEachA { label, varno, slice, expr, body, .. } => {
                self.exec_stmt_foreach_a(label.as_deref(), *varno, *slice, expr, body)
            }
            PlStmt::ReturnNext { expr, retvarno, .. } => {
                self.exec_stmt_return_next(expr.as_ref(), *retvarno)
            }
            PlStmt::ReturnQuery { query, dynquery, params, .. } => {
                self.exec_stmt_return_query(query.as_ref(), dynquery.as_ref(), params)
            }
            PlStmt::Open { curvar, cursor_options, argquery, query, dynquery, params, .. } => self
                .exec_stmt_open(
                    *curvar,
                    *cursor_options,
                    argquery.as_ref(),
                    query.as_ref(),
                    dynquery.as_ref(),
                    params,
                ),
            PlStmt::Fetch { target, curvar, direction, how_many, expr, is_move, .. } => {
                self.exec_stmt_fetch(*target, *curvar, *direction, *how_many, expr.as_ref(), *is_move)
            }
            PlStmt::Close { curvar, .. } => self.exec_stmt_close(*curvar),
            PlStmt::ForC { label, var, curvar, argquery, body, .. } => {
                self.exec_stmt_forc(label.as_deref(), *var, *curvar, argquery.as_ref(), body)
            }
            PlStmt::DynForS { label, var, query, params, body, .. } => {
                self.exec_stmt_dynfors(label.as_deref(), *var, query, params, body)
            }
        }
    }

    // exec_stmt_getdiag (pl_exec.c:2410).
    fn exec_stmt_getdiag(&mut self, is_stacked: bool, items: &[GetDiagItem]) -> PgResult<i32> {
        if is_stacked && self.cur_error.is_none() {
            return Err(exec_err(
                types_error::ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER,
                "GET STACKED DIAGNOSTICS cannot be used outside an exception handler"
                    .to_string(),
            ));
        }
        const OIDOID: Oid = 26;
        for item in items {
            match item.kind {
                GETDIAG_ROW_COUNT => {
                    let v = Datum::from_i64(self.eval_processed as i64);
                    self.exec_assign_value(item.target, v, false, INT8OID, -1)?;
                }
                GETDIAG_ROUTINE_OID => {
                    let v = Datum::from_oid(self.func.fn_oid);
                    self.exec_assign_value(item.target, v, false, OIDOID, -1)?;
                }
                GETDIAG_CONTEXT => {
                    let s = get_error_context_stack();
                    self.exec_assign_c_string(item.target, Some(&s))?;
                }
                _ => {
                    let e = self.cur_error.as_ref().expect("stacked item without cur_error");
                    let s: Option<String> = match item.kind {
                        GETDIAG_ERROR_CONTEXT => e.context.clone(),
                        GETDIAG_ERROR_DETAIL => e.detail.clone(),
                        GETDIAG_ERROR_HINT => e.hint.clone(),
                        GETDIAG_RETURNED_SQLSTATE => Some(unpack_sql_state(e.sqlstate)),
                        GETDIAG_COLUMN_NAME => e.column_name.clone(),
                        GETDIAG_CONSTRAINT_NAME => e.constraint_name.clone(),
                        GETDIAG_DATATYPE_NAME => e.datatype_name.clone(),
                        GETDIAG_MESSAGE_TEXT => Some(e.message.clone()),
                        GETDIAG_TABLE_NAME => e.table_name.clone(),
                        GETDIAG_SCHEMA_NAME => e.schema_name.clone(),
                        other => panic!("unrecognized diagnostic item kind: {other}"),
                    };
                    self.exec_assign_c_string(item.target, s.as_deref())?;
                }
            }
        }
        self.exec_eval_cleanup();
        Ok(RC_OK)
    }

    // exec_assign_c_string: a NULL C string assigns an empty string.
    fn exec_assign_c_string(&mut self, target: Dno, s: Option<&str>) -> PgResult<()> {
        let s = s.unwrap_or("");
        let v = varlena::cstring_to_text(self.eval_ctx.mcx(), s.as_bytes())?;
        self.exec_assign_value(target, fmgr::varlena_result(v), false, TEXTOID, -1)
    }

    // exec_stmt_case (pl_exec.c:2556).
    fn exec_stmt_case(
        &mut self,
        t_expr: Option<&PlExpr>,
        t_varno: Dno,
        whens: &'a [(PlExpr, Vec<PlStmt>)],
        have_else: bool,
        else_stmts: &'a [PlStmt],
    ) -> PgResult<i32> {
        let have_t = t_expr.is_some();
        if let Some(te) = t_expr {
            let (t_val, isnull, t_typoid, t_typmod) = self.exec_eval_expr(te)?;
            {
                let cur = self.var_type(t_varno);
                if cur.typoid != t_typoid || cur.atttypmod != t_typmod {
                    let ty = crate::comp::CompState::build_datatype(
                        t_typoid,
                        t_typmod,
                        self.func.fn_input_collation,
                    )?;
                    self.var_type_overrides.insert(t_varno, ty);
                }
            }
            let (tl, bv) = {
                let t = self.var_type(t_varno);
                (t.typlen, t.typbyval)
            };
            let stored = self.copy_to_datum_ctx(t_val, isnull, tl, bv)?;
            self.set_var(t_varno, stored, isnull);
            self.exec_eval_cleanup();
        }

        for (expr, stmts) in whens {
            let (value, isnull) = self.exec_eval_boolean(expr)?;
            self.exec_eval_cleanup();
            if !isnull && value {
                if have_t {
                    self.set_var(t_varno, Datum::null(), true);
                }
                return self.exec_stmts(stmts);
            }
        }

        if have_t {
            self.set_var(t_varno, Datum::null(), true);
        }
        if !have_else {
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(types_error::ERRCODE_CASE_NOT_FOUND)
                    .errmsg("case not found")
                    .errhint("CASE statement is missing ELSE part.")
                    .into_error(),
            ));
        }
        self.exec_stmts(else_stmts)
    }

    // exec_stmt_foreach_a (pl_exec.c:3008).
    fn exec_stmt_foreach_a(
        &mut self,
        label: Option<&str>,
        varno: Dno,
        slice: i32,
        expr: &PlExpr,
        body: &'a [PlStmt],
    ) -> PgResult<i32> {
        use arrayfuncs::foundation::read_dims_lbounds;

        let (value, isnull, arrtype, arrtypmod) = self.exec_eval_expr(expr)?;
        if isnull {
            return Err(exec_err(
                types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                "FOREACH expression must not be null".to_string(),
            ));
        }
        let elemtype = lsyscache::typ::get_element_type(arrtype)?;
        if !OidIsValid(elemtype) {
            return Err(exec_err(
                types_error::ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "FOREACH expression must yield an array, not type {}",
                    format_type::format_type_be(arrtype)?
                ),
            ));
        }

        // C's private stmt_mcontext: the array copy must survive the body's
        // eval resets.
        let stmt_ctx = Ctx::new("PLpgSQL FOREACH");
        // SAFETY: non-null by-ref array datum; the ref lives only for the
        // detoast copy below.
        let vr = unsafe { datum::VarlenaRef::from_ptr(value.as_usize() as *const u8) };
        let arr = detoast::detoast_attr(stmt_ctx.mcx(), vr.as_bytes())?;
        self.exec_eval_cleanup();
        let arr: &[u8] = &arr;

        let (ndim, dims, lbs) = read_dims_lbounds(arr);
        if slice < 0 || slice > ndim {
            return Err(exec_err(
                types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR,
                format!("slice dimension ({slice}) is out of the valid range 0..{ndim}"),
            ));
        }

        let loop_var_elem = match &self.func.datums[varno as usize] {
            PlDatum::Rec(_) | PlDatum::Row(_) => types_core::InvalidOid,
            _ => lsyscache::typ::get_element_type(self.var_type(varno).typoid)?,
        };
        if slice > 0 && !OidIsValid(loop_var_elem) {
            return Err(exec_err(
                types_error::ERRCODE_DATATYPE_MISMATCH,
                "FOREACH ... SLICE loop variable must be of an array type".to_string(),
            ));
        }
        if slice == 0 && OidIsValid(loop_var_elem) {
            return Err(exec_err(
                types_error::ERRCODE_DATATYPE_MISMATCH,
                "FOREACH loop variable must not be of an array type".to_string(),
            ));
        }

        let (elmlen, elmbyval, elmalign) = lsyscache::typ::get_typlenbyvalalign(elemtype)?;
        let (elems, nulls) = arrayfuncs::construct::deconstruct_array(
            stmt_ctx.mcx(),
            arr,
            elmlen as i32,
            elmbyval,
            elmalign as u8,
            true,
        )?;

        let mut found = false;
        let mut rc = RC_OK;
        if slice == 0 {
            for i in 0..elems.len() {
                found = true;
                self.exec_assign_value(varno, elems[i], nulls[i], elemtype, arrtypmod)?;
                self.exec_eval_cleanup();
                rc = self.exec_stmts(body)?;
                if let Some(r) = self.loop_rc(label, rc) {
                    rc = r;
                    break;
                }
            }
        } else {
            let outer: usize = dims[..(ndim - slice) as usize]
                .iter()
                .map(|&d| d as usize)
                .product();
            let slice_dims = &dims[(ndim - slice) as usize..ndim as usize];
            let slice_lbs = &lbs[(ndim - slice) as usize..ndim as usize];
            let slice_len: usize = slice_dims.iter().map(|&d| d as usize).product();
            for s in 0..outer {
                let sub = arrayfuncs::construct::construct_md_array(
                    stmt_ctx.mcx(),
                    &elems[s * slice_len..(s + 1) * slice_len],
                    Some(&nulls[s * slice_len..(s + 1) * slice_len]),
                    slice,
                    slice_dims,
                    slice_lbs,
                    elemtype,
                    elmlen as i32,
                    elmbyval,
                    elmalign as u8,
                )?;
                found = true;
                self.exec_assign_value(
                    varno,
                    Datum::from_usize(sub.as_ptr() as usize),
                    false,
                    arrtype,
                    arrtypmod,
                )?;
                self.exec_eval_cleanup();
                rc = self.exec_stmts(body)?;
                if let Some(r) = self.loop_rc(label, rc) {
                    rc = r;
                    break;
                }
            }
        }

        self.exec_set_found(found);
        Ok(rc)
    }

    // LOOP_RC_PROCESSING: Some(rc) = terminate loop with rc, None = iterate.
    fn loop_rc(&mut self, label: Option<&str>, rc: i32) -> Option<i32> {
        match rc {
            RC_OK => None,
            RC_RETURN => Some(RC_RETURN),
            RC_EXIT => {
                if self.exitlabel.is_none() {
                    Some(RC_OK)
                } else if label.is_some() && self.exitlabel.as_deref() == label {
                    self.exitlabel = None;
                    Some(RC_OK)
                } else {
                    Some(RC_EXIT)
                }
            }
            RC_CONTINUE => {
                if self.exitlabel.is_none() {
                    None
                } else if label.is_some() && self.exitlabel.as_deref() == label {
                    self.exitlabel = None;
                    None
                } else {
                    Some(RC_CONTINUE)
                }
            }
            _ => unreachable!("bad rc"),
        }
    }

    fn exec_stmt_block(&mut self, block: &'a PlBlock) -> PgResult<i32> {
        self.frame.text.set(Some("during statement block local variable initialization"));
        for &dno in &block.initvarnos {
            match &self.func.datums[dno as usize] {
                PlDatum::Var(v) => {
                    if let Some(default_val) = &v.default_val {
                        self.exec_assign_expr(dno, default_val)?;
                    } else {
                        self.set_var(dno, Datum::null(), true);
                        if v.notnull {
                            return Err(exec_err(
                                types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                                format!(
                                    "variable \"{}\" declared NOT NULL cannot default to NULL",
                                    v.refname
                                ),
                            ));
                        }
                        // C (pl_exec.c:1702-1719): a defaultless domain var
                        // gets NULL assigned through an UNKNOWN cast so the
                        // domain's NOT NULL/CHECK constraints run.
                        if v.datatype.typtype == TYPTYPE_DOMAIN {
                            self.exec_assign_value(dno, Datum::null(), true, UNKNOWNOID, -1)?;
                        }
                    }
                }
                PlDatum::Rec(_) => {
                    self.datums[dno as usize] = DatumVal::Rec(None);
                }
                _ => {}
            }
        }
        self.frame.text.set(None);

        let rc = if let Some(exc) = &block.exceptions {
            self.exec_block_with_exceptions(block, exc)?
        } else {
            self.exec_stmts(&block.body)?
        };
        self.frame.text.set(None);

        // C's block rc handling: CONTINUE never matches a block; EXIT matches
        // only on a label match.
        match rc {
            RC_OK | RC_RETURN | RC_CONTINUE => Ok(rc),
            RC_EXIT => {
                if self.exitlabel.is_some()
                    && block.label.as_deref() == self.exitlabel.as_deref()
                {
                    self.exitlabel = None;
                    Ok(RC_OK)
                } else {
                    Ok(RC_EXIT)
                }
            }
            _ => unreachable!("bad rc"),
        }
    }

    // exec_stmt_block's exception arm: body under an internal subtransaction;
    // on error, roll the subxact back and run the first matching handler.
    fn exec_block_with_exceptions(
        &mut self,
        block: &'a PlBlock,
        exc: &'a ExceptionBlock,
    ) -> PgResult<i32> {
        let save_owner = resowner::CurrentResourceOwner();
        self.frame.text.set(Some("during statement block entry"));
        xact::BeginInternalSubTransaction(None)?;
        self.frame.text.set(None);

        // Loud panics unwind without a PgResult; the subtransaction must
        // still die with the unwind or a later ROLLBACK walks a stack with a
        // leaked SUBINPROGRESS over a block-less top (C's PG_CATCH cannot be
        // bypassed this way).
        let body_result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.exec_stmts(&block.body)
        })) {
            Ok(r) => r,
            Err(payload) => {
                let _ = xact::RollbackAndReleaseCurrentSubTransaction();
                resowner::SetCurrentResourceOwner(save_owner);
                std::panic::resume_unwind(payload);
            }
        };

        // C's PG_TRY extends over the return-value transfer and the subxact
        // release; errors there reach the same handlers.
        let attempt = body_result.and_then(|rc| {
            self.frame.text.set(Some("during statement block exit"));
            // The return value must survive subxact exit (C datumTransfer
            // out of the subxact eval_econtext).
            if rc == RC_RETURN && !self.retisnull && self.ret_rec.is_none()
                && OidIsValid(self.rettype)
            {
                let (typlen, typbyval) = lsyscache::typ::get_typlenbyval(self.rettype)?;
                self.retval = self.copy_to_datum_ctx(self.retval, false, typlen, typbyval)?;
            }
            xact::ReleaseCurrentSubTransaction()?;
            resowner::SetCurrentResourceOwner(save_owner);
            self.frame.text.set(None);
            Ok(rc)
        });

        match attempt {
            Ok(rc) => Ok(rc),
            Err(e) => {
                // Only ERROR is catchable; FATAL/PANIC never longjmp in C
                // (the backend exits) — release the subxact and propagate.
                if e.level > ERROR {
                    let _ = xact::RollbackAndReleaseCurrentSubTransaction();
                    resowner::SetCurrentResourceOwner(save_owner);
                    return Err(e);
                }
                // Bake this frame's context with the throw-time stmt/text
                // before the cleanup markers overwrite them (C's callback ran
                // at errfinish).
                let edata = attach_frame_context_at_catch(e, self);
                self.frame.text.set(Some("during exception cleanup"));
                xact::RollbackAndReleaseCurrentSubTransaction()?;
                resowner::SetCurrentResourceOwner(save_owner);
                // The subxact abort freed tuple tables made inside it
                // (AtEOSubXact_SPI); drop the handle without a second free.
                self.eval_tuptable = None;
                self.eval_ctx.reset();

                let matched = exc
                    .exc_list
                    .iter()
                    .find(|x| exception_matches_conditions(&edata, &x.conditions));
                match matched {
                    Some(exception) => {
                        let ss = unpack_sql_state(edata.sqlstate);
                        self.assign_text_var(exc.sqlstate_varno, &ss)?;
                        let msg = edata.message.clone();
                        self.assign_text_var(exc.sqlerrm_varno, &msg)?;
                        let save = self.cur_error.replace(edata);
                        self.frame.text.set(None);
                        let rc = self.exec_stmts(&exception.action)?;
                        self.cur_error = save;
                        Ok(rc)
                    }
                    None => Err(edata),
                }
            }
        }
    }

    pub fn assign_text_var(&mut self, dno: Dno, s: &str) -> PgResult<()> {
        let v = varlena::cstring_to_text(self.datum_ctx.mcx(), s.as_bytes())?;
        self.set_var(dno, fmgr::varlena_result(v), false);
        Ok(())
    }

    fn exec_assign_expr(&mut self, target: Dno, expr: &PlExpr) -> PgResult<()> {
        // C's plan prepare resolves the recfield target via make_datum_param
        // -> exec_get_datum_type, which errors positionless under the SPI
        // context callback; mirror that before planning.
        if let PlDatum::RecField(f) = &self.func.datums[target as usize] {
            if self.recfield_type(f)?.is_none() {
                let recname = match &self.func.datums[f.recparentno as usize] {
                    PlDatum::Rec(r) => r.refname.clone(),
                    _ => String::new(),
                };
                let err = if matches!(&self.datums[f.recparentno as usize], DatumVal::Rec(Some(_)))
                {
                    exec_err(
                        types_error::ERRCODE_UNDEFINED_COLUMN,
                        format!("record \"{recname}\" has no field \"{}\"", f.fieldname),
                    )
                } else {
                    Box::new(
                        elog::ereport(ERROR)
                            .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                            .errmsg(format!("record \"{recname}\" is not assigned yet"))
                            .errdetail(
                                "The tuple structure of a not-yet-assigned record is indeterminate.",
                            )
                            .into_error(),
                    )
                };
                return Err(spi_ctx_err(err, &expr.query, expr.parse_mode));
            }
        }
        self.ensure_plan(expr, 0)?;
        let (value, isnull, valtype, valtypmod) = self.exec_eval_expr(expr)?;
        self.exec_assign_value(target, value, isnull, valtype, valtypmod)?;
        self.exec_eval_cleanup();
        Ok(())
    }

    // exec_assign_value.
    pub(crate) fn exec_assign_value(
        &mut self,
        target: Dno,
        value: Datum,
        mut isnull: bool,
        valtype: Oid,
        valtypmod: i32,
    ) -> PgResult<()> {
        match &self.func.datums[target as usize] {
            PlDatum::Var(v) => {
                let (reqtype, reqtypmod, typlen, typbyval, notnull, refname) = (
                    v.datatype.typoid,
                    v.datatype.atttypmod,
                    v.datatype.typlen,
                    v.datatype.typbyval,
                    v.notnull,
                    v.refname.clone(),
                );
                let newvalue =
                    self.exec_cast_value(value, &mut isnull, valtype, valtypmod, reqtype, reqtypmod)?;
                if isnull && notnull {
                    return Err(exec_err(
                        types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                        format!(
                            "null value cannot be assigned to variable \"{refname}\" declared NOT NULL"
                        ),
                    ));
                }
                let stored = self.copy_to_datum_ctx(newvalue, isnull, typlen, typbyval)?;
                self.set_var(target, stored, isnull);
                Ok(())
            }
            PlDatum::RecField(f) => {
                let recno = f.recparentno;
                let want = f.fieldname.to_ascii_lowercase();
                let recname = match &self.func.datums[recno as usize] {
                    PlDatum::Rec(r) => r.refname.clone(),
                    _ => String::new(),
                };
                let DatumVal::Rec(Some(rv)) = &self.datums[recno as usize] else {
                    return Err(Box::new(
                        elog::ereport(ERROR)
                            .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                            .errmsg(format!("record \"{recname}\" is not assigned yet"))
                            .errdetail(
                                "The tuple structure of a not-yet-assigned record is indeterminate.",
                            )
                            .into_error(),
                    ));
                };
                let mut found: Option<(usize, Oid, i32, i16, bool)> = None;
                for (i, n) in rv.desc.names.iter().enumerate() {
                    if !rv.desc.dropped[i] && *n == want {
                        found = Some((
                            i,
                            rv.desc.types[i],
                            rv.desc.typmods[i],
                            rv.desc.typlens[i],
                            rv.desc.typbyvals[i],
                        ));
                        break;
                    }
                }
                let Some((i, ftype, ftypmod, flen, fbyval)) = found else {
                    return Err(exec_err(
                        types_error::ERRCODE_UNDEFINED_COLUMN,
                        format!("record \"{recname}\" has no field \"{}\"", f.fieldname),
                    ));
                };
                let newvalue =
                    self.exec_cast_value(value, &mut isnull, valtype, valtypmod, ftype, ftypmod)?;
                let stored = self.copy_to_datum_ctx(newvalue, isnull, flen, fbyval)?;
                if let DatumVal::Rec(Some(rv)) = &mut self.datums[recno as usize] {
                    rv.values[i] = stored;
                    rv.nulls[i] = isnull;
                    rv.empty = false;
                }
                Ok(())
            }
            PlDatum::Rec(_) => {
                if isnull {
                    // C exec_move_row(rec, NULL, NULL): an empty record of
                    // the rec's own type.
                    if self.rec_meta(target).rectypeid != RECORDOID {
                        self.instantiate_empty_rec(target)?;
                    } else if let DatumVal::Rec(Some(rv)) = &mut self.datums[target as usize] {
                        for i in 0..rv.values.len() {
                            rv.values[i] = Datum::null();
                            rv.nulls[i] = true;
                        }
                        rv.empty = true;
                    }
                    return Ok(());
                }
                if valtype != RECORDOID && !lsyscache::typ::type_is_rowtype(valtype)? {
                    return Err(exec_err(
                        types_error::ERRCODE_DATATYPE_MISMATCH,
                        "cannot assign non-composite value to a record variable".to_string(),
                    ));
                }
                let (desc, src, values, nulls) = self.deconstruct_composite(value)?;
                self.move_rec_from_values(target, &desc, src, &values, &nulls)
            }
            PlDatum::Row(r) => {
                let varnos = r.varnos.clone();
                if isnull {
                    for dno in varnos {
                        self.exec_assign_value(dno, Datum::null(), true, UNKNOWNOID, -1)?;
                    }
                    return Ok(());
                }
                if valtype != RECORDOID && !lsyscache::typ::type_is_rowtype(valtype)? {
                    return Err(exec_err(
                        types_error::ERRCODE_DATATYPE_MISMATCH,
                        "cannot assign non-composite value to a row variable".to_string(),
                    ));
                }
                let (desc, _src, values, nulls) = self.deconstruct_composite(value)?;
                let natts = desc.types.len();
                let mut anum = 0usize;
                for dno in varnos {
                    while anum < natts && desc.dropped[anum] {
                        anum += 1;
                    }
                    let (v, vn, vt, vm) = if anum < natts {
                        let r = (values[anum], nulls[anum], desc.types[anum], desc.typmods[anum]);
                        anum += 1;
                        r
                    } else {
                        (Datum::null(), true, UNKNOWNOID, -1)
                    };
                    self.exec_assign_value(dno, v, vn, vt, vm)?;
                }
                Ok(())
            }
        }
    }

    fn exec_stmt_fori(
        &mut self,
        label: Option<&str>,
        var: Dno,
        lower: &PlExpr,
        upper: &PlExpr,
        step: Option<&PlExpr>,
        reverse: bool,
        body: &'a [PlStmt],
    ) -> PgResult<i32> {
        let (vt, vm) = {
            let t = self.var_type(var);
            (t.typoid, t.atttypmod)
        };

        let (v, mut isnull, t, m) = self.exec_eval_expr(lower)?;
        let v = self.exec_cast_value(v, &mut isnull, t, m, vt, vm)?;
        if isnull {
            return Err(exec_err(
                types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                "lower bound of FOR loop cannot be null".to_string(),
            ));
        }
        let mut loop_value = v.as_i32();
        self.exec_eval_cleanup();

        let (v, mut isnull, t, m) = self.exec_eval_expr(upper)?;
        let v = self.exec_cast_value(v, &mut isnull, t, m, vt, vm)?;
        if isnull {
            return Err(exec_err(
                types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                "upper bound of FOR loop cannot be null".to_string(),
            ));
        }
        let end_value = v.as_i32();
        self.exec_eval_cleanup();

        let step_value = if let Some(sx) = step {
            let (v, mut isnull, t, m) = self.exec_eval_expr(sx)?;
            let v = self.exec_cast_value(v, &mut isnull, t, m, vt, vm)?;
            if isnull {
                return Err(exec_err(
                    types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                    "BY value of FOR loop cannot be null".to_string(),
                ));
            }
            self.exec_eval_cleanup();
            let sv = v.as_i32();
            if sv <= 0 {
                return Err(exec_err(
                    types_error::ERRCODE_INVALID_PARAMETER_VALUE,
                    "BY value of FOR loop must be greater than zero".to_string(),
                ));
            }
            sv
        } else {
            1
        };

        let mut found = false;
        let mut rc = RC_OK;
        loop {
            if reverse {
                if loop_value < end_value {
                    break;
                }
            } else if loop_value > end_value {
                break;
            }
            found = true;
            self.set_var(var, Datum::from_i32(loop_value), false);
            rc = self.exec_stmts(body)?;
            if let Some(r) = self.loop_rc(label, rc) {
                rc = r;
                if r != RC_OK {
                    self.exec_set_found_for(found);
                    return Ok(r);
                }
                break;
            }
            // Increment with overflow guard (C checks bounds against i32).
            if reverse {
                match loop_value.checked_sub(step_value) {
                    Some(nv) => loop_value = nv,
                    None => break,
                }
            } else {
                match loop_value.checked_add(step_value) {
                    Some(nv) => loop_value = nv,
                    None => break,
                }
            }
        }
        self.exec_set_found_for(found);
        Ok(rc)
    }

    fn exec_set_found_for(&mut self, found: bool) {
        self.exec_set_found(found);
    }

    // exec_stmt_fors + exec_for_query over SPI cursors.
    fn exec_stmt_fors(
        &mut self,
        label: Option<&str>,
        var: Dno,
        query: &PlExpr,
        body: &'a [PlStmt],
    ) -> PgResult<i32> {
        self.ensure_plan(query, CURSOR_OPT_PARALLEL_OK)?;
        let (plan, paramnos, argtypes) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&query.expr_id).expect("plan ensured");
            (e.plan, e.paramnos.clone(), e.argtypes.clone())
        });
        let (values, nulls) = self.setup_params(&paramnos, &argtypes)?;
        let cursor = SPI_cursor_open(None, plan, &values, &nulls, self.readonly_func)
            .map_err(|e| spi_ctx_err(e, &query.query, query.parse_mode))?;

        let result = self.exec_for_query(label, var, &cursor, body, true);

        // Close only on success (C longjmps past it): a failed intra-loop
        // COMMIT/ROLLBACK already dropped the portal in its abort arm.
        match result {
            Ok(rc) => {
                SPI_cursor_close(cursor)?;
                Ok(rc)
            }
            Err(e) => Err(e),
        }
    }

    fn exec_for_query(
        &mut self,
        label: Option<&str>,
        var: Dno,
        cursor: &SpiCursor,
        body: &'a [PlStmt],
        prefetch_ok: bool,
    ) -> PgResult<i32> {
        let mut found = false;
        let mut rc = RC_OK;
        let prefetch_ok = prefetch_ok && self.atomic;

        // C pins the loop portal so an intra-loop COMMIT/ROLLBACK converts it
        // to held (HoldPinnedPortals) instead of dropping it. On error the
        // pin is cleared by AtCleanup_Portals, as in C.
        portalmem::PinPortal(&cursor.portal)?;
        SPI_cursor_fetch(cursor, true, if prefetch_ok { 10 } else { 1 })?;
        let mut tuptab = spi::SPI_tuptable();
        let mut n = spi::SPI_processed();

        if n == 0 {
            if let Some(t) = tuptab {
                self.move_row_null(var, t)?;
                let _ = spi::SPI_freetuptable(t);
            }
            self.exec_eval_cleanup();
        } else {
            found = true;
        }

        'outer: while n > 0 {
            let t = tuptab.expect("fetch returned rows");
            for i in 0..n as usize {
                self.move_row_from_tuptable(var, t, i)?;
                self.exec_eval_cleanup();
                rc = self.exec_stmts(body)?;
                match self.loop_rc(label, rc) {
                    None => {}
                    Some(r) => {
                        rc = r;
                        let _ = spi::SPI_freetuptable(t);
                        break 'outer;
                    }
                }
                rc = RC_OK;
            }
            let _ = spi::SPI_freetuptable(t);
            SPI_cursor_fetch(cursor, true, if prefetch_ok { 50 } else { 1 })?;
            tuptab = spi::SPI_tuptable();
            n = spi::SPI_processed();
        }

        self.exec_set_found(found);
        portalmem::UnpinPortal(&cursor.portal)?;
        Ok(rc)
    }

    fn rec_desc_of(&self, tuptab: TuptabHandle) -> PgResult<(RecDesc, std::rc::Rc<types_tuple::TupleDescData<'static>>)> {
        let td = spi::tuptable_with(tuptab, |t| {
            tupdesc::CreateTupleDescCopy(self.datum_ctx.mcx(), &t.tupdesc)
        })?;
        Ok((RecDesc::from_tupdesc(&td), std::rc::Rc::new(td)))
    }

    // exec_move_row with a NULL source tuple.
    fn move_row_null(&mut self, var: Dno, tuptab: TuptabHandle) -> PgResult<()> {
        match &self.func.datums[var as usize] {
            PlDatum::Rec(_) => {
                let (desc, src_desc) = self.rec_desc_of(tuptab)?;
                let n = desc.types.len();
                self.move_rec_from_values(
                    var,
                    &desc,
                    src_desc,
                    &vec![Datum::null(); n],
                    &vec![true; n],
                )
            }
            PlDatum::Row(r) => {
                let varnos = r.varnos.clone();
                for dno in varnos {
                    self.exec_assign_value(dno, Datum::null(), true, UNKNOWNOID, -1)?;
                }
                Ok(())
            }
            _ => panic!("plpgsql exec_move_row: bad target datum {var}"),
        }
    }

    // exec_move_row from tuptable row i.
    fn move_row_from_tuptable(&mut self, var: Dno, tuptab: TuptabHandle, i: usize) -> PgResult<()> {
        match &self.func.datums[var as usize] {
            PlDatum::Rec(_) => {
                let (desc, src_desc) = self.rec_desc_of(tuptab)?;
                let natts = desc.types.len();
                let mut values = vec![Datum::null(); natts];
                let mut nulls = vec![true; natts];
                spi::tuptable_with(tuptab, |t| {
                    for f in 0..natts {
                        let (v, isnull) =
                            spi::SPI_getbinval(&t.vals[i], &t.tupdesc, (f + 1) as i32);
                        values[f] = v;
                        nulls[f] = isnull;
                    }
                });
                self.move_rec_from_values(var, &desc, src_desc, &values, &nulls)
            }
            PlDatum::Row(r) => {
                let varnos = r.varnos.clone();
                self.move_row_into_varnos(&varnos, tuptab, i)
            }
            _ => panic!("plpgsql exec_move_row: bad target datum {var}"),
        }
    }

    // exec_move_row_from_fields REC-target arm: a RECORD rec adopts the
    // source shape; a named-composite rec coerces field-by-field onto its
    // declared tupdesc (source read positionally, dropped columns skipped on
    // both sides, missing sources become NULL).
    fn move_rec_from_values(
        &mut self,
        recno: Dno,
        srcdesc: &RecDesc,
        src_tupdesc: std::rc::Rc<types_tuple::TupleDescData<'static>>,
        values: &[Datum],
        nulls: &[bool],
    ) -> PgResult<()> {
        let rectypeid = self.rec_meta(recno).rectypeid;
        if rectypeid == RECORDOID {
            let natts = srcdesc.types.len();
            let mut out_values = values.to_vec();
            for f in 0..natts {
                if !srcdesc.dropped[f] {
                    out_values[f] = self.copy_to_datum_ctx(
                        out_values[f],
                        nulls[f],
                        srcdesc.typlens[f],
                        srcdesc.typbyvals[f],
                    )?;
                }
            }
            self.datums[recno as usize] = DatumVal::Rec(Some(RecValue {
                desc: srcdesc.clone(),
                values: out_values,
                nulls: nulls.to_vec(),
                src_desc: Some(src_tupdesc),
                empty: false,
            }));
            return Ok(());
        }

        let var_td = typcache::lookup_rowtype_tupdesc_copy(self.datum_ctx.mcx(), rectypeid, -1)?;
        let dst = RecDesc::from_tupdesc(&var_td);
        let vtd_natts = dst.types.len();
        let td_natts = srcdesc.types.len();
        let mut newvalues = vec![Datum::null(); vtd_natts];
        let mut newnulls = vec![true; vtd_natts];
        let mut anum = 0usize;
        for fnum in 0..vtd_natts {
            if dst.dropped[fnum] {
                continue;
            }
            while anum < td_natts && srcdesc.dropped[anum] {
                anum += 1;
            }
            let (value, mut isnull, valtype, valtypmod) = if anum < td_natts {
                let r = (values[anum], nulls[anum], srcdesc.types[anum], srcdesc.typmods[anum]);
                anum += 1;
                r
            } else {
                (Datum::null(), true, UNKNOWNOID, -1)
            };
            let v = self.exec_cast_value(
                value,
                &mut isnull,
                valtype,
                valtypmod,
                dst.types[fnum],
                dst.typmods[fnum],
            )?;
            newvalues[fnum] =
                self.copy_to_datum_ctx(v, isnull, dst.typlens[fnum], dst.typbyvals[fnum])?;
            newnulls[fnum] = isnull;
        }
        self.datums[recno as usize] = DatumVal::Rec(Some(RecValue {
            desc: dst,
            values: newvalues,
            nulls: newnulls,
            src_desc: Some(std::rc::Rc::new(var_td)),
            empty: false,
        }));
        Ok(())
    }

    // make_tuple_from_row (pl_exec.c:7491) shaped as a RecValue.
    fn row_as_rec_value(&mut self, rowno: Dno) -> PgResult<RecValue> {
        let (varnos, fieldnames) = match &self.func.datums[rowno as usize] {
            PlDatum::Row(r) => (r.varnos.clone(), r.fieldnames.clone()),
            _ => panic!("plpgsql: datum {rowno} is not a Row"),
        };
        let n = varnos.len();
        let mcx = self.datum_ctx.mcx();
        let mut td = tupdesc::CreateTemplateTupleDesc(mcx, n as i32)?;
        let mut values = Vec::with_capacity(n);
        let mut nulls = Vec::with_capacity(n);
        for (i, &dno) in varnos.iter().enumerate() {
            let (typoid, typmod) = match &self.func.datums[dno as usize] {
                PlDatum::Var(_) => {
                    let t = self.var_type(dno);
                    (t.typoid, t.atttypmod)
                }
                PlDatum::Rec(r) => (r.rectypeid, -1),
                _ => panic!("plpgsql row member {dno} is not a Var or Rec"),
            };
            tupdesc::TupleDescInitEntry(
                &mut td,
                (i + 1) as i16,
                Some(&fieldnames[i]),
                typoid,
                typmod,
                0,
            )?;
            let (v, isnull) = self.datum_as_param(dno)?;
            values.push(v);
            nulls.push(isnull);
        }
        td.tdtypeid = RECORDOID;
        td.tdtypmod = -1;
        let desc = RecDesc::from_tupdesc(&td);
        for i in 0..n {
            values[i] =
                self.copy_to_datum_ctx(values[i], nulls[i], desc.typlens[i], desc.typbyvals[i])?;
        }
        Ok(RecValue {
            desc,
            values,
            nulls,
            src_desc: Some(std::rc::Rc::new(td)),
            empty: false,
        })
    }

    // deconstruct_composite_datum (pl_exec.c:7546) — plain composite Datum.
    pub(crate) fn deconstruct_composite(
        &mut self,
        value: Datum,
    ) -> PgResult<(RecDesc, std::rc::Rc<types_tuple::TupleDescData<'static>>, Vec<Datum>, Vec<bool>)> {
        // SAFETY: non-null composite datum — a live HeapTupleHeader image.
        let td_hdr = unsafe { &*(value.as_usize() as *const types_tuple::HeapTupleHeaderData) };
        let tup_type = td_hdr.type_id();
        let tup_typmod = td_hdr.typmod();
        let t_len = td_hdr.datum_length();
        let tupdesc =
            typcache::lookup_rowtype_tupdesc_copy(self.datum_ctx.mcx(), tup_type, tup_typmod)?;
        let desc = RecDesc::from_tupdesc(&tupdesc);
        let natts = desc.types.len();
        let mut values = vec![Datum::null(); natts];
        let mut nulls = vec![true; natts];
        // SAFETY: header address + declared datum length form the image.
        let htd = unsafe {
            types_tuple::HeapTupleData::from_raw_parts(
                value.as_usize() as *const u8,
                t_len,
                types_tuple::ItemPointerData::invalid(),
                types_core::InvalidOid,
            )
        };
        types_tuple::heap_deform_tuple(&htd, &tupdesc, &mut values, &mut nulls);
        Ok((desc, std::rc::Rc::new(tupdesc), values, nulls))
    }

    fn exec_stmt_return(&mut self, expr: Option<&PlExpr>, retvarno: Dno) -> PgResult<()> {
        if self.func.fn_retset {
            return Ok(());
        }
        if retvarno >= 0 {
            match &self.func.datums[retvarno as usize] {
                PlDatum::Var(v) => {
                    let (value, isnull) = self.get_var(retvarno);
                    self.retval = value;
                    self.retisnull = isnull;
                    self.rettype = v.datatype.typoid;
                    if self.func.fn_retistuple && !self.retisnull {
                        return Err(exec_err(
                            types_error::ERRCODE_DATATYPE_MISMATCH,
                            "cannot return non-composite value from function returning composite type"
                                .to_string(),
                        ));
                    }
                }
                PlDatum::Rec(r) => {
                    let rectypeid = r.rectypeid;
                    if !self.func.fn_retistuple && !self.func.fn_retset {
                        // C keeps retval a composite Datum (ExpandedRecordGetDatum);
                        // scalar-returning functions IO-cast it at exit, so the
                        // ret_rec path must not leave retval unset (null deref in
                        // record_out).
                        let (d, isnull) = self.rec_as_composite_datum(retvarno)?;
                        self.ret_rec = None;
                        self.retval = d;
                        self.retisnull = isnull;
                        self.rettype = rectypeid;
                        return Ok(());
                    }
                    match &self.datums[retvarno as usize] {
                        DatumVal::Rec(Some(rv)) if !rv.empty => {
                            self.ret_rec = Some(rv.clone());
                            self.retisnull = false;
                            self.rettype =
                                if rectypeid != RECORDOID { rectypeid } else { RECORDOID };
                        }
                        _ => {
                            self.ret_rec = None;
                            self.retisnull = true;
                            self.rettype = rectypeid;
                        }
                    }
                }
                PlDatum::Row(row) => {
                    // exec_eval_datum ROW arm: rows materialize through their
                    // member variables (multiple OUT parameters).
                    let rv = self.row_as_rec_value(row.dno)?;
                    self.ret_rec = Some(rv);
                    self.retisnull = false;
                    self.rettype = RECORDOID;
                }
                _ => panic!("plpgsql: bad retvarno"),
            }
            return Ok(());
        }
        if let Some(expr) = expr {
            let (value, isnull, rettype, _typmod) = self.exec_eval_expr(expr)?;
            self.retval = value;
            self.retisnull = isnull;
            self.rettype = rettype;
            if self.func.fn_retistuple
                && !isnull
                && !lsyscache::typ::type_is_rowtype(rettype)?
            {
                return Err(exec_err(
                    types_error::ERRCODE_DATATYPE_MISMATCH,
                    "cannot return non-composite value from function returning composite type"
                        .to_string(),
                ));
            }
            // No exec_eval_cleanup: the value must survive to function exit
            // (nothing runs after RC_RETURN).
            return Ok(());
        }
        // RETURN without expr in a void function (or procedure).
        self.retval = Datum::null();
        self.retisnull = true;
        self.rettype = types_core::InvalidOid;
        Ok(())
    }

    fn exec_stmt_raise(&mut self, stmt: &PlStmt) -> PgResult<i32> {
        let PlStmt::Raise { elog_level, condname, message, params, options, .. } = stmt else {
            unreachable!()
        };

        if condname.is_none() && message.is_none() && options.is_empty() {
            // Bare RAISE: re-throw the active handler's error unchanged.
            if let Some(err) = &self.cur_error {
                return Err(err.clone());
            }
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(types_error::ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER)
                    .errmsg("RAISE without parameters cannot be used outside an exception handler")
                    .into_error(),
            ));
        }

        let mut err_code: Option<SqlState> = None;
        let mut cond: Option<String> = None;
        if let Some(cn) = condname {
            err_code = Some(recognize_err_condition(cn)?);
            cond = Some(cn.clone());
        }

        let mut err_message: Option<String> = None;
        if let Some(msg) = message {
            let mut ds = String::new();
            let bytes = msg.as_bytes();
            let mut pi = 0usize;
            let mut i = 0usize;
            while i < bytes.len() {
                if bytes[i] == b'%' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'%' {
                        ds.push('%');
                        i += 2;
                        continue;
                    }
                    let p = &params[pi];
                    pi += 1;
                    let (v, isnull, t, _m) = self.exec_eval_expr(p)?;
                    if isnull {
                        ds.push_str("<NULL>");
                    } else {
                        ds.push_str(&self.convert_value_to_string(v, t)?);
                    }
                    self.exec_eval_cleanup();
                    i += 1;
                } else {
                    // Preserve raw bytes (message text is server-encoded).
                    ds.push(bytes[i] as char);
                    i += 1;
                }
            }
            err_message = Some(ds);
        }

        let mut err_detail: Option<String> = None;
        let mut err_hint: Option<String> = None;
        let mut err_column: Option<String> = None;
        let mut err_constraint: Option<String> = None;
        let mut err_datatype: Option<String> = None;
        let mut err_table: Option<String> = None;
        let mut err_schema: Option<String> = None;
        for opt in options {
            let (v, isnull, t, _m) = self.exec_eval_expr(&opt.expr)?;
            if isnull {
                return Err(exec_err(
                    types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                    "RAISE statement option cannot be null".to_string(),
                ));
            }
            let extval = self.convert_value_to_string(v, t)?;
            self.exec_eval_cleanup();
            let dup = |name: &str| -> Box<PgError> {
                exec_err(
                    types_error::ERRCODE_SYNTAX_ERROR,
                    format!("RAISE option already specified: {name}"),
                )
            };
            match opt.opt_type {
                PLPGSQL_RAISEOPTION_ERRCODE => {
                    if err_code.is_some() {
                        return Err(dup("ERRCODE"));
                    }
                    err_code = Some(recognize_err_condition(&extval)?);
                    cond = Some(extval);
                }
                PLPGSQL_RAISEOPTION_MESSAGE => {
                    if err_message.is_some() {
                        return Err(dup("MESSAGE"));
                    }
                    err_message = Some(extval);
                }
                PLPGSQL_RAISEOPTION_DETAIL => {
                    if err_detail.is_some() {
                        return Err(dup("DETAIL"));
                    }
                    err_detail = Some(extval);
                }
                PLPGSQL_RAISEOPTION_HINT => {
                    if err_hint.is_some() {
                        return Err(dup("HINT"));
                    }
                    err_hint = Some(extval);
                }
                PLPGSQL_RAISEOPTION_COLUMN => {
                    if err_column.is_some() {
                        return Err(dup("COLUMN"));
                    }
                    err_column = Some(extval);
                }
                PLPGSQL_RAISEOPTION_CONSTRAINT => {
                    if err_constraint.is_some() {
                        return Err(dup("CONSTRAINT"));
                    }
                    err_constraint = Some(extval);
                }
                PLPGSQL_RAISEOPTION_DATATYPE => {
                    if err_datatype.is_some() {
                        return Err(dup("DATATYPE"));
                    }
                    err_datatype = Some(extval);
                }
                PLPGSQL_RAISEOPTION_TABLE => {
                    if err_table.is_some() {
                        return Err(dup("TABLE"));
                    }
                    err_table = Some(extval);
                }
                PLPGSQL_RAISEOPTION_SCHEMA => {
                    if err_schema.is_some() {
                        return Err(dup("SCHEMA"));
                    }
                    err_schema = Some(extval);
                }
                _ => panic!("unrecognized raise option: {}", opt.opt_type),
            }
        }

        if err_code.is_none() && *elog_level >= crate::gram::ELOG_ERROR {
            err_code = Some(types_error::ERRCODE_RAISE_EXCEPTION);
        }
        let err_message = match err_message {
            Some(m) => m,
            None => match cond.take() {
                Some(c) => c,
                // C: unpack_sql_state(err_code) with err_code 0 = "00000".
                None => match err_code {
                    Some(code) => unpack_sql_state(code),
                    None => "00000".to_string(),
                },
            },
        };

        if *elog_level >= crate::gram::ELOG_ERROR {
            let mut b = elog::ereport(ERROR).errmsg_internal(err_message);
            if let Some(c) = err_code {
                b = b.errcode(c);
            }
            if let Some(d) = err_detail {
                b = b.errdetail_internal(d);
            }
            if let Some(h) = err_hint {
                b = b.errhint(h);
            }
            let mut e = b.into_error();
            set_raise_fields(
                &mut e,
                err_column,
                err_constraint,
                err_datatype,
                err_table,
                err_schema,
            );
            return Err(Box::new(e));
        }

        let level = match *elog_level {
            crate::gram::WARNING => types_error::WARNING,
            crate::gram::NOTICE => types_error::NOTICE,
            crate::gram::INFO => types_error::INFO,
            crate::gram::LOG => types_error::LOG,
            _ => types_error::DEBUG1,
        };
        let mut b = elog::ereport(level).errmsg_internal(err_message);
        if let Some(c) = err_code {
            b = b.errcode(c);
        }
        if let Some(d) = err_detail {
            b = b.errdetail_internal(d);
        }
        if let Some(h) = err_hint {
            b = b.errhint(h);
        }
        b.finish(types_error::ErrorLocation::new("pl_exec.c", 0, "exec_stmt_raise"))?;
        Ok(RC_OK)
    }

    fn exec_stmt_assert(&mut self, cond: &PlExpr, message: Option<&PlExpr>) -> PgResult<i32> {
        // plpgsql_check_asserts: DefineCustomBoolVariable is the extension-GUC
        // lane; the SET-created placeholder carries the session value and an
        // unset name means C's default (true).
        let enabled = guc::GetConfigOption("plpgsql.check_asserts", true, false)?
            .map_or(true, |v| !matches!(v.as_str(), "off" | "false" | "no" | "0" | "f" | "n"));
        if !enabled {
            return Ok(RC_OK);
        }
        let (value, isnull) = self.exec_eval_boolean(cond)?;
        self.exec_eval_cleanup();
        if isnull || !value {
            let mut msg: Option<String> = None;
            if let Some(mx) = message {
                let (v, isnull, t, _m) = self.exec_eval_expr(mx)?;
                if !isnull {
                    msg = Some(self.convert_value_to_string(v, t)?);
                }
                self.exec_eval_cleanup();
            }
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(types_error::ERRCODE_ASSERT_FAILURE)
                    .errmsg(msg.unwrap_or_else(|| "assertion failed".to_string()))
                    .into_error(),
            ));
        }
        Ok(RC_OK)
    }

    fn exec_stmt_execsql(
        &mut self,
        expr: &PlExpr,
        into: bool,
        strict: bool,
        target: Dno,
    ) -> PgResult<i32> {
        self.ensure_plan(expr, CURSOR_OPT_PARALLEL_OK)?;
        let (plan, paramnos, argtypes) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&expr.expr_id).expect("plan ensured");
            (e.plan, e.paramnos.clone(), e.argtypes.clone())
        });

        let mod_stmt = spi::SPI_plan_command_tags(plan).iter().any(|&tag| {
            tag == types_portal::CMDTAG_INSERT
                || tag == types_portal::CMDTAG_UPDATE
                || tag == types_portal::CMDTAG_DELETE
                || tag == types_portal::CMDTAG_MERGE
        });

        let tcount: i64 = if into {
            if strict || mod_stmt {
                2
            } else {
                1
            }
        } else {
            0
        };

        let (values, nulls) = self.setup_params(&paramnos, &argtypes)?;
        let _frame = FrameGuard::push_spi(&expr.query, expr.parse_mode);
        let rc = spi::SPI_execute_plan(plan, &values, &nulls, self.readonly_func, tcount)
            .map_err(|e| spi_ctx_err(e, &expr.query, expr.parse_mode))?;

        match rc {
            spi::SPI_OK_SELECT
            | spi::SPI_OK_INSERT
            | spi::SPI_OK_UPDATE
            | spi::SPI_OK_DELETE
            | spi::SPI_OK_MERGE
            | spi::SPI_OK_INSERT_RETURNING
            | spi::SPI_OK_UPDATE_RETURNING
            | spi::SPI_OK_DELETE_RETURNING
            | spi::SPI_OK_MERGE_RETURNING => {
                let found = spi::SPI_processed() != 0;
                self.exec_set_found(found);
            }
            spi::SPI_OK_SELINTO | spi::SPI_OK_UTILITY => {}
            spi::SPI_OK_REWRITTEN => self.exec_set_found(false),
            spi::SPI_ERROR_COPY => {
                return Err(exec_err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "cannot COPY to/from client in PL/pgSQL".to_string(),
                ));
            }
            spi::SPI_ERROR_TRANSACTION => {
                return Err(exec_err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "unsupported transaction command in PL/pgSQL".to_string(),
                ));
            }
            other => panic!(
                "SPI_execute_plan failed executing query \"{}\": rc {other}",
                expr.query
            ),
        }

        self.eval_processed = spi::SPI_processed();

        if into {
            let Some(tuptab) = spi::SPI_tuptable() else {
                return Err(exec_err(
                    types_error::ERRCODE_SYNTAX_ERROR,
                    "INTO used with a command that cannot return data".to_string(),
                ));
            };
            let n = spi::SPI_processed();
            if n == 0 {
                if strict {
                    let _ = spi::SPI_freetuptable(tuptab);
                    let mut b = elog::ereport(ERROR)
                        .errcode(types_error::ERRCODE_NO_DATA_FOUND)
                        .errmsg("query returned no rows");
                    if let Some(d) = self.format_expr_params(expr)? {
                        b = b.errdetail_internal(format!("parameters: {d}"));
                    }
                    return Err(Box::new(b.into_error()));
                }
                self.move_row_null(target, tuptab)?;
                let _ = spi::SPI_freetuptable(tuptab);
            } else {
                if n > 1 && (strict || mod_stmt) {
                    let _ = spi::SPI_freetuptable(tuptab);
                    let mut b = elog::ereport(ERROR)
                        .errcode(types_error::ERRCODE_TOO_MANY_ROWS)
                        .errmsg("query returned more than one row")
                        .errhint("Make sure the query returns a single row, or use LIMIT 1.");
                    if let Some(d) = self.format_expr_params(expr)? {
                        b = b.errdetail_internal(format!("parameters: {d}"));
                    }
                    return Err(Box::new(b.into_error()));
                }
                self.move_row_from_tuptable(target, tuptab, 0)?;
                let _ = spi::SPI_freetuptable(tuptab);
            }
            self.exec_eval_cleanup();
        } else if let Some(tuptab) = spi::SPI_tuptable() {
            let _ = spi::SPI_freetuptable(tuptab);
            let mut b = elog::ereport(ERROR)
                .errcode(types_error::ERRCODE_SYNTAX_ERROR)
                .errmsg("query has no destination for result data");
            if rc == spi::SPI_OK_SELECT {
                b = b.errhint(
                    "If you want to discard the results of a SELECT, use PERFORM instead.",
                );
            }
            return Err(Box::new(b.into_error()));
        }

        Ok(RC_OK)
    }

    // exec_stmt_call (pl_exec.c:2196); DO shares the statement shape but is a
    // named loud at the grammar. procedure_resowner is not carried: it only
    // matters when the callee ends the transaction, and intra-procedure
    // COMMIT/ROLLBACK is loud.
    fn exec_stmt_call(&mut self, expr: &PlExpr, is_call: bool) -> PgResult<i32> {
        self.ensure_plan(expr, 0)?;
        let (plan, paramnos, argtypes) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&expr.expr_id).expect("plan ensured");
            (e.plan, e.paramnos.clone(), e.argtypes.clone())
        });
        // C Assert(!expr->expr_simple_expr): a CALL/DO is never simple.
        debug_assert!(!matches!(self.simple_cache.get(&expr.expr_id), Some(Some(_))));

        let target = if is_call {
            Some(self.make_callstmt_target(expr, plan)?)
        } else {
            None
        };

        let (values, nulls) = self.setup_params(&paramnos, &argtypes)?;
        let before_lxid = current_lxid();
        let rc = spi::SPI_execute_plan_extended(
            plan,
            &values,
            &nulls,
            self.readonly_func,
            true,
            0,
        )
        .map_err(|e| spi_ctx_err(e, &expr.query, expr.parse_mode))?;
        if rc < 0 {
            panic!(
                "SPI_execute_plan_extended failed executing query \"{}\": rc {rc}",
                expr.query
            );
        }
        let after_lxid = current_lxid();
        if before_lxid != after_lxid {
            self.rebuild_simple_exprs();
        }

        let n = spi::SPI_processed();
        let tuptab = spi::SPI_tuptable();
        if n == 1 {
            let t = tuptab.expect("SPI_processed row without tuptable");
            let Some(target) = &target else {
                let _ = spi::SPI_freetuptable(t);
                return Err(exec_err(
                    types_error::ERRCODE_INTERNAL_ERROR,
                    "DO statement returned a row".to_string(),
                ));
            };
            let target = target.clone();
            self.move_row_into_varnos(&target, t, 0)?;
        } else if n > 1 {
            if let Some(t) = tuptab {
                let _ = spi::SPI_freetuptable(t);
            }
            return Err(exec_err(
                types_error::ERRCODE_INTERNAL_ERROR,
                "procedure call returned more than one row".to_string(),
            ));
        }

        self.exec_eval_cleanup();
        if let Some(t) = tuptab {
            let _ = spi::SPI_freetuptable(t);
        }
        Ok(RC_OK)
    }

    // exec_stmt_commit / exec_stmt_rollback (pl_exec.c:4956/4980).
    fn exec_stmt_commit_rollback(&mut self, commit: bool, chain: bool) -> PgResult<i32> {
        match (commit, chain) {
            (true, false) => spi::SPI_commit()?,
            (true, true) => spi::SPI_commit_and_chain()?,
            (false, false) => spi::SPI_rollback()?,
            (false, true) => spi::SPI_rollback_and_chain()?,
        }
        self.rebuild_simple_exprs();
        Ok(RC_OK)
    }

    // C's post-xact-end econtext rebuild (simple_eval_estate = NULL +
    // plpgsql_create_econtext): compiled states and pins here are
    // estate-owned, so dropping the caches and rebuilding lazily is the
    // whole rebuild. Cast states follow C's cast_lxid revalidation.
    fn rebuild_simple_exprs(&mut self) {
        for (_, e) in self.simple_cache.drain() {
            if let Some(se) = e {
                plancache::ReleaseCachedPlan(se.cplan);
            }
        }
        self.cast_cache.clear();
    }

    // make_callstmt_target (pl_exec.c:2288): OUT-arg Params -> row varnos,
    // cached per expr like C's stmt->target (function lifetime).
    fn make_callstmt_target(
        &mut self,
        expr: &PlExpr,
        plan: SpiPlanPtr,
    ) -> PgResult<Vec<Dno>> {
        if let Some(v) = CALL_TARGETS.with(|t| t.borrow().get(&expr.expr_id).cloned()) {
            return Ok(v);
        }
        const PROARGMODE_INOUT: i8 = b'b' as i8;
        const PROARGMODE_OUT: i8 = b'o' as i8;

        let not_call_stmt = || {
            exec_err(
                types_error::ERRCODE_INTERNAL_ERROR,
                "query for CALL statement is not a CallStmt".to_string(),
            )
        };
        let Some((psrc, _)) = spi::SPI_plan_single_source(plan) else {
            return Err(not_call_stmt());
        };
        let cplan = plancache::GetCachedPlan(
            psrc,
            types_portal::ParamListHandle::NULL,
            None,
            types_portal::QueryEnvHandle::NULL,
        )?;
        let built = (|| -> PgResult<Vec<Dno>> {
            let stmts = plancache::CachedPlanStmtList(cplan);
            if stmts.len() != 1 {
                return Err(not_call_stmt());
            }
            let Some(stmt) = stmts[0].utilityStmt.and_then(|u| u.as_call_stmt()) else {
                return Err(not_call_stmt());
            };
            let funcexpr = stmt.funcexpr.expect("analyzed CallStmt has funcexpr");
            let arrays = syscache_seams::pg_proc_result_arrays::call(
                self.eval_ctx.mcx(),
                funcexpr.funcid,
            )?
            .unwrap_or_else(|| {
                panic!("cache lookup failed for function {}", funcexpr.funcid)
            });

            let mut varnos: Vec<Dno> = Vec::new();
            if let Some(argmodes) = &arrays.proargmodes {
                for (i, &mode) in argmodes.iter().enumerate() {
                    if mode != PROARGMODE_INOUT && mode != PROARGMODE_OUT {
                        continue;
                    }
                    let param = (varnos.len() < stmt.outargs.len())
                        .then(|| stmt.outargs.nth(varnos.len()))
                        .and_then(|n| n.as_variant::<types_nodes::primnodes::Param>());
                    let Some(param) = param else {
                        let msg = match arrays
                            .proargnames
                            .as_ref()
                            .and_then(|names| names.get(i))
                            .map(|s| s.as_str())
                            .filter(|s| !s.is_empty())
                        {
                            Some(name) => format!(
                                "procedure parameter \"{name}\" is an output parameter but corresponding argument is not writable"
                            ),
                            None => format!(
                                "procedure parameter {} is an output parameter but corresponding argument is not writable",
                                i + 1
                            ),
                        };
                        return Err(exec_err(types_error::ERRCODE_SYNTAX_ERROR, msg));
                    };
                    // paramid is dno + 1 (make_datum_param).
                    let dno = param.paramid - 1;
                    self.exec_check_assignable(dno)?;
                    varnos.push(dno);
                }
            }
            debug_assert_eq!(varnos.len(), stmt.outargs.len());
            Ok(varnos)
        })();
        plancache::ReleaseCachedPlan(cplan);
        let varnos = built?;
        CALL_TARGETS.with(|t| {
            t.borrow_mut().insert(expr.expr_id, varnos.clone());
        });
        Ok(varnos)
    }

    // exec_check_assignable (pl_exec.c).
    fn exec_check_assignable(&self, dno: Dno) -> PgResult<()> {
        match &self.func.datums[dno as usize] {
            PlDatum::Var(v) => {
                if v.isconst {
                    return Err(exec_err(
                        types_error::ERRCODE_ERROR_IN_ASSIGNMENT,
                        format!("variable \"{}\" is declared CONSTANT", v.refname),
                    ));
                }
                Ok(())
            }
            PlDatum::Rec(_) | PlDatum::Row(_) => Ok(()),
            PlDatum::RecField(f) => self.exec_check_assignable(f.recparentno),
        }
    }

    // exec_move_row scalar-list arm, over an explicit varno list.
    fn move_row_into_varnos(
        &mut self,
        varnos: &[Dno],
        tuptab: TuptabHandle,
        i: usize,
    ) -> PgResult<()> {
        let (desc, _src) = self.rec_desc_of(tuptab)?;
        let natts = desc.types.len();
        let mut anum = 0usize;
        for &dno in varnos {
            while anum < natts && desc.dropped[anum] {
                anum += 1;
            }
            let (v, isnull, vt, vm) = if anum < natts {
                let (v, isnull) = spi::tuptable_with(tuptab, |t| {
                    spi::SPI_getbinval(&t.vals[i], &t.tupdesc, (anum + 1) as i32)
                });
                let r = (v, isnull, desc.types[anum], desc.typmods[anum]);
                anum += 1;
                r
            } else {
                (Datum::null(), true, UNKNOWNOID, -1)
            };
            self.exec_assign_value(dno, v, isnull, vt, vm)?;
        }
        Ok(())
    }

    // exec_eval_using_params: unknown-typed params become text; by-ref
    // values are copied so they survive per-param cleanup.
    #[allow(clippy::type_complexity)]
    fn exec_eval_using_params(
        &mut self,
        params: &[PlExpr],
    ) -> PgResult<(Vec<Oid>, Vec<Datum>, Vec<bool>)> {
        let n = params.len();
        let mut ptypes = Vec::with_capacity(n);
        let mut pvalues = Vec::with_capacity(n);
        let mut pnulls = Vec::with_capacity(n);
        for p in params {
            self.ensure_plan(p, CURSOR_OPT_PARALLEL_OK)?;
            let (mut v, isnull, mut t, _m) = self.exec_eval_expr(p)?;
            if t == UNKNOWNOID {
                t = TEXTOID;
                if !isnull {
                    // SAFETY: unknown-typed datums carry C-string representation.
                    let s = unsafe {
                        core::ffi::CStr::from_ptr(v.as_usize() as *const core::ffi::c_char)
                    };
                    let tv = varlena::cstring_to_text(self.datum_ctx.mcx(), s.to_bytes())?;
                    v = fmgr::varlena_result(tv);
                }
            } else if !isnull {
                let (tl, tbv) = lsyscache::typ::get_typlenbyval(t)?;
                v = self.copy_to_datum_ctx(v, isnull, tl, tbv)?;
            }
            ptypes.push(t);
            pvalues.push(v);
            pnulls.push(isnull);
            self.exec_eval_cleanup();
        }
        Ok((ptypes, pvalues, pnulls))
    }

    // exec_stmt_dynexecute: one-shot SPI execution of a computed query string
    // with USING params; INTO [STRICT] mirrors execsql's destination rules.
    fn exec_stmt_dynexecute(
        &mut self,
        query: &PlExpr,
        into: bool,
        strict: bool,
        target: Dno,
        params: &[PlExpr],
    ) -> PgResult<i32> {
        let (qv, isnull, restype, _m) = self.exec_eval_expr(query)?;
        if isnull {
            return Err(exec_err(
                types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                "query string argument of EXECUTE is null".to_string(),
            ));
        }
        let querystr = self.convert_value_to_string(qv, restype)?;
        self.exec_eval_cleanup();

        let (ptypes, pvalues, pnulls) = self.exec_eval_using_params(params)?;

        let _frame =
            FrameGuard::push_spi(&querystr, parser_seams::RawParseMode::RAW_PARSE_DEFAULT);
        let rc = spi::SPI_execute_extended(&querystr, &ptypes, &pvalues, &pnulls, self.readonly_func)
            .map_err(|e| spi_ctx_err(e, &querystr, parser_seams::RawParseMode::RAW_PARSE_DEFAULT))?;

        match rc {
            spi::SPI_OK_SELECT
            | spi::SPI_OK_INSERT
            | spi::SPI_OK_UPDATE
            | spi::SPI_OK_DELETE
            | spi::SPI_OK_MERGE
            | spi::SPI_OK_INSERT_RETURNING
            | spi::SPI_OK_UPDATE_RETURNING
            | spi::SPI_OK_DELETE_RETURNING
            | spi::SPI_OK_MERGE_RETURNING
            | spi::SPI_OK_UTILITY
            | spi::SPI_OK_REWRITTEN
            | 0 => {}
            spi::SPI_OK_SELINTO => {
                return Err(Box::new(
                    elog::ereport(ERROR)
                        .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("EXECUTE of SELECT ... INTO is not implemented")
                        .errhint(
                            "You might want to use EXECUTE ... INTO or EXECUTE CREATE TABLE ... AS instead.",
                        )
                        .into_error(),
                ));
            }
            spi::SPI_ERROR_COPY => {
                return Err(exec_err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "cannot COPY to/from client in PL/pgSQL".to_string(),
                ));
            }
            spi::SPI_ERROR_TRANSACTION => {
                return Err(exec_err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "EXECUTE of transaction commands is not implemented".to_string(),
                ));
            }
            other => panic!(
                "SPI_execute_extended failed executing query \"{querystr}\": rc {other}"
            ),
        }

        self.eval_processed = spi::SPI_processed();

        if into {
            let Some(tuptab) = spi::SPI_tuptable() else {
                return Err(exec_err(
                    types_error::ERRCODE_SYNTAX_ERROR,
                    "INTO used with a command that cannot return data".to_string(),
                ));
            };
            let n = spi::SPI_processed();
            if n == 0 {
                if strict {
                    let _ = spi::SPI_freetuptable(tuptab);
                    let mut b = elog::ereport(ERROR)
                        .errcode(types_error::ERRCODE_NO_DATA_FOUND)
                        .errmsg("query returned no rows");
                    if let Some(d) = self.format_prepared_params(&ptypes, &pvalues, &pnulls)? {
                        b = b.errdetail_internal(format!("parameters: {d}"));
                    }
                    return Err(Box::new(b.into_error()));
                }
                self.move_row_null(target, tuptab)?;
                let _ = spi::SPI_freetuptable(tuptab);
            } else {
                if n > 1 && strict {
                    let _ = spi::SPI_freetuptable(tuptab);
                    let mut b = elog::ereport(ERROR)
                        .errcode(types_error::ERRCODE_TOO_MANY_ROWS)
                        .errmsg("query returned more than one row");
                    if let Some(d) = self.format_prepared_params(&ptypes, &pvalues, &pnulls)? {
                        b = b.errdetail_internal(format!("parameters: {d}"));
                    }
                    return Err(Box::new(b.into_error()));
                }
                self.move_row_from_tuptable(target, tuptab, 0)?;
                let _ = spi::SPI_freetuptable(tuptab);
            }
            self.exec_eval_cleanup();
        } else if let Some(tuptab) = spi::SPI_tuptable() {
            // Historically EXECUTE without INTO discards any result rows.
            let _ = spi::SPI_freetuptable(tuptab);
        }

        Ok(RC_OK)
    }

    // exec_eval_integer (pl_exec.c): cast to int4.
    fn exec_eval_integer(&mut self, expr: &PlExpr) -> PgResult<(i32, bool)> {
        const INT4OID: Oid = 23;
        let (v, mut isnull, t, m) = self.exec_eval_expr(expr)?;
        let v = self.exec_cast_value(v, &mut isnull, t, m, INT4OID, -1)?;
        Ok((v.as_i32(), isnull))
    }

    // exec_init_tuple_store (pl_exec.c:3669).
    fn init_tuple_store(&mut self) -> PgResult<()> {
        let Some(rsi) = self.rsi else {
            return Err(exec_err(
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                "set-valued function called in context that cannot accept a set".to_string(),
            ));
        };
        if rsi.allowed_modes & fmgr::SFRM_Materialize == 0 || rsi.expected_desc.is_none() {
            return Err(exec_err(
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                "materialize mode required, but it is not allowed in this context".to_string(),
            ));
        }
        // SAFETY: expectedDesc contract — armed by the executor with the scan
        // tupdesc, live for the duration of this call.
        let expected = unsafe {
            rsi.expected_desc
                .expect("checked above")
                .cast::<types_tuple::TupleDescData<'_>>()
                .as_ref()
        };
        let td = tupdesc::CreateTupleDescCopy(self.datum_ctx.mcx(), expected)?;
        let random = rsi.allowed_modes & fmgr::SFRM_Materialize_Random != 0;
        self.tuple_store = Some(tuplestore::Tuplestore::begin_heap(
            random,
            false,
            init_small::globals::work_mem(),
        ));
        self.tuple_store_desc = Some(td);
        Ok(())
    }

    pub fn take_tuple_store(&mut self) -> Option<tuplestore::Tuplestore> {
        self.tuple_store.take()
    }

    // exec_stmt_return_next (pl_exec.c:3326).
    fn exec_stmt_return_next(&mut self, expr: Option<&PlExpr>, retvarno: Dno) -> PgResult<i32> {
        if !self.func.fn_retset {
            return Err(exec_err(
                types_error::ERRCODE_SYNTAX_ERROR,
                "cannot use RETURN NEXT in a non-SETOF function".to_string(),
            ));
        }
        if self.tuple_store.is_none() {
            self.init_tuple_store()?;
        }
        let dst = RecDesc::from_tupdesc(self.tuple_store_desc.as_ref().expect("initialized"));
        let natts = dst.types.len();

        if retvarno >= 0 {
            match &self.func.datums[retvarno as usize] {
                PlDatum::Var(v) => {
                    let (typoid, typmod) = (v.datatype.typoid, v.datatype.atttypmod);
                    if natts != 1 {
                        return Err(exec_err(
                            types_error::ERRCODE_DATATYPE_MISMATCH,
                            "wrong result type supplied in RETURN NEXT".to_string(),
                        ));
                    }
                    let (value, mut isnull) = self.get_var(retvarno);
                    let v = self.exec_cast_value(
                        value,
                        &mut isnull,
                        typoid,
                        typmod,
                        dst.types[0],
                        dst.typmods[0],
                    )?;
                    self.put_tuple_store_values(&[v], &[isnull])?;
                }
                PlDatum::Rec(_) => {
                    if matches!(&self.datums[retvarno as usize], DatumVal::Rec(None)) {
                        self.instantiate_empty_rec(retvarno)?;
                    }
                    let (srcdesc, values, nulls) = match &self.datums[retvarno as usize] {
                        DatumVal::Rec(Some(rv)) => {
                            (rv.desc.clone(), rv.values.clone(), rv.nulls.clone())
                        }
                        _ => unreachable!("instantiated above"),
                    };
                    let (v, n) = convert_values_by_position(
                        &srcdesc,
                        &values,
                        &nulls,
                        &dst,
                        "wrong record type supplied in RETURN NEXT",
                    )?;
                    self.put_tuple_store_values(&v, &n)?;
                }
                PlDatum::Row(row) => {
                    let rv = self.row_as_rec_value(row.dno)?;
                    let (v, n) = convert_values_by_position(
                        &rv.desc,
                        &rv.values,
                        &rv.nulls,
                        &dst,
                        "wrong record type supplied in RETURN NEXT",
                    )?;
                    self.put_tuple_store_values(&v, &n)?;
                }
                _ => panic!("plpgsql: bad retvarno in RETURN NEXT"),
            }
        } else if let Some(expr) = expr {
            let (value, mut isnull, rettype, rettypmod) = self.exec_eval_expr(expr)?;
            if self.func.fn_retistuple {
                if !isnull {
                    if !lsyscache::typ::type_is_rowtype(rettype)? {
                        return Err(exec_err(
                            types_error::ERRCODE_DATATYPE_MISMATCH,
                            "cannot return non-composite value from function returning composite type"
                                .to_string(),
                        ));
                    }
                    let (srcdesc, _src, values, nulls) = self.deconstruct_composite(value)?;
                    let (v, n) = convert_values_by_position(
                        &srcdesc,
                        &values,
                        &nulls,
                        &dst,
                        "returned record type does not match expected record type",
                    )?;
                    self.put_tuple_store_values(&v, &n)?;
                } else {
                    self.put_tuple_store_values(
                        &vec![Datum::null(); natts],
                        &vec![true; natts],
                    )?;
                }
            } else {
                if natts != 1 {
                    return Err(exec_err(
                        types_error::ERRCODE_DATATYPE_MISMATCH,
                        "wrong result type supplied in RETURN NEXT".to_string(),
                    ));
                }
                let v = self.exec_cast_value(
                    value,
                    &mut isnull,
                    rettype,
                    rettypmod,
                    dst.types[0],
                    dst.typmods[0],
                )?;
                self.put_tuple_store_values(&[v], &[isnull])?;
            }
        } else {
            return Err(exec_err(
                types_error::ERRCODE_SYNTAX_ERROR,
                "RETURN NEXT must have a parameter".to_string(),
            ));
        }
        self.exec_eval_cleanup();
        Ok(RC_OK)
    }

    fn put_tuple_store_values(&mut self, values: &[Datum], nulls: &[bool]) -> PgResult<()> {
        let td = self.tuple_store_desc.as_ref().expect("tuple store initialized");
        self.tuple_store
            .as_mut()
            .expect("tuple store initialized")
            .putvalues(td, values, nulls)
    }

    // exec_stmt_return_query (pl_exec.c:3544): rows stream into the
    // tuplestore after a per-batch structure check.
    fn exec_stmt_return_query(
        &mut self,
        query: Option<&PlExpr>,
        dynquery: Option<&PlExpr>,
        params: &[PlExpr],
    ) -> PgResult<i32> {
        if !self.func.fn_retset {
            return Err(exec_err(
                types_error::ERRCODE_SYNTAX_ERROR,
                "cannot use RETURN QUERY in a non-SETOF function".to_string(),
            ));
        }
        if self.tuple_store.is_none() {
            self.init_tuple_store()?;
        }
        let dst = RecDesc::from_tupdesc(self.tuple_store_desc.as_ref().expect("initialized"));

        let ctx_query;
        let rc = if let Some(query) = query {
            ctx_query = query.query.clone();
            self.ensure_plan(query, CURSOR_OPT_PARALLEL_OK)?;
            self.exec_run_select(query, 0)?
        } else {
            let dynquery = dynquery.expect("RETURN QUERY has a query");
            let (qv, isnull, restype, _m) = self.exec_eval_expr(dynquery)?;
            if isnull {
                return Err(exec_err(
                    types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                    "query string argument of EXECUTE is null".to_string(),
                ));
            }
            let querystr = self.convert_value_to_string(qv, restype)?;
            self.exec_eval_cleanup();
            ctx_query = querystr.clone();
            let (ptypes, pvalues, pnulls) = self.exec_eval_using_params(params)?;
            let _frame =
                FrameGuard::push_spi(&querystr, parser_seams::RawParseMode::RAW_PARSE_DEFAULT);
            spi::SPI_execute_extended(&querystr, &ptypes, &pvalues, &pnulls, self.readonly_func)
                .map_err(|e| {
                    spi_ctx_err(e, &querystr, parser_seams::RawParseMode::RAW_PARSE_DEFAULT)
                })?
        };

        // must_return_tuples contract (spi.c:2570).
        let Some(tuptab) = spi::SPI_tuptable() else {
            let tag = match rc {
                spi::SPI_OK_INSERT => "INSERT",
                spi::SPI_OK_UPDATE => "UPDATE",
                spi::SPI_OK_DELETE => "DELETE",
                spi::SPI_OK_MERGE => "MERGE",
                spi::SPI_OK_SELINTO => "SELECT INTO",
                spi::SPI_OK_UTILITY => "UTILITY",
                _ => "SQL",
            };
            return Err(exec_err(
                types_error::ERRCODE_SYNTAX_ERROR,
                format!("{tag} query does not return tuples"),
            ));
        };

        let (srcdesc, _src) = self.rec_desc_of(tuptab)?;
        let n = spi::SPI_processed() as usize;
        let natts = srcdesc.types.len();
        for i in 0..n {
            let mut values = vec![Datum::null(); natts];
            let mut nulls = vec![true; natts];
            spi::tuptable_with(tuptab, |t| {
                for f in 0..natts {
                    let (v, isnull) = spi::SPI_getbinval(&t.vals[i], &t.tupdesc, (f + 1) as i32);
                    values[f] = v;
                    nulls[f] = isnull;
                }
            });
            // C's mismatch fires inside the tuplestore DestReceiver, under
            // the SPI statement context.
            let (v, nn) = convert_values_by_position(
                &srcdesc,
                &values,
                &nulls,
                &dst,
                "structure of query does not match function result type",
            )
            .map_err(|e| {
                spi_ctx_err(e, &ctx_query, parser_seams::RawParseMode::RAW_PARSE_DEFAULT)
            })?;
            self.put_tuple_store_values(&v, &nn)?;
        }
        let _ = spi::SPI_freetuptable(tuptab);
        self.eval_tuptable = None;
        self.exec_eval_cleanup();

        self.eval_processed = n as u64;
        self.exec_set_found(n != 0);
        Ok(RC_OK)
    }

    // ------------------------------------------------------------------
    // Cursors
    // ------------------------------------------------------------------

    fn cursor_var_name(&mut self, curvar: Dno) -> PgResult<Option<String>> {
        let (value, isnull) = self.get_var(curvar);
        if isnull {
            return Ok(None);
        }
        Ok(Some(self.convert_value_to_string(value, TEXTOID)?))
    }

    fn cursor_var_name_required(&mut self, curvar: Dno) -> PgResult<String> {
        match self.cursor_var_name(curvar)? {
            Some(n) => Ok(n),
            None => {
                let refname = match &self.func.datums[curvar as usize] {
                    PlDatum::Var(v) => v.refname.clone(),
                    _ => String::new(),
                };
                Err(exec_err(
                    types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                    format!("cursor variable \"{refname}\" is null"),
                ))
            }
        }
    }

    fn find_open_portal(&mut self, curname: &str) -> PgResult<types_portal::Portal<'static>> {
        match spi::SPI_cursor_find_portal(curname) {
            Some(p) => Ok(p),
            None => Err(exec_err(
                types_error::ERRCODE_UNDEFINED_CURSOR,
                format!("cursor \"{curname}\" does not exist"),
            )),
        }
    }

    // exec_stmt_open's shared tail for bound cursors and OPEN FOR query:
    // check name free, run the cursor's plan into a portal, store the portal
    // name back if the refcursor was null.
    fn open_cursor_portal(
        &mut self,
        curvar: Dno,
        curname: Option<&str>,
        query: &PlExpr,
        cursor_options: i32,
    ) -> PgResult<()> {
        self.ensure_plan(query, cursor_options)?;
        let (plan, paramnos, argtypes) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&query.expr_id).expect("plan ensured");
            (e.plan, e.paramnos.clone(), e.argtypes.clone())
        });
        let (values, nulls) = self.setup_params(&paramnos, &argtypes)?;
        let cursor = spi::SPI_cursor_open(curname, plan, &values, &nulls, self.readonly_func)
            .map_err(|e| spi_ctx_err(e, &query.query, query.parse_mode))?;
        if curname.is_none() {
            let name = cursor.portal.borrow().name.as_str().to_string();
            self.assign_text_var(curvar, &name)?;
        }
        self.exec_eval_cleanup();
        Ok(())
    }

    // exec_stmt_open (pl_exec.c:4657).
    #[allow(clippy::too_many_arguments)]
    fn exec_stmt_open(
        &mut self,
        curvar: Dno,
        cursor_options: i32,
        argquery: Option<&PlExpr>,
        query: Option<&PlExpr>,
        dynquery: Option<&PlExpr>,
        params: &[PlExpr],
    ) -> PgResult<i32> {
        let curname = self.cursor_var_name(curvar)?;
        if let Some(n) = &curname {
            if spi::SPI_cursor_find(n).is_some() {
                return Err(exec_err(
                    types_error::ERRCODE_DUPLICATE_CURSOR,
                    format!("cursor \"{n}\" already in use"),
                ));
            }
        }

        if let Some(q) = query {
            self.open_cursor_portal(curvar, curname.as_deref(), q, cursor_options)?;
            return Ok(RC_OK);
        }
        if let Some(dq) = dynquery {
            let portal =
                self.exec_dynquery_with_params(dq, params, curname.as_deref(), cursor_options)?;
            if curname.is_none() {
                let name = portal.borrow().name.as_str().to_string();
                self.assign_text_var(curvar, &name)?;
            }
            return Ok(RC_OK);
        }

        // OPEN of a bound cursor: evaluate declared args, then its query.
        let (cq, argrow, bound_options) = match &self.func.datums[curvar as usize] {
            PlDatum::Var(v) => (
                v.cursor_explicit_expr.as_ref().expect("bound cursor"),
                v.cursor_explicit_argrow,
                v.cursor_options,
            ),
            _ => panic!("plpgsql: OPEN curvar is not a Var"),
        };
        if let Some(aq) = argquery {
            if argrow < 0 {
                return Err(exec_err(
                    types_error::ERRCODE_SYNTAX_ERROR,
                    "arguments given for cursor without arguments".to_string(),
                ));
            }
            self.exec_stmt_execsql(aq, true, false, argrow)?;
        } else if argrow >= 0 {
            return Err(exec_err(
                types_error::ERRCODE_SYNTAX_ERROR,
                "arguments required for cursor".to_string(),
            ));
        }
        self.open_cursor_portal(curvar, curname.as_deref(), cq, bound_options)?;
        Ok(RC_OK)
    }

    // exec_dynquery_with_params (pl_exec.c:8800): dynamic source into a
    // one-shot cursor portal.
    fn exec_dynquery_with_params(
        &mut self,
        dynquery: &PlExpr,
        params: &[PlExpr],
        curname: Option<&str>,
        cursor_options: i32,
    ) -> PgResult<types_portal::Portal<'static>> {
        let (qv, isnull, restype, _m) = self.exec_eval_expr(dynquery)?;
        if isnull {
            return Err(exec_err(
                types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                "query string argument of EXECUTE is null".to_string(),
            ));
        }
        let querystr = self.convert_value_to_string(qv, restype)?;
        self.exec_eval_cleanup();
        let (ptypes, pvalues, pnulls) = self.exec_eval_using_params(params)?;
        let cursor = spi::SPI_cursor_open_extended(
            curname,
            &querystr,
            &ptypes,
            &pvalues,
            &pnulls,
            self.readonly_func,
            cursor_options,
        )
        .map_err(|e| spi_ctx_err(e, &querystr, parser_seams::RawParseMode::RAW_PARSE_DEFAULT))?;
        Ok(cursor.portal)
    }

    // exec_stmt_fetch / exec_stmt_move (pl_exec.c:4822).
    fn exec_stmt_fetch(
        &mut self,
        target: Dno,
        curvar: Dno,
        direction: i32,
        how_many: i64,
        expr: Option<&PlExpr>,
        is_move: bool,
    ) -> PgResult<i32> {
        let curname = self.cursor_var_name_required(curvar)?;
        let portal = self.find_open_portal(&curname)?;

        let mut how_many = how_many;
        if let Some(e) = expr {
            let (v, isnull) = self.exec_eval_integer(e)?;
            if isnull {
                return Err(exec_err(
                    types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                    "relative or absolute cursor position is null".to_string(),
                ));
            }
            how_many = v as i64;
            self.exec_eval_cleanup();
        }

        let dir = fetch_direction_of(direction);
        let n;
        if !is_move {
            spi::SPI_scroll_cursor_fetch(&portal, dir, how_many)?;
            n = spi::SPI_processed();
            let tuptab = spi::SPI_tuptable().expect("SPI fetch stored tuptable");
            if n == 0 {
                self.move_row_null(target, tuptab)?;
            } else {
                self.move_row_from_tuptable(target, tuptab, 0)?;
            }
            self.exec_eval_cleanup();
            let _ = spi::SPI_freetuptable(tuptab);
        } else {
            spi::SPI_scroll_cursor_move(&portal, dir, how_many)?;
            n = spi::SPI_processed();
        }

        self.eval_processed = n;
        self.exec_set_found(n != 0);
        Ok(RC_OK)
    }

    // exec_stmt_close (pl_exec.c:4913).
    fn exec_stmt_close(&mut self, curvar: Dno) -> PgResult<i32> {
        let curname = self.cursor_var_name_required(curvar)?;
        let portal = self.find_open_portal(&curname)?;
        spi::SPI_cursor_close_portal(&portal)?;
        Ok(RC_OK)
    }

    // exec_stmt_forc (pl_exec.c:2868).
    fn exec_stmt_forc(
        &mut self,
        label: Option<&str>,
        var: Dno,
        curvar: Dno,
        argquery: Option<&PlExpr>,
        body: &'a [PlStmt],
    ) -> PgResult<i32> {
        let curname = self.cursor_var_name(curvar)?;
        if let Some(n) = &curname {
            if spi::SPI_cursor_find(n).is_some() {
                return Err(exec_err(
                    types_error::ERRCODE_DUPLICATE_CURSOR,
                    format!("cursor \"{n}\" already in use"),
                ));
            }
        }

        let (cq, argrow, bound_options) = match &self.func.datums[curvar as usize] {
            PlDatum::Var(v) => (
                v.cursor_explicit_expr.as_ref().expect("bound cursor"),
                v.cursor_explicit_argrow,
                v.cursor_options,
            ),
            _ => panic!("plpgsql: FOR-cursor curvar is not a Var"),
        };
        if let Some(aq) = argquery {
            if argrow < 0 {
                return Err(exec_err(
                    types_error::ERRCODE_SYNTAX_ERROR,
                    "arguments given for cursor without arguments".to_string(),
                ));
            }
            self.exec_stmt_execsql(aq, true, false, argrow)?;
        } else if argrow >= 0 {
            return Err(exec_err(
                types_error::ERRCODE_SYNTAX_ERROR,
                "arguments required for cursor".to_string(),
            ));
        }

        self.ensure_plan(cq, bound_options)?;
        let (plan, paramnos, argtypes) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&cq.expr_id).expect("plan ensured");
            (e.plan, e.paramnos.clone(), e.argtypes.clone())
        });
        let (values, nulls) = self.setup_params(&paramnos, &argtypes)?;
        let cursor =
            spi::SPI_cursor_open(curname.as_deref(), plan, &values, &nulls, self.readonly_func)
                .map_err(|e| spi_ctx_err(e, &cq.query, cq.parse_mode))?;
        if curname.is_none() {
            let name = cursor.portal.borrow().name.as_str().to_string();
            self.assign_text_var(curvar, &name)?;
        }
        self.exec_eval_cleanup();

        // No prefetch: the cursor is user-visible (WHERE CURRENT OF).
        let result = self.exec_for_query(label, var, &cursor, body, false);

        let rc = match result {
            Ok(rc) => {
                spi::SPI_cursor_close(cursor)?;
                rc
            }
            Err(e) => return Err(e),
        };
        if curname.is_none() {
            self.set_var(curvar, Datum::null(), true);
        }
        Ok(rc)
    }

    // exec_stmt_dynfors (pl_exec.c): FOR-over-EXECUTE.
    fn exec_stmt_dynfors(
        &mut self,
        label: Option<&str>,
        var: Dno,
        query: &PlExpr,
        params: &[PlExpr],
        body: &'a [PlStmt],
    ) -> PgResult<i32> {
        let portal = self.exec_dynquery_with_params(query, params, None, 0)?;
        let cursor = spi::SpiCursor::from_portal(portal);
        let result = self.exec_for_query(label, var, &cursor, body, true);
        match result {
            Ok(rc) => {
                spi::SPI_cursor_close(cursor)?;
                Ok(rc)
            }
            Err(e) => Err(e),
        }
    }
}

// build_attrmap_by_position semantics applied to values (attmap.c:64-152):
// typmod-aware positional match over non-dropped columns, missing sources
// disallowed, with C's two errdetail texts under the caller's errmsg.
pub(crate) fn convert_values_by_position(
    src: &RecDesc,
    values: &[Datum],
    nulls: &[bool],
    dst: &RecDesc,
    msg: &str,
) -> PgResult<(Vec<Datum>, Vec<bool>)> {
    #[cold]
    fn mismatch(msg: &str, detail: String) -> Box<PgError> {
        Box::new(
            elog::ereport(ERROR)
                .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                .errmsg(msg.to_string())
                .errdetail(detail)
                .into_error(),
        )
    }
    let n = dst.types.len();
    let mut out_values = vec![Datum::null(); n];
    let mut out_nulls = vec![true; n];
    let mut j = 0usize;
    let mut nincols = 0;
    let mut noutcols = 0;
    for i in 0..n {
        if dst.dropped[i] {
            continue;
        }
        noutcols += 1;
        while j < src.types.len() {
            if src.dropped[j] {
                j += 1;
                continue;
            }
            nincols += 1;
            if dst.types[i] != src.types[j]
                || (dst.typmods[i] != src.typmods[j] && dst.typmods[i] >= 0)
            {
                return Err(mismatch(
                    msg,
                    format!(
                        "Returned type {} does not match expected type {} in column \"{}\" (position {}).",
                        format_type::format_type_with_typemod(src.types[j], src.typmods[j])?,
                        format_type::format_type_with_typemod(dst.types[i], dst.typmods[i])?,
                        dst.names[i],
                        noutcols
                    ),
                ));
            }
            out_values[i] = values[j];
            out_nulls[i] = nulls[j];
            j += 1;
            break;
        }
    }
    let extra = src.types[j..]
        .iter()
        .enumerate()
        .filter(|&(k, _)| !src.dropped[j + k])
        .count();
    if extra > 0 || nincols != noutcols {
        return Err(mismatch(
            msg,
            format!(
                "Number of returned columns ({}) does not match expected column count ({}).",
                nincols + extra,
                noutcols
            ),
        ));
    }
    Ok((out_values, out_nulls))
}

fn fetch_direction_of(direction: i32) -> types_portal::FetchDirection {
    match direction {
        FETCH_FORWARD => types_portal::FetchDirection::FETCH_FORWARD,
        FETCH_BACKWARD => types_portal::FetchDirection::FETCH_BACKWARD,
        FETCH_ABSOLUTE => types_portal::FetchDirection::FETCH_ABSOLUTE,
        FETCH_RELATIVE => types_portal::FetchDirection::FETCH_RELATIVE,
        other => panic!("unrecognized fetch direction: {other}"),
    }
}

// plpgsql_recognize_err_condition(allow_sqlstate=true) returning the state.
fn recognize_err_condition(condname: &str) -> PgResult<SqlState> {
    if condname.len() == 5
        && condname
            .bytes()
            .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
    {
        let b = condname.as_bytes();
        return Ok(types_error::make_sqlstate([b[0], b[1], b[2], b[3], b[4]]));
    }
    for &(name, code) in EXCEPTION_LABEL_MAP {
        if name == condname {
            return Ok(types_error::make_sqlstate(code));
        }
    }
    Err(exec_err(
        types_error::ERRCODE_UNDEFINED_OBJECT,
        format!("unrecognized exception condition \"{condname}\""),
    ))
}

// MyProc->vxid.lxid (exec_stmt_call's before/after transaction check).
fn current_lxid() -> u32 {
    let procno = lmgr_proc::MyProc().expect("MyProc is not set");
    lmgr_proc::GetPGProcByNumber(procno)
        .vxid
        .lxid
        .load(core::sync::atomic::Ordering::Relaxed)
}

fn unpack_sql_state(code: SqlState) -> String {
    String::from_utf8_lossy(&types_error::unpack_sqlstate(code)).into_owned()
}

// exception_matches_conditions: OTHERS (state 0) matches everything except
// query_canceled and assert_failure; category codes match their class.
fn exception_matches_conditions(e: &PgError, conds: &[PlCondition]) -> bool {
    for c in conds {
        let cs = c.sqlerrstate;
        if cs.0 == 0 {
            if e.sqlstate != types_error::ERRCODE_QUERY_CANCELED
                && e.sqlstate != types_error::ERRCODE_ASSERT_FAILURE
            {
                return true;
            }
        } else if e.sqlstate == cs
            || (types_error::errcode_is_category(cs)
                && types_error::errcode_to_category(e.sqlstate) == cs)
        {
            return true;
        }
    }
    false
}

// C bakes every live frame's context line at errfinish; this port attaches a
// frame's line at the first boundary the error crosses in that frame (catch
// site or frame exit). plpgsql_context_attached tracks "already attached for
// the current innermost frame"; frame exit clears it so outer frames attach.
#[cold]
pub(crate) fn attach_frame_context_at_catch(
    e: Box<types_error::PgError>,
    estate: &Estate<'_>,
) -> Box<types_error::PgError> {
    if e.plpgsql_context_attached {
        return e;
    }
    let mut e = attach_frame_line(e, estate);
    e.plpgsql_context_attached = true;
    e
}

#[cold]
pub(crate) fn attach_frame_context_at_exit(
    mut e: Box<types_error::PgError>,
    estate: &Estate<'_>,
) -> Box<types_error::PgError> {
    if e.plpgsql_context_attached {
        e.plpgsql_context_attached = false;
        return e;
    }
    attach_frame_line(e, estate)
}

fn frame_context_line(estate: &Estate<'_>) -> String {
    frame_context_line_of(&estate.frame)
}

fn frame_context_line_of(frame: &FrameShared) -> String {
    let sig = &frame.sig;
    if let Some(t) = frame.text.get() {
        match frame.stmt.get() {
            Some((lineno, _)) if lineno > 0 => {
                format!("PL/pgSQL function {sig} line {lineno} {t}")
            }
            _ => format!("PL/pgSQL function {sig} {t}"),
        }
    } else if let Some((lineno, typename)) = frame.stmt.get() {
        if lineno > 0 {
            format!("PL/pgSQL function {sig} line {lineno} at {typename}")
        } else {
            format!("PL/pgSQL function {sig}")
        }
    } else {
        format!("PL/pgSQL function {sig}")
    }
}

#[cold]
fn attach_frame_line(
    mut e: Box<types_error::PgError>,
    estate: &Estate<'_>,
) -> Box<types_error::PgError> {
    let line = frame_context_line(estate);
    match e.context.take() {
        Some(prev) => e.context = Some(format!("{prev}\n{line}")),
        None => e.context = Some(line),
    }
    e
}

fn set_raise_fields(
    e: &mut PgError,
    column: Option<String>,
    constraint: Option<String>,
    datatype: Option<String>,
    table: Option<String>,
    schema: Option<String>,
) {
    e.column_name = column;
    e.constraint_name = constraint;
    e.datatype_name = datatype;
    e.table_name = table;
    e.schema_name = schema;
}
