// pl_exec.c, phase-1 subset. Statement set: block (no exceptions), assign,
// if, loop/while/fori/fors, exit/continue, return, raise, assert, execsql
// (incl. INTO [STRICT]), perform, getdiag(row_count). Expressions ride SPI
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
// - RAISE below ERROR carries no context line (psql's SHOW_CONTEXT=errors
//   hides server-sent context for notices; wire images differ).
//
// Std collections justified as in ast.rs (invocation-lifetime bookkeeping,
// never per row on a steady path).
use std::collections::HashMap;

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
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;
const UNKNOWNOID: Oid = 705;
const RECORDOID: Oid = 2249;
const VOIDOID: Oid = 2278;

const CURSOR_OPT_PARALLEL_OK: i32 = 0x0100;

pub(crate) struct Ctx(*mut MemoryContext);

impl Ctx {
    pub fn new(name: &'static str) -> Ctx {
        Ctx(Box::into_raw(Box::new(MemoryContext::new(name))))
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

pub struct RecValue {
    pub desc: RecDesc,
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
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
    static EXPR_PLANS: core::cell::RefCell<HashMap<u32, PlanEntry>> =
        core::cell::RefCell::new(HashMap::new());
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
}

pub struct Estate<'a> {
    pub func: &'a PlFunction,
    pub datums: Vec<DatumVal>,
    pub retval: Datum,
    pub retisnull: bool,
    pub rettype: Oid,
    pub eval_processed: u64,
    eval_tuptable: Option<TuptabHandle>,
    pub exitlabel: Option<String>,
    pub readonly_func: bool,
    pub atomic: bool,
    // Stable param image for compiled expressions (address baked into
    // cached ExprStates; the Box never moves while cached states live).
    param_buf: Box<[ParamExternData]>,
    simple_cache: HashMap<u32, Option<SimpleExpr>>,
    cast_cache: HashMap<(Oid, i32, Oid, i32), CastEntry>,
    // Invocation-lifetime var values (C's "procedure" context).
    datum_ctx: Ctx,
    // Per-evaluation scratch (C's eval_mcontext); reset by exec_eval_cleanup.
    eval_ctx: Ctx,
    pub err_stmt: Option<(i32, &'static str)>,
    pub err_text: Option<&'static str>,
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
            eval_processed: 0,
            eval_tuptable: None,
            exitlabel: None,
            readonly_func,
            atomic,
            param_buf: param_buf.into_boxed_slice(),
            simple_cache: HashMap::new(),
            cast_cache: HashMap::new(),
            datum_ctx: Ctx::new("PLpgSQL per-invocation values"),
            eval_ctx: Ctx::new("PLpgSQL eval scratch"),
            err_stmt: None,
            err_text: None,
        }
    }

    fn var_type(&self, dno: Dno) -> &PlType {
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

    // datumCopy into the invocation context (by-ref survives statements).
    fn copy_to_datum_ctx(&self, value: Datum, isnull: bool, typlen: i16, typbyval: bool) -> PgResult<Datum> {
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
        let have = EXPR_PLANS.with(|t| t.borrow().contains_key(&expr.expr_id));
        if have {
            return Ok(());
        }
        let (hooks_names, params_by_dno, recs) = self.build_hook_tables(expr)?;
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
        let hooks = parser_small1::PlpgsqlHookState {
            names: &name_entries,
            params_by_dno: &params_by_dno,
            recs: &rec_names,
            resolve_option: match self.func.resolve_option {
                crate::comp::PLPGSQL_RESOLVE_VARIABLE => {
                    parser_small1::PlpgsqlResolveOption::Variable
                }
                crate::comp::PLPGSQL_RESOLVE_COLUMN => parser_small1::PlpgsqlResolveOption::Column,
                _ => parser_small1::PlpgsqlResolveOption::Error,
            },
            used: &used,
        };
        let plan = spi::SPI_prepare_plpgsql(&expr.query, expr.parse_mode, &hooks, cursor_options)?;
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
        &self,
        expr: &PlExpr,
    ) -> PgResult<(
        Vec<(String, Dno, Oid, i32, Oid)>,
        Vec<Option<(Oid, i32, Oid)>>,
        Vec<String>,
    )> {
        let func = self.func;
        let mut names: Vec<(String, Dno, Oid, i32, Oid)> = Vec::new();
        let mut recs: Vec<String> = Vec::new();
        let have = |names: &Vec<(String, Dno, Oid, i32, Oid)>, k: &str| {
            names.iter().any(|(n, ..)| n == k)
        };

        let mut params_by_dno: Vec<Option<(Oid, i32, Oid)>> = Vec::new();
        for d in &func.datums {
            params_by_dno.push(match d {
                PlDatum::Var(v) => {
                    Some((v.datatype.typoid, v.datatype.atttypmod, v.datatype.collation))
                }
                PlDatum::RecField(f) => self.recfield_type(f)?,
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
                        let info = (
                            key.clone(),
                            v.dno,
                            v.datatype.typoid,
                            v.datatype.atttypmod,
                            v.datatype.collation,
                        );
                        if !have(&names, &key) {
                            names.push(info.clone());
                        }
                        pending.push(info);
                    }
                }
                NsType::Rec => {
                    let recname = item.name.to_ascii_lowercase();
                    let recno = item.itemno;
                    // Whole-record references are unported: a marker entry
                    // (InvalidOid) panics in the resolve hook.
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
                    } else {
                        pending.clear();
                        pending_recs.clear();
                    }
                }
            }
            cur = item.prev;
        }
        Ok((names, params_by_dno, recs))
    }

    // exec_get_datum_type-ish for RECFIELD: type from the rec's live value.
    fn recfield_type(&self, f: &PlRecField) -> PgResult<Option<(Oid, i32, Oid)>> {
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

    fn datum_as_param(&self, dno: Dno) -> PgResult<(Datum, bool)> {
        match &self.func.datums[dno as usize] {
            PlDatum::Var(_) => Ok(self.get_var(dno)),
            PlDatum::RecField(f) => {
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

        // Invalidation check for a cached state.
        if let Some(entry) = self.simple_cache.get(&expr.expr_id) {
            match entry {
                None => return Ok(None),
                Some(se) => {
                    if !plancache::CachedPlanIsValid(se.psrc) {
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
        let rc = spi::SPI_execute_plan(plan, &values, &nulls, self.readonly_func, maxtuples)?;
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
        if !self.cast_cache.contains_key(&key) {
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

        let mut param: Box<[ParamExternData; 1]> = Box::new([ParamExternData {
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

        let mut pstate = parser_small1::make_parsestate(mcx, None);
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
            return Ok(CastEntry { state: None, param });
        };
        // No-op relabeling of the bare placeholder: skip evaluation.
        if let Some(r) = cast_expr.as_relabel_type() {
            if r.arg.as_variant::<Param>().is_some() {
                return Ok(CastEntry { state: None, param });
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
            return Ok(CastEntry { state: None, param });
        };
        state.arm_result_mcx(self.eval_ctx.mcx());
        Ok(CastEntry { state: Some(state), param })
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
        self.exec_stmt_block(block)
    }

    fn exec_stmts(&mut self, stmts: &'a [PlStmt]) -> PgResult<i32> {
        let save = self.err_stmt;
        for s in stmts {
            self.err_stmt = Some((stmt_lineno(s), stmt_typename(s)));
            let rc = self.exec_stmt(s)?;
            if rc != RC_OK {
                self.err_stmt = save;
                return Ok(rc);
            }
        }
        self.err_stmt = save;
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
            PlStmt::GetDiag { items, .. } => {
                for item in items {
                    debug_assert_eq!(item.kind, GETDIAG_ROW_COUNT);
                    let v = Datum::from_i64(self.eval_processed as i64);
                    self.exec_assign_value(item.target, v, false, INT8OID, -1)?;
                }
                self.exec_eval_cleanup();
                Ok(RC_OK)
            }
        }
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
        for &dno in &block.initvarnos {
            match &self.func.datums[dno as usize] {
                PlDatum::Var(v) => {
                    if let Some(default_val) = &v.default_val {
                        self.err_text = Some("during statement block local variable initialization");
                        self.exec_assign_expr(dno, default_val)?;
                        self.err_text = None;
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
                    }
                }
                PlDatum::Rec(_) => {
                    self.datums[dno as usize] = DatumVal::Rec(None);
                }
                _ => {}
            }
        }
        self.exec_stmts(&block.body)
    }

    fn exec_assign_expr(&mut self, target: Dno, expr: &PlExpr) -> PgResult<()> {
        self.ensure_plan(expr, 0)?;
        let (value, isnull, valtype, valtypmod) = self.exec_eval_expr(expr)?;
        self.exec_assign_value(target, value, isnull, valtype, valtypmod)?;
        self.exec_eval_cleanup();
        Ok(())
    }

    // exec_assign_value (Var arm; Rec/RecField assignment is a named loud).
    fn exec_assign_value(
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
            PlDatum::RecField(_) | PlDatum::Rec(_) | PlDatum::Row(_) => panic!(
                "exec_assign_value (pl_exec.c): assignment to record/row targets \
                 unported — unit backend-pl-plpgsql-exec"
            ),
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
        let cursor = SPI_cursor_open(None, plan, &values, &nulls, self.readonly_func)?;

        let result = self.exec_for_query(label, var, &cursor, body, true);

        let close = SPI_cursor_close(cursor);
        match result {
            Ok(rc) => {
                close?;
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
        Ok(rc)
    }

    fn rec_desc_of(tuptab: TuptabHandle) -> RecDesc {
        spi::tuptable_with(tuptab, |t| {
            let natts = t.tupdesc.attrs.len();
            let mut d = RecDesc {
                names: Vec::with_capacity(natts),
                types: Vec::with_capacity(natts),
                typmods: Vec::with_capacity(natts),
                typlens: Vec::with_capacity(natts),
                typbyvals: Vec::with_capacity(natts),
                dropped: Vec::with_capacity(natts),
            };
            for a in t.tupdesc.attrs.iter() {
                d.names
                    .push(String::from_utf8_lossy(a.attname.name_str()).to_ascii_lowercase());
                d.types.push(a.atttypid);
                d.typmods.push(a.atttypmod);
                d.typlens.push(a.attlen);
                d.typbyvals.push(a.attbyval);
                d.dropped.push(a.attisdropped);
            }
            d
        })
    }

    // exec_move_row with a NULL source tuple.
    fn move_row_null(&mut self, var: Dno, tuptab: TuptabHandle) -> PgResult<()> {
        match &self.func.datums[var as usize] {
            PlDatum::Rec(_) => {
                let desc = Self::rec_desc_of(tuptab);
                let n = desc.types.len();
                self.datums[var as usize] = DatumVal::Rec(Some(RecValue {
                    desc,
                    values: vec![Datum::null(); n],
                    nulls: vec![true; n],
                }));
                Ok(())
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
                let desc = Self::rec_desc_of(tuptab);
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
                // Copy by-ref fields into the invocation context (the tuple
                // table is freed per fetch batch).
                for f in 0..natts {
                    if !desc.dropped[f] {
                        values[f] = self.copy_to_datum_ctx(
                            values[f],
                            nulls[f],
                            desc.typlens[f],
                            desc.typbyvals[f],
                        )?;
                    }
                }
                self.datums[var as usize] = DatumVal::Rec(Some(RecValue { desc, values, nulls }));
                Ok(())
            }
            PlDatum::Row(r) => {
                let varnos = r.varnos.clone();
                let desc = Self::rec_desc_of(tuptab);
                let natts = desc.types.len();
                let mut anum = 0usize;
                for dno in varnos {
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
            _ => panic!("plpgsql exec_move_row: bad target datum {var}"),
        }
    }

    fn exec_stmt_return(&mut self, expr: Option<&PlExpr>, retvarno: Dno) -> PgResult<()> {
        if self.func.fn_retset {
            panic!("plpgsql exec_stmt_return: SETOF return unported");
        }
        if retvarno >= 0 {
            match &self.func.datums[retvarno as usize] {
                PlDatum::Var(v) => {
                    let (value, isnull) = self.get_var(retvarno);
                    self.retval = value;
                    self.retisnull = isnull;
                    self.rettype = v.datatype.typoid;
                }
                PlDatum::Rec(_) | PlDatum::Row(_) => panic!(
                    "exec_stmt_return (pl_exec.c): returning record/row variables \
                     unported — unit backend-pl-plpgsql-exec"
                ),
                _ => panic!("plpgsql: bad retvarno"),
            }
            return Ok(());
        }
        if let Some(expr) = expr {
            let (value, isnull, rettype, _typmod) = self.exec_eval_expr(expr)?;
            self.retval = value;
            self.retisnull = isnull;
            self.rettype = rettype;
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
                None => {
                    let code = err_code.expect("errcode set for ERROR levels");
                    unpack_sql_state(code)
                }
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
        let rc = spi::SPI_execute_plan(plan, &values, &nulls, self.readonly_func, tcount)?;

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
                    return Err(Box::new(
                        elog::ereport(ERROR)
                            .errcode(types_error::ERRCODE_NO_DATA_FOUND)
                            .errmsg("query returned no rows")
                            .into_error(),
                    ));
                }
                self.move_row_null(target, tuptab)?;
                let _ = spi::SPI_freetuptable(tuptab);
            } else {
                if n > 1 && (strict || mod_stmt) {
                    let _ = spi::SPI_freetuptable(tuptab);
                    return Err(Box::new(
                        elog::ereport(ERROR)
                            .errcode(types_error::ERRCODE_TOO_MANY_ROWS)
                            .errmsg("query returned more than one row")
                            .errhint("Make sure the query returns a single row, or use LIMIT 1.")
                            .into_error(),
                    ));
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

fn unpack_sql_state(code: SqlState) -> String {
    String::from_utf8_lossy(&types_error::unpack_sqlstate(code)).into_owned()
}

fn set_raise_fields(
    e: &mut PgError,
    column: Option<String>,
    constraint: Option<String>,
    datatype: Option<String>,
    table: Option<String>,
    schema: Option<String>,
) {
    let _ = (e, column, constraint, datatype, table, schema);
    // PG_DIAG_* generic fields: types_error carries no slots for them yet;
    // RAISE ... USING COLUMN/CONSTRAINT/DATATYPE/TABLE/SCHEMA values are
    // accepted but not transported (divergence recorded in notes).
}
