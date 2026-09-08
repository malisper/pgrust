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
//
// Value ownership follows pl_exec.c: the invocation's datum context is an
// AllocSet, a Var's by-ref value is a datumCopy the variable owns (freeval)
// and pfrees on reassignment (assign_simple_var 8817-8825), a record's
// field images are owned per field and freed when the record or the field
// is replaced (assign_record_var 8854-8869 / expanded_record_set_field's
// "not part of the tuple" rule), and a FOR-over-query loop only swaps the
// tuple into the loop record while its tupdesc identity holds (5913-5920).
//
// Std collections justified as in ast.rs (invocation-lifetime bookkeeping,
// never per row on a steady path).
use std::collections::HashMap;

type FxHashMap<K, V> = HashMap<K, V, rustc_hash::FxBuildHasher>;

use datum::{Datum, NullableDatum};
use mcx::{Mcx, MemoryContext};
use spi::{
    SPI_cursor_close, SPI_cursor_fetch, SPI_cursor_open, SPI_cursor_open_with_paramlist,
    SpiCursor, SpiPlanPtr, TuptabHandle,
};
use types_core::{Oid, OidIsValid};
use types_error::{PgError, PgResult, SqlState, ERROR};
use types_portal::params::ParamExternData;

use crate::ast::*;
use crate::errcodes::EXCEPTION_LABEL_MAP;

// CHECK_FOR_INTERRUPTS() (miscadmin.h), the heapam pattern: cold seam call
// gated on the InterruptPending flag. A raised cancel/die comes back as the
// Err; exception_matches_conditions keeps OTHERS from catching it (C parity).
#[cold]
#[inline(never)]
fn process_interrupts() -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()
}

#[inline(always)]
fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return process_interrupts();
    }
    Ok(())
}

pub const RC_OK: i32 = 0;
pub const RC_EXIT: i32 = 1;
pub const RC_RETURN: i32 = 2;
pub const RC_CONTINUE: i32 = 3;

const BOOLOID: Oid = 16;
const INT8OID: Oid = 20;
const TEXTOID: Oid = 25;
const UNKNOWNOID: Oid = 705;
const RECORDOID: Oid = 2249;
const INTERNALOID: Oid = 2281;
const TYPTYPE_DOMAIN: i8 = b'd' as i8;

const CURSOR_OPT_NO_SCROLL: i32 = 0x0004;
const CURSOR_OPT_PARALLEL_OK: i32 = 0x0800;

pub(crate) struct Ctx(*mut MemoryContext);

impl Ctx {
    // Reset-wholesale scratch (C's eval_mcontext / stmt_mcontext discipline).
    pub fn new(name: &'static str) -> Ctx {
        Ctx(Box::into_raw(Box::new(MemoryContext::new_bump(name))))
    }
    // C's datum_context (plpgsql_estate_setup, pl_exec.c:4027: the SPI
    // procedure context, an AllocSet): values are pfree'd one at a time on
    // reassignment, so the backend must give chunks back. Never reset()
    // (the aset reset asserts nothing is still charged); dropped with the
    // estate.
    pub fn new_aset(name: &'static str) -> Ctx {
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
            // attname as stored: expanded_record_lookup_field compares the
            // (scanner-downcased or quoted) field name with namestrcmp,
            // case-sensitively (expandedrecord.c:1032).
            d.names.push(String::from_utf8_lossy(a.attname.name_str()).into_owned());
            d.types.push(a.atttypid);
            d.typmods.push(a.atttypmod);
            d.typlens.push(a.attlen);
            d.typbyvals.push(a.attbyval);
            d.dropped.push(a.attisdropped);
        }
        d
    }
}

// The heap tuple a record was bound from (C's expanded record fvalue, kept
// across field assignments); absent for a record built from fields.
#[derive(Clone, Copy)]
pub struct RecSysAttrs {
    ctid: types_tuple::ItemPointerData,
    tableoid: Oid,
    xmin: u32,
    xmax: u32,
    cmd: u32,
}

impl RecSysAttrs {
    pub fn from_tuple(t: &types_tuple::HeapTupleData<'_>) -> RecSysAttrs {
        let h = t.t_data();
        RecSysAttrs {
            ctid: t.t_self,
            tableoid: t.t_tableOid,
            xmin: h.xmin_raw(),
            xmax: h.xmax_raw(),
            cmd: h.raw_command_id(),
        }
    }

    // heap_getsysattr; the ctid Datum points into self.
    pub fn get(&self, attnum: i16) -> Datum {
        match attnum as i32 {
            types_tuple::SelfItemPointerAttributeNumber => {
                Datum::from_usize(&self.ctid as *const _ as usize)
            }
            types_tuple::MinTransactionIdAttributeNumber => Datum::from_u32(self.xmin),
            types_tuple::MaxTransactionIdAttributeNumber => Datum::from_u32(self.xmax),
            types_tuple::MinCommandIdAttributeNumber
            | types_tuple::MaxCommandIdAttributeNumber => Datum::from_u32(self.cmd),
            types_tuple::TableOidAttributeNumber => Datum::from_oid(self.tableoid),
            other => panic!("invalid attnum: {other}"),
        }
    }
}

pub enum RecFieldRef {
    User(usize),
    Sys(&'static types_tuple::FormData_pg_attribute),
}

// expanded_record_lookup_field: user columns, then system attributes.
pub fn rec_lookup_field(desc: &RecDesc, fieldname: &str) -> Option<RecFieldRef> {
    for (i, n) in desc.names.iter().enumerate() {
        if !desc.dropped[i] && n == fieldname {
            return Some(RecFieldRef::User(i));
        }
    }
    catalog_heap::SystemAttributeByName(fieldname).map(RecFieldRef::Sys)
}

fn compatible_tupdescs(src: &types_tuple::TupleDescData<'_>, dst: &types_tuple::TupleDescData<'_>) -> bool {
    if dst.attrs.len() != src.attrs.len() {
        return false;
    }
    dst.attrs.iter().zip(src.attrs.iter()).all(|(d, s)| {
        if d.attisdropped != s.attisdropped {
            false
        } else if !d.attisdropped {
            d.atttypid == s.atttypid && (d.atttypmod < 0 || d.atttypmod == s.atttypmod)
        } else {
            d.attlen == s.attlen && d.attalign == s.attalign
        }
    })
}

// A deconstructed expanded record (values always deconstructed; src_desc
// keeps the physical tupdesc so the record can re-materialize as a
// composite Datum, dropped columns included).
pub struct RecValue {
    pub desc: RecDesc,
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
    pub src_desc: Option<std::rc::Rc<types_tuple::TupleDescData<'static>>>,
    /// C ExpandedRecordIsEmpty: shape known, no row stored — reads as SQL
    /// NULL as a whole, fields read NULL, assignment makes it non-empty.
    pub empty: bool,
    pub sys: Option<std::rc::Rc<RecSysAttrs>>,
    /// C ER_FLAG_FVALUE_VALID: the stored tuple still matches the fields; a
    /// field assignment clears it but keeps `sys` readable.
    pub fvalue_valid: bool,
    /// Per field: the image in `values` is a copy this record made in the
    /// invocation's datum context (datumCopy layout) and frees when the field
    /// or the record is replaced — expandedrecord.c's "free the old field
    /// value if it is not part of the tuple". False for values that point
    /// into a caller-owned tuple or image.
    pub owned: Vec<bool>,
}

impl Clone for RecValue {
    // A clone shares the field images but not their ownership: the datum
    // entry the value came from frees them exactly once (C: one
    // DeleteExpandedObject per expanded record, assign_record_var).
    fn clone(&self) -> RecValue {
        RecValue {
            desc: self.desc.clone(),
            values: self.values.clone(),
            nulls: self.nulls.clone(),
            src_desc: self.src_desc.clone(),
            empty: self.empty,
            sys: self.sys.clone(),
            fvalue_valid: self.fvalue_valid,
            owned: vec![false; self.values.len()],
        }
    }
}

// A rec materialized into the eval scratch, keyed by the datum's address:
// the stand-in for C's expanded-record pointer identity.
#[derive(Clone)]
struct RecOrigin {
    recno: Dno,
    sys: Option<std::rc::Rc<RecSysAttrs>>,
    fvalue_valid: bool,
}

pub enum DatumVal {
    /// `freeval` is C PLpgSQL_var.freeval: the by-ref value is a datumCopy in
    /// the invocation's datum context that the variable owns and pfrees on
    /// reassignment; false for values that live in the caller's memory
    /// (arguments) or are by-value / NULL.
    Var { value: Datum, isnull: bool, freeval: bool },
    Rec(Option<RecValue>),
    None,
}

// pfree(DatumGetPointer(value)) for a copy made by copy_to_datum_ctx: the
// datumCopy layout (datumGetSize bytes at palloc alignment, datum.c) is
// recomputed from the stored image — the role of C's chunk header — so the
// AllocSet chunk returns to its own freelist. Expanded objects are never
// stored (agg_datum_copy flattens them), so a -1 image is always a flat or
// toast-pointer varlena whose varsize_any is the size that was allocated.
//
// # Safety
// `value` is a non-null by-ref datum previously returned by
// `copy_to_datum_ctx` INTO `mcx`, of `typlen` discipline, with no other
// owner (a freeval Var or an owned RecValue field).
unsafe fn free_datum_copy(mcx: Mcx<'_>, value: Datum, typlen: i16) {
    let p = value.as_usize() as *mut u8;
    if p.is_null() {
        return;
    }
    // SAFETY: forwarded caller contract.
    let size = unsafe {
        match typlen {
            -1 => types_tuple::varatt::varsize_any(p),
            n if n > 0 => n as usize,
            -2 => {
                let mut len = 0usize;
                while *p.add(len) != 0 {
                    len += 1;
                }
                len + 1
            }
            n => panic!("datumGetSize (datum.c): by-ref value with typlen {n} not ported"),
        }
    };
    let layout = core::alloc::Layout::from_size_align(size, 8).expect("datumCopy layout");
    // SAFETY: `p` is the live allocation of exactly this layout in `mcx`.
    unsafe {
        mcx::Allocator::deallocate(&mcx, core::ptr::NonNull::new_unchecked(p), layout);
    }
}

struct PlanEntry {
    plan: SpiPlanPtr,
    // Rc slices: every execution reads these under the EXPR_PLANS borrow and
    // must release the borrow before running SPI — the clone is per
    // execution, so it must be a refcount bump, not a Vec copy (PROCPERF P2).
    paramnos: std::rc::Rc<[Dno]>,
    argtypes: std::rc::Rc<[Oid]>,
    // C stmt->mod_stmt (pl_exec.c exec_stmt_execsql: computed once, cached
    // via mod_stmt_set): command tags are parse-time facts of the plan
    // sources, fixed for the entry's lifetime; a re-prepare rebuilds the
    // entry and recomputes.
    mod_stmt: bool,
    // Owned copy of the parser-hook tables the plan was prepared under, for
    // plancache revalidation (C: parserSetup + expr->func->cur_estate). Types
    // are as-of the last prepare; a between-executions change re-prepares via
    // the ensure_plan probe first, so revalidation only ever sees the tables
    // of the in-flight execution.
    hooks: std::rc::Rc<HookSnapshot>,
    // C PLpgSQL_expr's expr_simple_* fields (pl_exec.c): the compiled
    // simple-expression fast path, FUNCTION lifetime like C's (C stores it
    // in the function's AST; the side table keeps the shared AST immutable).
    // Dropped with the entry (re-prepare, free_function_plans), so a Ready
    // state implies its SPI plan and plansource are live.
    simple: SimpleState,
}

struct HookSnapshot {
    names: Vec<(String, Dno, Oid, i32, Oid)>,
    params_by_dno: Vec<Option<(Oid, i32, Oid)>>,
    arg_dnos: Vec<Dno>,
    recs: Vec<String>,
    valueless: Vec<String>,
    resolve_option: parser_small1::PlpgsqlResolveOption,
}

// RevalidateCachedQuery's re-analysis arm for a plpgsql expression source
// (C: plpgsql_parser_setup with parserSetupArg = expr): re-analyze the
// retained raw tree under the hook tables the plan was prepared with.
fn reanalyze_plpgsql_expr(
    _h: plancache::CachedPlanSourceHandle,
    qmcx: Mcx<'static>,
    raw: &'static types_nodes::rawnodes::RawStmt<'static>,
    query_string: &'static str,
    _param_types: &'static [Oid],
    query_env: types_portal::QueryEnvHandle,
    arg: i32,
) -> PgResult<mcx::PgVec<'static, types_nodes::parsenodes::Query<'static>>> {
    let expr_id = arg as u32;
    let snap = EXPR_PLANS
        .with(|t| t.borrow().get(&expr_id).map(|e| std::rc::Rc::clone(&e.hooks)))
        .expect("reanalyze_plpgsql_expr: plan entry lives while its source does");
    let used = core::cell::RefCell::new(Vec::new());
    let name_entries: Vec<parser_small1::PlpgsqlNameEntry> = snap
        .names
        .iter()
        .map(|(key, dno, t, m, c)| parser_small1::PlpgsqlNameEntry {
            key,
            dno: *dno,
            typoid: *t,
            typmod: *m,
            collation: *c,
        })
        .collect();
    let rec_names: Vec<&str> = snap.recs.iter().map(|s| s.as_str()).collect();
    let valueless_names: Vec<&str> = snap.valueless.iter().map(|s| s.as_str()).collect();
    let hooks = parser_small1::PlpgsqlHookState {
        names: &name_entries,
        params_by_dno: &snap.params_by_dno,
        arg_dnos: &snap.arg_dnos,
        recs: &rec_names,
        valueless_recs: &valueless_names,
        resolve_option: snap.resolve_option,
        used: &used,
    };
    let query =
        analyze_seams::parse_analyze_plpgsql::call(qmcx, raw, query_string, &hooks, query_env)?;
    if query.commandType == types_nodes::nodes_enums::CmdType::CMD_UTILITY {
        let mut v = mcx::PgVec::new_in(qmcx);
        v.try_reserve_exact(1).map_err(|_| qmcx.oom(1))?;
        v.push(query);
        Ok(v)
    } else {
        rewrite_handler_seams::query_rewrite::call(qmcx, query)
    }
}

std::thread_local! {
    // expr_id -> saved SPI plan (C stores expr->plan in the function AST;
    // the side table keeps the shared AST immutable). Entries die with the
    // compiled function (free_function_plans).
    // tls-dtor: try_with-safe — SimpleExpr::drop is proc_exit-guarded and plancache CACHE is ManuallyDrop (dtor-less), so late releases cannot hit a destroyed key.
    static EXPR_PLANS: core::cell::RefCell<FxHashMap<u32, PlanEntry>> =
        core::cell::RefCell::new(FxHashMap::default());
    // expr_id -> CALL OUT-arg row varnos (C stmt->target, cached in fn_cxt).
    static CALL_TARGETS: core::cell::RefCell<FxHashMap<u32, Vec<Dno>>> =
        core::cell::RefCell::new(FxHashMap::default());
}

pub fn free_function_plans(expr_ids: &[u32]) {
    // Entries drop outside the borrow: SimpleExpr::drop releases plan pins
    // and fn_extra memos (arbitrary drop code must not run under the map's
    // RefCell borrow).
    let entries: Vec<PlanEntry> = EXPR_PLANS.with(|t| {
        let mut t = t.borrow_mut();
        expr_ids.iter().filter_map(|id| t.remove(id)).collect()
    });
    for e in entries {
        let PlanEntry { plan, simple, .. } = e;
        // Release the simple-expr plan pin before its plansource goes away
        // with the SPI plan (the reverse order is still safe — plancache
        // tombstones a dropped source while refcounted plans survive — but
        // this keeps the common path off the tombstone lane).
        drop(simple);
        spi::SPI_freeplan(plan);
    }
    CALL_TARGETS.with(|t| {
        let mut t = t.borrow_mut();
        for id in expr_ids {
            t.remove(id);
        }
    });
}

// C PLpgSQL_expr expr_simple_expr/expr_simple_state lifecycle states.
enum SimpleState {
    /// Simplicity not yet determined (C: exec_simple_check_plan not run;
    /// here the check is deferred to the first evaluation, as before).
    Unknown,
    /// Determined not simple (C: expr_simple_expr left NULL, permanently —
    /// pl_exec.c:6036 returns false without ever rechecking).
    NotSimple,
    /// Compiled and idle, ready to evaluate.
    Ready(Box<SimpleExpr>),
    /// Taken out by an in-flight evaluation or (re)build (C
    /// expr_simple_in_use, pl_exec.c:6042): a re-entrant evaluation of the
    /// same expression — recursion via a function called inside it — takes
    /// the SPI path instead. C also uses the flag to quarantine a tree whose
    /// evaluation was aborted by an error until the next transaction
    /// (pl_exec.c:6004-6008); here an error unwind simply never puts the
    /// taken state back, and the next use rebuilds from the plan.
    InUse,
}

// The compiled simple-expression fast path for one expression (C
// PLpgSQL_expr.expr_simple_state + expr_simple_plan/plansource + the type
// fields exec_save_simple_expr stashes, pl_exec.c:8336-8350).
//
// Lifetime: FUNCTION-scoped, revalidated per evaluation. C re-pins the plan
// per transaction (expr_simple_plan_lxid + the resowner arm of
// CachedPlanIsSimplyValid, pl_exec.c:6060-6070) because its pins live in a
// transaction-lifetime resowner, and rebuilds the ExprState per transaction
// (expr_simple_lxid, pl_exec.c:6172) because its memory home — the shared
// simple_eval_estate — dies at transaction end (plpgsql_xact_cb,
// pl_exec.c:8701). Neither constraint exists here: the plan pin is a manual
// refcount and the program owns its memory (`ctx`), so both survive until
// CachedPlanIsSimplyValid fails. That check runs before EVERY evaluation
// (pl_exec.c:6060) and covers everything the compile bakes in: source and
// plan validity reflect relcache invals plus PROCOID/TYPEOID invalItems
// (inlined-function redefinition, ALTER DOMAIN constraint changes — see
// plancache "CoerceToDomain a TYPEOID" dependency note), and the
// search_path match is checked exactly as C does.
struct SimpleExpr {
    // Identity of the SPI plan this was built from; put_simple() refuses to
    // reinstall a state whose plan entry was re-prepared mid-evaluation.
    plan: SpiPlanPtr,
    state: mcx::PgBox<'static, execexpr::ExprState<'static>>,
    cplan: types_portal::CachedPlanHandle,
    psrc: plancache::CachedPlanSourceHandle,
    rettype: Oid,
    rettypmod: i32,
    // C expr_simple_mutable (exec_save_simple_expr, pl_exec.c:8349): only
    // expressions containing mutable functions need the CCI + fresh-snapshot
    // ceremony per evaluation (pl_exec.c:6198-6204).
    mutable: bool,
    // Datum references compile to ParamCallback steps (C EEOP_PARAM_CALLBACK
    // via plpgsql_param_compile, pl_exec.c:6465-6488) that fetch through
    // simple_param_eval from the estate armed for each evaluation
    // (pl_exec.c:6154-6164), only when the program reaches them.
    // Owns every allocation `state` points into; declared last so it drops
    // after `state` (C: the state dies with simple_eval_estate's context).
    #[allow(dead_code)] // never read: held for ownership + drop order only
    ctx: Ctx,
}

impl Drop for SimpleExpr {
    fn drop(&mut self) {
        // fn_extra memos are real heap boxes released only here — arena
        // teardown never runs them (FuncFrame::new_in's contract; the
        // memleak4 estate-reset lesson).
        self.state.release_frames();
        // A FATAL mid-evaluation unwinds a stack-held (taken-out) state
        // AFTER proc_exit's callbacks force-cleared the plancache
        // (ReleaseAllCachedPlansAtExit); releasing then would be a
        // stale-handle panic INSIDE the unwind (abort). The thread is
        // exiting — the pin bookkeeping is already gone with the cache.
        if !elog::config::proc_exit_inprogress() {
            plancache::ReleaseCachedPlan(self.cplan);
        }
    }
}

std::thread_local! {
    // One-shot registration flag for the backend-exit release below.
    static SIMPLE_EXIT_RELEASE: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

// Backend-exit release of every compiled simple expression's plan pin.
// Registered when the first Ready state is created, i.e. strictly AFTER
// plancache's InitPlanCache registered ReleaseAllCachedPlansAtExit; the
// on_proc_exit list drains LIFO, so this runs FIRST — before the plancache
// force-clears its slots. Without it, EXPR_PLANS' thread-local destructor
// would ReleaseCachedPlan into an already-cleared plancache (stale-handle
// panic; TLS destructor order across crates is not a lifecycle).
fn release_simple_states_at_exit(_code: i32, _arg: usize) {
    let orphans: Vec<SimpleState> = EXPR_PLANS.with(|t| {
        let mut t = t.borrow_mut();
        t.values_mut()
            .map(|e| core::mem::replace(&mut e.simple, SimpleState::Unknown))
            .collect()
    });
    // Drops (plan pins, fn_extra) run outside the map borrow, plancache
    // still intact.
    drop(orphans);
}

fn register_simple_exit_release() {
    SIMPLE_EXIT_RELEASE.with(|c| {
        if !c.get() {
            // installed() guard: unit-test rigs run without ipc.
            if ipc_seams::on_proc_exit::is_installed() {
                ipc_seams::on_proc_exit::call(release_simple_states_at_exit, 0);
            }
            c.set(true);
        }
    });
}

// The plpgsql_param_eval_* family behind plpgsql_param_compile's
// EEOP_PARAM_CALLBACK steps (pl_exec.c:6465-6488; _var :6667, _var_ro :6693,
// _recfield :6739, _generic :6803): fetch the datum's CURRENT value when the
// executor reaches the Param, with the per-eval type safety check
// (pl_exec.c:6797/6838) that datum_as_param carries. `arg` is the evaluating
// estate (C paramFetchArg), armed by eval_simple_body.
unsafe fn simple_param_eval(arg: *mut (), paramid: i32, paramtype: Oid) -> PgResult<NullableDatum> {
    assert!(!arg.is_null(), "plpgsql: simple expression evaluated without an armed estate");
    // SAFETY: eval_simple_body arms `arg` from its own `&mut Estate`
    // immediately before running the program and uses nothing else through
    // that borrow until the program returns (C's ecxt_param_list_info->
    // paramFetchArg contract); the evaluation is single-threaded.
    let estate = unsafe { &mut *arg.cast::<Estate<'_>>() };
    // paramid's are 1-based, dnos 0-based (pl_exec.c:6579).
    let (value, isnull) = estate.datum_as_param(paramid - 1, Some(paramtype))?;
    Ok(NullableDatum { value, isnull })
}

// Take the expression's simple state for evaluation, leaving InUse behind
// (C expr_simple_in_use = true). `take_unknown` gates whether an
// undetermined expression may proceed to a build: only the post-ensure_plan
// caller may build (the plan probe there refreshes stale hook tables first).
enum SimpleTake {
    /// Not eligible right now: no entry, not simple, or already in use.
    Skip,
    Ready(Box<SimpleExpr>),
    /// Undetermined; caller owns the build. Carries the plan identity so no
    /// second map borrow is required.
    Build { plan: SpiPlanPtr },
}

fn take_simple(expr_id: u32, take_unknown: bool) -> SimpleTake {
    EXPR_PLANS.with(|t| {
        let mut t = t.borrow_mut();
        let Some(e) = t.get_mut(&expr_id) else {
            return SimpleTake::Skip;
        };
        match e.simple {
            SimpleState::NotSimple | SimpleState::InUse => SimpleTake::Skip,
            SimpleState::Ready(_) => {
                let SimpleState::Ready(se) =
                    core::mem::replace(&mut e.simple, SimpleState::InUse)
                else {
                    unreachable!()
                };
                SimpleTake::Ready(se)
            }
            SimpleState::Unknown => {
                if !take_unknown {
                    return SimpleTake::Skip;
                }
                e.simple = SimpleState::InUse;
                SimpleTake::Build { plan: e.plan }
            }
        }
    })
}

// Put a taken state back (C expr_simple_in_use = false). If the entry was
// re-prepared or freed while the evaluation ran (a nested SPI evaluation of
// the same expression can do both), the taken state is orphaned: dropping it
// releases its pin, and the new entry redetermines from scratch.
fn put_simple(expr_id: u32, plan: SpiPlanPtr, state: SimpleState) {
    let orphan = EXPR_PLANS.with(|t| {
        let mut t = t.borrow_mut();
        if let Some(e) = t.get_mut(&expr_id) {
            if e.plan == plan && matches!(e.simple, SimpleState::InUse) {
                e.simple = state;
                return None;
            }
        }
        Some(state)
    });
    // Foreign drop code (plan pins, fn_extra) runs outside the map borrow.
    drop(orphan);
}

struct CastEntry {
    // None = no-op relabeling.
    state: Option<mcx::PgBox<'static, execexpr::ExprState<'static>>>,
    // Stable slot the compiled Param step points into.
    param: Box<[ParamExternData; 1]>,
    // PlanCacheInvalCounter at build; any bump forces a rebuild (coarse
    // stand-in for C's per-CachedExpression is_valid, pl_exec.c:7982).
    inval_gen: u64,
    // C cast_lxid: the ExprState is rebuilt each transaction so baked-in
    // domain constraint sets stay fresh (get_cast_hashentry, pl_exec.c:8109).
    lxid: u32,
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
    cast_cache: FxHashMap<(Oid, i32, Oid, i32), CastEntry>,
    // Invocation-lifetime var values (C's "procedure" context).
    datum_ctx: Ctx,
    // Per-evaluation scratch (C's eval_mcontext); reset by exec_eval_cleanup.
    eval_ctx: Ctx,
    // Execution-copy datatype changes (C mutates its per-estate datum copies;
    // the shared AST here is immutable). Consulted wherever a Var's declared
    // type feeds plan/param typing.
    var_type_overrides: FxHashMap<Dno, PlType>,
    // C's per-execution rec copy of rectypeid; absent = the compiled OID.
    rec_type_overrides: FxHashMap<Dno, Oid>,
    rec_origins: Vec<(usize, RecOrigin)>,
    pub frame: std::rc::Rc<FrameShared>,
}

// C's plpgsql_exec_error_callback arg, shared with the thread-local context
// stack so GET DIAGNOSTICS PG_CONTEXT can render every live frame without
// aliasing the estates.
pub struct FrameShared {
    pub sig: String,
    pub stmt: core::cell::Cell<Option<(i32, &'static str)>>,
    pub text: core::cell::Cell<Option<&'static str>>,
    // err_var: its declaration lineno wins over the statement's
    // (plpgsql_exec_error_callback, pl_exec.c).
    pub var_lineno: core::cell::Cell<Option<i32>>,
}

enum CtxFrame {
    Pl(std::rc::Rc<FrameShared>),
    Spi(String),
}

std::thread_local! {
    static CONTEXT_FRAMES: core::cell::RefCell<Vec<CtxFrame>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

pub struct FrameGuard(Option<u64>);

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
        FrameGuard(Some(cb))
    }

    // The SPI level's own _SPI_error_callback frame (spi crate
    // SpiErrorContext, pushed by SPI_prepare*/SPI_execute*/SPI_cursor_open*)
    // describes reports raised under it; this frame only mirrors it for
    // GET DIAGNOSTICS PG_CONTEXT (get_error_context_stack).
    fn push_spi(query: &str, mode: parser_seams::RawParseMode) -> FrameGuard {
        let line = spi_context_line(query, mode);
        CONTEXT_FRAMES.with(|s| s.borrow_mut().push(CtxFrame::Spi(line)));
        FrameGuard(None)
    }
}

impl Drop for FrameGuard {
    fn drop(&mut self) {
        if let Some(cb) = self.0 {
            elog::pop_emit_context_callback(cb);
        }
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
#[track_caller]
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
    // Dedup only an immediately-repeated line (same-level double handling by
    // the spi port). C runs its callback once per LEVEL: recursive re-entry
    // legitimately repeats the same expression line at every depth, with the
    // function's own context line interposed between repeats — a whole-
    // context `contains` check collapsed those to one line (wire-metadata
    // workflow, recursive-context finding).
    if e.context.as_deref().and_then(|c| c.lines().last()) == Some(line.as_str()) {
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

// The compiled-param safety check's report (plpgsql_param_eval_recfield /
// plpgsql_param_eval_generic, pl_exec.c:6798-6803): paramid is dno+1.
#[track_caller]
#[cold]
fn param_type_mismatch(dno: Dno, current: Oid, planned: Oid) -> Box<PgError> {
    let cur = format_type::format_type_be(current)
        .unwrap_or_else(|_| format!("type {current}"));
    let plan = format_type::format_type_be(planned)
        .unwrap_or_else(|_| format!("type {planned}"));
    exec_err(
        types_error::ERRCODE_DATATYPE_MISMATCH,
        format!(
            "type of parameter {} ({cur}) does not match that when preparing the plan ({plan})",
            dno + 1
        ),
    )
}

// Dnos with a Param in the analyzed tree.
struct ExternParamDnos(Vec<Dno>);

impl<'mcx> nodes_core::NodeWalker<'mcx> for ExternParamDnos {
    fn visit(&mut self, node: types_nodes::Node<'mcx>) -> PgResult<bool> {
        if let Some(p) = node.as_param() {
            let dno = p.paramid - 1;
            if p.paramkind == types_nodes::primnodes::ParamKind::PARAM_EXTERN
                && !self.0.contains(&dno)
            {
                self.0.push(dno);
            }
            return Ok(false);
        }
        if let Some(q) = node.as_query() {
            return nodes_core::query_tree_walker(q, self, 0);
        }
        nodes_core::expression_tree_walker(node, self)
    }

    fn visit_query_ref(&mut self, q: &'mcx types_nodes::parsenodes::Query<'mcx>) -> PgResult<bool> {
        nodes_core::query_tree_walker(q, self, 0)
    }
}

fn referenced_param_dnos<'mcx>(
    queries: &[types_nodes::parsenodes::Query<'mcx>],
) -> PgResult<Vec<Dno>> {
    let mut w = ExternParamDnos(Vec::new());
    for q in queries {
        nodes_core::query_tree_walker(q, &mut w, 0)?;
    }
    Ok(w.0)
}

// transformPLAssignStmt (analyze.c) hook-resolves the assignment target
// first (so it heads the hook-resolved list) and drops its Param unless
// indirection reads it. C fetches a paramnos member only when a Param
// evaluates it (lazy plpgsql_param_fetch; plpgsql_param_compile steps), so
// `r.f1 := $1` never runs pl_exec.c:6797's check on r.f1 — the RETURN does.
fn drop_unread_assign_target<'mcx>(
    paramnos: &mut Vec<Dno>,
    queries: &[types_nodes::parsenodes::Query<'mcx>],
) -> PgResult<()> {
    if let Some(&target) = paramnos.first() {
        if !referenced_param_dnos(queries)?.contains(&target) {
            paramnos.retain(|&d| d != target);
        }
    }
    Ok(())
}

impl<'a> Estate<'a> {
    pub fn new(func: &'a PlFunction, readonly_func: bool, atomic: bool) -> Estate<'a> {
        let mut datums = Vec::with_capacity(func.datums.len());
        for d in &func.datums {
            datums.push(match d {
                PlDatum::Var(_) => DatumVal::Var { value: Datum::null(), isnull: true, freeval: false },
                PlDatum::Rec(_) => DatumVal::Rec(None),
                _ => DatumVal::None,
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
            cast_cache: FxHashMap::default(),
            datum_ctx: Ctx::new_aset("PLpgSQL per-invocation values"),
            eval_ctx: Ctx::new("PLpgSQL eval scratch"),
            var_type_overrides: FxHashMap::default(),
            rec_type_overrides: FxHashMap::default(),
            rec_origins: Vec::new(),
            frame: std::rc::Rc::new(FrameShared {
                sig: func.fn_signature.clone(),
                stmt: core::cell::Cell::new(None),
                text: core::cell::Cell::new(None),
                var_lineno: core::cell::Cell::new(None),
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

    // assign_simple_var (pl_exec.c:8778-8830) after its detoast arm (which
    // assign_copy_to_datum_ctx runs before the copy): free the previous
    // value if the variable owns it, install the new one and record whether
    // it is ours to free. `freeable` is true only for a copy
    // copy_to_datum_ctx made (by-ref, non-null).
    pub fn set_var(&mut self, dno: Dno, value: Datum, isnull: bool, freeable: bool) {
        let typlen = self.var_type(dno).typlen;
        let mcx = self.datum_ctx.mcx();
        match &mut self.datums[dno as usize] {
            DatumVal::Var { value: v, isnull: n, freeval } => {
                if *freeval {
                    // SAFETY: freeval marks a copy_to_datum_ctx copy in
                    // datum_ctx owned solely by this variable; every reader
                    // of the old image finished before the assignment (the
                    // new value was computed and copied first).
                    unsafe { free_datum_copy(mcx, *v, typlen) };
                }
                *v = value;
                *n = isnull;
                *freeval = freeable && !isnull;
            }
            _ => panic!("plpgsql: assign to non-Var datum {dno}"),
        }
    }

    pub fn get_var(&self, dno: Dno) -> (Datum, bool) {
        match &self.datums[dno as usize] {
            DatumVal::Var { value, isnull, .. } => (*value, *isnull),
            _ => panic!("plpgsql: read of non-Var datum {dno}"),
        }
    }

    fn exec_set_found(&mut self, state: bool) {
        let dno = self.func.found_varno;
        self.set_var(dno, Datum::from_bool(state), false, false);
    }

    // assign_record_var (pl_exec.c:8854-8869): install the new record value
    // and free the old one — its owned field images here; the tupdesc copy
    // goes with the last Rc holder (C: DeleteExpandedObject of the previous
    // expanded record, whose private context held both).
    fn assign_record_var(&mut self, recno: Dno, new: Option<RecValue>) {
        let old = core::mem::replace(&mut self.datums[recno as usize], DatumVal::Rec(new));
        if let DatumVal::Rec(Some(mut rv)) = old {
            self.free_rec_fields(&mut rv);
        }
    }

    fn free_rec_fields(&self, rv: &mut RecValue) {
        let mcx = self.datum_ctx.mcx();
        for f in 0..rv.values.len() {
            if rv.owned[f] {
                // SAFETY: owned marks a copy_to_datum_ctx copy in datum_ctx
                // held only by this record value.
                unsafe { free_datum_copy(mcx, rv.values[f], rv.desc.typlens[f]) };
                rv.owned[f] = false;
            }
        }
    }

    // exec_eval_cleanup.
    fn exec_eval_cleanup(&mut self) {
        if let Some(t) = self.eval_tuptable.take() {
            let _ = spi::SPI_freetuptable(t);
        }
        self.reset_eval_ctx();
    }

    fn reset_eval_ctx(&mut self) {
        self.rec_origins.clear();
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

    // C assign_simple_var / expanded_record_set_* expand_external
    // (pl_exec.c 18.3:8786-8808): when !estate->atomic, stored values must
    // not keep on-disk toast pointers across a commit; expanded datums stay
    // and inline-compressed values stay compressed (detoast_external_attr).
    pub(crate) fn assign_copy_to_datum_ctx(
        &self,
        value: Datum,
        isnull: bool,
        typlen: i16,
        typbyval: bool,
    ) -> PgResult<Datum> {
        if !self.atomic && !isnull && typlen == -1 {
            let p = value.as_usize() as *const u8;
            // SAFETY: non-null by-ref varlena datum.
            unsafe {
                if types_tuple::varatt::varatt_is_1b_e(p)
                    && !types_tuple::varatt::varatt_is_external_expanded(p)
                {
                    // VarlenaRef is the 4B-only lane; external pointers size
                    // via varsize_any.
                    let attr =
                        core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p));
                    // pl_exec.c:8803-8811: detoast in the eval_mcontext
                    // (released at the next exec_eval_cleanup), then
                    // datumCopy into the function's context so the stored
                    // image has the layout free_datum_copy expects.
                    let out = detoast::detoast_external_attr(self.eval_ctx.mcx(), attr)?;
                    return self.copy_to_datum_ctx(
                        Datum::from_usize(out.as_ptr() as usize),
                        false,
                        -1,
                        false,
                    );
                }
            }
        }
        self.copy_to_datum_ctx(value, isnull, typlen, typbyval)
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
            // C's RevalidateCachedQuery re-analyzes in place; this port
            // re-prepares whenever the probe says the source needs re-analysis
            // (invalidation, search_path mismatch, RLS environment change) so
            // the hook tables refresh against current datum/tupdesc types. An
            // invalidation landing after the probe (lock-time sinval) is
            // covered by the installed reanalyze_plpgsql_expr hook instead.
            Some(true) => {
                // Re-prepare: the whole entry goes, including any compiled
                // simple expression riding it — its pin is on the source
                // being dropped (handles are generation-checked; the entry
                // invariant is "a Ready state implies its entry is live").
                // An in-flight evaluation of this expression (InUse) keeps
                // its own pin and put_simple() orphan-drops it on return.
                if let Some(e) =
                    EXPR_PLANS.with(|t| t.borrow_mut().remove(&expr.expr_id))
                {
                    let PlanEntry { plan, simple, .. } = e;
                    drop(simple);
                    let _ = spi::SPI_freeplan(plan);
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
        let resolve_option = match self.func.resolve_option {
            crate::comp::PLPGSQL_RESOLVE_VARIABLE => parser_small1::PlpgsqlResolveOption::Variable,
            crate::comp::PLPGSQL_RESOLVE_COLUMN => parser_small1::PlpgsqlResolveOption::Column,
            _ => parser_small1::PlpgsqlResolveOption::Error,
        };
        let hooks = parser_small1::PlpgsqlHookState {
            names: &name_entries,
            params_by_dno: &params_by_dno,
            arg_dnos: &self.func.fn_argvarnos,
            recs: &rec_names,
            valueless_recs: &valueless_names,
            resolve_option,
            used: &used,
        };
        // Warnings emitted during parse/analysis (scanner escape warnings)
        // internalize their cursor onto the expression text, as under C's
        // _SPI_error_callback during SPI_prepare_extended.
        let prep_frame = FrameGuard::push_spi(&expr.query, expr.parse_mode);
        let plan = spi::SPI_prepare_plpgsql(&expr.query, expr.parse_mode, &hooks, cursor_options)
            .map_err(|e| spi_ctx_err(e, &expr.query, expr.parse_mode))?;
        drop(prep_frame);
        if spi::SPI_keepplan(plan) != 0 {
            panic!("plpgsql exec_prepare_plan: SPI_keepplan failed");
        }
        if let Some((psrc, _)) = spi::SPI_plan_single_source(plan) {
            plancache::SetCachedPlanReanalyze(psrc, reanalyze_plpgsql_expr, expr.expr_id as i32);
        }
        let mut paramnos = used.into_inner();
        if matches!(
            expr.parse_mode,
            parser_seams::RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN1
                | parser_seams::RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN2
                | parser_seams::RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN3
        ) {
            if let Some((psrc, _)) = spi::SPI_plan_single_source(plan) {
                drop_unread_assign_target(&mut paramnos, plancache::SourceQueryList(psrc))?;
            }
        }
        paramnos.sort_unstable();
        let paramnos: std::rc::Rc<[Dno]> = paramnos.into();
        let argtypes: std::rc::Rc<[Oid]> = params_by_dno
            .iter()
            .map(|s| s.map(|(t, _, _)| t).unwrap_or(types_core::InvalidOid))
            .collect();
        // C exec_stmt_execsql's one-time mod_stmt derivation (mod_stmt_set).
        let mod_stmt = spi::SPI_plan_command_tags(plan).iter().any(|&tag| {
            tag == types_portal::CMDTAG_INSERT
                || tag == types_portal::CMDTAG_UPDATE
                || tag == types_portal::CMDTAG_DELETE
                || tag == types_portal::CMDTAG_MERGE
        });
        let hooks = std::rc::Rc::new(HookSnapshot {
            names: hooks_names,
            params_by_dno,
            arg_dnos: self.func.fn_argvarnos.clone(),
            recs,
            valueless,
            resolve_option,
        });
        let old = EXPR_PLANS.with(|t| {
            t.borrow_mut().insert(
                expr.expr_id,
                PlanEntry { plan, paramnos, argtypes, mod_stmt, hooks, simple: SimpleState::Unknown },
            )
        });
        debug_assert!(old.is_none(), "ensure_plan: stale path removed the old entry");
        drop(old);
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
                PlDatum::Rec(r) => {
                    let (t, m) = self.rec_param_type_mod(r.dno)?;
                    Some((t, m, types_core::InvalidOid))
                }
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
                        // Names as the scanners left them (downcased unless
                        // quoted): resolve_column_ref compares with strcmp.
                        let key = item.name.clone();
                        let t = self.var_type(v.dno);
                        let info = (key.clone(), v.dno, t.typoid, t.atttypmod, t.collation);
                        if !have(&names, &key) {
                            names.push(info.clone());
                        }
                        pending.push(info);
                    }
                }
                NsType::Rec => {
                    let recname = item.name.clone();
                    let recno = item.itemno;
                    let (rec_t, rec_m) = self.rec_param_type_mod(recno)?;
                    let marker = (recname.clone(), recno, rec_t, rec_m, types_core::InvalidOid);
                    if !have(&names, &recname) {
                        names.push(marker.clone());
                    }
                    pending.push(marker);
                    // C resolve_column_ref resolves a qualified name via
                    // ns_lookup (nearest rec/row binding wins; Var entries
                    // are skipped for qualified lookups) and only THEN
                    // errors if that rec is unassigned. So the unqualified
                    // `recname` / `recname.field` keys and the valueless
                    // marking belong ONLY to the nearest rec binding for the
                    // name — an outer unassigned rec shadowed by a nearer
                    // same-named rec (e.g. a cursor FOR loop's private
                    // record over an explicitly DECLAREd one,
                    // P2-CURSORFOR-SHADOW) must not poison the prefix, and
                    // fields the nearer rec lacks must not leak through from
                    // the shadowed one. Label-qualified (pending) entries
                    // are unaffected: they resolve by qualified name.
                    let is_visible_rec_binding = !recs.contains(&recname);
                    for d in &func.datums {
                        if let PlDatum::RecField(f) = d {
                            if f.recparentno == recno {
                                if let Some((t, m, c)) = self.recfield_type(f)? {
                                    let key = format!("{recname}.{}", f.fieldname);
                                    let info = (key.clone(), f.dno, t, m, c);
                                    if is_visible_rec_binding && !have(&names, &key) {
                                        names.push(info.clone());
                                    }
                                    pending.push(info);
                                }
                            }
                        }
                    }
                    if is_visible_rec_binding {
                        recs.push(recname.clone());
                    }
                    if matches!(&self.datums[recno as usize], DatumVal::Rec(None))
                        && self.rec_typeid(recno) == RECORDOID
                    {
                        if is_visible_rec_binding {
                            valueless.push(recname.clone());
                        }
                        pending_valueless.push(recname.clone());
                    }
                    pending_recs.push(recname);
                }
                NsType::Row => {}
                NsType::Label => {
                    if !item.name.is_empty() {
                        let label = item.name.clone();
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
    #[allow(dead_code)] // ported helper; see rec_param_type_mod (PR48 composite-coercion lane)
    fn rec_param_type(&self, recno: Dno) -> Oid {
        self.rec_typeid(recno)
    }

    // C plpgsql_exec_get_datum_type_info REC arm (pl_exec.c:5541-5562): a
    // RECORD-declared rec with an expanded record reports the value's own
    // rowtype (erh->er_typeid) — for a trigger's NEW/OLD, bound from the
    // relation's tupdesc, that is the relation's named composite type. The
    // typmod is -1 in every arm ("do NOT return the mutable typmod of a
    // RECORD variable"). A rec without a value (C erh == NULL) reports the
    // declared type. The anonymous-record arm keeps reporting the blessed
    // typmod, which the header stamped by rec_as_composite_datum matches;
    // assign_record_type_typmod dedups by shape so it is stable.
    fn rec_param_type_mod(&mut self, recno: Dno) -> PgResult<(Oid, i32)> {
        let rectypeid = self.rec_typeid(recno);
        if rectypeid != RECORDOID {
            return Ok((rectypeid, -1));
        }
        let DatumVal::Rec(Some(rv)) = &self.datums[recno as usize] else {
            return Ok((RECORDOID, -1));
        };
        let src = rv.src_desc.clone().expect("RecValue carries its source tupdesc");
        if src.tdtypeid != RECORDOID {
            // C: erh->er_typeid (named composite), typmod -1. C reports this
            // for an EMPTY expanded record too — the trigger path builds
            // erh for both NEW and OLD before either holds a tuple.
            return Ok((src.tdtypeid, -1));
        }
        if rv.empty {
            return Ok((RECORDOID, -1));
        }
        if src.tdtypmod >= 0 {
            return Ok((RECORDOID, src.tdtypmod));
        }
        let mcx = self.eval_ctx.mcx();
        let mut td = tupdesc::CreateTupleDescCopy(mcx, &src)?;
        td.tdtypeid = RECORDOID;
        if td.tdtypmod < 0 {
            typcache::assign_record_type_typmod(&mut td)?;
        }
        Ok((RECORDOID, td.tdtypmod))
    }

    // exec_eval_datum REC arm: materialize the record as a composite Datum in
    // the eval scratch (valueless/empty record is a plain NULL).
    fn rec_as_composite_datum(&mut self, recno: Dno) -> PgResult<(Datum, bool)> {
        let rectypeid = self.rec_typeid(recno);
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
        let origin = RecOrigin { recno, sys: rv.sys.clone(), fvalue_valid: rv.fvalue_valid };
        let mcx = self.eval_ctx.mcx();
        let mut td = tupdesc::CreateTupleDescCopy(mcx, &src)?;
        if rectypeid != RECORDOID {
            // C: the expanded record's tupdesc carries er_typeid = the BASE
            // rowtype for a composite-domain rec; the flattened composite
            // datum's header says the base type, while the declared (domain)
            // type is what exec_eval_datum reports.
            td.tdtypeid = Self::rec_base_typeid(rectypeid)?;
            td.tdtypmod = -1;
        } else if td.tdtypeid == RECORDOID {
            if td.tdtypmod < 0 {
                typcache::assign_record_type_typmod(&mut td)?;
            }
        }
        // else: the source tupdesc already carries the value's real rowtype
        // (C exec_eval_datum REC arm, pl_exec.c:5364-5375: a RECORD-declared
        // rec reports erh->er_typeid/er_typmod, the named composite for a
        // trigger's NEW/OLD); stamping RECORD over it made every
        // composite-typed call site reject the value.
        let tup = heaptuple::heap_form_tuple(mcx, &td, &values, &nulls)?;
        let img = tup.header_ptr();
        core::mem::forget(tup);
        self.rec_origins.push((img as usize, origin));
        Ok((Datum::from_usize(img as usize), false))
    }

    fn rec_meta(&self, recno: Dno) -> &PlRec {
        match &self.func.datums[recno as usize] {
            PlDatum::Rec(r) => r,
            _ => panic!("plpgsql: datum {recno} is not a Rec"),
        }
    }

    fn rec_typeid(&self, recno: Dno) -> Oid {
        match self.rec_type_overrides.get(&recno) {
            Some(&t) => t,
            None => self.rec_meta(recno).rectypeid,
        }
    }

    // Re-resolves the rowtype from the name as written once its typcache
    // entry has been invalidated (kept by OID without a name).
    fn revalidate_rectypeid(&mut self, recno: Dno) -> PgResult<()> {
        let rec = self.rec_meta(recno);
        if rec.rectypeid == RECORDOID {
            return Ok(());
        }
        let compiled = rec.rectypeid;
        let ident = rec
            .datatype
            .as_ref()
            .and_then(|t| t.rec_ident.clone())
            .expect("named composite rec carries its typcache identity");
        let cached_id = ident.tcache.borrow().upgrade().map_or(0, |e| e.tupdesc_identifier());
        if cached_id != ident.tupdesc_id.get() {
            if let Some(name) = &ident.origtypname {
                ident.typoid.set(resolve_origtypname(name)?.0);
            }
            let entry = crate::comp::composite_typcache_entry(ident.typoid.get())?;
            ident.tupdesc_id.set(entry.tupdesc_identifier());
            *ident.tcache.borrow_mut() = std::rc::Rc::downgrade(&entry);
        }
        let current = ident.typoid.get();
        if current == compiled {
            self.rec_type_overrides.remove(&recno);
        } else {
            self.rec_type_overrides.insert(recno, current);
        }
        Ok(())
    }

    // A rec declared with a domain-over-composite type (PLPGSQL_TTYPE_REC
    // via pl_comp.c's TYPTYPE_DOMAIN arm) keeps the domain oid in rectypeid;
    // its tupdesc is the base type's (C expandedrecord DOMAIN_BASE_INFO).
    fn rec_typeid_is_domain(rectypeid: Oid) -> PgResult<bool> {
        if rectypeid == RECORDOID {
            return Ok(false);
        }
        Ok(lsyscache::typ::get_typtype(rectypeid)? == TYPTYPE_DOMAIN)
    }

    // C getBaseType via lookup_type_cache DOMAIN_BASE_INFO: the rowtype the
    // rec's tuples physically carry (identity for a plain composite).
    fn rec_base_typeid(rectypeid: Oid) -> PgResult<Oid> {
        if Self::rec_typeid_is_domain(rectypeid)? {
            lsyscache::typ::getBaseType(rectypeid)
        } else {
            Ok(rectypeid)
        }
    }

    // C expandedrecord.c check_domain_on_current_fields /
    // check_domain_for_new_field: materialize the (prospective) field set as
    // a tuple over the base tupdesc and run domain_check against the
    // DECLARED domain type. The tuple lives in the eval scratch (freed by
    // exec_eval_cleanup), like rec_as_composite_datum's materialization.
    fn rec_domain_check_fields(
        &mut self,
        rectypeid: Oid,
        td: &types_tuple::TupleDescData<'static>,
        values: &[Datum],
        nulls: &[bool],
    ) -> PgResult<()> {
        let mcx = self.eval_ctx.mcx();
        let tup = heaptuple::heap_form_tuple(mcx, td, values, nulls)?;
        let img = tup.header_ptr();
        core::mem::forget(tup);
        adt_domains::domain_check(Datum::from_usize(img as usize), false, rectypeid)
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
        self.revalidate_rectypeid(recno)?;
        let rectypeid = self.rec_typeid(recno);
        // C make_expanded_record_from_typeid: a composite-domain rec gets its
        // BASE type's tupdesc (typcache DOMAIN_BASE_INFO).
        let base = Self::rec_base_typeid(rectypeid)?;
        let td = typcache::lookup_rowtype_tupdesc_copy(self.datum_ctx.mcx(), base, -1)?;
        let desc = RecDesc::from_tupdesc(&td);
        let n = desc.types.len();
        self.assign_record_var(
            recno,
            Some(RecValue {
                desc,
                values: vec![Datum::null(); n],
                nulls: vec![true; n],
                src_desc: Some(std::rc::Rc::new(td)),
                empty: true,
                sys: None,
                fvalue_valid: false,
                owned: vec![false; n],
            }),
        );
        Ok(())
    }

    // exec_get_datum_type-ish for RECFIELD: type from the rec's live value;
    // a valueless named-composite rec is instantiated first (C
    // exec_get_datum_type_info). Valueless RECORD recs stay None — the 55000
    // fires only if the SQL expression actually references a field
    // (resolve_column_ref's valueless_recs arm).
    fn recfield_type(&mut self, f: &PlRecField) -> PgResult<Option<(Oid, i32, Oid)>> {
        if matches!(&self.datums[f.recparentno as usize], DatumVal::Rec(None))
            && self.rec_typeid(f.recparentno) != RECORDOID
        {
            self.instantiate_empty_rec(f.recparentno)?;
        }
        if let DatumVal::Rec(Some(rv)) = &self.datums[f.recparentno as usize] {
            match rec_lookup_field(&rv.desc, &f.fieldname) {
                Some(RecFieldRef::User(i)) => {
                    let t = rv.desc.types[i];
                    let coll = lsyscache::typ::get_typcollation(t)?;
                    return Ok(Some((t, rv.desc.typmods[i], coll)));
                }
                Some(RecFieldRef::Sys(a)) => {
                    return Ok(Some((a.atttypid, a.atttypmod, a.attcollation)));
                }
                None => {}
            }
        }
        Ok(None)
    }

    // setup_param_list: current datum values for the plan's paramnos as
    // (values, nulls) views for SPI. (The compiled simple-expression path
    // fetches lazily through simple_param_eval instead.)
    fn setup_params(&mut self, entry_paramnos: &[Dno], argtypes: &[Oid]) -> PgResult<(Vec<Datum>, Vec<bool>)> {
        let n = argtypes.len();
        let mut values = vec![Datum::null(); n];
        let mut nulls = vec![true; n];
        for &dno in entry_paramnos {
            let (v, isnull) = self.datum_as_param(dno, Some(argtypes[dno as usize]))?;
            values[dno as usize] = v;
            nulls[dno as usize] = isnull;
        }
        Ok((values, nulls))
    }

    // Params of a plan _SPI_execute_plan runs are evaluated under its
    // _SPI_error_callback (spi.c), so a fetch failure carries the SQL line.
    fn setup_params_under_spi(
        &mut self,
        expr: &PlExpr,
        entry_paramnos: &[Dno],
        argtypes: &[Oid],
    ) -> PgResult<(Vec<Datum>, Vec<bool>)> {
        self.setup_params(entry_paramnos, argtypes)
            .map_err(|e| spi_ctx_err(e, &expr.query, expr.parse_mode))
    }

    // `planned` = the Param's type as of plan preparation. C's compiled
    // param steps re-check the datum's CURRENT type on every evaluation for
    // record and record-field datums — they can change type under a cached
    // plan (ALTER TABLE beneath %ROWTYPE) — and error rather than misread
    // the datum (plpgsql_param_eval_recfield / _generic "safety check",
    // pl_exec.c:6797/6838). Plain Vars take C's unchecked fast path
    // (plpgsql_param_eval_var). None = caller is not a plan-param seam.
    fn datum_as_param(&mut self, dno: Dno, planned: Option<Oid>) -> PgResult<(Datum, bool)> {
        let func = self.func;
        match &func.datums[dno as usize] {
            PlDatum::Var(_) => Ok(self.get_var(dno)),
            PlDatum::Rec(r) => {
                if let Some(planned) = planned {
                    // plpgsql_param_eval_generic's safety check compares the
                    // type exec_eval_datum reports (pl_exec.c:6836-6843): the
                    // DECLARED type only for a non-RECORD rec; a
                    // RECORD-declared rec holding a value reports the value's
                    // own rowtype (erh->er_typeid) — the same type plan
                    // preparation resolved via rec_param_type_mod.
                    let (current, _) = self.rec_param_type_mod(r.dno)?;
                    if current != planned {
                        return Err(param_type_mismatch(dno, current, planned));
                    }
                }
                self.rec_as_composite_datum(dno)
            }
            PlDatum::RecField(f) => {
                if matches!(&self.datums[f.recparentno as usize], DatumVal::Rec(None))
                    && self.rec_typeid(f.recparentno) != RECORDOID
                {
                    self.instantiate_empty_rec(f.recparentno)?;
                }
                if let DatumVal::Rec(Some(rv)) = &self.datums[f.recparentno as usize] {
                    let found = rec_lookup_field(&rv.desc, &f.fieldname);
                    if let Some(found) = found {
                        let (current, fetched) = match found {
                            RecFieldRef::User(i) => (rv.desc.types[i], (rv.values[i], rv.nulls[i])),
                            RecFieldRef::Sys(a) => (
                                a.atttypid,
                                match &rv.sys {
                                    Some(s) => (s.get(a.attnum), false),
                                    None => (Datum::null(), true),
                                },
                            ),
                        };
                        if let Some(planned) = planned {
                            // plpgsql_param_eval_recfield's per-eval
                            // safety check (pl_exec.c:6797).
                            if current != planned {
                                return Err(param_type_mismatch(dno, current, planned));
                            }
                        }
                        return Ok(fetched);
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
        // Steady state first, before any plan-cache traffic: C's
        // exec_eval_expr (pl_exec.c:5673) prepares the plan once per
        // function lifetime and exec_eval_simple_expr revalidates with
        // CachedPlanIsSimplyValid alone — no GetCachedPlan, no
        // RevalidateCachedQuery, no reanalysis probe. The probe below runs
        // only when this misses (first use, invalidation, search_path
        // change, recursion, not-simple), which is also when its hook-table
        // refresh is actually needed.
        if let Some(r) = self.exec_eval_simple_fast(expr)? {
            return Ok(r);
        }

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

    // Steady-state arm of exec_eval_simple_expr: evaluate through an
    // existing Ready state, touching no plan-cache machinery beyond the
    // per-eval CachedPlanIsSimplyValid gate (C pl_exec.c:6060). Ok(None) =
    // fall to the slow path (which starts with the ensure_plan probe).
    fn exec_eval_simple_fast(
        &mut self,
        expr: &PlExpr,
    ) -> PgResult<Option<(Datum, bool, Oid, i32)>> {
        let se = match take_simple(expr.expr_id, false) {
            SimpleTake::Ready(se) => se,
            SimpleTake::Skip | SimpleTake::Build { .. } => return Ok(None),
        };
        let se = simple_expr_portal_snapshot(expr.expr_id, se)?;
        // Every exit below must restore the InUse slot (an error leaving it
        // InUse would silently demote this expression to SPI forever).
        match plancache::CachedPlanIsSimplyValid(se.psrc, se.cplan) {
            Ok(true) => self.eval_simple_taken(expr, se).map(Some),
            // C's replan arm resets to "not simple" and rebuilds in place
            // (pl_exec.c:6072-6142); here the slow path redetermines after
            // the ensure_plan probe has had its chance to re-prepare with
            // fresh hook tables.
            Ok(false) => {
                let plan = se.plan;
                drop(se);
                put_simple(expr.expr_id, plan, SimpleState::Unknown);
                Ok(None)
            }
            Err(e) => {
                let plan = se.plan;
                drop(se);
                put_simple(expr.expr_id, plan, SimpleState::Unknown);
                Err(e)
            }
        }
    }

    // exec_eval_simple_expr; Ok(None) = not simple, take the SPI path.
    // Caller ran ensure_plan.
    fn exec_eval_simple_expr(
        &mut self,
        expr: &PlExpr,
    ) -> PgResult<Option<(Datum, bool, Oid, i32)>> {
        // After the take, the slot reads InUse; every exit below goes
        // through put_simple.
        let build = match take_simple(expr.expr_id, true) {
            // NotSimple (C expr_simple_expr == NULL, pl_exec.c:6036) or
            // recursion/error quarantine (expr_simple_in_use, :6042).
            SimpleTake::Skip => return Ok(None),
            SimpleTake::Ready(se) => {
                let se = simple_expr_portal_snapshot(expr.expr_id, se)?;
                match plancache::CachedPlanIsSimplyValid(se.psrc, se.cplan) {
                    Ok(true) => return self.eval_simple_taken(expr, se).map(Some),
                    Ok(false) => {}
                    Err(e) => {
                        // Restore the slot (an error leaving InUse would
                        // demote this expression to SPI forever).
                        let plan = se.plan;
                        drop(se);
                        put_simple(expr.expr_id, plan, SimpleState::Unknown);
                        return Err(e);
                    }
                }
                // Invalidated since the fast path last saw it (or the fast
                // path just parked it): release and redetermine — C's
                // replan arm (pl_exec.c:6072-6142) folded into the shared
                // build below, which also re-runs exec_is_simple_query as
                // C does (:6119).
                drop(se);
                EXPR_PLANS.with(|t| {
                    let t = t.borrow();
                    t.get(&expr.expr_id).expect("plan ensured").plan
                })
            }
            SimpleTake::Build { plan } => {
                if let Err(e) = pquery::EnsurePortalSnapshotExists() {
                    put_simple(expr.expr_id, plan, SimpleState::Unknown);
                    return Err(e);
                }
                plan
            }
        };
        let plan = build;
        let se = match self.build_simple_expr(expr, plan) {
            Ok(se) => se,
            Err(e) => {
                put_simple(expr.expr_id, plan, SimpleState::Unknown);
                return Err(e);
            }
        };
        let Some(se) = se else {
            put_simple(expr.expr_id, plan, SimpleState::NotSimple);
            return Ok(None);
        };
        self.eval_simple_taken(expr, se).map(Some)
    }

    // exec_simple_check_plan (pl_exec.c:8133) + exec_save_simple_expr
    // (pl_exec.c:8271), deferred to the first evaluation as before.
    // Ok(None) = not simple. The caller owns the InUse slot.
    fn build_simple_expr(
        &mut self,
        expr: &PlExpr,
        plan: SpiPlanPtr,
    ) -> PgResult<Option<Box<SimpleExpr>>> {
        let Some((psrc, _)) = spi::SPI_plan_single_source(plan) else {
            return Ok(None);
        };
        // Planning errors (const-fold, etc.) carry the parse-mode context
        // line: C wraps GetCachedPlan in _SPI_error_callback
        // (spi.c SPI_plan_get_cached_plan).
        let cplan = plancache::GetCachedPlan(
            psrc,
            types_portal::ParamListHandle::NULL,
            None,
            types_portal::QueryEnvHandle::NULL,
        )
        .map_err(|e| spi_ctx_err(e, &expr.query, expr.parse_mode))?;
        let built = (|| -> PgResult<Option<Box<SimpleExpr>>> {
            // exec_is_simple_query (pl_exec.c): decided on the analyzed
            // querytree, not just the plan shape — an embedded SubPlan
            // (hasSubLinks) survives the bare-Result test below, and C
            // routes every such query through SPI.
            let queries = plancache::SourceQueryList(psrc);
            if queries.len() != 1 || !exec_is_simple_query(&queries[0]) {
                return Ok(None);
            }
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
            let Some(plan_expr) = simple_result_expr(stmt) else {
                return Ok(None);
            };
            // C exec_eval_simple_expr compiles under estate->paramLI, whose
            // paramCompile hook (plpgsql_param_compile, pl_exec.c:6465-6488)
            // turns every datum Param into an EEOP_PARAM_CALLBACK step; the
            // Param node's paramtype is the plan-time type the per-eval
            // safety check compares against (pl_exec.c:6797/6838).
            let bind = types_portal::params::ParamBind {
                param_callback: Some(simple_param_eval),
                ..types_portal::params::ParamBind::NONE
            };
            // Function-lifetime memory home for the compiled program (C
            // compiles into simple_eval_estate->es_query_cxt and rebuilds
            // per transaction, pl_exec.c:6172-6180; see SimpleExpr's
            // lifetime note for why no rebuild is needed here).
            let ctx = Ctx::new("PLpgSQL simple expression");
            let Some(state) = execexpr::exec_init_expr(ctx.mcx(), Some(plan_expr.0), bind)?
            else {
                return Ok(None);
            };
            // C exec_save_simple_expr (pl_exec.c:8349): immutable
            // expressions skip the per-eval CCI + snapshot ceremony.
            let mutable = clauses::contain_mutable_functions(plan_expr.0)?;
            Ok(Some(Box::new(SimpleExpr {
                plan,
                state,
                cplan,
                psrc,
                rettype: plan_expr.1,
                rettypmod: plan_expr.2,
                mutable,
                ctx,
            })))
        })();
        match built {
            Ok(Some(se)) => {
                // First Ready state of this backend: arrange orderly pin
                // release at proc_exit (see release_simple_states_at_exit).
                register_simple_exit_release();
                Ok(Some(se))
            }
            Ok(None) => {
                plancache::ReleaseCachedPlan(cplan);
                Ok(None)
            }
            Err(e) => {
                plancache::ReleaseCachedPlan(cplan);
                Err(e)
            }
        }
    }

    // The evaluation proper (pl_exec.c:6146-6241): write params, arm the
    // result context, snapshot ceremony if needed, run the program. The
    // taken state goes back Ready on success; an error drops it (C
    // quarantines via expr_simple_in_use instead — see SimpleState::InUse).
    fn eval_simple_taken(
        &mut self,
        expr: &PlExpr,
        mut se: Box<SimpleExpr>,
    ) -> PgResult<(Datum, bool, Oid, i32)> {
        let result = self.eval_simple_body(&mut se);
        let plan = se.plan;
        match result {
            Ok(r) => {
                put_simple(expr.expr_id, plan, SimpleState::Ready(se));
                Ok(r)
            }
            Err(e) => {
                drop(se);
                put_simple(expr.expr_id, plan, SimpleState::Unknown);
                Err(e)
            }
        }
    }

    fn eval_simple_body(&mut self, se: &mut SimpleExpr) -> PgResult<(Datum, bool, Oid, i32)> {
        // C pl_exec.c:6154-6164: the program evaluates under an econtext
        // whose ecxt_param_list_info->paramFetchArg is THIS estate; its
        // ParamCallback steps fetch datums through simple_param_eval only
        // when reached (an untaken CASE arm never touches its record).
        se.state.arm_param_callback_arg((self as *mut Estate<'a>).cast());
        // By-ref results land in THIS invocation's eval scratch: the state
        // is shared across estates now, so it re-arms per evaluation (C
        // evaluates under get_eval_mcontext, pl_exec.c:6197).
        se.state.arm_result_mcx(self.eval_ctx.mcx());
        // C pl_exec.c:6198-6204: stable/volatile functions must see our own
        // updates — CCI + fresh snapshot — but immutable-only expressions
        // and read-only functions skip the ceremony.
        let mut pushed = false;
        if se.mutable && !self.readonly_func {
            xact::CommandCounterIncrement()?;
            let snap = snapmgr::GetTransactionSnapshot()?;
            snapmgr::PushActiveSnapshot(&snap)?;
            pushed = true;
        }
        let result = (|| {
            let mut slots = execexpr::EvalSlots { scan: None, inner: None, outer: None };
            let r = execexpr::exec_eval_expr(&mut se.state, &mut slots)?;
            Ok((r.value, r.isnull, se.rettype, se.rettypmod))
        })();
        // The shared program must never keep a pointer to a dead estate.
        se.state.arm_param_callback_arg(core::ptr::null_mut());
        if pushed {
            let popped = snapmgr::PopActiveSnapshot();
            if result.is_ok() {
                popped?;
            }
        }
        result
    }

    // C exec_run_select: non-SELECT is 42601. RETURN QUERY is not this helper.
    fn exec_run_select(&mut self, expr: &PlExpr, maxtuples: i64) -> PgResult<i32> {
        let rc = self.exec_spi_plan(expr, maxtuples, false)?;
        if rc != spi::SPI_OK_SELECT {
            let msg = if rc == spi::SPI_OK_SELINTO {
                "query is SELECT INTO, but it should be plain SELECT"
            } else {
                "query is not a SELECT"
            };
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(types_error::ERRCODE_SYNTAX_ERROR)
                    .errmsg(msg)
                    .errcontext_msg(format!("query: {}", expr.query))
                    .into_error(),
            ));
        }
        Ok(rc)
    }

    // C: SPI_execute_plan_extended with options.params = setup_param_list
    // (hooked) and options.must_return_tuples per the statement (RETURN
    // QUERY passes true, pl_exec.c exec_stmt_return_query).
    fn exec_spi_plan(
        &mut self,
        expr: &PlExpr,
        maxtuples: i64,
        must_return_tuples: bool,
    ) -> PgResult<i32> {
        let (plan, paramnos, argtypes) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&expr.expr_id).expect("plan ensured");
            (e.plan, e.paramnos.clone(), e.argtypes.clone())
        });
        let (values, nulls) = self.setup_params_under_spi(expr, &paramnos, &argtypes)?;
        let _frame = FrameGuard::push_spi(&expr.query, expr.parse_mode);
        let rc = spi::SPI_execute_plan_extended(
            plan,
            &values,
            &nulls,
            true,
            self.readonly_func,
            false,
            must_return_tuples,
            maxtuples,
        )
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
        let cur_lxid = current_lxid();
        let stale = match self.cast_cache.get(&key) {
            Some(e) => e.inval_gen != cur_gen || e.lxid != cur_lxid,
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
                // upstream 54649de65f08 (18.6): Reject calls from SQL to functions that take or return type internal.
                if srctype == INTERNALOID || dsttype == INTERNALOID {
                    return Err(exec_err(
                        types_error::ERRCODE_CANNOT_COERCE,
                        format!(
                            "cannot cast type {} to {}",
                            format_type::format_type_be(srctype)?,
                            format_type::format_type_be(dsttype)?
                        ),
                    ));
                }
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
            return Ok(CastEntry { state: None, param, inval_gen, lxid: current_lxid() });
        };
        // No-op relabeling of the bare placeholder: skip evaluation.
        if let Some(r) = cast_expr.as_relabel_type() {
            if r.arg.as_variant::<Param>().is_some() {
                return Ok(CastEntry { state: None, param, inval_gen, lxid: current_lxid() });
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
            param_callback: None,
        };
        let Some(mut state) = execexpr::exec_init_expr(mcx, Some(cast_expr), bind)? else {
            return Ok(CastEntry { state: None, param, inval_gen, lxid: current_lxid() });
        };
        state.arm_result_mcx(self.eval_ctx.mcx());
        Ok(CastEntry { state: Some(state), param, inval_gen, lxid: current_lxid() })
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
        // Byte-faithful conversion, matching C's convert_value_to_string, which
        // returns the type output function's server-encoding bytes verbatim.
        // NEVER lossily substitute U+FFFD for non-UTF-8 bytes: the result feeds
        // dynamic-SQL text (EXECUTE), RAISE messages, and USING-param formatting,
        // and silent substitution would corrupt the executed statement. Valid
        // bytes pass through unchanged; genuinely invalid input raises a
        // catchable encoding error instead of being silently mangled.
        let bytes = s.to_bytes();
        match core::str::from_utf8(bytes) {
            Ok(v) => Ok(v.to_owned()),
            Err(e) => {
                let start = e.valid_up_to();
                let n = e.error_len().unwrap_or(bytes.len() - start).max(1);
                let mut seq = String::new();
                for b in &bytes[start..(start + n).min(bytes.len())] {
                    if !seq.is_empty() {
                        seq.push(' ');
                    }
                    seq.push_str(&format!("0x{b:02x}"));
                }
                Err(exec_err(
                    types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,
                    format!("invalid byte sequence for encoding \"UTF8\": {seq}"),
                ))
            }
        }
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

    // exec_eval_datum's reported type for a parameter datum: the datum's OWN
    // current type, exactly as C's exec_eval_datum yields it. This is what
    // format_expr_params must format each value with — NOT the type stored in
    // the (mutable) EXPR_PLANS entry. The plan entry's argtypes can be swapped
    // out from under an outer frame by a nested re-preparation whose estate
    // holds a different type for the same dno (e.g. a RECORD field that is
    // TEXTOID in the nested estate but int8 in the outer one). Formatting the
    // outer datum with the plan's (now-wrong) type oid selects the wrong output
    // function and dereferences the raw datum as the wrong type — a type
    // confusion / arbitrary-read primitive. Deriving the type from the datum
    // itself makes the output function always match the value.
    fn param_datum_type(&mut self, dno: Dno) -> PgResult<Oid> {
        let func = self.func;
        match &func.datums[dno as usize] {
            PlDatum::Var(_) => Ok(self.var_type(dno).typoid),
            PlDatum::Rec(r) => Ok(self.rec_param_type_mod(r.dno)?.0),
            PlDatum::RecField(f) => Ok(self
                .recfield_type(f)?
                .map(|(t, _, _)| t)
                // datum_as_param resolves the same field just before this and
                // errors if it is absent, so a value here implies Some.
                .expect("recfield type resolvable after datum_as_param")),
            _ => panic!("plpgsql: datum {dno} cannot be a parameter"),
        }
    }

    // format_expr_params (pl_exec.c); None when the expr takes no parameters.
    fn format_expr_params(&mut self, expr: &PlExpr) -> PgResult<Option<String>> {
        if !self.func.print_strict_params {
            return Ok(None);
        }
        let paramnos = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&expr.expr_id).expect("plan ensured");
            e.paramnos.clone()
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
            let (v, isnull) = self.datum_as_param(dno, None)?;
            if isnull {
                out.push_str("NULL");
            } else {
                // C's format_expr_params types each value via exec_eval_datum,
                // i.e. from the datum itself — never from the plan's argtypes.
                let valtype = self.param_datum_type(dno)?;
                let sv = self.convert_value_to_string(v, valtype)?;
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

// exec_is_simple_query (pl_exec.c) items 2-4: plain rtable-less SELECT, none
// of the listed features, single result column.
fn exec_is_simple_query(query: &types_nodes::parsenodes::Query<'_>) -> bool {
    if query.commandType != types_nodes::nodes_enums::CmdType::CMD_SELECT {
        return false;
    }
    if !query.rtable.is_nil() {
        return false;
    }
    let (fromlist_empty, quals_none) = match query.jointree {
        Some(jt) => (jt.fromlist.is_nil(), jt.quals.is_none()),
        None => (true, true),
    };
    if query.hasAggs
        || query.hasWindowFuncs
        || query.hasTargetSRFs
        || query.hasSubLinks
        || !query.cteList.is_nil()
        || !fromlist_empty
        || !quals_none
        || !query.groupClause.is_nil()
        || !query.groupingSets.is_nil()
        || query.havingQual.is_some()
        || !query.windowClause.is_nil()
        || !query.distinctClause.is_nil()
        || !query.sortClause.is_nil()
        || query.limitOffset.is_some()
        || query.limitCount.is_some()
        || query.setOperations.is_some()
    {
        return false;
    }
    query.targetList.len() == 1
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
        // pl_exec.c:1644
        check_for_interrupts()?;
        let rc = self.exec_stmt_block(block)?;
        self.frame.stmt.set(None);
        Ok(rc)
    }

    fn exec_stmts(&mut self, stmts: &'a [PlStmt]) -> PgResult<i32> {
        if stmts.is_empty() {
            // pl_exec.c:2001-2010: CHECK_FOR_INTERRUPTS() even though there
            // is no statement — prevents hangup in a tight loop if, for
            // instance, there is a LOOP construct with an empty body.
            check_for_interrupts()?;
            return Ok(RC_OK);
        }
        let save = self.frame.stmt.get();
        for s in stmts {
            self.frame.stmt.set(Some((stmt_lineno(s), stmt_typename(s))));
            // pl_exec.c:2023: per statement, before the dispatch switch
            check_for_interrupts()?;
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
                        None,
                    )?;
                    self.var_type_overrides.insert(t_varno, ty);
                }
            }
            let (tl, bv) = {
                let t = self.var_type(t_varno);
                (t.typlen, t.typbyval)
            };
            let stored = self.assign_copy_to_datum_ctx(t_val, isnull, tl, bv)?;
            self.set_var(t_varno, stored, isnull, !isnull && !bv);
            self.exec_eval_cleanup();
        }

        for (expr, stmts) in whens {
            let (value, isnull) = self.exec_eval_boolean(expr)?;
            self.exec_eval_cleanup();
            if !isnull && value {
                if have_t {
                    self.set_var(t_varno, Datum::null(), true, false);
                }
                return self.exec_stmts(stmts);
            }
        }

        if have_t {
            self.set_var(t_varno, Datum::null(), true, false);
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
        let arr = array_datum_copy(stmt_ctx.mcx(), value)?;
        self.exec_eval_cleanup();
        let arr: &[u8] = &arr;

        let (ndim, dims, lbs) = read_dims_lbounds(arr);
        if slice < 0 || slice > ndim {
            return Err(exec_err(
                types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR,
                format!("slice dimension ({slice}) is out of the valid range 0..{ndim}"),
            ));
        }

        // pl_exec.c:3070-3077: a record/row loop variable is never an array
        // (and may be uninitialized); anything else is probed through
        // plpgsql_exec_get_datum_type, whose RECFIELD arm instantiates a
        // named-composite record and looks the field up in the live tuple.
        let func: &'a PlFunction = self.func;
        let loop_var_elem = match &func.datums[varno as usize] {
            PlDatum::Rec(_) | PlDatum::Row(_) => types_core::InvalidOid,
            PlDatum::RecField(f) => {
                let ftype = match self.recfield_type(f)? {
                    Some((t, _, _)) => t,
                    None => {
                        // C plpgsql_exec_get_datum_type RECFIELD arm
                        // (pl_exec.c:5474-5498): a still-NULL RECORD-typed
                        // record is instantiate_empty_record_variable's
                        // 55000; an assigned one without the field is 42703.
                        if matches!(&self.datums[f.recparentno as usize], DatumVal::Rec(None)) {
                            self.instantiate_empty_rec(f.recparentno)?;
                        }
                        let recname = match &func.datums[f.recparentno as usize] {
                            PlDatum::Rec(r) => r.refname.clone(),
                            _ => String::new(),
                        };
                        return Err(exec_err(
                            types_error::ERRCODE_UNDEFINED_COLUMN,
                            format!("record \"{recname}\" has no field \"{}\"", f.fieldname),
                        ));
                    }
                };
                lsyscache::typ::get_element_type(ftype)?
            }
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
                // C's LOOP_RC_PROCESSING (pl_exec.c): a consumed CONTINUE
                // resets rc to OK before the next iteration, so exhaustion
                // right after CONTINUE returns RC_OK, not a stale RC_CONTINUE.
                rc = RC_OK;
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
                // See the slice==0 arm: consumed CONTINUE must reset rc.
                rc = RC_OK;
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
            // estate->err_var = datum (exec_stmt_block): the context line
            // carries the variable's declaration lineno.
            self.frame.var_lineno.set(Some(match &self.func.datums[dno as usize] {
                PlDatum::Var(v) => v.lineno,
                PlDatum::Rec(r) => r.lineno,
                _ => 0,
            }));
            match &self.func.datums[dno as usize] {
                PlDatum::Var(v) => {
                    if let Some(default_val) = &v.default_val {
                        self.exec_assign_expr(dno, default_val)?;
                    } else {
                        self.set_var(dno, Datum::null(), true, false);
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
                PlDatum::Rec(r) => {
                    // pl_exec.c:1727-1748 exec_move_row(rec, NULL, NULL):
                    // the previous expanded record is deleted.
                    self.assign_record_var(dno, None);
                    if let Some(default_val) = &r.default_val {
                        self.exec_assign_expr(dno, default_val)?;
                    } else if r.datatype.as_ref().is_some_and(|d| d.typtype == TYPTYPE_DOMAIN) {
                        // C pl_exec.c:1737-1748: a defaultless rec runs
                        // exec_move_row(NULL, NULL); for a composite-domain
                        // rec that makes an empty expanded record and
                        // domain-checks the NULL, so a NULL-rejecting domain
                        // errors during block local variable initialization.
                        self.revalidate_rectypeid(dno)?;
                        adt_domains::domain_check(Datum::null(), true, self.rec_typeid(dno))?;
                        self.instantiate_empty_rec(dno)?;
                    }
                }
                _ => {}
            }
        }
        self.frame.var_lineno.set(None);
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
        // unwind-ok: c-callback
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
                self.reset_eval_ctx();

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

    // assign_text_var (pl_exec.c:8838-8842): CStringGetTextDatum, stored as
    // a freeable copy (built in the eval scratch, datumCopy'd into the
    // function's context like every other owned value).
    pub fn assign_text_var(&mut self, dno: Dno, s: &str) -> PgResult<()> {
        let v = varlena::cstring_to_text(self.eval_ctx.mcx(), s.as_bytes())?;
        let stored = self.copy_to_datum_ctx(fmgr::varlena_result(v), false, -1, false)?;
        self.set_var(dno, stored, false, true);
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
                let stored = self.assign_copy_to_datum_ctx(newvalue, isnull, typlen, typbyval)?;
                self.set_var(target, stored, isnull, !isnull && !typbyval);
                Ok(())
            }
            PlDatum::RecField(f) => {
                let recno = f.recparentno;
                let recname = match &self.func.datums[recno as usize] {
                    PlDatum::Rec(r) => r.refname.clone(),
                    _ => String::new(),
                };
                // pl_exec.c:5215-5226: a NULL record with a named composite
                // type is instantiated empty before field assignment; only a
                // RECORD-typed rec errors (inside instantiate_empty_rec).
                if matches!(&self.datums[recno as usize], DatumVal::Rec(None)) {
                    self.instantiate_empty_rec(recno)?;
                }
                let DatumVal::Rec(Some(rv)) = &self.datums[recno as usize] else {
                    panic!("plpgsql exec_assign_value: rec \"{recname}\" valueless after instantiate");
                };
                let i = match rec_lookup_field(&rv.desc, &f.fieldname) {
                    Some(RecFieldRef::User(i)) => i,
                    Some(RecFieldRef::Sys(_)) => {
                        return Err(exec_err(
                            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                            format!("cannot assign to system column \"{}\"", f.fieldname),
                        ));
                    }
                    None => {
                        return Err(exec_err(
                            types_error::ERRCODE_UNDEFINED_COLUMN,
                            format!("record \"{recname}\" has no field \"{}\"", f.fieldname),
                        ));
                    }
                };
                let (ftype, ftypmod, flen, fbyval) = (
                    rv.desc.types[i],
                    rv.desc.typmods[i],
                    rv.desc.typlens[i],
                    rv.desc.typbyvals[i],
                );
                let newvalue =
                    self.exec_cast_value(value, &mut isnull, valtype, valtypmod, ftype, ftypmod)?;
                let stored = self.assign_copy_to_datum_ctx(newvalue, isnull, flen, fbyval)?;
                // C expanded_record_set_field → check_domain_for_new_field:
                // the PROSPECTIVE tuple (current fields with the new value
                // swapped in) must satisfy the composite domain before the
                // field is committed; a violation leaves the rec unchanged.
                let rectypeid = self.rec_typeid(recno);
                if Self::rec_typeid_is_domain(rectypeid)? {
                    let (src, mut pv, mut pn) = match &self.datums[recno as usize] {
                        DatumVal::Rec(Some(rv)) => (
                            rv.src_desc.clone().expect("RecValue carries its source tupdesc"),
                            rv.values.clone(),
                            rv.nulls.clone(),
                        ),
                        _ => unreachable!("instantiated above"),
                    };
                    pv[i] = stored;
                    pn[i] = isnull;
                    self.rec_domain_check_fields(rectypeid, &src, &pv, &pn)?;
                }
                let mcx = self.datum_ctx.mcx();
                if let DatumVal::Rec(Some(rv)) = &mut self.datums[recno as usize] {
                    // expanded_record_set_field_internal (expandedrecord.c):
                    // free the old field value if it is not part of the
                    // stored tuple — here, if this record made the copy.
                    if rv.owned[i] {
                        // SAFETY: owned marks a copy_to_datum_ctx copy held
                        // only by this field; `stored` was copied first.
                        unsafe { free_datum_copy(mcx, rv.values[i], flen) };
                    }
                    rv.values[i] = stored;
                    rv.nulls[i] = isnull;
                    rv.owned[i] = !isnull && !fbyval;
                    rv.empty = false;
                    rv.fvalue_valid = false;
                }
                Ok(())
            }
            PlDatum::Rec(r) => {
                if isnull {
                    // pl_exec.c:5178-5184: NOT NULL rec rejects the NULL
                    // before exec_move_row runs.
                    if r.notnull {
                        return Err(exec_err(
                            types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED,
                            format!(
                                "null value cannot be assigned to variable \"{}\" declared NOT NULL",
                                r.refname
                            ),
                        ));
                    }
                    // C exec_move_row(rec, NULL, NULL) (pl_exec.c:6922-6945):
                    // domain → empty expanded record + domain_check; else
                    // erh = NULL. RECORD field access then 55000 via
                    // instantiate_empty_record_variable; a named composite
                    // instantiates empty on first field touch.
                    if r.datatype.as_ref().is_some_and(|d| d.typtype == TYPTYPE_DOMAIN) {
                        self.revalidate_rectypeid(target)?;
                        adt_domains::domain_check(Datum::null(), true, self.rec_typeid(target))?;
                        self.instantiate_empty_rec(target)?;
                    } else {
                        self.assign_record_var(target, None);
                    }
                    return Ok(());
                }
                if valtype != RECORDOID && !lsyscache::typ::type_is_rowtype(valtype)? {
                    return Err(exec_err(
                        types_error::ERRCODE_DATATYPE_MISMATCH,
                        "cannot assign non-composite value to a record variable".to_string(),
                    ));
                }
                // C exec_move_row_from_datum, expanded-record source: r := r
                // is a no-op; a still-valid tuple travels when the target
                // takes it as is (declared RECORD, declared as the source's
                // rowtype, or already holding it); else no tuple is stored.
                let origin = self
                    .rec_origins
                    .iter()
                    .find(|(p, _)| *p == value.as_usize())
                    .map(|(_, o)| o.clone());
                if let Some(o) = &origin {
                    if o.recno == target {
                        return Ok(());
                    }
                    self.revalidate_rectypeid(target)?;
                }
                let (desc, src, values, nulls, sys) = self.deconstruct_composite(value)?;
                let sys = match origin {
                    Some(o) => {
                        let rectypeid = self.rec_typeid(target);
                        let held = match &self.datums[target as usize] {
                            DatumVal::Rec(Some(tv)) => tv.src_desc.as_ref().is_some_and(|td| {
                                td.tdtypeid == src.tdtypeid
                                    && (src.tdtypeid != RECORDOID
                                        || (src.tdtypmod == td.tdtypmod && src.tdtypmod >= 0))
                            }),
                            _ => false,
                        };
                        if o.fvalue_valid
                            && (rectypeid == RECORDOID || rectypeid == src.tdtypeid || held)
                        {
                            o.sys
                        } else {
                            None
                        }
                    }
                    None => Some(sys),
                };
                self.move_rec_from_values(target, &desc, src, &values, &nulls, sys, true, true)
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
                // C exec_move_row_from_datum -> exec_move_row_from_fields
                // (pl_exec.c:7796, 7181-7330): the ROW arm, including the
                // strict_multi_assignment check.
                let (desc, _src, values, nulls, _) = self.deconstruct_composite(value)?;
                self.move_row_fields_into_varnos(&varnos, &desc, |anum| (values[anum], nulls[anum]))
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
            self.set_var(var, Datum::from_i32(loop_value), false, false);
            rc = self.exec_stmts(body)?;
            if let Some(r) = self.loop_rc(label, rc) {
                rc = r;
                if r != RC_OK {
                    self.exec_set_found_for(found);
                    return Ok(r);
                }
                break;
            }
            // C's LOOP_RC_PROCESSING (pl_exec.c): a consumed CONTINUE resets
            // rc to OK, so exhaustion right after a final-iteration CONTINUE
            // returns RC_OK instead of leaking a stale RC_CONTINUE.
            rc = RC_OK;
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
        // C exec_run_select (pl_exec.c:5770): portal-returning runs prepare
        // with CURSOR_OPT_NO_SCROLL and WITHOUT CURSOR_OPT_PARALLEL_OK —
        // intra-loop user code makes parallel unsafe, and NO_SCROLL pins the
        // portal so spi.c's default-scrollability probe never auto-upgrades a
        // FOR-loop portal to SCROLL (which would arm the wave-10 cursor store
        // and its per-row fill program for a strictly forward internal loop —
        // the SE11 B1 +18.15% tax).
        self.ensure_plan(query, CURSOR_OPT_NO_SCROLL)?;
        let (plan, paramnos, argtypes) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&query.expr_id).expect("plan ensured");
            (e.plan, e.paramnos.clone(), e.argtypes.clone())
        });
        let (values, nulls) = self.setup_params(&paramnos, &argtypes)?;
        let cursor = SPI_cursor_open_with_paramlist(None, plan, &values, &nulls, self.readonly_func)
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
        // C's previous_id / tupdescs_match (pl_exec.c:5844-5845).
        let mut previous: Option<std::rc::Rc<types_tuple::TupleDescData<'static>>> = None;
        let mut tupdescs_match = true;

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
                self.move_loop_row(var, t, i, &mut previous, &mut tupdescs_match)?;
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

    // exec_for_query's per-row assignment (pl_exec.c:5905-5945): once the
    // loop record holds a tupdesc (previous_id) that the query's rows fit
    // (tupdescs_match), each row only swaps the tuple into the record. A
    // body assignment that replaced the record's descriptor, a NULLed
    // record, a row target, or a rowtype needing coercion takes the full
    // exec_move_row path, after which the match is re-tested (once false it
    // stays false) and the record's descriptor becomes the new identity.
    fn move_loop_row(
        &mut self,
        var: Dno,
        tuptab: TuptabHandle,
        i: usize,
        previous: &mut Option<std::rc::Rc<types_tuple::TupleDescData<'static>>>,
        tupdescs_match: &mut bool,
    ) -> PgResult<()> {
        if !matches!(&self.func.datums[var as usize], PlDatum::Rec(_)) {
            return self.move_row_from_tuptable(var, tuptab, i);
        }
        let held = match &self.datums[var as usize] {
            DatumVal::Rec(Some(rv)) => rv.src_desc.clone(),
            _ => None,
        };
        if let (Some(prev), Some(cur)) = (previous.as_ref(), held.as_ref()) {
            if std::rc::Rc::ptr_eq(prev, cur) && *tupdescs_match {
                return self.rec_set_tuple_from_tuptable(var, tuptab, i);
            }
        }
        self.move_row_from_tuptable(var, tuptab, i)?;
        let src_desc = match &self.datums[var as usize] {
            DatumVal::Rec(Some(rv)) => rv.src_desc.clone(),
            _ => None,
        };
        if *tupdescs_match {
            let rectypeid = self.rec_typeid(var);
            *tupdescs_match = rectypeid == RECORDOID
                || spi::tuptable_with(tuptab, |t| {
                    rectypeid == t.tupdesc.tdtypeid
                        || src_desc
                            .as_ref()
                            .is_some_and(|d| compatible_tupdescs(&t.tupdesc, d))
                });
        }
        *previous = src_desc;
        Ok(())
    }

    // expanded_record_set_tuple (expandedrecord.c) for the loop's tuple-swap
    // arm: deconstruct the row against the record's own descriptor (the rows
    // fit it — tupdescs_match), copy the by-ref fields into the function's
    // context, domain-check a composite-domain record on the prospective
    // fields, then swap the images in and free the previous ones. The
    // descriptor, its RecDesc and the tupdesc copy are reused untouched.
    fn rec_set_tuple_from_tuptable(
        &mut self,
        recno: Dno,
        tuptab: TuptabHandle,
        i: usize,
    ) -> PgResult<()> {
        let natts = match &self.datums[recno as usize] {
            DatumVal::Rec(Some(rv)) => rv.values.len(),
            _ => unreachable!("tuple swap on a valueless record"),
        };
        let mut values = vec![Datum::null(); natts];
        let mut nulls = vec![true; natts];
        let sys = spi::tuptable_with(tuptab, |t| {
            for f in 0..natts {
                let (v, isnull) = spi::SPI_getbinval(&t.vals[i], &t.tupdesc, (f + 1) as i32);
                values[f] = v;
                nulls[f] = isnull;
            }
            std::rc::Rc::new(RecSysAttrs::from_tuple(&t.vals[i]))
        });
        let mut owned = vec![false; natts];
        {
            let DatumVal::Rec(Some(rv)) = &self.datums[recno as usize] else {
                unreachable!("checked above")
            };
            for f in 0..natts {
                if !rv.desc.dropped[f] {
                    values[f] = self.assign_copy_to_datum_ctx(
                        values[f],
                        nulls[f],
                        rv.desc.typlens[f],
                        rv.desc.typbyvals[f],
                    )?;
                    owned[f] = !nulls[f] && !rv.desc.typbyvals[f];
                }
            }
        }
        // check_domain_for_new_tuple: the prospective fields must satisfy
        // a composite domain before they replace the current ones.
        let rectypeid = self.rec_typeid(recno);
        if Self::rec_base_typeid(rectypeid)? != rectypeid {
            let td = match &self.datums[recno as usize] {
                DatumVal::Rec(Some(rv)) => {
                    rv.src_desc.clone().expect("RecValue carries its source tupdesc")
                }
                _ => unreachable!("checked above"),
            };
            if let Err(e) = self.rec_domain_check_fields(rectypeid, &td, &values, &nulls) {
                let mcx = self.datum_ctx.mcx();
                if let DatumVal::Rec(Some(rv)) = &self.datums[recno as usize] {
                    for f in 0..natts {
                        if owned[f] {
                            // SAFETY: fresh copies above, held by nobody.
                            unsafe { free_datum_copy(mcx, values[f], rv.desc.typlens[f]) };
                        }
                    }
                }
                return Err(e);
            }
        }
        let mcx = self.datum_ctx.mcx();
        if let DatumVal::Rec(Some(rv)) = &mut self.datums[recno as usize] {
            for f in 0..natts {
                if rv.owned[f] {
                    // SAFETY: owned marks a copy_to_datum_ctx copy held only
                    // by this record; the new images were copied first.
                    unsafe { free_datum_copy(mcx, rv.values[f], rv.desc.typlens[f]) };
                }
            }
            rv.values = values;
            rv.nulls = nulls;
            rv.owned = owned;
            rv.sys = Some(sys);
            rv.fvalue_valid = true;
            rv.empty = false;
        }
        Ok(())
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
                // C's NULL-tuple arm (exec_move_row !HeapTupleIsValid):
                // deconstruct_expanded_record makes a row of NULLs with no
                // strict_multi_assignment and NO composite-domain check.
                self.move_rec_from_values(
                    var,
                    &desc,
                    src_desc,
                    &vec![Datum::null(); n],
                    &vec![true; n],
                    None,
                    false,
                    false,
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
                let sys = spi::tuptable_with(tuptab, |t| {
                    for f in 0..natts {
                        let (v, isnull) =
                            spi::SPI_getbinval(&t.vals[i], &t.tupdesc, (f + 1) as i32);
                        values[f] = v;
                        nulls[f] = isnull;
                    }
                    std::rc::Rc::new(RecSysAttrs::from_tuple(&t.vals[i]))
                });
                self.move_rec_from_values(var, &desc, src_desc, &values, &nulls, Some(sys), true, true)
            }
            PlDatum::Row(r) => {
                let varnos = r.varnos.clone();
                self.move_row_into_varnos(&varnos, tuptab, i)
            }
            _ => panic!("plpgsql exec_move_row: bad target datum {var}"),
        }
    }

    // The strict_multi_assignment report (pl_exec.c:7286-7297); Err only at
    // ERROR level, WARNING is emitted and execution continues.
    fn strict_multiassignment_report(
        &self,
        level: types_error::ErrorLevel,
    ) -> PgResult<()> {
        let b = elog::ereport(level)
            .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
            .errmsg("number of source and target fields in assignment does not match")
            .errdetail(format!(
                "strict_multi_assignment check of {} is active.",
                if level == ERROR { "extra_errors" } else { "extra_warnings" }
            ))
            .errhint("Make sure the query returns the exact list of columns.");
        if level == ERROR {
            return Err(Box::new(b.into_error()));
        }
        b.finish(types_error::ErrorLocation::new(file!(), line!() as i32, "exec_move_row_from_fields"))
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
        // Kept only where C stores the tuple itself rather than its fields.
        sys: Option<std::rc::Rc<RecSysAttrs>>,
        sma_check: bool,
        // C: assignments through expanded_record_set_fields/set_tuple run
        // check_domain_on_current_fields; the row-of-NULLs arm (source
        // tupdesc but no tuple: INTO with no rows, FOR over zero rows) goes
        // through deconstruct_expanded_record instead and is NOT checked.
        domain_check: bool,
    ) -> PgResult<()> {
        let rectypeid = self.rec_typeid(recno);
        if rectypeid == RECORDOID {
            let natts = srcdesc.types.len();
            let mut out_values = values.to_vec();
            for f in 0..natts {
                if !srcdesc.dropped[f] {
                    out_values[f] = self.assign_copy_to_datum_ctx(
                        out_values[f],
                        nulls[f],
                        srcdesc.typlens[f],
                        srcdesc.typbyvals[f],
                    )?;
                }
            }
            let owned = (0..natts)
                .map(|f| !srcdesc.dropped[f] && !nulls[f] && !srcdesc.typbyvals[f])
                .collect();
            self.assign_record_var(
                recno,
                Some(RecValue {
                    desc: srcdesc.clone(),
                    values: out_values,
                    nulls: nulls.to_vec(),
                    src_desc: Some(src_tupdesc),
                    empty: false,
                    fvalue_valid: sys.is_some(),
                    sys,
                    owned,
                }),
            );
            return Ok(());
        }

        self.revalidate_rectypeid(recno)?;
        let rectypeid = self.rec_typeid(recno);
        let base = Self::rec_base_typeid(rectypeid)?;
        let var_td = typcache::lookup_rowtype_tupdesc_copy(self.datum_ctx.mcx(), base, -1)?;
        let dst = RecDesc::from_tupdesc(&var_td);
        let sys = if rectypeid == src_tupdesc.tdtypeid || compatible_tupdescs(&src_tupdesc, &var_td) {
            sys
        } else {
            None
        };
        let vtd_natts = dst.types.len();
        let td_natts = srcdesc.types.len();
        // strict_multi_assignment reads the GUCs at execution
        // (pl_exec.c:7196-7202); active only with a source tupdesc, which
        // this arm always has.
        let sma_level = if sma_check {
            crate::handler::extra_checks_level(crate::comp::XCHECK_STRICTMULTIASSIGNMENT)?
        } else {
            None
        };
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
                if let Some(level) = sma_level {
                    self.strict_multiassignment_report(level)?;
                }
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
                self.assign_copy_to_datum_ctx(v, isnull, dst.typlens[fnum], dst.typbyvals[fnum])?;
            newnulls[fnum] = isnull;
        }
        // Unassigned source attributes, dropped columns skipped
        // (pl_exec.c:7311-7331).
        if let Some(level) = sma_level {
            while anum < td_natts && srcdesc.dropped[anum] {
                anum += 1;
            }
            if anum < td_natts {
                self.strict_multiassignment_report(level)?;
            }
        }
        // Domain check BEFORE committing: C builds/checks the new expanded
        // record first and only then assign_record_var's it, so a violation
        // leaves the variable's old value intact.
        if domain_check && base != rectypeid {
            self.rec_domain_check_fields(rectypeid, &var_td, &newvalues, &newnulls)?;
        }
        let owned = (0..vtd_natts)
            .map(|f| !dst.dropped[f] && !newnulls[f] && !dst.typbyvals[f])
            .collect();
        self.assign_record_var(
            recno,
            Some(RecValue {
                desc: dst,
                values: newvalues,
                nulls: newnulls,
                src_desc: Some(std::rc::Rc::new(var_td)),
                empty: false,
                fvalue_valid: sys.is_some(),
                sys,
                owned,
            }),
        );
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
                PlDatum::Rec(r) => (self.rec_typeid(r.dno), -1),
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
            let (v, isnull) = self.datum_as_param(dno, None)?;
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
        let owned = (0..n).map(|i| !nulls[i] && !desc.typbyvals[i]).collect();
        Ok(RecValue {
            desc,
            values,
            nulls,
            src_desc: Some(std::rc::Rc::new(td)),
            empty: false,
            sys: None,
            fvalue_valid: false,
            owned,
        })
    }

    // deconstruct_composite_datum (pl_exec.c:7546) — plain composite Datum;
    // the identity is C's tmptup (no ctid, no tableoid).
    #[allow(clippy::type_complexity)]
    pub(crate) fn deconstruct_composite(
        &mut self,
        value: Datum,
    ) -> PgResult<(
        RecDesc,
        std::rc::Rc<types_tuple::TupleDescData<'static>>,
        Vec<Datum>,
        Vec<bool>,
        std::rc::Rc<RecSysAttrs>,
    )> {
        let mcx = self.datum_ctx.mcx();
        let p = value.as_usize() as *const u8;
        // C DatumGetHeapTupleHeader: detoast first — the datum can arrive
        // short-headed (e.g. deformed back out of a materialized minimal
        // tuple), compressed, or expanded; only the detoasted 4-byte-header
        // form is a readable HeapTupleHeader image.
        // SAFETY: non-null composite datum — a live varlena-headed image
        // readable for its varsize.
        let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
        // Leaked into the datum context: the deformed byref values below
        // point into this image and outlive this call (C detoasts into the
        // caller's context and never pfrees either — pl_exec.c's "might
        // leak memory" note).
        let rec = detoast::detoast_attr(mcx, raw)?.leak();
        // SAFETY: detoasted composite image; header prefix is in bounds.
        let td_hdr = unsafe { &*(rec.as_ptr() as *const types_tuple::HeapTupleHeaderData) };
        let tup_type = td_hdr.type_id();
        let tup_typmod = td_hdr.typmod();
        let t_len = td_hdr.datum_length();
        let tupdesc = typcache::lookup_rowtype_tupdesc_copy(mcx, tup_type, tup_typmod)?;
        let desc = RecDesc::from_tupdesc(&tupdesc);
        let natts = desc.types.len();
        let mut values = vec![Datum::null(); natts];
        let mut nulls = vec![true; natts];
        // SAFETY: MAXALIGN'd detoasted image of datum_length() bytes.
        let htd = unsafe {
            types_tuple::HeapTupleData::from_raw_parts(
                rec.as_ptr(),
                t_len,
                types_tuple::ItemPointerData::invalid(),
                types_core::InvalidOid,
            )
        };
        types_tuple::heap_deform_tuple(&htd, &tupdesc, &mut values, &mut nulls);
        let sys = std::rc::Rc::new(RecSysAttrs::from_tuple(&htd));
        Ok((desc, std::rc::Rc::new(tupdesc), values, nulls, sys))
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
                PlDatum::Rec(_) => {
                    let rectypeid = self.rec_typeid(retvarno);
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
        // RETURN without expr in a void function (or procedure). C returns a
        // non-null VOID datum for functions (pl_exec.c:3303-3314); procedures
        // stay null.
        const VOIDOID: Oid = 2278;
        const PROKIND_PROCEDURE: i8 = b'p' as i8;
        if self.func.fn_rettype == VOIDOID && self.func.fn_prokind != PROKIND_PROCEDURE {
            self.retval = Datum::from_usize(0);
            self.retisnull = false;
            self.rettype = VOIDOID;
        } else {
            self.retval = Datum::null();
            self.retisnull = true;
            self.rettype = types_core::InvalidOid;
        }
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

        // C keeps err_code as an int where MAKE_SQLSTATE('0','0','0','0','0')
        // == 0 is "unset": `if (err_code)` at pl_exec.c:3850 lets a later
        // USING ERRCODE override SQLSTATE '00000' and pl_exec.c:3890 defaults
        // it to ERRCODE_RAISE_EXCEPTION. None here models exactly that 0.
        let mut err_code: Option<SqlState> = None;
        let mut cond: Option<String> = None;
        if let Some(cn) = condname {
            err_code = raise_err_code(recognize_err_condition(cn)?);
            cond = Some(cn.clone());
        }

        let mut err_message: Option<String> = None;
        if let Some(msg) = message {
            let mut ds = String::new();
            let mut pi = 0usize;
            // C pl_exec.c:3774-3799 walks the message byte-wise; '%' cannot
            // occur inside a multibyte sequence, so a char walk is
            // equivalent — and unlike the old byte-as-char loop it does not
            // Latin-1-widen multibyte text ("café" became "cafÃ©", which
            // the client-encoding conversion then faithfully mojibake'd).
            let mut it = msg.chars().peekable();
            while let Some(c) = it.next() {
                if c == '%' {
                    if it.peek() == Some(&'%') {
                        it.next();
                        ds.push('%');
                        continue;
                    }
                    // C: "should have been checked at compile time"
                    // (pl_exec.c:3794-3796).
                    let Some(p) = params.get(pi) else {
                        return Err(exec_err(
                            types_error::ERRCODE_INTERNAL_ERROR,
                            "unexpected RAISE parameter list length".to_string(),
                        ));
                    };
                    pi += 1;
                    let (v, isnull, t, _m) = self.exec_eval_expr(p)?;
                    if isnull {
                        ds.push_str("<NULL>");
                    } else {
                        ds.push_str(&self.convert_value_to_string(v, t)?);
                    }
                    self.exec_eval_cleanup();
                } else {
                    ds.push(c);
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
                    err_code = raise_err_code(recognize_err_condition(&extval)?);
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
        // pl_exec.c:3909-3923 — the `ereport(stmt->elog_level, ...)` that
        // RAISE lands on carries err_generic_string for COLUMN, CONSTRAINT,
        // DATATYPE, TABLE and SCHEMA at every level, not only at ERROR.
        let mut e = b.into_error();
        set_raise_fields(
            &mut e,
            err_column,
            err_constraint,
            err_datatype,
            err_table,
            err_schema,
        );
        elog::ThrowErrorData(e.with_error_location(types_error::ErrorLocation::new(
            "pl_exec.c",
            3909,
            "exec_stmt_raise",
        )))?;
        Ok(RC_OK)
    }

    fn exec_stmt_assert(&mut self, cond: &PlExpr, message: Option<&PlExpr>) -> PgResult<i32> {
        // plpgsql_check_asserts via the mutation-keyed GUC snapshot
        // (handler.rs PROCPERF P2); unset means C's default (true).
        let enabled = crate::handler::check_asserts_enabled();
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
        let (plan, paramnos, argtypes, mod_stmt) = EXPR_PLANS.with(|t| {
            let t = t.borrow();
            let e = t.get(&expr.expr_id).expect("plan ensured");
            (e.plan, e.paramnos.clone(), e.argtypes.clone(), e.mod_stmt)
        });

        // too_many_rows extra check reads the GUCs at execution, not compile
        // (pl_exec.c:4217-4220).
        let too_many_rows_level = crate::handler::extra_checks_level(
            crate::comp::XCHECK_TOOMANYROWS,
        )?;

        let tcount: i64 = if into {
            if strict || mod_stmt || too_many_rows_level.is_some() {
                2
            } else {
                1
            }
        } else {
            0
        };

        let (values, nulls) = self.setup_params_under_spi(expr, &paramnos, &argtypes)?;
        let _frame = FrameGuard::push_spi(&expr.query, expr.parse_mode);
        let rc = spi::SPI_execute_plan_with_paramlist(plan, &values, &nulls, self.readonly_func, tcount)
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
                if n > 1 && (strict || mod_stmt || too_many_rows_level.is_some()) {
                    // errlevel per pl_exec.c:4404: strict/mod_stmt force
                    // ERROR; otherwise the extra-check level applies.
                    let errlevel = if strict || mod_stmt {
                        ERROR
                    } else {
                        too_many_rows_level.expect("guarded by is_some")
                    };
                    let mut b = elog::ereport(errlevel)
                        .errcode(types_error::ERRCODE_TOO_MANY_ROWS)
                        .errmsg("query returned more than one row")
                        .errhint("Make sure the query returns a single row, or use LIMIT 1.");
                    if let Some(d) = self.format_expr_params(expr)? {
                        b = b.errdetail_internal(format!("parameters: {d}"));
                    }
                    if errlevel == ERROR {
                        let _ = spi::SPI_freetuptable(tuptab);
                        return Err(Box::new(b.into_error()));
                    }
                    b.finish(types_error::ErrorLocation::new(file!(), line!() as i32, "exec_stmt_execsql"))?;
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
        debug_assert!(EXPR_PLANS.with(|t| {
            !matches!(
                t.borrow().get(&expr.expr_id).map(|e| &e.simple),
                Some(SimpleState::Ready(_))
            )
        }));

        let target = if is_call {
            Some(self.make_callstmt_target(expr, plan)?)
        } else {
            None
        };

        let (values, nulls) = self.setup_params_under_spi(expr, &paramnos, &argtypes)?;
        let before_lxid = current_lxid();
        let rc = spi::SPI_execute_plan_extended(
            plan,
            &values,
            &nulls,
            // C exec_stmt_call: options.params = setup_param_list (hooked).
            true,
            self.readonly_func,
            true,
            false,
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
    // plpgsql_create_econtext, pl_exec.c:4967/2256): C must rebuild because
    // the compiled states lived in the transaction's simple_eval_estate and
    // their pins in the transaction's resowner (plpgsql_xact_cb,
    // pl_exec.c:8701). The function-lifetime SimpleExpr entries here own
    // their memory and hold manual plan refcounts, and every use starts
    // with CachedPlanIsSimplyValid — so they survive the transaction
    // boundary intact, which is exactly the amortization C gets from its
    // re-pin arm (CachedPlanIsSimplyValid + expr_simple_plan_lxid,
    // pl_exec.c:6060-6070) without the per-transaction recompile. Cast
    // states stay estate-owned and follow C's cast_lxid revalidation.
    fn rebuild_simple_exprs(&mut self) {
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
            // pl_exec.c:2324-2328 elog(ERROR, "cache lookup failed for
            // function %u", funcexpr->funcid) -- catchable, SQLSTATE XX000
            // (elog's default); it must not abort the backend.
            .ok_or_else(|| {
                exec_err(
                    types_error::ERRCODE_INTERNAL_ERROR,
                    format!("cache lookup failed for function {}", funcexpr.funcid),
                )
            })?;

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
            PlDatum::Rec(r) => {
                if r.isconst {
                    return Err(exec_err(
                        types_error::ERRCODE_ERROR_IN_ASSIGNMENT,
                        format!("variable \"{}\" is declared CONSTANT", r.refname),
                    ));
                }
                Ok(())
            }
            PlDatum::Row(_) => Ok(()),
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
        self.move_row_fields_into_varnos(varnos, &desc, |anum| {
            spi::tuptable_with(tuptab, |t| {
                spi::SPI_getbinval(&t.vals[i], &t.tupdesc, (anum + 1) as i32)
            })
        })
    }

    // exec_move_row_from_fields' ROW arm (pl_exec.c:7181-7330) over an
    // explicit varno list: sources are read positionally with dropped
    // columns skipped, a missing source becomes NULL, and
    // strict_multi_assignment (pl_exec.c:7196-7202, 7288-7297, 7315-7330)
    // reports a short or a long source at the configured level.
    fn move_row_fields_into_varnos(
        &mut self,
        varnos: &[Dno],
        desc: &RecDesc,
        mut fetch: impl FnMut(usize) -> (Datum, bool),
    ) -> PgResult<()> {
        let natts = desc.types.len();
        let sma_level =
            crate::handler::extra_checks_level(crate::comp::XCHECK_STRICTMULTIASSIGNMENT)?;
        let mut anum = 0usize;
        for &dno in varnos {
            while anum < natts && desc.dropped[anum] {
                anum += 1;
            }
            let (v, isnull, vt, vm) = if anum < natts {
                let (v, isnull) = fetch(anum);
                let r = (v, isnull, desc.types[anum], desc.typmods[anum]);
                anum += 1;
                r
            } else {
                if let Some(level) = sma_level {
                    self.strict_multiassignment_report(level)?;
                }
                (Datum::null(), true, UNKNOWNOID, -1)
            };
            self.exec_assign_value(dno, v, isnull, vt, vm)?;
        }
        if let Some(level) = sma_level {
            while anum < natts && desc.dropped[anum] {
                anum += 1;
            }
            if anum < natts {
                self.strict_multiassignment_report(level)?;
            }
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
        let rc = spi::SPI_execute_extended(
            &querystr,
            &ptypes,
            &pvalues,
            &pnulls,
            self.readonly_func,
            false,
        )
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
                    let (srcdesc, _src, values, nulls, _) = self.deconstruct_composite(value)?;
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
        let ctx_mode;
        let rc = if let Some(query) = query {
            ctx_query = query.query.clone();
            ctx_mode = query.parse_mode;
            self.ensure_plan(query, CURSOR_OPT_PARALLEL_OK)?;
            self.exec_spi_plan(query, 0, true)?
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
            ctx_mode = parser_seams::RawParseMode::RAW_PARSE_DEFAULT;
            let (ptypes, pvalues, pnulls) = self.exec_eval_using_params(params)?;
            let _frame =
                FrameGuard::push_spi(&querystr, parser_seams::RawParseMode::RAW_PARSE_DEFAULT);
            // must_return_tuples = true (pl_exec.c:3626): the SPI level
            // describes its own errors — "empty query does not return
            // tuples" is raised before its callback names a query, so it
            // carries no SQL-statement line (spi.c:2494 vs 2509).
            spi::SPI_execute_extended(
                &querystr,
                &ptypes,
                &pvalues,
                &pnulls,
                self.readonly_func,
                true,
            )?
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
            // C raises this inside _SPI_execute_plan under
            // _SPI_error_callback (spi.c:2552-2570): the query rides along
            // as an "SQL statement" context line.
            return Err(spi_ctx_err(
                exec_err(
                    types_error::ERRCODE_SYNTAX_ERROR,
                    format!("{tag} query does not return tuples"),
                ),
                &ctx_query,
                ctx_mode,
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
        let cursor = spi::SPI_cursor_open_with_paramlist(curname, plan, &values, &nulls, self.readonly_func)
            .map_err(|e| spi_ctx_err(e, &query.query, query.parse_mode))?;
        if curname.is_none() {
            // Verify assignability before storing the portal name
            // (pl_exec.c:4803-4807 -> exec_check_assignable).
            self.exec_check_assignable(curvar)?;
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
                // pl_exec.c:4727-4730.
                self.exec_check_assignable(curvar)?;
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
            spi::SPI_cursor_open_with_paramlist(curname.as_deref(), plan, &values, &nulls, self.readonly_func)
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
            self.set_var(curvar, Datum::null(), true, false);
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
        // C exec_stmt_dynfors (pl_exec.c:4635) opens the implicit cursor
        // with CURSOR_OPT_NO_SCROLL — same FOR-loop pinning as exec_stmt_fors.
        let portal =
            self.exec_dynquery_with_params(query, params, None, CURSOR_OPT_NO_SCROLL)?;
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
    #[track_caller]
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

fn resolve_origtypname(name: &OrigTypeName) -> PgResult<(Oid, i32)> {
    let cx = MemoryContext::new("plpgsql rowtype re-resolution");
    let mcx = cx.mcx();
    match name {
        OrigTypeName::Sql(s) => parse_utilcmd::parseTypeString(mcx, s),
        OrigTypeName::Names(parts) => {
            let mut nodes: mcx::PgVec<'_, types_nodes::Node<'_>> =
                mcx::vec_with_capacity_in(mcx, parts.len())?;
            for p in parts {
                let s = core::str::from_utf8(mcx::PgString::from_str_in(p, mcx)?.into_bytes().leak())
                    .expect("copied from a str");
                nodes.push(types_nodes::Node::mk_string(mcx, s)?);
            }
            let tn = types_nodes::rawnodes::TypeName {
                names: types_nodes::NodeList::from_slice(mcx, &nodes)?,
                typemod: -1,
                location: -1,
                ..Default::default()
            };
            parse_utilcmd::typenameTypeIdAndMod(mcx, None, &tn)
        }
    }
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
// exec_stmt_raise's `int err_code` (pl_exec.c:3828): SQLSTATE 00000 is the
// integer 0, i.e. "no code given".
fn raise_err_code(code: SqlState) -> Option<SqlState> {
    (code.0 != 0).then_some(code)
}

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
    // err_var lineno wins over err_stmt's (plpgsql_exec_error_callback).
    let lineno = match frame.var_lineno.get() {
        Some(l) => Some(l),
        None => frame.stmt.get().map(|(l, _)| l),
    };
    if let Some(t) = frame.text.get() {
        match lineno {
            Some(lineno) if lineno > 0 => {
                format!("PL/pgSQL function {sig} line {lineno} {t}")
            }
            _ => format!("PL/pgSQL function {sig} {t}"),
        }
    } else if let Some((_, typename)) = frame.stmt.get() {
        match lineno {
            Some(lineno) if lineno > 0 => {
                format!("PL/pgSQL function {sig} line {lineno} at {typename}")
            }
            _ => format!("PL/pgSQL function {sig}"),
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

// The portal-level snapshot goes back (a COMMIT/ROLLBACK drops every
// snapshot) before the plan is validated or replanned, so the assignment
// can still detoast what the eval fetched under it.
fn simple_expr_portal_snapshot(expr_id: u32, se: Box<SimpleExpr>) -> PgResult<Box<SimpleExpr>> {
    match pquery::EnsurePortalSnapshotExists() {
        Ok(()) => Ok(se),
        Err(e) => {
            let plan = se.plan;
            drop(se);
            put_simple(expr_id, plan, SimpleState::Unknown);
            Err(e)
        }
    }
}

// DatumGetArrayTypePCopy: the FOREACH array, in any varlena form, as a
// private 4B-header copy in `mcx`.
fn array_datum_copy<'mcx>(mcx: Mcx<'mcx>, value: Datum) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let p = value.as_usize() as *const u8;
    // SAFETY: non-null by-ref datum, readable for its header-declared size.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    detoast::detoast_attr(mcx, raw)
}

#[cfg(test)]
pub(crate) mod cfi_tests {
    use super::*;

    pub(crate) fn tiny_function() -> PlFunction {
        PlFunction {
            fn_signature: "inline_code_block".into(),
            fn_oid: types_core::InvalidOid,
            fn_xmin: 0,
            fn_tid: (0, 0),
            fn_input_collation: types_core::InvalidOid,
            fn_rettype: 2278, // VOIDOID
            fn_rettyplen: 4,
            fn_retbyval: true,
            fn_retistuple: false,
            fn_retisdomain: false,
            fn_retset: false,
            fn_readonly: false,
            fn_prokind: b'f' as i8,
            fn_is_trigger: FnTrigger::NotTrigger,
            fn_nargs: 0,
            fn_argvarnos: Vec::new(),
            fn_arg_is_input: Vec::new(),
            new_varno: -1,
            old_varno: -1,
            found_varno: -1,
            out_param_varno: -1,
            datums: Vec::new(),
            ns: Vec::new(),
            action: PlBlock {
                lineno: 1,
                label: None,
                body: Vec::new(),
                initvarnos: Vec::new(),
                exceptions: None,
            },
            resolve_option: 0,
            print_strict_params: false,
            nstatements: 0,
            expr_ids: Vec::new(),
        }
    }

    // Witness for pl_exec.c:2001-2010's empty-body hangup guard: with the
    // interrupt flag raised, exec_stmts on an EMPTY statement list (a `LOOP`
    // with an empty body iterates exactly this) must reach
    // CHECK_FOR_INTERRUPTS and surface its cancel error — pre-fix, the empty
    // list fell straight through to RC_OK and the loop was unkillable.
    #[test]
    fn empty_stmts_reach_check_for_interrupts() {
        postgres_seams::check_for_interrupts::set(|| {
            Err(Box::new(
                PgError::error("canceling statement due to user request".to_string())
                    .with_sqlstate(types_error::ERRCODE_QUERY_CANCELED),
            ))
        });

        let func = tiny_function();
        let mut estate = Estate::new(&func, false, true);

        init_small::globals::SetInterruptPending(false);
        assert_eq!(estate.exec_stmts(&[]).unwrap(), RC_OK, "no interrupt: RC_OK");

        init_small::globals::SetInterruptPending(true);
        let err = estate.exec_stmts(&[]).err().unwrap();
        init_small::globals::SetInterruptPending(false);
        assert_eq!(err.sqlstate, types_error::ERRCODE_QUERY_CANCELED);
    }

    // RAISE literal text must survive byte-for-byte: the old scan loop pushed
    // each UTF-8 byte as a char (Latin-1 widening), so "café" reached the
    // client-encoding conversion as "cafÃ©" and every non-UTF8 client saw
    // mojibake (driver-sweep finding M-1).
    #[test]
    fn raise_message_literal_multibyte_not_widened() {
        let func = tiny_function();
        let mut estate = Estate::new(&func, false, true);
        let stmt = PlStmt::Raise {
            lineno: 1,
            elog_level: crate::gram::ELOG_ERROR,
            condname: None,
            message: Some("café %% x".into()),
            params: Vec::new(),
            options: Vec::new(),
        };
        let err = estate.exec_stmt_raise(&stmt).err().unwrap();
        assert_eq!(err.message, "café % x");
        assert_eq!(err.sqlstate, types_error::ERRCODE_RAISE_EXCEPTION);
    }

    // C pl_exec.c:3794-3796: parameter-list underrun is an XX000 elog, not a
    // panic (the old loop indexed params[] unchecked).
    #[test]
    fn raise_message_param_underrun_is_internal_error() {
        let func = tiny_function();
        let mut estate = Estate::new(&func, false, true);
        let stmt = PlStmt::Raise {
            lineno: 1,
            elog_level: crate::gram::ELOG_ERROR,
            condname: None,
            message: Some("value: %".into()),
            params: Vec::new(),
            options: Vec::new(),
        };
        let err = estate.exec_stmt_raise(&stmt).err().unwrap();
        assert_eq!(err.sqlstate, types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(err.message, "unexpected RAISE parameter list length");
    }

    // Type-confusion guard (finding idx 83): format_expr_params must type each
    // STRICT-error parameter from the DATUM's own declared type (C's
    // exec_eval_datum), never from a re-fetched, swappable plan argtype. This
    // witnesses that param_datum_type reports the Var's declared type — so a
    // mutated EXPR_PLANS entry can never steer the output function onto a
    // wrong-typed raw datum (textout on a by-value int8, etc.).
    #[test]
    fn param_datum_type_comes_from_datum_not_plan() {
        const INT8OID: Oid = 20;
        let plty = PlType {
            typoid: INT8OID,
            ttype: TypeKind::Scalar,
            typlen: 8,
            typbyval: true,
            typtype: b'b' as i8,
            collation: types_core::InvalidOid,
            typisarray: false,
            atttypmod: -1,
            typinput: types_core::InvalidOid,
            typioparam: types_core::InvalidOid,
            rec_ident: None,
        };
        let mut func = tiny_function();
        func.datums.push(PlDatum::Var(PlVar {
            dno: 0,
            refname: "v".into(),
            lineno: 1,
            datatype: plty,
            isconst: false,
            notnull: false,
            default_val: None,
            promise: PROMISE_NONE,
            cursor_explicit_expr: None,
            cursor_explicit_argrow: -1,
            cursor_options: 0,
        }));

        let mut estate = Estate::new(&func, false, true);
        assert_eq!(estate.param_datum_type(0).unwrap(), INT8OID);
    }

    // expanded_record_lookup_field + fetch_field over system attributes, and
    // pl_exec.c:5243's refusal to assign them (pre-fix: "has no field").
    #[test]
    fn record_fields_include_system_attributes() {
        const INT4OID: Oid = 23;
        const TIDOID: Oid = 27;
        const OIDOID: Oid = 26;
        let mut func = tiny_function();
        func.datums.push(PlDatum::Rec(PlRec {
            dno: 0,
            refname: "r".into(),
            lineno: 1,
            rectypeid: RECORDOID,
            datatype: None,
            isconst: false,
            notnull: false,
            default_val: None,
        }));
        for (dno, name) in [(1, "ctid"), (2, "tableoid"), (3, "nosuch")] {
            func.datums.push(PlDatum::RecField(PlRecField {
                dno,
                recparentno: 0,
                fieldname: name.into(),
            }));
        }
        let mut estate = Estate::new(&func, false, true);
        let mcx = estate.datum_mcx();
        let mut td = tupdesc::CreateTemplateTupleDesc(mcx, 1).unwrap();
        tupdesc::TupleDescInitEntry(&mut td, 1, Some("f1"), INT4OID, -1, 0).unwrap();
        let mut tup = heaptuple::heap_form_tuple(mcx, &td, &[Datum::from_i32(7)], &[false]).unwrap();
        tup.t_self = types_tuple::ItemPointerData::new(0, 1);
        tup.t_tableOid = 4242;
        let bound = |sys: Option<RecSysAttrs>| RecValue {
            desc: RecDesc::from_tupdesc(&td),
            values: vec![Datum::from_i32(7)],
            nulls: vec![false],
            owned: vec![false],
            src_desc: Some(std::rc::Rc::new(tupdesc::CreateTupleDescCopy(mcx, &td).unwrap())),
            empty: false,
            fvalue_valid: sys.is_some(),
            sys: sys.map(std::rc::Rc::new),
        };

        estate.datums[0] = DatumVal::Rec(Some(bound(Some(RecSysAttrs::from_tuple(&tup)))));
        let PlDatum::RecField(ctid) = &func.datums[1] else { unreachable!() };
        assert_eq!(estate.recfield_type(ctid).unwrap(), Some((TIDOID, -1, types_core::InvalidOid)));
        let (d, isnull) = estate.datum_as_param(1, Some(TIDOID)).unwrap();
        assert!(!isnull);
        // SAFETY: a tid Datum is a pointer to an ItemPointerData.
        let tid = unsafe { *(d.as_usize() as *const types_tuple::ItemPointerData) };
        assert_eq!(types_tuple::ItemPointerGetBlockNumber(&tid), 0);
        assert_eq!(types_tuple::ItemPointerGetOffsetNumber(&tid), 1);
        assert_eq!(estate.datum_as_param(2, Some(OIDOID)).unwrap(), (Datum::from_oid(4242), false));
        let err = estate.datum_as_param(3, None).unwrap_err();
        assert_eq!(err.message(), "record \"r\" has no field \"nosuch\"");
        let err = estate.exec_assign_value(1, Datum::null(), true, UNKNOWNOID, -1).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.message(), "cannot assign to system column \"ctid\"");

        estate.datums[0] = DatumVal::Rec(Some(bound(None)));
        assert_eq!(estate.datum_as_param(1, None).unwrap(), (Datum::null(), true));
    }

    // Whole-record assignment between recs carries the source tuple's system
    // columns while it is valid; r := r is a no-op; a field assignment stops
    // copies, not direct reads (pre-fix the copy read (4294967295,0)).
    #[test]
    fn whole_record_assignment_carries_the_source_tuple() {
        const INT4OID: Oid = 23;
        const TIDOID: Oid = 27;
        const OIDOID: Oid = 26;
        let mut func = tiny_function();
        for (dno, name) in [(0, "src"), (1, "dst")] {
            func.datums.push(PlDatum::Rec(PlRec {
                dno,
                refname: name.into(),
                lineno: 1,
                rectypeid: RECORDOID,
                datatype: None,
                isconst: false,
                notnull: false,
                default_val: None,
            }));
        }
        let fields = [(2, 1, "ctid"), (3, 1, "tableoid"), (4, 0, "f1"), (5, 0, "ctid")];
        for (dno, recparentno, name) in fields {
            func.datums.push(PlDatum::RecField(PlRecField { dno, recparentno, fieldname: name.into() }));
        }
        let mut estate = Estate::new(&func, false, true);
        let mcx = estate.datum_mcx();
        let mut td = tupdesc::CreateTemplateTupleDesc(mcx, 1).unwrap();
        tupdesc::TupleDescInitEntry(&mut td, 1, Some("f1"), INT4OID, -1, 0).unwrap();
        let mut tup = heaptuple::heap_form_tuple(mcx, &td, &[Datum::from_i32(7)], &[false]).unwrap();
        tup.t_self = types_tuple::ItemPointerData::new(0, 1);
        tup.t_tableOid = 4242;
        estate.datums[0] = DatumVal::Rec(Some(RecValue {
            desc: RecDesc::from_tupdesc(&td),
            values: vec![Datum::from_i32(7)],
            nulls: vec![false],
            owned: vec![false],
            src_desc: Some(std::rc::Rc::new(tupdesc::CreateTupleDescCopy(mcx, &td).unwrap())),
            empty: false,
            sys: Some(std::rc::Rc::new(RecSysAttrs::from_tuple(&tup))),
            fvalue_valid: true,
        }));
        let ctid_of = |estate: &mut Estate<'_>, dno: Dno| -> Option<(u32, u16)> {
            let (d, isnull) = estate.datum_as_param(dno, Some(TIDOID)).unwrap();
            (!isnull).then(|| {
                // SAFETY: a tid Datum is a pointer to an ItemPointerData.
                let tid = unsafe { *(d.as_usize() as *const types_tuple::ItemPointerData) };
                let block = types_tuple::ItemPointerGetBlockNumberNoCheck(&tid);
                (block, types_tuple::ItemPointerGetOffsetNumberNoCheck(&tid))
            })
        };
        // target := source through the param seam, as a simple expression does.
        let assign = |estate: &mut Estate<'_>, target: Dno, source: Dno| {
            let (v, isnull) = estate.datum_as_param(source, None).unwrap();
            let (t, m) = estate.rec_param_type_mod(source).unwrap();
            estate.exec_assign_value(target, v, isnull, t, m).unwrap();
        };

        assign(&mut estate, 1, 0);
        assert_eq!(ctid_of(&mut estate, 2), Some((0, 1)));
        assert_eq!(estate.datum_as_param(3, Some(OIDOID)).unwrap(), (Datum::from_oid(4242), false));

        estate.exec_assign_value(4, Datum::from_i32(8), false, INT4OID, -1).unwrap();
        assert_eq!(ctid_of(&mut estate, 5), Some((0, 1)));
        assign(&mut estate, 0, 0);
        assert_eq!(ctid_of(&mut estate, 5), Some((0, 1)));
        assign(&mut estate, 1, 0);
        assert_eq!(ctid_of(&mut estate, 2), None);
        assert_eq!(estate.datum_as_param(3, Some(OIDOID)).unwrap(), (Datum::null(), true));
        estate.exec_eval_cleanup();
        assert!(estate.rec_origins.is_empty());
    }

    // upstream 54649de65f08 (18.6): the I/O-coercion fallback never casts to or from internal.
    #[test]
    fn cast_to_or_from_internal_is_42846() {
        install_type_seams();

        let func = tiny_function();
        let mut estate = Estate::new(&func, false, true);
        let err = match estate.build_cast_entry(INTERNALOID, -1, TEXTOID, -1) {
            Ok(_) => panic!("internal -> text was given a cast entry"),
            Err(e) => e,
        };
        assert_eq!(err.sqlstate(), types_error::ERRCODE_CANNOT_COERCE);
        assert_eq!(err.message(), "cannot cast type internal to text");
    }

    const TEXTOID: Oid = 25;
    const COMP_OLD: Oid = 7001;
    const COMP_NEW: Oid = 7002;
    const COMP_OLD_REL: Oid = 7101;
    const COMP_NEW_REL: Oid = 7102;
    const NSP: Oid = 7200;

    std::thread_local! {
        static COMP_BY_NAME: core::cell::Cell<Oid> = const { core::cell::Cell::new(COMP_OLD) };
    }

    fn typcache_row(name: &str, typtype: i8, typrelid: Oid) -> syscache_seams::PgTypeTypcacheShape {
        let mut typname = types_tuple::NameData::default();
        typname.namestrcpy(name);
        syscache_seams::PgTypeTypcacheShape {
            typname,
            typlen: 4,
            typbyval: true,
            typalign: b'i' as i8,
            typstorage: b'p' as i8,
            typtype,
            typisdefined: true,
            typrelid,
            typsubscript: types_core::InvalidOid,
            typelem: types_core::InvalidOid,
            typarray: types_core::InvalidOid,
            typcollation: types_core::InvalidOid,
        }
    }

    fn fake_relation_open<'mcx>(
        mcx: Mcx<'mcx>,
        oid: Oid,
        _lockmode: types_rel::LOCKMODE,
    ) -> PgResult<types_rel::Relation<'mcx>> {
        use core::cell::Cell;
        let (reltype, colname) = match oid {
            COMP_OLD_REL => (COMP_OLD, "f1"),
            COMP_NEW_REL => (COMP_NEW, "f0"),
            other => panic!("unexpected relation_open({other})"),
        };
        let mut a = types_tuple::FormData_pg_attribute::default();
        a.attname.namestrcpy(colname);
        a.atttypid = 23;
        a.attnum = 1;
        a.attlen = 4;
        a.attbyval = true;
        a.attalign = b'i' as i8;
        a.atttypmod = -1;
        let mut td = tupdesc::CreateTupleDesc(mcx, &[a])?;
        td.tdtypeid = reltype;
        td.tdtypmod = -1;
        let mut relname = types_tuple::NameData::default();
        relname.namestrcpy("comp");
        let rd_rel = types_rel::FormData_pg_class {
            relname,
            relnamespace: NSP,
            reltype,
            relowner: 10,
            relam: 0,
            relfilenode: 0,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: types_core::RELPERSISTENCE_PERMANENT,
            relkind: types_rel::RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: types_rel::REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        };
        let data = types_rel::RelationData {
            rd_locator: Default::default(),
            rd_smgr: Default::default(),
            rd_id: oid,
            rd_backend: types_core::INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: Cell::new(true),
            rd_createSubid: Cell::new(0),
            rd_newRelfilelocatorSubid: Cell::new(0),
            rd_firstRelfilelocatorSubid: Cell::new(0),
            rd_droppedSubid: Cell::new(0),
            rd_lockInfo: types_rel::LockInfoData {
                lockRelId: types_rel::LockRelId { relId: oid, dbId: 5 },
            },
            rd_rel,
            rd_att: std::rc::Rc::new(td),
            rd_index: None,
            rd_opcintype: mcx::PgVec::new_in(mcx),
            rd_opfamily: mcx::PgVec::new_in(mcx),
            rd_indoption: mcx::PgVec::new_in(mcx),
            rd_indcollation: mcx::PgVec::new_in(mcx),
            rd_options: None,
            pgstat_enabled: Cell::new(false),
            pgstat_link: Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(),
            rd_amcache_gin: Default::default(),
            rd_amcache_spgist: Default::default(),
            rd_support: mcx::PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false,
            rd_hasrules: false,
        };
        Ok(types_rel::Relation::open(data, None))
    }

    // Seams install once per process: one catalog table for every test.
    pub(crate) fn install_type_seams() {
        static SEAMS: std::sync::Once = std::sync::Once::new();
        SEAMS.call_once(|| {
            syscache_seams::pg_type_typtype::set(|typid| {
                Ok(match typid {
                    RECORDOID => Some(b'p' as i8),
                    23 => Some(b'b' as i8),
                    COMP_OLD | COMP_NEW => Some(b'c' as i8),
                    _ => None,
                })
            });
            syscache_seams::lookup_pg_type_shape::set(|_| {
                Ok(Some(types_tuple::PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typstorage: b'p' as i8,
                    typcollation: types_core::InvalidOid,
                }))
            });
            syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
                Ok(match typid {
                    INTERNALOID => Some(typcache_row("internal", b'p' as i8, types_core::InvalidOid)),
                    TEXTOID => Some(typcache_row("text", b'p' as i8, types_core::InvalidOid)),
                    COMP_OLD => Some(typcache_row("comp", b'c' as i8, COMP_OLD_REL)),
                    COMP_NEW => Some(typcache_row("comp", b'c' as i8, COMP_NEW_REL)),
                    _ => None,
                })
            });
            namespace_seams::type_is_visible::set(|_| Ok(true));
            syscache_seams::syscache_hash_value_typeoid::set(Ok);
            relation_seams::relation_open::set(fake_relation_open);
            relcache_seams::relation_cache_invalidate_entry::set(|_| Ok(()));
            syscache_seams::lookup_pg_namespace_oid_by_name::set(|nsp| {
                Ok(if nsp == "s" { NSP } else { types_core::InvalidOid })
            });
            aclchk_seams::object_aclcheck::set(|_, _, _, _| Ok(0));
            miscinit_seams::get_user_id::set(|| 10);
            syscache_seams::lookup_pg_type_oid_by_name::set(|name, nsp| {
                Ok(if name == "comp" && nsp == NSP {
                    COMP_BY_NAME.with(|c| c.get())
                } else {
                    types_core::InvalidOid
                })
            });
            syscache_seams::pg_type_isdefined::set(|_| Ok(Some(true)));
        });
    }

    // revalidate_rectypeid: the compiled OID stands until a relcache inval
    // clears the entry's identifier; then the name is resolved anew, once.
    #[test]
    fn stale_rowtype_is_re_resolved_by_name() {
        install_type_seams();
        let ident = {
            let tcache = crate::comp::composite_typcache_entry(COMP_OLD).unwrap();
            std::rc::Rc::new(RecTypeIdent {
                origtypname: Some(OrigTypeName::Names(vec!["s".into(), "comp".into()])),
                typoid: core::cell::Cell::new(COMP_OLD),
                tupdesc_id: core::cell::Cell::new(tcache.tupdesc_identifier()),
                tcache: core::cell::RefCell::new(std::rc::Rc::downgrade(&tcache)),
            })
        };
        let cached_id = || ident.tcache.borrow().upgrade().unwrap().tupdesc_identifier();
        let mut func = tiny_function();
        func.datums.push(PlDatum::Rec(PlRec {
            dno: 0,
            refname: "r".into(),
            lineno: 1,
            rectypeid: COMP_OLD,
            datatype: Some(PlType {
                typoid: COMP_OLD,
                ttype: TypeKind::Rec,
                typlen: -1,
                typbyval: false,
                typtype: b'c' as i8,
                collation: types_core::InvalidOid,
                typisarray: false,
                atttypmod: -1,
                typinput: types_core::InvalidOid,
                typioparam: types_core::InvalidOid,
                rec_ident: Some(ident.clone()),
            }),
            isconst: false,
            notnull: false,
            default_val: None,
        }));
        let mut estate = Estate::new(&func, false, true);

        estate.revalidate_rectypeid(0).unwrap();
        assert_eq!(estate.rec_typeid(0), COMP_OLD);

        COMP_BY_NAME.with(|c| c.set(COMP_NEW));
        inval::local::LocalExecuteInvalidationMessage(&types_storage::SharedInvalidationMessage::Relcache(
            types_storage::SharedInvalRelcacheMsg { dbId: types_core::InvalidOid, relId: COMP_OLD_REL },
        ))
        .unwrap();
        assert_eq!(cached_id(), 0);
        estate.revalidate_rectypeid(0).unwrap();
        assert_eq!(estate.rec_typeid(0), COMP_NEW);
        assert_eq!(ident.typoid.get(), COMP_NEW);
        assert_ne!(ident.tupdesc_id.get(), 0);
        assert_eq!(cached_id(), ident.tupdesc_id.get());

        COMP_BY_NAME.with(|c| c.set(types_core::InvalidOid));
        estate.revalidate_rectypeid(0).unwrap();
        assert_eq!(estate.rec_typeid(0), COMP_NEW);

        let mut estate2 = Estate::new(&func, false, true);
        assert_eq!(estate2.rec_typeid(0), COMP_OLD);
        estate2.revalidate_rectypeid(0).unwrap();
        assert_eq!(estate2.rec_typeid(0), COMP_NEW);
    }

    // A small array deformed out of a heap tuple arrives short-headed;
    // DatumGetArrayTypePCopy takes every varlena form.
    #[test]
    fn foreach_array_copy_accepts_short_header_datum() {
        const INT4OID: Oid = 23;
        let ctx = MemoryContext::new("foreach-test");
        let mcx = ctx.mcx();
        let values = [Datum::from_i32(1), Datum::from_i32(2), Datum::from_i32(3)];
        let full = arrayfuncs::construct::construct_md_array(
            mcx, &values, None, 1, &[3], &[1], INT4OID, 4, true, b'i',
        )
        .unwrap();
        let payload = &full[datum::VARHDRSZ..];
        let mut short: Vec<u8> = Vec::with_capacity(1 + payload.len());
        short.push((((1 + payload.len()) as u8) << 1) | 0x01);
        short.extend_from_slice(payload);
        let copy = array_datum_copy(mcx, Datum::from_usize(short.as_ptr() as usize)).unwrap();
        assert_eq!(&copy[..], &full[..]);
        let copy4 = array_datum_copy(mcx, Datum::from_usize(full.as_ptr() as usize)).unwrap();
        assert_eq!(&copy4[..], &full[..]);
    }

    // The snapshot step precedes any plan use: with neither a snapshot nor
    // a portal it is C's elog, while reaching the plan reports "not simple".
    #[test]
    fn simple_expr_restores_portal_snapshot_before_plan_use() {
        let func = tiny_function();
        let mut estate = Estate::new(&func, false, false);
        let expr = PlExpr {
            query: "1".into(),
            parse_mode: parser_seams::RawParseMode::RAW_PARSE_PLPGSQL_EXPR,
            ns: 0,
            expr_id: 0xF5F5_0001,
            target_param: -1,
        };
        EXPR_PLANS.with(|t| {
            t.borrow_mut().insert(
                expr.expr_id,
                PlanEntry {
                    plan: SpiPlanPtr(u64::MAX),
                    paramnos: std::rc::Rc::from(Vec::<Dno>::new()),
                    argtypes: std::rc::Rc::from(Vec::<Oid>::new()),
                    mod_stmt: false,
                    hooks: std::rc::Rc::new(HookSnapshot {
                        names: Vec::new(),
                        params_by_dno: Vec::new(),
                        arg_dnos: Vec::new(),
                        recs: Vec::new(),
                        valueless: Vec::new(),
                        resolve_option: parser_small1::PlpgsqlResolveOption::Error,
                    }),
                    simple: SimpleState::Unknown,
                },
            );
        });
        let r = estate.exec_eval_simple_expr(&expr);
        EXPR_PLANS.with(|t| t.borrow_mut().remove(&expr.expr_id));
        let err = r.err().expect("no portal snapshot can be re-established here");
        assert_eq!(err.message(), "cannot execute SQL without an outer snapshot or portal");
    }
}

#[cfg(test)]
mod param_fetch_tests {
    use super::*;
    use types_nodes::nodes_enums::CmdType;
    use types_nodes::parsenodes::Query;
    use types_nodes::primnodes::{Param, ParamKind, TargetEntry};
    use types_nodes::{Node, NodeList};

    fn param<'mcx>(m: mcx::Mcx<'mcx>, dno: Dno) -> Node<'mcx> {
        Node::mk(
            m,
            Param {
                paramkind: ParamKind::PARAM_EXTERN,
                paramid: dno + 1,
                paramtype: 23,
                paramtypmod: -1,
                paramcollid: types_core::InvalidOid,
                location: -1,
            },
        )
        .unwrap()
    }

    fn select_of<'mcx>(m: mcx::Mcx<'mcx>, expr: Node<'mcx>) -> Query<'mcx> {
        let tle = Node::mk(
            m,
            TargetEntry {
                expr,
                resno: 1,
                resname: None,
                ressortgroupref: 0,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: false,
            },
        )
        .unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            targetList: NodeList::make1(m, tle).unwrap(),
            ..Default::default()
        }
    }

    // `r.f1 := $1` analyzes to `SELECT $1`: the target (dno 3, resolved
    // first) has no Param, so it is not fetched (plpgsql_cache's RETURN
    // reads it); a target its own source reads stays.
    #[test]
    fn unread_assignment_target_is_not_fetched() {
        let cx = mcx::MemoryContext::new("plpgsql param walk test");
        let m = cx.mcx();
        let mut used = vec![3, 0];
        drop_unread_assign_target(&mut used, &[select_of(m, param(m, 0))]).unwrap();
        assert_eq!(used, vec![0]);

        let mut used = vec![3, 0];
        drop_unread_assign_target(&mut used, &[select_of(m, param(m, 3))]).unwrap();
        assert_eq!(used, vec![3, 0]);
        assert_eq!(referenced_param_dnos(&[select_of(m, param(m, 3))]).unwrap(), vec![3]);
    }

    // Execute-path param fetch failures carry the SQL statement line
    // (plpgsql_cache's `select pg_typeof(r.a) into t`); the bare fetch does not.
    #[test]
    fn spi_param_setup_errors_carry_the_statement_context() {
        let mut func = super::cfi_tests::tiny_function();
        func.datums = vec![
            PlDatum::Rec(PlRec {
                dno: 0,
                refname: "r".into(),
                lineno: 1,
                rectypeid: RECORDOID,
                datatype: None,
                isconst: false,
                notnull: false,
                default_val: None,
            }),
            PlDatum::RecField(PlRecField { dno: 1, recparentno: 0, fieldname: "a".into() }),
        ];
        let mut estate = Estate::new(&func, false, true);
        let expr = PlExpr {
            query: "select pg_typeof(r.a)".into(),
            parse_mode: parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
            ns: 0,
            expr_id: 0,
            target_param: -1,
        };
        let argtypes = [types_core::InvalidOid, 23];
        let err = estate.setup_params_under_spi(&expr, &[1], &argtypes).unwrap_err();
        assert_eq!(err.sqlstate, types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        assert_eq!(err.context.as_deref(), Some("SQL statement \"select pg_typeof(r.a)\""));
        let err = estate.setup_params(&[1], &argtypes).unwrap_err();
        assert_eq!(err.context, None);
    }
}
