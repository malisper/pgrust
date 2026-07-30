// nodeMemoize.c. Entries live in an owned AllocSet child context: eviction
// frees for real (C pfree), purge is a wholesale reset. Divergence from C's
// simplehash: entry slots are stable u32 handles (hashbrown indexes them), so
// C's post-eviction entry re-finds are unnecessary; the LRU is intrusive
// links on the entries themselves rather than on the key. Memory accounting
// uses C's LP64 struct sizes so EXPLAIN numbers match byte-for-byte.
// Parallel (DSM) arms are dead until the parallel lanes land.
#![allow(non_snake_case)]

use core::alloc::Layout;
use core::ptr::NonNull;
use std::rc::Rc;

use ::datum::Datum;
use ::execexpr::{exec_eval_expr, exec_qual, EvalSlots, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{Allocator, Mcx, MemoryContext, PgBox, PgVec};
use ::types_core::instrument::MemoizeInstrumentation;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_nodes::plannodes::Memoize;
use ::types_nodes::primnodes::ParamKind;
use ::types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::types_tuple::{varatt, MinimalTupleData, TupleDescData};

pub fn init_seams() {}

#[inline(always)]
fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

// C LP64 sizeof(MemoizeEntry) / sizeof(MemoizeKey) / sizeof(MemoizeTuple):
// the accounting constants EXPLAIN's Memory Usage is derived from.
const SIZEOF_MEMOIZE_ENTRY: u64 = 24;
const SIZEOF_MEMOIZE_KEY: u64 = 24;
const SIZEOF_MEMOIZE_TUPLE: u64 = 16;

// Cached tuples carry their chain link as an 8-byte prefix ahead of the
// minimal-tuple image (C's separate MemoizeTuple node, collapsed into the
// same allocation).
const TUPLE_PREFIX: usize = 8;

const INVALID: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct KeyAttr {
    byval: bool,
    len: i16,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MemoStatus {
    CacheLookup,
    FetchNextTuple,
    FillingCache,
    BypassMode,
    EndOfScan,
}

struct CacheEntry {
    params: NonNull<MinimalTupleData>,
    tuplehead: Option<NonNull<u8>>,
    hash: u32,
    complete: bool,
    lru_prev: u32,
    lru_next: u32,
    // Kernel-cached key (byval single-key kernels only; Datum::null()
    // under ProbeKernel::Expr): probes compare this word instead of
    // storing + deforming the entry's params image per candidate.
    key: Datum,
    key_isnull: bool,
}

enum KeyExpr<'mcx> {
    Param(u32),
    Expr(PgBox<'mcx, ExprState<'mcx>>),
}

// Single-int-key probe kernel (execgrouping ProbeKernel precedent): the
// dominant Memoize shape is one PARAM_EXEC int4/int8 key, where C's probe
// cost is a simplehash inline compare while the Expr path pays probeslot
// clear/store + a hash interp round trip + a per-candidate entry-tuple
// store/deform + an eq interp round trip. Kernel hash matches the expr path
// bit-for-bit (EEOP_HASHDATUM_FIRST: NULL hashes as 0, init value 0) and eq
// is NOT DISTINCT — exactly exec_build_grouping_equal's fold.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ProbeKernel {
    Expr,
    Int4,
    Int8,
    // Binary-mode single byval key (the LATERAL shape — C forces binary_mode
    // for lateral_vars): hash/eq are datum_image ops over the full Datum
    // word, so one kernel covers every byval type.
    ByvalImage,
}

impl ProbeKernel {
    fn select(nkeys: usize, hashfns: &[Oid], eqfns: &[Oid]) -> ProbeKernel {
        if nkeys == 1 {
            match (hashfns[0], eqfns[0]) {
                (450, 65) => return ProbeKernel::Int4,
                (949, 467) => return ProbeKernel::Int8,
                _ => {}
            }
        }
        ProbeKernel::Expr
    }
}

// hashfunc.c hashint8's cross-type-compatible fold to 32 bits.
#[inline(always)]
fn hashint8_fold(key: Datum) -> u32 {
    let val = key.as_i64();
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    lohalf ^ if val >= 0 { hihalf } else { !hihalf }
}

pub trait MemoizeChild<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
}

pub struct MemoizeState<'mcx> {
    pub plan: &'mcx Memoize<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    mstatus: MemoStatus,
    nkeys: usize,
    tableslot: SlotData<'mcx>,
    probeslot: SlotData<'mcx>,
    param_exprs: PgVec<'mcx, KeyExpr<'mcx>>,
    hash_expr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    eq_expr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    key_attrs: PgVec<'mcx, KeyAttr>,
    kernel: ProbeKernel,
    binary_mode: bool,
    singlerow: bool,
    entries: PgVec<'mcx, Option<CacheEntry>>,
    free_slots: PgVec<'mcx, u32>,
    hashtab: hashbrown::HashTable<u32>,
    built: bool,
    lru_head: u32,
    lru_tail: u32,
    table_ctx: NonNull<MemoryContext>,
    mem_used: u64,
    mem_limit: u64,
    entry: u32,
    last_tuple: Option<NonNull<u8>>,
    stats: MemoizeInstrumentation,
}

/// `ExecEstimateCacheEntryOverheadBytes` (nodeMemoize.c), C LP64 sizes.
pub fn exec_estimate_cache_entry_overhead_bytes(ntuples: f64) -> f64 {
    (SIZEOF_MEMOIZE_ENTRY + SIZEOF_MEMOIZE_KEY) as f64 + SIZEOF_MEMOIZE_TUPLE as f64 * ntuples
}

// Droppy context inside a no-drop arena: the query context's reset callback
// is its destructor (docs/no-drop.md; nodeagg precedent) — error paths too.
fn make_table_ctx(mcx: Mcx<'_>) -> PgResult<NonNull<MemoryContext>> {
    let layout = Layout::new::<MemoryContext>();
    let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: NonNull<MemoryContext> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(mcx.context().new_child("MemoizeHashTable")) };
    // SAFETY: fires exactly once, before the arena bytes are reclaimed.
    mcx.context()
        .register_reset_callback(move || unsafe { core::ptr::drop_in_place(p.as_ptr()) });
    Ok(p)
}

pub fn child_eflags(eflags: i32) -> i32 {
    eflags
}

pub fn exec_init_memoize<'mcx>(
    node: &'mcx Memoize<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    result_desc: Rc<TupleDescData<'static>>,
    hashkeydesc: Rc<TupleDescData<'static>>,
) -> PgResult<MemoizeState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    let mcx = estate.es_query_cxt;
    let nkeys = node.numKeys as usize;
    debug_assert!(nkeys > 0 && nkeys == node.param_exprs.len());

    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot = estate
        .exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::MinimalTuple);

    let tableslot = exectuples::make_tuple_table_slot(
        mcx,
        TupleSlotKind::MinimalTuple,
        Some(hashkeydesc.clone()),
    );
    let probeslot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(hashkeydesc.clone()));

    let params = estate.param_bind();
    // Droppy elements: released in exec_end_memoize (windowagg argstates
    // precedent), so the no-drop vec ctors don't apply. Bare PARAM_EXEC keys
    // (the common shape replace_nestloop_params produces) read the param
    // slot directly instead of an interpreter round trip per probe.
    let mut param_exprs: PgVec<'mcx, KeyExpr<'mcx>> = PgVec::new_in(mcx);
    for expr in &node.param_exprs {
        match expr.as_param().filter(|p| p.paramkind == ParamKind::PARAM_EXEC) {
            Some(p) => param_exprs.push(KeyExpr::Param(p.paramid as u32)),
            None => param_exprs.push(KeyExpr::Expr(
                execexpr::exec_init_expr(mcx, Some(expr), params)?.expect("cache key expr"),
            )),
        }
    }

    let mut key_attrs: PgVec<'mcx, KeyAttr> = ::mcx::vec_with_capacity_in(mcx, nkeys)?;
    for i in 0..nkeys {
        let att = hashkeydesc.attr(i);
        key_attrs.push(KeyAttr { byval: att.attbyval, len: att.attlen });
    }

    let mut kernel = ProbeKernel::Expr;
    let (hash_expr, eq_expr) = if node.binary_mode {
        if nkeys == 1 && key_attrs[0].byval {
            kernel = ProbeKernel::ByvalImage;
        }
        (None, None)
    } else {
        let mut hashfns: PgVec<'mcx, Oid> = ::mcx::vec_with_capacity_in(mcx, nkeys)?;
        let mut eqfns: PgVec<'mcx, Oid> = ::mcx::vec_with_capacity_in(mcx, nkeys)?;
        let mut cols: PgVec<'mcx, i16> = ::mcx::vec_with_capacity_in(mcx, nkeys)?;
        for (i, &hashop) in node.hashOperators.iter().enumerate() {
            let Some((left_hashfn, _)) = lsyscache::get_op_hash_functions(hashop)? else {
                return Err(no_hash_function(hashop));
            };
            hashfns.push(left_hashfn);
            eqfns.push(lsyscache::get_opcode(hashop)?);
            cols.push(i as i16 + 1);
        }
        let hash_expr = execexpr::exec_build_hash32_from_attrs(
            mcx,
            &hashkeydesc,
            &hashfns,
            node.collations,
            &cols,
            0,
        )?;
        let eq_expr = execexpr::exec_build_grouping_equal(
            mcx,
            &hashkeydesc,
            &hashkeydesc,
            &cols,
            &eqfns,
            node.collations,
        )?;
        kernel = ProbeKernel::select(nkeys, &hashfns, &eqfns);
        (Some(hash_expr), Some(eq_expr))
    };

    Ok(MemoizeState {
        plan: node,
        ps_ExprContext,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        mstatus: MemoStatus::CacheLookup,
        nkeys,
        tableslot,
        probeslot,
        param_exprs,
        hash_expr,
        eq_expr,
        key_attrs,
        kernel,
        binary_mode: node.binary_mode,
        singlerow: node.singlerow,
        entries: PgVec::new_in(mcx),
        free_slots: PgVec::new_in(mcx),
        hashtab: hashbrown::HashTable::new(),
        built: false,
        lru_head: INVALID,
        lru_tail: INVALID,
        table_ctx: make_table_ctx(mcx)?,
        mem_used: 0,
        mem_limit: nodehash::get_hash_memory_limit() as u64,
        entry: INVALID,
        last_tuple: None,
        stats: MemoizeInstrumentation::default(),
    })
}

#[track_caller]
#[cold]
#[inline(never)]
fn no_hash_function(hashop: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "could not find hash function for hash operator {hashop}"
    )))
}

// SAFETY contract: the holder outlives the state (query-context reset
// callback drops it after the plan tree is dead).
fn table_mcx<'a>(ctx: NonNull<MemoryContext>) -> Mcx<'a> {
    unsafe { ctx.as_ref() }.mcx()
}

impl<'mcx> MemoizeState<'mcx> {
    fn lru_unlink(&mut self, ix: u32) {
        let (prev, next) = {
            let e = self.entries[ix as usize].as_ref().expect("live entry");
            (e.lru_prev, e.lru_next)
        };
        match prev {
            INVALID => self.lru_head = next,
            p => self.entries[p as usize].as_mut().expect("live entry").lru_next = next,
        }
        match next {
            INVALID => self.lru_tail = prev,
            n => self.entries[n as usize].as_mut().expect("live entry").lru_prev = prev,
        }
    }

    fn lru_push_tail(&mut self, ix: u32) {
        let old_tail = self.lru_tail;
        {
            let e = self.entries[ix as usize].as_mut().expect("live entry");
            e.lru_prev = old_tail;
            e.lru_next = INVALID;
        }
        match old_tail {
            INVALID => self.lru_head = ix,
            t => self.entries[t as usize].as_mut().expect("live entry").lru_next = ix,
        }
        self.lru_tail = ix;
    }

    fn lru_move_tail(&mut self, ix: u32) {
        if self.lru_tail != ix {
            self.lru_unlink(ix);
            self.lru_push_tail(ix);
        }
    }
}

// SAFETY (all image helpers): images live in the table context until freed
// here or reset. size must equal the exact alloc layout (t_len, or
// TUPLE_PREFIX + t_len): the Aset is exact-accounting.
unsafe fn free_image(mcx: Mcx<'_>, base: NonNull<u8>, size: usize) {
    unsafe { mcx.deallocate(base, Layout::from_size_align_unchecked(size, 8)) };
}

#[inline]
unsafe fn tuple_of(node: NonNull<u8>) -> NonNull<MinimalTupleData> {
    unsafe { NonNull::new_unchecked(node.as_ptr().add(TUPLE_PREFIX)).cast() }
}

#[inline]
unsafe fn next_of(node: NonNull<u8>) -> Option<NonNull<u8>> {
    unsafe { NonNull::new(node.as_ptr().cast::<*mut u8>().read()) }
}

#[inline]
unsafe fn set_next(node: NonNull<u8>, next: Option<NonNull<u8>>) {
    let raw = next.map_or(core::ptr::null_mut(), NonNull::as_ptr);
    unsafe { node.as_ptr().cast::<*mut u8>().write(raw) };
}

#[inline]
unsafe fn tuple_t_len(node: NonNull<u8>) -> u32 {
    unsafe { tuple_of(node).as_ref().t_len }
}

/// `datum_image_hash` (datum.c); detoast scratch lands in the per-tuple mcx.
fn datum_image_hash(mcx: Mcx<'_>, value: Datum, byval: bool, len: i16) -> PgResult<u32> {
    if byval {
        // Truncate to the attlen width first: a formed-then-deformed datum
        // may differ from the original in the upper bits (C 49315de).
        let raw = truncate_byval(value, len).to_ne_bytes();
        return Ok(hashfn::hash_bytes(&raw));
    }
    let p = value.as_usize() as *const u8;
    if len > 0 {
        // SAFETY: by-ref fixed-length datum, live for the eval cycle.
        return Ok(hashfn::hash_bytes(unsafe {
            core::slice::from_raw_parts(p, len as usize)
        }));
    }
    if len == -1 {
        return Ok(hashfn::hash_bytes(varlena_data(mcx, p)?));
    }
    debug_assert!(len == -2);
    // SAFETY: by-ref cstring datum, NUL-terminated.
    let s = unsafe { core::ffi::CStr::from_ptr(p.cast()) };
    Ok(hashfn::hash_bytes(s.to_bytes_with_nul()))
}

/// `datum_image_eq` (datum.c); detoast scratch lands in `mcx`.
fn datum_image_eq(mcx: Mcx<'_>, a: Datum, b: Datum, byval: bool, len: i16) -> PgResult<bool> {
    if byval {
        return Ok(truncate_byval(a, len) == truncate_byval(b, len));
    }
    let (pa, pb) = (a.as_usize() as *const u8, b.as_usize() as *const u8);
    if len > 0 {
        // SAFETY: by-ref fixed-length datums, live for the eval cycle.
        return Ok(unsafe {
            core::slice::from_raw_parts(pa, len as usize)
                == core::slice::from_raw_parts(pb, len as usize)
        });
    }
    if len == -1 {
        return Ok(varlena_data(mcx, pa)? == varlena_data(mcx, pb)?);
    }
    debug_assert!(len == -2);
    // SAFETY: by-ref cstring datums, NUL-terminated.
    unsafe {
        Ok(core::ffi::CStr::from_ptr(pa.cast()) == core::ffi::CStr::from_ptr(pb.cast()))
    }
}

// Sign-truncation to attlen width, then zero-extended back to a word so both
// operands normalize identically (C DatumGetChar/Int16/Int32 in datum.c).
fn truncate_byval(v: Datum, len: i16) -> usize {
    let x = v.as_usize();
    match len {
        1 => x as u8 as usize,
        2 => x as u16 as usize,
        4 => x as u32 as usize,
        _ => x,
    }
}

// Raw payload, detoasted when external/compressed (C hashes/compares
// VARDATA_ANY over toast_raw_datum_size bytes).
fn varlena_data<'m>(mcx: Mcx<'m>, p: *const u8) -> PgResult<&'m [u8]> {
    // SAFETY: by-ref varlena datum readable through its header.
    unsafe {
        let flat = if varatt::varatt_is_1b_e(p) || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p)) {
            let image = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            detoast_seams::detoast_attr::call(mcx, image)?.leak().as_ptr()
        } else {
            p
        };
        if varatt::varatt_is_1b(flat) {
            Ok(core::slice::from_raw_parts(
                flat.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(flat) - varatt::VARHDRSZ_SHORT,
            ))
        } else {
            Ok(core::slice::from_raw_parts(
                flat.add(varatt::VARHDRSZ),
                varatt::varsize_4b(flat) - varatt::VARHDRSZ,
            ))
        }
    }
}

#[inline]
fn eval_key<'mcx>(
    key: &mut KeyExpr<'mcx>,
    ecxt: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    match key {
        KeyExpr::Param(pid) => {
            let prm = &estate.es_param_exec_vals[*pid as usize];
            debug_assert!(!prm.exec_plan, "nestloop params are never pending");
            Ok((prm.value, prm.isnull))
        }
        KeyExpr::Expr(expr) => {
            let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
            // SAFETY: the per-tuple context outlives this eval (reset-only).
            unsafe { expr.arm_result_mcx_raw(per_tuple) };
            let mut slots = EvalSlots { scan: None, inner: None, outer: None };
            let r = exec_eval_expr(expr, &mut slots)?;
            Ok((r.value, r.isnull))
        }
    }
}

fn prepare_probe_slot<'mcx>(
    node: &mut MemoizeState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    exectuples::exec_clear_tuple(&mut node.probeslot, estate.es_query_cxt);
    for i in 0..node.param_exprs.len() {
        let (value, isnull) = eval_key(&mut node.param_exprs[i], node.ps_ExprContext, estate)?;
        let base = node.probeslot.base_mut();
        base.tts_values[i] = value;
        base.tts_isnull[i] = isnull;
    }
    exectuples::exec_store_virtual_tuple(&mut node.probeslot);
    Ok(())
}

fn probe_hash<'mcx>(
    node: &mut MemoizeState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<u32> {
    let per_tuple = estate.ecxt(node.ps_ExprContext).per_tuple_mcx();
    if node.binary_mode {
        let mut hashkey: u32 = 0;
        let base = node.probeslot.base();
        for (i, &KeyAttr { byval, len }) in node.key_attrs.iter().enumerate() {
            hashkey = hashkey.rotate_left(1);
            if !base.tts_isnull[i] {
                hashkey ^= datum_image_hash(per_tuple, base.tts_values[i], byval, len)?;
            }
        }
        Ok(hashfn::murmurhash32(hashkey))
    } else {
        let hash_expr = node.hash_expr.as_mut().expect("logical-mode hash expr");
        // SAFETY: the per-tuple context outlives this eval (reset-only).
        unsafe { hash_expr.arm_result_mcx_raw(per_tuple) };
        let mut slots =
            EvalSlots { scan: None, inner: Some(&mut node.probeslot), outer: None };
        let r = exec_eval_expr(hash_expr, &mut slots)?;
        debug_assert!(!r.isnull);
        Ok(hashfn::murmurhash32(r.value.as_u32()))
    }
}

fn probe_equal<'mcx>(
    tableslot: &mut SlotData<'mcx>,
    probeslot: &mut SlotData<'mcx>,
    eq_expr: Option<&mut PgBox<'mcx, ExprState<'mcx>>>,
    key_attrs: &[KeyAttr],
    binary_mode: bool,
    per_tuple: Mcx<'_>,
    slot_mcx: Mcx<'mcx>,
    params: NonNull<MinimalTupleData>,
) -> PgResult<bool> {
    // SAFETY: entry images live in the table context until evicted/reset.
    unsafe { exectuples::exec_store_minimal_tuple_ptr(tableslot, slot_mcx, params) };
    if binary_mode {
        exectuples::slot_getallattrs(tableslot);
        exectuples::slot_getallattrs(probeslot);
        let t = tableslot.base();
        let p = probeslot.base();
        for (i, &KeyAttr { byval, len }) in key_attrs.iter().enumerate() {
            if t.tts_isnull[i] != p.tts_isnull[i] {
                return Ok(false);
            }
            if t.tts_isnull[i] {
                continue;
            }
            if !datum_image_eq(per_tuple, t.tts_values[i], p.tts_values[i], byval, len)? {
                return Ok(false);
            }
        }
        Ok(true)
    } else {
        let eq = eq_expr.expect("logical-mode eq expr");
        // SAFETY: the per-tuple context outlives this eval (reset-only).
        unsafe { eq.arm_result_mcx_raw(per_tuple) };
        let mut slots =
            EvalSlots { scan: None, inner: Some(tableslot), outer: Some(probeslot) };
        exec_qual(Some(eq), &mut slots)
    }
}

fn build_hash_table(node: &mut MemoizeState<'_>) {
    let mut size = node.plan.est_entries;
    if size == 0 {
        size = 1024;
    }
    node.hashtab.reserve(size as usize, |_| unreachable!("empty table rehash"));
    node.built = true;
}

fn empty_entry_bytes(params_len: u32) -> u64 {
    SIZEOF_MEMOIZE_ENTRY + SIZEOF_MEMOIZE_KEY + params_len as u64
}

fn cache_tuple_bytes(t_len: u32) -> u64 {
    SIZEOF_MEMOIZE_TUPLE + t_len as u64
}

fn entry_purge_tuples(node: &mut MemoizeState<'_>, ix: u32) {
    let mcx = table_mcx(node.table_ctx);
    let entry = node.entries[ix as usize].as_mut().expect("live entry");
    let mut freed: u64 = 0;
    let mut cur = entry.tuplehead;
    while let Some(t) = cur {
        // SAFETY: owned chain nodes in the table context.
        unsafe {
            let next = next_of(t);
            let t_len = tuple_t_len(t);
            freed += cache_tuple_bytes(t_len);
            free_image(mcx, t, TUPLE_PREFIX + t_len as usize);
            cur = next;
        }
    }
    entry.complete = false;
    entry.tuplehead = None;
    node.mem_used -= freed;
}

fn remove_cache_entry(node: &mut MemoizeState<'_>, ix: u32) {
    node.lru_unlink(ix);
    entry_purge_tuples(node, ix);
    let mcx = table_mcx(node.table_ctx);
    let entry = node.entries[ix as usize].take().expect("live entry");
    node.mem_used -= empty_entry_bytes(unsafe { entry.params.as_ref() }.t_len);
    match node.hashtab.find_entry(entry.hash as u64, |&e| e == ix) {
        Ok(occ) => {
            occ.remove();
        }
        Err(_) => panic!("could not find memoization table entry"),
    }
    // SAFETY: the key image is owned by this entry.
    unsafe { free_image(mcx, entry.params.cast(), entry.params.as_ref().t_len as usize) };
    node.free_slots.push(ix);
}

fn cache_purge_all(node: &mut MemoizeState<'_>) {
    let evictions = node.entries.iter().filter(|e| e.is_some()).count() as u64;
    cache_free_all(node);
    node.stats.cache_evictions += evictions;
}

// C resets the table context wholesale; this aset's accounting demands
// balanced frees, so walk the entries first (purge/end only — rare).
fn cache_free_all(node: &mut MemoizeState<'_>) {
    let mcx = table_mcx(node.table_ctx);
    for i in 0..node.entries.len() {
        if node.entries[i].is_some() {
            entry_purge_tuples(node, i as u32);
            let entry = node.entries[i].take().expect("live entry");
            // SAFETY: the key image is owned by this entry.
            unsafe { free_image(mcx, entry.params.cast(), entry.params.as_ref().t_len as usize) };
        }
    }
    // SAFETY: sole reference; every allocation was just freed.
    unsafe { (*node.table_ctx.as_ptr()).reset() };
    node.entries.clear();
    node.free_slots.clear();
    node.hashtab.clear();
    node.lru_head = INVALID;
    node.lru_tail = INVALID;
    node.last_tuple = None;
    node.entry = INVALID;
    node.mem_used = 0;
}

/// Returns false when `specialkey` got evicted.
fn cache_reduce_memory(node: &mut MemoizeState<'_>, specialkey: u32) -> bool {
    let mut specialkey_intact = true;
    let mut evictions: u64 = 0;
    if node.mem_used > node.stats.mem_peak {
        node.stats.mem_peak = node.mem_used;
    }
    debug_assert!(node.mem_used > node.mem_limit);
    let mut ix = node.lru_head;
    while ix != INVALID {
        let next = node.entries[ix as usize].as_ref().expect("live entry").lru_next;
        if ix == specialkey {
            specialkey_intact = false;
        }
        remove_cache_entry(node, ix);
        evictions += 1;
        if node.mem_used <= node.mem_limit {
            break;
        }
        ix = next;
    }
    node.stats.cache_evictions += evictions;
    specialkey_intact
}

/// `cache_lookup`; stable handles skip C's post-eviction re-find.
fn cache_lookup<'mcx>(
    node: &mut MemoizeState<'mcx>,
    estate: &mut EStateData<'mcx>,
    found: &mut bool,
) -> PgResult<Option<u32>> {
    let kernel = node.kernel;
    let (hash, kernel_key, kernel_isnull);
    let existing = if kernel == ProbeKernel::Expr {
        prepare_probe_slot(node, estate)?;
        hash = probe_hash(node, estate)?;
        (kernel_key, kernel_isnull) = (Datum::null(), false);
        let per_tuple = estate.ecxt(node.ps_ExprContext).per_tuple_mcx();
        let slot_mcx = estate.es_query_cxt;

        let MemoizeState {
            entries, hashtab, tableslot, probeslot, eq_expr, key_attrs, binary_mode, ..
        } = node;
        let mut eq_err: Option<Box<PgError>> = None;
        let existing = hashtab
            .find(hash as u64, |&ix| {
                let e = entries[ix as usize].as_ref().expect("live entry");
                if e.hash != hash {
                    return false;
                }
                match probe_equal(
                    tableslot,
                    probeslot,
                    eq_expr.as_mut(),
                    key_attrs,
                    *binary_mode,
                    per_tuple,
                    slot_mcx,
                    e.params,
                ) {
                    Ok(m) => m,
                    Err(err) => {
                        eq_err = Some(err);
                        false
                    }
                }
            })
            .copied();
        if let Some(err) = eq_err {
            return Err(err);
        }
        existing
    } else {
        let (key, isnull) = eval_key(&mut node.param_exprs[0], node.ps_ExprContext, estate)?;
        (kernel_key, kernel_isnull) = (key, isnull);
        let h32 = match (kernel, isnull) {
            (_, true) => 0,
            (ProbeKernel::Int4, _) => hashfn::hash_bytes_uint32(key.as_u32()),
            (ProbeKernel::Int8, _) => hashfn::hash_bytes_uint32(hashint8_fold(key)),
            // datum_image_hash byval arm: the full Datum word's bytes.
            _ => hashfn::hash_bytes(&key.as_usize().to_ne_bytes()),
        };
        hash = hashfn::murmurhash32(h32);
        let MemoizeState { entries, hashtab, .. } = node;
        // NOT DISTINCT over the entry's cached key word: grouping-equal fold
        // in logical mode, the binary probe_equal isnull fold for ByvalImage
        // (identical shape); byval datum_image_eq is the full-word compare.
        hashtab
            .find(hash as u64, |&ix| {
                let e = entries[ix as usize].as_ref().expect("live entry");
                e.hash == hash
                    && match (isnull, e.key_isnull) {
                        (false, false) => match kernel {
                            ProbeKernel::Int4 => e.key.as_i32() == key.as_i32(),
                            ProbeKernel::Int8 => e.key.as_i64() == key.as_i64(),
                            _ => e.key.as_usize() == key.as_usize(),
                        },
                        (a, b) => a & b,
                    }
            })
            .copied()
    };
    if let Some(ix) = existing {
        *found = true;
        node.lru_move_tail(ix);
        return Ok(Some(ix));
    }
    *found = false;
    if kernel != ProbeKernel::Expr {
        // Misses are the rare leg: build the probeslot image only now (the
        // entry's params minimal tuple keeps C's accounting + rescan shape).
        exectuples::exec_clear_tuple(&mut node.probeslot, estate.es_query_cxt);
        let base = node.probeslot.base_mut();
        base.tts_values[0] = kernel_key;
        base.tts_isnull[0] = kernel_isnull;
        exectuples::exec_store_virtual_tuple(&mut node.probeslot);
    }

    let params = exectuples::exec_copy_slot_minimal_tuple(
        &mut node.probeslot,
        estate.es_query_cxt,
        table_mcx(node.table_ctx),
        0,
    )?;
    let params_ptr =
        NonNull::new(params.as_ptr().cast_mut().cast::<MinimalTupleData>()).expect("key image");
    let params_len = params.t_len();
    core::mem::forget(params);

    let ix = match node.free_slots.pop() {
        Some(ix) => ix,
        None => {
            let ix = node.entries.len() as u32;
            assert!(ix != INVALID, "memoize cache slot ids exhausted");
            node.entries.push(None);
            ix
        }
    };
    node.entries[ix as usize] = Some(CacheEntry {
        params: params_ptr,
        tuplehead: None,
        hash,
        complete: false,
        lru_prev: INVALID,
        lru_next: INVALID,
        key: kernel_key,
        key_isnull: kernel_isnull,
    });
    let entries_ref = &node.entries;
    node.hashtab.insert_unique(hash as u64, ix, |&i| {
        entries_ref[i as usize].as_ref().expect("live entry").hash as u64
    });
    node.mem_used += empty_entry_bytes(params_len);
    node.lru_push_tail(ix);
    node.last_tuple = None;

    if node.mem_used > node.mem_limit && !cache_reduce_memory(node, ix) {
        return Ok(None);
    }
    Ok(Some(ix))
}

fn cache_store_tuple<'mcx>(
    node: &mut MemoizeState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: ExecSlotId,
) -> PgResult<bool> {
    let slot_mcx = estate.es_query_cxt;
    let tup = exectuples::exec_copy_slot_minimal_tuple(
        estate.slot_mut(slot),
        slot_mcx,
        table_mcx(node.table_ctx),
        TUPLE_PREFIX,
    )?;
    let t_len = tup.t_len();
    // SAFETY: prefix sits TUPLE_PREFIX bytes before the image.
    let tnode = unsafe {
        NonNull::new_unchecked(tup.as_ptr().cast_mut().sub(TUPLE_PREFIX))
    };
    core::mem::forget(tup);
    // SAFETY: fresh chain node.
    unsafe { set_next(tnode, None) };

    node.mem_used += cache_tuple_bytes(t_len);
    let ix = node.entry;
    let entry = node.entries[ix as usize].as_mut().expect("current entry");
    if entry.tuplehead.is_none() {
        entry.tuplehead = Some(tnode);
    } else {
        // SAFETY: last_tuple is the live tail of this entry's chain.
        unsafe { set_next(node.last_tuple.expect("chain tail"), Some(tnode)) };
    }
    node.last_tuple = Some(tnode);

    if node.mem_used > node.mem_limit && !cache_reduce_memory(node, ix) {
        return Ok(false);
    }
    Ok(true)
}

pub fn exec_memoize<'mcx, C: MemoizeChild<'mcx>>(
    node: &mut MemoizeState<'mcx>,
    outer: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    cfi()?;
    estate.reset_expr_context(node.ps_ExprContext);

    loop {
        match node.mstatus {
            MemoStatus::CacheLookup => {
                debug_assert!(node.entry == INVALID);
                if !node.built {
                    build_hash_table(node);
                }
                let mut found = false;
                let entry = cache_lookup(node, estate, &mut found)?;

                if found {
                    let ix = entry.expect("found entries are never None");
                    if node.entries[ix as usize].as_ref().expect("live entry").complete {
                        node.stats.cache_hits += 1;
                        let head =
                            node.entries[ix as usize].as_ref().expect("live entry").tuplehead;
                        node.last_tuple = head;
                        node.entry = ix;
                        let Some(t) = head else {
                            node.mstatus = MemoStatus::EndOfScan;
                            return Ok(None);
                        };
                        node.mstatus = MemoStatus::FetchNextTuple;
                        let slot = node.ps_ResultTupleSlot;
                        let mcx = estate.es_query_cxt;
                        // SAFETY: cached image lives until eviction; no
                        // inserts happen while fetching from this entry.
                        unsafe {
                            exectuples::exec_store_minimal_tuple_ptr(
                                estate.slot_mut(slot),
                                mcx,
                                tuple_of(t),
                            )
                        };
                        return Ok(Some(slot));
                    }
                }
                node.stats.cache_misses += 1;
                if found {
                    // Incomplete scans restart from scratch: the outer node
                    // gives no ordering guarantee across rescans.
                    entry_purge_tuples(node, entry.expect("found entry"));
                }

                let Some(outerslot) = outer.exec_proc(estate)? else {
                    if let Some(ix) = entry {
                        node.entries[ix as usize].as_mut().expect("live entry").complete = true;
                    }
                    node.mstatus = MemoStatus::EndOfScan;
                    return Ok(None);
                };
                node.entry = entry.unwrap_or(INVALID);

                if entry.is_none() || !cache_store_tuple(node, estate, outerslot)? {
                    node.stats.cache_overflows += 1;
                    node.mstatus = MemoStatus::BypassMode;
                } else {
                    let ix = node.entry;
                    node.entries[ix as usize].as_mut().expect("live entry").complete =
                        node.singlerow;
                    node.mstatus = MemoStatus::FillingCache;
                }
                return copy_to_result(node, estate, outerslot);
            }
            MemoStatus::FetchNextTuple => {
                debug_assert!(node.entry != INVALID);
                // SAFETY: live chain node of the current entry.
                let next = unsafe { next_of(node.last_tuple.expect("cache-hit cursor")) };
                node.last_tuple = next;
                let Some(t) = next else {
                    node.mstatus = MemoStatus::EndOfScan;
                    return Ok(None);
                };
                let slot = node.ps_ResultTupleSlot;
                let mcx = estate.es_query_cxt;
                // SAFETY: cached image lives until eviction.
                unsafe {
                    exectuples::exec_store_minimal_tuple_ptr(
                        estate.slot_mut(slot),
                        mcx,
                        tuple_of(t),
                    )
                };
                return Ok(Some(slot));
            }
            MemoStatus::FillingCache => {
                let ix = node.entry;
                debug_assert!(ix != INVALID);
                let Some(outerslot) = outer.exec_proc(estate)? else {
                    node.entries[ix as usize].as_mut().expect("live entry").complete = true;
                    node.mstatus = MemoStatus::EndOfScan;
                    return Ok(None);
                };
                if node.entries[ix as usize].as_ref().expect("live entry").complete {
                    return Err(Box::new(PgError::error(
                        "cache entry already complete".to_string(),
                    )));
                }
                if !cache_store_tuple(node, estate, outerslot)? {
                    node.stats.cache_overflows += 1;
                    node.mstatus = MemoStatus::BypassMode;
                }
                return copy_to_result(node, estate, outerslot);
            }
            MemoStatus::BypassMode => {
                let Some(outerslot) = outer.exec_proc(estate)? else {
                    node.mstatus = MemoStatus::EndOfScan;
                    return Ok(None);
                };
                return copy_to_result(node, estate, outerslot);
            }
            MemoStatus::EndOfScan => return Ok(None),
        }
    }
}

fn copy_to_result<'mcx>(
    node: &MemoizeState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outerslot: ExecSlotId,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    let result = node.ps_ResultTupleSlot;
    let table = &mut estate.es_tupleTable[..];
    let [dst, src] = table
        .get_disjoint_mut([result.0 as usize, outerslot.0 as usize])
        .expect("distinct in-range memoize slot ids");
    exectuples::exec_copy_slot(dst, src, mcx, mcx)?;
    Ok(Some(result))
}

pub fn exec_end_memoize(node: &mut MemoizeState<'_>) {
    #[cfg(debug_assertions)]
    {
        let mut mem: u64 = 0;
        for e in node.entries.iter().flatten() {
            // SAFETY: live entry images.
            unsafe {
                mem += empty_entry_bytes(e.params.as_ref().t_len);
                let mut cur = e.tuplehead;
                while let Some(t) = cur {
                    mem += cache_tuple_bytes(tuple_t_len(t));
                    cur = next_of(t);
                }
            }
        }
        debug_assert!(mem == node.mem_used, "memoize memory accounting drift");
    }
    cache_free_all(node);
    node.hashtab = hashbrown::HashTable::new();
    for e in node.param_exprs.iter_mut() {
        if let KeyExpr::Expr(e) = e {
            e.release_frames();
        }
    }
    if let Some(e) = node.hash_expr.as_mut() {
        e.release_frames();
    }
    if let Some(e) = node.eq_expr.as_mut() {
        e.release_frames();
    }
    node.hash_expr = None;
    node.eq_expr = None;
    node.tableslot.base_mut().tts_tupleDescriptor = None;
    node.probeslot.base_mut().tts_tupleDescriptor = None;
    node.ps_ResultTupleDesc = None;
}

/// `ExecReScanMemoize`, cache side; the caller handles the outer child per
/// the chgParam protocol.
pub fn exec_rescan_memoize(node: &mut MemoizeState<'_>) {
    node.mstatus = MemoStatus::CacheLookup;
    node.entry = INVALID;
    node.last_tuple = None;
}

/// The `bms_nonempty_difference(outerPlan->chgParam, keyparamids)` purge.
pub fn exec_rescan_memoize_purge(node: &mut MemoizeState<'_>) {
    cache_purge_all(node);
}

pub fn keyparamids<'a>(node: &MemoizeState<'a>) -> &'a ::types_nodes::bitmapset::Bitmapset<'a> {
    &node.plan.keyparamids
}

/// show_memoize_info's stats read; mem_peak resolved as C's display does
/// (mem_used when no eviction ever set a peak).
pub fn memoize_stats(node: &MemoizeState<'_>) -> MemoizeInstrumentation {
    let mut s = node.stats;
    if s.mem_peak == 0 {
        s.mem_peak = node.mem_used;
    }
    s
}

// Exempt: cache memory is released via exec_end_memoize and the table
// context's registered destructor.
mcx::forget_safe_nodrop!(MemoStatus, KeyAttr, ProbeKernel);
mcx::forget_safe_struct!(
    CacheEntry { params, tuplehead, hash, complete, lru_prev, lru_next, key,
        key_isnull },
    MemoizeState<'_> { plan, ps_ExprContext, ps_ResultTupleSlot, mstatus, nkeys,
        key_attrs, kernel, binary_mode, singlerow, entries, free_slots, built,
        lru_head, lru_tail, table_ctx, mem_used, mem_limit, entry, last_tuple;
        ps_ResultTupleDesc, tableslot, probeslot, param_exprs, hash_expr,
        eq_expr, hashtab, stats },
);
