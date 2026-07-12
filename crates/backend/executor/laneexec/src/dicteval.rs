// DictEval (dict-pushdown Inc1, docs/design/dict-pushdown.md §2-§4): one
// generic step evaluating a pure expression over a dict-encoded text column
// once per distinct dictionary value per row group. Admission is fail-closed
// (IMMUTABLE-only, strict internal-language builtins, deterministic
// collation, inline const siblings); evaluation is LAZY by default — a memo
// slot fills at the first selected-row occurrence of its code, in driver row
// order, so the first error fires at exactly the row C's per-row drive would
// raise it — and EAGER only for kernels carrying a non-erroring proof.
// Any surprise (representation, non-inline dict entry, memo byte cap)
// demotes the window to the per-row program; the memo path never raises
// from a context the per-row drive wouldn't reach.
use datum::{Datum, NullableDatum};
use exectuples::{SoaBatch, SoaDictLane};
use mcx::MemoryContext;
use types_core::{Oid, OidIsValid};
use types_error::PgResult;
use types_fmgr::{FmgrInfo, LocalFcinfo};

// §4 byte gate on memoized by-ref results per prog per epoch. ClickHouse has
// no query-time analog (its dictionary-side function execution is type-gated
// and unbounded; low_cardinality_max_dictionary_size bounds dictionaries at
// part-WRITE time only), so this cap is a deviation — a safety valve, sized
// so the shipped 12-35K-entry URL dicts can never trip it (see the design
// doc's §4 amendment).
pub const DICTEVAL_MEMO_CAP: usize = 16 << 20;

const KERNEL_MAX_ARGS: usize = 8;
// pg_proc.h INTERNALlanguageId: only internal-language builtins are
// admitted — their Rust ports are the production per-row implementations,
// so parity holds by construction and no SPI/plpgsql can run under a kernel.
const INTERNAL_LANGUAGE_ID: Oid = 12;
const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;
const TYPTYPE_PSEUDO: i8 = b'p' as i8;

const F_TEXTLEN: Oid = 1257;
const F_CHAR_LENGTH: Oid = 1381;
const F_TEXTOCTETLEN: Oid = 1258;

/// One call of the admitted composition, walker-extracted (node-free so the
/// surface owns the Node context and this module owns catalog authority).
/// `args[var_argno]` is ignored — that slot receives the inner value (the
/// dict entry for calls[0], the previous call's result above).
pub struct DictCallSpec {
    pub fn_oid: Oid,
    pub collation: Oid,
    pub var_argno: u16,
    pub args: Vec<NullableDatum>,
    /// Const siblings are by-val or proven-inline varlena (walker contract).
    pub rettype: Oid,
}

pub struct DictExprSpec {
    pub col: u16,
    /// Innermost first: calls[0] consumes the column's text payload.
    pub calls: Vec<DictCallSpec>,
}

struct FmgrCall {
    flinfo: FmgrInfo,
    collation: Oid,
    var_argno: u16,
    args: Vec<NullableDatum>,
}

enum DictKernel {
    /// length()/octet_length(): non-erroring proof — pg_mbstrlen is total
    /// over arbitrary bytes and any inline text's length fits int4 — so the
    /// whole-dictionary EAGER fill can never raise an error the per-row
    /// drive wouldn't reach.
    TextLen { octet: bool },
    /// Generic composition through the functions' production entry points.
    /// No proof, LAZY only.
    Fmgr(Vec<FmgrCall>),
    #[cfg(test)]
    Test(Box<dyn Fn(&[u8]) -> PgResult<NullableDatum>>),
}

enum OutForm {
    /// memo[code] = Some(result) once filled; by-ref results live in `arena`.
    Value { memo: Vec<Option<NullableDatum>> },
    /// Boolean roots: 2 bits/code (value + filled). Consumed by the Inc2
    /// predicate surface; the core mechanics land here so both forms share
    /// the epoch/cap/demote contract.
    #[allow(dead_code)]
    Bitmap { bits: Vec<u64>, filled: Vec<u64> },
}

/// Per-window representation of a prog's source column.
pub enum ColView<'a> {
    Dict(SoaDictLane),
    Raw { values: &'a [Datum], isnull: &'a [bool] },
    Missing,
}

#[derive(Debug)]
pub enum Prepared {
    Ready,
    /// The window must run the checked per-row program (demote-never-raise).
    Demote(&'static str),
}

pub struct DictEvalProg {
    expr_id: u64,
    col: u16,
    kernel: DictKernel,
    eager: bool,
    ret_byval: bool,
    out: OutForm,
    memo_epoch: Option<u64>,
    /// By-ref results and chain intermediates; reset only at epoch change,
    /// strictly after every consumer of the previous window copied out (the
    /// proj.rs lifetime contract).
    arena: Option<MemoryContext>,
    /// Memoized by-ref result bytes this epoch (cap accounting).
    memo_bytes: usize,
    byte_cap: usize,
    /// Kernel may raise (no non-erroring proof). Fallible progs are refused
    /// next to other error-capable per-row work (surface admission rule).
    fallible: bool,
    scratch_vals: Vec<Datum>,
    scratch_nulls: Vec<bool>,
    logged: bool,
}

impl DictEvalProg {
    pub fn col(&self) -> u16 {
        self.col
    }

    /// Retarget the source column (projected-scan remap). Epochs are the
    /// scan's shared RG counter — column identity is NOT in the memo key —
    /// so retargeting must invalidate unconditionally.
    pub fn set_col(&mut self, col: u16) {
        if col != self.col {
            self.col = col;
            self.reset_epoch(None, 0);
        }
    }

    pub fn expr_id(&self) -> u64 {
        self.expr_id
    }

    pub fn fallible(&self) -> bool {
        self.fallible
    }

    /// Derived lane view for the fold: valid for the rows selected by the
    /// last successful prepare_batch over this batch.
    pub fn scratch(&self) -> (&[Datum], &[bool]) {
        (&self.scratch_vals, &self.scratch_nulls)
    }
}

/// FNV-1a over the call chain (§7 memo identity). Const datum words hash by
/// value/pointer — stable within one plan, which is all expr_id must
/// disambiguate; syntactically identical subtrees NOT sharing a memo is
/// merely unoptimized, never unsound.
fn expr_id_of(spec: &DictExprSpec) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut mix = |b: u64| {
        for byte in b.to_le_bytes() {
            h ^= byte as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    mix(spec.col as u64);
    for c in &spec.calls {
        mix(c.fn_oid as u64);
        mix(c.collation as u64);
        mix(c.var_argno as u64);
        for (i, a) in c.args.iter().enumerate() {
            if i == c.var_argno as usize {
                continue;
            }
            mix(a.isnull as u64);
            if !a.isnull {
                mix(a.value.as_usize() as u64);
            }
        }
    }
    h
}

/// Fail-closed compilation of an admitted-shape spec into a value-form prog
/// (the agg-args surface). Refusal keeps the classic per-row path; the
/// reason feeds the surface's DEBUG1 marker.
pub fn compile_value(spec: &DictExprSpec) -> Result<Box<DictEvalProg>, &'static str> {
    let (kernel, eager, fallible, rettype) = compile_kernel(spec)?;
    let ret_byval = match lsyscache::get_typlenbyval(rettype) {
        Ok((_, byval)) => byval,
        Err(_) => return Err("rettype not resolvable"),
    };
    // Fmgr chains allocate the input text image and every intermediate in
    // the arena even when the final result is by-val.
    let arena = matches!(kernel, DictKernel::Fmgr(_))
        .then(|| MemoryContext::new_bump("DictEvalMemoContext"));
    Ok(Box::new(DictEvalProg {
        expr_id: expr_id_of(spec),
        col: spec.col,
        kernel,
        eager,
        ret_byval,
        out: OutForm::Value { memo: Vec::new() },
        memo_epoch: None,
        arena,
        memo_bytes: 0,
        byte_cap: DICTEVAL_MEMO_CAP,
        fallible,
        scratch_vals: Vec::new(),
        scratch_nulls: Vec::new(),
        logged: false,
    }))
}

fn compile_kernel(
    spec: &DictExprSpec,
) -> Result<(DictKernel, bool, bool, Oid), &'static str> {
    if spec.calls.is_empty() {
        return Err("empty call chain");
    }
    if !syscache_seams::lookup_pg_proc_shape::is_installed() {
        return Err("pg_proc seam not installed");
    }
    for c in &spec.calls {
        if c.args.len() > KERNEL_MAX_ARGS {
            return Err("too many args");
        }
        if c.var_argno as usize >= c.args.len() {
            return Err("var argno out of range");
        }
        let Ok(Some(shape)) = syscache_seams::lookup_pg_proc_shape::call(c.fn_oid) else {
            return Err("function not in pg_proc");
        };
        if shape.prolang != INTERNAL_LANGUAGE_ID {
            return Err("not an internal-language builtin");
        }
        if shape.provolatile != PROVOLATILE_IMMUTABLE {
            return Err("not IMMUTABLE");
        }
        if shape.proretset {
            return Err("set-returning");
        }
        // Strictness: dict lanes are NULL-free by contract, but raw windows
        // and NULL Const siblings are not — strict kernels get
        // NULL-in/NULL-out for free; non-strict semantics are refused.
        if !shape.proisstrict {
            return Err("not strict");
        }
        if shape.pronargs as usize != c.args.len() {
            return Err("arity mismatch");
        }
        if c.args.iter().enumerate().any(|(i, a)| i != c.var_argno as usize && a.isnull) {
            // A NULL const under a strict fn makes the whole expr NULL per
            // row; the per-row path answers that shape without us.
            return Err("null const sibling");
        }
        // Walker-supplied node types must agree with the catalog — a
        // mismatch would mis-derive ret_byval and defeat the byte cap.
        if shape.prorettype != c.rettype {
            return Err("rettype disagrees with catalog");
        }
        // Polymorphic/pseudo signatures may consult fn_expr (unset in the
        // kernel fcinfo) — refuse anything whose catalog result type is not
        // concrete; the walker refuses pseudo-typed arguments.
        if !matches!(lsyscache::get_typtype(shape.prorettype), Ok(t) if t != TYPTYPE_PSEUDO) {
            return Err("pseudo-type result");
        }
        if OidIsValid(c.collation) && !crate::dict::collation_usable(c.collation) {
            return Err("collation not usable");
        }
    }
    let rettype = spec.calls.last().unwrap().rettype;
    // Proof-carrying fast kernel: a lone length()/octet_length() call.
    if spec.calls.len() == 1 {
        let c = &spec.calls[0];
        match c.fn_oid {
            F_TEXTLEN | F_CHAR_LENGTH => {
                return Ok((DictKernel::TextLen { octet: false }, true, false, rettype))
            }
            F_TEXTOCTETLEN => {
                return Ok((DictKernel::TextLen { octet: true }, true, false, rettype))
            }
            _ => {}
        }
    }
    let mut calls = Vec::with_capacity(spec.calls.len());
    for c in &spec.calls {
        let Ok(flinfo) = fmgr_core::fmgr_info(c.fn_oid) else {
            return Err("fmgr resolution failed");
        };
        if flinfo.fn_retset {
            return Err("set-returning");
        }
        calls.push(FmgrCall {
            flinfo,
            collation: c.collation,
            var_argno: c.var_argno,
            args: c.args.clone(),
        });
    }
    Ok((DictKernel::Fmgr(calls), false, true, rettype))
}

/// Pseudo-type argument refusal for the walker: any call whose CATALOG
/// argument types are non-concrete may consult fn_expr at run time.
pub fn func_args_concrete(mcx: mcx::Mcx<'_>, fn_oid: Oid) -> bool {
    let Ok((_, argtypes)) = lsyscache::get_func_signature(mcx, fn_oid) else {
        return false;
    };
    argtypes
        .iter()
        .all(|&t| matches!(lsyscache::get_typtype(t), Ok(tt) if tt != TYPTYPE_PSEUDO))
}

// One kernel application over a text payload. Fmgr chains run every call's
// production entry point with results (and intermediates) in the arena —
// parity by construction (the TextLenLane rule: the memo path uses the same
// implementations as the per-row drive).
fn eval_kernel(
    kernel: &mut DictKernel,
    arena: Option<&MemoryContext>,
    s: &[u8],
) -> PgResult<NullableDatum> {
    match kernel {
        DictKernel::TextLen { octet } => {
            let n = if *octet { varlena::textoctetlen(s) } else { varlena::text_length(s)? };
            Ok(NullableDatum { value: Datum::from_i32(n), isnull: false })
        }
        DictKernel::Fmgr(calls) => {
            // The dict entry payload re-images as a fresh 4B-header text
            // datum for the first call's arg; the arena owns it like every
            // intermediate and result (§4 epoch-lifetime contract).
            let mcx = arena.expect("fmgr kernel always carries an arena").mcx();
            let image = varlena::cstring_to_text(mcx, s)?.into_image();
            let mut cur =
                NullableDatum { value: Datum::from_usize(image.as_ptr() as usize), isnull: false };
            core::mem::forget(image);
            for call in calls.iter_mut() {
                if cur.isnull {
                    // Strict by admission: NULL propagates without a call.
                    return Ok(NullableDatum { value: Datum::null(), isnull: true });
                }
                let mut fcinfo = LocalFcinfo::<KERNEL_MAX_ARGS>::fresh(call.collation);
                fcinfo.nargs = call.args.len() as i16;
                for (i, a) in call.args.iter().enumerate() {
                    fcinfo.args[i] = *a;
                }
                fcinfo.args[call.var_argno as usize] = cur;
                // SAFETY: the arena outlives the call; results allocated
                // there follow the §4 epoch-lifetime contract.
                unsafe { fcinfo.set_result_mcx(mcx) };
                let d = call.flinfo.invoke(&mut fcinfo)?;
                cur = NullableDatum { value: d, isnull: fcinfo.isnull };
            }
            Ok(cur)
        }
        #[cfg(test)]
        DictKernel::Test(f) => f(s),
    }
}

fn byref_size(d: Datum) -> usize {
    let p = d.as_usize() as *const u8;
    if p.is_null() {
        return 0;
    }
    // SAFETY: a non-null by-ref result datum is readable through its header.
    unsafe { types_tuple::varatt::varsize_any(p) }
}

impl DictEvalProg {
    fn reset_epoch(&mut self, epoch: Option<u64>, ndict: usize) {
        match &mut self.out {
            OutForm::Value { memo } => {
                memo.clear();
                memo.resize(ndict, None);
            }
            OutForm::Bitmap { bits, filled } => {
                let words = ndict.div_ceil(64);
                bits.clear();
                bits.resize(words, 0);
                filled.clear();
                filled.resize(words, 0);
            }
        }
        if let Some(a) = &mut self.arena {
            a.reset();
        }
        self.memo_bytes = 0;
        self.memo_epoch = epoch;
    }

    fn account(&mut self, nd: NullableDatum) -> bool {
        if !self.ret_byval && !nd.isnull {
            self.memo_bytes += byref_size(nd.value);
        }
        self.memo_bytes <= self.byte_cap
    }
}

/// Prepare every prog's derived lane for one staged batch. `rows` is the
/// selected bitmap (guard-demoted batches must not reach here). LAZY memo
/// slots fill ROW-MAJOR ACROSS PROGS — per selected row, progs in plan
/// order — so with several fallible exprs in flight the first raise is
/// still the per-row drive's first raise (the §3.1 argument needs the
/// cross-expression interleaving too). Demote(_) means the whole batch must
/// run the checked per-row program; the caller applies it before any lane
/// transition or filter effect.
pub fn prepare_batch(
    progs: &mut [Box<DictEvalProg>],
    soa: &SoaBatch<'_>,
    rows: &[u64],
    nrows: u32,
) -> PgResult<Prepared> {
    let mut views: Vec<ColView<'_>> = Vec::with_capacity(progs.len());
    for p in progs.iter() {
        views.push(match soa.dict_lane(p.col as usize) {
            Some(lane) => ColView::Dict(lane),
            None if soa.col_datum_ready(p.col as usize) => ColView::Raw {
                values: soa.col_values(p.col as usize),
                isnull: soa.col_isnull(p.col as usize),
            },
            None => ColView::Missing,
        });
    }
    prepare_views(progs, &views, rows, nrows)
}

fn prepare_views(
    progs: &mut [Box<DictEvalProg>],
    views: &[ColView<'_>],
    rows: &[u64],
    nrows: u32,
) -> PgResult<Prepared> {
    let nrows = nrows as usize;
    for (p, w) in progs.iter_mut().zip(views) {
        match w {
            ColView::Missing => return Ok(Prepared::Demote("representation")),
            ColView::Dict(lane) => {
                if p.memo_epoch != Some(lane.table.epoch) {
                    if !p.logged {
                        p.logged = true;
                        crate::log_dicteval_engaged(p.col, lane.table.ndict, p.eager);
                    }
                    p.reset_epoch(Some(lane.table.epoch), lane.table.ndict as usize);
                    if p.eager {
                        // EAGER whole-dict fill: admissible only under the
                        // kernel's non-erroring proof; a raise here would
                        // cover codes no selected row carries, so any error
                        // (seam gaps included) demotes instead.
                        if fill_eager(p, *lane).is_err() {
                            p.reset_epoch(None, 0);
                            return Ok(Prepared::Demote("eager fill"));
                        }
                    }
                }
            }
            ColView::Raw { .. } => {
                // Raw windows have no memo (a stale one from a previous dict
                // RG must not survive its arena reset), and their by-ref
                // results live only within the batch — reset per batch so
                // consecutive raw windows never accumulate toward the cap.
                p.reset_epoch(None, 0);
            }
        }
        // Consumers read only prepared (selected) rows; slots for unselected
        // rows are never read, so the fill matters only when the batch grew.
        if p.scratch_vals.len() != nrows {
            p.scratch_vals.clear();
            p.scratch_vals.resize(nrows, Datum::null());
            p.scratch_nulls.clear();
            p.scratch_nulls.resize(nrows, true);
        }
    }
    for (w, &word) in rows.iter().enumerate() {
        let mut bits = word;
        while bits != 0 {
            let i = w * 64 + bits.trailing_zeros() as usize;
            bits &= bits - 1;
            if i >= nrows {
                break;
            }
            for (k, p) in progs.iter_mut().enumerate() {
                let nd = match &views[k] {
                    ColView::Missing => unreachable!("missing views demoted above"),
                    ColView::Dict(lane) => {
                        // SAFETY: SoaDictLane contract — codes covers the
                        // staged window's rows, dict covers ndict entries.
                        let code = unsafe { *lane.codes.add(i) } as usize;
                        let DictEvalProg {
                            kernel, arena, out, memo_bytes, byte_cap, ret_byval, ..
                        } = &mut **p;
                        let OutForm::Value { memo } = out else {
                            return Ok(Prepared::Demote("bitmap prog on value surface"));
                        };
                        match memo[code] {
                            Some(nd) => nd,
                            None => {
                                // SAFETY: code < ndict by the lane contract.
                                let entry = unsafe { *lane.table.dict.add(code) };
                                let Some(s) = crate::dict::inline_varlena_payload(entry)
                                else {
                                    return Ok(Prepared::Demote("non-inline dict entry"));
                                };
                                let nd = eval_kernel(kernel, arena.as_ref(), s)?;
                                if !*ret_byval && !nd.isnull {
                                    *memo_bytes += byref_size(nd.value);
                                }
                                if *memo_bytes > *byte_cap {
                                    return Ok(Prepared::Demote("memo byte cap"));
                                }
                                memo[code] = Some(nd);
                                nd
                            }
                        }
                    }
                    ColView::Raw { values, isnull } => {
                        if isnull[i] {
                            // Slots are reused across batches: NULL rows
                            // must write, not rely on a default fill.
                            p.scratch_nulls[i] = true;
                            continue;
                        }
                        let Some(s) = crate::dict::inline_varlena_payload(values[i]) else {
                            return Ok(Prepared::Demote("non-inline raw datum"));
                        };
                        let nd = eval_kernel(&mut p.kernel, p.arena.as_ref(), s)?;
                        if !p.account(nd) {
                            return Ok(Prepared::Demote("memo byte cap"));
                        }
                        nd
                    }
                };
                p.scratch_vals[i] = nd.value;
                p.scratch_nulls[i] = nd.isnull;
            }
        }
    }
    Ok(Prepared::Ready)
}

// Demote-signal fill: Err(()) means "this window must run per-row", never an
// error surface of its own (eager kernels are non-erroring by proof; any
// PgResult error here is a seam/representation surprise, not C's error).
fn fill_eager(p: &mut DictEvalProg, lane: SoaDictLane) -> Result<(), ()> {
    for code in 0..lane.table.ndict as usize {
        // SAFETY: dict covers ndict entries (SoaDictLane contract).
        let entry = unsafe { *lane.table.dict.add(code) };
        let Some(s) = crate::dict::inline_varlena_payload(entry) else {
            return Err(());
        };
        let nd = eval_kernel(&mut p.kernel, p.arena.as_ref(), s).map_err(|_| ())?;
        if !p.account(nd) {
            return Err(());
        }
        match &mut p.out {
            OutForm::Bitmap { bits, filled } => {
                if !nd.isnull && nd.value.as_usize() & 1 == 1 {
                    bits[code / 64] |= 1 << (code % 64);
                }
                filled[code / 64] |= 1 << (code % 64);
            }
            OutForm::Value { memo } => memo[code] = Some(nd),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    fn text_image(s: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(s.len() + 4);
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_ne_bytes());
        v.extend_from_slice(s);
        v
    }

    struct DictFixture {
        _images: Vec<Vec<u8>>,
        datums: Vec<Datum>,
        codes: Vec<u32>,
    }

    impl DictFixture {
        fn new(entries: &[&[u8]], codes: &[u32]) -> Self {
            let images: Vec<Vec<u8>> = entries.iter().map(|e| text_image(e)).collect();
            let datums =
                images.iter().map(|i| Datum::from_usize(i.as_ptr() as usize)).collect();
            DictFixture { _images: images, datums, codes: codes.to_vec() }
        }

        fn lane(&self, epoch: u64) -> SoaDictLane {
            SoaDictLane {
                codes: self.codes.as_ptr(),
                table: exectuples::SoaDictTable {
                    dict: self.datums.as_ptr(),
                    ndict: self.datums.len() as u32,
                    epoch,
                    sorted: false,
                },
            }
        }
    }

    fn prog(
        eager: bool,
        fallible: bool,
        cap: usize,
        f: impl Fn(&[u8]) -> PgResult<NullableDatum> + 'static,
    ) -> Box<DictEvalProg> {
        Box::new(DictEvalProg {
            expr_id: 1,
            col: 0,
            kernel: DictKernel::Test(Box::new(f)),
            eager,
            ret_byval: true,
            out: OutForm::Value { memo: Vec::new() },
            memo_epoch: None,
            arena: None,
            memo_bytes: 0,
            byte_cap: cap,
            fallible,
            scratch_vals: Vec::new(),
            scratch_nulls: Vec::new(),
            logged: true,
        })
    }

    fn len_kernel(counter: Rc<Cell<u32>>) -> impl Fn(&[u8]) -> PgResult<NullableDatum> {
        move |s| {
            counter.set(counter.get() + 1);
            Ok(NullableDatum { value: Datum::from_i32(s.len() as i32), isnull: false })
        }
    }

    fn rows(bits: &[usize], n: usize) -> Vec<u64> {
        let mut w = vec![0u64; n.div_ceil(64)];
        for &b in bits {
            w[b / 64] |= 1 << (b % 64);
        }
        w
    }

    #[test]
    fn lazy_fills_once_per_code_per_epoch() {
        let fx = DictFixture::new(&[b"a", b"bb", b"ccc"], &[0, 1, 0, 1, 2]);
        let n = Rc::new(Cell::new(0));
        let mut ps = vec![prog(false, true, usize::MAX, len_kernel(n.clone()))];
        let sel = rows(&[0, 1, 2, 3, 4], 5);
        let views = [ColView::Dict(fx.lane(7))];
        assert!(matches!(
            prepare_views(&mut ps, &views, &sel, 5).unwrap(),
            Prepared::Ready
        ));
        assert_eq!(n.get(), 3);
        let (vals, nulls) = ps[0].scratch();
        assert_eq!(
            vals.iter().map(|d| d.as_i32()).collect::<Vec<_>>(),
            vec![1, 2, 1, 2, 3]
        );
        assert!(nulls.iter().all(|&x| !x));
        // Same epoch: memo hits only.
        prepare_views(&mut ps, &views, &sel, 5).unwrap();
        assert_eq!(n.get(), 3);
        // Epoch change: full invalidation.
        let views = [ColView::Dict(fx.lane(8))];
        prepare_views(&mut ps, &views, &sel, 5).unwrap();
        assert_eq!(n.get(), 6);
    }

    #[test]
    fn lazy_error_fires_at_first_selected_row_only() {
        let fx = DictFixture::new(&[b"ok", b"bad", b"ok2"], &[0, 1, 2]);
        let mk = || {
            prog(false, true, usize::MAX, |s| {
                if s == b"bad" {
                    Err(types_error::PgError::error("boom").into())
                } else {
                    Ok(NullableDatum { value: Datum::from_i32(s.len() as i32), isnull: false })
                }
            })
        };
        // Row 1 (the erroring code) masked out: no raise, its slot unfilled.
        let mut ps = vec![mk()];
        let views = [ColView::Dict(fx.lane(1))];
        assert!(matches!(
            prepare_views(&mut ps, &views, &rows(&[0, 2], 3), 3).unwrap(),
            Prepared::Ready
        ));
        // Row 1 selected: the raise surfaces from its slot fill.
        let mut ps = vec![mk()];
        let err = prepare_views(&mut ps, &views, &rows(&[0, 1, 2], 3), 3).unwrap_err();
        assert!(format!("{err:?}").contains("boom"));
    }

    #[test]
    fn cross_prog_error_order_is_row_major() {
        // Prog A errors at row 3's code, prog B at row 1's code: the per-row
        // drive raises B first, so must we.
        let fx = DictFixture::new(&[b"r0", b"r1", b"r2", b"r3"], &[0, 1, 2, 3]);
        let a = prog(false, true, usize::MAX, |s| {
            if s == b"r3" {
                Err(types_error::PgError::error("prog A").into())
            } else {
                Ok(NullableDatum { value: Datum::from_i32(0), isnull: false })
            }
        });
        let b = prog(false, true, usize::MAX, |s| {
            if s == b"r1" {
                Err(types_error::PgError::error("prog B").into())
            } else {
                Ok(NullableDatum { value: Datum::from_i32(0), isnull: false })
            }
        });
        let mut ps = vec![a, b];
        let views = [ColView::Dict(fx.lane(1)), ColView::Dict(fx.lane(1))];
        let err = prepare_views(&mut ps, &views, &rows(&[0, 1, 2, 3], 4), 4).unwrap_err();
        assert!(format!("{err:?}").contains("prog B"));
    }

    #[test]
    fn eager_fills_whole_dict_on_epoch_change() {
        let fx = DictFixture::new(&[b"a", b"bb", b"ccc", b"dddd"], &[0, 0, 0]);
        let n = Rc::new(Cell::new(0));
        let mut ps = vec![prog(true, false, usize::MAX, len_kernel(n.clone()))];
        let views = [ColView::Dict(fx.lane(1))];
        prepare_views(&mut ps, &views, &rows(&[0], 3), 3).unwrap();
        // One selected row, but the proof-carrying eager fill covers ndict.
        assert_eq!(n.get(), 4);
        let OutForm::Value { memo } = &ps[0].out else { unreachable!() };
        assert!(memo.iter().all(|m| m.is_some()));
    }

    #[test]
    fn byte_cap_demotes_instead_of_raising() {
        static BIG: &[u8] = &[0u8; 64];
        let img: &'static Vec<u8> = Box::leak(Box::new(text_image(BIG)));
        let fx = DictFixture::new(&[b"a", b"b"], &[0, 1]);
        let mut p = prog(false, true, 80, move |_| {
            Ok(NullableDatum { value: Datum::from_usize(img.as_ptr() as usize), isnull: false })
        });
        p.ret_byval = false;
        let mut ps = vec![p];
        let views = [ColView::Dict(fx.lane(1))];
        match prepare_views(&mut ps, &views, &rows(&[0, 1], 2), 2).unwrap() {
            Prepared::Demote(r) => assert_eq!(r, "memo byte cap"),
            Prepared::Ready => panic!("expected cap demote"),
        }
    }

    #[test]
    fn raw_window_invalidates_dict_memo() {
        let fx = DictFixture::new(&[b"a", b"bb"], &[0, 1]);
        let n = Rc::new(Cell::new(0));
        let mut ps = vec![prog(false, true, usize::MAX, len_kernel(n.clone()))];
        let sel = rows(&[0, 1], 2);
        prepare_views(&mut ps, &views_dict(&fx, 3), &sel, 2).unwrap();
        assert_eq!(n.get(), 2);
        // Raw window between two dict windows: per-row eval, memo dropped.
        let raw_imgs: Vec<Vec<u8>> = vec![text_image(b"xyz"), text_image(b"q")];
        let raw_vals: Vec<Datum> =
            raw_imgs.iter().map(|i| Datum::from_usize(i.as_ptr() as usize)).collect();
        let isnull = vec![false, true];
        let views = [ColView::Raw { values: &raw_vals, isnull: &isnull }];
        prepare_views(&mut ps, &views, &sel, 2).unwrap();
        assert_eq!(n.get(), 3);
        let (vals, nulls) = ps[0].scratch();
        assert_eq!(vals[0].as_i32(), 3);
        assert!(!nulls[0] && nulls[1]);
        assert!(ps[0].memo_epoch.is_none());
        // Same dict epoch again: must refill (the arena reset dropped it).
        prepare_views(&mut ps, &views_dict(&fx, 3), &sel, 2).unwrap();
        assert_eq!(n.get(), 5);
    }

    fn views_dict(fx: &DictFixture, epoch: u64) -> [ColView<'_>; 1] {
        [ColView::Dict(fx.lane(epoch))]
    }

    #[test]
    fn missing_view_demotes_representation() {
        let mut ps = vec![prog(false, true, usize::MAX, |_| {
            Ok(NullableDatum { value: Datum::from_i32(0), isnull: false })
        })];
        match prepare_views(&mut ps, &[ColView::Missing], &rows(&[0], 1), 1).unwrap() {
            Prepared::Demote(r) => assert_eq!(r, "representation"),
            Prepared::Ready => panic!("expected representation demote"),
        }
    }

    #[test]
    fn bitmap_form_eager_fill() {
        let fx = DictFixture::new(&[b"x", b"yy", b"zzz"], &[0, 1, 2]);
        let mut p = prog(true, false, usize::MAX, |s| {
            Ok(NullableDatum { value: Datum::from_i32((s.len() % 2 == 0) as i32), isnull: false })
        });
        p.out = OutForm::Bitmap { bits: Vec::new(), filled: Vec::new() };
        p.reset_epoch(Some(1), 3);
        fill_eager(&mut p, fx.lane(1)).unwrap();
        let OutForm::Bitmap { bits, filled } = &p.out else { unreachable!() };
        assert_eq!(filled[0] & 0b111, 0b111);
        assert_eq!(bits[0] & 0b111, 0b010);
    }
}
