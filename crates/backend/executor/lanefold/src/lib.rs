// Lane-native aggregate transition fold — the whole-batch int-family
// transition kernels harvested from the cbstore branch's
// `nodeagg/src/lanefold.rs` (inter-query-scheduling worktree), delivered as a
// standalone crate for the lane-executor-v2 hash-agg breaker.
//
// # Harvest provenance (what was kept vs stripped)
//
// KEPT (the vertical slice's core):
// - The `classify_trans` transfn-oid whitelist: COUNT(*)/COUNT(any),
//   SUM/AVG(int2/int4), SUM/AVG(int8) (Phase-3 extension: int8_avg_accum's
//   Int128AggState carrier), MIN/MAX(int2/int4/int8/date/timestamp/
//   timestamptz), over a bare outer Var or an admitted affine OpExpr
//   (`(v / divk) * mulk + addend` from int24pl/int42mi/int4mul/int24div/...).
//   Fold-coverage tier 2 adds MIN/MAX(float4/float8) (float.c larger/smaller
//   with NaN-greatest, last-tied-wins bit semantics), bool_and/bool_or/every
//   (booland/boolor_statefunc), and bit_and/bit_or(int2/int4/int8) — all
//   strict NULL-init transfns, all TYPE-level non-erroring. Tier 3 adds
//   MIN/MAX(text/varchar/bpchar) under the memcmp collation tier (C/POSIX
//   only) with a per-batch inline-varlena proof (vguards) and C's exact
//   datumCopy-into-aggcontext transvalue discipline. The strlenfold tier
//   (lane-v2-strlenfold, CB Q28) adds the int4 SUM/AVG/MIN/MAX/bit kinds
//   over `length(text Var)` / `octet_length(text Var)` — Var-pointer-backed
//   integer lane widths (VarLenBytes/VarLenChars) whose kernels read the
//   char/byte count straight off the inline payload (uguard-proven exact
//   for UTF-8), never materializing per-row result datums.
// - The TYPE-level non-erroring proof (`safe_interval`/`type_proof`): an
//   admitted expression is only folded unchecked when every value of the
//   Var's type width provably lands inside int4 — otherwise the admission
//   carries a DATA-level `Guard` interval that `check_guards` must re-prove
//   per batch (zone-map or exact lane pass) before the fold may run; a failed
//   proof demotes the whole batch to the checked per-row program, which
//   raises C's error at C's row (the interval is exact, so a demoted batch
//   always raises).
// - The whole-batch fold kernels (`fold_batch`) and the grouped per-row-lane
//   fold (`fold_rows_grouped`), byte-parity contract intact: every kernel
//   folds a commutative, non-erroring transition whose result is independent
//   of row order (i64 wrapping addition, min/max), so batch-major evaluation
//   is bit-identical to C's row-major transition order.
// - The cross-aggregate CSE schedule (`build_cse`): SumBase groups
//   Sum/AvgAccum/CountAny over one (col, divk) into a single (count, raw-sum)
//   pass with per-member `mulk*S + addend*c` derivation (legal in the
//   mod-2^64 ring); MinMax groups structurally identical transforms into one
//   scan.
// - The int8[2] {count,sum} AVG transarray discipline (`avg_apply`,
//   `new_int8_transarray`) matching C numeric.c's Int8TransTypeData carrier.
//
// STRIPPED (cbstore/lane-v1 wiring that does not exist on lane-executor-v2):
// - Dict-coded windows: DictEval derived lanes, the dict-group memo, textlen
//   lanes, and the per-(code, group) text MIN/MAX memo (`TextMmLane`).
//   (The metadata-answered transitions — `classify_meta`/`MetaTrans` — were
//   re-harvested 2026-07-12 for the lane-v2 metaagg arm; see below.)
// - The exact-DISTINCT hash sets (`DistinctSet`) and the parallel
//   partial-distinct sharding — separate machinery from the transition fold.
// - The specialized group-probe `KeyTable`, projected-scan column remaps, the
//   residual ExprState, and the once-per-plan logging markers: those belong
//   to the consuming node, not the kernel.
//
// # Integration point for the hash-agg breaker
//
// At plan build: run `classify(mcx, specs)` over the pertrans specs. It
// returns a `LanePlan` when at least one transition admits; `plan.resid`
// lists the transnos that did NOT admit (the breaker keeps its per-row
// program for those), and `plan.cols` lists the lane columns the fold reads.
//
// Per batch of transition inputs:
// 1. If `plan.guarded`, run `check_guards(&plan, &cols, rows, zone_minmax)`;
//    on `GuardCheck::Demote` run the WHOLE batch through the checked per-row
//    program (never mix a partial fold with per-row transitions).
// 2. Ungrouped (one group): `fold_batch(&plan, &cols, rows, nrows, pergroup,
//    aggcxt)`.
// 3. Grouped: after the per-row hash probe snapshots each row's pergroup
//    pointer, `fold_rows_grouped(&plan, &cols, &idxs, &groups, aggcxt)`.
// AvgAccum pergroups must be initialized with `new_int8_transarray` (C's
// non-null initval); Sum/Min/Max start `no_trans_value`/NULL per C.
// Int128AvgAccum (sum/avg(int8)) needs no pre-init: its INTERNAL
// Int128AggState is lazily allocated by the fold in `aggcxt` — the SAME
// aggcontext the per-row `int8_avg_accum` reaches via fcinfo->context, so
// fold-fed and demoted/residual batches accumulate into one shared state.
//
// Anything not admitted classifies out (fail-open, per transition): the
// breaker falls back to its per-row transition program for that agg shape.

use core::ptr::NonNull;

use ::adt_numeric::aggregates::{do_int128_accum, Int128AggState};
use ::datum::Datum;
use ::execexpr::{AggPerGroup, AggTransSpec, OUTER_VAR};
use ::exectuples::SoaBatch;
use ::mcx::{Mcx, PgVec};
use ::types_core::catalog::{
    BOOLOID, BPCHAROID, C_COLLATION_OID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
    INT8OID, POSIX_COLLATION_OID, TEXTOID, TIMESTAMPOID, TIMESTAMPTZOID, VARCHAROID,
};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_nodes::node_tree::Node;

#[cfg(test)]
mod tests;

const F_INT8INC: Oid = 1219;
const F_INT8INC_ANY: Oid = 2804;
const F_INT2_SUM: Oid = 1840;
const F_INT4_SUM: Oid = 1841;
const F_INT2_AVG_ACCUM: Oid = 1962;
const F_INT4_AVG_ACCUM: Oid = 1963;
const F_INT8_AVG_ACCUM: Oid = 2746;
const F_INT4LARGER: Oid = 768;
const F_INT4SMALLER: Oid = 769;
const F_INT2LARGER: Oid = 770;
const F_INT2SMALLER: Oid = 771;
const F_INT8LARGER: Oid = 1236;
const F_INT8SMALLER: Oid = 1237;
const F_DATE_LARGER: Oid = 1138;
const F_DATE_SMALLER: Oid = 1139;
// 1195/1196 are the timestamptz pair, 2035/2036 the timestamp pair (both
// share the C impl; the aggregates bind them per input type).
const F_TIMESTAMP_SMALLER: Oid = 2035;
const F_TIMESTAMP_LARGER: Oid = 2036;
const F_TIMESTAMPTZ_SMALLER: Oid = 1195;
const F_TIMESTAMPTZ_LARGER: Oid = 1196;
// Fold-coverage tier 2: float MIN/MAX, bool_and/bool_or, bit_and/bit_or.
// Every transfn below is strict with a NULL initval in pg_aggregate (same
// discipline as the int larger/smaller whitelist) and TYPE-level non-erroring
// (pure comparison / AND / OR — no arithmetic, no guard tier needed).
const F_FLOAT4LARGER: Oid = 209;
const F_FLOAT4SMALLER: Oid = 211;
const F_FLOAT8LARGER: Oid = 223;
const F_FLOAT8SMALLER: Oid = 224;
const F_BOOLAND_STATEFUNC: Oid = 2515;
const F_BOOLOR_STATEFUNC: Oid = 2516;
const F_INT2AND: Oid = 1892;
const F_INT2OR: Oid = 1893;
const F_INT4AND: Oid = 1898;
const F_INT4OR: Oid = 1899;
const F_INT8AND: Oid = 1904;
const F_INT8OR: Oid = 1905;
// Fold-coverage tier 3: text/bpchar MIN/MAX (varlena.c text_larger/smaller,
// varchar.c bpchar_larger/smaller). Strict + NULL-init per pg_aggregate.
// Collation-dependent: admitted ONLY under a provably-memcmp collation (C /
// POSIX — varstr_cmp's non-locale fast path, which cannot error or call into
// libc/ICU); every other inputcollid refuses at classify. Varlena inputs are
// additionally DATA-gated per batch: the fold reads payload bytes in place,
// so compressed/external datums demote the whole batch to the checked
// per-row program (which detoasts exactly as C does).
const F_TEXT_LARGER: Oid = 458;
const F_TEXT_SMALLER: Oid = 459;
const F_BPCHAR_LARGER: Oid = 1063;
const F_BPCHAR_SMALLER: Oid = 1064;

// String-length fold inputs (lane-v2-strlenfold): the textlen pg_proc family
// (varlena.c textlen — length/char_length over text, plus the varchar
// aliases the parser resolves through the binary-coercion relabel) and
// textoctetlen (octet_length). An int-family transition over
// `length(text Var)` admits with a Var-pointer-backed integer lane width:
// the lane holds the varlena datum pointers (str-tier staging, vguarded) and
// the kernels read each selected row's CHARACTER length straight off the
// inline payload — no fmgr call, no per-row result datum. bpcharlen (1372)
// has bcTruelen trailing-blank semantics and stays refused.
const F_TEXTLEN: [Oid; 4] = [1257, 1317, 1369, 1381];
const F_TEXTOCTETLEN: Oid = 1374;
// pg_wchar.h pg_enc: PG_UTF8 = 6 (the only multibyte server encoding the
// char-count kernel admits — see classify_len_arg).
const PG_UTF8: i32 = 6;

const F_INT4MUL: Oid = 141;
const F_INT24MUL: Oid = 170;
const F_INT42MUL: Oid = 171;
const F_INT24DIV: Oid = 172;
const F_INT4PL: Oid = 177;
const F_INT24PL: Oid = 178;
const F_INT42PL: Oid = 179;
const F_INT4MI: Oid = 181;
const F_INT24MI: Oid = 182;
const F_INT42MI: Oid = 183;

// numeric.c Int8TransTypeData carrier: 2-element no-nulls int8 array.
pub const ARR_OVERHEAD_NONULLS_1: usize = 24;
pub const INT8_TRANSARRAY_SIZE: usize = ARR_OVERHEAD_NONULLS_1 + 16;

/// The (values, isnull) column lanes a fold reads. Implemented for
/// `SoaBatch`; test harnesses (and any other batch container) provide their
/// own. `col_values(c)`/`col_isnull(c)` must cover every staged row for
/// every column in `LanePlan::cols`.
pub trait LaneCols {
    fn col_values(&self, c: usize) -> &[Datum];
    fn col_isnull(&self, c: usize) -> &[bool];
}

impl LaneCols for SoaBatch<'_> {
    #[inline(always)]
    fn col_values(&self, c: usize) -> &[Datum] {
        SoaBatch::col_values(self, c)
    }

    #[inline(always)]
    fn col_isnull(&self, c: usize) -> &[bool] {
        SoaBatch::col_isnull(self, c)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LaneWidth {
    I16,
    I32,
    I64,
    // Datum-lane widths (fold-coverage tier 2): floats fold on the raw datum
    // word (bit-pattern-preserving; float.h comparison semantics), bools on
    // the canonical bool datum. Never guarded, never affine-transformed —
    // classify only admits them as bare Vars.
    F32,
    F64,
    Bool,
    // Varlena pointer lane (fold-coverage tier 3, text/bpchar MIN/MAX): the
    // lane value is the in-page varlena datum pointer. Never integer-guarded,
    // never affine-transformed; every Var-width lane instead carries a vguard
    // (inline-form batch proof) — see LanePlan::vguards.
    Var,
    // String-length lanes (lane-v2-strlenfold): the lane value is a varlena
    // datum pointer (vguarded like Var), but the kernels READ it as an
    // integer — the admitted `length(v)`/`octet_length(v)` result.
    // VarLenBytes = payload byte count (octet_length under any server
    // encoding; textlen under a 1-byte-max encoding, text_length's
    // max_length==1 arm). VarLenChars = UTF-8 character count, computed as
    // bytes minus continuation bytes; exact-parity with C textlen's pg_mblen
    // walk is guaranteed by the per-batch uguard proof (valid UTF-8, no
    // embedded NUL) — see LanePlan::uguards/check_guards. Both are
    // TYPE-level non-erroring on guard-passed batches (result in
    // [0, 2^30) ⊂ int4) and admit only as the bare textlen-family FuncExpr
    // (no affine composition).
    VarLenBytes,
    VarLenChars,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LaneKind {
    CountStar,
    CountAny,
    // int2_sum/int4_sum: i64 accumulate, NULL init, not strict.
    Sum,
    // int2/int4_avg_accum: in-place {count,sum} int8[2] transarray.
    AvgAccum,
    // int8_avg_accum (sum(int8) AND avg(int8) share it): INTERNAL
    // Int128AggState {n, sum_x} pointer, NULL initval, NOT strict — C
    // allocates the state in the aggcontext on the group's FIRST transfn
    // call (null-input rows included) and accumulates only non-null inputs
    // (numeric.c int8_avg_accum -> do_int128_accum, HAVE_INT128 arm).
    Int128AvgAccum,
    // strict byval larger/smaller with signed total order.
    Min,
    Max,
    // float4/float8 larger/smaller (float.c): strict, NULL init, pure
    // comparison — TYPE-level safe. C's float_gt/float_lt order NaN as
    // GREATER than everything (NaN ties NaN), and larger/smaller return the
    // SECOND argument on a tie, so the fold keeps the LAST tied datum's bits
    // in row order (load-bearing for -0.0 vs 0.0 and NaN payloads).
    FMin,
    FMax,
    // booland/boolor_statefunc (bool.c): strict, NULL init, arg1 && / || arg2
    // — TYPE-level safe, associative and commutative up to the canonical
    // bool datum C recomputes each transition.
    BoolAnd,
    BoolOr,
    // int2/int4/int8 and/or (int.c, int8.c): strict, NULL init, bitwise
    // AND/OR — TYPE-level safe, associative/commutative bit-exact (sign
    // extension commutes with AND/OR, so the i64 fold truncated to res_width
    // equals C's native-width op).
    BitAnd,
    BitOr,
    // text_larger/text_smaller (varlena.c): strict, NULL init, C-collation
    // memcmp + length tiebreak (varstrfastcmp_c). C returns arg1 only on a
    // STRICT win (cmp > 0 / < 0), so every tie — including equal-payload
    // datums with different header forms (short vs 4B) — takes the SECOND
    // argument: last-tied-wins on datum identity, associative on datums
    // (the last element of the winning tie class survives any grouping).
    // The winning input datum is datumCopy'd into the agg context exactly at
    // C's ExecAggCopyTransValue points (copy iff the returned datum is not
    // the stored transvalue).
    StrMin,
    StrMax,
    // bpchar_larger/bpchar_smaller (varchar.c): strict, NULL init,
    // trailing-blank-trimmed C-collation compare (bcTruelen + varstr_cmp).
    // OPPOSITE tie rule from text: C returns arg1 on cmp >= 0 / <= 0, so
    // ties keep the FIRST argument (the stored transvalue survives a tie;
    // first-tied-wins is likewise associative). Ties here include strings
    // differing only in trailing blanks — the survivor keeps ITS padding.
    BpMin,
    BpMax,
}

impl LaneWidth {
    fn range(self) -> (i64, i64) {
        match self {
            LaneWidth::I16 => (i16::MIN as i64, i16::MAX as i64),
            LaneWidth::I32 => (i32::MIN as i64, i32::MAX as i64),
            LaneWidth::I64 => (i64::MIN, i64::MAX),
            // Length lanes: a varlena payload is < 2^30 bytes (1GB toast
            // limit), and the char count never exceeds the byte count. Only
            // reachable through type_proof if a transform admission is ever
            // extended over length args; today's bare admission never guards.
            LaneWidth::VarLenBytes | LaneWidth::VarLenChars => (0, (1 << 30) - 1),
            // Datum lanes are never guarded (classify admits them only under
            // TYPE-level-safe folds over bare Vars); Var lanes carry vguards
            // instead of integer intervals.
            LaneWidth::F32 | LaneWidth::F64 | LaneWidth::Bool | LaneWidth::Var => unreachable!(),
        }
    }
}

// DATA-level admission (proof-carrying tier below the TYPE proof): the
// admitted expression is only overflow-free for lane values inside
// [lo, hi] (the exact safe_interval). Every batch must prove its selected
// non-null values sit inside the interval — from the zone map (granule
// min/max, a superset of the batch) or an exact lane pass — before the
// unchecked fold may run; a failed proof demotes the whole batch to the
// checked per-row program, which raises C's error at C's row.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Guard {
    pub lo: i64,
    pub hi: i64,
}

// Cold per-plan guard side-table entry (lanetrans-compact): the data-level
// Guard interval hoisted OUT of the per-lane LaneTrans. Touched only by
// check_guards — a separate once-per-batch pass that runs only when the plan
// is guarded — never by the per-row fold kernels. `col`/`width` mirror the
// guarded lane's read.
#[derive(Clone, Copy, Debug)]
pub struct GuardEntry {
    pub col: u16,
    pub width: LaneWidth,
    pub lo: i64,
    pub hi: i64,
}

// addend/mulk/divk are i32: every admitted coefficient is an int4 Const (or a
// ±1 identity), so the affine transform's stored coefficients provably fit
// i32; they widen to i64 at the use site, leaving the fold arithmetic
// byte-identical.
#[derive(Clone, Copy, Debug)]
pub struct LaneTrans {
    pub kind: LaneKind,
    pub col: u16,
    // Lane read width (the admitted Var's type) vs the transvalue store
    // width (the transfn's argument/result type — int4 for the int2-Var
    // OpExpr admissions). Min/Max must store at res_width or an in-range
    // int4 result truncates through the int2 datum constructor.
    pub width: LaneWidth,
    pub res_width: LaneWidth,
    // Admitted arg expression, per selected row: ((v / divk) * mulk) + addend
    // with v the lane value. Ops are exclusive (single OpExpr admission), so
    // composition order is never observable.
    pub addend: i32,
    pub mulk: i32,
    pub divk: i32,
    pub transno: u16,
}

const _: () = assert!(core::mem::size_of::<LaneTrans>() <= 24);

// Branchy on the loop-invariant transform fields so LLVM unswitches the
// per-row loops: the dominant addend-only shape must stay a bare add (an
// unconditional sdiv per row cost Q7-class min/max folds ~13% on CB).
#[inline(always)]
fn xform(t: &LaneTrans, v: i64) -> i64 {
    let v = if t.divk != 1 { v / t.divk as i64 } else { v };
    let v = if t.mulk != 1 { v.wrapping_mul(t.mulk as i64) } else { v };
    v.wrapping_add(t.addend as i64)
}

// Cross-aggregate CSE (agg-rewrite-cse): transitions sharing one base lane
// pass. SumBase groups Sum/AvgAccum/CountAny over one (col, divk) — a single
// (count, raw-sum) batch pass; each member's delta derives as
// mulk*S + addend*c. MinMax groups structurally identical Min/Max transforms
// — one batch scan advances every member. Derivation legality: wrapping i64
// ops are the mod-2^64 ring, where multiplication distributes over addition,
// so mulk*Σv' + addend*c bit-equals the per-row Σ(v'*mulk + addend) fold —
// and every per-row term is int4-proven (type/zone/data admission), so both
// equal C's checked per-row evaluation, accumulated with C's own unchecked
// int8 transvalue arithmetic. Groups only ever fold on a fully proven batch:
// check_guards demotes the WHOLE batch to the checked per-row program before
// fold_batch runs (and a demoted batch always raises — the interval is
// exact), so no partial CSE state ever combines with a per-row fold.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CseGroupKind {
    SumBase,
    MinMax,
}

#[derive(Clone, Copy, Debug)]
pub struct CseGroup {
    pub kind: CseGroupKind,
    pub start: u16,
    pub len: u16,
}

/// The classified lane plan: the admitted transitions, their CSE schedule,
/// the cold guard side-table, the lane columns the fold reads, and the
/// transnos that did NOT admit (the caller's per-row residual set).
pub struct LanePlan<'mcx> {
    pub trans: PgVec<'mcx, LaneTrans>,
    // CSE schedule: groups over cse_members (indices into trans); cse_skip is
    // parallel to trans and marks members. Only the ungrouped fold_batch
    // consumes it — grouped folds stay per-trans.
    pub cse: PgVec<'mcx, CseGroup>,
    pub cse_members: PgVec<'mcx, u16>,
    pub cse_skip: PgVec<'mcx, bool>,
    // Cold guard side-table: one entry per guarded lane (empty when the plan
    // is not guarded), in trans order, so check_guards' demote-on-first-fail
    // order is deterministic.
    pub guards: PgVec<'mcx, GuardEntry>,
    // Varlena lane columns (str MIN/MAX inputs), deduped, in trans order: the
    // per-batch inline-form proof check_guards must run — every selected
    // non-null datum must be a plain inline varlena (1B short or 4B
    // uncompressed) or the whole batch demotes to the checked per-row
    // program (which detoasts compressed/external datums exactly as C does).
    pub vguards: PgVec<'mcx, u16>,
    // UTF-8 countability proof columns (VarLenChars lanes), deduped, always a
    // subset of vguards: every selected non-null payload must be valid UTF-8
    // with no embedded NUL or the whole batch demotes — the predicate under
    // which the fold's continuation-byte count is bit-equal to C textlen's
    // pg_mblen walk (stored text is verified server encoding, so a demote
    // here is corrupt-data territory, never a perf path).
    pub uguards: PgVec<'mcx, u16>,
    pub cols: PgVec<'mcx, u16>,
    // Transnos classify refused: the caller keeps its checked per-row
    // transition program for these.
    pub resid: PgVec<'mcx, usize>,
    // Any admitted transition carries a data-level proof obligation (integer
    // Guard interval or varlena vguard): check_guards must run per batch
    // before the fold.
    pub guarded: bool,
}

// Admitted arg shape: v |-> ((v / divk) * mulk) + addend over the lane Var v.
#[derive(Clone, Copy)]
struct LaneArg {
    col: u16,
    width: LaneWidth,
    addend: i64,
    mulk: i64,
    divk: i64,
    guard: Option<Guard>,
}

const PLAIN: (i64, i64, i64) = (0, 1, 1);

// True floor/ceil division for either divisor sign. (Port fix: the cbstore
// original used div_euclid-based forms that are only floor/ceil for b > 0;
// for a negative non-unit mulk with inexact division they widened the safe
// interval by one on each side, admitting the two boundary values whose
// checked C evaluation raises. Exercised by safe_interval_is_exact.)
fn floor_div(a: i64, b: i64) -> i64 {
    let (q, r) = (a / b, a % b);
    if r != 0 && ((r < 0) != (b < 0)) {
        q - 1
    } else {
        q
    }
}

fn ceil_div(a: i64, b: i64) -> i64 {
    let (q, r) = (a / b, a % b);
    if r != 0 && ((r < 0) == (b < 0)) {
        q + 1
    } else {
        q
    }
}

/// The v-interval on which the admitted OpExpr's checked C evaluation cannot
/// raise: the int4-typed result (v/divk)*mulk + addend must fit int4 for
/// every v inside it. Exact for these transforms (monotone in v), so a lane
/// whose values all sit inside the interval evaluates unchecked to the same
/// bytes C's checked per-row ops produce. divk is admitted only as int24div's
/// nonzero const (int2/int4 -> int4 cannot overflow and k != 0 rules out the
/// division-by-zero raise), so the div interval is unbounded.
pub fn safe_interval(addend: i64, mulk: i64, divk: i64) -> (i64, i64) {
    if divk != 1 {
        debug_assert!(mulk == 1 && addend == 0);
        return (i64::MIN, i64::MAX);
    }
    // i32::MIN <= v*mulk + addend <= i32::MAX, |addend| <= 2^31 so the
    // subtraction stays exact in i64.
    let a = i32::MIN as i64 - addend;
    let b = i32::MAX as i64 - addend;
    match mulk {
        0 => {
            if a <= 0 && 0 <= b {
                (i64::MIN, i64::MAX)
            } else {
                (1, 0)
            }
        }
        m if m > 0 => (ceil_div(a, m), floor_div(b, m)),
        m => (ceil_div(b, m), floor_div(a, m)),
    }
}

/// TYPE-level proof: every value of the Var's type width is inside the safe
/// interval (the int2 +/- const admission cites 2^15-bounded inputs; the
/// unchecked i64 SUM accumulation beneath it is safe because even 2^31-max
/// int4 terms over any feasible rowcount stay under 2^63: 2^31 * 10^8 < 2^63,
/// matching C's own unchecked int8 transvalue arithmetic).
pub fn type_proof(width: LaneWidth, addend: i64, mulk: i64, divk: i64) -> bool {
    let (lo, hi) = safe_interval(addend, mulk, divk);
    let (wmin, wmax) = width.range();
    lo <= wmin && wmax <= hi
}

fn width_of(vartype: Oid) -> Option<LaneWidth> {
    match vartype {
        INT2OID => Some(LaneWidth::I16),
        INT4OID | DATEOID => Some(LaneWidth::I32),
        INT8OID | TIMESTAMPOID | TIMESTAMPTZOID => Some(LaneWidth::I64),
        FLOAT4OID => Some(LaneWidth::F32),
        FLOAT8OID => Some(LaneWidth::F64),
        BOOLOID => Some(LaneWidth::Bool),
        TEXTOID | VARCHAROID | BPCHAROID => Some(LaneWidth::Var),
        _ => None,
    }
}

// Outer-slot Var of a lane-readable type; the fused drive's outer slot is the
// scan tuple (no projection admitted with outer reads), so varattno-1 is the
// SoA column.
pub fn classify_var(expr: Node<'_>, expected: Oid) -> Option<(u16, LaneWidth)> {
    let v = expr.as_var()?;
    if v.varno != OUTER_VAR || v.varlevelsup != 0 || v.varattno < 1 || v.vartype != expected {
        return None;
    }
    Some((v.varattno as u16 - 1, width_of(v.vartype)?))
}

// Str-fold arg admission. text_larger/smaller's argument is a text Var, a
// varchar Var under the parser's binary-coercion RelabelType (min/max(varchar)
// resolve to the text aggregates), or a text Var under a collation-only
// RelabelType (eval_const_expressions rewrites `v COLLATE "C"` that way) —
// a relabel changes only the type/collation label, never the datum bytes,
// and the comparison collation is the Aggref's inputcollid (already gated).
// bpchar has its own transfn pair and admits only a bare bpchar Var. No
// OpExpr shapes ever admit for varlena lanes.
fn classify_str_var(expr: Node<'_>, expected: Oid) -> Option<(u16, LaneWidth)> {
    let expr = match expr.as_relabel_type() {
        Some(r) if r.resulttype == expected => r.arg,
        Some(_) => return None,
        None => expr,
    };
    if expected == TEXTOID {
        return classify_var(expr, TEXTOID).or_else(|| classify_var(expr, VARCHAROID));
    }
    classify_var(expr, expected)
}

// The provably-memcmp collation tier: C (950) and POSIX (951) resolve in
// varstr_cmp's non-locale fast path (varstrfastcmp_c — pure memcmp + length
// tiebreak, cannot error, allocate, or call libc/ICU). DEFAULT (100) may
// alias a C-semantics database collation but refuses: classify has no
// catalog access and the OID whitelist must stay self-contained; libc/ICU
// collations refuse because their per-row comparison can allocate and (ICU)
// error mid-batch, which the fold cannot replay at C's row.
fn str_collation_safe(collid: Oid) -> bool {
    collid == C_COLLATION_OID || collid == POSIX_COLLATION_OID
}

// textlen-family FuncExpr over a text lane Var (or the varchar
// binary-coercion relabel): `length(v)`/`char_length(v)` (textlen) and
// `octet_length(v)` (textoctetlen), int4-result, strict (result NULL-ness ==
// the Var's NULL-ness), TYPE-level non-erroring on a guard-passed batch. The
// lane width picks the read kernel by server encoding, resolved ONCE at
// classify (the database encoding is fixed for the backend's lifetime):
// octet_length and 1-byte-max-encoding textlen read the payload byte count
// (text_length's max_length==1 arm — no walk, no NUL stop, cannot error);
// UTF-8 textlen reads bytes − continuation bytes under the per-batch uguard
// proof. Every other multibyte encoding refuses (their pg_mblen walks have
// no vectorizable count), as does a missing encoding seam (test harnesses
// must install one to admit char-length).
fn classify_len_arg(expr: Node<'_>) -> Option<(u16, LaneWidth)> {
    let f = expr.as_func_expr()?;
    if f.funcretset || f.args.len() != 1 {
        return None;
    }
    let octet = f.funcid == F_TEXTOCTETLEN;
    if !octet && !F_TEXTLEN.contains(&f.funcid) {
        return None;
    }
    let (col, _) = classify_str_var(f.args.iter().next()?, TEXTOID)?;
    let width = if octet {
        LaneWidth::VarLenBytes
    } else {
        if !::mbutils_seams::pg_database_encoding_max_length::is_installed()
            || !::mbutils_seams::get_database_encoding::is_installed()
        {
            return None;
        }
        if ::mbutils_seams::pg_database_encoding_max_length::call() == 1 {
            LaneWidth::VarLenBytes
        } else if ::mbutils_seams::get_database_encoding::call() == PG_UTF8 {
            LaneWidth::VarLenChars
        } else {
            return None;
        }
    };
    Some((col, width))
}

fn classify_arg(expr: Node<'_>, expected: Oid) -> Option<LaneArg> {
    if let Some((col, width)) = classify_var(expr, expected) {
        return Some(LaneArg { col, width, addend: 0, mulk: 1, divk: 1, guard: None });
    }
    if expected != INT4OID {
        return None;
    }
    // Bare textlen-family admission (no affine composition, no integer
    // guard — the result interval [0, 2^30) is inside int4 by type; the
    // data-level obligation is the vguard/uguard pair attached per column
    // in classify()).
    if let Some((col, width)) = classify_len_arg(expr) {
        return Some(LaneArg { col, width, addend: 0, mulk: 1, divk: 1, guard: None });
    }
    let op = expr.as_op_expr()?;
    if op.opretset || op.args.len() != 2 {
        return None;
    }
    let mut it = op.args.iter();
    let (a, b) = (it.next()?, it.next()?);
    // (var operand, const operand, var type, transform builder). int42div
    // (const / var) is not a v-monotone affine transform and stays refused.
    let (var, konst, vartype, mk): (_, _, Oid, fn(i64) -> (i64, i64, i64)) = match op.opfuncid {
        F_INT24PL => (a, b, INT2OID, |k| (k, 1, 1)),
        F_INT42PL => (b, a, INT2OID, |k| (k, 1, 1)),
        F_INT24MI => (a, b, INT2OID, |k| (-k, 1, 1)),
        F_INT42MI => (b, a, INT2OID, |k| (k, -1, 1)),
        F_INT24MUL => (a, b, INT2OID, |k| (0, k, 1)),
        F_INT42MUL => (b, a, INT2OID, |k| (0, k, 1)),
        F_INT24DIV => (a, b, INT2OID, |k| (0, 1, k)),
        F_INT4PL => (a, b, INT4OID, |k| (k, 1, 1)),
        F_INT4MI => (a, b, INT4OID, |k| (-k, 1, 1)),
        F_INT4MUL => (a, b, INT4OID, |k| (0, k, 1)),
        _ => return None,
    };
    let (col, width) = classify_var(var, vartype)?;
    let c = konst.as_const()?;
    if c.constisnull || c.consttype != INT4OID {
        return None;
    }
    let k = c.constvalue.as_i32() as i64;
    let (addend, mulk, divk) = mk(k);
    if divk == 0 {
        // int24div by a zero const raises division-by-zero per row in C;
        // refusal keeps that raise on the per-row program.
        return None;
    }
    let guard = if type_proof(width, addend, mulk, divk) {
        None
    } else {
        let (lo, hi) = safe_interval(addend, mulk, divk);
        if lo > hi {
            return None;
        }
        Some(Guard { lo, hi })
    };
    Some(LaneArg { col, width, addend, mulk, divk, guard })
}

/// NULL-ness of an admitted arg equals the Var's NULL-ness (strict operators
/// over a non-null Const), so CountAny reads only the Var's isnull lane.
pub fn classify_trans(
    spec: &AggTransSpec<'_, '_>,
    transno: usize,
) -> Option<(LaneTrans, Option<GuardEntry>)> {
    if spec.combine || spec.ordered.is_some() || spec.aggfilter.is_some() || spec.cur_agg.is_some()
    {
        return None;
    }
    let transno = u16::try_from(transno).ok()?;
    let arg = |expected: Oid| -> Option<(LaneArg, LaneWidth)> {
        if spec.args.len() != 1 {
            return None;
        }
        let tle = spec.args.iter().next()?.as_target_entry()?;
        Some((classify_arg(tle.expr, expected)?, width_of(expected)?))
    };
    // Varlena str arg: bare Var (or the text-over-varchar relabel), Var-width
    // lane, no transform, no integer guard (the vguard is per-column, built
    // by classify()).
    let varg = |expected: Oid| -> Option<(LaneArg, LaneWidth)> {
        if !str_collation_safe(spec.inputcollid) || spec.args.len() != 1 {
            return None;
        }
        let tle = spec.args.iter().next()?.as_target_entry()?;
        let (col, width) = classify_str_var(tle.expr, expected)?;
        let (addend, mulk, divk) = PLAIN;
        Some((LaneArg { col, width, addend, mulk, divk, guard: None }, LaneWidth::Var))
    };
    let mk = |kind, (a, res_width): (LaneArg, LaneWidth)| {
        let guard = a
            .guard
            .map(|g| GuardEntry { col: a.col, width: a.width, lo: g.lo, hi: g.hi });
        Some((
            LaneTrans {
                kind,
                col: a.col,
                width: a.width,
                res_width,
                addend: a.addend as i32,
                mulk: a.mulk as i32,
                divk: a.divk as i32,
                transno,
            },
            guard,
        ))
    };
    let plain = |col, width| {
        let (addend, mulk, divk) = PLAIN;
        (LaneArg { col, width, addend, mulk, divk, guard: None }, width)
    };
    match spec.transfn_oid {
        F_INT8INC if spec.args.is_nil() && !spec.init_value_is_null => {
            mk(LaneKind::CountStar, plain(0, LaneWidth::I64))
        }
        F_INT8INC_ANY if !spec.init_value_is_null => {
            if spec.args.len() != 1 {
                return None;
            }
            let tle = spec.args.iter().next()?.as_target_entry()?;
            let v = tle.expr.as_var()?;
            if v.varno != OUTER_VAR || v.varlevelsup != 0 || v.varattno < 1 {
                return None;
            }
            mk(LaneKind::CountAny, plain(v.varattno as u16 - 1, LaneWidth::I64))
        }
        F_INT2_SUM if spec.init_value_is_null => mk(LaneKind::Sum, arg(INT2OID)?),
        F_INT4_SUM if spec.init_value_is_null => mk(LaneKind::Sum, arg(INT4OID)?),
        F_INT2_AVG_ACCUM if !spec.init_value_is_null => mk(LaneKind::AvgAccum, arg(INT2OID)?),
        F_INT4_AVG_ACCUM if !spec.init_value_is_null => mk(LaneKind::AvgAccum, arg(INT4OID)?),
        // sum(int8)/avg(int8): bare int8 Var only (classify_arg's OpExpr
        // admissions are int4-result-only), so no transform and no guard.
        // TYPE-level non-erroring proof: the transition is
        // `state.n += 1; state.sum_x += (i128)v` — unchecked int128
        // arithmetic in C too, and int128 accumulation of int8 terms cannot
        // reach the rails for any feasible rowcount (2^63-max terms need
        // > 2^64 rows to leave i128), so the fold can never raise an error
        // C's per-row evaluation wouldn't.
        F_INT8_AVG_ACCUM if spec.init_value_is_null => {
            mk(LaneKind::Int128AvgAccum, arg(INT8OID)?)
        }
        F_INT2LARGER => mk(LaneKind::Max, arg(INT2OID)?),
        F_INT2SMALLER => mk(LaneKind::Min, arg(INT2OID)?),
        F_INT4LARGER => mk(LaneKind::Max, arg(INT4OID)?),
        F_INT4SMALLER => mk(LaneKind::Min, arg(INT4OID)?),
        F_INT8LARGER => mk(LaneKind::Max, arg(INT8OID)?),
        F_INT8SMALLER => mk(LaneKind::Min, arg(INT8OID)?),
        F_DATE_LARGER => mk(LaneKind::Max, arg(DATEOID)?),
        F_DATE_SMALLER => mk(LaneKind::Min, arg(DATEOID)?),
        F_TIMESTAMP_LARGER => mk(LaneKind::Max, arg(TIMESTAMPOID)?),
        F_TIMESTAMP_SMALLER => mk(LaneKind::Min, arg(TIMESTAMPOID)?),
        F_TIMESTAMPTZ_LARGER => mk(LaneKind::Max, arg(TIMESTAMPTZOID)?),
        F_TIMESTAMPTZ_SMALLER => mk(LaneKind::Min, arg(TIMESTAMPTZOID)?),
        // Fold-coverage tier 2 (all strict + NULL-init per pg_aggregate, all
        // TYPE-level safe — no guards). Floats and bools admit only bare Vars
        // (classify_arg's OpExpr path is int4-only); the int4 bitwise pair
        // additionally admits the affine OpExpr shapes, whose guard/proof
        // tiers apply exactly as for SUM/MIN/MAX.
        F_FLOAT4LARGER => mk(LaneKind::FMax, arg(FLOAT4OID)?),
        F_FLOAT4SMALLER => mk(LaneKind::FMin, arg(FLOAT4OID)?),
        F_FLOAT8LARGER => mk(LaneKind::FMax, arg(FLOAT8OID)?),
        F_FLOAT8SMALLER => mk(LaneKind::FMin, arg(FLOAT8OID)?),
        F_BOOLAND_STATEFUNC => mk(LaneKind::BoolAnd, arg(BOOLOID)?),
        F_BOOLOR_STATEFUNC => mk(LaneKind::BoolOr, arg(BOOLOID)?),
        F_INT2AND => mk(LaneKind::BitAnd, arg(INT2OID)?),
        F_INT2OR => mk(LaneKind::BitOr, arg(INT2OID)?),
        F_INT4AND => mk(LaneKind::BitAnd, arg(INT4OID)?),
        F_INT4OR => mk(LaneKind::BitOr, arg(INT4OID)?),
        F_INT8AND => mk(LaneKind::BitAnd, arg(INT8OID)?),
        F_INT8OR => mk(LaneKind::BitOr, arg(INT8OID)?),
        // Fold-coverage tier 3 (strict + NULL-init per pg_aggregate): text /
        // bpchar MIN/MAX, admitted only under the memcmp collation tier
        // (varg's str_collation_safe gate) over bare varlena Vars. The
        // vguard obligation (inline-form batch proof) attaches per column in
        // classify().
        F_TEXT_LARGER => mk(LaneKind::StrMax, varg(TEXTOID)?),
        F_TEXT_SMALLER => mk(LaneKind::StrMin, varg(TEXTOID)?),
        F_BPCHAR_LARGER => mk(LaneKind::BpMax, varg(BPCHAROID)?),
        F_BPCHAR_SMALLER => mk(LaneKind::BpMin, varg(BPCHAROID)?),
        _ => None,
    }
}

// ===========================================================================
// Metadata-answerable transitions (re-harvested from the lane-v1 metacount /
// footer-sums work; value-correctness proven end-to-end in
// notes/q4-avg-quarantine-resolution.md): COUNT(*) / COUNT(bare Var) — equal
// on cbstore, which stores no NULLs (writer::append_row errors on NULL) —
// MIN/MAX over a bare int-family Var, answered from footer row counts and
// zone maps, and SUM/AVG over an int-family Var with an affine divk==1
// transform, answered from footer i128 sums as mulk*S + addend*N (the
// agg-rewrite-cse SumBase derivation lifted to part metadata). Guarded
// transforms carry the interval for the admission site's footer-minmax
// re-proof.
// ===========================================================================

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MetaKind {
    Count,
    Min,
    Max,
    // sum(int2/int4): i64 datum end state (NULL over zero rows).
    Sum,
    // avg/sum(int2/int4): int8[2] {count,sum} transarray end state.
    AvgAccum,
    // sum/avg(int8) via int8_avg_accum: Int128AggState end state.
    Sum128,
}

impl MetaKind {
    pub fn needs_sum(self) -> bool {
        matches!(self, MetaKind::Sum | MetaKind::AvgAccum | MetaKind::Sum128)
    }
}

#[derive(Clone, Copy)]
pub struct MetaTrans {
    pub kind: MetaKind,
    pub col: u16,
    pub transno: u16,
    // Sum/AvgAccum affine coefficients (0/1 identities elsewhere): the
    // metadata fold is mulk*S + addend*N over the footer sum S and visible
    // row count N.
    pub addend: i32,
    pub mulk: i32,
    // Data-level guard interval: the admission site must prove the visible
    // rows' footer (min, max) sits inside [lo, hi] or refuse the meta arm
    // (the scan path would raise C's int4 overflow error per row).
    pub guard: Option<(i64, i64)>,
}

/// Metadata-answerable plan: `Some` iff EVERY transition is footer-answerable
/// (all-or-nothing — the meta arm answers the whole node from metadata or not
/// at all; there is no per-transition residual feed with zero rows staged).
pub fn classify_meta<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
) -> Option<PgVec<'mcx, MetaTrans>> {
    let mut out: PgVec<'mcx, MetaTrans> = PgVec::new_in(mcx);
    for (transno, spec) in specs.iter().enumerate() {
        let (t, g) = classify_trans(spec, transno)?;
        let plain = (t.addend, t.mulk, t.divk) == (0, 1, 1) && g.is_none();
        // Min/Max require the FULL plain shape: a mulk/divk transform is
        // monotone but not identity, so the zone-map entry is not the
        // transformed aggregate's answer (min(v*3) != min(v)). Sum/AvgAccum
        // admit affine transforms with divk == 1 (the agg-rewrite-cse
        // composition): the metadata fold derives mulk*S + addend*N in the
        // same mod-2^64 ring as the SumBase derivation, and a data-level
        // guard re-proves against the footer min/max over every visible row
        // (the admission site refuses the arm when the interval fails).
        // Integer division is not linear — divk != 1 refuses. Int128AvgAccum
        // is bare-Var-only by classify_arg (int8 has no OpExpr admission),
        // so `plain` always holds where it classifies. The lane-v1 tiers
        // with no part-metadata answer refuse: floats (zone entries carry
        // i64-widened INT-family decode values, not float order), bools,
        // bitwise and/or (not derivable from min/max/sum), and the varlena
        // str tier (text zone entries carry byte lengths).
        let affine = t.divk == 1;
        // The length widths (VarLenBytes/VarLenChars) are NOT
        // footer-answerable: their lane value is computed off the varlena
        // payload, and the part footer carries no length sums (text zone
        // entries carry byte-length bounds only) — every meta arm below is
        // integer-lane-only.
        let int_width = matches!(t.width, LaneWidth::I16 | LaneWidth::I32 | LaneWidth::I64);
        let kind = match t.kind {
            LaneKind::CountStar | LaneKind::CountAny => MetaKind::Count,
            LaneKind::Min if plain && int_width => MetaKind::Min,
            LaneKind::Max if plain && int_width => MetaKind::Max,
            LaneKind::Sum if affine && int_width => MetaKind::Sum,
            LaneKind::AvgAccum if affine && int_width => MetaKind::AvgAccum,
            LaneKind::Int128AvgAccum if plain && int_width => MetaKind::Sum128,
            _ => return None,
        };
        out.push(MetaTrans {
            kind,
            col: t.col,
            transno: t.transno,
            addend: t.addend,
            mulk: t.mulk,
            guard: g.map(|g| (g.lo, g.hi)),
        });
    }
    (!out.is_empty()).then_some(out)
}

/// Min/max NULL-init strictness requires strict transfns; every admitted
/// larger/smaller is strict in the catalog, and the count/avg initvals are
/// non-null by catalog. classify() re-derives nothing from the catalog at run
/// time — the OID whitelist IS the semantic contract.
///
/// Returns None when no transition admits (the caller keeps its whole
/// per-row program); otherwise `resid` carries the refused transnos.
pub fn classify<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
) -> Option<LanePlan<'mcx>> {
    let mut trans: PgVec<'mcx, LaneTrans> = PgVec::new_in(mcx);
    let mut guards: PgVec<'mcx, GuardEntry> = PgVec::new_in(mcx);
    let mut resid: PgVec<'mcx, usize> = PgVec::new_in(mcx);
    for (transno, spec) in specs.iter().enumerate() {
        match classify_trans(spec, transno) {
            Some((t, g)) => {
                trans.push(t);
                if let Some(g) = g {
                    guards.push(g);
                }
            }
            None => resid.push(transno),
        }
    }
    if trans.is_empty() {
        return None;
    }
    let mut cols: PgVec<'mcx, u16> = PgVec::new_in(mcx);
    for t in trans.iter() {
        if t.kind != LaneKind::CountStar && !cols.contains(&t.col) {
            cols.push(t.col);
        }
    }
    // Varlena lanes carry the per-batch inline-form proof obligation (one
    // entry per distinct str/length column); VarLenChars lanes additionally
    // carry the UTF-8 countability obligation.
    let mut vguards: PgVec<'mcx, u16> = PgVec::new_in(mcx);
    let mut uguards: PgVec<'mcx, u16> = PgVec::new_in(mcx);
    for t in trans.iter() {
        if matches!(t.width, LaneWidth::Var | LaneWidth::VarLenBytes | LaneWidth::VarLenChars)
            && !vguards.contains(&t.col)
        {
            vguards.push(t.col);
        }
        if t.width == LaneWidth::VarLenChars && !uguards.contains(&t.col) {
            uguards.push(t.col);
        }
    }
    let (cse, cse_members, cse_skip) = build_cse(mcx, &trans);
    let guarded = !guards.is_empty() || !vguards.is_empty();
    Some(LanePlan {
        trans,
        cse,
        cse_members,
        cse_skip,
        guards,
        vguards,
        uguards,
        cols,
        resid,
        guarded,
    })
}

/// CSE schedule over classified transitions. SumBase: Sum/AvgAccum cluster by
/// (col, divk) — addend/mulk live in the per-member derivation (Int128AvgAccum
/// stays OUT: its carrier is i128, not the i64 SumBase pass; sum(int8) +
/// avg(int8) over one column fold as independent per-trans kernels); a CountAny
/// joins any cluster on its col (the non-null count is transform-independent),
/// else CountAnys cluster by col alone. MinMax: exact structural duplicates
/// (same kind/col/transform) share one batch scan. Groups need >= 2 members
/// (a singleton saves nothing); residual transitions never reach classify, so
/// they can't join a group.
pub fn build_cse<'mcx>(
    mcx: Mcx<'mcx>,
    trans: &[LaneTrans],
) -> (PgVec<'mcx, CseGroup>, PgVec<'mcx, u16>, PgVec<'mcx, bool>) {
    #[derive(PartialEq)]
    enum Key {
        // width is part of Sum/MinMax structural identity: one text column
        // can host lanes of different reads (VarLenChars for length() vs
        // VarLenBytes for octet_length()) — same col, different values.
        Sum { col: u16, width: LaneWidth, divk: i32 },
        Count { col: u16 },
        // res_width is part of MinMax structural identity: a bare int2 Var
        // and an int2+0 OpExpr share coefficients but store transvalues at
        // different widths.
        MinMax {
            max: bool,
            col: u16,
            width: LaneWidth,
            res_width: LaneWidth,
            addend: i32,
            mulk: i32,
            divk: i32,
        },
    }
    let mut clusters: Vec<(Key, Vec<u16>)> = Vec::new();
    let mut join = |key: Key, ti: u16| match clusters.iter_mut().find(|(k, _)| *k == key) {
        Some((_, v)) => v.push(ti),
        None => clusters.push((key, vec![ti])),
    };
    for (ti, t) in trans.iter().enumerate() {
        let ti = ti as u16;
        match t.kind {
            LaneKind::Sum | LaneKind::AvgAccum => {
                join(Key::Sum { col: t.col, width: t.width, divk: t.divk }, ti)
            }
            LaneKind::Min | LaneKind::Max => join(
                Key::MinMax {
                    max: t.kind == LaneKind::Max,
                    col: t.col,
                    width: t.width,
                    res_width: t.res_width,
                    addend: t.addend,
                    mulk: t.mulk,
                    divk: t.divk,
                },
                ti,
            ),
            // CountStar/CountAny cluster below; the tier-2/3 datum-lane kinds
            // (FMin/FMax/BoolAnd/BoolOr/BitAnd/BitOr/Str*/Bp*) are excluded
            // from CSE: SumBase's derivation is ring arithmetic
            // (inapplicable), and the MinMax scan share is conservatively not
            // extended to the tie-sensitive float/str rules or the bitwise
            // folds — they keep their independent per-trans kernels.
            _ => {}
        }
    }
    for (ti, t) in trans.iter().enumerate() {
        if t.kind != LaneKind::CountAny {
            continue;
        }
        let col = t.col;
        match clusters.iter_mut().find(
            |(k, _)| matches!(k, Key::Sum { col: c, .. } | Key::Count { col: c } if *c == col),
        ) {
            Some((_, v)) => v.push(ti as u16),
            None => clusters.push((Key::Count { col }, vec![ti as u16])),
        }
    }
    let mut groups: PgVec<'mcx, CseGroup> = PgVec::new_in(mcx);
    let mut members: PgVec<'mcx, u16> = PgVec::new_in(mcx);
    let mut skip: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    for _ in trans {
        skip.push(false);
    }
    for (key, tis) in clusters {
        if tis.len() < 2 {
            continue;
        }
        let kind = match key {
            Key::MinMax { .. } => CseGroupKind::MinMax,
            _ => CseGroupKind::SumBase,
        };
        let start = members.len() as u16;
        for ti in tis {
            skip[ti as usize] = true;
            members.push(ti);
        }
        groups.push(CseGroup { kind, start, len: members.len() as u16 - start });
    }
    (groups, members, skip)
}

// chars = bytes − UTF-8 continuation bytes. On a uguard-passed payload
// (valid UTF-8, no embedded NUL) this equals C textlen's
// pg_mbstrlen_with_len walk exactly: every lead byte's claimed length is the
// sequence's true length, the walk never NUL-stops early, and the final
// character never overruns the slice. The byte test is branch-free and
// LLVM auto-vectorizes the count.
#[inline(always)]
fn utf8_char_count(s: &[u8]) -> i64 {
    let cont = s.iter().filter(|&&b| (b & 0xC0) == 0x80).count();
    (s.len() - cont) as i64
}

// Only called from the unsafe fold/guard entry points: for the length
// widths, the caller contract (vguard-passed batch) makes the selected
// non-null lane values live inline varlena pointers.
#[inline(always)]
fn lane_value(values: &[Datum], width: LaneWidth, i: usize) -> i64 {
    match width {
        LaneWidth::I16 => values[i].as_i16() as i64,
        LaneWidth::I32 => values[i].as_i32() as i64,
        LaneWidth::I64 => values[i].as_i64(),
        // SAFETY: vguard-passed inline varlena (see fn comment); uguard
        // makes the UTF-8 count exact (see utf8_char_count).
        LaneWidth::VarLenBytes => unsafe { str_payload(values[i]).len() as i64 },
        LaneWidth::VarLenChars => utf8_char_count(unsafe { str_payload(values[i]) }),
        // Datum-lane kinds read the datum word directly, never through the
        // integer lane read (and are never integer-guarded).
        LaneWidth::F32 | LaneWidth::F64 | LaneWidth::Bool | LaneWidth::Var => unreachable!(),
    }
}

// (count, sum of transformed values) over selected non-null rows. The
// addend-only shape keeps the hoisted c*addend form (one multiply per batch);
// mul/div transforms fold per row — each transformed term is int4-proven, so
// the i64 batch sum stays exact.
#[inline(always)]
fn sum_selected(t: &LaneTrans, values: &[Datum], isnull: &[bool], rows: &[u64]) -> (i64, i64) {
    let mut c = 0i64;
    let mut s = 0i64;
    if t.mulk == 1 && t.divk == 1 {
        for_each_row(rows, |i| {
            if !isnull[i] {
                c += 1;
                s = s.wrapping_add(lane_value(values, t.width, i));
            }
        });
        s = s.wrapping_add(c.wrapping_mul(t.addend as i64));
    } else {
        for_each_row(rows, |i| {
            if !isnull[i] {
                c += 1;
                s = s.wrapping_add(xform(t, lane_value(values, t.width, i)));
            }
        });
    }
    (c, s)
}

#[inline(always)]
fn count_apply(pg: &mut AggPerGroup, c: i64) {
    pg.trans_value = Datum::from_i64(pg.trans_value.as_i64().wrapping_add(c));
    pg.trans_value_is_null = false;
    pg.no_trans_value = false;
}

#[inline(always)]
fn sum_apply(pg: &mut AggPerGroup, delta: i64) {
    let old = if pg.trans_value_is_null { 0 } else { pg.trans_value.as_i64() };
    pg.trans_value = Datum::from_i64(old.wrapping_add(delta));
    pg.trans_value_is_null = false;
}

#[inline(always)]
fn avg_apply(pg: &mut AggPerGroup, c: i64, delta: i64) {
    assert!(!pg.trans_value_is_null, "avg transarray is never NULL");
    let arr = pg.trans_value.as_usize() as *mut u8;
    // SAFETY: aggcontext-lived transarray, shape validated.
    unsafe {
        assert!(
            ::types_tuple::varatt::varatt_is_4b_u(arr)
                && ::types_tuple::varatt::varsize_4b(arr) == INT8_TRANSARRAY_SIZE
                && arr.add(8).cast::<i32>().read() == 0,
            "expected 2-element int8 array"
        );
        let td = arr.add(ARR_OVERHEAD_NONULLS_1).cast::<i64>();
        *td = (*td).wrapping_add(c);
        *td.add(1) = (*td.add(1)).wrapping_add(delta);
    }
}

// C int8_avg_accum's makePolyNumAggState arm (numeric.c 5911): get the
// group's aggcontext-lived Int128AggState, allocating it on the group's
// first transfn call. The caller invokes this exactly when C would call the
// (non-strict) transfn — once per selected row, NULL inputs included — so
// the allocated-vs-NULL state distinction stays bit-equal to the per-row
// program even for all-NULL groups (observable through int8_avg_serialize
// under a partial-agg finalize: NULL trans vs an n=0 state serialize
// differently). `no_trans_value` is deliberately left untouched: the
// per-row non-strict byval trans step (execexpr agg_trans_byval) never
// writes it, and a fold-then-demote group must present the exact pergroup
// image the per-row program produces.
#[inline]
fn int128_state(pg: &mut AggPerGroup, aggcxt: Mcx<'_>) -> PgResult<*mut Int128AggState> {
    if !pg.trans_value_is_null {
        return Ok(pg.trans_value.as_usize() as *mut Int128AggState);
    }
    const { assert!(!core::mem::needs_drop::<Int128AggState>()) }
    let layout = core::alloc::Layout::new::<Int128AggState>();
    let raw =
        ::mcx::Allocator::allocate(&aggcxt, layout).map_err(|_| aggcxt.oom(layout.size()))?;
    let p = raw.cast::<Int128AggState>().as_ptr();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(Int128AggState::new(false)) };
    pg.trans_value = Datum::from_usize(p as usize);
    pg.trans_value_is_null = false;
    Ok(p)
}

// (non-null count, Σv as i128) over selected rows of an int8 lane. The i128
// batch sum is EXACT (never wraps): |Σ| <= nrows * 2^63 << 2^127 for any
// batch a staged window can hold, so adding it to the running state.sum_x
// once is bit-equal to C's per-row `sum_x += (int128)v` sequence (int128
// addition is associative when no step overflows, and the running sum has
// C's own overflow envelope — leaving i128 needs > 2^64 max-magnitude rows,
// infeasible; C's accumulation is equally unchecked).
#[inline(always)]
fn sum128_selected(t: &LaneTrans, values: &[Datum], isnull: &[bool], rows: &[u64]) -> (i64, i128) {
    debug_assert_eq!((t.addend, t.mulk, t.divk), (0, 1, 1), "bare-Var admission only");
    let mut c = 0i64;
    let mut s = 0i128;
    for_each_row(rows, |i| {
        if !isnull[i] {
            c += 1;
            s += lane_value(values, t.width, i) as i128;
        }
    });
    (c, s)
}

/// One {count,sum} int8[2] transarray in `mcx`, header shaped exactly as C
/// construct_array produces it (4B varlena, 1-D, dataoffset 0 = no nulls,
/// int8 elems, dim 2, lbound 1) — the AvgAccum pergroup's non-null initval.
/// The consuming node must install this before the first fold touches an
/// AvgAccum transition (avg_apply validates the shape).
pub fn new_int8_transarray(mcx: Mcx<'_>) -> Datum {
    let mut buf: PgVec<'_, u64> = ::mcx::vec_from_elem_in(mcx, 0u64, 5);
    let p = buf.as_mut_ptr().cast::<u8>();
    // SAFETY: 40 in-bounds bytes, 8-aligned; leaked into the mcx arena below.
    unsafe {
        p.cast::<u32>().write((INT8_TRANSARRAY_SIZE as u32) << 2);
        p.add(4).cast::<i32>().write(1); // ndim
        p.add(8).cast::<i32>().write(0); // dataoffset (no nulls)
        p.add(12).cast::<u32>().write(20); // elemtype = int8
        p.add(16).cast::<i32>().write(2); // dim[0]
        p.add(20).cast::<i32>().write(1); // lbound[0]
    }
    let d = Datum::from_usize(p as usize);
    core::mem::forget(buf);
    d
}

// (count, Σv') over selected non-null rows with v' = v/divk — the shared
// base accumulator every SumBase member derives from.
#[inline(always)]
fn base_sum(
    width: LaneWidth,
    divk: i64,
    values: &[Datum],
    isnull: &[bool],
    rows: &[u64],
) -> (i64, i64) {
    let mut c = 0i64;
    let mut s = 0i64;
    for_each_row(rows, |i| {
        if !isnull[i] {
            c += 1;
            let v = lane_value(values, width, i);
            s = s.wrapping_add(if divk != 1 { v / divk } else { v });
        }
    });
    (c, s)
}

// Per-member delta off the shared base: mulk*S + addend*c, bit-equal to the
// per-row Σ(v'*mulk + addend) in mod-2^64 ring arithmetic (see CseGroup).
#[inline(always)]
fn cse_delta(t: &LaneTrans, c: i64, s: i64) -> i64 {
    let s = if t.mulk != 1 { s.wrapping_mul(t.mulk as i64) } else { s };
    s.wrapping_add(c.wrapping_mul(t.addend as i64))
}

/// Whole-batch ungrouped fold: apply every admitted transition over the
/// selected rows of the staged batch, CSE groups first, then the ungrouped
/// per-trans kernels. `aggcxt` is the agg (transvalue) memory context — where
/// C's ExecAggCopyTransValue copies by-ref transvalues; only the str kinds
/// allocate (their datumCopy on a strict install/replace), and only they can
/// fail (OOM), so integer/float/bool plans never see the Err path.
///
/// # Safety
/// `pergroup_base` is the node's once-allocated pergroup array covering every
/// transno in the plan; rows selected by `rows` carry valid lane values in
/// `cols` for every plan column (`rows` has one bit per staged row,
/// `nrows <= rows.len() * 64`); AvgAccum pergroups hold a live
/// `new_int8_transarray`-shaped transvalue; Int128AvgAccum pergroups are
/// either NULL or hold a live aggcontext `Int128AggState` pointer, and
/// `aggcxt` IS that aggcontext (the arena the per-row transfn reaches via
/// fcinfo->context); str-kind (Var-width) lanes carry live varlena datum
/// pointers, and their non-empty pergroup transvalues are live inline
/// varlenas (this fold's own aggcxt copies). If the plan is guarded, the
/// caller must have run `check_guards` on this batch and gotten `Pass`.
pub unsafe fn fold_batch(
    plan: &LanePlan<'_>,
    cols: &impl LaneCols,
    rows: &[u64],
    nrows: usize,
    pergroup_base: NonNull<AggPerGroup>,
    aggcxt: Mcx<'_>,
) -> PgResult<()> {
    let nsel: u32 = rows.iter().map(|w| w.count_ones()).sum();
    for g in plan.cse.iter() {
        let members = &plan.cse_members[g.start as usize..(g.start + g.len) as usize];
        // SAFETY: transno < pergroup length (caller contract).
        let pg_of = |transno: u16| unsafe { &mut *pergroup_base.as_ptr().add(transno as usize) };
        let t0 = &plan.trans[members[0] as usize];
        match g.kind {
            CseGroupKind::SumBase => {
                // Any Sum/AvgAccum member defines the base lane read; a
                // count-only group never reads the value lane (CountAny's
                // width field is not the column's width).
                let lane = members
                    .iter()
                    .map(|&m| &plan.trans[m as usize])
                    .find(|t| t.kind != LaneKind::CountAny);
                let (c, s) = match lane {
                    Some(l) => {
                        let (values, isnull) =
                            (cols.col_values(l.col as usize), cols.col_isnull(l.col as usize));
                        base_sum(l.width, l.divk as i64, values, isnull, rows)
                    }
                    None => {
                        let isnull = cols.col_isnull(t0.col as usize);
                        let mut c = 0i64;
                        for_each_row(rows, |i| c += !isnull[i] as i64);
                        (c, 0)
                    }
                };
                for &m in members {
                    let t = &plan.trans[m as usize];
                    debug_assert_eq!(t.col, t0.col);
                    let pg = pg_of(t.transno);
                    match t.kind {
                        LaneKind::CountAny => count_apply(pg, c),
                        LaneKind::Sum if c > 0 => sum_apply(pg, cse_delta(t, c, s)),
                        LaneKind::AvgAccum if c > 0 => avg_apply(pg, c, cse_delta(t, c, s)),
                        _ => {}
                    }
                }
            }
            CseGroupKind::MinMax => {
                let (values, isnull) =
                    (cols.col_values(t0.col as usize), cols.col_isnull(t0.col as usize));
                let mut m: Option<i64> = None;
                let want_max = t0.kind == LaneKind::Max;
                for_each_row(rows, |i| {
                    if !isnull[i] {
                        let v = xform(t0, lane_value(values, t0.width, i));
                        m = Some(match m {
                            None => v,
                            Some(p) => {
                                if want_max {
                                    p.max(v)
                                } else {
                                    p.min(v)
                                }
                            }
                        });
                    }
                });
                if let Some(v) = m {
                    for &mi in members {
                        minmax_advance(t0, pg_of(plan.trans[mi as usize].transno), v, want_max);
                    }
                }
            }
        }
    }
    for (ti, t) in plan.trans.iter().enumerate() {
        if plan.cse_skip[ti] {
            continue;
        }
        // SAFETY: transno < pergroup length (caller contract).
        let pg = unsafe { &mut *pergroup_base.as_ptr().add(t.transno as usize) };
        if t.kind == LaneKind::CountStar {
            count_apply(pg, nsel as i64);
            continue;
        }
        let (values, isnull) =
            (cols.col_values(t.col as usize), cols.col_isnull(t.col as usize));
        debug_assert!(values.len() >= nrows && isnull.len() >= nrows);
        match t.kind {
            LaneKind::CountStar => unreachable!(),
            LaneKind::CountAny => {
                let mut c = 0i64;
                for_each_row(rows, |i| {
                    c += !isnull[i] as i64;
                });
                count_apply(pg, c);
            }
            LaneKind::Sum => {
                let (c, s) = sum_selected(t, values, isnull, rows);
                if c > 0 {
                    sum_apply(pg, s);
                }
            }
            LaneKind::AvgAccum => {
                let (c, s) = sum_selected(t, values, isnull, rows);
                if c > 0 {
                    avg_apply(pg, c, s);
                }
            }
            LaneKind::Int128AvgAccum => {
                // C calls the non-strict transfn once per SELECTED row (NULL
                // inputs included), so any selected row allocates the state;
                // only the non-null inputs accumulate (see sum128_selected
                // for the reassociation proof).
                let (c, s) = sum128_selected(t, values, isnull, rows);
                if nsel > 0 {
                    let st = int128_state(pg, aggcxt)?;
                    // SAFETY: aggcontext-lived state installed by
                    // int128_state or the per-row transfn chain (caller
                    // contract); sole reference during the fold.
                    unsafe {
                        (*st).n += c;
                        (*st).sum_x += s;
                    }
                }
            }
            LaneKind::Min | LaneKind::Max => {
                let mut m: Option<i64> = None;
                let want_max = t.kind == LaneKind::Max;
                for_each_row(rows, |i| {
                    if !isnull[i] {
                        let v = xform(t, lane_value(values, t.width, i));
                        m = Some(match m {
                            None => v,
                            Some(p) => {
                                if want_max {
                                    p.max(v)
                                } else {
                                    p.min(v)
                                }
                            }
                        });
                    }
                });
                if let Some(v) = m {
                    minmax_advance(t, pg, v, want_max);
                }
            }
            // Batch pre-fold in row order, then one advance: legal because
            // larger/smaller's last-tied-wins rule is associative on bit
            // patterns (see f_keep).
            LaneKind::FMin | LaneKind::FMax => {
                let want_max = t.kind == LaneKind::FMax;
                let mut m: Option<Datum> = None;
                for_each_row(rows, |i| {
                    if !isnull[i] {
                        let d = values[i];
                        m = Some(match m {
                            None => d,
                            Some(p) => {
                                if f_keep(t.width, want_max, p, d) {
                                    p
                                } else {
                                    d
                                }
                            }
                        });
                    }
                });
                if let Some(d) = m {
                    fmm_advance(t, pg, d, want_max);
                }
            }
            LaneKind::BoolAnd | LaneKind::BoolOr => {
                let want_and = t.kind == LaneKind::BoolAnd;
                let mut m: Option<bool> = None;
                for_each_row(rows, |i| {
                    if !isnull[i] {
                        let v = values[i].as_bool();
                        m = Some(match m {
                            None => v,
                            Some(p) => {
                                if want_and {
                                    p && v
                                } else {
                                    p || v
                                }
                            }
                        });
                    }
                });
                if let Some(v) = m {
                    bool_advance(pg, v, want_and);
                }
            }
            LaneKind::BitAnd | LaneKind::BitOr => {
                let want_and = t.kind == LaneKind::BitAnd;
                let mut m: Option<i64> = None;
                for_each_row(rows, |i| {
                    if !isnull[i] {
                        let v = xform(t, lane_value(values, t.width, i));
                        m = Some(match m {
                            None => v,
                            Some(p) => {
                                if want_and {
                                    p & v
                                } else {
                                    p | v
                                }
                            }
                        });
                    }
                });
                if let Some(v) = m {
                    bit_advance(t, pg, v, want_and);
                }
            }
            // Batch pre-fold in row order, then one advance: legal because
            // both str tie rules (text last-tied-wins, bpchar first-tied-
            // wins) are associative on datum identity (see str_keep). The
            // single advance also matches C's allocation pattern for the
            // ungrouped case only in TOTAL bytes surviving (one copy of the
            // final winner); AGG_PLAIN has no memory-fed spill decisions, so
            // the intermediate-copy difference is unobservable.
            LaneKind::StrMin | LaneKind::StrMax | LaneKind::BpMin | LaneKind::BpMax => {
                let mut m: Option<Datum> = None;
                for_each_row(rows, |i| {
                    if !isnull[i] {
                        let d = values[i];
                        // SAFETY: vguard-passed batch — inline varlenas.
                        m = Some(match m {
                            None => d,
                            Some(p) => {
                                if unsafe { str_keep(t.kind, p, d) } {
                                    p
                                } else {
                                    d
                                }
                            }
                        });
                    }
                });
                if let Some(d) = m {
                    // SAFETY: vguard-passed batch (inline varlenas), live
                    // pergroup + aggcxt (caller contract).
                    unsafe { str_advance(t, pg, d, aggcxt)? };
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
pub enum GuardCheck {
    // All guards proven for this batch; flags say which tier fired (zone =
    // granule min/max, data = exact lane pass).
    Pass { zone: bool, data: bool },
    // Some guarded lane holds an out-of-interval selected value: the batch
    // must run the checked per-row program (which raises C's error at C's
    // row — the interval is exact, so a demoted batch always raises).
    Demote,
}

/// Per-batch data-level proof for every guarded transition. The zone bounds
/// cover the staged window's whole granule (a superset of the selected rows),
/// so a zone pass is conservative; the lane pass is exact — its failure is
/// exactly "the would-error mask is non-empty". Varlena vguards (str lanes)
/// have no zone tier: the exact lane pass verifies every selected non-null
/// datum is a plain inline varlena (1B short or 4B uncompressed) — a
/// compressed or external datum demotes the whole batch to the checked
/// per-row program, which detoasts exactly as C does.
///
/// # Safety
/// For every vguard column, rows selected by `rows` with a false isnull bit
/// carry lane values that are live varlena datum pointers readable through
/// their first header byte.
pub unsafe fn check_guards(
    plan: &LanePlan<'_>,
    cols: &impl LaneCols,
    rows: &[u64],
    zone_minmax: impl Fn(u16) -> Option<(i64, i64)>,
) -> GuardCheck {
    let mut zone = false;
    let mut data = false;
    for g in plan.guards.iter() {
        if let Some((mn, mx)) = zone_minmax(g.col) {
            if g.lo <= mn && mx <= g.hi {
                zone = true;
                continue;
            }
        }
        let values = cols.col_values(g.col as usize);
        let isnull = cols.col_isnull(g.col as usize);
        let mut ok = true;
        for_each_row(rows, |i| {
            if !isnull[i] {
                let v = lane_value(values, g.width, i);
                ok &= g.lo <= v && v <= g.hi;
            }
        });
        if !ok {
            return GuardCheck::Demote;
        }
        data = true;
    }
    for &c in plan.vguards.iter() {
        let values = cols.col_values(c as usize);
        let isnull = cols.col_isnull(c as usize);
        let mut ok = true;
        for_each_row(rows, |i| {
            if !isnull[i] {
                let p = values[i].as_usize() as *const u8;
                // SAFETY: selected non-null varlena lane pointer readable at
                // its header byte (caller contract).
                ok &= unsafe {
                    (::types_tuple::varatt::varatt_is_1b(p)
                        && !::types_tuple::varatt::varatt_is_1b_e(p))
                        || ::types_tuple::varatt::varatt_is_4b_u(p)
                };
            }
        });
        if !ok {
            return GuardCheck::Demote;
        }
        data = true;
    }
    // UTF-8 countability proof (VarLenChars lanes): valid UTF-8, no embedded
    // NUL — under it the fold's continuation-byte count is bit-equal to C
    // textlen's pg_mblen walk (no early NUL stop, no trailing-char overrun
    // error, every lead byte's claimed length true). Runs strictly AFTER the
    // vguard loop above: uguard columns are always vguard columns, so a
    // non-inline datum has already demoted before str_payload runs here.
    for &c in plan.uguards.iter() {
        let values = cols.col_values(c as usize);
        let isnull = cols.col_isnull(c as usize);
        let mut ok = true;
        for_each_row(rows, |i| {
            if !isnull[i] {
                // SAFETY: vguard-passed inline varlena (loop above).
                let s = unsafe { str_payload(values[i]) };
                ok &= core::str::from_utf8(s).is_ok() && !s.contains(&0);
            }
        });
        if !ok {
            return GuardCheck::Demote;
        }
        data = true;
    }
    GuardCheck::Pass { zone, data }
}

// Strict larger/smaller advance against the stored transvalue, at the
// transfn's result width (int4 for the int2-Var OpExpr admissions — storing
// at the lane width truncated in-range int4 results through from_i16).
#[inline(always)]
fn minmax_advance(t: &LaneTrans, pg: &mut AggPerGroup, v: i64, want_max: bool) {
    if pg.no_trans_value {
        pg.trans_value = store_res(t, v);
        pg.trans_value_is_null = false;
        pg.no_trans_value = false;
    } else if !pg.trans_value_is_null {
        let old = load_res(t, pg);
        let next = if want_max { old.max(v) } else { old.min(v) };
        if next != old {
            pg.trans_value = store_res(t, next);
        }
    }
}

// Integer transvalue store/load at the transfn's result width (shared by the
// int Min/Max and BitAnd/BitOr advances).
#[inline(always)]
fn store_res(t: &LaneTrans, v: i64) -> Datum {
    match t.res_width {
        LaneWidth::I16 => Datum::from_i16(v as i16),
        LaneWidth::I32 => Datum::from_i32(v as i32),
        LaneWidth::I64 => Datum::from_i64(v),
        // res_width is always an integer width (the transfn's result type;
        // length-lane transitions store at I32).
        LaneWidth::F32
        | LaneWidth::F64
        | LaneWidth::Bool
        | LaneWidth::Var
        | LaneWidth::VarLenBytes
        | LaneWidth::VarLenChars => unreachable!(),
    }
}

#[inline(always)]
fn load_res(t: &LaneTrans, pg: &AggPerGroup) -> i64 {
    match t.res_width {
        LaneWidth::I16 => pg.trans_value.as_i16() as i64,
        LaneWidth::I32 => pg.trans_value.as_i32() as i64,
        LaneWidth::I64 => pg.trans_value.as_i64(),
        LaneWidth::F32
        | LaneWidth::F64
        | LaneWidth::Bool
        | LaneWidth::Var
        | LaneWidth::VarLenBytes
        | LaneWidth::VarLenChars => unreachable!(),
    }
}

// float.h float4_gt/float8_gt over datum lanes: gt(a, b) iff b is not NaN and
// (a is NaN or a > b) — NaN sorts greatest (ties NaN), matching the btree
// float opclass C's MIN/MAX planagg rewrite relies on.
#[inline(always)]
fn f_gt(width: LaneWidth, a: Datum, b: Datum) -> bool {
    match width {
        LaneWidth::F32 => {
            let (x, y) = (a.as_f32(), b.as_f32());
            !y.is_nan() && (x.is_nan() || x > y)
        }
        LaneWidth::F64 => {
            let (x, y) = (a.as_f64(), b.as_f64());
            !y.is_nan() && (x.is_nan() || x > y)
        }
        _ => unreachable!(),
    }
}

// float.h float4_lt/float8_lt: lt(a, b) iff a is not NaN and (b is NaN or
// a < b).
#[inline(always)]
fn f_lt(width: LaneWidth, a: Datum, b: Datum) -> bool {
    match width {
        LaneWidth::F32 => {
            let (x, y) = (a.as_f32(), b.as_f32());
            !x.is_nan() && (y.is_nan() || x < y)
        }
        LaneWidth::F64 => {
            let (x, y) = (a.as_f64(), b.as_f64());
            !x.is_nan() && (y.is_nan() || x < y)
        }
        _ => unreachable!(),
    }
}

// C float4/float8 larger(a, b) = gt(a, b) ? a : b (smaller uses lt): the
// state survives only a STRICT win, so every tie — equal values, 0.0 vs -0.0,
// NaN vs NaN (any payloads) — is taken by the SECOND argument. As a fold,
// "keep cur iff cur strictly beats v, else take v" selects the LAST datum of
// the winning tie class in row order. That rule is associative on bit
// patterns (the last tied element wins under any grouping), so the batch
// pre-fold below combines with the transvalue exactly as C's per-row
// transition sequence does.
#[inline(always)]
fn f_keep(width: LaneWidth, want_max: bool, cur: Datum, v: Datum) -> bool {
    if want_max {
        f_gt(width, cur, v)
    } else {
        f_lt(width, cur, v)
    }
}

// Strict float larger/smaller advance: the stored transvalue is the winning
// input datum's exact bits (C stores the argument datum, never a recomputed
// float), replaced on ties per f_keep.
#[inline(always)]
fn fmm_advance(t: &LaneTrans, pg: &mut AggPerGroup, d: Datum, want_max: bool) {
    if pg.no_trans_value {
        pg.trans_value = d;
        pg.trans_value_is_null = false;
        pg.no_trans_value = false;
    } else if !pg.trans_value_is_null && !f_keep(t.width, want_max, pg.trans_value, d) {
        pg.trans_value = d;
    }
}

// Strict booland/boolor_statefunc advance. C recomputes the canonical bool
// datum every transition (arg1 && arg2), and the first strict install copies
// the input's canonical bool datum, so from_bool is byte-identical either
// way.
#[inline(always)]
fn bool_advance(pg: &mut AggPerGroup, v: bool, want_and: bool) {
    if pg.no_trans_value {
        pg.trans_value = Datum::from_bool(v);
        pg.trans_value_is_null = false;
        pg.no_trans_value = false;
    } else if !pg.trans_value_is_null {
        let old = pg.trans_value.as_bool();
        pg.trans_value = Datum::from_bool(if want_and { old && v } else { old || v });
    }
}

// Strict int2/int4/int8 and/or advance at the transfn's result width. The
// lane read sign-extends to i64 and the store truncates back, which commutes
// with AND/OR — bit-identical to C's native-width op (and to C's signed
// *GetDatum sign extension into the datum word).
#[inline(always)]
fn bit_advance(t: &LaneTrans, pg: &mut AggPerGroup, v: i64, want_and: bool) {
    if pg.no_trans_value {
        pg.trans_value = store_res(t, v);
        pg.trans_value_is_null = false;
        pg.no_trans_value = false;
    } else if !pg.trans_value_is_null {
        let old = load_res(t, pg);
        pg.trans_value = store_res(t, if want_and { old & v } else { old | v });
    }
}

// VARDATA_ANY/VARSIZE_ANY_EXHDR over an inline varlena (1B short or 4B
// uncompressed) — the only forms a vguard-passed lane or an aggcxt transvalue
// copy can hold.
//
// # Safety
// `d` is a live varlena datum pointer in one of the two inline forms.
#[inline(always)]
unsafe fn str_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: forwarded caller contract.
    unsafe {
        if ::types_tuple::varatt::varatt_is_1b(p) {
            core::slice::from_raw_parts(p.add(1), ::types_tuple::varatt::varsize_1b(p) - 1)
        } else {
            debug_assert!(::types_tuple::varatt::varatt_is_4b_u(p));
            core::slice::from_raw_parts(p.add(4), ::types_tuple::varatt::varsize_4b(p) - 4)
        }
    }
}

// The str transfn's keep-vs-replace decision against the current winner.
// text_larger/smaller (varlena.c): result = (text_cmp >/< 0) ? arg1 : arg2 —
// the state survives only a STRICT win, so every tie (equal payloads under
// memcmp+length, whatever the header forms) takes the SECOND argument:
// last-tied-wins on datum identity. bpchar_larger/smaller (varchar.c):
// result = (cmp >=/<= 0) ? arg1 : arg2 over bcTruelen-trimmed operands — the
// state SURVIVES a tie (first-tied-wins), and ties include strings differing
// only in trailing blanks. Both rules are associative on datum identity (the
// last/first element of the winning tie class survives any grouping), which
// is what legalizes the batch pre-fold + single advance.
//
// # Safety
// As `str_payload`, for both datums.
#[inline(always)]
unsafe fn str_keep(kind: LaneKind, cur: Datum, v: Datum) -> bool {
    // SAFETY: forwarded caller contract.
    let (a, b) = unsafe { (str_payload(cur), str_payload(v)) };
    match kind {
        // varstrfastcmp_c IS varstr_cmp's C/POSIX-collation result (memcmp +
        // length tiebreak) — the admission gate proved the collation.
        LaneKind::StrMax => ::varlena::varstrfastcmp_c(a, b) > 0,
        LaneKind::StrMin => ::varlena::varstrfastcmp_c(a, b) < 0,
        LaneKind::BpMax => ::varlena::bpcharfastcmp_c(a, b) >= 0,
        LaneKind::BpMin => ::varlena::bpcharfastcmp_c(a, b) <= 0,
        _ => unreachable!(),
    }
}

// Strict str larger/smaller advance: C returns one of the two argument
// datums, and advance_transition_function datumCopies the result into the
// agg context whenever it is not the stored transvalue (ExecAggCopyTransValue
// — ported as execexpr's agg_plain_trans_byref/agg_init_group discipline:
// copy on install, copy on replace, never on keep; the bump aggcontext
// reclaims replaced copies at group reset instead of C's pfree). Copying the
// input datum verbatim (agg_datum_copy = datumCopy: VARSIZE_ANY bytes)
// preserves its exact header form, so transvalue bytes — and the allocation
// SEQUENCE, which feeds hash-agg memory accounting and therefore spill
// decisions — match the per-row path exactly.
//
// # Safety
// As `str_keep`; `aggcxt` is the live agg context; `pg` is the transition's
// live pergroup cell.
#[inline(always)]
unsafe fn str_advance(
    t: &LaneTrans,
    pg: &mut AggPerGroup,
    d: Datum,
    aggcxt: Mcx<'_>,
) -> PgResult<()> {
    // SAFETY: forwarded caller contract (inline varlena datum, live aggcxt).
    unsafe {
        if pg.no_trans_value {
            pg.trans_value = ::execexpr::agg_datum_copy(aggcxt, d, -1)?;
            pg.trans_value_is_null = false;
            pg.no_trans_value = false;
        } else if !pg.trans_value_is_null && !str_keep(t.kind, pg.trans_value, d) {
            pg.trans_value = ::execexpr::agg_datum_copy(aggcxt, d, -1)?;
        }
    }
    Ok(())
}

::mcx::forget_safe_nodrop!(LaneTrans, CseGroup, GuardEntry);
// SAFETY census: every field is an arena PgVec of no-drop elements (or bool).
::mcx::forget_safe_struct!(
    LanePlan<'_> { trans, cse, cse_members, cse_skip, guards, vguards, uguards, cols, resid, guarded },
);

#[inline(always)]
fn for_each_row(rows: &[u64], mut f: impl FnMut(usize)) {
    for (w, &word) in rows.iter().enumerate() {
        let mut bits = word;
        while bits != 0 {
            f(w * 64 + bits.trailing_zeros() as usize);
            bits &= bits - 1;
        }
    }
}

/// Grouped fold: the probe stays per-row (the hash lookup repoints the
/// pergroup cell; the caller snapshots it per row), transitions batch per
/// pergroup-pointer lane. Per-group accumulation order within a transition is
/// batch order = plan order; across transitions the fold is transition-major,
/// bit-identical for these commutative kernels.
///
/// # Safety
/// `groups[k]` is the live pergroup array the hash lookup installed for row
/// `idxs[k]` of this batch (entries are never moved or freed within a batch;
/// spill mode only redirects NEW groups); `idxs` rows carry valid lane values
/// for every plan column; AvgAccum pergroups hold a live transarray;
/// Int128AvgAccum pergroups are NULL or hold a live aggcontext
/// `Int128AggState` pointer, with `aggcxt` that same aggcontext; str-kind
/// lanes and pergroups as in `fold_batch`. Guarded plans require a prior
/// `check_guards` `Pass` on this batch. The str kinds advance per row (no
/// batch pre-fold): each improvement datumCopies into `aggcxt` exactly where
/// the per-row program would, keeping hash-agg memory accounting — and so
/// spill decisions — byte-identical.
pub unsafe fn fold_rows_grouped(
    plan: &LanePlan<'_>,
    cols: &impl LaneCols,
    idxs: &[u32],
    groups: &[NonNull<AggPerGroup>],
    aggcxt: Mcx<'_>,
) -> PgResult<()> {
    debug_assert_eq!(idxs.len(), groups.len());
    for t in plan.trans.iter() {
        let transno = t.transno as usize;
        if t.kind == LaneKind::CountStar {
            for &g in groups {
                // SAFETY: caller contract.
                let pg = unsafe { &mut *g.as_ptr().add(transno) };
                pg.trans_value = Datum::from_i64(pg.trans_value.as_i64().wrapping_add(1));
                pg.trans_value_is_null = false;
                pg.no_trans_value = false;
            }
            continue;
        }
        let (values, isnull) =
            (cols.col_values(t.col as usize), cols.col_isnull(t.col as usize));
        if t.kind == LaneKind::Int128AvgAccum {
            // Dedicated row loop: the non-strict transfn runs for NULL
            // inputs too (state alloc on the group's first row of any
            // nullness — C parity), so this kind must not take the shared
            // skip-null path below.
            debug_assert_eq!((t.addend, t.mulk, t.divk), (0, 1, 1), "bare-Var admission only");
            for (&i, &g) in idxs.iter().zip(groups.iter()) {
                let i = i as usize;
                // SAFETY: caller contract.
                let pg = unsafe { &mut *g.as_ptr().add(transno) };
                let st = int128_state(pg, aggcxt)?;
                if !isnull[i] {
                    // SAFETY: aggcontext-lived state from int128_state or
                    // the per-row transfn chain; sole reference here —
                    // bit-identical by construction (the per-row path's own
                    // transition body).
                    unsafe {
                        do_int128_accum(&mut *st, lane_value(values, t.width, i) as i128)
                    };
                }
            }
            continue;
        }
        for (&i, &g) in idxs.iter().zip(groups.iter()) {
            let i = i as usize;
            if isnull[i] {
                continue;
            }
            // SAFETY: caller contract.
            let pg = unsafe { &mut *g.as_ptr().add(transno) };
            // t.kind is loop-invariant: LLVM unswitches, and the integer lane
            // read/transform stays out of the datum-lane arms.
            match t.kind {
                LaneKind::CountStar | LaneKind::Int128AvgAccum => unreachable!(),
                LaneKind::CountAny => count_apply(pg, 1),
                LaneKind::Sum => sum_apply(pg, xform(t, lane_value(values, t.width, i))),
                LaneKind::AvgAccum => avg_apply(pg, 1, xform(t, lane_value(values, t.width, i))),
                LaneKind::Min | LaneKind::Max => {
                    let v = xform(t, lane_value(values, t.width, i));
                    minmax_advance(t, pg, v, t.kind == LaneKind::Max);
                }
                LaneKind::FMin | LaneKind::FMax => {
                    fmm_advance(t, pg, values[i], t.kind == LaneKind::FMax);
                }
                LaneKind::BoolAnd | LaneKind::BoolOr => {
                    bool_advance(pg, values[i].as_bool(), t.kind == LaneKind::BoolAnd);
                }
                LaneKind::BitAnd | LaneKind::BitOr => {
                    let v = xform(t, lane_value(values, t.width, i));
                    bit_advance(t, pg, v, t.kind == LaneKind::BitAnd);
                }
                LaneKind::StrMin | LaneKind::StrMax | LaneKind::BpMin | LaneKind::BpMax => {
                    // SAFETY: vguard-passed batch + live aggcxt (caller
                    // contract).
                    unsafe { str_advance(t, pg, values[i], aggcxt)? };
                }
            }
        }
    }
    Ok(())
}
