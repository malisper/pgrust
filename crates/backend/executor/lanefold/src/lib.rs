// Lane-native aggregate transition fold — the whole-batch int-family
// transition kernels harvested from the pgrcolumnar branch's
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
//   (lane-v2-strlenfold, the length()-arg plain-agg class) adds the int4 SUM/AVG/MIN/MAX/bit kinds
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
// STRIPPED (pgrcolumnar/lane-v1 wiring that does not exist on lane-executor-v2):
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

use ::adt_numeric::aggregates::{do_int128_accum, Int128AggState, NumericAggState};
use ::adt_float::aggregates::{
    check_float8_array, float8_accum, float8_regr_accum, write_float8_transarray,
    FLOAT8_ARRAY_HDRSZ,
};
use ::adt_float::{float4_pl, float8_pl};
use ::datum::Datum;
use ::execexpr::{AggPerGroup, AggTransSpec, OUTER_VAR};
use ::exectuples::{SoaBatch, SoaDictLane};
use ::mcx::{Mcx, PgVec};
use ::types_core::catalog::{
    BOOLOID, BPCHAROID, C_COLLATION_OID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
    DEFAULT_COLLATION_OID, INT8OID, NUMERICOID, POSIX_COLLATION_OID, TEXTOID, TIMESTAMPOID,
    TIMESTAMPTZOID, VARCHAROID,
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

// Fold-trans tier (lane-v2-lanefold-trans, knob PGRUST_LANE_V2_FOLD_TRANS,
// default OFF): the AGG_SEQ fused arm's residual transition class
// (notes/se-aggseq-adjudication.md §4's owed increment). Unlike every prior
// tier, these transitions are ORDER-SENSITIVE (float addition does not
// reassociate), so their kernels are order-preserving sequential folds: the
// per-row C arithmetic (adt_float's accum_kernel / float8_pl /
// float8_regr_accum, adt_numeric's do_numeric_accum — fp-contract parity
// included) applied row by row in row order into C's own state. No batch
// reassociation, no SIMD tree-sum: the win is the elided per-row executor
// ceremony, not different math.
// v1 (this increment) admits the FLOAT SUM and FLOAT AVG/VAR/STDDEV family —
// the order-sensitive core: sum(float4/float8) rides float4pl/float8pl over a
// byval float transvalue; avg/var_samp/var_pop/stddev_samp/stddev_pop over
// float4/float8 all ride float4_accum/float8_accum over ONE float8[3]
// Youngs-Cramer transarray. Both are strict with the aggregate's catalog
// initval (SUM: NULL; ACCUM: the '{0,0,0}' float8[3]).
const F_FLOAT4PL: Oid = 204;
const F_FLOAT8PL: Oid = 218;
const F_FLOAT4_ACCUM: Oid = 208;
const F_FLOAT8_ACCUM: Oid = 222;
// Fold-trans increment 2 (lane AGGSEQ-FOLD2, same PGRUST_LANE_V2_FOLD_TRANS
// knob): the remaining v1 named refusals converted to folds.
//  * numeric_avg_accum (2858 — sum(numeric) 2114 and avg(numeric) 2103 both
//    bind it): NOT strict, NULL initval, INTERNAL NumericAggState (the
//    se/agg-poly relocation substrate's per-backend state). The fold drives
//    C's exact per-row do_numeric_accum in row order; 1B-short images expand
//    into aligned SCRATCH (never the aggcontext — allocation sequence and
//    hash-agg memory accounting must match the per-row path, whose expand
//    lands in the reset-per-row tuple context). Varlena lane => vguarded.
//  * float8_regr_accum (2806, corr/covar_pop/covar_samp/regr_* — TWO-arg)
//    + int8inc_float8_float8 (2805, regr_count): the second lane column
//    rides LaneTrans::col2; Youngs-Cramer bivariate updates in row order
//    (same ordering discipline as FAccum, fp-contract parity included).
//  * F64 cast reads (the v1 coverage-plan row: i2tod 235 / i4tod 316 /
//    i8tod 482 / ftod 311 over a bare Var): C's exact scalar cast per row
//    (i2/i4/f4 exact, i8 rounds ties-to-even exactly as C's (float8) cast),
//    non-erroring, admitted for every F64-reading fold-trans input
//    (float8pl / float8_accum / float8_regr_accum args).
//  * FILTER'd transitions (spec.aggfilter): a per-transition mask, applied
//    BEFORE the kernel — C evaluates the FILTER per row BEFORE the
//    transition, so the fold folds exactly the filter-passing rows in row
//    order. Classifiable predicate forms: bool Var, NOT bool Var, and a
//    same-width int2/4/8 Var-vs-Const comparison (either operand order).
const F_NUMERIC_AVG_ACCUM: Oid = 2858;
const F_FLOAT8_REGR_ACCUM: Oid = 2806;
const F_INT8INC_FLOAT8_FLOAT8: Oid = 2805;
const F_I2TOD: Oid = 235;
const F_FTOD: Oid = 311;
const F_I4TOD: Oid = 316;
const F_I8TOD: Oid = 482;
// FILTER comparison operator functions (int.c / int8.c), same-width only —
// the cross-width int24/int48 comparisons stay refused (their coercion
// semantics add nothing the planner doesn't already normalize away).
const F_INT2EQ: Oid = 63;
const F_INT2NE: Oid = 145;
const F_INT2LT: Oid = 64;
const F_INT2GT: Oid = 146;
const F_INT2LE: Oid = 148;
const F_INT2GE: Oid = 151;
const F_INT4EQ: Oid = 65;
const F_INT4NE: Oid = 144;
const F_INT4LT: Oid = 66;
const F_INT4GT: Oid = 147;
const F_INT4LE: Oid = 149;
const F_INT4GE: Oid = 150;
const F_INT8EQ: Oid = 467;
const F_INT8NE: Oid = 468;
const F_INT8LT: Oid = 469;
const F_INT8GT: Oid = 470;
const F_INT8LE: Oid = 471;
const F_INT8GE: Oid = 472;
//
// NAMED REFUSALS (documented follow-ups, NOT wrong folds — the profit-bar
// law: anything not byte-exact refuses to the arm/per-row path):
//  * numeric_accum (1834) + int2/4/8_accum (stddev/variance over numeric/int
//    args, sum_x2-carrying): out of the proven envelope, matching the
//    agg-poly car's own named refusal.
//  * FILTER predicates beyond {bool Var, NOT bool Var, same-width int2/4/8
//    Var-vs-Const comparison}: refuse to the per-row/arm path. Volatile
//    predicates are structurally unreachable (no function-calling form
//    classifies), preserving C's per-row evaluation for them.
//  * FILTER on a transition carrying a data-level integer Guard: refused —
//    the "demoted batch always raises" guard law does not hold once a
//    filter can exclude the offending row; the combination keeps C's
//    raise-at-C's-row discipline by refusing.
//  * ordered/DISTINCT/combine-phase transitions: refuse exactly as base.
//  * Parallel export of the fold-trans kinds: kind_admits stays refusing
//    (serial-only tier); numeric can later ride the agg-poly NumericAgg
//    manifest.

/// Process-constant knob for the fold-trans tier (R-KNOBS discipline:
/// `PGRUST_LANE_V2_FOLD_TRANS`, default OFF — the branch convention for
/// unproven increments). OFF = the new classify arms refuse and every plan
/// is byte-identical to base. AtomicU8 (not OnceLock) so units can A/B
/// in-process via [`fold_trans_set_for_tests`] — the k1_latemat idiom.
pub fn fold_trans_enabled() -> bool {
    match FOLD_TRANS.load(core::sync::atomic::Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = matches!(
                std::env::var("PGRUST_LANE_V2_FOLD_TRANS").as_deref(),
                Ok("1") | Ok("on")
            );
            FOLD_TRANS.store(
                if on { 2 } else { 1 },
                core::sync::atomic::Ordering::Relaxed,
            );
            on
        }
    }
}

/// Test-only override (any caller may flip it; production code never does).
pub fn fold_trans_set_for_tests(on: bool) {
    FOLD_TRANS.store(if on { 2 } else { 1 }, core::sync::atomic::Ordering::Relaxed);
}

// 0 = unresolved (read env on first use), 1 = off, 2 = on.
static FOLD_TRANS: core::sync::atomic::AtomicU8 = core::sync::atomic::AtomicU8::new(0);

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

/// Planner-probe mirror of [`classify_len_arg`]'s FUNCID + ENCODING half
/// (stragg-coverage LENARG car): does this funcid belong to the
/// textlen-family fold-arg vocabulary under the CURRENT server encoding?
/// The Var/shape half (bare text/varchar Var on the scanned rel) lives at
/// parse altitude in the m5 probe; keeping the funcid table + encoding
/// gates HERE, next to the classifier that consumes them, is the
/// knob-coherence law — a funcid admitted here MUST classify in
/// `classify_len_arg` over a staged text lane, or the probe suppresses a
/// shape the fold refuses (suppress-then-serial). octet_length reads the
/// payload byte count (encoding-free); the char-length aliases need the
/// vectorizable count kernels (1-byte-max encodings or UTF-8) and refuse
/// on a missing encoding seam exactly as the classifier does.
pub fn len_arg_funcid_admits(funcid: Oid) -> bool {
    if funcid == F_TEXTOCTETLEN {
        return true;
    }
    if !F_TEXTLEN.contains(&funcid) {
        return false;
    }
    if !::mbutils_seams::pg_database_encoding_max_length::is_installed()
        || !::mbutils_seams::get_database_encoding::is_installed()
    {
        return false;
    }
    ::mbutils_seams::pg_database_encoding_max_length::call() == 1
        || ::mbutils_seams::get_database_encoding::call() == PG_UTF8
}

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
    /// Length-staged column (lane-v2-asciilen): the feed answered this
    /// column's lane as `Datum::from_i64(length)` values — the admitted
    /// `length(v)`/`octet_length(v)` result computed AT THE FILL with C's
    /// exact semantics (per-dict-code table / header read / C mb walk) — so
    /// the VarLen kernels read it as a plain I64 lane and the vguard/uguard
    /// batch proofs are vacuous for it (no datum is ever dereferenced).
    /// Feeds without length staging keep the default.
    #[inline(always)]
    fn col_len_staged(&self, _c: usize) -> bool {
        false
    }

    /// Dict-code side channel for a str MIN/MAX column (lane-v2-dictminmax).
    /// `Some(lane)` is the feed's PROOF, per staged batch, that
    ///
    /// 1. for every SELECTED non-null staged row `i`, `col_values(c)[i]` is
    ///    the (inline varlena) datum `lane.table.datum(lane.code(i))` — the
    ///    column's datum cells were gathered from this very dictionary, so
    ///    the datum a kernel advances/copies IS the code's decoded value;
    /// 2. when `lane.table.sorted`, the dictionary entries are DEDUPLICATED
    ///    and strictly ascending in `varstrfastcmp_c` order (byte-lexicographic
    ///    memcmp + length tiebreak — pgrcolumnar's writer sorts payload byte
    ///    slices with exactly that order), so within this epoch
    ///    `sign(varstrfastcmp_c(dict[a], dict[b])) == sign(cmp(a, b))` and
    ///    equal codes are the SAME datum pointer.
    ///
    /// The kernels consult it only for `StrMin`/`StrMax` (the admission gate
    /// already proved a memcmp-tier collation, under which varstr_cmp IS
    /// varstrfastcmp_c) and only when `sorted`; bpchar kinds never use it
    /// (bcTruelen's trailing-blank trim breaks the code order). Feeds
    /// without a dict view keep the default.
    #[inline(always)]
    fn col_codes(&self, _c: usize) -> Option<SoaDictLane> {
        None
    }
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

    #[inline(always)]
    fn col_len_staged(&self, c: usize) -> bool {
        SoaBatch::len_want(self, c) != 0
    }
}

/// The width a kernel reads column `col` at: length-staged VarLen lanes read
/// as I64 (the fill materialized the integer answers), everything else at
/// the classify width.
#[inline(always)]
fn read_width(cols: &impl LaneCols, col: u16, width: LaneWidth) -> LaneWidth {
    match width {
        LaneWidth::VarLenBytes | LaneWidth::VarLenChars if cols.col_len_staged(col as usize) => {
            LaneWidth::I64
        }
        w => w,
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
    // Fold-trans tier (lane-v2-lanefold-trans; ORDER-SENSITIVE). float4pl /
    // float8pl (float.c): sum(float4)/sum(float8). Strict, NULL init, byval
    // float transvalue. The first non-null selected row STORES its value raw
    // (C's strict-transfn null-state special case — no float_pl on call 1),
    // every later row does `state = float_pl(state, v)?` — which raises C's
    // overflow-to-infinity error at C's exact row. NOT commutative: the fold
    // walks selected rows in row order and applies float_pl one at a time
    // (no batch reassociation), so the bytes match C's per-row sum. `width`
    // and `res_width` are F32 for float4, F64 for float8.
    FSum,
    // float4_accum / float8_accum (float.c): avg/var_samp/var_pop/
    // stddev_samp/stddev_pop over float4/float8. Strict, NON-null initval
    // (the catalog '{0,0,0}' float8[3]), so the transvalue is a live
    // float8[3] Youngs-Cramer transarray from the first row — never
    // no_trans_value. Per non-null row `[n,sx,sxx] = accum([n,sx,sxx], v)?`
    // (float4_accum widens v: f32->f64 before accumulate), written back in
    // place. ORDER-SENSITIVE (Youngs-Cramer sxx term): the fold walks row
    // order, no reassociation. `width` = F32/F64 (how to read the input
    // Var); the transvalue is always the float8[3] array.
    FAccum,
    // numeric_avg_accum (numeric.c): sum(numeric)/avg(numeric). NOT strict,
    // NULL initval, INTERNAL NumericAggState allocated in the aggcontext on
    // the group's FIRST transfn call (null-input rows included — exactly the
    // Int128AvgAccum discipline); only non-null inputs accumulate, each
    // through C's exact do_numeric_accum body (special NaN/±inf counting,
    // max-dscale bookkeeping, exact sum_x digit accumulation) in row order.
    // The lane is a vguarded varlena pointer lane (`width` = Var): 1B-short
    // images expand into aligned scratch per row (C's DatumGetNumeric
    // detoast), compressed/external datums demote the whole batch.
    NumAccum,
    // float8_regr_accum (float.c): corr/covar_pop/covar_samp/regr_slope/
    // regr_intercept/regr_r2/regr_sxx/regr_syy/regr_sxy/regr_avgx/regr_avgy.
    // Strict TWO-arg (Y = arg 1 reads `col`, X = arg 2 reads `col2`; a row
    // participates iff BOTH are non-null), NON-null '{0,0,0,0,0,0}'
    // float8[6] initval — a live bivariate Youngs-Cramer transarray from the
    // first row. ORDER-SENSITIVE exactly as FAccum (the sxx/syy/sxy terms
    // depend on the running means): row-order walk, no reassociation,
    // fp-contract (mul_add) parity with compiled C, C's overflow ereport at
    // C's row. Inputs read through the `fconv`/`fconv2` F64 conversions.
    FRegrAccum,
    // int8inc_float8_float8 (int8.c): regr_count. Strict TWO-arg with the
    // non-null '0' initval — counts rows where BOTH args are non-null.
    // Reads no values (order-free, commutative), only the two isnull lanes.
    Count2,
}

/// F64 input conversion for a fold-trans lane read: how a kernel turns the
/// staged lane value into the f64 the transfn consumes. `None` = the lane is
/// a bare float8 datum. The cast tags mirror C's non-erroring float8 cast
/// functions over a bare Var (i2tod/i4tod/i8tod/ftod): i2/i4/f4 widen
/// exactly; i8 rounds ties-to-even exactly as C's `(float8) int64` cast.
/// Bare float4_accum inputs carry `F4` (C widens f32→f64 inside the
/// transfn). NULL-ness of a converted input equals the Var's (strict casts).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FloatConv {
    None,
    I2,
    I4,
    I8,
    F4,
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

/// Sentinel for [`LaneTrans::filter`]: no FILTER on this transition.
pub const NO_FILTER: u8 = u8::MAX;

// addend/mulk/divk are i32: every admitted coefficient is an int4 Const (or a
// ±1 identity), so the affine transform's stored coefficients provably fit
// i32; they widen to i64 at the use site, leaving the fold arithmetic
// byte-identical.
#[derive(Clone, Copy, Debug)]
pub struct LaneTrans {
    pub kind: LaneKind,
    pub col: u16,
    // Second input column (fold-trans two-arg kinds FRegrAccum/Count2:
    // Y reads `col`, X reads `col2`). One-arg kinds mirror `col` here so the
    // field is always a valid staged column index.
    pub col2: u16,
    // Lane read width (the admitted Var's type) vs the transvalue store
    // width (the transfn's argument/result type — int4 for the int2-Var
    // OpExpr admissions). Min/Max must store at res_width or an in-range
    // int4 result truncates through the int2 datum constructor.
    pub width: LaneWidth,
    pub res_width: LaneWidth,
    // F64 input conversions for the fold-trans F64-reading kinds (`col` /
    // `col2` respectively); `FloatConv::None` everywhere else.
    pub fconv: FloatConv,
    pub fconv2: FloatConv,
    // Index into LanePlan::filters ([`NO_FILTER`] = unfiltered): the
    // per-transition FILTER mask, applied BEFORE the kernel.
    pub filter: u8,
    // Admitted arg expression, per selected row: ((v / divk) * mulk) + addend
    // with v the lane value. Ops are exclusive (single OpExpr admission), so
    // composition order is never observable.
    pub addend: i32,
    pub mulk: i32,
    pub divk: i32,
    pub transno: u16,
}

const _: () = assert!(core::mem::size_of::<LaneTrans>() <= 24);

/// Comparison operator of an admitted int Var-vs-Const FILTER predicate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Admitted FILTER predicate form. C evaluates the FILTER expression per row
/// BEFORE the transition and runs the transition only when the result is
/// non-NULL true — a NULL filter input (NULL bool Var / NULL comparison
/// operand under these strict operators) yields NULL and SKIPS the row, which
/// is exactly what [`filter_passes`] encodes (`isnull ⇒ false`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FilterPred {
    /// bare bool Var: pass iff non-null true.
    BoolVar,
    /// NOT (bool Var): pass iff non-null false.
    NotBoolVar,
    /// same-width int2/4/8 `Var OP Const` (Const-first operand order admits
    /// with the operator mirrored): pass iff the Var is non-null and the
    /// comparison holds.
    Cmp(FilterOp),
}

/// One classified FILTER predicate (side table, shared by identical filters
/// across transitions). `width` is the filter column's lane read width
/// (Bool for the bool forms; I16/I32/I64 for comparisons); `konst` is the
/// comparison constant sign-extended to i64 (0 for the bool forms).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FilterEntry {
    pub pred: FilterPred,
    pub col: u16,
    pub width: LaneWidth,
    pub konst: i64,
}

// Branchy on the loop-invariant transform fields so LLVM unswitches the
// per-row loops: the dominant addend-only shape must stay a bare add (an
// unconditional sdiv per row cost byval min/max folds ~13% on the analytics banks).
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
    // Classified FILTER predicates (deduped); LaneTrans::filter indexes here.
    // Filter columns are staged like lane columns (they join `cols`), and the
    // kernels apply the mask BEFORE folding — check_guards' vguard/uguard
    // proofs deliberately run over the UNMASKED selection (a superset): a
    // conservative demote re-evaluates the filter in the per-row program, so
    // over-proving is correct, never wrong.
    pub filters: PgVec<'mcx, FilterEntry>,
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

// True floor/ceil division for either divisor sign. (Port fix: the pgrcolumnar
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
        // Numeric rides the varlena pointer lane (NumAccum only — the fold
        // reads the datum's payload in place under the vguard proof, exactly
        // as the str tier; no other admission passes NUMERICOID here).
        TEXTOID | VARCHAROID | BPCHAROID | NUMERICOID => Some(LaneWidth::Var),
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
// tiebreak, cannot error, allocate, or call libc/ICU). DEFAULT (100) admits
// exactly when the database default locale resolves collate-C (q22coexist:
// C's lc_collate_is_c(DEFAULT) — varstr_cmp then takes the SAME
// varstrfastcmp_c path, so the fold comparator is bit-identical). The
// resolution reads the backend-init-installed default locale (a thread-local
// Cell — no catalog access, so classify stays self-contained) and
// fail-closes when uninstalled (unit-test contexts). Non-C-resolving
// DEFAULT and libc/ICU collations refuse: their per-row comparison can
// allocate and (ICU) error mid-batch, which the fold cannot replay at C's
// row.
pub fn str_collation_safe(collid: Oid) -> bool {
    if collid == C_COLLATION_OID || collid == POSIX_COLLATION_OID {
        return true;
    }
    collid == DEFAULT_COLLATION_OID
        && ::pg_locale::default_locale_installed()
        && ::pg_locale::pg_newlocale_from_collation(collid)
            .is_ok_and(|l| l.collate_is_c && l.deterministic)
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

// F64 input admission for the fold-trans kinds (float8pl / float8_accum /
// float8_regr_accum args): a bare float8 Var, or one of C's non-erroring
// float8 cast FuncExprs over a bare Var — i2tod (235) / i4tod (316) / i8tod
// (482) / ftod (311). The per-row conversion is C's exact scalar cast
// (i2/i4/f4 widen exactly; i8 is IEEE round-ties-to-even in both C's
// `(float8) int64` and Rust's `as f64`), cannot error, and the strict cast
// makes the result's NULL-ness the Var's. Returns the lane column, the
// lane's read width, and the conversion tag.
fn classify_f64_arg(expr: Node<'_>) -> Option<(u16, LaneWidth, FloatConv)> {
    if let Some((col, w)) = classify_var(expr, FLOAT8OID) {
        return Some((col, w, FloatConv::None));
    }
    let f = expr.as_func_expr()?;
    if f.funcretset || f.args.len() != 1 {
        return None;
    }
    let (vartype, width, conv) = match f.funcid {
        F_I2TOD => (INT2OID, LaneWidth::I16, FloatConv::I2),
        F_I4TOD => (INT4OID, LaneWidth::I32, FloatConv::I4),
        F_I8TOD => (INT8OID, LaneWidth::I64, FloatConv::I8),
        F_FTOD => (FLOAT4OID, LaneWidth::F32, FloatConv::F4),
        _ => return None,
    };
    let (col, _) = classify_var(f.args.iter().next()?, vartype)?;
    Some((col, width, conv))
}

/// FILTER predicate admission (fold-trans tier). Classifiable forms — the
/// exact envelope the NAMED-refusal ledger states:
/// * bare bool Var,
/// * `NOT (bool Var)` (a NOT_EXPR BoolExpr over a bare bool Var),
/// * same-width int2/4/8 Var-vs-Const comparison (eq/ne/lt/le/gt/ge; either
///   operand order — a Const-first shape admits with the operator mirrored).
/// Everything else returns None and the TRANSITION refuses to the per-row /
/// arm path (volatile predicates are structurally unreachable: no
/// function-calling form is admitted).
pub fn classify_filter(expr: Node<'_>) -> Option<FilterEntry> {
    if let Some((col, _)) = classify_var(expr, BOOLOID) {
        return Some(FilterEntry {
            pred: FilterPred::BoolVar,
            col,
            width: LaneWidth::Bool,
            konst: 0,
        });
    }
    if let Some(b) = expr.as_bool_expr() {
        if b.boolop != ::types_nodes::primnodes::BoolExprType::NOT_EXPR || b.args.len() != 1 {
            return None;
        }
        let (col, _) = classify_var(b.args.iter().next()?, BOOLOID)?;
        return Some(FilterEntry {
            pred: FilterPred::NotBoolVar,
            col,
            width: LaneWidth::Bool,
            konst: 0,
        });
    }
    let op = expr.as_op_expr()?;
    if op.opretset || op.args.len() != 2 {
        return None;
    }
    let (vartype, width, fop) = match op.opfuncid {
        F_INT2EQ => (INT2OID, LaneWidth::I16, FilterOp::Eq),
        F_INT2NE => (INT2OID, LaneWidth::I16, FilterOp::Ne),
        F_INT2LT => (INT2OID, LaneWidth::I16, FilterOp::Lt),
        F_INT2LE => (INT2OID, LaneWidth::I16, FilterOp::Le),
        F_INT2GT => (INT2OID, LaneWidth::I16, FilterOp::Gt),
        F_INT2GE => (INT2OID, LaneWidth::I16, FilterOp::Ge),
        F_INT4EQ => (INT4OID, LaneWidth::I32, FilterOp::Eq),
        F_INT4NE => (INT4OID, LaneWidth::I32, FilterOp::Ne),
        F_INT4LT => (INT4OID, LaneWidth::I32, FilterOp::Lt),
        F_INT4LE => (INT4OID, LaneWidth::I32, FilterOp::Le),
        F_INT4GT => (INT4OID, LaneWidth::I32, FilterOp::Gt),
        F_INT4GE => (INT4OID, LaneWidth::I32, FilterOp::Ge),
        F_INT8EQ => (INT8OID, LaneWidth::I64, FilterOp::Eq),
        F_INT8NE => (INT8OID, LaneWidth::I64, FilterOp::Ne),
        F_INT8LT => (INT8OID, LaneWidth::I64, FilterOp::Lt),
        F_INT8LE => (INT8OID, LaneWidth::I64, FilterOp::Le),
        F_INT8GT => (INT8OID, LaneWidth::I64, FilterOp::Gt),
        F_INT8GE => (INT8OID, LaneWidth::I64, FilterOp::Ge),
        _ => return None,
    };
    let mut it = op.args.iter();
    let (a, b) = (it.next()?, it.next()?);
    // Var OP Const, or Const OP Var with the operator mirrored (a < v ≡
    // v > a — the CONSTANT stays on the right of the stored predicate).
    let (col, konst, fop) = match classify_var(a, vartype) {
        Some((col, _)) => (col, b.as_const()?, fop),
        None => {
            let (col, _) = classify_var(b, vartype)?;
            let mirrored = match fop {
                FilterOp::Eq => FilterOp::Eq,
                FilterOp::Ne => FilterOp::Ne,
                FilterOp::Lt => FilterOp::Gt,
                FilterOp::Le => FilterOp::Ge,
                FilterOp::Gt => FilterOp::Lt,
                FilterOp::Ge => FilterOp::Le,
            };
            (col, a.as_const()?, mirrored)
        }
    };
    if konst.constisnull || konst.consttype != vartype {
        return None;
    }
    let k = match width {
        LaneWidth::I16 => konst.constvalue.as_i16() as i64,
        LaneWidth::I32 => konst.constvalue.as_i32() as i64,
        _ => konst.constvalue.as_i64(),
    };
    Some(FilterEntry { pred: FilterPred::Cmp(fop), col, width, konst: k })
}

/// One row's FILTER verdict (C ExecEvalAggFilter semantics: run the
/// transition iff the predicate is non-NULL true; the admitted forms are
/// strict in the Var, so a NULL filter input skips).
#[inline(always)]
fn filter_passes(f: &FilterEntry, values: &[Datum], isnull: &[bool], i: usize) -> bool {
    if isnull[i] {
        return false;
    }
    match f.pred {
        FilterPred::BoolVar => values[i].as_bool(),
        FilterPred::NotBoolVar => !values[i].as_bool(),
        FilterPred::Cmp(op) => {
            let v = lane_value(values, f.width, i);
            match op {
                FilterOp::Eq => v == f.konst,
                FilterOp::Ne => v != f.konst,
                FilterOp::Lt => v < f.konst,
                FilterOp::Le => v <= f.konst,
                FilterOp::Gt => v > f.konst,
                FilterOp::Ge => v >= f.konst,
            }
        }
    }
}

// The masked selection for one filter over one staged batch: out[w] keeps
// exactly the selected rows whose filter predicate passes. The kernels then
// fold the MASKED rows in row order — "mask first, then fold", which is
// bit-equal to C's per-row filter-then-transition sequence because the
// admitted predicates are pure per-row reads.
fn build_filter_mask(
    f: &FilterEntry,
    cols: &impl LaneCols,
    rows: &[u64],
    out: &mut Vec<u64>,
) {
    let (values, isnull) = (cols.col_values(f.col as usize), cols.col_isnull(f.col as usize));
    out.clear();
    out.reserve(rows.len());
    for (w, &word) in rows.iter().enumerate() {
        let mut bits = word;
        let mut keep = 0u64;
        while bits != 0 {
            let b = bits.trailing_zeros();
            if filter_passes(f, values, isnull, w * 64 + b as usize) {
                keep |= 1u64 << b;
            }
            bits &= bits - 1;
        }
        out.push(keep);
    }
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
///
/// Returns (transition, integer guard, FILTER predicate). The filter's
/// side-table index is assigned by [`classify`] (LaneTrans::filter is
/// [`NO_FILTER`] here); a guarded admission never carries a filter (the
/// named guard×FILTER refusal — see the tier doc).
pub fn classify_trans(
    spec: &AggTransSpec<'_, '_>,
    transno: usize,
) -> Option<(LaneTrans, Option<GuardEntry>, Option<FilterEntry>)> {
    if spec.combine || spec.ordered.is_some() || spec.cur_agg.is_some() {
        return None;
    }
    // FILTER admission (fold-trans tier, knob-gated): classify the predicate
    // or refuse the whole transition. Knob OFF keeps base's unconditional
    // aggfilter refusal — byte-identical estate.
    let filter = match spec.aggfilter {
        None => None,
        Some(f) if fold_trans_enabled() => Some(classify_filter(f)?),
        Some(_) => return None,
    };
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
                col2: a.col,
                width: a.width,
                res_width,
                fconv: FloatConv::None,
                fconv2: FloatConv::None,
                filter: NO_FILTER,
                addend: a.addend as i32,
                mulk: a.mulk as i32,
                divk: a.divk as i32,
                transno,
            },
            guard,
            filter,
        ))
    };
    // Fold-trans F64/two-arg builder: never integer-guarded, never affine.
    let mk2 = |kind, col, col2, width, res_width, fconv, fconv2| {
        Some((
            LaneTrans {
                kind,
                col,
                col2,
                width,
                res_width,
                fconv,
                fconv2,
                filter: NO_FILTER,
                addend: 0,
                mulk: 1,
                divk: 1,
                transno,
            },
            None,
            filter,
        ))
    };
    // Single F64-read admission (bare f8 Var or F64 cast read).
    let f64arg = || -> Option<(u16, LaneWidth, FloatConv)> {
        if spec.args.len() != 1 {
            return None;
        }
        classify_f64_arg(spec.args.iter().next()?.as_target_entry()?.expr)
    };
    // Two F64-read admission (Y = arg 1, X = arg 2 — C float8_regr_accum's
    // argument order).
    let f64args2 = || -> Option<((u16, LaneWidth, FloatConv), (u16, LaneWidth, FloatConv))> {
        if spec.args.len() != 2 {
            return None;
        }
        let mut it = spec.args.iter();
        let y = classify_f64_arg(it.next()?.as_target_entry()?.expr)?;
        let x = classify_f64_arg(it.next()?.as_target_entry()?.expr)?;
        Some((y, x))
    };
    let plain = |col, width| {
        let (addend, mulk, divk) = PLAIN;
        (LaneArg { col, width, addend, mulk, divk, guard: None }, width)
    };
    let out = match spec.transfn_oid {
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
        // Fold-trans tier (knob-gated OFF): the AGG_SEQ residual FLOAT
        // family. sum(float*) needs a NULL initval (byval float state); the
        // accum family needs the non-null '{0,0,0}' float8[3] initval.
        // float4-typed inputs admit only a bare Var (no cast to float4 is
        // admitted); F64-typed inputs additionally admit the i2tod/i4tod/
        // i8tod/ftod cast reads. No transform, no integer guard, no CSE.
        F_FLOAT4PL if fold_trans_enabled() && spec.init_value_is_null => {
            mk(LaneKind::FSum, arg(FLOAT4OID)?)
        }
        F_FLOAT8PL if fold_trans_enabled() && spec.init_value_is_null => {
            let (col, width, conv) = f64arg()?;
            mk2(LaneKind::FSum, col, col, width, LaneWidth::F64, conv, FloatConv::None)
        }
        F_FLOAT4_ACCUM if fold_trans_enabled() && !spec.init_value_is_null => {
            // Bare float4 Var; the kernel widens f32→f64 exactly as C's
            // float4_accum — encoded as the F4 conversion tag.
            let (a, _) = arg(FLOAT4OID)?;
            mk2(
                LaneKind::FAccum,
                a.col,
                a.col,
                LaneWidth::F32,
                LaneWidth::F32,
                FloatConv::F4,
                FloatConv::None,
            )
        }
        F_FLOAT8_ACCUM if fold_trans_enabled() && !spec.init_value_is_null => {
            let (col, width, conv) = f64arg()?;
            mk2(LaneKind::FAccum, col, col, width, LaneWidth::F64, conv, FloatConv::None)
        }
        // Fold-trans increment 2: numeric sum/avg — NOT strict, NULL initval,
        // INTERNAL NumericAggState (lazily aggcontext-allocated by the fold,
        // the Int128AvgAccum discipline). Bare numeric Var only; the Var
        // width puts the column under the vguard proof (inline varlena) and
        // the kernel runs C's exact do_numeric_accum per row in row order.
        F_NUMERIC_AVG_ACCUM if fold_trans_enabled() && spec.init_value_is_null => {
            mk(LaneKind::NumAccum, arg(NUMERICOID)?)
        }
        // corr/covar/regr family: strict two-arg float8_regr_accum over the
        // '{0,0,0,0,0,0}' float8[6] initval; Y rides `col`, X rides `col2`.
        F_FLOAT8_REGR_ACCUM if fold_trans_enabled() && !spec.init_value_is_null => {
            let ((cy, wy, vy), (cx, _, vx)) = f64args2()?;
            mk2(LaneKind::FRegrAccum, cy, cx, wy, LaneWidth::F64, vy, vx)
        }
        // regr_count: strict two-arg counter, non-null '0' initval.
        F_INT8INC_FLOAT8_FLOAT8 if fold_trans_enabled() && !spec.init_value_is_null => {
            let ((cy, wy, vy), (cx, _, vx)) = f64args2()?;
            mk2(LaneKind::Count2, cy, cx, wy, LaneWidth::I64, vy, vx)
        }
        _ => None,
    };
    let (t, g, f) = out?;
    // NAMED refusal: FILTER on a transition carrying a data-level integer
    // Guard — the "demoted batch always raises" law does not survive a mask
    // that can exclude the offending row.
    if g.is_some() && f.is_some() {
        return None;
    }
    Some((t, g, f))
}

// ===========================================================================
// Metadata-answerable transitions (re-harvested from the lane-v1 metacount /
// footer-sums work; value-correctness proven end-to-end in
// notes/q4-avg-quarantine-resolution.md): COUNT(*) / COUNT(bare Var) — equal
// on pgrcolumnar, which stores no NULLs (writer::append_row errors on NULL) —
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
        let (t, g, f) = classify_trans(spec, transno)?;
        // A FILTER'd transition is never footer-answerable: the footer
        // aggregates every visible row, the filter a per-row subset.
        if f.is_some() {
            return None;
        }
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
    let mut filters: PgVec<'mcx, FilterEntry> = PgVec::new_in(mcx);
    let mut resid: PgVec<'mcx, usize> = PgVec::new_in(mcx);
    for (transno, spec) in specs.iter().enumerate() {
        match classify_trans(spec, transno) {
            Some((mut t, g, f)) => {
                if let Some(fe) = f {
                    // Dedup identical predicates (one mask build serves every
                    // transition sharing the filter). The u8 index space is
                    // ample (transnos are u16, filters ≤ transitions), but
                    // fail closed on the sentinel bound anyway.
                    let idx = match filters.iter().position(|e| *e == fe) {
                        Some(i) => i,
                        None if filters.len() < NO_FILTER as usize => {
                            filters.push(fe);
                            filters.len() - 1
                        }
                        None => {
                            resid.push(transno);
                            continue;
                        }
                    };
                    t.filter = idx as u8;
                }
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
        // Two-arg kinds read a second lane; FILTER'd transitions read the
        // predicate column — both must be staged like any lane column.
        if matches!(t.kind, LaneKind::FRegrAccum | LaneKind::Count2) && !cols.contains(&t.col2) {
            cols.push(t.col2);
        }
        if t.filter != NO_FILTER {
            let fc = filters[t.filter as usize].col;
            if !cols.contains(&fc) {
                cols.push(fc);
            }
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
        filters,
        cols,
        resid,
        guarded,
    })
}

/// SE-GROUPONLY (night/subquery-admission): the VACUOUS fold plan for a
/// ZERO-transition hashed aggregation — grouping-only builds (bare
/// `GROUP BY` emit under a parent consumer, `SELECT DISTINCT`, the
/// grouped-subquery inner the arena-strings profile caught at a 7.2x
/// admission cliff). `classify` deliberately refuses empty spec sets
/// (line "trans.is_empty() => None" — nothing to fold); this constructor
/// is the one legal source of an empty plan, minted only by nodeagg's
/// knob-gated init arm: no transitions, no lane columns, no guards — every
/// fold call over it is a no-op by construction (`fold_rows_grouped*`
/// iterate `trans`), and the staged feeds' entire value is the BATCHED
/// GROUP PROBE (the compact tables / staged K2 legs) replacing the
/// row-at-a-time TupleHashTable lookup world.
pub fn empty_plan<'mcx>(mcx: Mcx<'mcx>) -> LanePlan<'mcx> {
    LanePlan {
        trans: PgVec::new_in(mcx),
        cse: PgVec::new_in(mcx),
        cse_members: PgVec::new_in(mcx),
        cse_skip: PgVec::new_in(mcx),
        guards: PgVec::new_in(mcx),
        vguards: PgVec::new_in(mcx),
        uguards: PgVec::new_in(mcx),
        filters: PgVec::new_in(mcx),
        cols: PgVec::new_in(mcx),
        resid: PgVec::new_in(mcx),
        guarded: false,
    }
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
        // FILTER'd transitions never join CSE: the shared base pass folds an
        // unmasked selection, and per-member masks would break the shared-
        // scan derivation. They keep independent per-trans kernels.
        if t.filter != NO_FILTER {
            continue;
        }
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
        if t.kind != LaneKind::CountAny || t.filter != NO_FILTER {
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
fn sum_selected(
    t: &LaneTrans,
    width: LaneWidth,
    values: &[Datum],
    isnull: &[bool],
    rows: &[u64],
) -> (i64, i64) {
    let mut c = 0i64;
    let mut s = 0i64;
    if t.mulk == 1 && t.divk == 1 {
        for_each_row(rows, |i| {
            if !isnull[i] {
                c += 1;
                s = s.wrapping_add(lane_value(values, width, i));
            }
        });
        s = s.wrapping_add(c.wrapping_mul(t.addend as i64));
    } else {
        for_each_row(rows, |i| {
            if !isnull[i] {
                c += 1;
                s = s.wrapping_add(xform(t, lane_value(values, width, i)));
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

/// avgpack (the nodeagg SINK builds' inline AvgInt8 representation): the
/// transno's 16-byte `AggPerGroup` slot IS the state, reinterpreted as
/// `[count: i64, sum: i64]` — no aggcontext transarray exists for a packed
/// transno. Same arithmetic as [`avg_apply`] (C `int4_avg_accum`'s element
/// adds), only the storage moved. The slot carries no null flags: AvgInt8
/// states are never SQL-null (non-null `{0,0}` initval, strict transfn) and
/// `count == 0` encodes the all-NULL-input group (C's `int8_avg` finalizes
/// exactly that to NULL).
#[inline(always)]
fn avg_apply_packed(pg: &mut AggPerGroup, c: i64, delta: i64) {
    const {
        assert!(core::mem::size_of::<AggPerGroup>() == 16);
        assert!(core::mem::align_of::<AggPerGroup>() == 8);
    }
    let w = (pg as *mut AggPerGroup).cast::<i64>();
    // SAFETY: the slot is 16 repr(C) bytes, 8-aligned — two i64 words; the
    // caller admitted this transno as packed (sink build contract).
    unsafe {
        *w = (*w).wrapping_add(c);
        *w.add(1) = (*w.add(1)).wrapping_add(delta);
    }
}

/// Whether `transno` is packed under `avgpack_mask` (bit per transno; the
/// mask's builder never sets bits for transnos >= 64).
#[inline(always)]
fn avgpack_of(mask: u64, transno: u16) -> bool {
    (transno as u32) < 64 && (mask >> transno) & 1 == 1
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
fn sum128_selected(
    t: &LaneTrans,
    width: LaneWidth,
    values: &[Datum],
    isnull: &[bool],
    rows: &[u64],
) -> (i64, i128) {
    debug_assert_eq!((t.addend, t.mulk, t.divk), (0, 1, 1), "bare-Var admission only");
    let mut c = 0i64;
    let mut s = 0i128;
    for_each_row(rows, |i| {
        if !isnull[i] {
            c += 1;
            s += lane_value(values, width, i) as i128;
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

/// One `{0,0,0}` float8[3] Youngs-Cramer transarray in `mcx`, header shaped
/// exactly as C's `construct_array_builtin(FLOAT8OID)` produces the aggregate
/// initcond `'{0,0,0}'` — the FAccum pergroup's NON-null initval (avg / var /
/// stddev over float4/float8). The consuming node must install this into
/// every FAccum pergroup before the first fold touches it (the drive's
/// initialize_aggregates copies the catalog byref initval; the grouped
/// install seeds new groups with THIS image). `faccum_advance` validates the
/// shape, so a mis-shaped install louds rather than corrupts.
pub fn new_float8_transarray(mcx: Mcx<'_>) -> Datum {
    // 24-byte header + 3×8 float8 = 48 bytes = 6 u64 words.
    let mut buf: PgVec<'_, u64> = ::mcx::vec_from_elem_in(mcx, 0u64, 6);
    let p = buf.as_mut_ptr().cast::<u8>();
    // SAFETY: 48 in-bounds bytes, 8-aligned; leaked into the mcx arena below.
    // write_float8_transarray fills the header + three 0.0 words exactly as
    // C's initcond image (identical to what a per-row float8_accum first
    // call reads).
    let n = unsafe {
        let sl = core::slice::from_raw_parts_mut(p, 48);
        write_float8_transarray(&[0.0, 0.0, 0.0], sl)
    };
    debug_assert_eq!(n, 48);
    let d = Datum::from_usize(p as usize);
    core::mem::forget(buf);
    d
}

/// One `{0,0,0,0,0,0}` float8[6] bivariate Youngs-Cramer transarray in `mcx`
/// — the FRegrAccum pergroup's NON-null initval (corr/covar/regr_* over two
/// float8 args), header shaped exactly as C's initcond image. Same install
/// discipline as [`new_float8_transarray`]; `fregr_advance` validates the
/// shape, so a mis-shaped install louds rather than corrupts.
pub fn new_float8_regr_transarray(mcx: Mcx<'_>) -> Datum {
    // 24-byte header + 6×8 float8 = 72 bytes = 9 u64 words.
    let mut buf: PgVec<'_, u64> = ::mcx::vec_from_elem_in(mcx, 0u64, 9);
    let p = buf.as_mut_ptr().cast::<u8>();
    // SAFETY: 72 in-bounds bytes, 8-aligned; leaked into the mcx arena below.
    let n = unsafe {
        let sl = core::slice::from_raw_parts_mut(p, 72);
        write_float8_transarray(&[0.0; 6], sl)
    };
    debug_assert_eq!(n, 72);
    let d = Datum::from_usize(p as usize);
    core::mem::forget(buf);
    d
}

// The staged lane value as the transfn's f64 input, per the classify-time
// conversion tag (C's exact scalar cast semantics — see FloatConv).
#[inline(always)]
fn conv_f64(conv: FloatConv, values: &[Datum], i: usize) -> f64 {
    match conv {
        FloatConv::None => values[i].as_f64(),
        FloatConv::I2 => values[i].as_i16() as f64,
        FloatConv::I4 => values[i].as_i32() as f64,
        FloatConv::I8 => values[i].as_i64() as f64,
        FloatConv::F4 => values[i].as_f32() as f64,
    }
}

// C numeric_avg_accum's makeNumericAggState arm (numeric.c, via
// agg_state_arg): get the group's aggcontext-lived NumericAggState,
// allocating it on the group's first transfn call — the transfn is NOT
// strict, so the caller invokes this once per (filter-passing) selected row,
// NULL inputs included, exactly as int128_state. calc_sum_x2 = false
// (numeric_avg_accum; the sum_x2-carrying numeric_accum stays a named
// refusal). `no_trans_value` deliberately untouched (the int128_state note).
#[inline]
fn numeric_state(pg: &mut AggPerGroup, aggcxt: Mcx<'_>) -> PgResult<*mut NumericAggState> {
    if !pg.trans_value_is_null {
        return Ok(pg.trans_value.as_usize() as *mut NumericAggState);
    }
    const { assert!(!core::mem::needs_drop::<NumericAggState>()) }
    let layout = core::alloc::Layout::new::<NumericAggState>();
    let raw =
        ::mcx::Allocator::allocate(&aggcxt, layout).map_err(|_| aggcxt.oom(layout.size()))?;
    let p = raw.cast::<NumericAggState>().as_ptr();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(NumericAggState::new(false)) };
    pg.trans_value = Datum::from_usize(p as usize);
    pg.trans_value_is_null = false;
    Ok(p)
}

// One C per-row numeric_avg_accum accumulate over a vguard-passed lane datum:
// C's DatumGetNumeric detoasts a 1B-short varlena into CurrentMemoryContext
// (aligning the digits), then do_numeric_accum reads the payload. The fold's
// detour is stack/heap SCRATCH — never the aggcontext, so the aggcontext
// allocation sequence (and with it hash-agg memory accounting and spill
// decisions) stays byte-identical to the per-row path, whose expand lands in
// the reset-per-row tuple context. 4B-uncompressed payloads are read in
// place (heap aligns them; the alignment check is belt-and-braces — a
// misaligned image takes the copy path instead of panicking).
//
// # Safety
// `d` is a live inline varlena (vguard-passed); `st` is the group's
// aggcontext-lived state with no other live reference; `aggcxt` is that
// aggcontext (C `state->agg_context`, the digit buffers' home).
#[inline]
unsafe fn num_accum_row(
    st: &mut NumericAggState,
    d: Datum,
    aggcxt: Mcx<'_>,
) -> PgResult<()> {
    use ::adt_numeric::aggregates::do_numeric_accum;
    // SAFETY: caller contract (inline varlena).
    let payload = unsafe { str_payload(d) };
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract (header byte readable).
    let short = unsafe { ::types_tuple::varatt::varatt_is_1b(p) };
    if short || payload.as_ptr() as usize & 1 != 0 {
        // A 1B-short varlena's total size is <= 127 bytes, so the payload
        // fits the 126-byte stack scratch; the defensive misaligned-4B arm
        // may exceed it and takes a heap scratch instead.
        if payload.len() <= 126 {
            let mut buf = [0u16; 63];
            // SAFETY: 126 writable bytes, 2-aligned by construction.
            let dst = unsafe {
                core::slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<u8>(), 126)
            };
            dst[..payload.len()].copy_from_slice(payload);
            let num = ::adt_numeric::Num::from_payload(&dst[..payload.len()]);
            return do_numeric_accum(st, aggcxt, num);
        }
        let mut buf: Vec<u16> = vec![0; payload.len().div_ceil(2)];
        // SAFETY: buf covers payload.len() bytes, 2-aligned.
        let dst = unsafe {
            core::slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<u8>(), payload.len())
        };
        dst.copy_from_slice(payload);
        let num = ::adt_numeric::Num::from_payload(&dst[..payload.len()]);
        return do_numeric_accum(st, aggcxt, num);
    }
    let num = ::adt_numeric::Num::from_payload(payload);
    do_numeric_accum(st, aggcxt, num)
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

/// Granule-metadata fold admissibility (v7 pgrcolumnar footer length stats):
/// `Some(cols)` iff EVERY admitted transition is answerable from the pair
/// (passing row count, per-column Σ octet_length over passing rows) —
/// CountStar/CountAny (equal on pgrcolumnar, which stores no NULLs) and PLAIN
/// Sum/AvgAccum over VarLenBytes lanes (byte lengths are what the footer
/// sums carry; VarLenChars refuses). `cols` = the deduped length columns
/// whose footer sums the fold consumes. Integer-guarded plans refuse
/// (an integer Guard implies an OpExpr transform lane this fold cannot
/// host); vguards/uguards are staging proofs over lane READS and do not
/// apply — the metadata fold reads no lane.
pub fn granule_meta_len_cols(plan: &LanePlan<'_>) -> Option<Vec<u16>> {
    if !plan.resid.is_empty() || !plan.guards.is_empty() || !plan.filters.is_empty() {
        return None;
    }
    let mut cols: Vec<u16> = Vec::new();
    for t in plan.trans.iter() {
        match t.kind {
            LaneKind::CountStar | LaneKind::CountAny => {}
            LaneKind::Sum | LaneKind::AvgAccum
                if t.width == LaneWidth::VarLenBytes
                    && (t.addend, t.mulk, t.divk) == (0, 1, 1) =>
            {
                if !cols.contains(&t.col) {
                    cols.push(t.col);
                }
            }
            _ => return None,
        }
    }
    Some(cols)
}

/// Apply the plan's transitions for a metadata-answered granule: `passing`
/// selected rows (none NULL — pgrcolumnar), `sum_of(col)` = Σ octet_length over
/// exactly those rows (footer arithmetic). Bit-equal to `fold_batch` over
/// the same selection: count_apply/sum_apply/avg_apply are the identical
/// state mutations, integer sums are order-free (mod-2^64 ring), and the
/// CSE schedule is only a compute-sharing plan — per-transition application
/// yields the same increments. Caller proved the plan via
/// `granule_meta_len_cols` and `passing > 0` (zero passing rows apply
/// nothing, exactly as the per-row program would).
///
/// # Safety
/// `pergroup_base` follows fold_batch's contract (once-allocated pergroup
/// array covering every transno; AvgAccum pergroups hold a live
/// `new_int8_transarray`-shaped transvalue).
pub unsafe fn fold_granule_meta(
    plan: &LanePlan<'_>,
    passing: i64,
    sum_of: impl Fn(u16) -> i64,
    pergroup_base: NonNull<AggPerGroup>,
) {
    debug_assert!(passing > 0);
    for t in plan.trans.iter() {
        // SAFETY: transno < pergroup length (caller contract).
        let pg = unsafe { &mut *pergroup_base.as_ptr().add(t.transno as usize) };
        match t.kind {
            LaneKind::CountStar | LaneKind::CountAny => count_apply(pg, passing),
            LaneKind::Sum => sum_apply(pg, sum_of(t.col)),
            LaneKind::AvgAccum => avg_apply(pg, passing, sum_of(t.col)),
            _ => unreachable!("granule_meta_len_cols admitted the plan"),
        }
    }
}

/// Column requests for the whole-RG / whole-granule footer-metadata fold
/// (`agg_meta_cols` / `fold_agg_meta` — the footer-stat consumption arm of
/// the PLAIN fold drive): `mm_cols` want exact footer (min, max) entries
/// (Min/Max transitions plus every integer-guard column for the caller's
/// interval re-proof), `sum_cols` want exact i128 footer sums (v4 RG
/// sums — RG altitude only; the format stores no granule sums), `len_cols`
/// want Σ octet_length (v7 granule length stats, foldable to RG altitude).
pub struct AggMetaCols {
    pub mm_cols: Vec<u16>,
    pub sum_cols: Vec<u16>,
    pub len_cols: Vec<u16>,
}

/// Footer-metadata fold admissibility for an ALL-ROWS-PASSING scan unit (a
/// wholly visible row group / granule every one of whose rows passes the
/// scan qual — the caller proves that from the zone maps): `Some` iff there
/// are no residual transitions and EVERY admitted transition is answerable
/// from (row count, exact footer (min, max), exact footer sums,
/// Σ octet_length):
///   * CountStar/CountAny — the unit's row count (pgrcolumnar stores no NULLs,
///     so the non-null count IS the row count, any column type);
///   * Min/Max over PLAIN int lanes — the footer extremes are attained
///     values of the unit's rows (identity transform only: min(v*3) is not
///     derivable from min(v), the classify_meta rule);
///   * Sum/AvgAccum over affine divk==1 int lanes — delta = mulk*S +
///     addend*N in the wrapping-i64 ring (the SumBase/cse_delta derivation
///     with the footer sum standing in for the batch sum: S ≡ Σv mod 2^64);
///   * Int128AvgAccum over bare int8 lanes — the i128 footer sum is the
///     exact Σv (see sum128_selected's reassociation proof; the footer
///     accumulation has the same non-overflow envelope);
///   * Sum/AvgAccum over PLAIN VarLenBytes lanes — Σ octet_length is
///     exactly the v7 length-stats sum (VarLenChars refuses: no char-count
///     stats exist).
/// Integer guards admit — `mm_cols` carries their columns and the CALLER
/// must prove every guard interval against the unit's footer (min, max)
/// (all rows fold, so the footer extremes are exactly check_guards' value
/// domain) or decline the unit. vguards are per-batch staging proofs over
/// lane READS — the metadata fold reads no lanes, so they are vacuous;
/// uguards only arise on VarLenChars lanes, which refuse above. The
/// float/bool/bitwise/str tiers refuse exactly as classify_meta (no footer
/// answer exists).
pub fn agg_meta_cols(plan: &LanePlan<'_>) -> Option<AggMetaCols> {
    // FILTER'd plans are never footer-answerable (the footer aggregates
    // every visible row; a filter selects a per-row subset).
    if !plan.resid.is_empty() || !plan.filters.is_empty() {
        return None;
    }
    let mut out =
        AggMetaCols { mm_cols: Vec::new(), sum_cols: Vec::new(), len_cols: Vec::new() };
    let push = |v: &mut Vec<u16>, c: u16| {
        if !v.contains(&c) {
            v.push(c);
        }
    };
    for t in plan.trans.iter() {
        let plain = (t.addend, t.mulk, t.divk) == (0, 1, 1);
        let affine = t.divk == 1;
        let int_width = matches!(t.width, LaneWidth::I16 | LaneWidth::I32 | LaneWidth::I64);
        match t.kind {
            LaneKind::CountStar | LaneKind::CountAny => {}
            LaneKind::Min | LaneKind::Max if plain && int_width => {
                push(&mut out.mm_cols, t.col)
            }
            LaneKind::Sum | LaneKind::AvgAccum if affine && int_width => {
                push(&mut out.sum_cols, t.col)
            }
            LaneKind::Sum | LaneKind::AvgAccum
                if plain && t.width == LaneWidth::VarLenBytes =>
            {
                push(&mut out.len_cols, t.col)
            }
            LaneKind::Int128AvgAccum if plain && int_width => {
                push(&mut out.sum_cols, t.col)
            }
            _ => return None,
        }
    }
    for g in plan.guards.iter() {
        push(&mut out.mm_cols, g.col);
    }
    Some(out)
}

/// Apply the plan's transitions for a footer-metadata-answered scan unit
/// (whole RG or whole granule) of `rows` (> 0) all-passing rows: `mm_of` =
/// the unit's exact attained (min, max) per requested int column, `sum_of` =
/// the unit's exact i128 value sum per requested int column, `len_of` =
/// Σ octet_length per requested VarLenBytes column. Bit-equal to
/// `fold_batch` over an all-ones selection of the unit's rows:
/// count/sum/avg/minmax/int128 applies are the identical state mutations,
/// the sum deltas live in the same mod-2^64 ring (`meta_delta` ==
/// `cse_delta` with the footer sum for the batch sum), the i128 sum is the
/// exact per-row accumulation, and the min/max advance sees the attained
/// extreme instead of every row (same survivor under a strict total order).
/// The CSE schedule is only a compute-sharing plan — per-transition
/// application yields the same increments (the fold_granule_meta precedent).
/// Caller proved the plan via `agg_meta_cols` and, when guarded, proved
/// every guard interval against the unit's (min, max).
///
/// # Safety
/// `pergroup_base` follows fold_batch's contract (once-allocated pergroup
/// array covering every transno; AvgAccum pergroups hold a live
/// `new_int8_transarray`-shaped transvalue; Int128AvgAccum pergroups are
/// NULL or hold a live aggcontext `Int128AggState` pointer, with `aggcxt`
/// that same aggcontext).
pub unsafe fn fold_agg_meta(
    plan: &LanePlan<'_>,
    rows: i64,
    mm_of: impl Fn(u16) -> (i64, i64),
    sum_of: impl Fn(u16) -> i128,
    len_of: impl Fn(u16) -> i64,
    pergroup_base: NonNull<AggPerGroup>,
    aggcxt: Mcx<'_>,
) -> PgResult<()> {
    debug_assert!(rows > 0);
    for t in plan.trans.iter() {
        // SAFETY: transno < pergroup length (caller contract).
        let pg = unsafe { &mut *pergroup_base.as_ptr().add(t.transno as usize) };
        match t.kind {
            LaneKind::CountStar | LaneKind::CountAny => count_apply(pg, rows),
            LaneKind::Sum if t.width == LaneWidth::VarLenBytes => {
                sum_apply(pg, len_of(t.col))
            }
            LaneKind::AvgAccum if t.width == LaneWidth::VarLenBytes => {
                avg_apply(pg, rows, len_of(t.col))
            }
            LaneKind::Sum => sum_apply(pg, meta_delta(t, rows, sum_of(t.col))),
            LaneKind::AvgAccum => avg_apply(pg, rows, meta_delta(t, rows, sum_of(t.col))),
            LaneKind::Int128AvgAccum => {
                let st = int128_state(pg, aggcxt)?;
                // SAFETY: aggcontext-lived state installed by int128_state or
                // the per-row transfn chain (caller contract).
                unsafe {
                    (*st).n += rows;
                    (*st).sum_x += sum_of(t.col);
                }
            }
            LaneKind::Min => minmax_advance(t, pg, mm_of(t.col).0, false),
            LaneKind::Max => minmax_advance(t, pg, mm_of(t.col).1, true),
            _ => unreachable!("agg_meta_cols admitted the plan"),
        }
    }
    Ok(())
}

// The Sum/AvgAccum member delta off a unit's exact footer sum: mulk*S +
// addend*N in the wrapping-i64 ring — `cse_delta(t, rows, S mod 2^64)`
// verbatim (divk == 1 by admission, so the shared base sum IS Σv).
#[inline(always)]
fn meta_delta(t: &LaneTrans, rows: i64, s: i128) -> i64 {
    cse_delta(t, rows, s as i64)
}

/// Whole-batch ungrouped fold: apply every admitted transition over the
/// selected rows of the staged batch, CSE groups first, then the ungrouped
/// per-trans kernels. `aggcxt` is the agg (transvalue) memory context — where
/// C's ExecAggCopyTransValue copies by-ref transvalues; only the str kinds
/// allocate (their datumCopy on a strict install/replace). Fallible arms:
/// the str kinds (OOM) and the fold-trans float kinds (FSum/FAccum raise C's
/// exact overflow ereport at C's row — see try_for_each_row); every other
/// kind never sees the Err path.
///
/// # Safety
/// `pergroup_base` is the node's once-allocated pergroup array covering every
/// transno in the plan; rows selected by `rows` carry valid lane values in
/// `cols` for every plan column (`rows` has one bit per staged row,
/// `nrows <= rows.len() * 64`); AvgAccum pergroups hold a live
/// `new_int8_transarray`-shaped transvalue; FAccum pergroups hold a live
/// aggcontext float8[3] transarray (`new_float8_transarray` / the drive's
/// byref '{0,0,0}' initval copy); Int128AvgAccum pergroups are
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
                        base_sum(read_width(cols, l.col, l.width), l.divk as i64, values, isnull, rows)
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
                let w0 = read_width(cols, t0.col, t0.width);
                let mut m: Option<i64> = None;
                let want_max = t0.kind == LaneKind::Max;
                for_each_row(rows, |i| {
                    if !isnull[i] {
                        let v = xform(t0, lane_value(values, w0, i));
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
    // Per-transition FILTER masks (fold-trans tier): std-heap scratch (never
    // the agg context — accounting must not move), rebuilt only when the
    // filter index changes so consecutive transitions sharing one predicate
    // reuse the mask. "Mask first, then fold": every arm below runs over
    // `trows` (= `rows` unfiltered), which is bit-equal to C's per-row
    // filter-then-transition sequence because the admitted predicates are
    // pure per-row reads.
    let mut fmask: Vec<u64> = Vec::new();
    let mut fmask_for: u32 = u32::MAX;
    for (ti, t) in plan.trans.iter().enumerate() {
        if plan.cse_skip[ti] {
            continue;
        }
        // SAFETY: transno < pergroup length (caller contract).
        let pg = unsafe { &mut *pergroup_base.as_ptr().add(t.transno as usize) };
        let (trows, tnsel): (&[u64], u32) = if t.filter == NO_FILTER {
            (rows, nsel)
        } else {
            if fmask_for != t.filter as u32 {
                build_filter_mask(&plan.filters[t.filter as usize], cols, rows, &mut fmask);
                fmask_for = t.filter as u32;
            }
            let cnt: u32 = fmask.iter().map(|w| w.count_ones()).sum();
            (&fmask, cnt)
        };
        if t.kind == LaneKind::CountStar {
            count_apply(pg, tnsel as i64);
            continue;
        }
        let (values, isnull) =
            (cols.col_values(t.col as usize), cols.col_isnull(t.col as usize));
        let w = read_width(cols, t.col, t.width);
        debug_assert!(values.len() >= nrows && isnull.len() >= nrows);
        match t.kind {
            LaneKind::CountStar => unreachable!(),
            LaneKind::CountAny => {
                let mut c = 0i64;
                for_each_row(trows, |i| {
                    c += !isnull[i] as i64;
                });
                count_apply(pg, c);
            }
            LaneKind::Sum => {
                let (c, s) = sum_selected(t, w, values, isnull, trows);
                if c > 0 {
                    sum_apply(pg, s);
                }
            }
            LaneKind::AvgAccum => {
                let (c, s) = sum_selected(t, w, values, isnull, trows);
                if c > 0 {
                    avg_apply(pg, c, s);
                }
            }
            LaneKind::Int128AvgAccum => {
                // C calls the non-strict transfn once per SELECTED (and
                // filter-passing) row — NULL inputs included — so any such
                // row allocates the state; only the non-null inputs
                // accumulate (see sum128_selected for the reassociation
                // proof).
                let (c, s) = sum128_selected(t, w, values, isnull, trows);
                if tnsel > 0 {
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
            // Same non-strict state-existence discipline for the numeric
            // family, but the accumulation itself is C's exact per-row
            // do_numeric_accum in row order (value-order-insensitive exact
            // arithmetic — the row walk keeps even the error positions and
            // digit-buffer growth sequence C's).
            LaneKind::NumAccum => {
                if tnsel > 0 {
                    let st = numeric_state(pg, aggcxt)?;
                    try_for_each_row(trows, |i| {
                        if !isnull[i] {
                            // SAFETY: vguard-passed inline varlena; state
                            // aggcontext-lived, sole reference here.
                            unsafe { num_accum_row(&mut *st, values[i], aggcxt) }
                        } else {
                            Ok(())
                        }
                    })?;
                }
            }
            // Strict two-arg bivariate accum: a row participates iff BOTH
            // inputs are non-null; row-order walk (order-sensitive).
            LaneKind::FRegrAccum => {
                let (values2, isnull2) =
                    (cols.col_values(t.col2 as usize), cols.col_isnull(t.col2 as usize));
                try_for_each_row(trows, |i| {
                    if !isnull[i] && !isnull2[i] {
                        let y = conv_f64(t.fconv, values, i);
                        let x = conv_f64(t.fconv2, values2, i);
                        // SAFETY: FRegr pergroups hold a live aggcontext
                        // float8[6] transarray (caller contract).
                        unsafe { fregr_advance(pg, y, x) }
                    } else {
                        Ok(())
                    }
                })?;
            }
            // Strict two-arg counter (regr_count): both-non-null rows count.
            LaneKind::Count2 => {
                let isnull2 = cols.col_isnull(t.col2 as usize);
                let mut c = 0i64;
                for_each_row(trows, |i| {
                    c += (!isnull[i] && !isnull2[i]) as i64;
                });
                count_apply(pg, c);
            }
            LaneKind::Min | LaneKind::Max => {
                let mut m: Option<i64> = None;
                let want_max = t.kind == LaneKind::Max;
                for_each_row(trows, |i| {
                    if !isnull[i] {
                        let v = xform(t, lane_value(values, w, i));
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
                for_each_row(trows, |i| {
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
                for_each_row(trows, |i| {
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
                for_each_row(trows, |i| {
                    if !isnull[i] {
                        let v = xform(t, lane_value(values, w, i));
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
                // Dict-code fast arm (lane-v2-dictminmax): under a SORTED
                // dict view (LaneCols::col_codes contract) the batch winner
                // is the min/max CODE among selected non-null rows — an
                // integer scan, no payload memcmp. The advanced datum is the
                // winning row's values cell, which the contract pins to
                // `dict[code]`: equal codes are the SAME pointer, so the
                // picked datum is bit-identical to the memcmp pre-fold's
                // last-tied winner (dedup makes ties within an epoch
                // impossible across DIFFERENT datums). Str kinds only —
                // bpchar's trailing-blank trim breaks the code order.
                let code_lane = match t.kind {
                    LaneKind::StrMin | LaneKind::StrMax => {
                        cols.col_codes(t.col as usize).filter(|l| l.table.sorted)
                    }
                    _ => None,
                };
                if let Some(lane) = code_lane {
                    let want_max = t.kind == LaneKind::StrMax;
                    let mut best: Option<(u32, usize)> = None;
                    for_each_row(trows, |i| {
                        if !isnull[i] {
                            let c = lane.code(i);
                            best = Some(match best {
                                None => (c, i),
                                Some((b, bi)) => {
                                    if if want_max { c > b } else { c < b } {
                                        (c, i)
                                    } else {
                                        (b, bi)
                                    }
                                }
                            });
                        }
                    });
                    if let Some((_, i)) = best {
                        // SAFETY: col_codes contract — values[i] is the
                        // inline varlena dict datum; live pergroup + aggcxt.
                        unsafe { str_advance(t, pg, values[i], aggcxt, None)? };
                    }
                    continue;
                }
                let mut m: Option<Datum> = None;
                for_each_row(trows, |i| {
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
                    unsafe { str_advance(t, pg, d, aggcxt, None)? };
                }
            }
            // Fold-trans tier: ORDER-PRESERVING sequential float folds. No
            // batch pre-fold, no reassociation — try_for_each_row walks the
            // selected rows in ascending row order and every advance applies
            // C's exact per-row arithmetic against the running transvalue,
            // so the state bits (and an overflow error's row position) match
            // the per-row program.
            LaneKind::FSum => {
                try_for_each_row(trows, |i| {
                    if !isnull[i] {
                        let d = if t.fconv == FloatConv::None {
                            values[i]
                        } else {
                            Datum::from_f64(conv_f64(t.fconv, values, i))
                        };
                        fsum_advance(t, pg, d)
                    } else {
                        Ok(())
                    }
                })?;
            }
            LaneKind::FAccum => {
                try_for_each_row(trows, |i| {
                    if !isnull[i] {
                        // SAFETY: FAccum pergroups hold a live aggcontext
                        // float8[3] transarray (caller contract); sole
                        // reference during the fold.
                        unsafe { faccum_advance(pg, conv_f64(t.fconv, values, i)) }
                    } else {
                        Ok(())
                    }
                })?;
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
        // Length-staged columns carry i64 lengths, not datum pointers: the
        // inline-form proof is vacuous (nothing dereferences a datum) and
        // running it on integer bit patterns would be UB.
        if cols.col_len_staged(c as usize) {
            continue;
        }
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
        // Length-staged columns: the fill computed C's exact answer (mb-walk
        // parity) per value — no countability proof needed, and the lane
        // holds i64s, not payloads.
        if cols.col_len_staged(c as usize) {
            continue;
        }
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

// ORDER-PRESERVING float SUM advance (float4pl/float8pl). Strict, NULL init:
// the first non-null selected row STORES its input datum's bits raw (C's
// strict-transfn null-state special case — advance_transition_function skips
// the transfn on the first call), every later row does the checked float_pl
// which raises C's overflow-to-infinity error. NOT commutative — the caller
// walks selected rows in row order and calls this one at a time, so the
// running state's bits equal C's per-row `state = float_pl(state, v)`
// sequence. `t.res_width` selects f32 (float4) vs f64 (float8) arithmetic.
#[inline]
fn fsum_advance(t: &LaneTrans, pg: &mut AggPerGroup, d: Datum) -> PgResult<()> {
    if pg.no_trans_value {
        // First non-null input: store the argument datum's exact bits (C
        // stores the input, never a recomputed value — load-bearing for
        // -0.0 and the exact float4/float8 bit pattern).
        pg.trans_value = d;
        pg.trans_value_is_null = false;
        pg.no_trans_value = false;
    } else if !pg.trans_value_is_null {
        match t.res_width {
            LaneWidth::F32 => {
                let s = float4_pl(pg.trans_value.as_f32(), d.as_f32())?;
                pg.trans_value = Datum::from_f32(s);
            }
            LaneWidth::F64 => {
                let s = float8_pl(pg.trans_value.as_f64(), d.as_f64())?;
                pg.trans_value = Datum::from_f64(s);
            }
            _ => unreachable!("FSum res_width is F32 or F64"),
        }
    }
    Ok(())
}

// ORDER-PRESERVING float AVG/VAR/STDDEV advance (float4_accum/float8_accum).
// Strict with the non-null '{0,0,0}' float8[3] initval: the transvalue is a
// live Youngs-Cramer transarray from the first row (never no_trans_value),
// so this reads [n,sx,sxx] in place, applies C's accum kernel for ONE
// non-null input in row order, and writes the three words back. `x` is the
// transfn's f64 input, already widened/converted by the caller's
// `conv_f64(t.fconv, ..)` — C's float4_accum widens f32→f64 before the same
// kernel, so routing every width through float8_accum is bit-identical.
// Order-sensitive (the sxx term depends on the running mean) — the caller
// walks row order, no batch reassociation. The accum kernel raises C's
// overflow error at C's row.
//
// # Safety
// `pg.trans_value` is a live aggcontext float8[3] transarray
// (`new_float8_transarray` / the drive's byref initval copy); sole reference
// during the call.
#[inline]
unsafe fn faccum_advance(pg: &mut AggPerGroup, x: f64) -> PgResult<()> {
    debug_assert!(!pg.trans_value_is_null, "FAccum transarray is never NULL");
    let arr = pg.trans_value.as_usize() as *mut u8;
    // SAFETY: aggcontext-lived transarray; validate the shape (as avg_apply)
    // before reading/writing the three data words.
    let trans = unsafe {
        let image = core::slice::from_raw_parts(arr, ::types_tuple::varatt::varsize_4b(arr));
        check_float8_array::<3>(image, "float8_accum")?
    };
    let out = float8_accum(trans, x)?;
    // SAFETY: the agg frame owns this transarray as a mutable image (the
    // per-row float8_accum writes the same three words in place); shape
    // verified above.
    unsafe {
        let data = arr.add(FLOAT8_ARRAY_HDRSZ);
        for (k, v) in out.iter().enumerate() {
            data.add(8 * k).cast::<[u8; 8]>().write(v.to_ne_bytes());
        }
    }
    Ok(())
}

// ORDER-PRESERVING bivariate advance (float8_regr_accum): reads the live
// float8[6] transarray in place, applies C's exact bivariate Youngs-Cramer
// update for ONE (y, x) input pair in row order (fp-contract/mul_add parity
// lives in adt_float's kernel), writes the six words back. The caller
// enforces C's two-arg strictness (both inputs non-null) and the row-order
// walk; the kernel raises C's overflow ereport at C's row.
//
// # Safety
// `pg.trans_value` is a live aggcontext float8[6] transarray
// (`new_float8_regr_transarray` / the drive's byref '{0,0,0,0,0,0}' initval
// copy); sole reference during the call.
#[inline]
unsafe fn fregr_advance(pg: &mut AggPerGroup, y: f64, x: f64) -> PgResult<()> {
    debug_assert!(!pg.trans_value_is_null, "FRegr transarray is never NULL");
    let arr = pg.trans_value.as_usize() as *mut u8;
    // SAFETY: aggcontext-lived transarray; shape-validated before the write.
    let trans = unsafe {
        let image = core::slice::from_raw_parts(arr, ::types_tuple::varatt::varsize_4b(arr));
        check_float8_array::<6>(image, "float8_regr_accum")?
    };
    let out = float8_regr_accum(trans, y, x)?;
    // SAFETY: as faccum_advance (six words, shape verified).
    unsafe {
        let data = arr.add(FLOAT8_ARRAY_HDRSZ);
        for (k, v) in out.iter().enumerate() {
            data.add(8 * k).cast::<[u8; 8]>().write(v.to_ne_bytes());
        }
    }
    Ok(())
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

// ---------------------------------------------------------------------------
// Table-owned by-ref str transvalue store (GL-DICTDRAIN-3).
// ---------------------------------------------------------------------------

/// Table-owned store for the by-ref str MIN/MAX transvalues of a MIGRATING
/// sink table (GL-DICTDRAIN-3). The dict-coded sink drain's Local-owned
/// table is LENT to whichever pool thread serves each morsel, so a memory-
/// CONTEXT home for its text transvalues — per (thread, query) by
/// construction — breaks the replace-free's allocator-exactness: a copy
/// allocated by thread A's context and replace-freed on thread B enters
/// B's freelist while A's context still owns the chunk, so ONE chunk gets
/// handed out twice and a LIVE pergroup is left holding a freed pointer
/// (the t45 'aggregation sink shape violation' revert — the 7th
/// byref/representation incident; the violated invariant was
/// SinkTableHandle's own Send proof: "every state block is byval-POD").
///
/// This store travels WITH the table (inside `CompactHash`, moved by the
/// existing lend/reclaim), so every alloc AND free hits the same allocator
/// no matter which thread runs the morsel — C's one-context-per-worker-
/// table invariant, restored for a migrating table. The free-on-replace
/// keeps the GL-DICTDRAIN-2 churn bound (superseded copies are reclaimed,
/// never accumulated), and Drop releases every slab on every path — abort
/// included — so nothing leaks.
///
/// Concurrency: `Send` by declaration; soundness rides the drive's
/// discipline — mutation only inside a morsel drain (the Local is `&mut`
/// per claim, one thread at a time), and the combine/emit phase only READS
/// value bytes through state-block pointers (never touching the store's
/// own structure).
pub struct StrStateArena {
    /// pow2 size-class freelists (16..=1024 bytes, classes 0..=6): parked
    /// chunk addresses, LIFO. The chunk's bytes are dead while parked.
    free: [Vec<usize>; 7],
    /// Slabs backing the classed chunks (u64-backed for varlena alignment).
    slabs: Vec<Box<[u64]>>,
    /// Bump cursor into the LAST slab, in u64 words.
    cursor: usize,
    /// Oversize (> 1024 B) values: exact per-value boxes, keyed by address,
    /// dropped exactly on replace.
    big: std::collections::HashMap<usize, Box<[u64]>>,
    /// Retained bytes (slabs + big) — the drain's byref budget term.
    bytes: usize,
}

// SAFETY: owned heap storage only; see the struct doc's concurrency
// contract (mutation is &mut-serialized by the lend/reclaim discipline,
// cross-phase access is read-only through state-block pointers).
unsafe impl Send for StrStateArena {}

const STR_ARENA_SLAB_WORDS: usize = 8192; // 64 KB slabs.

impl Default for StrStateArena {
    fn default() -> Self {
        StrStateArena {
            free: Default::default(),
            slabs: Vec::new(),
            cursor: 0,
            big: Default::default(),
            bytes: 0,
        }
    }
}

impl StrStateArena {
    /// Size class for `size` bytes: pow2 16..=1024 → 0..=6; None = big.
    #[inline]
    fn class_of(size: usize) -> Option<usize> {
        if size > 1024 {
            return None;
        }
        let c = size.max(1).next_power_of_two().max(16);
        Some(c.trailing_zeros() as usize - 4)
    }

    /// Allocate one chunk of `size` bytes (8-aligned).
    fn alloc(&mut self, size: usize) -> *mut u8 {
        match Self::class_of(size) {
            Some(cl) => {
                if let Some(p) = self.free[cl].pop() {
                    return p as *mut u8;
                }
                let words = (16usize << cl) / 8;
                if self.slabs.is_empty()
                    || self.cursor + words > self.slabs.last().expect("checked").len()
                {
                    self.slabs.push(vec![0u64; STR_ARENA_SLAB_WORDS].into_boxed_slice());
                    self.cursor = 0;
                    self.bytes += STR_ARENA_SLAB_WORDS * 8;
                }
                let slab = self.slabs.last_mut().expect("just ensured");
                let p = slab[self.cursor..].as_mut_ptr() as *mut u8;
                self.cursor += words;
                p
            }
            None => {
                let words = size.div_ceil(8);
                let b = vec![0u64; words].into_boxed_slice();
                let p = b.as_ptr() as usize;
                self.bytes += words * 8;
                self.big.insert(p, b);
                p as *mut u8
            }
        }
    }

    /// Park one chunk (the replace-free). `size` MUST be the value's
    /// VARSIZE_ANY — the same number `copy` allocated by (allocator-exact
    /// by construction: same store, same class function).
    fn dealloc(&mut self, p: usize, size: usize) {
        match Self::class_of(size) {
            Some(cl) => self.free[cl].push(p),
            None => {
                let b = self.big.remove(&p);
                debug_assert!(b.is_some(), "big-value free of an unowned pointer");
                if let Some(b) = b {
                    self.bytes -= b.len() * 8;
                }
            }
        }
    }

    /// datumCopy of a plain varlena image into the store (VARSIZE_ANY
    /// bytes, 8-aligned — `agg_datum_copy`'s exact copy semantics).
    ///
    /// # Safety
    /// `d` is a non-null plain varlena datum readable for its full size.
    pub unsafe fn copy(&mut self, d: Datum) -> Datum {
        let p = d.as_usize() as *const u8;
        // SAFETY: caller contract.
        let size = unsafe { ::types_tuple::varatt::varsize_any(p) };
        let dst = self.alloc(size);
        // SAFETY: fresh chunk of >= size bytes; source readable.
        unsafe { core::ptr::copy_nonoverlapping(p, dst, size) };
        Datum::from_usize(dst as usize)
    }

    /// [`Self::copy`] of `d` REPLACING a stored transvalue: copy first,
    /// then park the superseded copy (C's ExecAggCopyTransValue pfree).
    ///
    /// # Safety
    /// As `copy`; `old` is a live value THIS store allocated.
    pub unsafe fn replace(&mut self, old: Datum, d: Datum) -> Datum {
        let new = unsafe { self.copy(d) };
        let op = old.as_usize();
        if op != 0 {
            // SAFETY: `old` is a live plain varlena this store allocated.
            let size = unsafe { ::types_tuple::varatt::varsize_any(op as *const u8) };
            self.dealloc(op, size);
        }
        new
    }

    /// Whether `p` is a chunk address THIS store handed out — the
    /// allocator-exactness precondition of [`Self::replace`], queryable so
    /// callers can assert their store-ownership invariant instead of
    /// asserting it in prose. O(slabs); debug/test use only.
    pub fn owns(&self, p: usize) -> bool {
        self.big.contains_key(&p)
            || self.slabs.iter().any(|s| {
                let base = s.as_ptr() as usize;
                p >= base && p < base + s.len() * 8
            })
    }

    /// Retained bytes (the drain's byref budget accounting term).
    pub fn bytes(&self) -> usize {
        self.bytes
            + self.free.iter().map(|f| f.capacity() * 8).sum::<usize>()
            + self.slabs.capacity() * core::mem::size_of::<Box<[u64]>>()
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
// The `sa` store, when armed (the dict-coded sink drain's migrating
// tables), REPLACES the context as the copy destination — same bytes, same
// copy/replace points, allocator-exact frees that survive thread migration
// (StrStateArena doc). `None` = the context discipline verbatim (classic
// builds byte-identical, allocation sequence included).
//
// # Safety
// As `str_keep`; `aggcxt` is the live agg context; `pg` is the transition's
// live pergroup cell; a `Some(sa)` owns every prior copy stored in `pg`.
#[inline(always)]
unsafe fn str_advance(
    t: &LaneTrans,
    pg: &mut AggPerGroup,
    d: Datum,
    aggcxt: Mcx<'_>,
    sa: Option<&mut StrStateArena>,
) -> PgResult<()> {
    // SAFETY: forwarded caller contract (inline varlena datum, live aggcxt).
    unsafe {
        if pg.no_trans_value {
            pg.trans_value = match sa {
                Some(a) => a.copy(d),
                None => ::execexpr::agg_datum_copy(aggcxt, d, -1)?,
            };
            pg.trans_value_is_null = false;
            pg.no_trans_value = false;
        } else if !pg.trans_value_is_null && !str_keep(t.kind, pg.trans_value, d) {
            // Replace = copy + free of the superseded copy (C's
            // ExecAggCopyTransValue discipline; the context arm's free is a
            // no-op on bump contexts, so classic builds keep the exact
            // allocation sequence).
            pg.trans_value = match sa {
                Some(a) => a.replace(pg.trans_value, d),
                None => ::execexpr::agg_datum_replace(aggcxt, pg.trans_value, d, -1)?,
            };
        }
    }
    Ok(())
}

::mcx::forget_safe_nodrop!(LaneTrans, CseGroup, GuardEntry, FilterEntry);
// SAFETY census: every field is an arena PgVec of no-drop elements (or bool).
::mcx::forget_safe_struct!(
    LanePlan<'_> { trans, cse, cse_members, cse_skip, guards, vguards, uguards, filters, cols, resid, guarded },
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

// Fallible row walk for the ORDER-SENSITIVE fold-trans kernels: same
// ascending row order as `for_each_row`, short-circuiting on the first Err —
// which is exactly C's raise-at-row discipline (no row after the raising one
// ever accumulates).
#[inline(always)]
fn try_for_each_row(rows: &[u64], mut f: impl FnMut(usize) -> PgResult<()>) -> PgResult<()> {
    for (w, &word) in rows.iter().enumerate() {
        let mut bits = word;
        while bits != 0 {
            f(w * 64 + bits.trailing_zeros() as usize)?;
            bits &= bits - 1;
        }
    }
    Ok(())
}

/// Per-(group, transition) memo for the grouped str MIN/MAX dict-code path:
/// keyed by the pergroup CELL address (base + transno — unique per group per
/// transition, stable for the life of one build feed). `gen` is the
/// validity generation: the feed MUST call `invalidate()` whenever any row
/// of the build advanced an admitted str transition OUTSIDE
/// `fold_rows_grouped_mm` (a guard demote, a fallback row, an arrival-probe
/// accept, a dicteval demote — anything running the per-row transition
/// program), because the memo mirrors the transvalue and a bypassed advance
/// silently desynchronizes it. Entries from older generations read as
/// absent. Plain std heap (never the agg context): scratch memory must not
/// perturb hash-agg memory accounting.
pub struct StrMmScratch {
    map: std::collections::HashMap<usize, MmMemo, core::hash::BuildHasherDefault<PtrHash>>,
    gen: u32,
}

#[derive(Clone, Copy)]
struct MmMemo {
    gen: u32,
    /// Dictionary identity the memo's `code` lives in.
    epoch: u64,
    /// Best (min for StrMin / max for StrMax) code this path advanced for
    /// the group in `epoch`.
    code: u32,
    /// True: the transvalue is an aggcxt copy of `dict[code]` (byte-equal
    /// payload). False: the transvalue STRICTLY beats `dict[code]` under the
    /// kind's order (a previous-epoch winner that survived this epoch's
    /// codes so far).
    tv_code: bool,
}

/// Multiply-shift pointer hash (Fibonacci constant): deterministic, one
/// multiply — the per-row memo probe must not pay SipHash.
#[derive(Default)]
pub struct PtrHash(u64);

impl core::hash::Hasher for PtrHash {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }

    #[inline(always)]
    fn write_usize(&mut self, i: usize) {
        self.0 = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 ^= self.0 >> 29;
    }
}

impl Default for StrMmScratch {
    fn default() -> Self {
        StrMmScratch { map: Default::default(), gen: 0 }
    }
}

impl StrMmScratch {
    /// Drop every memo (O(1) generation bump). See the struct doc for the
    /// feed's invalidation obligations.
    pub fn invalidate(&mut self) {
        self.gen = match self.gen.checked_add(1) {
            Some(g) => g,
            None => {
                self.map.clear();
                0
            }
        };
    }
}

/// The grouped str MIN/MAX advance through the dict-code memo — bit-identical
/// transvalue AND datumCopy sequence to `str_advance(t, pg, d)` with
/// `d = values[i] = dict[code]` (the `col_codes` contract), proven case by
/// case (MIN shown; MAX mirrors with the order flipped):
///
/// * memo hit, `code` worse than `memo.code` (`>` for MIN): the transvalue
///   is `dict[memo.code]` (tv_code) or strictly smaller (¬tv_code); either
///   way `tv < dict[code]` strictly, so the per-row advance KEEPS with no
///   copy — and so do we, without touching the payload.
/// * memo hit, `code == memo.code`: tv_code ⇒ memcmp tie ⇒ the per-row
///   advance REPLACES (text's last-tied-wins datumCopies every tied row) —
///   we copy identically; ¬tv_code ⇒ `tv < dict[code]` strict ⇒ keep.
/// * memo hit, `code` better (`<` for MIN): tv_code ⇒ `tv = dict[memo.code]
///   > dict[code]` ⇒ replace + copy, memo.code = code; ¬tv_code ⇒ order
///   unknown ⇒ ONE payload compare (exactly the per-row advance's memcmp)
///   decides keep (memo.code = code, tv still strictly smaller) vs replace.
/// * memo miss / stale epoch / stale gen: run the plain advance body (its
///   memcmp, its copies — the install case included), then seed the memo
///   from the outcome: replaced-or-installed ⇒ tv_code (the transvalue IS
///   the copy of `dict[code]`); kept ⇒ ¬tv_code (`tv` strictly beat it).
///
/// Every branch's copy set equals the per-row advance's copy set, so
/// aggcontext allocation SEQUENCE — and with it hash-agg memory accounting
/// and spill decisions — is unchanged.
///
/// # Safety
/// As `str_advance`; `d == dict[code]` per the `col_codes` contract; `key`
/// is the live pergroup CELL address for `(group, t.transno)`.
#[inline(always)]
unsafe fn str_advance_coded(
    t: &LaneTrans,
    pg: &mut AggPerGroup,
    d: Datum,
    code: u32,
    epoch: u64,
    key: usize,
    mm: &mut StrMmScratch,
    aggcxt: Mcx<'_>,
    mut sa: Option<&mut StrStateArena>,
) -> PgResult<()> {
    // Copy/replace through the table-owned store when armed (str_advance's
    // dispatch, hoisted so every arm below shares it).
    macro_rules! sa_replace {
        () => {
            match sa.as_deref_mut() {
                // SAFETY: forwarded caller contract.
                Some(a) => unsafe { a.replace(pg.trans_value, d) },
                // SAFETY: forwarded caller contract.
                None => unsafe {
                    ::execexpr::agg_datum_replace(aggcxt, pg.trans_value, d, -1)?
                },
            }
        };
    }
    let want_max = t.kind == LaneKind::StrMax;
    let gen = mm.gen;
    if let Some(m) = mm.map.get_mut(&key) {
        if m.gen == gen && m.epoch == epoch && !pg.no_trans_value && !pg.trans_value_is_null {
            let better = if want_max { code > m.code } else { code < m.code };
            if !better {
                if code == m.code && m.tv_code {
                    // Tie against the code-valued transvalue: text's
                    // last-tied-wins replaces (and copies) per tied row.
                    pg.trans_value = sa_replace!();
                }
                return Ok(());
            }
            if m.tv_code {
                pg.trans_value = sa_replace!();
                m.code = code;
                return Ok(());
            }
            // Cross-epoch survivor vs a better code: one payload compare —
            // exactly the memcmp the per-row advance would run.
            // SAFETY: forwarded caller contract (inline varlenas).
            if unsafe { str_keep(t.kind, pg.trans_value, d) } {
                m.code = code;
            } else {
                pg.trans_value = sa_replace!();
                m.code = code;
                m.tv_code = true;
            }
            return Ok(());
        }
    }
    // Miss / stale: the plain advance body (str_advance inlined so the
    // outcome — installed / replaced / kept — seeds the memo).
    let tv_code;
    // SAFETY: forwarded caller contract.
    unsafe {
        if pg.no_trans_value {
            pg.trans_value = match sa.as_deref_mut() {
                Some(a) => a.copy(d),
                None => ::execexpr::agg_datum_copy(aggcxt, d, -1)?,
            };
            pg.trans_value_is_null = false;
            pg.no_trans_value = false;
            tv_code = true;
        } else if !pg.trans_value_is_null {
            if str_keep(t.kind, pg.trans_value, d) {
                tv_code = false;
            } else {
                // Replace discipline: see str_advance.
                pg.trans_value = sa_replace!();
                tv_code = true;
            }
        } else {
            // NULL transvalue outside the install state: str_advance leaves
            // it untouched; no memo (the state is not code-describable).
            return Ok(());
        }
    }
    mm.map.insert(key, MmMemo { gen, epoch, code, tv_code });
    Ok(())
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
/// for every plan column; AvgAccum pergroups hold a live transarray; FAccum
/// pergroups hold a live aggcontext float8[3] transarray;
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
    // SAFETY: forwarded caller contract.
    unsafe { fold_rows_grouped_mm(plan, cols, idxs, groups, aggcxt, None, 0, None) }
}

/// `fold_rows_grouped` with the str MIN/MAX dict-code memo (lane-v2-
/// dictminmax): when the feed passes a scratch AND `cols.col_codes` answers
/// a SORTED dict view for a `StrMin`/`StrMax` column, each row's advance
/// routes through `str_advance_coded` — integer code compares against the
/// per-group memo instead of a payload memcmp per row, with the transvalue
/// bytes and the datumCopy sequence provably unchanged (see
/// `str_advance_coded`). The feed owns the scratch's invalidation
/// obligations (`StrMmScratch` doc).
///
/// # Safety
/// As `fold_rows_grouped`; a passed `mm` additionally requires the
/// `col_codes` contract for every answered column and the scratch's
/// generation to have been invalidated after any out-of-band advance.
/// `avgpack_mask` bits name AvgAccum transnos whose pergroup slot holds the
/// PACKED inline `[count, sum]` state ([`avg_apply_packed`]) instead of a
/// transarray pointer — nodeagg sink builds only; every other caller passes
/// 0. A mask bit on a non-AvgAccum transno is a caller bug. A `Some(sa)`
/// routes every str copy/replace through the TABLE-OWNED state store
/// instead of `aggcxt` (migrating sink tables — [`StrStateArena`] doc);
/// the store must own every prior str transvalue of these pergroups.
pub unsafe fn fold_rows_grouped_mm(
    plan: &LanePlan<'_>,
    cols: &impl LaneCols,
    idxs: &[u32],
    groups: &[NonNull<AggPerGroup>],
    aggcxt: Mcx<'_>,
    mut mm: Option<&mut StrMmScratch>,
    avgpack_mask: u64,
    mut sa: Option<&mut StrStateArena>,
) -> PgResult<()> {
    debug_assert_eq!(idxs.len(), groups.len());
    for t in plan.trans.iter() {
        let transno = t.transno as usize;
        // Per-transition FILTER (fold-trans tier): the mask applies BEFORE
        // the transition, per row, in row order — filter-failing rows are
        // skipped exactly where C's per-row program skips them.
        let filt = (t.filter != NO_FILTER).then(|| {
            let f = &plan.filters[t.filter as usize];
            (f, cols.col_values(f.col as usize), cols.col_isnull(f.col as usize))
        });
        let passes = |i: usize| match &filt {
            None => true,
            Some((f, fv, fn_)) => filter_passes(f, fv, fn_, i),
        };
        if t.kind == LaneKind::CountStar {
            if filt.is_none() {
                for &g in groups {
                    // SAFETY: caller contract.
                    let pg = unsafe { &mut *g.as_ptr().add(transno) };
                    pg.trans_value = Datum::from_i64(pg.trans_value.as_i64().wrapping_add(1));
                    pg.trans_value_is_null = false;
                    pg.no_trans_value = false;
                }
            } else {
                for (&i, &g) in idxs.iter().zip(groups.iter()) {
                    if !passes(i as usize) {
                        continue;
                    }
                    // SAFETY: caller contract.
                    let pg = unsafe { &mut *g.as_ptr().add(transno) };
                    pg.trans_value = Datum::from_i64(pg.trans_value.as_i64().wrapping_add(1));
                    pg.trans_value_is_null = false;
                    pg.no_trans_value = false;
                }
            }
            continue;
        }
        let (values, isnull) =
            (cols.col_values(t.col as usize), cols.col_isnull(t.col as usize));
        let w = read_width(cols, t.col, t.width);
        // Dict-code memo route for this transition (str kinds only, sorted
        // dict view up, scratch provided) — resolved once per transition.
        let code_lane = match (t.kind, mm.as_deref_mut()) {
            (LaneKind::StrMin | LaneKind::StrMax, Some(_)) => {
                cols.col_codes(t.col as usize).filter(|l| l.table.sorted)
            }
            _ => None,
        };
        if t.kind == LaneKind::Int128AvgAccum {
            // Dedicated row loop: the non-strict transfn runs for NULL
            // inputs too (state alloc on the group's first filter-passing
            // row of any nullness — C parity), so this kind must not take
            // the shared skip-null path below.
            debug_assert_eq!((t.addend, t.mulk, t.divk), (0, 1, 1), "bare-Var admission only");
            for (&i, &g) in idxs.iter().zip(groups.iter()) {
                let i = i as usize;
                if !passes(i) {
                    continue;
                }
                // SAFETY: caller contract.
                let pg = unsafe { &mut *g.as_ptr().add(transno) };
                let st = int128_state(pg, aggcxt)?;
                if !isnull[i] {
                    // SAFETY: aggcontext-lived state from int128_state or
                    // the per-row transfn chain; sole reference here —
                    // bit-identical by construction (the per-row path's own
                    // transition body).
                    unsafe {
                        do_int128_accum(&mut *st, lane_value(values, w, i) as i128)
                    };
                }
            }
            continue;
        }
        if t.kind == LaneKind::NumAccum {
            // Dedicated row loop (non-strict, the Int128AvgAccum discipline)
            // driving C's exact per-row do_numeric_accum in row order.
            for (&i, &g) in idxs.iter().zip(groups.iter()) {
                let i = i as usize;
                if !passes(i) {
                    continue;
                }
                // SAFETY: caller contract.
                let pg = unsafe { &mut *g.as_ptr().add(transno) };
                let st = numeric_state(pg, aggcxt)?;
                if !isnull[i] {
                    // SAFETY: vguard-passed inline varlena; aggcontext-lived
                    // state, sole reference here.
                    unsafe { num_accum_row(&mut *st, values[i], aggcxt)? };
                }
            }
            continue;
        }
        if matches!(t.kind, LaneKind::FRegrAccum | LaneKind::Count2) {
            // Dedicated two-arg loops: strictness spans BOTH inputs, which
            // the shared single-column skip-null path cannot express. Row
            // order per group is batch order — the ordering discipline the
            // bivariate Youngs-Cramer updates need.
            let (values2, isnull2) =
                (cols.col_values(t.col2 as usize), cols.col_isnull(t.col2 as usize));
            for (&i, &g) in idxs.iter().zip(groups.iter()) {
                let i = i as usize;
                if !passes(i) || isnull[i] || isnull2[i] {
                    continue;
                }
                // SAFETY: caller contract.
                let pg = unsafe { &mut *g.as_ptr().add(transno) };
                match t.kind {
                    LaneKind::FRegrAccum => {
                        let y = conv_f64(t.fconv, values, i);
                        let x = conv_f64(t.fconv2, values2, i);
                        // SAFETY: FRegr pergroups hold a live aggcontext
                        // float8[6] transarray (caller contract).
                        unsafe { fregr_advance(pg, y, x)? };
                    }
                    _ => count_apply(pg, 1),
                }
            }
            continue;
        }
        for (&i, &g) in idxs.iter().zip(groups.iter()) {
            let i = i as usize;
            if !passes(i) || isnull[i] {
                continue;
            }
            // SAFETY: caller contract.
            let pg = unsafe { &mut *g.as_ptr().add(transno) };
            // t.kind is loop-invariant: LLVM unswitches, and the integer lane
            // read/transform stays out of the datum-lane arms.
            match t.kind {
                LaneKind::CountStar
                | LaneKind::Int128AvgAccum
                | LaneKind::NumAccum
                | LaneKind::FRegrAccum
                | LaneKind::Count2 => unreachable!("dedicated loops above"),
                LaneKind::CountAny => count_apply(pg, 1),
                LaneKind::Sum => sum_apply(pg, xform(t, lane_value(values, w, i))),
                // avgpack_of is loop-invariant per transition (LLVM
                // unswitches with the kind).
                LaneKind::AvgAccum if avgpack_of(avgpack_mask, t.transno) => {
                    avg_apply_packed(pg, 1, xform(t, lane_value(values, w, i)));
                }
                LaneKind::AvgAccum => avg_apply(pg, 1, xform(t, lane_value(values, w, i))),
                LaneKind::Min | LaneKind::Max => {
                    let v = xform(t, lane_value(values, w, i));
                    minmax_advance(t, pg, v, t.kind == LaneKind::Max);
                }
                LaneKind::FMin | LaneKind::FMax => {
                    fmm_advance(t, pg, values[i], t.kind == LaneKind::FMax);
                }
                LaneKind::BoolAnd | LaneKind::BoolOr => {
                    bool_advance(pg, values[i].as_bool(), t.kind == LaneKind::BoolAnd);
                }
                LaneKind::BitAnd | LaneKind::BitOr => {
                    let v = xform(t, lane_value(values, w, i));
                    bit_advance(t, pg, v, t.kind == LaneKind::BitAnd);
                }
                LaneKind::StrMin | LaneKind::StrMax | LaneKind::BpMin | LaneKind::BpMax => {
                    match (&code_lane, mm.as_deref_mut()) {
                        (Some(lane), Some(scratch)) => {
                            let cell = unsafe { g.as_ptr().add(transno) } as usize;
                            // SAFETY: col_codes contract (values[i] ==
                            // dict[code], inline varlena) + live pergroup and
                            // aggcxt (caller contract); sa owns prior copies
                            // (fn contract).
                            unsafe {
                                str_advance_coded(
                                    t,
                                    pg,
                                    values[i],
                                    lane.code(i),
                                    lane.table.epoch,
                                    cell,
                                    scratch,
                                    aggcxt,
                                    sa.as_deref_mut(),
                                )?
                            };
                        }
                        // SAFETY: vguard-passed batch + live aggcxt (caller
                        // contract); sa owns prior copies (fn contract).
                        _ => unsafe {
                            str_advance(t, pg, values[i], aggcxt, sa.as_deref_mut())?
                        },
                    }
                }
                // Fold-trans tier: per-row ORDER-PRESERVING float advances.
                // This loop already visits rows in batch order per
                // transition, which is exactly the discipline the
                // non-commutative float kernels need — each group's state
                // sees its rows in C's row order.
                LaneKind::FSum => {
                    let d = if t.fconv == FloatConv::None {
                        values[i]
                    } else {
                        Datum::from_f64(conv_f64(t.fconv, values, i))
                    };
                    fsum_advance(t, pg, d)?
                }
                // SAFETY: FAccum pergroups hold a live aggcontext float8[3]
                // transarray (caller contract); sole reference here.
                LaneKind::FAccum => unsafe { faccum_advance(pg, conv_f64(t.fconv, values, i))? },
            }
        }
    }
    Ok(())
}

// ===========================================================================
// Code-histogram grouped build (lane-v2-codehist): for shapes where ONE
// dict-coded column feeds the group key AND every admitted transition, the
// feed counts selected rows per (epoch, code) and advances each
// (group, code) ONCE with multiplicity n instead of once per row.
// Multiplicity legality per kind (vs n sequential per-row transitions):
//   count(*)/count(col):   += n            (dict values are non-null — the
//                                           feed's contract — so strict
//                                           count(col) counts every row)
//   sum/avg int lanes:     sum += n*v mod 2^64 — bit-equal to n wrapping
//                          adds of the int4-proven per-row term v
//   int8 avg/sum (i128):   n += n, sum_x += n*(v as i128) — n sequential
//                          i128 adds cannot hit an intermediate the product
//                          form misses (|v| < 2^63, n < 2^32)
//   MIN/MAX (int + str):   idempotent under repetition on the VALUE; the
//                          str kinds' last-tied-wins re-COPIES per tied row
//                          in the per-row path, so the single advance
//                          COLLAPSES aggcontext allocations — the feed must
//                          therefore refuse spill-eligible builds
//                          (agg_hash_spill_unlikely), where the allocation
//                          sequence could steer spill decisions
//   bit_and/bit_or:        idempotent (v OP v == v)
// Everything else refuses at plan_code_hostable.
// ===========================================================================

/// Whether this plan carries a BY-REF str transvalue — a `min/max(text)` or
/// `min/max(varchar)` lane whose transvalue is a plain varlena that
/// [`str_advance`] must COPY on install and copy-then-free on replace.
///
/// GL-SINKCRASH-2: this is the class predicate for the byref-str transvalue
/// discipline, and it exists as ONE function precisely because the discipline
/// was previously enforced case by case, per feature gate, with no shared
/// predicate — which is how a sink drain came to copy these transvalues into a
/// per-(thread, query) memory context while the table holding the pointers
/// migrated across pool threads. Both the ARMING of the table-owned
/// [`StrStateArena`] and the fail-closed check that it was armed must read
/// this same expression, or they can disagree.
///
/// `BpMin`/`BpMax` are deliberately INCLUDED: `fold_rows_grouped_mm` routes
/// them through `str_advance` exactly like the text kinds, so they are the same
/// allocator-home hazard even though the sink's combine whitelist does not (yet)
/// admit a bpchar transvalue. A future oid added to that whitelist must not
/// silently inherit an unarmed store.
pub fn plan_has_str_trans(plan: &LanePlan<'_>) -> bool {
    plan.trans.iter().any(|t| {
        matches!(
            t.kind,
            LaneKind::StrMin | LaneKind::StrMax | LaneKind::BpMin | LaneKind::BpMax
        )
    })
}

/// Code-hostable plan shape: no residual transitions, no integer guard
/// intervals, and every transition either reads no column (`CountStar`) or
/// reads ONE common column with a multiplicity-legal kind (table above).
/// Returns that common column (plan space; the caller maps it to the scan
/// column and requires it to BE the dict key input). Plans of pure
/// `CountStar` return None — nothing code-valued to host.
pub fn plan_code_hostable(plan: &LanePlan<'_>) -> Option<u16> {
    // FILTER'd plans refuse: the per-(epoch, code) multiplicity would count
    // filtered-out rows.
    if !plan.resid.is_empty() || !plan.guards.is_empty() || !plan.filters.is_empty() {
        return None;
    }
    let mut col: Option<u16> = None;
    for t in plan.trans.iter() {
        if t.kind == LaneKind::CountStar {
            continue;
        }
        match t.kind {
            LaneKind::CountAny
            | LaneKind::Sum
            | LaneKind::AvgAccum
            | LaneKind::Int128AvgAccum
            | LaneKind::Min
            | LaneKind::Max
            | LaneKind::StrMin
            | LaneKind::StrMax
            | LaneKind::BitAnd
            | LaneKind::BitOr => {}
            _ => return None,
        }
        match col {
            None => col = Some(t.col),
            Some(c) if c == t.col => {}
            Some(_) => return None,
        }
    }
    col
}

/// Per-code data proof — the row-domain `check_guards` vguard/uguard checks
/// applied to ONE dict value (values of selected rows ⊆ touched dict
/// entries, so per-touched-code proofs cover exactly the selected value
/// set). False = the feed must route the batch through the per-row program,
/// which re-proves row-domain and demotes identically.
///
/// # Safety
/// `d` is readable at its varlena header byte (a staged window's dict entry).
pub unsafe fn datum_code_guards_ok(plan: &LanePlan<'_>, d: Datum) -> bool {
    if !plan.vguards.is_empty() {
        let p = d.as_usize() as *const u8;
        // SAFETY: caller contract (header byte readable).
        let inline = unsafe {
            (::types_tuple::varatt::varatt_is_1b(p) && !::types_tuple::varatt::varatt_is_1b_e(p))
                || ::types_tuple::varatt::varatt_is_4b_u(p)
        };
        if !inline {
            return false;
        }
        if !plan.uguards.is_empty() {
            // SAFETY: inline form proven above.
            let s = unsafe { str_payload(d) };
            if core::str::from_utf8(s).is_err() || s.contains(&0) {
                return false;
            }
        }
    }
    true
}

/// Per-code transition inputs, computed ONCE per (epoch, code) at first
/// touch while the window's dict datum is valid: `out[ti]` = the transformed
/// integer lane value for int-valued kinds (undefined/0 for count and str
/// kinds). Pointer-free — safe to hold across windows for the epoch flush.
///
/// # Safety
/// `d` passed `datum_code_guards_ok` for this plan (inline varlena; UTF-8
/// proven when a chars-width lane reads it).
pub unsafe fn code_trans_vals(plan: &LanePlan<'_>, d: Datum, out: &mut Vec<i64>) {
    out.clear();
    for t in plan.trans.iter() {
        let v = match t.kind {
            LaneKind::Sum
            | LaneKind::AvgAccum
            | LaneKind::Int128AvgAccum
            | LaneKind::Min
            | LaneKind::Max
            | LaneKind::BitAnd
            | LaneKind::BitOr => {
                let one = [d];
                xform(t, lane_value(&one, t.width, 0))
            }
            _ => 0,
        };
        out.push(v);
    }
}

/// Advance every transition of `plan` for ONE (group, code) with
/// multiplicity `n` (legality table in the section doc). `vals` is this
/// code's `code_trans_vals`; `strd` is the code's varlena image (the feed's
/// pointer-free scratch copy — byte-identical to the dict entry, so
/// `agg_datum_copy` of it is byte-identical to a per-row copy).
///
/// # Safety
/// As `fold_rows_grouped` for `pergroup_base`/`aggcxt`; `strd` is a live
/// inline varlena when the plan carries str kinds; `n >= 1`. `avgpack_mask`
/// as `fold_rows_grouped_mm` (packed inline AvgAccum slots — sink builds
/// only; other callers pass 0).
pub unsafe fn fold_code_group(
    plan: &LanePlan<'_>,
    vals: &[i64],
    strd: Datum,
    n: i64,
    pergroup_base: NonNull<AggPerGroup>,
    aggcxt: Mcx<'_>,
    avgpack_mask: u64,
) -> PgResult<()> {
    debug_assert!(n >= 1 && vals.len() == plan.trans.len());
    for (ti, t) in plan.trans.iter().enumerate() {
        // SAFETY: transno < pergroup length (caller contract).
        let pg = unsafe { &mut *pergroup_base.as_ptr().add(t.transno as usize) };
        match t.kind {
            LaneKind::CountStar | LaneKind::CountAny => count_apply(pg, n),
            LaneKind::Sum => sum_apply(pg, (n).wrapping_mul(vals[ti])),
            LaneKind::AvgAccum if avgpack_of(avgpack_mask, t.transno) => {
                avg_apply_packed(pg, n, (n).wrapping_mul(vals[ti]));
            }
            LaneKind::AvgAccum => avg_apply(pg, n, (n).wrapping_mul(vals[ti])),
            LaneKind::Int128AvgAccum => {
                let st = int128_state(pg, aggcxt)?;
                // SAFETY: aggcontext-lived state (int128_state / the per-row
                // transfn chain); sole reference here.
                unsafe {
                    (*st).n += n;
                    (*st).sum_x += vals[ti] as i128 * n as i128;
                }
            }
            LaneKind::Min | LaneKind::Max => {
                minmax_advance(t, pg, vals[ti], t.kind == LaneKind::Max);
            }
            LaneKind::BitAnd | LaneKind::BitOr => {
                bit_advance(t, pg, vals[ti], t.kind == LaneKind::BitAnd);
            }
            LaneKind::StrMin | LaneKind::StrMax => {
                // SAFETY: live inline varlena scratch image + live pergroup
                // and aggcxt (caller contract).
                unsafe { str_advance(t, pg, strd, aggcxt, None)? };
            }
            _ => unreachable!("plan_code_hostable admitted an unhostable kind"),
        }
    }
    Ok(())
}
