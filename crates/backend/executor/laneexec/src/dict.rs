// Dict-memo tier (lane-executor increment 9 / the POC's S3 storage stage):
// text predicates over pgrcolumnar DictCoded lanes evaluate ONCE per distinct
// dictionary code per epoch (row group) via the PRODUCTION adt helpers, then
// per row as one memo index. Admission is fail-closed: only predicates
// proven non-erroring for every input at translate time may run eagerly
// across rows (the same legality bar as the vectorized int clauses — an
// erroring predicate could raise on dictionary entries or rows the per-row
// drive would never visit under LIMIT/short-circuit).
use adt_like::IcScratch;
use datum::Datum;
use exectuples::{SoaBatch, SoaDictLane, SoaTextSpan};
use mcx::MemoryContext;
use types_core::catalog::{C_COLLATION_OID, DEFAULT_COLLATION_OID};
use types_core::{Oid, OidIsValid};
use types_error::{PgError, PgResult};
use types_tuple::varatt;

use crate::interp::SelVec;
use crate::shape::{LaneCmpClause, LaneCmpRhs};

const F_TEXTEQ: Oid = 67;
const F_TEXTNE: Oid = 157;
const F_TEXTLIKE: Oid = 850;
const F_TEXTNLIKE: Oid = 851;
const F_LIKE: Oid = 1569;
const F_NOTLIKE: Oid = 1570;
const F_BPCHARLIKE: Oid = 1631;
const F_BPCHARNLIKE: Oid = 1632;
const F_TEXTICLIKE: Oid = 1633;
const F_TEXTICNLIKE: Oid = 1634;
const F_BPCHARICLIKE: Oid = 1660;
const F_BPCHARICNLIKE: Oid = 1661;
const F_TEXTICREGEXEQ: Oid = 1238;
const F_TEXTICREGEXNE: Oid = 1239;
const F_TEXTREGEXEQ: Oid = 1254;
const F_TEXTREGEXNE: Oid = 1256;

// match_text recurses once per unconsumed '%'; the cap keeps the stitch-free
// depth far under check_stack_depth's limit so the eager evaluation can never
// raise the stack error a per-row drive might not reach.
const LIKE_MAX_PERCENTS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DictPredOp {
    Like,
    NotLike,
    Eq,
    Ne,
    ILike,
    NotILike,
    // ~ / !~ / ~* / !~* with a constant pattern (regex-dict-memo INC3):
    // LAZY per-code memo — pg_regexec can error (REG_CANCEL and friends),
    // so unlike the total LIKE/eq kernels the evaluation set must be
    // exactly the surviving rows' values, filled on first reach.
    Regex,
    NotRegex,
    IcRegex,
    NotIcRegex,
}

impl DictPredOp {
    fn negated(self) -> bool {
        matches!(
            self,
            DictPredOp::NotLike
                | DictPredOp::Ne
                | DictPredOp::NotILike
                | DictPredOp::NotRegex
                | DictPredOp::NotIcRegex
        )
    }

    fn is_regex(self) -> bool {
        matches!(
            self,
            DictPredOp::Regex | DictPredOp::NotRegex | DictPredOp::IcRegex | DictPredOp::NotIcRegex
        )
    }

    fn regex_cflags(self) -> i32 {
        match self {
            DictPredOp::IcRegex | DictPredOp::NotIcRegex => {
                ::regex::REG_ADVANCED | ::regex::REG_ICASE
            }
            _ => ::regex::REG_ADVANCED,
        }
    }
}

/// Byte kernel for a `_`-free LIKE pattern (or a texteq constant). For a
/// deterministic collation these shapes are byte-exact against match_text in
/// single-byte encodings and UTF-8: a `%` scan can only anchor a valid-UTF-8
/// needle at character boundaries (self-synchronization), so byte-level
/// prefix/suffix/substring occurrence coincides with the character-level
/// semantics.
enum LikeKernel {
    /// All-`%` pattern: every non-NULL value matches.
    Any,
    Exact(Vec<u8>),
    Prefix(Vec<u8>),
    Suffix(Vec<u8>),
    Contains(SubstrFinder),
    /// `a%b`: starts_with(a) && ends_with(b), non-overlapping.
    PrefixSuffix(Vec<u8>, Vec<u8>),
}

impl LikeKernel {
    fn matches(&self, s: &[u8]) -> bool {
        match self {
            LikeKernel::Any => true,
            LikeKernel::Exact(k) => s == &k[..],
            LikeKernel::Prefix(p) => s.starts_with(p),
            LikeKernel::Suffix(x) => s.ends_with(x),
            LikeKernel::Contains(f) => f.find(s),
            LikeKernel::PrefixSuffix(p, x) => {
                s.len() >= p.len() + x.len() && s.starts_with(p) && s.ends_with(x)
            }
        }
    }

    fn shape(&self) -> &'static str {
        match self {
            LikeKernel::Any => "any",
            LikeKernel::Exact(_) => "exact",
            LikeKernel::Prefix(_) => "prefix",
            LikeKernel::Suffix(_) => "suffix",
            LikeKernel::Contains(_) => "contains",
            LikeKernel::PrefixSuffix(..) => "prefix+suffix",
        }
    }
}

/// Constant-needle substring search built once at translate; memmem's
/// SIMD-prefiltered searcher (same occurrence set as the byte scan).
struct SubstrFinder {
    finder: memchr::memmem::Finder<'static>,
}

impl SubstrFinder {
    fn new(needle: Vec<u8>) -> Self {
        debug_assert!(!needle.is_empty());
        SubstrFinder { finder: memchr::memmem::Finder::new(&needle).into_owned() }
    }

    fn find(&self, hay: &[u8]) -> bool {
        self.finder.find(hay).is_some()
    }

    #[inline]
    fn find_pos(&self, hay: &[u8]) -> Option<usize> {
        self.finder.find(hay)
    }

    #[inline]
    fn needle_len(&self) -> usize {
        self.finder.needle().len()
    }
}

/// Fail-closed classification of a LIKE pattern into a byte kernel: only
/// unescaped `%` wildcards (no `_`), at most two literal segments, and the
/// two-segment form anchored at both ends. Anything else keeps the scalar
/// match_text path.
fn like_kernel_for(pat: &[u8]) -> Option<LikeKernel> {
    let mut segs: Vec<Vec<u8>> = Vec::new();
    let mut cur: Vec<u8> = Vec::new();
    let mut leading_pct = false;
    let mut trailing_pct = false;
    let mut any_pct = false;
    let mut i = 0usize;
    while i < pat.len() {
        match pat[i] {
            b'\\' => {
                // like_pattern_safe rejected a pattern-final escape.
                cur.push(*pat.get(i + 1)?);
                trailing_pct = false;
                i += 2;
            }
            b'%' => {
                any_pct = true;
                if cur.is_empty() && segs.is_empty() {
                    leading_pct = true;
                }
                if !cur.is_empty() {
                    segs.push(core::mem::take(&mut cur));
                }
                trailing_pct = true;
                i += 1;
            }
            b'_' => return None,
            b => {
                cur.push(b);
                trailing_pct = false;
                i += 1;
            }
        }
    }
    if !cur.is_empty() {
        segs.push(cur);
    }
    match (segs.len(), leading_pct, trailing_pct) {
        (0, ..) => Some(if any_pct { LikeKernel::Any } else { LikeKernel::Exact(Vec::new()) }),
        (1, false, false) => Some(LikeKernel::Exact(segs.pop().unwrap())),
        (1, false, true) => Some(LikeKernel::Prefix(segs.pop().unwrap())),
        (1, true, false) => Some(LikeKernel::Suffix(segs.pop().unwrap())),
        (1, true, true) => Some(LikeKernel::Contains(SubstrFinder::new(segs.pop().unwrap()))),
        (2, false, false) => {
            let x = segs.pop().unwrap();
            let p = segs.pop().unwrap();
            Some(LikeKernel::PrefixSuffix(p, x))
        }
        _ => None,
    }
}

pub struct DictClause {
    pub col: u16,
    op: DictPredOp,
    collation: Oid,
    konst: Vec<u8>,
    kernel: Option<LikeKernel>,
    ic: IcScratch,
    /// ILIKE-only: satisfies texticlike's mcx parameter. The admitted paths
    /// (ctype_is_c lower_into / single-byte SbIc) never allocate from it.
    arena: Option<MemoryContext>,
    memo_epoch: Option<u64>,
    memo: Vec<bool>,
    // Sorted-dict prefix fast path: matching codes form [lo, hi) in rank
    // order — two binary searches per RG replace the per-entry memo fill.
    // None when the memo path answered this epoch.
    range: Option<(u32, u32)>,
    // Regex ops only: lazy tri-state per-code memo (0 unset / 1 match /
    // 2 no-match), filled on the first SURVIVING row that reaches a code —
    // the per-row drive's evaluation set, so error identity holds.
    memo_tri: Vec<u8>,
    // First-engagement trace flag for the blob-wide contains path.
    blob_logged: bool,
    // First-engagement trace flag for the dict-arena sweep memo fill.
    sweep_logged: bool,
}

impl DictClause {
    /// `col <> ''` (textne against the empty string). Its verdict is a pure
    /// length predicate — TRUE iff the payload is non-empty — because the
    /// dict tier only admits deterministic collations (collation_usable), so
    /// a granule length-stats consumer can fold it arithmetically: rejected
    /// rows are exactly the empty-string rows.
    pub(crate) fn is_ne_empty(&self) -> bool {
        matches!(self.op, DictPredOp::Ne) && self.konst.is_empty()
    }

    /// Condition-cache admission: true iff this clause's per-row evaluation
    /// is deterministic AND non-erroring for every row (the admission
    /// proofs: like_pattern_safe / ilike_infallible / collation_usable).
    /// Regex ops refuse — their lazy tri-memo preserves error identity by
    /// evaluating exactly the per-row drive's row set, which a cache hit
    /// would skip.
    pub(crate) fn cache_safe(&self) -> bool {
        !self.op.is_regex()
    }

    // Prewhere static cost classes (translate.rs StagedClause contract):
    // dict eq/ne = 4, dict LIKE = 5.
    pub(crate) fn cost_class(&self) -> u8 {
        match self.op {
            DictPredOp::Eq | DictPredOp::Ne => 4,
            DictPredOp::Like | DictPredOp::NotLike => 5,
            // Case-folding predicate: costlier than LIKE, cheaper than the
            // requal tail.
            DictPredOp::ILike | DictPredOp::NotILike => 6,
            // Full regex match per distinct value: costliest memo tier.
            DictPredOp::Regex
            | DictPredOp::NotRegex
            | DictPredOp::IcRegex
            | DictPredOp::NotIcRegex => 7,
        }
    }
}

/// Fail-closed classification of one structurally-parsed text clause into a
/// dict-memoizable predicate. None = not memoizable; the clause ends the
/// vectorizable prefix exactly as before (per-row requal suffix).
pub(crate) fn dict_clause_for(cl: &LaneCmpClause) -> Option<DictClause> {
    let op = match cl.fn_oid {
        F_TEXTLIKE | F_LIKE | F_BPCHARLIKE => DictPredOp::Like,
        F_TEXTNLIKE | F_NOTLIKE | F_BPCHARNLIKE => DictPredOp::NotLike,
        F_TEXTEQ => DictPredOp::Eq,
        F_TEXTNE => DictPredOp::Ne,
        F_TEXTICLIKE | F_BPCHARICLIKE => DictPredOp::ILike,
        F_TEXTICNLIKE | F_BPCHARICNLIKE => DictPredOp::NotILike,
        F_TEXTREGEXEQ => DictPredOp::Regex,
        F_TEXTREGEXNE => DictPredOp::NotRegex,
        F_TEXTICREGEXEQ => DictPredOp::IcRegex,
        F_TEXTICREGEXNE => DictPredOp::NotIcRegex,
        _ => return None,
    };
    let is_like = matches!(op, DictPredOp::Like | DictPredOp::NotLike);
    let is_iclike = matches!(op, DictPredOp::ILike | DictPredOp::NotILike);
    // Commuted LIKE/regex makes the COLUMN the pattern: pattern validity
    // can't be proven at translate time. Eq/Ne are symmetric, commutation is
    // free.
    if cl.commuted && (is_like || is_iclike || op.is_regex()) {
        return None;
    }
    let LaneCmpRhs::Const(k) = cl.rhs else { return None };
    let konst = inline_varlena_payload(k)?.to_vec();
    if !collation_usable(cl.collation) {
        return None;
    }
    if (is_like || is_iclike) && !like_pattern_safe(&konst) {
        return None;
    }
    if is_iclike && !ilike_infallible(cl.collation) {
        return None;
    }
    if op.is_regex() {
        // Compile at translate time through the production cache seam: a bad
        // pattern refuses the tier and the per-row drive raises C's error at
        // the same first row. The exec seam serves the per-value matches.
        if !regexp_seams::RE_compile_and_cache::is_installed()
            || !regexp_seams::RE_compile_and_execute::is_installed()
        {
            return None;
        }
        let mut probe = MemoryContext::new_bump("LaneDictRegexProbe");
        let compiled = regexp_seams::RE_compile_and_cache::call(
            probe.mcx(),
            &konst,
            op.regex_cflags(),
            cl.collation,
        )
        .is_ok();
        probe.reset();
        if !compiled {
            return None;
        }
    }
    let kernel = match op {
        DictPredOp::Like | DictPredOp::NotLike => like_kernel_for(&konst),
        DictPredOp::Eq | DictPredOp::Ne => Some(LikeKernel::Exact(konst.clone())),
        _ => None,
    };
    if let Some(k) = &kernel {
        crate::log_dict_kernel(cl.col, k.shape());
    }
    if op.is_regex() {
        crate::log_dict_regex(cl.col, matches!(op, DictPredOp::IcRegex | DictPredOp::NotIcRegex));
    }
    Some(DictClause {
        col: cl.col,
        op,
        collation: cl.collation,
        konst,
        kernel,
        ic: IcScratch::default(),
        arena: (is_iclike || op.is_regex())
            .then(|| MemoryContext::new_bump("LaneDictPredContext")),
        memo_epoch: None,
        memo: Vec::new(),
        range: None,
        memo_tri: Vec::new(),
        blob_logged: false,
        sweep_logged: false,
    })
}

// ILIKE-specific non-erroring gate mirroring generic_text_ic_like's branch
// structure after collation_usable (valid + deterministic): the multibyte/ICU
// arm is admissible only when ctype_is_c (infallible ASCII lower_into, no
// str_tolower allocation) AND the encoding is UTF-8 (the non-UTF8 multibyte
// matcher is unported and errors); the single-byte non-ICU arm (SbIc, locale
// tolower per byte) is total.
fn ilike_infallible(coll: Oid) -> bool {
    let Ok(locale) = pg_locale::pg_newlocale_from_collation(coll) else {
        return false;
    };
    if mbutils::pg_database_encoding_max_length() > 1
        || locale.provider == pg_locale::COLLPROVIDER_ICU
    {
        locale.ctype_is_c && mbutils::GetDatabaseEncoding() == wchar::PG_UTF8
    } else {
        true
    }
}

// Non-erroring gate shared by every op: the collation must resolve without a
// catalog error and be deterministic (texteq's fast path is then a pure
// memcmp; generic_match_text's nondeterministic error arm is unreachable).
// Guards keep the probes non-panicking in seamless test/boot environments.
pub fn collation_usable(coll: Oid) -> bool {
    if !OidIsValid(coll) {
        return false;
    }
    if coll == DEFAULT_COLLATION_OID && !pg_locale::default_locale_installed() {
        return false;
    }
    if coll != C_COLLATION_OID
        && coll != DEFAULT_COLLATION_OID
        && !syscache_seams::lookup_pg_collation_locale_row::is_installed()
    {
        return false;
    }
    matches!(pg_locale::pg_newlocale_from_collation(coll), Ok(l) if l.deterministic)
}

// LIKE-specific non-erroring gates: the deterministic matcher errors only on
// a pattern-final escape ("LIKE pattern must not end with escape character"),
// recursion depth (one frame per '%'), and the unported non-UTF8 multibyte
// arm — all pure functions of pattern + encoding, provable here.
fn like_pattern_safe(pat: &[u8]) -> bool {
    if mbutils::pg_database_encoding_max_length() != 1
        && mbutils::GetDatabaseEncoding() != wchar::PG_UTF8
    {
        return false;
    }
    let mut percents = 0usize;
    let mut i = 0usize;
    while i < pat.len() {
        match pat[i] {
            b'\\' => {
                if i + 1 >= pat.len() {
                    return false;
                }
                i += 2;
            }
            b'%' => {
                percents += 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    percents <= LIKE_MAX_PERCENTS
}

/// Payload bytes of an inline varlena datum (1B-short or 4B-uncompressed);
/// None for external/compressed images (fail closed — a toasted pattern
/// const refuses translation, and pgrcolumnar lanes never publish one).
pub fn inline_varlena_payload<'a>(d: Datum) -> Option<&'a [u8]> {
    let p = d.as_usize() as *const u8;
    if p.is_null() {
        return None;
    }
    // SAFETY: a non-null text datum points at a live varlena image readable
    // through its header (const arg slot / pgrcolumnar-published lane datum).
    unsafe {
        if varatt::varatt_is_1b_e(p) {
            return None;
        }
        if varatt::varatt_is_1b(p) {
            return Some(core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            ));
        }
        if !varatt::varatt_is_4b_u(p) {
            return None;
        }
        Some(core::slice::from_raw_parts(
            p.add(varatt::VARHDRSZ),
            varatt::varsize_4b(p) - varatt::VARHDRSZ,
        ))
    }
}

#[cold]
#[inline(never)]
pub fn non_inline_lane_datum() -> Box<PgError> {
    PgError::error("lane dict tier: non-inline varlena in a text lane").into()
}

// One predicate evaluation: the byte kernel when the pattern classified
// (byte-exact by the LikeKernel argument), else the production predicate.
// Arg order per the original clause (Eq/Ne are symmetric so commutation
// needs no swap; commuted LIKE/ILIKE refused above). Fields are passed
// split so callers can hold a disjoint borrow of the memo.
fn eval_pred(
    op: DictPredOp,
    kernel: Option<&LikeKernel>,
    arena: Option<&MemoryContext>,
    ic: &mut IcScratch,
    s: &[u8],
    konst: &[u8],
    coll: Oid,
) -> PgResult<bool> {
    if let Some(k) = kernel {
        return Ok(k.matches(s) != op.negated());
    }
    match op {
        DictPredOp::Like => adt_like::textlike(s, konst, coll),
        DictPredOp::NotLike => adt_like::textnlike(s, konst, coll),
        DictPredOp::Eq => varlena::texteq(s, konst, coll),
        DictPredOp::Ne => varlena::textne(s, konst, coll),
        DictPredOp::ILike => adt_like::texticlike(arena.unwrap().mcx(), s, konst, coll, ic),
        DictPredOp::NotILike => adt_like::texticnlike(arena.unwrap().mcx(), s, konst, coll, ic),
        DictPredOp::Regex | DictPredOp::NotRegex | DictPredOp::IcRegex | DictPredOp::NotIcRegex => {
            Ok(regex_match(op, arena.unwrap(), s, konst, coll)? != op.negated())
        }
    }
}

// One production match (textregexeq's RE_compile_and_execute path — Spencer
// always; the regex_engine GUC dispatches only replace_text_regexp) with the
// wchar conversion scratch in the clause arena (reset at epoch change).
fn regex_match(
    op: DictPredOp,
    arena: &MemoryContext,
    s: &[u8],
    pattern: &[u8],
    coll: Oid,
) -> PgResult<bool> {
    regexp_seams::RE_compile_and_execute::call(
        arena.mcx(),
        pattern,
        s,
        op.regex_cflags(),
        coll,
        &mut [],
    )
}

/// Evaluate the translated dict clauses over the staged batch, ANDing into
/// `sv`. Clause order preserved; every clause is non-erroring by admission,
/// so cross-clause reordering against the int tier is unobservable (the
/// increment-1 legality argument). DictCoded lanes memoize per (dict, epoch);
/// windows the AM answered Raw evaluate the same predicate per surviving row
/// over the filled Datum lane — bytes identical to the per-row drive.
pub(crate) fn eval_dict_clauses(
    clauses: &mut [DictClause],
    soa: &SoaBatch<'_>,
    nrows: u32,
    sv: &mut SelVec,
) -> PgResult<()> {
    for cl in clauses.iter_mut() {
        if sv.count() == 0 {
            return Ok(());
        }
        match soa.dict_lane(cl.col as usize) {
            Some(lane) => eval_dict_lane(cl, lane, nrows, sv)?,
            None => eval_raw_rows(cl, soa, nrows, sv)?,
        }
    }
    Ok(())
}

fn eval_dict_lane(
    cl: &mut DictClause,
    lane: SoaDictLane,
    nrows: u32,
    sv: &mut SelVec,
) -> PgResult<()> {
    // SAFETY: SoaDictLane contract — codes covers the staged window's rows,
    // dict covers ndict entries, both valid while the window is staged.
    let (codes, dict) = unsafe {
        (
            core::slice::from_raw_parts(lane.codes, nrows as usize),
            core::slice::from_raw_parts(lane.table.dict, lane.table.ndict as usize),
        )
    };
    if cl.op.is_regex() {
        return eval_dict_lane_lazy(cl, lane, codes, dict, sv);
    }
    if cl.memo_epoch != Some(lane.table.epoch) {
        // Whole-dict reads follow (rank binary search / contains sweep /
        // per-entry memo fill): materialize a lazy sub-framed dict up front.
        // Once per epoch — the same total decompress the eager build paid.
        lane.table.ensure_all();
        cl.range = if lane.table.sorted { prefix_code_range(cl, dict)? } else { None };
        if cl.range.is_some() {
            if cl.memo_epoch.is_none() {
                crate::log_dict_range(cl.col, lane.table.ndict);
            }
            cl.memo.clear();
        } else {
            if cl.memo_epoch.is_none() {
                crate::log_dict_memo(cl.col, lane.table.ndict);
            }
            cl.memo.clear();
            // Contains-kernel fast fill (q22fix2): ONE memmem sweep over the
            // dict arena replaces the per-entry finder calls below, when the
            // dict's images prove ascending-contiguous and all-inline. A
            // false return leaves the memo cleared and the per-entry loop
            // (identical verdicts, identical first error) runs instead.
            let swept = match &cl.kernel {
                Some(LikeKernel::Contains(f)) => fill_memo_contains_sweep(
                    f,
                    dict,
                    lane.table.contig,
                    cl.op.negated(),
                    &mut cl.memo,
                ),
                _ => false,
            };
            if swept {
                if !cl.sweep_logged {
                    cl.sweep_logged = true;
                    crate::log_dict_sweep(cl.col, lane.table.ndict);
                }
            } else {
                cl.memo.clear();
                cl.memo.reserve(dict.len());
                for &d in dict {
                    let s = inline_varlena_payload(d).ok_or_else(non_inline_lane_datum)?;
                    cl.memo.push(eval_pred(
                        cl.op,
                        cl.kernel.as_ref(),
                        cl.arena.as_ref(),
                        &mut cl.ic,
                        s,
                        &cl.konst,
                        cl.collation,
                    )?);
                }
            }
        }
        cl.memo_epoch = Some(lane.table.epoch);
    }
    let snapshot = sv.clone();
    if let Some((lo, hi)) = cl.range {
        let neg = cl.op.negated();
        for i in snapshot.iter() {
            let c = codes[i as usize];
            if ((lo..hi).contains(&c)) == neg {
                sv.clear(i);
            }
        }
        return Ok(());
    }
    for i in snapshot.iter() {
        // Dict lanes are NULL-free by contract (pgrcolumnar S2); strict-NULL
        // handling returns with nullable dict lanes.
        if !cl.memo[codes[i as usize] as usize] {
            sv.clear(i);
        }
    }
    Ok(())
}

// Lazy per-code memo for the regex ops: tri-state, filled on the first
// surviving row that reaches a code — pg_regexec may error, so the
// evaluation set must be exactly the per-row drive's (values of surviving
// rows, in row order; the first raise aborts at the same row).
fn eval_dict_lane_lazy(
    cl: &mut DictClause,
    lane: SoaDictLane,
    codes: &[u32],
    dict: &[Datum],
    sv: &mut SelVec,
) -> PgResult<()> {
    if cl.memo_epoch != Some(lane.table.epoch) {
        if cl.memo_epoch.is_none() {
            crate::log_dict_memo(cl.col, lane.table.ndict);
        }
        cl.memo_tri.clear();
        cl.memo_tri.resize(dict.len(), 0u8);
        if let Some(a) = cl.arena.as_mut() {
            a.reset();
        }
        cl.memo_epoch = Some(lane.table.epoch);
    }
    let neg = cl.op.negated();
    let snapshot = sv.clone();
    for i in snapshot.iter() {
        let code = codes[i as usize] as usize;
        let m = match cl.memo_tri[code] {
            1 => true,
            2 => false,
            _ => {
                // Lazy sub-framed dict: materialize this code's entry bytes
                // (the regex memo deliberately avoids whole-dict fills).
                lane.table.ensure_code(code as u32);
                let s = inline_varlena_payload(dict[code]).ok_or_else(non_inline_lane_datum)?;
                let m = regex_match(
                    cl.op,
                    cl.arena.as_ref().expect("regex clauses carry an arena"),
                    s,
                    &cl.konst,
                    cl.collation,
                )?;
                cl.memo_tri[code] = if m { 1 } else { 2 };
                m
            }
        };
        if m == neg {
            sv.clear(i);
        }
    }
    Ok(())
}

/// Contains-kernel memo fill by ONE memmem sweep over the dictionary arena
/// (q22fix2; the `eval_contains_blob` shape over dict entries instead of row
/// values). Engages only when (0) the PUBLISHER witnessed the images'
/// readability (`contig` — SoaDictTable's whole-span witness; ascending
/// pointers alone never establish one allocation: separate allocations
/// ascend by accident and the sweep then reads the foreign gap between
/// them — GL-TESTFIX-1 F-R1-1, ASan heap-buffer-overflow), and the dict's
/// images prove (a) strictly ascending pointers — writer layout order —
/// and (b) all inline (1B-short / 4B-U): the per-entry loop raises
/// `non_inline_lane_datum` on ANY non-inline entry (eager fill), so the
/// sweep must refuse whenever one exists anywhere, not only when it owns a
/// hit. Under (0)+(a)+(b) the span between consecutive image starts is
/// readable (same allocation) and the hit→entry mapping is the blob
/// pass's: a hit fully inside entry e's payload marks e and resumes at
/// entry e+1's image start; header/padding/straddle hits resume one byte
/// later. An entry is marked iff its payload contains the needle — exactly
/// the per-entry kernel verdict — and `memo[e] = matched != negated`
/// matches `eval_pred`'s kernel arm. True = memo fully written (len ==
/// dict.len()); false = memo untouched beyond a possible resize, caller
/// must clear + run the per-entry loop.
fn fill_memo_contains_sweep(
    f: &SubstrFinder,
    dict: &[Datum],
    contig: bool,
    negated: bool,
    memo: &mut Vec<bool>,
) -> bool {
    let n = dict.len();
    if n == 0 || !contig {
        return false;
    }
    // Ascending + all-inline proof pass (pointer compares + one header byte
    // per entry; refusal keeps the per-entry loop byte-identical, including
    // its first-non-inline error).
    let base = dict[0].as_usize();
    let mut prev = 0usize;
    for d in dict {
        let p = d.as_usize();
        if p < base + prev || inline_varlena_payload(*d).is_none() {
            return false;
        }
        prev = p - base + 1;
    }
    // Span end: the last (highest) image's payload end.
    let last = inline_varlena_payload(dict[n - 1]).expect("proven inline above");
    let len = last.as_ptr() as usize + last.len() - base;
    // SAFETY: the publisher's `contig` witness guarantees every image lies
    // in ONE readable allocation, and the ascending pass above placed
    // [base, base+len) between the first and last image of that
    // allocation; readable for the dict's lifetime (staged-window
    // contract).
    let hay = unsafe { core::slice::from_raw_parts(base as *const u8, len) };
    let nlen = f.needle_len();
    memo.clear();
    memo.resize(n, negated);
    let mut e = 0usize;
    let mut pos = 0usize;
    while pos < hay.len() {
        let Some(h) = f.find_pos(&hay[pos..]) else { break };
        let h = pos + h;
        // Owning entry: the last entry whose image starts at or before the hit.
        while e + 1 < n && dict[e + 1].as_usize() - base <= h {
            e += 1;
        }
        let payload = inline_varlena_payload(dict[e]).expect("proven inline above");
        let pstart = payload.as_ptr() as usize - base;
        let pend = pstart + payload.len();
        if h >= pstart && h + nlen <= pend {
            memo[e] = !negated;
            e += 1;
            if e >= n {
                break;
            }
            pos = dict[e].as_usize() - base;
        } else {
            pos = h + 1;
        }
    }
    true
}

// Sorted-dict prefix range: matching entries are the contiguous rank run
// [lo, hi); both boundary predicates are monotone over the byte-sorted dict
// (no successor-prefix computation, so no 0xFF edge). Non-erroring beyond
// the memo path's non-inline check by kernel admission.
fn prefix_code_range(cl: &DictClause, dict: &[Datum]) -> PgResult<Option<(u32, u32)>> {
    if !matches!(cl.op, DictPredOp::Like | DictPredOp::NotLike) {
        return Ok(None);
    }
    let Some(LikeKernel::Prefix(p)) = &cl.kernel else { return Ok(None) };
    debug_assert!(!p.is_empty(), "empty prefix classifies as Any/Exact, never Prefix");
    let lo = dict_partition_point(dict, |s| s < &p[..])?;
    let hi = dict_partition_point(dict, |s| s < &p[..] || s.starts_with(p))?;
    debug_assert!(lo <= hi && hi as usize <= dict.len());
    Ok(Some((lo, hi)))
}

fn dict_partition_point(dict: &[Datum], pred: impl Fn(&[u8]) -> bool) -> PgResult<u32> {
    let (mut lo, mut hi) = (0usize, dict.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let s = inline_varlena_payload(dict[mid]).ok_or_else(non_inline_lane_datum)?;
        if pred(s) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo as u32)
}

// Raw-window fallback (RawText/Lz4Text row groups, or a dict RG the AM chose
// not to tag): the same production predicate per SURVIVING row — evaluation
// set and order match the per-row drive for these rows, and the predicate is
// non-erroring by admission, so eager evaluation stays unobservable.
fn eval_raw_rows(
    cl: &mut DictClause,
    soa: &SoaBatch<'_>,
    nrows: u32,
    sv: &mut SelVec,
) -> PgResult<()> {
    let values = soa.col_values(cl.col as usize);
    let isnull = soa.col_isnull(cl.col as usize);
    // Blob-wide contains (likeband; the strsearch parity note's measured-best
    // shape, 0.64-0.94x of CH's Volnitsky): when the AM published a
    // contiguity witness for this window, run ONE memmem pass over the whole
    // span and map hits back to rows, instead of one finder call per row.
    // Engaged only on full-selection windows — the single-clause LIKE band
    // shape (selective text-qual class); under a partial selection the per-row loop
    // touches fewer bytes than the blob would. Occurrence set is identical
    // to the per-row kernel (hits are validated against the owning row's
    // payload bounds; boundary-straddling and header/padding hits resume at
    // +1, per the parity note's boundary-rejection requirement).
    if sv.count() == nrows {
        if let (Some(LikeKernel::Contains(f)), Some(span)) =
            (&cl.kernel, soa.text_span(cl.col as usize))
        {
            if eval_contains_blob(f, span, values, isnull, cl.op.negated(), nrows, sv) {
                if !cl.blob_logged {
                    cl.blob_logged = true;
                    crate::log_blob_kernel(cl.col);
                }
                return Ok(());
            }
        }
    }
    let snapshot = sv.clone();
    for i in snapshot.iter() {
        let idx = i as usize;
        if isnull[idx] {
            sv.clear(i);
            continue;
        }
        let s = inline_varlena_payload(values[idx]).ok_or_else(non_inline_lane_datum)?;
        if !eval_pred(
            cl.op,
            cl.kernel.as_ref(),
            cl.arena.as_ref(),
            &mut cl.ic,
            s,
            &cl.konst,
            cl.collation,
        )? {
            sv.clear(i);
        }
    }
    Ok(())
}

/// One blob-wide memmem pass over a contiguous text-span window, mapping
/// hits back to rows through the ascending pointer lane. True = the clause
/// is fully answered into `sv`; false = bail (a non-inline image — never
/// published by pgrcolumnar — was met before proving a row), and the per-row
/// loop must run instead (raising its own error at the identical first row).
///
/// Identity argument (byte-exact vs the per-row kernel): memmem yields the
/// leftmost occurrence at/after `pos`. A hit fully inside row r's payload
/// marks r and resumes at row r+1's image start — skipping only bytes of a
/// row already proven. A hit touching a header/padding byte or straddling a
/// row boundary resumes one byte later — skipping nothing else. So the scan
/// visits every occurrence that starts inside any row's payload, i.e. a row
/// is marked iff its payload contains the needle — exactly `k.matches(s)`.
fn eval_contains_blob(
    f: &SubstrFinder,
    span: SoaTextSpan,
    values: &[Datum],
    isnull: &[bool],
    negated: bool,
    nrows: u32,
    sv: &mut SelVec,
) -> bool {
    let n = nrows as usize;
    if n == 0 {
        return true;
    }
    let base = span.base as usize;
    // SAFETY: SoaTextSpan contract — one live readable span holding the
    // window's images; values are ascending pointers into it.
    let hay = unsafe { core::slice::from_raw_parts(span.base, span.len) };
    let nlen = f.needle_len();
    let mut hit = [0u64; crate::interp::SEL_WORDS];
    let mut row = 0usize;
    let mut pos = 0usize;
    while pos < hay.len() {
        let Some(h) = f.find_pos(&hay[pos..]) else { break };
        let h = pos + h;
        // Owning row: the last row whose image starts at or before the hit.
        while row + 1 < n && values[row + 1].as_usize() - base <= h {
            row += 1;
        }
        let Some(payload) = inline_varlena_payload(values[row]) else {
            return false;
        };
        let pstart = payload.as_ptr() as usize - base;
        let pend = pstart + payload.len();
        if h >= pstart && h + nlen <= pend {
            hit[row / 64] |= 1u64 << (row % 64);
            row += 1;
            if row >= n {
                break;
            }
            pos = values[row].as_usize() - base;
        } else {
            pos = h + 1;
        }
    }
    // Strict clause semantics over the (full) selection: NULL fails; else
    // keep iff matched != negated.
    let snapshot = sv.clone();
    for i in snapshot.iter() {
        let idx = i as usize;
        if isnull[idx] {
            sv.clear(i);
            continue;
        }
        let matched = hit[idx / 64] & (1u64 << (idx % 64)) != 0;
        if matched == negated {
            sv.clear(i);
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape_of(pat: &[u8]) -> Option<&'static str> {
        like_kernel_for(pat).map(|k| k.shape())
    }

    // Build a pgrcolumnar-shaped text blob: complete 4B-U varlena images,
    // back-to-back, 4-byte aligned (the writer's push_varlena_image layout),
    // plus the ascending pointer lane into it.
    fn blob_of(rows: &[&[u8]]) -> (Vec<u8>, Vec<usize>) {
        let mut blob = Vec::new();
        let mut offs = Vec::new();
        for r in rows {
            offs.push(blob.len());
            blob.extend_from_slice(&::datum::set_varsize_4b(4 + r.len()));
            blob.extend_from_slice(r);
            while blob.len() % 4 != 0 {
                blob.push(0);
            }
        }
        (blob, offs)
    }

    fn run_blob(rows: &[&[u8]], needle: &[u8], negated: bool) -> Vec<bool> {
        let (blob, offs) = blob_of(rows);
        let values: Vec<Datum> =
            offs.iter().map(|&o| Datum::from_usize(blob.as_ptr() as usize + o)).collect();
        let isnull = vec![false; rows.len()];
        let span = SoaTextSpan { base: blob.as_ptr(), len: blob.len() };
        let f = SubstrFinder::new(needle.to_vec());
        let n = rows.len() as u32;
        let mut sv = SelVec::all(n);
        assert!(eval_contains_blob(&f, span, &values, &isnull, negated, n, &mut sv));
        (0..n).map(|i| sv.contains(i)).collect()
    }

    fn run_dict_sweep(entries: &[&[u8]], needle: &[u8], negated: bool) -> Option<Vec<bool>> {
        let (blob, offs) = blob_of(entries);
        let dict: Vec<Datum> =
            offs.iter().map(|&o| Datum::from_usize(blob.as_ptr() as usize + o)).collect();
        let f = SubstrFinder::new(needle.to_vec());
        let mut memo = Vec::new();
        // contig: the helper's blob IS one Vec allocation.
        fill_memo_contains_sweep(&f, &dict, true, negated, &mut memo).then_some(memo)
    }

    #[test]
    fn dict_sweep_matches_per_entry_fill() {
        let entries: &[&[u8]] =
            &[b"hello world", b"", b"worldly", b"say hell", b"w", b"aworld", b"world"];
        for negated in [false, true] {
            let memo = run_dict_sweep(entries, b"world", negated).expect("contiguous dict sweeps");
            let expect: Vec<bool> = entries
                .iter()
                .map(|e| {
                    LikeKernel::Contains(SubstrFinder::new(b"world".to_vec())).matches(e)
                        != negated
                })
                .collect();
            assert_eq!(memo, expect);
        }
    }

    #[test]
    fn dict_sweep_rejects_boundary_straddle() {
        // Same construction as blob_rejects_boundary_straddle: the needle
        // occurs across entry 0's tail + entry 1's header byte, inside no
        // entry's payload.
        let e1 = [b'z'; 21];
        let entries: &[&[u8]] = &[b"xxab", &e1];
        assert_eq!(run_dict_sweep(entries, b"ab\x64", false).unwrap(), [false, false]);
        assert_eq!(run_dict_sweep(entries, b"ab", false).unwrap(), [true, false]);
    }

    #[test]
    fn dict_sweep_refuses_non_ascending() {
        let (blob, offs) = blob_of(&[b"aaa", b"bbb"]);
        let mut dict: Vec<Datum> =
            offs.iter().map(|&o| Datum::from_usize(blob.as_ptr() as usize + o)).collect();
        dict.swap(0, 1);
        let f = SubstrFinder::new(b"a".to_vec());
        let mut memo = Vec::new();
        assert!(!fill_memo_contains_sweep(&f, &dict, true, false, &mut memo));
    }

    /// F-R1-1 pin: WITHOUT the publisher witness the sweep must refuse, no
    /// matter how contiguous the images happen to look — ascending pointers
    /// across separate allocations are an accident, not a readability proof.
    #[test]
    fn dict_sweep_refuses_without_contig_witness() {
        let (blob, offs) = blob_of(&[b"aaa", b"bbb"]);
        let dict: Vec<Datum> =
            offs.iter().map(|&o| Datum::from_usize(blob.as_ptr() as usize + o)).collect();
        let f = SubstrFinder::new(b"a".to_vec());
        let mut memo = Vec::new();
        assert!(!fill_memo_contains_sweep(&f, &dict, false, false, &mut memo));
    }

    #[test]
    fn dict_sweep_differential_vs_per_entry() {
        // Deterministic pseudo-random dicts: sweep memo must equal the
        // per-entry kernel fill for every entry, needle, and polarity.
        let mut seed = 0xdeadbeefcafef00du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet = b"abcd";
        for _case in 0..200 {
            let n = (next() % 17) as usize + 1;
            let entries: Vec<Vec<u8>> = (0..n)
                .map(|_| {
                    let len = (next() % 24) as usize;
                    (0..len).map(|_| alphabet[(next() % 4) as usize]).collect()
                })
                .collect();
            let nlen = (next() % 5) as usize + 1;
            let needle: Vec<u8> = (0..nlen).map(|_| alphabet[(next() % 4) as usize]).collect();
            let refs: Vec<&[u8]> = entries.iter().map(|r| r.as_slice()).collect();
            let negated = next() % 2 == 0;
            let memo = run_dict_sweep(&refs, &needle, negated).expect("sweeps");
            let k = LikeKernel::Contains(SubstrFinder::new(needle.clone()));
            let expect: Vec<bool> = refs.iter().map(|e| k.matches(e) != negated).collect();
            assert_eq!(memo, expect, "needle {needle:?} entries {entries:?}");
        }
    }

    #[test]
    fn blob_contains_basic() {
        let rows: &[&[u8]] = &[b"hello world", b"", b"worldly", b"say hell", b"w", b"aworld"];
        assert_eq!(run_blob(rows, b"world", false), [true, false, true, false, false, true]);
        assert_eq!(run_blob(rows, b"world", true), [false, true, false, true, true, false]);
    }

    #[test]
    fn blob_rejects_boundary_straddle() {
        // Row 1's 4B-U header first byte is (21+4)<<2 & 0xFF = 100 = b'd':
        // the needle "ab\x64" occurs contiguously in the BLOB (row 0's tail
        // "ab" + row 1's header byte) but inside no row's payload — the
        // per-row kernel finds nothing, and the blob pass must agree.
        let row1 = [b'z'; 21];
        let rows: &[&[u8]] = &[b"xxab", &row1];
        assert_eq!(run_blob(rows, b"ab\x64", false), [false, false]);
        // Control: the same tail needle fully inside row 0 still hits.
        assert_eq!(run_blob(rows, b"ab", false), [true, false]);
    }

    #[test]
    fn blob_resumes_into_next_row() {
        // Adjacent hits: accepting row r must not skip an occurrence in r+1.
        let rows: &[&[u8]] = &[b"needle needle", b"needle", b"x", b"needle"];
        assert_eq!(run_blob(rows, b"needle", false), [true, true, false, true]);
    }

    #[test]
    fn blob_matches_perrow_kernel_differential() {
        // Deterministic pseudo-random corpus: the blob verdict must equal
        // the per-row finder verdict on every row, for every needle.
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet = b"abcd";
        for _case in 0..200 {
            let nrows = (next() % 17) as usize + 1;
            let rows: Vec<Vec<u8>> = (0..nrows)
                .map(|_| {
                    let len = (next() % 24) as usize;
                    (0..len).map(|_| alphabet[(next() % 4) as usize]).collect()
                })
                .collect();
            let nlen = (next() % 5) as usize + 1;
            let needle: Vec<u8> = (0..nlen).map(|_| alphabet[(next() % 4) as usize]).collect();
            let refs: Vec<&[u8]> = rows.iter().map(|r| r.as_slice()).collect();
            let f = SubstrFinder::new(needle.clone());
            let want: Vec<bool> = rows.iter().map(|r| f.find(r)).collect();
            assert_eq!(run_blob(&refs, &needle, false), want, "needle={needle:?} rows={rows:?}");
        }
    }

    #[test]
    fn classifier_shapes() {
        assert_eq!(shape_of(b"abc"), Some("exact"));
        assert_eq!(shape_of(b""), Some("exact"));
        assert_eq!(shape_of(b"abc%"), Some("prefix"));
        assert_eq!(shape_of(b"%abc"), Some("suffix"));
        assert_eq!(shape_of(b"%abc%"), Some("contains"));
        assert_eq!(shape_of(b"%%abc%%"), Some("contains"));
        assert_eq!(shape_of(b"ab%cd"), Some("prefix+suffix"));
        assert_eq!(shape_of(b"%"), Some("any"));
        assert_eq!(shape_of(b"%%"), Some("any"));
        assert_eq!(shape_of(b"a_c"), None);
        assert_eq!(shape_of(b"a%b%c"), None);
        assert_eq!(shape_of(b"%a%b"), None);
        assert_eq!(shape_of(b"a%b%"), None);
        // Escapes resolve to literals before classification.
        assert_eq!(shape_of(b"a\\%b"), Some("exact"));
        assert_eq!(shape_of(b"a\\_c%"), Some("prefix"));
        assert_eq!(shape_of(b"ab\\%"), Some("exact"));
    }

    #[test]
    fn classifier_matches() {
        let cases: &[(&[u8], &[u8], bool)] = &[
            (b"abc", b"abc", true),
            (b"abc", b"abcd", false),
            (b"", b"", true),
            (b"", b"x", false),
            (b"abc%", b"abc", true),
            (b"abc%", b"abcz", true),
            (b"abc%", b"zabc", false),
            (b"%abc", b"zabc", true),
            (b"%abc", b"abcz", false),
            (b"%abc%", b"zzabczz", true),
            (b"%abc%", b"ab", false),
            (b"%abc%", b"abc", true),
            (b"ab%cd", b"abcd", true),
            (b"ab%cd", b"abzzcd", true),
            (b"ab%cd", b"abd", false),
            // Overlap: the %-gap may be empty but never negative.
            (b"ab%bc", b"abc", false),
            (b"ab%bc", b"abbc", true),
            (b"%", b"", true),
            (b"%", b"anything", true),
            (b"a\\%b", b"a%b", true),
            (b"a\\%b", b"axb", false),
        ];
        for &(pat, s, want) in cases {
            let k = like_kernel_for(pat).expect("classifiable");
            assert_eq!(k.matches(s), want, "pat={:?} s={:?}", pat, s);
        }
    }

    #[test]
    fn kernel_vs_production_exhaustive() {
        // Every pattern over {a,b,%,\_,\\} up to length 4 that classifies,
        // against adt_like::textlike (C collation) on every string over
        // {a,b} up to length 5. Byte-exactness of the kernel is load-bearing
        // for the qual bitmap; this pins it over the whole small universe.
        let pat_alpha: &[u8] = b"ab%_\\";
        let str_alpha: &[u8] = b"ab";
        let mut pats: Vec<Vec<u8>> = vec![Vec::new()];
        for _ in 0..4 {
            let prev = pats.clone();
            for p in prev {
                for &c in pat_alpha {
                    let mut q = p.clone();
                    q.push(c);
                    pats.push(q);
                }
            }
            pats.dedup();
        }
        let mut strs: Vec<Vec<u8>> = vec![Vec::new()];
        for _ in 0..5 {
            let prev = strs.clone();
            for s in prev {
                for &c in str_alpha {
                    let mut q = s.clone();
                    q.push(c);
                    strs.push(q);
                }
            }
            strs.dedup();
        }
        for pat in &pats {
            if !like_pattern_safe(pat) {
                continue;
            }
            let Some(k) = like_kernel_for(pat) else { continue };
            for s in &strs {
                let want = adt_like::textlike(s, pat, C_COLLATION_OID).unwrap();
                assert_eq!(k.matches(s), want, "pat={:?} s={:?}", pat, s);
            }
        }
    }

    #[test]
    fn substr_finder_edges() {
        let f = SubstrFinder::new(b"aab".to_vec());
        assert!(f.find(b"aaab"));
        assert!(f.find(b"aab"));
        assert!(!f.find(b"aa"));
        assert!(!f.find(b""));
        assert!(f.find(b"xyzaab"));
        assert!(!f.find(b"aba aba"));
        let g = SubstrFinder::new(b"g".to_vec());
        assert!(g.find(b"zag"));
        assert!(!g.find(b"za"));
    }
}
