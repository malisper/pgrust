// SoA page-batch deform (upstream batch executor design, CF 6176). INVARIANT:
// soa_store_prefix leaves the slot exactly as slot_getsomeattrs(ncols) would
// (resume offset + SLOW flag), so lazy deform past the prefix is unchanged.
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::AttrNumber;
use ::types_slot::{SlotData, TTS_FLAG_SLOW};
use ::types_storage::bufpage::MaxHeapTuplesPerPage;
use ::types_tuple::tupmacs::{
    att_addlength_pointer, att_isnull, att_nominal_alignby, att_pointer_alignby, fetch_att,
};
use ::types_tuple::{CompactAttribute, HeapTupleData, SizeofHeapTupleHeader};

pub const SOA_MAX_ROWS: usize = MaxHeapTuplesPerPage;
pub const SOA_BM_WORDS: usize = SOA_MAX_ROWS.div_ceil(64);

/// Visit the positions of `pos..n` whose bit is SET in `live`, ascending;
/// `live = None` visits every position (the dense loop, unchanged codegen).
/// WORD-GRANULAR SKIP (the qualed-top-n sort-feed lever, one shared impl):
/// an all-clear word advances 64 positions in one compare, and set bits
/// within a sparse word iterate by trailing-zeros — so a batched loop whose
/// per-position work is nothing on cleared bits (the caller's contract: a
/// cleared bit is a definitive rejection with no observable effect) collapses
/// its per-row ceremony to one word test per 64 rows on sparse selections,
/// at ~one extra compare per word on dense ones.
///
/// The visited stream is EXACTLY the plain loop's surviving positions in the
/// same order (differential-tested); an `Err` from `f` stops the walk at that
/// position, exactly as the plain loop's `?` would. Bits at or past `n` are
/// ignored (the last word is masked as a belt — producers stage zeros there
/// by contract); when `Some`, `live` must cover bit `n - 1`.
#[inline(always)]
pub fn for_each_live<E>(
    live: Option<&[u64]>,
    pos: u32,
    n: u32,
    mut f: impl FnMut(u32) -> Result<(), E>,
) -> Result<(), E> {
    let Some(words) = live else {
        for i in pos..n {
            f(i)?;
        }
        return Ok(());
    };
    if pos >= n {
        return Ok(());
    }
    let last = ((n - 1) / 64) as usize;
    debug_assert!(words.len() > last, "live words must cover bit n-1");
    let mut w = (pos / 64) as usize;
    // Mask off bits below `pos` in the first word; bits at/past `n` are
    // zero by the producer's contract (belt: mask the last word anyway).
    let mut bits = words[w] & (!0u64 << (pos % 64));
    loop {
        if w == last && n % 64 != 0 {
            bits &= (1u64 << (n % 64)) - 1;
        }
        while bits != 0 {
            let i = (w as u32) * 64 + bits.trailing_zeros();
            bits &= bits - 1;
            f(i)?;
        }
        if w == last {
            return Ok(());
        }
        w += 1;
        bits = words[w];
    }
}

/// Single-body variant of [`for_each_live`]: the SAME visited stream (`None`
/// = every position), but the dense case runs through the same word loop
/// with all-set words — so the caller's row body is instantiated ONCE, not
/// duplicated into separate dense and sparse loop arms. Use this when `f`
/// is large and the dense case is hot: duplicating a big row body into two
/// arms measurably regressed the hashgroup span accept's unfiltered leg
/// (narrow-sort DISTINCT shapes +9%), while the all-set word walk costs ~2 extra ops per row.
#[inline(always)]
pub fn for_each_live_onebody<E>(
    live: Option<&[u64]>,
    pos: u32,
    n: u32,
    mut f: impl FnMut(u32) -> Result<(), E>,
) -> Result<(), E> {
    if pos >= n {
        return Ok(());
    }
    let last = ((n - 1) / 64) as usize;
    debug_assert!(live.is_none_or(|w| w.len() > last), "live words must cover bit n-1");
    #[inline(always)]
    fn word_at(live: Option<&[u64]>, w: usize) -> u64 {
        match live {
            Some(s) => s[w],
            None => !0u64,
        }
    }
    let mut w = (pos / 64) as usize;
    // Mask off bits below `pos` in the first word; bits at/past `n` are
    // masked at the last word (dense words need the mask; bitmap producers
    // stage zeros there by contract — the mask is their belt).
    let mut bits = word_at(live, w) & (!0u64 << (pos % 64));
    loop {
        if w == last && n % 64 != 0 {
            bits &= (1u64 << (n % 64)) - 1;
        }
        while bits != 0 {
            let i = (w as u32) * 64 + bits.trailing_zeros();
            bits &= bits - 1;
            f(i)?;
        }
        if w == last {
            return Ok(());
        }
        w += 1;
        bits = word_at(live, w);
    }
}

/// Fixed-width prefix plan: every column below `ncols` has `attlen > 0`.
///
/// WALK-TAIL VARIANT (AGGSEQ-STAGE, the heap grouped staging seam): a plan
/// built by [`SoaDeformPlan::try_new_walk`] additionally carries
/// `walk_from = Some(w)` — columns `0..w` (the maximal fixed-width HEAD)
/// keep the static offset chain and deform column-major exactly as today,
/// and columns `w..ncols` (`w` = the first `attlen <= 0` attribute) deform
/// per row via [`soa_deform_walk_tail`], because a column PAST a varlena
/// has a row-dependent offset (the varlena's length varies per row) and no
/// static-offset deform exists for it. For walk plans `end_off` is the
/// HEAD end (the walk pass's start offset), not the row end — the walk
/// writes the per-row `end_off`.
pub struct SoaDeformPlan<'mcx> {
    ncols: u16,
    end_off: u32,
    offs: PgVec<'mcx, u32>,
    /// `Some(w)` = walk-tail plan: `offs` covers `0..w` only and columns
    /// `w..ncols` deform per row. `None` = every plan shipped before
    /// AGGSEQ-STAGE (fixed-width prefix / columnar / varkey-unused).
    walk_from: Option<u16>,
    // Deform-JIT batch kernel (docs/optimizations/jit-deform.md): replaces
    // the AOT column pass on dense full-prefix batches when armed.
    jit: Option<alloc::rc::Rc<jit_deform::DeformKernel>>,
}

impl<'mcx> SoaDeformPlan<'mcx> {
    pub fn try_new(
        mcx: Mcx<'mcx>,
        atts: &[CompactAttribute],
        ncols: usize,
    ) -> Option<SoaDeformPlan<'mcx>> {
        if ncols == 0 || ncols > atts.len() || ncols > u16::MAX as usize {
            return None;
        }
        let mut offs = ::mcx::vec_with_capacity_in_infallible(mcx, ncols);
        let mut off = 0usize;
        for att in &atts[..ncols] {
            if att.attlen <= 0 {
                return None;
            }
            off = att_nominal_alignby(off, att.attalignby);
            offs.push(off as u32);
            off += att.attlen as usize;
        }
        Some(SoaDeformPlan {
            ncols: ncols as u16,
            end_off: off as u32,
            offs,
            walk_from: None,
            jit: None,
        })
    }

    /// Walk-tail plan (AGGSEQ-STAGE): host a heap prefix that CROSSES a
    /// varlena column — the shape [`SoaDeformPlan::try_new`] refuses.
    /// Callers try `try_new` FIRST; this returns None when `try_new` would
    /// have succeeded (all fixed-width — no walk needed), when the prefix
    /// carries a cstring (`attlen == -2`, the varkey plan's own refusal —
    /// `att_addlength_pointer` on cstrings strlen-walks and no live shape
    /// needs it) or any other non-positive `attlen != -1`, or when the ask
    /// is empty/oversized.
    pub fn try_new_walk(
        mcx: Mcx<'mcx>,
        atts: &[CompactAttribute],
        ncols: usize,
    ) -> Option<SoaDeformPlan<'mcx>> {
        if ncols == 0 || ncols > atts.len() || ncols > u16::MAX as usize {
            return None;
        }
        let wf = atts[..ncols].iter().position(|a| a.attlen <= 0)?;
        if atts[wf..ncols].iter().any(|a| a.attlen <= 0 && a.attlen != -1) {
            return None; // cstring (or unknown non-varlena) inside the prefix
        }
        let mut offs = ::mcx::vec_with_capacity_in_infallible(mcx, wf);
        let mut off = 0usize;
        for att in &atts[..wf] {
            debug_assert!(att.attlen > 0);
            off = att_nominal_alignby(off, att.attalignby);
            offs.push(off as u32);
            off += att.attlen as usize;
        }
        Some(SoaDeformPlan {
            ncols: ncols as u16,
            end_off: off as u32,
            offs,
            walk_from: Some(wf as u16),
            jit: None,
        })
    }

    /// `Some(w)` for walk-tail plans (columns `w..ncols` deform per row).
    #[inline]
    pub fn walk_from(&self) -> Option<u16> {
        self.walk_from
    }

    #[inline]
    pub fn ncols(&self) -> u16 {
        self.ncols
    }

    /// Arm the JIT batch kernel; layout identity with this plan is required
    /// (same ncols and offset chain) so batch output is bit-identical.
    /// Walk-tail plans never arm (the kernel deforms the WHOLE prefix at
    /// static offsets; a walk tail has none).
    pub fn arm_jit(&mut self, k: alloc::rc::Rc<jit_deform::DeformKernel>) {
        debug_assert!(self.walk_from.is_none(), "walk-tail plans never arm the JIT kernel");
        debug_assert!(k.ncols() == self.ncols && k.end_off() == self.end_off);
        self.jit = Some(k);
    }

    /// Placeholder for varkey-mode batches; the varkey pass never reads it.
    pub fn unused(mcx: Mcx<'mcx>) -> SoaDeformPlan<'mcx> {
        SoaDeformPlan { ncols: 0, end_off: 0, offs: PgVec::new_in(mcx), walk_from: None, jit: None }
    }

    /// Columnar-AM plan: `ncols` only, NO offset chain — usable exclusively
    /// with batch fills that ignore tuple offsets (pgrcolumnar's `batch_deform`
    /// stages decoded Datums per column, so varlena columns are stageable
    /// and the fixed-width-prefix restriction does not apply). The heap
    /// deform paths must never see this plan (they index `offs`); callers
    /// install it only on pgrcolumnar scan states, whose `TableScanDesc`
    /// dispatch never reaches the heap deform (`is_virtual` guards it).
    pub fn columnar(mcx: Mcx<'mcx>, ncols: usize) -> Option<SoaDeformPlan<'mcx>> {
        if ncols == 0 || ncols > u16::MAX as usize {
            return None;
        }
        Some(SoaDeformPlan {
            ncols: ncols as u16,
            end_off: 0,
            offs: PgVec::new_in(mcx),
            walk_from: None,
            jit: None,
        })
    }

    /// Alias of [`SoaDeformPlan::columnar`] (likeband's name for the same
    /// virtual, offset-chain-free columnar plan).
    pub fn virtual_prefix(mcx: Mcx<'mcx>, ncols: usize) -> Option<SoaDeformPlan<'mcx>> {
        Self::columnar(mcx, ncols)
    }

    /// True for `columnar`/`virtual_prefix` plans (no offset chain despite
    /// `ncols > 0`). A walk-tail plan whose head is empty (`walk_from == 0`,
    /// a varlena at column 0) also has an empty chain but is NOT virtual —
    /// the explicit `walk_from` disambiguates.
    #[inline]
    pub fn is_virtual(&self) -> bool {
        self.ncols > 0 && self.offs.is_empty() && self.walk_from.is_none()
    }
}

/// Identity + content handle of one per-row-group dictionary (pgrcolumnar dict
/// encoding): decoded text Datums, code = index. The pointer is the storage
/// adapter's and stays valid while the window is staged (until the next
/// batch fill / endscan) — the same lifetime contract as the text Datums the
/// adapter publishes into slots.
///
/// EPOCH DISCIPLINE (phase4 design §2/§8.1): `epoch` is the row-group index
/// within the scan. A scan pins its `Arc<Part>` for its whole lifetime, so the
/// rg-index is unique and rescan-stable per scan — a part-cache refresh can
/// never swap dictionary content under a live scan (a reopen is a new scan,
/// hence a new pin and a fresh epoch space). Consumers key per-code memos on
/// `epoch` and clear them whenever it changes; they never compare pointers.
///
/// BREAKER SURVIVAL (design addendum): this handle is deliberately a small
/// Copy value separate from the per-window codes so a later materialization
/// path (join build narrowing payloads to codes, DuckDB-style) can store one
/// `SoaDictTable` per staged run and re-emit dict lanes on probe. Validation
/// is `same_identity` (epoch match); nothing here prevents carrying it —
/// implementing that plumbing is explicitly out of scope for now.
///
/// CONTRACT: dict-coded columns are NULL-free today (pgrcolumnar stores no
/// NULLs). That is a per-chunk proof the filler asserts by writing
/// `isnull = false` on gather — NOT a type invariant of this struct; the
/// per-lane isnull currency stays so a NULL-capable pgrcolumnar v2 only changes
/// the fillers (phase4 design §8.3).
#[derive(Clone, Copy)]
pub struct SoaDictTable {
    pub dict: *const Datum,
    pub ndict: u32,
    pub epoch: u64,
    /// Dict entries are byte-sorted (codes are rank order) — gates dict
    /// range predicates. False keeps the per-entry memo path.
    pub sorted: bool,
    /// v7 GLOBAL STITCH (part-global dictionaries): local code ->
    /// part-global byte-rank code, `ndict` entries; null when the part
    /// carries no stitch for this column. Global codes are stable across
    /// EVERY epoch of the scan (`gepoch` names that identity), strictly
    /// byte-rank ordered part-wide, and dense in `0..gndv` over the part's
    /// union dict. Consumers keying memos on `(gepoch, global_code)` never
    /// reset at epoch rolls; value materialization still goes through
    /// `dict[local_code]` (the current RG's dict covers every local code of
    /// its windows).
    pub stitch: *const u32,
    /// Part-global dict size (valid iff `stitch` is non-null).
    pub gndv: u32,
    /// Scan-unique stitch identity: stable across every RG (and rescan) of
    /// one pinned scan, distinct across scans. 0 iff `stitch` is null.
    pub gepoch: u64,
    /// LAZY SUB-FRAMED DICTIONARY SEAM (pgrcolumnar CHUNK_FLAG_DICT_FRAMED):
    /// null = every entry's bytes are materialized (the historical
    /// contract). Non-null = `dict[code]` POINTERS are always valid but a
    /// code's entry BYTES exist only after an ensure covering it: `datum()`
    /// / `ensure_code()` ensure per code; any consumer reading entry bytes
    /// OUTSIDE those accessors (whole-dict sweeps, rank binary searches, raw
    /// `dict` slices) must call `ensure_all()` first. Same lifetime as
    /// `dict` (the publisher's staged-window contract).
    pub lazy: *const (),
    pub lazy_ensure: Option<unsafe fn(*const (), u32)>,
    pub lazy_ensure_all: Option<unsafe fn(*const ())>,
    /// Dict-images readability witness (GL-TESTFIX-1 F-R1-1; the dict-tier
    /// twin of [`SoaTextSpan`]'s publisher law): true = the PUBLISHER
    /// guarantees every entry's varlena image lies inside ONE readable
    /// allocation it owns (the decode arena / the mmap'd part payload), so
    /// whole-span readers (the contains-memo memmem sweep) may form one
    /// slice over [first image, last image end) after re-verifying
    /// ascending+inline. Ascending pointers alone can NEVER establish this
    /// — separate allocations ascend by accident, and a sweep over the gap
    /// reads foreign memory (ASan heap-buffer-overflow, F-R1-1). Only a
    /// fill that owns the backing buffer may set it; false = per-entry
    /// reads only.
    pub contig: bool,
}

impl SoaDictTable {
    /// Memo/carry validation: same dictionary content. Epoch alone decides
    /// (rg-index per pinned scan — see the struct doc); pointer equality is
    /// deliberately not consulted (an arena could reuse an address).
    #[inline]
    pub fn same_identity(&self, other: &SoaDictTable) -> bool {
        self.epoch == other.epoch
    }

    /// Local code -> part-global code. Caller gates on `has_stitch()`;
    /// bounds are the filler's contract (`code < ndict`).
    #[inline]
    pub fn global_code(&self, code: u32) -> u32 {
        debug_assert!(!self.stitch.is_null() && code < self.ndict);
        // SAFETY: filler contract — `stitch` spans `ndict` u32s (Part::stitch
        // length-checked against the dict) and lives as long as the mmap'd
        // part the scan pins.
        unsafe { *self.stitch.add(code as usize) }
    }

    #[inline]
    pub fn has_stitch(&self) -> bool {
        !self.stitch.is_null()
    }

    /// Decode one code. Bounds are the filler's contract (`code < ndict`).
    /// Lazy sub-framed dicts ensure the code's entry bytes first (the seam
    /// no-ops on fully-materialized tables — `lazy_ensure` is None).
    #[inline]
    pub fn datum(&self, code: u32) -> Datum {
        debug_assert!(code < self.ndict);
        self.ensure_code(code);
        // SAFETY: filler contract — `dict` spans `ndict` Datums and outlives
        // the staged window; `code` is in range for every staged row.
        unsafe { *self.dict.add(code as usize) }
    }

    /// Materialize one code's entry bytes (lazy sub-framed dicts; no-op
    /// otherwise). For consumers that pass raw `dict` slices onward but know
    /// the exact codes they will read.
    #[inline]
    pub fn ensure_code(&self, code: u32) {
        if let Some(f) = self.lazy_ensure {
            // SAFETY: seam contract — `lazy` is the publisher's live ensure
            // state for this table (window lifetime, like `dict` itself).
            unsafe { f(self.lazy, code) };
        }
    }

    /// Decode one code WITHOUT the lazy ensure. ONLY for cells covered by
    /// a stale-cell contract (PREWHERE-armed batches: consumers read
    /// SELECTED rows only — `seq_scan_batch_lane_armed`): the pointer is
    /// always valid, its BYTES may be unmaterialized.
    #[inline]
    pub fn datum_no_ensure(&self, code: u32) -> Datum {
        debug_assert!(code < self.ndict);
        // SAFETY: filler contract as `datum` (pointer validity is
        // ensure-independent).
        unsafe { *self.dict.add(code as usize) }
    }

    /// Materialize EVERY entry's bytes (lazy sub-framed dicts; no-op
    /// otherwise). Required before whole-dict sweeps, rank binary searches,
    /// or any raw `dict` slice read not covered code-by-code.
    #[inline]
    pub fn ensure_all(&self) {
        if let Some(f) = self.lazy_ensure_all {
            // SAFETY: seam contract, as `ensure_code`.
            unsafe { f(self.lazy) };
        }
    }
}

/// Dict-coded lane view of one staged column: u32 codes (one per staged row)
/// into a per-row-group dictionary. Published by a columnar AM's batch fill
/// when the consumer opted in (`set_dict_want`); the column's values/isnull
/// cells are NOT filled while a dict lane answer is up (`col_datum_ready`).
#[derive(Clone, Copy)]
pub struct SoaDictLane {
    pub codes: *const u32,
    pub table: SoaDictTable,
}

impl SoaDictLane {
    /// Row `i`'s code. Valid for `i < nrows` of the staged window.
    #[inline]
    pub fn code(&self, i: usize) -> u32 {
        // SAFETY: filler contract — `codes` spans the staged window's rows.
        unsafe { *self.codes.add(i) }
    }

    /// Row `i`'s decoded Datum (`dict[codes[i]]` gather).
    #[inline]
    pub fn datum(&self, i: usize) -> Datum {
        self.table.datum(self.code(i))
    }
}

/// Contiguity witness for a text column's staged window (likeband blob-wide
/// kernel): the window's varlena images sit back-to-back (modulo alignment
/// padding) inside ONE readable span — the columnar AM's decode arena or the
/// raw mmap blob — and the column's value cells are ASCENDING pointers into
/// it. Published per window by a columnar fill that can prove the layout;
/// heap fills never publish one (page tuples are not contiguous). Consumers
/// (the contains-LIKE blob kernel) run one substring search over the whole
/// span and map hits back to rows through the pointer lane, rejecting hits
/// that straddle row boundaries (headers/padding) — the per-row occurrence
/// set is therefore identical to a per-row search.
#[derive(Clone, Copy)]
pub struct SoaTextSpan {
    pub base: *const u8,
    pub len: usize,
}

pub struct SoaBatch<'mcx> {
    ncols: u16,
    nrows: u32,
    values: PgVec<'mcx, Datum>,
    isnull: PgVec<'mcx, bool>,
    end_off: PgVec<'mcx, u32>,
    slow: PgVec<'mcx, bool>,
    fallback: [u64; SOA_BM_WORDS],
    // Kind 0 rows deform column-major from tps.
    tps: PgVec<'mcx, *const u8>,
    kinds: PgVec<'mcx, u8>,
    // OR of all staged kinds: 0 = every row is kind 0 (dense lane).
    kinds_or: u8,
    // Representation-tag columns (dict lanes): `dict_want` is set once at
    // lane arm for columns the lane program reads as codes+dict; the AM's
    // batch fill answers per window with `dict_lanes` (and skips the Datum
    // fill for answered columns — their values/isnull cells stay stale and
    // only the dict-lane reader consumes them). Heap deform never sets
    // these: on heap batches every column stays Raw and the kinds/jit paths
    // are untouched (a dict lane can only be published by a columnar fill,
    // which bypasses soa_classify_row/soa_deform_columns entirely).
    dict_want: PgVec<'mcx, bool>,
    dict_lanes: PgVec<'mcx, Option<SoaDictLane>>,
    dict_any: bool,
    // Lane-read mask (columnar lane-armed scans): when armed, only masked
    // columns need their Datum cells filled by the batch fill — every other
    // consumer on those scans reads the slot the AM's store_slot populates.
    // Unarmed = fill all needed columns exactly as before (fail open).
    lane_read: PgVec<'mcx, bool>,
    lane_read_any: bool,
    // Per-window text-span witnesses (blob-wide contains kernel); same
    // window-boundary discipline as dict lanes: cleared at begin, re-answered
    // (or left None = per-row) by the fill every window.
    text_spans: PgVec<'mcx, Option<SoaTextSpan>>,
    text_any: bool,
    // Length-lane arming (fold length admissions over pgrcolumnar text columns):
    // 0 = none, LEN_WANT_BYTES = octet length, LEN_WANT_CHARS = UTF-8
    // character length. Armed once at feed arm (pgrcolumnar scans only); the
    // AM's batch fill answers the column's values cells with
    // `Datum::from_i64(length)` for every staged SELECTED row instead of
    // varlena datum pointers (post-qual for dict-answered qual columns — the
    // gather/convert step). Heap fills never see an armed column (the
    // arming seam refuses non-pgrcolumnar scans).
    len_want: PgVec<'mcx, u8>,
    len_any: bool,
}

/// `len_want` kinds (see the field doc).
pub const LEN_WANT_BYTES: u8 = 1;
pub const LEN_WANT_CHARS: u8 = 2;


impl<'mcx> SoaBatch<'mcx> {
    pub fn new_in(mcx: Mcx<'mcx>, ncols: u16) -> SoaBatch<'mcx> {
        let cells = ncols as usize * SOA_MAX_ROWS;
        SoaBatch {
            ncols,
            nrows: 0,
            values: ::mcx::vec_from_elem_in(mcx, Datum::null(), cells),
            isnull: ::mcx::vec_from_elem_in(mcx, false, cells),
            end_off: ::mcx::vec_from_elem_in(mcx, 0u32, SOA_MAX_ROWS),
            slow: ::mcx::vec_from_elem_in(mcx, false, SOA_MAX_ROWS),
            fallback: [0; SOA_BM_WORDS],
            tps: ::mcx::vec_from_elem_in(mcx, core::ptr::null(), SOA_MAX_ROWS),
            kinds: ::mcx::vec_from_elem_in(mcx, 0u8, SOA_MAX_ROWS),
            kinds_or: 0,
            dict_want: ::mcx::vec_from_elem_in(mcx, false, ncols as usize),
            dict_lanes: ::mcx::vec_from_elem_in(mcx, None, ncols as usize),
            dict_any: false,
            lane_read: ::mcx::vec_from_elem_in(mcx, false, ncols as usize),
            lane_read_any: false,
            text_spans: ::mcx::vec_from_elem_in(mcx, None, ncols as usize),
            text_any: false,
            len_want: ::mcx::vec_from_elem_in(mcx, 0u8, ncols as usize),
            len_any: false,
        }
    }

    #[inline]
    pub fn begin(&mut self, nrows: u32) {
        debug_assert!(nrows as usize <= SOA_MAX_ROWS);
        self.nrows = nrows;
        self.fallback = [0; SOA_BM_WORDS];
        self.kinds_or = 0;
        // Window boundary: dict lane answers are per-window; stale codes
        // pointers (and possibly a stale epoch) must never survive into the
        // next fill — the AM re-answers (or the fill goes Raw) every window.
        if self.dict_any {
            self.dict_lanes.fill(None);
        }
        // Same discipline for text-span witnesses: spans are per-window
        // (arena/blob pointers); stale spans must never survive a re-fill.
        if self.text_any {
            self.text_spans.fill(None);
            self.text_any = false;
        }
    }

    /// AM-side per-window contiguity answer for a Raw-filled text column
    /// (see `SoaTextSpan`). The column's values cells MUST also be filled
    /// (the span complements the pointer lane, it does not replace it).
    #[inline]
    pub fn set_text_span(&mut self, c: usize, span: SoaTextSpan) {
        self.text_spans[c] = Some(span);
        self.text_any = true;
    }

    #[inline]
    pub fn text_span(&self, c: usize) -> Option<SoaTextSpan> {
        if !self.text_any {
            return None;
        }
        self.text_spans[c]
    }

    /// Lane arm (once, at scan/program build): the consumer reads column `c`
    /// as codes+dict when the staged window is dict-coded. The AM may still
    /// answer Raw per window (non-dict-encoded chunk) — consumers must take
    /// `dict_lane(c) == None` as "this window is Raw", never as an error.
    pub fn set_dict_want(&mut self, c: u16) {
        self.dict_want[c as usize] = true;
        self.dict_any = true;
    }

    #[inline]
    pub fn dict_want(&self, c: usize) -> bool {
        self.dict_any && self.dict_want[c]
    }

    /// AM-side per-window answer; implies the column's values/isnull cells
    /// were NOT filled for this window. Only columns that opted in may be
    /// answered — a filler must gather `dict[code]` to Raw for everyone else.
    #[inline]
    pub fn set_dict_lane(&mut self, c: usize, lane: SoaDictLane) {
        debug_assert!(self.dict_want[c]);
        self.dict_lanes[c] = Some(lane);
    }

    #[inline]
    pub fn dict_lane(&self, c: usize) -> Option<SoaDictLane> {
        if !self.dict_any {
            return None;
        }
        self.dict_lanes[c]
    }

    /// Escape hatch: materialize column `c`'s dict lane into its values/
    /// isnull cells (the one-instruction `dict[code]` gather) and clear the
    /// lane, flipping `col_datum_ready` back on. Byte-identical to the
    /// filler's own full-decode Raw fill by the dict contract (code =
    /// dictionary index of the decoded Datum). isnull is written explicitly:
    /// NULL-free is this window's proof, not a structural assumption.
    pub fn gather_dict_lane(&mut self, c: usize) {
        let Some(lane) = self.dict_lane(c) else { return };
        let n = self.nrows as usize;
        let values = &mut self.values[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
        let isnull = &mut self.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
        isnull.fill(false);
        for (i, v) in values.iter_mut().enumerate() {
            *v = lane.datum(i);
        }
        self.dict_lanes[c] = None;
    }

    /// `gather_dict_lane` under a PREWHERE selection (stale-cell contract:
    /// consumers of this batch read SELECTED rows only): identical cell
    /// writes, but a lazy sub-framed dict ensures only selected rows'
    /// codes — unselected cells hold valid POINTERS whose bytes may stay
    /// unmaterialized. Bit i of `sel` = staged row i selected.
    pub fn gather_dict_lane_sel(&mut self, c: usize, sel: &[u64]) {
        let Some(lane) = self.dict_lane(c) else { return };
        if lane.table.lazy_ensure.is_none() {
            return self.gather_dict_lane(c);
        }
        let n = self.nrows as usize;
        let values = &mut self.values[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
        let isnull = &mut self.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
        isnull.fill(false);
        for (i, v) in values.iter_mut().enumerate() {
            let code = lane.code(i);
            if sel.get(i / 64).is_some_and(|w| w >> (i % 64) & 1 == 1) {
                lane.table.ensure_code(code);
            }
            *v = lane.table.datum_no_ensure(code);
        }
        self.dict_lanes[c] = None;
    }

    /// Length-lane arm (once, at fold-feed arm): column `c`'s values cells
    /// carry `Datum::from_i64(length)` for staged selected rows (see the
    /// field doc). `kind` is `LEN_WANT_BYTES`/`LEN_WANT_CHARS`.
    pub fn set_len_want(&mut self, c: u16, kind: u8) {
        debug_assert!(matches!(kind, LEN_WANT_BYTES | LEN_WANT_CHARS));
        self.len_want[c as usize] = kind;
        self.len_any = true;
    }

    #[inline]
    pub fn len_want(&self, c: usize) -> u8 {
        if !self.len_any {
            return 0;
        }
        self.len_want[c]
    }

    #[inline]
    pub fn len_any(&self) -> bool {
        self.len_any
    }

    /// Drop a dict-lane answer WITHOUT gathering (the length-lane gather:
    /// the AM's `batch_fill_len_col` re-answered the values cells as i64
    /// lengths off the scan-side decode state, so `col_datum_ready` must
    /// flip back on without the `dict[code]` datum gather).
    #[inline]
    pub fn clear_dict_lane(&mut self, c: usize) {
        self.dict_lanes[c] = None;
    }

    /// Lane arm: column `c` is read by the lane program from the SoA batch.
    pub fn set_lane_read(&mut self, c: u16) {
        self.lane_read[c as usize] = true;
        self.lane_read_any = true;
    }

    /// AM-side fill gate: false = no SoA consumer reads this column's Datum
    /// cells on this scan (the fill may leave them stale).
    #[inline]
    pub fn lane_fill_wanted(&self, c: usize) -> bool {
        !self.lane_read_any || self.lane_read[c]
    }

    #[inline]
    pub fn lane_read_armed(&self) -> bool {
        self.lane_read_any
    }

    /// Column `c`'s values/isnull cells are valid for this window's
    /// non-fallback rows (no dict-lane answer, fill not skipped).
    #[inline]
    pub fn col_datum_ready(&self, c: usize) -> bool {
        c < self.ncols as usize && self.dict_lane(c).is_none() && self.lane_fill_wanted(c)
    }

    #[inline]
    pub fn ncols(&self) -> u16 {
        self.ncols
    }

    #[inline]
    pub fn nrows(&self) -> u32 {
        self.nrows
    }

    #[inline]
    pub fn is_fallback(&self, i: u32) -> bool {
        self.fallback[(i / 64) as usize] & (1u64 << (i % 64)) != 0
    }

    /// Rows the deform skipped; a batched qual must re-check these per row.
    #[inline]
    pub fn fallback_words(&self) -> &[u64] {
        &self.fallback
    }

    /// OR extra forced-fallback rows into the batch (a batched qual kernel
    /// found them undecidable — e.g. compressed/external varlena datums on
    /// the varkey lane): they take the same per-row re-check path as
    /// deform-skipped rows.
    #[inline]
    pub fn mark_fallback_words(&mut self, words: &[u64]) {
        for (w, m) in self.fallback.iter_mut().zip(words) {
            *w |= m;
        }
    }

    /// Column `c`'s values for the staged batch.
    #[inline]
    pub fn col_values(&self, c: usize) -> &[Datum] {
        &self.values[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + self.nrows as usize]
    }

    #[inline]
    pub fn col_isnull(&self, c: usize) -> &[bool] {
        &self.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + self.nrows as usize]
    }

    // Columnar-AM staging (pgrcolumnar): the AM writes decoded vectors directly
    // (subject to `lane_fill_wanted`; dict-answered columns skip the fill).
    #[inline]
    pub fn col_values_mut(&mut self, c: usize) -> &mut [Datum] {
        &mut self.values[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + self.nrows as usize]
    }

    #[inline]
    pub fn col_isnull_mut(&mut self, c: usize) -> &mut [bool] {
        &mut self.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + self.nrows as usize]
    }
}

/// Varlena sort-key column plan: stage per-row pointers to one `attlen == -1`
/// column (the fused-sort direct key feed; fixed-width keys use SoaDeformPlan).
#[derive(Clone, Copy)]
pub struct SoaVarKeyPlan {
    key: u16,
    // Alignment-probe start when every preceding attr is fixed-width.
    fixed_start: Option<u32>,
    key_alignby: u8,
}

impl SoaVarKeyPlan {
    pub fn key(&self) -> u16 {
        self.key
    }

    pub fn try_new(atts: &[CompactAttribute], key: usize) -> Option<SoaVarKeyPlan> {
        if key >= atts.len() || key >= u16::MAX as usize || atts[key].attlen != -1 {
            return None;
        }
        if atts[..key].iter().any(|a| a.attlen == -2) {
            return None;
        }
        let mut off = 0usize;
        let mut fixed = true;
        for att in &atts[..key] {
            if att.attlen <= 0 {
                fixed = false;
                break;
            }
            off = att_nominal_alignby(off, att.attalignby);
            off += att.attlen as usize;
        }
        Some(SoaVarKeyPlan {
            key: key as u16,
            fixed_start: fixed.then_some(off as u32),
            key_alignby: atts[key].attalignby,
        })
    }
}

/// Stage row `i`'s key pointer into column 0 of `soa`; narrow tuples get the
/// fallback bit (lazy emit path). Value/null identical to slot deform of the
/// key attribute — same page pointer, same null-bitmap semantics.
#[inline(always)]
pub fn soa_stage_varkey(
    soa: &mut SoaBatch<'_>,
    plan: &SoaVarKeyPlan,
    atts: &[CompactAttribute],
    i: u32,
    tuple: &HeapTupleData<'_>,
) {
    let idx = i as usize;
    debug_assert!(soa.ncols >= 1 && idx < SOA_MAX_ROWS);
    if (tuple.t_data().natts() as usize) <= plan.key as usize {
        soa.fallback[idx / 64] |= 1u64 << (idx % 64);
        return;
    }
    if !tuple.has_nulls() {
        if let Some(start) = plan.fixed_start {
            let tp = tuple.getstruct();
            // SAFETY: null-free tuple with natts > key: the fixed prefix ends
            // at `start` and the key varlena's first byte is readable there.
            unsafe {
                let off =
                    att_pointer_alignby(start as usize, plan.key_alignby, -1, tp.add(start as usize));
                *soa.values.get_unchecked_mut(idx) = Datum::from_usize(tp.add(off) as usize);
                *soa.isnull.get_unchecked_mut(idx) = false;
            }
            return;
        }
    }
    soa_stage_varkey_walk(soa, plan, atts, idx, tuple);
}

#[inline(never)]
fn soa_stage_varkey_walk(
    soa: &mut SoaBatch<'_>,
    plan: &SoaVarKeyPlan,
    atts: &[CompactAttribute],
    idx: usize,
    tuple: &HeapTupleData<'_>,
) {
    let tp = tuple.getstruct();
    let hasnulls = tuple.has_nulls();
    // SAFETY: in-bounds offset within the image (t_len >= header).
    let bp = unsafe { tuple.header_ptr().add(SizeofHeapTupleHeader) };
    let key = plan.key as usize;
    let mut off = 0usize;
    for c in 0..=key {
        // SAFETY: c <= key < natts (checked by the caller); the walk visits
        // attributes present in the tuple, as deform_internal's slow lane.
        unsafe {
            if hasnulls && att_isnull(c, bp) {
                if c == key {
                    soa.values[idx] = Datum::null();
                    soa.isnull[idx] = true;
                    return;
                }
                continue;
            }
            let att = atts.get_unchecked(c);
            let attlen = att.attlen as i32;
            if attlen == -1 {
                off = att_pointer_alignby(off, att.attalignby, -1, tp.add(off));
            } else {
                off = att_nominal_alignby(off, att.attalignby);
            }
            if c == key {
                soa.values[idx] = Datum::from_usize(tp.add(off) as usize);
                soa.isnull[idx] = false;
                return;
            }
            off = att_addlength_pointer(off, attlen, tp.add(off));
        }
    }
}

/// Fixed-lane rows park their data pointer; hasnulls rows deform here
/// (offsets shift past nulls); narrow tuples fall back to the lazy path.
#[inline(always)]
pub fn soa_classify_row(
    soa: &mut SoaBatch<'_>,
    plan: &SoaDeformPlan<'_>,
    atts: &[CompactAttribute],
    i: u32,
    tuple: &HeapTupleData<'_>,
) {
    let ncols = plan.ncols as usize;
    let idx = i as usize;
    debug_assert!(soa.ncols as usize == ncols && idx < SOA_MAX_ROWS && ncols <= atts.len());
    // SAFETY: idx < SOA_MAX_ROWS; arrays sized SOA_MAX_ROWS.
    unsafe {
        if (tuple.t_data().natts() as usize) < ncols {
            soa.fallback[idx / 64] |= 1u64 << (idx % 64);
            *soa.kinds.get_unchecked_mut(idx) = 2;
            soa.kinds_or |= 2;
            return;
        }
        if tuple.has_nulls() {
            *soa.kinds.get_unchecked_mut(idx) = 1;
            soa.kinds_or |= 1;
            // Walk-tail plans need the varlena-aware nulls walk (the fixed
            // twin advances by `attlen` and cannot cross an `attlen <= 0`
            // column); one predictable branch, kind-1 rows only.
            if plan.walk_from.is_some() {
                return soa_deform_tuple_nulls_walk(soa, atts, idx, ncols, tuple);
            }
            return soa_deform_tuple_nulls(soa, atts, idx, ncols, tuple);
        }
        *soa.kinds.get_unchecked_mut(idx) = 0;
        *soa.tps.get_unchecked_mut(idx) = tuple.getstruct();
        // Walk-tail plans: this is the HEAD end (a stale intermediate) —
        // the walk pass overwrites both cells per row before any publish
        // (a head-only qual ask skips the walk, but that shape never
        // publishes: `publish = false` on qual-only stagings).
        *soa.end_off.get_unchecked_mut(idx) = plan.end_off;
        *soa.slow.get_unchecked_mut(idx) = false;
    }
}

/// Column-major deform of kind-0 rows: offset/width are loop constants,
/// each inner loop a monomorphic load/store pair per row.
pub fn soa_deform_columns(
    soa: &mut SoaBatch<'_>,
    plan: &SoaDeformPlan<'_>,
    atts: &[CompactAttribute],
    qual_col_only: Option<u16>,
) {
    debug_assert!(!plan.is_virtual(), "virtual prefix plans are cbstore-only (no offset chain)");
    let n = soa.nrows as usize;
    let ncols = plan.ncols as usize;
    // Walk-tail plans (AGGSEQ-STAGE): the static column-major pass covers
    // the fixed HEAD only; the tail deforms per row below. A single-column
    // ask resident in the tail runs the walk alone (the walk fills the
    // whole tail — including the asked column); a head-resident ask keeps
    // today's single static column and skips the walk (bitmap-only
    // consumer: qual-only stagings never publish).
    let wf = plan.walk_from.map_or(ncols, |w| w as usize);
    let (first, last) = match qual_col_only {
        Some(c) if (c as usize) < wf => (c as usize, c as usize + 1),
        Some(_) => (0, 0),
        None => (0, wf),
    };
    // Dense lane: every staged row is kind 0, so the per-row kind test drops
    // and the isnull column becomes one vectorizable fill.
    let dense = soa.kinds_or == 0;
    if qual_col_only.is_none() {
        if let Some(k) = plan.jit.as_deref() {
            debug_assert!(k.ncols() as usize == ncols);
            // SAFETY: kind-0 rows are null-free with natts >= ncols (kernel
            // domain); the kernel stores ncols datums at SOA_MAX_ROWS*8
            // stride from &values[i] — in bounds of the ncols*SOA_MAX_ROWS
            // buffer for every i < n <= SOA_MAX_ROWS. Layout identity with
            // the plan is arm_jit's contract; output is bit-identical to the
            // interpreter pass below (jit_deform_matches_aot_and_interpreter).
            unsafe {
                let base = soa.values.as_mut_ptr();
                if dense {
                    for c in 0..ncols {
                        soa.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n].fill(false);
                    }
                    for i in 0..n {
                        k.soa(*soa.tps.get_unchecked(i), base.add(i), SOA_MAX_ROWS * 8);
                    }
                } else {
                    // Kind-1 rows were already deformed at classify; kind-2
                    // rows carry the fallback bit — only their cells stay
                    // stale, and no reader consumes fallback cells.
                    let isnull = soa.isnull.as_mut_ptr();
                    for i in 0..n {
                        if *soa.kinds.get_unchecked(i) != 0 {
                            continue;
                        }
                        k.soa(*soa.tps.get_unchecked(i), base.add(i), SOA_MAX_ROWS * 8);
                        for c in 0..ncols {
                            *isnull.add(c * SOA_MAX_ROWS + i) = false;
                        }
                    }
                }
            }
            return;
        }
    }
    // Non-JIT pass: the generic interpreter fetch (fetch_att, runtime
    // dispatch) — the monomorphized shape-class column loops are removed
    // (docs/optimizations/jit-deform.md rung 3); JIT-unavailable environments
    // accept interpreter cost by charter.
    for c in first..last {
        let att = &atts[c];
        let off = plan.offs[c] as usize;
        let attbyval = att.attbyval;
        let attlen = att.attlen as i32;
        // SAFETY: kind-0 rows are null-free with natts >= ncols, so tp + off
        // is inside the tuple data area for every prefix column.
        unsafe {
            let values = &mut soa.values[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
            let isnull = &mut soa.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
            let tps = &soa.tps[..n];
            let kinds = &soa.kinds[..n];
            if dense {
                isnull.fill(false);
                for i in 0..n {
                    *values.get_unchecked_mut(i) =
                        fetch_att((*tps.get_unchecked(i)).add(off), attbyval, attlen);
                }
            } else {
                for i in 0..n {
                    if *kinds.get_unchecked(i) == 0 {
                        *values.get_unchecked_mut(i) =
                            fetch_att((*tps.get_unchecked(i)).add(off), attbyval, attlen);
                        *isnull.get_unchecked_mut(i) = false;
                    }
                }
            }
        }
    }
    // Walk-tail pass (AGGSEQ-STAGE): columns at/past the first varlena
    // deform per row. Skipped only for a head-resident single-column ask
    // (see the (first, last) derivation above).
    if plan.walk_from.is_some() && !matches!(qual_col_only, Some(c) if (c as usize) < wf) {
        soa_deform_walk_tail(soa, plan, atts);
    }
}

/// Per-row sequential walk deform of a walk-tail plan's tail columns
/// (`walk_from..ncols`) for kind-0 rows — the varlena-crossing half of the
/// AGGSEQ-STAGE staging seam. Each row walks its attributes once, exactly
/// `deform_internal`'s slow-lane discipline: `att_pointer_alignby` for
/// varlena (the staged cell is the same in-page pointer Datum the per-row
/// slot deform stores — pin-holding ABI R3v), `att_nominal_alignby` +
/// `fetch_att` for fixed-width, `att_addlength_pointer` advance. Writes
/// the per-row resume state the per-row path itself would leave in the
/// slot: `end_off` = the offset after the last prefix column and
/// `slow = true` (`deform_internal` sets slow after any `attlen <= 0`
/// attribute, and the tail's first column IS one). `attcacheoff` is
/// deliberately never written here — post-varlena columns are never
/// cacheable and the produced offsets/values are identical either way.
///
/// Kind discipline is `soa_deform_columns`'s: kind-1 hasnulls rows were
/// fully deformed at classify (`soa_deform_tuple_nulls_walk`), kind-2
/// narrow rows carry the fallback bit and no reader consumes their cells.
fn soa_deform_walk_tail(
    soa: &mut SoaBatch<'_>,
    plan: &SoaDeformPlan<'_>,
    atts: &[CompactAttribute],
) {
    let wf = plan.walk_from.expect("walk pass on a walk-tail plan") as usize;
    let n = soa.nrows as usize;
    let ncols = plan.ncols as usize;
    debug_assert!(soa.ncols as usize == ncols && wf < ncols && ncols <= atts.len());
    let start_off = plan.end_off as usize; // head end (0 when wf == 0)
    let dense = soa.kinds_or == 0;
    for i in 0..n {
        // SAFETY: kind-0 rows are null-free with natts >= ncols (the
        // classify contract) and parked their tuple pointer off the
        // still-pinned page — the walk visits attributes present in the
        // tuple, as deform_internal's slow lane.
        unsafe {
            if !dense && *soa.kinds.get_unchecked(i) != 0 {
                continue;
            }
            let tp = *soa.tps.get_unchecked(i);
            let mut off = start_off;
            for c in wf..ncols {
                let att = atts.get_unchecked(c);
                let attlen = att.attlen as i32;
                if attlen == -1 {
                    off = att_pointer_alignby(off, att.attalignby, -1, tp.add(off));
                } else {
                    off = att_nominal_alignby(off, att.attalignby);
                }
                *soa.values.get_unchecked_mut(c * SOA_MAX_ROWS + i) =
                    fetch_att(tp.add(off), att.attbyval, attlen);
                *soa.isnull.get_unchecked_mut(c * SOA_MAX_ROWS + i) = false;
                off = att_addlength_pointer(off, attlen, tp.add(off));
            }
            *soa.end_off.get_unchecked_mut(i) = off as u32;
            *soa.slow.get_unchecked_mut(i) = true;
        }
    }
}

/// `soa_deform_columns` generalized to an explicit column SET with an
/// optional selection (K1 inc-2 late materialization, wave-9 WS-AH):
/// the interpreter (first,last) loop over a caller-stated `cols` list —
/// staging narrows to {qual clause cols ∪ key cols}, and survivor
/// completion fills the deferred columns for `sel`-selected rows only.
///
/// Selection discipline (`sel` = 64-row selection words, LSB-first like
/// the kernel qual bitmaps): all-zero words skip whole (word-skip),
/// words at or above [`DENSE_WORD_CUTOVER`] set bits take the dense row
/// loop (over-filling unselected kind-0 cells — idempotent value movement
/// into cells no reader consumes), sparser words walk their set bits. Rows
/// past `nrows` are masked off here, so a caller may pass the staged
/// bitmap verbatim. Kind discipline is `soa_deform_columns`'s: only
/// kind-0 rows fill (kind-1 hasnulls rows deformed fully at classify —
/// their cells are already live and their `tps` was never parked; kind-2
/// narrow rows carry the fallback bit and no reader consumes their
/// cells), so forced-fallback bits OR'd into a qual bitmap are harmless
/// here.
///
/// The deform-JIT batch kernel is deliberately NOT consulted: it deforms
/// the whole prefix in one pass (rail J — no-qual/all-columns shapes keep
/// `soa_deform_columns`' JIT path; this pass exists for qual-armed
/// narrowed stagings and survivor completion only). Idempotent per
/// (column, row): re-filling a cell rewrites the identical value.
/// Per-word set-bit count at which `soa_deform_columns_set`'s selected
/// pass switches from the bit-walk to the dense row loop (K1 inc-3). 48/64
/// keeps the cutover on the clear-win side: the dense loop's wasted fills
/// (≤ 16 cells) are cheaper than 48+ iterations of bit-extraction, and the
/// letter's high-selectivity loss lived at ~58-set words. Correctness does
/// not depend on the value (over-fill is idempotent; no reader consumes
/// unselected cells) — it is a speed dial only.
const DENSE_WORD_CUTOVER: u32 = 48;

pub fn soa_deform_columns_set(
    soa: &mut SoaBatch<'_>,
    plan: &SoaDeformPlan<'_>,
    atts: &[CompactAttribute],
    cols: &[u16],
    sel: Option<&[u64]>,
) {
    debug_assert!(!plan.is_virtual(), "virtual prefix plans are cbstore-only (no offset chain)");
    // AGGSEQ-STAGE: the K1-latemat split never arms over a walk-tail plan
    // (`seq_scan_k1_latemat_arm` refuses NAMED `k1-latemat-varwalk`) — this
    // pass indexes the static offset chain, which covers the head only.
    debug_assert!(
        plan.walk_from.is_none(),
        "column-set deform over a walk-tail plan (latemat must refuse varwalk stagings)"
    );
    let n = soa.nrows as usize;
    if n == 0 {
        return;
    }
    let ncols = plan.ncols as usize;
    let dense = soa.kinds_or == 0;
    let nwords = n.div_ceil(64);
    let tail_mask = if n % 64 == 0 { u64::MAX } else { (1u64 << (n % 64)) - 1 };
    for &c in cols {
        let c = c as usize;
        debug_assert!(c < ncols && ncols <= atts.len());
        let att = &atts[c];
        let off = plan.offs[c] as usize;
        let attbyval = att.attbyval;
        let attlen = att.attlen as i32;
        // SAFETY: kind-0 rows are null-free with natts >= ncols (the
        // classify contract), so tp + off is inside the tuple data area
        // for every prefix column — exactly `soa_deform_columns`' bound.
        unsafe {
            let values = &mut soa.values[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
            let isnull = &mut soa.isnull[c * SOA_MAX_ROWS..c * SOA_MAX_ROWS + n];
            let tps = &soa.tps[..n];
            let kinds = &soa.kinds[..n];
            match sel {
                None => {
                    if dense {
                        isnull.fill(false);
                        for i in 0..n {
                            *values.get_unchecked_mut(i) =
                                fetch_att((*tps.get_unchecked(i)).add(off), attbyval, attlen);
                        }
                    } else {
                        for i in 0..n {
                            if *kinds.get_unchecked(i) == 0 {
                                *values.get_unchecked_mut(i) =
                                    fetch_att((*tps.get_unchecked(i)).add(off), attbyval, attlen);
                                *isnull.get_unchecked_mut(i) = false;
                            }
                        }
                    }
                }
                Some(sel) => {
                    for w in 0..nwords {
                        let word_mask = if w == nwords - 1 { tail_mask } else { u64::MAX };
                        let mut bits = sel.get(w).copied().unwrap_or(0) & word_mask;
                        if bits == 0 {
                            continue; // word-skip: 64 rejected rows, zero work
                        }
                        let base = w * 64;
                        if bits == word_mask || bits.count_ones() >= DENSE_WORD_CUTOVER {
                            // Dense fill: every (or nearly every) row of the
                            // word selected. Inc-3 density cutover: at high
                            // density the branch-free row loop beats the
                            // bit-walk below, and filling the few UNSELECTED
                            // kind-0 cells is idempotent value movement into
                            // cells no reader consumes (qual-dead rows never
                            // emit, fold, or spill — the wave-9 AH letter's
                            // high-selectivity Ir loss was exactly this
                            // bit-walk at ~58/64-set words).
                            // SAFETY: every kind-0 row of the batch parked
                            // its `tps` pointer at classify off the
                            // still-pinned page, selected or not — the same
                            // bound as the unselected `sel == None` pass.
                            let hi = (base + 64).min(n);
                            for i in base..hi {
                                if dense || *kinds.get_unchecked(i) == 0 {
                                    *values.get_unchecked_mut(i) = fetch_att(
                                        (*tps.get_unchecked(i)).add(off),
                                        attbyval,
                                        attlen,
                                    );
                                    *isnull.get_unchecked_mut(i) = false;
                                }
                            }
                        } else {
                            while bits != 0 {
                                let i = base + bits.trailing_zeros() as usize;
                                bits &= bits - 1;
                                if dense || *kinds.get_unchecked(i) == 0 {
                                    *values.get_unchecked_mut(i) = fetch_att(
                                        (*tps.get_unchecked(i)).add(off),
                                        attbyval,
                                        attlen,
                                    );
                                    *isnull.get_unchecked_mut(i) = false;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Kind-1 (hasnulls) row deform for WALK-TAIL plans (AGGSEQ-STAGE): the
/// varlena-aware twin of [`soa_deform_tuple_nulls`] — `deform_internal`'s
/// slow-lane discipline over the whole prefix (nulls consume no space;
/// varlena cells stage in-page pointer Datums; `slow` mirrors the per-row
/// path: true once a null OR an `attlen <= 0` attribute was seen inside
/// the prefix).
#[inline(never)]
fn soa_deform_tuple_nulls_walk(
    soa: &mut SoaBatch<'_>,
    atts: &[CompactAttribute],
    idx: usize,
    ncols: usize,
    tuple: &HeapTupleData<'_>,
) {
    let tp = tuple.getstruct();
    // SAFETY: hasnulls tuples carry a bitmap covering natts >= ncols bits.
    let bp = unsafe { tuple.header_ptr().add(SizeofHeapTupleHeader) };
    let mut off = 0usize;
    let mut slow = false;
    for c in 0..ncols {
        // SAFETY: c < ncols <= natts; deform_internal's slow lane.
        unsafe {
            if att_isnull(c, bp) {
                soa.values[c * SOA_MAX_ROWS + idx] = Datum::null();
                soa.isnull[c * SOA_MAX_ROWS + idx] = true;
                slow = true;
                continue;
            }
            let att = &atts[c];
            let attlen = att.attlen as i32;
            if attlen == -1 {
                off = att_pointer_alignby(off, att.attalignby, -1, tp.add(off));
            } else {
                off = att_nominal_alignby(off, att.attalignby);
            }
            soa.values[c * SOA_MAX_ROWS + idx] =
                fetch_att(tp.add(off), att.attbyval, attlen);
            soa.isnull[c * SOA_MAX_ROWS + idx] = false;
            off = att_addlength_pointer(off, attlen, tp.add(off));
            if attlen <= 0 {
                slow = true;
            }
        }
    }
    soa.end_off[idx] = off as u32;
    soa.slow[idx] = slow;
}

#[inline(never)]
fn soa_deform_tuple_nulls(
    soa: &mut SoaBatch<'_>,
    atts: &[CompactAttribute],
    idx: usize,
    ncols: usize,
    tuple: &HeapTupleData<'_>,
) {
    let tp = tuple.getstruct();
    // SAFETY: hasnulls tuples carry a bitmap covering natts >= ncols bits.
    let bp = unsafe { tuple.header_ptr().add(SizeofHeapTupleHeader) };
    let mut off = 0usize;
    let mut slow = false;
    for c in 0..ncols {
        // SAFETY: c < ncols <= natts; deform_internal's slow lane, fixed-width.
        unsafe {
            if att_isnull(c, bp) {
                soa.values[c * SOA_MAX_ROWS + idx] = Datum::null();
                soa.isnull[c * SOA_MAX_ROWS + idx] = true;
                slow = true;
                continue;
            }
            let att = &atts[c];
            off = att_nominal_alignby(off, att.attalignby);
            soa.values[c * SOA_MAX_ROWS + idx] =
                fetch_att(tp.add(off), att.attbyval, att.attlen as i32);
            soa.isnull[c * SOA_MAX_ROWS + idx] = false;
            off += att.attlen as usize;
        }
    }
    soa.end_off[idx] = off as u32;
    soa.slow[idx] = slow;
}

/// false = the row wasn't batch-deformed, the lazy path applies.
#[inline(always)]
pub fn soa_store_prefix<'mcx>(slot: &mut SlotData<'mcx>, soa: &SoaBatch<'_>, i: u32) -> bool {
    if soa.is_fallback(i) {
        return false;
    }
    let h = match slot {
        SlotData::BufferHeap(b) => &mut b.base,
        SlotData::Heap(h) => h,
        // Columnar-AM (pgrcolumnar) scans use virtual slots the AM's
        // batch_store_slot fully populates; the prefix publish is a no-op
        // (and MUST be: dict-answered / fill-skipped SoA cells are stale).
        SlotData::Virtual(_) => return true,
        _ => unreachable!("soa_store_prefix on non-heap slot"),
    };
    let ncols = soa.ncols as usize;
    let idx = i as usize;
    debug_assert!(h.base.tts_values.len() >= ncols && (i as usize) < soa.end_off.len());
    // SAFETY: slot arrays span descriptor natts >= ncols (plan-build bound).
    unsafe {
        h.off = *soa.end_off.get_unchecked(idx);
        let slow = *soa.slow.get_unchecked(idx);
        let base = &mut h.base;
        for c in 0..ncols {
            *base.tts_values.get_unchecked_mut(c) = *soa.values.get_unchecked(c * SOA_MAX_ROWS + idx);
            *base.tts_isnull.get_unchecked_mut(c) = *soa.isnull.get_unchecked(c * SOA_MAX_ROWS + idx);
        }
        base.tts_nvalid = ncols as AttrNumber;
        if slow {
            base.tts_flags |= TTS_FLAG_SLOW;
        } else {
            base.tts_flags &= !TTS_FLAG_SLOW;
        }
    }
    true
}

mcx::forget_safe_nodrop!(SoaVarKeyPlan);
mcx::forget_safe_nodrop!(SoaDictTable);
mcx::forget_safe_nodrop!(SoaDictLane);
mcx::forget_safe_nodrop!(SoaTextSpan);

// jit exempt: released in exec_end_seq_scan (the bloom-filter Rc precedent).
mcx::forget_safe_struct!(
    SoaDeformPlan<'_> { ncols, end_off, offs, walk_from; jit },
    SoaBatch<'_> { ncols, nrows, values, isnull, end_off, slow, fallback, tps, kinds, kinds_or, dict_want, dict_lanes, dict_any, lane_read, lane_read_any, text_spans, text_any, len_want, len_any },
);
