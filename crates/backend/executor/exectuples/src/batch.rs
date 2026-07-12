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

/// Fixed-width prefix plan: every column below `ncols` has `attlen > 0`.
pub struct SoaDeformPlan<'mcx> {
    ncols: u16,
    end_off: u32,
    offs: PgVec<'mcx, u32>,
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
        Some(SoaDeformPlan { ncols: ncols as u16, end_off: off as u32, offs, jit: None })
    }

    #[inline]
    pub fn ncols(&self) -> u16 {
        self.ncols
    }

    /// Arm the JIT batch kernel; layout identity with this plan is required
    /// (same ncols and offset chain) so batch output is bit-identical.
    pub fn arm_jit(&mut self, k: alloc::rc::Rc<jit_deform::DeformKernel>) {
        debug_assert!(k.ncols() == self.ncols && k.end_off() == self.end_off);
        self.jit = Some(k);
    }

    /// Placeholder for varkey-mode batches; the varkey pass never reads it.
    pub fn unused(mcx: Mcx<'mcx>) -> SoaDeformPlan<'mcx> {
        SoaDeformPlan { ncols: 0, end_off: 0, offs: PgVec::new_in(mcx), jit: None }
    }

    /// Columnar-AM plan: `ncols` only, NO offset chain — usable exclusively
    /// with batch fills that ignore tuple offsets (cbstore's `batch_deform`
    /// stages decoded Datums per column, so varlena columns are stageable
    /// and the fixed-width-prefix restriction does not apply). The heap
    /// deform paths must never see this plan (they index `offs`); callers
    /// install it only on cbstore scan states, whose `TableScanDesc`
    /// dispatch never reaches the heap deform (`is_virtual` guards it).
    pub fn columnar(mcx: Mcx<'mcx>, ncols: usize) -> Option<SoaDeformPlan<'mcx>> {
        if ncols == 0 || ncols > u16::MAX as usize {
            return None;
        }
        Some(SoaDeformPlan { ncols: ncols as u16, end_off: 0, offs: PgVec::new_in(mcx), jit: None })
    }

    /// Alias of [`SoaDeformPlan::columnar`] (likeband's name for the same
    /// virtual, offset-chain-free columnar plan).
    pub fn virtual_prefix(mcx: Mcx<'mcx>, ncols: usize) -> Option<SoaDeformPlan<'mcx>> {
        Self::columnar(mcx, ncols)
    }

    /// True for `columnar`/`virtual_prefix` plans (no offset chain despite
    /// `ncols > 0`).
    #[inline]
    pub fn is_virtual(&self) -> bool {
        self.ncols > 0 && self.offs.is_empty()
    }
}

/// Identity + content handle of one per-row-group dictionary (cbstore dict
/// encoding): decoded text Datums, code = index. The pointer is the storage
/// adapter's and stays valid while the window is staged (until the next
/// batch fill / endscan) — the same lifetime contract as the text Datums the
/// adapter publishes into slots.
///
/// EPOCH DISCIPLINE (phase4 design §2/§8.1): `epoch` is the row-group index
/// within the scan. A scan pins its `Rc<Part>` for its whole lifetime, so the
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
/// CONTRACT: dict-coded columns are NULL-free today (cbstore stores no
/// NULLs). That is a per-chunk proof the filler asserts by writing
/// `isnull = false` on gather — NOT a type invariant of this struct; the
/// per-lane isnull currency stays so a NULL-capable cbstore v2 only changes
/// the fillers (phase4 design §8.3).
#[derive(Clone, Copy)]
pub struct SoaDictTable {
    pub dict: *const Datum,
    pub ndict: u32,
    pub epoch: u64,
    /// Dict entries are byte-sorted (codes are rank order) — gates dict
    /// range predicates. False keeps the per-entry memo path.
    pub sorted: bool,
}

impl SoaDictTable {
    /// Memo/carry validation: same dictionary content. Epoch alone decides
    /// (rg-index per pinned scan — see the struct doc); pointer equality is
    /// deliberately not consulted (an arena could reuse an address).
    #[inline]
    pub fn same_identity(&self, other: &SoaDictTable) -> bool {
        self.epoch == other.epoch
    }

    /// Decode one code. Bounds are the filler's contract (`code < ndict`).
    #[inline]
    pub fn datum(&self, code: u32) -> Datum {
        debug_assert!(code < self.ndict);
        // SAFETY: filler contract — `dict` spans `ndict` Datums and outlives
        // the staged window; `code` is in range for every staged row.
        unsafe { *self.dict.add(code as usize) }
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
}

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

    // Columnar-AM staging (cbstore): the AM writes decoded vectors directly
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
            return soa_deform_tuple_nulls(soa, atts, idx, ncols, tuple);
        }
        *soa.kinds.get_unchecked_mut(idx) = 0;
        *soa.tps.get_unchecked_mut(idx) = tuple.getstruct();
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
    let (first, last) = match qual_col_only {
        Some(c) => (c as usize, c as usize + 1),
        None => (0, ncols),
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
        // Columnar-AM (cbstore) scans use virtual slots the AM's
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
    SoaDeformPlan<'_> { ncols, end_off, offs; jit },
    SoaBatch<'_> { ncols, nrows, values, isnull, end_off, slow, fallback, tps, kinds, kinds_or, dict_want, dict_lanes, dict_any, lane_read, lane_read_any, text_spans, text_any },
);
