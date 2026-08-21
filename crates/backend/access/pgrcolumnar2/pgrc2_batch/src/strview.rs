//! StrView (M4-N): the 16-byte German-style string cell and its boundary
//! plumbing — `lanev3-strview.md` is the charter, binding verbatim.
//!
//! ## The cell (charter §2)
//!
//! 16 bytes, align 8, **no tag bit — `len <= 12` is the discriminant**:
//!
//! ```text
//! bytes 0..4    len: u32
//! bytes 4..8    prefix: first 4 string bytes          } word0: one comparable u64
//! bytes 8..16   inline form (len <= 12): bytes 4..12  } word1
//!               pointer form (len > 12): ptr -> varlena-shaped payload
//! ```
//!
//! Invariants (each with a pinning test in this PR; `StrCell::check` is the
//! runtime checker the born-RED teeth fire):
//!
//! 1. **Zero-padding law** — prefix and inline tail are zeroed past `len`;
//!    whole-word compares and hashes read no garbage.
//! 2. **Canonical form** — length decides the form, so equal strings have
//!    byte-identical inline cells. There is no tag to get wrong; the
//!    constructors are the enforcement surface and [`StrCell::from_bytes`]
//!    is the canonical producer.
//! 3. **Layout pin** — `size_of::<StrCell>() == 16`, align 8, field offsets
//!    pinned (the issue-#69 template — the layout prices engagement).
//! 4. **Varlena-shaped payload law** — pointer-form payloads are stored
//!    `[4-byte varlena header][bytes]` so the cell's pointer IS a valid PG
//!    datum (§7b; the pgrc2 producer pins are already green — this crate
//!    consumes the shape). The cell's own `len` field is NOT a varlena
//!    header; never hand out a pointer into the cell.
//!
//! ## Overlay law (the M4-N addition to the C1 overlay contract)
//!
//! `ColRep::StrView` is an overlay whose payload ([`StrViews`]) rides
//! BESIDE the datum lane, never replacing it: **every producer keeps the
//! column's datum cells valid plain varlena pointers for every selected
//! valid row.** Consequences, each load-bearing:
//!
//! - the rep-blind hosted face (`RowRef::Batch`-class readers) needs no
//!   edit — a StrView column handed to per-row fmgr reads correct varlena
//!   datums;
//! - [`crate::Column::gather_back`] degrades by a rep flip (no
//!   materialization, no arena);
//! - retention reduces to the existing `VarlenaPtr` copy-at-the-consumer
//!   discipline — a retainer never sees a cell.
//!
//! ## Byref / claim scope (charter §4)
//!
//! Views are claim-scoped to their batch and never outlive it: cells live
//! in the [`ClaimArena`] (epoch-trapped), long-form payloads alias either
//! arena images, pinned source images, or generation-stable dictionary
//! payload regions — all claim-scoped or handle-stable. **StrView state is
//! never strandable**: `Batch::begin` drops the overlay (R4), retention
//! copies payloads at the consumer, and spill never sees a cell (pages
//! serialize payloads, never pointers — the lx_spill swizzle law). The
//! byref-vocabulary entry is `lx_spill::ByrefClass::StrView`, whose
//! teardown home records exactly this: structurally unreachable at
//! teardown, fail closed if ever observed.
//!
//! Zero `thread_local!`: the fmgr scratch ([`StrScratch`]) is a passed
//! capability owned by the caller (§7.1 session-context law).

use core::ptr::NonNull;

use ::datum::Datum;
use ::types_tuple::varatt;

use crate::arena::ClaimArena;
use crate::batch::Column;
use crate::rep::ColRep;
use crate::selection::Selection;
use crate::staging::classify_varlena;

/// Longest string stored inline in the cell (charter §2).
pub const STRVIEW_INLINE_MAX: usize = 12;

/// Seed for the form-specialized cell hash (ASCII "lxstrvw1"). The mixing
/// primitives are `order_key::hash` — the shared family, never forked.
const STRVIEW_HASH_SEED: u64 = u64::from_le_bytes(*b"lxstrvw1");

/// The 16-byte string-view cell (charter §2). Plain `Copy` data — the
/// pointer form stores its pointer as bytes, so the type is POD and lanes
/// of cells are memcpy-safe.
///
/// Reading THROUGH a pointer-form cell is claim-scoped (R1–R6): the
/// payload it aliases must be live — arena values until reset, pinned
/// source images for the batch claim, dictionary payload regions for the
/// handle's life.
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StrCell {
    // pub(crate) for the layout pins (offset_of) and the in-crate fast
    // paths; construction outside the constructors is checker/test
    // currency only (from_raw).
    pub(crate) len: u32,
    pub(crate) prefix: [u8; 4],
    pub(crate) tail: [u8; 8],
}

impl StrCell {
    /// The zero cell: the canonical inline empty string. Unselected and
    /// null rows carry it (deterministic lanes, checker-clean).
    #[inline]
    pub const fn zeroed() -> StrCell {
        StrCell { len: 0, prefix: [0; 4], tail: [0; 8] }
    }

    /// Build an inline cell (`s.len() <= 12`), zero-padded per invariant 1.
    #[inline]
    pub fn inline_from_bytes(s: &[u8]) -> StrCell {
        debug_assert!(s.len() <= STRVIEW_INLINE_MAX);
        let mut prefix = [0u8; 4];
        let mut tail = [0u8; 8];
        let n = s.len();
        let np = n.min(4);
        prefix[..np].copy_from_slice(&s[..np]);
        if n > 4 {
            tail[..n - 4].copy_from_slice(&s[4..]);
        }
        StrCell { len: n as u32, prefix, tail }
    }

    /// Build a pointer-form cell over an existing plain 4B-U varlena image
    /// (zero-copy — the image IS the payload store, invariant 4).
    ///
    /// # Safety
    ///
    /// `image` points to a live plain 4B-U varlena whose payload length is
    /// `> 12`, live for the claim scope the cell is staged into.
    #[inline]
    pub unsafe fn long_from_image(image: *const u8) -> StrCell {
        // SAFETY: caller contract — live 4B-U image.
        let total = unsafe { varatt::varsize_4b(image) };
        let len = total - varatt::VARHDRSZ;
        debug_assert!(len > STRVIEW_INLINE_MAX, "long_from_image wants len > 12 (canonical form)");
        let mut prefix = [0u8; 4];
        // SAFETY: payload has > 12 bytes; first 4 are readable.
        unsafe {
            core::ptr::copy_nonoverlapping(image.add(varatt::VARHDRSZ), prefix.as_mut_ptr(), 4);
        }
        StrCell { len: len as u32, prefix, tail: (image as usize as u64).to_ne_bytes() }
    }

    /// The canonical constructor: length decides the form (invariant 2).
    /// Long payloads are homed in `arena` as `[4B header][bytes]`
    /// (invariant 4; ≥8-aligned per the arena law).
    pub fn from_bytes(s: &[u8], arena: &mut ClaimArena) -> StrCell {
        if s.len() <= STRVIEW_INLINE_MAX {
            return StrCell::inline_from_bytes(s);
        }
        let total = varatt::VARHDRSZ + s.len();
        let dst = arena.alloc(total).as_ptr();
        // SAFETY: fresh arena range of `total` bytes.
        unsafe {
            dst.cast::<u32>().write(varatt::set_varsize_4b_word(total as u32));
            core::ptr::copy_nonoverlapping(s.as_ptr(), dst.add(varatt::VARHDRSZ), s.len());
            StrCell::long_from_image(dst)
        }
    }

    /// Build a cell from a PLAIN INLINE varlena image (1B short or 4B-U —
    /// the `inline_proven` diet). Zero-copy where the form allows it:
    /// 4B-U long images are aliased in place; short-form long payloads
    /// (13..=126 bytes) are rehomed through `arena` (the one copy the
    /// charter prices at ~free during the detoast pass).
    ///
    /// # Safety
    ///
    /// `p` points to a live plain-inline varlena image (1B short or 4B-U),
    /// readable for its header-declared extent, live for the claim scope.
    pub unsafe fn from_plain_image(p: *const u8, arena: &mut ClaimArena) -> StrCell {
        // SAFETY: caller contract for all header/payload reads below.
        unsafe {
            if varatt::varatt_is_1b(p) {
                let len = varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT;
                let payload = core::slice::from_raw_parts(p.add(varatt::VARHDRSZ_SHORT), len);
                // from_bytes decides the form: inline for <= 12, arena
                // 4B image for the short-form long payloads.
                StrCell::from_bytes(payload, arena)
            } else {
                debug_assert!(varatt::varatt_is_4b_u(p), "from_plain_image wants plain inline");
                let len = varatt::varsize_4b(p) - varatt::VARHDRSZ;
                if len <= STRVIEW_INLINE_MAX {
                    let payload = core::slice::from_raw_parts(p.add(varatt::VARHDRSZ), len);
                    StrCell::inline_from_bytes(payload)
                } else {
                    StrCell::long_from_image(p)
                }
            }
        }
    }

    /// Raw constructor — checker/test currency (the born-RED teeth build
    /// defective cells through this), never a producer face.
    #[doc(hidden)]
    pub fn from_raw(len: u32, prefix: [u8; 4], tail: [u8; 8]) -> StrCell {
        StrCell { len, prefix, tail }
    }

    /// String byte length.
    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// TRUE for the inline form (`len <= 12` — the discriminant).
    #[inline]
    pub fn is_inline(&self) -> bool {
        self.len as usize <= STRVIEW_INLINE_MAX
    }

    /// Cell bytes 0..8 as one comparable word (len + prefix). Equal word0
    /// is a precondition of string equality; differing word0 proves
    /// inequality with zero memory touches (charter §3).
    #[inline]
    pub fn word0(&self) -> u64 {
        (self.len as u64) | ((u32::from_ne_bytes(self.prefix) as u64) << 32)
    }

    /// Cell bytes 8..16 as one word: inline content tail (zero-padded) for
    /// the inline form, the payload pointer bits for the pointer form.
    #[inline]
    pub fn word1(&self) -> u64 {
        u64::from_ne_bytes(self.tail)
    }

    /// The first-4-bytes prefix (zero-padded past `len`).
    #[inline]
    pub fn prefix_bytes(&self) -> [u8; 4] {
        self.prefix
    }

    /// Pointer-form payload image pointer (the valid-PG-datum pointer,
    /// invariant 4). Meaningless for inline cells.
    #[inline]
    pub fn image_ptr(&self) -> *const u8 {
        debug_assert!(!self.is_inline());
        usize::from_ne_bytes(self.tail) as *const u8
    }

    /// The string bytes.
    ///
    /// # Safety
    ///
    /// For pointer-form cells the caller asserts the claim scope (payload
    /// live). Inline cells borrow from the cell itself. The returned slice
    /// must not outlive the claim.
    #[inline]
    pub unsafe fn content<'a>(&'a self) -> &'a [u8] {
        if self.is_inline() {
            // Inline content is contiguous across cell bytes 4..16
            // (repr(C): prefix then tail).
            // SAFETY: in-bounds view of the cell's own 12 content bytes.
            unsafe { core::slice::from_raw_parts(self.prefix.as_ptr(), self.len as usize) }
        } else {
            // SAFETY: caller contract — live 4B-U payload of `len` bytes.
            unsafe {
                core::slice::from_raw_parts(
                    self.image_ptr().add(varatt::VARHDRSZ),
                    self.len as usize,
                )
            }
        }
    }

    /// Equality (charter §3): word0 differs ⇒ unequal, zero memory
    /// touches. Equal and inline ⇒ word1 decides. Equal and long ⇒ memcmp
    /// payloads — the only dereferencing case, reached only by true
    /// length+prefix ties.
    ///
    /// Byte equality is value equality under DETERMINISTIC collations only
    /// — the election gate (`strview_op_admissible`) is the caller's law.
    ///
    /// # Safety
    ///
    /// Claim scope for pointer-form payloads (both cells).
    #[inline]
    pub unsafe fn eq(a: &StrCell, b: &StrCell) -> bool {
        if a.word0() != b.word0() {
            return false;
        }
        if a.is_inline() {
            return a.word1() == b.word1();
        }
        // SAFETY: caller contract.
        unsafe { a.content() == b.content() }
    }

    /// Form-specialized hash (charter §3): inline cells hash their two
    /// words directly (valid by the zero-padding law); long cells hash
    /// word0 + payload bytes. Deterministic per string since the form is
    /// length-determined; `eq(a, b)` ⇒ `hash(a) == hash(b)` structurally.
    ///
    /// Mixing primitives are the shared `order_key::hash` family. This is
    /// an execution-internal hash (the lanetable precedent) — no semantic
    /// tie to `hashtext`.
    ///
    /// # Safety
    ///
    /// Claim scope for pointer-form payloads.
    #[inline]
    pub unsafe fn hash(&self) -> u64 {
        use ::order_key::hash::{fold64, mix64};
        let acc = fold64(STRVIEW_HASH_SEED, self.word0());
        if self.is_inline() {
            return mix64(fold64(acc, self.word1()));
        }
        // SAFETY: caller contract.
        let bytes = unsafe { self.content() };
        let mut acc = acc;
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            acc = fold64(acc, u64::from_le_bytes(c.try_into().expect("len 8")));
        }
        let rem = chunks.remainder();
        if !rem.is_empty() {
            let mut t = [0u8; 8];
            t[..rem.len()].copy_from_slice(rem);
            acc = fold64(acc, u64::from_le_bytes(t));
        }
        mix64(acc)
    }

    /// C-collation ordering (charter §3): memcmp order with the length
    /// tiebreak (`varstrfastcmp_c` semantics — content first, length sorts
    /// after content; word0 is never compared as an ordering key).
    ///
    /// Fast paths: both-inline compares the 12 content bytes as two
    /// big-endian words (EXACT for arbitrary bytes: padding zeros only tie
    /// against real zeros or shorter strings, and a full-word tie hands
    /// the verdict to the length compare — the same argument
    /// `order_key::transform::be_prefix_embed` documents); both-long
    /// compares in-cell prefixes first (4 real bytes — both lengths ≥ 13)
    /// and only ties chase payloads.
    ///
    /// # Safety
    ///
    /// Claim scope for pointer-form payloads.
    pub unsafe fn cmp_c(a: &StrCell, b: &StrCell) -> core::cmp::Ordering {
        use core::cmp::Ordering;
        if a.is_inline() && b.is_inline() {
            let a0 = u64::from_be_bytes([
                a.prefix[0], a.prefix[1], a.prefix[2], a.prefix[3], a.tail[0], a.tail[1],
                a.tail[2], a.tail[3],
            ]);
            let b0 = u64::from_be_bytes([
                b.prefix[0], b.prefix[1], b.prefix[2], b.prefix[3], b.tail[0], b.tail[1],
                b.tail[2], b.tail[3],
            ]);
            let c = a0.cmp(&b0);
            if c != Ordering::Equal {
                return c;
            }
            let a1 = u32::from_be_bytes([a.tail[4], a.tail[5], a.tail[6], a.tail[7]]);
            let b1 = u32::from_be_bytes([b.tail[4], b.tail[5], b.tail[6], b.tail[7]]);
            let c = a1.cmp(&b1);
            if c != Ordering::Equal {
                return c;
            }
            return a.len.cmp(&b.len);
        }
        if !a.is_inline() && !b.is_inline() {
            // Both lengths >= 13: all 4 prefix bytes are real content.
            let ap = u32::from_be_bytes(a.prefix);
            let bp = u32::from_be_bytes(b.prefix);
            let c = ap.cmp(&bp);
            if c != Ordering::Equal {
                return c;
            }
        }
        // SAFETY: caller contract. Slice Ord IS memcmp-then-length.
        unsafe { a.content().cmp(b.content()) }
    }

    /// The runtime invariant checker (invariants 1, 2-as-detectable, 4).
    /// The born-RED teeth drive this with seeded defects.
    ///
    /// # Safety
    ///
    /// Pointer-form cells are dereferenced (header + first 4 payload
    /// bytes) — claim scope required.
    pub unsafe fn check(&self) -> Result<(), &'static str> {
        let len = self.len as usize;
        if len <= STRVIEW_INLINE_MAX {
            // Invariant 1: zero padding past len across the 12 content
            // bytes (a pointer accidentally stored against a short length
            // — the invariant-2 defect class — trips this too).
            for i in len..STRVIEW_INLINE_MAX {
                let byte = if i < 4 { self.prefix[i] } else { self.tail[i - 4] };
                if byte != 0 {
                    return Err("strview: inline padding not zeroed (invariant 1)");
                }
            }
            Ok(())
        } else {
            let p = self.image_ptr();
            if p.is_null() {
                return Err("strview: null payload pointer (invariant 4)");
            }
            // SAFETY: caller contract — live image header.
            let header = unsafe { p.cast::<u32>().read_unaligned() };
            // SAFETY: header word read above; 4B-U test is a bit test.
            if !unsafe { varatt::varatt_is_4b_u(p) } {
                return Err("strview: payload not a plain 4B-U varlena (invariant 4)");
            }
            let total = varatt::varsize_4b_word(header) as usize;
            if total != varatt::VARHDRSZ + len {
                return Err("strview: payload length disagrees with cell len (invariant 4)");
            }
            // SAFETY: payload has len > 12 bytes; first 4 readable.
            let mut first4 = [0u8; 4];
            unsafe {
                core::ptr::copy_nonoverlapping(p.add(varatt::VARHDRSZ), first4.as_mut_ptr(), 4)
            };
            if first4 != self.prefix {
                return Err("strview: prefix disagrees with payload bytes (invariant 1/4)");
            }
            Ok(())
        }
    }
}

/// The `ColRep::StrView` overlay payload: a lane of [`StrCell`]s in the
/// claim arena + the arena epoch it was minted under (the R4 reuse trap —
/// consumers `assert_epoch` before reading).
///
/// The overlay law (module docs): the column's datum lane REMAINS the
/// authoritative varlena lane; cells are a side lane for native kernels.
#[derive(Clone, Copy)]
pub struct StrViews {
    cells: NonNull<StrCell>,
    epoch: u64,
}

impl StrViews {
    /// Publish a cell lane.
    ///
    /// # Safety
    ///
    /// The producer asserts: `cells` points to at least `nrows` cells for
    /// the batch this lane is staged into, allocated under the claim
    /// arena's `epoch`; every cell satisfies [`StrCell::check`]; the
    /// column's datum lane holds valid plain varlena pointers for every
    /// selected valid row (the overlay law); pointer-form payloads are
    /// live for the claim scope.
    #[inline]
    pub unsafe fn publish(cells: NonNull<StrCell>, epoch: u64) -> StrViews {
        StrViews { cells, epoch }
    }

    /// Raw cells pointer (identity/debugging; prefer [`StrViews::cells`]).
    #[inline]
    pub fn cells_ptr(self) -> NonNull<StrCell> {
        self.cells
    }

    /// The per-row cells for a batch staging `nrows` rows.
    ///
    /// # Safety
    ///
    /// Caller asserts the publish contract still holds (claim-scoped, R1)
    /// and `nrows` does not exceed the published row count. Callers with
    /// the arena in hand `assert_epoch(self.epoch())` first.
    #[inline]
    pub unsafe fn cells<'a>(self, nrows: usize) -> &'a [StrCell] {
        // SAFETY: publish contract.
        unsafe { core::slice::from_raw_parts(self.cells.as_ptr(), nrows) }
    }

    /// The arena epoch the lane was minted under.
    #[inline]
    pub fn epoch(self) -> u64 {
        self.epoch
    }
}

impl PartialEq for StrViews {
    fn eq(&self, other: &Self) -> bool {
        self.cells == other.cells && self.epoch == other.epoch
    }
}

impl Eq for StrViews {}

impl core::fmt::Debug for StrViews {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StrViews")
            .field("cells", &self.cells)
            .field("epoch", &self.epoch)
            .finish()
    }
}

impl Column {
    /// The StrView overlay payload, when staged.
    #[inline]
    pub fn strviews(&self) -> Option<StrViews> {
        match self.rep {
            ColRep::StrView(sv) => Some(sv),
            _ => None,
        }
    }

    /// The varlena→StrView producer (charter §4: the detoast pass builds
    /// cells while it already copies bytes). Builds a cell lane over the
    /// SELECTED rows of a varlena column whose selected valid datums are
    /// plain inline (the post-`detoast_selected`/`prove_inline` diet),
    /// proving as it goes.
    ///
    /// Zero-copy discipline: 4B-U long images are aliased in place;
    /// short-form long payloads (13..=126 bytes) are rehomed as arena 4B
    /// images; `<= 12`-byte strings inline into the cell. The datum lane
    /// is NEVER touched — it stays the authoritative varlena lane (overlay
    /// law), which is what makes [`Column::gather_back`] a rep flip.
    ///
    /// Returns FALSE (fail-open, column unchanged) when any selected valid
    /// datum still needs detoast — the caller detoasts and retries, or
    /// stays on the hosted route. Unselected and null rows get the zero
    /// cell.
    ///
    /// # Safety
    ///
    /// The column is staged per R1–R6: every selected valid datum points
    /// to a live varlena image readable for its header-declared extent,
    /// live for the claim scope; `nrows` covers every selected position.
    pub unsafe fn build_strviews(
        &mut self,
        sel: &Selection,
        nrows: usize,
        arena: &mut ClaimArena,
    ) -> bool {
        debug_assert!(
            matches!(self.base, ColRep::Varlena { .. }),
            "build_strviews wants a varlena-based column, base {:?}",
            self.base
        );
        if !matches!(self.rep, ColRep::Varlena { .. }) {
            // Overlay already staged (dict lanes route through
            // build_strviews_from_dict; anything else gathers back first).
            return false;
        }
        let lane = arena.alloc(nrows * core::mem::size_of::<StrCell>());
        let cells = lane.cast::<StrCell>();
        // SAFETY: fresh arena range of nrows cells; zero cell = all-zero.
        unsafe {
            core::ptr::write_bytes(cells.as_ptr(), 0, nrows);
        }
        for &pos in sel.as_slice() {
            let row = pos as usize;
            if !self.validity.is_valid(row) {
                continue;
            }
            let p = self.datums[row].as_usize() as *const u8;
            // SAFETY: caller contract — live image header.
            if unsafe { classify_varlena(p) }.needs_detoast() {
                // Fail open: the rep is untouched; the wasted arena bytes
                // die at the claim reset.
                return false;
            }
            // SAFETY: plain inline per the classify above; claim scope per
            // caller contract. Row < nrows per caller contract.
            unsafe {
                cells.as_ptr().add(row).write(StrCell::from_plain_image(p, arena));
            }
        }
        // The pass proved every selected valid datum plain inline.
        // SAFETY: publish contract established by this pass (cells under
        // the live epoch; datum lane untouched and plain-inline-proven).
        self.rep = ColRep::StrView(unsafe { StrViews::publish(cells, arena.epoch()) });
        true
    }

    /// The dict→StrView materialization (charter §4/§5: zero-copy views
    /// into the dictionary's payload region, which `DictHandle`/`DictEpoch`
    /// keeps generation-stable). The first real consumer of the
    /// `ColRep::DictCodes` overlay.
    ///
    /// For every row: the entry datum (a pointer into the generation-stable
    /// payload region — already a valid plain 4B-U image per §7b) fills the
    /// datum lane (the overlay law: hosted faces and degrade stay free),
    /// and the cell is built zero-copy — long entries alias the dict image;
    /// `<= 12`-byte entries inline (the only byte copies, ≤ 12 bytes each).
    /// Lengths come from the dict index ([`crate::DictSpace::byte_len`]),
    /// never a payload fault.
    ///
    /// Returns FALSE (fail-open, column unchanged) when the rep is not a
    /// dict overlay or the dict's base is not varlena-class.
    ///
    /// DictCodes stays the preferred rep where admission grants it (u32
    /// compare/hash beats 16-byte cells); this materialization is the
    /// lattice edge for kernels that need string values.
    ///
    /// # Safety
    ///
    /// The dict publish contract holds (claim-scoped codes + live provider
    /// per R1–R6); `nrows` does not exceed the published row count.
    pub unsafe fn build_strviews_from_dict(&mut self, nrows: usize, arena: &mut ClaimArena) -> bool {
        let ColRep::DictCodes(dc) = self.rep else {
            return false;
        };
        // SAFETY: publish contract (caller).
        let codes = unsafe { dc.codes(nrows) };
        let space = unsafe { dc.dict().space() };
        if !matches!(space.base_rep(), ColRep::Varlena { .. }) {
            return false;
        }
        debug_assert!(
            space.base_rep().same_base_class(self.base),
            "dict base_rep {:?} not the column's base class {:?}",
            space.base_rep(),
            self.base
        );
        self.ensure_rows(nrows);
        let lane = arena.alloc(nrows * core::mem::size_of::<StrCell>());
        let cells = lane.cast::<StrCell>();
        // SAFETY: fresh arena range of nrows cells.
        unsafe {
            core::ptr::write_bytes(cells.as_ptr(), 0, nrows);
        }
        for row in 0..nrows {
            // Dict lanes are all-valid (zero-null-proof rule); keep the
            // validity read anyway — a null row degrades to the zero cell
            // and a null datum, never a wild read.
            if !self.validity.is_valid(row) {
                self.datums[row] = Datum::null();
                continue;
            }
            let code = codes[row];
            let d = space.entry_datum(code);
            self.datums[row] = d;
            let len = space.byte_len(code) as usize;
            let image = d.as_usize() as *const u8;
            let cell = if len <= STRVIEW_INLINE_MAX {
                // SAFETY: §7b — the entry datum is a plain 4B-U image with
                // `len` payload bytes, generation-stable.
                let payload =
                    unsafe { core::slice::from_raw_parts(image.add(varatt::VARHDRSZ), len) };
                StrCell::inline_from_bytes(payload)
            } else {
                // SAFETY: §7b image, len > 12 — zero-copy alias.
                unsafe { StrCell::long_from_image(image) }
            };
            // SAFETY: row < nrows cells written above.
            unsafe { cells.as_ptr().add(row).write(cell) };
        }
        // SAFETY: publish contract established (datum lane = entry datums,
        // plain images per §7b; cells under the live epoch).
        self.rep = ColRep::StrView(unsafe { StrViews::publish(cells, arena.epoch()) });
        true
    }
}

/// The CONTIGUITY WITNESS (M5d.cells.q20-span; the v2 `SoaTextSpan`
/// lineage — `pgrcolumnar/src/scan.rs` `staged_text_span` + the
/// `exectuples::SoaTextSpan` publisher law, carried): a certificate that
/// one staged window's varlena images for a text column sit back-to-back
/// (modulo the arena's 8-byte alignment padding) inside ONE live readable
/// span, and the column's datum cells are STRICTLY ASCENDING pointers
/// into it.
///
/// Supplied by the READER where true (pgrc2_scan's verbatim granule
/// staging: `decode_full` bump-allocates every image into one
/// `ByteArena` pass in row order — the producer re-PROVES monotonicity
/// per window, verify-don't-assume, before publishing). Absence is the
/// default everywhere else (dict-gathered lanes, heap staging, delta
/// paths publish nothing) and DEMOTES the consumer to the per-value scan
/// — a typed census row, never a refusal (charter §4.6 default-open
/// posture: the witness gates ADMISSIBILITY of the fused span kernel).
///
/// Consumers (the STR-SPAN fused kernel) run ONE blob-wide substring
/// search over the span and map hits to rows through the datum lane,
/// rejecting hits that touch header/padding bytes or straddle a row
/// boundary — the per-row occurrence set is therefore identical to a
/// per-value search (the v2 identity argument, `laneexec/src/dict.rs`
/// `eval_contains_blob`).
///
/// Claim scope: the span aliases the granule decode scratch — the same
/// lifetime the column's datum pointers already rely on (R1–R6). The
/// witness is per-window advisory state, rebuilt with the column at
/// every window (`Batch` columns are reconstructed per window), so it is
/// never strandable.
#[derive(Clone, Copy, Debug)]
pub struct TextSpan {
    base: NonNull<u8>,
    len: usize,
}

impl TextSpan {
    /// Publish a span witness.
    ///
    /// # Safety
    ///
    /// The producer asserts: `base..base+len` is ONE live readable
    /// allocation for the claim scope of the batch this witness rides;
    /// every selected valid datum of the witnessed column points at a
    /// plain 4B-U varlena image whose full extent lies inside the span;
    /// the window's datum pointers are strictly ascending in row order.
    #[inline]
    pub unsafe fn publish(base: NonNull<u8>, len: usize) -> TextSpan {
        TextSpan { base, len }
    }

    /// Span base pointer.
    #[inline]
    pub fn base(self) -> NonNull<u8> {
        self.base
    }

    /// Span length in bytes.
    #[inline]
    pub fn len(self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    /// The span bytes.
    ///
    /// # Safety
    ///
    /// Caller asserts the publish contract still holds (claim-scoped)
    /// and the returned slice does not outlive the claim.
    #[inline]
    pub unsafe fn bytes<'a>(self) -> &'a [u8] {
        // SAFETY: publish contract.
        unsafe { core::slice::from_raw_parts(self.base.as_ptr(), self.len) }
    }

    /// The verify-don't-assume CERTIFIER (the v2 `staged_text_span` law:
    /// the writer/decoder never reorders the images, but the hit→row
    /// mapping depends on it — re-prove per window, never assume). The
    /// caller supplies PROVENANCE + LIVENESS: `lo..hi` is the ONE live
    /// readable allocation its decode pass staged this window's images
    /// into (the granule decode arena). The certifier re-proves the
    /// checkable half: every row valid, every datum a strictly-ascending
    /// pointer inside the allocation, the last image a plain 4B-U whose
    /// extent stays inside it. Any failure returns None — the consumer's
    /// typed demotion to the per-value path (charter §4.6: absence
    /// demotes, never refuses).
    ///
    /// # Safety
    ///
    /// `lo..hi` is one live allocation, readable for the claim scope the
    /// witness will ride (the only dereference is the last image's
    /// header, proven in-bounds before the read).
    pub unsafe fn certify(
        datums: &[Datum],
        validity: &crate::Validity,
        lo: usize,
        hi: usize,
    ) -> Option<TextSpan> {
        if datums.is_empty() || hi <= lo {
            return None;
        }
        let mut prev = 0usize;
        for (r, d) in datums.iter().enumerate() {
            // All-valid windows only (null datums are not pointers; the
            // v2 posture — a null anywhere demotes the window).
            if !validity.is_valid(r) {
                return None;
            }
            let p = d.as_usize();
            // Strictly ascending, inside the allocation, header readable.
            if p <= prev || p < lo || p.checked_add(varatt::VARHDRSZ)? > hi {
                return None;
            }
            // Every image must be plain 4B-U: the span consumer reads
            // payload bounds as `varsize_4b` per owning row — any other
            // header form would diverge from the per-value twin.
            // SAFETY: header readable (bounds proven above); allocation
            // live per the caller contract.
            if !unsafe { varatt::varatt_is_4b_u(p as *const u8) } {
                return None;
            }
            prev = p;
        }
        // SAFETY: header read proven in-bounds + 4B-U proven above.
        let end = prev.checked_add(unsafe { varatt::varsize_4b(prev as *const u8) })?;
        if end > hi {
            return None;
        }
        let first = datums[0].as_usize();
        // SAFETY: the publish contract is exactly what this pass proved
        // (ascending in-allocation pointers; extent inside the span) plus
        // the caller's provenance/liveness assertion.
        Some(unsafe { TextSpan::publish(NonNull::new(first as *mut u8)?, end - first) })
    }
}

/// The fmgr scratch (charter §4): materializes inline cells as short-format
/// varlenas for hosted/per-row-fmgr consumption — 1-byte header + ≤12
/// bytes ⇒ **≤13 bytes**, two word stores. Long cells hand their payload
/// pointer through unchanged (zero-copy by invariant 4).
///
/// A passed capability, caller-owned, ZERO thread-locals: the owner resets
/// at expression-eval cadence (C-parity with the fmgr argument-lifetime
/// contract — `PG_GETARG_TEXT_PP`-class readers accept packed varlenas).
/// Built on [`ClaimArena`], inheriting the four arena laws (≥8-aligned
/// values, value stability until reset, epoch reuse-trap, worker-private).
pub struct StrScratch {
    arena: ClaimArena,
}

impl Default for StrScratch {
    fn default() -> Self {
        Self::new()
    }
}

impl StrScratch {
    pub fn new() -> StrScratch {
        StrScratch { arena: ClaimArena::new() }
    }

    /// Materialize `cell` as a varlena datum for the hosted boundary.
    ///
    /// Inline form: writes a 1B-short image (`[header][len bytes]`,
    /// ≤13 bytes) into the scratch with two word stores. Pointer form:
    /// returns the payload pointer AS the datum (invariant 4) — zero-copy.
    ///
    /// The returned datum is valid until [`StrScratch::reset`] (inline
    /// form) or the claim scope's end (pointer form).
    ///
    /// # Safety
    ///
    /// Claim scope for pointer-form cells (the payload the datum aliases
    /// must be live for the datum's use).
    #[inline]
    pub unsafe fn materialize(&mut self, cell: &StrCell) -> Datum {
        if !cell.is_inline() {
            return Datum::from_usize(cell.image_ptr() as usize);
        }
        let len = cell.len();
        let total = varatt::VARHDRSZ_SHORT + len;
        debug_assert!(total <= 13);
        // Two word stores: byte 0 = short header, bytes 1..13 = the cell's
        // 12 content bytes (padding slop rides free; extent is `total`).
        let mut img = [0u8; 16];
        img[1..5].copy_from_slice(&cell.prefix);
        img[5..13].copy_from_slice(&cell.tail);
        // SAFETY: img[0] is writable; total <= VARATT_SHORT_MAX.
        unsafe { varatt::set_varsize_short(img.as_mut_ptr(), total) };
        let dst = self.arena.alloc(16).as_ptr();
        let lo = u64::from_ne_bytes(img[0..8].try_into().expect("len 8"));
        let hi = u64::from_ne_bytes(img[8..16].try_into().expect("len 8"));
        // SAFETY: fresh 16-byte ≥8-aligned arena range.
        unsafe {
            dst.cast::<u64>().write(lo);
            dst.add(8).cast::<u64>().write(hi);
        }
        Datum::from_usize(dst as usize)
    }

    /// Reset at expression-eval cadence (caller-owned). Every inline-form
    /// datum minted since the last reset is DEAD (arena epoch bump — the
    /// reuse trap).
    pub fn reset(&mut self) {
        self.arena.reset();
    }

    /// Bytes currently allocated this cadence (witness currency).
    pub fn used(&self) -> usize {
        self.arena.used()
    }
}
