//! Row-layout spill pages (charter C5 law): fixed-width rows and var-len
//! data on SEPARATE pages, with explicit pointer swizzle on reload.
//!
//! A page is a self-describing byte buffer: [`PageHdr`] at offset 0 (32
//! bytes, layout-pinned), then either row_size-strided rows (row pages) or
//! append-only var cells (var pages). The ON-DISK image is the page buffer
//! verbatim with every ref word in UNSWIZZLED form — a written page never
//! contains a raw address; swizzling is a resident-only, batch-scoped state
//! applied and reversed by the pool ([`crate::pool::SpillPool::swizzle`]).
//!
//! Ref words ([`VarRef`], 8 bytes, layout-pinned) live at header-declared
//! offsets inside each row, so the swizzle walk is MECHANICAL — the pool
//! needs no consumer callback and no per-row type knowledge. Var cells are
//! `[len: u32][reserved: u32][payload][pad to 8]`: payloads are 8-aligned
//! (the pgrc2_batch arena alignment law, kept with margin) and a swizzled ref is
//! the PAYLOAD address, so `len` sits at `addr - 8`.
//!
//! Header validation is FAIL-CLOSED on reload: magic, kind, count/used
//! consistency, and ref-offset bounds are all checked before any byte is
//! interpreted; a torn or foreign page is an error, never a misread.

use ::elog::ereport;
use ::types_error::{PgResult, ERROR};

/// Pool/page grain. 64 KiB: small enough that the 2-page pool floor stays
/// near C's own per-open-BufFile overhead scale under tiny work_mem, large
/// enough to amortize one open/write/close event per page. The SIZE prices
/// engagement, so it is layout-pinned; consumers re-fit their flush cadence
/// against it under the witnessed-ladder discipline, never by editing the
/// constant silently.
pub const PAGE_SIZE: usize = 64 * 1024;

/// Ref words per row layout (header carries a fixed slot array).
pub const MAX_ROW_REFS: usize = 8;

/// Header magic (fail-closed reload validation).
const PAGE_MAGIC: u16 = 0xA55F;

/// Var-cell header bytes ahead of the payload: `[len: u32][reserved: u32]`.
const VAR_CELL_HDR: usize = 8;

/// `PageHdr::size_max` for jumbo var pages (`MaxAllocSize` parity: a single
/// spill value larger than 1 GiB is refused exactly where C's palloc would
/// have refused it long before).
pub const MAX_VAR_CELL: usize = 0x4000_0000 - VAR_CELL_HDR - HDR_LEN;

/// Page kind discriminator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PageKind {
    /// Fixed-width rows, refs at declared offsets.
    Row = 1,
    /// Append-only var-len cells.
    Var = 2,
}

/// The 32-byte page header, present at offset 0 of every page and of every
/// on-disk page image. `#[repr(C)]` + the layout pin in `tests::layout`
/// make the byte format an explicit contract (sizeof-priced gates need
/// layout pins).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct PageHdr {
    /// [`PAGE_MAGIC`].
    pub magic: u16,
    /// [`PageKind`] as u8.
    pub kind: u8,
    /// Row pages: live ref slots in `ref_offs`. Var pages: 0.
    pub nrefs: u8,
    /// Row pages: bytes per row (multiple of 8, ≥ 8). Var pages: 0.
    pub row_size: u32,
    /// Row pages: rows stored. Var pages: cells stored.
    pub count: u32,
    /// Bytes used including this header (append watermark).
    pub used: u32,
    /// Row pages: byte offsets of the [`VarRef`] words within each row
    /// (slots `0..nrefs` live, ascending, 8-aligned). Var pages: zeroed.
    pub ref_offs: [u16; MAX_ROW_REFS],
}

/// Header length as stored (== `size_of::<PageHdr>()`, pinned).
pub const HDR_LEN: usize = 32;

const _: () = assert!(core::mem::size_of::<PageHdr>() == HDR_LEN);
const _: () = assert!(core::mem::align_of::<PageHdr>() <= 8);
const _: () = assert!(PAGE_SIZE % 8 == 0 && HDR_LEN % 8 == 0);

impl PageHdr {
    /// Read the header out of a page buffer (no validation — see
    /// [`validate_page`] for the fail-closed reload path).
    pub fn read(buf: &[u8]) -> PageHdr {
        debug_assert!(buf.len() >= HDR_LEN);
        let mut h = PageHdr {
            magic: u16::from_ne_bytes([buf[0], buf[1]]),
            kind: buf[2],
            nrefs: buf[3],
            row_size: u32::from_ne_bytes(buf[4..8].try_into().unwrap()),
            count: u32::from_ne_bytes(buf[8..12].try_into().unwrap()),
            used: u32::from_ne_bytes(buf[12..16].try_into().unwrap()),
            ref_offs: [0; MAX_ROW_REFS],
        };
        for (i, slot) in h.ref_offs.iter_mut().enumerate() {
            let at = 16 + i * 2;
            *slot = u16::from_ne_bytes([buf[at], buf[at + 1]]);
        }
        h
    }

    /// Write the header into a page buffer.
    pub fn write(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= HDR_LEN);
        buf[0..2].copy_from_slice(&self.magic.to_ne_bytes());
        buf[2] = self.kind;
        buf[3] = self.nrefs;
        buf[4..8].copy_from_slice(&self.row_size.to_ne_bytes());
        buf[8..12].copy_from_slice(&self.count.to_ne_bytes());
        buf[12..16].copy_from_slice(&self.used.to_ne_bytes());
        for (i, slot) in self.ref_offs.iter().enumerate() {
            let at = 16 + i * 2;
            buf[at..at + 2].copy_from_slice(&slot.to_ne_bytes());
        }
    }
}

/// A row page's shape: row width plus the ref-word offsets the mechanical
/// swizzle walk visits. Constructed VALIDATED; the same invariants are
/// re-checked fail-closed on reload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowLayout {
    row_size: u32,
    nrefs: u8,
    ref_offs: [u16; MAX_ROW_REFS],
}

impl RowLayout {
    /// `row_size` must be a positive multiple of 8 that fits at least one
    /// row per page; `refs` (≤ [`MAX_ROW_REFS`]) must be 8-aligned,
    /// strictly ascending, in-row offsets.
    pub fn new(row_size: u32, refs: &[u16]) -> PgResult<RowLayout> {
        let ok = row_size >= 8
            && row_size % 8 == 0
            && (row_size as usize) <= PAGE_SIZE - HDR_LEN
            && refs.len() <= MAX_ROW_REFS
            && refs.windows(2).all(|w| w[0] < w[1])
            && refs.iter().all(|&o| o % 8 == 0 && (o as u32) + 8 <= row_size);
        if !ok {
            return spill_err(format!(
                "invalid spill row layout: row_size {row_size}, refs {refs:?}"
            ));
        }
        let mut ref_offs = [0u16; MAX_ROW_REFS];
        ref_offs[..refs.len()].copy_from_slice(refs);
        Ok(RowLayout { row_size, nrefs: refs.len() as u8, ref_offs })
    }

    pub fn row_size(&self) -> u32 {
        self.row_size
    }

    /// Live ref-word offsets.
    pub fn refs(&self) -> &[u16] {
        &self.ref_offs[..self.nrefs as usize]
    }

    /// Rows a [`PAGE_SIZE`] page holds under this layout.
    pub fn rows_per_page(&self) -> u32 {
        ((PAGE_SIZE - HDR_LEN) / self.row_size as usize) as u32
    }
}

/// One 8-byte ref word inside a fixed-width row, naming var-len bytes on a
/// var page. Two states, distinguished by bit 0:
///
/// - **UNSWIZZLED** (bit 0 = 1) — the at-rest and ON-DISK form:
///   `(page_index << 32) | (cell_offset << 1) | 1`. Cell offsets are
///   header-relative byte offsets of the CELL start (≥ [`HDR_LEN`], so the
///   encoding `1` — page 0, offset 0 — is unreachable and serves as
///   [`VarRef::NULL`]).
/// - **SWIZZLED** (bit 0 = 0) — a raw PAYLOAD address, valid only inside a
///   [`crate::pool::SwizzleToken`] scope (payloads are 8-aligned, so bit 0
///   of a real address is always 0). `len` is at `addr - 8`.
///
/// The word is `#[repr(transparent)]` u64 and layout-pinned: rows embed it
/// byte-for-byte, and the on-disk row image is the in-memory row image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct VarRef(pub u64);

impl VarRef {
    /// The null ref (no value): unswizzled-tagged, unreachable encoding.
    pub const NULL: VarRef = VarRef(1);

    /// Unswizzled form for a cell at `cell_off` on var page `page_index`.
    pub fn encode(page_index: u32, cell_off: u32) -> VarRef {
        debug_assert!(cell_off as usize >= HDR_LEN);
        debug_assert!(cell_off < (1 << 31));
        VarRef(((page_index as u64) << 32) | ((cell_off as u64) << 1) | 1)
    }

    /// True for the at-rest/on-disk form (bit 0 set) — including NULL.
    pub fn is_unswizzled(&self) -> bool {
        self.0 & 1 == 1
    }

    pub fn is_null(&self) -> bool {
        self.0 == Self::NULL.0
    }

    /// Decode the unswizzled form → (page_index, cell_off). Panics on a
    /// swizzled word (debug) — callers branch on [`VarRef::is_unswizzled`].
    pub fn decode(&self) -> (u32, u32) {
        debug_assert!(self.is_unswizzled() && !self.is_null());
        ((self.0 >> 32) as u32, ((self.0 & 0xFFFF_FFFF) >> 1) as u32)
    }

    /// The swizzled (resident) form for a payload address.
    ///
    /// # Safety contract (enforced by the pool, asserted here)
    ///
    /// `addr` must be an 8-aligned payload address inside a pinned var
    /// page; the word must be unswizzled again before the pin is released.
    pub fn swizzled(addr: *const u8) -> VarRef {
        debug_assert!(addr as usize % 8 == 0 && !addr.is_null());
        VarRef(addr as u64)
    }

    /// Resolve a SWIZZLED ref to its payload slice.
    ///
    /// # Safety
    ///
    /// Only valid inside the [`crate::pool::SwizzleToken`] scope that
    /// produced the word (the state-pointer lifetime law): the target page
    /// is pinned and the address is the one `swizzle` wrote.
    pub unsafe fn payload<'a>(&self) -> &'a [u8] {
        debug_assert!(!self.is_unswizzled());
        let addr = self.0 as *const u8;
        // SAFETY: caller contract — addr is a live pinned var-page payload
        // whose length prefix sits at addr - 8 (the cell format).
        unsafe {
            let len = u32::from_ne_bytes(*(addr.sub(8) as *const [u8; 4])) as usize;
            core::slice::from_raw_parts(addr, len)
        }
    }
}

const _: () = assert!(core::mem::size_of::<VarRef>() == 8);

// ---------------------------------------------------------------------------
// Page initialization + fail-closed validation
// ---------------------------------------------------------------------------

/// Initialize `buf` as an empty row page under `layout`.
pub fn init_row_page(buf: &mut [u8], layout: &RowLayout) {
    debug_assert!(buf.len() >= PAGE_SIZE);
    let hdr = PageHdr {
        magic: PAGE_MAGIC,
        kind: PageKind::Row as u8,
        nrefs: layout.nrefs,
        row_size: layout.row_size,
        count: 0,
        used: HDR_LEN as u32,
        ref_offs: layout.ref_offs,
    };
    hdr.write(buf);
}

/// Initialize `buf` as an empty var page.
pub fn init_var_page(buf: &mut [u8]) {
    debug_assert!(buf.len() >= HDR_LEN);
    let hdr = PageHdr {
        magic: PAGE_MAGIC,
        kind: PageKind::Var as u8,
        nrefs: 0,
        row_size: 0,
        count: 0,
        used: HDR_LEN as u32,
        ref_offs: [0; MAX_ROW_REFS],
    };
    hdr.write(buf);
}

/// Fail-closed reload validation: magic, kind, watermark bounds, row/ref
/// arithmetic. `expect_kind` pins the slot's own record of what it wrote
/// against what came back (a crossed extent slot is an error, not a
/// misread).
pub fn validate_page(buf: &[u8], expect_kind: PageKind) -> PgResult<PageHdr> {
    if buf.len() < HDR_LEN {
        return spill_err(format!("spill page too short: {} bytes", buf.len()));
    }
    let h = PageHdr::read(buf);
    let kind_ok = h.kind == expect_kind as u8;
    let used_ok = (h.used as usize) >= HDR_LEN && (h.used as usize) <= buf.len();
    let shape_ok = match expect_kind {
        PageKind::Row => {
            h.row_size >= 8
                && h.row_size % 8 == 0
                && (h.nrefs as usize) <= MAX_ROW_REFS
                && h.ref_offs[..h.nrefs as usize].windows(2).all(|w| w[0] < w[1])
                && h.ref_offs[..h.nrefs as usize]
                    .iter()
                    .all(|&o| o % 8 == 0 && (o as u32) + 8 <= h.row_size)
                && h.used as usize == HDR_LEN + h.count as usize * h.row_size as usize
        }
        PageKind::Var => h.row_size == 0 && h.nrefs == 0,
    };
    if h.magic != PAGE_MAGIC || !kind_ok || !used_ok || !shape_ok {
        return spill_err(format!(
            "invalid spill page header (magic {:#06x}, kind {}, row_size {}, count {}, used {})",
            h.magic, h.kind, h.row_size, h.count, h.used
        ));
    }
    Ok(h)
}

// ---------------------------------------------------------------------------
// Typed views over a pinned page buffer
// ---------------------------------------------------------------------------
//
// Views borrow the raw buffer slice the pool resolves from a PagePin; the
// pin is the proof of residency and the borrow ends before the pin can be
// returned (unpin consumes the pin by value). Mutating views are minted
// only by pool methods that mark the slot dirty.

/// Read view over a row page.
pub struct RowPageRef<'a> {
    buf: &'a [u8],
    hdr: PageHdr,
}

impl<'a> RowPageRef<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> RowPageRef<'a> {
        let hdr = PageHdr::read(buf);
        debug_assert_eq!(hdr.kind, PageKind::Row as u8);
        RowPageRef { buf, hdr }
    }

    pub fn count(&self) -> u32 {
        self.hdr.count
    }

    pub fn row_size(&self) -> u32 {
        self.hdr.row_size
    }

    pub fn row(&self, i: u32) -> &'a [u8] {
        assert!(i < self.hdr.count, "row index {i} out of {}", self.hdr.count);
        let at = HDR_LEN + i as usize * self.hdr.row_size as usize;
        &self.buf[at..at + self.hdr.row_size as usize]
    }

    /// The ref word at layout slot `slot` of row `i`.
    pub fn ref_word(&self, i: u32, slot: usize) -> VarRef {
        assert!(slot < self.hdr.nrefs as usize);
        let row = self.row(i);
        let at = self.hdr.ref_offs[slot] as usize;
        VarRef(u64::from_ne_bytes(row[at..at + 8].try_into().unwrap()))
    }
}

/// Write view over a row page (minted dirty).
pub struct RowPageMut<'a> {
    buf: &'a mut [u8],
    hdr: PageHdr,
}

impl<'a> RowPageMut<'a> {
    pub(crate) fn new(buf: &'a mut [u8]) -> RowPageMut<'a> {
        let hdr = PageHdr::read(buf);
        debug_assert_eq!(hdr.kind, PageKind::Row as u8);
        RowPageMut { buf, hdr }
    }

    pub fn count(&self) -> u32 {
        self.hdr.count
    }

    pub fn row_size(&self) -> u32 {
        self.hdr.row_size
    }

    /// Append one row image (`len == row_size`); `None` when the page is
    /// full — the consumer allocs its next page (never an error: page
    /// rotation is the designed cadence).
    pub fn try_push_row(&mut self, row: &[u8]) -> Option<u32> {
        assert_eq!(row.len(), self.hdr.row_size as usize, "row image width");
        let at = HDR_LEN + self.hdr.count as usize * self.hdr.row_size as usize;
        if at + row.len() > self.buf.len() {
            return None;
        }
        self.buf[at..at + row.len()].copy_from_slice(row);
        self.hdr.count += 1;
        self.hdr.used = (at + row.len()) as u32;
        self.hdr.write(self.buf);
        Some(self.hdr.count - 1)
    }

    pub fn row_mut(&mut self, i: u32) -> &mut [u8] {
        assert!(i < self.hdr.count, "row index {i} out of {}", self.hdr.count);
        let at = HDR_LEN + i as usize * self.hdr.row_size as usize;
        &mut self.buf[at..at + self.hdr.row_size as usize]
    }

    pub fn ref_word(&self, i: u32, slot: usize) -> VarRef {
        assert!(slot < self.hdr.nrefs as usize && i < self.hdr.count);
        let at =
            HDR_LEN + i as usize * self.hdr.row_size as usize + self.hdr.ref_offs[slot] as usize;
        VarRef(u64::from_ne_bytes(self.buf[at..at + 8].try_into().unwrap()))
    }

    /// Store a ref word (at-rest writes store the UNSWIZZLED form; the pool
    /// alone writes swizzled words, inside a token scope).
    pub fn set_ref_word(&mut self, i: u32, slot: usize, r: VarRef) {
        assert!(slot < self.hdr.nrefs as usize && i < self.hdr.count);
        let at =
            HDR_LEN + i as usize * self.hdr.row_size as usize + self.hdr.ref_offs[slot] as usize;
        self.buf[at..at + 8].copy_from_slice(&r.0.to_ne_bytes());
    }
}

/// Read view over a var page.
pub struct VarPageRef<'a> {
    buf: &'a [u8],
}

impl<'a> VarPageRef<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> VarPageRef<'a> {
        debug_assert_eq!(PageHdr::read(buf).kind, PageKind::Var as u8);
        VarPageRef { buf }
    }

    /// The payload of the cell at `cell_off` (bounds-checked against the
    /// watermark — a stale offset fails loudly).
    pub fn get(&self, cell_off: u32) -> &'a [u8] {
        let hdr = PageHdr::read(self.buf);
        let at = cell_off as usize;
        assert!(
            at >= HDR_LEN && at + VAR_CELL_HDR <= hdr.used as usize,
            "var cell offset {cell_off} out of bounds"
        );
        let len = u32::from_ne_bytes(self.buf[at..at + 4].try_into().unwrap()) as usize;
        assert!(at + VAR_CELL_HDR + len <= hdr.used as usize, "var cell length {len} torn");
        &self.buf[at + VAR_CELL_HDR..at + VAR_CELL_HDR + len]
    }
}

/// Write view over a var page (minted dirty).
pub struct VarPageMut<'a> {
    buf: &'a mut [u8],
    hdr: PageHdr,
}

impl<'a> VarPageMut<'a> {
    pub(crate) fn new(buf: &'a mut [u8]) -> VarPageMut<'a> {
        let hdr = PageHdr::read(buf);
        debug_assert_eq!(hdr.kind, PageKind::Var as u8);
        VarPageMut { buf, hdr }
    }

    /// Append one cell; returns its cell offset (for [`VarRef::encode`]),
    /// or `None` when the page cannot hold it — the consumer allocs its
    /// next var page (or a jumbo page via
    /// [`crate::pool::SpillPool::alloc_var_for`] when the VALUE alone
    /// exceeds a page).
    pub fn try_append(&mut self, payload: &[u8]) -> Option<u32> {
        let cell = VAR_CELL_HDR + payload.len();
        let cell_padded = (cell + 7) & !7;
        let at = ((self.hdr.used as usize) + 7) & !7;
        if at + cell_padded > self.buf.len() {
            return None;
        }
        self.buf[at..at + 4].copy_from_slice(&(payload.len() as u32).to_ne_bytes());
        self.buf[at + 4..at + 8].copy_from_slice(&0u32.to_ne_bytes());
        self.buf[at + 8..at + 8 + payload.len()].copy_from_slice(payload);
        self.hdr.count += 1;
        self.hdr.used = (at + cell_padded) as u32;
        self.hdr.write(self.buf);
        Some(at as u32)
    }

    pub fn get(&self, cell_off: u32) -> &[u8] {
        let at = cell_off as usize;
        assert!(at >= HDR_LEN && at + VAR_CELL_HDR <= self.hdr.used as usize);
        let len = u32::from_ne_bytes(self.buf[at..at + 4].try_into().unwrap()) as usize;
        &self.buf[at + VAR_CELL_HDR..at + VAR_CELL_HDR + len]
    }
}

/// Error-site location helper (the fd/vfd `loc` pattern: report where in
/// OUR source the error was raised; `#[track_caller]` resolves to the call
/// site).
#[track_caller]
pub(crate) fn loc(funcname: &'static str) -> ::types_error::ErrorLocation {
    let site = core::panic::Location::caller();
    ::types_error::ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

/// Raise an internal-class spill error (fail-closed paths: header
/// corruption, layout misuse, pool exhaustion).
#[track_caller]
pub(crate) fn spill_err<T>(msg: String) -> PgResult<T> {
    ereport(ERROR).errmsg_internal(msg).finish(loc("sqe_spill"))?;
    unreachable!("ereport(ERROR) returned");
}
