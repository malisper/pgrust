//! The PRIVATE, work_mem-accounted spill page pool (O-M2-4: engagement-
//! private, charged against the participant's budget; shared_buffers
//! integration DECLINED — C-parity in mechanism, BufFile is private
//! buffering too).
//!
//! ## Ownership and threading
//!
//! One pool per participant working set (a sink Local, or a combine task's
//! own working state). The pool is single-owner `&mut` state: `Send + Sync`
//! as a TYPE (it rides a Local through seal; compile-time witness in
//! `tests::byref`), but it has NO interior mutability and NO lock —
//! cross-thread handoff happens only at the sealed staging points the sink
//! contract already sequences. Worker-private pools + by-name file streams
//! are the whole concurrency story (crate docs: no new shared state, no
//! new loom obligations).
//!
//! ## The pin/swizzle discipline (state-pointer lifetime law)
//!
//! - [`PagePin`] is proof of residency: pinned frames never move, never
//!   evict, never unload. Pins are `!Send`, `#[must_use]`, and returned by
//!   VALUE to [`SpillPool::unpin`] — a use-after-unpin is a compile error,
//!   not a runtime race.
//! - [`SpillPool::swizzle`] pins the row page and every referenced var
//!   page, rewrites ref words to raw payload addresses, and returns a
//!   [`SwizzleToken`]; [`SpillPool::unswizzle`] reverses it. Raw addresses
//!   exist ONLY inside a token's scope — between batches, at the staging
//!   point, the token is consumed and every ref word is back in its
//!   at-rest (on-disk) form. Eviction can therefore never observe a
//!   swizzled page: swizzled ⇒ pinned ⇒ not evictable.
//!
//! ## Budget law
//!
//! `budget_bytes` comes from the CONSUMER (the C-parity work_mem formulas
//! are the M2-D faces; this crate reads no GUC). The pool enforces it as a
//! cap on resident frame bytes, evicting unpinned pages (write-back on
//! dirty) to stay under. Two documented departures from a naive hard cap,
//! both C-parity in spirit:
//!
//! - **floor**: two pages minimum — a working set needs a row page and a
//!   var page resident to make progress (C's BufFile likewise keeps its
//!   buffer regardless of work_mem);
//! - **jumbo overshoot**: a single value larger than the whole budget
//!   evicts everything evictable and then proceeds overshooting (C's hash
//!   table also overshoots before its spill machinery reacts — refusal is
//!   not degradation); the overshoot is witnessed by
//!   `peak_resident_bytes`.
//!
//! A demand that cannot be met because every frame is PINNED is an
//! internal ERROR — consumers flush at their own crossing checks BEFORE
//! pinning that much (checks at morsel/flush cadence, never per row).
//!
//! ## On-disk identity
//!
//! Unloaded pages live in the pool's own [`SpillFile`] at STABLE
//! page-aligned extent slots assigned at first unload: re-unloads rewrite
//! in place, so pool files never grow from re-spill (temp_file_limit,
//! charged on growth, sees each page once). Reloads validate the page
//! header FAIL-CLOSED before any byte is interpreted. A failed write-back
//! leaves the page RESIDENT (no torn unload state); a failed reload
//! releases the frame and leaves the page unloaded.

use core::marker::PhantomData;

use ::types_error::PgResult;

use crate::page::{
    init_row_page, init_var_page, spill_err, validate_page, PageHdr, PageKind, RowLayout,
    RowPageMut, RowPageRef, VarPageMut, VarPageRef, VarRef, HDR_LEN, MAX_VAR_CELL, PAGE_SIZE,
};
use crate::set::SpillFile;
use crate::SpillMetrics;

/// Pool-local page identity (index into the slot table; stable for the
/// pool's lifetime — a slot is never reused for a different page).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PageId(pub u32);

/// Proof of residency for one page (byref class `PagePin`). `!Send`; must
/// be returned by value to [`SpillPool::unpin`] before the staging point.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// fn f() { assert_send::<sqe_spill::PagePin>(); }
/// ```
#[must_use = "a PagePin must be returned to SpillPool::unpin"]
#[derive(Debug)]
pub struct PagePin {
    id: PageId,
    _not_send: PhantomData<*const ()>,
}

impl PagePin {
    pub fn id(&self) -> PageId {
        self.id
    }
}

/// Scope witness for swizzled (raw-address) ref words in one row page
/// (byref class `SwizzledRefs`). `!Send`; must be consumed by
/// [`SpillPool::unswizzle`] at the staging point. Dropping it un-consumed
/// is a debug-build panic (the drop-bomb belt): the row page would be
/// stranded pinned with raw addresses at rest.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// fn f() { assert_send::<sqe_spill::SwizzleToken>(); }
/// ```
#[must_use = "a SwizzleToken must be consumed by SpillPool::unswizzle"]
#[derive(Debug)]
pub struct SwizzleToken {
    row: PageId,
    /// Pinned var pages backing the raw addresses: (page, frame base),
    /// sorted by base for the unswizzle reverse lookup. The token CARRIES
    /// these pin counts (plus the row page's) until unswizzle returns them.
    vars: Vec<(PageId, usize)>,
    consumed: bool,
    _not_send: PhantomData<*const ()>,
}

impl SwizzleToken {
    pub fn row_page(&self) -> PageId {
        self.row
    }
}

impl Drop for SwizzleToken {
    fn drop(&mut self) {
        if !self.consumed && !std::thread::panicking() {
            debug_assert!(false, "SwizzleToken dropped without SpillPool::unswizzle");
        }
    }
}

enum SlotState {
    Resident { buf: Box<[u8]>, pins: u32, dirty: bool, swizzled: bool },
    Unloaded,
}

struct Slot {
    kind: PageKind,
    /// Frame length in bytes (== PAGE_SIZE except jumbo var pages).
    frame_bytes: usize,
    /// Stable on-disk location in PAGE_SIZE units, assigned at first
    /// unload (in-place rewrite thereafter; jumbo pages take consecutive
    /// units).
    ext_page: Option<u64>,
    state: SlotState,
}

/// The pool. See the module docs for the four laws (ownership,
/// pin/swizzle, budget, on-disk identity).
pub struct SpillPool {
    budget_bytes: usize,
    resident_bytes: usize,
    slots: Vec<Slot>,
    /// Reusable PAGE_SIZE frames from evicted/unloaded pages (jumbo frames
    /// are freed, not cached).
    free_frames: Vec<Box<[u8]>>,
    file: SpillFile,
    next_ext_page: u64,
    live_pins: u64,
    evict_cursor: usize,
    metrics: SpillMetrics,
}

impl SpillPool {
    /// `budget_bytes` is the participant's spill working-set budget
    /// (consumer-derived from the C formulas); floored at two pages.
    pub fn new(budget_bytes: usize, file: SpillFile) -> SpillPool {
        SpillPool {
            budget_bytes: budget_bytes.max(2 * PAGE_SIZE),
            resident_bytes: 0,
            slots: Vec::new(),
            free_frames: Vec::new(),
            file,
            next_ext_page: 0,
            live_pins: 0,
            evict_cursor: 0,
            metrics: SpillMetrics::default(),
        }
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget_bytes
    }

    /// Resident frame bytes right now (the consumer's crossing checks read
    /// this at flush cadence).
    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    /// Live pins (teardown-contract witness; tests read this).
    pub fn live_pins(&self) -> u64 {
        self.live_pins
    }

    /// Measured-only counters (fold with [`SpillMetrics::merge`] at seal).
    pub fn metrics(&self) -> SpillMetrics {
        self.metrics
    }

    pub fn page_kind(&self, id: PageId) -> PageKind {
        self.slots[id.0 as usize].kind
    }

    pub fn is_resident(&self, id: PageId) -> bool {
        matches!(self.slots[id.0 as usize].state, SlotState::Resident { .. })
    }

    // ------------------------------------------------------------- alloc --

    /// New empty row page under `layout`; returned pinned (the caller is
    /// about to write rows).
    pub fn alloc_row(&mut self, layout: &RowLayout) -> PgResult<PagePin> {
        let mut buf = self.acquire_frame(PAGE_SIZE)?;
        init_row_page(&mut buf, layout);
        Ok(self.install(PageKind::Row, buf))
    }

    /// New empty var page; returned pinned.
    pub fn alloc_var(&mut self) -> PgResult<PagePin> {
        let mut buf = self.acquire_frame(PAGE_SIZE)?;
        init_var_page(&mut buf);
        Ok(self.install(PageKind::Var, buf))
    }

    /// New empty var page sized to hold ONE cell of `payload_len` bytes —
    /// the jumbo arm for values a standard page cannot hold. Frame length
    /// is the page-multiple round-up (extent slots stay page-strided).
    pub fn alloc_var_for(&mut self, payload_len: usize) -> PgResult<PagePin> {
        if payload_len > MAX_VAR_CELL {
            return spill_err(format!(
                "spill value of {payload_len} bytes exceeds the maximum ({MAX_VAR_CELL})"
            ));
        }
        let need = HDR_LEN + 8 + payload_len.div_ceil(8) * 8;
        let frame = need.div_ceil(PAGE_SIZE) * PAGE_SIZE;
        let mut buf = self.acquire_frame(frame)?;
        init_var_page(&mut buf);
        Ok(self.install(PageKind::Var, buf))
    }

    fn install(&mut self, kind: PageKind, buf: Box<[u8]>) -> PagePin {
        let frame_bytes = buf.len();
        let id = PageId(self.slots.len() as u32);
        self.slots.push(Slot {
            kind,
            frame_bytes,
            ext_page: None,
            state: SlotState::Resident { buf, pins: 1, dirty: true, swizzled: false },
        });
        self.live_pins += 1;
        PagePin { id, _not_send: PhantomData }
    }

    // --------------------------------------------------------- pin/unpin --

    /// Pin a page, reloading it (one I/O event, header-validated
    /// fail-closed) if unloaded.
    pub fn pin(&mut self, id: PageId) -> PgResult<PagePin> {
        let i = id.0 as usize;
        if let SlotState::Resident { pins, .. } = &mut self.slots[i].state {
            *pins += 1;
            self.live_pins += 1;
            return Ok(PagePin { id, _not_send: PhantomData });
        }
        let kind = self.slots[i].kind;
        let frame_bytes = self.slots[i].frame_bytes;
        let ext_page = self.slots[i].ext_page.expect("unloaded page has an extent slot");
        let mut buf = self.acquire_frame(frame_bytes)?;
        let read_r = self
            .file
            .read_at_unchecked(ext_page * PAGE_SIZE as u64, &mut buf)
            .and_then(|()| validate_page(&buf, kind).map(|_| ()));
        if let Err(e) = read_r {
            // Failed reload: release the frame, stay Unloaded (no torn
            // resident state).
            self.release_frame(buf);
            return Err(e);
        }
        self.metrics.pages_reloaded += 1;
        self.metrics.bytes_read += frame_bytes as u64;
        self.slots[i].state = SlotState::Resident { buf, pins: 1, dirty: false, swizzled: false };
        self.live_pins += 1;
        Ok(PagePin { id, _not_send: PhantomData })
    }

    /// Return a pin. Unpinned resident pages become evictable.
    pub fn unpin(&mut self, pin: PagePin) {
        let SlotState::Resident { pins, .. } = &mut self.slots[pin.id.0 as usize].state else {
            unreachable!("a pinned page cannot be unloaded");
        };
        debug_assert!(*pins > 0);
        *pins -= 1;
        self.live_pins -= 1;
    }

    // ------------------------------------------------------------ unload --

    /// Explicitly unload a page (write-back if dirty or never written).
    /// Errors on pinned/swizzled pages — unload is a staging-point
    /// operation by law.
    pub fn unload(&mut self, id: PageId) -> PgResult<()> {
        let i = id.0 as usize;
        match &self.slots[i].state {
            SlotState::Unloaded => return Ok(()),
            SlotState::Resident { pins, swizzled, .. } => {
                if *pins > 0 || *swizzled {
                    return spill_err(format!(
                        "cannot unload spill page {}: pinned or swizzled (staging-point law)",
                        id.0
                    ));
                }
            }
        }
        self.write_back_and_release(i)
    }

    /// Write back (if dirty or never written) and release slot `i`'s
    /// frame. Caller guarantees resident + unpinned + unswizzled. On a
    /// write error the page STAYS resident.
    fn write_back_and_release(&mut self, i: usize) -> PgResult<()> {
        let frame_bytes = self.slots[i].frame_bytes;
        let first = self.slots[i].ext_page.is_none();
        if first {
            self.slots[i].ext_page = Some(self.next_ext_page);
            self.next_ext_page += (frame_bytes / PAGE_SIZE) as u64;
        }
        let dirty = matches!(self.slots[i].state, SlotState::Resident { dirty: true, .. });
        if dirty || first {
            let off = self.slots[i].ext_page.unwrap() * PAGE_SIZE as u64;
            let SlotState::Resident { buf, .. } = &self.slots[i].state else {
                unreachable!("caller checked residency");
            };
            self.file.write_at(off, buf)?;
            self.metrics.pages_unloaded += 1;
            self.metrics.bytes_written += frame_bytes as u64;
        }
        let SlotState::Resident { buf, .. } =
            core::mem::replace(&mut self.slots[i].state, SlotState::Unloaded)
        else {
            unreachable!("caller checked residency");
        };
        self.release_frame(buf);
        Ok(())
    }

    fn release_frame(&mut self, buf: Box<[u8]>) {
        self.resident_bytes -= buf.len();
        if buf.len() == PAGE_SIZE {
            self.free_frames.push(buf);
        }
    }

    /// Acquire a frame of `len` bytes under the budget, evicting unpinned
    /// pages as needed (the ONLY implicit unload site; everything else is
    /// staging-point-explicit).
    fn acquire_frame(&mut self, len: usize) -> PgResult<Box<[u8]>> {
        while self.resident_bytes + len > self.budget_bytes {
            if !self.evict_one()? {
                if len > self.budget_bytes {
                    // Jumbo overshoot (module docs, budget law): everything
                    // evictable is out; proceed and witness the peak.
                    break;
                }
                return spill_err(format!(
                    "spill page pool exhausted: {} resident + {} needed > {} budget, \
                     nothing evictable (all pages pinned)",
                    self.resident_bytes, len, self.budget_bytes
                ));
            }
        }
        let buf = match self.free_frames.pop() {
            Some(f) if f.len() == len => f,
            Some(f) => {
                self.free_frames.push(f);
                vec![0u8; len].into_boxed_slice()
            }
            None => vec![0u8; len].into_boxed_slice(),
        };
        self.resident_bytes += len;
        self.metrics.peak_resident_bytes =
            self.metrics.peak_resident_bytes.max(self.resident_bytes as u64);
        Ok(buf)
    }

    /// Evict one unpinned resident page (clock scan from the cursor).
    /// `Ok(false)` when nothing is evictable.
    fn evict_one(&mut self) -> PgResult<bool> {
        let n = self.slots.len();
        for step in 0..n {
            let i = (self.evict_cursor + step) % n;
            if let SlotState::Resident { pins: 0, swizzled: false, .. } = self.slots[i].state {
                self.evict_cursor = (i + 1) % n;
                self.write_back_and_release(i)?;
                self.metrics.pool_evictions += 1;
                return Ok(true);
            }
        }
        Ok(false)
    }

    // ------------------------------------------------------------- views --

    fn resident_buf(&self, pin: &PagePin) -> &[u8] {
        let SlotState::Resident { buf, .. } = &self.slots[pin.id.0 as usize].state else {
            unreachable!("a pinned page cannot be unloaded");
        };
        buf
    }

    /// Mutable buffer access marks the slot dirty (view minting = intent
    /// to write; unload/eviction then rewrites the on-disk image).
    fn resident_buf_mut(&mut self, pin: &PagePin) -> &mut [u8] {
        let SlotState::Resident { buf, dirty, .. } = &mut self.slots[pin.id.0 as usize].state
        else {
            unreachable!("a pinned page cannot be unloaded");
        };
        *dirty = true;
        buf
    }

    pub fn row_page<'a>(&'a self, pin: &PagePin) -> RowPageRef<'a> {
        debug_assert_eq!(self.slots[pin.id.0 as usize].kind, PageKind::Row);
        RowPageRef::new(self.resident_buf(pin))
    }

    pub fn row_page_mut<'a>(&'a mut self, pin: &PagePin) -> RowPageMut<'a> {
        debug_assert_eq!(self.slots[pin.id.0 as usize].kind, PageKind::Row);
        RowPageMut::new(self.resident_buf_mut(pin))
    }

    pub fn var_page<'a>(&'a self, pin: &PagePin) -> VarPageRef<'a> {
        debug_assert_eq!(self.slots[pin.id.0 as usize].kind, PageKind::Var);
        VarPageRef::new(self.resident_buf(pin))
    }

    pub fn var_page_mut<'a>(&'a mut self, pin: &PagePin) -> VarPageMut<'a> {
        debug_assert_eq!(self.slots[pin.id.0 as usize].kind, PageKind::Var);
        VarPageMut::new(self.resident_buf_mut(pin))
    }

    // ----------------------------------------------------------- swizzle --

    /// Swizzle every ref word of `row` to raw payload addresses, pinning
    /// the row page and every referenced var page (reloading as needed).
    /// Addresses are valid until [`SpillPool::unswizzle`] — the batch
    /// staging point (the state-pointer lifetime law).
    pub fn swizzle(&mut self, row: PageId) -> PgResult<SwizzleToken> {
        if matches!(
            self.slots[row.0 as usize].state,
            SlotState::Resident { swizzled: true, .. }
        ) {
            // One token per row page at a time: a second token's vars list
            // could not reverse the first token's addresses.
            return spill_err(format!("spill page {} is already swizzled", row.0));
        }
        let row_pin = self.pin(row)?;
        // Pass 1 (read-only): the referenced var pages.
        let mut var_ids: Vec<u32> = Vec::new();
        {
            let buf = self.resident_buf(&row_pin);
            let hdr = PageHdr::read(buf);
            for_each_ref_word(buf, &hdr, |word| {
                if word.is_unswizzled() && !word.is_null() {
                    let (vp, _) = word.decode();
                    if !var_ids.contains(&vp) {
                        var_ids.push(vp);
                    }
                }
                None
            });
        }
        // Pass 2: pin them (reload as needed), record frame bases. The
        // pins are absorbed into the token (live_pins stays counted).
        let mut vars: Vec<(PageId, usize)> = Vec::with_capacity(var_ids.len());
        for vp in &var_ids {
            match self.pin(PageId(*vp)) {
                Ok(pin) => {
                    let base = self.resident_buf(&pin).as_ptr() as usize;
                    core::mem::forget(pin);
                    vars.push((PageId(*vp), base));
                }
                Err(e) => {
                    // Unwind the pins taken so far; no words were rewritten.
                    for (id, _) in vars.drain(..) {
                        self.unpin(PagePin { id, _not_send: PhantomData });
                    }
                    self.unpin(row_pin);
                    return Err(e);
                }
            }
        }
        vars.sort_by_key(|&(_, base)| base);
        // Pass 3: rewrite words. Direct buffer access, not the
        // dirty-marking view: the swizzle round-trip restores at-rest
        // bytes exactly, so it does not by itself dirty the page.
        {
            let SlotState::Resident { buf, swizzled, .. } = &mut self.slots[row.0 as usize].state
            else {
                unreachable!("row page is pinned");
            };
            let hdr = PageHdr::read(buf);
            let base_of = |vp: u32| -> usize {
                vars.iter().find(|&&(id, _)| id.0 == vp).expect("pinned in pass 2").1
            };
            for_each_ref_word_mut(buf, &hdr, |word| {
                if word.is_unswizzled() && !word.is_null() {
                    let (vp, off) = word.decode();
                    // Payload address: frame base + cell offset + cell
                    // header (the VarRef swizzled-form contract).
                    Some(VarRef((base_of(vp) + off as usize + 8) as u64))
                } else {
                    None
                }
            });
            *swizzled = true;
        }
        core::mem::forget(row_pin); // the token carries the row pin
        Ok(SwizzleToken { row, vars, consumed: false, _not_send: PhantomData })
    }

    /// Reverse [`SpillPool::swizzle`] at the staging point: every raw
    /// address returns to its at-rest form, and every pin the token
    /// carried is released.
    pub fn unswizzle(&mut self, mut tok: SwizzleToken) -> PgResult<()> {
        let row = tok.row;
        {
            let vars = &tok.vars;
            let SlotState::Resident { buf, swizzled, .. } = &mut self.slots[row.0 as usize].state
            else {
                unreachable!("the token holds the row pin");
            };
            debug_assert!(*swizzled, "unswizzle of an unswizzled page");
            let hdr = PageHdr::read(buf);
            let mut bad_addr: Option<usize> = None;
            for_each_ref_word_mut(buf, &hdr, |word| {
                if !word.is_unswizzled() && bad_addr.is_none() {
                    let addr = word.0 as usize;
                    let Some(vi) = owner_of(vars, addr) else {
                        bad_addr = Some(addr);
                        return None;
                    };
                    let (vp, base) = vars[vi];
                    Some(VarRef::encode(vp.0, (addr - base - 8) as u32))
                } else {
                    None
                }
            });
            if let Some(addr) = bad_addr {
                return spill_err(format!(
                    "unswizzle: address {addr:#x} is below every pinned var page"
                ));
            }
            *swizzled = false;
        }
        // Return the pins the token carried.
        for (vp, _) in tok.vars.drain(..) {
            self.unpin(PagePin { id: vp, _not_send: PhantomData });
        }
        self.unpin(PagePin { id: row, _not_send: PhantomData });
        tok.consumed = true;
        Ok(())
    }
}

/// The var page owning a swizzled payload address: the greatest frame
/// base strictly below it. A payload never sits at its own page's base
/// (cells start past the header), so an exact base match names the NEXT
/// page's frame, not the owner.
pub(crate) fn owner_of(vars: &[(PageId, usize)], addr: usize) -> Option<usize> {
    match vars.binary_search_by(|&(_, b)| b.cmp(&addr)) {
        Ok(0) | Err(0) => None,
        Ok(i) | Err(i) => Some(i - 1),
    }
}

/// Walk every ref word of a row page (read-only).
fn for_each_ref_word(buf: &[u8], hdr: &PageHdr, mut f: impl FnMut(VarRef) -> Option<VarRef>) {
    for i in 0..hdr.count {
        for slot in 0..hdr.nrefs as usize {
            let at = HDR_LEN + i as usize * hdr.row_size as usize + hdr.ref_offs[slot] as usize;
            let w = VarRef(u64::from_ne_bytes(buf[at..at + 8].try_into().unwrap()));
            let r = f(w);
            debug_assert!(r.is_none(), "read-only walk returned a rewrite");
        }
    }
}

/// Walk every ref word of a row page, rewriting where the callback returns
/// a replacement.
fn for_each_ref_word_mut(
    buf: &mut [u8],
    hdr: &PageHdr,
    mut f: impl FnMut(VarRef) -> Option<VarRef>,
) {
    for i in 0..hdr.count {
        for slot in 0..hdr.nrefs as usize {
            let at = HDR_LEN + i as usize * hdr.row_size as usize + hdr.ref_offs[slot] as usize;
            let w = VarRef(u64::from_ne_bytes(buf[at..at + 8].try_into().unwrap()));
            if let Some(nw) = f(w) {
                buf[at..at + 8].copy_from_slice(&nw.0.to_ne_bytes());
            }
        }
    }
}

impl Drop for SpillPool {
    /// Teardown (crate contract): frames are freed here regardless of
    /// path; on-disk bytes are the [`crate::SpillSet`]'s to delete. Debug
    /// builds assert the byref discipline held — zero live pins, zero
    /// swizzled pages — except during unwind, where stranded pins are the
    /// EXPECTED shape (the pool dies with its Local; addresses die with
    /// the frames).
    fn drop(&mut self) {
        if !std::thread::panicking() {
            debug_assert_eq!(self.live_pins, 0, "SpillPool dropped with live pins");
            debug_assert!(
                !self
                    .slots
                    .iter()
                    .any(|s| matches!(s.state, SlotState::Resident { swizzled: true, .. })),
                "SpillPool dropped with swizzled pages"
            );
        }
    }
}
