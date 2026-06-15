//! System-table scan vocabulary (`access/genam.h`).

use types_snapshot::SnapshotData;

/// `SysScanDescData` (`access/genam.h`).
///
/// C spells the struct out: `heap_rel`, `irel`, the live `TableScanDescData`
/// / `IndexScanDescData` pointers, `snapshot`, and the result `slot`. Those
/// live scan-state pointers are `'mcx`-bearing values (`Relation<'mcx>`,
/// `IndexScanDesc<'mcx>`, `SlotData<'mcx>`) that the genam owner
/// (`access/index/genam.c`) allocates — but the seam contract carries this
/// descriptor with **no lifetime parameter** (consumers hold it on the stack
/// across many `systable_getnext` calls, each with its own per-row `mcx`, and
/// hand back `&mut SysScanDescData`). So the descriptor cannot name an `'mcx`.
///
/// To bridge that, the descriptor OWNS the memory context the genam owner
/// allocated the live scan state in (`scan_cx`, a stable heap address) and
/// holds the live state behind a lifetime-erased boxed trait object
/// ([`SysScanLive`]). The genam owner installs a concrete `SysScanLive` whose
/// real lifetime is tied to `scan_cx`; the erasure is sound because `scan_cx`
/// is dropped *after* the live state (drop order is field-declaration order:
/// `live` before `scan_cx`), so the borrows the live state holds into
/// `scan_cx` never dangle. This is the same raw-`'mcx`-erasure the AM-private
/// `void *opaque` carrier uses, not an introduced registry — the real owned
/// scan state rides through unchanged, only its lifetime is hidden across this
/// one type edge.
///
/// Consumers never construct one — they receive it from `systable_beginscan*`
/// (wrapped in the seam crate's scan guard) and hand it back to
/// `systable_getnext*` / `systable_endscan*`.
pub struct SysScanDescData {
    /// `snapshot` — the snapshot to unregister at end of scan, or `None`
    /// (C's NULL: the caller's snapshot, nothing to unregister). Kept here
    /// (lifetime-free) so `systable_endscan` can `UnregisterSnapshot` it even
    /// though the live state is erased.
    pub snapshot: Option<SnapshotData>,
    /// The lifetime-erased live scan state the genam owner installed (the
    /// `heap_rel` / `irel` / `iscan` / `scan` / `slot` of C's
    /// `SysScanDescData`). `None` only transiently while the owner is moving
    /// the state out at end of scan.
    live: Option<Box<dyn SysScanLive>>,
    /// The memory context the live scan state was allocated in. Declared
    /// **after** `live` so Rust drops `live` first, then `scan_cx`: the erased
    /// borrows the live state holds into this context are released before the
    /// context's backing storage is freed.
    #[allow(dead_code)]
    scan_cx: Box<mcx::MemoryContext>,
}

/// The genam owner's live scan state, type-erased so it can ride in the
/// lifetime-free [`SysScanDescData`]. The owner's concrete struct (carrying
/// `Relation<'mcx>` / `IndexScanDesc<'mcx>` / `SlotData<'mcx>`) implements
/// this; the trait itself exposes nothing for callers (the owner downcasts
/// back to its concrete type), it merely makes the box object-safe and gives
/// the owner a stable place to hang `Drop` glue for the `'mcx` state.
pub trait SysScanLive {
    /// A human-readable name of the concrete live-state type, for
    /// panic/debug messages only.
    fn live_type_name(&self) -> &'static str;
}

impl core::fmt::Debug for SysScanDescData {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SysScanDescData")
            .field("snapshot", &self.snapshot.is_some())
            .field("live", &self.live.as_ref().map(|l| l.live_type_name()))
            .finish()
    }
}

impl SysScanDescData {
    /// Construct a descriptor from the genam owner's live scan state.
    ///
    /// `scan_cx` is the boxed context the live state was allocated in;
    /// `live` is the owner's concrete `SysScanLive` whose real lifetime is
    /// tied to `*scan_cx`. The caller supplies it with that real lifetime; we
    /// erase it to `'static` here. SAFETY: `*scan_cx` is moved into the
    /// returned value and dropped strictly after `live` (see the field-order
    /// note above), so the erased borrows never outlive their backing context.
    ///
    /// Called only by the genam owner's installed `systable_beginscan*`.
    pub fn new<'mcx>(
        scan_cx: Box<mcx::MemoryContext>,
        live: Box<dyn SysScanLive + 'mcx>,
        snapshot: Option<SnapshotData>,
    ) -> Self {
        // Erase the `'mcx` lifetime of the boxed trait object to `'static`.
        // Sound by the drop-order invariant documented on `scan_cx`.
        let live: Box<dyn SysScanLive + 'static> = unsafe {
            core::mem::transmute::<Box<dyn SysScanLive + 'mcx>, Box<dyn SysScanLive + 'static>>(live)
        };
        SysScanDescData {
            snapshot,
            live: Some(live),
            scan_cx,
        }
    }

    /// Borrow the live scan state mutably, re-fabricating an `'a` lifetime.
    ///
    /// SAFETY: the real backing context (`scan_cx`) outlives `&'a mut self`,
    /// so the `'a`-bounded borrow into it is valid. The returned reference is
    /// tied to `'a` and so cannot be stored beyond the call.
    ///
    /// Called only by the genam owner's installed `systable_getnext*` /
    /// `systable_recheck_tuple`.
    pub fn live_mut<'a>(&'a mut self) -> &'a mut (dyn SysScanLive + 'a) {
        let l = self
            .live
            .as_deref_mut()
            .expect("SysScanDescData live state already taken");
        // Shorten the erased `'static` back to the borrow `'a`.
        unsafe {
            core::mem::transmute::<&'a mut (dyn SysScanLive + 'static), &'a mut (dyn SysScanLive + 'a)>(
                l,
            )
        }
    }

    /// Take the live scan state out, re-fabricating an `'a` lifetime, so the
    /// genam owner can run `index_endscan` / `table_endscan` on the owned
    /// `'mcx` values. SAFETY as for [`live_mut`](Self::live_mut); the context
    /// `scan_cx` is still owned by `self` (dropped after the taken state when
    /// the whole descriptor is dropped), so the borrows remain valid for the
    /// duration of the owner's teardown within `'a`.
    ///
    /// Called only by the genam owner's installed `systable_endscan*`.
    pub fn take_live<'a>(&'a mut self) -> Box<dyn SysScanLive + 'a> {
        let l = self
            .live
            .take()
            .expect("SysScanDescData live state already taken");
        unsafe {
            core::mem::transmute::<Box<dyn SysScanLive + 'static>, Box<dyn SysScanLive + 'a>>(l)
        }
    }
}
