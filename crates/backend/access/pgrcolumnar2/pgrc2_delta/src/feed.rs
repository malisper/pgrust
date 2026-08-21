//! The delta scan-feed contract: how visible delta-relation rows reach the
//! lx scan-merge (the B side of `lx_source::hetero::PairSource`).
//!
//! Dependency direction (lx crates depend on pgrc2 crates, never the
//! reverse), so this contract speaks FORMAT currency — datum words and raw
//! byte images — and the lx side (`lx_source::pgrc::DeltaWindowSource`)
//! adapts it into staged batches.
//!
//! ## The detoast law (identical-StageSchema prerequisite)
//!
//! A feed implementor hands values in the SAME canonical currency sealed
//! decode produces (spec §18.1 + §19.4):
//!
//! - byval classes (ints, floats, bool): the DATUM WORD exactly as decode
//!   would emit it — sign-extended iff the class is signed, zero-extended
//!   otherwise, float bits in the low word, bool 0/1;
//! - `Fixed(N)`: the N raw bytes;
//! - varlena: the **detoasted, decompressed PAYLOAD bytes** (no header —
//!   the lx side writes the 4-byte header + payload into its ≥8-aligned
//!   staging arena, making the staged cell byte-identical to a sealed
//!   decode output).
//!
//! At product grain (M5-M) the feed detoasts at fetch; the crate-grain
//! [`crate::testkit::SimHeap`] stores canonical currency directly. Either
//! way the scan-merge differential (sealed+delta ≡ fully sealed) is the
//! enforcement.
//!
//! ## Visibility
//!
//! A feed is bound to ONE snapshot at construction and serves exactly the
//! delta rows visible under it — visibility is the heap side's (ordinary
//! heap MVCC; C-exact by construction at product grain). The feed never
//! re-decides visibility per fetch: one snapshot, one row set, stable for
//! the scan's life.
//!
//! ## Geometry
//!
//! Windows are heap BLOCK ranges (the granule-map basis the lx side builds
//! claims over — the heap source's own claim geometry). `fetch_window`
//! returns the visible rows of a block range in (block, offnum) order;
//! rows per window vary (heap pages are not row-count-uniform).

use crate::DeltaResult;

/// One value cell in feed currency (see the module doc's detoast law).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeltaCell {
    Null,
    /// Byval datum word (canonical: extension/float-bits rules applied).
    Word(u64),
    /// Fixed(N) raw bytes, or varlena detoasted PAYLOAD bytes (no header).
    Bytes(Vec<u8>),
}

/// One fetched window of visible delta rows (all storage columns, row-
/// parallel). `tids` carry the delta-relation addresses the lx side packs
/// into delta rowids (`crate::rowid::pack_delta_rowid`).
#[derive(Clone, Debug, Default)]
pub struct DeltaRowsWindow {
    pub nrows: u32,
    /// (block, offnum) per row, ascending.
    pub tids: Vec<(u32, u16)>,
    /// Per storage column (outer), per row (inner): `cols[c].len() ==
    /// nrows` for every c.
    pub cols: Vec<Vec<DeltaCell>>,
}

impl DeltaRowsWindow {
    pub fn clear(&mut self) {
        self.nrows = 0;
        self.tids.clear();
        self.cols.clear();
    }
}

/// The scan-feed face (implemented by the heap side; consumed by the lx
/// delta window source). One feed instance per worker per scan (worker-
/// private, R2) over one bound snapshot.
pub trait DeltaFeed {
    /// Storage-column count (must equal the pair schema's data columns).
    fn ncols(&self) -> usize;

    /// Total heap blocks under this feed's snapshot-stable view — the
    /// granule-map total the lx side claims over. Stable for the feed's
    /// life (the snapshot bounds the row set; later appends are invisible
    /// and MAY live beyond this bound — they are simply never fetched).
    fn nblocks(&self) -> u64;

    /// Fetch the visible rows of `blocks` (half-open block range) into
    /// `out` (cleared first). Rows arrive in (block, offnum) order with
    /// every storage column populated (the feed serves whole rows; the lx
    /// side stages eagerly or completes deferred columns from the held
    /// window — both read the same fetch).
    fn fetch_window(
        &mut self,
        blocks: core::ops::Range<u64>,
        out: &mut DeltaRowsWindow,
    ) -> DeltaResult<()>;
}
