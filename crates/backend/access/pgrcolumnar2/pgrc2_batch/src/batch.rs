//! The batch and column containers (C1 base column = Datum words +
//! validity; overlays ride the [`ColRep`] tag with fail-open gather-back).

use ::datum::Datum;

use crate::rep::ColRep;
use crate::selection::Selection;
use crate::validity::Validity;

/// One base column: (Datum[], validity bitmask) plus its representation
/// tag. `base` is the catalog-derived base tag ([`ColRep::is_base`]) the
/// column degrades to; `rep` is the current (possibly overlay)
/// representation.
///
/// Ownership: datum cells may alias externally-owned images — pinned heap
/// pages (R3/R3v multi-pin), decode scratch (R4), dictionary storage, or
/// the claim arena. All of it is claim-scoped and worker-private (R1/R2);
/// consumers needing batch-outliving bytes copy at the consumer.
#[derive(Debug)]
pub struct Column {
    /// Current representation of the datum cells.
    pub rep: ColRep,
    /// The base tag gather-back degrades to (frozen per column at plan
    /// time from catalog properties — [`crate::StagingClass::base_rep`]).
    pub base: ColRep,
    pub datums: Vec<Datum>,
    pub validity: Validity,
    /// The contiguity witness (M5d.cells.q20-span): present iff the
    /// reader certified this window's varlena images as one dense span
    /// with ascending datum pointers ([`crate::TextSpan`]). Advisory —
    /// consumers that ignore it are unaffected; absence demotes the
    /// fused span kernel to the per-value path (typed, never a refusal).
    /// Rebuilt with the column per window; never survives a claim.
    pub text_span: Option<crate::TextSpan>,
}

impl Column {
    /// A column whose base representation is `base` (must satisfy
    /// [`ColRep::is_base`]).
    pub fn new(base: ColRep) -> Column {
        debug_assert!(base.is_base(), "Column::new wants a base tag, got {base:?}");
        Column { rep: base, base, datums: Vec::new(), validity: Validity::new(), text_span: None }
    }

    /// Size the datum/validity stores for `nrows` staged rows (newly grown
    /// datum cells are null words; validity starts all-NULL).
    pub fn ensure_rows(&mut self, nrows: usize) {
        if self.datums.len() < nrows {
            self.datums.resize(nrows, Datum::null());
        }
        if self.validity.words.len() < nrows.div_ceil(64) {
            self.validity.words.resize(nrows.div_ceil(64), 0);
        }
    }

    /// Stage this column as a Const overlay: every logical row carries
    /// (`value`, not-`is_null`). `datums[0]`/validity bit 0 hold the value
    /// per the [`ColRep::Const`] convention.
    pub fn set_const(&mut self, value: Datum, is_null: bool, nrows: usize) {
        self.ensure_rows(nrows.max(1));
        self.datums[0] = value;
        if is_null {
            self.validity.reset_all_null(nrows.max(1));
        } else {
            self.validity.reset_all_valid(nrows.max(1));
        }
        self.rep = ColRep::Const;
    }

    /// Fail-open degrade: materialize the base datum column from any
    /// overlay rep, **losslessly**, over rows `0..nrows`; `rep` becomes
    /// `base` (C1: every overlay rep degrades to the base datum column).
    /// Base reps are a no-op.
    ///
    /// Safety of the overlay dereferences rests on the overlay publish
    /// contracts ([`crate::DictCodes::publish`]): gather-back must run
    /// inside the claim scope that staged the overlay (R1).
    pub fn gather_back(&mut self, nrows: usize) {
        match self.rep {
            ColRep::DictCodes(dc) => {
                self.ensure_rows(nrows);
                // SAFETY: the publish contract (claim-scoped codes + live
                // dict provider; all-valid column per the zero-null-proof
                // rule) — asserted by the producer at publish time.
                let codes = unsafe { dc.codes(nrows) };
                let space = unsafe { dc.dict().space() };
                debug_assert!(
                    space.base_rep().same_base_class(self.base),
                    "dict base_rep {:?} not the column's base class {:?}",
                    space.base_rep(),
                    self.base
                );
                for row in 0..nrows {
                    if self.validity.is_valid(row) {
                        self.datums[row] = space.entry_datum(codes[row]);
                    } else {
                        self.datums[row] = Datum::null();
                    }
                }
                // Dict entries define what the cells now are (C6 stores
                // detoasted plain images, so this may carry the inline
                // proof the raw base lacks).
                self.rep = space.base_rep();
            }
            ColRep::StrView(_) => {
                // The StrView overlay law (strview.rs module docs): the
                // datum lane stayed the authoritative varlena lane at
                // publish — every producer fills/keeps plain varlena
                // pointers for the selected valid rows. Degrade is a rep
                // flip; the cells die with the claim.
                debug_assert!(
                    matches!(self.base, ColRep::Varlena { .. }),
                    "StrView overlay on a non-varlena base {:?}",
                    self.base
                );
                self.rep = ColRep::Varlena { inline_proven: true };
            }
            ColRep::Const => {
                self.ensure_rows(nrows);
                let value = self.datums[0];
                let valid = self.validity.is_valid(0);
                for cell in &mut self.datums[..nrows] {
                    *cell = value;
                }
                if valid {
                    self.validity.reset_all_valid(nrows);
                } else {
                    self.validity.reset_all_null(nrows);
                }
                self.rep = self.base;
            }
            _ => debug_assert!(self.rep.is_base()),
        }
    }
}

/// One execution batch: base columns + the surviving-position selection.
///
/// Geometry: at most [`crate::LX_BATCH_ROWS`] staged rows (S1-RULED 1024;
/// heap staging above one page = multi-pin claims per the S1 verdict — the
/// batch aliases 4–5 pinned page images, released at batch end).
/// R1–R6 ownership per the crate docs; batches are worker-private and
/// deliberately `!Send` once overlays/pointer cells are staged.
#[derive(Debug, Default)]
pub struct Batch {
    pub cols: Vec<Column>,
    /// Physical rows staged (≤ [`crate::LX_BATCH_ROWS`]).
    pub nrows: u32,
    /// Surviving positions; kernels iterate the selection, never
    /// `0..nrows`.
    pub sel: Selection,
}

impl Batch {
    pub fn new() -> Batch {
        Batch { cols: Vec::new(), nrows: 0, sel: Selection::new() }
    }

    /// Begin a refill: clear the selection and row count and drop every
    /// overlay rep back to the column's base tag. This is the R4
    /// stale-pointer discipline — overlay payloads (dict codes ptrs, const
    /// images) alias claim-scoped producer state and must never survive a
    /// re-stage (v2 `SoaBatch::begin` precedent; the decode-arena reuse
    /// trap). Datum cells and validity are left for the source to
    /// overwrite.
    pub fn begin(&mut self) {
        self.nrows = 0;
        self.sel.clear();
        for col in &mut self.cols {
            col.rep = col.base;
            // R4: the contiguity witness is claim-scoped advisory state —
            // it dies with the window like every overlay.
            col.text_span = None;
        }
    }

    /// Reset the selection to identity (`0..nrows`).
    pub fn sel_all(&mut self) {
        self.sel.fill_identity(self.nrows);
    }
}
