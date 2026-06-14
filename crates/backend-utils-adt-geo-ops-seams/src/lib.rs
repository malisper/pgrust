//! Seam declarations for the `backend-utils-adt-geo-ops` unit
//! (`utils/adt/geo_ops.c`, plus the fuzzy float comparators and `pg_hypot`
//! from `utils/geo_decls.h`): the geometric operator support functions used by
//! the `box`/`polygon` opclasses.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly (mirror-PG-and-panic for an unported callee).

#![allow(non_snake_case)]

use types_core::geo::BOX;
use types_error::PgResult;

// ---------------------------------------------------------------------------
// Fuzzy float comparators (geo_decls.h FPlt/FPle/FPgt/FPge).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `FPlt(A, B)` (geo_decls.h): fuzzy `A < B`.
    pub fn FPlt(a: f64, b: f64) -> bool
);
seam_core::seam!(
    /// `FPle(A, B)` (geo_decls.h): fuzzy `A <= B`.
    pub fn FPle(a: f64, b: f64) -> bool
);
seam_core::seam!(
    /// `FPgt(A, B)` (geo_decls.h): fuzzy `A > B`.
    pub fn FPgt(a: f64, b: f64) -> bool
);
seam_core::seam!(
    /// `FPge(A, B)` (geo_decls.h): fuzzy `A >= B`.
    pub fn FPge(a: f64, b: f64) -> bool
);

seam_core::seam!(
    /// `HYPOT(A, B)` = `pg_hypot(A, B)` (geo_ops.c): `sqrt(A*A + B*B)` with
    /// over/underflow handling; `ereport(ERROR)`s on overflow, hence
    /// `PgResult`.
    pub fn HYPOT(a: f64, b: f64) -> PgResult<f64>
);

// ---------------------------------------------------------------------------
// box <-> box boolean operators (geo_ops.c).  All are `Datum NAME(box, box)`
// fmgr functions returning `bool`; none ereport, so a plain `bool` return.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `box_overlap(box, box)` (geo_ops.c).
    pub fn box_overlap(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_contain(box, box)` (geo_ops.c): does the first box contain the second?
    pub fn box_contain(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_contained(box, box)` (geo_ops.c): is the first box contained by the second?
    pub fn box_contained(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_same(box, box)` (geo_ops.c).
    pub fn box_same(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_left(box, box)` (geo_ops.c).
    pub fn box_left(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_overleft(box, box)` (geo_ops.c).
    pub fn box_overleft(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_right(box, box)` (geo_ops.c).
    pub fn box_right(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_overright(box, box)` (geo_ops.c).
    pub fn box_overright(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_above(box, box)` (geo_ops.c).
    pub fn box_above(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_overabove(box, box)` (geo_ops.c).
    pub fn box_overabove(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_below(box, box)` (geo_ops.c).
    pub fn box_below(a: &BOX, b: &BOX) -> bool
);
seam_core::seam!(
    /// `box_overbelow(box, box)` (geo_ops.c).
    pub fn box_overbelow(a: &BOX, b: &BOX) -> bool
);
