//! Seam declarations for the `backend-access-spg-proc` unit
//! (`access/spgist/spgproc.c`): the common SP-GiST supporting procedures.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly (mirror-PG-and-panic for an unported callee).

use ::types_core::geo::{Point, SpgKey};
use ::types_error::PgResult;

seam_core::seam!(
    /// `spg_key_orderbys_distances(Datum key, bool isLeaf, ScanKey orderbys, int norderbys)`
    /// (spgproc.c): the distances from `key` to each ordering-scan key.
    ///
    /// The C `Datum key` + `bool isLeaf` pair is carried by [`SpgKey`]
    /// (`InnerBox` for `isLeaf == false`, `LeafPoint` for `true`); the
    /// `ScanKey orderbys` array's `sk_argument`s are pre-decoded to `Point`s.
    /// `point_box_distance` calls `HYPOT`, which can `ereport(ERROR)`, hence
    /// `PgResult`.
    pub fn spg_key_orderbys_distances(key: &SpgKey, orderby_points: &[Point]) -> PgResult<Vec<f64>>
);
