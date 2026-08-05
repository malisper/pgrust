//! Seams for the contrib/btree_gin scalar opclasses (btree_gin.c),
//! installed by the btree_gin crate.

use datum::Datum;
use gin_vocab::GinBtreeType;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// Opclass FUNCTION 1 comparator with btree semantics; numeric/enum
    /// accept their leftmost sentinels (null pointer / InvalidOid).
    pub fn btree_compare(ty: GinBtreeType, a: Datum, b: Datum, collation: Oid) -> PgResult<i32>
);

seam_core::seam!(
    /// gin_btree_extract_value core: the single entry datum (varlena types
    /// detoasted into mcx).
    pub fn btree_extract_value<'m>(
        mcx: mcx::Mcx<'m>,
        ty: GinBtreeType,
        value: Datum,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// gin_btree_extract_query core: (entry, partial_match, orig). For the
    /// < / <= strategies the entry is the type's leftmost value and the scan
    /// walks forward from it (C's QueryInfo trick); orig is the (detoasted)
    /// query datum comparePartial compares against.
    pub fn btree_extract_query<'m>(
        mcx: mcx::Mcx<'m>,
        ty: GinBtreeType,
        query: Datum,
        strategy: u16,
    ) -> PgResult<(Datum, bool, Datum)>
);

seam_core::seam!(
    /// gin_btree_compare_prefix core: 0 match, 1 stop scan, -1 continue
    /// without match.
    pub fn btree_compare_prefix(
        ty: GinBtreeType,
        orig: Datum,
        key: Datum,
        strategy: u16,
        collation: Oid,
    ) -> PgResult<i32>
);
