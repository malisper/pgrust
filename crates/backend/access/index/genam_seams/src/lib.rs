use datum::Datum;
use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_rel::RelationData;
use types_scan::scankey::ScanKeyData;
use types_tuple::HeapTupleData;

seam_core::seam!(
    // pg_get_indexdef_columns_extended(KEYS_ONLY) (ruleutils.c), for
    // BuildIndexValueDescription's key-column list (expression columns
    // deparse; ruleutils sits above genam).
    pub fn pg_get_indexdef_columns_keys_only(
        mcx: Mcx<'_>,
        indexrelid: Oid,
    ) -> PgResult<Option<String>>
);

seam_core::seam!(
    // BuildIndexValueDescription (genam.c) for callers below genam in the
    // crate graph (nbtree unique violations). None mirrors C's NULL: the
    // ACL/RLS gate hid the key, the caller omits the DETAIL line.
    pub fn build_index_value_description(
        index_relation: &RelationData<'_>,
        values: &[Datum],
        isnull: &[bool],
    ) -> PgResult<Option<String>>
);

seam_core::seam!(
    // systable_beginscan(relation, index_oid, index_ok, NULL /*snapshot*/,
    // nkeys, keys) + the systable_getnext loop + systable_endscan (genam.c),
    // inverted: `consume` runs once per matching tuple and returns false to
    // stop the scan (the catcache single-tuple "break after first match").
    // The caller holds the relation open and locked. Returns `ordered`
    // (sysscan->irel != NULL: results arrive in index order). A by-reference
    // sk_argument Datum points at caller-framed key bytes (NameData buffer /
    // 4-byte-header varlena / oidvector image) that outlive the call.
    pub fn systable_scan_catalog(
        relation: &RelationData<'_>,
        index_oid: types_core::Oid,
        index_ok: bool,
        keys: &[ScanKeyData],
        consume: &mut dyn FnMut(&HeapTupleData<'_>) -> PgResult<bool>,
    ) -> PgResult<bool>
);
