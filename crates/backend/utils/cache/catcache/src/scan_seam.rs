//! Miss-path catalog-scan seam (installed by genam when it lands).

use types_error::PgResult;
use types_rel::RelationData;
use types_scan::scankey::ScanKeyData;
use types_tuple::HeapTupleData;

seam_core::seam!(
    // systable_beginscan + the systable_getnext loop + systable_endscan,
    // inverted: `consume` runs per matching tuple; false stops the scan.
    // Caller holds the relation open+locked. Returns `ordered` (irel !=
    // NULL). By-ref sk_argument Datums point at caller-framed key images
    // that outlive the call.
    pub fn systable_scan_catalog(
        relation: &RelationData<'_>,
        index_oid: types_core::Oid,
        index_ok: bool,
        keys: &[ScanKeyData],
        consume: &mut dyn FnMut(&HeapTupleData<'_>) -> PgResult<bool>,
    ) -> PgResult<bool>
);

/// Genam's one-shot installer.
pub fn install_systable_scan(f: systable_scan_catalog::Signature) {
    systable_scan_catalog::set(f);
}
