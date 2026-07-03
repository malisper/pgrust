use types_core::Oid;
use types_rel::RelationData;

seam_core::seam!(
    pub fn is_catalog_relation(relation: &RelationData<'_>) -> bool
);

seam_core::seam!(
    pub fn is_toast_relation(relation: &RelationData<'_>) -> bool
);

seam_core::seam!(
    pub fn is_shared_relation(relid: Oid) -> bool
);

seam_core::seam!(
    pub fn is_catalog_relation_oid(relid: Oid) -> bool
);

seam_core::seam!(
    // GetNewOidWithIndex (catalog.c) -- the catalog crate's OID-generation
    // half is unported; uninstalled call = loud panic.
    pub fn get_new_oid_with_index<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation: &RelationData<'mcx>,
        index_id: Oid,
        oidcolumn: i16,
    ) -> types_error::PgResult<Oid>
);
