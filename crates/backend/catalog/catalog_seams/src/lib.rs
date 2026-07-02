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
