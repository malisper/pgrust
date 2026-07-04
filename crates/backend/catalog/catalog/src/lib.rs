// catalog.c: classification predicates + OID generation (pg_nextoid and
// pg_stop_making_pinned_objects stay with the fmgr-callable surface).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod oid;
pub use oid::{Anum_pg_class_oid, ClassOidIndexId, GetNewOidWithIndex, GetNewRelFileNumber};

use types_core::{
    FirstUnpinnedObjectId, Oid, DATABASE_RELATION_ID, PG_CATALOG_NAMESPACE, PG_TOAST_NAMESPACE,
    RELATION_RELATION_ID, TABLE_SPACE_RELATION_ID,
};
use types_rel::{FormData_pg_class, RelationData};

#[cfg(test)]
mod tests;

// Verified against REL_18_3 generated catalog headers (pg_*_d.h).
pub const AuthIdRelationId: Oid = 1260;
pub const AuthMemRelationId: Oid = 1261;
pub const DbRoleSettingRelationId: Oid = 2964;
pub const ParameterAclRelationId: Oid = 6243;
pub const ReplicationOriginRelationId: Oid = 6000;
pub const SharedDependRelationId: Oid = 1214;
pub const SharedDescriptionRelationId: Oid = 2396;
pub const SharedSecLabelRelationId: Oid = 3592;
pub const SubscriptionRelationId: Oid = 6100;
pub const AuthIdOidIndexId: Oid = 2677;
pub const AuthIdRolnameIndexId: Oid = 2676;
pub const AuthMemMemRoleIndexId: Oid = 2695;
pub const AuthMemRoleMemIndexId: Oid = 2694;
pub const AuthMemOidIndexId: Oid = 6303;
pub const AuthMemGrantorIndexId: Oid = 6302;
pub const DatabaseNameIndexId: Oid = 2671;
pub const DatabaseOidIndexId: Oid = 2672;
pub const DbRoleSettingDatidRolidIndexId: Oid = 2965;
pub const ParameterAclOidIndexId: Oid = 6247;
pub const ParameterAclParnameIndexId: Oid = 6246;
pub const ReplicationOriginIdentIndex: Oid = 6001;
pub const ReplicationOriginNameIndex: Oid = 6002;
pub const SharedDependDependerIndexId: Oid = 1232;
pub const SharedDependReferenceIndexId: Oid = 1233;
pub const SharedDescriptionObjIndexId: Oid = 2397;
pub const SharedSecLabelObjectIndexId: Oid = 3593;
pub const SubscriptionNameIndexId: Oid = 6115;
pub const SubscriptionObjectIndexId: Oid = 6114;
pub const TablespaceNameIndexId: Oid = 2698;
pub const TablespaceOidIndexId: Oid = 2697;
pub const PgDatabaseToastTable: Oid = 4177;
pub const PgDatabaseToastIndex: Oid = 4178;
pub const PgDbRoleSettingToastTable: Oid = 2966;
pub const PgDbRoleSettingToastIndex: Oid = 2967;
pub const PgParameterAclToastTable: Oid = 6244;
pub const PgParameterAclToastIndex: Oid = 6245;
pub const PgShdescriptionToastTable: Oid = 2846;
pub const PgShdescriptionToastIndex: Oid = 2847;
pub const PgShseclabelToastTable: Oid = 4060;
pub const PgShseclabelToastIndex: Oid = 4061;
pub const PgSubscriptionToastTable: Oid = 4183;
pub const PgSubscriptionToastIndex: Oid = 4184;
pub const PgTablespaceToastTable: Oid = 4185;
pub const PgTablespaceToastIndex: Oid = 4186;
pub const SecLabelRelationId: Oid = 3596;
pub const SecLabelObjectIndexId: Oid = 3597;
pub const LargeObjectRelationId: Oid = 2613;
pub const NamespaceRelationId: Oid = 2615;
pub const AccessMethodRelationId: Oid = 2601;
pub const CollationRelationId: Oid = 3456;
pub const OperatorClassRelationId: Oid = 2616;
pub const PG_PUBLIC_NAMESPACE: Oid = 2200;

pub fn IsSystemRelation(relation: &RelationData<'_>) -> bool {
    IsSystemClass(relation.rd_id, &relation.rd_rel)
}

pub fn IsSystemClass(relid: Oid, reltuple: &FormData_pg_class) -> bool {
    // IsCatalogRelationOid is a bit faster, so test that first.
    IsCatalogRelationOid(relid) || IsToastClass(reltuple)
}

pub fn IsCatalogRelation(relation: &RelationData<'_>) -> bool {
    IsCatalogRelationOid(relation.rd_id)
}

pub fn IsCatalogRelationOid(relid: Oid) -> bool {
    relid < FirstUnpinnedObjectId
}

pub fn IsCatalogTextUniqueIndexOid(relid: Oid) -> bool {
    matches!(
        relid,
        ParameterAclParnameIndexId
            | ReplicationOriginNameIndex
            | SecLabelObjectIndexId
            | SharedSecLabelObjectIndexId
    )
}

pub fn IsInplaceUpdateRelation(relation: &RelationData<'_>) -> bool {
    IsInplaceUpdateOid(relation.rd_id)
}

pub fn IsInplaceUpdateOid(relid: Oid) -> bool {
    relid == RELATION_RELATION_ID || relid == DATABASE_RELATION_ID
}

pub fn IsToastRelation(relation: &RelationData<'_>) -> bool {
    IsToastNamespace(relation.rd_rel.relnamespace)
}

pub fn IsToastClass(reltuple: &FormData_pg_class) -> bool {
    IsToastNamespace(reltuple.relnamespace)
}

pub fn IsCatalogNamespace(namespaceId: Oid) -> bool {
    namespaceId == PG_CATALOG_NAMESPACE
}

pub fn IsToastNamespace(namespaceId: Oid) -> bool {
    namespaceId == PG_TOAST_NAMESPACE
        || namespace_seams::is_temp_toast_namespace::call(namespaceId)
}

pub fn IsReservedName(name: &str) -> bool {
    let b = name.as_bytes();
    b.len() >= 3 && b[0] == b'p' && b[1] == b'g' && b[2] == b'_'
}

pub fn IsSharedRelation(relationId: Oid) -> bool {
    // The shared catalogs (look for BKI_SHARED_RELATION).
    if matches!(
        relationId,
        AuthIdRelationId
            | AuthMemRelationId
            | DATABASE_RELATION_ID
            | DbRoleSettingRelationId
            | ParameterAclRelationId
            | ReplicationOriginRelationId
            | SharedDependRelationId
            | SharedDescriptionRelationId
            | SharedSecLabelRelationId
            | SubscriptionRelationId
            | TABLE_SPACE_RELATION_ID
    ) {
        return true;
    }
    // Their indexes.
    if matches!(
        relationId,
        AuthIdOidIndexId
            | AuthIdRolnameIndexId
            | AuthMemMemRoleIndexId
            | AuthMemRoleMemIndexId
            | AuthMemOidIndexId
            | AuthMemGrantorIndexId
            | DatabaseNameIndexId
            | DatabaseOidIndexId
            | DbRoleSettingDatidRolidIndexId
            | ParameterAclOidIndexId
            | ParameterAclParnameIndexId
            | ReplicationOriginIdentIndex
            | ReplicationOriginNameIndex
            | SharedDependDependerIndexId
            | SharedDependReferenceIndexId
            | SharedDescriptionObjIndexId
            | SharedSecLabelObjectIndexId
            | SubscriptionNameIndexId
            | SubscriptionObjectIndexId
            | TablespaceNameIndexId
            | TablespaceOidIndexId
    ) {
        return true;
    }
    // Their toast tables and toast indexes.
    matches!(
        relationId,
        PgDatabaseToastTable
            | PgDatabaseToastIndex
            | PgDbRoleSettingToastTable
            | PgDbRoleSettingToastIndex
            | PgParameterAclToastTable
            | PgParameterAclToastIndex
            | PgShdescriptionToastTable
            | PgShdescriptionToastIndex
            | PgShseclabelToastTable
            | PgShseclabelToastIndex
            | PgSubscriptionToastTable
            | PgSubscriptionToastIndex
            | PgTablespaceToastTable
            | PgTablespaceToastIndex
    )
}

pub fn IsPinnedObject(classId: Oid, objectId: Oid) -> bool {
    // The OID generator skips [FirstUnpinnedObjectId, FirstNormalObjectId) on
    // wraparound, so user objects are never considered pinned.
    if objectId >= FirstUnpinnedObjectId {
        return false;
    }
    // Large object OIDs can be user-assigned.
    if classId == LargeObjectRelationId {
        return false;
    }
    if classId == NamespaceRelationId && objectId == PG_PUBLIC_NAMESPACE {
        return false;
    }
    // Unpinned so template0 and template1 can be rebuilt from each other.
    if classId == DATABASE_RELATION_ID {
        return false;
    }
    true
}

pub fn init_seams() {
    catalog_seams::is_catalog_relation::set(IsCatalogRelation);
    catalog_seams::is_toast_relation::set(IsToastRelation);
    catalog_seams::is_shared_relation::set(IsSharedRelation);
    catalog_seams::is_catalog_relation_oid::set(IsCatalogRelationOid);
    catalog_seams::get_new_oid_with_index::set(|mcx, rel, index_id, oidcolumn| {
        GetNewOidWithIndex(mcx, rel, index_id, oidcolumn)
    });
}
