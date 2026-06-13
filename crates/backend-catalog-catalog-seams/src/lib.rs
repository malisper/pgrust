//! Seam declarations for the `backend-catalog-catalog` unit
//! (`catalog/catalog.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::primitive::Oid;

seam_core::seam!(
    /// `IsPinnedObject(classId, objectId)` (catalog/catalog.c): is the object
    /// required for basic database functionality? Pure OID-range arithmetic —
    /// infallible.
    pub fn is_pinned_object(class_id: Oid, object_id: Oid) -> bool
);

seam_core::seam!(
    /// `IsSharedRelation(relationId)` (catalog/catalog.c): is the relation a
    /// shared catalog (lives in the global tablespace, visible from every
    /// database)? Lookup against a fixed OID set — infallible.
    pub fn is_shared_relation(relation_id: Oid) -> bool
);
