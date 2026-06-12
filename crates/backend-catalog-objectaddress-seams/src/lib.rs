//! Seam declarations for the `backend-catalog-objectaddress` unit
//! (`catalog/objectaddress.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_nodes::parsenodes::ObjectType;

seam_core::seam!(
    /// `get_relkind_objtype(relkind)` (objectaddress.c): map a `pg_class`
    /// relkind to the `ObjectType` used in error messages. Total mapping
    /// (unknown relkinds return `OBJECT_TABLE`); cannot `ereport`.
    pub fn get_relkind_objtype(relkind: u8) -> ObjectType
);
