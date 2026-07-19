//! Seams for alter.c entry points needed below the commands layer
//! (commands_alter transitively depends on catalog_objectaddress, which
//! depends on extension — so extension.c's AlterExtensionNamespace reaches
//! AlterObjectNamespace_oid through this seam).

use mcx::{Mcx, PgVec};
use pg_depend::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // AlterObjectNamespace_oid (alter.c): relocate one object of any class;
    // returns its old namespace OID (InvalidOid = class has no namespace).
    pub fn alter_object_namespace_oid<'mcx>(
        mcx: Mcx<'mcx>,
        class_id: Oid,
        objid: Oid,
        nsp_oid: Oid,
        objs_moved: &mut PgVec<'mcx, ObjectAddress>,
    ) -> PgResult<Oid>
);
