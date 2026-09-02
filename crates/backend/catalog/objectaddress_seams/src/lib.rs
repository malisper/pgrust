#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::ObjectType;
use types_nodes::Node;
use types_rel::{Relation, LOCKMODE};

// Marshal shape of pg_depend::ObjectAddress (a pg_depend dep here would cycle
// through pg_shdepend).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ObjectAddr {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: i32,
}

seam_core::seam!(
    pub fn get_object_description(
        mcx: Mcx<'_>,
        class_id: Oid,
        object_id: Oid,
        object_sub_id: i32,
        missing_ok: bool,
    ) -> PgResult<Option<String>>
);

seam_core::seam!(
    pub fn get_object_address<'mcx>(
        mcx: Mcx<'mcx>,
        objtype: ObjectType,
        object: Node<'mcx>,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<(ObjectAddr, Option<Relation<'mcx>>)>
);

seam_core::seam!(
    pub fn check_object_ownership<'mcx, 'a>(
        mcx: Mcx<'mcx>,
        roleid: Oid,
        objtype: ObjectType,
        address: ObjectAddr,
        object: Node<'mcx>,
        relation: Option<&'a Relation<'mcx>>,
    ) -> PgResult<()>
);

// upstream c8cd3d6976f7 (18.6): Avoid orphaned objects dependencies
// get_object_catcache_oid / get_object_oid_index / get_object_attnum_oid /
// get_object_class_descr (objectaddress.c) for crates below
// catalog_objectaddress (pg_depend's dependencyLockAndCheckObject).
#[derive(Clone, Copy)]
pub struct ObjectClassProps {
    pub oid_catcache_id: i32,
    pub oid_index_oid: Oid,
    pub attnum_oid: i32,
    pub class_descr: &'static str,
}

seam_core::seam!(
    pub fn get_object_class_props(class_id: Oid) -> ObjectClassProps
);
