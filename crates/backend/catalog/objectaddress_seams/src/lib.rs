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
