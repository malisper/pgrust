use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::ObjectType;
use types_nodes::Node;
use types_rel::{Relation, LOCKMODE};

seam_core::seam!(
    pub fn get_object_address<'mcx>(
        mcx: Mcx<'mcx>,
        objtype: ObjectType,
        object: Node<'mcx>,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<(ObjectAddress, Option<Relation<'mcx>>)>
);

seam_core::seam!(
    pub fn check_object_ownership<'mcx>(
        mcx: Mcx<'mcx>,
        roleid: Oid,
        objtype: ObjectType,
        address: ObjectAddress,
        object: Node<'mcx>,
    ) -> PgResult<()>
);
