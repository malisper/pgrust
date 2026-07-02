use std::rc::Rc;
use types_core::Oid;
use types_error::PgResult;
use types_rel::RelationData;

seam_core::seam!(
    pub fn relation_id_get_relation(
        relation_id: Oid,
    ) -> PgResult<Option<Rc<RelationData<'static>>>>
);
