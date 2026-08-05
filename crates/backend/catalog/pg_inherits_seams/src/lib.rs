use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn type_inherits_from(subclass_type_id: Oid, superclass_type_id: Oid) -> PgResult<bool>
);
