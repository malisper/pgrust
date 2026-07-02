use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn get_default_opclass(type_id: Oid, am_id: Oid) -> PgResult<Oid>
);
