use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn get_type_output_info(type_oid: Oid) -> PgResult<(Oid, bool)>
);

seam_core::seam!(
    pub fn get_type_binary_output_info(type_oid: Oid) -> PgResult<(Oid, bool)>
);

seam_core::seam!(
    // getBaseTypeAndTypmod (lsyscache.c): C updates *typmod in place; a
    // non-domain input returns (typid, typmod) unchanged.
    pub fn get_base_type_and_typmod(typid: Oid, typmod: i32) -> PgResult<(Oid, i32)>
);
