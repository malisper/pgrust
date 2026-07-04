use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn get_object_description(
        mcx: Mcx<'_>,
        class_id: Oid,
        object_id: Oid,
        object_sub_id: i32,
        missing_ok: bool,
    ) -> PgResult<Option<String>>
);
