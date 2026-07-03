use types_core::Oid;
use types_error::PgResult;

pub mod builtins;

seam_core::seam!(
    pub fn nextval_internal(relid: Oid, check_permissions: bool) -> PgResult<i64>
);

seam_core::seam!(
    pub fn currval_internal(relid: Oid) -> PgResult<i64>
);

seam_core::seam!(
    pub fn lastval_internal() -> PgResult<i64>
);

seam_core::seam!(
    pub fn do_setval(relid: Oid, next: i64, iscalled: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn delete_sequence_tuple(relid: Oid) -> PgResult<()>
);
