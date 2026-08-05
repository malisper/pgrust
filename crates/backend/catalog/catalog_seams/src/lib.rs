use types_core::Oid;
use types_rel::RelationData;

seam_core::seam!(
    pub fn is_catalog_relation(relation: &RelationData<'_>) -> bool
);

seam_core::seam!(
    pub fn is_toast_relation(relation: &RelationData<'_>) -> bool
);

seam_core::seam!(
    pub fn is_shared_relation(relid: Oid) -> bool
);

seam_core::seam!(
    pub fn is_catalog_relation_oid(relid: Oid) -> bool
);

seam_core::seam!(
    // function_parse_error_transpose (pg_proc.c): remap a validator error's
    // position from function-body offsets onto the original command's string
    // literal (true = error had a position and was handled). Installed by
    // pg_proc; sql_functions calls it (a direct dep cycles via pquery ->
    // execmain -> seams_init).
    pub fn function_parse_error_transpose<'a>(
        e: &'a mut types_error::PgError,
        prosrc: &'a str,
    ) -> bool
);

seam_core::seam!(
    pub fn get_new_oid_with_index<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation: &types_rel::Relation<'mcx>,
        index_id: Oid,
        oidcolumn: i16,
    ) -> types_error::PgResult<Oid>
);
