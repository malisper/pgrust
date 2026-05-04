// :HACK: Keep executor permission helpers on the old path while catalog-row privilege
// logic lives in pgrust_catalog_store.
use super::ExecutorContext;
use crate::backend::parser::CatalogLookup;

pub(crate) use pgrust_catalog_store::relation_has_table_privilege;

struct ExecutorPermissionCatalog<'a>(&'a dyn CatalogLookup);

impl pgrust_executor::PermissionCatalog for ExecutorPermissionCatalog<'_> {
    fn class_row_by_oid(&self, relation_oid: u32) -> Option<pgrust_catalog_data::PgClassRow> {
        self.0.class_row_by_oid(relation_oid)
    }

    fn authid_rows(&self) -> Vec<pgrust_catalog_data::PgAuthIdRow> {
        self.0.authid_rows()
    }

    fn auth_members_rows(&self) -> Vec<pgrust_catalog_data::PgAuthMembersRow> {
        self.0.auth_members_rows()
    }
}

pub(crate) fn relation_values_visible_for_error_detail(
    relation_oid: u32,
    ctx: &ExecutorContext,
) -> bool {
    let Some(catalog) = ctx.catalog.as_deref() else {
        return pgrust_executor::relation_values_visible_for_error_detail(
            relation_oid,
            ctx.current_user_oid,
            None,
        );
    };
    let catalog = ExecutorPermissionCatalog(catalog);
    pgrust_executor::relation_values_visible_for_error_detail(
        relation_oid,
        ctx.current_user_oid,
        Some(&catalog),
    )
}
