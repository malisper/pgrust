// :HACK: Keep the historical root module path while rewrite lives in its crate.
// Root wrappers are still needed for rewrite paths that can re-enter semantic
// analysis while deparsing or expanding stored view queries.
#[allow(unused_imports)]
pub(crate) use pgrust_rewrite::{
    NonUpdatableViewColumn, NonUpdatableViewColumnReason, ResolvedAutoViewTarget, RlsWriteCheck,
    RlsWriteCheckSource, TargetRlsState, ViewDmlEvent, ViewDmlRewriteError, ViewPrivilegeContext,
    apply_query_row_security, build_target_relation_row_security,
    build_target_relation_row_security_for_user, classify_view_dml_rules,
    collect_query_relation_privileges, format_stored_rule_definition, load_view_return_select,
    refresh_query_relation_descriptors, relation_has_row_security, relation_has_security_invoker,
    relation_row_security_is_enabled_for_user, render_relation_expr_sql,
    render_relation_expr_sql_for_constraint, render_relation_expr_sql_for_information_schema,
    render_view_query_sql, split_stored_rule_action_sql, split_stored_view_definition_sql,
    with_restrict_nonsystem_view_expansion,
};

use crate::backend::parser::analyze::CatalogLookup;
use crate::include::catalog::PgRewriteRow;
use crate::include::nodes::parsenodes::{ParseError, Query};
use crate::include::nodes::primnodes::RelationDesc;

pub(crate) fn pg_rewrite_query(
    query: Query,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Query>, ParseError> {
    crate::backend::parser::analyze::with_root_analyze_services(|| {
        pgrust_rewrite::pg_rewrite_query(query, catalog)
    })
}

pub(crate) fn load_view_return_query(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    alias: Option<&str>,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<Query, ParseError> {
    crate::backend::parser::analyze::with_root_analyze_services(|| {
        pgrust_rewrite::load_view_return_query(
            relation_oid,
            relation_desc,
            alias,
            catalog,
            expanded_views,
        )
    })
}

pub(crate) fn format_view_definition(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<String, ParseError> {
    crate::backend::parser::analyze::with_root_analyze_services(|| {
        pgrust_rewrite::format_view_definition(relation_oid, relation_desc, catalog)
    })
}

pub(crate) fn format_view_definition_unpretty(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<String, ParseError> {
    crate::backend::parser::analyze::with_root_analyze_services(|| {
        pgrust_rewrite::format_view_definition_unpretty(relation_oid, relation_desc, catalog)
    })
}

pub(crate) fn format_stored_rule_definition_with_catalog(
    rule: &PgRewriteRow,
    relation_name: &str,
    catalog: &dyn CatalogLookup,
) -> String {
    crate::backend::parser::analyze::with_root_analyze_services(|| {
        pgrust_rewrite::format_stored_rule_definition_with_catalog(rule, relation_name, catalog)
    })
}

pub(crate) fn resolve_auto_updatable_view_target(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    event: ViewDmlEvent,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<ResolvedAutoViewTarget, ViewDmlRewriteError> {
    crate::backend::parser::analyze::with_root_analyze_services(|| {
        pgrust_rewrite::resolve_auto_updatable_view_target(
            relation_oid,
            relation_desc,
            event,
            catalog,
            expanded_views,
        )
    })
}
