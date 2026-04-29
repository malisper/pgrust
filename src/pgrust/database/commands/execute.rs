use super::super::*;
use crate::backend::executor::{
    ExecutorTransactionState, SharedExecutorTransactionState, execute_planned_stmt,
    execute_readonly_statement_with_config,
};
use crate::backend::parser::{
    CatalogLookup, CommonTableExpr, CteBody, FromItem, InsertSource, InsertStatement, ParseOptions,
    SelectStatement, bind_insert_with_outer_scopes_and_ctes, bound_cte_from_query_rows,
};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::notices::push_warning_with_hint;
use crate::backend::utils::misc::stack_depth::StackDepthGuard;
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::ReplicaIdentityKind;
use crate::include::nodes::pathnodes::PlannerConfig;
use crate::include::nodes::primnodes::QueryColumn;
use crate::pl::plpgsql::execute_do_with_gucs;

fn restrict_nonsystem_view_enabled(gucs: &std::collections::HashMap<String, String>) -> bool {
    gucs.get("restrict_nonsystem_relation_kind")
        .map(|value| {
            value
                .split(',')
                .any(|part| part.trim().trim_matches('\'').eq_ignore_ascii_case("view"))
        })
        .unwrap_or(false)
}

fn normalize_direct_guc_name(name: &str) -> String {
    name.trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_ascii_lowercase()
}

fn parse_direct_bool_guc(value: &str) -> Option<bool> {
    match value
        .trim()
        .trim_matches('\'')
        .to_ascii_lowercase()
        .as_str()
    {
        "on" | "true" | "yes" | "1" | "t" => Some(true),
        "off" | "false" | "no" | "0" | "f" => Some(false),
        _ => None,
    }
}

fn direct_guc_default(name: &str) -> Option<&'static str> {
    match name {
        "enable_partitionwise_join" => Some("off"),
        "enable_partitionwise_aggregate" => Some("off"),
        "enable_seqscan"
        | "enable_indexscan"
        | "enable_indexonlyscan"
        | "enable_bitmapscan"
        | "enable_nestloop"
        | "enable_hashjoin"
        | "enable_mergejoin"
        | "enable_memoize"
        | "enable_material"
        | "enable_hashagg"
        | "enable_sort" => Some("on"),
        "debug_parallel_query" => Some("off"),
        "max_parallel_workers_per_gather" => Some("2"),
        _ => None,
    }
}

fn direct_bool_config(
    gucs: &std::collections::HashMap<String, String>,
    name: &str,
    default: bool,
) -> bool {
    gucs.get(name)
        .and_then(|value| parse_direct_bool_guc(value))
        .unwrap_or(default)
}

fn direct_usize_config(
    gucs: &std::collections::HashMap<String, String>,
    name: &str,
    default: usize,
) -> usize {
    gucs.get(name)
        .and_then(|value| value.trim().trim_matches('\'').parse::<usize>().ok())
        .unwrap_or(default)
}

fn direct_planner_config(gucs: &std::collections::HashMap<String, String>) -> PlannerConfig {
    PlannerConfig {
        enable_partitionwise_join: direct_bool_config(gucs, "enable_partitionwise_join", false),
        enable_partitionwise_aggregate: direct_bool_config(
            gucs,
            "enable_partitionwise_aggregate",
            false,
        ),
        enable_seqscan: direct_bool_config(gucs, "enable_seqscan", true),
        enable_indexscan: direct_bool_config(gucs, "enable_indexscan", true),
        enable_indexonlyscan: direct_bool_config(gucs, "enable_indexonlyscan", true),
        enable_bitmapscan: direct_bool_config(gucs, "enable_bitmapscan", true),
        enable_nestloop: direct_bool_config(gucs, "enable_nestloop", true),
        enable_hashjoin: direct_bool_config(gucs, "enable_hashjoin", true),
        enable_mergejoin: direct_bool_config(gucs, "enable_mergejoin", true),
        enable_memoize: direct_bool_config(gucs, "enable_memoize", true),
        enable_material: direct_bool_config(gucs, "enable_material", true),
        retain_partial_index_filters: false,
        enable_hashagg: direct_bool_config(gucs, "enable_hashagg", true),
        enable_sort: direct_bool_config(gucs, "enable_sort", true),
        force_parallel_gather: direct_bool_config(gucs, "debug_parallel_query", false),
        max_parallel_workers_per_gather: direct_usize_config(
            gucs,
            "max_parallel_workers_per_gather",
            2,
        ),
    }
}

fn reject_restricted_view_access(name: &str, catalog: &dyn CatalogLookup) -> Result<(), ExecError> {
    let Some(entry) = catalog.lookup_any_relation(name) else {
        return Ok(());
    };
    if entry.relkind == 'v'
        && entry.namespace_oid != crate::include::catalog::PG_CATALOG_NAMESPACE_OID
    {
        return Err(ExecError::DetailedError {
            message: format!("access to non-system view \"{name}\" is restricted"),
            detail: None,
            hint: None,
            sqlstate: "42501",
        });
    }
    Ok(())
}

fn reject_restricted_views_in_select(
    select: &SelectStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for cte in &select.with {
        reject_restricted_views_in_cte_body(&cte.body, catalog)?;
    }
    if let Some(from) = &select.from {
        reject_restricted_views_in_from_item(from, catalog)?;
    }
    if let Some(set_op) = &select.set_operation {
        for input in &set_op.inputs {
            reject_restricted_views_in_select(input, catalog)?;
        }
    }
    Ok(())
}

fn reject_restricted_views_in_cte_body(
    body: &CteBody,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    match body {
        CteBody::Select(select) => reject_restricted_views_in_select(select, catalog),
        CteBody::Values(_) => Ok(()),
        CteBody::Insert(insert) => reject_restricted_views_in_insert(insert, catalog),
        CteBody::RecursiveUnion {
            anchor, recursive, ..
        } => {
            reject_restricted_views_in_cte_body(anchor, catalog)?;
            reject_restricted_views_in_select(recursive, catalog)
        }
    }
}

fn reject_restricted_views_in_from_item(
    item: &FromItem,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    match item {
        FromItem::Table { name, .. } => reject_restricted_view_access(name, catalog),
        FromItem::DerivedTable(select) => reject_restricted_views_in_select(select, catalog),
        FromItem::Join { left, right, .. } => {
            reject_restricted_views_in_from_item(left, catalog)?;
            reject_restricted_views_in_from_item(right, catalog)
        }
        FromItem::Alias { source, .. }
        | FromItem::Lateral(source)
        | FromItem::TableSample { source, .. } => {
            reject_restricted_views_in_from_item(source, catalog)
        }
        FromItem::Values { .. }
        | FromItem::FunctionCall { .. }
        | FromItem::JsonTable(_)
        | FromItem::XmlTable(_) => Ok(()),
    }
}

fn reject_restricted_views_in_insert(
    insert: &InsertStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for cte in &insert.with {
        reject_restricted_views_in_cte_body(&cte.body, catalog)?;
    }
    reject_restricted_view_access(&insert.table_name, catalog)?;
    if let InsertSource::Select(select) = &insert.source {
        reject_restricted_views_in_select(select, catalog)?;
    }
    Ok(())
}

fn autocommit_datetime_config(config: &DateTimeConfig) -> DateTimeConfig {
    let statement_timestamp_usecs = config
        .statement_timestamp_usecs
        .unwrap_or_else(crate::backend::utils::time::datetime::current_postgres_timestamp_usecs);
    let transaction_timestamp_usecs = config
        .transaction_timestamp_usecs
        .unwrap_or(statement_timestamp_usecs);
    let mut config = config.clone();
    config.statement_timestamp_usecs = Some(statement_timestamp_usecs);
    config.transaction_timestamp_usecs = Some(transaction_timestamp_usecs);
    config
}

fn statement_timestamp_usecs(config: &DateTimeConfig) -> i64 {
    config
        .statement_timestamp_usecs
        .unwrap_or_else(crate::backend::utils::time::datetime::current_postgres_timestamp_usecs)
}

fn apply_writable_cte_column_aliases(
    cte: &CommonTableExpr,
    mut columns: Vec<QueryColumn>,
) -> Result<Vec<QueryColumn>, ExecError> {
    if cte.column_names.is_empty() {
        return Ok(columns);
    }
    if cte.column_names.len() != columns.len() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CTE column alias count matching query width",
            actual: format!(
                "CTE query has {} columns but {} column aliases were specified",
                columns.len(),
                cte.column_names.len()
            ),
        }));
    }
    for (column, name) in columns.iter_mut().zip(cte.column_names.iter()) {
        column.name = name.clone();
    }
    Ok(columns)
}

fn oa_sql_tokens(sql: &str) -> Vec<String> {
    sql.split_whitespace()
        .map(oa_clean_sql_token)
        .filter(|token| !token.is_empty())
        .collect()
}

fn oa_clean_sql_token(token: &str) -> String {
    let trimmed = token.trim_matches(|ch: char| matches!(ch, ';' | ',' | '(' | ')'));
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        return trimmed[1..trimmed.len() - 1].replace("\"\"", "\"");
    }
    trimmed.to_string()
}

fn oa_token_after(tokens: &[String], pattern: &[&str]) -> Option<String> {
    tokens
        .windows(pattern.len().saturating_add(1))
        .find(|window| {
            pattern.iter().enumerate().all(|(idx, expected)| {
                window
                    .get(idx)
                    .is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
            })
        })
        .and_then(|window| window.get(pattern.len()).cloned())
}

fn oa_first_token_after_prefix(sql: &str, prefix: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with(prefix) {
        return None;
    }
    trimmed
        .get(prefix.len()..)?
        .split_whitespace()
        .next()
        .map(oa_clean_sql_token)
}

fn oa_default_acl_objtype(name: &str) -> Result<char, ExecError> {
    match name.to_ascii_lowercase().as_str() {
        "table" | "tables" => Ok('r'),
        "sequence" | "sequences" => Ok('S'),
        "function" | "functions" | "routine" | "routines" => Ok('f'),
        "type" | "types" => Ok('T'),
        "schema" | "schemas" => Ok('n'),
        _ => Err(ExecError::DetailedError {
            message: format!("unrecognized default ACL object type \"{name}\""),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
    }
}

fn oa_unsupported_ddl(feature: &str, sql: &str) -> ExecError {
    ExecError::Parse(ParseError::FeatureNotSupported(format!("{feature}: {sql}")))
}

impl Database {
    fn execute_object_address_unsupported_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::UnsupportedStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<Option<StatementResult>, ExecError> {
        match stmt.feature {
            "ALTER DEFAULT PRIVILEGES" => {
                self.execute_alter_default_privileges_for_object_address(
                    client_id,
                    &stmt.sql,
                    configured_search_path,
                )?;
                Ok(Some(StatementResult::AffectedRows(0)))
            }
            "CREATE TRANSFORM" => {
                self.execute_create_transform_for_object_address(
                    client_id,
                    &stmt.sql,
                    configured_search_path,
                )?;
                Ok(Some(StatementResult::AffectedRows(0)))
            }
            "CREATE SUBSCRIPTION" => {
                self.execute_create_subscription_for_object_address(client_id, &stmt.sql);
                Ok(Some(StatementResult::AffectedRows(0)))
            }
            "DROP SUBSCRIPTION" => {
                self.execute_drop_subscription_for_object_address(&stmt.sql);
                Ok(Some(StatementResult::AffectedRows(0)))
            }
            _ => Ok(None),
        }
    }

    fn execute_alter_default_privileges_for_object_address(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        // :HACK: Track only object-address identity rows, not default privilege enforcement.
        let tokens = oa_sql_tokens(sql);
        let role_name = oa_token_after(&tokens, &["for", "role"])
            .ok_or_else(|| oa_unsupported_ddl("ALTER DEFAULT PRIVILEGES", sql))?;
        let namespace_name = oa_token_after(&tokens, &["in", "schema"]);
        let object_kind = oa_token_after(&tokens, &["on"])
            .ok_or_else(|| oa_unsupported_ddl("ALTER DEFAULT PRIVILEGES", sql))?;
        let objtype = oa_default_acl_objtype(&object_kind)?;
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let role = catalog
            .authid_rows()
            .into_iter()
            .find(|row| row.rolname.eq_ignore_ascii_case(&role_name))
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("role \"{role_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        let namespace = namespace_name
            .as_deref()
            .map(|name| {
                catalog
                    .namespace_rows()
                    .into_iter()
                    .find(|row| row.nspname.eq_ignore_ascii_case(name))
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })
            })
            .transpose()?;
        self.object_addresses.write().upsert_default_acl(
            role.oid,
            role.rolname,
            namespace.as_ref().map(|row| row.oid),
            namespace.map(|row| row.nspname),
            objtype,
        );
        Ok(())
    }

    fn execute_create_transform_for_object_address(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        // :HACK: Record transform identity only; transform execution is intentionally absent.
        let tokens = oa_sql_tokens(sql);
        let type_name = oa_token_after(&tokens, &["for"])
            .ok_or_else(|| oa_unsupported_ddl("CREATE TRANSFORM", sql))?;
        let language_name = oa_token_after(&tokens, &["language"])
            .ok_or_else(|| oa_unsupported_ddl("CREATE TRANSFORM", sql))?;
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let raw_type = crate::backend::parser::parse_type_name(&type_name).unwrap_or_else(|_| {
            crate::backend::parser::RawTypeName::Named {
                name: type_name.clone(),
                array_bounds: 0,
            }
        });
        let sql_type = crate::backend::parser::resolve_raw_type_name(&raw_type, &catalog)
            .map_err(ExecError::Parse)?;
        let type_oid = catalog
            .type_oid_for_sql_type(sql_type)
            .filter(|oid| *oid != 0)
            .unwrap_or(sql_type.type_oid);
        if type_oid == 0 {
            return Err(ExecError::Parse(ParseError::UnsupportedType(type_name)));
        }
        let language = catalog
            .language_row_by_name(&language_name)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("language \"{language_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        self.object_addresses
            .write()
            .upsert_transform(type_oid, language.oid);
        Ok(())
    }

    fn execute_create_subscription_for_object_address(&self, client_id: ClientId, sql: &str) {
        // :HACK: Store enough subscription identity for object-address regression coverage.
        if let Some(name) = oa_first_token_after_prefix(sql, "create subscription") {
            self.object_addresses
                .write()
                .upsert_subscription(name, self.auth_state(client_id).current_user_oid());
        }
        push_warning_with_hint(
            "subscription was created, but is not connected",
            "To initiate replication, you must manually create the replication slot, enable the subscription, and refresh the subscription.",
        );
    }

    fn execute_drop_subscription_for_object_address(&self, sql: &str) {
        if let Some(name) = oa_first_token_after_prefix(sql, "drop subscription") {
            self.object_addresses.write().drop_subscription(&name);
        }
    }

    pub(crate) fn execute_alter_table_replica_identity_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::AlterTableReplicaIdentityStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) =
            crate::pgrust::database::ddl::lookup_table_or_partitioned_table_for_alter_table(
                &catalog,
                &stmt.table_name,
                stmt.if_exists,
            )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        let (identity, index_oid) = match &stmt.identity {
            ReplicaIdentityKind::Default => ('d', None),
            ReplicaIdentityKind::Full => ('f', None),
            ReplicaIdentityKind::Nothing => ('n', None),
            ReplicaIdentityKind::Index(index_name) => {
                let index = catalog
                    .index_relations_for_heap(relation.relation_oid)
                    .into_iter()
                    .find(|index| index.name.eq_ignore_ascii_case(index_name))
                    .ok_or_else(|| {
                        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                            expected: "index on table",
                            actual: format!(
                                "index \"{}\" does not exist for table \"{}\"",
                                index_name, stmt.table_name
                            ),
                        })
                    })?;
                if !index.index_meta.indisunique {
                    return Err(ExecError::Parse(
                        crate::backend::parser::ParseError::DetailedError {
                            message: format!(
                                "cannot use non-unique index \"{}\" as replica identity",
                                index_name
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42809",
                        },
                    ));
                }
                ('i', Some(index.relation_oid))
            }
        };

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: None,
            interrupts,
        };
        let mut catalog_effects = Vec::new();
        let result = self
            .catalog
            .write()
            .set_replica_identity_mvcc(relation.relation_oid, identity, index_oid, &ctx)
            .map(|effect| {
                catalog_effects.push(effect);
                StatementResult::AffectedRows(0)
            })
            .map_err(map_catalog_error);
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_truncate_table_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::TruncateTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let mut rewrite_oids = Vec::new();
        let mut truncated_relation_oids = Vec::new();

        for table_name in &stmt.table_names {
            let entry = match catalog.lookup_any_relation(table_name) {
                Some(entry) if entry.relkind == 'r' || entry.relkind == 'p' => entry,
                Some(_) => {
                    return Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: table_name.clone(),
                        expected: "table",
                    }));
                }
                None => {
                    return Err(ExecError::Parse(ParseError::UnknownTable(
                        table_name.clone(),
                    )));
                }
            };
            let truncate_targets = if entry.relkind == 'p' {
                partitioned_truncate_targets(&catalog, entry.relation_oid)
            } else if catalog.has_subclass(entry.relation_oid) {
                return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                    "TRUNCATE on inherited parents is not supported yet".into(),
                )));
            } else {
                vec![entry]
            };

            for target in truncate_targets {
                if !truncated_relation_oids.contains(&target.relation_oid) {
                    truncated_relation_oids.push(target.relation_oid);
                }

                if !rewrite_oids.contains(&target.relation_oid) {
                    rewrite_oids.push(target.relation_oid);
                }
                for index in catalog.index_relations_for_heap(target.relation_oid) {
                    if !rewrite_oids.contains(&index.relation_oid) {
                        rewrite_oids.push(index.relation_oid);
                    }
                }
                if let Some(toast) = target.toast {
                    if !rewrite_oids.contains(&toast.relation_oid) {
                        rewrite_oids.push(toast.relation_oid);
                    }
                    for index in catalog.index_relations_for_heap(toast.relation_oid) {
                        if !rewrite_oids.contains(&index.relation_oid) {
                            rewrite_oids.push(index.relation_oid);
                        }
                    }
                }
            }
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .rewrite_relation_storage_mvcc(&rewrite_oids, &ctx)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        {
            let stats_state = self.session_stats_state(client_id);
            let mut stats_state = stats_state.write();
            for relation_oid in truncated_relation_oids {
                stats_state.note_relation_truncate(relation_oid);
            }
        }
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub fn execute(&self, client_id: ClientId, sql: &str) -> Result<StatementResult, ExecError> {
        self.execute_with_search_path_and_datetime_config(
            client_id,
            sql,
            None,
            &DateTimeConfig::default(),
        )
    }

    fn direct_gucs_for_client(
        &self,
        client_id: ClientId,
    ) -> std::collections::HashMap<String, String> {
        self.session_guc_states
            .read()
            .get(&client_id)
            .cloned()
            .unwrap_or_default()
    }

    fn apply_direct_guc_statement(
        &self,
        client_id: ClientId,
        stmt: &Statement,
    ) -> Result<Option<StatementResult>, ExecError> {
        match stmt {
            Statement::Set(set_stmt) => {
                let name = normalize_direct_guc_name(&set_stmt.name);
                if direct_guc_default(&name).is_some() {
                    let mut states = self.session_guc_states.write();
                    let gucs = states.entry(client_id).or_default();
                    if let Some(value) = set_stmt.value.as_ref() {
                        if parse_direct_bool_guc(value).is_none() {
                            return Err(ExecError::Parse(ParseError::UnrecognizedParameter(
                                value.clone(),
                            )));
                        }
                        gucs.insert(name, value.trim().trim_matches('\'').to_ascii_lowercase());
                    } else {
                        gucs.remove(&name);
                    }
                }
                Ok(Some(StatementResult::AffectedRows(0)))
            }
            Statement::Reset(reset_stmt) => {
                let mut states = self.session_guc_states.write();
                if let Some(name) = reset_stmt.name.as_ref() {
                    let name = normalize_direct_guc_name(name);
                    if let Some(gucs) = states.get_mut(&client_id) {
                        gucs.remove(&name);
                    }
                } else {
                    states.remove(&client_id);
                }
                Ok(Some(StatementResult::AffectedRows(0)))
            }
            Statement::Show(show_stmt) => {
                let name = normalize_direct_guc_name(&show_stmt.name);
                let Some(default) = direct_guc_default(&name) else {
                    return Ok(Some(StatementResult::AffectedRows(0)));
                };
                let gucs = self.direct_gucs_for_client(client_id);
                let value = gucs.get(&name).map(String::as_str).unwrap_or(default);
                Ok(Some(StatementResult::Query {
                    columns: vec![QueryColumn::text(show_stmt.name.clone())],
                    column_names: vec![show_stmt.name.clone()],
                    rows: vec![vec![Value::Text(value.into())]],
                }))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn execute_with_search_path(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_with_search_path_and_datetime_config(
            client_id,
            sql,
            configured_search_path,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_with_search_path_and_datetime_config(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
    ) -> Result<StatementResult, ExecError> {
        stacker::grow(32 * 1024 * 1024, || {
            StackDepthGuard::enter(datetime_config.max_stack_depth_kb).run(|| {
                let stmt = self.plan_cache.get_statement_with_options(
                    sql,
                    ParseOptions {
                        max_stack_depth_kb: datetime_config.max_stack_depth_kb,
                        ..ParseOptions::default()
                    },
                )?;
                if let Some(result) = self.apply_direct_guc_statement(client_id, &stmt)? {
                    return Ok(result);
                }
                let gucs = self.direct_gucs_for_client(client_id);
                let planner_config = direct_planner_config(&gucs);
                self.execute_statement_with_search_path_datetime_config_gucs_and_planner_config(
                    client_id,
                    stmt,
                    configured_search_path,
                    datetime_config,
                    &gucs,
                    planner_config,
                )
            })
        })
    }

    pub(crate) fn execute_statement_with_search_path(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_and_datetime_config(
            client_id,
            stmt,
            configured_search_path,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_statement_with_search_path_and_datetime_config(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_datetime_config_and_gucs(
            client_id,
            stmt,
            configured_search_path,
            datetime_config,
            &std::collections::HashMap::new(),
        )
    }

    pub(crate) fn execute_statement_with_search_path_datetime_config_and_gucs(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_datetime_config_gucs_and_planner_config(
            client_id,
            stmt,
            configured_search_path,
            datetime_config,
            gucs,
            PlannerConfig::default(),
        )
    }

    pub(crate) fn execute_statement_with_config(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        planner_config: PlannerConfig,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_datetime_config_gucs_and_planner_config(
            client_id,
            stmt,
            configured_search_path,
            datetime_config,
            &std::collections::HashMap::new(),
            planner_config,
        )
    }

    pub(crate) fn execute_statement_with_search_path_datetime_config_gucs_and_planner_config(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        planner_config: PlannerConfig,
    ) -> Result<StatementResult, ExecError> {
        self.execute_statement_with_search_path_datetime_config_gucs_planner_config_and_random_state(
            client_id,
            stmt,
            configured_search_path,
            datetime_config,
            gucs,
            planner_config,
            crate::backend::executor::PgPrngState::shared(),
        )
    }

    pub(crate) fn execute_statement_with_search_path_datetime_config_gucs_planner_config_and_random_state(
        &self,
        client_id: ClientId,
        stmt: Statement,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        planner_config: PlannerConfig,
        random_state: std::sync::Arc<parking_lot::Mutex<crate::backend::executor::PgPrngState>>,
    ) -> Result<StatementResult, ExecError> {
        let datetime_config = autocommit_datetime_config(datetime_config);
        let statement_lock_scope_id = Some(self.allocate_statement_lock_scope_id());
        let stats_state = self.session_stats_state(client_id);
        stats_state.write().begin_top_level_xact();
        let advisory_locks = std::sync::Arc::clone(&self.advisory_locks);
        let row_locks = std::sync::Arc::clone(&self.row_locks);
        let result = self.execute_statement_with_search_path_inner(
            client_id,
            stmt,
            statement_lock_scope_id,
            configured_search_path,
            &datetime_config,
            gucs,
            planner_config,
            random_state,
        );
        if let Some(scope_id) = statement_lock_scope_id {
            advisory_locks.unlock_all_statement(client_id, scope_id);
            row_locks.unlock_all_statement(client_id, scope_id);
        }
        match &result {
            Ok(_) => stats_state.write().commit_top_level_xact(&self.stats),
            Err(_) => stats_state.write().rollback_top_level_xact(),
        }
        result
    }

    pub(crate) fn finish_txn_with_async_notifications(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        result: Result<StatementResult, ExecError>,
        catalog_effects: &[CatalogMutationEffect],
        temp_effects: &[TempMutationEffect],
        sequence_effects: &[SequenceMutationEffect],
        pending_async_notifications: Vec<PendingNotification>,
    ) -> Result<StatementResult, ExecError> {
        let result = self.finish_txn(
            client_id,
            xid,
            result,
            catalog_effects,
            temp_effects,
            sequence_effects,
        );
        if result.is_ok() {
            self.async_notify_runtime
                .publish(client_id, &pending_async_notifications);
        }
        result
    }

    fn execute_notify_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::NotifyStatement,
    ) -> Result<StatementResult, ExecError> {
        let mut pending_async_notifications = Vec::new();
        queue_pending_notification(
            &mut pending_async_notifications,
            &stmt.channel,
            stmt.payload.as_deref().unwrap_or(""),
        )?;
        self.async_notify_runtime
            .publish(client_id, &pending_async_notifications);
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_listen_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::ListenStatement,
    ) -> StatementResult {
        self.async_notify_runtime.listen(client_id, &stmt.channel);
        StatementResult::AffectedRows(0)
    }

    fn execute_unlisten_stmt(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::UnlistenStatement,
    ) -> StatementResult {
        self.async_notify_runtime
            .unlisten(client_id, stmt.channel.as_deref());
        StatementResult::AffectedRows(0)
    }

    fn execute_statement_with_search_path_inner(
        &self,
        client_id: ClientId,
        stmt: Statement,
        statement_lock_scope_id: Option<u64>,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        planner_config: PlannerConfig,
        random_state: std::sync::Arc<parking_lot::Mutex<crate::backend::executor::PgPrngState>>,
    ) -> Result<StatementResult, ExecError> {
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::commands::tablecmds::{
            check_planned_stmt_select_for_update_privileges, check_planned_stmt_select_privileges,
            execute_truncate_table,
        };
        let interrupts = self.interrupt_state(client_id);
        let session_replication_role = self.session_replication_role(client_id);

        match stmt {
            Statement::AlterTableMulti(ref statements) => {
                for sql in statements {
                    let substmt = crate::backend::parser::parse_statement(sql)?;
                    self.execute_statement_with_search_path_inner(
                        client_id,
                        substmt,
                        statement_lock_scope_id,
                        configured_search_path,
                        datetime_config,
                        gucs,
                        planner_config,
                        std::sync::Arc::clone(&random_state),
                    )?;
                }
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Do(ref do_stmt) => execute_do_with_gucs(do_stmt, gucs),
            Statement::SetConstraints(_) => {
                crate::backend::utils::misc::notices::push_warning(
                    "SET CONSTRAINTS can only be used in transaction blocks",
                );
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Checkpoint(_) => {
                let auth = self.auth_state(client_id);
                let auth_catalog = self.auth_catalog(client_id, None).map_err(|err| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "authorization catalog",
                        actual: format!("{err:?}"),
                    })
                })?;
                if !auth.has_effective_membership(
                    crate::include::catalog::PG_CHECKPOINT_OID,
                    &auth_catalog,
                ) {
                    return Err(ExecError::DetailedError {
                        message: "permission denied to execute CHECKPOINT command".into(),
                        detail: Some(
                            "Only roles with privileges of the \"pg_checkpoint\" role may execute this command."
                                .into(),
                        ),
                        hint: None,
                        sqlstate: "42501",
                    });
                }
                self.request_checkpoint(
                    crate::backend::access::transam::CheckpointRequestFlags::sql(),
                )?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Notify(ref notify_stmt) => self.execute_notify_stmt(client_id, notify_stmt),
            Statement::Listen(ref listen_stmt) => {
                Ok(self.execute_listen_stmt(client_id, listen_stmt))
            }
            Statement::Unlisten(ref unlisten_stmt) => {
                Ok(self.execute_unlisten_stmt(client_id, unlisten_stmt))
            }
            Statement::Load(_) | Statement::Discard(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::Analyze(ref analyze_stmt) => self.execute_analyze_stmt_with_search_path(
                client_id,
                analyze_stmt,
                configured_search_path,
            ),
            Statement::CreateIndex(ref create_stmt) => self
                .execute_create_index_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                    65_536,
                ),
            Statement::ReindexIndex(ref reindex_stmt) => self
                .execute_reindex_index_stmt_with_search_path(
                    client_id,
                    reindex_stmt,
                    configured_search_path,
                ),
            Statement::CreateStatistics(ref create_stmt) => self
                .execute_create_statistics_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterStatistics(ref alter_stmt) => self
                .execute_alter_statistics_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateTextSearchDictionary(ref create_stmt) => self
                .execute_create_text_search_dictionary_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterTextSearchDictionary(ref alter_stmt) => self
                .execute_alter_text_search_dictionary_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateTextSearchConfiguration(ref create_stmt) => self
                .execute_create_text_search_configuration_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterTextSearchConfiguration(ref alter_stmt) => self
                .execute_alter_text_search_configuration_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::DropTextSearchConfiguration(ref drop_stmt) => self
                .execute_drop_text_search_configuration_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropStatistics(ref drop_stmt) => self
                .execute_drop_statistics_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableOwner(ref alter_stmt) => self
                .execute_alter_table_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableRename(ref rename_stmt) => self
                .execute_alter_table_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetSchema(ref alter_stmt) => self
                .execute_alter_table_set_schema_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetTablespace(ref alter_stmt) => self
                .execute_alter_table_set_tablespace_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableReset(ref alter_stmt) => self
                .execute_alter_table_reset_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetPersistence(ref alter_stmt) => self
                .execute_alter_table_set_persistence_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexRename(ref rename_stmt) => self
                .execute_alter_index_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexAttachPartition(ref attach_stmt) => self
                .execute_alter_index_attach_partition_stmt_with_search_path(
                    client_id,
                    attach_stmt,
                    configured_search_path,
                ),
            Statement::AlterViewRename(ref rename_stmt) => self
                .execute_alter_view_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterViewRenameColumn(ref rename_stmt) => self
                .execute_alter_view_rename_column_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterViewSetSchema(ref alter_stmt) => self
                .execute_alter_view_set_schema_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterMaterializedViewSetSchema(ref alter_stmt) => self
                .execute_alter_materialized_view_set_schema_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterMaterializedViewSetAccessMethod(ref alter_stmt) => self
                .execute_alter_materialized_view_set_access_method_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexAlterColumnStatistics(ref alter_stmt) => self
                .execute_alter_index_alter_column_statistics_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexAlterColumnOptions(ref alter_stmt) => self
                .execute_alter_index_alter_column_options_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableCompound(ref compound_stmt) => {
                for action in &compound_stmt.actions {
                    self.execute_statement_with_search_path_inner(
                        client_id,
                        action.clone(),
                        statement_lock_scope_id,
                        configured_search_path,
                        datetime_config,
                        gucs,
                        planner_config,
                        std::sync::Arc::clone(&random_state),
                    )?;
                }
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::AlterViewOwner(ref alter_stmt) => self
                .execute_alter_view_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableRenameColumn(ref rename_stmt) => self
                .execute_alter_table_rename_column_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAddColumn(ref alter_stmt) => self
                .execute_alter_table_add_column_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAddColumns(ref alter_stmt) => {
                let mut result = Ok(StatementResult::AffectedRows(0));
                for column in &alter_stmt.columns {
                    result = self.execute_alter_table_add_column_stmt_with_search_path(
                        client_id,
                        &AlterTableAddColumnStatement {
                            if_exists: alter_stmt.if_exists,
                            missing_ok: false,
                            only: alter_stmt.only,
                            table_name: alter_stmt.table_name.clone(),
                            column: column.clone(),
                            fdw_options: None,
                        },
                        configured_search_path,
                    );
                    if result.is_err() {
                        break;
                    }
                }
                result
            }
            Statement::AlterTableDropColumn(ref drop_stmt) => self
                .execute_alter_table_drop_column_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnType(ref alter_stmt) => self
                .execute_alter_table_alter_column_type_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                    &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
                ),
            Statement::AlterTableAlterColumnDefault(ref alter_stmt) => self
                .execute_alter_table_alter_column_default_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnExpression(ref alter_stmt) => self
                .execute_alter_table_alter_column_expression_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnCompression(ref alter_stmt) => self
                .execute_alter_table_alter_column_compression_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnStorage(ref alter_stmt) => self
                .execute_alter_table_alter_column_storage_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnOptions(ref alter_stmt) => self
                .execute_alter_table_alter_column_options_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnStatistics(ref alter_stmt) => self
                .execute_alter_table_alter_column_statistics_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterColumnIdentity(ref alter_stmt) => self
                .execute_alter_table_alter_column_identity_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAddConstraint(ref alter_stmt) => self
                .execute_alter_table_add_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                    None,
                ),
            Statement::AlterTableDropConstraint(ref alter_stmt) => self
                .execute_alter_table_drop_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAlterConstraint(ref alter_stmt) => self
                .execute_alter_table_alter_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableRenameConstraint(ref alter_stmt) => self
                .execute_alter_table_rename_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetNotNull(ref alter_stmt) => self
                .execute_alter_table_set_not_null_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableDropNotNull(ref alter_stmt) => self
                .execute_alter_table_drop_not_null_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableValidateConstraint(ref alter_stmt) => self
                .execute_alter_table_validate_constraint_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableInherit(ref alter_stmt) => self
                .execute_alter_table_inherit_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableNoInherit(ref alter_stmt) => self
                .execute_alter_table_no_inherit_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableOf(ref alter_stmt) => self
                .execute_alter_table_of_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableNotOf(ref alter_stmt) => self
                .execute_alter_table_not_of_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableAttachPartition(ref alter_stmt) => self
                .execute_alter_table_attach_partition_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableDetachPartition(ref alter_stmt) => self
                .execute_alter_table_detach_partition_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSetRowSecurity(ref alter_stmt) => self
                .execute_alter_table_set_row_security_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableReplicaIdentity(ref alter_stmt) => self
                .execute_alter_table_replica_identity_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterPolicy(ref alter_stmt) => self
                .execute_alter_policy_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableSet(ref alter_stmt) => self
                .execute_alter_table_set_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterIndexSet(ref alter_stmt) => self
                .execute_alter_index_set_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::Show(_)
            | Statement::Set(_)
            | Statement::Reset(_)
            | Statement::Prepare(_)
            | Statement::Execute(_)
            | Statement::Deallocate(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::CreateRole(ref create_stmt) => {
                self.execute_create_role_stmt(client_id, create_stmt, None)
            }
            Statement::CreateDatabase(ref create_stmt) => {
                self.execute_create_database_stmt(client_id, create_stmt)
            }
            Statement::AlterDatabase(ref alter_stmt) => {
                self.execute_alter_database_stmt(client_id, alter_stmt)
            }
            Statement::AlterRole(ref alter_stmt) => {
                self.execute_alter_role_stmt(client_id, alter_stmt)
            }
            Statement::DropRole(ref drop_stmt) => self.execute_drop_role_stmt(client_id, drop_stmt),
            Statement::DropDatabase(ref drop_stmt) => {
                self.execute_drop_database_stmt(client_id, drop_stmt)
            }
            Statement::GrantObject(ref grant_stmt) => self
                .execute_grant_object_stmt_with_search_path(
                    client_id,
                    grant_stmt,
                    configured_search_path,
                ),
            Statement::RevokeObject(ref revoke_stmt) => self
                .execute_revoke_object_stmt_with_search_path(
                    client_id,
                    revoke_stmt,
                    configured_search_path,
                ),
            Statement::GrantRoleMembership(ref grant_stmt) => {
                self.execute_grant_role_membership_stmt(client_id, grant_stmt)
            }
            Statement::RevokeRoleMembership(ref revoke_stmt) => {
                self.execute_revoke_role_membership_stmt(client_id, revoke_stmt)
            }
            Statement::DropOwned(ref drop_stmt) => {
                self.execute_drop_owned_stmt(client_id, drop_stmt)
            }
            Statement::ReassignOwned(ref reassign_stmt) => {
                self.execute_reassign_owned_stmt(client_id, reassign_stmt)
            }
            Statement::CommentOnDatabase(ref comment_stmt) => {
                self.execute_comment_on_database_stmt(client_id, comment_stmt)
            }
            Statement::CommentOnRole(ref comment_stmt) => {
                self.execute_comment_on_role_stmt(client_id, comment_stmt)
            }
            Statement::SetSessionAuthorization(ref set_stmt) => {
                self.execute_set_session_authorization_stmt(client_id, set_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::ResetSessionAuthorization(ref reset_stmt) => {
                self.execute_reset_session_authorization_stmt(client_id, reset_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::SetRole(ref set_stmt) => {
                self.execute_set_role_stmt(client_id, set_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::ResetRole(ref reset_stmt) => {
                self.execute_reset_role_stmt(client_id, reset_stmt)?;
                Ok(StatementResult::AffectedRows(0))
            }
            Statement::Unsupported(ref unsupported_stmt) => {
                if let Some(result) = self.execute_object_address_unsupported_stmt(
                    client_id,
                    unsupported_stmt,
                    configured_search_path,
                )? {
                    return Ok(result);
                }
                if unsupported_stmt.feature == "ALTER TABLE form" {
                    let lower = unsupported_stmt.sql.to_ascii_lowercase();
                    if lower.contains(" set without oids") {
                        return Ok(StatementResult::AffectedRows(0));
                    }
                    if lower.contains(" set with oids") {
                        return Err(ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "valid ALTER TABLE form",
                            actual: "syntax error at or near \"WITH\"".into(),
                        }));
                    }
                }
                Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                    "{}: {}",
                    unsupported_stmt.feature, unsupported_stmt.sql
                ))))
            }
            Statement::Call(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "CALL execution".into(),
            ))),
            Statement::CopyFrom(_) | Statement::CopyTo(_) => {
                Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "COPY handled by session layer",
                    actual: "COPY".into(),
                }))
            }
            Statement::CreateFunction(ref create_stmt) => self
                .execute_create_function_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateProcedure(ref create_stmt) => self
                .execute_create_procedure_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateAggregate(ref create_stmt) => self
                .execute_create_aggregate_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterAggregateRename(ref rename_stmt) => self
                .execute_alter_aggregate_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::CreateCast(ref create_stmt) => self
                .execute_create_cast_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateOperator(ref create_stmt) => self
                .execute_create_operator_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateOperatorClass(ref create_stmt) => self
                .execute_create_operator_class_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateOperatorFamily(ref create_stmt) => self
                .execute_create_operator_family_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterOperatorFamily(ref alter_stmt) => self
                .execute_alter_operator_family_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterOperatorClass(ref alter_stmt) => self
                .execute_alter_operator_class_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::DropOperatorFamily(ref drop_stmt) => self
                .execute_drop_operator_family_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::CreateTextSearch(ref create_stmt) => self
                .execute_create_text_search_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterTextSearch(ref alter_stmt) => self
                .execute_alter_text_search_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateSchema(ref create_stmt) => self
                .execute_create_schema_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateTablespace(ref create_stmt) => {
                self.execute_create_tablespace_stmt(client_id, create_stmt, false)
            }
            Statement::AlterSchemaOwner(ref alter_stmt) => self
                .execute_alter_schema_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterSchemaRename(ref alter_stmt) => self
                .execute_alter_schema_rename_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterPublication(ref alter_stmt) => self
                .execute_alter_publication_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterOperator(ref alter_stmt) => self
                .execute_alter_operator_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterConversion(ref alter_stmt) => self
                .execute_alter_conversion_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterProcedure(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "ALTER PROCEDURE".into(),
            ))),
            Statement::AlterRoutine(ref alter_stmt) => self
                .execute_alter_routine_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateSequence(ref create_stmt) => self
                .execute_create_sequence_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::Merge(ref merge_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = crate::backend::parser::plan_merge(merge_stmt, &catalog)?;
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: None,
                    trigger_depth: 0,
                };
                let result = crate::backend::commands::tablecmds::execute_merge(
                    bound, &catalog, &mut ctx, xid, 0,
                );
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                result
            }
            Statement::CommentOnTable(ref comment_stmt) => self
                .execute_comment_on_table_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnColumn(ref comment_stmt) => self
                .execute_comment_on_column_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnView(ref comment_stmt) => self
                .execute_comment_on_view_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnIndex(ref comment_stmt) => self
                .execute_comment_on_index_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnType(ref comment_stmt) => self
                .execute_comment_on_type_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnAggregate(ref comment_stmt) => self
                .execute_comment_on_aggregate_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnFunction(ref comment_stmt) => self
                .execute_comment_on_function_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnOperator(ref comment_stmt) => self
                .execute_comment_on_operator_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnConstraint(ref comment_stmt) => self
                .execute_comment_on_constraint_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnRule(ref comment_stmt) => self
                .execute_comment_on_rule_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnTrigger(ref comment_stmt) => self
                .execute_comment_on_trigger_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnEventTrigger(ref comment_stmt) => self
                .execute_comment_on_event_trigger_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnDomain(ref comment_stmt) => self
                .execute_comment_on_domain_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnConversion(ref comment_stmt) => self
                .execute_comment_on_conversion_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnForeignDataWrapper(ref comment_stmt) => {
                self.execute_comment_on_foreign_data_wrapper_stmt(client_id, comment_stmt)
            }
            Statement::CommentOnForeignServer(ref comment_stmt) => {
                self.execute_comment_on_foreign_server_stmt(client_id, comment_stmt)
            }
            Statement::CommentOnPublication(ref comment_stmt) => self
                .execute_comment_on_publication_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CommentOnStatistics(ref comment_stmt) => self
                .execute_comment_on_statistics_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::CreateForeignDataWrapper(ref create_stmt) => self
                .execute_create_foreign_data_wrapper_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateForeignServer(ref create_stmt) => {
                self.execute_create_foreign_server_stmt(client_id, create_stmt)
            }
            Statement::CreateLanguage(ref create_stmt) => {
                self.execute_create_language_stmt(client_id, create_stmt)
            }
            Statement::AlterLanguage(ref alter_stmt) => {
                self.execute_alter_language_stmt(client_id, alter_stmt)
            }
            Statement::DropLanguage(ref drop_stmt) => {
                self.execute_drop_language_stmt(client_id, drop_stmt)
            }
            Statement::CreateUserMapping(ref create_stmt) => {
                self.execute_create_user_mapping_stmt(client_id, create_stmt)
            }
            Statement::CreateForeignTable(ref create_stmt) => {
                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let mut catalog_effects = Vec::new();
                let result = self
                    .execute_create_foreign_table_stmt_in_transaction_with_search_path(
                        client_id,
                        create_stmt,
                        xid,
                        0,
                        configured_search_path,
                        &mut catalog_effects,
                    );
                let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
                guard.disarm();
                result
            }
            Statement::ImportForeignSchema(ref import_stmt) => {
                self.execute_import_foreign_schema_stmt(client_id, import_stmt)
            }
            Statement::AlterForeignDataWrapper(ref alter_stmt) => self
                .execute_alter_foreign_data_wrapper_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterForeignDataWrapperOwner(ref alter_stmt) => {
                self.execute_alter_foreign_data_wrapper_owner_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignDataWrapperRename(ref alter_stmt) => {
                self.execute_alter_foreign_data_wrapper_rename_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignServer(ref alter_stmt) => {
                self.execute_alter_foreign_server_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignServerOwner(ref alter_stmt) => {
                self.execute_alter_foreign_server_owner_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignServerRename(ref alter_stmt) => {
                self.execute_alter_foreign_server_rename_stmt(client_id, alter_stmt)
            }
            Statement::AlterForeignTableOptions(ref alter_stmt) => self
                .execute_alter_foreign_table_options_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterUserMapping(ref alter_stmt) => {
                self.execute_alter_user_mapping_stmt(client_id, alter_stmt)
            }
            Statement::DropForeignDataWrapper(ref drop_stmt) => {
                self.execute_drop_foreign_data_wrapper_stmt(client_id, drop_stmt)
            }
            Statement::DropForeignServer(ref drop_stmt) => {
                self.execute_drop_foreign_server_stmt(client_id, drop_stmt)
            }
            Statement::DropUserMapping(ref drop_stmt) => {
                self.execute_drop_user_mapping_stmt(client_id, drop_stmt)
            }
            Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) => {
                let visible_catalog =
                    self.lazy_catalog_lookup(client_id, None, configured_search_path);
                if restrict_nonsystem_view_enabled(gucs) {
                    match &stmt {
                        Statement::Select(select) => {
                            reject_restricted_views_in_select(select, &visible_catalog)?;
                        }
                        Statement::Explain(explain) => {
                            if let Statement::Select(select) = explain.statement.as_ref() {
                                reject_restricted_views_in_select(select, &visible_catalog)?;
                            }
                        }
                        _ => {}
                    }
                }
                let (stmt, planned_select, planned_select_for_update, rels) = {
                    let mut rels = std::collections::BTreeSet::new();
                    let mut planned_select = None;
                    let mut planned_select_for_update = false;
                    match &stmt {
                        Statement::Select(select) => {
                            let planned_stmt = crate::backend::parser::pg_plan_query_with_config(
                                select,
                                &visible_catalog,
                                planner_config,
                            )?;
                            collect_rels_from_planned_stmt(&planned_stmt, &mut rels);
                            planned_select_for_update = select.locking_clause.is_some();
                            planned_select = Some(planned_stmt);
                        }
                        Statement::Values(_) => {}
                        Statement::Explain(explain) => {
                            if let Statement::Select(select) = explain.statement.as_ref() {
                                let planned_stmt =
                                    crate::backend::parser::pg_plan_query_with_config(
                                        select,
                                        &visible_catalog,
                                        planner_config,
                                    )?;
                                collect_rels_from_planned_stmt(&planned_stmt, &mut rels);
                            }
                        }
                        _ => unreachable!(),
                    }
                    (
                        stmt,
                        planned_select,
                        planned_select_for_update,
                        rels.into_iter().collect::<Vec<_>>(),
                    )
                };

                lock_relations_interruptible(
                    &self.table_locks,
                    client_id,
                    &rels,
                    interrupts.as_ref(),
                )?;

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let transaction_state: SharedExecutorTransactionState =
                    Arc::new(parking_lot::Mutex::new(ExecutorTransactionState {
                        xid: None,
                        cid: 0,
                        transaction_snapshot: None,
                    }));
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    transaction_state: Some(Arc::clone(&transaction_state)),
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(
                        visible_catalog.clone(),
                    )),
                    scalar_function_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                    trigger_depth: 0,
                };
                let result = match planned_select {
                    Some(planned_stmt) => {
                        if planned_select_for_update {
                            check_planned_stmt_select_for_update_privileges(&planned_stmt, &ctx)?;
                        } else {
                            check_planned_stmt_select_privileges(&planned_stmt, &ctx)?;
                        }
                        execute_planned_stmt(planned_stmt, &mut ctx)
                    }
                    None => execute_readonly_statement_with_config(
                        stmt,
                        &visible_catalog,
                        &mut ctx,
                        planner_config,
                    ),
                };
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                let mut catalog_effects = std::mem::take(&mut ctx.catalog_effects);
                let temp_effects = std::mem::take(&mut ctx.temp_effects);
                let pending_catalog_effects = std::mem::take(&mut ctx.pending_catalog_effects);
                let pending_table_locks = std::mem::take(&mut ctx.pending_table_locks);
                catalog_effects.extend(pending_catalog_effects);
                drop(ctx);
                let xid = transaction_state.lock().xid;
                let result = if let Some(xid) = xid {
                    let validation_catalog =
                        self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                    let result = result.and_then(|result| {
                        crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                            self,
                            client_id,
                            &validation_catalog,
                            xid,
                            1,
                            Arc::clone(&interrupts),
                            datetime_config,
                            &deferred_foreign_keys,
                        )?;
                        Ok(result)
                    });
                    self.finish_txn_with_async_notifications(
                        client_id,
                        xid,
                        result,
                        &catalog_effects,
                        &temp_effects,
                        &[],
                        pending_async_notifications,
                    )
                } else {
                    if result.is_ok() {
                        self.async_notify_runtime
                            .publish(client_id, &pending_async_notifications);
                    }
                    result
                };

                unlock_relations(&self.table_locks, client_id, &pending_table_locks);
                unlock_relations(&self.table_locks, client_id, &rels);
                result
            }
            Statement::Insert(ref insert_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                if restrict_nonsystem_view_enabled(gucs) {
                    reject_restricted_views_in_insert(insert_stmt, &catalog)?;
                }
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                    trigger_depth: 0,
                };
                let mut locked_rels = Vec::new();
                let result = (|| {
                    let has_writable_ctes = insert_stmt
                        .with
                        .iter()
                        .any(|cte| matches!(cte.body, CteBody::Insert(_)));
                    if !has_writable_ctes {
                        let bound = bind_insert(insert_stmt, &catalog)?;
                        let prepared =
                            super::rules::prepare_bound_insert_for_execution(bound, &catalog)?;
                        let lock_requests = merge_table_lock_requests(
                            &insert_foreign_key_lock_requests(&prepared.stmt),
                            &prepared.extra_lock_requests,
                        );
                        crate::backend::storage::lmgr::lock_table_requests_interruptible(
                            &self.table_locks,
                            client_id,
                            &lock_requests,
                            interrupts.as_ref(),
                        )?;
                        locked_rels.extend(table_lock_relations(&lock_requests));
                        return super::rules::execute_bound_insert_with_rules(
                            prepared.stmt,
                            &catalog,
                            &mut ctx,
                            xid,
                            0,
                        );
                    }

                    if insert_stmt.with_recursive {
                        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                            "WITH RECURSIVE containing data-modifying statements is not supported"
                                .into(),
                        )));
                    }

                    let mut materialized_ctes = Vec::new();
                    let mut outer_insert = insert_stmt.clone();
                    outer_insert.with.clear();

                    for cte in &insert_stmt.with {
                        let CteBody::Insert(cte_insert) = &cte.body else {
                            outer_insert.with.push(cte.clone());
                            continue;
                        };
                        if cte_insert.with_recursive
                            || cte_insert
                                .with
                                .iter()
                                .any(|nested| matches!(nested.body, CteBody::Insert(_)))
                        {
                            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                                "nested writable CTEs are not supported".into(),
                            )));
                        }
                        if cte_insert.returning.is_empty() {
                            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                                "writable CTE without RETURNING is not supported".into(),
                            )));
                        }

                        let bound = bind_insert_with_outer_scopes_and_ctes(
                            cte_insert,
                            &catalog,
                            &[],
                            &materialized_ctes,
                        )?;
                        let prepared =
                            super::rules::prepare_bound_insert_for_execution(bound, &catalog)?;
                        let lock_requests = merge_table_lock_requests(
                            &insert_foreign_key_lock_requests(&prepared.stmt),
                            &prepared.extra_lock_requests,
                        );
                        crate::backend::storage::lmgr::lock_table_requests_interruptible(
                            &self.table_locks,
                            client_id,
                            &lock_requests,
                            interrupts.as_ref(),
                        )?;
                        locked_rels.extend(table_lock_relations(&lock_requests));
                        let result = super::rules::execute_bound_insert_with_rules(
                            prepared.stmt,
                            &catalog,
                            &mut ctx,
                            xid,
                            0,
                        )?;
                        let StatementResult::Query { columns, rows, .. } = result else {
                            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                                "writable CTE without RETURNING is not supported".into(),
                            )));
                        };
                        let columns = apply_writable_cte_column_aliases(cte, columns)?;
                        materialized_ctes.push(bound_cte_from_query_rows(
                            cte.name.clone(),
                            columns,
                            &rows,
                        ));
                    }

                    let bound = bind_insert_with_outer_scopes_and_ctes(
                        &outer_insert,
                        &catalog,
                        &[],
                        &materialized_ctes,
                    )?;
                    let prepared =
                        super::rules::prepare_bound_insert_for_execution(bound, &catalog)?;
                    let lock_requests = merge_table_lock_requests(
                        &insert_foreign_key_lock_requests(&prepared.stmt),
                        &prepared.extra_lock_requests,
                    );
                    crate::backend::storage::lmgr::lock_table_requests_interruptible(
                        &self.table_locks,
                        client_id,
                        &lock_requests,
                        interrupts.as_ref(),
                    )?;
                    locked_rels.extend(table_lock_relations(&lock_requests));
                    super::rules::execute_bound_insert_with_rules(
                        prepared.stmt,
                        &catalog,
                        &mut ctx,
                        xid,
                        0,
                    )
                })();
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::Update(ref update_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_update(update_stmt, &catalog)?;
                let prepared = super::rules::prepare_bound_update_for_execution(bound, &catalog)?;
                let lock_requests = merge_table_lock_requests(
                    &update_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                crate::backend::storage::lmgr::lock_table_requests_interruptible(
                    &self.table_locks,
                    client_id,
                    &lock_requests,
                    interrupts.as_ref(),
                )?;
                let locked_rels = table_lock_relations(&lock_requests);

                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                    trigger_depth: 0,
                };
                let result = super::rules::execute_bound_update_with_rules(
                    prepared.stmt,
                    &catalog,
                    &mut ctx,
                    xid,
                    0,
                    Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                );
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::Delete(ref delete_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_delete(delete_stmt, &catalog)?;
                let prepared = super::rules::prepare_bound_delete_for_execution(bound, &catalog)?;
                let lock_requests = merge_table_lock_requests(
                    &delete_foreign_key_lock_requests(&prepared.stmt),
                    &prepared.extra_lock_requests,
                );
                crate::backend::storage::lmgr::lock_table_requests_interruptible(
                    &self.table_locks,
                    client_id,
                    &lock_requests,
                    interrupts.as_ref(),
                )?;
                let locked_rels = table_lock_relations(&lock_requests);

                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let deferred_foreign_keys =
                    crate::backend::executor::DeferredForeignKeyTracker::default();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: Some(deferred_foreign_keys.clone()),
                    trigger_depth: 0,
                };
                let result = super::rules::execute_bound_delete_with_rules(
                    prepared.stmt,
                    &catalog,
                    &mut ctx,
                    xid,
                    Some((&self.txns, &self.txn_waiter, interrupts.as_ref())),
                );
                let pending_async_notifications =
                    std::mem::take(&mut ctx.pending_async_notifications);
                drop(ctx);
                let validation_catalog =
                    self.lazy_catalog_lookup(client_id, Some((xid, 1)), configured_search_path);
                let result = result.and_then(|result| {
                    crate::pgrust::database::foreign_keys::validate_deferred_foreign_key_constraints(
                        self,
                        client_id,
                        &validation_catalog,
                        xid,
                        1,
                        Arc::clone(&interrupts),
                        datetime_config,
                        &deferred_foreign_keys,
                    )?;
                    Ok(result)
                });
                let result = self.finish_txn_with_async_notifications(
                    client_id,
                    xid,
                    result,
                    &[],
                    &[],
                    &[],
                    pending_async_notifications,
                );
                guard.disarm();
                unlock_relations(&self.table_locks, client_id, &locked_rels);
                result
            }
            Statement::CreateTable(ref create_stmt) => self
                .execute_create_table_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateDomain(ref create_stmt) => self
                .execute_create_domain_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterDomain(ref alter_stmt) => self
                .execute_alter_domain_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateConversion(ref create_stmt) => self
                .execute_create_conversion_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateCollation(ref create_stmt) => self
                .execute_create_collation_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreatePublication(ref create_stmt) => self
                .execute_create_publication_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateTrigger(ref create_stmt) => self
                .execute_create_trigger_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateEventTrigger(ref create_stmt) => self
                .execute_create_event_trigger_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::AlterTableTriggerState(ref alter_stmt) => self
                .execute_alter_table_trigger_state_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterEventTrigger(ref alter_stmt) => self
                .execute_alter_event_trigger_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterEventTriggerOwner(ref alter_stmt) => self
                .execute_alter_event_trigger_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterTriggerRename(ref alter_stmt) => self
                .execute_alter_trigger_rename_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterEventTriggerRename(ref alter_stmt) => self
                .execute_alter_event_trigger_rename_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreatePolicy(ref create_stmt) => self
                .execute_create_policy_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateType(ref create_stmt) => self
                .execute_create_type_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::DropCast(ref drop_stmt) => self.execute_drop_cast_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::AlterType(ref alter_stmt) => self.execute_alter_type_stmt_with_search_path(
                client_id,
                alter_stmt,
                configured_search_path,
            ),
            Statement::AlterTypeOwner(ref alter_stmt) => self
                .execute_alter_type_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::CreateView(ref create_stmt) => self
                .execute_create_view_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateRule(ref create_stmt) => self
                .execute_create_rule_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                ),
            Statement::CreateTableAs(ref create_stmt) => self
                .execute_create_table_as_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    None,
                    0,
                    configured_search_path,
                    planner_config,
                ),
            Statement::RefreshMaterializedView(ref refresh_stmt) => self
                .execute_refresh_materialized_view_stmt_with_search_path(
                    client_id,
                    refresh_stmt,
                    None,
                    0,
                    configured_search_path,
                ),
            Statement::Cluster(ref cluster_stmt) => self.execute_cluster_stmt_with_search_path(
                client_id,
                cluster_stmt,
                configured_search_path,
            ),
            Statement::DropTable(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let result = self.execute_drop_table_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                    &mut temp_effects,
                );
                let result =
                    self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
                guard.disarm();
                result
            }
            Statement::DropIndex(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let mut catalog_effects = Vec::new();
                let result = self.execute_drop_index_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                );
                let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
                guard.disarm();
                result
            }
            Statement::DropDomain(ref drop_stmt) => self.execute_drop_domain_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::DropFunction(ref drop_stmt) => self
                .execute_drop_function_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropProcedure(ref drop_stmt) => self
                .execute_drop_procedure_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropRoutine(ref drop_stmt) => self
                .execute_drop_routine_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropAggregate(ref drop_stmt) => self
                .execute_drop_aggregate_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropOperator(ref drop_stmt) => self
                .execute_drop_operator_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropConversion(ref drop_stmt) => self
                .execute_drop_conversion_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropCollation(ref drop_stmt) => self
                .execute_drop_collation_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropPublication(ref drop_stmt) => self
                .execute_drop_publication_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropTrigger(ref drop_stmt) => self
                .execute_drop_trigger_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropEventTrigger(ref drop_stmt) => self
                .execute_drop_event_trigger_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    configured_search_path,
                ),
            Statement::DropPolicy(ref drop_stmt) => self.execute_drop_policy_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::DropType(ref drop_stmt) => self.execute_drop_type_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::DropSequence(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let mut sequence_effects = Vec::new();
                let result = self.execute_drop_sequence_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                    &mut temp_effects,
                    &mut sequence_effects,
                );
                let result = self.finish_txn(
                    client_id,
                    xid,
                    result,
                    &catalog_effects,
                    &temp_effects,
                    &sequence_effects,
                );
                guard.disarm();
                result
            }
            Statement::DropView(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let result = self.execute_drop_view_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                    &mut temp_effects,
                );
                let result =
                    self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
                guard.disarm();
                result
            }
            Statement::DropMaterializedView(ref drop_stmt) => self
                .execute_drop_materialized_view_stmt_with_search_path(
                    client_id,
                    drop_stmt,
                    None,
                    0,
                    configured_search_path,
                ),
            Statement::DropRule(ref drop_stmt) => self.execute_drop_rule_stmt_with_search_path(
                client_id,
                drop_stmt,
                configured_search_path,
            ),
            Statement::DropSchema(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard =
                    AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
                let mut catalog_effects = Vec::new();
                let result = self.execute_drop_schema_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                );
                let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
                guard.disarm();
                result
            }
            Statement::AlterSequence(ref alter_stmt) => self
                .execute_alter_sequence_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterSequenceOwner(ref alter_stmt) => self
                .execute_alter_sequence_owner_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::AlterSequenceRename(ref rename_stmt) => self
                .execute_alter_sequence_rename_stmt_with_search_path(
                    client_id,
                    rename_stmt,
                    configured_search_path,
                ),
            Statement::TruncateTable(ref truncate_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let mut relations = Vec::new();
                for name in &truncate_stmt.table_names {
                    let Some(entry) = catalog.lookup_any_relation(name) else {
                        continue;
                    };
                    if !relations
                        .iter()
                        .any(|existing: &crate::backend::parser::BoundRelation| {
                            existing.relation_oid == entry.relation_oid
                        })
                    {
                        relations.push(entry.clone());
                    }
                    if entry.relkind == 'p' {
                        for target in partitioned_truncate_targets(&catalog, entry.relation_oid) {
                            if relations.iter().any(
                                |existing: &crate::backend::parser::BoundRelation| {
                                    existing.relation_oid == target.relation_oid
                                },
                            ) {
                                continue;
                            }
                            relations.push(target);
                        }
                    }
                }
                let target_relation_oids = relations
                    .iter()
                    .map(|relation| relation.relation_oid)
                    .collect::<Vec<_>>();
                for relation in &relations {
                    reject_relation_with_referencing_foreign_keys_except(
                        &catalog,
                        relation.relation_oid,
                        &target_relation_oids,
                        "TRUNCATE on table without referencing foreign keys",
                    )?;
                }
                let rels = relations
                    .iter()
                    .map(|relation| relation.rel)
                    .collect::<Vec<_>>();
                lock_tables_interruptible(
                    &self.table_locks,
                    client_id,
                    &rels,
                    TableLockMode::AccessExclusive,
                    interrupts.as_ref(),
                )?;

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    data_dir: Some(self.cluster.base_dir.clone()),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    lock_status_provider: Some(std::sync::Arc::new(self.clone())),
                    sequences: Some(self.sequences.clone()),
                    large_objects: Some(self.large_objects.clone()),
                    stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
                    async_notify_runtime: Some(self.async_notify_runtime.clone()),
                    advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                    row_locks: std::sync::Arc::clone(&self.row_locks),
                    checkpoint_stats: self.checkpoint_stats_snapshot(),
                    datetime_config: datetime_config.clone(),
                    statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
                    gucs: gucs.clone(),
                    interrupts: Arc::clone(&interrupts),
                    stats: std::sync::Arc::clone(&self.stats),
                    session_stats: self.session_stats_state(client_id),
                    snapshot,
                    transaction_state: None,
                    client_id,
                    current_database_name: self.current_database_name(),
                    session_user_oid: self.auth_state(client_id).session_user_oid(),
                    current_user_oid: self.auth_state(client_id).current_user_oid(),
                    active_role_oid: self.auth_state(client_id).active_role_oid(),
                    session_replication_role,
                    statement_lock_scope_id,
                    transaction_lock_scope_id: None,
                    next_command_id: 0,
                    default_toast_compression:
                        crate::include::access::htup::AttributeCompression::Pglz,
                    random_state: std::sync::Arc::clone(&random_state),
                    expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                    case_test_values: Vec::new(),
                    system_bindings: Vec::new(),
                    subplans: Vec::new(),
                    timed: false,
                    allow_side_effects: true,
                    pending_async_notifications: Vec::new(),
                    catalog_effects: Vec::new(),
                    temp_effects: Vec::new(),
                    database: Some(self.clone()),
                    pending_catalog_effects: Vec::new(),
                    pending_table_locks: Vec::new(),
                    catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    scalar_function_cache: std::collections::HashMap::new(),
                    plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                    pinned_cte_tables: std::collections::HashMap::new(),
                    cte_tables: std::collections::HashMap::new(),
                    cte_producers: std::collections::HashMap::new(),
                    recursive_worktables: std::collections::HashMap::new(),
                    deferred_foreign_keys: None,
                    trigger_depth: 0,
                };
                let result = execute_truncate_table(
                    truncate_stmt.clone(),
                    &catalog,
                    &mut ctx,
                    INVALID_TRANSACTION_ID,
                );
                drop(ctx);
                for rel in rels {
                    self.table_locks.unlock_table(rel, client_id);
                }
                result
            }
            Statement::Vacuum(ref vacuum_stmt) => self.execute_vacuum_stmt_with_search_path(
                client_id,
                vacuum_stmt,
                configured_search_path,
                Some(gucs),
            ),
            Statement::SetTransaction(_)
            | Statement::Begin(_)
            | Statement::Commit
            | Statement::Rollback
            | Statement::Savepoint(_)
            | Statement::ReleaseSavepoint(_)
            | Statement::RollbackTo(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::DeclareCursor(_)
            | Statement::Fetch(_)
            | Statement::Move(_)
            | Statement::ClosePortal(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "session command handled by session layer",
                actual: "session command".into(),
            })),
        }
    }

    pub fn execute_streaming(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
    ) -> Result<SelectGuard, ExecError> {
        self.execute_streaming_with_search_path_and_datetime_config(
            client_id,
            select_stmt,
            txn_ctx,
            None,
            None,
            None,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_streaming_with_search_path(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        configured_search_path: Option<&[String]>,
    ) -> Result<SelectGuard, ExecError> {
        self.execute_streaming_with_search_path_and_datetime_config(
            client_id,
            select_stmt,
            txn_ctx,
            None,
            None,
            configured_search_path,
            &DateTimeConfig::default(),
        )
    }

    pub(crate) fn execute_streaming_with_search_path_and_datetime_config(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        statement_lock_scope_id: Option<u64>,
        transaction_lock_scope_id: Option<u64>,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
    ) -> Result<SelectGuard, ExecError> {
        self.execute_streaming_with_config(
            client_id,
            select_stmt,
            txn_ctx,
            statement_lock_scope_id,
            transaction_lock_scope_id,
            configured_search_path,
            datetime_config,
            &std::collections::HashMap::new(),
            None,
            PlannerConfig::default(),
        )
    }

    pub(crate) fn execute_streaming_with_config(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        statement_lock_scope_id: Option<u64>,
        transaction_lock_scope_id: Option<u64>,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        snapshot_override: Option<crate::backend::access::transam::xact::Snapshot>,
        planner_config: PlannerConfig,
    ) -> Result<SelectGuard, ExecError> {
        self.execute_streaming_with_config_and_random_state(
            client_id,
            select_stmt,
            txn_ctx,
            statement_lock_scope_id,
            transaction_lock_scope_id,
            configured_search_path,
            datetime_config,
            gucs,
            snapshot_override,
            planner_config,
            crate::backend::executor::PgPrngState::shared(),
        )
    }

    pub(crate) fn execute_streaming_with_config_and_random_state(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        statement_lock_scope_id: Option<u64>,
        transaction_lock_scope_id: Option<u64>,
        configured_search_path: Option<&[String]>,
        datetime_config: &DateTimeConfig,
        gucs: &std::collections::HashMap<String, String>,
        snapshot_override: Option<crate::backend::access::transam::xact::Snapshot>,
        planner_config: PlannerConfig,
        random_state: std::sync::Arc<parking_lot::Mutex<crate::backend::executor::PgPrngState>>,
    ) -> Result<SelectGuard, ExecError> {
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::executor::executor_start;

        let visible_catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let visible_catalog_snapshot = Some(crate::backend::executor::executor_catalog(
            visible_catalog.clone(),
        ));
        let (query_desc, rels) = {
            let query_desc = crate::backend::executor::create_query_desc(
                crate::backend::parser::pg_plan_query_with_config(
                    select_stmt,
                    &visible_catalog,
                    planner_config,
                )?,
                None,
            );
            let mut rels = std::collections::BTreeSet::new();
            collect_rels_from_planned_stmt(&query_desc.planned_stmt, &mut rels);
            (query_desc, rels.into_iter().collect::<Vec<_>>())
        };
        let privilege_planned_stmt = query_desc.planned_stmt.clone();

        let transaction_snapshot = snapshot_override.clone();
        let (snapshot, command_id) = match (snapshot_override, txn_ctx) {
            (Some(snapshot), Some((_xid, cid))) => (snapshot, cid),
            (Some(snapshot), None) => {
                let cid = snapshot.current_cid;
                (snapshot, cid)
            }
            (None, Some((xid, cid))) => (self.txns.read().snapshot_for_command(xid, cid)?, cid),
            (None, None) => (self.txns.read().snapshot(INVALID_TRANSACTION_ID)?, 0),
        };
        let transaction_state: SharedExecutorTransactionState =
            std::sync::Arc::new(parking_lot::Mutex::new(ExecutorTransactionState {
                xid: (snapshot.current_xid != INVALID_TRANSACTION_ID)
                    .then_some(snapshot.current_xid),
                cid: command_id,
                transaction_snapshot,
            }));
        let columns = query_desc.columns();
        let column_names = query_desc.column_names();
        let state = executor_start(query_desc.planned_stmt.plan_tree);
        let interrupts = self.interrupt_state(client_id);
        let session_replication_role = self.session_replication_role(client_id);
        let ctx = ExecutorContext {
            pool: std::sync::Arc::clone(&self.pool),
            data_dir: Some(self.cluster.base_dir.clone()),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(std::sync::Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            stats_import_runtime: Some(std::sync::Arc::new(self.clone())),
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
            row_locks: std::sync::Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: datetime_config.clone(),
            statement_timestamp_usecs: statement_timestamp_usecs(datetime_config),
            gucs: gucs.clone(),
            interrupts,
            stats: std::sync::Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            transaction_state: Some(transaction_state),
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role,
            statement_lock_scope_id,
            transaction_lock_scope_id,
            next_command_id: command_id,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            random_state,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: query_desc.planned_stmt.subplans,
            timed: false,
            allow_side_effects: true,
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: Some(self.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            catalog: visible_catalog_snapshot,
            scalar_function_cache: std::collections::HashMap::new(),
            plpgsql_function_cache: self.plpgsql_function_cache(client_id),
            pinned_cte_tables: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: Some(
                crate::backend::executor::DeferredForeignKeyTracker::default(),
            ),
            trigger_depth: 0,
        };
        if select_stmt.locking_clause.is_some() {
            crate::backend::commands::tablecmds::check_planned_stmt_select_for_update_privileges(
                &privilege_planned_stmt,
                &ctx,
            )?;
        } else {
            crate::backend::commands::tablecmds::check_planned_stmt_select_privileges(
                &privilege_planned_stmt,
                &ctx,
            )?;
        }
        lock_relations_interruptible(&self.table_locks, client_id, &rels, ctx.interrupts.as_ref())?;

        Ok(SelectGuard {
            state,
            ctx,
            columns,
            column_names,
            rels,
            table_locks: std::sync::Arc::clone(&self.table_locks),
            client_id,
            advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
            row_locks: std::sync::Arc::clone(&self.row_locks),
            statement_lock_scope_id,
            interrupt_guard: None,
            catalog_effect_start: 0,
            base_command_id: command_id,
        })
    }
}

fn partitioned_truncate_targets(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    root_oid: u32,
) -> Vec<crate::backend::parser::BoundRelation> {
    catalog
        .find_all_inheritors(root_oid)
        .into_iter()
        .filter(|oid| *oid != root_oid)
        .filter_map(|oid| catalog.relation_by_oid(oid))
        .filter(|entry| entry.relkind == 'r')
        .collect()
}
