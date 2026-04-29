use super::super::*;
use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::commands::tablecmds::collect_matching_rows_heap;
use crate::backend::executor::{ExecutorContext, eval_expr};
use crate::backend::parser::{
    AlterDomainAction, AlterDomainStatement, CatalogLookup, DomainConstraintSpec,
    DomainConstraintSpecKind, ParseError, SqlType, bind_expr_with_outer_and_ctes, parse_expr,
    scope_for_relation,
};
use crate::backend::utils::cache::lsyscache::relation_entry_by_oid;
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::PgClassRow;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::TupleSlot;
use crate::include::nodes::primnodes::{RelationDesc, ToastRelationRef};
use crate::pgrust::database::ddl::map_catalog_error;
use crate::pgrust::database::{DomainConstraintEntry, DomainConstraintKind, DomainEntry};

impl Database {
    pub(crate) fn execute_alter_domain_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterDomainStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        match &stmt.action {
            AlterDomainAction::SetDefault { default } => {
                let key = self.resolve_domain_key(&stmt.domain_name, configured_search_path)?;
                let mut domains = self.domains.write();
                let domain = domains
                    .get_mut(&key)
                    .ok_or_else(|| domain_does_not_exist_error(&stmt.domain_name))?;
                domain.default = default.clone();
            }
            AlterDomainAction::SetNotNull => {
                self.alter_domain_set_not_null(client_id, stmt, configured_search_path)?;
            }
            AlterDomainAction::DropNotNull => {
                let key = self.resolve_domain_key(&stmt.domain_name, configured_search_path)?;
                let mut domains = self.domains.write();
                let Some(domain) = domains.get_mut(&key) else {
                    return Err(domain_does_not_exist_error(&stmt.domain_name));
                };
                domain
                    .constraints
                    .retain(|constraint| !matches!(constraint.kind, DomainConstraintKind::NotNull));
                refresh_domain_legacy_fields(domain);
            }
            AlterDomainAction::AddConstraint(spec) => {
                self.alter_domain_add_constraint(client_id, stmt, spec, configured_search_path)?;
            }
            AlterDomainAction::DropConstraint {
                constraint_name,
                if_exists,
                ..
            } => {
                self.alter_domain_drop_constraint(
                    stmt,
                    constraint_name,
                    *if_exists,
                    configured_search_path,
                )?;
            }
            AlterDomainAction::ValidateConstraint { constraint_name } => {
                self.alter_domain_validate_constraint(
                    client_id,
                    stmt,
                    constraint_name,
                    configured_search_path,
                )?;
            }
            AlterDomainAction::RenameDomain { new_name } => {
                self.alter_domain_rename(stmt, new_name, configured_search_path)?;
            }
            AlterDomainAction::RenameConstraint {
                constraint_name,
                new_name,
            } => {
                self.alter_domain_rename_constraint(
                    stmt,
                    constraint_name,
                    new_name,
                    configured_search_path,
                )?;
            }
            AlterDomainAction::SetSchema { new_schema } => {
                self.alter_domain_set_schema(client_id, stmt, new_schema, configured_search_path)?;
            }
            AlterDomainAction::OwnerTo { new_owner } => {
                self.alter_domain_owner_to(client_id, stmt, new_owner, configured_search_path)?;
            }
        }

        self.refresh_catalog_store_dynamic_type_rows(client_id, configured_search_path);
        self.invalidate_backend_cache_state(client_id);
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    fn resolve_domain_key(
        &self,
        domain_name: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<String, ExecError> {
        let (normalized, _, _) =
            self.normalize_domain_name_for_create(0, domain_name, configured_search_path)?;
        Ok(normalized)
    }

    fn alter_domain_set_not_null(
        &self,
        client_id: ClientId,
        stmt: &AlterDomainStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let key = self.resolve_domain_key(&stmt.domain_name, configured_search_path)?;
        let domain = self
            .domains
            .read()
            .get(&key)
            .cloned()
            .ok_or_else(|| domain_does_not_exist_error(&stmt.domain_name))?;
        if domain.not_null {
            return Ok(());
        }
        let name = choose_domain_constraint_name(&domain, &format!("{}_not_null", domain.name));
        let constraint = DomainConstraintEntry {
            oid: self.allocate_dynamic_type_oids(1, None, None)?,
            name,
            kind: DomainConstraintKind::NotNull,
            expr: None,
            validated: true,
            enforced: true,
        };
        self.validate_domain_constraint_existing_values(
            client_id,
            &domain,
            &constraint,
            configured_search_path,
        )?;
        let mut domains = self.domains.write();
        let Some(domain) = domains.get_mut(&key) else {
            return Err(domain_does_not_exist_error(&stmt.domain_name));
        };
        domain.constraints.push(constraint);
        refresh_domain_legacy_fields(domain);
        Ok(())
    }

    fn alter_domain_add_constraint(
        &self,
        client_id: ClientId,
        stmt: &AlterDomainStatement,
        spec: &DomainConstraintSpec,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let key = self.resolve_domain_key(&stmt.domain_name, configured_search_path)?;
        let domain = self
            .domains
            .read()
            .get(&key)
            .cloned()
            .ok_or_else(|| domain_does_not_exist_error(&stmt.domain_name))?;
        if matches!(spec.kind, DomainConstraintSpecKind::NotNull) && domain.not_null {
            return Ok(());
        }
        let base_name = match &spec.kind {
            DomainConstraintSpecKind::Check { .. } => format!("{}_check", domain.name),
            DomainConstraintSpecKind::NotNull => format!("{}_not_null", domain.name),
        };
        let name = match &spec.name {
            Some(name) => {
                if domain
                    .constraints
                    .iter()
                    .any(|constraint| constraint.name.eq_ignore_ascii_case(name))
                {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "constraint \"{}\" for domain \"{}\" already exists",
                            name, domain.name
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42710",
                    });
                }
                name.clone()
            }
            None => choose_domain_constraint_name(&domain, &base_name),
        };
        let constraint = DomainConstraintEntry {
            oid: self.allocate_dynamic_type_oids(1, None, None)?,
            name,
            kind: match spec.kind {
                DomainConstraintSpecKind::Check { .. } => DomainConstraintKind::Check,
                DomainConstraintSpecKind::NotNull => DomainConstraintKind::NotNull,
            },
            expr: match &spec.kind {
                DomainConstraintSpecKind::Check { expr } => Some(expr.clone()),
                DomainConstraintSpecKind::NotNull => None,
            },
            validated: !spec.not_valid,
            enforced: true,
        };
        if !spec.not_valid {
            self.validate_domain_constraint_existing_values(
                client_id,
                &domain,
                &constraint,
                configured_search_path,
            )?;
        }
        let mut domains = self.domains.write();
        let Some(domain) = domains.get_mut(&key) else {
            return Err(domain_does_not_exist_error(&stmt.domain_name));
        };
        domain.constraints.push(constraint);
        refresh_domain_legacy_fields(domain);
        Ok(())
    }

    fn alter_domain_drop_constraint(
        &self,
        stmt: &AlterDomainStatement,
        constraint_name: &str,
        if_exists: bool,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let key = self.resolve_domain_key(&stmt.domain_name, configured_search_path)?;
        let mut domains = self.domains.write();
        let Some(domain) = domains.get_mut(&key) else {
            return Err(domain_does_not_exist_error(&stmt.domain_name));
        };
        let Some(index) = domain
            .constraints
            .iter()
            .position(|constraint| constraint.name.eq_ignore_ascii_case(constraint_name))
        else {
            if if_exists {
                push_notice(format!(
                    "constraint \"{}\" of domain \"{}\" does not exist, skipping",
                    constraint_name, domain.name
                ));
                return Ok(());
            }
            return Err(domain_constraint_does_not_exist_error(
                constraint_name,
                &domain.name,
            ));
        };
        domain.constraints.remove(index);
        refresh_domain_legacy_fields(domain);
        Ok(())
    }

    fn alter_domain_validate_constraint(
        &self,
        client_id: ClientId,
        stmt: &AlterDomainStatement,
        constraint_name: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let key = self.resolve_domain_key(&stmt.domain_name, configured_search_path)?;
        let domain = self
            .domains
            .read()
            .get(&key)
            .cloned()
            .ok_or_else(|| domain_does_not_exist_error(&stmt.domain_name))?;
        let constraint = domain
            .constraints
            .iter()
            .find(|constraint| constraint.name.eq_ignore_ascii_case(constraint_name))
            .cloned()
            .ok_or_else(|| domain_constraint_does_not_exist_error(constraint_name, &domain.name))?;
        if !matches!(constraint.kind, DomainConstraintKind::Check) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "constraint \"{}\" of domain \"{}\" is not a check constraint",
                    constraint_name, domain.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42809",
            });
        }
        if constraint.validated {
            return Ok(());
        }
        self.validate_domain_constraint_existing_values(
            client_id,
            &domain,
            &constraint,
            configured_search_path,
        )?;
        let mut domains = self.domains.write();
        let Some(domain) = domains.get_mut(&key) else {
            return Err(domain_does_not_exist_error(&stmt.domain_name));
        };
        if let Some(constraint) = domain
            .constraints
            .iter_mut()
            .find(|constraint| constraint.name.eq_ignore_ascii_case(constraint_name))
        {
            constraint.validated = true;
        }
        Ok(())
    }

    fn alter_domain_rename(
        &self,
        stmt: &AlterDomainStatement,
        new_name: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let old_key = self.resolve_domain_key(&stmt.domain_name, configured_search_path)?;
        let mut domains = self.domains.write();
        let mut domain = domains
            .remove(&old_key)
            .ok_or_else(|| domain_does_not_exist_error(&stmt.domain_name))?;
        let schema_key = old_key
            .rsplit_once('.')
            .map(|(schema, _)| schema.to_string())
            .unwrap_or_else(|| "public".to_string());
        let new_key = format!("{}.{}", schema_key, new_name.to_ascii_lowercase());
        if domains.values().any(|existing| {
            existing.namespace_oid == domain.namespace_oid
                && existing.name.eq_ignore_ascii_case(new_name)
        }) {
            domains.insert(old_key, domain);
            return Err(type_already_exists_error(new_name));
        }
        domain.name = new_name.to_ascii_lowercase();
        domains.insert(new_key, domain);
        Ok(())
    }

    fn alter_domain_rename_constraint(
        &self,
        stmt: &AlterDomainStatement,
        constraint_name: &str,
        new_name: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let key = self.resolve_domain_key(&stmt.domain_name, configured_search_path)?;
        let mut domains = self.domains.write();
        let Some(domain) = domains.get_mut(&key) else {
            return Err(domain_does_not_exist_error(&stmt.domain_name));
        };
        if domain
            .constraints
            .iter()
            .any(|constraint| constraint.name.eq_ignore_ascii_case(new_name))
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "constraint \"{}\" for domain \"{}\" already exists",
                    new_name, domain.name
                ),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        let Some(constraint) = domain
            .constraints
            .iter_mut()
            .find(|constraint| constraint.name.eq_ignore_ascii_case(constraint_name))
        else {
            return Err(domain_constraint_does_not_exist_error(
                constraint_name,
                &domain.name,
            ));
        };
        constraint.name = new_name.to_string();
        Ok(())
    }

    fn alter_domain_set_schema(
        &self,
        client_id: ClientId,
        stmt: &AlterDomainStatement,
        new_schema: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let old_key = self.resolve_domain_key(&stmt.domain_name, configured_search_path)?;
        let namespace_oid = self
            .visible_namespace_oid_by_name(client_id, None, &new_schema.to_ascii_lowercase())
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema \"{}\" does not exist", new_schema),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            })?;
        let mut domains = self.domains.write();
        let mut domain = domains
            .remove(&old_key)
            .ok_or_else(|| domain_does_not_exist_error(&stmt.domain_name))?;
        if domains.values().any(|existing| {
            existing.namespace_oid == namespace_oid
                && existing.name.eq_ignore_ascii_case(&domain.name)
        }) {
            let old_key_restore = old_key;
            domains.insert(old_key_restore, domain);
            return Err(type_already_exists_error(&stmt.domain_name));
        }
        domain.namespace_oid = namespace_oid;
        let new_key = format!(
            "{}.{}",
            new_schema.to_ascii_lowercase(),
            domain.name.to_ascii_lowercase()
        );
        domains.insert(new_key, domain);
        Ok(())
    }

    fn alter_domain_owner_to(
        &self,
        client_id: ClientId,
        stmt: &AlterDomainStatement,
        new_owner: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let owner_oid = if new_owner.eq_ignore_ascii_case("current_user")
            || new_owner.eq_ignore_ascii_case("current_role")
        {
            self.auth_state(client_id).current_user_oid()
        } else if new_owner.eq_ignore_ascii_case("session_user") {
            self.auth_state(client_id).session_user_oid()
        } else {
            let auth_catalog = self
                .auth_catalog(client_id, None)
                .map_err(map_catalog_error)?;
            crate::backend::catalog::roles::find_role_by_name(auth_catalog.roles(), new_owner)
                .ok_or_else(|| role_does_not_exist_error(new_owner))?
                .oid
        };
        let key = self.resolve_domain_key(&stmt.domain_name, configured_search_path)?;
        let mut domains = self.domains.write();
        let Some(domain) = domains.get_mut(&key) else {
            return Err(domain_does_not_exist_error(&stmt.domain_name));
        };
        domain.owner_oid = owner_oid;
        Ok(())
    }

    fn validate_domain_constraint_existing_values(
        &self,
        client_id: ClientId,
        domain: &DomainEntry,
        constraint: &DomainConstraintEntry,
        configured_search_path: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        for class in catalog.class_rows() {
            if !matches!(class.relkind, 'r' | 'm') {
                continue;
            }
            let Some(relation) = relation_for_domain_validation(self, client_id, &catalog, &class)
            else {
                continue;
            };
            for (column_index, column) in relation.desc.columns.iter().enumerate() {
                if column.dropped || !sql_type_uses_domain(column.sql_type, domain.oid) {
                    continue;
                }
                validate_domain_constraint_for_relation_column(
                    self,
                    client_id,
                    &catalog,
                    &relation,
                    &class.relname,
                    column_index,
                    domain,
                    constraint,
                )?;
            }
        }
        Ok(())
    }
}

fn relation_for_domain_validation(
    db: &Database,
    client_id: ClientId,
    catalog: &dyn CatalogLookup,
    class: &PgClassRow,
) -> Option<crate::backend::parser::BoundRelation> {
    let entry = relation_entry_by_oid(db, client_id, None, class.oid)?;
    let toast = if entry.reltoastrelid == 0 {
        None
    } else {
        relation_entry_by_oid(db, client_id, None, entry.reltoastrelid).map(|toast| {
            ToastRelationRef {
                rel: toast.rel,
                relation_oid: toast.relation_oid,
            }
        })
    };
    let _ = catalog;
    Some(crate::backend::parser::BoundRelation {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast,
        namespace_oid: entry.namespace_oid,
        owner_oid: entry.owner_oid,
        of_type_oid: entry.of_type_oid,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relispopulated: entry.relispopulated,
        relispartition: entry.relispartition,
        relpartbound: entry.relpartbound,
        desc: entry.desc,
        partitioned_table: entry.partitioned_table,
        partition_spec: entry.partition_spec,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_domain_constraint_for_relation_column(
    db: &Database,
    client_id: ClientId,
    catalog: &dyn CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
    relation_name: &str,
    column_index: usize,
    domain: &DomainEntry,
    constraint: &DomainConstraintEntry,
) -> Result<(), ExecError> {
    let mut ctx = ddl_executor_context_for_domain(db, client_id)?;
    let rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, &mut ctx)?;
    let column_name = relation.desc.columns[column_index].name.clone();
    match constraint.kind {
        DomainConstraintKind::NotNull => {
            for (_, values) in rows {
                if matches!(values.get(column_index), Some(Value::Null) | None) {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "column \"{}\" of table \"{}\" contains null values",
                            column_name, relation_name
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "23502",
                    });
                }
            }
        }
        DomainConstraintKind::Check => {
            let Some(expr_sql) = constraint.expr.as_deref() else {
                return Ok(());
            };
            let raw = parse_expr(expr_sql).map_err(ExecError::Parse)?;
            let desc = RelationDesc {
                columns: vec![column_desc("value", domain.sql_type, true)],
            };
            let scope = scope_for_relation(None, &desc);
            let bound = bind_expr_with_outer_and_ctes(&raw, &scope, catalog, &[], None, &[])
                .map_err(ExecError::Parse)?;
            for (_, values) in rows {
                let value = values.get(column_index).cloned().unwrap_or(Value::Null);
                if matches!(value, Value::Null) {
                    continue;
                }
                let mut slot = TupleSlot::virtual_row(vec![value]);
                match eval_expr(&bound, &mut slot, &mut ctx)? {
                    Value::Null | Value::Bool(true) => {}
                    Value::Bool(false) => {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "column \"{}\" of table \"{}\" contains values that violate the new constraint",
                                column_name, relation_name
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "23514",
                        });
                    }
                    _ => {
                        return Err(ExecError::DetailedError {
                            message: "CHECK constraint expression must return boolean".into(),
                            detail: Some(format!(
                                "constraint \"{}\" on domain \"{}\" produced a non-boolean value",
                                constraint.name, domain.name
                            )),
                            hint: None,
                            sqlstate: "42804",
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

fn ddl_executor_context_for_domain(
    db: &Database,
    client_id: ClientId,
) -> Result<ExecutorContext, ExecError> {
    let snapshot = db
        .txns
        .read()
        .snapshot_for_command(INVALID_TRANSACTION_ID, 0)?;
    Ok(ExecutorContext {
        pool: std::sync::Arc::clone(&db.pool),
        data_dir: None,
        txns: db.txns.clone(),
        txn_waiter: Some(db.txn_waiter.clone()),
        lock_status_provider: Some(std::sync::Arc::new(db.clone())),
        sequences: Some(db.sequences.clone()),
        large_objects: Some(db.large_objects.clone()),
        stats_import_runtime: None,
        async_notify_runtime: Some(db.async_notify_runtime.clone()),
        advisory_locks: std::sync::Arc::clone(&db.advisory_locks),
        row_locks: std::sync::Arc::clone(&db.row_locks),
        checkpoint_stats: db.checkpoint_stats_snapshot(),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        statement_timestamp_usecs:
            crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
        gucs: std::collections::HashMap::new(),
        interrupts: db.interrupt_state(client_id),
        stats: std::sync::Arc::clone(&db.stats),
        session_stats: db.session_stats_state(client_id),
        snapshot,
        transaction_state: None,
        client_id,
        current_database_name: db.current_database_name(),
        session_user_oid: db.auth_state(client_id).session_user_oid(),
        current_user_oid: db.auth_state(client_id).current_user_oid(),
        active_role_oid: db.auth_state(client_id).active_role_oid(),
        session_replication_role: db.session_replication_role(client_id),
        statement_lock_scope_id: None,
        transaction_lock_scope_id: None,
        next_command_id: 0,
        default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
        random_state: crate::backend::executor::PgPrngState::shared(),
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: false,
        pending_async_notifications: Vec::new(),
        catalog_effects: Vec::new(),
        temp_effects: Vec::new(),
        database: Some(db.clone()),
        pending_catalog_effects: Vec::new(),
        pending_table_locks: Vec::new(),
        catalog: None,
        scalar_function_cache: std::collections::HashMap::new(),
        srf_rows_cache: std::collections::HashMap::new(),
        plpgsql_function_cache: db.plpgsql_function_cache(client_id),
        pinned_cte_tables: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
    })
}

fn sql_type_uses_domain(sql_type: SqlType, domain_oid: u32) -> bool {
    sql_type.type_oid == domain_oid
        || (sql_type.is_array && sql_type_uses_domain(sql_type.element_type(), domain_oid))
}

fn choose_domain_constraint_name(domain: &DomainEntry, base_name: &str) -> String {
    if !domain
        .constraints
        .iter()
        .any(|constraint| constraint.name.eq_ignore_ascii_case(base_name))
    {
        return base_name.to_string();
    }
    for suffix in 1.. {
        let candidate = format!("{base_name}{suffix}");
        if !domain
            .constraints
            .iter()
            .any(|constraint| constraint.name.eq_ignore_ascii_case(&candidate))
        {
            return candidate;
        }
    }
    unreachable!("unbounded domain constraint suffix search")
}

fn refresh_domain_legacy_fields(domain: &mut DomainEntry) {
    domain.not_null = domain
        .constraints
        .iter()
        .any(|constraint| matches!(constraint.kind, DomainConstraintKind::NotNull));
    domain.check = domain
        .constraints
        .iter()
        .find(|constraint| matches!(constraint.kind, DomainConstraintKind::Check))
        .and_then(|constraint| constraint.expr.clone());
}

fn domain_does_not_exist_error(name: &str) -> ExecError {
    ExecError::Parse(ParseError::UnsupportedType(name.to_string()))
}

fn domain_constraint_does_not_exist_error(constraint_name: &str, domain_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "constraint \"{}\" of domain \"{}\" does not exist",
            constraint_name, domain_name
        ),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn type_already_exists_error(type_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("type \"{}\" already exists", type_name),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn role_does_not_exist_error(role_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("role \"{}\" does not exist", role_name),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}
