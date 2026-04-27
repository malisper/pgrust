use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::parser::analyze::infer_relation_expr_sql_type;
use crate::backend::parser::{
    AlterPublicationAction, AlterPublicationStatement, BoundRelation, CatalogLookup,
    CommentOnPublicationStatement, CreatePublicationStatement, DropPublicationStatement,
    PublicationObjectSpec, PublicationOption, PublicationOptions, PublicationSchemaName,
    PublicationTableSpec, PublicationTargetSpec, PublishGeneratedColumns, RawTypeName, SqlExpr,
    SqlType, SqlTypeKind, function_arg_values, parse_expr,
};
use crate::include::catalog::{
    CURRENT_DATABASE_NAME, PG_CATALOG_NAMESPACE_OID, PG_TOAST_NAMESPACE_OID, PUBLISH_GENCOLS_NONE,
    PUBLISH_GENCOLS_STORED, PgPublicationNamespaceRow, PgPublicationRelRow, PgPublicationRow,
};
use crate::include::nodes::parsenodes::RawXmlExprOp;
use crate::pgrust::database::ddl::{
    ensure_can_set_role, ensure_relation_owner, format_sql_type_name,
};

struct ResolvedPublicationTargets {
    relation_rows: Vec<PgPublicationRelRow>,
    namespace_rows: Vec<PgPublicationNamespaceRow>,
}

#[derive(Clone, Copy)]
enum PublicationMembershipKind {
    Table,
    Schema,
}

impl Database {
    pub(crate) fn execute_create_publication_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreatePublicationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_publication_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_publication_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreatePublicationStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let current_role = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .ok_or_else(current_role_missing_error)?;
        if !self.user_has_database_create_privilege(&auth, &auth_catalog) {
            return Err(permission_denied_for_database_error());
        }

        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if catcache
            .publication_row_by_name(&stmt.publication_name)
            .is_some()
        {
            return Err(duplicate_publication_error(&stmt.publication_name));
        }
        if stmt.target.for_all_tables && !current_role.rolsuper {
            return Err(must_be_superuser_error(
                "must be superuser to create FOR ALL TABLES publication",
            ));
        }
        let resolved = resolve_publication_targets(
            self,
            client_id,
            xid,
            cid,
            configured_search_path,
            &stmt.target,
            DuplicateHandling::Error,
            true,
        )?;
        if !resolved.namespace_rows.is_empty() && !current_role.rolsuper {
            return Err(must_be_superuser_error(
                "must be superuser to create FOR TABLES IN SCHEMA publication",
            ));
        }

        let mut row = publication_row_defaults(&stmt.publication_name, auth.current_user_oid());
        row.puballtables = stmt.target.for_all_tables;
        apply_publication_options(&mut row, &stmt.options)?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupt_state(client_id)),
        };
        let (_, effect) = self
            .catalog
            .write()
            .create_publication_mvcc(row, resolved.relation_rows, resolved.namespace_rows, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_publication_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterPublicationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_publication_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_publication_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterPublicationStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let current_role = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .ok_or_else(current_role_missing_error)?;
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let publication = catcache
            .publication_row_by_name(&stmt.publication_name)
            .cloned()
            .ok_or_else(|| publication_does_not_exist_error(&stmt.publication_name))?;
        if !auth.has_effective_membership(publication.pubowner, &auth_catalog) {
            return Err(must_be_publication_owner_error(&publication.pubname));
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupt_state(client_id)),
        };

        let effect = match &stmt.action {
            AlterPublicationAction::Rename { new_name } => self
                .catalog
                .write()
                .replace_publication_row_mvcc(
                    PgPublicationRow {
                        pubname: new_name.to_ascii_lowercase(),
                        ..publication
                    },
                    &ctx,
                )
                .map_err(map_catalog_error)?,
            AlterPublicationAction::OwnerTo { new_owner } => {
                let new_owner_row = find_role_by_name(auth_catalog.roles(), new_owner)
                    .cloned()
                    .ok_or_else(|| role_does_not_exist_error(new_owner))?;
                ensure_can_set_role(self, client_id, new_owner_row.oid, &new_owner_row.rolname)?;
                if !self.role_has_database_create_privilege(new_owner_row.oid, &auth_catalog) {
                    return Err(permission_denied_for_database_error());
                }
                if (publication.puballtables
                    || !catcache
                        .publication_namespace_rows_for_publication(publication.oid)
                        .is_empty())
                    && !new_owner_row.rolsuper
                {
                    return Err(must_be_superuser_error(
                        "new owner of FOR ALL TABLES or schema publication must be superuser",
                    ));
                }
                // :HACK: PostgreSQL tracks publication ownership through shared
                // dependency state. Until pgrust grows pg_shdepend, owner
                // changes flow through the explicit owner field and role-owned
                // object scans instead.
                self.catalog
                    .write()
                    .replace_publication_row_mvcc(
                        PgPublicationRow {
                            pubowner: new_owner_row.oid,
                            ..publication
                        },
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
            AlterPublicationAction::SetOptions(options) => {
                let mut updated = publication.clone();
                apply_publication_options(&mut updated, options)?;
                self.catalog
                    .write()
                    .replace_publication_row_mvcc(updated, &ctx)
                    .map_err(map_catalog_error)?
            }
            AlterPublicationAction::AddObjects(target) => {
                if publication.puballtables {
                    return Err(publication_all_tables_membership_error(
                        &publication.pubname,
                        publication_membership_kind(target),
                    ));
                }
                let resolved = resolve_publication_targets(
                    self,
                    client_id,
                    xid,
                    cid,
                    configured_search_path,
                    target,
                    DuplicateHandling::Error,
                    true,
                )?;
                if !resolved.namespace_rows.is_empty() && !current_role.rolsuper {
                    return Err(must_be_superuser_error(
                        "must be superuser to add or set schemas",
                    ));
                }
                let existing_rel_rows =
                    catcache.publication_rel_rows_for_publication(publication.oid);
                let existing_namespace_rows =
                    catcache.publication_namespace_rows_for_publication(publication.oid);
                let existing_rel_oids = existing_rel_rows
                    .iter()
                    .map(|row| row.prrelid)
                    .collect::<BTreeSet<_>>();
                let existing_namespace_oids = existing_namespace_rows
                    .iter()
                    .map(|row| row.pnnspid)
                    .collect::<BTreeSet<_>>();
                for row in &resolved.relation_rows {
                    if existing_rel_oids.contains(&row.prrelid) {
                        return Err(publication_relation_already_member_error(
                            self,
                            client_id,
                            Some((xid, cid)),
                            configured_search_path,
                            &publication.pubname,
                            row.prrelid,
                        ));
                    }
                }
                for row in &resolved.namespace_rows {
                    if existing_namespace_oids.contains(&row.pnnspid) {
                        return Err(publication_schema_already_member_error(
                            self,
                            client_id,
                            Some((xid, cid)),
                            &publication.pubname,
                            row.pnnspid,
                        ));
                    }
                }
                let mut new_rel_rows = existing_rel_rows;
                new_rel_rows.extend(resolved.relation_rows);
                let mut new_namespace_rows = existing_namespace_rows;
                new_namespace_rows.extend(resolved.namespace_rows);
                self.catalog
                    .write()
                    .replace_publication_memberships_mvcc(
                        publication.oid,
                        new_rel_rows,
                        new_namespace_rows,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
            AlterPublicationAction::DropObjects(target) => {
                if publication.puballtables {
                    return Err(publication_all_tables_membership_error(
                        &publication.pubname,
                        publication_membership_kind(target),
                    ));
                }
                reject_publication_drop_filters(target)?;
                let resolved = resolve_publication_targets(
                    self,
                    client_id,
                    xid,
                    cid,
                    configured_search_path,
                    target,
                    DuplicateHandling::Error,
                    false,
                )?;
                let existing_rel_rows =
                    catcache.publication_rel_rows_for_publication(publication.oid);
                let existing_namespace_rows =
                    catcache.publication_namespace_rows_for_publication(publication.oid);
                let target_rel_oids = resolved
                    .relation_rows
                    .iter()
                    .map(|row| row.prrelid)
                    .collect::<BTreeSet<_>>();
                let target_namespace_oids = resolved
                    .namespace_rows
                    .iter()
                    .map(|row| row.pnnspid)
                    .collect::<BTreeSet<_>>();
                for target_oid in &target_rel_oids {
                    if !existing_rel_rows
                        .iter()
                        .any(|row| row.prrelid == *target_oid)
                    {
                        return Err(publication_relation_not_member_error(
                            self,
                            client_id,
                            Some((xid, cid)),
                            configured_search_path,
                            &publication.pubname,
                            *target_oid,
                        ));
                    }
                }
                for target_oid in &target_namespace_oids {
                    if !existing_namespace_rows
                        .iter()
                        .any(|row| row.pnnspid == *target_oid)
                    {
                        return Err(publication_schema_not_member_error(
                            self,
                            client_id,
                            Some((xid, cid)),
                            &publication.pubname,
                            *target_oid,
                        ));
                    }
                }
                let new_rel_rows = existing_rel_rows
                    .into_iter()
                    .filter(|row| !target_rel_oids.contains(&row.prrelid))
                    .collect::<Vec<_>>();
                let new_namespace_rows = existing_namespace_rows
                    .into_iter()
                    .filter(|row| !target_namespace_oids.contains(&row.pnnspid))
                    .collect::<Vec<_>>();
                self.catalog
                    .write()
                    .replace_publication_memberships_mvcc(
                        publication.oid,
                        new_rel_rows,
                        new_namespace_rows,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
            AlterPublicationAction::SetObjects(target) => {
                if publication.puballtables {
                    return Err(publication_all_tables_membership_error(
                        &publication.pubname,
                        publication_membership_kind(target),
                    ));
                }
                let resolved = resolve_publication_targets(
                    self,
                    client_id,
                    xid,
                    cid,
                    configured_search_path,
                    target,
                    DuplicateHandling::Dedup,
                    true,
                )?;
                if !resolved.namespace_rows.is_empty() && !current_role.rolsuper {
                    return Err(must_be_superuser_error(
                        "must be superuser to set TABLES IN SCHEMA for publication",
                    ));
                }
                self.catalog
                    .write()
                    .replace_publication_memberships_mvcc(
                        publication.oid,
                        resolved.relation_rows,
                        resolved.namespace_rows,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
        };

        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_publication_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropPublicationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_publication_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_publication_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropPublicationStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if stmt.cascade {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "DROP PUBLICATION CASCADE".into(),
            )));
        }
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut current_cid = cid;
        let mut dropped = 0usize;

        for publication_name in &stmt.publication_names {
            let catcache = self
                .backend_catcache(client_id, Some((xid, current_cid)))
                .map_err(map_catalog_error)?;
            let Some(publication) = catcache.publication_row_by_name(publication_name).cloned()
            else {
                if stmt.if_exists {
                    continue;
                }
                return Err(publication_does_not_exist_error(publication_name));
            };
            if !auth.has_effective_membership(publication.pubowner, &auth_catalog) {
                return Err(must_be_publication_owner_error(&publication.pubname));
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: Arc::clone(&self.interrupt_state(client_id)),
            };
            let (_, effect) = self
                .catalog
                .write()
                .drop_publication_mvcc(publication.oid, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            self.plan_cache.invalidate_all();
            catalog_effects.push(effect);
            dropped = dropped.saturating_add(1);
            current_cid = current_cid.saturating_add(1);
        }

        Ok(StatementResult::AffectedRows(dropped))
    }

    pub(crate) fn execute_comment_on_publication_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnPublicationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_publication_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_publication_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnPublicationStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let publication = catcache
            .publication_row_by_name(&stmt.publication_name)
            .cloned()
            .ok_or_else(|| publication_does_not_exist_error(&stmt.publication_name))?;
        if !auth.has_effective_membership(publication.pubowner, &auth_catalog) {
            return Err(must_be_publication_owner_error(&publication.pubname));
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupt_state(client_id)),
        };
        let effect = self
            .catalog
            .write()
            .comment_publication_mvcc(publication.oid, stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}

#[derive(Clone, Copy)]
enum DuplicateHandling {
    Error,
    Dedup,
}

fn resolve_publication_targets(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    configured_search_path: Option<&[String]>,
    target: &PublicationTargetSpec,
    duplicates: DuplicateHandling,
    require_relation_ownership: bool,
) -> Result<ResolvedPublicationTargets, ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
    let _catcache = db
        .backend_catcache(client_id, Some((xid, cid)))
        .map_err(map_catalog_error)?;
    let mut seen_relations = BTreeMap::new();
    let mut seen_namespaces = BTreeSet::new();
    let mut relation_rows = Vec::new();
    let mut namespace_rows = Vec::new();

    for object in &target.objects {
        match object {
            PublicationObjectSpec::Table(table) => {
                let relation = lookup_publication_relation(&catalog, &table.relation_name)?;
                validate_publishable_relation(db, client_id, &relation, &table.relation_name)?;
                if require_relation_ownership {
                    ensure_relation_owner(db, client_id, &relation, &table.relation_name)?;
                }
                if let Some(existing_has_filter) =
                    seen_relations.insert(relation.relation_oid, table.where_clause.is_some())
                {
                    if matches!(duplicates, DuplicateHandling::Error) {
                        if existing_has_filter || table.where_clause.is_some() {
                            return Err(publication_relation_conflicting_filter_error(
                                &table.relation_name,
                            ));
                        }
                        return Err(publication_relation_duplicate_error(&table.relation_name));
                    }
                    continue;
                }
                let prqual = validate_publication_row_filter(&catalog, &relation, table)?;
                relation_rows.push(PgPublicationRelRow {
                    oid: 0,
                    prpubid: 0,
                    prrelid: relation.relation_oid,
                    prqual,
                    prattrs: publication_column_numbers(
                        &relation,
                        &table.relation_name,
                        &table.column_names,
                    )?,
                });
            }
            PublicationObjectSpec::Schema(schema) => {
                let namespace = resolve_publication_schema(
                    db,
                    client_id,
                    Some((xid, cid)),
                    configured_search_path,
                    schema,
                )?;
                validate_publishable_schema(db, client_id, &namespace.nspname, namespace.oid)?;
                if !seen_namespaces.insert(namespace.oid) {
                    if matches!(duplicates, DuplicateHandling::Error) {
                        return Err(publication_schema_duplicate_error(&namespace.nspname));
                    }
                    continue;
                }
                namespace_rows.push(PgPublicationNamespaceRow {
                    oid: 0,
                    pnpubid: 0,
                    pnnspid: namespace.oid,
                });
            }
        }
    }

    Ok(ResolvedPublicationTargets {
        relation_rows,
        namespace_rows,
    })
}

fn reject_publication_drop_filters(target: &PublicationTargetSpec) -> Result<(), ExecError> {
    if target.objects.iter().any(|object| {
        matches!(
            object,
            PublicationObjectSpec::Table(PublicationTableSpec {
                where_clause: Some(_),
                ..
            })
        )
    }) {
        return Err(ExecError::DetailedError {
            message: "cannot use a WHERE clause when removing a table from a publication".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    Ok(())
}

fn publication_column_numbers(
    relation: &BoundRelation,
    relation_name: &str,
    column_names: &[String],
) -> Result<Option<Vec<i16>>, ExecError> {
    if column_names.is_empty() {
        return Ok(None);
    }

    let mut attrs = Vec::with_capacity(column_names.len());
    for column_name in column_names {
        let Some((idx, _)) = relation
            .desc
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        else {
            return Err(ExecError::DetailedError {
                message: format!(
                    "column \"{column_name}\" of relation \"{relation_name}\" does not exist"
                ),
                detail: None,
                hint: None,
                sqlstate: "42703",
            });
        };
        let attr_no = i16::try_from(idx + 1).map_err(|_| ExecError::DetailedError {
            message: format!("too many columns in relation \"{relation_name}\""),
            detail: None,
            hint: None,
            sqlstate: "54011",
        })?;
        if attrs.contains(&attr_no) {
            return Err(ExecError::DetailedError {
                message: format!("duplicate column \"{column_name}\" in publication column list"),
                detail: None,
                hint: None,
                sqlstate: "42701",
            });
        }
        attrs.push(attr_no);
    }
    Ok(Some(attrs))
}

fn validate_publication_row_filter(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    table: &PublicationTableSpec,
) -> Result<Option<String>, ExecError> {
    let Some(filter) = table.where_clause.as_deref() else {
        return Ok(None);
    };
    if filter.contains("=#>") {
        return Err(invalid_publication_where_error(
            "User-defined operators are not allowed.",
        ));
    }
    let expr = parse_expr(filter).map_err(ExecError::Parse)?;
    validate_publication_filter_expr(&expr)?;
    if !publication_filter_returns_bool_by_syntax(&expr)
        && !filter
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("xmlexists")
    {
        let sql_type = infer_relation_expr_sql_type(filter, None, &relation.desc, catalog)
            .map_err(ExecError::Parse)?;
        if sql_type != SqlType::new(SqlTypeKind::Bool) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "argument of PUBLICATION WHERE must be type boolean, not type {}",
                    format_sql_type_name(sql_type)
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
    }
    if filter
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("xmlexists")
    {
        return Ok(Some(filter.trim().to_string()));
    }
    Ok(Some(
        render_publication_filter_expr(&expr).unwrap_or_else(|| filter.trim().to_string()),
    ))
}

fn publication_filter_returns_bool_by_syntax(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::Xml(xml) if xml.op == RawXmlExprOp::IsDocument
    )
}

fn validate_publication_filter_expr(expr: &SqlExpr) -> Result<(), ExecError> {
    use SqlExpr::*;

    // :HACK: PostgreSQL validates publication filters from the fully bound
    // expression tree, including function/operator provenance and volatility.
    // pgrust does not retain enough of that metadata here yet, so keep this
    // narrow syntactic guard until publication filters use a dedicated binder.
    match expr {
        FuncCall { name, args, .. } => {
            let normalized = name.rsplit('.').next().unwrap_or(name).to_ascii_lowercase();
            if matches!(normalized.as_str(), "avg" | "count" | "max" | "min" | "sum") {
                return Err(ExecError::DetailedError {
                    message: "aggregate functions are not allowed in WHERE".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42803",
                });
            }
            if normalized == "random" || normalized.starts_with("testpub_") {
                return Err(invalid_publication_where_error(
                    "User-defined or built-in mutable functions are not allowed.",
                ));
            }
            for arg in function_arg_values(args) {
                validate_publication_filter_expr(arg)?;
            }
        }
        BinaryOperator { left, right, .. } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(right)?;
            return Err(invalid_publication_where_error(
                "User-defined operators are not allowed.",
            ));
        }
        InSubquery { expr, .. } => {
            validate_publication_filter_expr(expr)?;
            return Err(invalid_publication_where_error(
                "Only columns, constants, built-in operators, built-in data types, built-in collations, and immutable built-in functions are allowed.",
            ));
        }
        ScalarSubquery(_) | ArraySubquery(_) | Exists(_) | QuantifiedSubquery { .. } => {
            return Err(invalid_publication_where_error(
                "Only columns, constants, built-in operators, built-in data types, built-in collations, and immutable built-in functions are allowed.",
            ));
        }
        Column(name) if name.eq_ignore_ascii_case("ctid") => {
            return Err(invalid_publication_where_error(
                "System columns are not allowed.",
            ));
        }
        Add(left, right)
        | Sub(left, right)
        | BitAnd(left, right)
        | BitOr(left, right)
        | BitXor(left, right)
        | Shl(left, right)
        | Shr(left, right)
        | Mul(left, right)
        | Div(left, right)
        | Mod(left, right)
        | Concat(left, right)
        | Eq(left, right)
        | NotEq(left, right)
        | Lt(left, right)
        | LtEq(left, right)
        | Gt(left, right)
        | GtEq(left, right)
        | RegexMatch(left, right)
        | And(left, right)
        | Or(left, right)
        | IsDistinctFrom(left, right)
        | IsNotDistinctFrom(left, right)
        | Overlaps(left, right)
        | ArrayOverlap(left, right)
        | ArrayContains(left, right)
        | ArrayContained(left, right)
        | JsonbContains(left, right)
        | JsonbContained(left, right)
        | JsonbExists(left, right)
        | JsonbExistsAny(left, right)
        | JsonbExistsAll(left, right)
        | JsonbPathExists(left, right)
        | JsonbPathMatch(left, right)
        | JsonGet(left, right)
        | JsonGetText(left, right)
        | JsonPath(left, right)
        | JsonPathText(left, right)
        | AtTimeZone {
            expr: left,
            zone: right,
        } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(right)?;
        }
        UnaryPlus(inner)
        | Negate(inner)
        | BitNot(inner)
        | Cast(inner, _)
        | Collate { expr: inner, .. }
        | IsNull(inner)
        | IsNotNull(inner)
        | Not(inner)
        | FieldSelect { expr: inner, .. }
        | Subscript { expr: inner, .. } => validate_publication_filter_expr(inner)?,
        Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            validate_publication_filter_expr(expr)?;
            validate_publication_filter_expr(pattern)?;
            if let Some(escape) = escape {
                validate_publication_filter_expr(escape)?;
            }
        }
        Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                validate_publication_filter_expr(arg)?;
            }
            for when in args {
                validate_publication_filter_expr(&when.expr)?;
                validate_publication_filter_expr(&when.result)?;
            }
            if let Some(defresult) = defresult {
                validate_publication_filter_expr(defresult)?;
            }
        }
        ArrayLiteral(values) | Row(values) => {
            for value in values {
                validate_publication_filter_expr(value)?;
            }
        }
        QuantifiedArray { left, array, .. } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(array)?;
        }
        ArraySubscript { array, subscripts } => {
            validate_publication_filter_expr(array)?;
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    validate_publication_filter_expr(lower)?;
                }
                if let Some(upper) = &subscript.upper {
                    validate_publication_filter_expr(upper)?;
                }
            }
        }
        GeometryUnaryOp { expr, .. } | PrefixOperator { expr, .. } => {
            validate_publication_filter_expr(expr)?;
        }
        GeometryBinaryOp { left, right, .. } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(right)?;
        }
        Random => {
            return Err(invalid_publication_where_error(
                "User-defined or built-in mutable functions are not allowed.",
            ));
        }
        Xml(xml) => {
            for child in xml.child_exprs() {
                validate_publication_filter_expr(child)?;
            }
        }
        Column(_)
        | Default
        | Const(_)
        | IntegerLiteral(_)
        | NumericLiteral(_)
        | CurrentDate
        | CurrentCatalog
        | CurrentSchema
        | CurrentUser
        | SessionUser
        | CurrentRole
        | CurrentTime { .. }
        | CurrentTimestamp { .. }
        | LocalTime { .. }
        | LocalTimestamp { .. } => {}
    }
    Ok(())
}

fn render_publication_filter_expr(expr: &SqlExpr) -> Option<String> {
    use SqlExpr::*;

    Some(match expr {
        And(left, right) => format!(
            "({} AND {})",
            render_publication_filter_expr(left)?,
            render_publication_filter_expr(right)?
        ),
        Or(left, right) => format!(
            "({} OR {})",
            render_publication_filter_expr(left)?,
            render_publication_filter_expr(right)?
        ),
        Eq(left, right) => render_publication_binary_expr(left, "=", right)?,
        NotEq(left, right) => render_publication_binary_expr(left, "<>", right)?,
        Lt(left, right) => render_publication_binary_expr(left, "<", right)?,
        LtEq(left, right) => render_publication_binary_expr(left, "<=", right)?,
        Gt(left, right) => render_publication_binary_expr(left, ">", right)?,
        GtEq(left, right) => render_publication_binary_expr(left, ">=", right)?,
        IsNull(inner) => format!("({} IS NULL)", render_publication_filter_term(inner)?),
        IsNotNull(inner) => format!("({} IS NOT NULL)", render_publication_filter_term(inner)?),
        IsDistinctFrom(left, right) => format!(
            "({} IS DISTINCT FROM {})",
            render_publication_filter_term(left)?,
            render_publication_filter_term(right)?
        ),
        IsNotDistinctFrom(left, right) => format!(
            "({} IS NOT DISTINCT FROM {})",
            render_publication_filter_term(left)?,
            render_publication_filter_term(right)?
        ),
        Not(inner) => format!("(NOT {})", render_publication_filter_term(inner)?),
        _ => render_publication_filter_term(expr)?,
    })
}

fn render_publication_binary_expr(left: &SqlExpr, op: &str, right: &SqlExpr) -> Option<String> {
    Some(format!(
        "({} {} {})",
        render_publication_filter_term(left)?,
        op,
        render_publication_filter_term(right)?
    ))
}

fn render_publication_filter_term(expr: &SqlExpr) -> Option<String> {
    use SqlExpr::*;

    Some(match expr {
        Column(name) => name.clone(),
        IntegerLiteral(value) | NumericLiteral(value) => value.clone(),
        Const(value) => render_publication_const(value)?,
        Cast(inner, ty) => format!(
            "{}::{}",
            render_publication_filter_term(inner)?,
            render_publication_type_name(ty)
        ),
        Collate { expr, collation } => {
            format!(
                "{} COLLATE {}",
                render_publication_filter_term(expr)?,
                collation
            )
        }
        Add(left, right) => render_publication_arithmetic_expr(left, "+", right)?,
        Sub(left, right) => render_publication_arithmetic_expr(left, "-", right)?,
        Mul(left, right) => render_publication_arithmetic_expr(left, "*", right)?,
        Div(left, right) => render_publication_arithmetic_expr(left, "/", right)?,
        Mod(left, right) => render_publication_arithmetic_expr(left, "%", right)?,
        UnaryPlus(inner) => format!("+{}", render_publication_filter_term(inner)?),
        Negate(inner) => format!("-{}", render_publication_filter_term(inner)?),
        FuncCall { name, args, .. } => {
            let rendered_args = function_arg_values(args)
                .map(|arg| render_publication_filter_term(arg))
                .collect::<Option<Vec<_>>>()?
                .join(", ");
            format!("{name}({rendered_args})")
        }
        _ => return None,
    })
}

fn render_publication_arithmetic_expr(left: &SqlExpr, op: &str, right: &SqlExpr) -> Option<String> {
    Some(format!(
        "({} {} {})",
        render_publication_filter_term(left)?,
        op,
        render_publication_filter_term(right)?
    ))
}

fn render_publication_const(value: &Value) -> Option<String> {
    Some(match value {
        Value::Null => "NULL".into(),
        Value::Bool(true) => "true".into(),
        Value::Bool(false) => "false".into(),
        Value::Int16(value) => value.to_string(),
        Value::Int32(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Numeric(value) => value.render(),
        Value::Text(text) => format!("'{}'::text", escape_publication_string_literal(text)),
        Value::TextRef(_, _) => format!(
            "'{}'::text",
            escape_publication_string_literal(value.as_text().unwrap_or_default())
        ),
        Value::Xml(text) => format!("'{}'::xml", escape_publication_string_literal(text)),
        _ => return None,
    })
}

fn render_publication_type_name(ty: &RawTypeName) -> String {
    match ty {
        RawTypeName::Builtin(sql_type) => format_sql_type_name(*sql_type).into(),
        RawTypeName::Serial(kind) => match kind {
            crate::backend::parser::SerialKind::Small => "smallserial".into(),
            crate::backend::parser::SerialKind::Regular => "serial".into(),
            crate::backend::parser::SerialKind::Big => "bigserial".into(),
        },
        RawTypeName::Named { name, .. } => name.clone(),
        RawTypeName::Record => "record".into(),
    }
}

fn escape_publication_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn lookup_publication_relation(
    catalog: &dyn CatalogLookup,
    relation_name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(relation_name) {
        Some(relation) if matches!(relation.relkind, 'r' | 'p') => Ok(relation),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: relation_name.to_string(),
            expected: "table",
        })),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            relation_name.to_string(),
        ))),
    }
}

fn resolve_publication_schema(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    schema: &crate::backend::parser::PublicationSchemaSpec,
) -> Result<crate::include::catalog::PgNamespaceRow, ExecError> {
    let schema_name = match &schema.schema_name {
        PublicationSchemaName::Name(name) => name.clone(),
        PublicationSchemaName::CurrentSchema => db
            .effective_search_path(client_id, configured_search_path)
            .into_iter()
            .find(|schema_name| {
                !schema_name.is_empty()
                    && schema_name != "$user"
                    && !schema_name.eq_ignore_ascii_case("pg_catalog")
            })
            .ok_or(ParseError::NoSchemaSelectedForCreate)
            .map_err(ExecError::Parse)?,
    };
    db.backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .namespace_by_name(&schema_name)
        .cloned()
        .filter(|row| !db.other_session_temp_namespace_oid(client_id, row.oid))
        .ok_or_else(|| schema_does_not_exist_error(&schema_name))
}

fn validate_publishable_relation(
    db: &Database,
    client_id: ClientId,
    relation: &BoundRelation,
    relation_name: &str,
) -> Result<(), ExecError> {
    if !matches!(relation.relkind, 'r' | 'p') {
        return Err(ExecError::DetailedError {
            message: format!("cannot add relation \"{relation_name}\" to publication"),
            detail: Some("This operation is not supported for relation type.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    if relation.relpersistence == 't'
        || db.other_session_temp_namespace_oid(client_id, relation.namespace_oid)
    {
        return Err(ExecError::DetailedError {
            message: format!("cannot add relation \"{relation_name}\" to publication"),
            detail: Some("This operation is not supported for temporary tables.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    if relation.relpersistence == 'u' {
        return Err(ExecError::DetailedError {
            message: format!("cannot add relation \"{relation_name}\" to publication"),
            detail: Some("This operation is not supported for unlogged tables.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID
        || relation.namespace_oid == PG_TOAST_NAMESPACE_OID
    {
        return Err(ExecError::DetailedError {
            message: format!("cannot add relation \"{relation_name}\" to publication"),
            detail: Some("This operation is not supported for system tables.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok(())
}

fn validate_publishable_schema(
    db: &Database,
    client_id: ClientId,
    schema_name: &str,
    namespace_oid: u32,
) -> Result<(), ExecError> {
    if namespace_oid == PG_CATALOG_NAMESPACE_OID || namespace_oid == PG_TOAST_NAMESPACE_OID {
        return Err(ExecError::DetailedError {
            message: format!("cannot add schema \"{schema_name}\" to publication"),
            detail: Some("This operation is not supported for system schemas.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    if db.other_session_temp_namespace_oid(client_id, namespace_oid)
        || schema_name.starts_with("pg_temp_")
        || schema_name.starts_with("pg_toast_temp_")
    {
        return Err(ExecError::DetailedError {
            message: format!("cannot add schema \"{schema_name}\" to publication"),
            detail: Some("Temporary schemas cannot be replicated.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok(())
}

fn publication_membership_kind(target: &PublicationTargetSpec) -> PublicationMembershipKind {
    if target
        .objects
        .iter()
        .all(|object| matches!(object, PublicationObjectSpec::Schema(_)))
    {
        PublicationMembershipKind::Schema
    } else {
        PublicationMembershipKind::Table
    }
}

fn publication_row_defaults(publication_name: &str, owner_oid: u32) -> PgPublicationRow {
    PgPublicationRow {
        oid: 0,
        pubname: publication_name.to_ascii_lowercase(),
        pubowner: owner_oid,
        puballtables: false,
        pubinsert: true,
        pubupdate: true,
        pubdelete: true,
        pubtruncate: true,
        pubviaroot: false,
        pubgencols: PUBLISH_GENCOLS_NONE,
    }
}

fn apply_publication_options(
    publication: &mut PgPublicationRow,
    options: &PublicationOptions,
) -> Result<(), ExecError> {
    let mut seen = BTreeSet::new();
    for option in &options.options {
        let option_name = publication_option_name(option);
        if !seen.insert(option_name.clone()) {
            return Err(ExecError::Parse(
                ParseError::ConflictingOrRedundantOptions {
                    option: option_name,
                },
            ));
        }
        match option {
            PublicationOption::Publish(actions) => {
                publication.pubinsert = actions.insert;
                publication.pubupdate = actions.update;
                publication.pubdelete = actions.delete;
                publication.pubtruncate = actions.truncate;
            }
            PublicationOption::PublishViaPartitionRoot(value) => {
                publication.pubviaroot = *value;
            }
            PublicationOption::PublishGeneratedColumns(value) => {
                publication.pubgencols = match value {
                    PublishGeneratedColumns::None => PUBLISH_GENCOLS_NONE,
                    PublishGeneratedColumns::Stored => PUBLISH_GENCOLS_STORED,
                };
            }
            PublicationOption::Raw { name, .. } => {
                return Err(ExecError::Parse(
                    ParseError::UnrecognizedPublicationParameter(name.clone()),
                ));
            }
        }
    }
    Ok(())
}

fn publication_option_name(option: &PublicationOption) -> String {
    match option {
        PublicationOption::Publish(_) => "publish".into(),
        PublicationOption::PublishViaPartitionRoot(_) => "publish_via_partition_root".into(),
        PublicationOption::PublishGeneratedColumns(_) => "publish_generated_columns".into(),
        PublicationOption::Raw { name, .. } => name.clone(),
    }
}

fn current_role_missing_error() -> ExecError {
    ExecError::DetailedError {
        message: "current role does not exist".into(),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn permission_denied_for_database_error() -> ExecError {
    ExecError::DetailedError {
        message: format!("permission denied for database {CURRENT_DATABASE_NAME}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn duplicate_publication_error(publication_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("publication \"{publication_name}\" already exists"),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_does_not_exist_error(publication_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("publication \"{publication_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn role_does_not_exist_error(role_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("role \"{role_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn schema_does_not_exist_error(schema_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("schema \"{schema_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "3F000",
    }
}

fn must_be_superuser_error(message: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn must_be_publication_owner_error(publication_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("must be owner of publication {publication_name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn publication_all_tables_membership_error(
    publication_name: &str,
    membership_kind: PublicationMembershipKind,
) -> ExecError {
    ExecError::DetailedError {
        message: format!("publication \"{publication_name}\" is defined as FOR ALL TABLES"),
        detail: Some(match membership_kind {
            PublicationMembershipKind::Table => {
                "Tables cannot be added to or dropped from FOR ALL TABLES publications.".into()
            }
            PublicationMembershipKind::Schema => {
                "Schemas cannot be added to or dropped from FOR ALL TABLES publications.".into()
            }
        }),
        hint: None,
        sqlstate: "55000",
    }
}

fn publication_relation_duplicate_error(relation_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("relation \"{relation_name}\" specified more than once"),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_relation_conflicting_filter_error(relation_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("conflicting or redundant WHERE clauses for table \"{relation_name}\""),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn invalid_publication_where_error(detail: &str) -> ExecError {
    ExecError::DetailedError {
        message: "invalid publication WHERE expression".into(),
        detail: Some(detail.into()),
        hint: None,
        sqlstate: "42P17",
    }
}

fn publication_schema_duplicate_error(schema_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("schema \"{schema_name}\" specified more than once"),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_relation_already_member_error(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    publication_name: &str,
    relation_oid: u32,
) -> ExecError {
    let relation_name = db
        .relation_display_name(client_id, txn_ctx, configured_search_path, relation_oid)
        .unwrap_or_else(|| relation_oid.to_string());
    ExecError::DetailedError {
        message: format!(
            "relation \"{relation_name}\" is already member of publication \"{publication_name}\""
        ),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_schema_already_member_error(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    publication_name: &str,
    namespace_oid: u32,
) -> ExecError {
    let schema_name = publication_namespace_name(db, client_id, txn_ctx, namespace_oid)
        .unwrap_or_else(|| namespace_oid.to_string());
    ExecError::DetailedError {
        message: format!(
            "schema \"{schema_name}\" is already member of publication \"{publication_name}\""
        ),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_relation_not_member_error(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    publication_name: &str,
    relation_oid: u32,
) -> ExecError {
    let relation_name = db
        .relation_display_name(client_id, txn_ctx, configured_search_path, relation_oid)
        .unwrap_or_else(|| relation_oid.to_string());
    ExecError::DetailedError {
        message: format!(
            "relation \"{relation_name}\" is not member of publication \"{publication_name}\""
        ),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn publication_schema_not_member_error(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    publication_name: &str,
    namespace_oid: u32,
) -> ExecError {
    let schema_name = publication_namespace_name(db, client_id, txn_ctx, namespace_oid)
        .unwrap_or_else(|| namespace_oid.to_string());
    ExecError::DetailedError {
        message: format!(
            "schema \"{schema_name}\" is not member of publication \"{publication_name}\""
        ),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn publication_namespace_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    namespace_oid: u32,
) -> Option<String> {
    db.backend_catcache(client_id, txn_ctx)
        .ok()
        .and_then(|catcache| {
            catcache
                .namespace_by_oid(namespace_oid)
                .map(|row| row.nspname.clone())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::{StatementResult, Value};
    use crate::pgrust::session::Session;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_publication_cmds_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn role_oid(db: &Database, role_name: &str) -> u32 {
        db.backend_catcache(1, None)
            .unwrap()
            .authid_rows()
            .into_iter()
            .find(|row| row.rolname == role_name)
            .unwrap()
            .oid
    }

    fn publication_owner_name(db: &Database, publication_name: &str) -> String {
        let catcache = db.backend_catcache(1, None).unwrap();
        let owner_oid = catcache
            .publication_row_by_name(publication_name)
            .map(|row| row.pubowner)
            .unwrap();
        catcache
            .authid_rows()
            .into_iter()
            .find(|row| row.oid == owner_oid)
            .map(|row| row.rolname)
            .unwrap()
    }

    #[test]
    fn alter_publication_owner_to_checks_database_create_on_target_role() {
        let base = temp_dir("owner_target_create");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role tenant").unwrap();
        superuser.execute(&db, "create role target").unwrap();
        superuser
            .execute(&db, "grant create on database regression to tenant")
            .unwrap();

        let tenant_oid = role_oid(&db, "tenant");
        let target_oid = role_oid(&db, "target");
        let mut tenant = Session::new(2);
        tenant.set_session_authorization_oid(tenant_oid);
        tenant
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        tenant
            .execute(&db, "create publication pub for table widgets")
            .unwrap();

        superuser
            .execute(&db, "revoke create on database regression from tenant")
            .unwrap();
        superuser
            .execute(&db, "grant create on database regression to target")
            .unwrap();
        superuser
            .execute(&db, "grant target to tenant with inherit false, set true")
            .unwrap();

        let auth = db.auth_state(2);
        let auth_catalog = db.auth_catalog(2, None).unwrap();
        assert!(!db.user_has_database_create_privilege(&auth, &auth_catalog));
        assert!(db.role_has_database_create_privilege(target_oid, &auth_catalog));

        tenant
            .execute(&db, "alter publication pub owner to target")
            .unwrap();
        assert_eq!(publication_owner_name(&db, "pub"), "target");
    }

    #[test]
    fn alter_publication_owner_to_rejects_target_without_database_create_privilege() {
        let base = temp_dir("owner_target_missing_create");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role tenant").unwrap();
        superuser.execute(&db, "create role target").unwrap();
        superuser
            .execute(&db, "grant create on database regression to tenant")
            .unwrap();

        let mut tenant = Session::new(2);
        tenant.set_session_authorization_oid(role_oid(&db, "tenant"));
        tenant
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        tenant
            .execute(&db, "create publication pub for table widgets")
            .unwrap();

        superuser
            .execute(&db, "revoke create on database regression from tenant")
            .unwrap();
        superuser
            .execute(&db, "grant target to tenant with inherit false, set true")
            .unwrap();

        let err = tenant
            .execute(&db, "alter publication pub owner to target")
            .unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                message,
                sqlstate: "42501",
                ..
            } if message == format!("permission denied for database {CURRENT_DATABASE_NAME}")
        ));
        assert_eq!(publication_owner_name(&db, "pub"), "tenant");
    }

    #[test]
    fn alter_publication_owner_to_requires_superuser_for_all_tables_publication() {
        let base = temp_dir("owner_for_all_tables_superuser");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role target").unwrap();
        session
            .execute(&db, "grant create on database regression to target")
            .unwrap();
        session
            .execute(&db, "create publication pub for all tables")
            .unwrap();

        let err = session
            .execute(&db, "alter publication pub owner to target")
            .unwrap_err();
        assert!(
            format!("{err:?}")
                .contains("new owner of FOR ALL TABLES or schema publication must be superuser")
        );
        assert_eq!(publication_owner_name(&db, "pub"), "postgres");
    }

    #[test]
    fn alter_publication_owner_to_requires_superuser_for_schema_publication() {
        let base = temp_dir("owner_schema_publication_superuser");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role target").unwrap();
        session
            .execute(&db, "grant create on database regression to target")
            .unwrap();
        session.execute(&db, "create schema pub_test").unwrap();
        session
            .execute(&db, "create publication pub for tables in schema pub_test")
            .unwrap();

        let err = session
            .execute(&db, "alter publication pub owner to target")
            .unwrap_err();
        assert!(
            format!("{err:?}")
                .contains("new owner of FOR ALL TABLES or schema publication must be superuser")
        );
        assert_eq!(publication_owner_name(&db, "pub"), "postgres");
    }

    #[test]
    fn alter_publication_for_all_tables_errors_include_membership_detail() {
        let base = temp_dir("foralltables_detail");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create publication pub for all tables")
            .unwrap();

        let table_err = session
            .execute(&db, "alter publication pub add table widgets")
            .unwrap_err();
        match table_err {
            ExecError::DetailedError { detail, .. } => assert_eq!(
                detail.as_deref(),
                Some("Tables cannot be added to or dropped from FOR ALL TABLES publications.")
            ),
            other => panic!("expected detailed error, got {other:?}"),
        }

        let schema_err = session
            .execute(&db, "alter publication pub add tables in schema public")
            .unwrap_err();
        match schema_err {
            ExecError::DetailedError { detail, .. } => assert_eq!(
                detail.as_deref(),
                Some("Schemas cannot be added to or dropped from FOR ALL TABLES publications.")
            ),
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn publication_membership_errors_use_relation_names() {
        let base = temp_dir("membership_relation_names");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create table gadgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create publication pub for table widgets")
            .unwrap();

        let duplicate = session
            .execute(&db, "alter publication pub add table widgets")
            .unwrap_err();
        let duplicate_text = format!("{duplicate:?}");
        assert!(duplicate_text.contains("widgets"));
        assert!(duplicate_text.contains("is already member of publication"));

        let missing = session
            .execute(&db, "alter publication pub drop table gadgets")
            .unwrap_err();
        let missing_text = format!("{missing:?}");
        assert!(missing_text.contains("gadgets"));
        assert!(missing_text.contains("is not member of publication"));
    }

    #[test]
    fn publication_add_relation_errors_match_postgres_text() {
        let base = temp_dir("add_relation_errors");
        let db = Database::open(&base, 16).unwrap();
        let temp_relation = BoundRelation {
            rel: crate::backend::storage::smgr::RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: 1,
            },
            relation_oid: 1,
            toast: None,
            namespace_oid: crate::include::catalog::PUBLIC_NAMESPACE_OID,
            owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            of_type_oid: 0,
            relpersistence: 't',
            relkind: 'r',
            relispopulated: true,
            relispartition: false,
            relpartbound: None,
            desc: crate::include::nodes::primnodes::RelationDesc {
                columns: Vec::new(),
            },
            partitioned_table: None,
            partition_spec: None,
        };

        let temp_err =
            validate_publishable_relation(&db, 1, &temp_relation, "temp_items").unwrap_err();
        match temp_err {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(message, "cannot add relation \"temp_items\" to publication");
                assert_eq!(
                    detail.as_deref(),
                    Some("This operation is not supported for temporary tables.")
                );
                assert_eq!(sqlstate, "22023");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }

        let unlogged_relation = BoundRelation {
            rel: crate::backend::storage::smgr::RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: 2,
            },
            relation_oid: 2,
            toast: None,
            namespace_oid: crate::include::catalog::PUBLIC_NAMESPACE_OID,
            owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            of_type_oid: 0,
            relpersistence: 'u',
            relkind: 'r',
            relispopulated: true,
            relispartition: false,
            relpartbound: None,
            desc: crate::include::nodes::primnodes::RelationDesc {
                columns: Vec::new(),
            },
            partitioned_table: None,
            partition_spec: None,
        };

        let unlogged_err =
            validate_publishable_relation(&db, 1, &unlogged_relation, "unlogged_items")
                .unwrap_err();
        match unlogged_err {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(
                    message,
                    "cannot add relation \"unlogged_items\" to publication"
                );
                assert_eq!(
                    detail.as_deref(),
                    Some("This operation is not supported for unlogged tables.")
                );
                assert_eq!(sqlstate, "22023");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn publication_add_schema_errors_match_postgres_text() {
        let base = temp_dir("add_schema_errors");
        let db = Database::open(&base, 16).unwrap();

        let system_err =
            validate_publishable_schema(&db, 1, "pg_catalog", PG_CATALOG_NAMESPACE_OID)
                .unwrap_err();
        match system_err {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(message, "cannot add schema \"pg_catalog\" to publication");
                assert_eq!(
                    detail.as_deref(),
                    Some("This operation is not supported for system schemas.")
                );
                assert_eq!(sqlstate, "22023");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn session_authorization_path_can_create_schema_qualified_publication_memberships() {
        let base = temp_dir("session_auth_schema_membership");
        let db = Database::open(&base, 16).unwrap();
        let mut admin = Session::new(1);
        admin
            .execute(&db, "create role regress_publication_user login superuser")
            .unwrap();

        let mut session = Session::new(2);
        session
            .execute(&db, "set session authorization regress_publication_user")
            .unwrap();
        session.execute(&db, "create schema pub_test").unwrap();
        session
            .execute(&db, "create table testpub_tbl1 (id int4)")
            .unwrap();
        session
            .execute(
                &db,
                "create table pub_test.testpub_nopk (foo int4, bar int4)",
            )
            .unwrap();
        session
            .execute(&db, "create publication pub for table testpub_tbl1")
            .unwrap();
        session
            .execute(&db, "alter publication pub add tables in schema pub_test")
            .unwrap();

        let publication_oid = db
            .backend_catcache(2, None)
            .unwrap()
            .publication_row_by_name("pub")
            .map(|row| row.oid)
            .unwrap();
        let rows = match session
            .execute(
                &db,
                &format!(
                    "select n.nspname \
                     from pg_catalog.pg_namespace n \
                          join pg_catalog.pg_publication_namespace pn on n.oid = pn.pnnspid \
                     where pn.pnpubid = '{}' \
                     order by 1",
                    publication_oid
                ),
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        };
        assert_eq!(rows, vec![vec![Value::Text("pub_test".into())]]);
    }

    #[test]
    fn create_publication_stores_table_filter_and_column_list() {
        let base = temp_dir("table_filter_column_list");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4, name text)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for table only widgets(id) where (id > 0)",
            )
            .unwrap();

        let catcache = db.backend_catcache(1, None).unwrap();
        let publication_oid = catcache.publication_row_by_name("pub").unwrap().oid;
        let rows = catcache.publication_rel_rows_for_publication(publication_oid);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].prqual.as_deref(), Some("(id > 0)"));
        assert_eq!(rows[0].prattrs, Some(vec![1]));
    }

    #[test]
    fn alter_publication_drop_rejects_where_clause_without_losing_publication() {
        let base = temp_dir("drop_filter_keeps_publication");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for table widgets where (id > 0)",
            )
            .unwrap();

        let err = session
            .execute(
                &db,
                "alter publication pub drop table widgets where (id > 0)",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(
                    message,
                    "cannot use a WHERE clause when removing a table from a publication"
                );
                assert_eq!(sqlstate, "42601");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }

        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .publication_row_by_name("pub")
                .is_some()
        );
    }

    #[test]
    fn publication_row_filter_rejects_invalid_expressions() {
        let base = temp_dir("invalid_publication_row_filters");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4, note text)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for table widgets where (id > 0)",
            )
            .unwrap();

        let duplicate = session
            .execute(
                &db,
                "create publication dup for table widgets where (id > 0), widgets",
            )
            .unwrap_err();
        match duplicate {
            ExecError::DetailedError { message, .. } => assert_eq!(
                message,
                "conflicting or redundant WHERE clauses for table \"widgets\""
            ),
            other => panic!("expected duplicate filter error, got {other:?}"),
        }

        let non_bool = session
            .execute(&db, "alter publication pub set table widgets where (1234)")
            .unwrap_err();
        match non_bool {
            ExecError::DetailedError { message, .. } => assert_eq!(
                message,
                "argument of PUBLICATION WHERE must be type boolean, not type integer"
            ),
            other => panic!("expected non-boolean filter error, got {other:?}"),
        }

        let aggregate = session
            .execute(
                &db,
                "alter publication pub set table widgets where (id < avg(id))",
            )
            .unwrap_err();
        match aggregate {
            ExecError::DetailedError { message, .. } => {
                assert_eq!(message, "aggregate functions are not allowed in WHERE");
            }
            other => panic!("expected aggregate filter error, got {other:?}"),
        }
    }
}
