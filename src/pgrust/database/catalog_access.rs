use super::*;
use crate::backend::utils::cache::syscache::{SearchSysCache1, SysCacheId, SysCacheTuple};

impl Database {
    pub(crate) fn temp_db_oid(temp_backend_id: TempBackendId) -> u32 {
        TEMP_DB_OID_BASE.saturating_add(temp_backend_id)
    }

    pub(crate) fn temp_namespace_name(temp_backend_id: TempBackendId) -> String {
        format!("pg_temp_{temp_backend_id}")
    }

    pub(crate) fn temp_namespace_oid(temp_backend_id: TempBackendId) -> u32 {
        Self::temp_db_oid(temp_backend_id)
    }

    pub(crate) fn temp_toast_namespace_name(temp_backend_id: TempBackendId) -> String {
        format!("pg_toast_temp_{temp_backend_id}")
    }

    pub(crate) fn temp_toast_namespace_oid(temp_backend_id: TempBackendId) -> u32 {
        TEMP_TOAST_NAMESPACE_OID_BASE.saturating_add(temp_backend_id)
    }

    pub(crate) fn has_active_temp_namespace(&self, client_id: ClientId) -> bool {
        self.temp_relations
            .read()
            .contains_key(&self.temp_backend_id(client_id))
    }

    pub(super) fn owned_temp_namespace(&self, client_id: ClientId) -> Option<TempNamespace> {
        self.temp_relations
            .read()
            .get(&self.temp_backend_id(client_id))
            .cloned()
    }

    pub(crate) fn other_session_temp_namespace_oid(
        &self,
        client_id: ClientId,
        namespace_oid: u32,
    ) -> bool {
        let temp_backend_id = self.temp_backend_id(client_id);
        (namespace_oid >= TEMP_DB_OID_BASE
            && namespace_oid < TEMP_TOAST_NAMESPACE_OID_BASE
            && namespace_oid != Self::temp_namespace_oid(temp_backend_id))
            || (namespace_oid >= TEMP_TOAST_NAMESPACE_OID_BASE
                && namespace_oid != Self::temp_toast_namespace_oid(temp_backend_id))
    }

    pub(super) fn invalidate_backend_cache_state(&self, client_id: ClientId) {
        invalidate_backend_cache_state(self, client_id);
    }

    fn visible_namespace_by_name(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        schema_name: &str,
    ) -> Option<crate::include::catalog::PgNamespaceRow> {
        if let Some(namespace) = self.owned_temp_namespace(client_id)
            && (schema_name.eq_ignore_ascii_case("pg_temp")
                || namespace.name.eq_ignore_ascii_case(schema_name))
        {
            return Some(crate::include::catalog::PgNamespaceRow {
                oid: namespace.oid,
                nspname: namespace.name,
                nspowner: namespace.owner_oid,
                nspacl: None,
            });
        }

        let mut lookup_names = vec![schema_name.to_string()];
        let folded = schema_name.to_ascii_lowercase();
        if folded != schema_name {
            lookup_names.push(folded);
        }
        lookup_names.into_iter().find_map(|lookup_name| {
            SearchSysCache1(
                self,
                client_id,
                txn_ctx,
                SysCacheId::NAMESPACENAME,
                Value::Text(lookup_name.into()),
            )
            .ok()?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Namespace(row) => Some(row),
                _ => None,
            })
            .filter(|row| !self.other_session_temp_namespace_oid(client_id, row.oid))
        })
    }

    pub(crate) fn visible_namespace_oid_by_name(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        schema_name: &str,
    ) -> Option<u32> {
        self.visible_namespace_by_name(client_id, txn_ctx, schema_name)
            .map(|row| row.oid)
    }

    fn resolve_create_relation_target(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        explicit_schema_name: Option<&str>,
        object_name: &str,
        persistence: TablePersistence,
        configured_search_path: Option<&[String]>,
        allow_temporary_namespace: bool,
    ) -> Result<(String, u32, TablePersistence), ParseError> {
        let relation_name = object_name.to_string();
        let temp_backend_id = self.temp_backend_id(client_id);
        let temp_namespace = self.owned_temp_namespace(client_id);
        let is_temp_schema_name = |schema: &str| {
            schema.eq_ignore_ascii_case("pg_temp")
                || temp_namespace
                    .as_ref()
                    .is_some_and(|ns| ns.name.eq_ignore_ascii_case(schema))
        };

        if let Some(schema_name) = explicit_schema_name {
            let normalized_schema = schema_name.to_ascii_lowercase();
            if normalized_schema == "pg_catalog" {
                return Err(ParseError::UnsupportedQualifiedName(format!(
                    "{normalized_schema}.{relation_name}"
                )));
            }
            if is_temp_schema_name(&normalized_schema) {
                if persistence == TablePersistence::Unlogged {
                    return Err(ParseError::DetailedError {
                        message: "only temporary relations may be created in temporary schemas"
                            .into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                if !allow_temporary_namespace {
                    return Err(ParseError::UnexpectedToken {
                        expected: "permanent view",
                        actual: "temporary view".into(),
                    });
                }
                return Ok((
                    relation_name,
                    Self::temp_namespace_oid(temp_backend_id),
                    TablePersistence::Temporary,
                ));
            }
            let namespace = self
                .visible_namespace_by_name(client_id, txn_ctx, schema_name)
                .ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "existing schema",
                    actual: format!("schema \"{schema_name}\" does not exist"),
                })?;
            let storage_name = if namespace.oid == PUBLIC_NAMESPACE_OID {
                relation_name.clone()
            } else {
                format!("{}.{}", namespace.nspname, relation_name)
            };
            return Ok((storage_name, namespace.oid, persistence));
        }

        if allow_temporary_namespace && persistence == TablePersistence::Temporary {
            return Ok((
                relation_name.clone(),
                Self::temp_namespace_oid(temp_backend_id),
                TablePersistence::Temporary,
            ));
        }

        let configured_path = configured_search_path.map(|search_path| {
            search_path
                .iter()
                .map(|schema| schema.trim().to_ascii_lowercase())
                .filter(|schema| !schema.is_empty())
                .collect::<Vec<_>>()
        });
        let search_path = configured_path.unwrap_or_else(|| vec!["public".into()]);

        for schema_name in search_path {
            if schema_name.is_empty() || schema_name == "$user" || schema_name == "pg_catalog" {
                continue;
            }
            if allow_temporary_namespace && is_temp_schema_name(&schema_name) {
                if persistence == TablePersistence::Unlogged {
                    return Err(ParseError::DetailedError {
                        message: "only temporary relations may be created in temporary schemas"
                            .into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                return Ok((
                    relation_name.clone(),
                    Self::temp_namespace_oid(temp_backend_id),
                    TablePersistence::Temporary,
                ));
            }
            if let Some(namespace) =
                self.visible_namespace_by_name(client_id, txn_ctx, &schema_name)
            {
                let storage_name = if namespace.oid == PUBLIC_NAMESPACE_OID {
                    relation_name.clone()
                } else {
                    format!("{}.{}", namespace.nspname, relation_name)
                };
                return Ok((storage_name, namespace.oid, persistence));
            }
        }

        Err(ParseError::NoSchemaSelectedForCreate)
    }

    pub(crate) fn effective_search_path(
        &self,
        client_id: ClientId,
        configured_search_path: Option<&[String]>,
    ) -> Vec<String> {
        namespace_effective_search_path(
            self.owned_temp_namespace(client_id)
                .as_ref()
                .map(|ns| ns.name.as_str()),
            configured_search_path,
        )
    }

    pub(super) fn normalize_create_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        stmt: &CreateTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, u32, TablePersistence), ParseError> {
        let (table_name, persistence) = normalize_create_table_name(stmt)?;
        self.resolve_create_relation_target(
            client_id,
            txn_ctx,
            stmt.schema_name.as_deref(),
            &table_name,
            persistence,
            configured_search_path,
            true,
        )
    }

    pub(super) fn normalize_create_table_as_stmt_with_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        stmt: &CreateTableAsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, u32, TablePersistence), ParseError> {
        let (table_name, persistence) = normalize_create_table_as_name(stmt)?;
        self.resolve_create_relation_target(
            client_id,
            txn_ctx,
            stmt.schema_name.as_deref(),
            &table_name,
            persistence,
            configured_search_path,
            true,
        )
    }

    pub(super) fn normalize_create_view_stmt_with_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        stmt: &CreateViewStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, u32), ParseError> {
        let view_name = normalize_create_view_name(stmt)?;
        let (storage_name, namespace_oid, _) = self.resolve_create_relation_target(
            client_id,
            txn_ctx,
            stmt.schema_name.as_deref(),
            &view_name,
            stmt.persistence,
            configured_search_path,
            false,
        )?;
        Ok((storage_name, namespace_oid))
    }

    pub(super) fn normalize_create_type_name_with_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        schema_name: Option<&str>,
        type_name: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, u32), ParseError> {
        let lowered_name = type_name.to_ascii_lowercase();
        let temp_namespace = self.owned_temp_namespace(client_id);
        let is_temp_schema_name = |schema: &str| {
            schema.eq_ignore_ascii_case("pg_temp")
                || temp_namespace
                    .as_ref()
                    .is_some_and(|ns| ns.name.eq_ignore_ascii_case(schema))
        };

        if let Some(schema_name) = schema_name {
            let normalized_schema = schema_name.to_ascii_lowercase();
            if normalized_schema == "pg_catalog" {
                return Err(ParseError::UnsupportedQualifiedName(format!(
                    "{normalized_schema}.{lowered_name}"
                )));
            }
            if is_temp_schema_name(&normalized_schema) {
                return Err(ParseError::UnexpectedToken {
                    expected: "permanent type",
                    actual: "temporary type".into(),
                });
            }
            let namespace = self
                .visible_namespace_by_name(client_id, txn_ctx, &normalized_schema)
                .ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "existing schema",
                    actual: format!("schema \"{normalized_schema}\" does not exist"),
                })?;
            let storage_name = if namespace.oid == PUBLIC_NAMESPACE_OID {
                lowered_name
            } else {
                format!("{}.{}", namespace.nspname, lowered_name)
            };
            return Ok((storage_name, namespace.oid));
        }

        let search_path = self.effective_search_path(client_id, configured_search_path);
        for schema_name in search_path {
            if schema_name.is_empty() || schema_name == "$user" || schema_name == "pg_catalog" {
                continue;
            }
            if is_temp_schema_name(&schema_name) {
                continue;
            }
            if let Some(namespace) =
                self.visible_namespace_by_name(client_id, txn_ctx, &schema_name)
            {
                let storage_name = if namespace.oid == PUBLIC_NAMESPACE_OID {
                    lowered_name.clone()
                } else {
                    format!("{}.{}", namespace.nspname, lowered_name)
                };
                return Ok((storage_name, namespace.oid));
            }
        }

        Err(ParseError::NoSchemaSelectedForCreate)
    }

    pub(super) fn normalize_create_type_stmt_with_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        stmt: &CreateCompositeTypeStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, u32), ParseError> {
        self.normalize_create_type_name_with_search_path(
            client_id,
            txn_ctx,
            stmt.schema_name.as_deref(),
            &stmt.type_name,
            configured_search_path,
        )
    }

    pub(crate) fn lazy_catalog_lookup(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
    ) -> LazyCatalogLookup {
        if txn_ctx.is_none() {
            self.accept_invalidation_messages(client_id);
        }
        let search_path = self.effective_search_path(client_id, configured_search_path);
        LazyCatalogLookup {
            db: self.clone(),
            client_id,
            txn_ctx,
            search_path,
        }
    }

    pub(crate) fn describe_relation_by_oid(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> Option<RelCacheEntry> {
        describe_relation_by_oid(self, client_id, txn_ctx, relation_oid)
    }

    pub(crate) fn relation_namespace_name(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> Option<String> {
        relation_namespace_name(self, client_id, txn_ctx, relation_oid)
    }

    pub(crate) fn relation_display_name(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
        relation_oid: u32,
    ) -> Option<String> {
        relation_display_name(
            self,
            client_id,
            txn_ctx,
            configured_search_path,
            relation_oid,
        )
    }

    pub(super) fn normalize_create_sequence_stmt_with_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        stmt: &CreateSequenceStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, u32, TablePersistence), ParseError> {
        self.resolve_create_relation_target(
            client_id,
            txn_ctx,
            stmt.schema_name.as_deref(),
            &stmt.sequence_name,
            stmt.persistence,
            configured_search_path,
            true,
        )
    }

    pub(crate) fn has_index_on_relation(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> bool {
        has_index_on_relation(self, client_id, txn_ctx, relation_oid)
    }

    pub(crate) fn access_method_name_for_relation(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> Option<String> {
        access_method_name_for_relation(self, client_id, txn_ctx, relation_oid)
    }

    pub(crate) fn constraint_rows_for_relation(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> Vec<PgConstraintRow> {
        constraint_rows_for_relation(self, client_id, txn_ctx, relation_oid)
    }
}
