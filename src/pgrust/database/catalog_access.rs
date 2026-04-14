use super::*;

impl Database {
    pub(crate) fn temp_db_oid(client_id: ClientId) -> u32 {
        TEMP_DB_OID_BASE.saturating_add(client_id)
    }

    pub(crate) fn temp_namespace_name(client_id: ClientId) -> String {
        format!("pg_temp_{client_id}")
    }

    pub(crate) fn temp_namespace_oid(client_id: ClientId) -> u32 {
        Self::temp_db_oid(client_id)
    }

    pub(crate) fn temp_toast_namespace_name(client_id: ClientId) -> String {
        format!("pg_toast_temp_{client_id}")
    }

    pub(crate) fn temp_toast_namespace_oid(client_id: ClientId) -> u32 {
        TEMP_TOAST_NAMESPACE_OID_BASE.saturating_add(client_id)
    }

    #[cfg(test)]
    pub(super) fn has_active_temp_namespace(&self, client_id: ClientId) -> bool {
        self.temp_relations.read().contains_key(&client_id)
    }

    pub(super) fn owned_temp_namespace(&self, client_id: ClientId) -> Option<TempNamespace> {
        self.temp_relations.read().get(&client_id).cloned()
    }

    pub(crate) fn other_session_temp_namespace_oid(
        &self,
        client_id: ClientId,
        namespace_oid: u32,
    ) -> bool {
        (namespace_oid >= TEMP_DB_OID_BASE
            && namespace_oid < TEMP_TOAST_NAMESPACE_OID_BASE
            && namespace_oid != Self::temp_namespace_oid(client_id))
            || (namespace_oid >= TEMP_TOAST_NAMESPACE_OID_BASE
                && namespace_oid != Self::temp_toast_namespace_oid(client_id))
    }

    pub(super) fn invalidate_session_catalog_state(&self, client_id: ClientId) {
        invalidate_session_catalog_state(self, client_id);
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
        stmt: &CreateTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, TablePersistence), ParseError> {
        namespace_normalize_create_table_stmt_with_search_path(stmt, configured_search_path)
    }

    pub(super) fn normalize_create_table_as_stmt_with_search_path(
        &self,
        stmt: &CreateTableAsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, TablePersistence), ParseError> {
        namespace_normalize_create_table_as_stmt_with_search_path(stmt, configured_search_path)
    }

    pub(super) fn normalize_create_view_stmt_with_search_path(
        &self,
        stmt: &CreateViewStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<String, ParseError> {
        if stmt
            .schema_name
            .as_deref()
            .is_some_and(|schema| schema.eq_ignore_ascii_case("pg_temp"))
        {
            return Err(ParseError::UnexpectedToken {
                expected: "permanent view",
                actual: "temporary view".into(),
            });
        }
        namespace_normalize_create_view_stmt_with_search_path(stmt, configured_search_path)
    }

    pub(crate) fn lazy_catalog_lookup(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
    ) -> LazyCatalogLookup<'_> {
        let search_path = self.effective_search_path(client_id, configured_search_path);
        LazyCatalogLookup {
            db: self,
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
