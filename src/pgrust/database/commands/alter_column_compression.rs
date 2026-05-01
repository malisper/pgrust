use super::super::*;
use crate::backend::parser::{BoundRelation, CatalogLookup};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, validate_alter_table_alter_column_compression,
};

fn lookup_relation_for_alter_column_compression(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'f' | 'm') => Ok(Some(entry)),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table or materialized view",
        })),
        None if if_exists => {
            push_notice(format!(r#"relation "{name}" does not exist, skipping"#));
            Ok(None)
        }
        None => Err(ExecError::Parse(ParseError::UnknownTable(name.to_string()))),
    }
}

impl Database {
    pub(crate) fn execute_alter_table_alter_column_compression_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnCompressionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_relation_for_alter_column_compression(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_alter_table_alter_column_compression_stmt_in_transaction_with_search_path(
                client_id,
                alter_stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_alter_column_compression_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnCompressionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_relation_for_alter_column_compression(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE ALTER COLUMN SET COMPRESSION",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let (column_name, compression) = validate_alter_table_alter_column_compression(
            &relation.desc,
            &alter_stmt.column_name,
            alter_stmt.compression,
        )?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let effect = self
            .catalog
            .write()
            .alter_table_set_column_compression_mvcc(
                relation.relation_oid,
                &column_name,
                compression,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
