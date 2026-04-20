use super::super::*;
use crate::include::catalog::PG_CATALOG_NAMESPACE_OID;
use crate::pgrust::database::ddl::{
    ensure_relation_owner, lookup_heap_relation_for_alter_table,
    validate_alter_table_alter_column_options,
};
use std::collections::BTreeMap;

fn split_attoption(value: &str) -> Option<(String, String)> {
    let (name, option_value) = value.split_once('=')?;
    Some((name.to_string(), option_value.to_string()))
}

fn apply_attoptions_patch(
    current: Option<&Vec<String>>,
    action: &crate::backend::parser::AlterColumnOptionsAction,
) -> Option<Vec<String>> {
    let mut options = current
        .into_iter()
        .flatten()
        .filter_map(|value| split_attoption(value))
        .collect::<BTreeMap<_, _>>();
    match action {
        crate::backend::parser::AlterColumnOptionsAction::Set(new_options) => {
            for option in new_options {
                options.insert(option.name.to_ascii_lowercase(), option.value.clone());
            }
        }
        crate::backend::parser::AlterColumnOptionsAction::Reset(names) => {
            for name in names {
                options.remove(&name.to_ascii_lowercase());
            }
        }
    }
    (!options.is_empty()).then(|| {
        options
            .into_iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect()
    })
}

impl Database {
    pub(crate) fn execute_alter_table_alter_column_options_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnOptionsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
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
            .execute_alter_table_alter_column_options_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_table_alter_column_options_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterTableAlterColumnOptionsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        _catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) = lookup_heap_relation_for_alter_table(
            &catalog,
            &alter_stmt.table_name,
            alter_stmt.if_exists,
        )?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for ALTER TABLE ALTER COLUMN SET/RESET options",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &alter_stmt.table_name)?;
        let column_name =
            validate_alter_table_alter_column_options(&relation.desc, &alter_stmt.column_name)?;
        let current_column = relation
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(&column_name))
            .expect("validated column exists");
        let attoptions =
            apply_attoptions_patch(current_column.attoptions.as_ref(), &alter_stmt.action);

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .alter_table_set_column_options_mvcc(
                relation.relation_oid,
                &column_name,
                attoptions,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        _catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
