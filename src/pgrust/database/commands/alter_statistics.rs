use super::super::*;
use crate::backend::utils::misc::notices::{push_notice, push_warning};
use crate::pgrust::database::commands::create_statistics::resolve_statistics_name_for_lookup;
use crate::pgrust::database::ddl::normalize_statistics_target;

impl Database {
    pub(crate) fn execute_alter_statistics_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterStatisticsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_alter_statistics_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            0,
            0,
            configured_search_path,
            &mut Vec::new(),
        )
    }

    pub(crate) fn execute_alter_statistics_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &crate::backend::parser::AlterStatisticsStatement,
        _xid: TransactionId,
        _cid: CommandId,
        configured_search_path: Option<&[String]>,
        _catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let statistics_target = normalize_statistics_target(alter_stmt.statistics_target)?;
        let Some(name) = resolve_statistics_name_for_lookup(
            self,
            client_id,
            None,
            &alter_stmt.statistics_name,
            configured_search_path,
        ) else {
            if alter_stmt.if_exists {
                push_notice(format!(
                    "statistics object \"{}\" does not exist, skipping",
                    alter_stmt.statistics_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "statistics object \"{}\" does not exist",
                    alter_stmt.statistics_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };

        if let Some(entry) = self.statistics_objects.write().get_mut(&name) {
            entry.statistics_target = statistics_target.value;
        }
        if let Some(warning) = statistics_target.warning {
            push_warning(warning);
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
