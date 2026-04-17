use super::super::*;
use crate::backend::executor::StatementResult;
use crate::backend::parser::{
    CommentOnConversionStatement, CreateConversionStatement, DropConversionStatement,
};

impl Database {
    pub(crate) fn execute_create_conversion_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateConversionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let (normalized, object_name, namespace_oid) = self
            .normalize_conversion_name_for_create(&stmt.conversion_name, configured_search_path)?;
        let current_user_oid = self.auth_state(client_id).current_user_oid();
        let mut conversions = self.conversions.write();
        if conversions.contains_key(&normalized)
            || conversion_lookup_storage_key(&conversions, &stmt.conversion_name)
                .is_some_and(|key| key != normalized)
        {
            return Err(ExecError::DetailedError {
                message: format!("conversion \"{}\" already exists", object_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }

        let for_encoding = stmt.for_encoding.to_ascii_uppercase();
        let to_encoding = stmt.to_encoding.to_ascii_uppercase();
        if stmt.is_default
            && conversions.values().any(|existing| {
                existing.namespace_oid == namespace_oid
                    && existing.is_default
                    && existing.for_encoding.eq_ignore_ascii_case(&for_encoding)
                    && existing.to_encoding.eq_ignore_ascii_case(&to_encoding)
            })
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "default conversion for {} to {} already exists",
                    for_encoding, to_encoding
                ),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }

        let oid = {
            let catalog = self.catalog.write();
            let snapshot = catalog.catalog_snapshot().map_err(map_catalog_error)?;
            let next_catalog_oid = snapshot.next_oid();
            conversions
                .values()
                .map(|conversion| conversion.oid.saturating_add(1))
                .max()
                .unwrap_or(next_catalog_oid)
                .max(next_catalog_oid)
        };
        conversions.insert(
            normalized,
            ConversionEntry {
                oid,
                name: object_name,
                namespace_oid,
                for_encoding,
                to_encoding,
                function_name: stmt.function_name.to_ascii_lowercase(),
                is_default: stmt.is_default,
                owner_oid: current_user_oid,
                comment: None,
            },
        );
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_conversion_stmt_with_search_path(
        &self,
        _client_id: ClientId,
        stmt: &DropConversionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let (normalized, object_name, _) = self
            .normalize_conversion_name_for_create(&stmt.conversion_name, configured_search_path)?;
        let mut conversions = self.conversions.write();
        let storage_key = conversion_lookup_storage_key(&conversions, &stmt.conversion_name)
            .unwrap_or(normalized);
        if conversions.remove(&storage_key).is_none() {
            if stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("conversion \"{}\" does not exist", object_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        }
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_conversion_stmt_with_search_path(
        &self,
        _client_id: ClientId,
        stmt: &CommentOnConversionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let (normalized, object_name, _) = self
            .normalize_conversion_name_for_create(&stmt.conversion_name, configured_search_path)?;
        let mut conversions = self.conversions.write();
        let storage_key = conversion_lookup_storage_key(&conversions, &stmt.conversion_name)
            .unwrap_or(normalized);
        let Some(conversion) = conversions.get_mut(&storage_key) else {
            return Err(ExecError::DetailedError {
                message: format!("conversion \"{}\" does not exist", object_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        conversion.comment = stmt.comment.clone();
        Ok(StatementResult::AffectedRows(0))
    }
}

fn conversion_lookup_storage_key(
    conversions: &std::collections::BTreeMap<String, ConversionEntry>,
    raw_name: &str,
) -> Option<String> {
    let lowered = raw_name.to_ascii_lowercase();
    if conversions.contains_key(&lowered) {
        return Some(lowered);
    }
    let public_name = if lowered.contains('.') {
        lowered
    } else {
        format!("public.{lowered}")
    };
    conversions.contains_key(&public_name).then_some(public_name)
}
