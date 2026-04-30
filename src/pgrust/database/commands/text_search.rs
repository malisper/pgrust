use super::super::*;
use crate::backend::executor::StatementResult;
use crate::backend::parser::{
    AlterTextSearchAction, AlterTextSearchStatement, CreateTextSearchStatement,
    DropTextSearchStatement, TextSearchObjectKind, TextSearchParameter,
};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::{
    DEFAULT_TS_PARSER_OID, PG_CATALOG_NAMESPACE_OID, PUBLIC_NAMESPACE_OID, PgTsConfigMapRow,
    PgTsConfigRow, PgTsDictRow, PgTsParserRow, PgTsTemplateRow,
};
use crate::pgrust::database::ddl::ensure_can_set_role;

fn normalize_text_search_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn text_search_kind_name(kind: TextSearchObjectKind) -> &'static str {
    match kind {
        TextSearchObjectKind::Dictionary => "text search dictionary",
        TextSearchObjectKind::Configuration => "text search configuration",
        TextSearchObjectKind::Template => "text search template",
        TextSearchObjectKind::Parser => "text search parser",
    }
}

fn text_search_short_kind_name(kind: TextSearchObjectKind) -> &'static str {
    match kind {
        TextSearchObjectKind::Dictionary => "dictionary",
        TextSearchObjectKind::Configuration => "configuration",
        TextSearchObjectKind::Template => "template",
        TextSearchObjectKind::Parser => "parser",
    }
}

fn text_search_duplicate_error(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    kind: TextSearchObjectKind,
    object_name: &str,
    namespace_oid: u32,
) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "{} \"{}\" already exists in schema \"{}\"",
            text_search_kind_name(kind),
            object_name,
            namespace_name(db, client_id, txn_ctx, namespace_oid)
                .unwrap_or_else(|_| namespace_oid.to_string())
        ),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn text_search_owner_error(kind: TextSearchObjectKind, object_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "must be owner of {} {}",
            text_search_kind_name(kind),
            object_name
        ),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn text_search_parameter_error(kind: TextSearchObjectKind, parameter: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "text search {} parameter \"{}\" not recognized",
            text_search_short_kind_name(kind),
            parameter
        ),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn namespace_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    namespace_oid: u32,
) -> Result<String, ExecError> {
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .namespace_by_oid(namespace_oid)
        .map(|row| row.nspname.clone())
        .unwrap_or_else(|| namespace_oid.to_string()))
}

fn resolve_text_search_create_namespace(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    schema_name: Option<&str>,
    configured_search_path: Option<&[String]>,
) -> Result<u32, ExecError> {
    if let Some(schema_name) = schema_name {
        return db
            .visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema \"{schema_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            });
    }
    for schema in db.effective_search_path(client_id, configured_search_path) {
        if matches!(schema.as_str(), "" | "$user" | "pg_temp" | "pg_catalog") {
            continue;
        }
        if let Some(namespace_oid) = db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema) {
            return Ok(namespace_oid);
        }
    }
    Ok(PUBLIC_NAMESPACE_OID)
}

fn text_search_lookup_namespaces(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    schema_name: Option<&str>,
    configured_search_path: Option<&[String]>,
    include_pg_catalog: bool,
) -> Result<Vec<u32>, ExecError> {
    if let Some(schema_name) = schema_name {
        return db
            .visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
            .map(|oid| vec![oid])
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema \"{schema_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            });
    }
    let mut namespaces = Vec::new();
    for schema in db.effective_search_path(client_id, configured_search_path) {
        if matches!(schema.as_str(), "" | "$user" | "pg_temp") {
            continue;
        }
        if let Some(namespace_oid) = db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema) {
            namespaces.push(namespace_oid);
        }
    }
    if include_pg_catalog && !namespaces.contains(&PG_CATALOG_NAMESPACE_OID) {
        namespaces.push(PG_CATALOG_NAMESPACE_OID);
    }
    Ok(namespaces)
}

fn parameter_value<'a>(parameters: &'a [TextSearchParameter], name: &str) -> Option<&'a str> {
    parameters
        .iter()
        .find(|parameter| parameter.name.eq_ignore_ascii_case(name))
        .map(|parameter| parameter.value.as_str())
}

fn reject_unknown_parameters(
    kind: TextSearchObjectKind,
    parameters: &[TextSearchParameter],
    valid: &[&str],
) -> Result<(), ExecError> {
    for parameter in parameters {
        if !valid
            .iter()
            .any(|valid_name| parameter.name.eq_ignore_ascii_case(valid_name))
        {
            return Err(text_search_parameter_error(kind, &parameter.name));
        }
    }
    Ok(())
}

fn proc_oid_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    name: &str,
) -> Result<u32, ExecError> {
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .proc_rows()
        .into_iter()
        .find(|row| row.proname.eq_ignore_ascii_case(name))
        .map(|row| row.oid)
        .unwrap_or(0))
}

fn lookup_ts_template(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    raw_name: &str,
    configured_search_path: Option<&[String]>,
    include_pg_catalog: bool,
) -> Result<Option<PgTsTemplateRow>, ExecError> {
    let (schema_name, object_name) = raw_name
        .split_once('.')
        .map(|(schema, name)| (Some(schema), name))
        .unwrap_or((None, raw_name));
    let object_name = normalize_text_search_name(object_name);
    let namespaces = text_search_lookup_namespaces(
        db,
        client_id,
        txn_ctx,
        schema_name,
        configured_search_path,
        include_pg_catalog,
    )?;
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .ts_template_rows()
        .into_iter()
        .find(|row| {
            namespaces.contains(&row.tmplnamespace)
                && row.tmplname.eq_ignore_ascii_case(&object_name)
        }))
}

fn lookup_ts_config(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    raw_name: &str,
    configured_search_path: Option<&[String]>,
    include_pg_catalog: bool,
) -> Result<Option<PgTsConfigRow>, ExecError> {
    let (schema_name, object_name) = raw_name
        .split_once('.')
        .map(|(schema, name)| (Some(schema), name))
        .unwrap_or((None, raw_name));
    let object_name = normalize_text_search_name(object_name);
    let namespaces = text_search_lookup_namespaces(
        db,
        client_id,
        txn_ctx,
        schema_name,
        configured_search_path,
        include_pg_catalog,
    )?;
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .ts_config_rows()
        .into_iter()
        .find(|row| {
            namespaces.contains(&row.cfgnamespace) && row.cfgname.eq_ignore_ascii_case(&object_name)
        }))
}

fn lookup_ts_dict(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    schema_name: Option<&str>,
    object_name: &str,
    configured_search_path: Option<&[String]>,
) -> Result<Option<PgTsDictRow>, ExecError> {
    let object_name = normalize_text_search_name(object_name);
    let namespaces = text_search_lookup_namespaces(
        db,
        client_id,
        txn_ctx,
        schema_name,
        configured_search_path,
        false,
    )?;
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .ts_dict_rows()
        .into_iter()
        .find(|row| {
            namespaces.contains(&row.dictnamespace)
                && row.dictname.eq_ignore_ascii_case(&object_name)
        }))
}

fn lookup_ts_parser(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    schema_name: Option<&str>,
    object_name: &str,
    configured_search_path: Option<&[String]>,
    include_pg_catalog: bool,
) -> Result<Option<PgTsParserRow>, ExecError> {
    let object_name = normalize_text_search_name(object_name);
    let namespaces = text_search_lookup_namespaces(
        db,
        client_id,
        txn_ctx,
        schema_name,
        configured_search_path,
        include_pg_catalog,
    )?;
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .ts_parser_rows()
        .into_iter()
        .find(|row| {
            namespaces.contains(&row.prsnamespace) && row.prsname.eq_ignore_ascii_case(&object_name)
        }))
}

fn lookup_ts_object(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    kind: TextSearchObjectKind,
    schema_name: Option<&str>,
    object_name: &str,
    configured_search_path: Option<&[String]>,
) -> Result<Option<TextSearchCatalogRow>, ExecError> {
    Ok(match kind {
        TextSearchObjectKind::Dictionary => lookup_ts_dict(
            db,
            client_id,
            txn_ctx,
            schema_name,
            object_name,
            configured_search_path,
        )?
        .map(TextSearchCatalogRow::Dictionary),
        TextSearchObjectKind::Configuration => lookup_ts_config(
            db,
            client_id,
            txn_ctx,
            &schema_name
                .map(|schema| format!("{schema}.{object_name}"))
                .unwrap_or_else(|| object_name.to_string()),
            configured_search_path,
            false,
        )?
        .map(TextSearchCatalogRow::Configuration),
        TextSearchObjectKind::Template => lookup_ts_template(
            db,
            client_id,
            txn_ctx,
            &schema_name
                .map(|schema| format!("{schema}.{object_name}"))
                .unwrap_or_else(|| object_name.to_string()),
            configured_search_path,
            false,
        )?
        .map(TextSearchCatalogRow::Template),
        TextSearchObjectKind::Parser => lookup_ts_parser(
            db,
            client_id,
            txn_ctx,
            schema_name,
            object_name,
            configured_search_path,
            false,
        )?
        .map(TextSearchCatalogRow::Parser),
    })
}

#[derive(Clone)]
enum TextSearchCatalogRow {
    Dictionary(PgTsDictRow),
    Configuration(PgTsConfigRow),
    Template(PgTsTemplateRow),
    Parser(PgTsParserRow),
}

fn text_search_row_namespace(row: &TextSearchCatalogRow) -> u32 {
    match row {
        TextSearchCatalogRow::Dictionary(row) => row.dictnamespace,
        TextSearchCatalogRow::Configuration(row) => row.cfgnamespace,
        TextSearchCatalogRow::Template(row) => row.tmplnamespace,
        TextSearchCatalogRow::Parser(row) => row.prsnamespace,
    }
}

fn text_search_row_name(row: &TextSearchCatalogRow) -> &str {
    match row {
        TextSearchCatalogRow::Dictionary(row) => &row.dictname,
        TextSearchCatalogRow::Configuration(row) => &row.cfgname,
        TextSearchCatalogRow::Template(row) => &row.tmplname,
        TextSearchCatalogRow::Parser(row) => &row.prsname,
    }
}

fn text_search_row_oid(row: &TextSearchCatalogRow) -> u32 {
    match row {
        TextSearchCatalogRow::Dictionary(row) => row.oid,
        TextSearchCatalogRow::Configuration(row) => row.oid,
        TextSearchCatalogRow::Template(row) => row.oid,
        TextSearchCatalogRow::Parser(row) => row.oid,
    }
}

fn text_search_object_name_parts(raw_name: &str) -> (Option<&str>, &str) {
    raw_name
        .split_once('.')
        .map(|(schema, name)| (Some(schema), name))
        .unwrap_or((None, raw_name))
}

fn text_search_missing_error(kind: TextSearchObjectKind, object_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "{} \"{}\" does not exist",
            text_search_kind_name(kind),
            object_name.rsplit('.').next().unwrap_or(object_name)
        ),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn push_missing_text_search_notice(kind: TextSearchObjectKind, object_name: &str) {
    push_notice(format!(
        "{} \"{}\" does not exist, skipping",
        text_search_kind_name(kind),
        object_name.rsplit('.').next().unwrap_or(object_name)
    ));
}

fn text_search_object_display(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    row: &TextSearchCatalogRow,
) -> Result<String, ExecError> {
    let schema = namespace_name(db, client_id, txn_ctx, text_search_row_namespace(row))?;
    Ok(format!("{schema}.{}", text_search_row_name(row)))
}

fn text_search_dependency_error(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    target: &TextSearchCatalogRow,
    dependent: &TextSearchCatalogRow,
) -> ExecError {
    let target_kind = text_search_kind_name(match target {
        TextSearchCatalogRow::Dictionary(_) => TextSearchObjectKind::Dictionary,
        TextSearchCatalogRow::Configuration(_) => TextSearchObjectKind::Configuration,
        TextSearchCatalogRow::Template(_) => TextSearchObjectKind::Template,
        TextSearchCatalogRow::Parser(_) => TextSearchObjectKind::Parser,
    });
    let dependent_kind = text_search_kind_name(match dependent {
        TextSearchCatalogRow::Dictionary(_) => TextSearchObjectKind::Dictionary,
        TextSearchCatalogRow::Configuration(_) => TextSearchObjectKind::Configuration,
        TextSearchCatalogRow::Template(_) => TextSearchObjectKind::Template,
        TextSearchCatalogRow::Parser(_) => TextSearchObjectKind::Parser,
    });
    let target_display = text_search_object_display(db, client_id, txn_ctx, target)
        .unwrap_or_else(|_| text_search_row_name(target).to_string());
    let dependent_display = text_search_object_display(db, client_id, txn_ctx, dependent)
        .unwrap_or_else(|_| text_search_row_name(dependent).to_string());
    ExecError::DetailedError {
        message: format!(
            "cannot drop {target_kind} {target_display} because other objects depend on it"
        ),
        detail: Some(format!(
            "{dependent_kind} {dependent_display} depends on {target_kind} {target_display}"
        )),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    }
}

fn ensure_text_search_superuser(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    kind: TextSearchObjectKind,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|role| role.rolsuper)
    {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be superuser to drop {}", text_search_kind_name(kind)),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn ensure_text_search_drop_permission(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    row: &TextSearchCatalogRow,
) -> Result<(), ExecError> {
    match row {
        TextSearchCatalogRow::Dictionary(row) => ensure_text_search_owner(
            db,
            client_id,
            txn_ctx,
            TextSearchObjectKind::Dictionary,
            row.dictowner,
            &row.dictname,
        ),
        TextSearchCatalogRow::Configuration(row) => ensure_text_search_owner(
            db,
            client_id,
            txn_ctx,
            TextSearchObjectKind::Configuration,
            row.cfgowner,
            &row.cfgname,
        ),
        TextSearchCatalogRow::Template(_) => {
            ensure_text_search_superuser(db, client_id, txn_ctx, TextSearchObjectKind::Template)
        }
        TextSearchCatalogRow::Parser(_) => {
            ensure_text_search_superuser(db, client_id, txn_ctx, TextSearchObjectKind::Parser)
        }
    }
}

struct TextSearchCatalogSnapshot {
    configs: Vec<PgTsConfigRow>,
    config_maps: Vec<PgTsConfigMapRow>,
    dicts: Vec<PgTsDictRow>,
}

#[derive(Default)]
struct TextSearchDropPlan {
    configs: Vec<PgTsConfigRow>,
    dicts: Vec<PgTsDictRow>,
    templates: Vec<PgTsTemplateRow>,
    parsers: Vec<PgTsParserRow>,
}

impl TextSearchDropPlan {
    fn contains(&self, row: &TextSearchCatalogRow) -> bool {
        match row {
            TextSearchCatalogRow::Configuration(row) => {
                self.configs.iter().any(|existing| existing.oid == row.oid)
            }
            TextSearchCatalogRow::Dictionary(row) => {
                self.dicts.iter().any(|existing| existing.oid == row.oid)
            }
            TextSearchCatalogRow::Template(row) => self
                .templates
                .iter()
                .any(|existing| existing.oid == row.oid),
            TextSearchCatalogRow::Parser(row) => {
                self.parsers.iter().any(|existing| existing.oid == row.oid)
            }
        }
    }

    fn push(&mut self, row: TextSearchCatalogRow) {
        if self.contains(&row) {
            return;
        }
        match row {
            TextSearchCatalogRow::Configuration(row) => self.configs.push(row),
            TextSearchCatalogRow::Dictionary(row) => self.dicts.push(row),
            TextSearchCatalogRow::Template(row) => self.templates.push(row),
            TextSearchCatalogRow::Parser(row) => self.parsers.push(row),
        }
    }
}

fn collect_text_search_drop_plan(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    target: TextSearchCatalogRow,
    cascade: bool,
    snapshot: &TextSearchCatalogSnapshot,
    plan: &mut TextSearchDropPlan,
) -> Result<(), ExecError> {
    if plan.contains(&target) {
        return Ok(());
    }
    match &target {
        TextSearchCatalogRow::Configuration(_) => {}
        TextSearchCatalogRow::Dictionary(row) => {
            let dependent_configs = snapshot.configs.iter().filter(|config| {
                snapshot
                    .config_maps
                    .iter()
                    .any(|map| map.mapcfg == config.oid && map.mapdict == row.oid)
            });
            for config in dependent_configs {
                let dependent = TextSearchCatalogRow::Configuration(config.clone());
                if !cascade {
                    return Err(text_search_dependency_error(
                        db, client_id, txn_ctx, &target, &dependent,
                    ));
                }
                push_notice(format!(
                    "drop cascades to {} {}",
                    text_search_kind_name(TextSearchObjectKind::Configuration),
                    text_search_object_display(db, client_id, txn_ctx, &dependent)
                        .unwrap_or_else(|_| config.cfgname.clone())
                ));
                collect_text_search_drop_plan(
                    db, client_id, txn_ctx, dependent, cascade, snapshot, plan,
                )?;
            }
        }
        TextSearchCatalogRow::Template(row) => {
            let dependent_dicts = snapshot
                .dicts
                .iter()
                .filter(|dict| dict.dicttemplate == row.oid);
            for dict in dependent_dicts {
                let dependent = TextSearchCatalogRow::Dictionary(dict.clone());
                if !cascade {
                    return Err(text_search_dependency_error(
                        db, client_id, txn_ctx, &target, &dependent,
                    ));
                }
                push_notice(format!(
                    "drop cascades to {} {}",
                    text_search_kind_name(TextSearchObjectKind::Dictionary),
                    text_search_object_display(db, client_id, txn_ctx, &dependent)
                        .unwrap_or_else(|_| dict.dictname.clone())
                ));
                collect_text_search_drop_plan(
                    db, client_id, txn_ctx, dependent, cascade, snapshot, plan,
                )?;
            }
        }
        TextSearchCatalogRow::Parser(row) => {
            let dependent_configs = snapshot
                .configs
                .iter()
                .filter(|config| config.cfgparser == row.oid);
            for config in dependent_configs {
                let dependent = TextSearchCatalogRow::Configuration(config.clone());
                if !cascade {
                    return Err(text_search_dependency_error(
                        db, client_id, txn_ctx, &target, &dependent,
                    ));
                }
                push_notice(format!(
                    "drop cascades to {} {}",
                    text_search_kind_name(TextSearchObjectKind::Configuration),
                    text_search_object_display(db, client_id, txn_ctx, &dependent)
                        .unwrap_or_else(|_| config.cfgname.clone())
                ));
                collect_text_search_drop_plan(
                    db, client_id, txn_ctx, dependent, cascade, snapshot, plan,
                )?;
            }
        }
    }
    plan.push(target);
    Ok(())
}

fn duplicate_text_search_exists(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    kind: TextSearchObjectKind,
    object_name: &str,
    namespace_oid: u32,
    current_oid: Option<u32>,
) -> Result<bool, ExecError> {
    let catcache = db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    let exists = match kind {
        TextSearchObjectKind::Dictionary => catcache.ts_dict_rows().into_iter().any(|row| {
            row.dictnamespace == namespace_oid
                && row.dictname.eq_ignore_ascii_case(object_name)
                && current_oid.is_none_or(|oid| row.oid != oid)
        }),
        TextSearchObjectKind::Configuration => catcache.ts_config_rows().into_iter().any(|row| {
            row.cfgnamespace == namespace_oid
                && row.cfgname.eq_ignore_ascii_case(object_name)
                && current_oid.is_none_or(|oid| row.oid != oid)
        }),
        TextSearchObjectKind::Template => catcache.ts_template_rows().into_iter().any(|row| {
            row.tmplnamespace == namespace_oid
                && row.tmplname.eq_ignore_ascii_case(object_name)
                && current_oid.is_none_or(|oid| row.oid != oid)
        }),
        TextSearchObjectKind::Parser => catcache.ts_parser_rows().into_iter().any(|row| {
            row.prsnamespace == namespace_oid
                && row.prsname.eq_ignore_ascii_case(object_name)
                && current_oid.is_none_or(|oid| row.oid != oid)
        }),
    };
    Ok(exists)
}

fn ensure_text_search_owner(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    kind: TextSearchObjectKind,
    owner_oid: u32,
    object_name: &str,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth.can_set_role(owner_oid, &auth_catalog) {
        Ok(())
    } else {
        Err(text_search_owner_error(kind, object_name))
    }
}

impl Database {
    pub(crate) fn execute_create_text_search_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateTextSearchStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_text_search_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_create_text_search_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateTextSearchStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let object_name = normalize_text_search_name(&stmt.object_name);
        let namespace_oid = resolve_text_search_create_namespace(
            self,
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            configured_search_path,
        )?;
        if duplicate_text_search_exists(
            self,
            client_id,
            Some((xid, cid)),
            stmt.kind,
            &object_name,
            namespace_oid,
            None,
        )? {
            return Err(text_search_duplicate_error(
                self,
                client_id,
                Some((xid, cid)),
                stmt.kind,
                &object_name,
                namespace_oid,
            ));
        }

        let current_user_oid = self.auth_state(client_id).current_user_oid();
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = match stmt.kind {
            TextSearchObjectKind::Dictionary => {
                reject_unknown_parameters(stmt.kind, &stmt.parameters, &["template"])?;
                let template_name = parameter_value(&stmt.parameters, "template")
                    .ok_or_else(|| text_search_parameter_error(stmt.kind, "template"))?;
                let template = lookup_ts_template(
                    self,
                    client_id,
                    Some((xid, cid)),
                    template_name,
                    configured_search_path,
                    true,
                )?
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("text search template \"{template_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
                let row = PgTsDictRow {
                    oid: 0,
                    dictname: object_name,
                    dictnamespace: namespace_oid,
                    dictowner: current_user_oid,
                    dicttemplate: template.oid,
                    dictinitoption: None,
                };
                self.catalog
                    .write()
                    .create_ts_dict_mvcc(row, &ctx)
                    .map(|(_, effect)| effect)?
            }
            TextSearchObjectKind::Configuration => {
                reject_unknown_parameters(stmt.kind, &stmt.parameters, &["copy", "parser"])?;
                let cfgparser = if let Some(copy_name) = parameter_value(&stmt.parameters, "copy") {
                    lookup_ts_config(
                        self,
                        client_id,
                        Some((xid, cid)),
                        copy_name,
                        configured_search_path,
                        true,
                    )?
                    .map(|row| row.cfgparser)
                    .unwrap_or(DEFAULT_TS_PARSER_OID)
                } else if let Some(parser_name) = parameter_value(&stmt.parameters, "parser") {
                    lookup_ts_parser(
                        self,
                        client_id,
                        Some((xid, cid)),
                        None,
                        parser_name,
                        configured_search_path,
                        true,
                    )?
                    .map(|row| row.oid)
                    .unwrap_or(DEFAULT_TS_PARSER_OID)
                } else {
                    DEFAULT_TS_PARSER_OID
                };
                let row = PgTsConfigRow {
                    oid: 0,
                    cfgname: object_name,
                    cfgnamespace: namespace_oid,
                    cfgowner: current_user_oid,
                    cfgparser,
                };
                self.catalog
                    .write()
                    .create_ts_config_mvcc(row, &ctx)
                    .map(|(_, effect)| effect)?
            }
            TextSearchObjectKind::Template => {
                reject_unknown_parameters(stmt.kind, &stmt.parameters, &["init", "lexize"])?;
                let tmplinit = parameter_value(&stmt.parameters, "init")
                    .map(|name| proc_oid_by_name(self, client_id, Some((xid, cid)), name))
                    .transpose()?;
                let lexize_name = parameter_value(&stmt.parameters, "lexize")
                    .ok_or_else(|| text_search_parameter_error(stmt.kind, "lexize"))?;
                let row = PgTsTemplateRow {
                    oid: 0,
                    tmplname: object_name,
                    tmplnamespace: namespace_oid,
                    tmplinit,
                    tmpllexize: proc_oid_by_name(self, client_id, Some((xid, cid)), lexize_name)?,
                };
                self.catalog
                    .write()
                    .create_ts_template_mvcc(row, &ctx)
                    .map(|(_, effect)| effect)?
            }
            TextSearchObjectKind::Parser => {
                reject_unknown_parameters(
                    stmt.kind,
                    &stmt.parameters,
                    &["start", "gettoken", "end", "headline", "lextypes"],
                )?;
                let proc = |name: &str| -> Result<u32, ExecError> {
                    let parameter = parameter_value(&stmt.parameters, name)
                        .ok_or_else(|| text_search_parameter_error(stmt.kind, name))?;
                    proc_oid_by_name(self, client_id, Some((xid, cid)), parameter)
                };
                let prsheadline = parameter_value(&stmt.parameters, "headline")
                    .map(|name| proc_oid_by_name(self, client_id, Some((xid, cid)), name))
                    .transpose()?;
                let row = PgTsParserRow {
                    oid: 0,
                    prsname: object_name,
                    prsnamespace: namespace_oid,
                    prsstart: proc("start")?,
                    prstoken: proc("gettoken")?,
                    prsend: proc("end")?,
                    prsheadline,
                    prslextype: proc("lextypes")?,
                };
                self.catalog
                    .write()
                    .create_ts_parser_mvcc(row, &ctx)
                    .map(|(_, effect)| effect)?
            }
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_text_search_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTextSearchStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_text_search_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_text_search_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterTextSearchStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let current = lookup_ts_object(
            self,
            client_id,
            Some((xid, cid)),
            stmt.kind,
            stmt.schema_name.as_deref(),
            &stmt.object_name,
            configured_search_path,
        )?
        .ok_or_else(|| ExecError::DetailedError {
            message: format!(
                "{} \"{}\" does not exist",
                text_search_kind_name(stmt.kind),
                stmt.object_name
            ),
            detail: None,
            hint: None,
            sqlstate: "42704",
        })?;

        let object_name = text_search_row_name(&current).to_string();
        let current_namespace = text_search_row_namespace(&current);
        let mut new_name = object_name.clone();
        let mut new_namespace = current_namespace;
        let mut new_owner = None;

        match &stmt.action {
            AlterTextSearchAction::Rename {
                new_name: rename_to,
            } => {
                new_name = normalize_text_search_name(rename_to);
            }
            AlterTextSearchAction::OwnerTo {
                new_owner: role_name,
            } => {
                let owner_oid = match &current {
                    TextSearchCatalogRow::Dictionary(row) => row.dictowner,
                    TextSearchCatalogRow::Configuration(row) => row.cfgowner,
                    TextSearchCatalogRow::Template(_) | TextSearchCatalogRow::Parser(_) => {
                        return Err(ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "object with owner",
                            actual: text_search_kind_name(stmt.kind).into(),
                        }));
                    }
                };
                ensure_text_search_owner(
                    self,
                    client_id,
                    Some((xid, cid)),
                    stmt.kind,
                    owner_oid,
                    &object_name,
                )?;
                let auth_catalog = self
                    .auth_catalog(client_id, Some((xid, cid)))
                    .map_err(map_catalog_error)?;
                let role = auth_catalog
                    .role_by_name(role_name)
                    .cloned()
                    .ok_or_else(|| {
                        ExecError::Parse(crate::backend::commands::rolecmds::role_management_error(
                            format!("role \"{role_name}\" does not exist"),
                        ))
                    })?;
                ensure_can_set_role(self, client_id, role.oid, &role.rolname)?;
                new_owner = Some(role.oid);
            }
            AlterTextSearchAction::SetSchema { new_schema } => {
                new_namespace = self
                    .visible_namespace_oid_by_name(client_id, Some((xid, cid)), new_schema)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{new_schema}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?;
            }
        }

        if matches!(
            stmt.kind,
            TextSearchObjectKind::Dictionary | TextSearchObjectKind::Configuration
        ) {
            let owner_oid = match &current {
                TextSearchCatalogRow::Dictionary(row) => row.dictowner,
                TextSearchCatalogRow::Configuration(row) => row.cfgowner,
                _ => unreachable!(),
            };
            ensure_text_search_owner(
                self,
                client_id,
                Some((xid, cid)),
                stmt.kind,
                owner_oid,
                &object_name,
            )?;
        }

        let current_oid = match &current {
            TextSearchCatalogRow::Dictionary(row) => row.oid,
            TextSearchCatalogRow::Configuration(row) => row.oid,
            TextSearchCatalogRow::Template(row) => row.oid,
            TextSearchCatalogRow::Parser(row) => row.oid,
        };
        if (new_name != object_name || new_namespace != current_namespace)
            && duplicate_text_search_exists(
                self,
                client_id,
                Some((xid, cid)),
                stmt.kind,
                &new_name,
                new_namespace,
                Some(current_oid),
            )?
        {
            return Err(text_search_duplicate_error(
                self,
                client_id,
                Some((xid, cid)),
                stmt.kind,
                &new_name,
                new_namespace,
            ));
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
        let effect = match current {
            TextSearchCatalogRow::Dictionary(old_row) => {
                let mut row = old_row.clone();
                row.dictname = new_name;
                row.dictnamespace = new_namespace;
                if let Some(owner) = new_owner {
                    row.dictowner = owner;
                }
                self.catalog
                    .write()
                    .replace_ts_dict_mvcc(&old_row, row, &ctx)
                    .map(|(_, effect)| effect)?
            }
            TextSearchCatalogRow::Configuration(old_row) => {
                let mut row = old_row.clone();
                row.cfgname = new_name;
                row.cfgnamespace = new_namespace;
                if let Some(owner) = new_owner {
                    row.cfgowner = owner;
                }
                self.catalog
                    .write()
                    .replace_ts_config_mvcc(&old_row, row, &ctx)
                    .map(|(_, effect)| effect)?
            }
            TextSearchCatalogRow::Template(old_row) => {
                let mut row = old_row.clone();
                row.tmplname = new_name;
                row.tmplnamespace = new_namespace;
                self.catalog
                    .write()
                    .replace_ts_template_mvcc(&old_row, row, &ctx)
                    .map(|(_, effect)| effect)?
            }
            TextSearchCatalogRow::Parser(old_row) => {
                let mut row = old_row.clone();
                row.prsname = new_name;
                row.prsnamespace = new_namespace;
                self.catalog
                    .write()
                    .replace_ts_parser_mvcc(&old_row, row, &ctx)
                    .map(|(_, effect)| effect)?
            }
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_text_search_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropTextSearchStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_text_search_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_drop_text_search_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropTextSearchStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let catcache = self
            .backend_catcache(client_id, txn_ctx)
            .map_err(map_catalog_error)?;
        let snapshot = TextSearchCatalogSnapshot {
            configs: catcache.ts_config_rows(),
            config_maps: catcache.ts_config_map_rows(),
            dicts: catcache.ts_dict_rows(),
        };
        let mut plan = TextSearchDropPlan::default();

        for raw_name in &stmt.object_names {
            let (schema_name, object_name) = text_search_object_name_parts(raw_name);
            if let Some(schema_name) = schema_name
                && self
                    .visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                    .is_none()
            {
                if stmt.if_exists {
                    push_notice(format!("schema \"{schema_name}\" does not exist, skipping"));
                    continue;
                }
                return Err(ExecError::DetailedError {
                    message: format!("schema \"{schema_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "3F000",
                });
            }
            let Some(row) = lookup_ts_object(
                self,
                client_id,
                txn_ctx,
                stmt.kind,
                schema_name,
                object_name,
                configured_search_path,
            )?
            else {
                if stmt.if_exists {
                    push_missing_text_search_notice(stmt.kind, raw_name);
                    continue;
                }
                return Err(text_search_missing_error(stmt.kind, raw_name));
            };
            ensure_text_search_drop_permission(self, client_id, txn_ctx, &row)?;
            collect_text_search_drop_plan(
                self,
                client_id,
                txn_ctx,
                row,
                stmt.cascade,
                &snapshot,
                &mut plan,
            )?;
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

        for config in plan.configs {
            let map_rows = snapshot
                .config_maps
                .iter()
                .filter(|row| row.mapcfg == config.oid)
                .cloned()
                .collect::<Vec<_>>();
            let effect = self
                .catalog
                .write()
                .drop_ts_config_mvcc(config, map_rows, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        for dict in plan.dicts {
            let effect = self
                .catalog
                .write()
                .drop_ts_dict_mvcc(dict, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        for template in plan.templates {
            let effect = self
                .catalog
                .write()
                .drop_ts_template_mvcc(template, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        for parser in plan.parsers {
            let effect = self
                .catalog
                .write()
                .drop_ts_parser_mvcc(parser, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        if !catalog_effects.is_empty() {
            self.plan_cache.invalidate_all();
        }
        Ok(StatementResult::AffectedRows(0))
    }
}
