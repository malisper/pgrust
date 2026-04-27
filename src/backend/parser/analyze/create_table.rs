use std::collections::BTreeSet;

use crate::backend::access::common::toast_compression::ensure_attribute_compression_supported;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{
    ColumnConstraint, ConstraintAttributes, CreateTableElement, CreateTableLikeClause,
    CreateTableLikeOption, RawTypeName, SequenceOptionsSpec, SerialKind, SqlExpr, SqlType,
    SqlTypeKind, TableConstraint,
};
use crate::include::access::htup::{AttributeCompression, AttributeStorage};
use crate::include::catalog::{CONSTRAINT_CHECK, CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE};
use crate::include::nodes::primnodes::expr_contains_set_returning;
use crate::pgrust::database::ddl::format_sql_type_name;

use super::{
    CatalogLookup, CheckConstraintAction, CreateTableStatement, ForeignKeyConstraintAction,
    IndexBackedConstraintAction, LoweredPartitionSpec, NotNullConstraintAction, ParseError,
    PartitionBoundSpec, normalize_create_table_constraints, raw_type_name_hint,
    resolve_collation_oid, resolve_raw_type_name, validate_generated_columns,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredCreateTable {
    pub of_type_oid: u32,
    pub relation_desc: RelationDesc,
    pub not_null_actions: Vec<NotNullConstraintAction>,
    pub check_actions: Vec<CheckConstraintAction>,
    pub constraint_actions: Vec<IndexBackedConstraintAction>,
    pub foreign_key_actions: Vec<ForeignKeyConstraintAction>,
    pub owned_sequences: Vec<OwnedSequenceSpec>,
    pub like_post_create_actions: Vec<CreateTableLikePostCreateAction>,
    pub parent_oids: Vec<u32>,
    pub partition_spec: Option<LoweredPartitionSpec>,
    pub partition_parent_oid: Option<u32>,
    pub partition_bound: Option<PartitionBoundSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedSequenceSpec {
    pub column_index: usize,
    pub column_name: String,
    pub serial_kind: SerialKind,
    pub sql_type: SqlType,
    pub options: SequenceOptionsSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableLikePostCreateAction {
    pub source_relation_oid: u32,
    pub include_comments: bool,
    pub include_statistics: bool,
}

pub fn create_relation_desc(
    stmt: &CreateTableStatement,
    catalog: &dyn CatalogLookup,
) -> Result<RelationDesc, ParseError> {
    Ok(lower_create_table(stmt, catalog)?.relation_desc)
}

pub fn lower_create_table(
    stmt: &CreateTableStatement,
    catalog: &dyn CatalogLookup,
) -> Result<LoweredCreateTable, ParseError> {
    let (typed_stmt, of_type_oid) = expand_create_table_of_type(stmt, catalog)?;
    let (expanded, like_post_create_actions) =
        expand_create_table_like_clauses(&typed_stmt, catalog)?;
    let columns = expanded.columns().cloned().collect::<Vec<_>>();
    let normalized = normalize_create_table_constraints(&expanded, catalog)?;
    let constraint_actions = normalized.index_backed.clone();
    let mut owned_sequences = Vec::new();

    let mut seen_keys = BTreeSet::new();
    for action in &constraint_actions {
        let key = action
            .columns
            .iter()
            .map(|column| column.to_ascii_lowercase())
            .collect::<Vec<_>>();
        if !seen_keys.insert(key) {
            return Err(ParseError::UnexpectedToken {
                expected: "distinct PRIMARY KEY/UNIQUE definitions",
                actual: format!(
                    "duplicate key definition on ({})",
                    action.columns.join(", ")
                ),
            });
        }
    }

    let not_nulls_by_column = normalized
        .not_nulls
        .iter()
        .map(|constraint| (constraint.column.to_ascii_lowercase(), constraint))
        .collect::<std::collections::BTreeMap<_, _>>();

    let relation_desc = RelationDesc {
        columns: columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                if super::raw_type_name_is_unknown(&column.ty) {
                    return Err(ParseError::DetailedError {
                        message: format!("column \"{}\" has pseudo-type unknown", column.name),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                let sql_type = match column.ty {
                    crate::backend::parser::RawTypeName::Serial(_) => {
                        raw_type_name_hint(&column.ty)
                    }
                    _ => resolve_raw_type_name(&column.ty, catalog)?,
                };
                if sql_type.kind == SqlTypeKind::AnyArray {
                    return Err(ParseError::UnsupportedType("anyarray".into()));
                }
                if matches!(sql_type.kind, SqlTypeKind::Cstring)
                    || sql_type.type_oid == crate::include::catalog::UNKNOWN_TYPE_OID
                {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "column \"{}\" has pseudo-type {}",
                            column.name,
                            super::sql_type_name(sql_type)
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P16",
                    });
                }
                if matches!(sql_type.kind, SqlTypeKind::Shell) {
                    let type_name = match &column.ty {
                        crate::backend::parser::RawTypeName::Named { name, .. } => name.as_str(),
                        _ => &column.name,
                    };
                    return Err(ParseError::DetailedError {
                        message: format!("type \"{type_name}\" is only a shell"),
                        detail: None,
                        hint: None,
                        sqlstate: "42809",
                    });
                }
                let not_null = not_nulls_by_column.get(&column.name.to_ascii_lowercase());
                let serial_kind = match column.ty {
                    crate::backend::parser::RawTypeName::Serial(kind) => Some(kind),
                    _ => None,
                };
                if column.generated.is_some() && column.default_expr.is_some() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "generated column without DEFAULT",
                        actual: format!(
                            "both default and generation expression specified for column \"{}\" of table \"{}\"",
                            column.name, stmt.table_name
                        ),
                    });
                }
                if column.identity.is_some()
                    && (column.generated.is_some()
                        || column.default_expr.is_some()
                        || serial_kind.is_some())
                {
                    let actual = if column.generated.is_some() {
                        format!(
                            "both identity and generation expression specified for column \"{}\" of table \"{}\"",
                            column.name, stmt.table_name
                        )
                    } else {
                        format!("conflicting identity definition for column \"{}\"", column.name)
                    };
                    return Err(ParseError::UnexpectedToken {
                        expected: "identity column without DEFAULT, generated expression, or serial type",
                        actual,
                    });
                }
                if serial_kind.is_some() && column.generated.is_some() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "non-serial generated column",
                        actual: format!(
                            "both serial and generation expression specified for column \"{}\"",
                            column.name
                        ),
                    });
                }
                if serial_kind.is_some() && column.default_expr.is_some() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "serial column without explicit DEFAULT",
                        actual: format!(
                            "multiple default values specified for column \"{}\"",
                            column.name
                        ),
                    });
                }
                let nullable = not_null.is_none() && serial_kind.is_none() && column.identity.is_none();
                let mut desc = column_desc(column.name.clone(), sql_type, nullable);
                if let Some(collation) = column.collation.as_deref() {
                    if !super::is_collatable_type(sql_type) {
                        return Err(ParseError::DetailedError {
                            message: format!(
                                "collations are not supported by type {}",
                                super::sql_type_name(sql_type)
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42804",
                        });
                    }
                    desc.collation_oid = resolve_collation_oid(collation, catalog)?;
                }
                if let Some(not_null) = not_null {
                    desc.not_null_constraint_name = Some(not_null.constraint_name.clone());
                    desc.not_null_constraint_validated = !not_null.not_valid;
                    desc.not_null_constraint_is_local = not_null.is_local;
                    desc.not_null_constraint_inhcount = not_null.inhcount;
                    desc.not_null_constraint_no_inherit = not_null.no_inherit;
                    desc.not_null_primary_key_owned = not_null.primary_key_owned;
                }
                if let Some(generated) = &column.generated {
                    desc.default_expr = Some(generated.expr_sql.clone());
                    desc.generated = Some(generated.kind);
                } else if let Some(identity) = &column.identity {
                    desc.identity = Some(identity.kind);
                } else {
                    desc.default_expr = column.default_expr.clone();
                    if desc.default_expr.is_none()
                        && let Some(type_oid) = catalog.type_oid_for_sql_type(sql_type)
                        && let Some(type_default) = catalog.type_default_sql(type_oid)
                    {
                        desc.default_expr = Some(type_default);
                    }
                    if let Some(default_sql) = desc.default_expr.as_deref() {
                        validate_column_default_expr(default_sql, catalog)?;
                    }
                    desc.missing_default_value = desc
                        .default_expr
                        .as_deref()
                        .and_then(|sql| super::derive_literal_default_value(sql, sql_type).ok());
                }
                if let Some(serial_kind) = serial_kind {
                    desc.default_expr = None;
                    desc.missing_default_value = None;
                    owned_sequences.push(OwnedSequenceSpec {
                        column_index: index,
                        column_name: column.name.clone(),
                        serial_kind,
                        sql_type,
                        options: SequenceOptionsSpec::default(),
                    });
                }
                if let Some(identity) = &column.identity {
                    desc.default_expr = None;
                    desc.missing_default_value = None;
                    owned_sequences.push(OwnedSequenceSpec {
                        column_index: index,
                        column_name: column.name.clone(),
                        serial_kind: serial_kind_for_identity_sql_type(sql_type)?,
                        sql_type,
                        options: identity.options.clone(),
                    });
                }
                if let Some(storage) = column.storage {
                    desc.storage.attstorage = storage;
                }
                if let Some(compression) = column.compression {
                    validate_create_column_compression(sql_type, compression)?;
                    desc.storage.attcompression = compression;
                }
                Ok(desc)
            })
            .collect::<Result<Vec<_>, _>>()?,
    };

    validate_generated_columns(&relation_desc, catalog)?;

    Ok(LoweredCreateTable {
        of_type_oid,
        relation_desc,
        not_null_actions: normalized.not_nulls,
        check_actions: normalized.checks,
        constraint_actions,
        foreign_key_actions: normalized.foreign_keys,
        owned_sequences,
        like_post_create_actions,
        parent_oids: Vec::new(),
        partition_spec: None,
        partition_parent_oid: None,
        partition_bound: None,
    })
}

fn expand_create_table_of_type(
    stmt: &CreateTableStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(CreateTableStatement, u32), ParseError> {
    let Some(type_name) = stmt.of_type_name.as_deref() else {
        return Ok((stmt.clone(), 0));
    };
    if !stmt.inherits.is_empty() {
        return Err(ParseError::DetailedError {
            message: "typed tables cannot inherit".into(),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    if stmt.partition_spec.is_some() || stmt.partition_of.is_some() {
        return Err(ParseError::DetailedError {
            message: "typed tables cannot be partitioned".into(),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    let type_row = catalog
        .type_by_name(type_name)
        .ok_or_else(|| ParseError::UnsupportedType(type_name.to_string()))?;
    if matches!(type_row.sql_type.kind, SqlTypeKind::Shell) {
        return Err(ParseError::DetailedError {
            message: format!("type \"{}\" is only a shell", type_row.typname),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    if !matches!(type_row.sql_type.kind, SqlTypeKind::Composite) || type_row.typrelid == 0 {
        return Err(ParseError::DetailedError {
            message: format!("type {} is not a composite type", type_row.typname),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    let class_row =
        catalog
            .class_row_by_oid(type_row.typrelid)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "composite type relation",
                actual: format!("missing relation oid {}", type_row.typrelid),
            })?;
    if class_row.relkind != 'c' {
        return Err(ParseError::DetailedError {
            message: format!("type {} is the row type of another table", type_row.typname),
            detail: Some(
                "A typed table must use a stand-alone composite type created with CREATE TYPE."
                    .into(),
            ),
            hint: None,
            sqlstate: "42809",
        });
    }
    let type_relation = catalog
        .lookup_relation_by_oid(type_row.typrelid)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "composite type relation",
            actual: format!("missing relation oid {}", type_row.typrelid),
        })?;

    let mut typed_columns = type_relation
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| {
            CreateTableElement::Column(crate::backend::parser::ColumnDef {
                name: column.name.clone(),
                ty: RawTypeName::Builtin(column.sql_type),
                collation: None,
                default_expr: None,
                generated: None,
                identity: None,
                storage: None,
                compression: None,
                constraints: Vec::new(),
            })
        })
        .collect::<Vec<_>>();

    let mut seen_options = BTreeSet::new();
    let mut table_constraints = Vec::new();
    for element in &stmt.elements {
        match element {
            CreateTableElement::TypedColumnOptions(options) => {
                let lookup_name = options.name.to_ascii_lowercase();
                if !seen_options.insert(lookup_name.clone()) {
                    return Err(ParseError::DetailedError {
                        message: format!("column \"{}\" specified more than once", options.name),
                        detail: None,
                        hint: None,
                        sqlstate: "42701",
                    });
                }
                let Some(CreateTableElement::Column(column)) =
                    typed_columns.iter_mut().find(|element| match element {
                        CreateTableElement::Column(column) => {
                            column.name.eq_ignore_ascii_case(&lookup_name)
                        }
                        _ => false,
                    })
                else {
                    return Err(ParseError::UnknownColumn(options.name.clone()));
                };
                column.collation = options.collation.clone();
                column.default_expr = options.default_expr.clone();
                column.generated = options.generated.clone();
                column.identity = options.identity.clone();
                column.storage = options.storage;
                column.compression = options.compression;
                column.constraints = options.constraints.clone();
            }
            CreateTableElement::Constraint(constraint) => {
                table_constraints.push(CreateTableElement::Constraint(constraint.clone()));
            }
            CreateTableElement::Column(column) => {
                return Err(ParseError::DetailedError {
                    message: format!(
                        "column \"{}\" conflicts with typed table row type",
                        column.name
                    ),
                    detail: Some(
                        "Use WITH OPTIONS to add column options to typed-table columns.".into(),
                    ),
                    hint: None,
                    sqlstate: "42701",
                });
            }
            CreateTableElement::PartitionColumnOverride(_) | CreateTableElement::Like(_) => {
                return Err(ParseError::DetailedError {
                    message: "CREATE TABLE OF does not support this table element".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                });
            }
        }
    }
    typed_columns.extend(table_constraints);

    let mut expanded = stmt.clone();
    expanded.of_type_name = None;
    expanded.elements = typed_columns;
    Ok((expanded, type_row.oid))
}

fn expand_create_table_like_clauses(
    stmt: &CreateTableStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(CreateTableStatement, Vec<CreateTableLikePostCreateAction>), ParseError> {
    if !stmt
        .elements
        .iter()
        .any(|element| matches!(element, CreateTableElement::Like(_)))
    {
        return Ok((stmt.clone(), Vec::new()));
    }

    let mut expanded = stmt.clone();
    expanded.elements = Vec::new();
    let mut post_create_actions = Vec::new();
    for element in &stmt.elements {
        match element {
            CreateTableElement::Column(_)
            | CreateTableElement::TypedColumnOptions(_)
            | CreateTableElement::PartitionColumnOverride(_)
            | CreateTableElement::Constraint(_) => {
                expanded.elements.push(element.clone());
            }
            CreateTableElement::Like(like) => {
                let (elements, post_create_action) = expand_like_clause(like, catalog)?;
                expanded.elements.extend(elements);
                if let Some(action) = post_create_action {
                    post_create_actions.push(action);
                }
            }
        }
    }
    Ok((expanded, post_create_actions))
}

#[derive(Debug, Clone, Copy)]
struct LikeExpansionOptions {
    defaults: bool,
    constraints: bool,
    indexes: bool,
    identity: bool,
    generated: bool,
    comments: bool,
    storage: bool,
    compression: bool,
    statistics: bool,
}

impl LikeExpansionOptions {
    fn from_clause(clause: &CreateTableLikeClause) -> Self {
        let mut options = Self {
            defaults: false,
            constraints: false,
            indexes: false,
            identity: false,
            generated: false,
            comments: false,
            storage: false,
            compression: false,
            statistics: false,
        };
        for option in &clause.options {
            match option {
                CreateTableLikeOption::IncludingDefaults => options.defaults = true,
                CreateTableLikeOption::IncludingConstraints => options.constraints = true,
                CreateTableLikeOption::IncludingIndexes => options.indexes = true,
                CreateTableLikeOption::IncludingIdentity => options.identity = true,
                CreateTableLikeOption::IncludingGenerated => options.generated = true,
                CreateTableLikeOption::IncludingComments => options.comments = true,
                CreateTableLikeOption::IncludingStorage => options.storage = true,
                CreateTableLikeOption::IncludingCompression => options.compression = true,
                CreateTableLikeOption::IncludingStatistics => options.statistics = true,
                CreateTableLikeOption::IncludingAll => {
                    options.defaults = true;
                    options.constraints = true;
                    options.indexes = true;
                    options.identity = true;
                    options.generated = true;
                    options.comments = true;
                    options.storage = true;
                    options.compression = true;
                    options.statistics = true;
                }
                CreateTableLikeOption::ExcludingDefaults => options.defaults = false,
                CreateTableLikeOption::ExcludingConstraints => options.constraints = false,
                CreateTableLikeOption::ExcludingIndexes => options.indexes = false,
                CreateTableLikeOption::ExcludingIdentity => options.identity = false,
                CreateTableLikeOption::ExcludingGenerated => options.generated = false,
                CreateTableLikeOption::ExcludingComments => options.comments = false,
                CreateTableLikeOption::ExcludingStorage => options.storage = false,
                CreateTableLikeOption::ExcludingCompression => options.compression = false,
                CreateTableLikeOption::ExcludingStatistics => options.statistics = false,
                CreateTableLikeOption::ExcludingAll => {
                    options.defaults = false;
                    options.constraints = false;
                    options.indexes = false;
                    options.identity = false;
                    options.generated = false;
                    options.comments = false;
                    options.storage = false;
                    options.compression = false;
                    options.statistics = false;
                }
            }
        }
        options
    }
}

fn expand_like_clause(
    clause: &CreateTableLikeClause,
    catalog: &dyn CatalogLookup,
) -> Result<
    (
        Vec<CreateTableElement>,
        Option<CreateTableLikePostCreateAction>,
    ),
    ParseError,
> {
    let source = catalog
        .lookup_any_relation(&clause.relation_name)
        .ok_or_else(|| ParseError::UnknownTable(clause.relation_name.clone()))?;
    if !matches!(source.relkind, 'r' | 'p' | 'v' | 'm' | 'f' | 'c') {
        return Err(ParseError::FeatureNotSupported(format!(
            "CREATE TABLE LIKE source relation kind {}",
            source.relkind
        )));
    }
    let options = LikeExpansionOptions::from_clause(clause);
    let mut elements = source
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| {
            let mut constraints = Vec::new();
            if !column.storage.nullable {
                constraints.push(ColumnConstraint::NotNull {
                    attributes: ConstraintAttributes {
                        name: column.not_null_constraint_name.clone(),
                        not_valid: !column.not_null_constraint_validated,
                        no_inherit: column.not_null_constraint_no_inherit,
                        ..ConstraintAttributes::default()
                    },
                });
            }
            let generated = if options.generated {
                column.generated.and_then(|kind| {
                    column.default_expr.clone().map(|expr_sql| {
                        crate::include::nodes::parsenodes::ColumnGeneratedDef { expr_sql, kind }
                    })
                })
            } else {
                None
            };
            CreateTableElement::Column(crate::backend::parser::ColumnDef {
                name: column.name.clone(),
                ty: RawTypeName::Builtin(column.sql_type),
                collation: None,
                default_expr: if generated.is_some() {
                    None
                } else if options.defaults && column.generated.is_none() {
                    column.default_expr.clone()
                } else {
                    None
                },
                generated,
                identity: options
                    .identity
                    .then_some(column.identity)
                    .flatten()
                    .map(
                        |kind| crate::include::nodes::parsenodes::ColumnIdentityDef {
                            kind,
                            options: SequenceOptionsSpec::default(),
                        },
                    ),
                storage: options.storage.then_some(column.storage.attstorage),
                compression: match (options.compression, column.storage.attcompression) {
                    (false, _) | (true, AttributeCompression::Default) => None,
                    (true, compression) => Some(compression),
                },
                constraints,
            })
        })
        .collect::<Vec<_>>();

    let constraints = catalog.constraint_rows_for_relation(source.relation_oid);
    if options.constraints {
        elements.extend(constraints.iter().filter_map(|row| {
            if row.contype != CONSTRAINT_CHECK {
                return None;
            }
            Some(CreateTableElement::Constraint(TableConstraint::Check {
                attributes: ConstraintAttributes {
                    name: Some(row.conname.clone()),
                    not_valid: !row.convalidated,
                    no_inherit: row.connoinherit,
                    enforced: Some(row.conenforced),
                    ..ConstraintAttributes::default()
                },
                expr_sql: row.conbin.clone()?,
            }))
        }));
    }

    if options.indexes {
        for row in constraints
            .iter()
            .filter(|row| row.contype == CONSTRAINT_PRIMARY || row.contype == CONSTRAINT_UNIQUE)
        {
            let Some(columns) = constraint_column_names(row.conkey.as_deref(), &source.desc) else {
                continue;
            };
            let attributes = ConstraintAttributes {
                deferrable: row.condeferrable.then_some(true),
                initially_deferred: row.condeferred.then_some(true),
                ..ConstraintAttributes::default()
            };
            let without_overlaps = row.conperiod.then(|| columns.last().cloned()).flatten();
            elements.push(CreateTableElement::Constraint(
                if row.contype == CONSTRAINT_PRIMARY {
                    TableConstraint::PrimaryKey {
                        attributes,
                        columns,
                        include_columns: Vec::new(),
                        without_overlaps,
                    }
                } else {
                    TableConstraint::Unique {
                        attributes,
                        columns,
                        include_columns: Vec::new(),
                        without_overlaps,
                    }
                },
            ));
        }
    }

    let post_create_action =
        (options.comments || options.statistics).then_some(CreateTableLikePostCreateAction {
            source_relation_oid: source.relation_oid,
            include_comments: options.comments,
            include_statistics: options.statistics,
        });

    Ok((elements, post_create_action))
}

fn constraint_column_names(attnums: Option<&[i16]>, desc: &RelationDesc) -> Option<Vec<String>> {
    attnums?
        .iter()
        .map(|attnum| {
            let index = usize::try_from(*attnum).ok()?.checked_sub(1)?;
            let column = desc.columns.get(index)?;
            (!column.dropped).then(|| column.name.clone())
        })
        .collect()
}

fn validate_column_default_expr(
    default_sql: &str,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    let parsed = crate::backend::parser::parse_expr(default_sql)?;
    if raw_expr_any(&parsed, &|expr| matches!(expr, SqlExpr::Column(_))) {
        return Err(default_expr_error(
            "cannot use column reference in DEFAULT expression",
        ));
    }
    if super::agg::expr_contains_agg(catalog, &parsed) {
        return Err(default_expr_error(
            "aggregate functions are not allowed in DEFAULT expressions",
        ));
    }
    if raw_expr_any(&parsed, &|expr| {
        matches!(expr, SqlExpr::FuncCall { over: Some(_), .. })
    }) {
        return Err(default_expr_error(
            "window functions are not allowed in DEFAULT expressions",
        ));
    }
    if raw_expr_any(&parsed, &|expr| {
        matches!(
            expr,
            SqlExpr::ScalarSubquery(_)
                | SqlExpr::ArraySubquery(_)
                | SqlExpr::Exists(_)
                | SqlExpr::InSubquery { .. }
                | SqlExpr::QuantifiedSubquery { .. }
        )
    }) {
        return Err(default_expr_error(
            "cannot use subquery in DEFAULT expression",
        ));
    }
    let (bound, _) = super::bind_scalar_expr_in_scope(&parsed, &[], catalog)?;
    if expr_contains_set_returning(&bound) {
        return Err(default_expr_error(
            "set-returning functions are not allowed in DEFAULT expressions",
        ));
    }
    Ok(())
}

fn default_expr_error(message: impl Into<String>) -> ParseError {
    ParseError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn raw_expr_any(expr: &SqlExpr, predicate: &impl Fn(&SqlExpr) -> bool) -> bool {
    if predicate(expr) {
        return true;
    }
    match expr {
        SqlExpr::Column(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. }
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_) => false,
        SqlExpr::FuncCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            args.args()
                .iter()
                .any(|arg| raw_expr_any(&arg.value, predicate))
                || order_by
                    .iter()
                    .any(|item| raw_expr_any(&item.expr, predicate))
                || within_group.as_deref().is_some_and(|items| {
                    items.iter().any(|item| raw_expr_any(&item.expr, predicate))
                })
                || filter
                    .as_deref()
                    .is_some_and(|expr| raw_expr_any(expr, predicate))
        }
        SqlExpr::InSubquery { expr, .. } => raw_expr_any(expr, predicate),
        SqlExpr::QuantifiedSubquery { left, .. } => raw_expr_any(left, predicate),
        SqlExpr::PrefixOperator { expr, .. } | SqlExpr::FieldSelect { expr, .. } => {
            raw_expr_any(expr, predicate)
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            elements.iter().any(|expr| raw_expr_any(expr, predicate))
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            raw_expr_any(array, predicate)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_deref()
                        .is_some_and(|expr| raw_expr_any(expr, predicate))
                        || subscript
                            .upper
                            .as_deref()
                            .is_some_and(|expr| raw_expr_any(expr, predicate))
                })
        }
        SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::Overlaps(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::QuantifiedArray {
            left, array: right, ..
        }
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::GeometryBinaryOp { left, right, .. }
        | SqlExpr::AtTimeZone {
            expr: left,
            zone: right,
        }
        | SqlExpr::BinaryOperator { left, right, .. } => {
            raw_expr_any(left, predicate) || raw_expr_any(right, predicate)
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            raw_expr_any(expr, predicate)
                || raw_expr_any(pattern, predicate)
                || escape
                    .as_ref()
                    .is_some_and(|expr| raw_expr_any(expr, predicate))
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_deref()
                .is_some_and(|expr| raw_expr_any(expr, predicate))
                || args.iter().any(|arm| {
                    raw_expr_any(&arm.expr, predicate) || raw_expr_any(&arm.result, predicate)
                })
                || defresult
                    .as_deref()
                    .is_some_and(|expr| raw_expr_any(expr, predicate))
        }
        SqlExpr::Cast(inner, _)
        | SqlExpr::Collate { expr: inner, .. }
        | SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => raw_expr_any(inner, predicate),
        SqlExpr::Xml(xml) => xml.child_exprs().any(|expr| raw_expr_any(expr, predicate)),
    }
}

fn serial_kind_for_identity_sql_type(sql_type: SqlType) -> Result<SerialKind, ParseError> {
    match sql_type.kind {
        SqlTypeKind::Int2 if !sql_type.is_array => Ok(SerialKind::Small),
        SqlTypeKind::Int4 if !sql_type.is_array => Ok(SerialKind::Regular),
        SqlTypeKind::Int8 if !sql_type.is_array => Ok(SerialKind::Big),
        _ => Err(ParseError::UnexpectedToken {
            expected: "smallint, integer, or bigint identity column",
            actual: format_sql_type_name(sql_type),
        }),
    }
}

fn validate_create_column_compression(
    sql_type: SqlType,
    compression: AttributeCompression,
) -> Result<(), ParseError> {
    ensure_attribute_compression_supported(compression).map_err(|err| match err {
        crate::backend::executor::ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        } => ParseError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
        other => ParseError::FeatureNotSupportedMessage(format!("{other:?}")),
    })?;

    let type_default = column_desc("attcompression_check", sql_type, true)
        .storage
        .attstorage;
    if compression != AttributeCompression::Default && type_default == AttributeStorage::Plain {
        return Err(ParseError::DetailedError {
            message: format!(
                "column data type {} does not support compression",
                format_sql_type_name(sql_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{
        ColumnDef, ColumnGeneratedKind, ConstraintAttributes, CreateTableElement, OnCommitAction,
        RawTypeName, SqlType, TablePersistence,
    };
    use crate::include::catalog::PgConstraintRow;

    #[derive(Default)]
    struct LikeCatalog {
        relation: Option<crate::backend::parser::BoundRelation>,
        constraints: Vec<PgConstraintRow>,
    }

    impl crate::backend::parser::CatalogLookup for LikeCatalog {
        fn lookup_any_relation(&self, name: &str) -> Option<crate::backend::parser::BoundRelation> {
            self.relation
                .as_ref()
                .filter(|_| name == "source_table")
                .cloned()
        }

        fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
            self.relation
                .as_ref()
                .filter(|relation| relation.relation_oid == relation_oid)
                .map(|_| self.constraints.clone())
                .unwrap_or_default()
        }
    }

    #[test]
    fn lower_create_table_rejects_anyarray_columns() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "bad_anyarray".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "a".into(),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::AnyArray)),
                collation: None,
                default_expr: None,
                generated: None,
                identity: None,
                storage: None,
                compression: None,
                constraints: vec![],
            })],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        assert_eq!(
            lower_create_table(
                &stmt,
                &crate::backend::parser::analyze::LiteralDefaultCatalog
            ),
            Err(ParseError::UnsupportedType("anyarray".into()))
        );
    }

    #[test]
    fn lower_create_table_rejects_unknown_pseudotype_columns() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "bad_unknown".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "u".into(),
                ty: RawTypeName::Named {
                    name: "unknown".into(),
                    array_bounds: 0,
                },
                collation: None,
                default_expr: None,
                generated: None,
                identity: None,
                storage: None,
                compression: None,
                constraints: vec![],
            })],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        assert_eq!(
            lower_create_table(
                &stmt,
                &crate::backend::parser::analyze::LiteralDefaultCatalog
            ),
            Err(ParseError::DetailedError {
                message: "column \"u\" has pseudo-type unknown".into(),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            })
        );
    }

    #[test]
    fn lower_create_table_rejects_invalid_default_expressions() {
        fn stmt(default_expr: &str) -> CreateTableStatement {
            CreateTableStatement {
                schema_name: None,
                table_name: "bad_default".into(),
                of_type_name: None,
                persistence: TablePersistence::Permanent,
                on_commit: OnCommitAction::PreserveRows,
                elements: vec![CreateTableElement::Column(ColumnDef {
                    name: "u".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                    collation: None,
                    default_expr: Some(default_expr.into()),
                    generated: None,
                    identity: None,
                    storage: None,
                    compression: None,
                    constraints: vec![],
                })],
                options: Vec::new(),
                inherits: Vec::new(),
                partition_spec: None,
                partition_of: None,
                partition_bound: None,
                if_not_exists: false,
            }
        }

        for (default_expr, expected) in [
            ("u", "cannot use column reference in DEFAULT expression"),
            (
                "sum(1)",
                "aggregate functions are not allowed in DEFAULT expressions",
            ),
            (
                "sum(1) over ()",
                "window functions are not allowed in DEFAULT expressions",
            ),
            ("(select 1)", "cannot use subquery in DEFAULT expression"),
            (
                "generate_series(1, 2)",
                "set-returning functions are not allowed in DEFAULT expressions",
            ),
        ] {
            assert!(matches!(
                lower_create_table(
                    &stmt(default_expr),
                    &crate::backend::parser::analyze::LiteralDefaultCatalog
                ),
                Err(ParseError::DetailedError { message, sqlstate, .. })
                    if message == expected && sqlstate == "0A000"
            ));
        }
    }

    #[test]
    fn lower_create_table_materializes_not_null_metadata() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "items".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![
                CreateTableElement::Column(ColumnDef {
                    name: "id".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                    collation: None,
                    default_expr: None,
                    generated: None,
                    identity: None,
                    storage: None,
                    compression: None,
                    constraints: vec![crate::backend::parser::ColumnConstraint::PrimaryKey {
                        attributes: ConstraintAttributes::default(),
                    }],
                }),
                CreateTableElement::Column(ColumnDef {
                    name: "note".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
                    collation: None,
                    default_expr: None,
                    generated: None,
                    identity: None,
                    storage: None,
                    compression: None,
                    constraints: vec![crate::backend::parser::ColumnConstraint::NotNull {
                        attributes: ConstraintAttributes {
                            name: Some("items_note_nn".into()),
                            not_valid: true,
                            no_inherit: true,
                            ..ConstraintAttributes::default()
                        },
                    }],
                }),
            ],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        let lowered = lower_create_table(
            &stmt,
            &crate::backend::parser::analyze::LiteralDefaultCatalog,
        )
        .unwrap();
        assert_eq!(lowered.not_null_actions.len(), 2);
        assert_eq!(
            lowered.relation_desc.columns[0]
                .not_null_constraint_name
                .as_deref(),
            Some("items_id_not_null")
        );
        assert!(lowered.relation_desc.columns[0].not_null_primary_key_owned);
        assert_eq!(
            lowered.relation_desc.columns[1]
                .not_null_constraint_name
                .as_deref(),
            Some("items_note_nn")
        );
        assert!(!lowered.relation_desc.columns[1].not_null_constraint_validated);
        assert!(lowered.relation_desc.columns[1].not_null_constraint_no_inherit);
        assert!(lowered.not_null_actions[1].no_inherit);
    }

    #[test]
    fn lower_create_table_like_including_all_expands_columns_and_constraints() {
        let mut source_desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        source_desc.columns[0].not_null_constraint_name = Some("source_id_not_null".into());
        source_desc.columns[0].default_expr = Some("7".into());
        let catalog = LikeCatalog {
            relation: Some(crate::backend::parser::BoundRelation {
                rel: crate::RelFileLocator {
                    spc_oid: 1,
                    db_oid: 1,
                    rel_number: 42,
                },
                relation_oid: 42,
                toast: None,
                namespace_oid: crate::include::catalog::PUBLIC_NAMESPACE_OID,
                owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                of_type_oid: 0,
                relpersistence: 'p',
                relkind: 'r',
                relispopulated: true,
                relispartition: false,
                relpartbound: None,
                desc: source_desc,
                partitioned_table: None,
                partition_spec: None,
            }),
            constraints: vec![
                PgConstraintRow {
                    oid: 100,
                    conname: "source_check".into(),
                    connamespace: crate::include::catalog::PUBLIC_NAMESPACE_OID,
                    contype: CONSTRAINT_CHECK,
                    condeferrable: false,
                    condeferred: false,
                    conenforced: true,
                    convalidated: true,
                    conrelid: 42,
                    contypid: 0,
                    conindid: 0,
                    conparentid: 0,
                    confrelid: 0,
                    confupdtype: 'a',
                    confdeltype: 'a',
                    confmatchtype: 's',
                    conkey: None,
                    confkey: None,
                    conpfeqop: None,
                    conppeqop: None,
                    conffeqop: None,
                    confdelsetcols: None,
                    conexclop: None,
                    conbin: Some("id > 0".into()),
                    conislocal: true,
                    coninhcount: 0,
                    connoinherit: false,
                    conperiod: false,
                },
                PgConstraintRow {
                    oid: 101,
                    conname: "source_pkey".into(),
                    connamespace: crate::include::catalog::PUBLIC_NAMESPACE_OID,
                    contype: CONSTRAINT_PRIMARY,
                    condeferrable: false,
                    condeferred: false,
                    conenforced: true,
                    convalidated: true,
                    conrelid: 42,
                    contypid: 0,
                    conindid: 102,
                    conparentid: 0,
                    confrelid: 0,
                    confupdtype: 'a',
                    confdeltype: 'a',
                    confmatchtype: 's',
                    conkey: Some(vec![1]),
                    confkey: None,
                    conpfeqop: None,
                    conppeqop: None,
                    conffeqop: None,
                    confdelsetcols: None,
                    conexclop: None,
                    conbin: None,
                    conislocal: true,
                    coninhcount: 0,
                    connoinherit: false,
                    conperiod: false,
                },
            ],
        };
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "copy_table".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Like(CreateTableLikeClause {
                relation_name: "source_table".into(),
                options: vec![CreateTableLikeOption::IncludingAll],
            })],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        let lowered = lower_create_table(&stmt, &catalog).unwrap();
        assert_eq!(lowered.relation_desc.columns.len(), 2);
        assert_eq!(
            lowered.relation_desc.columns[0].default_expr.as_deref(),
            Some("7")
        );
        assert_eq!(lowered.check_actions[0].constraint_name, "source_check");
        assert_eq!(
            lowered.constraint_actions[0].constraint_name.as_deref(),
            Some("copy_table_pkey")
        );
        assert_eq!(lowered.constraint_actions[0].columns, vec!["id"]);
    }

    #[test]
    fn lower_create_table_like_generated_option_controls_generated_columns() {
        let mut source_desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("doubled", SqlType::new(SqlTypeKind::Int4), true),
            ],
        };
        source_desc.columns[1].default_expr = Some("id * 2".into());
        source_desc.columns[1].generated = Some(ColumnGeneratedKind::Stored);
        let catalog = LikeCatalog {
            relation: Some(crate::backend::parser::BoundRelation {
                rel: crate::RelFileLocator {
                    spc_oid: 1,
                    db_oid: 1,
                    rel_number: 42,
                },
                relation_oid: 42,
                toast: None,
                namespace_oid: crate::include::catalog::PUBLIC_NAMESPACE_OID,
                owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                of_type_oid: 0,
                relpersistence: 'p',
                relkind: 'r',
                relispopulated: true,
                relispartition: false,
                relpartbound: None,
                desc: source_desc,
                partitioned_table: None,
                partition_spec: None,
            }),
            constraints: Vec::new(),
        };
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "copy_table".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Like(CreateTableLikeClause {
                relation_name: "source_table".into(),
                options: vec![CreateTableLikeOption::IncludingDefaults],
            })],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        let lowered = lower_create_table(&stmt, &catalog).unwrap();
        assert_eq!(lowered.relation_desc.columns.len(), 2);
        assert_eq!(lowered.relation_desc.columns[1].default_expr, None);
        assert_eq!(lowered.relation_desc.columns[1].generated, None);

        let stmt = CreateTableStatement {
            elements: vec![CreateTableElement::Like(CreateTableLikeClause {
                relation_name: "source_table".into(),
                options: vec![CreateTableLikeOption::IncludingGenerated],
            })],
            ..stmt
        };
        let lowered = lower_create_table(&stmt, &catalog).unwrap();
        assert_eq!(
            lowered.relation_desc.columns[1].default_expr.as_deref(),
            Some("id * 2")
        );
        assert_eq!(
            lowered.relation_desc.columns[1].generated,
            Some(ColumnGeneratedKind::Stored)
        );
    }

    #[test]
    fn lower_create_table_rejects_conflicting_not_null_no_inherit() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "items".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![
                CreateTableElement::Column(ColumnDef {
                    name: "id".into(),
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                    collation: None,
                    default_expr: None,
                    generated: None,
                    identity: None,
                    storage: None,
                    compression: None,
                    constraints: vec![crate::backend::parser::ColumnConstraint::PrimaryKey {
                        attributes: ConstraintAttributes::default(),
                    }],
                }),
                CreateTableElement::Constraint(crate::backend::parser::TableConstraint::NotNull {
                    attributes: ConstraintAttributes {
                        no_inherit: true,
                        ..ConstraintAttributes::default()
                    },
                    column: "id".into(),
                }),
            ],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        assert_eq!(
            lower_create_table(
                &stmt,
                &crate::backend::parser::analyze::LiteralDefaultCatalog
            ),
            Err(ParseError::InvalidTableDefinition(
                "conflicting NO INHERIT declaration for not-null constraint on column \"id\""
                    .into(),
            ))
        );
    }

    #[test]
    fn lower_create_table_tracks_serial_columns() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "items".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "id".into(),
                ty: RawTypeName::Serial(SerialKind::Regular),
                collation: None,
                default_expr: None,
                generated: None,
                identity: None,
                storage: None,
                compression: None,
                constraints: vec![],
            })],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        let lowered = lower_create_table(
            &stmt,
            &crate::backend::parser::analyze::LiteralDefaultCatalog,
        )
        .unwrap();
        assert_eq!(
            lowered.relation_desc.columns[0].sql_type,
            SqlType::new(SqlTypeKind::Int4)
        );
        assert!(!lowered.relation_desc.columns[0].storage.nullable);
        assert_eq!(lowered.owned_sequences.len(), 1);
        assert_eq!(lowered.owned_sequences[0].column_index, 0);
        assert_eq!(lowered.owned_sequences[0].column_name, "id");
        assert_eq!(lowered.owned_sequences[0].serial_kind, SerialKind::Regular);
    }

    #[test]
    fn lower_create_table_rejects_explicit_default_on_serial() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "items".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "id".into(),
                ty: RawTypeName::Serial(SerialKind::Regular),
                collation: None,
                default_expr: Some("7".into()),
                generated: None,
                identity: None,
                storage: None,
                compression: None,
                constraints: vec![],
            })],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        assert_eq!(
            lower_create_table(
                &stmt,
                &crate::backend::parser::analyze::LiteralDefaultCatalog
            ),
            Err(ParseError::UnexpectedToken {
                expected: "serial column without explicit DEFAULT",
                actual: "multiple default values specified for column \"id\"".into(),
            })
        );
    }

    #[test]
    fn lower_create_table_applies_column_compression() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "cmdata".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "f1".into(),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
                collation: None,
                default_expr: None,
                generated: None,
                identity: None,
                storage: None,
                compression: Some(AttributeCompression::Pglz),
                constraints: vec![],
            })],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        let lowered = lower_create_table(
            &stmt,
            &crate::backend::parser::analyze::LiteralDefaultCatalog,
        )
        .unwrap();
        assert_eq!(
            lowered.relation_desc.columns[0].storage.attcompression,
            AttributeCompression::Pglz
        );
    }

    #[test]
    fn lower_create_table_rejects_compression_for_plain_types() {
        let stmt = CreateTableStatement {
            schema_name: None,
            table_name: "cmdata".into(),
            of_type_name: None,
            persistence: TablePersistence::Permanent,
            on_commit: OnCommitAction::PreserveRows,
            elements: vec![CreateTableElement::Column(ColumnDef {
                name: "f1".into(),
                ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4)),
                collation: None,
                default_expr: None,
                generated: None,
                identity: None,
                storage: None,
                compression: Some(AttributeCompression::Pglz),
                constraints: vec![],
            })],
            options: Vec::new(),
            inherits: Vec::new(),
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists: false,
        };

        assert!(matches!(
            lower_create_table(
                &stmt,
                &crate::backend::parser::analyze::LiteralDefaultCatalog
            ),
            Err(ParseError::DetailedError { message, sqlstate, .. })
                if message == "column data type integer does not support compression"
                    && sqlstate == "0A000"
        ));
    }
}
