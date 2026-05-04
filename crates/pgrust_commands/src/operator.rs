use pgrust_analyze::{CatalogLookup, resolve_raw_type_name};
use pgrust_catalog_data::PgOperatorRow;
use pgrust_nodes::parsenodes::{ParseError, QualifiedNameRef, RawTypeName};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorCommandError {
    Parse(ParseError),
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

pub fn operator_signature_display(
    name: &str,
    left_type: u32,
    right_type: u32,
    format_type: impl Fn(u32) -> String,
) -> String {
    match (left_type, right_type) {
        (0, 0) => name.to_string(),
        (0, right) => format!("{name} {}", format_type(right)),
        (left, 0) => format!("{} {name}", format_type(left)),
        (left, right) => format!("{} {name} {}", format_type(left), format_type(right)),
    }
}

pub fn unsupported_postfix_operator_error() -> OperatorCommandError {
    detailed_error("postfix operators are not supported", None, "0A000")
}

pub fn resolve_operator_type_oid(
    catalog: &dyn CatalogLookup,
    arg: &Option<RawTypeName>,
) -> Result<u32, OperatorCommandError> {
    match arg {
        Some(arg) => {
            if matches!(
                arg,
                RawTypeName::Named { name, .. } if name.eq_ignore_ascii_case("setof")
            ) {
                return Err(detailed_error(
                    "SETOF type not allowed for operator argument",
                    None,
                    "42601",
                ));
            }
            let sql_type =
                resolve_raw_type_name(arg, catalog).map_err(OperatorCommandError::Parse)?;
            catalog.type_oid_for_sql_type(sql_type).ok_or_else(|| {
                OperatorCommandError::Parse(ParseError::UnsupportedType(format!("{arg:?}")))
            })
        }
        None => Ok(0),
    }
}

pub fn resolve_proc_oid_for_name(
    catalog: &dyn CatalogLookup,
    target: &QualifiedNameRef,
    arg_type_oids: &[u32],
    missing_message: String,
) -> Result<u32, OperatorCommandError> {
    let target_namespace_oid = target.schema_name.as_deref().and_then(|schema| {
        catalog
            .namespace_rows()
            .into_iter()
            .find(|row| row.nspname.eq_ignore_ascii_case(schema))
            .map(|row| row.oid)
    });
    let desired = arg_type_oids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    catalog
        .proc_rows_by_name(&target.name)
        .into_iter()
        .find(|row| {
            row.proname.eq_ignore_ascii_case(&target.name)
                && row.proargtypes == desired
                && target
                    .schema_name
                    .as_ref()
                    .map(|_| target_namespace_oid == Some(row.pronamespace))
                    .unwrap_or(true)
        })
        .map(|row| row.oid)
        .ok_or_else(|| {
            OperatorCommandError::Parse(ParseError::DetailedError {
                message: missing_message,
                detail: None,
                hint: None,
                sqlstate: "42883",
            })
        })
}

pub fn lookup_operator_row_in_rows(
    rows: impl IntoIterator<Item = PgOperatorRow>,
    namespace_oid: Option<u32>,
    name: &str,
    left_type: u32,
    right_type: u32,
) -> Option<PgOperatorRow> {
    rows.into_iter().find(|row| {
        row.oprname.eq_ignore_ascii_case(name)
            && namespace_oid.is_none_or(|oid| row.oprnamespace == oid)
            && row.oprleft == left_type
            && row.oprright == right_type
    })
}

fn detailed_error(
    message: impl Into<String>,
    detail: Option<String>,
    sqlstate: &'static str,
) -> OperatorCommandError {
    OperatorCommandError::Detailed {
        message: message.into(),
        detail,
        hint: None,
        sqlstate,
    }
}
