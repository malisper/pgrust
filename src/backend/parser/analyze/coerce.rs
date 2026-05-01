use super::*;
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::bootstrap_pg_cast_rows;
use crate::include::catalog::{
    builtin_multirange_name_for_sql_type, builtin_range_name_for_sql_type,
    builtin_type_name_for_oid,
};

pub(super) fn coerce_bound_expr(expr: Expr, from: SqlType, to: SqlType) -> Expr {
    if from == to {
        return expr;
    }
    if let Some(expr) = lower_special_cast(&expr, from, to) {
        return expr;
    }
    Expr::Cast(Box::new(expr), to)
}

pub fn is_binary_coercible_type(from: SqlType, to: SqlType) -> bool {
    let from = from.element_type();
    let to = to.element_type();

    if from == to {
        return true;
    }

    if text_typmod_coercion_is_required(from, to) {
        return false;
    }

    if matches!(
        from.kind,
        SqlTypeKind::AnyRange | SqlTypeKind::AnyCompatibleRange
    ) && to.is_range()
    {
        return true;
    }

    if matches!(
        from.kind,
        SqlTypeKind::AnyMultirange | SqlTypeKind::AnyCompatibleMultirange
    ) && to.is_multirange()
    {
        return true;
    }

    let from_oid = sql_type_oid(from);
    let to_oid = sql_type_oid(to);

    if from_oid == 0 || to_oid == 0 {
        return false;
    }

    bootstrap_pg_cast_rows()
        .into_iter()
        .any(|row| row.castsource == from_oid && row.casttarget == to_oid && row.castmethod == 'b')
}

fn text_typmod_coercion_is_required(from: SqlType, to: SqlType) -> bool {
    !from.is_array
        && !to.is_array
        && to.typmod >= SqlType::VARHDRSZ
        && matches!(to.kind, SqlTypeKind::Char | SqlTypeKind::Varchar)
}

fn lower_special_cast(expr: &Expr, from: SqlType, to: SqlType) -> Option<Expr> {
    if matches!(from.element_type().kind, SqlTypeKind::Char)
        && matches!(to.element_type().kind, SqlTypeKind::Text)
        && !from.is_array
        && !to.is_array
    {
        return Some(Expr::builtin_func(
            BuiltinScalarFunction::BpcharToText,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![expr.clone()],
        ));
    }
    if matches!(from.element_type().kind, SqlTypeKind::Char)
        && matches!(to.element_type().kind, SqlTypeKind::Varchar)
        && !from.is_array
        && !to.is_array
    {
        let trimmed = Expr::builtin_func(
            BuiltinScalarFunction::BpcharToText,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![expr.clone()],
        );
        return Some(if text_typmod_coercion_is_required(from, to) {
            Expr::Cast(Box::new(trimmed), to)
        } else {
            Expr::builtin_func(
                BuiltinScalarFunction::BpcharToText,
                Some(to),
                false,
                vec![expr.clone()],
            )
        });
    }
    if matches!(
        from.element_type().kind,
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
    ) && matches!(to.element_type().kind, SqlTypeKind::RegClass)
        && !from.is_array
        && !to.is_array
    {
        return Some(Expr::builtin_func(
            BuiltinScalarFunction::TextToRegClass,
            Some(SqlType::new(SqlTypeKind::RegClass)),
            false,
            vec![expr.clone()],
        ));
    }
    if matches!(from.element_type().kind, SqlTypeKind::RegProc)
        && matches!(to.element_type().kind, SqlTypeKind::Text)
        && !from.is_array
        && !to.is_array
    {
        return Some(Expr::builtin_func(
            BuiltinScalarFunction::RegProcToText,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![expr.clone()],
        ));
    }
    if matches!(from.element_type().kind, SqlTypeKind::RegClass)
        && matches!(to.element_type().kind, SqlTypeKind::Text)
        && !from.is_array
        && !to.is_array
    {
        return Some(Expr::builtin_func(
            BuiltinScalarFunction::RegClassToText,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![expr.clone()],
        ));
    }
    if matches!(from.element_type().kind, SqlTypeKind::RegRole)
        && matches!(to.element_type().kind, SqlTypeKind::Text)
        && !from.is_array
        && !to.is_array
    {
        return Some(Expr::builtin_func(
            BuiltinScalarFunction::RegRoleToText,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![expr.clone()],
        ));
    }
    if matches!(from.element_type().kind, SqlTypeKind::RegType)
        && matches!(to.element_type().kind, SqlTypeKind::Text)
        && !from.is_array
        && !to.is_array
    {
        return Some(Expr::builtin_func(
            BuiltinScalarFunction::RegTypeToText,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![expr.clone()],
        ));
    }
    if matches!(from.element_type().kind, SqlTypeKind::RegOper)
        && matches!(to.element_type().kind, SqlTypeKind::Text)
        && !from.is_array
        && !to.is_array
    {
        return Some(Expr::builtin_func(
            BuiltinScalarFunction::RegOperToText,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![expr.clone()],
        ));
    }
    if matches!(from.element_type().kind, SqlTypeKind::RegOperator)
        && matches!(to.element_type().kind, SqlTypeKind::Text)
        && !from.is_array
        && !to.is_array
    {
        return Some(Expr::builtin_func(
            BuiltinScalarFunction::RegOperatorToText,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![expr.clone()],
        ));
    }
    if matches!(from.element_type().kind, SqlTypeKind::RegProcedure)
        && matches!(to.element_type().kind, SqlTypeKind::Text)
        && !from.is_array
        && !to.is_array
    {
        return Some(Expr::builtin_func(
            BuiltinScalarFunction::RegProcedureToText,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![expr.clone()],
        ));
    }
    if matches!(from.element_type().kind, SqlTypeKind::RegCollation)
        && matches!(to.element_type().kind, SqlTypeKind::Text)
        && !from.is_array
        && !to.is_array
    {
        return Some(Expr::builtin_func(
            BuiltinScalarFunction::RegCollationToText,
            Some(SqlType::new(SqlTypeKind::Text)),
            false,
            vec![expr.clone()],
        ));
    }
    None
}

pub(super) fn resolve_numeric_binary_type(
    op: &'static str,
    left: SqlType,
    right: SqlType,
) -> Result<SqlType, ParseError> {
    use SqlTypeKind::*;
    let left = left.element_type();
    let right = right.element_type();
    if op == "%" && (matches!(left.kind, Float4 | Float8) || matches!(right.kind, Float4 | Float8))
    {
        return Err(ParseError::UndefinedOperator {
            op: op.into(),
            left_type: sql_type_name(left),
            right_type: sql_type_name(right),
        });
    }
    if !is_numeric_family(left) || !is_numeric_family(right) {
        return Err(ParseError::UndefinedOperator {
            op: op.into(),
            left_type: sql_type_name(left),
            right_type: sql_type_name(right),
        });
    }
    if matches!(left.kind, Float8) || matches!(right.kind, Float8) {
        return Ok(SqlType::new(Float8));
    }
    if matches!(left.kind, Float4) || matches!(right.kind, Float4) {
        return Ok(SqlType::new(Float4));
    }
    if matches!(left.kind, Numeric) || matches!(right.kind, Numeric) {
        return Ok(SqlType::new(Numeric));
    }
    if matches!(left.kind, Int8) || matches!(right.kind, Int8) {
        return Ok(SqlType::new(Int8));
    }
    if matches!(
        left.kind,
        Int4 | Oid | RegProc | RegType | RegRole | RegNamespace | RegOper | RegCollation
    ) || matches!(
        right.kind,
        Int4 | Oid | RegProc | RegType | RegRole | RegNamespace | RegOper | RegCollation
    ) {
        return Ok(SqlType::new(Int4));
    }
    Ok(SqlType::new(Int2))
}

pub(super) fn resolve_generate_series_common_type(
    start: SqlType,
    stop: SqlType,
    step: Option<SqlType>,
) -> Result<SqlType, ParseError> {
    if matches!(
        start.kind,
        SqlTypeKind::Timestamp | SqlTypeKind::TimestampTz
    ) || matches!(stop.kind, SqlTypeKind::Timestamp | SqlTypeKind::TimestampTz)
    {
        let common = if matches!(start.kind, SqlTypeKind::TimestampTz)
            || matches!(stop.kind, SqlTypeKind::TimestampTz)
        {
            SqlType::new(SqlTypeKind::TimestampTz)
        } else {
            SqlType::new(SqlTypeKind::Timestamp)
        };
        if !matches!(step.map(|ty| ty.kind), Some(SqlTypeKind::Interval) | None) {
            return Err(ParseError::UnexpectedToken {
                expected: "generate_series timestamp bounds and interval step",
                actual: step
                    .map(sql_type_name)
                    .unwrap_or_else(|| "missing step".into()),
            });
        }
        return Ok(common);
    }
    let mut common = resolve_numeric_binary_type("+", start, stop)?;
    if let Some(step_type) = step {
        common = resolve_numeric_binary_type("+", common, step_type)?;
    }
    if !matches!(
        common.kind,
        SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Numeric
    ) {
        return Err(ParseError::UnexpectedToken {
            expected: "generate_series integer or numeric arguments",
            actual: sql_type_name(common),
        });
    }
    Ok(common)
}

pub(crate) fn sql_type_name(ty: SqlType) -> String {
    let base = if ty.is_range() {
        builtin_range_name_for_sql_type(ty).unwrap_or("range")
    } else if ty.is_multirange() {
        builtin_multirange_name_for_sql_type(ty).unwrap_or("multirange")
    } else if !ty.is_array && ty.type_oid != 0 {
        return builtin_type_name_for_oid(ty.type_oid).unwrap_or_else(|| ty.type_oid.to_string());
    } else {
        match ty.kind {
            SqlTypeKind::AnyElement => "anyelement",
            SqlTypeKind::AnyArray => "anyarray",
            SqlTypeKind::AnyRange => "anyrange",
            SqlTypeKind::AnyMultirange => "anymultirange",
            SqlTypeKind::AnyCompatible => "anycompatible",
            SqlTypeKind::AnyCompatibleArray => "anycompatiblearray",
            SqlTypeKind::AnyCompatibleRange => "anycompatiblerange",
            SqlTypeKind::AnyCompatibleMultirange => "anycompatiblemultirange",
            SqlTypeKind::AnyEnum => "anyenum",
            SqlTypeKind::Enum => return ty.type_oid.to_string(),
            SqlTypeKind::Record => "record",
            SqlTypeKind::Composite => "record",
            SqlTypeKind::Shell => "shell",
            SqlTypeKind::Internal => "internal",
            SqlTypeKind::Trigger => "trigger",
            SqlTypeKind::EventTrigger => "event_trigger",
            SqlTypeKind::Void => "void",
            SqlTypeKind::Cstring => "cstring",
            SqlTypeKind::FdwHandler => "fdw_handler",
            SqlTypeKind::Int2 => "smallint",
            SqlTypeKind::Int2Vector => "int2vector",
            SqlTypeKind::Int4 => "integer",
            SqlTypeKind::Int8 => "bigint",
            SqlTypeKind::Name => "name",
            SqlTypeKind::Oid => "oid",
            SqlTypeKind::RegProc => "regproc",
            SqlTypeKind::RegClass => "regclass",
            SqlTypeKind::RegType => "regtype",
            SqlTypeKind::RegRole => "regrole",
            SqlTypeKind::RegNamespace => "regnamespace",
            SqlTypeKind::RegOper => "regoper",
            SqlTypeKind::RegOperator => "regoperator",
            SqlTypeKind::RegProcedure => "regprocedure",
            SqlTypeKind::RegCollation => "regcollation",
            SqlTypeKind::Tid => "tid",
            SqlTypeKind::Xid => "xid",
            SqlTypeKind::OidVector => "oidvector",
            SqlTypeKind::Bit => "bit",
            SqlTypeKind::VarBit => "bit varying",
            SqlTypeKind::Bytea => "bytea",
            SqlTypeKind::Uuid => "uuid",
            SqlTypeKind::Inet => "inet",
            SqlTypeKind::Cidr => "cidr",
            SqlTypeKind::MacAddr => "macaddr",
            SqlTypeKind::MacAddr8 => "macaddr8",
            SqlTypeKind::Float4 => "real",
            SqlTypeKind::Float8 => "double precision",
            SqlTypeKind::Money => "money",
            SqlTypeKind::Numeric => "numeric",
            SqlTypeKind::Json => "json",
            SqlTypeKind::Jsonb => "jsonb",
            SqlTypeKind::JsonPath => "jsonpath",
            SqlTypeKind::Xml => "xml",
            SqlTypeKind::Date => "date",
            SqlTypeKind::Time => "time without time zone",
            SqlTypeKind::TimeTz => "time with time zone",
            SqlTypeKind::Interval => "interval",
            SqlTypeKind::TsVector => "tsvector",
            SqlTypeKind::TsQuery => "tsquery",
            SqlTypeKind::RegConfig => "regconfig",
            SqlTypeKind::RegDictionary => "regdictionary",
            SqlTypeKind::PgLsn => "pg_lsn",
            SqlTypeKind::Text => "text",
            SqlTypeKind::Bool => "boolean",
            SqlTypeKind::Point => "point",
            SqlTypeKind::Lseg => "lseg",
            SqlTypeKind::Path => "path",
            SqlTypeKind::Box => "box",
            SqlTypeKind::Polygon => "polygon",
            SqlTypeKind::Line => "line",
            SqlTypeKind::Circle => "circle",
            SqlTypeKind::Timestamp => "timestamp without time zone",
            SqlTypeKind::TimestampTz => "timestamp with time zone",
            SqlTypeKind::PgNodeTree => "pg_node_tree",
            SqlTypeKind::InternalChar => "\"char\"",
            SqlTypeKind::Char => "character",
            SqlTypeKind::Varchar => "character varying",
            SqlTypeKind::Range
            | SqlTypeKind::Int4Range
            | SqlTypeKind::Int8Range
            | SqlTypeKind::NumericRange
            | SqlTypeKind::DateRange
            | SqlTypeKind::TimestampRange
            | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
            SqlTypeKind::Multirange => unreachable!("multirange handled above"),
        }
    };
    if ty.is_array {
        format!("{base}[]")
    } else {
        base.to_string()
    }
}

pub(super) fn is_numeric_family(ty: SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            SqlTypeKind::Int2
                | SqlTypeKind::Int4
                | SqlTypeKind::Int8
                | SqlTypeKind::Oid
                | SqlTypeKind::RegProc
                | SqlTypeKind::RegClass
                | SqlTypeKind::RegType
                | SqlTypeKind::RegRole
                | SqlTypeKind::RegNamespace
                | SqlTypeKind::RegOper
                | SqlTypeKind::RegOperator
                | SqlTypeKind::RegProcedure
                | SqlTypeKind::RegCollation
                | SqlTypeKind::RegConfig
                | SqlTypeKind::RegDictionary
                | SqlTypeKind::Float4
                | SqlTypeKind::Float8
                | SqlTypeKind::Numeric
        )
}

pub(super) fn is_integer_family(ty: SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            SqlTypeKind::Int2
                | SqlTypeKind::Int4
                | SqlTypeKind::Int8
                | SqlTypeKind::Oid
                | SqlTypeKind::RegProc
                | SqlTypeKind::RegClass
                | SqlTypeKind::RegType
                | SqlTypeKind::RegRole
                | SqlTypeKind::RegNamespace
                | SqlTypeKind::RegOper
                | SqlTypeKind::RegOperator
                | SqlTypeKind::RegProcedure
                | SqlTypeKind::RegCollation
                | SqlTypeKind::RegConfig
                | SqlTypeKind::RegDictionary
        )
}

pub(super) fn is_bit_string_type(ty: SqlType) -> bool {
    !ty.is_array && matches!(ty.kind, SqlTypeKind::Bit | SqlTypeKind::VarBit)
}

pub(super) fn is_macaddr_type(ty: SqlType) -> bool {
    !ty.is_array && matches!(ty.kind, SqlTypeKind::MacAddr | SqlTypeKind::MacAddr8)
}

pub(super) fn is_geometry_type(ty: SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            SqlTypeKind::Point
                | SqlTypeKind::Lseg
                | SqlTypeKind::Path
                | SqlTypeKind::Box
                | SqlTypeKind::Polygon
                | SqlTypeKind::Line
                | SqlTypeKind::Circle
        )
}

pub(super) fn is_text_like_type(ty: SqlType) -> bool {
    matches!(
        ty.element_type().kind,
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
    )
}

fn is_string_literal_expr(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
    )
}

fn is_null_literal_expr(expr: &SqlExpr) -> bool {
    matches!(expr, SqlExpr::Const(Value::Null))
}

pub(super) fn unknown_string_literal_peer_type(peer_type: SqlType) -> Option<SqlType> {
    if peer_type.is_array {
        return Some(peer_type);
    }
    if is_numeric_family(peer_type) {
        return Some(peer_type.element_type());
    }
    if matches!(peer_type.element_type().kind, SqlTypeKind::Money) {
        return Some(SqlType::new(SqlTypeKind::Money));
    }
    if is_bit_string_type(peer_type) {
        return Some(SqlType::new(SqlTypeKind::VarBit));
    }
    match peer_type.element_type().kind {
        SqlTypeKind::Date => return Some(SqlType::new(SqlTypeKind::Date)),
        SqlTypeKind::Time => return Some(SqlType::new(SqlTypeKind::Time)),
        SqlTypeKind::TimeTz => return Some(SqlType::new(SqlTypeKind::TimeTz)),
        SqlTypeKind::Timestamp => return Some(SqlType::new(SqlTypeKind::Timestamp)),
        SqlTypeKind::TimestampTz => return Some(SqlType::new(SqlTypeKind::TimestampTz)),
        SqlTypeKind::Interval => return Some(SqlType::new(SqlTypeKind::Interval)),
        SqlTypeKind::Jsonb => return Some(SqlType::new(SqlTypeKind::Jsonb)),
        SqlTypeKind::JsonPath => return Some(SqlType::new(SqlTypeKind::JsonPath)),
        SqlTypeKind::Bytea => return Some(SqlType::new(SqlTypeKind::Bytea)),
        SqlTypeKind::Uuid => return Some(SqlType::new(SqlTypeKind::Uuid)),
        SqlTypeKind::Enum if peer_type.type_oid != 0 => return Some(peer_type),
        SqlTypeKind::Composite if peer_type.type_oid != 0 => return Some(peer_type),
        SqlTypeKind::Record if peer_type.type_oid != 0 => return Some(peer_type),
        SqlTypeKind::InternalChar => return Some(SqlType::new(SqlTypeKind::InternalChar)),
        SqlTypeKind::Name => return Some(SqlType::new(SqlTypeKind::Name)),
        SqlTypeKind::Varchar => return Some(SqlType::new(SqlTypeKind::Varchar)),
        SqlTypeKind::Char => return Some(SqlType::new(SqlTypeKind::Char)),
        SqlTypeKind::Inet => return Some(SqlType::new(SqlTypeKind::Inet)),
        SqlTypeKind::Cidr => return Some(SqlType::new(SqlTypeKind::Cidr)),
        SqlTypeKind::MacAddr => return Some(SqlType::new(SqlTypeKind::MacAddr)),
        SqlTypeKind::MacAddr8 => return Some(SqlType::new(SqlTypeKind::MacAddr8)),
        SqlTypeKind::TsQuery => return Some(SqlType::new(SqlTypeKind::TsQuery)),
        SqlTypeKind::TsVector => return Some(SqlType::new(SqlTypeKind::TsVector)),
        SqlTypeKind::Tid => return Some(SqlType::new(SqlTypeKind::Tid)),
        SqlTypeKind::Int2Vector => return Some(SqlType::new(SqlTypeKind::Int2Vector)),
        SqlTypeKind::OidVector => return Some(SqlType::new(SqlTypeKind::OidVector)),
        SqlTypeKind::Void => return Some(SqlType::new(SqlTypeKind::Void)),
        SqlTypeKind::Shell => return Some(SqlType::new(SqlTypeKind::Shell)),
        SqlTypeKind::Cstring => return Some(SqlType::new(SqlTypeKind::Cstring)),
        SqlTypeKind::FdwHandler => return Some(SqlType::new(SqlTypeKind::FdwHandler)),
        SqlTypeKind::RegClass => return Some(SqlType::new(SqlTypeKind::RegClass)),
        SqlTypeKind::RegType => return Some(SqlType::new(SqlTypeKind::RegType)),
        SqlTypeKind::RegRole => return Some(SqlType::new(SqlTypeKind::RegRole)),
        SqlTypeKind::RegNamespace => return Some(SqlType::new(SqlTypeKind::RegNamespace)),
        SqlTypeKind::RegOperator => return Some(SqlType::new(SqlTypeKind::RegOperator)),
        SqlTypeKind::RegProcedure => return Some(SqlType::new(SqlTypeKind::RegProcedure)),
        SqlTypeKind::RegConfig => return Some(SqlType::new(SqlTypeKind::RegConfig)),
        SqlTypeKind::RegDictionary => return Some(SqlType::new(SqlTypeKind::RegDictionary)),
        SqlTypeKind::PgLsn => return Some(SqlType::new(SqlTypeKind::PgLsn)),
        _ => {}
    }
    if peer_type.is_array {
        match peer_type.kind {
            SqlTypeKind::TsQuery => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::TsQuery)));
            }
            SqlTypeKind::TsVector => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::TsVector)));
            }
            SqlTypeKind::Void => return Some(SqlType::array_of(SqlType::new(SqlTypeKind::Void))),
            SqlTypeKind::Shell => return Some(SqlType::array_of(SqlType::new(SqlTypeKind::Shell))),
            SqlTypeKind::Cstring => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::Cstring)));
            }
            SqlTypeKind::FdwHandler => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::FdwHandler)));
            }
            SqlTypeKind::RegClass => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::RegClass)));
            }
            SqlTypeKind::RegType => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::RegType)));
            }
            SqlTypeKind::RegRole => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::RegRole)));
            }
            SqlTypeKind::RegNamespace => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::RegNamespace)));
            }
            SqlTypeKind::RegOperator => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::RegOperator)));
            }
            SqlTypeKind::RegProcedure => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::RegProcedure)));
            }
            SqlTypeKind::RegConfig => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::RegConfig)));
            }
            SqlTypeKind::RegDictionary => {
                return Some(SqlType::array_of(SqlType::new(SqlTypeKind::RegDictionary)));
            }
            _ => {}
        }
    }
    if is_geometry_type(peer_type) {
        return Some(peer_type.element_type());
    }
    if peer_type.is_range() {
        return Some(peer_type.element_type());
    }
    None
}

pub(super) fn coerce_unknown_string_literal_type(
    expr: &SqlExpr,
    expr_type: SqlType,
    peer_type: SqlType,
) -> SqlType {
    if matches!(expr, SqlExpr::Const(Value::Null)) {
        if !matches!(peer_type.kind, SqlTypeKind::Text) || peer_type.type_oid != 0 {
            return peer_type;
        }
    }
    if is_string_literal_expr(expr) {
        if let Some(coerced) = unknown_string_literal_peer_type(peer_type) {
            return coerced;
        }
    }
    expr_type
}

pub(super) fn coerce_unknown_set_operation_literal_type(
    expr: &SqlExpr,
    expr_type: SqlType,
    peer_type: SqlType,
) -> SqlType {
    if is_null_literal_expr(expr) {
        return peer_type;
    }
    coerce_unknown_string_literal_type(expr, expr_type, peer_type)
}

pub(super) fn should_use_text_concat(
    left_expr: &SqlExpr,
    left_type: SqlType,
    right_expr: &SqlExpr,
    right_type: SqlType,
) -> bool {
    if left_type.is_array || right_type.is_array {
        return false;
    }
    is_text_like_type(left_type)
        || is_text_like_type(right_type)
        || is_string_literal_expr(left_expr)
        || is_string_literal_expr(right_expr)
}

pub(super) fn resolve_common_scalar_type(left: SqlType, right: SqlType) -> Option<SqlType> {
    if left.is_array || right.is_array {
        if left.is_array && right.is_array {
            let common_element =
                resolve_common_scalar_type(left.element_type(), right.element_type())?;
            return Some(SqlType::array_of(common_element));
        }
        return None;
    }
    let left = left.element_type();
    let right = right.element_type();
    if left == right {
        return Some(left);
    }
    if matches!(left.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
        && matches!(right.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
    {
        if left.kind == SqlTypeKind::Composite
            && right.kind == SqlTypeKind::Composite
            && left.typrelid != 0
            && left.typrelid == right.typrelid
        {
            return Some(left);
        }
        return Some(SqlType::record(crate::include::catalog::RECORD_TYPE_OID));
    }
    if matches!(left.kind, SqlTypeKind::Varchar) && matches!(right.kind, SqlTypeKind::Varchar) {
        return Some(SqlType::new(SqlTypeKind::Varchar));
    }
    if matches!(left.kind, SqlTypeKind::Char) && matches!(right.kind, SqlTypeKind::Char) {
        return Some(SqlType::new(SqlTypeKind::Char));
    }
    if is_text_like_type(left) && is_text_like_type(right) {
        return Some(SqlType::new(SqlTypeKind::Text));
    }
    if (matches!(left.kind, SqlTypeKind::InternalChar) && is_text_like_type(right))
        || (matches!(right.kind, SqlTypeKind::InternalChar) && is_text_like_type(left))
    {
        return Some(SqlType::new(SqlTypeKind::Text));
    }
    if is_bit_string_type(left) && is_bit_string_type(right) {
        if matches!(left.kind, SqlTypeKind::VarBit) || matches!(right.kind, SqlTypeKind::VarBit) {
            return Some(SqlType::new(SqlTypeKind::VarBit));
        }
        if left.bit_len() == right.bit_len() {
            return Some(left);
        }
        return Some(SqlType::new(SqlTypeKind::VarBit));
    }
    if matches!(left.kind, SqlTypeKind::Inet | SqlTypeKind::Cidr)
        && matches!(right.kind, SqlTypeKind::Inet | SqlTypeKind::Cidr)
    {
        return Some(SqlType::new(SqlTypeKind::Inet));
    }
    match (left.kind, right.kind) {
        (SqlTypeKind::Date, SqlTypeKind::Timestamp)
        | (SqlTypeKind::Timestamp, SqlTypeKind::Date) => {
            return Some(SqlType::new(SqlTypeKind::Timestamp));
        }
        (SqlTypeKind::Date, SqlTypeKind::TimestampTz)
        | (SqlTypeKind::TimestampTz, SqlTypeKind::Date)
        | (SqlTypeKind::Timestamp, SqlTypeKind::TimestampTz)
        | (SqlTypeKind::TimestampTz, SqlTypeKind::Timestamp) => {
            return Some(SqlType::new(SqlTypeKind::TimestampTz));
        }
        _ => {}
    }
    if is_numeric_family(left) && is_numeric_family(right) {
        return resolve_numeric_binary_type("+", left, right).ok();
    }
    if let Some(temporal) = resolve_common_temporal_type(left, right) {
        return Some(temporal);
    }
    None
}

fn resolve_common_temporal_type(left: SqlType, right: SqlType) -> Option<SqlType> {
    use SqlTypeKind::{Date, Time, TimeTz, Timestamp, TimestampTz};

    match (left.kind, right.kind) {
        (TimestampTz, Date | Timestamp) | (Date | Timestamp, TimestampTz) => {
            Some(SqlType::new(TimestampTz))
        }
        (Timestamp, Date) | (Date, Timestamp) => Some(SqlType::new(Timestamp)),
        (TimeTz, Time) | (Time, TimeTz) => Some(SqlType::new(TimeTz)),
        _ => None,
    }
}

pub(super) fn resolve_array_concat_element_type(
    left: SqlType,
    right: SqlType,
) -> Result<SqlType, ParseError> {
    let left_elem = left.element_type();
    let right_elem = right.element_type();
    if left.is_array && right.is_array {
        return resolve_common_scalar_type(left_elem, right_elem).ok_or(
            ParseError::UndefinedOperator {
                op: "||".into(),
                left_type: sql_type_name(left),
                right_type: sql_type_name(right),
            },
        );
    }
    if left.is_array {
        return resolve_common_scalar_type(left_elem, right_elem).ok_or(
            ParseError::UndefinedOperator {
                op: "||".into(),
                left_type: sql_type_name(left),
                right_type: sql_type_name(right),
            },
        );
    }
    if right.is_array {
        return resolve_common_scalar_type(left_elem, right_elem).ok_or(
            ParseError::UndefinedOperator {
                op: "||".into(),
                left_type: sql_type_name(left),
                right_type: sql_type_name(right),
            },
        );
    }
    Err(ParseError::UndefinedOperator {
        op: "||".into(),
        left_type: sql_type_name(left),
        right_type: sql_type_name(right),
    })
}

pub(super) fn infer_integer_literal_type(value: &str) -> SqlType {
    let normalized = normalize_numeric_literal_token(value);
    if normalized.parse::<i32>().is_ok() {
        SqlType::new(SqlTypeKind::Int4)
    } else if normalized.parse::<i64>().is_ok() {
        SqlType::new(SqlTypeKind::Int8)
    } else {
        SqlType::new(SqlTypeKind::Numeric)
    }
}

pub(super) fn infer_arithmetic_sql_type(expr: &SqlExpr, left: SqlType, right: SqlType) -> SqlType {
    use SqlTypeKind::*;

    let left = left.element_type();
    let right = right.element_type();

    match expr {
        SqlExpr::Add(_, _) if matches!((left.kind, right.kind), (Date, Time) | (Time, Date)) => {
            return SqlType::new(Timestamp);
        }
        SqlExpr::Add(_, _)
            if matches!((left.kind, right.kind), (Date, TimeTz) | (TimeTz, Date)) =>
        {
            return SqlType::new(TimestampTz);
        }
        SqlExpr::Sub(_, _) if matches!((left.kind, right.kind), (Date, Time)) => {
            return SqlType::new(Timestamp);
        }
        SqlExpr::Sub(_, _)
            if matches!(
                (left.kind, right.kind),
                (Timestamp, Timestamp) | (TimestampTz, TimestampTz)
            ) =>
        {
            return SqlType::new(Interval);
        }
        _ => {}
    }

    let has_interval = matches!(left.kind, Interval) || matches!(right.kind, Interval);
    if has_interval {
        return match expr {
            SqlExpr::Add(_, _)
                if matches!((left.kind, right.kind), (Time, Interval) | (Interval, Time)) =>
            {
                SqlType::new(Time)
            }
            SqlExpr::Sub(_, _) if matches!((left.kind, right.kind), (Time, Interval)) => {
                SqlType::new(Time)
            }
            SqlExpr::Add(_, _)
                if matches!(
                    (left.kind, right.kind),
                    (TimeTz, Interval) | (Interval, TimeTz)
                ) =>
            {
                SqlType::new(TimeTz)
            }
            SqlExpr::Sub(_, _) if matches!((left.kind, right.kind), (TimeTz, Interval)) => {
                SqlType::new(TimeTz)
            }
            SqlExpr::Add(_, _) if matches!(left.kind, Date) || matches!(right.kind, Date) => {
                SqlType::new(Timestamp)
            }
            SqlExpr::Add(_, _)
                if matches!(left.kind, Timestamp | TimestampTz)
                    || matches!(right.kind, Timestamp | TimestampTz) =>
            {
                if matches!(left.kind, TimestampTz) || matches!(right.kind, TimestampTz) {
                    SqlType::new(TimestampTz)
                } else {
                    SqlType::new(Timestamp)
                }
            }
            SqlExpr::Sub(_, _) if matches!(left.kind, Date) && matches!(right.kind, Interval) => {
                SqlType::new(Timestamp)
            }
            SqlExpr::Sub(_, _)
                if matches!(left.kind, Timestamp | TimestampTz)
                    && matches!(right.kind, Interval) =>
            {
                SqlType::new(left.kind)
            }
            SqlExpr::Add(_, _) | SqlExpr::Sub(_, _) => SqlType::new(Interval),
            SqlExpr::Mul(_, _) | SqlExpr::Div(_, _) => SqlType::new(Interval),
            _ => SqlType::new(Interval),
        };
    }

    if matches!(expr, SqlExpr::Add(_, _) | SqlExpr::Sub(_, _))
        && matches!(left.kind, PgLsn)
        && (matches!(right.kind, PgLsn) || is_numeric_family(right))
    {
        return if matches!(right.kind, PgLsn) {
            SqlType::new(Numeric)
        } else {
            SqlType::new(PgLsn)
        };
    }
    if matches!(expr, SqlExpr::Add(_, _)) && is_numeric_family(left) && matches!(right.kind, PgLsn)
    {
        return SqlType::new(PgLsn);
    }

    let has_float8 = matches!(left.kind, Float8) || matches!(right.kind, Float8);
    let has_float4 = matches!(left.kind, Float4) || matches!(right.kind, Float4);
    if has_float8 {
        return SqlType::new(Float8);
    }
    if has_float4 {
        return SqlType::new(Float4);
    }
    if matches!(left.kind, Numeric) || matches!(right.kind, Numeric) {
        return SqlType::new(Numeric);
    }

    let widest_int = if matches!(left.kind, Int8) || matches!(right.kind, Int8) {
        Int8
    } else if matches!(left.kind, Int4) || matches!(right.kind, Int4) {
        Int4
    } else {
        Int2
    };

    match expr {
        SqlExpr::Div(_, _) | SqlExpr::Mod(_, _) => SqlType::new(widest_int),
        SqlExpr::Add(_, _) | SqlExpr::Sub(_, _) | SqlExpr::Mul(_, _) => SqlType::new(widest_int),
        SqlExpr::BitAnd(_, _)
        | SqlExpr::BitOr(_, _)
        | SqlExpr::BitXor(_, _)
        | SqlExpr::Shl(_, _)
        | SqlExpr::Shr(_, _) => {
            if is_bit_string_type(left) && (is_bit_string_type(right) || is_integer_family(right)) {
                left
            } else {
                left
            }
        }
        _ => SqlType::new(Int4),
    }
}

pub(super) fn infer_concat_sql_type(expr: &SqlExpr, left: SqlType, right: SqlType) -> SqlType {
    let _ = expr;
    if left.kind == SqlTypeKind::Jsonb
        && !left.is_array
        && right.kind == SqlTypeKind::Jsonb
        && !right.is_array
    {
        return SqlType::new(SqlTypeKind::Jsonb);
    }
    if left.kind == SqlTypeKind::Bytea
        && !left.is_array
        && right.kind == SqlTypeKind::Bytea
        && !right.is_array
    {
        return SqlType::new(SqlTypeKind::Bytea);
    }
    if is_bit_string_type(left) && is_bit_string_type(right) {
        return resolve_common_scalar_type(left, right)
            .unwrap_or(SqlType::new(SqlTypeKind::VarBit));
    }
    if left.is_array || right.is_array {
        if let Ok(element_type) = resolve_array_concat_element_type(left, right) {
            return SqlType::array_of(element_type);
        }
    }
    SqlType::new(SqlTypeKind::Text)
}

pub(super) fn bind_integer_literal(value: &str) -> Result<Value, ParseError> {
    let normalized = normalize_numeric_literal_token(value);
    if let Ok(parsed) = normalized.parse::<i32>() {
        Ok(Value::Int32(parsed))
    } else if let Ok(parsed) = normalized.parse::<i64>() {
        Ok(Value::Int64(parsed))
    } else if normalized.chars().all(|ch| ch.is_ascii_digit()) {
        Ok(Value::Numeric(normalized.into_owned().into()))
    } else {
        Err(ParseError::InvalidInteger(value.to_string()))
    }
}

pub(super) fn bind_numeric_literal(value: &str) -> Result<Value, ParseError> {
    let normalized = normalize_numeric_literal_token(value);
    normalized
        .parse::<f64>()
        .map(|_| Value::Numeric(normalized.into_owned().into()))
        .map_err(|_| ParseError::InvalidNumeric(value.to_string()))
}

pub(super) fn normalize_numeric_literal_token(value: &str) -> std::borrow::Cow<'_, str> {
    if value.as_bytes().contains(&b'_') {
        std::borrow::Cow::Owned(value.replace('_', ""))
    } else {
        std::borrow::Cow::Borrowed(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_coercible_preserves_character_typmod_coercion() {
        assert!(!is_binary_coercible_type(
            SqlType::new(SqlTypeKind::Text),
            SqlType::with_char_len(SqlTypeKind::Char, 10),
        ));
        assert!(!is_binary_coercible_type(
            SqlType::new(SqlTypeKind::Text),
            SqlType::with_char_len(SqlTypeKind::Varchar, 10),
        ));
        assert!(is_binary_coercible_type(
            SqlType::new(SqlTypeKind::Text),
            SqlType::new(SqlTypeKind::Char),
        ));
    }
}
