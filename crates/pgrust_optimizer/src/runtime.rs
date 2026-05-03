use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use pgrust_analyze::CatalogLookup;
use pgrust_nodes::datetime::DateADT;
use pgrust_nodes::datum::{InetValue, RecordValue, Value};
use pgrust_nodes::parsenodes::{ParseError, Query, SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::{BuiltinScalarFunction, Expr, RelationPrivilegeRequirement};

#[derive(Debug, Clone)]
pub enum OptimizerEvalError {
    Parse(ParseError),
    DetailedError {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
    DivisionByZero(String),
    Other(String),
}

pub type ExecError = OptimizerEvalError;

#[derive(Debug, Clone, Copy, Default)]
pub struct DateTimeConfig;

#[derive(Debug, Clone, Copy)]
pub enum UnaryEvalOp {
    Negate,
    BitwiseNot,
}

#[derive(Debug, Clone, Copy)]
pub enum BinaryEvalOp {
    Add,
    Sub,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    ShiftLeft,
    ShiftRight,
    Mul,
    Div,
    Mod,
    Concat,
}

pub trait OptimizerServices: Sync {
    fn cast_value(&self, value: Value, ty: SqlType) -> Result<Value, ExecError> {
        default_cast_value(value, ty)
    }

    fn cast_value_with_source_type(
        &self,
        value: Value,
        source_type: Option<SqlType>,
        ty: SqlType,
        catalog: Option<&dyn CatalogLookup>,
        datetime_config: &DateTimeConfig,
    ) -> Result<Value, ExecError> {
        let _ = (source_type, catalog, datetime_config);
        self.cast_value(value, ty)
    }

    fn compare_order_values(
        &self,
        left: &Value,
        right: &Value,
        collation_oid: Option<u32>,
        nulls_first: Option<bool>,
        descending: bool,
    ) -> Result<Ordering, ExecError> {
        let _ = (collation_oid, nulls_first);
        let ordering = default_value_ordering(left, right)?;
        Ok(if descending {
            ordering.reverse()
        } else {
            ordering
        })
    }

    fn compare_values(
        &self,
        op: &'static str,
        left: Value,
        right: Value,
        collation_oid: Option<u32>,
    ) -> Result<Value, ExecError> {
        let ordering = self.compare_order_values(&left, &right, collation_oid, None, false)?;
        let value = match op {
            "=" => ordering == Ordering::Equal,
            "<>" => ordering != Ordering::Equal,
            "<" => ordering == Ordering::Less,
            "<=" => ordering != Ordering::Greater,
            ">" => ordering == Ordering::Greater,
            ">=" => ordering != Ordering::Less,
            _ => {
                return Err(ExecError::Other(format!(
                    "unsupported portable comparison operator {op}"
                )));
            }
        };
        Ok(Value::Bool(value))
    }

    fn eval_unary_op(&self, op: UnaryEvalOp, value: Value) -> Result<Value, ExecError> {
        let _ = (op, value);
        Err(ExecError::Other(
            "operator evaluation requires root optimizer services".into(),
        ))
    }

    fn eval_binary_op(
        &self,
        op: BinaryEvalOp,
        left: Value,
        right: Value,
    ) -> Result<Value, ExecError> {
        let _ = (op, left, right);
        Err(ExecError::Other(
            "operator evaluation requires root optimizer services".into(),
        ))
    }

    fn not_equal_values(
        &self,
        left: Value,
        right: Value,
        collation_oid: Option<u32>,
    ) -> Result<Value, ExecError> {
        let value = self.compare_values("=", left, right, collation_oid)?;
        match value {
            Value::Bool(value) => Ok(Value::Bool(!value)),
            other => Err(ExecError::Other(format!(
                "constant comparison returned non-bool value {other:?}"
            ))),
        }
    }

    fn order_values(
        &self,
        op: &'static str,
        left: Value,
        right: Value,
        collation_oid: Option<u32>,
    ) -> Result<Value, ExecError> {
        let ordering = self.compare_order_values(&left, &right, collation_oid, None, false)?;
        Ok(Value::Bool(match op {
            "<" => ordering == Ordering::Less,
            "<=" => ordering != Ordering::Greater,
            ">" => ordering == Ordering::Greater,
            ">=" => ordering != Ordering::Less,
            _ => false,
        }))
    }

    fn order_record_image_values(
        &self,
        op: &'static str,
        left: &RecordValue,
        right: &RecordValue,
    ) -> Result<Value, ExecError> {
        let _ = (op, left, right);
        Err(ExecError::Other(
            "record-image comparison requires root optimizer services".into(),
        ))
    }

    fn values_are_distinct(&self, left: &Value, right: &Value) -> bool {
        left != right
    }

    fn statistics_value_key(&self, value: &Value) -> Option<String> {
        default_statistics_value_key(value)
    }

    fn eval_geometry_function(
        &self,
        func: BuiltinScalarFunction,
        values: &[Value],
    ) -> Option<Result<Value, ExecError>> {
        let _ = (func, values);
        None
    }

    fn eval_power_function(&self, values: &[Value]) -> Result<Value, ExecError> {
        let _ = values;
        Err(ExecError::Other(
            "power evaluation requires root optimizer services".into(),
        ))
    }

    fn eval_range_function(
        &self,
        func: BuiltinScalarFunction,
        values: &[Value],
        result_type: Option<SqlType>,
        func_variadic: bool,
        catalog: Option<&dyn CatalogLookup>,
        datetime_config: &DateTimeConfig,
    ) -> Option<Result<Value, ExecError>> {
        let _ = (
            func,
            values,
            result_type,
            func_variadic,
            catalog,
            datetime_config,
        );
        None
    }

    fn parse_date_text(
        &self,
        text: &str,
        datetime_config: &DateTimeConfig,
    ) -> Result<DateADT, String> {
        let _ = (text, datetime_config);
        Err("date parsing requires root optimizer services".into())
    }

    fn hash_value_extended(
        &self,
        value: &Value,
        opclass: Option<u32>,
        seed: u64,
    ) -> Result<Option<u64>, String> {
        let _ = opclass;
        default_hash_value_extended(value, seed)
    }

    fn hash_combine64(&self, left: u64, right: u64) -> u64 {
        default_hash_combine64(left, right)
    }

    fn access_method_supports_index_scan(&self, am_oid: u32) -> bool {
        let _ = am_oid;
        false
    }

    fn access_method_supports_bitmap_scan(&self, am_oid: u32) -> bool {
        let _ = am_oid;
        false
    }

    fn pg_rewrite_query(
        &self,
        query: Query,
        catalog: &dyn CatalogLookup,
    ) -> Result<Vec<Query>, ParseError> {
        let _ = catalog;
        Ok(vec![query])
    }

    fn collect_query_relation_privileges(
        &self,
        query: &Query,
    ) -> Vec<RelationPrivilegeRequirement> {
        query
            .rtable
            .iter()
            .filter_map(|rte| rte.permission.clone())
            .collect()
    }

    fn render_explain_expr(&self, expr: &Expr, column_names: &[String]) -> String {
        let _ = column_names;
        format!("{expr:?}")
    }
}

struct DefaultOptimizerServices;

impl OptimizerServices for DefaultOptimizerServices {}

static DEFAULT_SERVICES: DefaultOptimizerServices = DefaultOptimizerServices;

thread_local! {
    static SERVICE_STACK: RefCell<Vec<&'static dyn OptimizerServices>> = const { RefCell::new(Vec::new()) };
}

pub fn with_optimizer_services<T>(
    services: &'static dyn OptimizerServices,
    f: impl FnOnce() -> T,
) -> T {
    SERVICE_STACK.with(|stack| stack.borrow_mut().push(services));
    let result = f();
    SERVICE_STACK.with(|stack| {
        let popped = stack.borrow_mut().pop();
        debug_assert!(popped.is_some());
    });
    result
}

fn with_services<T>(f: impl FnOnce(&dyn OptimizerServices) -> T) -> T {
    let services =
        SERVICE_STACK.with(|stack| stack.borrow().last().copied().unwrap_or(&DEFAULT_SERVICES));
    f(services)
}

pub fn cast_value(value: Value, ty: SqlType) -> Result<Value, ExecError> {
    with_services(|services| services.cast_value(value, ty))
}

pub fn cast_value_with_source_type_catalog_and_config(
    value: Value,
    source_type: Option<SqlType>,
    ty: SqlType,
    catalog: Option<&dyn CatalogLookup>,
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    with_services(|services| {
        services.cast_value_with_source_type(value, source_type, ty, catalog, datetime_config)
    })
}

pub fn compare_order_values(
    left: &Value,
    right: &Value,
    collation_oid: Option<u32>,
    nulls_first: Option<bool>,
    descending: bool,
) -> Result<Ordering, ExecError> {
    with_services(|services| {
        services.compare_order_values(left, right, collation_oid, nulls_first, descending)
    })
}

pub fn compare_values(
    op: &'static str,
    left: Value,
    right: Value,
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    with_services(|services| services.compare_values(op, left, right, collation_oid))
}

pub fn negate_value(value: Value) -> Result<Value, ExecError> {
    with_services(|services| services.eval_unary_op(UnaryEvalOp::Negate, value))
}

pub fn bitwise_not_value(value: Value) -> Result<Value, ExecError> {
    with_services(|services| services.eval_unary_op(UnaryEvalOp::BitwiseNot, value))
}

macro_rules! binary_eval {
    ($name:ident, $op:expr) => {
        pub fn $name(left: Value, right: Value) -> Result<Value, ExecError> {
            with_services(|services| services.eval_binary_op($op, left, right))
        }
    };
}

binary_eval!(add_values, BinaryEvalOp::Add);
binary_eval!(sub_values, BinaryEvalOp::Sub);
binary_eval!(bitwise_and_values, BinaryEvalOp::BitwiseAnd);
binary_eval!(bitwise_or_values, BinaryEvalOp::BitwiseOr);
binary_eval!(bitwise_xor_values, BinaryEvalOp::BitwiseXor);
binary_eval!(shift_left_values, BinaryEvalOp::ShiftLeft);
binary_eval!(shift_right_values, BinaryEvalOp::ShiftRight);
binary_eval!(mul_values, BinaryEvalOp::Mul);
binary_eval!(div_values, BinaryEvalOp::Div);
binary_eval!(mod_values, BinaryEvalOp::Mod);
binary_eval!(concat_values, BinaryEvalOp::Concat);

pub fn not_equal_values(
    left: Value,
    right: Value,
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    with_services(|services| services.not_equal_values(left, right, collation_oid))
}

pub fn order_values(
    op: &'static str,
    left: Value,
    right: Value,
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    with_services(|services| services.order_values(op, left, right, collation_oid))
}

pub fn order_record_image_values(
    op: &'static str,
    left: &RecordValue,
    right: &RecordValue,
) -> Result<Value, ExecError> {
    with_services(|services| services.order_record_image_values(op, left, right))
}

pub fn values_are_distinct(left: &Value, right: &Value) -> bool {
    with_services(|services| services.values_are_distinct(left, right))
}

pub fn statistics_value_key(value: &Value) -> Option<String> {
    with_services(|services| services.statistics_value_key(value))
}

pub fn eval_geometry_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Option<Result<Value, ExecError>> {
    with_services(|services| services.eval_geometry_function(func, values))
}

pub fn eval_power_function(values: &[Value]) -> Result<Value, ExecError> {
    with_services(|services| services.eval_power_function(values))
}

pub fn eval_range_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    result_type: Option<SqlType>,
    func_variadic: bool,
    catalog: Option<&dyn CatalogLookup>,
    datetime_config: &DateTimeConfig,
) -> Option<Result<Value, ExecError>> {
    with_services(|services| {
        services.eval_range_function(
            func,
            values,
            result_type,
            func_variadic,
            catalog,
            datetime_config,
        )
    })
}

pub fn parse_date_text(text: &str, datetime_config: &DateTimeConfig) -> Result<DateADT, String> {
    with_services(|services| services.parse_date_text(text, datetime_config))
}

pub fn hash_value_extended(
    value: &Value,
    opclass: Option<u32>,
    seed: u64,
) -> Result<Option<u64>, String> {
    with_services(|services| services.hash_value_extended(value, opclass, seed))
}

pub fn hash_combine64(left: u64, right: u64) -> u64 {
    with_services(|services| services.hash_combine64(left, right))
}

pub const HASH_PARTITION_SEED: u64 = 0x7A5B_2236_7996_DCFD;

pub fn access_method_supports_index_scan(am_oid: u32) -> bool {
    with_services(|services| services.access_method_supports_index_scan(am_oid))
}

pub fn access_method_supports_bitmap_scan(am_oid: u32) -> bool {
    with_services(|services| services.access_method_supports_bitmap_scan(am_oid))
}

pub fn pg_rewrite_query(
    query: Query,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Query>, ParseError> {
    with_services(|services| services.pg_rewrite_query(query, catalog))
}

pub fn collect_query_relation_privileges(query: &Query) -> Vec<RelationPrivilegeRequirement> {
    with_services(|services| services.collect_query_relation_privileges(query))
}

pub fn render_explain_expr(expr: &Expr, column_names: &[String]) -> String {
    with_services(|services| services.render_explain_expr(expr, column_names))
}

pub fn network_prefix(value: &InetValue) -> InetValue {
    InetValue {
        addr: mask_addr(value.addr, value.bits, false),
        bits: value.bits,
    }
}

pub fn network_btree_upper_bound(value: &InetValue) -> InetValue {
    let mut upper = InetValue {
        addr: mask_addr(value.addr, value.bits, true),
        bits: value.bits,
    };
    upper.bits = upper.max_bits();
    upper
}

fn mask_addr(addr: IpAddr, bits: u8, fill_host: bool) -> IpAddr {
    match addr {
        IpAddr::V4(addr) => {
            let raw = u32::from(addr);
            let mask = prefix_mask_u32(bits);
            let raw = if fill_host { raw | !mask } else { raw & mask };
            IpAddr::V4(Ipv4Addr::from(raw))
        }
        IpAddr::V6(addr) => {
            let raw = u128::from(addr);
            let mask = prefix_mask_u128(bits);
            let raw = if fill_host { raw | !mask } else { raw & mask };
            IpAddr::V6(Ipv6Addr::from(raw))
        }
    }
}

fn prefix_mask_u32(bits: u8) -> u32 {
    if bits == 0 {
        0
    } else {
        u32::MAX << (32 - bits)
    }
}

fn prefix_mask_u128(bits: u8) -> u128 {
    if bits == 0 {
        0
    } else {
        u128::MAX << (128 - bits)
    }
}

fn default_hash_combine64(mut left: u64, right: u64) -> u64 {
    left ^= right
        .wrapping_add(0x49a0_f4dd_15e5_a8e3)
        .wrapping_add(left << 54)
        .wrapping_add(left >> 7);
    left
}

fn default_cast_value(value: Value, ty: SqlType) -> Result<Value, ExecError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    if value.sql_type_hint().is_some_and(|source| source == ty) {
        return Ok(value);
    }
    match ty.kind {
        SqlTypeKind::Int2 => integer_value(&value)
            .and_then(|value| i16::try_from(value).ok())
            .map(Value::Int16)
            .ok_or_else(|| ExecError::Other(format!("cannot cast {value:?} to int2"))),
        SqlTypeKind::Int4 | SqlTypeKind::Oid => integer_value(&value)
            .and_then(|value| i32::try_from(value).ok())
            .map(Value::Int32)
            .ok_or_else(|| ExecError::Other(format!("cannot cast {value:?} to int4"))),
        SqlTypeKind::Int8 => integer_value(&value)
            .map(Value::Int64)
            .ok_or_else(|| ExecError::Other(format!("cannot cast {value:?} to int8"))),
        SqlTypeKind::Text | SqlTypeKind::Varchar | SqlTypeKind::Char | SqlTypeKind::Name => value
            .as_text()
            .map(|text| Value::Text(text.to_string().into()))
            .or_else(|| Some(Value::Text(format!("{value:?}").into())))
            .ok_or_else(|| ExecError::Other(format!("cannot cast {value:?} to text"))),
        SqlTypeKind::Bool => match value {
            Value::Bool(value) => Ok(Value::Bool(value)),
            Value::Text(text) => text
                .parse::<bool>()
                .map(Value::Bool)
                .map_err(|_| ExecError::Other("cannot cast text to bool".into())),
            other => Err(ExecError::Other(format!("cannot cast {other:?} to bool"))),
        },
        SqlTypeKind::Timestamp => match value {
            Value::Timestamp(value) => Ok(Value::Timestamp(value)),
            Value::TimestampTz(value) => Ok(Value::Timestamp(
                pgrust_nodes::datetime::TimestampADT(value.0),
            )),
            other => Err(ExecError::Other(format!(
                "cannot cast {other:?} to timestamp"
            ))),
        },
        SqlTypeKind::TimestampTz => match value {
            Value::TimestampTz(value) => Ok(Value::TimestampTz(value)),
            Value::Timestamp(value) => Ok(Value::TimestampTz(
                pgrust_nodes::datetime::TimestampTzADT(value.0),
            )),
            other => Err(ExecError::Other(format!(
                "cannot cast {other:?} to timestamptz"
            ))),
        },
        _ => Err(ExecError::Other(format!(
            "portable cast to {:?} is not supported",
            ty.kind
        ))),
    }
}

fn default_value_ordering(left: &Value, right: &Value) -> Result<Ordering, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(
            match (matches!(left, Value::Null), matches!(right, Value::Null)) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => Ordering::Equal,
            },
        );
    }
    if let (Some(left), Some(right)) = (integer_value(left), integer_value(right)) {
        return Ok(left.cmp(&right));
    }
    match (left, right) {
        (Value::Float64(left), Value::Float64(right)) => left
            .partial_cmp(right)
            .ok_or_else(|| ExecError::Other("cannot compare NaN values".into())),
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        (Value::Date(left), Value::Date(right)) => Ok(left.cmp(right)),
        (Value::Time(left), Value::Time(right)) => Ok(left.cmp(right)),
        (Value::Timestamp(left), Value::Timestamp(right)) => Ok(left.cmp(right)),
        (Value::TimestampTz(left), Value::TimestampTz(right)) => Ok(left.cmp(right)),
        _ => match (left.as_text(), right.as_text()) {
            (Some(left), Some(right)) => Ok(left.cmp(right)),
            _ => Err(ExecError::Other(format!(
                "portable comparison for {left:?} and {right:?} is not supported"
            ))),
        },
    }
}

fn integer_value(value: &Value) -> Option<i64> {
    match value {
        Value::Int16(value) => Some(i64::from(*value)),
        Value::Int32(value) => Some(i64::from(*value)),
        Value::Int64(value) => Some(*value),
        Value::EnumOid(value) => Some(i64::from(*value)),
        _ => None,
    }
}

fn default_hash_value_extended(value: &Value, seed: u64) -> Result<Option<u64>, String> {
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    match value {
        Value::Int16(value) => value.hash(&mut hasher),
        Value::Int32(value) => value.hash(&mut hasher),
        Value::Int64(value) => value.hash(&mut hasher),
        Value::EnumOid(value) => value.hash(&mut hasher),
        Value::Bool(value) => value.hash(&mut hasher),
        Value::Text(value) => value.hash(&mut hasher),
        Value::TextRef(_, value) => value.hash(&mut hasher),
        Value::Date(value) => value.hash(&mut hasher),
        Value::Timestamp(value) => value.hash(&mut hasher),
        Value::TimestampTz(value) => value.hash(&mut hasher),
        other => format!("{other:?}").hash(&mut hasher),
    }
    Ok(Some(hasher.finish()))
}

fn default_statistics_value_key(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Int16(v) => Some(v.to_string()),
        Value::Int32(v) => Some(v.to_string()),
        Value::Int64(v) => Some(v.to_string()),
        Value::Float64(v) => Some(v.to_string()),
        Value::Bool(v) => Some(v.to_string()),
        Value::Text(text) => Some(text.to_string()),
        Value::TextRef(_, _) => Some(value.as_text().unwrap_or_default().to_string()),
        Value::Numeric(v) => Some(v.render()),
        Value::Json(text) | Value::JsonPath(text) => Some(text.to_string()),
        other => other
            .as_text()
            .map(str::to_string)
            .or_else(|| Some(format!("{other:?}"))),
    }
}
