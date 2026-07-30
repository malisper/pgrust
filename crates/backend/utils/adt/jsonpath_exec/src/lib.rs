//! jsonpath executor (jsonpath_exec.c): executeItem machinery over the
//! flattened JsonPath and on-disk jsonb containers, item methods, filter
//! predicates, the jsonb_path_* / @? / @@ SQL surface, and the JSON_TABLE
//! plan walk (json_table). The @?/@@ GIN index strategies stay loud in
//! adt_jsonb::gin.

pub mod builtins;
pub mod json_table;
#[cfg(test)]
mod tests;

extern crate alloc;

use adt_date::{DateADT, TimeADT, TimeTzADT};
use adt_formatting::ParsedDatetime;
use adt_jsonb::build::{convert_to_jsonb, ArenaVec, JsonbValue as BuildValue};
use adt_jsonb::container::{
    container_is_array, container_is_object, container_is_scalar, container_size, get_ith_value,
    get_key_value, JsonbItem,
};
use adt_jsonb::iter::{JsonbIterator, WjbToken};
use adt_jsonpath::path::{jsp_init, jsp_init_by_buffer, ItemType, JsonPathItem, JSONPATH_LAX};
use adt_numeric::{Num, NumericImage};
use adt_datetime::consts::Timestamp;
use datum::Datum;
use mcx::{Mcx, PgVec};
use stack_depth::check_stack_depth;
use types_core::catalog::{
    BOOLOID, DATEOID, DEFAULT_COLLATION_OID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID,
    JSONBOID, JSONOID, NUMERICOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID,
    VARCHAROID,
};
use types_core::Oid;
use types_error::{
    PgError, PgResult, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_SQL_JSON_SUBSCRIPT, ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM,
    ERRCODE_NON_NUMERIC_SQL_JSON_ITEM, ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED,
    ERRCODE_SQL_JSON_ARRAY_NOT_FOUND, ERRCODE_SQL_JSON_MEMBER_NOT_FOUND,
    ERRCODE_SQL_JSON_NUMBER_NOT_FOUND, ERRCODE_SQL_JSON_OBJECT_NOT_FOUND,
    ERRCODE_SQL_JSON_SCALAR_REQUIRED, ERRCODE_UNDEFINED_OBJECT,
};

pub fn init_seams() {}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Jper {
    Ok,
    NotFound,
    Error,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum JpBool {
    False,
    True,
    Unknown,
}

/// C: JsonbValue as seen during jsonpath execution. `Numeric` is a full
/// 4B-header numeric varlena image; `Binary` is a container window (never a
/// raw-scalar container — scalars are extracted at every entry point).
#[derive(Clone, Copy, Debug)]
pub enum JbV<'a> {
    Null,
    Bool(bool),
    Numeric(&'a [u8]),
    String(&'a [u8]),
    Binary(&'a [u8]),
    Datetime {
        value: ParsedDatetime,
        typmod: i32,
        tz: i32,
    },
}

const _: () = assert!(core::mem::size_of::<JbV<'_>>() <= 40);

fn jbv_from_item<'a>(item: JsonbItem<'a>) -> JbV<'a> {
    match item {
        JsonbItem::Null => JbV::Null,
        JsonbItem::Bool(b) => JbV::Bool(b),
        JsonbItem::Numeric(n) => JbV::Numeric(n),
        JsonbItem::String(s) => JbV::String(s),
        JsonbItem::Binary(c) => JbV::Binary(c),
        JsonbItem::Array { .. } | JsonbItem::Object { .. } => {
            panic!("jbv_from_item: begin-token item is not a value")
        }
    }
}

// C: JsonbType — never returns Binary as is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum JbKind {
    Null,
    Bool,
    Numeric,
    String,
    Datetime,
    Array,
    Object,
}

fn jsonb_kind(v: &JbV<'_>) -> JbKind {
    match v {
        JbV::Null => JbKind::Null,
        JbV::Bool(_) => JbKind::Bool,
        JbV::Numeric(_) => JbKind::Numeric,
        JbV::String(_) => JbKind::String,
        JbV::Datetime { .. } => JbKind::Datetime,
        JbV::Binary(c) => {
            debug_assert!(!container_is_scalar(c));
            if container_is_object(c) {
                JbKind::Object
            } else if container_is_array(c) {
                JbKind::Array
            } else {
                panic!("invalid jsonb container type");
            }
        }
    }
}

/// C: JsonValueList (singleton shortcut folded into the growable vec; a bump
/// vec's first grow is the cost of C's list_make1).
pub struct JsonValueList<'a, 'mcx> {
    items: PgVec<'mcx, JbV<'a>>,
}

impl<'a, 'mcx> JsonValueList<'a, 'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> PgResult<JsonValueList<'a, 'mcx>> {
        Ok(JsonValueList {
            items: mcx::vec_with_capacity_in(mcx, 0)?,
        })
    }

    fn append(&mut self, v: JbV<'a>) {
        self.items.push(v);
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn head(&self) -> Option<&JbV<'a>> {
        self.items.first()
    }

    pub fn as_slice(&self) -> &[JbV<'a>] {
        &self.items
    }
}

/// C: JsonPathVariable (the SQL/JSON PASSING clause hook for GetJsonPathVar).
pub struct JsonPathVariable<'a> {
    pub name: &'a [u8],
    pub typid: Oid,
    pub typmod: i32,
    pub value: Datum,
    pub isnull: bool,
}

pub enum JsonPathVars<'a, 'x> {
    None,
    /// The `vars` jsonb argument's container payload.
    Jsonb(&'a [u8]),
    /// SQL/JSON PASSING variables (JSON_EXISTS/QUERY/VALUE executor path).
    List(&'x [JsonPathVariable<'a>]),
}

#[derive(Clone, Copy)]
struct BaseObject<'a> {
    jbc: Option<&'a [u8]>,
    id: i32,
}

struct ExecCtx<'a, 'x, 'mcx> {
    mcx: Mcx<'mcx>,
    vars: &'x JsonPathVars<'a, 'x>,
    root: JbV<'a>,
    current: JbV<'a>,
    base_object: BaseObject<'a>,
    last_generated_object_id: i32,
    innermost_array_size: i32,
    lax_mode: bool,
    ignore_structural_errors: bool,
    throw_errors: bool,
    use_tz: bool,
}

type ExecRes = PgResult<Jper>;
type Found<'f, 'a, 'mcx> = Option<&'f mut JsonValueList<'a, 'mcx>>;

macro_rules! return_error {
    ($cxt:expr, $err:expr) => {
        if $cxt.throw_errors {
            return Err(Box::new($err))
        } else {
            return Ok(Jper::Error)
        }
    };
}

#[cold]
#[inline(never)]
fn method_applies_error(code: types_error::SqlState, op: &str, what: &str) -> PgError {
    PgError::error(format!(
        "jsonpath item method .{op}() can only be applied to {what}"
    ))
    .with_sqlstate(code)
}

#[cold]
#[inline(never)]
fn invalid_arg_error(arg: &str, op: &str, typ: &str) -> PgError {
    PgError::error(format!(
        "argument \"{arg}\" of jsonpath item method .{op}() is invalid for type {typ}"
    ))
    .with_sqlstate(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
}

#[cold]
#[inline(never)]
fn nan_or_inf_error(op: &str) -> PgError {
    PgError::error(format!(
        "NaN or Infinity is not allowed for jsonpath item method .{op}()"
    ))
    .with_sqlstate(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
}

fn operation_name(typ: ItemType) -> &'static str {
    match typ {
        ItemType::And => "&&",
        ItemType::Or => "||",
        ItemType::Equal => "==",
        ItemType::NotEqual => "!=",
        ItemType::Less => "<",
        ItemType::Greater => ">",
        ItemType::LessOrEqual => "<=",
        ItemType::GreaterOrEqual => ">=",
        ItemType::Add | ItemType::Plus => "+",
        ItemType::Sub | ItemType::Minus => "-",
        ItemType::Mul => "*",
        ItemType::Div => "/",
        ItemType::Mod => "%",
        ItemType::Type => "type",
        ItemType::Size => "size",
        ItemType::Abs => "abs",
        ItemType::Floor => "floor",
        ItemType::Ceiling => "ceiling",
        ItemType::Double => "double",
        ItemType::Datetime => "datetime",
        ItemType::KeyValue => "keyvalue",
        ItemType::StartsWith => "starts with",
        ItemType::LikeRegex => "like_regex",
        ItemType::Bigint => "bigint",
        ItemType::Boolean => "boolean",
        ItemType::Date => "date",
        ItemType::Decimal => "decimal",
        ItemType::Integer => "integer",
        ItemType::Number => "number",
        ItemType::StringFunc => "string",
        ItemType::Time => "time",
        ItemType::TimeTz => "time_tz",
        ItemType::Timestamp => "timestamp",
        ItemType::TimestampTz => "timestamp_tz",
        _ => panic!("unrecognized jsonpath item type: {}", typ as i32),
    }
}

fn num_of(image: &[u8]) -> Num<'_> {
    Num::from_payload(&image[4..])
}

fn leak_numeric<'mcx>(mcx: Mcx<'mcx>, img: &NumericImage) -> PgResult<&'mcx [u8]> {
    Ok(mcx::slice_in(mcx, img.as_bytes())?.leak())
}

fn numeric_out_str<'mcx>(mcx: Mcx<'mcx>, num: Num<'_>) -> PgResult<&'mcx [u8]> {
    let mut scratch: Vec<u8> = Vec::new();
    adt_numeric::numeric_out_into(num, &mut scratch);
    Ok(mcx::slice_in(mcx, &scratch)?.leak())
}

// Strings inside jsonb documents and jsonpath literals are validated
// server-encoding text.
fn as_str(bytes: &[u8]) -> &str {
    // SAFETY: see above.
    unsafe { core::str::from_utf8_unchecked(bytes) }
}

/// C: JsonbArraySize — -1 when not an array.
fn jsonb_array_size(jb: &JbV<'_>) -> i32 {
    if let JbV::Binary(c) = jb {
        if container_is_array(c) && !container_is_scalar(c) {
            return container_size(c) as i32;
        }
    }
    -1
}

/// C: getScalar.
fn get_scalar<'v, 'a>(v: &'v JbV<'a>, kind: JbKind) -> Option<&'v JbV<'a>> {
    match jsonb_kind(v) {
        k if k == kind && !matches!(v, JbV::Binary(_)) => Some(v),
        _ => None,
    }
}

impl<'a, 'x, 'mcx: 'a> ExecCtx<'a, 'x, 'mcx> {
    fn strict_absence_of_errors(&self) -> bool {
        !self.lax_mode
    }
    fn auto_unwrap(&self) -> bool {
        self.lax_mode
    }
    fn auto_wrap(&self) -> bool {
        self.lax_mode
    }

    /// C: setBaseObject.
    fn set_base_object(&mut self, jbv: &JbV<'a>, id: i32) -> BaseObject<'a> {
        let prev = self.base_object;
        self.base_object = BaseObject {
            jbc: match jbv {
                JbV::Binary(c) => Some(c),
                _ => None,
            },
            id,
        };
        prev
    }

    fn count_vars(&self) -> PgResult<i32> {
        match self.vars {
            JsonPathVars::None => Ok(0),
            JsonPathVars::Jsonb(c) => {
                if !container_is_object(c) {
                    return Err(Box::new(
                        PgError::error("\"vars\" argument is not an object")
                            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                            .with_detail(
                                "Jsonpath parameters should be encoded as key-value pairs of \"vars\" object.",
                            ),
                    ));
                }
                Ok(1)
            }
            JsonPathVars::List(vars) => Ok(vars.len() as i32),
        }
    }

    /// C: getJsonPathVariable + the getVar callbacks.
    fn get_json_path_variable(&mut self, item: &JsonPathItem<'a>) -> PgResult<JbV<'a>> {
        let name = item.get_string();
        let found: Option<(JbV<'a>, JbV<'a>, i32)> = match self.vars {
            JsonPathVars::None => None,
            JsonPathVars::Jsonb(c) => get_key_value(c, name).map(|item| {
                (jbv_from_item(item), JbV::Binary(c), 1)
            }),
            JsonPathVars::List(vars) => {
                let mut hit = None;
                for (i, var) in vars.iter().enumerate() {
                    if var.name == name {
                        hit = Some((i as i32 + 1, var));
                        break;
                    }
                }
                match hit {
                    None => None,
                    Some((id, var)) => {
                        let v = if var.isnull {
                            JbV::Null
                        } else {
                            json_item_from_datum(self.mcx, var.value, var.typid, var.typmod)?
                        };
                        Some((v, v, if var.isnull { 0 } else { id }))
                    }
                }
            }
        };
        match found {
            None => Err(Box::new(
                PgError::error(format!(
                    "could not find jsonpath variable \"{}\"",
                    String::from_utf8_lossy(name)
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            )),
            Some((v, base, id)) => {
                if id > 0 {
                    self.set_base_object(&base, id);
                }
                Ok(v)
            }
        }
    }

    /// C: getJsonPathItem.
    fn get_json_path_item(&mut self, item: &JsonPathItem<'a>) -> PgResult<JbV<'a>> {
        match item.typ {
            ItemType::Null => Ok(JbV::Null),
            ItemType::Bool => Ok(JbV::Bool(item.get_bool())),
            ItemType::Numeric => Ok(JbV::Numeric(item.get_numeric())),
            ItemType::String => Ok(JbV::String(item.get_string())),
            ItemType::Variable => self.get_json_path_variable(item),
            _ => panic!("unexpected jsonpath item type"),
        }
    }

    fn execute_item(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        found: Found<'_, 'a, 'mcx>,
    ) -> ExecRes {
        self.execute_item_opt_unwrap_target(jsp, jb, found, self.auto_unwrap())
    }

    /// C: executeItemOptUnwrapTarget.
    fn execute_item_opt_unwrap_target(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        mut found: Found<'_, 'a, 'mcx>,
        unwrap: bool,
    ) -> ExecRes {
        check_stack_depth()?;
        postgres_seams::check_for_interrupts::call()?;

        let mut res = Jper::NotFound;

        match jsp.typ {
            ItemType::Null
            | ItemType::Bool
            | ItemType::Numeric
            | ItemType::String
            | ItemType::Variable => {
                let next = jsp.next();
                if next.is_none() && found.is_none() && jsp.typ != ItemType::Variable {
                    // Skip evaluation, but not for variables: a missing
                    // variable must still error.
                    return Ok(Jper::Ok);
                }
                let base_object = self.base_object;
                let v = self.get_json_path_item(jsp)?;
                res = self.execute_next_item(Some(jsp), next.as_ref(), &v, found)?;
                self.base_object = base_object;
            }

            ItemType::And
            | ItemType::Or
            | ItemType::Not
            | ItemType::IsUnknown
            | ItemType::Equal
            | ItemType::NotEqual
            | ItemType::Less
            | ItemType::Greater
            | ItemType::LessOrEqual
            | ItemType::GreaterOrEqual
            | ItemType::Exists
            | ItemType::StartsWith
            | ItemType::LikeRegex => {
                let st = self.execute_bool_item(jsp, jb, true)?;
                res = self.append_bool_result(jsp, found, st)?;
            }

            ItemType::Add => {
                return self.execute_binary_arithm_expr(jsp, jb, adt_numeric::numeric_add_common, found)
            }
            ItemType::Sub => {
                return self.execute_binary_arithm_expr(jsp, jb, adt_numeric::numeric_sub_common, found)
            }
            ItemType::Mul => {
                return self.execute_binary_arithm_expr(jsp, jb, adt_numeric::numeric_mul_common, found)
            }
            ItemType::Div => {
                return self.execute_binary_arithm_expr(jsp, jb, adt_numeric::numeric_div_common, found)
            }
            ItemType::Mod => {
                return self.execute_binary_arithm_expr(jsp, jb, adt_numeric::numeric_mod_common, found)
            }
            ItemType::Plus => return self.execute_unary_arithm_expr(jsp, jb, None, found),
            ItemType::Minus => {
                return self.execute_unary_arithm_expr(jsp, jb, Some(adt_numeric::numeric_uminus), found)
            }

            ItemType::AnyArray => {
                if jsonb_kind(jb) == JbKind::Array {
                    let next = jsp.next();
                    res = self.execute_item_unwrap_target_array(
                        next.as_ref(),
                        jb,
                        found,
                        self.auto_unwrap(),
                    )?;
                } else if self.auto_wrap() {
                    res = self.execute_next_item(Some(jsp), None, jb, found)?;
                } else if !self.ignore_structural_errors {
                    return_error!(
                        self,
                        PgError::error(
                            "jsonpath wildcard array accessor can only be applied to an array",
                        )
                        .with_sqlstate(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND)
                    );
                }
            }

            ItemType::AnyKey => {
                if jsonb_kind(jb) == JbKind::Object {
                    let next = jsp.next();
                    let JbV::Binary(c) = jb else {
                        panic!("invalid jsonb object type");
                    };
                    return self.execute_any_item(
                        next.as_ref(),
                        c,
                        found,
                        1,
                        1,
                        1,
                        false,
                        self.auto_unwrap(),
                    );
                } else if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                } else if !self.ignore_structural_errors {
                    debug_assert!(found.is_some());
                    return_error!(
                        self,
                        PgError::error(
                            "jsonpath wildcard member accessor can only be applied to an object",
                        )
                        .with_sqlstate(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND)
                    );
                }
            }

            ItemType::IndexArray => {
                if jsonb_kind(jb) == JbKind::Array || self.auto_wrap() {
                    let saved_innermost = self.innermost_array_size;
                    let mut size = jsonb_array_size(jb);
                    let singleton = size < 0;
                    let next = jsp.next();
                    if singleton {
                        size = 1;
                    }
                    self.innermost_array_size = size;

                    'subscripts: for i in 0..jsp.content.array.nelems {
                        let (from, to) = jsp.array_subscript(i);
                        let mut index_from = 0;
                        let mut index_to = 0;
                        res = self.get_array_index(&from, jb, &mut index_from)?;
                        if res == Jper::Error {
                            break;
                        }
                        if let Some(to) = to {
                            res = self.get_array_index(&to, jb, &mut index_to)?;
                            if res == Jper::Error {
                                break;
                            }
                        } else {
                            index_to = index_from;
                        }

                        if !self.ignore_structural_errors
                            && (index_from < 0 || index_from > index_to || index_to >= size)
                        {
                            return_error!(
                                self,
                                PgError::error("jsonpath array subscript is out of bounds")
                                    .with_sqlstate(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT)
                            );
                        }

                        let index_from = index_from.max(0);
                        let index_to = index_to.min(size - 1);

                        res = Jper::NotFound;

                        for index in index_from..=index_to {
                            let v: JbV<'a>;
                            if singleton {
                                v = *jb;
                            } else {
                                let JbV::Binary(c) = jb else {
                                    panic!("invalid jsonb array value type");
                                };
                                match get_ith_value(c, index as u32) {
                                    Some(item) => v = jbv_from_item(item),
                                    None => continue,
                                }
                            }

                            if next.is_none() && found.is_none() {
                                return Ok(Jper::Ok);
                            }

                            res = self.execute_next_item(
                                Some(jsp),
                                next.as_ref(),
                                &v,
                                found.as_deref_mut(),
                            )?;

                            if res == Jper::Error {
                                break 'subscripts;
                            }
                            if res == Jper::Ok && found.is_none() {
                                break 'subscripts;
                            }
                        }
                    }

                    self.innermost_array_size = saved_innermost;
                } else if !self.ignore_structural_errors {
                    return_error!(
                        self,
                        PgError::error("jsonpath array accessor can only be applied to an array")
                            .with_sqlstate(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND)
                    );
                }
            }

            ItemType::Any => {
                let next = jsp.next();
                // First try without any intermediate steps.
                if jsp.content.anybounds.first == 0 {
                    let saved = self.ignore_structural_errors;
                    self.ignore_structural_errors = true;
                    res = self.execute_next_item(
                        Some(jsp),
                        next.as_ref(),
                        jb,
                        found.as_deref_mut(),
                    )?;
                    self.ignore_structural_errors = saved;
                    if res == Jper::Ok && found.is_none() {
                        return Ok(res);
                    }
                }
                if let JbV::Binary(c) = jb {
                    res = self.execute_any_item(
                        next.as_ref(),
                        c,
                        found,
                        1,
                        jsp.content.anybounds.first,
                        jsp.content.anybounds.last,
                        true,
                        self.auto_unwrap(),
                    )?;
                }
            }

            ItemType::Key => {
                if jsonb_kind(jb) == JbKind::Object {
                    let key = jsp.get_string();
                    let JbV::Binary(c) = jb else {
                        panic!("invalid jsonb object type");
                    };
                    match get_key_value(c, key) {
                        Some(item) => {
                            let v = jbv_from_item(item);
                            res = self.execute_next_item(Some(jsp), None, &v, found)?;
                        }
                        None if !self.ignore_structural_errors => {
                            debug_assert!(found.is_some());
                            if !self.throw_errors {
                                return Ok(Jper::Error);
                            }
                            return Err(Box::new(
                                PgError::error(format!(
                                    "JSON object does not contain key \"{}\"",
                                    String::from_utf8_lossy(key)
                                ))
                                .with_sqlstate(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND),
                            ));
                        }
                        None => {}
                    }
                } else if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                } else if !self.ignore_structural_errors {
                    debug_assert!(found.is_some());
                    return_error!(
                        self,
                        PgError::error(
                            "jsonpath member accessor can only be applied to an object",
                        )
                        .with_sqlstate(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND)
                    );
                }
            }

            ItemType::Current => {
                let current = self.current;
                res = self.execute_next_item(Some(jsp), None, &current, found)?;
            }

            ItemType::Root => {
                let root = self.root;
                let base_object = self.set_base_object(&root, 0);
                res = self.execute_next_item(Some(jsp), None, &root, found)?;
                self.base_object = base_object;
            }

            ItemType::Filter => {
                if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                }
                let arg = jsp.arg();
                let st = self.execute_nested_bool_item(&arg, jb)?;
                if st != JpBool::True {
                    res = Jper::NotFound;
                } else {
                    res = self.execute_next_item(Some(jsp), None, jb, found)?;
                }
            }

            ItemType::Type => {
                let name: &'static str = match jsonb_kind(jb) {
                    JbKind::Object => "object",
                    JbKind::Array => "array",
                    JbKind::String => "string",
                    JbKind::Numeric => "number",
                    JbKind::Bool => "boolean",
                    JbKind::Null => "null",
                    JbKind::Datetime => match jb {
                        JbV::Datetime { value, .. } => match value {
                            ParsedDatetime::Date(_) => "date",
                            ParsedDatetime::Time(_) => "time without time zone",
                            ParsedDatetime::TimeTz(_) => "time with time zone",
                            ParsedDatetime::Timestamp(_) => "timestamp without time zone",
                            ParsedDatetime::TimestampTz(_) => "timestamp with time zone",
                        },
                        _ => unreachable!(),
                    },
                };
                let v = JbV::String(name.as_bytes());
                res = self.execute_next_item(Some(jsp), None, &v, found)?;
            }

            ItemType::Size => {
                let mut size = jsonb_array_size(jb);
                if size < 0 {
                    if !self.auto_wrap() {
                        if !self.ignore_structural_errors {
                            return_error!(
                                self,
                                method_applies_error(
                                    ERRCODE_SQL_JSON_ARRAY_NOT_FOUND,
                                    operation_name(jsp.typ),
                                    "an array",
                                )
                            );
                        }
                        return Ok(res);
                    }
                    size = 1;
                }
                let img = adt_numeric::int64_to_numeric(size as i64);
                let v = JbV::Numeric(leak_numeric(self.mcx, &img)?);
                res = self.execute_next_item(Some(jsp), None, &v, found)?;
            }

            ItemType::Abs => {
                return self.execute_numeric_item_method(jsp, jb, unwrap, NumericMethod::Abs, found)
            }
            ItemType::Floor => {
                return self.execute_numeric_item_method(jsp, jb, unwrap, NumericMethod::Floor, found)
            }
            ItemType::Ceiling => {
                return self.execute_numeric_item_method(jsp, jb, unwrap, NumericMethod::Ceiling, found)
            }

            ItemType::Double => {
                if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                }
                let op = operation_name(jsp.typ);
                let v: JbV<'a>;
                match jb {
                    JbV::Numeric(image) => {
                        let tmp = numeric_out_str(self.mcx, num_of(image))?;
                        let mut esc = SoftErrorContext::new(false);
                        let val = adt_float::float8in_internal(
                            as_str(tmp),
                            None,
                            "double precision",
                            as_str(tmp),
                            Some(&mut esc),
                        )?;
                        if esc.error_occurred() {
                            return_error!(self, invalid_arg_error(as_str(tmp), op, "double precision"));
                        }
                        if val.is_infinite() || val.is_nan() {
                            return_error!(self, nan_or_inf_error(op));
                        }
                        v = *jb;
                    }
                    JbV::String(s) => {
                        let tmp: &[u8] = mcx::slice_in(self.mcx, s)?.leak();
                        let mut esc = SoftErrorContext::new(false);
                        let val = adt_float::float8in_internal(
                            as_str(tmp),
                            None,
                            "double precision",
                            as_str(tmp),
                            Some(&mut esc),
                        )?;
                        if esc.error_occurred() {
                            return_error!(self, invalid_arg_error(as_str(tmp), op, "double precision"));
                        }
                        if val.is_infinite() || val.is_nan() {
                            return_error!(self, nan_or_inf_error(op));
                        }
                        let img = adt_numeric::float8_numeric(val)?;
                        v = JbV::Numeric(leak_numeric(self.mcx, &img)?);
                    }
                    _ => {
                        return_error!(
                            self,
                            method_applies_error(
                                ERRCODE_NON_NUMERIC_SQL_JSON_ITEM,
                                op,
                                "a string or numeric value",
                            )
                        );
                    }
                }
                res = self.execute_next_item(Some(jsp), None, &v, found)?;
            }

            ItemType::Datetime
            | ItemType::Date
            | ItemType::Time
            | ItemType::TimeTz
            | ItemType::Timestamp
            | ItemType::TimestampTz => {
                if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                }
                return self.execute_datetime_method(jsp, jb, found);
            }

            ItemType::KeyValue => {
                if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                }
                return self.execute_keyvalue_method(jsp, jb, found);
            }

            ItemType::Last => {
                if self.innermost_array_size < 0 {
                    panic!("evaluating jsonpath LAST outside of array subscript");
                }
                let next = jsp.next();
                if next.is_none() && found.is_none() {
                    return Ok(Jper::Ok);
                }
                let last = self.innermost_array_size - 1;
                let img = adt_numeric::int64_to_numeric(last as i64);
                let v = JbV::Numeric(leak_numeric(self.mcx, &img)?);
                res = self.execute_next_item(Some(jsp), next.as_ref(), &v, found)?;
            }

            ItemType::Bigint => {
                if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                }
                let op = operation_name(jsp.typ);
                let val: i64;
                match jb {
                    JbV::Numeric(image) => match adt_numeric::numeric_int8(num_of(image)) {
                        Ok(v) => val = v,
                        Err(_) => {
                            let s = numeric_out_str(self.mcx, num_of(image))?;
                            return_error!(self, invalid_arg_error(as_str(s), op, "bigint"));
                        }
                    },
                    JbV::String(s) => {
                        let mut esc = SoftErrorContext::new(false);
                        match adt_int8::int8in(as_str(s), Some(&mut esc)) {
                            Ok(v) if !esc.error_occurred() => val = v,
                            _ => {
                                return_error!(self, invalid_arg_error(as_str(s), op, "bigint"));
                            }
                        }
                    }
                    _ => {
                        return_error!(
                            self,
                            method_applies_error(
                                ERRCODE_NON_NUMERIC_SQL_JSON_ITEM,
                                op,
                                "a string or numeric value",
                            )
                        );
                    }
                }
                let img = adt_numeric::int8_numeric(val);
                let v = JbV::Numeric(leak_numeric(self.mcx, &img)?);
                res = self.execute_next_item(Some(jsp), None, &v, found)?;
            }

            ItemType::Boolean => {
                if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                }
                let op = operation_name(jsp.typ);
                let bval: bool;
                match jb {
                    JbV::Bool(b) => bval = *b,
                    JbV::Numeric(image) => {
                        let tmp = numeric_out_str(self.mcx, num_of(image))?;
                        let mut esc = SoftErrorContext::new(false);
                        match adt_int::int4in(as_str(tmp), Some(&mut esc)) {
                            Ok(ival) if !esc.error_occurred() => bval = ival != 0,
                            _ => {
                                return_error!(self, invalid_arg_error(as_str(tmp), op, "boolean"));
                            }
                        }
                    }
                    JbV::String(s) => match adt_bool::parse_bool_with_len(s) {
                        Some(b) => bval = b,
                        None => {
                            return_error!(self, invalid_arg_error(as_str(s), op, "boolean"));
                        }
                    },
                    _ => {
                        return_error!(
                            self,
                            method_applies_error(
                                ERRCODE_NON_NUMERIC_SQL_JSON_ITEM,
                                op,
                                "a boolean, string, or numeric value",
                            )
                        );
                    }
                }
                let v = JbV::Bool(bval);
                res = self.execute_next_item(Some(jsp), None, &v, found)?;
            }

            ItemType::Decimal | ItemType::Number => {
                if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                }
                let op = operation_name(jsp.typ);
                let mut num_image: &'a [u8];
                let mut numstr: Option<&'a [u8]> = None;
                match jb {
                    JbV::Numeric(image) => {
                        if num_of(image).is_nan() || num_of(image).is_inf() {
                            return_error!(self, nan_or_inf_error(op));
                        }
                        if jsp.typ == ItemType::Decimal {
                            numstr = Some(numeric_out_str(self.mcx, num_of(image))?);
                        }
                        num_image = image;
                    }
                    JbV::String(s) => {
                        let tmp: &[u8] = mcx::slice_in(self.mcx, s)?.leak();
                        numstr = Some(tmp);
                        let mut esc = SoftErrorContext::new(false);
                        match adt_numeric::numeric_in(as_str(tmp), -1, Some(&mut esc))? {
                            Some(img) if !esc.error_occurred() => {
                                if img.num().is_nan() || img.num().is_inf() {
                                    return_error!(self, nan_or_inf_error(op));
                                }
                                num_image = leak_numeric(self.mcx, &img)?;
                            }
                            _ => {
                                return_error!(self, invalid_arg_error(as_str(tmp), op, "numeric"));
                            }
                        }
                    }
                    _ => {
                        return_error!(
                            self,
                            method_applies_error(
                                ERRCODE_NON_NUMERIC_SQL_JSON_ITEM,
                                op,
                                "a string or numeric value",
                            )
                        );
                    }
                }

                if jsp.typ == ItemType::Decimal && jsp.content.args.left != 0 {
                    let elem = jsp.left_arg();
                    if elem.typ != ItemType::Numeric {
                        panic!("invalid jsonpath item type for .decimal() precision");
                    }
                    let precision = match adt_numeric::numeric_int4(num_of(elem.get_numeric())) {
                        Ok(v) => v,
                        Err(_) => {
                            return_error!(
                                self,
                                PgError::error(format!(
                                    "precision of jsonpath item method .{op}() is out of range for type integer"
                                ))
                                .with_sqlstate(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                            );
                        }
                    };
                    let mut scale = 0;
                    if jsp.content.args.right != 0 {
                        let elem = jsp.right_arg();
                        if elem.typ != ItemType::Numeric {
                            panic!("invalid jsonpath item type for .decimal() scale");
                        }
                        scale = match adt_numeric::numeric_int4(num_of(elem.get_numeric())) {
                            Ok(v) => v,
                            Err(_) => {
                                return_error!(
                                    self,
                                    PgError::error(format!(
                                        "scale of jsonpath item method .{op}() is out of range for type integer"
                                    ))
                                    .with_sqlstate(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                                );
                            }
                        };
                    }

                    // C round-trips through numerictypmodin; its range errors
                    // are thrown, not suppressed.
                    let dtypmod = adt_numeric::numerictypmodin_core(&[precision, scale])?;

                    let numstr = numstr.expect("numstr set for .decimal()");
                    let mut esc = SoftErrorContext::new(false);
                    match adt_numeric::numeric_in(as_str(numstr), dtypmod, Some(&mut esc))? {
                        Some(img) if !esc.error_occurred() => {
                            num_image = leak_numeric(self.mcx, &img)?;
                        }
                        _ => {
                            return_error!(self, invalid_arg_error(as_str(numstr), op, "numeric"));
                        }
                    }
                }

                let v = JbV::Numeric(num_image);
                res = self.execute_next_item(Some(jsp), None, &v, found)?;
            }

            ItemType::Integer => {
                if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                }
                let op = operation_name(jsp.typ);
                let val: i32;
                match jb {
                    JbV::Numeric(image) => match adt_numeric::numeric_int4(num_of(image)) {
                        Ok(v) => val = v,
                        Err(_) => {
                            let s = numeric_out_str(self.mcx, num_of(image))?;
                            return_error!(self, invalid_arg_error(as_str(s), op, "integer"));
                        }
                    },
                    JbV::String(s) => {
                        let mut esc = SoftErrorContext::new(false);
                        match adt_int::int4in(as_str(s), Some(&mut esc)) {
                            Ok(v) if !esc.error_occurred() => val = v,
                            _ => {
                                return_error!(self, invalid_arg_error(as_str(s), op, "integer"));
                            }
                        }
                    }
                    _ => {
                        return_error!(
                            self,
                            method_applies_error(
                                ERRCODE_NON_NUMERIC_SQL_JSON_ITEM,
                                op,
                                "a string or numeric value",
                            )
                        );
                    }
                }
                let img = adt_numeric::int4_numeric(val);
                let v = JbV::Numeric(leak_numeric(self.mcx, &img)?);
                res = self.execute_next_item(Some(jsp), None, &v, found)?;
            }

            ItemType::StringFunc => {
                if unwrap && jsonb_kind(jb) == JbKind::Array {
                    return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
                }
                let tmp: &'a [u8] = match jb {
                    JbV::String(s) => s,
                    JbV::Numeric(image) => numeric_out_str(self.mcx, num_of(image))?,
                    JbV::Bool(b) => {
                        if *b {
                            b"true"
                        } else {
                            b"false"
                        }
                    }
                    JbV::Datetime { value, tz, .. } => encode_datetime(self.mcx, value, *tz)?,
                    JbV::Null | JbV::Binary(_) => {
                        return_error!(
                            self,
                            method_applies_error(
                                ERRCODE_NON_NUMERIC_SQL_JSON_ITEM,
                                operation_name(jsp.typ),
                                "a boolean, string, numeric, or datetime value",
                            )
                        );
                    }
                };
                let v = JbV::String(tmp);
                res = self.execute_next_item(Some(jsp), None, &v, found)?;
            }

            ItemType::Subscript => {
                panic!("unrecognized jsonpath item type: {}", jsp.typ as i32)
            }
        }

        Ok(res)
    }

    /// C: executeItemUnwrapTargetArray.
    fn execute_item_unwrap_target_array(
        &mut self,
        jsp: Option<&JsonPathItem<'a>>,
        jb: &JbV<'a>,
        found: Found<'_, 'a, 'mcx>,
        unwrap_elements: bool,
    ) -> ExecRes {
        let JbV::Binary(c) = jb else {
            panic!("invalid jsonb array value type");
        };
        self.execute_any_item(jsp, c, found, 1, 1, 1, false, unwrap_elements)
    }

    /// C: executeNextItem.
    fn execute_next_item(
        &mut self,
        cur: Option<&JsonPathItem<'a>>,
        next: Option<&JsonPathItem<'a>>,
        v: &JbV<'a>,
        found: Found<'_, 'a, 'mcx>,
    ) -> ExecRes {
        let owned_next;
        let (has_next, next) = match cur {
            None => (next.is_some(), next),
            Some(cur) => match next {
                Some(_) => (cur.has_next(), next),
                None => {
                    owned_next = cur.next();
                    (owned_next.is_some(), owned_next.as_ref())
                }
            },
        };

        if has_next {
            return self.execute_item(next.expect("has_next"), v, found);
        }

        if let Some(found) = found {
            found.append(*v);
        }

        Ok(Jper::Ok)
    }

    /// C: executeItemOptUnwrapResult.
    fn execute_item_opt_unwrap_result(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        unwrap: bool,
        found: &mut JsonValueList<'a, 'mcx>,
    ) -> ExecRes {
        if unwrap && self.auto_unwrap() {
            let mut seq = JsonValueList::new(self.mcx)?;
            let res = self.execute_item(jsp, jb, Some(&mut seq))?;
            if res == Jper::Error {
                return Ok(res);
            }
            for item in seq.as_slice() {
                if jsonb_kind(item) == JbKind::Array {
                    self.execute_item_unwrap_target_array(None, item, Some(found), false)?;
                } else {
                    found.append(*item);
                }
            }
            return Ok(Jper::Ok);
        }

        self.execute_item(jsp, jb, Some(found))
    }

    /// C: executeItemOptUnwrapResultNoThrow.
    fn execute_item_opt_unwrap_result_no_throw(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        unwrap: bool,
        found: Option<&mut JsonValueList<'a, 'mcx>>,
    ) -> ExecRes {
        let saved = self.throw_errors;
        self.throw_errors = false;
        let res = match found {
            Some(found) => self.execute_item_opt_unwrap_result(jsp, jb, unwrap, found),
            None => self.execute_item(jsp, jb, None),
        };
        self.throw_errors = saved;
        res
    }

    /// C: executeBoolItem.
    fn execute_bool_item(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        can_have_next: bool,
    ) -> PgResult<JpBool> {
        check_stack_depth()?;

        if !can_have_next && jsp.has_next() {
            panic!("boolean jsonpath item cannot have next item");
        }

        match jsp.typ {
            ItemType::And => {
                let res = self.execute_bool_item(&jsp.left_arg(), jb, false)?;
                if res == JpBool::False {
                    return Ok(JpBool::False);
                }
                // SQL/JSON requires the second arg to be checked on error.
                let res2 = self.execute_bool_item(&jsp.right_arg(), jb, false)?;
                Ok(if res2 == JpBool::True { res } else { res2 })
            }
            ItemType::Or => {
                let res = self.execute_bool_item(&jsp.left_arg(), jb, false)?;
                if res == JpBool::True {
                    return Ok(JpBool::True);
                }
                let res2 = self.execute_bool_item(&jsp.right_arg(), jb, false)?;
                Ok(if res2 == JpBool::False { res } else { res2 })
            }
            ItemType::Not => {
                let res = self.execute_bool_item(&jsp.arg(), jb, false)?;
                Ok(match res {
                    JpBool::Unknown => JpBool::Unknown,
                    JpBool::True => JpBool::False,
                    JpBool::False => JpBool::True,
                })
            }
            ItemType::IsUnknown => {
                let res = self.execute_bool_item(&jsp.arg(), jb, false)?;
                Ok(if res == JpBool::Unknown {
                    JpBool::True
                } else {
                    JpBool::False
                })
            }
            ItemType::Equal
            | ItemType::NotEqual
            | ItemType::Less
            | ItemType::Greater
            | ItemType::LessOrEqual
            | ItemType::GreaterOrEqual => {
                let larg = jsp.left_arg();
                let rarg = jsp.right_arg();
                self.execute_predicate(
                    PredOp::Compare(jsp.typ),
                    &larg,
                    Some(&rarg),
                    jb,
                    true,
                )
            }
            ItemType::StartsWith => {
                let larg = jsp.left_arg();
                let rarg = jsp.right_arg();
                self.execute_predicate(PredOp::StartsWith, &larg, Some(&rarg), jb, false)
            }
            ItemType::LikeRegex => {
                let larg = jsp_init_by_buffer(jsp.buffer, jsp.base + jsp.content.like_regex.expr);
                let pos = jsp.content.like_regex.pattern_pos as usize;
                let len = jsp.content.like_regex.patternlen as usize;
                let pattern = &jsp.buffer[pos..pos + len];
                let cflags =
                    adt_jsonpath::gram::jsp_convert_regex_flags(jsp.content.like_regex.flags, None)?
                        .expect("regex flags validated at parse time");
                self.execute_predicate(
                    PredOp::LikeRegex { pattern, cflags },
                    &larg,
                    None,
                    jb,
                    false,
                )
            }
            ItemType::Exists => {
                let larg = jsp.arg();
                if self.strict_absence_of_errors() {
                    // Strict mode: a complete list is needed to check that
                    // there are no errors at all.
                    let mut vals = JsonValueList::new(self.mcx)?;
                    let res = self.execute_item_opt_unwrap_result_no_throw(
                        &larg,
                        jb,
                        false,
                        Some(&mut vals),
                    )?;
                    if res == Jper::Error {
                        return Ok(JpBool::Unknown);
                    }
                    Ok(if vals.is_empty() {
                        JpBool::False
                    } else {
                        JpBool::True
                    })
                } else {
                    let res = self.execute_item_opt_unwrap_result_no_throw(&larg, jb, false, None)?;
                    if res == Jper::Error {
                        return Ok(JpBool::Unknown);
                    }
                    Ok(if res == Jper::Ok {
                        JpBool::True
                    } else {
                        JpBool::False
                    })
                }
            }
            _ => panic!("invalid boolean jsonpath item type: {}", jsp.typ as i32),
        }
    }

    /// C: executeNestedBoolItem.
    fn execute_nested_bool_item(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
    ) -> PgResult<JpBool> {
        let prev = self.current;
        self.current = *jb;
        let res = self.execute_bool_item(jsp, jb, false);
        self.current = prev;
        res
    }

    /// C: executeAnyItem (.**, .*, [*]).
    #[allow(clippy::too_many_arguments)]
    fn execute_any_item(
        &mut self,
        jsp: Option<&JsonPathItem<'a>>,
        jbc: &'a [u8],
        mut found: Found<'_, 'a, 'mcx>,
        level: u32,
        first: u32,
        last: u32,
        ignore_structural_errors: bool,
        unwrap_next: bool,
    ) -> ExecRes {
        check_stack_depth()?;

        let mut res = Jper::NotFound;
        if level > last {
            return Ok(res);
        }

        let mut it = JsonbIterator::init(self.mcx, jbc)?;
        loop {
            let (mut tok, mut v) = it.next(true);
            if tok == WjbToken::Done {
                break;
            }
            if tok == WjbToken::Key {
                let (vtok, vval) = it.next(true);
                debug_assert_eq!(vtok, WjbToken::Value);
                tok = vtok;
                v = vval;
            }

            if tok == WjbToken::Value || tok == WjbToken::Elem {
                let is_binary = matches!(v, JsonbItem::Binary(_));
                if level >= first || (first == u32::MAX && last == u32::MAX && !is_binary) {
                    let jbv = jbv_from_item(v);
                    match jsp {
                        Some(jsp) => {
                            if ignore_structural_errors {
                                let saved = self.ignore_structural_errors;
                                self.ignore_structural_errors = true;
                                res = self.execute_item_opt_unwrap_target(
                                    jsp,
                                    &jbv,
                                    found.as_deref_mut(),
                                    unwrap_next,
                                )?;
                                self.ignore_structural_errors = saved;
                            } else {
                                res = self.execute_item_opt_unwrap_target(
                                    jsp,
                                    &jbv,
                                    found.as_deref_mut(),
                                    unwrap_next,
                                )?;
                            }
                            if res == Jper::Error {
                                break;
                            }
                            if res == Jper::Ok && found.is_none() {
                                break;
                            }
                        }
                        None => match found.as_deref_mut() {
                            Some(found) => found.append(jbv),
                            None => return Ok(Jper::Ok),
                        },
                    }
                }

                if level < last {
                    if let JsonbItem::Binary(child) = v {
                        res = self.execute_any_item(
                            jsp,
                            child,
                            found.as_deref_mut(),
                            level + 1,
                            first,
                            last,
                            ignore_structural_errors,
                            unwrap_next,
                        )?;
                        if res == Jper::Error {
                            break;
                        }
                        if res == Jper::Ok && found.is_none() {
                            break;
                        }
                    }
                }
            }
        }

        Ok(res)
    }

    /// C: executePredicate — existence semantics over item sequences with
    /// SQL/JSON three-valued logic.
    fn execute_predicate(
        &mut self,
        pred: PredOp<'a>,
        larg: &JsonPathItem<'a>,
        rarg: Option<&JsonPathItem<'a>>,
        jb: &JbV<'a>,
        unwrap_right_arg: bool,
    ) -> PgResult<JpBool> {
        let mut error = false;
        let mut found = false;

        // Left argument is always auto-unwrapped.
        let mut lseq = JsonValueList::new(self.mcx)?;
        let res = self.execute_item_opt_unwrap_result_no_throw(larg, jb, true, Some(&mut lseq))?;
        if res == Jper::Error {
            return Ok(JpBool::Unknown);
        }

        // Right argument is conditionally auto-unwrapped.
        let mut rseq = JsonValueList::new(self.mcx)?;
        if let Some(rarg) = rarg {
            let res = self.execute_item_opt_unwrap_result_no_throw(
                rarg,
                jb,
                unwrap_right_arg,
                Some(&mut rseq),
            )?;
            if res == Jper::Error {
                return Ok(JpBool::Unknown);
            }
        }

        for lval in lseq.as_slice() {
            // Loop over the right arg sequence, or do a single pass.
            let mut i = 0;
            loop {
                let rval = if rarg.is_some() {
                    match rseq.as_slice().get(i) {
                        Some(r) => Some(r),
                        None => break,
                    }
                } else {
                    if i > 0 {
                        break;
                    }
                    None
                };
                i += 1;

                let res = self.exec_pred_op(&pred, lval, rval)?;
                match res {
                    JpBool::Unknown => {
                        if self.strict_absence_of_errors() {
                            return Ok(JpBool::Unknown);
                        }
                        error = true;
                    }
                    JpBool::True => {
                        if !self.strict_absence_of_errors() {
                            return Ok(JpBool::True);
                        }
                        found = true;
                    }
                    JpBool::False => {}
                }
            }
        }

        if found {
            // Possible only in strict mode.
            return Ok(JpBool::True);
        }
        if error {
            // Possible only in lax mode.
            return Ok(JpBool::Unknown);
        }
        Ok(JpBool::False)
    }

    fn exec_pred_op(
        &mut self,
        pred: &PredOp<'a>,
        lval: &JbV<'a>,
        rval: Option<&JbV<'a>>,
    ) -> PgResult<JpBool> {
        match pred {
            PredOp::Compare(op) => {
                compare_items(*op, lval, rval.expect("comparison has rarg"), self.use_tz)
            }
            PredOp::StartsWith => {
                let whole = get_scalar(lval, JbKind::String);
                let initial = rval.and_then(|r| get_scalar(r, JbKind::String));
                match (whole, initial) {
                    (Some(JbV::String(w)), Some(JbV::String(i))) => {
                        Ok(if w.len() >= i.len() && &w[..i.len()] == *i {
                            JpBool::True
                        } else {
                            JpBool::False
                        })
                    }
                    _ => Ok(JpBool::Unknown),
                }
            }
            PredOp::LikeRegex { pattern, cflags } => {
                let Some(JbV::String(s)) = get_scalar(lval, JbKind::String) else {
                    return Ok(JpBool::Unknown);
                };
                let matched = adt_regexp::RE_compile_and_execute(
                    self.mcx,
                    pattern,
                    s,
                    *cflags,
                    DEFAULT_COLLATION_OID,
                    &mut [],
                )?;
                Ok(if matched { JpBool::True } else { JpBool::False })
            }
        }
    }

    /// C: executeBinaryArithmExpr on singleton numeric operands.
    fn execute_binary_arithm_expr(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        func: fn(Num<'_>, Num<'_>) -> PgResult<NumericImage>,
        found: Found<'_, 'a, 'mcx>,
    ) -> ExecRes {
        // By the standard only multiplicative operands are unwrapped; C
        // extends it to all binary arithmetic.
        let mut lseq = JsonValueList::new(self.mcx)?;
        let jper = self.execute_item_opt_unwrap_result(&jsp.left_arg(), jb, true, &mut lseq)?;
        if jper == Jper::Error {
            return Ok(jper);
        }

        let mut rseq = JsonValueList::new(self.mcx)?;
        let jper = self.execute_item_opt_unwrap_result(&jsp.right_arg(), jb, true, &mut rseq)?;
        if jper == Jper::Error {
            return Ok(jper);
        }

        let lnum = if lseq.len() == 1 {
            get_scalar(lseq.head().expect("len 1"), JbKind::Numeric)
        } else {
            None
        };
        let Some(JbV::Numeric(lval)) = lnum else {
            return_error!(
                self,
                PgError::error(format!(
                    "left operand of jsonpath operator {} is not a single numeric value",
                    operation_name(jsp.typ)
                ))
                .with_sqlstate(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED)
            );
        };

        let rnum = if rseq.len() == 1 {
            get_scalar(rseq.head().expect("len 1"), JbKind::Numeric)
        } else {
            None
        };
        let Some(JbV::Numeric(rval)) = rnum else {
            return_error!(
                self,
                PgError::error(format!(
                    "right operand of jsonpath operator {} is not a single numeric value",
                    operation_name(jsp.typ)
                ))
                .with_sqlstate(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED)
            );
        };

        let res = match func(num_of(lval), num_of(rval)) {
            Ok(img) => img,
            Err(e) => {
                if self.throw_errors {
                    return Err(e);
                }
                return Ok(Jper::Error);
            }
        };

        let next = jsp.next();
        if next.is_none() && found.is_none() {
            return Ok(Jper::Ok);
        }

        let v = JbV::Numeric(leak_numeric(self.mcx, &res)?);
        self.execute_next_item(Some(jsp), next.as_ref(), &v, found)
    }

    /// C: executeUnaryArithmExpr for each numeric item in the sequence.
    fn execute_unary_arithm_expr(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        func: Option<fn(Num<'_>) -> NumericImage>,
        mut found: Found<'_, 'a, 'mcx>,
    ) -> ExecRes {
        let mut seq = JsonValueList::new(self.mcx)?;
        let jper = self.execute_item_opt_unwrap_result(&jsp.arg(), jb, true, &mut seq)?;
        if jper == Jper::Error {
            return Ok(jper);
        }

        let mut jper = Jper::NotFound;
        let next = jsp.next();
        let has_next = next.is_some();

        for val in seq.as_slice() {
            let val = match get_scalar(val, JbKind::Numeric) {
                Some(v) => {
                    if found.is_none() && !has_next {
                        return Ok(Jper::Ok);
                    }
                    v
                }
                None => {
                    if found.is_none() && !has_next {
                        continue; // skip non-numerics processing
                    }
                    return_error!(
                        self,
                        PgError::error(format!(
                            "operand of unary jsonpath operator {} is not a numeric value",
                            operation_name(jsp.typ)
                        ))
                        .with_sqlstate(ERRCODE_SQL_JSON_NUMBER_NOT_FOUND)
                    );
                }
            };

            let v = match func {
                Some(f) => {
                    let JbV::Numeric(image) = val else { unreachable!() };
                    let img = f(num_of(image));
                    JbV::Numeric(leak_numeric(self.mcx, &img)?)
                }
                None => *val,
            };

            let jper2 = self.execute_next_item(Some(jsp), next.as_ref(), &v, found.as_deref_mut())?;

            if jper2 == Jper::Error {
                return Ok(jper2);
            }
            if jper2 == Jper::Ok {
                if found.is_none() {
                    return Ok(Jper::Ok);
                }
                jper = Jper::Ok;
            }
        }

        Ok(jper)
    }

    /// C: executeNumericItemMethod (.abs()/.floor()/.ceiling()).
    fn execute_numeric_item_method(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        unwrap: bool,
        method: NumericMethod,
        found: Found<'_, 'a, 'mcx>,
    ) -> ExecRes {
        if unwrap && jsonb_kind(jb) == JbKind::Array {
            return self.execute_item_unwrap_target_array(Some(jsp), jb, found, false);
        }

        let Some(JbV::Numeric(image)) = get_scalar(jb, JbKind::Numeric) else {
            return_error!(
                self,
                method_applies_error(
                    ERRCODE_NON_NUMERIC_SQL_JSON_ITEM,
                    operation_name(jsp.typ),
                    "a numeric value",
                )
            );
        };

        let img = match method {
            NumericMethod::Abs => adt_numeric::numeric_abs(num_of(image)),
            NumericMethod::Floor => adt_numeric::numeric_floor(num_of(image))?,
            NumericMethod::Ceiling => adt_numeric::numeric_ceil(num_of(image))?,
        };

        let next = jsp.next();
        if next.is_none() && found.is_none() {
            return Ok(Jper::Ok);
        }

        let v = JbV::Numeric(leak_numeric(self.mcx, &img)?);
        self.execute_next_item(Some(jsp), next.as_ref(), &v, found)
    }

    /// C: executeDateTimeMethod (.datetime() and typed variants; the ISO
    /// format inference loop is the no-argument lane).
    fn execute_datetime_method(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        found: Found<'_, 'a, 'mcx>,
    ) -> ExecRes {
        let op = operation_name(jsp.typ);
        let collid = DEFAULT_COLLATION_OID;
        let mut tz: i32 = 0;
        let mut typmod: i32 = -1;
        let mut time_precision: i32 = -1;

        let Some(&JbV::String(datetime)) = get_scalar(jb, JbKind::String) else {
            return_error!(
                self,
                method_applies_error(
                    ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION,
                    op,
                    "a string",
                )
            );
        };

        let mut value: Option<ParsedDatetime> = None;

        if jsp.typ == ItemType::Datetime && jsp.content.arg != 0 {
            let elem = jsp.arg();
            if elem.typ != ItemType::String {
                panic!("invalid jsonpath item type for .datetime() argument");
            }
            let template = elem.get_string();

            if self.throw_errors {
                value = adt_formatting::parse_datetime(
                    self.mcx, datetime, template, collid, true, &mut typmod, &mut tz, None,
                )?;
            } else {
                let mut esc = SoftErrorContext::new(false);
                value = adt_formatting::parse_datetime(
                    self.mcx,
                    datetime,
                    template,
                    collid,
                    true,
                    &mut typmod,
                    &mut tz,
                    Some(&mut esc),
                )?;
                if esc.error_occurred() {
                    value = None;
                }
            }
            if value.is_none() {
                return Ok(Jper::Error);
            }
        } else {
            // SQL/JSON standard ISO formats for date, timetz, time,
            // timestamptz, timestamp (+ the ISO 8601 "T" forms to_json emits).
            static FMT_STR: [&[u8]; 13] = [
                b"yyyy-mm-dd",
                b"HH24:MI:SS.USTZ",
                b"HH24:MI:SSTZ",
                b"HH24:MI:SS.US",
                b"HH24:MI:SS",
                b"yyyy-mm-dd HH24:MI:SS.USTZ",
                b"yyyy-mm-dd HH24:MI:SSTZ",
                b"yyyy-mm-dd\"T\"HH24:MI:SS.USTZ",
                b"yyyy-mm-dd\"T\"HH24:MI:SSTZ",
                b"yyyy-mm-dd HH24:MI:SS.US",
                b"yyyy-mm-dd HH24:MI:SS",
                b"yyyy-mm-dd\"T\"HH24:MI:SS.US",
                b"yyyy-mm-dd\"T\"HH24:MI:SS",
            ];

            if jsp.typ != ItemType::Datetime && jsp.typ != ItemType::Date && jsp.content.arg != 0 {
                let elem = jsp.arg();
                if elem.typ != ItemType::Numeric {
                    panic!("invalid jsonpath item type for {op} argument");
                }
                time_precision = match adt_numeric::numeric_int4(num_of(elem.get_numeric())) {
                    Ok(v) => v,
                    Err(_) => {
                        return_error!(
                            self,
                            PgError::error(format!(
                                "time precision of jsonpath item method .{op}() is out of range for type integer"
                            ))
                            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
                        );
                    }
                };
            }

            for fmt in FMT_STR {
                let mut esc = SoftErrorContext::new(false);
                let parsed = adt_formatting::parse_datetime(
                    self.mcx,
                    datetime,
                    fmt,
                    collid,
                    true,
                    &mut typmod,
                    &mut tz,
                    Some(&mut esc),
                )?;
                if !esc.error_occurred() {
                    value = parsed;
                    break;
                }
            }

            if value.is_none() {
                if jsp.typ == ItemType::Datetime {
                    return_error!(
                        self,
                        PgError::error(format!(
                            "datetime format is not recognized: \"{}\"",
                            String::from_utf8_lossy(datetime)
                        ))
                        .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
                        .with_hint("Use a datetime template argument to specify the input data format.")
                    );
                } else {
                    return_error!(
                        self,
                        PgError::error(format!(
                            "{op} format is not recognized: \"{}\"",
                            String::from_utf8_lossy(datetime)
                        ))
                        .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
                    );
                }
            }
        }

        let mut value = value.expect("checked above");

        // parse_datetime() returned the best-fitted type; coerce to the
        // method's type, erroring on incompatible casts.
        macro_rules! not_recognized {
            ($name:expr) => {
                return_error!(
                    self,
                    PgError::error(format!(
                        "{} format is not recognized: \"{}\"",
                        $name,
                        String::from_utf8_lossy(datetime)
                    ))
                    .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
                )
            };
        }

        match jsp.typ {
            ItemType::Datetime => {}
            ItemType::Date => {
                value = match value {
                    ParsedDatetime::Date(_) => value,
                    ParsedDatetime::Time(_) | ParsedDatetime::TimeTz(_) => not_recognized!("date"),
                    ParsedDatetime::Timestamp(ts) => {
                        ParsedDatetime::Date(adt_date::timestamp_date(ts)?)
                    }
                    ParsedDatetime::TimestampTz(ts) => {
                        self.check_timezone_is_used("timestamptz", "date")?;
                        ParsedDatetime::Date(adt_date::timestamptz_date(ts)?)
                    }
                };
            }
            ItemType::Time => {
                value = match value {
                    ParsedDatetime::Date(_) => not_recognized!("time"),
                    ParsedDatetime::Time(_) => value,
                    ParsedDatetime::TimeTz(ttz) => {
                        self.check_timezone_is_used("timetz", "time")?;
                        ParsedDatetime::Time(adt_date::timetz_time(&ttz))
                    }
                    ParsedDatetime::Timestamp(ts) => ParsedDatetime::Time(
                        adt_date::timestamp_time(ts)?.expect("finite timestamp"),
                    ),
                    ParsedDatetime::TimestampTz(ts) => {
                        self.check_timezone_is_used("timestamptz", "time")?;
                        ParsedDatetime::Time(
                            adt_date::timestamptz_time(ts)?.expect("finite timestamp"),
                        )
                    }
                };
                if time_precision != -1 {
                    // Warns when precision is reduced.
                    time_precision = adt_date::anytime_typmod_check(false, time_precision)?;
                    let ParsedDatetime::Time(mut t) = value else { unreachable!() };
                    adt_date::AdjustTimeForTypmod(&mut t, time_precision);
                    value = ParsedDatetime::Time(t);
                    typmod = time_precision;
                }
            }
            ItemType::TimeTz => {
                value = match value {
                    ParsedDatetime::Date(_) | ParsedDatetime::Timestamp(_) => {
                        not_recognized!("time_tz")
                    }
                    ParsedDatetime::Time(t) => {
                        self.check_timezone_is_used("time", "timetz")?;
                        ParsedDatetime::TimeTz(time_timetz_tz(t))
                    }
                    ParsedDatetime::TimeTz(_) => value,
                    ParsedDatetime::TimestampTz(ts) => ParsedDatetime::TimeTz(
                        adt_date::timestamptz_timetz(ts)?.expect("finite timestamp"),
                    ),
                };
                if time_precision != -1 {
                    time_precision = adt_date::anytime_typmod_check(true, time_precision)?;
                    let ParsedDatetime::TimeTz(mut t) = value else { unreachable!() };
                    adt_date::AdjustTimeForTypmod(&mut t.time, time_precision);
                    value = ParsedDatetime::TimeTz(t);
                    typmod = time_precision;
                }
            }
            ItemType::Timestamp => {
                value = match value {
                    ParsedDatetime::Date(d) => ParsedDatetime::Timestamp(adt_date::date2timestamp(d)?),
                    ParsedDatetime::Time(_) | ParsedDatetime::TimeTz(_) => {
                        not_recognized!("timestamp")
                    }
                    ParsedDatetime::Timestamp(_) => value,
                    ParsedDatetime::TimestampTz(ts) => {
                        self.check_timezone_is_used("timestamptz", "timestamp")?;
                        ParsedDatetime::Timestamp(adt_timestamp::timestamptz2timestamp(ts)?)
                    }
                };
                if time_precision != -1 {
                    time_precision = adt_timestamp::anytimestamp_typmod_check(false, time_precision)?;
                    let ParsedDatetime::Timestamp(mut ts) = value else { unreachable!() };
                    let mut esc2 = SoftErrorContext::new(false);
                    if !adt_timestamp::AdjustTimestampForTypmod(
                        &mut ts,
                        time_precision,
                        Some(&mut esc2),
                    )? || esc2.error_occurred()
                    {
                        return_error!(
                            self,
                            PgError::error(format!(
                                "time precision of jsonpath item method .{op}() is invalid"
                            ))
                            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
                        );
                    }
                    value = ParsedDatetime::Timestamp(ts);
                    typmod = time_precision;
                }
            }
            ItemType::TimestampTz => {
                value = match value {
                    ParsedDatetime::Date(d) => {
                        self.check_timezone_is_used("date", "timestamptz")?;
                        // JsonbValue keeps the tz separate.
                        let mut tm = adt_datetime::consts::pg_tm {
                            tm_mday: 1,
                            tm_mon: 1,
                            ..Default::default()
                        };
                        adt_datetime::j2date(
                            d + adt_datetime::POSTGRES_EPOCH_JDATE,
                            &mut tm.tm_year,
                            &mut tm.tm_mon,
                            &mut tm.tm_mday,
                        );
                        tm.tm_hour = 0;
                        tm.tm_min = 0;
                        tm.tm_sec = 0;
                        tz = session_tz_offset(&mut tm);
                        ParsedDatetime::TimestampTz(adt_date::date2timestamptz(d)?)
                    }
                    ParsedDatetime::Time(_) | ParsedDatetime::TimeTz(_) => {
                        not_recognized!("timestamp_tz")
                    }
                    ParsedDatetime::Timestamp(ts) => {
                        self.check_timezone_is_used("timestamp", "timestamptz")?;
                        let mut tm = adt_datetime::consts::pg_tm {
                            tm_mday: 1,
                            tm_mon: 1,
                            ..Default::default()
                        };
                        let mut fsec = 0;
                        if adt_timestamp::timestamp2tm(ts, None, &mut tm, &mut fsec, None, None)
                            .is_ok()
                        {
                            tz = session_tz_offset(&mut tm);
                        }
                        ParsedDatetime::TimestampTz(adt_timestamp::timestamp2timestamptz(ts)?)
                    }
                    ParsedDatetime::TimestampTz(_) => value,
                };
                if time_precision != -1 {
                    time_precision = adt_timestamp::anytimestamp_typmod_check(true, time_precision)?;
                    let ParsedDatetime::TimestampTz(mut ts) = value else { unreachable!() };
                    let mut esc2 = SoftErrorContext::new(false);
                    if !adt_timestamp::AdjustTimestampForTypmod(
                        &mut ts,
                        time_precision,
                        Some(&mut esc2),
                    )? || esc2.error_occurred()
                    {
                        return_error!(
                            self,
                            PgError::error(format!(
                                "time precision of jsonpath item method .{op}() is invalid"
                            ))
                            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
                        );
                    }
                    value = ParsedDatetime::TimestampTz(ts);
                    typmod = time_precision;
                }
            }
            _ => panic!("unrecognized jsonpath item type: {}", jsp.typ as i32),
        }

        let next = jsp.next();
        if next.is_none() && found.is_none() {
            return Ok(Jper::Ok);
        }

        let v = JbV::Datetime { value, typmod, tz };
        self.execute_next_item(Some(jsp), next.as_ref(), &v, found)
    }

    /// C: checkTimezoneIsUsedForCast.
    fn check_timezone_is_used(&self, type1: &str, type2: &str) -> PgResult<()> {
        if !self.use_tz {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot convert value from {type1} to {type2} without time zone usage"
                ))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_hint("Use *_tz() function for time zone support."),
            ));
        }
        Ok(())
    }

    /// C: executeKeyValueMethod — '{"key": k, "value": v, "id": id}' rows;
    /// id = 10^10 * base_object_id + offset_in_base_object.
    fn execute_keyvalue_method(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        mut found: Found<'_, 'a, 'mcx>,
    ) -> ExecRes {
        let mut res = Jper::NotFound;

        if jsonb_kind(jb) != JbKind::Object {
            return_error!(
                self,
                method_applies_error(
                    ERRCODE_SQL_JSON_OBJECT_NOT_FOUND,
                    operation_name(jsp.typ),
                    "an object",
                )
            );
        }
        let JbV::Binary(jbc) = jb else {
            return_error!(
                self,
                method_applies_error(
                    ERRCODE_SQL_JSON_OBJECT_NOT_FOUND,
                    operation_name(jsp.typ),
                    "an object",
                )
            );
        };

        if container_size(jbc) == 0 {
            return Ok(Jper::NotFound); // no key-value pairs
        }

        let next = jsp.next();
        let has_next = next.is_some();

        let base_ptr = self
            .base_object
            .jbc
            .map_or(0i64, |b| b.as_ptr() as i64);
        let mut id = jbc.as_ptr() as i64 - base_ptr;
        id += (self.base_object.id as i64) * 10_000_000_000;

        let idnum = adt_numeric::int64_to_numeric(id);
        let idval: &'mcx [u8] = leak_numeric(self.mcx, &idnum)?;

        let mut it = JsonbIterator::init(self.mcx, jbc)?;
        loop {
            let (tok, key) = it.next(true);
            if tok == WjbToken::Done {
                break;
            }
            if tok != WjbToken::Key {
                continue;
            }

            res = Jper::Ok;

            if !has_next && found.is_none() {
                break;
            }

            let (vtok, val) = it.next(true);
            debug_assert_eq!(vtok, WjbToken::Value);

            let mut ps = adt_jsonb::build::JsonbBuildState::new(self.mcx)?;
            ps.begin_object(false)?;
            ps.push_key(b"key")?;
            ps.push_value(build_value_from_item(self.mcx, key)?);
            ps.push_key(b"value")?;
            ps.push_value(build_value_from_item(self.mcx, val)?);
            ps.push_key(b"id")?;
            ps.push_value(BuildValue::Numeric(idval));
            let obj = ps.end_object()?.expect("root object");

            let image = convert_to_jsonb(self.mcx, &obj)?;
            let image: &'mcx [u8] = image.leak();
            let obj_v = JbV::Binary(&image[4..]);

            let id = self.last_generated_object_id;
            self.last_generated_object_id += 1;
            let base_object = self.set_base_object(&obj_v, id);

            res = self.execute_next_item(Some(jsp), next.as_ref(), &obj_v, found.as_deref_mut())?;

            self.base_object = base_object;

            if res == Jper::Error {
                return Ok(res);
            }
            if res == Jper::Ok && found.is_none() {
                break;
            }
        }

        Ok(res)
    }

    /// C: appendBoolResult.
    fn append_bool_result(
        &mut self,
        jsp: &JsonPathItem<'a>,
        found: Found<'_, 'a, 'mcx>,
        res: JpBool,
    ) -> ExecRes {
        let next = jsp.next();
        if next.is_none() && found.is_none() {
            return Ok(Jper::Ok); // found singleton boolean value
        }
        let jbv = match res {
            JpBool::Unknown => JbV::Null,
            JpBool::True => JbV::Bool(true),
            JpBool::False => JbV::Bool(false),
        };
        self.execute_next_item(Some(jsp), next.as_ref(), &jbv, found)
    }

    /// C: getArrayIndex — subscript expression to int32 with truncation.
    fn get_array_index(
        &mut self,
        jsp: &JsonPathItem<'a>,
        jb: &JbV<'a>,
        index: &mut i32,
    ) -> ExecRes {
        let mut found = JsonValueList::new(self.mcx)?;
        let res = self.execute_item(jsp, jb, Some(&mut found))?;
        if res == Jper::Error {
            return Ok(res);
        }

        let num = if found.len() == 1 {
            get_scalar(found.head().expect("len 1"), JbKind::Numeric)
        } else {
            None
        };
        let Some(JbV::Numeric(image)) = num else {
            return_error!(
                self,
                PgError::error("jsonpath array subscript is not a single numeric value")
                    .with_sqlstate(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT)
            );
        };

        let trunced = adt_numeric::numeric_trunc_common(num_of(image), 0)?;
        match adt_numeric::numeric_int4(trunced.num()) {
            Ok(v) => *index = v,
            Err(_) => {
                return_error!(
                    self,
                    PgError::error("jsonpath array subscript is out of integer range")
                        .with_sqlstate(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT)
                );
            }
        }

        Ok(Jper::Ok)
    }
}

#[derive(Clone, Copy)]
enum NumericMethod {
    Abs,
    Floor,
    Ceiling,
}

enum PredOp<'a> {
    Compare(ItemType),
    StartsWith,
    LikeRegex { pattern: &'a [u8], cflags: i32 },
}

/// C: time_timetz cast core (date.c) — attaches the session zone.
fn time_timetz_tz(t: TimeADT) -> TimeTzADT {
    adt_date::time_timetz(t)
}

fn session_tz_offset(tm: &mut adt_datetime::consts::pg_tm) -> i32 {
    let session = adt_datetime::tz::session_timezone()
        .unwrap_or_else(|| panic!("jsonpath datetime: session_timezone not initialized"));
    adt_datetime::tz::DetermineTimeZoneOffset(tm, session)
}

/// Deep-copy a read-side item into a build-side value tree (C: pushJsonbValue
/// jbvBinary expansion).
fn build_value_from_item<'mcx>(
    mcx: Mcx<'mcx>,
    item: JsonbItem<'_>,
) -> PgResult<BuildValue<'mcx>> {
    match item {
        JsonbItem::Null => Ok(BuildValue::Null),
        JsonbItem::Bool(b) => Ok(BuildValue::Bool(b)),
        JsonbItem::String(s) => Ok(BuildValue::String(mcx::slice_in(mcx, s)?.leak())),
        JsonbItem::Numeric(n) => {
            // Embedded numerics may be short-varlena; build wants 4B headers.
            Ok(BuildValue::Numeric(numeric_image_4b(mcx, n)?))
        }
        JsonbItem::Binary(c) => build_value_from_container(mcx, c),
        JsonbItem::Array { .. } | JsonbItem::Object { .. } => {
            panic!("build_value_from_item: begin-token item is not a value")
        }
    }
}

fn numeric_image_4b<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<&'mcx [u8]> {
    if image[0] & 0x01 == 0x01 {
        let payload = &image[1..];
        let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 4 + payload.len())?;
        mcx::vec_append_bytes(&mut v, &(((4 + payload.len()) as u32) << 2).to_ne_bytes())?;
        mcx::vec_append_bytes(&mut v, payload)?;
        Ok(v.leak())
    } else {
        Ok(mcx::slice_in(mcx, image)?.leak())
    }
}

fn build_value_from_container<'mcx>(mcx: Mcx<'mcx>, c: &[u8]) -> PgResult<BuildValue<'mcx>> {
    check_stack_depth()?;
    let n = container_size(c);
    if container_is_object(c) {
        let mut pairs: ArenaVec<'mcx, adt_jsonb::build::JsonbPair<'mcx>> =
            ArenaVec::with_capacity(mcx, (n as usize).max(1))?;
        let mut it = JsonbIterator::init(mcx, c)?;
        let (tok, _) = it.next(true);
        debug_assert_eq!(tok, WjbToken::BeginObject);
        let mut order = 0u32;
        loop {
            let (tok, k) = it.next(true);
            match tok {
                WjbToken::EndObject => break,
                WjbToken::Key => {
                    let JsonbItem::String(key) = k else {
                        panic!("unexpected jsonb type as object key");
                    };
                    let (vtok, v) = it.next(true);
                    debug_assert_eq!(vtok, WjbToken::Value);
                    pairs.push(
                        mcx,
                        adt_jsonb::build::JsonbPair {
                            key: mcx::slice_in(mcx, key)?.leak(),
                            value: build_value_from_item(mcx, v)?,
                            order,
                        },
                    )?;
                    order += 1;
                }
                _ => panic!("unexpected jsonb iterator token"),
            }
        }
        Ok(BuildValue::Object { pairs })
    } else {
        let mut elems: ArenaVec<'mcx, BuildValue<'mcx>> =
            ArenaVec::with_capacity(mcx, (n as usize).max(1))?;
        let mut it = JsonbIterator::init(mcx, c)?;
        let (tok, _) = it.next(true);
        debug_assert_eq!(tok, WjbToken::BeginArray);
        loop {
            let (tok, v) = it.next(true);
            match tok {
                WjbToken::EndArray => break,
                WjbToken::Elem => elems.push(mcx, build_value_from_item(mcx, v)?)?,
                _ => panic!("unexpected jsonb iterator token"),
            }
        }
        Ok(BuildValue::Array {
            elems,
            raw_scalar: false,
        })
    }
}

/// Build-side rendering of an executor value (C: JsonbValueToJsonb input
/// shapes; jbvDatetime serializes as its text form).
fn build_value_from_jbv<'mcx>(mcx: Mcx<'mcx>, v: &JbV<'_>) -> PgResult<BuildValue<'mcx>> {
    match v {
        JbV::Null => Ok(BuildValue::Null),
        JbV::Bool(b) => Ok(BuildValue::Bool(*b)),
        JbV::Numeric(n) => Ok(BuildValue::Numeric(numeric_image_4b(mcx, n)?)),
        JbV::String(s) => Ok(BuildValue::String(mcx::slice_in(mcx, s)?.leak())),
        JbV::Binary(c) => build_value_from_container(mcx, c),
        JbV::Datetime { value, tz, .. } => {
            Ok(BuildValue::String(encode_datetime(mcx, value, *tz)?))
        }
    }
}

/// C: JsonEncodeDateTime over the executor's datetime carrier.
fn encode_datetime<'mcx>(
    mcx: Mcx<'mcx>,
    value: &ParsedDatetime,
    tz: i32,
) -> PgResult<&'mcx [u8]> {
    let mut buf = [0u8; adt_datetime::consts::MAXDATELEN + 1];
    let ttz_store;
    let (datum, typid) = match value {
        ParsedDatetime::Date(d) => (Datum::from_i32(*d), DATEOID),
        ParsedDatetime::Time(t) => (Datum::from_i64(*t), TIMEOID),
        ParsedDatetime::TimeTz(t) => {
            ttz_store = *t;
            (
                Datum::from_usize(&ttz_store as *const TimeTzADT as usize),
                TIMETZOID,
            )
        }
        ParsedDatetime::Timestamp(ts) => (Datum::from_i64(*ts), TIMESTAMPOID),
        ParsedDatetime::TimestampTz(ts) => (Datum::from_i64(*ts), TIMESTAMPTZOID),
    };
    let len = adt_json::tojson::json_encode_datetime_tz(&mut buf, datum, typid, Some(tz))?;
    Ok(mcx::slice_in(mcx, &buf[..len])?.leak())
}

/// Serialize one executor value to a full jsonb varlena image
/// (C: JsonbValueToJsonb).
pub fn jbv_to_jsonb_image<'mcx>(mcx: Mcx<'mcx>, v: &JbV<'_>) -> PgResult<PgVec<'mcx, u8>> {
    match v {
        JbV::Binary(c) => {
            let mut out = mcx::vec_with_capacity_in(mcx, 4 + c.len())?;
            mcx::vec_append_bytes(&mut out, &((4 + c.len() as u32) << 2).to_ne_bytes())?;
            mcx::vec_append_bytes(&mut out, c)?;
            Ok(out)
        }
        scalar => {
            let mut elems: ArenaVec<'mcx, BuildValue<'mcx>> = ArenaVec::with_capacity(mcx, 1)?;
            elems.push(mcx, build_value_from_jbv(mcx, scalar)?)?;
            let val = BuildValue::Array {
                elems,
                raw_scalar: true,
            };
            convert_to_jsonb(mcx, &val)
        }
    }
}

/// C: wrapItemsInArray + JsonbValueToJsonb.
pub fn wrap_items_in_array_image<'mcx>(
    mcx: Mcx<'mcx>,
    items: &JsonValueList<'_, '_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut elems: ArenaVec<'mcx, BuildValue<'mcx>> =
        ArenaVec::with_capacity(mcx, items.len().max(1))?;
    for v in items.as_slice() {
        elems.push(mcx, build_value_from_jbv(mcx, v)?)?;
    }
    let val = BuildValue::Array {
        elems,
        raw_scalar: false,
    };
    convert_to_jsonb(mcx, &val)
}

/// C: compareItems with SQL/JSON comparison semantics.
fn compare_items(op: ItemType, jb1: &JbV<'_>, jb2: &JbV<'_>, use_tz: bool) -> PgResult<JpBool> {
    let k1 = jsonb_kind(jb1);
    let k2 = jsonb_kind(jb2);

    if k1 != k2 {
        if k1 == JbKind::Null || k2 == JbKind::Null {
            // Equality and order comparisons of null to non-null are false,
            // inequality is true.
            return Ok(if op == ItemType::NotEqual {
                JpBool::True
            } else {
                JpBool::False
            });
        }
        // Non-null items of different types are not comparable.
        return Ok(JpBool::Unknown);
    }

    let cmp: i32 = match (jb1, jb2) {
        (JbV::Null, JbV::Null) => 0,
        (JbV::Bool(b1), JbV::Bool(b2)) => {
            if b1 == b2 {
                0
            } else if *b1 {
                1
            } else {
                -1
            }
        }
        (JbV::Numeric(n1), JbV::Numeric(n2)) => {
            adt_numeric::cmp_numerics(num_of(n1), num_of(n2))
        }
        (JbV::String(s1), JbV::String(s2)) => {
            if op == ItemType::Equal {
                return Ok(if s1 == s2 { JpBool::True } else { JpBool::False });
            }
            // Unicode-codepoint collation; the server encoding is UTF-8
            // (byte order == codepoint order), so binary compare suffices.
            compare_strings(s1, s2)
        }
        (
            JbV::Datetime { value: v1, .. },
            JbV::Datetime { value: v2, .. },
        ) => match compare_datetime(v1, v2, use_tz)? {
            None => return Ok(JpBool::Unknown),
            Some(c) => c,
        },
        (JbV::Binary(_), JbV::Binary(_)) => return Ok(JpBool::Unknown),
        _ => panic!("invalid jsonb value type"),
    };

    let res = match op {
        ItemType::Equal => cmp == 0,
        ItemType::NotEqual => cmp != 0,
        ItemType::Less => cmp < 0,
        ItemType::Greater => cmp > 0,
        ItemType::LessOrEqual => cmp <= 0,
        ItemType::GreaterOrEqual => cmp >= 0,
        _ => panic!("unrecognized jsonpath operation: {}", op as i32),
    };

    Ok(if res { JpBool::True } else { JpBool::False })
}

fn compare_strings(s1: &[u8], s2: &[u8]) -> i32 {
    let n = s1.len().min(s2.len());
    match s1[..n].cmp(&s2[..n]) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Greater => 1,
        core::cmp::Ordering::Equal => {
            if s1.len() == s2.len() {
                0
            } else if s1.len() < s2.len() {
                -1
            } else {
                1
            }
        }
    }
}

#[track_caller]
#[cold]
fn tz_cast_error(type1: &str, type2: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "cannot convert value from {type1} to {type2} without time zone usage"
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_hint("Use *_tz() function for time zone support."),
    )
}

/// C: compareDatetime — Ok(None) = uncomparable (cast_error); a required-but
/// -unused timezone is a thrown error even in silent mode.
fn compare_datetime(
    v1: &ParsedDatetime,
    v2: &ParsedDatetime,
    use_tz: bool,
) -> PgResult<Option<i32>> {
    use ParsedDatetime::*;

    let check_tz = |t1: &str, t2: &str| -> PgResult<()> {
        if !use_tz {
            return Err(tz_cast_error(t1, t2));
        }
        Ok(())
    };

    let cmp = match (v1, v2) {
        (Date(d1), Date(d2)) => date_cmp(*d1, *d2),
        (Date(d1), Timestamp(ts2)) => adt_date::date_cmp_timestamp_internal(*d1, *ts2),
        (Date(d1), TimestampTz(ts2)) => {
            check_tz("date", "timestamptz")?;
            adt_date::date_cmp_timestamptz_internal(*d1, *ts2)
        }
        (Date(_), Time(_) | TimeTz(_)) => return Ok(None),

        (Time(t1), Time(t2)) => adt_date::time_cmp_internal(*t1, *t2),
        (Time(t1), TimeTz(t2)) => {
            check_tz("time", "timetz")?;
            adt_date::timetz_cmp_internal(&adt_date::time_timetz(*t1), t2)
        }
        (Time(_), Date(_) | Timestamp(_) | TimestampTz(_)) => return Ok(None),

        (TimeTz(t1), Time(t2)) => {
            check_tz("time", "timetz")?;
            adt_date::timetz_cmp_internal(t1, &adt_date::time_timetz(*t2))
        }
        (TimeTz(t1), TimeTz(t2)) => adt_date::timetz_cmp_internal(t1, t2),
        (TimeTz(_), Date(_) | Timestamp(_) | TimestampTz(_)) => return Ok(None),

        (Timestamp(ts1), Date(d2)) => -adt_date::date_cmp_timestamp_internal(*d2, *ts1),
        (Timestamp(ts1), Timestamp(ts2)) => timestamp_cmp(*ts1, *ts2),
        (Timestamp(ts1), TimestampTz(ts2)) => {
            check_tz("timestamp", "timestamptz")?;
            adt_timestamp::timestamp_cmp_timestamptz_internal(*ts1, *ts2)
        }
        (Timestamp(_), Time(_) | TimeTz(_)) => return Ok(None),

        (TimestampTz(ts1), Date(d2)) => {
            check_tz("date", "timestamptz")?;
            -adt_date::date_cmp_timestamptz_internal(*d2, *ts1)
        }
        (TimestampTz(ts1), Timestamp(ts2)) => {
            check_tz("timestamp", "timestamptz")?;
            -adt_timestamp::timestamp_cmp_timestamptz_internal(*ts2, *ts1)
        }
        (TimestampTz(ts1), TimestampTz(ts2)) => timestamp_cmp(*ts1, *ts2),
        (TimestampTz(_), Time(_) | TimeTz(_)) => return Ok(None),
    };

    Ok(Some(cmp))
}

fn date_cmp(d1: DateADT, d2: DateADT) -> i32 {
    match d1.cmp(&d2) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

fn timestamp_cmp(t1: Timestamp, t2: Timestamp) -> i32 {
    match t1.cmp(&t2) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// C: JsonItemFromDatum — the SQL/JSON PASSING value coercion.
fn json_item_from_datum<'mcx>(
    mcx: Mcx<'mcx>,
    val: Datum,
    typid: Oid,
    typmod: i32,
) -> PgResult<JbV<'mcx>> {
    match typid {
        BOOLOID => Ok(JbV::Bool(val.as_bool())),
        NUMERICOID => {
            // SAFETY: a NUMERICOID datum is a live numeric varlena.
            let p = val.as_usize() as *const u8;
            let image =
                unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
            Ok(JbV::Numeric(numeric_image_4b(mcx, image)?))
        }
        INT2OID => {
            let img = adt_numeric::int64_to_numeric(val.as_i16() as i64);
            Ok(JbV::Numeric(leak_numeric(mcx, &img)?))
        }
        INT4OID => {
            let img = adt_numeric::int4_numeric(val.as_i32());
            Ok(JbV::Numeric(leak_numeric(mcx, &img)?))
        }
        INT8OID => {
            let img = adt_numeric::int8_numeric(val.as_i64());
            Ok(JbV::Numeric(leak_numeric(mcx, &img)?))
        }
        FLOAT4OID => {
            let img = adt_numeric::float4_numeric(val.as_f32())?;
            Ok(JbV::Numeric(leak_numeric(mcx, &img)?))
        }
        FLOAT8OID => {
            let img = adt_numeric::float8_numeric(val.as_f64())?;
            Ok(JbV::Numeric(leak_numeric(mcx, &img)?))
        }
        TEXTOID | VARCHAROID => {
            // SAFETY: a text datum is a live varlena.
            let p = val.as_usize() as *const u8;
            let image =
                unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
            let data = if image[0] & 0x01 == 0x01 {
                &image[1..]
            } else {
                &image[4..]
            };
            Ok(JbV::String(mcx::slice_in(mcx, data)?.leak()))
        }
        DATEOID => Ok(JbV::Datetime {
            value: ParsedDatetime::Date(val.as_i32()),
            typmod,
            tz: 0,
        }),
        TIMEOID => Ok(JbV::Datetime {
            value: ParsedDatetime::Time(val.as_i64()),
            typmod,
            tz: 0,
        }),
        TIMETZOID => {
            // SAFETY: a TIMETZOID datum is a live pointer to TimeTzADT.
            let t = unsafe { *(val.as_usize() as *const TimeTzADT) };
            Ok(JbV::Datetime {
                value: ParsedDatetime::TimeTz(t),
                typmod,
                tz: 0,
            })
        }
        TIMESTAMPOID => Ok(JbV::Datetime {
            value: ParsedDatetime::Timestamp(val.as_i64()),
            typmod,
            tz: 0,
        }),
        TIMESTAMPTZOID => Ok(JbV::Datetime {
            value: ParsedDatetime::TimestampTz(val.as_i64()),
            typmod,
            tz: 0,
        }),
        JSONBOID => {
            // SAFETY: a JSONBOID datum is a live jsonb varlena (untoasted).
            let p = val.as_usize() as *const u8;
            let image =
                unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
            let payload: &'mcx [u8] = if image[0] & 0x01 == 0x01 {
                mcx::slice_in(mcx, &image[1..])?.leak()
            } else {
                mcx::slice_in(mcx, &image[4..])?.leak()
            };
            match adt_jsonb::io::extract_scalar(payload) {
                Some(item) => Ok(jbv_from_item(item)),
                None => Ok(JbV::Binary(payload)),
            }
        }
        JSONOID => {
            // SAFETY: a JSONOID datum is a live text varlena.
            let p = val.as_usize() as *const u8;
            let image =
                unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
            let data = if image[0] & 0x01 == 0x01 {
                &image[1..]
            } else {
                &image[4..]
            };
            let jb = adt_jsonb::io::jsonb_in(mcx, data, None)?
                .expect("hard errsave without escontext returns Err");
            let payload: &'mcx [u8] = &jb.leak()[4..];
            match adt_jsonb::io::extract_scalar(payload) {
                Some(item) => Ok(jbv_from_item(item)),
                None => Ok(JbV::Binary(payload)),
            }
        }
        _ => Err(Box::new(
            PgError::error(format!(
                "could not convert value of type {} to jsonpath",
                format_type::format_type_be(typid)?
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        )),
    }
}

/// C: executeJsonPath. `path_image` is the full jsonpath varlena image,
/// `jb_payload` the jsonb container payload.
pub fn execute_json_path<'a, 'x, 'mcx: 'a>(
    mcx: Mcx<'mcx>,
    path_image: &'a [u8],
    vars: &'x JsonPathVars<'a, 'x>,
    jb_payload: &'a [u8],
    throw_errors: bool,
    result: Option<&mut JsonValueList<'a, 'mcx>>,
    use_tz: bool,
) -> PgResult<Jper> {
    let jsp = jsp_init(path_image);
    let header = u32::from_ne_bytes(path_image[4..8].try_into().unwrap());
    let lax_mode = header & JSONPATH_LAX != 0;

    let jbv = match adt_jsonb::io::extract_scalar(jb_payload) {
        Some(item) => jbv_from_item(item),
        None => JbV::Binary(jb_payload),
    };

    let mut cxt = ExecCtx {
        mcx,
        vars,
        root: jbv,
        current: jbv,
        base_object: BaseObject { jbc: None, id: 0 },
        last_generated_object_id: 0,
        innermost_array_size: -1,
        lax_mode,
        ignore_structural_errors: lax_mode,
        throw_errors,
        use_tz,
    };
    cxt.last_generated_object_id = 1 + cxt.count_vars()?;

    if cxt.strict_absence_of_errors() && result.is_none() {
        // Strict mode needs the complete list to prove absence of errors.
        let mut vals = JsonValueList::new(mcx)?;
        let res = cxt.execute_item(&jsp, &jbv, Some(&mut vals))?;
        if res == Jper::Error {
            return Ok(res);
        }
        return Ok(if vals.is_empty() {
            Jper::NotFound
        } else {
            Jper::Ok
        });
    }

    let res = cxt.execute_item(&jsp, &jbv, result)?;
    debug_assert!(!throw_errors || res != Jper::Error);
    Ok(res)
}

/// C: jsonb_path_exists_internal core. Ok(None) = suppressed error (SQL NULL).
pub fn jsonb_path_exists_core<'mcx>(
    mcx: Mcx<'mcx>,
    jb_payload: &[u8],
    path_image: &[u8],
    vars: JsonPathVars<'_, '_>,
    silent: bool,
    tz: bool,
) -> PgResult<Option<bool>> {
    let res = execute_json_path(mcx, path_image, &vars, jb_payload, !silent, None, tz)?;
    Ok(match res {
        Jper::Error => None,
        r => Some(r == Jper::Ok),
    })
}

/// C: jsonb_path_match_internal core. Ok(None) = SQL NULL.
pub fn jsonb_path_match_core<'mcx>(
    mcx: Mcx<'mcx>,
    jb_payload: &[u8],
    path_image: &[u8],
    vars: JsonPathVars<'_, '_>,
    silent: bool,
    tz: bool,
) -> PgResult<Option<bool>> {
    let mut found = JsonValueList::new(mcx)?;
    execute_json_path(
        mcx,
        path_image,
        &vars,
        jb_payload,
        !silent,
        Some(&mut found),
        tz,
    )?;

    if found.len() == 1 {
        match found.head().expect("len 1") {
            JbV::Bool(b) => return Ok(Some(*b)),
            JbV::Null => return Ok(None),
            _ => {}
        }
    }

    if !silent {
        return Err(Box::new(
            PgError::error("single boolean result is expected")
                .with_sqlstate(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED),
        ));
    }

    Ok(None)
}

/// C: jsonb_path_query_internal collection pass — the SRF row set as
/// serialized jsonb images.
pub fn jsonb_path_query_core(
    mcx: Mcx<'_>,
    jb_payload: &[u8],
    path_image: &[u8],
    vars: JsonPathVars<'_, '_>,
    silent: bool,
    tz: bool,
) -> PgResult<alloc::vec::Vec<alloc::vec::Vec<u8>>> {
    let mut found = JsonValueList::new(mcx)?;
    execute_json_path(
        mcx,
        path_image,
        &vars,
        jb_payload,
        !silent,
        Some(&mut found),
        tz,
    )?;
    let mut rows = alloc::vec::Vec::with_capacity(found.len());
    for v in found.as_slice() {
        rows.push(jbv_to_jsonb_image(mcx, v)?[..].to_vec());
    }
    Ok(rows)
}

/// C: jsonb_path_query_array_internal core — full jsonb image.
pub fn jsonb_path_query_array_core<'mcx>(
    mcx: Mcx<'mcx>,
    jb_payload: &[u8],
    path_image: &[u8],
    vars: JsonPathVars<'_, '_>,
    silent: bool,
    tz: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut found = JsonValueList::new(mcx)?;
    execute_json_path(
        mcx,
        path_image,
        &vars,
        jb_payload,
        !silent,
        Some(&mut found),
        tz,
    )?;
    wrap_items_in_array_image(mcx, &found)
}

/// C: jsonb_path_query_first_internal core. Ok(None) = SQL NULL.
pub fn jsonb_path_query_first_core<'mcx>(
    mcx: Mcx<'mcx>,
    jb_payload: &[u8],
    path_image: &[u8],
    vars: JsonPathVars<'_, '_>,
    silent: bool,
    tz: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let mut found = JsonValueList::new(mcx)?;
    execute_json_path(
        mcx,
        path_image,
        &vars,
        jb_payload,
        !silent,
        Some(&mut found),
        tz,
    )?;
    match found.head() {
        Some(v) => Ok(Some(jbv_to_jsonb_image(mcx, v)?)),
        None => Ok(None),
    }
}

/// C: JsonWrapper (primnodes.h).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonWrapper {
    Unspec,
    None,
    Conditional,
    Unconditional,
}

/// C: JsonPathExists — executor-callable JSON_EXISTS. `error` reporting via
/// Ok(None) when soft.
pub fn json_path_exists<'mcx>(
    mcx: Mcx<'mcx>,
    jb_payload: &[u8],
    path_image: &[u8],
    soft_error: bool,
    vars: &[JsonPathVariable<'_>],
) -> PgResult<Option<bool>> {
    let res = execute_json_path(
        mcx,
        path_image,
        &JsonPathVars::List(vars),
        jb_payload,
        !soft_error,
        None,
        true,
    )?;
    Ok(match res {
        Jper::Error => None,
        r => Some(r == Jper::Ok),
    })
}

pub enum JsonPathQueryResult<'mcx> {
    /// A serialized jsonb image.
    Image(PgVec<'mcx, u8>),
    Empty,
    Error,
}

/// C: JsonPathQuery — executor-callable JSON_QUERY.
pub fn json_path_query<'mcx>(
    mcx: Mcx<'mcx>,
    jb_payload: &[u8],
    path_image: &[u8],
    wrapper: JsonWrapper,
    soft_error: bool,
    vars: &[JsonPathVariable<'_>],
    column_name: Option<&str>,
) -> PgResult<JsonPathQueryResult<'mcx>> {
    let mut found = JsonValueList::new(mcx)?;
    let res = execute_json_path(
        mcx,
        path_image,
        &JsonPathVars::List(vars),
        jb_payload,
        !soft_error,
        Some(&mut found),
        true,
    )?;
    if res == Jper::Error {
        return Ok(JsonPathQueryResult::Error);
    }

    let count = found.len();
    let wrap = match wrapper {
        _ if count == 0 => false,
        JsonWrapper::None | JsonWrapper::Unspec => false,
        JsonWrapper::Unconditional => true,
        JsonWrapper::Conditional => count > 1,
    };

    if wrap {
        return Ok(JsonPathQueryResult::Image(wrap_items_in_array_image(
            mcx, &found,
        )?));
    }

    if count > 1 {
        if soft_error {
            return Ok(JsonPathQueryResult::Error);
        }
        let msg = match column_name {
            Some(col) => format!(
                "JSON path expression for column \"{col}\" must return single item when no wrapper is requested"
            ),
            None => "JSON path expression in JSON_QUERY must return single item when no wrapper is requested"
                .to_string(),
        };
        return Err(Box::new(
            PgError::error(msg)
                .with_sqlstate(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM)
                .with_hint("Use the WITH WRAPPER clause to wrap SQL/JSON items into an array."),
        ));
    }

    match found.head() {
        Some(v) => Ok(JsonPathQueryResult::Image(jbv_to_jsonb_image(mcx, v)?)),
        None => Ok(JsonPathQueryResult::Empty),
    }
}

pub enum JsonPathValueResult<'a> {
    Value(JbV<'a>),
    Null,
    Empty,
    Error,
}

/// C: JsonPathValue — executor-callable JSON_VALUE (scalar result).
pub fn json_path_value<'a, 'mcx: 'a>(
    mcx: Mcx<'mcx>,
    jb_payload: &'a [u8],
    path_image: &'a [u8],
    soft_error: bool,
    vars: &[JsonPathVariable<'a>],
    column_name: Option<&str>,
) -> PgResult<JsonPathValueResult<'a>> {
    let mut found = JsonValueList::new(mcx)?;
    let res = execute_json_path(
        mcx,
        path_image,
        &JsonPathVars::List(vars),
        jb_payload,
        !soft_error,
        Some(&mut found),
        true,
    )?;
    if res == Jper::Error {
        return Ok(JsonPathValueResult::Error);
    }

    let count = found.len();
    if count == 0 {
        return Ok(JsonPathValueResult::Empty);
    }

    if count > 1 {
        if soft_error {
            return Ok(JsonPathValueResult::Error);
        }
        let msg = match column_name {
            Some(col) => format!(
                "JSON path expression for column \"{col}\" must return single scalar item"
            ),
            None => "JSON path expression in JSON_VALUE must return single scalar item".to_string(),
        };
        return Err(Box::new(
            PgError::error(msg).with_sqlstate(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM),
        ));
    }

    let mut res_v = *found.head().expect("len 1");
    if let JbV::Binary(c) = res_v {
        if container_is_scalar(c) {
            res_v = jbv_from_item(
                adt_jsonb::io::extract_scalar(c).expect("raw-scalar container"),
            );
        }
    }

    if matches!(res_v, JbV::Binary(_)) {
        if soft_error {
            return Ok(JsonPathValueResult::Error);
        }
        let msg = match column_name {
            Some(col) => format!(
                "JSON path expression for column \"{col}\" must return single scalar item"
            ),
            None => "JSON path expression in JSON_VALUE must return single scalar item".to_string(),
        };
        return Err(Box::new(
            PgError::error(msg).with_sqlstate(ERRCODE_SQL_JSON_SCALAR_REQUIRED),
        ));
    }

    if matches!(res_v, JbV::Null) {
        return Ok(JsonPathValueResult::Null);
    }

    Ok(JsonPathValueResult::Value(res_v))
}
