//! On-disk JsonPath varlena: flatten (parse tree -> binary), the JsonPathItem
//! reader, the canonical text printer, and the in/out/recv/send cores.

use ::mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, PgVec};
use ::stack_depth::check_stack_depth;
use ::stringinfo::StringInfo;
use ::types_error::{ereturn, PgError, PgResult, SoftErrorContext};
use ::types_error::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_SYNTAX_ERROR};

use crate::gram::{parsejsonpath, ParseItem, ParseValue};


/// On-disk item-type bytes (jsonpath.h JsonPathItemType); pg_upgrade freezes
/// the order, first four share jbvType discriminants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ItemType {
    Null = 0,
    String = 1,
    Numeric = 2,
    Bool = 3,
    And = 4,
    Or = 5,
    Not = 6,
    IsUnknown = 7,
    Equal = 8,
    NotEqual = 9,
    Less = 10,
    Greater = 11,
    LessOrEqual = 12,
    GreaterOrEqual = 13,
    Add = 14,
    Sub = 15,
    Mul = 16,
    Div = 17,
    Mod = 18,
    Plus = 19,
    Minus = 20,
    AnyArray = 21,
    AnyKey = 22,
    IndexArray = 23,
    Any = 24,
    Key = 25,
    Current = 26,
    Root = 27,
    Variable = 28,
    Filter = 29,
    Exists = 30,
    Type = 31,
    Size = 32,
    Abs = 33,
    Floor = 34,
    Ceiling = 35,
    Double = 36,
    Datetime = 37,
    KeyValue = 38,
    Subscript = 39,
    Last = 40,
    StartsWith = 41,
    LikeRegex = 42,
    Bigint = 43,
    Boolean = 44,
    Date = 45,
    Decimal = 46,
    Integer = 47,
    Number = 48,
    StringFunc = 49,
    Time = 50,
    TimeTz = 51,
    Timestamp = 52,
    TimestampTz = 53,
}

const ALL_ITEM_TYPES: [ItemType; 54] = [
    ItemType::Null,
    ItemType::String,
    ItemType::Numeric,
    ItemType::Bool,
    ItemType::And,
    ItemType::Or,
    ItemType::Not,
    ItemType::IsUnknown,
    ItemType::Equal,
    ItemType::NotEqual,
    ItemType::Less,
    ItemType::Greater,
    ItemType::LessOrEqual,
    ItemType::GreaterOrEqual,
    ItemType::Add,
    ItemType::Sub,
    ItemType::Mul,
    ItemType::Div,
    ItemType::Mod,
    ItemType::Plus,
    ItemType::Minus,
    ItemType::AnyArray,
    ItemType::AnyKey,
    ItemType::IndexArray,
    ItemType::Any,
    ItemType::Key,
    ItemType::Current,
    ItemType::Root,
    ItemType::Variable,
    ItemType::Filter,
    ItemType::Exists,
    ItemType::Type,
    ItemType::Size,
    ItemType::Abs,
    ItemType::Floor,
    ItemType::Ceiling,
    ItemType::Double,
    ItemType::Datetime,
    ItemType::KeyValue,
    ItemType::Subscript,
    ItemType::Last,
    ItemType::StartsWith,
    ItemType::LikeRegex,
    ItemType::Bigint,
    ItemType::Boolean,
    ItemType::Date,
    ItemType::Decimal,
    ItemType::Integer,
    ItemType::Number,
    ItemType::StringFunc,
    ItemType::Time,
    ItemType::TimeTz,
    ItemType::Timestamp,
    ItemType::TimestampTz,
];

pub const JSONPATH_VERSION: u32 = 0x01;
pub const JSONPATH_LAX: u32 = 0x8000_0000;
/// offsetof(JsonPath, data): varlena header + version word.
pub const JSONPATH_HDRSZ: usize = 8;
const VARHDRSZ: usize = 4;

#[inline]
fn intalign(len: usize) -> usize {
    (len + 3) & !3usize
}

fn buf_zeros(buf: &mut PgVec<'_, u8>, count: usize) -> PgResult<()> {
    static ZEROS: [u8; 8] = [0; 8];
    let mut left = count;
    while left > 0 {
        let n = left.min(ZEROS.len());
        vec_append_bytes(buf, &ZEROS[..n])?;
        left -= n;
    }
    Ok(())
}

#[inline]
fn patch_i32(buf: &mut PgVec<'_, u8>, at: usize, value: i32) {
    buf[at..at + 4].copy_from_slice(&value.to_ne_bytes());
}

fn align_int(buf: &mut PgVec<'_, u8>) -> PgResult<()> {
    let pad = intalign(buf.len()) - buf.len();
    buf_zeros(buf, pad)
}

fn reserve_item_pointer(buf: &mut PgVec<'_, u8>) -> PgResult<usize> {
    let pos = buf.len();
    vec_append_bytes(buf, &0i32.to_ne_bytes())?;
    Ok(pos)
}

#[track_caller]
#[cold]
#[inline(never)]
fn unrecognized_item_type(typ: ItemType) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "unrecognized jsonpath item type: {}",
        typ as i32
    )))
}

/// C: flattenJsonPathParseItem. Ok(Some(pos)) on success; Ok(None) after a
/// soft error was recorded in `escontext`.
fn flatten_item(
    buf: &mut PgVec<'_, u8>,
    escontext: &mut Option<&mut SoftErrorContext>,
    item: &ParseItem<'_>,
    nesting_level: i32,
    inside_array_subscript: bool,
) -> PgResult<Option<i32>> {
    check_stack_depth()?;

    let pos = buf.len() as i32 - JSONPATH_HDRSZ as i32;
    let mut chld: i32 = 0;
    let mut arg_nesting_level = 0i32;

    vec_append_bytes(buf, &[item.typ as u8])?;
    align_int(buf)?;
    let next = reserve_item_pointer(buf)?;

    match item.typ {
        ItemType::String | ItemType::Variable | ItemType::Key => {
            let ParseValue::String(s) = item.value else {
                unreachable!("String/Variable/Key carries String value");
            };
            vec_append_bytes(buf, &(s.len() as i32).to_ne_bytes())?;
            vec_append_bytes(buf, s)?;
            vec_append_bytes(buf, &[0])?;
        }
        ItemType::Numeric => {
            let ParseValue::Numeric(num) = item.value else {
                unreachable!("Numeric carries Numeric value");
            };
            vec_append_bytes(buf, num)?;
        }
        ItemType::Bool => {
            let ParseValue::Boolean(b) = item.value else {
                unreachable!("Bool carries Boolean value");
            };
            vec_append_bytes(buf, &[b as u8])?;
        }
        ItemType::And
        | ItemType::Or
        | ItemType::Equal
        | ItemType::NotEqual
        | ItemType::Less
        | ItemType::Greater
        | ItemType::LessOrEqual
        | ItemType::GreaterOrEqual
        | ItemType::Add
        | ItemType::Sub
        | ItemType::Mul
        | ItemType::Div
        | ItemType::Mod
        | ItemType::StartsWith
        | ItemType::Decimal => {
            let (left_item, right_item) = match item.value {
                ParseValue::Args { left, right } => (left, right),
                _ => unreachable!("binary op carries Args value"),
            };

            let left = reserve_item_pointer(buf)?;
            let right = reserve_item_pointer(buf)?;

            match left_item {
                None => chld = pos,
                Some(left_item) => {
                    match flatten_item(
                        buf,
                        escontext,
                        left_item,
                        nesting_level + arg_nesting_level,
                        inside_array_subscript,
                    )? {
                        Some(p) => chld = p,
                        None => return Ok(None),
                    }
                }
            }
            patch_i32(buf, left, chld - pos);

            match right_item {
                None => chld = pos,
                Some(right_item) => {
                    match flatten_item(
                        buf,
                        escontext,
                        right_item,
                        nesting_level + arg_nesting_level,
                        inside_array_subscript,
                    )? {
                        Some(p) => chld = p,
                        None => return Ok(None),
                    }
                }
            }
            patch_i32(buf, right, chld - pos);
        }
        ItemType::LikeRegex => {
            let (expr, pattern, flags) = match item.value {
                ParseValue::LikeRegex { expr, pattern, flags } => (expr, pattern, flags),
                _ => unreachable!("LikeRegex carries LikeRegex value"),
            };

            vec_append_bytes(buf, &flags.to_ne_bytes())?;
            let offs = reserve_item_pointer(buf)?;
            vec_append_bytes(buf, &(pattern.len() as i32).to_ne_bytes())?;
            vec_append_bytes(buf, pattern)?;
            vec_append_bytes(buf, &[0])?;

            let expr = expr.expect("LikeRegex expr is non-null");
            match flatten_item(buf, escontext, expr, nesting_level, inside_array_subscript)? {
                Some(p) => chld = p,
                None => return Ok(None),
            }
            patch_i32(buf, offs, chld - pos);
        }
        ItemType::Filter
        | ItemType::IsUnknown
        | ItemType::Not
        | ItemType::Plus
        | ItemType::Minus
        | ItemType::Exists
        | ItemType::Datetime
        | ItemType::Time
        | ItemType::TimeTz
        | ItemType::Timestamp
        | ItemType::TimestampTz => {
            if item.typ == ItemType::Filter {
                arg_nesting_level += 1;
            }

            let arg_item = match item.value {
                ParseValue::Arg(a) => a,
                ParseValue::None => None,
                _ => unreachable!("unary op carries Arg value"),
            };

            let arg = reserve_item_pointer(buf)?;

            match arg_item {
                None => chld = pos,
                Some(arg_item) => {
                    match flatten_item(
                        buf,
                        escontext,
                        arg_item,
                        nesting_level + arg_nesting_level,
                        inside_array_subscript,
                    )? {
                        Some(p) => chld = p,
                        None => return Ok(None),
                    }
                }
            }
            patch_i32(buf, arg, chld - pos);
        }
        ItemType::Null | ItemType::Root | ItemType::AnyArray | ItemType::AnyKey => {}
        ItemType::Current => {
            if nesting_level <= 0 {
                return ereturn(
                    escontext.as_deref_mut(),
                    None,
                    PgError::error("@ is not allowed in root expressions")
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                );
            }
        }
        ItemType::Last => {
            if !inside_array_subscript {
                return ereturn(
                    escontext.as_deref_mut(),
                    None,
                    PgError::error("LAST is allowed only in array subscripts")
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                );
            }
        }
        ItemType::IndexArray => {
            let elems = match item.value {
                ParseValue::Array(elems) => elems,
                _ => unreachable!("IndexArray carries Array value"),
            };

            vec_append_bytes(buf, &(elems.len() as i32).to_ne_bytes())?;
            let offset = buf.len();
            buf_zeros(buf, 4 * 2 * elems.len())?;

            for (i, elem) in elems.iter().enumerate() {
                let from = elem.from.expect("subscript from is non-null");
                let frompos =
                    match flatten_item(buf, escontext, from, nesting_level, true)? {
                        Some(p) => p - pos,
                        None => return Ok(None),
                    };

                let topos = if let Some(to) = elem.to {
                    match flatten_item(buf, escontext, to, nesting_level, true)? {
                        Some(p) => p - pos,
                        None => return Ok(None),
                    }
                } else {
                    0
                };

                let ppos = offset + i * 2 * 4;
                patch_i32(buf, ppos, frompos);
                patch_i32(buf, ppos + 4, topos);
            }
        }
        ItemType::Any => {
            let (first, last) = match item.value {
                ParseValue::AnyBounds { first, last } => (first, last),
                _ => unreachable!("Any carries AnyBounds value"),
            };
            vec_append_bytes(buf, &first.to_ne_bytes())?;
            vec_append_bytes(buf, &last.to_ne_bytes())?;
        }
        ItemType::Type
        | ItemType::Size
        | ItemType::Abs
        | ItemType::Floor
        | ItemType::Ceiling
        | ItemType::Double
        | ItemType::KeyValue
        | ItemType::Bigint
        | ItemType::Boolean
        | ItemType::Date
        | ItemType::Integer
        | ItemType::Number
        | ItemType::StringFunc => {}
        ItemType::Subscript => {
            return Err(unrecognized_item_type(item.typ));
        }
    }

    if let Some(next_item) = item.next.get() {
        match flatten_item(
            buf,
            escontext,
            next_item,
            nesting_level,
            inside_array_subscript,
        )? {
            Some(p) => chld = p - pos,
            None => return Ok(None),
        }
        patch_i32(buf, next, chld);
    }

    Ok(Some(pos))
}

#[inline]
fn set_varsize(data: &mut [u8], len: usize) {
    let word: u32 = if cfg!(target_endian = "big") {
        (len as u32) & 0x3FFF_FFFF
    } else {
        (len as u32) << 2
    };
    data[..VARHDRSZ].copy_from_slice(&word.to_ne_bytes());
}

#[cold]
fn invalid_input_syntax(
    escontext: Option<&mut SoftErrorContext>,
    input: &[u8],
) -> PgResult<()> {
    let in_str = String::from_utf8_lossy(input);
    ereturn(
        escontext,
        (),
        PgError::error(format!("invalid input syntax for type jsonpath: \"{in_str}\""))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

/// C: jsonPathFromCstring — parse + flatten into a full on-disk jsonpath
/// varlena image. Ok(None) after a soft error was recorded.
pub fn json_path_from_cstring<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let parsed = parsejsonpath(mcx, input, escontext.as_deref_mut())?;

    if escontext.as_ref().is_some_and(|c| c.error_occurred()) {
        return Ok(None);
    }

    let Some(parsed) = parsed else {
        return invalid_input_syntax(escontext.as_deref_mut(), input).map(|()| None);
    };

    let mut buf: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, 4 * input.len() + JSONPATH_HDRSZ)?;
    buf_zeros(&mut buf, JSONPATH_HDRSZ)?;

    let mut esc = escontext;
    if flatten_item(&mut buf, &mut esc, parsed.expr, 0, false)?.is_none() {
        return Ok(None);
    }

    let total_len = buf.len();
    let mut header = JSONPATH_VERSION;
    if parsed.lax {
        header |= JSONPATH_LAX;
    }
    set_varsize(&mut buf, total_len);
    buf[4..8].copy_from_slice(&header.to_ne_bytes());

    Ok(Some(buf))
}

// Reader over the flattened form (C: JsonPathItem / jspInitByBuffer).

#[inline]
fn jsonpath_header(image: &[u8]) -> u32 {
    u32::from_ne_bytes([image[4], image[5], image[6], image[7]])
}

#[inline]
fn jsonpath_data(image: &[u8]) -> &[u8] {
    &image[JSONPATH_HDRSZ..]
}

#[derive(Clone)]
pub struct JsonPathItem<'a> {
    pub typ: ItemType,
    pub next_pos: i32,
    /// The flattened node region (js->data); node offsets index into it.
    pub buffer: &'a [u8],
    /// This node's absolute offset within `buffer` (C: base - js->data).
    pub base: i32,
    pub content: Content,
}

#[derive(Clone, Copy, Default)]
pub struct Content {
    pub args: ContentArgs,
    pub arg: i32,
    pub array: ContentArray,
    pub anybounds: ContentAnyBounds,
    pub value: ContentValue,
    pub like_regex: ContentLikeRegex,
}

#[derive(Clone, Copy, Default)]
pub struct ContentArgs {
    pub left: i32,
    pub right: i32,
}

#[derive(Clone, Copy, Default)]
pub struct ContentArray {
    pub nelems: i32,
    pub elems_pos: i32,
}

#[derive(Clone, Copy, Default)]
pub struct ContentAnyBounds {
    pub first: u32,
    pub last: u32,
}

#[derive(Clone, Copy, Default)]
pub struct ContentValue {
    pub data_pos: i32,
    pub datalen: i32,
}

#[derive(Clone, Copy, Default)]
pub struct ContentLikeRegex {
    pub expr: i32,
    pub pattern_pos: i32,
    pub patternlen: i32,
    pub flags: u32,
}

#[inline]
fn read_int32(base: &[u8], pos: &mut usize) -> u32 {
    let v = u32::from_ne_bytes([base[*pos], base[*pos + 1], base[*pos + 2], base[*pos + 3]]);
    *pos += 4;
    v
}

pub fn jsp_init(image: &[u8]) -> JsonPathItem<'_> {
    debug_assert_eq!(jsonpath_header(image) & !JSONPATH_LAX, JSONPATH_VERSION);
    jsp_init_by_buffer(jsonpath_data(image), 0)
}

pub fn jsp_init_by_buffer(base: &[u8], pos: i32) -> JsonPathItem<'_> {
    let node_base = pos;
    let mut p = pos as usize;

    let typ_byte = base[p];
    p += 1;
    // The data region starts int-aligned, so C's pointer INTALIGN reduces to
    // offset INTALIGN.
    p = intalign(p);
    let next_pos = read_int32(base, &mut p) as i32;

    debug_assert!(typ_byte <= ItemType::TimestampTz as u8);
    let typ = ALL_ITEM_TYPES[typ_byte as usize];
    let mut content = Content::default();

    match typ {
        ItemType::Null
        | ItemType::Root
        | ItemType::Current
        | ItemType::AnyArray
        | ItemType::AnyKey
        | ItemType::Type
        | ItemType::Size
        | ItemType::Abs
        | ItemType::Floor
        | ItemType::Ceiling
        | ItemType::Double
        | ItemType::KeyValue
        | ItemType::Last
        | ItemType::Bigint
        | ItemType::Boolean
        | ItemType::Date
        | ItemType::Integer
        | ItemType::Number
        | ItemType::StringFunc => {}
        ItemType::String | ItemType::Key | ItemType::Variable => {
            content.value.datalen = read_int32(base, &mut p) as i32;
            content.value.data_pos = p as i32;
        }
        ItemType::Numeric | ItemType::Bool => {
            content.value.data_pos = p as i32;
        }
        ItemType::And
        | ItemType::Or
        | ItemType::Equal
        | ItemType::NotEqual
        | ItemType::Less
        | ItemType::Greater
        | ItemType::LessOrEqual
        | ItemType::GreaterOrEqual
        | ItemType::Add
        | ItemType::Sub
        | ItemType::Mul
        | ItemType::Div
        | ItemType::Mod
        | ItemType::StartsWith
        | ItemType::Decimal => {
            content.args.left = read_int32(base, &mut p) as i32;
            content.args.right = read_int32(base, &mut p) as i32;
        }
        ItemType::Not
        | ItemType::IsUnknown
        | ItemType::Exists
        | ItemType::Plus
        | ItemType::Minus
        | ItemType::Filter
        | ItemType::Datetime
        | ItemType::Time
        | ItemType::TimeTz
        | ItemType::Timestamp
        | ItemType::TimestampTz => {
            content.arg = read_int32(base, &mut p) as i32;
        }
        ItemType::IndexArray => {
            content.array.nelems = read_int32(base, &mut p) as i32;
            content.array.elems_pos = p as i32;
        }
        ItemType::Any => {
            content.anybounds.first = read_int32(base, &mut p);
            content.anybounds.last = read_int32(base, &mut p);
        }
        ItemType::LikeRegex => {
            content.like_regex.flags = read_int32(base, &mut p);
            content.like_regex.expr = read_int32(base, &mut p) as i32;
            content.like_regex.patternlen = read_int32(base, &mut p) as i32;
            content.like_regex.pattern_pos = p as i32;
        }
        ItemType::Subscript => {
            panic!("unrecognized jsonpath item type: {}", typ as i32);
        }
    }

    JsonPathItem { typ, next_pos, buffer: base, base: node_base, content }
}

impl<'a> JsonPathItem<'a> {
    #[inline]
    fn child_at(&self, off: i32) -> JsonPathItem<'a> {
        jsp_init_by_buffer(self.buffer, self.base + off)
    }

    #[inline]
    pub fn has_next(&self) -> bool {
        self.next_pos > 0
    }

    pub fn next(&self) -> Option<JsonPathItem<'a>> {
        if self.has_next() {
            Some(self.child_at(self.next_pos))
        } else {
            None
        }
    }

    pub fn arg(&self) -> JsonPathItem<'a> {
        self.child_at(self.content.arg)
    }

    pub fn left_arg(&self) -> JsonPathItem<'a> {
        self.child_at(self.content.args.left)
    }

    pub fn right_arg(&self) -> JsonPathItem<'a> {
        self.child_at(self.content.args.right)
    }

    pub fn get_bool(&self) -> bool {
        debug_assert_eq!(self.typ, ItemType::Bool);
        self.buffer[self.content.value.data_pos as usize] != 0
    }

    /// Full numeric varlena bytes, bounded by its own VARSIZE.
    pub fn get_numeric(&self) -> &'a [u8] {
        debug_assert_eq!(self.typ, ItemType::Numeric);
        let start = self.content.value.data_pos as usize;
        let raw = u32::from_ne_bytes([
            self.buffer[start],
            self.buffer[start + 1],
            self.buffer[start + 2],
            self.buffer[start + 3],
        ]);
        let vl = if cfg!(target_endian = "big") {
            (raw & 0x3FFF_FFFF) as usize
        } else {
            (raw >> 2) as usize
        };
        &self.buffer[start..start + vl]
    }

    pub fn get_string(&self) -> &'a [u8] {
        debug_assert!(matches!(
            self.typ,
            ItemType::Key | ItemType::String | ItemType::Variable
        ));
        let start = self.content.value.data_pos as usize;
        let len = self.content.value.datalen as usize;
        &self.buffer[start..start + len]
    }

    fn like_regex_pattern(&self) -> &'a [u8] {
        let start = self.content.like_regex.pattern_pos as usize;
        let len = self.content.like_regex.patternlen as usize;
        &self.buffer[start..start + len]
    }

    pub fn array_subscript(&self, i: i32) -> (JsonPathItem<'a>, Option<JsonPathItem<'a>>) {
        debug_assert_eq!(self.typ, ItemType::IndexArray);
        let pair = self.content.array.elems_pos as usize + (i as usize) * 2 * 4;
        let from_off = i32::from_ne_bytes([
            self.buffer[pair],
            self.buffer[pair + 1],
            self.buffer[pair + 2],
            self.buffer[pair + 3],
        ]);
        let to_off = i32::from_ne_bytes([
            self.buffer[pair + 4],
            self.buffer[pair + 5],
            self.buffer[pair + 6],
            self.buffer[pair + 7],
        ]);
        let from = self.child_at(from_off);
        if to_off == 0 {
            (from, None)
        } else {
            (from, Some(self.child_at(to_off)))
        }
    }
}

// Printer (C: jsonPathToCstring / printJsonPathItem).

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

fn operation_priority(op: ItemType) -> i32 {
    match op {
        ItemType::Or => 0,
        ItemType::And => 1,
        ItemType::Equal
        | ItemType::NotEqual
        | ItemType::Less
        | ItemType::Greater
        | ItemType::LessOrEqual
        | ItemType::GreaterOrEqual
        | ItemType::StartsWith => 2,
        ItemType::Add | ItemType::Sub => 3,
        ItemType::Mul | ItemType::Div | ItemType::Mod => 4,
        ItemType::Plus | ItemType::Minus => 5,
        _ => 6,
    }
}

fn append_u32(buf: &mut StringInfo<'_>, v: u32) -> PgResult<()> {
    let mut tmp = [0u8; 10];
    let n = numutils::pg_ultoa_n(v, &mut tmp);
    buf.append_bytes(&tmp[..n])
}

fn print_numeric(buf: &mut StringInfo<'_>, image: &[u8]) -> PgResult<()> {
    let mut scratch: Vec<u8> = Vec::new();
    adt_numeric::numeric_out_into(adt_numeric::Num::from_payload(&image[4..]), &mut scratch);
    buf.append_bytes(&scratch)
}

fn print_item(
    buf: &mut StringInfo<'_>,
    v: &JsonPathItem<'_>,
    in_key: bool,
    print_brackets: bool,
) -> PgResult<()> {
    check_stack_depth()?;

    match v.typ {
        ItemType::Null => buf.append_bytes(b"null")?,
        ItemType::String => adt_json::escape_json(buf, v.get_string())?,
        ItemType::Numeric => {
            if v.has_next() {
                buf.append_byte(b'(')?;
            }
            print_numeric(buf, v.get_numeric())?;
            if v.has_next() {
                buf.append_byte(b')')?;
            }
        }
        ItemType::Bool => {
            if v.get_bool() {
                buf.append_bytes(b"true")?;
            } else {
                buf.append_bytes(b"false")?;
            }
        }
        ItemType::And
        | ItemType::Or
        | ItemType::Equal
        | ItemType::NotEqual
        | ItemType::Less
        | ItemType::Greater
        | ItemType::LessOrEqual
        | ItemType::GreaterOrEqual
        | ItemType::Add
        | ItemType::Sub
        | ItemType::Mul
        | ItemType::Div
        | ItemType::Mod
        | ItemType::StartsWith => {
            if print_brackets {
                buf.append_byte(b'(')?;
            }
            let elem = v.left_arg();
            print_item(
                buf,
                &elem,
                false,
                operation_priority(elem.typ) <= operation_priority(v.typ),
            )?;
            buf.append_byte(b' ')?;
            buf.append_bytes(operation_name(v.typ).as_bytes())?;
            buf.append_byte(b' ')?;
            let elem = v.right_arg();
            print_item(
                buf,
                &elem,
                false,
                operation_priority(elem.typ) <= operation_priority(v.typ),
            )?;
            if print_brackets {
                buf.append_byte(b')')?;
            }
        }
        ItemType::Not => {
            buf.append_bytes(b"!(")?;
            print_item(buf, &v.arg(), false, false)?;
            buf.append_byte(b')')?;
        }
        ItemType::IsUnknown => {
            buf.append_byte(b'(')?;
            print_item(buf, &v.arg(), false, false)?;
            buf.append_bytes(b") is unknown")?;
        }
        ItemType::Plus | ItemType::Minus => {
            if print_brackets {
                buf.append_byte(b'(')?;
            }
            buf.append_byte(if v.typ == ItemType::Plus { b'+' } else { b'-' })?;
            let elem = v.arg();
            print_item(
                buf,
                &elem,
                false,
                operation_priority(elem.typ) <= operation_priority(v.typ),
            )?;
            if print_brackets {
                buf.append_byte(b')')?;
            }
        }
        ItemType::AnyArray => buf.append_bytes(b"[*]")?,
        ItemType::AnyKey => {
            if in_key {
                buf.append_byte(b'.')?;
            }
            buf.append_byte(b'*')?;
        }
        ItemType::IndexArray => {
            buf.append_byte(b'[')?;
            for i in 0..v.content.array.nelems {
                let (from, to) = v.array_subscript(i);
                if i != 0 {
                    buf.append_byte(b',')?;
                }
                print_item(buf, &from, false, false)?;
                if let Some(to) = to {
                    buf.append_bytes(b" to ")?;
                    print_item(buf, &to, false, false)?;
                }
            }
            buf.append_byte(b']')?;
        }
        ItemType::Any => {
            if in_key {
                buf.append_byte(b'.')?;
            }
            let first = v.content.anybounds.first;
            let last = v.content.anybounds.last;
            if first == 0 && last == u32::MAX {
                buf.append_bytes(b"**")?;
            } else if first == last {
                if first == u32::MAX {
                    buf.append_bytes(b"**{last}")?;
                } else {
                    buf.append_bytes(b"**{")?;
                    append_u32(buf, first)?;
                    buf.append_byte(b'}')?;
                }
            } else if first == u32::MAX {
                buf.append_bytes(b"**{last to ")?;
                append_u32(buf, last)?;
                buf.append_byte(b'}')?;
            } else if last == u32::MAX {
                buf.append_bytes(b"**{")?;
                append_u32(buf, first)?;
                buf.append_bytes(b" to last}")?;
            } else {
                buf.append_bytes(b"**{")?;
                append_u32(buf, first)?;
                buf.append_bytes(b" to ")?;
                append_u32(buf, last)?;
                buf.append_byte(b'}')?;
            }
        }
        ItemType::Key => {
            if in_key {
                buf.append_byte(b'.')?;
            }
            adt_json::escape_json(buf, v.get_string())?;
        }
        ItemType::Current => {
            debug_assert!(!in_key);
            buf.append_byte(b'@')?;
        }
        ItemType::Root => {
            debug_assert!(!in_key);
            buf.append_byte(b'$')?;
        }
        ItemType::Variable => {
            buf.append_byte(b'$')?;
            adt_json::escape_json(buf, v.get_string())?;
        }
        ItemType::Filter => {
            buf.append_bytes(b"?(")?;
            print_item(buf, &v.arg(), false, false)?;
            buf.append_byte(b')')?;
        }
        ItemType::Exists => {
            buf.append_bytes(b"exists (")?;
            print_item(buf, &v.arg(), false, false)?;
            buf.append_byte(b')')?;
        }
        ItemType::Type => buf.append_bytes(b".type()")?,
        ItemType::Size => buf.append_bytes(b".size()")?,
        ItemType::Abs => buf.append_bytes(b".abs()")?,
        ItemType::Floor => buf.append_bytes(b".floor()")?,
        ItemType::Ceiling => buf.append_bytes(b".ceiling()")?,
        ItemType::Double => buf.append_bytes(b".double()")?,
        ItemType::Datetime => {
            buf.append_bytes(b".datetime(")?;
            if v.content.arg != 0 {
                print_item(buf, &v.arg(), false, false)?;
            }
            buf.append_byte(b')')?;
        }
        ItemType::KeyValue => buf.append_bytes(b".keyvalue()")?,
        ItemType::Last => buf.append_bytes(b"last")?,
        ItemType::LikeRegex => {
            if print_brackets {
                buf.append_byte(b'(')?;
            }
            let elem = v.child_at(v.content.like_regex.expr);
            print_item(
                buf,
                &elem,
                false,
                operation_priority(elem.typ) <= operation_priority(v.typ),
            )?;
            buf.append_bytes(b" like_regex ")?;
            adt_json::escape_json(buf, v.like_regex_pattern())?;
            let flags = v.content.like_regex.flags;
            if flags != 0 {
                buf.append_bytes(b" flag \"")?;
                if flags & crate::gram::JSP_REGEX_ICASE != 0 {
                    buf.append_byte(b'i')?;
                }
                if flags & crate::gram::JSP_REGEX_DOTALL != 0 {
                    buf.append_byte(b's')?;
                }
                if flags & crate::gram::JSP_REGEX_MLINE != 0 {
                    buf.append_byte(b'm')?;
                }
                if flags & crate::gram::JSP_REGEX_WSPACE != 0 {
                    buf.append_byte(b'x')?;
                }
                if flags & crate::gram::JSP_REGEX_QUOTE != 0 {
                    buf.append_byte(b'q')?;
                }
                buf.append_byte(b'"')?;
            }
            if print_brackets {
                buf.append_byte(b')')?;
            }
        }
        ItemType::Bigint => buf.append_bytes(b".bigint()")?,
        ItemType::Boolean => buf.append_bytes(b".boolean()")?,
        ItemType::Date => buf.append_bytes(b".date()")?,
        ItemType::Decimal => {
            buf.append_bytes(b".decimal(")?;
            if v.content.args.left != 0 {
                print_item(buf, &v.left_arg(), false, false)?;
            }
            if v.content.args.right != 0 {
                buf.append_byte(b',')?;
                print_item(buf, &v.right_arg(), false, false)?;
            }
            buf.append_byte(b')')?;
        }
        ItemType::Integer => buf.append_bytes(b".integer()")?,
        ItemType::Number => buf.append_bytes(b".number()")?,
        ItemType::StringFunc => buf.append_bytes(b".string()")?,
        ItemType::Time => {
            buf.append_bytes(b".time(")?;
            if v.content.arg != 0 {
                print_item(buf, &v.arg(), false, false)?;
            }
            buf.append_byte(b')')?;
        }
        ItemType::TimeTz => {
            buf.append_bytes(b".time_tz(")?;
            if v.content.arg != 0 {
                print_item(buf, &v.arg(), false, false)?;
            }
            buf.append_byte(b')')?;
        }
        ItemType::Timestamp => {
            buf.append_bytes(b".timestamp(")?;
            if v.content.arg != 0 {
                print_item(buf, &v.arg(), false, false)?;
            }
            buf.append_byte(b')')?;
        }
        ItemType::TimestampTz => {
            buf.append_bytes(b".timestamp_tz(")?;
            if v.content.arg != 0 {
                print_item(buf, &v.arg(), false, false)?;
            }
            buf.append_byte(b')')?;
        }
        ItemType::Subscript => {
            return Err(unrecognized_item_type(v.typ));
        }
    }

    if let Some(elem) = v.next() {
        print_item(buf, &elem, true, true)?;
    }
    Ok(())
}

/// C: jsonPathToCstring over a full varlena image; the caller frames the
/// returned text (no trailing NUL here).
pub fn json_path_to_cstring_into<'mcx>(
    out: &mut StringInfo<'mcx>,
    image: &[u8],
    estimated_len: usize,
) -> PgResult<()> {
    out.enlarge(estimated_len)?;
    if jsonpath_header(image) & JSONPATH_LAX == 0 {
        out.append_bytes(b"strict ")?;
    }
    let v = jsp_init(image);
    print_item(out, &v, false, true)
}

// I/O cores.

/// C: jsonpath_in core. Ok(None) = soft error recorded in `escontext`.
pub fn jsonpath_in<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    json_path_from_cstring(mcx, input, escontext)
}

/// C: jsonpath_out core — NUL-terminated cstring bytes.
pub fn jsonpath_out<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = StringInfo::new_in(mcx)?;
    json_path_to_cstring_into(&mut out, image, image.len())?;
    let mut v = out.into_vec();
    v.push(0);
    Ok(v)
}

/// C: jsonpath_recv (binary framing: 1-byte version + text).
pub fn jsonpath_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let version = pqformat::pq_getmsgint(buf, 1)?;
    if version != JSONPATH_VERSION {
        return Err(Box::new(PgError::error(format!(
            "unsupported jsonpath version number: {version}"
        ))));
    }
    let rawbytes = buf.len().saturating_sub(buf.cursor);
    let str = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    match json_path_from_cstring(mcx, &str, None)? {
        Some(image) => Ok(image),
        None => unreachable!("hard errsave without escontext returns Err"),
    }
}

/// C: jsonpath_send — version byte + the text rendering.
pub fn jsonpath_send<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<::datum::Bytea<'mcx>> {
    let mut jtext = StringInfo::new_in(mcx)?;
    json_path_to_cstring_into(&mut jtext, image, image.len())?;
    let mut sbuf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint8(&mut sbuf, JSONPATH_VERSION as u8)?;
    pqformat::pq_sendtext(&mut sbuf, jtext.as_bytes())?;
    Ok(pqformat::pq_endtypsend(sbuf))
}
