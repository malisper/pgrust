use std::io::{self, Write};

use pgrust_nodes::Value;

use crate::pqcomm::{read_i16_bytes, read_i32_bytes};

#[derive(Debug, Clone)]
pub enum LargeObjectFastpathCall {
    Create {
        oid: u32,
    },
    Import {
        path: String,
        oid: Option<u32>,
    },
    Export {
        oid: u32,
        path: String,
    },
    Open {
        oid: u32,
        mode: i32,
    },
    Close {
        fd: i32,
    },
    Read {
        fd: i32,
        len: i32,
    },
    Write {
        fd: i32,
        data: Vec<u8>,
    },
    Lseek {
        fd: i32,
        offset: i64,
        whence: i32,
        result_i64: bool,
    },
    Creat {
        mode: i32,
    },
    Tell {
        fd: i32,
        result_i64: bool,
    },
    Unlink {
        oid: u32,
    },
    Truncate {
        fd: i32,
        len: i64,
    },
    FromBytea {
        oid: u32,
        data: Vec<u8>,
    },
    Get {
        oid: u32,
        offset: Option<i64>,
        len: Option<i32>,
    },
    Put {
        oid: u32,
        offset: i64,
        data: Vec<u8>,
    },
}

impl LargeObjectFastpathCall {
    pub fn requires_xid(&self, inv_write: i32) -> bool {
        match self {
            Self::Create { .. }
            | Self::Import { .. }
            | Self::Creat { .. }
            | Self::Unlink { .. }
            | Self::Write { .. }
            | Self::Truncate { .. }
            | Self::FromBytea { .. }
            | Self::Put { .. } => true,
            Self::Open { mode, .. } => *mode & inv_write != 0,
            Self::Export { .. }
            | Self::Close { .. }
            | Self::Read { .. }
            | Self::Lseek { .. }
            | Self::Tell { .. }
            | Self::Get { .. } => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FastpathResultType {
    Int4,
    Int8,
    Oid,
    Bytea,
    Void,
}

#[derive(Debug)]
pub struct ParsedFastpathCall {
    pub call: Option<LargeObjectFastpathCall>,
    pub result_type: FastpathResultType,
    pub result_format: i16,
}

#[derive(Debug)]
pub enum FastpathParseError<E> {
    Protocol(io::Error),
    Message(&'static str),
    UnsupportedFunction(u32),
    Bytea(E),
}

#[derive(Clone, Copy)]
enum FastpathArgType {
    Int4,
    Int8,
    Oid,
    Bytea,
    Text,
}

#[derive(Clone, Copy)]
enum FastpathFunction {
    LoCreate,
    LoImport,
    LoExport,
    LoOpen,
    LoClose,
    LoRead,
    LoWrite,
    LoLseek,
    LoCreat,
    LoTell,
    LoUnlink,
    LoTruncate,
    LoLseek64,
    LoTell64,
    LoTruncate64,
    LoFromBytea,
    LoGet,
    LoPut,
}

#[derive(Clone)]
enum FastpathArgValue {
    Int4(i32),
    Int8(i64),
    Oid(u32),
    Bytea(Vec<u8>),
    Text(String),
    Null,
}

pub fn send_function_call_response(
    stream: &mut impl Write,
    bytes: Option<&[u8]>,
) -> io::Result<()> {
    let payload_len = bytes.map_or(0, <[u8]>::len);
    let len = 4 + 4 + payload_len;
    stream.write_all(&[b'V'])?;
    stream.write_all(&(len as i32).to_be_bytes())?;
    match bytes {
        Some(bytes) => {
            stream.write_all(&(bytes.len() as i32).to_be_bytes())?;
            stream.write_all(bytes)?;
        }
        None => stream.write_all(&(-1_i32).to_be_bytes())?,
    }
    Ok(())
}

pub fn fastpath_i32(bytes: &[u8]) -> Result<i32, &'static str> {
    let array: [u8; 4] = bytes
        .try_into()
        .map_err(|_| "invalid int4 fastpath argument")?;
    Ok(i32::from_be_bytes(array))
}

pub fn fastpath_u32(bytes: &[u8]) -> Result<u32, &'static str> {
    let array: [u8; 4] = bytes
        .try_into()
        .map_err(|_| "invalid oid fastpath argument")?;
    Ok(u32::from_be_bytes(array))
}

pub fn fastpath_i64(bytes: &[u8]) -> Result<i64, &'static str> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| "invalid int8 fastpath argument")?;
    Ok(i64::from_be_bytes(array))
}

pub fn fastpath_value_i32(value: &Value) -> Result<i32, &'static str> {
    match value {
        Value::Int32(value) => Ok(*value),
        Value::Int64(value) => i32::try_from(*value).map_err(|_| "int4 result out of range"),
        _ => Err("fastpath result type mismatch"),
    }
}

pub fn fastpath_value_i64(value: &Value) -> Result<i64, &'static str> {
    match value {
        Value::Int64(value) => Ok(*value),
        Value::Int32(value) => Ok(i64::from(*value)),
        _ => Err("fastpath result type mismatch"),
    }
}

pub fn fastpath_value_u32(value: &Value) -> Result<u32, &'static str> {
    match value {
        Value::Int64(value) => u32::try_from(*value).map_err(|_| "oid result out of range"),
        Value::Int32(value) => u32::try_from(*value).map_err(|_| "oid result out of range"),
        _ => Err("fastpath result type mismatch"),
    }
}

pub fn fastpath_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

pub fn quote_fastpath_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn parse_large_object_fastpath_call<E>(
    body: &[u8],
    mut parse_bytea_text: impl FnMut(&str) -> Result<Vec<u8>, E>,
) -> Result<ParsedFastpathCall, FastpathParseError<E>> {
    let mut offset = 0usize;
    let function_oid =
        read_i32_bytes(body, &mut offset).map_err(FastpathParseError::Protocol)? as u32;
    let (function, result_type, arg_types) = fastpath_large_object_signature(function_oid)
        .ok_or(FastpathParseError::UnsupportedFunction(function_oid))?;

    let arg_format_count =
        read_i16_bytes(body, &mut offset).map_err(FastpathParseError::Protocol)?;
    if arg_format_count < 0 {
        return Err(FastpathParseError::Message(
            "negative argument format count",
        ));
    }
    let mut arg_formats = Vec::with_capacity(arg_format_count as usize);
    for _ in 0..arg_format_count {
        arg_formats.push(read_i16_bytes(body, &mut offset).map_err(FastpathParseError::Protocol)?);
    }

    let arg_count = read_i16_bytes(body, &mut offset).map_err(FastpathParseError::Protocol)?;
    if arg_count < 0 {
        return Err(FastpathParseError::Message("negative argument count"));
    }
    let arg_count = arg_count as usize;
    if arg_count != arg_types.len() {
        return Err(FastpathParseError::Message(
            "large-object fastpath arity mismatch",
        ));
    }

    let mut rendered_args = Vec::with_capacity(arg_count);
    for (idx, arg_type) in arg_types.iter().copied().enumerate() {
        let len = read_i32_bytes(body, &mut offset).map_err(FastpathParseError::Protocol)?;
        let bytes = if len < 0 {
            None
        } else {
            let len = len as usize;
            let end = offset.saturating_add(len);
            let slice = body
                .get(offset..end)
                .ok_or(FastpathParseError::Message("short fastpath argument"))?;
            offset = end;
            Some(slice)
        };
        let format = match arg_formats.as_slice() {
            [] => 0,
            [format] => *format,
            formats => *formats
                .get(idx)
                .ok_or(FastpathParseError::Message("missing argument format code"))?,
        };
        rendered_args.push(decode_fastpath_arg(
            bytes,
            format,
            arg_type,
            &mut parse_bytea_text,
        )?);
    }

    let result_format = read_i16_bytes(body, &mut offset).map_err(FastpathParseError::Protocol)?;
    if offset != body.len() {
        return Err(FastpathParseError::Message(
            "trailing data in fastpath call",
        ));
    }

    let call = fastpath_large_object_call(function, &rendered_args)?;
    Ok(ParsedFastpathCall {
        call,
        result_type,
        result_format,
    })
}

fn fastpath_large_object_signature(
    function_oid: u32,
) -> Option<(FastpathFunction, FastpathResultType, Vec<FastpathArgType>)> {
    use FastpathArgType as A;
    use FastpathFunction as F;
    use FastpathResultType as R;
    match function_oid {
        715 => Some((F::LoCreate, R::Oid, vec![A::Oid])),
        764 => Some((F::LoImport, R::Oid, vec![A::Text])),
        765 => Some((F::LoExport, R::Int4, vec![A::Oid, A::Text])),
        767 => Some((F::LoImport, R::Oid, vec![A::Text, A::Oid])),
        952 => Some((F::LoOpen, R::Int4, vec![A::Oid, A::Int4])),
        953 => Some((F::LoClose, R::Int4, vec![A::Int4])),
        954 => Some((F::LoRead, R::Bytea, vec![A::Int4, A::Int4])),
        955 => Some((F::LoWrite, R::Int4, vec![A::Int4, A::Bytea])),
        956 => Some((F::LoLseek, R::Int4, vec![A::Int4, A::Int4, A::Int4])),
        957 => Some((F::LoCreat, R::Oid, vec![A::Int4])),
        958 => Some((F::LoTell, R::Int4, vec![A::Int4])),
        964 => Some((F::LoUnlink, R::Int4, vec![A::Oid])),
        1004 => Some((F::LoTruncate, R::Int4, vec![A::Int4, A::Int4])),
        3170 => Some((F::LoLseek64, R::Int8, vec![A::Int4, A::Int8, A::Int4])),
        3171 => Some((F::LoTell64, R::Int8, vec![A::Int4])),
        3172 => Some((F::LoTruncate64, R::Int4, vec![A::Int4, A::Int8])),
        3457 => Some((F::LoFromBytea, R::Oid, vec![A::Oid, A::Bytea])),
        3458 => Some((F::LoGet, R::Bytea, vec![A::Oid])),
        3459 => Some((F::LoGet, R::Bytea, vec![A::Oid, A::Int8, A::Int4])),
        3460 => Some((F::LoPut, R::Void, vec![A::Oid, A::Int8, A::Bytea])),
        _ => None,
    }
}

fn decode_fastpath_arg<E>(
    bytes: Option<&[u8]>,
    format: i16,
    arg_type: FastpathArgType,
    parse_bytea_text: &mut impl FnMut(&str) -> Result<Vec<u8>, E>,
) -> Result<FastpathArgValue, FastpathParseError<E>> {
    let Some(bytes) = bytes else {
        return Ok(FastpathArgValue::Null);
    };
    match format {
        0 => decode_fastpath_text_arg(bytes, arg_type, parse_bytea_text),
        1 => decode_fastpath_binary_arg(bytes, arg_type),
        _ => Err(FastpathParseError::Message(
            "unsupported fastpath argument format",
        )),
    }
}

fn decode_fastpath_text_arg<E>(
    bytes: &[u8],
    arg_type: FastpathArgType,
    parse_bytea_text: &mut impl FnMut(&str) -> Result<Vec<u8>, E>,
) -> Result<FastpathArgValue, FastpathParseError<E>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| FastpathParseError::Message("invalid fastpath text argument"))?;
    let trimmed = text.trim();
    match arg_type {
        FastpathArgType::Int4 => trimmed
            .parse::<i32>()
            .map(FastpathArgValue::Int4)
            .map_err(|_| FastpathParseError::Message("invalid int4 fastpath argument")),
        FastpathArgType::Int8 => trimmed
            .parse::<i64>()
            .map(FastpathArgValue::Int8)
            .map_err(|_| FastpathParseError::Message("invalid int8 fastpath argument")),
        FastpathArgType::Oid => trimmed
            .parse::<u32>()
            .map(FastpathArgValue::Oid)
            .map_err(|_| FastpathParseError::Message("invalid oid fastpath argument")),
        FastpathArgType::Bytea => parse_bytea_text(text)
            .map(FastpathArgValue::Bytea)
            .map_err(FastpathParseError::Bytea),
        FastpathArgType::Text => Ok(FastpathArgValue::Text(text.to_string())),
    }
}

fn decode_fastpath_binary_arg<E>(
    bytes: &[u8],
    arg_type: FastpathArgType,
) -> Result<FastpathArgValue, FastpathParseError<E>> {
    Ok(match arg_type {
        FastpathArgType::Int4 => {
            FastpathArgValue::Int4(fastpath_i32(bytes).map_err(FastpathParseError::Message)?)
        }
        FastpathArgType::Int8 => {
            FastpathArgValue::Int8(fastpath_i64(bytes).map_err(FastpathParseError::Message)?)
        }
        FastpathArgType::Oid => {
            FastpathArgValue::Oid(fastpath_u32(bytes).map_err(FastpathParseError::Message)?)
        }
        FastpathArgType::Bytea => FastpathArgValue::Bytea(bytes.to_vec()),
        FastpathArgType::Text => {
            let text = std::str::from_utf8(bytes)
                .map_err(|_| FastpathParseError::Message("invalid fastpath text argument"))?;
            FastpathArgValue::Text(text.to_string())
        }
    })
}

fn fastpath_large_object_call<E>(
    function: FastpathFunction,
    args: &[FastpathArgValue],
) -> Result<Option<LargeObjectFastpathCall>, FastpathParseError<E>> {
    if args.iter().any(|arg| matches!(arg, FastpathArgValue::Null)) {
        return Ok(None);
    }
    use FastpathArgValue as V;
    use FastpathFunction as F;
    let call = match (function, args) {
        (F::LoCreate, [V::Oid(oid)]) => LargeObjectFastpathCall::Create { oid: *oid },
        (F::LoImport, [V::Text(path)]) => LargeObjectFastpathCall::Import {
            path: path.clone(),
            oid: None,
        },
        (F::LoImport, [V::Text(path), V::Oid(oid)]) => LargeObjectFastpathCall::Import {
            path: path.clone(),
            oid: Some(*oid),
        },
        (F::LoExport, [V::Oid(oid), V::Text(path)]) => LargeObjectFastpathCall::Export {
            oid: *oid,
            path: path.clone(),
        },
        (F::LoOpen, [V::Oid(oid), V::Int4(mode)]) => LargeObjectFastpathCall::Open {
            oid: *oid,
            mode: *mode,
        },
        (F::LoClose, [V::Int4(fd)]) => LargeObjectFastpathCall::Close { fd: *fd },
        (F::LoRead, [V::Int4(fd), V::Int4(len)]) => {
            LargeObjectFastpathCall::Read { fd: *fd, len: *len }
        }
        (F::LoWrite, [V::Int4(fd), V::Bytea(data)]) => LargeObjectFastpathCall::Write {
            fd: *fd,
            data: data.clone(),
        },
        (F::LoLseek, [V::Int4(fd), V::Int4(offset), V::Int4(whence)]) => {
            LargeObjectFastpathCall::Lseek {
                fd: *fd,
                offset: i64::from(*offset),
                whence: *whence,
                result_i64: false,
            }
        }
        (F::LoCreat, [V::Int4(mode)]) => LargeObjectFastpathCall::Creat { mode: *mode },
        (F::LoTell, [V::Int4(fd)]) => LargeObjectFastpathCall::Tell {
            fd: *fd,
            result_i64: false,
        },
        (F::LoUnlink, [V::Oid(oid)]) => LargeObjectFastpathCall::Unlink { oid: *oid },
        (F::LoTruncate, [V::Int4(fd), V::Int4(len)]) => LargeObjectFastpathCall::Truncate {
            fd: *fd,
            len: i64::from(*len),
        },
        (F::LoLseek64, [V::Int4(fd), V::Int8(offset), V::Int4(whence)]) => {
            LargeObjectFastpathCall::Lseek {
                fd: *fd,
                offset: *offset,
                whence: *whence,
                result_i64: true,
            }
        }
        (F::LoTell64, [V::Int4(fd)]) => LargeObjectFastpathCall::Tell {
            fd: *fd,
            result_i64: true,
        },
        (F::LoTruncate64, [V::Int4(fd), V::Int8(len)]) => {
            LargeObjectFastpathCall::Truncate { fd: *fd, len: *len }
        }
        (F::LoFromBytea, [V::Oid(oid), V::Bytea(data)]) => LargeObjectFastpathCall::FromBytea {
            oid: *oid,
            data: data.clone(),
        },
        (F::LoGet, [V::Oid(oid)]) => LargeObjectFastpathCall::Get {
            oid: *oid,
            offset: None,
            len: None,
        },
        (F::LoGet, [V::Oid(oid), V::Int8(offset), V::Int4(len)]) => LargeObjectFastpathCall::Get {
            oid: *oid,
            offset: Some(*offset),
            len: Some(*len),
        },
        (F::LoPut, [V::Oid(oid), V::Int8(offset), V::Bytea(data)]) => {
            LargeObjectFastpathCall::Put {
                oid: *oid,
                offset: *offset,
                data: data.clone(),
            }
        }
        _ => {
            return Err(FastpathParseError::Message(
                "large-object fastpath type mismatch",
            ));
        }
    };
    Ok(Some(call))
}

pub fn fastpath_result_bytes(
    value: &Value,
    result_type: FastpathResultType,
    format: i16,
    format_bytea_text: impl FnOnce(&[u8]) -> String,
) -> Result<Option<Vec<u8>>, &'static str> {
    if matches!(value, Value::Null) || matches!(result_type, FastpathResultType::Void) {
        return Ok(None);
    }
    match format {
        0 => Ok(Some(
            fastpath_result_text(value, result_type, format_bytea_text)?.into_bytes(),
        )),
        1 => fastpath_result_binary(value, result_type).map(Some),
        _ => Err("unsupported fastpath result format"),
    }
}

fn fastpath_result_text(
    value: &Value,
    result_type: FastpathResultType,
    format_bytea_text: impl FnOnce(&[u8]) -> String,
) -> Result<String, &'static str> {
    Ok(match result_type {
        FastpathResultType::Int4 => fastpath_value_i32(value)?.to_string(),
        FastpathResultType::Int8 => fastpath_value_i64(value)?.to_string(),
        FastpathResultType::Oid => fastpath_value_u32(value)?.to_string(),
        FastpathResultType::Bytea => match value {
            Value::Bytea(bytes) => format_bytea_text(bytes),
            _ => return Err("fastpath result type mismatch"),
        },
        FastpathResultType::Void => String::new(),
    })
}

fn fastpath_result_binary(
    value: &Value,
    result_type: FastpathResultType,
) -> Result<Vec<u8>, &'static str> {
    Ok(match result_type {
        FastpathResultType::Int4 => fastpath_value_i32(value)?.to_be_bytes().to_vec(),
        FastpathResultType::Int8 => fastpath_value_i64(value)?.to_be_bytes().to_vec(),
        FastpathResultType::Oid => fastpath_value_u32(value)?.to_be_bytes().to_vec(),
        FastpathResultType::Bytea => match value {
            Value::Bytea(bytes) => bytes.clone(),
            _ => return Err("fastpath result type mismatch"),
        },
        FastpathResultType::Void => Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fastpath_numeric_values_decode_and_encode() {
        assert_eq!(fastpath_i32(&1_i32.to_be_bytes()), Ok(1));
        assert_eq!(fastpath_u32(&2_u32.to_be_bytes()), Ok(2));
        assert_eq!(fastpath_i64(&3_i64.to_be_bytes()), Ok(3));
        assert_eq!(fastpath_value_i32(&Value::Int64(4)), Ok(4));
        assert_eq!(fastpath_value_i64(&Value::Int32(5)), Ok(5));
        assert_eq!(fastpath_value_u32(&Value::Int32(6)), Ok(6));
        assert!(fastpath_i32(&[1, 2]).is_err());
    }

    #[test]
    fn fastpath_text_helpers_render_wire_compatible_text() {
        assert_eq!(fastpath_hex(&[0, 15, 255]), "000fff");
        assert_eq!(quote_fastpath_sql_literal("a'b"), "'a''b'");
    }

    #[test]
    fn function_call_response_uses_v_message() {
        let mut out = Vec::new();
        send_function_call_response(&mut out, Some(&[1, 2])).unwrap();
        assert_eq!(out, vec![b'V', 0, 0, 0, 10, 0, 0, 0, 2, 1, 2]);
    }
}
