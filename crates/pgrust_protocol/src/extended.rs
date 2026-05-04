use std::io;

use crate::pqcomm::{read_cstr, read_i16_bytes, read_i32_bytes};
use crate::sql::{highest_sql_parameter_ref, quote_sql_string};

#[derive(Debug, Clone, Default)]
pub struct PreparedStatement {
    pub sql: String,
    pub param_type_oids: Vec<u32>,
    pub prepare_time: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseMessage {
    pub statement_name: String,
    pub raw_sql: String,
    pub param_type_oids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindMessage {
    pub portal_name: String,
    pub statement_name: String,
    pub param_formats: Vec<i16>,
    pub raw_params: Vec<Option<Vec<u8>>>,
    pub result_formats: Vec<i16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteMessage {
    pub portal_name: String,
    pub max_rows: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeTargetKind {
    Statement,
    Portal,
    Invalid(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeMessage {
    pub target: DescribeTargetKind,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseTargetKind {
    Statement,
    Portal,
    Invalid(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseMessage {
    pub target: CloseTargetKind,
    pub name: String,
}

#[derive(Debug)]
pub enum BindMessageError {
    Io(io::Error),
    UnsupportedParameterFormatCode,
    InvalidParameterFormatCodeCount,
    UnsupportedResultFormatCode,
}

impl From<io::Error> for BindMessageError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

#[derive(Debug, Clone)]
pub enum BoundParam {
    Null,
    Text(String),
    SqlExpression(String),
}

pub trait RegclassParamResolver {
    fn resolve_regclass_param(&self, value: &str) -> String;
}

pub fn required_bind_param_count(stmt: &PreparedStatement) -> usize {
    stmt.param_type_oids
        .len()
        .max(highest_sql_parameter_ref(&stmt.sql))
}

pub fn parse_parse_message(body: &[u8]) -> io::Result<ParseMessage> {
    let mut offset = 0;
    let statement_name = read_cstr(body, &mut offset)?;
    let raw_sql = read_cstr(body, &mut offset)?;
    let nparams = read_i16_bytes(body, &mut offset)? as usize;
    let mut param_type_oids = Vec::with_capacity(nparams);
    for _ in 0..nparams {
        param_type_oids.push(read_i32_bytes(body, &mut offset)? as u32);
    }
    Ok(ParseMessage {
        statement_name,
        raw_sql,
        param_type_oids,
    })
}

pub fn parse_bind_message(body: &[u8]) -> Result<BindMessage, BindMessageError> {
    let mut offset = 0;
    let portal_name = read_cstr(body, &mut offset)?;
    let statement_name = read_cstr(body, &mut offset)?;
    let n_format_codes = read_i16_bytes(body, &mut offset)? as usize;
    let mut param_formats = Vec::with_capacity(n_format_codes);
    for _ in 0..n_format_codes {
        param_formats.push(read_i16_bytes(body, &mut offset)?);
    }
    if param_formats.iter().any(|code| !matches!(*code, 0 | 1)) {
        return Err(BindMessageError::UnsupportedParameterFormatCode);
    }

    let nparams = read_i16_bytes(body, &mut offset)? as usize;
    if !(param_formats.is_empty() || param_formats.len() == 1 || param_formats.len() == nparams) {
        return Err(BindMessageError::InvalidParameterFormatCodeCount);
    }
    let mut raw_params = Vec::with_capacity(nparams);
    for _ in 0..nparams {
        let len = read_i32_bytes(body, &mut offset)?;
        if len < 0 {
            raw_params.push(None);
        } else {
            let len = len as usize;
            let end = offset.saturating_add(len);
            let bytes = body.get(offset..end).ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "short bind parameter")
            })?;
            offset = end;
            raw_params.push(Some(bytes.to_vec()));
        }
    }

    let n_result_codes = read_i16_bytes(body, &mut offset)? as usize;
    let mut result_formats = Vec::with_capacity(n_result_codes);
    for _ in 0..n_result_codes {
        result_formats.push(read_i16_bytes(body, &mut offset)?);
    }
    if result_formats.iter().any(|code| !matches!(*code, 0 | 1)) {
        return Err(BindMessageError::UnsupportedResultFormatCode);
    }

    Ok(BindMessage {
        portal_name,
        statement_name,
        param_formats,
        raw_params,
        result_formats,
    })
}

pub fn parse_execute_message(body: &[u8]) -> io::Result<ExecuteMessage> {
    let mut offset = 0;
    let portal_name = read_cstr(body, &mut offset)?;
    let max_rows = read_i32_bytes(body, &mut offset)?;
    Ok(ExecuteMessage {
        portal_name,
        max_rows,
    })
}

pub fn parse_describe_message(body: &[u8]) -> io::Result<DescribeMessage> {
    let mut offset = 0;
    let target_byte = body
        .get(offset)
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "describe target missing"))?;
    offset += 1;
    let name = read_cstr(body, &mut offset)?;
    let target = match target_byte {
        b'S' => DescribeTargetKind::Statement,
        b'P' => DescribeTargetKind::Portal,
        other => DescribeTargetKind::Invalid(other),
    };
    Ok(DescribeMessage { target, name })
}

pub fn parse_close_message(body: &[u8]) -> io::Result<CloseMessage> {
    let mut offset = 0;
    let target_byte = body
        .get(offset)
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "close target missing"))?;
    offset += 1;
    let name = read_cstr(body, &mut offset)?;
    let target = match target_byte {
        b'S' => CloseTargetKind::Statement,
        b'P' => CloseTargetKind::Portal,
        other => CloseTargetKind::Invalid(other),
    };
    Ok(CloseMessage { target, name })
}

pub fn substitute_params(
    sql: &str,
    params: &[BoundParam],
    resolver: &dyn RegclassParamResolver,
) -> String {
    let mut out = sql.to_string();
    for (i, param) in params.iter().enumerate() {
        let placeholder = format!("${}", i + 1);
        let regclass_value = match param {
            BoundParam::Null => "null".to_string(),
            BoundParam::Text(v) => resolver.resolve_regclass_param(v),
            BoundParam::SqlExpression(expr) => expr.clone(),
        };
        out = out.replace(
            &format!("{placeholder}::pg_catalog.regclass"),
            &regclass_value,
        );
        out = out.replace(&format!("{placeholder}::regclass"), &regclass_value);
        let value = match param {
            BoundParam::Null => "null".to_string(),
            BoundParam::Text(v) if v.parse::<i64>().is_ok() => v.clone(),
            BoundParam::Text(v) => quote_sql_string(v),
            BoundParam::SqlExpression(expr) => expr.clone(),
        };
        out = out.replace(&placeholder, &value);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Resolver;

    impl RegclassParamResolver for Resolver {
        fn resolve_regclass_param(&self, value: &str) -> String {
            match value {
                "widgets" => "42".into(),
                other => other.into(),
            }
        }
    }

    #[test]
    fn bind_param_count_uses_highest_sql_parameter_ref() {
        assert_eq!(highest_sql_parameter_ref("select 1"), 0);
        assert_eq!(highest_sql_parameter_ref("select $2, $10, $1"), 10);
        assert_eq!(highest_sql_parameter_ref("select '$9', $1, $$ $8 $$"), 1);
        assert_eq!(
            required_bind_param_count(&PreparedStatement {
                sql: "select $2".into(),
                param_type_oids: vec![],
                prepare_time: 0,
            }),
            2
        );
        assert_eq!(
            required_bind_param_count(&PreparedStatement {
                sql: "select 1".into(),
                param_type_oids: vec![23, 25],
                prepare_time: 0,
            }),
            2
        );
    }

    #[test]
    fn substitute_params_resolves_regclass_and_quotes_text() {
        assert_eq!(
            substitute_params(
                "select $1::regclass, $2, $3",
                &[
                    BoundParam::Text("widgets".into()),
                    BoundParam::Text("a'b".into()),
                    BoundParam::Null,
                ],
                &Resolver,
            ),
            "select 42, 'a''b', null"
        );
    }

    #[test]
    fn parse_parse_message_decodes_names_sql_and_types() {
        let mut body = Vec::new();
        body.extend_from_slice(b"s1\0select $1\0");
        body.extend_from_slice(&1_i16.to_be_bytes());
        body.extend_from_slice(&23_i32.to_be_bytes());

        assert_eq!(
            parse_parse_message(&body).unwrap(),
            ParseMessage {
                statement_name: "s1".into(),
                raw_sql: "select $1".into(),
                param_type_oids: vec![23],
            }
        );
    }

    #[test]
    fn parse_bind_message_decodes_formats_params_and_results() {
        let mut body = Vec::new();
        body.extend_from_slice(b"p1\0s1\0");
        body.extend_from_slice(&1_i16.to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
        body.extend_from_slice(&2_i16.to_be_bytes());
        body.extend_from_slice(&3_i32.to_be_bytes());
        body.extend_from_slice(b"abc");
        body.extend_from_slice(&(-1_i32).to_be_bytes());
        body.extend_from_slice(&1_i16.to_be_bytes());
        body.extend_from_slice(&1_i16.to_be_bytes());

        assert_eq!(
            parse_bind_message(&body).unwrap(),
            BindMessage {
                portal_name: "p1".into(),
                statement_name: "s1".into(),
                param_formats: vec![0],
                raw_params: vec![Some(b"abc".to_vec()), None],
                result_formats: vec![1],
            }
        );
    }

    #[test]
    fn parse_execute_message_decodes_portal_and_limit() {
        let mut body = Vec::new();
        body.extend_from_slice(b"p1\0");
        body.extend_from_slice(&7_i32.to_be_bytes());

        assert_eq!(
            parse_execute_message(&body).unwrap(),
            ExecuteMessage {
                portal_name: "p1".into(),
                max_rows: 7,
            }
        );
    }

    #[test]
    fn parse_describe_message_decodes_target_and_name() {
        assert_eq!(
            parse_describe_message(b"S\0").unwrap(),
            DescribeMessage {
                target: DescribeTargetKind::Statement,
                name: String::new(),
            }
        );
        assert_eq!(
            parse_describe_message(b"Pportal\0").unwrap(),
            DescribeMessage {
                target: DescribeTargetKind::Portal,
                name: "portal".into(),
            }
        );
        assert_eq!(
            parse_describe_message(b"Xbad\0").unwrap().target,
            DescribeTargetKind::Invalid(b'X')
        );
    }

    #[test]
    fn parse_close_message_decodes_target_and_name() {
        assert_eq!(
            parse_close_message(b"Sstmt\0").unwrap(),
            CloseMessage {
                target: CloseTargetKind::Statement,
                name: "stmt".into(),
            }
        );
        assert_eq!(
            parse_close_message(b"Pportal\0").unwrap(),
            CloseMessage {
                target: CloseTargetKind::Portal,
                name: "portal".into(),
            }
        );
        assert_eq!(
            parse_close_message(b"?bad\0").unwrap().target,
            CloseTargetKind::Invalid(b'?')
        );
    }
}
