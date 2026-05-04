use pgrust_nodes::Value;
use pgrust_nodes::parsenodes::{CopyForceQuote, CopyFormat, CopyToOptions};
use pgrust_nodes::primnodes::QueryColumn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyToSerializeError<E> {
    WrongColumnCount { expected: usize, actual: usize },
    Format(E),
}

impl<E> CopyToSerializeError<E> {
    pub fn map_format<F>(self, f: impl FnOnce(E) -> F) -> CopyToSerializeError<F> {
        match self {
            CopyToSerializeError::WrongColumnCount { expected, actual } => {
                CopyToSerializeError::WrongColumnCount { expected, actual }
            }
            CopyToSerializeError::Format(err) => CopyToSerializeError::Format(f(err)),
        }
    }
}

pub fn begin_copy_to_bytes(columns: &[QueryColumn], options: &CopyToOptions) -> Vec<u8> {
    match options.format {
        CopyFormat::Text => Vec::new(),
        CopyFormat::Csv if options.header => csv_header_bytes(columns, options),
        CopyFormat::Csv => Vec::new(),
        CopyFormat::Binary => {
            let mut out = Vec::new();
            out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
            out.extend_from_slice(&0_i32.to_be_bytes());
            out.extend_from_slice(&0_i32.to_be_bytes());
            out
        }
    }
}

pub fn copy_to_row_bytes<E>(
    columns: &[QueryColumn],
    row: &[Value],
    options: &CopyToOptions,
    mut text_formatter: impl FnMut(&Value, &QueryColumn) -> Result<Option<String>, E>,
    mut binary_encoder: impl FnMut(&Value, &QueryColumn) -> Result<Vec<u8>, E>,
) -> Result<Vec<u8>, CopyToSerializeError<E>> {
    match options.format {
        CopyFormat::Text => text_copy_row_bytes(columns, row, options, &mut text_formatter),
        CopyFormat::Csv => csv_copy_row_bytes(columns, row, options, &mut text_formatter),
        CopyFormat::Binary => binary_copy_row_bytes(columns, row, &mut binary_encoder),
    }
}

pub fn finish_copy_to_bytes(options: &CopyToOptions) -> Vec<u8> {
    if matches!(options.format, CopyFormat::Binary) {
        (-1_i16).to_be_bytes().to_vec()
    } else {
        Vec::new()
    }
}

pub fn append_text_field(out: &mut Vec<u8>, bytes: &[u8], delimiter: u8) {
    for &byte in bytes {
        match byte {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x0b => out.extend_from_slice(b"\\v"),
            0x0c => out.extend_from_slice(b"\\f"),
            byte if byte == delimiter => {
                out.push(b'\\');
                out.push(byte);
            }
            _ => out.push(byte),
        }
    }
}

pub fn append_csv_field(
    out: &mut Vec<u8>,
    bytes: &[u8],
    force_quote: bool,
    null_marker: &[u8],
    delimiter: u8,
    quote: u8,
    escape: u8,
) {
    let needs_quote = force_quote
        || bytes == null_marker
        || bytes
            .iter()
            .any(|byte| matches!(*byte, b'\n' | b'\r') || *byte == delimiter || *byte == quote);
    if !needs_quote {
        out.extend_from_slice(bytes);
        return;
    }
    out.push(quote);
    for &byte in bytes {
        if byte == quote || byte == escape {
            out.push(escape);
        }
        out.push(byte);
    }
    out.push(quote);
}

pub fn force_quote_column(
    force_quote: &CopyForceQuote,
    columns: &[QueryColumn],
    idx: usize,
) -> bool {
    match force_quote {
        CopyForceQuote::None => false,
        CopyForceQuote::All => true,
        CopyForceQuote::Columns(names) => names.iter().any(|name| name == &columns[idx].name),
    }
}

fn text_copy_row_bytes<E>(
    columns: &[QueryColumn],
    row: &[Value],
    options: &CopyToOptions,
    text_formatter: &mut impl FnMut(&Value, &QueryColumn) -> Result<Option<String>, E>,
) -> Result<Vec<u8>, CopyToSerializeError<E>> {
    validate_copy_row_width(row, columns)?;
    let delimiter = options.delimiter.as_bytes()[0];
    let mut line = Vec::new();
    for (idx, (value, column)) in row.iter().zip(columns).enumerate() {
        if idx > 0 {
            line.push(delimiter);
        }
        match text_formatter(value, column).map_err(CopyToSerializeError::Format)? {
            Some(text) => append_text_field(&mut line, text.as_bytes(), delimiter),
            None => line.extend_from_slice(options.null.as_bytes()),
        }
    }
    line.push(b'\n');
    Ok(line)
}

fn csv_header_bytes(columns: &[QueryColumn], options: &CopyToOptions) -> Vec<u8> {
    let delimiter = options.delimiter.as_bytes()[0];
    let quote = options.quote.as_bytes()[0];
    let escape = options.escape.as_bytes()[0];
    let mut line = Vec::new();
    for (idx, column) in columns.iter().enumerate() {
        if idx > 0 {
            line.push(delimiter);
        }
        append_csv_field(
            &mut line,
            column.name.as_bytes(),
            false,
            options.null.as_bytes(),
            delimiter,
            quote,
            escape,
        );
    }
    line.push(b'\n');
    line
}

fn csv_copy_row_bytes<E>(
    columns: &[QueryColumn],
    row: &[Value],
    options: &CopyToOptions,
    text_formatter: &mut impl FnMut(&Value, &QueryColumn) -> Result<Option<String>, E>,
) -> Result<Vec<u8>, CopyToSerializeError<E>> {
    validate_copy_row_width(row, columns)?;
    let delimiter = options.delimiter.as_bytes()[0];
    let quote = options.quote.as_bytes()[0];
    let escape = options.escape.as_bytes()[0];
    let mut line = Vec::new();
    for (idx, (value, column)) in row.iter().zip(columns).enumerate() {
        if idx > 0 {
            line.push(delimiter);
        }
        if matches!(value, Value::Null) {
            line.extend_from_slice(options.null.as_bytes());
            continue;
        }
        let text = text_formatter(value, column)
            .map_err(CopyToSerializeError::Format)?
            .unwrap_or_default();
        append_csv_field(
            &mut line,
            text.as_bytes(),
            force_quote_column(&options.force_quote, columns, idx),
            options.null.as_bytes(),
            delimiter,
            quote,
            escape,
        );
    }
    line.push(b'\n');
    Ok(line)
}

fn binary_copy_row_bytes<E>(
    columns: &[QueryColumn],
    row: &[Value],
    binary_encoder: &mut impl FnMut(&Value, &QueryColumn) -> Result<Vec<u8>, E>,
) -> Result<Vec<u8>, CopyToSerializeError<E>> {
    validate_copy_row_width(row, columns)?;
    let mut line = Vec::new();
    line.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for (value, column) in row.iter().zip(columns) {
        if matches!(value, Value::Null) {
            line.extend_from_slice(&(-1_i32).to_be_bytes());
            continue;
        }
        let payload = binary_encoder(value, column).map_err(CopyToSerializeError::Format)?;
        line.extend_from_slice(&(payload.len() as i32).to_be_bytes());
        line.extend_from_slice(&payload);
    }
    Ok(line)
}

fn validate_copy_row_width<E>(
    row: &[Value],
    columns: &[QueryColumn],
) -> Result<(), CopyToSerializeError<E>> {
    if row.len() == columns.len() {
        Ok(())
    } else {
        Err(CopyToSerializeError::WrongColumnCount {
            expected: columns.len(),
            actual: row.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_nodes::{SqlType, SqlTypeKind};

    fn text_column(name: &str) -> QueryColumn {
        QueryColumn {
            name: name.into(),
            sql_type: SqlType::new(SqlTypeKind::Text),
            wire_type_oid: None,
        }
    }

    fn int_column(name: &str) -> QueryColumn {
        QueryColumn {
            name: name.into(),
            sql_type: SqlType::new(SqlTypeKind::Int4),
            wire_type_oid: None,
        }
    }

    fn serialize(columns: &[QueryColumn], rows: &[Vec<Value>], options: CopyToOptions) -> Vec<u8> {
        let mut out = begin_copy_to_bytes(columns, &options);
        for row in rows {
            out.extend(
                copy_to_row_bytes(
                    columns,
                    row,
                    &options,
                    |value, _| match value {
                        Value::Null => Ok::<_, ()>(None),
                        Value::Text(text) => Ok(Some(text.to_string())),
                        Value::Int32(value) => Ok(Some(value.to_string())),
                        other => Ok(Some(other.as_text().unwrap_or_default().to_string())),
                    },
                    |value, _| match value {
                        Value::Int32(value) => Ok::<_, ()>(value.to_be_bytes().to_vec()),
                        _ => Ok(Vec::new()),
                    },
                )
                .unwrap(),
            );
        }
        out.extend(finish_copy_to_bytes(&options));
        out
    }

    #[test]
    fn text_field_escapes_copy_special_bytes() {
        let mut out = Vec::new();
        append_text_field(&mut out, b"a\tb\nc\\d|e", b'|');
        assert_eq!(out, b"a\\tb\\nc\\\\d\\|e");
    }

    #[test]
    fn csv_field_quotes_null_marker_and_escapes_quote() {
        let mut out = Vec::new();
        append_csv_field(&mut out, b"a,\"b\"", false, b"\\N", b',', b'"', b'"');
        assert_eq!(out, b"\"a,\"\"b\"\"\"");
    }

    #[test]
    fn force_quote_matches_requested_column_name() {
        let columns = vec![text_column("a"), text_column("b")];
        assert!(!force_quote_column(
            &CopyForceQuote::Columns(vec!["b".into()]),
            &columns,
            0
        ));
        assert!(force_quote_column(
            &CopyForceQuote::Columns(vec!["b".into()]),
            &columns,
            1
        ));
    }

    #[test]
    fn text_copy_escapes_special_bytes() {
        let columns = vec![text_column("a"), text_column("b")];
        let rows = vec![vec![Value::Text("a\tb\nc\\d".into()), Value::Null]];
        assert_eq!(
            serialize(&columns, &rows, CopyToOptions::default()),
            b"a\\tb\\nc\\\\d\t\\N\n"
        );
    }

    #[test]
    fn csv_copy_quotes_header_null_and_forced_columns() {
        let columns = vec![text_column("a,b"), text_column("plain")];
        let rows = vec![vec![Value::Text("".into()), Value::Text("x".into())]];
        let options = CopyToOptions {
            format: CopyFormat::Csv,
            encoding: None,
            delimiter: ",".into(),
            null: "".into(),
            header: true,
            quote: "\"".into(),
            escape: "\"".into(),
            force_quote: CopyForceQuote::Columns(vec!["plain".into()]),
        };
        assert_eq!(
            serialize(&columns, &rows, options),
            b"\"a,b\",plain\n\"\",\"x\"\n"
        );
    }

    #[test]
    fn binary_copy_writes_header_row_and_trailer() {
        let columns = vec![int_column("id"), text_column("name")];
        let rows = vec![vec![Value::Int32(7), Value::Null]];
        let out = serialize(
            &columns,
            &rows,
            CopyToOptions {
                format: CopyFormat::Binary,
                ..CopyToOptions::default()
            },
        );
        assert!(out.starts_with(b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0"));
        assert_eq!(&out[19..21], &2_i16.to_be_bytes());
        assert_eq!(&out[21..25], &4_i32.to_be_bytes());
        assert_eq!(&out[25..29], &7_i32.to_be_bytes());
        assert_eq!(&out[29..33], &(-1_i32).to_be_bytes());
        assert_eq!(&out[33..35], &(-1_i16).to_be_bytes());
    }
}
