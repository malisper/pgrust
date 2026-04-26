use std::io::Write;

use crate::backend::executor::{ExecError, Value};
use crate::backend::libpq::pqformat::{
    FloatFormatOptions, encode_binary_data_row_value, format_text_data_value,
};
use crate::backend::utils::misc::notices::take_notices as take_backend_notices;
use crate::include::nodes::parsenodes::{CopyForceQuote, CopyFormat, CopyToOptions};
use crate::include::nodes::primnodes::QueryColumn;
use crate::pl::plpgsql::{PlpgsqlNotice, RaiseLevel, take_notices as take_plpgsql_notices};

thread_local! {
    static COPY_TO_DML_CAPTURE: std::cell::RefCell<Option<Vec<CopyToDmlEvent>>> =
        const { std::cell::RefCell::new(None) };
}

#[derive(Debug, Clone)]
pub struct CopyToNotice {
    pub severity: &'static str,
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub position: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum CopyToDmlEvent {
    Notice(CopyToNotice),
    Row(Vec<Value>),
}

pub trait CopyToSink {
    fn begin(&mut self, _format: CopyFormat, _column_count: usize) -> Result<(), ExecError> {
        Ok(())
    }

    fn notice(
        &mut self,
        _severity: &'static str,
        _sqlstate: &'static str,
        _message: &str,
        _detail: Option<&str>,
        _position: Option<usize>,
    ) -> Result<(), ExecError> {
        Ok(())
    }

    fn write_all(&mut self, data: &[u8]) -> Result<(), ExecError>;

    fn finish(&mut self) -> Result<(), ExecError> {
        Ok(())
    }
}

pub struct IoCopyToSink<'a, W: Write> {
    writer: &'a mut W,
}

impl<'a, W: Write> IoCopyToSink<'a, W> {
    pub fn new(writer: &'a mut W) -> Self {
        Self { writer }
    }
}

impl<W: Write> CopyToSink for IoCopyToSink<'_, W> {
    fn write_all(&mut self, data: &[u8]) -> Result<(), ExecError> {
        self.writer.write_all(data).map_err(copy_io_error)
    }
}

pub fn begin_copy_to<S: CopyToSink + ?Sized>(
    sink: &mut S,
    columns: &[QueryColumn],
    options: &CopyToOptions,
) -> Result<(), ExecError> {
    sink.begin(options.format, columns.len())?;
    match options.format {
        CopyFormat::Text => {}
        CopyFormat::Csv => {
            if options.header {
                write_csv_header(sink, columns, options)?;
            }
        }
        CopyFormat::Binary => {
            sink.write_all(b"PGCOPY\n\xff\r\n\0")?;
            sink.write_all(&0_i32.to_be_bytes())?;
            sink.write_all(&0_i32.to_be_bytes())?;
        }
    }
    Ok(())
}

pub fn write_copy_to_row<S: CopyToSink + ?Sized>(
    sink: &mut S,
    columns: &[QueryColumn],
    row: &[Value],
    options: &CopyToOptions,
    float_format: &FloatFormatOptions,
) -> Result<(), ExecError> {
    match options.format {
        CopyFormat::Text => write_text_copy_row(sink, columns, row, options, float_format),
        CopyFormat::Csv => write_csv_copy_row(sink, columns, row, options, float_format),
        CopyFormat::Binary => write_binary_copy_row(sink, columns, row),
    }
}

pub fn finish_copy_to<S: CopyToSink + ?Sized>(
    sink: &mut S,
    options: &CopyToOptions,
) -> Result<(), ExecError> {
    if matches!(options.format, CopyFormat::Binary) {
        sink.write_all(&(-1_i16).to_be_bytes())?;
    }
    sink.finish()
}

pub fn write_copy_to<S: CopyToSink + ?Sized>(
    sink: &mut S,
    columns: &[QueryColumn],
    rows: &[Vec<Value>],
    options: &CopyToOptions,
    float_format: FloatFormatOptions,
) -> Result<usize, ExecError> {
    begin_copy_to(sink, columns, options)?;
    for row in rows {
        write_copy_to_row(sink, columns, row, options, &float_format)?;
    }
    finish_copy_to(sink, options)?;
    Ok(rows.len())
}

fn write_text_copy_row<S: CopyToSink + ?Sized>(
    sink: &mut S,
    columns: &[QueryColumn],
    row: &[Value],
    options: &CopyToOptions,
    float_format: &FloatFormatOptions,
) -> Result<(), ExecError> {
    let delimiter = options.delimiter.as_bytes()[0];
    validate_copy_row_width(row, columns)?;
    let mut line = Vec::new();
    for (idx, (value, column)) in row.iter().zip(columns).enumerate() {
        if idx > 0 {
            line.push(delimiter);
        }
        match format_text_data_value(value, column, float_format.clone(), None, None, None, None)? {
            Some(text) => append_copy_text_field(&mut line, text.as_bytes(), delimiter),
            None => line.extend_from_slice(options.null.as_bytes()),
        }
    }
    line.push(b'\n');
    sink.write_all(&line)
}

fn append_copy_text_field(out: &mut Vec<u8>, bytes: &[u8], delimiter: u8) {
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

fn write_csv_header<S: CopyToSink + ?Sized>(
    sink: &mut S,
    columns: &[QueryColumn],
    options: &CopyToOptions,
) -> Result<(), ExecError> {
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
    sink.write_all(&line)
}

fn write_csv_copy_row<S: CopyToSink + ?Sized>(
    sink: &mut S,
    columns: &[QueryColumn],
    row: &[Value],
    options: &CopyToOptions,
    float_format: &FloatFormatOptions,
) -> Result<(), ExecError> {
    let delimiter = options.delimiter.as_bytes()[0];
    let quote = options.quote.as_bytes()[0];
    let escape = options.escape.as_bytes()[0];
    validate_copy_row_width(row, columns)?;
    let mut line = Vec::new();
    for (idx, (value, column)) in row.iter().zip(columns).enumerate() {
        if idx > 0 {
            line.push(delimiter);
        }
        if matches!(value, Value::Null) {
            line.extend_from_slice(options.null.as_bytes());
            continue;
        }
        let text =
            format_text_data_value(value, column, float_format.clone(), None, None, None, None)?
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
    sink.write_all(&line)
}

fn append_csv_field(
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

fn force_quote_column(force_quote: &CopyForceQuote, columns: &[QueryColumn], idx: usize) -> bool {
    match force_quote {
        CopyForceQuote::None => false,
        CopyForceQuote::All => true,
        CopyForceQuote::Columns(names) => names.iter().any(|name| name == &columns[idx].name),
    }
}

fn write_binary_copy_row<S: CopyToSink + ?Sized>(
    sink: &mut S,
    columns: &[QueryColumn],
    row: &[Value],
) -> Result<(), ExecError> {
    validate_copy_row_width(row, columns)?;
    sink.write_all(&(columns.len() as i16).to_be_bytes())?;
    for (value, column) in row.iter().zip(columns) {
        if matches!(value, Value::Null) {
            sink.write_all(&(-1_i32).to_be_bytes())?;
            continue;
        }
        let payload = encode_binary_data_row_value(value, column.sql_type)?;
        sink.write_all(&(payload.len() as i32).to_be_bytes())?;
        sink.write_all(&payload)?;
    }
    Ok(())
}

pub fn begin_copy_to_dml_capture() {
    COPY_TO_DML_CAPTURE.with(|capture| {
        *capture.borrow_mut() = Some(Vec::new());
    });
}

pub fn capture_copy_to_dml_returning_row(row: Vec<Value>) {
    COPY_TO_DML_CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        let Some(events) = capture.as_mut() else {
            return;
        };
        events.push(CopyToDmlEvent::Row(row));
    });
}

pub fn capture_copy_to_dml_notices() {
    COPY_TO_DML_CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        let Some(events) = capture.as_mut() else {
            return;
        };
        drain_copy_to_dml_notices(events);
    });
}

pub fn finish_copy_to_dml_capture() -> Vec<CopyToDmlEvent> {
    COPY_TO_DML_CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        let mut events = capture.take().unwrap_or_default();
        drain_copy_to_dml_notices(&mut events);
        events
    })
}

fn drain_copy_to_dml_notices(events: &mut Vec<CopyToDmlEvent>) {
    for notice in take_backend_notices() {
        events.push(CopyToDmlEvent::Notice(CopyToNotice {
            severity: notice.severity,
            sqlstate: notice.sqlstate,
            message: notice.message,
            detail: notice.detail,
            position: notice.position,
        }));
    }
    for notice in take_plpgsql_notices() {
        if let Some(notice) = normalize_plpgsql_notice(notice) {
            events.push(CopyToDmlEvent::Notice(notice));
        }
    }
}

fn normalize_plpgsql_notice(notice: PlpgsqlNotice) -> Option<CopyToNotice> {
    let (severity, sqlstate) = match notice.level {
        RaiseLevel::Info => ("INFO", "00000"),
        RaiseLevel::Notice => ("NOTICE", "00000"),
        RaiseLevel::Warning => ("WARNING", "01000"),
        RaiseLevel::Exception => return None,
    };
    Some(CopyToNotice {
        severity,
        sqlstate,
        message: notice.message,
        detail: None,
        position: None,
    })
}

fn validate_copy_row_width(row: &[Value], columns: &[QueryColumn]) -> Result<(), ExecError> {
    if row.len() == columns.len() {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: "COPY row has wrong number of columns".into(),
        detail: Some(format!(
            "Expected {} columns but found {}.",
            columns.len(),
            row.len()
        )),
        hint: None,
        sqlstate: "XX000",
    })
}

fn copy_io_error(err: std::io::Error) -> ExecError {
    ExecError::DetailedError {
        message: format!("could not write COPY data: {err}"),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{SqlType, SqlTypeKind};

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
        let mut out = Vec::new();
        let mut sink = IoCopyToSink::new(&mut out);
        write_copy_to(
            &mut sink,
            columns,
            rows,
            &options,
            FloatFormatOptions::default(),
        )
        .unwrap();
        out
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
