use std::io::Write;

use crate::backend::executor::{ExecError, Value};
use crate::backend::libpq::pqformat::{
    FloatFormatOptions, encode_binary_data_row_value, format_text_data_value,
};
use crate::backend::utils::misc::notices::take_notices as take_backend_notices;
use crate::include::nodes::parsenodes::{CopyFormat, CopyToOptions};
use crate::include::nodes::primnodes::QueryColumn;
use crate::pl::plpgsql::{PlpgsqlNotice, RaiseLevel, take_notices as take_plpgsql_notices};
use pgrust_commands::copyto::{
    CopyToSerializeError, begin_copy_to_bytes, copy_to_row_bytes, finish_copy_to_bytes,
};
pub use pgrust_nodes::{CopyToDmlEvent, CopyToNotice};

thread_local! {
    static COPY_TO_DML_CAPTURE: std::cell::RefCell<Option<Vec<CopyToDmlEvent>>> =
        const { std::cell::RefCell::new(None) };
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
    sink.write_all(&begin_copy_to_bytes(columns, options))?;
    Ok(())
}

pub fn write_copy_to_row<S: CopyToSink + ?Sized>(
    sink: &mut S,
    columns: &[QueryColumn],
    row: &[Value],
    options: &CopyToOptions,
    float_format: &FloatFormatOptions,
) -> Result<(), ExecError> {
    let data = copy_to_row_bytes(
        columns,
        row,
        options,
        |value, column| {
            format_text_data_value(
                value,
                column,
                float_format.clone(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
        },
        |value, column| encode_binary_data_row_value(value, column.sql_type),
    )
    .map_err(copy_to_serialize_error_to_exec)?;
    sink.write_all(&data)
}

pub fn finish_copy_to<S: CopyToSink + ?Sized>(
    sink: &mut S,
    options: &CopyToOptions,
) -> Result<(), ExecError> {
    sink.write_all(&finish_copy_to_bytes(options))?;
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
        RaiseLevel::Log => return None,
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

fn copy_to_serialize_error_to_exec(err: CopyToSerializeError<ExecError>) -> ExecError {
    match err {
        CopyToSerializeError::WrongColumnCount { expected, actual } => ExecError::DetailedError {
            message: "COPY row has wrong number of columns".into(),
            detail: Some(format!("Expected {expected} columns but found {actual}.")),
            hint: None,
            sqlstate: "XX000",
        },
        CopyToSerializeError::Format(err) => err,
    }
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
    use crate::include::nodes::parsenodes::CopyForceQuote;

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
