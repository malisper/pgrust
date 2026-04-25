use wasm_bindgen::prelude::*;

use crate::backend::executor::{StatementResult, Value};
use crate::backend::libpq::pqformat::{format_exec_error, infer_command_tag};
use crate::pgrust::database::{Database, Session};
use crate::pgrust::session::ByteaOutputFormat;

#[wasm_bindgen]
pub struct WasmEngine {
    db: Database,
    session: Session,
}

#[wasm_bindgen]
impl WasmEngine {
    #[wasm_bindgen(constructor)]
    pub fn new(pool_size: Option<usize>) -> Result<WasmEngine, JsValue> {
        console_error_panic_hook::set_once();
        let db = Database::open_ephemeral(pool_size.unwrap_or(64))
            .map_err(|err| JsValue::from_str(&format!("{err:?}")))?;
        Ok(Self {
            db,
            session: Session::new(1),
        })
    }

    pub fn execute(&mut self, sql: &str) -> Result<String, JsValue> {
        match self.session.execute(&self.db, sql) {
            Ok(result) => Ok(result_to_json(sql, result)),
            Err(err) => {
                let message = format_exec_error(&err);
                let escaped = serde_json::to_string(&message)
                    .unwrap_or_else(|_| "\"query failed\"".to_string());
                Err(JsValue::from_str(&format!(
                    "{{\"ok\":false,\"error\":{escaped}}}"
                )))
            }
        }
    }

    pub fn reset(&mut self, pool_size: Option<usize>) -> Result<(), JsValue> {
        self.db = Database::open_ephemeral(pool_size.unwrap_or(64))
            .map_err(|err| JsValue::from_str(&format!("{err:?}")))?;
        self.session = Session::new(1);
        Ok(())
    }
}

fn result_to_json(sql: &str, result: StatementResult) -> String {
    match result {
        StatementResult::Query {
            columns,
            column_names,
            rows,
        } => {
            let columns = column_names
                .into_iter()
                .zip(columns)
                .map(|(name, column)| {
                    format!(
                        "{{\"name\":{},\"type\":{}}}",
                        json_string(&name),
                        json_string(&format!("{:?}", column.sql_type))
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let rows = rows
                .into_iter()
                .map(|row| {
                    format!(
                        "[{}]",
                        row.iter()
                            .map(render_value_json)
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "{{\"ok\":true,\"tag\":{},\"columns\":[{}],\"rows\":[{}]}}",
                json_string(&infer_command_tag(sql, 0)),
                columns,
                rows
            )
        }
        StatementResult::AffectedRows(count) => format!(
            "{{\"ok\":true,\"tag\":{},\"affected_rows\":{count}}}",
            json_string(&infer_command_tag(sql, count))
        ),
    }
}

fn render_value_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) if v.is_finite() => v.to_string(),
        Value::Numeric(v) => json_string(&v.render()),
        Value::Text(v) => json_string(v.as_str()),
        Value::TextRef(_, _) => json_string(value.as_text().unwrap_or("")),
        Value::Json(v) => v.to_string(),
        Value::Jsonb(v) => json_string(
            &crate::backend::executor::jsonb::render_jsonb_bytes(v)
                .unwrap_or_else(|_| "null".to_string()),
        ),
        Value::JsonPath(v) => json_string(v.as_str()),
        Value::Bytea(v) => json_string(&crate::backend::libpq::pqformat::format_bytea_text(
            v,
            ByteaOutputFormat::Hex,
        )),
        Value::Uuid(v) => json_string(&crate::backend::executor::render_uuid_text(v)),
        Value::Inet(v) => json_string(&v.render_inet()),
        Value::Cidr(v) => json_string(&v.render_cidr()),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => json_string(
            &crate::backend::executor::render_datetime_value_text(value).unwrap_or_default(),
        ),
        Value::Bit(v) => json_string(&v.render()),
        Value::TsVector(v) => json_string(&v.render()),
        Value::TsQuery(v) => json_string(&v.render()),
        Value::InternalChar(v) => {
            json_string(&crate::backend::executor::render_internal_char_text(*v))
        }
        Value::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(render_value_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::PgArray(array) => {
            json_string(&crate::backend::executor::format_array_value_text(array))
        }
        other => json_string(&format!("{other:?}")),
    }
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

#[cfg(test)]
mod tests {
    use super::render_value_json;
    use crate::backend::executor::Value;

    #[test]
    fn render_value_json_formats_jsonb_textually() {
        let value = Value::Jsonb(
            crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":1,\"b\":[true,null]}")
                .unwrap(),
        );
        assert_eq!(
            render_value_json(&value),
            "\"{\\\"a\\\":1,\\\"b\\\":[true,null]}\""
        );
    }
}
