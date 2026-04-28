use parking_lot::RwLock;
use std::collections::BTreeMap;

use crate::backend::executor::ExecError;
use crate::include::catalog::PgLargeobjectMetadataRow;
use crate::include::nodes::datum::Value;

#[derive(Debug, Default)]
pub struct LargeObjectRuntime {
    metadata: RwLock<BTreeMap<u32, PgLargeobjectMetadataRow>>,
}

impl LargeObjectRuntime {
    pub(crate) fn new_ephemeral() -> Self {
        Self::default()
    }

    pub(crate) fn create(&self, oid: u32, owner_oid: u32) -> Result<u32, ExecError> {
        let mut metadata = self.metadata.write();
        if metadata.contains_key(&oid) {
            return Err(ExecError::DetailedError {
                message: format!("large object {oid} already exists"),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        metadata.insert(
            oid,
            PgLargeobjectMetadataRow {
                oid,
                lomowner: owner_oid,
                lomacl: Vec::new(),
            },
        );
        Ok(oid)
    }

    pub(crate) fn unlink(&self, oid: u32) -> Result<i32, ExecError> {
        self.metadata
            .write()
            .remove(&oid)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("large object {oid} does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        Ok(1)
    }

    pub(crate) fn metadata_row(&self, oid: u32) -> Option<PgLargeobjectMetadataRow> {
        self.metadata.read().get(&oid).cloned()
    }

    pub(crate) fn metadata_rows(&self) -> Vec<Vec<Value>> {
        self.metadata
            .read()
            .values()
            .cloned()
            .map(|row| {
                vec![
                    Value::Int64(i64::from(row.oid)),
                    Value::Int64(i64::from(row.lomowner)),
                    Value::Null,
                ]
            })
            .collect()
    }
}
