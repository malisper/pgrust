use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value as JsonValue, json};

use crate::backend::catalog::catalog::{Catalog, CatalogError};

pub type DefaultExprMap = BTreeMap<(u32, i16), String>;

fn defaults_path(base_dir: &Path) -> PathBuf {
    base_dir.join("catalog").join("defaults.json")
}

pub fn load_default_exprs(base_dir: &Path) -> Result<DefaultExprMap, CatalogError> {
    let path = defaults_path(base_dir);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let text = fs::read_to_string(&path).map_err(|e| CatalogError::Io(e.to_string()))?;
    let json = serde_json::from_str::<JsonValue>(&text)
        .map_err(|_| CatalogError::Corrupt("invalid defaults json"))?;
    let Some(entries) = json.as_array() else {
        return Err(CatalogError::Corrupt("invalid defaults json root"));
    };

    let mut defaults = BTreeMap::new();
    for entry in entries {
        let relation_oid = entry
            .get("relation_oid")
            .and_then(JsonValue::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or(CatalogError::Corrupt("invalid default relation oid"))?;
        let attnum = entry
            .get("attnum")
            .and_then(JsonValue::as_i64)
            .and_then(|v| i16::try_from(v).ok())
            .ok_or(CatalogError::Corrupt("invalid default attnum"))?;
        let expr = entry
            .get("expr")
            .and_then(JsonValue::as_str)
            .ok_or(CatalogError::Corrupt("invalid default expr"))?;
        defaults.insert((relation_oid, attnum), expr.to_string());
    }

    Ok(defaults)
}

pub fn persist_default_exprs(base_dir: &Path, catalog: &Catalog) -> Result<(), CatalogError> {
    let path = defaults_path(base_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| CatalogError::Io(e.to_string()))?;
    }

    let entries = catalog
        .entries()
        .flat_map(|(_, entry)| {
            entry
                .desc
                .columns
                .iter()
                .enumerate()
                .filter_map(move |(index, column)| {
                    column.default_expr.as_ref().map(|expr| {
                        json!({
                            "relation_oid": entry.relation_oid,
                            "attnum": (index as i16) + 1,
                            "expr": expr,
                        })
                    })
                })
        })
        .collect::<Vec<_>>();

    let json = serde_json::to_string_pretty(&entries)
        .map_err(|_| CatalogError::Corrupt("unable to encode defaults json"))?;
    fs::write(path, json).map_err(|e| CatalogError::Io(e.to_string()))
}
