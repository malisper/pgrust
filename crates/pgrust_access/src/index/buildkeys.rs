use pgrust_catalog_data::{BOX_TYPE_OID, GIST_AM_OID, GTSVECTOR_TYPE_OID};
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::RelationDesc;

use crate::gist::support::geometry_ops::circle_bound_box;
use crate::{AccessError, AccessResult};

pub fn project_index_key_values(
    index_desc: &RelationDesc,
    indkey: &[i16],
    row_values: &[Value],
    expr_values: &[Value],
) -> AccessResult<Vec<Value>> {
    project_index_key_values_with_opckeytypes(index_desc, indkey, 0, &[], row_values, expr_values)
}

pub fn project_index_key_values_with_opckeytypes(
    index_desc: &RelationDesc,
    indkey: &[i16],
    am_oid: u32,
    opckeytype_oids: &[u32],
    row_values: &[Value],
    expr_values: &[Value],
) -> AccessResult<Vec<Value>> {
    let mut keys = Vec::with_capacity(index_desc.columns.len());
    let mut expr_iter = expr_values.iter();
    for (key_pos, attnum) in indkey.iter().enumerate() {
        let value = if *attnum > 0 {
            let idx = attnum.saturating_sub(1) as usize;
            row_values
                .get(idx)
                .cloned()
                .ok_or(AccessError::Corrupt("index key attnum out of range"))?
        } else {
            expr_iter.next().cloned().ok_or(AccessError::Corrupt(
                "missing projected index expression value",
            ))?
        };
        keys.push(coerce_index_key_to_opckeytype(
            value,
            am_oid,
            opckeytype_oids.get(key_pos).copied(),
        ));
    }
    Ok(keys)
}

pub fn coerce_index_key_to_opckeytype(
    value: Value,
    am_oid: u32,
    opckeytype_oid: Option<u32>,
) -> Value {
    if am_oid != GIST_AM_OID {
        return value;
    }
    match opckeytype_oid {
        Some(BOX_TYPE_OID) => match value {
            Value::Polygon(poly) => Value::Box(poly.bound_box),
            Value::Circle(circle) => Value::Box(circle_bound_box(&circle)),
            other => other,
        },
        Some(GTSVECTOR_TYPE_OID) => match value {
            Value::Null => Value::Null,
            Value::TsVector(_) => {
                // :HACK: pgrust's current GiST tsvector support is lossy and
                // always heap-rechecks. Store a compact gtsvector placeholder
                // instead of raw tsvector data so leaf tuples fit on pages.
                Value::TsVector(Default::default())
            }
            other => other,
        },
        _ => value,
    }
}
