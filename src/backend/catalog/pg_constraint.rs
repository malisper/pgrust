use crate::backend::executor::RelationDesc;
use crate::include::catalog::{CONSTRAINT_NOTNULL, PgConstraintRow};

pub fn sort_pg_constraint_rows(rows: &mut [PgConstraintRow]) {
    rows.sort_by(|left, right| {
        left.connamespace
            .cmp(&right.connamespace)
            .then_with(|| left.conrelid.cmp(&right.conrelid))
            .then_with(|| left.conname.cmp(&right.conname))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}

pub fn derived_pg_constraint_rows(
    relation_oid: u32,
    relation_name: &str,
    namespace_oid: u32,
    desc: &RelationDesc,
) -> Vec<PgConstraintRow> {
    desc.columns
        .iter()
        .enumerate()
        .filter(|(_, column)| !column.storage.nullable)
        .map(|(index, column)| {
            let attnum = index.saturating_add(1) as i16;
            PgConstraintRow {
                // :HACK: NOT NULL constraints are still derived from column metadata rather than
                // stored as first-class catalog objects, so use a stable synthetic OID until DDL
                // allocates and persists pg_constraint rows directly.
                oid: synthetic_not_null_constraint_oid(relation_oid, attnum),
                conname: format!("{relation_name}_{}_not_null", column.name),
                connamespace: namespace_oid,
                contype: CONSTRAINT_NOTNULL,
                condeferrable: false,
                condeferred: false,
                conenforced: true,
                convalidated: true,
                conrelid: relation_oid,
                contypid: 0,
                conindid: 0,
                conparentid: 0,
                confrelid: 0,
                confupdtype: ' ',
                confdeltype: ' ',
                confmatchtype: ' ',
                conislocal: true,
                coninhcount: 0,
                connoinherit: false,
                conperiod: false,
            }
        })
        .collect()
}

fn synthetic_not_null_constraint_oid(relation_oid: u32, attnum: i16) -> u32 {
    0x4e4e_0000 ^ relation_oid.rotate_left(7) ^ u32::from(attnum as u16)
}
