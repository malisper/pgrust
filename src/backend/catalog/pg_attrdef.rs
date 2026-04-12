use crate::include::catalog::PgAttrdefRow;

pub fn sort_pg_attrdef_rows(rows: &mut [PgAttrdefRow]) {
    rows.sort_by_key(|row| (row.adrelid, row.adnum));
}
