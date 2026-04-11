use crate::include::catalog::PgAttributeRow;

pub fn attnum_is_user_column(attnum: i16) -> bool {
    attnum > 0
}

pub fn sort_pg_attribute_rows(rows: &mut [PgAttributeRow]) {
    rows.sort_by_key(|row| row.attnum);
}
