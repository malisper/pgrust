use crate::include::catalog::PgTypeRow;

pub fn is_composite_type(row: &PgTypeRow) -> bool {
    row.typrelid != 0
}
