use pgrust_catalog_data::PgOpclassRow;

pub fn sort_pg_opclass_rows(rows: &mut [PgOpclassRow]) {
    rows.sort_by_key(|row| (row.opcmethod, row.opcfamily, row.opcname.clone()));
}
