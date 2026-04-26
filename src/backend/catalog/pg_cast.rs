use crate::include::catalog::{
    BPCHAR_TYPE_OID, BYTEA_TYPE_OID, CIDR_TYPE_OID, INET_TYPE_OID, PG_DEPENDENCIES_TYPE_OID,
    PG_MCV_LIST_TYPE_OID, PG_NDISTINCT_TYPE_OID, PG_NODE_TREE_TYPE_OID, PgCastRow, TEXT_TYPE_OID,
    VARCHAR_TYPE_OID, XML_TYPE_OID,
};

pub fn sort_pg_cast_rows(rows: &mut [PgCastRow]) {
    rows.sort_by_key(|row| {
        pg_cast_sanity_order(row).unwrap_or((1_000, row.castsource, row.casttarget, row.oid))
    });
}

fn pg_cast_sanity_order(row: &PgCastRow) -> Option<(u32, u32, u32, u32)> {
    let rank = match (row.castsource, row.casttarget, row.castmethod) {
        (TEXT_TYPE_OID, BPCHAR_TYPE_OID, 'b') => 0,
        (VARCHAR_TYPE_OID, BPCHAR_TYPE_OID, 'b') => 1,
        (PG_NODE_TREE_TYPE_OID, TEXT_TYPE_OID, 'b') => 2,
        (PG_NDISTINCT_TYPE_OID, BYTEA_TYPE_OID, 'b') => 3,
        (PG_DEPENDENCIES_TYPE_OID, BYTEA_TYPE_OID, 'b') => 4,
        (PG_MCV_LIST_TYPE_OID, BYTEA_TYPE_OID, 'b') => 5,
        (CIDR_TYPE_OID, INET_TYPE_OID, 'b') => 6,
        (XML_TYPE_OID, TEXT_TYPE_OID, 'b') => 7,
        (XML_TYPE_OID, VARCHAR_TYPE_OID, 'b') => 8,
        (XML_TYPE_OID, BPCHAR_TYPE_OID, 'b') => 9,
        _ => return None,
    };
    Some((rank, row.castsource, row.casttarget, row.oid))
}
