pub const PG_TOAST_NAMESPACE: &str = "pg_toast";

pub fn toast_relation_name(rel_oid: u32) -> String {
    format!("pg_toast_{rel_oid}")
}

pub fn toast_index_name(rel_oid: u32) -> String {
    format!("pg_toast_{rel_oid}_index")
}
