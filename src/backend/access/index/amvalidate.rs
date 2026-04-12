pub fn validate_index_am(am_oid: u32) -> bool {
    crate::backend::access::index::amapi::index_am_handler(am_oid).is_some()
}
