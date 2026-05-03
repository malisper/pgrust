// :HACK: root compatibility shim while AM catalog validation lives in
// `pgrust_access`. Root only supplies the runtime AM handler registry.
pub fn validate_index_am(am_oid: u32) -> bool {
    pgrust_access::index::amvalidate::validate_index_am(am_oid, |oid| {
        crate::backend::access::index::amapi::index_am_handler(oid).is_some()
    })
}
