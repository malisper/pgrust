// :HACK: root compatibility shim while BRIN AM validation lives in
// `pgrust_access`. Long term callers should import from `pgrust_access`
// directly once root access modules are wrapper-only.
pub(crate) fn validate_brin_am() -> bool {
    pgrust_access::brin::validate_brin_am()
}
