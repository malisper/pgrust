// :HACK: root compatibility shim while index scan descriptor state lives in
// `pgrust_access`. Long term callers should import from `pgrust_access`
// directly once root access modules are wrapper-only.
pub use pgrust_access::access::relscan::*;
