// :HACK: Preserve the historical root text-search path while implementation
// lives in `pgrust_expr`.
pub(crate) use pgrust_expr::tsearch::cache::*;
