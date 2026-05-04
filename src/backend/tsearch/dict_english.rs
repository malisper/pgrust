#![allow(unused_imports)]

// :HACK: Preserve the historical root text-search path while implementation
// lives in `pgrust_expr`.
pub(crate) use pgrust_expr::backend::tsearch::dict_english::*;
