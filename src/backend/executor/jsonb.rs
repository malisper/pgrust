// :HACK: root compatibility shim while JSONB parsing/rendering lives in
// `pgrust_expr`.
pub use pgrust_expr::jsonb::*;
