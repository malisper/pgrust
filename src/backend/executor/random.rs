// :HACK: Keep the historical root executor path while the PRNG implementation lives in pgrust_expr.
pub use pgrust_expr::backend::executor::random::PgPrngState;
