// :HACK: Preserve the old root CRC path while the portable scalar helper
// lives in pgrust_expr.
pub use pgrust_expr::utils::crc32c::*;
