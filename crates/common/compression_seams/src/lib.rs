//! Seam declarations for `common/compression.c` (PostgreSQL 18.3), the shared
//! compression-specification parser used by both frontend and backend.
//!
//! `common/compression.c` is not yet ported in this repo (it lives in the
//! unported `common-batch*` units). Its three public entry points are declared
//! here (the consumer-owned seam convention) and are installed by that unit's
//! `init_seams()` once it lands; until then a call panics loudly
//! (mirror-PG-and-panic). The shared types live in `types-compression`.

use ::compression::{PgCompressAlgorithm, PgCompressSpecification};

seam_core::seam!(
    /// `parse_compress_algorithm(name, &algorithm)` (compression.c) — map a
    /// compression-algorithm name to its enum value. Returns `Some(alg)` on a
    /// recognized name (`"gzip"`/`"lz4"`/`"zstd"`/`"none"`), `None` otherwise
    /// (the C `false` return).
    pub fn parse_compress_algorithm(name: &str) -> Option<PgCompressAlgorithm>
);

seam_core::seam!(
    /// `parse_compress_specification(algorithm, specification, result)`
    /// (compression.c) — parse the optional detail string (e.g.
    /// `"level=9,workers=4"`) for `algorithm` into a fully-populated
    /// [`PgCompressSpecification`]. A parse problem is reported via the result's
    /// `parse_error` field (the C `result->parse_error`), not a separate return.
    /// `specification == None` matches the C `NULL` detail string.
    pub fn parse_compress_specification(
        algorithm: PgCompressAlgorithm,
        specification: Option<&str>,
    ) -> PgCompressSpecification
);

seam_core::seam!(
    /// `validate_compress_specification(spec)` (compression.c) — check a parsed
    /// specification for semantic validity. Returns `Some(error_detail)` if the
    /// specification is invalid (the C non-NULL `char *` return), `None` if it
    /// is acceptable.
    pub fn validate_compress_specification(spec: &PgCompressSpecification) -> Option<String>
);
