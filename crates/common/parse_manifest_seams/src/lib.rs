//! Seam declarations for the streaming backup-manifest JSON parser
//! (`common/parse_manifest.c`): `json_parse_manifest_incremental_init`,
//! `json_parse_manifest_incremental_chunk`, and
//! `json_parse_manifest_incremental_shutdown`.
//!
//! The owning unit (`common/parse_manifest.c`) is not ported yet, so these
//! panic loudly until it lands. The parser is callback-driven in C; here the
//! parser tokenizes and the records it would feed to the
//! `JsonManifestParseContext` content callbacks (version / system-identifier /
//! per-file / per-wal-range) are returned to the caller as a
//! [`ParsedManifestChunk`] for the caller to replay through its own callbacks.
//! A parser error (the C `error_cb`, which `ereport(ERROR)`s) surfaces as
//! `Err`.

use ::types_error::PgResult;
use parse_manifest::{JsonManifestParseIncrementalStateHandle, ParsedManifestChunk};

seam_core::seam!(
    /// `json_parse_manifest_incremental_init(context)` (parse_manifest.c) —
    /// allocate the incremental parser state. The five callbacks the C
    /// `JsonManifestParseContext` carries are owned by the caller and replayed
    /// over the records this parser returns, so they are not passed here.
    /// Returns the opaque parser-state handle.
    pub fn json_parse_manifest_incremental_init() -> JsonManifestParseIncrementalStateHandle
);

seam_core::seam!(
    /// `json_parse_manifest_incremental_chunk(incstate, chunk, size, is_last)`
    /// (parse_manifest.c) — feed the next chunk of manifest text to the
    /// streaming parser, returning the content-callback records it decoded
    /// during this step, in document order. `is_last` marks the final chunk.
    /// `Err` carries the parser's `error_cb` (`ereport(ERROR)`).
    pub fn json_parse_manifest_incremental_chunk(
        incstate: JsonManifestParseIncrementalStateHandle,
        chunk: &[u8],
        is_last: bool,
    ) -> PgResult<ParsedManifestChunk>
);

seam_core::seam!(
    /// `json_parse_manifest_incremental_shutdown(incstate)` (parse_manifest.c)
    /// — release the incremental parser state. Infallible in C.
    pub fn json_parse_manifest_incremental_shutdown(
        incstate: JsonManifestParseIncrementalStateHandle,
    )
);
