//! C: src/common/parse_manifest.c — parse a backup manifest in JSON format.
//!
//! Two entry points, as in C: the whole-buffer `json_parse_manifest`, and the
//! incremental `json_parse_manifest_incremental_init` / `_chunk` /
//! `_shutdown` triple ([`JsonManifestParseIncrementalState`]) over
//! `adt_json::jsonapi::incremental::pg_parse_json_incremental`, which the
//! walsender's UPLOAD_MANIFEST chunk loop uses so that a manifest of any size
//! streams through a bounded staging buffer (basebackup_incremental.c
//! MIN_CHUNK/MAX_CHUNK) and the checksum covering everything but its last
//! line is accumulated chunk by chunk.
//!
//! Error identity: every failure funnels through the context's `error_cb`
//! with the exact C message text. Low-level shape errors carry C's
//! `could not parse backup manifest: <msg>` prefix
//! (json_manifest_parse_failure); the direct error_cb cases
//! (`unrecognized checksum algorithm: ...`, `invalid checksum for file ...`,
//! `manifest has no checksum`, `invalid manifest checksum: ...`,
//! `manifest checksum mismatch`) do not. The default error_cb maps to a
//! plain ERROR (ERRCODE_INTERNAL_ERROR), matching the server consumer's
//! `errmsg_internal` wrapper in basebackup_incremental.c; frontend-style
//! consumers can override error_cb to re-code.

use adt_json::jsonapi::incremental::JsonLexIncremental;
use adt_json::jsonapi::{parse_sem, JsonError, JsonLexDe, JsonSem, JsonSemToken};
use manifest::{pg_checksum_parse_type, PgChecksumType};
use mcx::Mcx;
use pg_sha2::{PgSha256Ctx, PG_SHA256_DIGEST_LENGTH};
use pg_string::isspace_c_locale;
use types_core::{TimeLineID, XLogRecPtr};
use types_error::{PgError, PgResult};

/// C: the JsonManifestParseContext callback table. Callbacks may fail with a
/// PgError (C's callbacks ereport and never return; Rust propagates).
pub trait JsonManifestParseContext {
    fn version_cb(&mut self, manifest_version: i32) -> PgResult<()>;
    fn system_identifier_cb(&mut self, manifest_system_identifier: u64) -> PgResult<()>;
    fn per_file_cb(
        &mut self,
        pathname: &[u8],
        size: u64,
        checksum_type: PgChecksumType,
        checksum_payload: Option<&[u8]>,
    ) -> PgResult<()>;
    fn per_wal_range_cb(
        &mut self,
        tli: TimeLineID,
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
    ) -> PgResult<()>;
    /// C: error_cb — builds the error the parse fails with. The message is
    /// the fully formatted C text; consumers wanting a different errcode or
    /// prefix override this.
    fn error_cb(&mut self, msg: String) -> Box<PgError> {
        PgError::error(msg).into()
    }
}

/// A borrowed context is a context (C passes `JsonManifestParseContext *`).
impl<T: JsonManifestParseContext + ?Sized> JsonManifestParseContext for &mut T {
    fn version_cb(&mut self, manifest_version: i32) -> PgResult<()> {
        (**self).version_cb(manifest_version)
    }
    fn system_identifier_cb(&mut self, manifest_system_identifier: u64) -> PgResult<()> {
        (**self).system_identifier_cb(manifest_system_identifier)
    }
    fn per_file_cb(
        &mut self,
        pathname: &[u8],
        size: u64,
        checksum_type: PgChecksumType,
        checksum_payload: Option<&[u8]>,
    ) -> PgResult<()> {
        (**self).per_file_cb(pathname, size, checksum_type, checksum_payload)
    }
    fn per_wal_range_cb(
        &mut self,
        tli: TimeLineID,
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
    ) -> PgResult<()> {
        (**self).per_wal_range_cb(tli, start_lsn, end_lsn)
    }
    fn error_cb(&mut self, msg: String) -> Box<PgError> {
        (**self).error_cb(msg)
    }
}

/// C: JsonManifestSemanticState.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SemState {
    ExpectToplevelStart,
    ExpectToplevelEnd,
    ExpectToplevelField,
    ExpectVersionValue,
    ExpectSystemIdentifierValue,
    ExpectFilesStart,
    ExpectFilesNext,
    ExpectThisFileField,
    ExpectThisFileValue,
    ExpectWalRangesStart,
    ExpectWalRangesNext,
    ExpectThisWalRangeField,
    ExpectThisWalRangeValue,
    ExpectManifestChecksumValue,
    ExpectEof,
}

/// C: JsonManifestFileField.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FileField {
    Path,
    EncodedPath,
    Size,
    LastModified,
    ChecksumAlgorithm,
    Checksum,
}

/// C: JsonManifestWALRangeField.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WalRangeField {
    Timeline,
    StartLsn,
    EndLsn,
}

/// C: JsonManifestParseState. The token fields are owned copies (C: the
/// lexer hands the semantic actions palloc'd strings, which they keep until
/// the enclosing object is finalized), so the same state serves the
/// whole-buffer parser and the incremental one, whose token buffers do not
/// outlive the chunk.
struct JsonManifestParseState<C: JsonManifestParseContext> {
    context: C,
    state: SemState,

    /* fields used for parsing objects in the list of files */
    file_field: FileField,
    pathname: Option<Vec<u8>>,
    encoded_pathname: Option<Vec<u8>>,
    size: Option<Vec<u8>>,
    algorithm: Option<Vec<u8>>,
    checksum: Option<Vec<u8>>,

    /* fields used for parsing objects in the list of WAL ranges */
    wal_range_field: WalRangeField,
    timeline: Option<Vec<u8>>,
    start_lsn: Option<Vec<u8>>,
    end_lsn: Option<Vec<u8>>,

    /* miscellaneous other stuff */
    saw_version_field: bool,
    manifest_checksum: Option<Vec<u8>>,
}

impl<C: JsonManifestParseContext> JsonManifestParseState<C> {
    /// C: the `parse->context = context; parse->state =
    /// JM_EXPECT_TOPLEVEL_START; parse->saw_version_field = false` setup
    /// shared by json_parse_manifest and json_parse_manifest_incremental_init.
    fn new(context: C) -> Self {
        JsonManifestParseState {
            context,
            state: SemState::ExpectToplevelStart,
            file_field: FileField::Path,
            pathname: None,
            encoded_pathname: None,
            size: None,
            algorithm: None,
            checksum: None,
            wal_range_field: WalRangeField::Timeline,
            timeline: None,
            start_lsn: None,
            end_lsn: None,
            saw_version_field: false,
            manifest_checksum: None,
        }
    }

    /// C: json_manifest_parse_failure.
    fn parse_failure<T>(&mut self, msg: impl core::fmt::Display) -> PgResult<T> {
        Err(self
            .context
            .error_cb(format!("could not parse backup manifest: {msg}")))
    }

    /// C: json_manifest_finalize_version.
    fn finalize_version(&mut self, token: &[u8]) -> PgResult<()> {
        debug_assert!(self.saw_version_field);

        /* Parse version. C: strtoi64(..., 10) then assignment to int. */
        let (version64, consumed) = c_strtoi64(token);
        if consumed != token.len() {
            return self.parse_failure("manifest version not an integer");
        }
        /* C truncates the int64 through the `int version` assignment. */
        let version = version64 as i32;

        if version != 1 && version != 2 {
            return self.parse_failure("unexpected manifest version");
        }

        self.context.version_cb(version)
    }

    /// C: json_manifest_finalize_system_identifier.
    fn finalize_system_identifier(&mut self, token: &[u8]) -> PgResult<()> {
        let (system_identifier, consumed) = c_strtou64(token);
        if consumed != token.len() {
            return self.parse_failure("system identifier in manifest not an integer");
        }
        self.context.system_identifier_cb(system_identifier)
    }

    /// C: json_manifest_finalize_file.
    fn finalize_file(&mut self) -> PgResult<()> {
        /* Pathname and size are required. */
        if self.pathname.is_none() && self.encoded_pathname.is_none() {
            return self.parse_failure("missing path name");
        }
        if self.pathname.is_some() && self.encoded_pathname.is_some() {
            return self.parse_failure("both path name and encoded path name");
        }
        let Some(size_token) = self.size.take() else {
            return self.parse_failure("missing size");
        };
        if self.algorithm.is_none() && self.checksum.is_some() {
            return self.parse_failure("checksum without algorithm");
        }

        /* Decode encoded pathname, if that's what we have. */
        let pathname: Vec<u8> = match self.encoded_pathname.take() {
            Some(encoded) => match hexdecode_string(&encoded) {
                Some(raw) => raw,
                None => return self.parse_failure("could not decode file name"),
            },
            None => self.pathname.take().expect("checked above"),
        };

        /* Parse size. */
        let (size, consumed) = c_strtou64(&size_token);
        if consumed != size_token.len() {
            return self.parse_failure("file size is not an integer");
        }

        /* Parse the checksum algorithm, if it's present. */
        let algorithm = self.algorithm.take();
        let checksum_type = match algorithm.as_deref() {
            None => PgChecksumType::None,
            Some(algorithm) => match pg_checksum_parse_type(algorithm) {
                Some(ty) => ty,
                None => {
                    return Err(self.context.error_cb(format!(
                        "unrecognized checksum algorithm: \"{}\"",
                        String::from_utf8_lossy(algorithm)
                    )))
                }
            },
        };

        /* Parse the checksum payload, if it's present. */
        let checksum = self.checksum.take();
        let checksum_payload: Option<Vec<u8>> = match checksum.as_deref() {
            None => None,
            Some(checksum) if checksum.is_empty() => None,
            Some(checksum) => match hexdecode_string(checksum) {
                Some(payload) => Some(payload),
                None => {
                    return Err(self.context.error_cb(format!(
                        "invalid checksum for file \"{}\": \"{}\"",
                        String::from_utf8_lossy(&pathname),
                        String::from_utf8_lossy(checksum)
                    )))
                }
            },
        };

        /* Invoke the callback with the details we've gathered. C pfrees
         * size/algorithm/checksum afterwards; taken above, so the state
         * never dangles. */
        self.context
            .per_file_cb(&pathname, size, checksum_type, checksum_payload.as_deref())
    }

    /// C: json_manifest_finalize_wal_range.
    fn finalize_wal_range(&mut self) -> PgResult<()> {
        /* Make sure all fields are present. */
        if self.timeline.is_none() {
            return self.parse_failure("missing timeline");
        }
        if self.start_lsn.is_none() {
            return self.parse_failure("missing start LSN");
        }
        if self.end_lsn.is_none() {
            return self.parse_failure("missing end LSN");
        }
        let timeline_token = self.timeline.take().expect("checked above");
        let start_lsn_token = self.start_lsn.take().expect("checked above");
        let end_lsn_token = self.end_lsn.take().expect("checked above");

        /* Parse timeline. C: strtoul(..., 10) assigned to a uint32 TLI. */
        let (tli64, consumed) = c_strtou64(&timeline_token);
        if consumed != timeline_token.len() {
            return self.parse_failure("timeline is not an integer");
        }
        let tli = tli64 as TimeLineID;
        let Some(start_lsn) = parse_xlogrecptr(&start_lsn_token) else {
            return self.parse_failure("could not parse start LSN");
        };
        let Some(end_lsn) = parse_xlogrecptr(&end_lsn_token) else {
            return self.parse_failure("could not parse end LSN");
        };

        /* Invoke the callback with the details we've gathered. */
        self.context.per_wal_range_cb(tli, start_lsn, end_lsn)
    }
}

impl<'m, C: JsonManifestParseContext> JsonSem<'m> for JsonManifestParseState<C> {
    /// C: json_manifest_object_start.
    fn object_start(&mut self, _lex: &adt_json::jsonapi::JsonLex<'_>) -> PgResult<bool> {
        match self.state {
            SemState::ExpectToplevelStart => {
                self.state = SemState::ExpectToplevelField;
            }
            SemState::ExpectFilesNext => {
                self.state = SemState::ExpectThisFileField;
                self.pathname = None;
                self.encoded_pathname = None;
                self.size = None;
                self.algorithm = None;
                self.checksum = None;
            }
            SemState::ExpectWalRangesNext => {
                self.state = SemState::ExpectThisWalRangeField;
                self.timeline = None;
                self.start_lsn = None;
                self.end_lsn = None;
            }
            _ => return self.parse_failure("unexpected object start"),
        }
        Ok(true)
    }

    /// C: json_manifest_object_end.
    fn object_end(&mut self, _lex: &adt_json::jsonapi::JsonLex<'_>) -> PgResult<bool> {
        match self.state {
            SemState::ExpectToplevelEnd => {
                self.state = SemState::ExpectEof;
            }
            SemState::ExpectThisFileField => {
                self.finalize_file()?;
                self.state = SemState::ExpectFilesNext;
            }
            SemState::ExpectThisWalRangeField => {
                self.finalize_wal_range()?;
                self.state = SemState::ExpectWalRangesNext;
            }
            _ => return self.parse_failure("unexpected object end"),
        }
        Ok(true)
    }

    /// C: json_manifest_array_start.
    fn array_start(&mut self, _lex: &adt_json::jsonapi::JsonLex<'_>) -> PgResult<bool> {
        match self.state {
            SemState::ExpectFilesStart => self.state = SemState::ExpectFilesNext,
            SemState::ExpectWalRangesStart => self.state = SemState::ExpectWalRangesNext,
            _ => return self.parse_failure("unexpected array start"),
        }
        Ok(true)
    }

    /// C: json_manifest_array_end.
    fn array_end(&mut self, _lex: &adt_json::jsonapi::JsonLex<'_>) -> PgResult<bool> {
        match self.state {
            SemState::ExpectFilesNext | SemState::ExpectWalRangesNext => {
                self.state = SemState::ExpectToplevelField;
            }
            _ => return self.parse_failure("unexpected array end"),
        }
        Ok(true)
    }

    /// C: json_manifest_object_field_start.
    fn object_field_start(
        &mut self,
        _lex: &adt_json::jsonapi::JsonLex<'_>,
        fname: &'m [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        match self.state {
            SemState::ExpectToplevelField => {
                /*
                 * Inside toplevel object. The version indicator should always
                 * be the first field.
                 */
                if !self.saw_version_field {
                    if fname != b"PostgreSQL-Backup-Manifest-Version" {
                        return self.parse_failure("expected version indicator");
                    }
                    self.state = SemState::ExpectVersionValue;
                    self.saw_version_field = true;
                } else if fname == b"System-Identifier" {
                    self.state = SemState::ExpectSystemIdentifierValue;
                } else if fname == b"Files" {
                    self.state = SemState::ExpectFilesStart;
                } else if fname == b"WAL-Ranges" {
                    self.state = SemState::ExpectWalRangesStart;
                } else if fname == b"Manifest-Checksum" {
                    self.state = SemState::ExpectManifestChecksumValue;
                } else {
                    return self.parse_failure("unrecognized top-level field");
                }
            }
            SemState::ExpectThisFileField => {
                self.file_field = match fname {
                    b"Path" => FileField::Path,
                    b"Encoded-Path" => FileField::EncodedPath,
                    b"Size" => FileField::Size,
                    b"Last-Modified" => FileField::LastModified,
                    b"Checksum-Algorithm" => FileField::ChecksumAlgorithm,
                    b"Checksum" => FileField::Checksum,
                    _ => return self.parse_failure("unexpected file field"),
                };
                self.state = SemState::ExpectThisFileValue;
            }
            SemState::ExpectThisWalRangeField => {
                self.wal_range_field = match fname {
                    b"Timeline" => WalRangeField::Timeline,
                    b"Start-LSN" => WalRangeField::StartLsn,
                    b"End-LSN" => WalRangeField::EndLsn,
                    _ => return self.parse_failure("unexpected WAL range field"),
                };
                self.state = SemState::ExpectThisWalRangeValue;
            }
            _ => return self.parse_failure("unexpected object field"),
        }
        Ok(true)
    }

    /// C: json_manifest_scalar.
    fn scalar(
        &mut self,
        _lex: &adt_json::jsonapi::JsonLex<'_>,
        token: JsonSemToken<'m>,
    ) -> PgResult<bool> {
        let token: &[u8] = match token {
            JsonSemToken::String(s) => s,
            JsonSemToken::Number(n) => n,
            /* C's need_escapes lexer hands the raw lexemes for these. */
            JsonSemToken::True => b"true",
            JsonSemToken::False => b"false",
            JsonSemToken::Null => b"null",
        };
        match self.state {
            SemState::ExpectVersionValue => {
                self.finalize_version(token)?;
                self.state = SemState::ExpectToplevelField;
            }
            SemState::ExpectSystemIdentifierValue => {
                self.finalize_system_identifier(token)?;
                self.state = SemState::ExpectToplevelField;
            }
            SemState::ExpectThisFileValue => {
                match self.file_field {
                    FileField::Path => self.pathname = Some(token.to_vec()),
                    FileField::EncodedPath => self.encoded_pathname = Some(token.to_vec()),
                    FileField::Size => self.size = Some(token.to_vec()),
                    FileField::LastModified => { /* unused */ }
                    FileField::ChecksumAlgorithm => self.algorithm = Some(token.to_vec()),
                    FileField::Checksum => self.checksum = Some(token.to_vec()),
                }
                self.state = SemState::ExpectThisFileField;
            }
            SemState::ExpectThisWalRangeValue => {
                match self.wal_range_field {
                    WalRangeField::Timeline => self.timeline = Some(token.to_vec()),
                    WalRangeField::StartLsn => self.start_lsn = Some(token.to_vec()),
                    WalRangeField::EndLsn => self.end_lsn = Some(token.to_vec()),
                }
                self.state = SemState::ExpectThisWalRangeField;
            }
            SemState::ExpectManifestChecksumValue => {
                self.state = SemState::ExpectToplevelEnd;
                self.manifest_checksum = Some(token.to_vec());
            }
            _ => return self.parse_failure("unexpected scalar"),
        }
        Ok(true)
    }
}

/// C: JsonManifestParseIncrementalState — the state for parsing a manifest
/// in pieces: the incremental JSON lexer, the semantic state (C: `sem` with
/// its `semstate`), and the running checksum over every chunk but the last
/// line. Built by [`Self::json_parse_manifest_incremental_init`], fed by
/// [`Self::json_parse_manifest_incremental_chunk`], torn down by
/// [`Self::json_parse_manifest_incremental_shutdown`], which hands the
/// context back.
pub struct JsonManifestParseIncrementalState<C: JsonManifestParseContext> {
    lex: JsonLexIncremental,
    parse: JsonManifestParseState<C>,
    /// C: incstate->manifest_ctx; consumed by the final chunk's
    /// verify_manifest_checksum (C: pg_cryptohash_free there).
    manifest_ctx: Option<PgSha256Ctx>,
}

impl<C: JsonManifestParseContext> JsonManifestParseIncrementalState<C> {
    /// C: json_parse_manifest_incremental_init(context) — set up for
    /// incremental parsing of the manifest (parse_manifest.c:129).
    pub fn json_parse_manifest_incremental_init(context: C) -> Self {
        JsonManifestParseIncrementalState {
            /* C: makeJsonLexContextIncremental(&lex, PG_UTF8, true) */
            lex: JsonLexIncremental::new(wchar::PG_UTF8, true),
            parse: JsonManifestParseState::new(context),
            manifest_ctx: Some(PgSha256Ctx::init_sha256()),
        }
    }

    /// C: json_parse_manifest_incremental_shutdown — free the state; the
    /// context it was built over is returned to the caller.
    pub fn json_parse_manifest_incremental_shutdown(self) -> C {
        self.parse.context
    }

    /// The context this parse reports into (C: sem.semstate->context).
    pub fn context(&self) -> &C {
        &self.parse.context
    }

    /// Mutable access to the context (see [`Self::context`]).
    pub fn context_mut(&mut self) -> &mut C {
        &mut self.parse.context
    }

    /// C: json_parse_manifest_incremental_chunk(incstate, chunk, size,
    /// is_last) — parse the manifest in pieces (parse_manifest.c:185). The
    /// caller must ensure that the final piece contains the final lines with
    /// the complete checksum. `mcx` backs the chunk's token scratch.
    pub fn json_parse_manifest_incremental_chunk(
        &mut self,
        mcx: Mcx<'_>,
        chunk: &[u8],
        is_last: bool,
    ) -> PgResult<()> {
        {
            let mut ch = self.lex.chunk(mcx, chunk, is_last);
            let res = ch.parse(&mut self.parse)?;

            let expected = if is_last { JsonError::Success } else { JsonError::Incomplete };

            if res != expected {
                let detail = ch.errdetail(res);
                return self.parse.parse_failure(detail);
            }
        }

        if is_last && self.parse.state != SemState::ExpectEof {
            return self.parse.parse_failure("manifest ended unexpectedly");
        }

        if !is_last {
            self.manifest_ctx
                .as_mut()
                .expect("checksum context lives until the last chunk")
                .update(chunk);
            Ok(())
        } else {
            let incr_ctx = self.manifest_ctx.take();
            verify_manifest_checksum(
                self.parse.manifest_checksum.as_deref(),
                &mut self.parse.context,
                chunk,
                incr_ctx,
            )
        }
    }
}

/// C: json_parse_manifest — main entrypoint to parse a JSON-format backup
/// manifest. For each file whose information is extracted from the manifest,
/// `context.per_file_cb` is invoked; likewise `per_wal_range_cb` per WAL
/// range. On any problem the error built by `context.error_cb` is returned.
///
/// `mcx` backs the de-escaped string tokens for the duration of the parse
/// (C's lexer pallocs into the current memory context); pass a short-lived
/// context — roughly the manifest text size is retained until it drops.
pub fn json_parse_manifest<C: JsonManifestParseContext + ?Sized>(
    mcx: Mcx<'_>,
    context: &mut C,
    buffer: &[u8],
) -> PgResult<()> {
    /* Set up our private parsing context. */
    let mut parse = JsonManifestParseState::new(context);

    /* Create a JSON lexing context (C: PG_UTF8, need_escapes=true). */
    let mut lex = JsonLexDe::new(mcx, buffer, wchar::PG_UTF8);

    /* Run the actual JSON parser. */
    let json_error = parse_sem(&mut lex, &mut parse)?;
    if json_error != JsonError::Success {
        let detail = lex.lex.errdetail(json_error);
        return parse.parse_failure(detail);
    }
    if parse.state != SemState::ExpectEof {
        return parse.parse_failure("manifest ended unexpectedly");
    }

    /* Verify the manifest checksum. */
    verify_manifest_checksum(parse.manifest_checksum.as_deref(), parse.context, buffer, None)
}

/// C: verify_manifest_checksum. The last line of the manifest file is
/// excluded from the manifest checksum, because the last line is expected to
/// contain the checksum that covers the rest of the file.
///
/// For an incremental parse, this is called on the last chunk of the
/// manifest only, with the cryptohash context that already covers every
/// earlier chunk passed in (`incr_ctx`); for a non-incremental parse
/// `incr_ctx` is None (C: NULL) and `buffer` is the whole manifest.
fn verify_manifest_checksum<C: JsonManifestParseContext + ?Sized>(
    manifest_checksum: Option<&[u8]>,
    context: &mut C,
    buffer: &[u8],
    incr_ctx: Option<PgSha256Ctx>,
) -> PgResult<()> {
    let parse_failure = |context: &mut C, msg: &str| {
        Err(context.error_cb(format!("could not parse backup manifest: {msg}")))
    };

    /* Find the last two newlines in the file. */
    let mut number_of_newlines: usize = 0;
    let mut ultimate_newline: usize = 0;
    let mut penultimate_newline: usize = 0;
    for (i, &b) in buffer.iter().enumerate() {
        if b == b'\n' {
            number_of_newlines += 1;
            penultimate_newline = ultimate_newline;
            ultimate_newline = i;
        }
    }

    /*
     * Make sure that the last newline is right at the end, and that there
     * are at least two lines total.
     */
    if number_of_newlines < 2 {
        return parse_failure(context, "expected at least 2 lines");
    }
    if ultimate_newline != buffer.len() - 1 {
        return parse_failure(context, "last line not newline-terminated");
    }

    /* Checksum the rest. */
    let mut manifest_ctx = incr_ctx.unwrap_or_else(PgSha256Ctx::init_sha256);
    manifest_ctx.update(&buffer[..penultimate_newline + 1]);
    let manifest_checksum_actual = manifest_ctx.final_sha256();

    /* Now verify it. */
    let Some(manifest_checksum) = manifest_checksum else {
        return Err(context.error_cb("manifest has no checksum".to_string()));
    };
    let expected = if manifest_checksum.len() == PG_SHA256_DIGEST_LENGTH * 2 {
        hexdecode_string(manifest_checksum)
    } else {
        None
    };
    let Some(manifest_checksum_expected) = expected else {
        return Err(context.error_cb(format!(
            "invalid manifest checksum: \"{}\"",
            String::from_utf8_lossy(manifest_checksum)
        )));
    };
    if manifest_checksum_actual[..] != manifest_checksum_expected[..] {
        return Err(context.error_cb("manifest checksum mismatch".to_string()));
    }
    Ok(())
}

/// C: hexdecode_char.
fn hexdecode_char(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// C: hexdecode_string, plus the callers' `length % 2 != 0` pre-check.
fn hexdecode_string(input: &[u8]) -> Option<Vec<u8>> {
    if input.len() % 2 != 0 {
        return None;
    }
    let mut result = Vec::with_capacity(input.len() / 2);
    for pair in input.chunks_exact(2) {
        let n1 = hexdecode_char(pair[0])?;
        let n2 = hexdecode_char(pair[1])?;
        result.push(n1 * 16 + n2);
    }
    Some(result)
}

/// C: strtoi64(s, &ep, 10) (glibc strtol on int64). Returns the value and the
/// number of bytes consumed (C's endptr offset): leading C-locale whitespace,
/// one optional sign, base-10 digits; clamps to i64::MIN/MAX on overflow
/// (ERANGE, which the C call sites do not check); no conversion consumes 0.
fn c_strtoi64(s: &[u8]) -> (i64, usize) {
    let mut i = 0;
    while i < s.len() && isspace_c_locale(s[i]) {
        i += 1;
    }
    let mut neg = false;
    match s.get(i) {
        Some(b'-') => {
            neg = true;
            i += 1;
        }
        Some(b'+') => i += 1,
        _ => {}
    }
    let digits_start = i;
    let mut acc: u64 = 0;
    let mut over = false;
    while i < s.len() && s[i].is_ascii_digit() {
        if !over {
            match acc
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(s[i] - b'0')))
            {
                Some(v) if v <= i64::MAX as u64 + 1 => acc = v,
                _ => over = true,
            }
        }
        i += 1;
    }
    if i == digits_start {
        return (0, 0);
    }
    let value = if over {
        if neg {
            i64::MIN
        } else {
            i64::MAX
        }
    } else if neg {
        if acc == i64::MAX as u64 + 1 {
            i64::MIN
        } else {
            -(acc as i64)
        }
    } else if acc > i64::MAX as u64 {
        i64::MAX
    } else {
        acc as i64
    };
    (value, i)
}

/// C: strtou64(s, &ep, 10) (glibc strtoul, 64-bit). A minus sign is accepted
/// and negates modulo 2^64 ("-1" parses as u64::MAX — verbatim glibc);
/// overflow clamps to u64::MAX regardless of sign.
fn c_strtou64(s: &[u8]) -> (u64, usize) {
    let mut i = 0;
    while i < s.len() && isspace_c_locale(s[i]) {
        i += 1;
    }
    let mut neg = false;
    match s.get(i) {
        Some(b'-') => {
            neg = true;
            i += 1;
        }
        Some(b'+') => i += 1,
        _ => {}
    }
    let digits_start = i;
    let mut acc: u64 = 0;
    let mut over = false;
    while i < s.len() && s[i].is_ascii_digit() {
        if !over {
            match acc
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(s[i] - b'0')))
            {
                Some(v) => acc = v,
                None => over = true,
            }
        }
        i += 1;
    }
    if i == digits_start {
        return (0, 0);
    }
    let value = if over {
        u64::MAX
    } else if neg {
        acc.wrapping_neg()
    } else {
        acc
    };
    (value, i)
}

/// One `%X` conversion of C sscanf: optional whitespace, optional sign,
/// optional 0x/0X prefix, at least one hex digit; the accumulated unsigned
/// long clamps at u64::MAX and is stored through a uint (truncation), per
/// glibc. Returns (value, bytes consumed) or None on matching failure.
fn scan_hex_u32(s: &[u8]) -> Option<(u32, usize)> {
    let mut i = 0;
    while i < s.len() && isspace_c_locale(s[i]) {
        i += 1;
    }
    let mut neg = false;
    match s.get(i) {
        Some(b'-') => {
            neg = true;
            i += 1;
        }
        Some(b'+') => i += 1,
        _ => {}
    }
    if s.get(i) == Some(&b'0')
        && matches!(s.get(i + 1), Some(b'x') | Some(b'X'))
        && s.get(i + 2).is_some_and(|b| b.is_ascii_hexdigit())
    {
        i += 2;
    }
    let digits_start = i;
    let mut acc: u64 = 0;
    let mut over = false;
    while i < s.len() {
        let Some(d) = hexdecode_char(s[i]) else { break };
        if !over {
            match acc
                .checked_mul(16)
                .and_then(|v| v.checked_add(u64::from(d)))
            {
                Some(v) => acc = v,
                None => over = true,
            }
        }
        i += 1;
    }
    if i == digits_start {
        return None;
    }
    let v64 = if over {
        u64::MAX
    } else if neg {
        acc.wrapping_neg()
    } else {
        acc
    };
    Some((v64 as u32, i))
}

/// C: parse_xlogrecptr — sscanf(input, "%X/%X", &hi, &lo) == 2. The literal
/// '/' must directly follow the first number (no whitespace skip for
/// literals); trailing garbage after the second number is ignored, as sscanf
/// only reports the conversion count.
fn parse_xlogrecptr(input: &[u8]) -> Option<XLogRecPtr> {
    let (hi, n) = scan_hex_u32(input)?;
    if input.get(n) != Some(&b'/') {
        return None;
    }
    let (lo, _) = scan_hex_u32(&input[n + 1..])?;
    Some((u64::from(hi)) << 32 | u64::from(lo))
}

/*
 * Convenience accumulator: the parsed manifest as plain owned data.
 *
 * Serves both Stage-3 UPLOAD_MANIFEST (basebackup_incremental needs path +
 * size for the backup_file_hash, the WAL-range list, and the version /
 * system-identifier checks) and Stage-5 pg_combinebackup's load_manifest
 * (which additionally keeps checksum type + payload per file for whole-file
 * reuse). Consumers with C-shaped side tables (e.g. a path-keyed hash built
 * during the parse) can instead implement JsonManifestParseContext directly,
 * exactly like C's callback structs.
 */

/// One per-file entry from the manifest (C consumers: manifest_file).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestFile {
    /// Raw path bytes: the de-escaped "Path" string, or the hex-decoded
    /// "Encoded-Path" (which need not be valid UTF-8).
    pub pathname: Vec<u8>,
    pub size: u64,
    pub checksum_type: PgChecksumType,
    /// None when the manifest carried no (or an empty) "Checksum" string.
    pub checksum_payload: Option<Vec<u8>>,
}

/// One WAL range from the manifest (C consumers: manifest_wal_range).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManifestWalRange {
    pub tli: TimeLineID,
    pub start_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
}

/// A fully parsed backup manifest.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParsedManifest {
    /// 1 or 2 (the only versions the parser accepts). Incremental backup and
    /// pg_combinebackup additionally require 2 — that check belongs to the
    /// consumers, as in C.
    pub version: i32,
    /// Present in v2 manifests ("System-Identifier"); v1 has none.
    pub system_identifier: Option<u64>,
    pub files: Vec<ManifestFile>,
    pub wal_ranges: Vec<ManifestWalRange>,
}

impl ParsedManifest {
    /// Parse `buffer` (a complete backup_manifest, checksum line included)
    /// into owned data, verifying the manifest checksum.
    pub fn parse(mcx: Mcx<'_>, buffer: &[u8]) -> PgResult<ParsedManifest> {
        struct Acc(ParsedManifest);
        impl JsonManifestParseContext for Acc {
            fn version_cb(&mut self, manifest_version: i32) -> PgResult<()> {
                self.0.version = manifest_version;
                Ok(())
            }
            fn system_identifier_cb(&mut self, sysid: u64) -> PgResult<()> {
                self.0.system_identifier = Some(sysid);
                Ok(())
            }
            fn per_file_cb(
                &mut self,
                pathname: &[u8],
                size: u64,
                checksum_type: PgChecksumType,
                checksum_payload: Option<&[u8]>,
            ) -> PgResult<()> {
                self.0.files.push(ManifestFile {
                    pathname: pathname.to_vec(),
                    size,
                    checksum_type,
                    checksum_payload: checksum_payload.map(<[u8]>::to_vec),
                });
                Ok(())
            }
            fn per_wal_range_cb(
                &mut self,
                tli: TimeLineID,
                start_lsn: XLogRecPtr,
                end_lsn: XLogRecPtr,
            ) -> PgResult<()> {
                self.0.wal_ranges.push(ManifestWalRange { tli, start_lsn, end_lsn });
                Ok(())
            }
        }

        let mut acc = Acc(ParsedManifest::default());
        json_parse_manifest(mcx, &mut acc, buffer)?;
        Ok(acc.0)
    }
}

#[cfg(test)]
mod tests;
