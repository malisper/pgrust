//! COPY command shared vocabulary (`commands/copy.h` / `commands/copyfrom_internal.h`).
//!
//! Two consumer families live here:
//!
//! * The COPY **TO** drivers (`commands/copyto.c`) read a filled
//!   [`CopyFormatOptions`] (the `commands/copy.h` struct, trimmed to the members
//!   they consume); the owning unit is `commands/copy.c` (`ProcessCopyOptions`
//!   fills the struct), so until that unit lands copyto obtains a filled
//!   `CopyFormatOptions` through the copy unit's seam. The C `char *` members
//!   carry NUL-terminated encoding strings; the owned model keeps them as
//!   context-allocated [`PgString`]s, and the per-column `bool *force_quote_flags`
//!   is a context-allocated [`PgVec`].
//!
//! * The COPY **FROM** parser (`commands/copyfromparse.c`) owns its byte-exact
//!   codec in its own crate, working on the owned byte buffers of
//!   [`CopyParseState`] (the parse-relevant subset of the C `CopyFromStateData`).
//!   Genuine cross-subsystem externals cross seams declared in
//!   `backend_commands_copyfromparse_seams`; the heterogeneous objects the parser
//!   only consults through seams are carried as opaque token newtypes.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use mcx::{PgString, PgVec};
use types_core::primitive::Oid;
use types_datum::datum::Datum;
use types_rel::Relation;

/* ===========================================================================
 * Copy enums (commands/copy.h, commands/copyfrom_internal.h).
 * =========================================================================== */

/// `typedef enum CopyHeaderChoice` (commands/copy.h). For COPY TO only
/// `COPY_HEADER_FALSE`/`COPY_HEADER_TRUE` occur (`COPY_HEADER_MATCH` is rejected
/// during option processing); the TO driver tests
/// `header_line != COPY_HEADER_FALSE`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CopyHeaderChoice {
    COPY_HEADER_FALSE = 0,
    COPY_HEADER_TRUE,
    COPY_HEADER_MATCH,
}

/// `typedef enum CopyOnErrorChoice` (commands/copy.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CopyOnErrorChoice {
    COPY_ON_ERROR_STOP = 0,
    COPY_ON_ERROR_IGNORE,
}

/// `typedef enum CopyLogVerbosityChoice` (commands/copy.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CopyLogVerbosityChoice {
    COPY_LOG_VERBOSITY_SILENT = -1,
    COPY_LOG_VERBOSITY_DEFAULT = 0,
    COPY_LOG_VERBOSITY_VERBOSE,
}

/// `typedef enum CopySource` (commands/copyfrom_internal.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CopySource {
    /// from file (or a piped program)
    COPY_FILE,
    /// from frontend
    COPY_FRONTEND,
    /// from callback function
    COPY_CALLBACK,
}

/// `typedef enum EolType` (commands/copyfrom_internal.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum EolType {
    EOL_UNKNOWN,
    EOL_NL,
    EOL_CR,
    EOL_CRNL,
}

/* ===========================================================================
 * CopyFormatOptions (commands/copy.h) — consumed by the COPY TO drivers.
 * =========================================================================== */

/// `CopyFormatOptions` (`commands/copy.h`), trimmed to the members the COPY TO
/// drivers read. The C struct's `char *` members carry NUL-terminated server-
/// or file-encoding strings; the owned model keeps them as context-allocated
/// [`PgString`]s (`null_print_len` collapses into the string length). The
/// per-column `bool *force_quote_flags` is a context-allocated [`PgVec`].
pub struct CopyFormatOptions<'mcx> {
    /// `int file_encoding` — file/remote side's encoding, -1 if unspecified.
    pub file_encoding: i32,
    /// `bool binary` — binary format?
    pub binary: bool,
    /// `bool csv_mode` — CSV format?
    pub csv_mode: bool,
    /// `CopyHeaderChoice header_line` — emit a header line?
    pub header_line: CopyHeaderChoice,
    /// `char *null_print` — NULL marker string (server encoding).
    pub null_print: PgString<'mcx>,
    /// `char *null_print_client` — `null_print` converted to file encoding.
    pub null_print_client: PgString<'mcx>,
    /// `char *delim` — column delimiter (1 byte).
    pub delim: u8,
    /// `char *quote` — CSV quote char (1 byte).
    pub quote: u8,
    /// `char *escape` — CSV escape char (1 byte).
    pub escape: u8,
    /// `List *force_quote` — column names for FORCE_QUOTE (`None` ⇒ NIL).
    pub force_quote: Option<PgVec<'mcx, PgString<'mcx>>>,
    /// `bool force_quote_all` — FORCE_QUOTE *?
    pub force_quote_all: bool,
    /// `bool *force_quote_flags` — per-physical-column FORCE_QUOTE flags.
    /// Empty until `BeginCopyTo` allocates it (`num_phys_attrs` entries).
    pub force_quote_flags: PgVec<'mcx, bool>,
}

/* ===========================================================================
 * Opaque cross-subsystem tokens (runtime-private table keys; NULL ⇒ None).
 *
 * Inherited opacity (types.md rule 6): the parser only consults these objects
 * through seams owned by the not-yet-ported list / executor / fmgr / libpq
 * subsystems, so they cross as keyed tokens, not stand-in structs.
 * =========================================================================== */

/// `List *` — the `attnumlist` (a parse/catalog list owned by the list
/// subsystem).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ListHandle(pub u64);

/// `ExprContext *` — per-tuple expression-evaluation context.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExprContextHandle(pub u64);

/// `Node *` — the soft-error `ErrorSaveContext` (`cstate->escontext`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EscontextHandle(pub u64);

/// A single `FmgrInfo *` slot (`&in_functions[i]`) for an input/receive
/// function.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FmgrInfoSlot(pub u64);

/// An `ExprState *` (`cstate->defexprs[m]`) for a column default expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExprStateHandle(pub u64);

/// `FILE *` opened for the COPY input (file or program pipe).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CopyFileHandle(pub u64);

/// A `copy_data_source_cb` callback handle (carried through unchanged).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DataSourceCbHandle(pub u64);

/// `StringInfo` — the frontend message buffer (`cstate->fe_msgbuf`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StringInfoHandle(pub u64);

/* ===========================================================================
 * Constants (copyfromparse.c + copyfrom_internal.h).
 * =========================================================================== */

/// `#define RAW_BUF_SIZE 65536` (copyfrom_internal.h) — `raw_buf` is always 64 kB.
pub const RAW_BUF_SIZE: i32 = 65536;
/// `#define INPUT_BUF_SIZE 65536` (copyfrom_internal.h) — `input_buf` is 64 kB
/// when encoding conversion is required.
pub const INPUT_BUF_SIZE: i32 = 65536;
/// `#define MAX_CONVERSION_INPUT_LENGTH 4` (mb/pg_wchar.h) — the longest valid
/// multibyte sequence over all supported encodings.
pub const MAX_CONVERSION_INPUT_LENGTH: i32 = 4;
/// `static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0"`
/// (copyfromparse.c:139). NOTE: there's a copy of this in copyto.c.
pub const BINARY_SIGNATURE: [u8; 11] = [
    b'P', b'G', b'C', b'O', b'P', b'Y', b'\n', 0o377, b'\r', b'\n', b'\0',
];

/* ===========================================================================
 * The in-crate owned parse state (parse-relevant subset of CopyFromStateData).
 * =========================================================================== */

/// The parse-format-options subset of `cstate->opts` consulted by the
/// copyfromparse codec — a direct projection of the parse-relevant fields of
/// the C `CopyFormatOptions`; the byte codec works on the single
/// delimiter / quote / escape bytes and the NULL / DEFAULT marker strings.
#[derive(Clone, Debug)]
pub struct CopyParseOptions {
    /// `bool binary` — binary format?
    pub binary: bool,
    /// `bool csv_mode` — CSV format?
    pub csv_mode: bool,
    /// `CopyHeaderChoice header_line` — header line handling.
    pub header_line: CopyHeaderChoice,
    /// `char *null_print` — NULL marker string (server encoding).
    pub null_print: String,
    /// `int null_print_len` — byte length of `null_print`.
    pub null_print_len: i32,
    /// `char *default_print` — DEFAULT marker string; `None` ⇒ unset.
    pub default_print: Option<String>,
    /// `int default_print_len` — byte length of `default_print`.
    pub default_print_len: i32,
    /// `char delim[0]` — single-byte column delimiter.
    pub delim: u8,
    /// `char quote[0]` — single-byte CSV quote char.
    pub quote: u8,
    /// `char escape[0]` — single-byte CSV escape char.
    pub escape: u8,
    /// `CopyOnErrorChoice on_error`.
    pub on_error: CopyOnErrorChoice,
    /// `CopyLogVerbosityChoice log_verbosity`.
    pub log_verbosity: CopyLogVerbosityChoice,
}

/// `typedef struct CopyFromStateData` (copyfrom_internal.h) — the **parse-state
/// subset** owned in-crate.
///
/// The byte buffers are owned `Vec<u8>`; the cursors / lengths / flags are
/// scalars; `rel` is the shared [`Relation`] alias the owner holds open; the
/// remaining externals (the `attnumlist` `List *`, the soft-error `escontext`,
/// the source `FILE *`) are opaque tokens.
///
/// `raw_buf` and `input_buf` are kept as distinct owned buffers. The C
/// "shortcut" where `input_buf` *points at* `raw_buf` when no transcoding is
/// needed is modeled by the `input_is_raw` flag: when set, the codec verifies
/// in place and tracks the verified prefix via `input_buf_len` against
/// `raw_buf`, exactly as the C does over the shared buffer (see
/// `CopyConvertBuf` / `CopyLoadRawBuf`).
#[derive(Debug)]
pub struct CopyParseState<'mcx> {
    /* ---- options (parse-relevant subset) ---- */
    pub opts: CopyParseOptions,

    /* ---- the COPY target / source externals ---- */
    /// `Relation rel`.
    pub rel: Relation<'mcx>,
    /// `List *attnumlist` — integer attnums being copied.
    pub attnumlist: ListHandle,
    /// `CopySource copy_src`.
    pub copy_src: CopySource,
    /// `FILE *copy_file` (when `copy_src == COPY_FILE`).
    pub copy_file: Option<CopyFileHandle>,
    /// `StringInfo fe_msgbuf` (when `copy_src == COPY_FRONTEND`).
    pub fe_msgbuf: Option<StringInfoHandle>,
    /// `copy_data_source_cb data_source_cb` (when `copy_src == COPY_CALLBACK`).
    pub data_source_cb: Option<DataSourceCbHandle>,
    /// `Node *escontext` — soft-error trap for ON_ERROR IGNORE.
    pub escontext: Option<EscontextHandle>,

    /* ---- encoding state ---- */
    /// `int file_encoding`.
    pub file_encoding: i32,
    /// `bool need_transcoding`.
    pub need_transcoding: bool,
    /// `Oid conversion_proc` — conversion procedure oid (when transcoding).
    pub conversion_proc: Oid,

    /* ---- progress ---- */
    /// `uint64 bytes_processed`.
    pub bytes_processed: u64,

    /* ---- line / error bookkeeping ---- */
    /// `uint64 cur_lineno`.
    pub cur_lineno: u64,
    /// `EolType eol_type`.
    pub eol_type: EolType,
    /// `bool line_buf_valid`.
    pub line_buf_valid: bool,

    /* ---- raw_buf: data source bytes (always RAW_BUF_SIZE + 1) ---- */
    /// `char *raw_buf` — capacity `RAW_BUF_SIZE + 1`.
    pub raw_buf: Vec<u8>,
    /// `int raw_buf_index` — next byte to process in raw_buf.
    pub raw_buf_index: i32,
    /// `int raw_buf_len` — total # of bytes stored in raw_buf.
    pub raw_buf_len: i32,
    /// `bool raw_reached_eof`.
    pub raw_reached_eof: bool,

    /* ---- input_buf: encoding-converted bytes ---- */
    /// `true` when no transcoding is needed and the C `input_buf` aliases
    /// `raw_buf` (the codec then verifies in place and reads from `raw_buf`).
    pub input_is_raw: bool,
    /// `char *input_buf` — capacity `INPUT_BUF_SIZE + 1` when transcoding.
    pub input_buf: Vec<u8>,
    /// `int input_buf_index`.
    pub input_buf_index: i32,
    /// `int input_buf_len`.
    pub input_buf_len: i32,
    /// `bool input_reached_eof`.
    pub input_reached_eof: bool,
    /// `bool input_reached_error`.
    pub input_reached_error: bool,

    /* ---- line_buf: one line (expanded on demand) ---- */
    /// `StringInfoData line_buf` — `.data` bytes.
    pub line_buf: Vec<u8>,

    /* ---- attribute_buf + raw_fields ---- */
    /// `StringInfoData attribute_buf` — de-escaped field storage.
    pub attribute_buf: Vec<u8>,
    /// `attribute_buf.cursor` (consumed by the binary receive function).
    pub attribute_cursor: i32,
    /// `int max_fields`.
    pub max_fields: i32,
    /// `char **raw_fields` — per-field byte ranges into `attribute_buf`; `None`
    /// ⇒ the field matched the NULL marker (C `raw_fields[k] == NULL`).
    pub raw_fields: Vec<Option<FieldRange>>,

    /* ---- per-attribute parse externals (handles + arrays) ---- */
    /// `bool *convert_select_flags` — `None` ⇒ the C pointer is NULL (no
    /// selective conversion); else one flag per physical attribute.
    pub convert_select_flags: Option<Vec<bool>>,
    /// `bool *force_notnull_flags` (CSV) — one per physical attribute.
    pub force_notnull_flags: Vec<bool>,
    /// `bool *force_null_flags` (CSV) — one per physical attribute.
    pub force_null_flags: Vec<bool>,
    /// `bool *defaults` — one per physical attribute; set when a field matched
    /// the DEFAULT marker.
    pub defaults: Vec<bool>,
    /// `int num_defaults`.
    pub num_defaults: i32,
    /// `int *defmap` — physical-attr indices having a default.
    pub defmap: Vec<i32>,

    /* ---- ON_ERROR IGNORE counters + error cursor ---- */
    /// `int64 num_errors`.
    pub num_errors: i64,
    /// `bool relname_only`.
    pub relname_only: bool,
    /// `char *cur_attname` — current column name (for error context).
    pub cur_attname: Option<String>,
    /// `char *cur_attval` — current column value (for error context).
    pub cur_attval: Option<String>,
}

/// A `[start, end)` byte range into `attribute_buf.data`, the idiomatic stand-in
/// for the C `char *` field pointer stored in `cstate->raw_fields[k]`. The
/// de-escaped field bytes live contiguously in `attribute_buf`; a field is the
/// byte slice `attribute_buf[start..end]` (NUL terminator excluded).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FieldRange {
    /// Offset of the first byte of the field in `attribute_buf`.
    pub start: usize,
    /// Offset one past the last byte of the field (the NUL position).
    pub end: usize,
}

/* ===========================================================================
 * Seam-signature value types.
 * =========================================================================== */

/// Result of one `CopyGetData` call: the bytes the source produced plus the EOF
/// flag the in-crate caller folds into `raw_reached_eof`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyGetDataResult {
    /// The bytes actually read (length = `bytesread`, ≤ `maxread`).
    pub data: Vec<u8>,
    /// Whether the source signalled EOF (`raw_reached_eof` must be set).
    pub reached_eof: bool,
}

/// Result of `pg_do_encoding_conversion_buf(..., noError=true)`: the number of
/// **source** bytes consumed and the converted bytes produced (NUL-terminated by
/// the C routine; the terminator is excluded from `converted`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodingConversionResult {
    /// `convertedlen` — number of source bytes consumed by the conversion.
    pub converted_src_len: i32,
    /// The converted bytes written to the destination (excludes the NUL).
    pub converted: Vec<u8>,
}

/// One physical-attribute descriptor fact the codec needs from the tuple
/// descriptor: the column name and its `atttypmod`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttrInfo {
    /// `NameStr(att->attname)`.
    pub attname: String,
    /// `att->atttypmod`.
    pub atttypmod: i32,
}

/// A single physical-attribute value produced by the per-row callback: the
/// `Datum` and its null flag, the idiomatic pair for the C `values[m]` /
/// `nulls[m]` cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttrValue {
    /// `values[m]`.
    pub datum: Datum,
    /// `nulls[m]`.
    pub isnull: bool,
}
