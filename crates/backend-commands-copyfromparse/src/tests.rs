//! Unit tests for the in-crate byte-exact codec of `copyfromparse.c`.
//!
//! These exercise the parser core that runs entirely in-crate over owned
//! buffers — the text / CSV attribute tokenizers, the line reader, and the
//! binary int / signature readers — without installing any seam runtime, by
//! constructing a [`CopyParseState`] directly and pre-loading its buffers.
//!
//! The genuine externals (data source reads, encoding ops, fmgr value calls)
//! are not driven here; the tests stay strictly within the byte codec, which
//! never reads `rel` / `attnumlist` on these lower-level tokenizer paths.

use super::*;
use mcx::{Mcx, MemoryContext, PgString, PgVec};
use types_copy::{CopyParseOptions, CopyParseState};
use types_rel::{FormData_pg_class, Relation, RelationData};
use types_storage::storage::RelFileLocator;
use types_tuple::heaptuple::TupleDescData;

fn opts(csv: bool) -> CopyParseOptions {
    CopyParseOptions {
        binary: false,
        csv_mode: csv,
        header_line: CopyHeaderChoice::COPY_HEADER_FALSE,
        null_print: "\\N".to_string(),
        null_print_len: 2,
        default_print: None,
        default_print_len: 0,
        delim: b'\t',
        quote: b'"',
        escape: b'"',
        on_error: CopyOnErrorChoice::COPY_ON_ERROR_STOP,
        log_verbosity: CopyLogVerbosityChoice::COPY_LOG_VERBOSITY_DEFAULT,
    }
}

/// A minimal `Relation` alias for the codec tests (these paths never read it).
fn test_relation<'mcx>(mcx: Mcx<'mcx>) -> Relation<'mcx> {
    let td = TupleDescData {
        natts: 0,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: PgVec::new_in(mcx),
        attrs: PgVec::new_in(mcx),
    };
    let data = RelationData {
        rd_id: 0,
        rd_locator: RelFileLocator {
            spcOid: 0,
            dbOid: 0,
            relNumber: 0,
        },
        rd_backend: types_core::primitive::INVALID_PROC_NUMBER,
        rd_rel: FormData_pg_class {
            relname: PgString::from_str_in("t", mcx).unwrap(),
            relnamespace: 0,
            relowner: 0,
            relrowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            reltoastrelid: 0,
            reltablespace: 0,
            relfilenode: 0,
            relisshared: false,
            relhassubclass: false,
            relpersistence: b'p',
            relkind: b'r',
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
        },
        rd_att: mcx::alloc_in(mcx, td).unwrap(),
        rd_options: None,
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
    };
    Relation::open(data, None)
}

/// Build a parse state with `line_buf` pre-loaded and `max_fields` set, so the
/// tokenizers run without touching any seam.
fn state_with_line<'mcx>(
    mcx: Mcx<'mcx>,
    csv: bool,
    line: &[u8],
    max_fields: i32,
) -> CopyParseState<'mcx> {
    CopyParseState {
        opts: opts(csv),
        rel: test_relation(mcx),
        attnumlist: types_copy::ListHandle(0),
        copy_src: CopySource::COPY_FILE,
        copy_file: None,
        fe_msgbuf: None,
        data_source_cb: None,
        escontext: None,
        file_encoding: 0,
        need_transcoding: false,
        conversion_proc: 0,
        bytes_processed: 0,
        cur_lineno: 0,
        eol_type: EolType::EOL_UNKNOWN,
        line_buf_valid: false,
        raw_buf: vec![0u8; (RAW_BUF_SIZE + 1) as usize],
        raw_buf_index: 0,
        raw_buf_len: 0,
        raw_reached_eof: false,
        input_is_raw: true,
        input_buf: Vec::new(),
        input_buf_index: 0,
        input_buf_len: 0,
        input_reached_eof: false,
        input_reached_error: false,
        line_buf: line.to_vec(),
        attribute_buf: Vec::new(),
        attribute_cursor: 0,
        max_fields,
        raw_fields: vec![None; max_fields.max(0) as usize],
        convert_select_flags: None,
        force_notnull_flags: Vec::new(),
        force_null_flags: Vec::new(),
        defaults: Vec::new(),
        num_defaults: 0,
        defmap: Vec::new(),
        num_errors: 0,
        relname_only: false,
        cur_attname: None,
        cur_attval: None,
    }
}

/// Return the de-escaped field values (None for a NULL field) for a parsed line.
fn fields(cstate: &CopyParseState, fldct: i32) -> Vec<Option<String>> {
    (0..fldct as usize)
        .map(|i| cstate.raw_fields[i].map(|r| field_str(cstate, r)))
        .collect()
}

#[test]
fn text_simple_tab_delimited() {
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), false, b"a\tbb\tccc", 4);
    let n = CopyReadAttributesText(&mut st).unwrap();
    assert_eq!(n, 3);
    assert_eq!(
        fields(&st, n),
        vec![
            Some("a".to_string()),
            Some("bb".to_string()),
            Some("ccc".to_string())
        ]
    );
}

#[test]
fn text_null_marker() {
    // Default null marker is "\N".
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), false, b"x\t\\N\ty", 4);
    let n = CopyReadAttributesText(&mut st).unwrap();
    assert_eq!(n, 3);
    assert_eq!(
        fields(&st, n),
        vec![Some("x".to_string()), None, Some("y".to_string())]
    );
}

#[test]
fn text_backslash_escapes() {
    // \t -> TAB(0x09) as a literal in the field. Use a non-tab delimiter so the
    // escaped \t stays in the field.
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), false, b"a\\tb", 2);
    st.opts.delim = b',';
    let n = CopyReadAttributesText(&mut st).unwrap();
    assert_eq!(n, 1);
    assert_eq!(fields(&st, n), vec![Some("a\tb".to_string())]);
}

#[test]
fn text_octal_and_hex_escapes() {
    // \101 == 'A' (octal 101 = 65); \x42 == 'B'.
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), false, b"\\101\\x42", 2);
    st.opts.delim = b',';
    let n = CopyReadAttributesText(&mut st).unwrap();
    assert_eq!(n, 1);
    assert_eq!(fields(&st, n), vec![Some("AB".to_string())]);
}

#[test]
fn text_trailing_empty_field() {
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), false, b"a\t", 4);
    let n = CopyReadAttributesText(&mut st).unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        fields(&st, n),
        vec![Some("a".to_string()), Some("".to_string())]
    );
}

#[test]
fn csv_quoted_fields() {
    // "a,b","c" with comma delimiter: two fields, first contains a literal comma.
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), true, b"\"a,b\",\"c\"", 4);
    st.opts.delim = b',';
    let n = CopyReadAttributesCSV(&mut st).unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        fields(&st, n),
        vec![Some("a,b".to_string()), Some("c".to_string())]
    );
}

#[test]
fn csv_doubled_quote_escape() {
    // "a""b" -> a"b  (quote==escape, the common case).
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), true, b"\"a\"\"b\"", 2);
    st.opts.delim = b',';
    let n = CopyReadAttributesCSV(&mut st).unwrap();
    assert_eq!(n, 1);
    assert_eq!(fields(&st, n), vec![Some("a\"b".to_string())]);
}

#[test]
fn csv_unquoted_null_marker() {
    // Unquoted \N matches the null marker; quoted "\N" does NOT (saw_quote).
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), true, b"\\N,\"\\N\"", 4);
    st.opts.delim = b',';
    let n = CopyReadAttributesCSV(&mut st).unwrap();
    assert_eq!(n, 2);
    assert_eq!(fields(&st, n), vec![None, Some("\\N".to_string())]);
}

#[test]
fn csv_unterminated_quoted_field_errors() {
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), true, b"\"abc", 2);
    st.opts.delim = b',';
    let r = CopyReadAttributesCSV(&mut st);
    assert!(r.is_err());
}

#[test]
fn text_zero_column_empty_line_ok() {
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), false, b"", 0);
    let n = CopyReadAttributesText(&mut st).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn text_zero_column_nonempty_line_errors() {
    let ctx = MemoryContext::new("test");
    let mut st = state_with_line(ctx.mcx(), false, b"x", 0);
    let r = CopyReadAttributesText(&mut st);
    assert!(r.is_err());
}

#[test]
fn get_decimal_from_hex_values() {
    assert_eq!(GetDecimalFromHex(b'0'), 0);
    assert_eq!(GetDecimalFromHex(b'9'), 9);
    assert_eq!(GetDecimalFromHex(b'a'), 10);
    assert_eq!(GetDecimalFromHex(b'F'), 15);
}

#[test]
fn copy_limit_printout_length_truncates() {
    let short = "hello";
    assert_eq!(CopyLimitPrintoutLength(short), "hello");
    let long = "x".repeat(150);
    let out = CopyLimitPrintoutLength(&long);
    assert!(out.ends_with("..."));
    // 100 bytes of data + "..." == 103 chars.
    assert_eq!(out.len(), 103);
}

#[test]
fn binary_signature_constant_matches_c() {
    // "PGCOPY\n\377\r\n\0"
    assert_eq!(
        BINARY_SIGNATURE,
        [b'P', b'G', b'C', b'O', b'P', b'Y', b'\n', 0o377, b'\r', b'\n', 0u8]
    );
}
