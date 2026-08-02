use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgVec};
use stringinfo::StringInfo;

use crate::from::{CopyFromState, CopySrc};
use crate::fromparse::{EolType, RAW_BUF_SIZE};
use crate::to::copy_attribute_out_text;
use crate::CopyFormatOptions;

fn test_ctx() -> &'static MemoryContext {
    thread_local! {
        static CTX: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("copy-test")));
    }
    CTX.with(|c| *c)
}

static SETUP: Once = Once::new();

fn setup_fd() {
    SETUP.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        aio_seams::pgaio_closing_fd::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        mbutils::init_seams();
    });
    thread_local! {
        static FD_READY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    FD_READY.with(|f| {
        if !f.get() {
            fd::InitFileAccess();
            f.set(true);
        }
    });
}

fn out_text(s: &[u8], delim: u8) -> Vec<u8> {
    let mcx = test_ctx().mcx();
    let mut buf = StringInfo::new_in(mcx).unwrap();
    copy_attribute_out_text(&mut buf, s, delim).unwrap();
    buf.as_bytes().to_vec()
}

#[test]
fn attribute_out_text_matches_c_escapes() {
    assert_eq!(out_text(b"plain", b'\t'), b"plain");
    assert_eq!(out_text(b"a\tb", b'\t'), b"a\\tb");
    assert_eq!(out_text(b"a\nb\rc", b'\t'), b"a\\nb\\rc");
    assert_eq!(out_text(b"a\x08\x0c\x0b", b'\t'), b"a\\b\\f\\v");
    assert_eq!(out_text(b"back\\slash", b'\t'), b"back\\\\slash");
    assert_eq!(out_text(b"a|b", b'|'), b"a\\|b");
    // Non-escaped control chars pass through (C's default arm).
    assert_eq!(out_text(b"a\x01b", b'\t'), b"a\x01b");
    // Multibyte UTF-8 passes through untouched on the server-encoding arm.
    assert_eq!(out_text("héllo⽇".as_bytes(), b'\t'), "héllo⽇".as_bytes());
}

fn mk_state<'mcx>(
    mcx: Mcx<'mcx>,
    delim: u8,
    null_print: &'static str,
) -> CopyFromState<'mcx, 'static> {
    CopyFromState {
        opts: CopyFormatOptions {
            file_encoding: wchar::PG_UTF8,
            binary: false,
            csv_mode: false,
            parquet: false,
            parquet_match_by_name: false,
            parquet_coerce_epoch: false,
            freeze: false,
            delim,
            quote: b'"',
            escape: b'"',
            null_print,
            default_print: None,
            header_line: crate::CopyHeaderChoice::False,
            force_quote: None,
            force_quote_all: false,
            force_notnull: None,
            force_notnull_all: false,
            force_null: None,
            force_null_all: false,
            convert_selectively: false,
            convert_select: None,
            on_error: crate::CopyOnErrorChoice::Stop,
            log_verbosity: crate::CopyLogVerbosityChoice::Default,
            reject_limit: 0,
        },
        src: CopySrc::File { fd: -1, filename: "" },
        raw_buf: mcx::vec_from_elem_in(mcx, 0u8, RAW_BUF_SIZE + 1),
        raw_buf_index: 0,
        raw_buf_len: 0,
        raw_reached_eof: false,
        input_reached_eof: false,
        input_reached_error: false,
        input_buf: None,
        input_buf_index: 0,
        input_buf_len: 0,
        line_buf: PgVec::new_in(mcx),
        line_buf_valid: false,
        attribute_buf: PgVec::new_in(mcx),
        binary_attr_buf: stringinfo::StringInfo::new_in(mcx).unwrap(),
        raw_fields: PgVec::new_in(mcx),
        max_fields: 8,
        eol_type: EolType::Unknown,
        cur_lineno: 0,
        cur_attidx: None,
        cur_attval_off: None,
        file_encoding: wchar::PG_UTF8,
        need_transcoding: false,
        conversion_proc: 0,
        convertcx: MemoryContext::new("COPY convert"),
        attnumlist: PgVec::new_in(mcx),
        in_functions: PgVec::new_in(mcx),
        typioparams: PgVec::new_in(mcx),
        atttypmods: PgVec::new_in(mcx),
        attnames: PgVec::new_in(mcx),
        force_notnull_flags: PgVec::new_in(mcx),
        force_null_flags: PgVec::new_in(mcx),
        convert_select_flags: None,
        defexprs: PgVec::new_in(mcx),
        defmap: PgVec::new_in(mcx),
        where_clause: types_nodes::NodeList::nil(),
        relname: String::new(),
        escontext: None,
        num_errors: 0,
        defaults: mcx::vec_from_elem_in(mcx, false, 8),
        bytes_processed: 0,
        volatile_defexprs: false,
    }
}

fn fields_of(state: &CopyFromState<'_, '_>, n: usize) -> Vec<Option<Vec<u8>>> {
    (0..n)
        .map(|i| {
            let off = state.raw_fields[i];
            if off < 0 {
                return None;
            }
            let rest = &state.attribute_buf[off as usize..];
            let end = rest.iter().position(|&b| b == 0).unwrap();
            Some(rest[..end].to_vec())
        })
        .collect()
}

fn split_line(line: &[u8], delim: u8, null_print: &'static str) -> Vec<Option<Vec<u8>>> {
    setup_fd();
    let mcx = test_ctx().mcx();
    let mut st = mk_state(mcx, delim, null_print);
    mcx::vec_append_bytes(&mut st.line_buf, line).unwrap();
    let n = st.copy_read_attributes_text().unwrap();
    fields_of(&st, n)
}

#[test]
fn read_attributes_text_decodes_escapes_and_nulls() {
    let f = split_line(b"a\tb", b'\t', "\\N");
    assert_eq!(f, vec![Some(b"a".to_vec()), Some(b"b".to_vec())]);

    let f = split_line(b"a\\tb\t\\N\t\\\\x\t\\101\\x41", b'\t', "\\N");
    assert_eq!(
        f,
        vec![
            Some(b"a\tb".to_vec()),
            None,
            Some(b"\\x".to_vec()),
            Some(b"AA".to_vec()),
        ]
    );

    // Escaped \N is data, not null; empty field is empty string.
    let f = split_line(b"\\\\N\t", b'\t', "\\N");
    assert_eq!(f, vec![Some(b"\\N".to_vec()), Some(b"".to_vec())]);

    // Octal/hex partials and passthrough of unknown escapes.
    let f = split_line(b"\\8\\x\\q", b'\t', "\\N");
    assert_eq!(f, vec![Some(b"8xq".to_vec())]);
}

#[test]
fn out_then_in_round_trips() {
    let nasty: &[&[u8]] = &[
        b"plain",
        b"tab\there",
        b"nl\nhere",
        b"cr\rhere",
        b"back\\slash",
        b"\x08\x0c\x0b\x01\x1f",
        "üñîçødé ⽇本".as_bytes(),
        b"\\N",
        b"N",
        b"",
    ];
    for &case in nasty {
        let encoded = out_text(case, b'\t');
        let fields = split_line(&encoded, b'\t', "\\N");
        assert_eq!(fields.len(), 1, "case {case:?}");
        assert_eq!(fields[0].as_deref(), Some(case), "case {case:?}");
    }
    // A full multi-field line with a NULL.
    let mut line = out_text(b"a\tb", b'\t');
    line.push(b'\t');
    line.extend_from_slice(b"\\N");
    line.push(b'\t');
    line.extend_from_slice(&out_text(b"c\\d", b'\t'));
    let fields = split_line(&line, b'\t', "\\N");
    assert_eq!(
        fields,
        vec![Some(b"a\tb".to_vec()), None, Some(b"c\\d".to_vec())]
    );
}

fn read_lines_from_file(content: &[u8]) -> (Vec<Vec<u8>>, bool) {
    setup_fd();
    let dir = std::env::temp_dir().join(format!("copy-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("lines-{:?}.data", std::thread::current().id()));
    std::fs::write(&path, content).unwrap();

    let mcx = test_ctx().mcx();
    let mut st = mk_state(mcx, b'\t', "\\N");
    let fd = fd::AllocateFile(path.to_str().unwrap(), "rb").unwrap();
    assert!(fd >= 0);
    st.src = CopySrc::File { fd, filename: "" };

    let mut lines = Vec::new();
    let mut saw_marker_eof = false;
    loop {
        let done = st.copy_read_line(false).unwrap();
        if done && st.line_buf.is_empty() {
            saw_marker_eof = true;
            break;
        }
        lines.push(st.line_buf.to_vec());
        if done {
            break;
        }
    }
    fd::FreeFile(fd).unwrap();
    (lines, saw_marker_eof)
}

#[test]
fn copy_read_line_splits_and_honors_end_marker() {
    let (lines, eof) = read_lines_from_file(b"one\ttwo\nthree\n");
    assert_eq!(lines, vec![b"one\ttwo".to_vec(), b"three".to_vec()]);
    assert!(eof);

    let (lines, _) = read_lines_from_file(b"a\n\\.\nignored\n");
    assert_eq!(lines, vec![b"a".to_vec()]);

    // Backslash-newline: the escaped pair is data, the line continues.
    let (lines, _) = read_lines_from_file(b"x\\\ny\n");
    assert_eq!(lines, vec![b"x\\\ny".to_vec()]);

    // A line larger than one refill unit survives buffer refills.
    let big = vec![b'z'; RAW_BUF_SIZE + 1234];
    let mut content = big.clone();
    content.push(b'\n');
    let (lines, _) = read_lines_from_file(&content);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0], big);
}

#[test]
fn crlf_and_cr_line_endings() {
    let (lines, _) = read_lines_from_file(b"a\r\nb\r\n");
    assert_eq!(lines, vec![b"a".to_vec(), b"b".to_vec()]);
    let (lines, _) = read_lines_from_file(b"a\rb\r");
    assert_eq!(lines, vec![b"a".to_vec(), b"b".to_vec()]);
}

fn out_csv(s: &[u8], force_quote: bool, single_attr: bool) -> Vec<u8> {
    let mcx = test_ctx().mcx();
    let mut buf = StringInfo::new_in(mcx).unwrap();
    let mut opts = mk_state(mcx, b',', "").opts;
    opts.csv_mode = true;
    // Callers fold C's pre-conversion null_print comparison into use_quote.
    let use_quote = force_quote || s == opts.null_print.as_bytes();
    crate::to::copy_attribute_out_csv(&mut buf, s, &opts, use_quote, single_attr, None).unwrap();
    buf.as_bytes().to_vec()
}

#[test]
fn attribute_out_csv_matches_c_quoting() {
    assert_eq!(out_csv(b"plain", false, false), b"plain");
    assert_eq!(out_csv(b"a,b", false, false), b"\"a,b\"");
    assert_eq!(out_csv(b"a\"b", false, false), b"\"a\"\"b\"");
    assert_eq!(out_csv(b"a\nb", false, false), b"\"a\nb\"");
    assert_eq!(out_csv(b"a\rb", false, false), b"\"a\rb\"");
    // Empty string matches null_print "" -> forced quoting.
    assert_eq!(out_csv(b"", false, false), b"\"\"");
    assert_eq!(out_csv(b"x", true, false), b"\"x\"");
    // Lone \. is quoted only in single-attribute rows.
    assert_eq!(out_csv(b"\\.", false, true), b"\"\\.\"");
    assert_eq!(out_csv(b"\\.", false, false), b"\\.");
    // Backslashes are not special in CSV.
    assert_eq!(out_csv(b"a\\b", false, false), b"a\\b");
}

fn split_line_csv(line: &[u8], null_print: &'static str) -> Vec<Option<Vec<u8>>> {
    setup_fd();
    let mcx = test_ctx().mcx();
    let mut st = mk_state(mcx, b',', null_print);
    st.opts.csv_mode = true;
    mcx::vec_append_bytes(&mut st.line_buf, line).unwrap();
    let n = st.copy_read_attributes_csv().unwrap();
    fields_of(&st, n)
}

#[test]
fn read_attributes_csv_matches_c() {
    let f = split_line_csv(b"a,b,c", "");
    assert_eq!(
        f,
        vec![Some(b"a".to_vec()), Some(b"b".to_vec()), Some(b"c".to_vec())]
    );

    // Unquoted empty matches null_print ""; quoted empty is an empty string.
    let f = split_line_csv(b"a,,\"\"", "");
    assert_eq!(f, vec![Some(b"a".to_vec()), None, Some(b"".to_vec())]);

    // Doubled quotes de-escape; delimiters inside quotes are data.
    let f = split_line_csv(b"\"a\"\"b\",\"c,d\"", "");
    assert_eq!(f, vec![Some(b"a\"b".to_vec()), Some(b"c,d".to_vec())]);

    // Quoted section adjacent to unquoted data (C allows partial quoting).
    let f = split_line_csv(b"ab\"cd\"ef", "");
    assert_eq!(f, vec![Some(b"abcdef".to_vec())]);

    // NULL marker respected only unquoted.
    let f = split_line_csv(b"NULL,\"NULL\"", "NULL");
    assert_eq!(f, vec![None, Some(b"NULL".to_vec())]);

    let err = {
        setup_fd();
        let mcx = test_ctx().mcx();
        let mut st = mk_state(mcx, b',', "");
        st.opts.csv_mode = true;
        mcx::vec_append_bytes(&mut st.line_buf, b"\"unterminated").unwrap();
        st.copy_read_attributes_csv().unwrap_err()
    };
    assert!(err.message().contains("unterminated CSV quoted field"));
}

#[test]
fn csv_out_then_in_round_trips() {
    let nasty: &[&[u8]] = &[
        b"plain",
        b"comma,here",
        b"quote\"here",
        b"nl\nhere",
        b"cr\rhere",
        b"back\\slash",
        "üñîçødé ⽇本".as_bytes(),
        b"NULL",
        b"",
    ];
    for &case in nasty {
        let encoded = out_csv(case, false, false);
        let fields = split_line_csv(&encoded, "");
        assert_eq!(fields.len(), 1, "case {case:?}");
        assert_eq!(fields[0].as_deref(), Some(case), "case {case:?}");
    }
}

fn read_lines_csv(content: &[u8]) -> Vec<Vec<u8>> {
    setup_fd();
    let dir = std::env::temp_dir().join(format!("copy-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("csvlines-{:?}.data", std::thread::current().id()));
    std::fs::write(&path, content).unwrap();

    let mcx = test_ctx().mcx();
    let mut st = mk_state(mcx, b',', "");
    st.opts.csv_mode = true;
    let fd = fd::AllocateFile(path.to_str().unwrap(), "rb").unwrap();
    st.src = CopySrc::File { fd, filename: "" };

    let mut lines = Vec::new();
    loop {
        let done = st.copy_read_line(true).unwrap();
        if done && st.line_buf.is_empty() {
            break;
        }
        lines.push(st.line_buf.to_vec());
        if done {
            break;
        }
    }
    fd::FreeFile(fd).unwrap();
    lines
}

#[test]
fn csv_read_line_keeps_quoted_newlines() {
    let lines = read_lines_csv(b"a,\"x\ny\"\nb,c\n");
    assert_eq!(lines, vec![b"a,\"x\ny\"".to_vec(), b"b,c".to_vec()]);
    // \. is not an end-of-copy marker in CSV mode (PG 18 semantics).
    let lines = read_lines_csv(b"\\.\nafter\n");
    assert_eq!(lines, vec![b"\\.".to_vec(), b"after".to_vec()]);
}

// ---- ProcessCopyOptions non-String option arguments (defGetString /
// defGetInt64 arms, define.c) ----

fn defelem<'m>(
    mcx: Mcx<'m>,
    name: &'m str,
    arg: Option<types_nodes::Node<'m>>,
) -> types_nodes::Node<'m> {
    types_nodes::Node::mk(
        mcx,
        types_nodes::parsenodes::DefElem {
            defname: Some(name),
            arg,
            location: -1,
            ..Default::default()
        },
    )
    .unwrap()
}

fn opt_list<'m>(mcx: Mcx<'m>, elems: &[types_nodes::Node<'m>]) -> types_nodes::NodeList<'m> {
    let mut l = types_nodes::NodeList::nil();
    for &e in elems {
        l.lappend(mcx, e).unwrap();
    }
    l
}

// COPY t TO f WITH (delimiter 1): C's defGetString renders the Integer as
// "1" and the single-byte checks then reject it as a catchable error, not a
// panic (delimiter "1" is in C's banned \\.a-z0-9 set).
#[test]
fn integer_arg_to_string_option_is_catchable() {
    let mcx = test_ctx().mcx();
    let arg = types_nodes::Node::mk(mcx, types_nodes::Integer { ival: 1 }).unwrap();
    let opts = opt_list(mcx, &[defelem(mcx, "delimiter", Some(arg))]);
    let Err(e) = crate::ProcessCopyOptions(mcx, false, &opts, None) else {
        panic!("expected error")
    };
    assert_eq!(e.message(), "COPY delimiter cannot be \"1\"");
}

// WITH (format 5): Integer through defGetString reaches the format
// dispatch with "5".
#[test]
fn integer_arg_to_format_option() {
    let mcx = test_ctx().mcx();
    let arg = types_nodes::Node::mk(mcx, types_nodes::Integer { ival: 5 }).unwrap();
    let opts = opt_list(mcx, &[defelem(mcx, "format", Some(arg))]);
    let Err(e) = crate::ProcessCopyOptions(mcx, false, &opts, None) else {
        panic!("expected error")
    };
    assert_eq!(e.message(), "COPY format \"5\" not recognized");
}

// WITH (delimiter (a, b)): a List argument goes through NameListToString,
// yielding the dot-joined "a.b" C produces, then the length check fires.
#[test]
fn list_arg_to_string_option_matches_c() {
    let mcx = test_ctx().mcx();
    let a = types_nodes::Node::mk(mcx, types_nodes::String { sval: "a" }).unwrap();
    let b = types_nodes::Node::mk(mcx, types_nodes::String { sval: "b" }).unwrap();
    let list = types_nodes::Node::mk_list(mcx, opt_list(mcx, &[a, b])).unwrap();
    let opts = opt_list(mcx, &[defelem(mcx, "delimiter", Some(list))]);
    let Err(e) = crate::ProcessCopyOptions(mcx, false, &opts, None) else {
        panic!("expected error")
    };
    assert_eq!(e.message(), "COPY delimiter must be a single one-byte character");
}

// WITH (freeze (on)): C's defGetBoolean falls through to defGetString,
// which renders the single-element list as "on" -> true.
#[test]
fn single_element_list_boolean_quirk() {
    let mcx = test_ctx().mcx();
    let on = types_nodes::Node::mk(mcx, types_nodes::String { sval: "on" }).unwrap();
    let list = types_nodes::Node::mk_list(mcx, opt_list(mcx, &[on])).unwrap();
    let opts = opt_list(mcx, &[defelem(mcx, "freeze", Some(list))]);
    let parsed = crate::ProcessCopyOptions(mcx, true, &opts, None).unwrap();
    assert!(parsed.freeze);
}

// REJECT_LIMIT with a lexer Float: defGetInt64's int8in arm accepts values
// beyond int4 (C copy.c defGetCopyRejectLimitOption).
#[test]
fn reject_limit_float_arg_via_defgetint64() {
    let mcx = test_ctx().mcx();
    let ignore = types_nodes::Node::mk(mcx, types_nodes::String { sval: "ignore" }).unwrap();
    let big = types_nodes::Node::mk(mcx, types_nodes::Float { fval: "3000000000" }).unwrap();
    let opts = opt_list(
        mcx,
        &[
            defelem(mcx, "on_error", Some(ignore)),
            defelem(mcx, "reject_limit", Some(big)),
        ],
    );
    let parsed = crate::ProcessCopyOptions(mcx, true, &opts, None).unwrap();
    assert_eq!(parsed.reject_limit, 3_000_000_000);
}

// REJECT_LIMIT with a Boolean argument: defGetInt64's default arm raises
// C's catchable "requires a numeric value" syntax error.
#[test]
fn reject_limit_boolean_arg_is_catchable() {
    let mcx = test_ctx().mcx();
    let ignore = types_nodes::Node::mk(mcx, types_nodes::String { sval: "ignore" }).unwrap();
    let b = types_nodes::Node::mk(mcx, types_nodes::Boolean { boolval: true }).unwrap();
    let opts = opt_list(
        mcx,
        &[
            defelem(mcx, "on_error", Some(ignore)),
            defelem(mcx, "reject_limit", Some(b)),
        ],
    );
    let Err(e) = crate::ProcessCopyOptions(mcx, true, &opts, None) else {
        panic!("expected error")
    };
    assert_eq!(e.message(), "reject_limit requires a numeric value");
}

// ---- client-only-encoding escape walks (CopyAttributeOutText/CSV,
// encoding_embeds_ascii arm) ----

// SJIS 0x95 0x5c is a two-byte character whose trail byte is ASCII '\':
// the embedded walk must NOT escape it, while the plain walk would.
#[test]
fn text_walk_skips_ascii_trail_bytes_under_sjis() {
    let mcx = test_ctx().mcx();
    let s = b"a\x95\x5cb";
    let mut buf = StringInfo::new_in(mcx).unwrap();
    crate::to::copy_attribute_out_text_embedded(&mut buf, s, b'\t', wchar::PG_SJIS).unwrap();
    assert_eq!(buf.as_bytes(), b"a\x95\x5cb");
    // The safe-encoding walk escapes the embedded backslash byte.
    assert_eq!(out_text(s, b'\t'), b"a\x95\\\\b");
    // A real ASCII backslash after a complete SJIS character still escapes.
    let mut buf2 = StringInfo::new_in(mcx).unwrap();
    crate::to::copy_attribute_out_text_embedded(&mut buf2, b"\x95\x5c\\x", b'\t', wchar::PG_SJIS)
        .unwrap();
    assert_eq!(buf2.as_bytes(), b"\x95\x5c\\\\x");
}

// Same trail-byte blindness for the CSV quote walk: an SJIS character whose
// trail byte equals the quote must be neither doubled nor trigger quoting.
#[test]
fn csv_walk_skips_ascii_trail_bytes_under_sjis() {
    let mcx = test_ctx().mcx();
    // 0x81 0x22: SJIS two-byte character with '"' as trail byte.
    let s = b"a\x81\x22b";
    let mut opts = mk_state(mcx, b',', "").opts;
    opts.csv_mode = true;
    let mut buf = StringInfo::new_in(mcx).unwrap();
    crate::to::copy_attribute_out_csv(&mut buf, s, &opts, false, false, Some(wchar::PG_SJIS))
        .unwrap();
    assert_eq!(buf.as_bytes(), b"a\x81\x22b");
    // The safe walk sees the '"' byte and quotes + doubles it.
    let mut buf2 = StringInfo::new_in(mcx).unwrap();
    crate::to::copy_attribute_out_csv(&mut buf2, s, &opts, false, false, None).unwrap();
    assert_eq!(buf2.as_bytes(), b"\"a\x81\"\"b\"");
}
