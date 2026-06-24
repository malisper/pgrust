//! psql `-a -q`-compatible output formatting (the subset pg_regress diffs).
//!
//! pg_regress compares the output of `psql -X -q` (with `-a` echoing each query)
//! against `expected/<name>.out`. The wasm single-user backend has no wire
//! protocol / psql, so it must produce this exact text itself. This module is the
//! platform-independent formatter — plain, fully unit-tested code (no wasm/OS
//! coupling) so it can be iterated to byte-exactness on native via `cargo test`,
//! then wired into the single-user `DestReceiver` on wasm.
//!
//! Implements psql's "aligned" print mode (`print_aligned` in
//! `src/bin/psql/print.c`) for the regress subset: a header row (column names
//! centered in each column), a `---+---` rule, right- or left-aligned data
//! cells, and the `(N row[s])` footer. Plus the small helpers the command loop
//! needs: query echo, command-completion tags, and the `ERROR:`/`LINE n:`/caret
//! error block.
//!
//! Byte-exactness notes (verified against real `expected/*.out` fixtures):
//! - Every cell has a 1-space left margin and 1-space right margin; the column
//!   separator is `" | "` (the margins of adjacent cells plus a `|`).
//! - The header name is CENTERED in the column's content width: `floor((w-len)/2)`
//!   spaces on the left, the remainder on the right.
//! - Numeric columns right-align data; others left-align.
//! - The rule line is `-`*(width+2) per column, joined by `+`.
//! - Header and rule for the LAST column keep the trailing margin space (psql
//!   does not trim it); data rows also keep the trailing space on the last
//!   column for left-aligned columns, but right-aligned last columns end exactly
//!   at the value (psql trims trailing whitespace on the final field of a row).
//!   The fixtures show: header/rule lines DO carry the trailing space; data rows
//!   are right-trimmed. We mirror that exactly.

/// Per-column print metadata.
#[derive(Clone, Debug)]
pub struct PsqlColumn {
    /// Column display name (the `AS` alias or attribute name).
    pub name: String,
    /// Right-align the data cells (psql does this for numeric type categories).
    pub right_align: bool,
}

/// Render one result set in psql aligned mode: header, rule, data rows, footer.
///
/// `rows[r][c]` is the already type-output-rendered string for column `c`, or
/// `None` for SQL NULL (psql prints NULL as an empty cell by default).
pub fn format_aligned(columns: &[PsqlColumn], rows: &[Vec<Option<String>>]) -> String {
    let ncols = columns.len();
    if ncols == 0 {
        // No columns (e.g. a utility statement routed here by mistake): psql
        // prints just the row count. Callers normally don't hit this.
        return format_row_count(rows.len());
    }

    // Column content widths = max(display-width of header, max data width).
    let mut widths: Vec<usize> = columns.iter().map(|c| display_width(&c.name)).collect();
    for row in rows {
        for (c, cell) in row.iter().enumerate() {
            if let Some(s) = cell {
                let w = display_width(s);
                if w > widths[c] {
                    widths[c] = w;
                }
            }
        }
    }

    let mut out = String::new();

    // Header row: " <centered name> | <centered name> ...".
    for c in 0..ncols {
        if c > 0 {
            out.push('|');
        }
        out.push(' ');
        out.push_str(&center(&columns[c].name, widths[c]));
        out.push(' ');
    }
    // Trailing margin space on the last column is kept (psql does not trim the
    // header line). Strip only the newline-less trailing we added? No: fixtures
    // show the header line ends with the right margin space then `\n`.
    out.push('\n');

    // Rule row: "-"*(w+2) per column joined by "+".
    for c in 0..ncols {
        if c > 0 {
            out.push('+');
        }
        for _ in 0..widths[c] + 2 {
            out.push('-');
        }
    }
    out.push('\n');

    // Data rows.
    //
    // psql's `print_aligned_text` (print.c) does NOT blanket right-trim a data
    // row. With the default border (1), the cell value itself is written
    // verbatim — so a `char(n)`/`bpchar` cell whose value is literal trailing
    // spaces keeps them. The only "trimming" is structural: for the LAST column
    // psql emits `finalspaces = (j < col_count - 1)` = false, meaning a
    // left-aligned last column gets NO trailing alignment padding, and NO
    // trailing margin space is printed after the last column. Right-aligned
    // cells never have trailing padding anyway. We mirror that exactly: pad
    // every non-last column (and its trailing margin), but for the last column
    // skip the alignment pad (when left-aligned) and the trailing margin space.
    for row in rows {
        let mut line = String::new();
        for c in 0..ncols {
            if c > 0 {
                line.push('|');
            }
            // Leading margin space (psql prints this for every column with
            // border != 0).
            line.push(' ');
            let cell = row.get(c).and_then(|v| v.as_deref()).unwrap_or("");
            let pad = widths[c].saturating_sub(display_width(cell));
            let is_last = c == ncols - 1;
            if columns[c].right_align {
                // Right aligned: leading spaces, then the value verbatim. No
                // trailing pad in any case.
                for _ in 0..pad {
                    line.push(' ');
                }
                line.push_str(cell);
            } else {
                // Left aligned: value verbatim, then trailing pad only if this
                // is not the last column (psql's `finalspaces`).
                line.push_str(cell);
                if !is_last {
                    for _ in 0..pad {
                        line.push(' ');
                    }
                }
            }
            // Trailing margin space: psql prints it only when not the last
            // column.
            if !is_last {
                line.push(' ');
            }
        }
        out.push_str(&line);
        out.push('\n');
    }

    out.push_str(&format_row_count(rows.len()));
    out
}

/// A non-fatal/fatal error to render in psql's client format.
#[derive(Clone, Debug, Default)]
pub struct PsqlError {
    /// Severity label ("ERROR", "NOTICE", "WARNING", "FATAL"). psql uppercases.
    pub severity: String,
    /// Primary message (the `errmsg`).
    pub message: String,
    /// `errdetail`, if any.
    pub detail: Option<String>,
    /// `errhint`, if any.
    pub hint: Option<String>,
    /// 1-based character position within the *current statement* (the
    /// `cursorpos` from `errposition()`), if the error carries one. psql turns
    /// this into the `LINE n:` echo + caret.
    pub position: Option<usize>,
    /// The statement text the error refers to (for the `LINE n:` echo). Required
    /// to render the position; if absent, the position is dropped (no LINE line).
    pub query: Option<String>,
}

/// Render an error block exactly as psql's `-a -q` client does (verified against
/// `expected/*.out`):
///
/// ```text
/// ERROR:  <message>
/// [DETAIL:  <detail>]
/// [HINT:  <hint>]
/// [LINE <n>: <source line containing the position>
///         ^]
/// ```
///
/// Note the TWO spaces after the `ERROR:`/`DETAIL:`/`HINT:` labels. The
/// `LINE`/caret block is emitted only when both `position` and `query` are
/// present. The caret column is computed over the (possibly multi-line) query:
/// psql shows the single source line that contains the position and points the
/// caret at the offending character within it.
pub fn format_error(err: &PsqlError) -> String {
    // Mirrors libpq's `pqBuildErrorMessage3` field order (default verbosity):
    //   SEVERITY:  message\n
    //   LINE <n>: <src>\n  <caret>\n      (the statement-position cursor block)
    //   DETAIL:  ...\n
    //   HINT:  ...\n
    // The position/caret block comes BEFORE DETAIL/HINT — not after.
    let mut out = String::new();
    let sev = if err.severity.is_empty() { "ERROR" } else { &err.severity };
    out.push_str(sev);
    out.push_str(":  ");
    out.push_str(&err.message);
    out.push('\n');
    if let (Some(pos), Some(query)) = (err.position, &err.query) {
        if pos >= 1 {
            out.push_str(&format_line_and_caret(query, pos));
        }
    }
    if let Some(d) = &err.detail {
        out.push_str("DETAIL:  ");
        out.push_str(d);
        out.push('\n');
    }
    if let Some(h) = &err.hint {
        out.push_str("HINT:  ");
        out.push_str(h);
        out.push('\n');
    }
    out
}

/// Build psql's `LINE <n>: <text>\n<pad>^\n` block. `pos` is a 1-based character
/// offset into `query`. psql finds the source line containing `pos`, prints
/// `LINE <lineno>: <that line>`, then a caret line whose `^` sits under the
/// offending character (accounting for the `"LINE <n>: "` prefix width).
fn format_line_and_caret(query: &str, pos: usize) -> String {
    // Walk lines, tracking the running 1-based char offset of each line's start.
    let mut start = 1usize; // 1-based offset of the current line's first char
    let mut lineno = 1usize;
    for line in query.split('\n') {
        let line_len = line.chars().count();
        let line_end = start + line_len; // exclusive (offset just past last char)
        // pos belongs to this line if start <= pos <= line_end (psql allows the
        // caret to sit just past the last char, e.g. "at end of input").
        if pos <= line_end {
            let col = pos - start; // 0-based column within the line
            let prefix = format!("LINE {lineno}: ");
            let mut out = String::new();
            out.push_str(&prefix);
            out.push_str(line);
            out.push('\n');
            // Caret: prefix width + col spaces, then '^'.
            for _ in 0..display_width(&prefix) + col {
                out.push(' ');
            }
            out.push('^');
            out.push('\n');
            return out;
        }
        start = line_end + 1; // +1 for the consumed '\n'
        lineno += 1;
    }
    // Position past the end of the query: point at end of the last line.
    String::new()
}

/// Echo a query the way `psql -a` does: print the SQL followed by a newline.
/// (pg_regress feeds whole statements; psql echoes each input line. For the
/// regress files the statement text already ends without a trailing newline, so
/// we add exactly one.)
pub fn echo_query(sql: &str) -> String {
    let mut s = sql.to_string();
    if !s.ends_with('\n') {
        s.push('\n');
    }
    s
}

/// The `(N row[s])` footer line (with trailing newline).
pub fn format_row_count(n: usize) -> String {
    if n == 1 {
        "(1 row)\n".to_string()
    } else {
        format!("({n} rows)\n")
    }
}

/// Center `s` within `width` columns: `floor((width-len)/2)` left spaces, the
/// rest on the right (psql's header centering). If `s` is wider than `width`
/// (cannot happen — width is the max — but be safe), return it unpadded.
fn center(s: &str, width: usize) -> String {
    let len = display_width(s);
    if len >= width {
        return s.to_string();
    }
    let total = width - len;
    let left = total / 2;
    let right = total - left;
    let mut out = String::with_capacity(width);
    for _ in 0..left {
        out.push(' ');
    }
    out.push_str(s);
    for _ in 0..right {
        out.push(' ');
    }
    out
}

/// Display width of a string. psql counts characters (and wide chars as 2) for
/// alignment; the regress subset is ASCII/Latin-1 single-width, so we use the
/// char count. (A multibyte east-asian-width refinement can be added later if a
/// specific file needs it; none of the simple files do.)
fn display_width(s: &str) -> usize {
    s.chars().count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, right: bool) -> PsqlColumn {
        PsqlColumn { name: name.to_string(), right_align: right }
    }

    // SELECT 1 AS one;  -> single numeric column.
    // expected/boolean.out lines 8-11:
    //  one \n-----\n   1\n(1 row)\n
    #[test]
    fn select_one() {
        let cols = vec![col("one", true)];
        let rows = vec![vec![Some("1".to_string())]];
        let got = format_aligned(&cols, &rows);
        assert_eq!(got, " one \n-----\n   1\n(1 row)\n");
    }

    // SELECT true AS true;  -> single text(bool) column, left-aligned, value "t".
    // expected/boolean.out:  true \n------\n t\n(1 row)\n
    #[test]
    fn select_bool_true() {
        let cols = vec![col("true", false)];
        let rows = vec![vec![Some("t".to_string())]];
        let got = format_aligned(&cols, &rows);
        assert_eq!(got, " true \n------\n t\n(1 row)\n");
    }

    // SELECT * FROM INT8_TBL;  two numeric cols (q1 width 16, q2 width 17 due to
    // the negative value), 5 rows. Bytes copied verbatim from
    // expected/int8.out (header centered, data right-aligned; q2 is 1 wider than
    // q1 because of "-4567890123456789"). Each line below is exact incl. the
    // header/rule trailing space and the right-trimmed data rows.
    #[test]
    fn int8_tbl_select_star() {
        let cols = vec![col("q1", true), col("q2", true)];
        let rows = vec![
            vec![Some("123".into()), Some("456".into())],
            vec![Some("123".into()), Some("4567890123456789".into())],
            vec![Some("4567890123456789".into()), Some("123".into())],
            vec![Some("4567890123456789".into()), Some("4567890123456789".into())],
            vec![Some("4567890123456789".into()), Some("-4567890123456789".into())],
        ];
        let got = format_aligned(&cols, &rows);
        // Built from explicit space counts to avoid source-whitespace ambiguity.
        let expected = concat!(
            "        q1        |        q2         \n",
            "------------------+-------------------\n",
            "              123 |               456\n",
            "              123 |  4567890123456789\n",
            " 4567890123456789 |               123\n",
            " 4567890123456789 |  4567890123456789\n",
            " 4567890123456789 | -4567890123456789\n",
            "(5 rows)\n",
        );
        assert_eq!(got, expected);
    }

    #[test]
    fn zero_rows() {
        let cols = vec![col("one", true)];
        let rows: Vec<Vec<Option<String>>> = vec![];
        let got = format_aligned(&cols, &rows);
        assert_eq!(got, " one \n-----\n(0 rows)\n");
    }

    // expected/int4.out lines 6-9:
    //   INSERT INTO INT4_TBL(f1) VALUES ('34.5');   (echoed by -a, not here)
    //   ERROR:  invalid input syntax for type integer: "34.5"
    //   LINE 1: INSERT INTO INT4_TBL(f1) VALUES ('34.5');
    //                                            ^
    // The '34.5' opens at character 42 (1-based) in the statement.
    #[test]
    fn error_with_line_and_caret() {
        let query = "INSERT INTO INT4_TBL(f1) VALUES ('34.5');";
        // caret sits under the opening quote of '34.5'. Find its 1-based pos.
        let pos = query.find("'34.5'").unwrap() + 1; // 1-based
        let err = PsqlError {
            severity: "ERROR".into(),
            message: "invalid input syntax for type integer: \"34.5\"".into(),
            detail: None,
            hint: None,
            position: Some(pos),
            query: Some(query.to_string()),
        };
        let got = format_error(&err);
        let expected = concat!(
            "ERROR:  invalid input syntax for type integer: \"34.5\"\n",
            "LINE 1: INSERT INTO INT4_TBL(f1) VALUES ('34.5');\n",
            "                                         ^\n",
        );
        assert_eq!(got, expected);
    }

    // Runtime error with no position (e.g. division by zero): just the ERROR
    // line, no LINE/caret. expected/int8.out: "ERROR:  division by zero".
    #[test]
    fn error_no_position() {
        let err = PsqlError {
            severity: "ERROR".into(),
            message: "division by zero".into(),
            ..Default::default()
        };
        assert_eq!(format_error(&err), "ERROR:  division by zero\n");
    }

    // ERROR + DETAIL (no position). expected/create_table.out:
    //   ERROR:  ALTER action SET LOGGED cannot be performed on relation "unlogged1"
    //   DETAIL:  This operation is not supported for partitioned tables.
    #[test]
    fn error_with_detail() {
        let err = PsqlError {
            severity: "ERROR".into(),
            message: "ALTER action SET LOGGED cannot be performed on relation \"unlogged1\"".into(),
            detail: Some("This operation is not supported for partitioned tables.".into()),
            ..Default::default()
        };
        let expected = concat!(
            "ERROR:  ALTER action SET LOGGED cannot be performed on relation \"unlogged1\"\n",
            "DETAIL:  This operation is not supported for partitioned tables.\n",
        );
        assert_eq!(format_error(&err), expected);
    }

    #[test]
    fn echo_adds_one_newline() {
        assert_eq!(echo_query("SELECT 1;"), "SELECT 1;\n");
        assert_eq!(echo_query("SELECT 1;\n"), "SELECT 1;\n");
    }

    // expected/char.out: SELECT * FROM CHAR_TBL; — char(1) column `f1`.
    // The 6th row's value is a single literal space (zero-length char input is
    // blank-padded to width 1). psql MUST preserve that trailing space: the row
    // is "  " (leading margin + the space value), NOT "" (over-trimmed).
    // Bytes copied verbatim from expected/char.out lines 29-38.
    #[test]
    fn char_tbl_preserves_trailing_space() {
        let cols = vec![col("f1", false)];
        let rows = vec![
            vec![Some("a".into())],
            vec![Some("A".into())],
            vec![Some("1".into())],
            vec![Some("2".into())],
            vec![Some("3".into())],
            vec![Some(" ".into())], // zero-length char -> blank-padded to " "
            vec![Some("c".into())],
        ];
        let got = format_aligned(&cols, &rows);
        let expected = concat!(
            " f1 \n",
            "----\n",
            " a\n",
            " A\n",
            " 1\n",
            " 2\n",
            " 3\n",
            "  \n", // <- leading margin + the literal blank-pad space, preserved
            " c\n",
            "(7 rows)\n",
        );
        assert_eq!(got, expected);
    }

    // expected/text.out: SELECT * FROM TEXT_TBL; — single text column, width 17.
    // A left-aligned LAST column gets NO trailing alignment padding (psql's
    // `finalspaces` is false for the last column). So " doh!" is exactly margin
    // + value, with no run of pad spaces. Header DOES keep its trailing space.
    // Bytes copied verbatim from expected/text.out lines 18-22.
    #[test]
    fn text_tbl_last_col_no_trailing_pad() {
        let cols = vec![col("f1", false)];
        let rows = vec![
            vec![Some("doh!".into())],
            vec![Some("hi de ho neighbor".into())],
        ];
        let got = format_aligned(&cols, &rows);
        let expected = concat!(
            "        f1         \n", // header centered, trailing margin kept
            "-------------------\n",
            " doh!\n",               // no trailing pad on last (left) column
            " hi de ho neighbor\n",
            "(2 rows)\n",
        );
        assert_eq!(got, expected);
    }

    // expected/text.out lines 26-30: an error with BOTH a position (LINE/caret)
    // and a HINT. libpq emits the caret block BEFORE the HINT.
    //   ERROR:  function length(integer) does not exist
    //   LINE 1: select length(42);
    //                  ^
    //   HINT:  No function matches the given name and argument types. ...
    #[test]
    fn error_hint_comes_after_line_caret() {
        let query = "select length(42);";
        let pos = query.find("length").unwrap() + 1; // 1-based, caret under 'l'
        let err = PsqlError {
            severity: "ERROR".into(),
            message: "function length(integer) does not exist".into(),
            detail: None,
            hint: Some(
                "No function matches the given name and argument types. \
                 You might need to add explicit type casts."
                    .into(),
            ),
            position: Some(pos),
            query: Some(query.to_string()),
        };
        let got = format_error(&err);
        let expected = concat!(
            "ERROR:  function length(integer) does not exist\n",
            "LINE 1: select length(42);\n",
            "               ^\n",
            "HINT:  No function matches the given name and argument types. \
             You might need to add explicit type casts.\n",
        );
        assert_eq!(got, expected);
    }

    // DETAIL + HINT + position together: order must be ERROR, LINE/caret,
    // DETAIL, HINT.
    #[test]
    fn error_detail_hint_position_order() {
        let query = "select foo;";
        let err = PsqlError {
            severity: "ERROR".into(),
            message: "msg".into(),
            detail: Some("det".into()),
            hint: Some("hnt".into()),
            position: Some(8),
            query: Some(query.to_string()),
        };
        let got = format_error(&err);
        let expected = concat!(
            "ERROR:  msg\n",
            "LINE 1: select foo;\n",
            "               ^\n",
            "DETAIL:  det\n",
            "HINT:  hnt\n",
        );
        assert_eq!(got, expected);
    }

    // SELECT NULL::int AS x;  -> right-aligned numeric, NULL = empty value.
    // psql does NOT trim the data row: a right-aligned last column still emits
    // the leading margin + full alignment padding. Verified against real
    // fixtures: arrays.out renders a single NULL int4 cell as "     " (margin +
    // 4 pad spaces over the width-4 "int4" header), NOT "".
    #[test]
    fn null_right_aligned_keeps_padding() {
        // Header "int4" forces width 4 so the padding is visible (matches the
        // arrays.out fixture exactly).
        let cols = vec![col("int4", true)];
        let rows = vec![vec![None]];
        let got = format_aligned(&cols, &rows);
        assert_eq!(got, " int4 \n------\n     \n(1 row)\n");
    }

    // SELECT NULL AS any_value;  -> left-aligned NULL last column = leading
    // margin only, no trailing pad. Verified against aggregates.out: the data
    // row is a single space.
    #[test]
    fn null_left_aligned_margin_only() {
        let cols = vec![col("any_value", false)];
        let rows = vec![vec![None]];
        let got = format_aligned(&cols, &rows);
        assert_eq!(got, " any_value \n-----------\n \n(1 row)\n");
    }
}
