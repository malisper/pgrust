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
    for row in rows {
        let mut line = String::new();
        for c in 0..ncols {
            if c > 0 {
                line.push('|');
            }
            line.push(' ');
            let cell = row.get(c).and_then(|v| v.as_deref()).unwrap_or("");
            let pad = widths[c].saturating_sub(display_width(cell));
            if columns[c].right_align {
                for _ in 0..pad {
                    line.push(' ');
                }
                line.push_str(cell);
            } else {
                line.push_str(cell);
                for _ in 0..pad {
                    line.push(' ');
                }
            }
            line.push(' ');
        }
        // psql right-trims trailing whitespace on each data row.
        let trimmed = line.trim_end_matches(' ');
        out.push_str(trimmed);
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
    let mut out = String::new();
    let sev = if err.severity.is_empty() { "ERROR" } else { &err.severity };
    out.push_str(sev);
    out.push_str(":  ");
    out.push_str(&err.message);
    out.push('\n');
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
    if let (Some(pos), Some(query)) = (err.position, &err.query) {
        if pos >= 1 {
            out.push_str(&format_line_and_caret(query, pos));
        }
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

    #[test]
    fn null_is_empty_cell() {
        // SELECT NULL::int AS x;  -> right-aligned numeric, NULL = empty cell.
        let cols = vec![col("x", true)];
        let rows = vec![vec![None]];
        let got = format_aligned(&cols, &rows);
        // width = max("x"=1, "")=1; header " x ", rule "---", data row is a
        // single empty (trimmed) cell -> "" then "\n".
        assert_eq!(got, " x \n---\n\n(1 row)\n");
    }
}
