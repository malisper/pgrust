// format.js — turn pgrust single-user (`postgres --single`) debug output into a
// readable psql-style table.
//
// The single-user `printtup` debug format looks like:
//   backend> \t 1: colname\t(typeid = 23, len = 4, ...)        <- header (one line per column)
//            \t 2: colname2\t(typeid = 25, ...)
//            \t----                                            <- header/row separator
//            \t 1: colname = "value"\t(typeid = ...)           <- row 1, column 1
//            \t 2: colname2 = "value"\t(typeid = ...)
//            \t----                                            <- row separator
//   backend>
//
// We reconstruct columns + rows and render an aligned ASCII table. NULLs print
// as empty (the single-user format emits no `= "..."` for NULL — handled).

// normalizeSingleUserInput — flatten multi-line SQL for `postgres --single`.
//
// The single-user backend reads stdin LINE BY LINE: every newline ends the
// current statement (it does NOT buffer until a `;`). So a statement split
// across several lines —
//     select n, n * n as square
//     from generate_series(1, 5) as n;
// — is parsed as two broken fragments ("select ..." then "from ...") and errors.
//
// We therefore re-emit the script with each complete SQL statement on ONE line:
// statements are delimited by top-level `;` (ignoring `;` inside string/quoted
// identifiers/dollar-quotes and -- / C-style comments); newlines WITHIN a
// statement collapse to spaces. This makes multi-line + multi-statement scripts
// behave the same as if each statement were typed on a single line.
export function normalizeSingleUserInput(sql) {
  if (!sql) return '';
  const statements = [];
  let buf = '';
  let i = 0;
  const n = sql.length;
  // states: 0 normal, 's' single-quote string, 'd' double-quote ident,
  // 'l' line comment, 'b' block comment, dollar tag string handled inline.
  let state = 0;
  let dollarTag = null;
  // We must collapse whitespace (newlines/indentation) ONLY outside string and
  // quoted-identifier literals — collapsing inside them would corrupt the data
  // (e.g. the Mandelbrot demo uses the two-space literal '  ' for blank cells and
  // string_agg(..., E'\n'); squishing those changes the result). So whitespace is
  // collapsed inline while in the normal (state 0) scanner, and literal bytes are
  // appended verbatim. push() then only trims the statement edges.
  const push = () => {
    const stmt = buf.trim();
    if (stmt) statements.push(stmt);
    buf = '';
  };
  while (i < n) {
    const c = sql[i];
    const c2 = sql[i + 1];
    if (state === 0) {
      if (c === '-' && c2 === '-') { state = 'l'; i += 2; continue; }
      if (c === '/' && c2 === '*') { state = 'b'; if (!buf.endsWith(' ')) buf += ' '; i += 2; continue; }
      if (c === "'") { state = 's'; buf += c; i++; continue; }
      if (c === '"') { state = 'd'; buf += c; i++; continue; }
      if (c === '$') {
        // dollar-quote: $tag$ ... $tag$  (tag is optional, [A-Za-z_][A-Za-z0-9_]*)
        const m = /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(i));
        if (m) { dollarTag = m[0]; buf += dollarTag; i += dollarTag.length; state = 'q'; continue; }
      }
      if (c === ';') { buf += ';'; push(); i++; continue; }
      // collapse any run of whitespace (incl. newlines) to a single space
      if (/\s/.test(c)) {
        if (!buf.endsWith(' ')) buf += ' ';
        i++; continue;
      }
      buf += c; i++; continue;
    }
    if (state === 's') { // single-quote string
      buf += c;
      if (c === "'") { if (c2 === "'") { buf += c2; i += 2; continue; } state = 0; }
      i++; continue;
    }
    if (state === 'd') { // double-quote identifier
      buf += c;
      if (c === '"') { if (c2 === '"') { buf += c2; i += 2; continue; } state = 0; }
      i++; continue;
    }
    if (state === 'l') { // line comment: skip to newline
      if (c === '\n') { state = 0; if (!buf.endsWith(' ')) buf += ' '; }
      i++; continue;
    }
    if (state === 'b') { // block comment: replace whole comment with one space
      if (c === '*' && c2 === '/') { state = 0; if (!buf.endsWith(' ')) buf += ' '; i += 2; continue; }
      i++; continue;
    }
    if (state === 'q') { // dollar-quoted string
      if (sql.startsWith(dollarTag, i)) { buf += dollarTag; i += dollarTag.length; state = 0; dollarTag = null; continue; }
      buf += c; i++; continue;
    }
  }
  push(); // trailing statement without a final ';'
  return statements.length ? statements.join('\n') + '\n' : '';
}

export function formatSingleUser(raw) {
  if (!raw) return '';
  // The single-user debug format is RECORD-oriented, not line-oriented: a single
  // tuple field is `\t N: name = "VALUE"\t(typeid = ...)`, and VALUE may itself
  // CONTAIN newlines (e.g. a string_agg joined with E'\n', like the Mandelbrot
  // demo). Splitting on '\n' and skipping continuation lines therefore drops the
  // closing quote and renders the cell empty. So we instead carve the raw text
  // into records: a record begins at a `\t N:` field marker or a `\t----`
  // separator and runs until (but not including) the next such marker, KEEPING
  // any embedded newlines. `backend>` prompts are stripped first.
  const text = raw.replace(/^(?:backend>\s?)+/gm, '');
  // Match each record start at the beginning of a line: a field (`\t<digits>:`)
  // or a separator (`\t----`). The record body extends to the next start / EOF.
  const startRe = /^\t *(?:\d+:|----)/gm;
  const records = [];
  let m;
  const marks = [];
  while ((m = startRe.exec(text)) !== null) marks.push(m.index);
  for (let k = 0; k < marks.length; k++) {
    const end = k + 1 < marks.length ? marks[k + 1] : text.length;
    records.push(text.slice(marks[k], end));
  }

  const cols = [];          // [name, ...]
  const rows = [];          // [ [v0, v1, ...] ]
  let curRow = null;
  let sawSeparator = false; // first ---- ends the header
  let anyTable = false;

  for (const rec of records) {
    // Strip the leading tab; the remaining body may span multiple lines.
    const body = rec.replace(/^\t/, '');

    if (body.startsWith('----')) {
      // separator: end of header, or end of a row
      if (curRow) { rows.push(curRow); curRow = null; }
      sawSeparator = true;
      continue;
    }

    // "<idx>: <rest>" where rest is either "name\t(typeid...)" (header)
    // or "name = \"value\"\t(typeid...)" (row cell). The trailing
    // "\t(typeid = ...)" descriptor is the reliable record terminator.
    const m2 = /^\s*(\d+):\s?([\s\S]*)$/.exec(body);
    if (!m2) continue;
    const idx = parseInt(m2[1], 10) - 1;
    let rest = m2[2];
    // drop the trailing "\t(typeid = ...)" descriptor (last occurrence, since the
    // value itself could contain a literal "\t(")
    const tabAt = rest.lastIndexOf('\t(');
    if (tabAt !== -1) rest = rest.slice(0, tabAt);

    if (!sawSeparator) {
      // header line: rest is the column name
      cols[idx] = rest.trim();
      anyTable = true;
    } else {
      // row cell: "name = \"value\"" — extract the quoted value (NULL = none).
      // value may contain embedded newlines, hence [\s\S]*.
      if (!curRow) curRow = new Array(cols.length).fill('');
      const vm = /=\s*"([\s\S]*)"\s*$/.exec(rest);
      curRow[idx] = vm ? vm[1] : '';  // no quoted value => NULL/empty
    }
  }
  if (curRow) rows.push(curRow);

  if (!anyTable) {
    // Not a SELECT result (DDL/DML, or an error that went to stderr). Strip the
    // bare `backend>` prompt noise so it doesn't render as "backend> backend>".
    const cleaned = raw.replace(/backend>\s?/g, '').trim();
    return cleaned; // may be '' — caller decides what to show (see formatRun)
  }

  return renderTable(cols.map((c) => c == null ? '?' : c), rows);
}

// stripLogPrefix — the engine's stderr diagnostics carry the C-faithful
// log_line_prefix default '%m [%p] ', e.g.
//   2026-07-18 05:47:30.613 GMT [1] ERROR:  division by zero
// (fabled's engine printed bare 'ERROR: ...' lines; anchoring the severity
// match at the line start therefore missed EVERY diagnostic from this engine —
// errors were invisible and failed writes rendered as success). Strip that
// prefix, when present, before matching severities. The pattern is tight
// (date, time, tz, [pid]) so a legitimate message line can't be mis-stripped.
const LOG_LINE_PREFIX_RE = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)? \S+ \[\d+\] /;
export function stripLogPrefix(line) {
  return line.replace(LOG_LINE_PREFIX_RE, '');
}

// decodeUtf8Chunks — join a list of UTF-8 byte chunks into one string.
//
// The wasm module's stdout/stderr arrive as raw byte chunks whose boundaries
// fall wherever the guest's write() calls happen — which can be in the MIDDLE
// of a multi-byte UTF-8 character (Chinese text is 3 bytes per character, for
// example). Decoding each chunk with its own one-shot TextDecoder turns such
// a split character into U+FFFD replacement garbage (public issue #34: garbled
// Chinese column headers on the web demo). A single decoder in streaming mode
// ({stream: true}) instead carries the trailing partial character over to the
// next chunk; the final argument-less decode() flushes anything left over at
// end-of-stream (an incomplete trailing sequence then becomes U+FFFD, which is
// the correct lossy behavior for genuinely truncated output).
export function decodeUtf8Chunks(chunks) {
  const dec = new TextDecoder();
  let out = '';
  for (const b of chunks) out += dec.decode(b, { stream: true });
  return out + dec.decode();
}

// formatRun — produce the full block a Run should show: the formatted table /
// command output, PLUS any ERROR/WARNING/NOTICE lines (the single-user backend
// writes these to STDERR, so a failing query otherwise renders as blank). This
// is what the UI and the node runner display.
export function formatRun(stdout, stderr) {
  const table = formatSingleUser(stdout || '');
  const diag = (stderr || '')
    .split('\n')
    .map((l) => stripLogPrefix(l.trim()))
    .filter((l) => /^(ERROR|FATAL|PANIC|WARNING|NOTICE|HINT|DETAIL):/.test(l));
  const parts = [];
  if (table) parts.push(table);
  if (diag.length) parts.push(diag.join('\n'));
  return parts.join('\n') || '(no output)';
}

function renderTable(headers, rows) {
  const ncol = headers.length;
  // A cell value may contain embedded newlines (e.g. a multi-line string_agg).
  // psql renders such a cell as stacked physical lines and sizes the column to
  // the WIDEST physical line, not the whole-string length — otherwise one long
  // multi-line value would blow the column width out to thousands of chars.
  const cellLines = (v) => String(v ?? '').split('\n');
  const lineWidth = (v) => {
    let w = 0;
    for (const ln of cellLines(v)) w = Math.max(w, ln.length);
    return w;
  };
  const widths = headers.map((h, i) => {
    let w = String(h).length;
    for (const r of rows) w = Math.max(w, lineWidth(r[i]));
    return w;
  });
  const pad = (s, w) => { s = String(s ?? ''); return ' ' + s + ' '.repeat(Math.max(0, w - s.length)) + ' '; };
  const sep = '+' + widths.map((w) => '-'.repeat(w + 2)).join('+') + '+';
  const out = [];
  out.push(sep);
  out.push('|' + headers.map((h, i) => pad(h, widths[i])).join('|') + '|');
  out.push(sep);
  for (const r of rows) {
    // Expand each cell into its physical lines, then emit one table line per
    // sub-line, padding shorter cells with blanks.
    const cellsLines = r.map((c) => cellLines(c));
    const height = Math.max(1, ...cellsLines.map((ls) => ls.length));
    for (let li = 0; li < height; li++) {
      out.push('|' + cellsLines.map((ls, i) => pad(ls[li] ?? '', widths[i])).join('|') + '|');
    }
  }
  out.push(sep);
  const n = rows.length;
  out.push(`(${n} row${n === 1 ? '' : 's'})`);
  return out.join('\n');
}
