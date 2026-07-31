// repl.js — the pgrust interactive-session terminal.
//
// A vanilla-JS port of the REPL logic in the design (project/pgrust.dc.html's
// <script data-dc-script> Component), driving the static DOM in index.html.
// Behavior matches the design's prototype exactly — banner, example buttons,
// input history, statement extraction on ';', psql-style meta-commands, and the
// table/raw/error/command/notice rendering with the design's exact colors —
// EXCEPT the backend: queries run against OUR wasm engine (backend.js +
// worker.js), not the design's mock.

import { installBackend, bootBackend, resetBackend, setStatusListener, getBuildInfo, getEngineMode, setPersist, getPersistState, setPersistListener, setClientMode, setPsqlHooks, sendPsqlText, flushPersist } from './backend.js';
import { jspiSupported } from './wiresession.js';

// ---- client selection ----------------------------------------------------
// 'psql' = the REAL Rust psql.wasm drives this terminal over the wire
// protocol — the DEFAULT wherever the engine has JSPI (feature-detected:
// WebAssembly.Suspending/promising — Chrome/Edge; WebKit/Safari has neither
// as of mid-2026). 'js' = the legacy JS REPL: the ?client=js opt-out, and the
// automatic fallback where JSPI is missing (a subtle note in the terminal
// says so — see PSQL_FALLBACK at boot).
//
// Both toolbar buttons work in BOTH modes: `reset` respawns psql and its
// server instance over a pristine datadir (doReset below), and `persist`
// snapshots the same VFS to OPFS (worker.js). psql mode's `reset` rebuilds the
// terminal in place — no page reload, so the compiled modules stay warm.
const CLIENT_PARAM = new URLSearchParams(window.location.search).get('client');
const JSPI_OK = jspiSupported();
const CLIENT = CLIENT_PARAM === 'js' ? 'js' : (JSPI_OK ? 'psql' : 'js');
// True when the user did not ASK for the JS REPL but got it anyway (no JSPI).
const PSQL_FALLBACK = CLIENT === 'js' && CLIENT_PARAM !== 'js';
setClientMode(CLIENT);

// ---- example queries ---------------------------------------------------------
// One build (wasm32-wasip1) with full float8 support — the old per-build
// warm-up split (Safari wasm32 had a Datum/float8 limitation) is gone.
const WARMUP = {
  group: 'warm up',
  label: 'Arithmetic, casts & float8',
  desc: 'operators, type casts, pi()',
  sql: "select\n  'pg' || 'rust'              as name,\n  6 * 7                       as answer,\n  upper('postgres in rust')   as shout,\n  pi()::numeric(10,4)         as pi;",
};

const SHARED_EXAMPLES = [
  { group: 'warm up', label: 'generate_series + window function', desc: 'running totals over a series',
    sql: "select\n  n,\n  n * n                          as square,\n  sum(n * n) over (order by n)   as running_sum\nfrom generate_series(1, 8) as n;" },
  { group: 'data & json', label: 'GROUP BY aggregation', desc: 'count / sum / avg over VALUES',
    sql: "select dept,\n       count(*)               as n,\n       sum(amount)            as total,\n       round(avg(amount), 1)  as avg\nfrom (values ('eng', 120), ('eng', 95),\n             ('sales', 80), ('sales', 105), ('sales', 60),\n             ('ops', 140)) as s(dept, amount)\ngroup by dept\norder by total desc;" },
  { group: 'data & json', label: 'JSONB extraction', desc: '->, ->>, jsonb_array_length',
    sql: "select\n  data ->> 'name'                     as name,\n  (data ->> 'stars')::int             as stars,\n  jsonb_array_length(data -> 'langs') as langs,\n  data -> 'langs' ->> 0               as top_lang\nfrom (values\n  ('{\"name\":\"pgrust\",\"stars\":4200,\"langs\":[\"rust\",\"sql\",\"c\"]}'::jsonb),\n  ('{\"name\":\"postgres\",\"stars\":15000,\"langs\":[\"c\",\"sql\"]}'::jsonb)\n) as repos(data);" },
  { group: 'data & json', label: 'Regex split + string_agg', desc: 'regexp_split_to_table, initcap',
    sql: "select string_agg(initcap(w), ' ' order by ord) as title,\n       count(*)                                  as words\nfrom regexp_split_to_table('rewriting postgres in rust', '\\s+')\n       with ordinality as t(w, ord);" },
  { group: 'data & json', label: 'Declarative table partitioning', desc: 'range partitions + automatic row routing',
    sql: `drop table if exists measurement cascade;

-- declarative range partitioning: parent + two child partitions
create table measurement (
  city     text,
  logdate  date,
  peaktemp int
) partition by range (logdate);

create table measurement_2024 partition of measurement
  for values from ('2024-01-01') to ('2025-01-01');
create table measurement_2025 partition of measurement
  for values from ('2025-01-01') to ('2026-01-01');

-- rows are routed to the right partition automatically
insert into measurement values
  ('SF',  '2024-06-15', 71),
  ('SF',  '2025-03-02', 64),
  ('NYC', '2024-12-20', 38),
  ('NYC', '2025-07-04', 88);

-- tableoid shows which partition each row actually landed in
select tableoid::regclass as partition, city, logdate, peaktemp
from measurement
order by logdate;` },
  { group: 'recursive ctes', label: 'Recursive CTE: Fibonacci', desc: 'UNION ALL recursion, bigint',
    sql: "with recursive fib(i, a, b) as (\n  select 1, 0::bigint, 1::bigint\n  union all\n  select i + 1, b, a + b\n  from fib\n  where i < 15\n)\nselect i, a as fib from fib;" },
  { group: 'recursive ctes', label: 'Mandelbrot set (recursive CTE)', desc: 'a fractal, generated entirely in SQL',
    sql: `WITH RECURSIVE points AS (
  SELECT (x::real / 20.0::real) AS r,
         (y::real / 20.0::real) AS c
  FROM generate_series(-40, 40) AS x
  CROSS JOIN generate_series(-40, 20) AS y
  ORDER BY r DESC, c ASC
), iterations AS (
     SELECT r,
            c,
            0.0::real AS zr,
            0.0::real AS zc,
            0 AS iteration
     FROM points
   UNION ALL
     SELECT r,
            c,
            zr*zr - zc*zc + c AS zr,
            2.0::real*zr*zc + r AS zc,
            iteration+1 AS iteration
     FROM iterations WHERE zr*zr + zc*zc < 4.0::real AND iteration < 100
), final_iteration AS (
  SELECT * FROM iterations WHERE iteration = 100
), marked_points AS (
   SELECT r,
          c,
          (CASE WHEN EXISTS (SELECT 1 FROM final_iteration i WHERE p.r = i.r AND p.c = i.c)
                THEN '**'
                ELSE '  '
           END) AS marker
   FROM points p
   ORDER BY r DESC, c ASC
), lines AS (
   SELECT r, string_agg(marker, '') AS r_text
   FROM marked_points
   GROUP BY r
   ORDER BY r DESC
) SELECT string_agg(r_text, E'\\n') FROM lines;` },
  { group: 'recursive ctes', label: 'Lisp interpreter (recursive CTE)', desc: 'a Lisp / \u03bb-calculus evaluator in JSONB \u2192 Fibonacci',
    sql: `WITH RECURSIVE loop AS (
    SELECT '{"stack": [{"type": "expr", "env": {"+": "+", "-": "-", "*": "*", "/": "/", ">": ">", "<": "<", "=": "=", "head": "head", "tail": "tail", "cons": "cons", "empty": "empty"}, "expr": [["lambda", ["f"], ["f", "f", 1, 0, 0]], ["lambda", ["self", "a", "b", "i"], ["if", [">", "i", 10], ["empty"], ["cons", "a", ["self", "self", ["+", "a", "b"], "a", ["+", "i", 1]]]]]]}]}'::jsonb AS STATE
  UNION ALL
  SELECT
    CASE
      WHEN frame_type = 'expr'
      THEN CASE WHEN jsonb_typeof(expr) = 'number'
                THEN jsonb_build_object('stack', stack - 0, 'result', expr)
                WHEN jsonb_typeof(expr) = 'string'
                THEN jsonb_build_object('stack', stack - 0, 'result', env -> expr_string)
                WHEN op_string = 'if'
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_if', 'expr', expr, 'env', env))  || (stack - 0))
                WHEN op_string = 'lambda'
                THEN jsonb_build_object('stack', stack - 0, 'result', jsonb_build_object('args', arg1, 'body', arg2, 'env', env))
                ELSE jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_args', 'left', expr, 'done', '[]'::jsonb, 'env', env))  || (stack - 0))
           END
      WHEN frame_type = 'eval_args'
      THEN CASE WHEN result IS NULL AND jsonb_array_length(args_left) = 0
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_call', 'expr', args_done, 'env', env)) || (stack - 0))
                WHEN result IS NULL
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', args_left -> 0, 'env', env), jsonb_build_object('type', 'eval_args', 'left', args_left - 0, 'done', args_done, 'env', env)) || stack - 0)
                ELSE jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_args', 'left', args_left, 'done', args_done || jsonb_build_array(result), 'env', env)) || (stack - 0))
           END
      WHEN frame_type = 'eval_call'
      THEN CASE WHEN op_string = '+'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint + arg2::text::bigint)
                WHEN op_string = '*'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint * arg2::text::bigint)
                WHEN op_string = '-'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint - arg2::text::bigint)
                WHEN op_string = '/'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint / arg2::text::bigint)
                WHEN op_string = '>'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint > arg2::text::bigint)
                WHEN op_string = '<'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint < arg2::text::bigint)
                WHEN op_string = '='
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1 = arg2)
                WHEN op_string = 'head'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1 -> 0)
                WHEN op_string = 'tail'
                THEN jsonb_build_object('stack', stack - 0, 'result', arg1 - 0)
                WHEN op_string = 'cons'
                THEN jsonb_build_object('stack', stack - 0, 'result', jsonb_build_array(arg1) || arg2)
                WHEN op_string = 'empty'
                THEN jsonb_build_object('stack', stack - 0, 'result', '[]'::jsonb)
                ELSE jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr',
                     'expr', (op -> 'body'),
                     'env', (op -> 'env') || jsonb_build_object(
                       COALESCE(op -> 'args' ->> 0, 'null'), arg1,
                       COALESCE(op -> 'args' ->> 1, 'null'), arg2,
                       COALESCE(op -> 'args' ->> 2, 'null'), arg3,
                       COALESCE(op -> 'args' ->> 3, 'null'), arg4)))
                  || (stack - 0))
           END
      WHEN frame_type = 'eval_if'
      THEN CASE WHEN result IS NULL
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg1, 'env', env)) || stack)
                WHEN result IS NOT NULL AND result::text::boolean
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg2, 'env', env)) || (stack - 0))
                WHEN result IS NOT NULL AND NOT result::text::boolean
                THEN jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg3, 'env', env)) || (stack - 0))
           END
      END
    FROM (
      SELECT state -> 'stack' -> 0 ->> 'type' AS frame_type,
             state -> 'stack' -> 0 -> 'expr' AS expr,
             state -> 'stack' -> 0 ->> 'expr' AS expr_string,
             state -> 'stack' -> 0 -> 'expr' -> 0 AS op,
             state -> 'stack' -> 0 -> 'expr' ->> 0 AS op_string,
             state -> 'stack' -> 0 -> 'expr' -> 1 AS arg1,
             state -> 'stack' -> 0 -> 'expr' -> 2 AS arg2,
             state -> 'stack' -> 0 -> 'expr' -> 3 AS arg3,
             state -> 'stack' -> 0 -> 'expr' -> 4 AS arg4,
             state -> 'stack' -> 0 -> 'left' AS args_left,
             state -> 'stack' -> 0 -> 'done' AS args_done,
             state -> 'stack' -> 0 -> 'env' AS env,
             state -> 'result' AS result,
             state -> 'stack' AS stack
             FROM loop
  ) sub
) SELECT state -> 'result' FROM loop WHERE jsonb_array_length(state -> 'stack') = 0 LIMIT 1;` },
];

function activeExamples() {
  return [WARMUP].concat(SHARED_EXAMPLES);
}

const HELP = [
  "psql-style meta-commands (preview subset):",
  "  \\?        show this help",
  "  \\dt       list tables",
  "  \\timing   toggle query timing",
  "  \\c        connection info",
  "  clear     clear the screen        (also Ctrl-L)",
  "  reset     restart the session",
  "Run SQL by ending a statement with a semicolon.",
  "Use Up / Down arrows to recall history.",
].join("\n");

// ---- DOM refs ----------------------------------------------------------------
const linesEl = document.getElementById('lines');
const inputEl = document.getElementById('input');
const promptEl = document.getElementById('prompt');
const scrollEl = document.getElementById('scroll');
const examplesEl = document.getElementById('examples');
const appEl = document.getElementById('app');
const bootScreenEl = document.getElementById('boot-screen');
const bootStatusEl = document.getElementById('boot-status');
const bootHintEl = document.getElementById('boot-hint');
const versionBadgeEl = document.getElementById('version-badge');
const buildNoteEl = document.getElementById('build-note');
const ACCENT = '#e0794a';
const MOBILE_QUERY = '(hover: none), (pointer: coarse), (max-width: 720px)';
const MAX_TRACKED_TEXT = 12000;

// ---- session state -----------------------------------------------------------
let timing = false;
let buffer = '';
let curPrompt = 'pgrust=# ';
let history = [];
let histIdx = null;
let q = [];
let draining = false;
let booted = false;
let blurAfterDrain = false;
let lastExampleId = null;

function updateBuildUi() {
  const info = getBuildInfo();
  if (versionBadgeEl) versionBadgeEl.textContent = info.shortLabel;
  if (bootHintEl) {
    bootHintEl.textContent = 'The wasm build loads PostgreSQL in your browser, then warms queries before the session opens.';
  }
  if (buildNoteEl) {
    buildNoteEl.textContent = info.note;
    buildNoteEl.style.display = info.build === 'wasm32' ? 'block' : 'none';
  }
}

// ---- analytics ---------------------------------------------------------------
function trackedText(value) {
  const text = String(value == null ? '' : value);
  if (text.length <= MAX_TRACKED_TEXT) return text;
  return text.slice(0, MAX_TRACKED_TEXT) + '\n...[truncated ' + (text.length - MAX_TRACKED_TEXT) + ' chars]';
}

function captureAnalytics(event, properties) {
  try {
    window.posthog?.capture(event, properties);
  } catch (_error) {
    // Keep demo usable if analytics script is blocked or unavailable.
  }
}

function resultAnalytics(res) {
  if (!res) return { kind: 'none' };
  const info = { kind: res.kind };
  if (res.kind === 'table') {
    info.row_count = Array.isArray(res.rows) ? res.rows.length : 0;
    info.column_count = Array.isArray(res.columns) ? res.columns.length : 0;
    info.columns = Array.isArray(res.columns) ? res.columns.map(String) : [];
  } else if (res.kind === 'raw') {
    info.row_count = res.rowCount != null ? res.rowCount : String(res.text || '').split('\n').length;
    info.output = trackedText(res.text);
  } else if (res.kind === 'command' || res.kind === 'notice') {
    info.output = trackedText(res.text);
  } else if (res.kind === 'error') {
    info.error = trackedText(res.text);
  }
  return info;
}

// ---- rendering ---------------------------------------------------------------
function appendLine(prompt, text, color) {
  const row = document.createElement('div');
  row.style.display = 'flex';
  const p = document.createElement('span');
  p.style.color = ACCENT; p.style.whiteSpace = 'pre'; p.style.flex = 'none';
  p.textContent = prompt || '';
  const t = document.createElement('span');
  t.style.color = color || '#c2c8d2'; t.style.whiteSpace = 'pre'; t.style.minWidth = '0';
  t.textContent = text == null ? '' : text;
  row.appendChild(p); row.appendChild(t);
  linesEl.appendChild(row);
}

function pushLine(prompt, text, color) {
  const parts = String(text == null ? '' : text).split('\n');
  for (let i = 0; i < parts.length; i++) {
    appendLine(i === 0 ? prompt : '', parts[i], color);
  }
  scrollEl.scrollTop = scrollEl.scrollHeight;
}

function startRunIndicator() {
  let row = null;
  let textEl = null;
  let dots = 1;
  let tickTimer = null;
  const showTimer = setTimeout(() => {
    row = document.createElement('div');
    row.style.display = 'flex';
    row.style.margin = '4px 0 6px';
    row.setAttribute('aria-live', 'polite');
    const p = document.createElement('span');
    p.style.color = ACCENT; p.style.whiteSpace = 'pre'; p.style.flex = 'none';
    p.textContent = '';
    textEl = document.createElement('span');
    textEl.className = 'query-runner';
    const dotEl = document.createElement('span');
    dotEl.className = 'query-runner-dot';
    const labelEl = document.createElement('span');
    labelEl.textContent = 'Running query.';
    textEl.appendChild(dotEl);
    textEl.appendChild(labelEl);
    row.appendChild(p); row.appendChild(textEl);
    linesEl.appendChild(row);
    scrollEl.scrollTop = scrollEl.scrollHeight;
    tickTimer = setInterval(() => {
      dots = dots % 3 + 1;
      labelEl.textContent = 'Running query' + '.'.repeat(dots);
    }, 350);
  }, 120);
  return () => {
    clearTimeout(showTimer);
    if (tickTimer) clearInterval(tickTimer);
    if (row) row.remove();
  };
}

function clearScreen() { linesEl.innerHTML = ''; }

// Push a complete statement to history. Since input is single-line, collapse
// embedded newlines to spaces so Up recalls a valid one-line form.
function pushHistory(stmt) {
  const oneLine = String(stmt).replace(/\s*\n\s*/g, ' ').replace(/\s+/g, ' ').trim();
  if (!oneLine) return;
  if (history.length && history[history.length - 1] === oneLine) { histIdx = null; return; }
  history.push(oneLine);
  histIdx = null;
}

// psql-mode history entry: newlines PRESERVED, exactly what readline stores.
// (pushHistory above is the JS REPL's line-flattening semantic; psql mode
// never uses it.)
function pushPsqlEntry(text) {
  const entry = String(text).replace(/\s+$/, '');
  if (!entry.trim()) return;
  if (history.length && history[history.length - 1] === entry) { histIdx = null; return; }
  history.push(entry);
  histIdx = null;
}

// The input is a textarea so a recalled multi-line entry displays intact;
// grow/shrink it to fit whatever value is put in it.
function setInputValue(v) {
  inputEl.value = v;
  autoGrowInput();
}
function autoGrowInput() {
  if (inputEl.value.includes('\n')) {
    inputEl.style.height = 'auto';
    inputEl.style.height = `${inputEl.scrollHeight}px`;
  } else {
    inputEl.style.height = '';
  }
}

function setPrompt(p) { curPrompt = p; promptEl.textContent = p; }

function focusInput() { try { inputEl.focus({ preventScroll: true }); } catch { inputEl.focus(); } }

function isMobileViewport() {
  return typeof window.matchMedia === 'function' && window.matchMedia(MOBILE_QUERY).matches;
}

function maybeFocusInput() {
  if (!isMobileViewport()) focusInput();
}

function blurInput() {
  if (document.activeElement === inputEl) inputEl.blur();
}

function scrollOutputIntoView() {
  requestAnimationFrame(() => {
    scrollEl.scrollTop = scrollEl.scrollHeight;
    if (isMobileViewport()) scrollEl.scrollIntoView({ block: 'nearest' });
  });
}

function banner() {
  const info = getBuildInfo();
  clearScreen();
  pushLine('', `pgrust (PostgreSQL 18.3 compatible) — ${info.label}`, '#9aa2ae');
  pushLine('', "Type \\? for psql meta-commands. End a statement with ; to run it.", '#6f7785');
  pushLine('', "Pick an example on the right, or try:  select 'pg' || 'rust';", '#6f7785');
  if (info.build === 'wasm32') pushLine('', info.note, '#7e8794');
  pushLine('', '', '#6f7785');
}

function bootLine(text) {
  // a transient status line shown during boot (replaced once ready)
  pushLine('', text, '#6f7785');
}

function setBootStatus(text) {
  if (bootStatusEl) bootStatusEl.textContent = text;
  if (booted) return;
  const last = linesEl.lastElementChild;
  if (last && last.querySelector('span:last-child')) {
    last.querySelector('span:last-child').textContent = text;
  }
}

function hideBootScreen() {
  if (appEl) appEl.classList.remove('app-loading');
  if (!bootScreenEl) return;
  bootScreenEl.classList.add('boot-hidden');
  window.setTimeout(() => { bootScreenEl.style.display = 'none'; }, 220);
}

function resetSessionUi() {
  buffer = '';
  setPrompt('pgrust=# ');
  history = [];
  histIdx = null;
  q = [];
  setInputValue('');
  banner();
}

// ---- table formatter (verbatim behavior from the design) ---------------------
function fmtTable(columns, rows, aligns) {
  const n = columns.length;
  const widths = [];
  for (let i = 0; i < n; i++) {
    let w = String(columns[i]).length;
    for (let r = 0; r < rows.length; r++) w = Math.max(w, String(rows[r][i]).length);
    widths.push(w);
  }
  const center = (s, w) => { s = String(s); const pad = w - s.length; const l = Math.floor(pad / 2); return ' '.repeat(l) + s + ' '.repeat(pad - l); };
  const pad = (v, w, a) => { v = String(v); return a === 'r' ? v.padStart(w) : v.padEnd(w); };
  const bar = '+' + widths.map((w) => '-'.repeat(w + 2)).join('+') + '+';
  const hdr = '|' + columns.map((c, i) => ' ' + center(c, widths[i]) + ' ').join('|') + '|';
  const body = rows.map((r) => '|' + r.map((v, i) => ' ' + pad(v, widths[i], (aligns && aligns[i]) || 'l') + ' ').join('|') + '|');
  return [bar, hdr, bar].concat(body).concat([bar]).join('\n');
}

function renderResult(res, dt) {
  if (res.kind === 'error') {
    pushLine('', 'ERROR:  ' + res.text, '#e0594d');
  } else if (res.kind === 'notice') {
    String(res.text).split('\n').forEach((t) => pushLine('', t, '#7e8794'));
  } else if (res.kind === 'command') {
    pushLine('', res.text, '#9aa2ae');
  } else if (res.kind === 'raw') {
    pushLine('', res.text, '#c2c8d2');
    const cnt = res.rowCount != null ? res.rowCount : String(res.text).split('\n').length;
    pushLine('', '(' + cnt + (cnt === 1 ? ' row)' : ' rows)'), '#6f7785');
  } else if (res.kind === 'table') {
    pushLine('', fmtTable(res.columns, res.rows, res.aligns || []), '#c2c8d2');
    const cnt = res.rows.length;
    pushLine('', '(' + cnt + (cnt === 1 ? ' row)' : ' rows)'), '#6f7785');
  }
  if (timing && res.kind !== 'notice' && res.kind !== 'error') {
    pushLine('', 'Time: ' + Number(dt).toFixed(3) + ' ms', '#566070');
  }
}

// ---- statement extraction (split on top-level ';') ---------------------------
function extractStatements() {
  const out = [];
  const buf = buffer;
  let start = 0, quote = null;
  for (let i = 0; i < buf.length; i++) {
    const ch = buf[i];
    if (quote) {
      if (ch === quote) { if (buf[i + 1] === quote) { i++; } else quote = null; }
    } else if (ch === "'" || ch === '"') {
      quote = ch;
    } else if (ch === ';') {
      const stmt = buf.slice(start, i).trim();
      if (stmt) { out.push(stmt); pushHistory(stmt); }
      start = i + 1;
    }
  }
  buffer = buf.slice(start);
  return out;
}

// ---- run pipeline ------------------------------------------------------------
function feedLines(lns, opts = {}) {
  q.push.apply(q, lns);
  if (opts.blurAfterRun) blurAfterDrain = true;
  drain();
}

async function drain() {
  if (draining) return;
  draining = true;
  while (q.length) {
    const line = q.shift();
    await processLine(line);
  }
  draining = false;
  scrollOutputIntoView();
  if (blurAfterDrain) {
    blurAfterDrain = false;
    blurInput();
  } else {
    maybeFocusInput();
  }
}

// ---- psql client mode: raw terminal streams --------------------------------
// psql's stdout is rendered verbatim; the trailing partial line (psql's
// prompt, printed without a newline while it waits for input) becomes the
// input prompt. stderr renders in the error/notice colors.
const psqlOutDecoder = new TextDecoder('utf-8');
const psqlErrDecoder = new TextDecoder('utf-8');
let psqlOutPending = '';
let psqlErrPending = '';

// Chunks that arrive while the terminal is being (re)built — boot, and reset —
// queue up instead of painting, so the UI reset that follows cannot wipe them.
// A fresh psql writes its banner the instant its instance starts, which is
// BEFORE the worker answers our reset request, hence the hold rather than a
// clear-then-listen.
let psqlHoldQueue = [];

function psqlHoldOutput() { if (!psqlHoldQueue) psqlHoldQueue = []; }

function psqlOnOutRaw(bytes) {
  if (psqlHoldQueue) { psqlHoldQueue.push(['out', bytes]); return; }
  psqlOnOut(bytes);
}

function psqlOnErrRaw(bytes) {
  if (psqlHoldQueue) { psqlHoldQueue.push(['err', bytes]); return; }
  psqlOnErr(bytes);
}

function psqlFlushHoldQueue() {
  const q = psqlHoldQueue;
  psqlHoldQueue = null;
  for (const [kind, bytes] of q || []) {
    if (kind === 'out') psqlOnOut(bytes); else psqlOnErr(bytes);
  }
}

function psqlOnOut(bytes) {
  psqlOutPending += psqlOutDecoder.decode(bytes, { stream: true });
  const parts = psqlOutPending.split('\n');
  psqlOutPending = parts.pop();
  for (const l of parts) pushLine('', l, '#c2c8d2');
  setPrompt(psqlOutPending);
  if (psqlAtPrompt()) {
    if (psqlPromptKind(psqlOutPending) === 'primary') flushPsqlHistory();
    psqlWakeIdle();
  }
  scrollEl.scrollTop = scrollEl.scrollHeight;
}

function psqlOnErr(bytes) {
  psqlErrPending += psqlErrDecoder.decode(bytes, { stream: true });
  const parts = psqlErrPending.split('\n');
  psqlErrPending = parts.pop();
  for (const l of parts) {
    const color = l.startsWith('ERROR') || l.startsWith('FATAL') ? '#e0594d' : '#7e8794';
    pushLine('', l, color);
  }
  scrollEl.scrollTop = scrollEl.scrollHeight;
}

// psql prints its prompt as a trailing partial line and only then reads the
// next line; "the pending partial line looks like a prompt" is therefore the
// only honest idle signal a raw byte stream offers. Used to PACE queued lines
// (an example is several lines): without it every queued line is echoed at
// once against a stale prompt, before the earlier lines' output arrives.
function psqlAtPrompt() {
  return /(?:=[#>]|-[#>]|\([#>]|[#>]) $/.test(psqlOutPending);
}

let psqlIdleWaiters = [];
function psqlWakeIdle() {
  const w = psqlIdleWaiters;
  psqlIdleWaiters = [];
  for (const f of w) f();
}

// Resolves at the next prompt — or after `ms`, so a prompt shape we failed to
// recognize can never wedge the terminal (the line is sent regardless; psql
// reads its stdin in order either way).
function psqlWaitIdle(ms = 5000) {
  if (psqlAtPrompt()) return Promise.resolve();
  return new Promise((resolve) => {
    let done = false;
    const finish = () => { if (!done) { done = true; resolve(); } };
    psqlIdleWaiters.push(finish);
    setTimeout(finish, ms);
  });
}

// ---- psql-mode history: STATEMENT-grained, matching real psql --------------
// Observed spec (PGDG psql 18.4 driven over a real PTY — see the
// psql-wasm-history lane report):
//   1. a multi-line statement is ONE history entry with its newlines
//      preserved (readline recalls the whole buffer);
//   2. Up walks whole statements, never the lines inside one;
//   3. backslash commands are their OWN entries, even when issued at a
//      continuation prompt (\r at postgres-# recalls as "\r");
//   4. a statement aborted mid-continuation (Ctrl-C, or \r buffer reset)
//      leaves NO history entry for the discarded lines.
// The prompt psql prints before each line is the grain signal — the same
// trailing-partial-line parse the queued-line pacing uses.
let psqlHistPending = []; // lines of the statement in progress

function psqlPromptKind(p) {
  if (/=[#>] $/.test(p)) return 'primary';
  if (/['"`][#>] $/.test(p)) return 'quote'; // inside a string: backslash is content
  return 'cont'; // -#, (#, and anything else psql-shaped
}

function recordPsqlHistory(line, promptText) {
  const kind = psqlPromptKind(promptText);
  const t = line.trim();
  if (kind !== 'quote' && t.charAt(0) === '\\') {
    // Backslash command: its own entry (spec 3). \r additionally DISCARDS the
    // statement accumulated so far (spec 4 — psql clears the query buffer and
    // the dropped lines never reach history).
    if (/^\\r\b/.test(t)) psqlHistPending = [];
    pushPsqlEntry(line);
    return;
  }
  if (kind === 'primary') psqlHistPending = []; // a new statement begins
  if (t !== '' || psqlHistPending.length) psqlHistPending.push(line);
}

// Called when psql paints a PRIMARY prompt: whatever accumulated belongs to a
// statement psql just finished (executed or errored — both land in history).
function flushPsqlHistory() {
  if (!psqlHistPending.length) return;
  pushPsqlEntry(psqlHistPending.join('\n'));
  psqlHistPending = [];
}

function psqlSendLine(line) {
  pushLine(curPrompt, line, '#dfe3ea');
  recordPsqlHistory(line, curPrompt);
  psqlOutPending = '';
  setPrompt('');
  sendPsqlText(line + '\n');
}

if (CLIENT === 'psql') {
  setPsqlHooks({
    onOut: psqlOnOutRaw,
    onErr: psqlOnErrRaw,
    onExit: (code, error) => {
      pushLine('', '', '#6f7785');
      pushLine('', `psql exited${code != null ? ` (code ${code})` : ''}${error ? `: ${error}` : ''} — press reset to start a fresh session.`, '#e0a24a');
      setPrompt('');
    },
  });
}

async function processLine(line) {
  if (CLIENT === 'psql') {
    await psqlWaitIdle();
    psqlSendLine(line);
    return;
  }
  pushLine(curPrompt, line, '#dfe3ea');
  const t = line.trim();
  if (buffer === '' && t === '') {
    captureAnalytics('wasm_demo_empty_run', {
      example_id: lastExampleId,
    });
  }
  if (buffer === '') {
    const low = t.toLowerCase();
    if (t.charAt(0) === '\\' || ['help', 'clear', 'quit', 'exit', 'reset'].indexOf(low) !== -1) {
      await meta(t);
      setPrompt('pgrust=# ');
      return;
    }
  }
  buffer = buffer ? buffer + '\n' + line : line;
  const stmts = extractStatements();
  setPrompt(buffer.trim() !== '' ? 'pgrust-# ' : 'pgrust=# ');
  for (let i = 0; i < stmts.length; i++) await runStatement(stmts[i]);
}

async function runStatement(sql) {
  const t0 = performance.now();
  const stopRunIndicator = startRunIndicator();
  let res;
  try { res = await window.pgrust.exec(sql); }
  catch (err) { res = { kind: 'error', text: String((err && err.message) || err) }; }
  stopRunIndicator();
  if (!res) res = { kind: 'notice', text: 'no result.' };
  const dt = res.durationMs != null ? res.durationMs : (performance.now() - t0);
  renderResult(res, dt);
  captureAnalytics('wasm_demo_query_ran', {
    example_id: lastExampleId,
    sql: trackedText(sql),
    statement_count: 1,
    duration_ms: dt,
    status: res.kind === 'error' ? 'error' : 'ok',
    ...resultAnalytics(res),
  });
  pushLine('', '', '#6f7785');
}

// ---- meta commands -----------------------------------------------------------
async function meta(t) {
  const cmd = t.toLowerCase();
  const note = (s) => pushLine('', s, '#7e8794');
  const done = () => pushLine('', '', '#6f7785');
  if (cmd === '\\?' || cmd === 'help') { HELP.split('\n').forEach(note); done(); return; }
  if (cmd === '\\timing') { timing = !timing; note('Timing is ' + (timing ? 'on' : 'off') + '.'); done(); return; }
  if (cmd === 'clear' || cmd === '\\clear' || cmd === '\\! clear') { clearScreen(); return; }
  if (cmd === 'reset' || cmd === '\\reset') { await doReset(); return; }
  if (cmd === '\\dt' || cmd === '\\d' || cmd.indexOf('\\dt ') === 0 || cmd.indexOf('\\d ') === 0) {
    // route \dt to the real engine via a catalog query, but keep the psql tag
    return metaListTables();
  }
  if (cmd === '\\conninfo') { note(`You are connected to database "postgres" as user "postgres" via the ${getBuildInfo().label}.`); done(); return; }
  if (cmd === '\\c' || cmd.indexOf('\\c ') === 0 || cmd.indexOf('\\connect') === 0) { note('You are now connected to database "postgres" as user "postgres".'); done(); return; }
  if (cmd === '\\q' || cmd === 'quit' || cmd === 'exit') { note('This is a browser preview — just close the tab. (\\q is a no-op here.)'); done(); return; }
  pushLine('', 'invalid command ' + t + '. Try \\? for help.', '#e0a24a'); done();
}

async function metaListTables() {
  // Ask the real engine for the user tables; render psql-ish.
  const sql = "select n.nspname as \"Schema\", c.relname as \"Name\", " +
    "case c.relkind when 'r' then 'table' when 'p' then 'partitioned table' when 'v' then 'view' " +
    "when 'm' then 'materialized view' when 'S' then 'sequence' when 'f' then 'foreign table' else c.relkind::text end as \"Type\" " +
    "from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
    "where c.relkind in ('r','p') and n.nspname not in ('pg_catalog','information_schema') " +
    "order by 1,2";
  let res;
  try { res = await window.pgrust.exec(sql); } catch (e) { res = { kind: 'error', text: String(e) }; }
  if (res && res.kind === 'table' && res.rows.length) {
    renderResult(res, res.durationMs || 0);
  } else {
    pushLine('', 'Did not find any relations.', '#7e8794');
  }
  pushLine('', '', '#6f7785');
}

// ---- example + control actions ----------------------------------------------
function runExample(ex) {
  lastExampleId = ex.label;
  captureAnalytics('wasm_demo_example_selected', {
    example_id: ex.label,
    example_group: ex.group,
  });
  const lns = ex.sql.trim().split('\n');
  histIdx = null;
  setInputValue('');
  const mobile = isMobileViewport();
  if (mobile) blurInput();
  feedLines(lns, { blurAfterRun: mobile });
}

// The scrollback header psql mode shows above the real psql banner (boot and
// after a reset both land here).
function psqlHandoverHeader() {
  pushLine('', `engine ready — ${getBuildInfo().label} (the terminal below IS psql; \\? for help, ?client=js for the legacy JS REPL).`, '#6f7785');
  pushLine('', '', '#6f7785');
  setPrompt('');
}

// psql mode reset: the worker retires psql + its server instance, swaps in a
// pristine datadir, and boots a fresh psql. We hold the new psql's output while
// that happens, then clear the terminal and release it, so what lands under the
// header is the REAL psql's banner and prompt — not a synthesized one.
async function doResetPsql() {
  pushLine('', 'restarting psql on a fresh datadir…', '#7e8794');
  psqlHoldOutput();
  let failure = null;
  let stranded = null;
  try {
    const r = await resetBackend();
    // The retired psql/server guests are expected to EXIT; one that had to be
    // abandoned at the teardown timeout is a leaked wasm instance, so say so
    // rather than pretending the reset was clean.
    if (r && r.retired && (!r.retired.psqlExited || !r.retired.serverExited)) {
      stranded = [!r.retired.psqlExited ? 'psql' : null, !r.retired.serverExited ? 'server' : null]
        .filter(Boolean).join(' + ');
    }
    captureAnalytics('wasm_demo_reset', { example_id: lastExampleId });
  } catch (e) {
    failure = (e && e.message) || String(e);
    captureAnalytics('wasm_demo_reset_failed', {
      example_id: lastExampleId,
      error: trackedText(failure),
    });
  }
  // Terminal state that belonged to the retired psql.
  buffer = '';
  history = [];
  histIdx = null;
  q = [];
  setInputValue('');
  psqlHistPending = [];
  psqlOutPending = '';
  psqlErrPending = '';
  clearScreen();
  if (failure) {
    pushLine('', 'reset failed: ' + failure, '#e0594d');
    pushLine('', '', '#6f7785');
    setPrompt('');
  } else {
    psqlHandoverHeader();
    if (stranded) {
      pushLine('', `warning: the previous ${stranded} instance did not exit within the teardown window (its memory stays held until the page reloads).`, '#e0a24a');
    }
  }
  psqlFlushHoldQueue();
}

async function doReset() {
  if (CLIENT === 'psql') { await doResetPsql(); return; }
  pushLine('', 'restarting session (restoring a fresh datadir)…', '#7e8794');
  try {
    await resetBackend();
    captureAnalytics('wasm_demo_reset', {
      example_id: lastExampleId,
    });
  } catch (e) {
    captureAnalytics('wasm_demo_reset_failed', {
      example_id: lastExampleId,
      error: trackedText((e && e.message) || e),
    });
  }
  resetSessionUi();
}

// ---- example sidebar render --------------------------------------------------
function buildExamples() {
  const order = ['warm up', 'data & json', 'recursive ctes'];
  examplesEl.innerHTML = '';
  for (const g of order) {
    const groupWrap = document.createElement('div');
    groupWrap.style.marginTop = '13px';
    const title = document.createElement('div');
    title.style.cssText = "font-size: 10px; letter-spacing: 0.14em; text-transform: uppercase; color: #4d5560; padding: 0 16px 5px;";
    title.textContent = g;
    groupWrap.appendChild(title);
    for (const ex of activeExamples().filter((e) => e.group === g)) {
      const btn = document.createElement('button');
      btn.className = 'example-btn';
      btn.style.cssText = "display: block; width: 100%; text-align: left; background: transparent; border: none; border-left: 2px solid transparent; padding: 7px 16px 7px 18px; cursor: pointer; font: inherit;";
      const label = document.createElement('span');
      label.style.cssText = "display: block; font-size: 12.5px; color: #d6dae1;";
      label.textContent = ex.label;
      const desc = document.createElement('span');
      desc.style.cssText = "display: block; font-size: 11px; color: #6b7280; margin-top: 1px;";
      desc.textContent = ex.desc;
      btn.appendChild(label); btn.appendChild(desc);
      btn.addEventListener('click', () => runExample(ex));
      groupWrap.appendChild(btn);
    }
    examplesEl.appendChild(groupWrap);
  }
}

// ---- input handlers ----------------------------------------------------------
inputEl.addEventListener('keydown', (e) => {
  const k = e.key;
  if (k === 'Enter') {
    e.preventDefault();
    const line = inputEl.value;
    setInputValue('');
    histIdx = null;
    const t = line.trim();
    // JS REPL only: meta lines enter history here. psql mode records history
    // statement-grained inside psqlSendLine (recordPsqlHistory).
    if (CLIENT !== 'psql' && buffer === '' && t !== '' &&
        (t.charAt(0) === '\\' || ['help', 'clear', 'quit', 'exit', 'reset'].indexOf(t.toLowerCase()) !== -1)) {
      pushHistory(line);
    }
    feedLines([line], { blurAfterRun: isMobileViewport() });
    return;
  }
  if (k === 'ArrowUp') {
    e.preventDefault();
    if (history.length) {
      histIdx = histIdx == null ? history.length - 1 : Math.max(0, histIdx - 1);
      setInputValue(history[histIdx]);
    }
    return;
  }
  if (k === 'ArrowDown') {
    e.preventDefault();
    if (histIdx != null) {
      histIdx++;
      if (histIdx >= history.length) { histIdx = null; setInputValue(''); }
      else setInputValue(history[histIdx]);
    }
    return;
  }
  if ((k === 'l' || k === 'L') && (e.ctrlKey || e.metaKey)) { e.preventDefault(); clearScreen(); return; }
  if (k === 'c' && e.ctrlKey) {
    // Ctrl-C IS the copy chord on Windows/Linux: when the user has text
    // selected anywhere (the scrollback, or inside the input itself), let the
    // browser copy it — do not eat the keystroke as a terminal interrupt.
    const sel = window.getSelection();
    if (sel && !sel.isCollapsed) return;
    if (inputEl.selectionStart !== inputEl.selectionEnd) return;
    e.preventDefault();
    pushLine(curPrompt, inputEl.value + '^C', '#7e8794');
    if (CLIENT !== 'psql') {
      buffer = ''; setPrompt('pgrust=# ');
    }
    setInputValue('');
    return;
  }
});

document.getElementById('btn-clear').addEventListener('click', () => { clearScreen(); maybeFocusInput(); });
document.getElementById('btn-reset').addEventListener('click', () => { doReset(); maybeFocusInput(); });

// ---- persist toggle (OPFS datadir snapshots — see worker.js/snapshot.js) -----
const persistBtnEl = document.getElementById('btn-persist');

function updatePersistUi() {
  const st = getPersistState();
  if (!st.available) {
    persistBtnEl.textContent = 'persist: n/a';
    persistBtnEl.disabled = true;
    persistBtnEl.title = 'This browser/profile has no OPFS storage for the worker (e.g. private mode) — the datadir lives in memory only.';
    return;
  }
  persistBtnEl.disabled = false;
  persistBtnEl.textContent = st.persist ? 'persist: on' : 'persist: off';
  persistBtnEl.style.color = st.persist ? '#ffd8c8' : '#8b93a1';
}

setPersistListener((st, note) => {
  updatePersistUi();
  if (note) pushLine('', note, '#7e8794');
});

persistBtnEl.addEventListener('click', async () => {
  const st = getPersistState();
  if (!st.available) return;
  persistBtnEl.disabled = true;
  try {
    await setPersist(!st.persist);
    captureAnalytics('wasm_demo_persist_toggled', { on: !st.persist });
  } catch (e) {
    pushLine('', 'persist toggle failed: ' + ((e && e.message) || e), '#e0594d');
  }
  updatePersistUi();
  maybeFocusInput();
});
// Teardown flush: closing the persistence loss window. Snapshots are
// leading-edge (the worker snapshots the moment the server goes idle), so by
// the time a human can react to a result the write is normally durable; these
// hooks cover the machine-fast reload/close, asking the worker for one final
// snapshot as the page goes away. Best-effort by nature — the browser tears
// the worker down with the document and does not wait for its queue.
window.addEventListener('pagehide', () => flushPersist());
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'hidden') flushPersist();
});

// ---- paste ------------------------------------------------------------------
// Real psql over a PTY treats a paste as raw bytes (observed, PGDG 18.4): every
// COMPLETE line executes as it arrives — statements run one after another,
// prompt-paced, one history entry per statement — and a trailing unterminated
// line stays in the editing buffer, unexecuted and not in history. Match that:
// complete lines feed the terminal (the same queue the sidebar examples use),
// the remainder lands in the input box at the caret.
function pasteIntoTerminal(text) {
  const t = String(text).replace(/\r\n?/g, '\n');
  if (!t) return;
  const v = inputEl.value;
  const s = inputEl.selectionStart != null ? inputEl.selectionStart : v.length;
  const e = inputEl.selectionEnd != null ? inputEl.selectionEnd : v.length;
  const combined = v.slice(0, s) + t + v.slice(e);
  const lastNl = combined.lastIndexOf('\n');
  if (lastNl === -1) {
    setInputValue(combined);
    const caret = s + t.length;
    try { inputEl.setSelectionRange(caret, caret); } catch { /* not focusable yet */ }
    return;
  }
  const lines = combined.slice(0, lastNl).split('\n');
  const rest = combined.slice(lastNl + 1);
  setInputValue(rest);
  try { inputEl.setSelectionRange(rest.length, rest.length); } catch { /* ditto */ }
  histIdx = null;
  feedLines(lines, { blurAfterRun: isMobileViewport() });
}

// Multi-line paste INTO the input: intercept so the complete lines run now
// (native insertion would just pile newlines into the box until Enter).
// Single-line paste keeps the browser's native insertion (caret/undo intact).
inputEl.addEventListener('paste', (e) => {
  const text = e.clipboardData ? e.clipboardData.getData('text/plain') : '';
  if (!text || !/[\r\n]/.test(text)) return;
  e.preventDefault();
  pasteIntoTerminal(text);
});

// Paste anywhere else on the page routes to the terminal, the way real
// terminal emulators behave. This is the fix for "paste does not work": after
// copying text out of the scrollback the input is deliberately NOT refocused
// (the selection would be lost), so the very next Cmd/Ctrl-V used to land on
// <body> and vanish. Other text fields (the updates-dialog email box) keep
// their native paste.
document.addEventListener('paste', (e) => {
  const tgt = e.target;
  if (tgt === inputEl) return; // the input's own handler owns this
  if (tgt && (tgt.tagName === 'INPUT' || tgt.tagName === 'TEXTAREA' || tgt.isContentEditable)) return;
  const text = e.clipboardData ? e.clipboardData.getData('text/plain') : '';
  if (!text) return;
  e.preventDefault();
  focusInput();
  pasteIntoTerminal(text);
});

// Click-to-focus, WITHOUT stealing a selection: a click that ends a
// drag-select (or lands inside any selection) must leave the selection alive
// so Cmd/Ctrl-C can copy it — refocusing the input here collapsed it (the
// "text cannot be copied out of the terminal" bug). A plain click has a
// collapsed selection and still refocuses.
scrollEl.addEventListener('click', () => {
  if (isMobileViewport()) return;
  const sel = window.getSelection();
  if (sel && !sel.isCollapsed) return;
  focusInput();
});
const updatesDialogEl = document.getElementById('updates-dialog');
const updatesFormEl = document.getElementById('updates-form');
const updatesEmailEl = document.getElementById('updates-email');
const updatesSubmitEl = document.getElementById('updates-submit');
const updatesStatusEl = document.getElementById('updates-status');
const updatesSuccessEl = document.getElementById('updates-success');
let updatesPlacement = 'direct';
let updatesReturnFocus = null;

function openUpdatesDialog(placement, trigger) {
  updatesPlacement = placement || 'direct';
  updatesReturnFocus = trigger || document.activeElement;
  updatesDialogEl.hidden = false;
  captureAnalytics('wasm_demo_updates_opened', { placement: updatesPlacement });
  window.setTimeout(() => {
    if (updatesFormEl.hidden) updatesSuccessEl.focus();
    else updatesEmailEl.focus();
  }, 0);
}

function closeUpdatesDialog() {
  updatesDialogEl.hidden = true;
  if (location.hash === '#updates') window.history.replaceState(null, '', location.pathname + location.search);
  if (updatesReturnFocus && typeof updatesReturnFocus.focus === 'function') updatesReturnFocus.focus();
}

document.querySelectorAll('[data-update-link]').forEach((trigger) => {
  trigger.addEventListener('click', () => openUpdatesDialog(trigger.dataset.updateLink, trigger));
});
document.querySelectorAll('[data-updates-close]').forEach((trigger) => {
  trigger.addEventListener('click', closeUpdatesDialog);
});
document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && !updatesDialogEl.hidden) closeUpdatesDialog();
});

updatesFormEl.addEventListener('submit', async (event) => {
  event.preventDefault();
  if (!updatesFormEl.reportValidity()) return;
  updatesSubmitEl.disabled = true;
  updatesSubmitEl.textContent = 'Joining...';
  updatesStatusEl.dataset.state = 'pending';
  updatesStatusEl.textContent = 'Adding you to pgrust updates...';
  captureAnalytics('wasm_demo_updates_submitted', { placement: updatesPlacement });

  try {
    const body = new URLSearchParams({
      email: updatesEmailEl.value,
      userGroup: 'pgrust.com',
      mailingLists: 'cmre353c700rl0j0g8euj0nle',
    });
    const response = await fetch(updatesFormEl.action, {
      method: 'POST',
      body,
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    });
    let result = null;
    try { result = await response.json(); } catch (_error) {}
    if (!response.ok || result?.success === false) {
      throw new Error(result?.message || 'Signup failed. Please try again.');
    }
    updatesFormEl.hidden = true;
    updatesSuccessEl.hidden = false;
    updatesSuccessEl.focus();
    captureAnalytics('wasm_demo_updates_succeeded', { placement: updatesPlacement });
    updatesFormEl.reset();
  } catch (error) {
    updatesStatusEl.dataset.state = 'error';
    updatesStatusEl.textContent = error?.message || 'Signup failed. Please try again.';
    updatesSubmitEl.disabled = false;
    updatesSubmitEl.textContent = 'Try again';
    captureAnalytics('wasm_demo_updates_failed', { placement: updatesPlacement });
  }
});

if (location.hash === '#updates') openUpdatesDialog('direct');

// ---- boot --------------------------------------------------------------------
installBackend();
banner();
bootLine('selecting wasm engine…');
setBootStatus('Selecting wasm engine...');

setStatusListener((text) => {
  setBootStatus(text);
});

bootBackend().then(() => {
  booted = true;
  updateBuildUi();
  updatePersistUi();
  buildExamples();
  if (CLIENT === 'psql') {
    // The REAL psql owns the terminal: clear the boot chatter, replay its
    // banner/prompt bytes, and hand over.
    clearScreen();
    psqlHandoverHeader();
    if (getPersistState().restored) {
      pushLine('', 'restored your persisted datadir from this browser’s storage (persist is on; reset wipes it).', '#7e8794');
    }
    psqlFlushHoldQueue();
    setBootStatus('Engine ready.');
    hideBootScreen();
    captureAnalytics('wasm_demo_loaded', { build: getBuildInfo().build, engine: 'psql' });
    maybeFocusInput();
    return;
  }
  banner();
  const engineNote = getEngineMode() === 'wire'
    ? 'one live protocol session (temp tables, prepared statements, and transactions span statements)'
    : 'single-user postgres over a persistent in-memory datadir';
  pushLine('', `engine ready — ${getBuildInfo().label}.`, '#6f7785');
  pushLine('', `${engineNote}.`, '#6f7785');
  if (PSQL_FALLBACK) {
    // Not an error: this browser simply has no JSPI (WebAssembly.Suspending),
    // which the real-psql client needs, so the JS REPL drives the terminal.
    pushLine('', 'note: this browser has no JSPI, so the JS REPL emulates psql here (Chrome/Edge get the real psql.wasm).', '#566070');
  }
  if (getPersistState().restored) {
    pushLine('', 'restored your persisted datadir from this browser’s storage (persist is on; reset wipes it).', '#7e8794');
  }
  pushLine('', '', '#6f7785');
  setBootStatus('Engine ready.');
  hideBootScreen();
  captureAnalytics('wasm_demo_loaded', {
    build: getBuildInfo().build,
    engine: getEngineMode(),
  });
  maybeFocusInput();
}).catch((e) => {
  const msg = 'failed to boot wasm engine: ' + (e && e.message || e);
  setBootStatus(msg);
  pushLine('', msg, '#e0594d');
  pushLine('', 'This preview needs WebAssembly support in the browser.', '#7e8794');
  captureAnalytics('wasm_demo_boot_failed', {
    build: getBuildInfo().build,
    error: trackedText(msg),
  });
});
