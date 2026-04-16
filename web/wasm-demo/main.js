import init, { WasmEngine } from "./pkg/pgrust.js";

const output = document.getElementById("output");
const liveSql = document.getElementById("live-sql");
const runLive = document.getElementById("run-live");
const reset = document.getElementById("reset");
const clear = document.getElementById("clear");
const status = document.getElementById("status");
const exampleSelect = document.getElementById("example-select");
const exampleNote = document.getElementById("example-note");

const EXAMPLES = [
  {
    id: "seq-scan",
    label: "Sequential Scan",
    note:
      "A plain table filter with no index. `EXPLAIN (ANALYZE, BUFFERS)` should stay on a sequential scan path.",
    sql: `create table if not exists wasm_seq_scan_demo (
  id int4,
  region text,
  score int4
);
delete from wasm_seq_scan_demo;
insert into wasm_seq_scan_demo values
  (1, 'west', 5),
  (2, 'east', 18),
  (3, 'west', 12),
  (4, 'central', 27),
  (5, 'east', 9);
explain (analyze, buffers)
select id, region, score
from wasm_seq_scan_demo
where score >= 12
order by id;`,
  },
  {
    id: "hash-join",
    label: "Hash Join",
    note:
      "Two tables joined on an integer key. This is a compact way to inspect hash-join planning and output.",
    sql: `create table if not exists wasm_hash_customers (
  customer_id int4,
  name text,
  tier text
);
create table if not exists wasm_hash_orders (
  order_id int4,
  customer_id int4,
  total int4
);
delete from wasm_hash_customers;
delete from wasm_hash_orders;
insert into wasm_hash_customers values
  (1, 'Ada', 'gold'),
  (2, 'Ben', 'silver'),
  (3, 'Cora', 'gold');
insert into wasm_hash_orders values
  (101, 1, 44),
  (102, 1, 65),
  (103, 3, 27),
  (104, 2, 18);
explain (analyze, buffers)
select c.name, o.order_id, o.total
from wasm_hash_customers c
join wasm_hash_orders o on o.customer_id = c.customer_id
where o.total >= 25
order by o.order_id;`,
  },
  {
    id: "plpgsql-function",
    label: "Custom PL/pgSQL Function",
    note:
      "Builds a FizzBuzz-style `LANGUAGE plpgsql` table function using `FOR ... LOOP`, `ELSIF`, and `RETURN NEXT`. Use `Reset Database` before rerunning this example unchanged, because `CREATE FUNCTION` here does not support `IF NOT EXISTS` yet.",
    sql: `create function wasm_fizzbuzz(limit int4)
returns table(n int4, label text)
language plpgsql
as $fn$
begin
  for i in 1..limit loop
    n := i;
    if i % 15 = 0 then
      label := 'fizzbuzz';
    elsif i % 3 = 0 then
      label := 'fizz';
    elsif i % 5 = 0 then
      label := 'buzz';
    else
      label := i::text;
    end if;
    return next;
  end loop;
  return;
end
$fn$;
select *
from wasm_fizzbuzz(100);`,
  },
  {
    id: "json",
    label: "JSON and JSONB",
    note:
      "Loads json/jsonb values and queries nested fields, containment, and scalar extraction.",
    sql: `create table if not exists wasm_json_demo (
  id int4,
  payload jsonb
);
delete from wasm_json_demo;
insert into wasm_json_demo values
  (1, '{"user":"ana","active":true,"tags":["sql","wasm"]}'),
  (2, '{"user":"ben","active":false,"tags":["planner"]}'),
  (3, '{"user":"cy","active":true,"tags":["executor","json"]}');
select
  id,
  payload ->> 'user' as user_name,
  payload @> '{"active": true}'::jsonb as active,
  payload -> 'tags' as tags
from wasm_json_demo
order by id;`,
  },
  {
    id: "stats",
    label: "Stats and ANALYZE",
    note:
      "Runs `ANALYZE` and then queries `pg_stats` so you can inspect collected statistics for a demo table.",
    sql: `create table if not exists wasm_stats_demo (
  bucket int4,
  category text
);
delete from wasm_stats_demo;
insert into wasm_stats_demo values
  (1, 'alpha'),
  (1, 'alpha'),
  (2, 'beta'),
  (2, 'beta'),
  (2, 'gamma'),
  (3, 'gamma'),
  (3, 'gamma'),
  (4, 'delta');
analyze wasm_stats_demo;
select
  attname,
  n_distinct,
  null_frac
from pg_stats
where tablename = 'wasm_stats_demo'
order by attname;`,
  },
  {
    id: "aggregate",
    label: "Aggregate and Group By",
    note:
      "A grouped aggregate over a small fact table. Good for checking grouping and aggregate output.",
    sql: `create table if not exists wasm_agg_demo (
  day text,
  amount int4
);
delete from wasm_agg_demo;
insert into wasm_agg_demo values
  ('mon', 12),
  ('mon', 5),
  ('tue', 9),
  ('wed', 4),
  ('wed', 11);
select
  day,
  count(*) as rows_seen,
  sum(amount) as total_amount,
  avg(amount) as avg_amount
from wasm_agg_demo
group by day
order by day;`,
  },
];

let engine;
let transcript = "";
const MAX_TRACKED_TEXT = 12000;

function trackedText(value) {
  const text = String(value ?? "");
  if (text.length <= MAX_TRACKED_TEXT) {
    return text;
  }
  return `${text.slice(0, MAX_TRACKED_TEXT)}\n…[truncated ${text.length - MAX_TRACKED_TEXT} chars]`;
}

function captureAnalytics(event, properties) {
  try {
    window.posthog?.capture(event, properties);
  } catch (_error) {
    // Ignore analytics failures so the demo stays usable without PostHog.
  }
}

function setStatus(value) {
  status.textContent = value;
}

function writeTranscript(value) {
  transcript = value;
  output.textContent = transcript;
  output.scrollTop = output.scrollHeight;
}

function appendTranscript(value) {
  writeTranscript(transcript ? `${transcript}\n${value}` : value);
}

function resizeLivePrompt() {
  liveSql.style.height = "0px";
  liveSql.style.height = `${Math.max(96, liveSql.scrollHeight)}px`;
}

function renderBootMessage(message) {
  return message;
}

function splitStatements(script) {
  const statements = [];
  let current = "";
  let singleQuoted = false;
  let doubleQuoted = false;
  let dollarQuotedTag = null;

  function readDollarQuotedTag(start) {
    if (script[start] !== "$") {
      return null;
    }
    let end = start + 1;
    while (end < script.length) {
      const ch = script[end];
      if (ch === "$") {
        return script.slice(start, end + 1);
      }
      const isIdentifierChar =
        (ch >= "a" && ch <= "z") ||
        (ch >= "A" && ch <= "Z") ||
        (ch >= "0" && ch <= "9") ||
        ch === "_";
      if (!isIdentifierChar) {
        return null;
      }
      end += 1;
    }
    return null;
  }

  for (let i = 0; i < script.length; i++) {
    const ch = script[i];
    const prev = i > 0 ? script[i - 1] : "";

    if (dollarQuotedTag) {
      if (script.startsWith(dollarQuotedTag, i)) {
        current += dollarQuotedTag;
        i += dollarQuotedTag.length - 1;
        dollarQuotedTag = null;
      } else {
        current += ch;
      }
      continue;
    }

    if (!singleQuoted && !doubleQuoted) {
      const nextDollarTag = readDollarQuotedTag(i);
      if (nextDollarTag) {
        current += nextDollarTag;
        i += nextDollarTag.length - 1;
        dollarQuotedTag = nextDollarTag;
        continue;
      }
    }

    if (ch === "'" && !doubleQuoted && prev !== "\\") {
      singleQuoted = !singleQuoted;
    } else if (ch === '"' && !singleQuoted && prev !== "\\") {
      doubleQuoted = !doubleQuoted;
    }

    if (ch === ";" && !singleQuoted && !doubleQuoted) {
      const trimmed = current.trim();
      if (trimmed) {
        statements.push(trimmed);
      }
      current = "";
      continue;
    }

    current += ch;
  }

  const trailing = current.trim();
  if (trailing) {
    statements.push(trailing);
  }
  return statements;
}

function formatPromptedStatement(statement) {
  const lines = statement.trim().split("\n");
  return lines
    .map((line, index) => `${index === 0 ? "pgrust=#" : "       ->"} ${line}`)
    .join("\n");
}

function stringifyValue(value) {
  if (value === null) {
    return "";
  }
  if (Array.isArray(value)) {
    return JSON.stringify(value);
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function padCell(value, width) {
  return `${value}${" ".repeat(Math.max(0, width - value.length))}`;
}

function formatTable(result) {
  const headers = result.columns.map((column) => column.name);
  const rowStrings = result.rows.map((row) => row.map(stringifyValue));
  const widths = headers.map((header, index) =>
    Math.max(
      header.length,
      ...rowStrings.map((row) => (row[index] ?? "").length),
    ),
  );
  const headerLine = headers
    .map((header, index) => padCell(header, widths[index]))
    .join(" | ");
  const separator = widths.map((width) => "-".repeat(width)).join("-+-");
  const body =
    rowStrings.length === 0
      ? ""
      : rowStrings
          .map((row) =>
            row.map((value, index) => padCell(value, widths[index])).join(" | "),
          )
          .join("\n");
  const rowCount = `(${result.rows.length} row${result.rows.length === 1 ? "" : "s"})`;

  return body
    ? `${headerLine}\n${separator}\n${body}\n${rowCount}`
    : `${headerLine}\n${separator}\n${rowCount}`;
}

function formatResult(result) {
  if (!result.ok) {
    return `ERROR:  ${result.error}`;
  }
  if (Array.isArray(result.columns) && Array.isArray(result.rows)) {
    return formatTable(result);
  }
  if (result.tag) {
    return result.tag;
  }
  return JSON.stringify(result, null, 2);
}

function renderError(error) {
  try {
    const parsed = JSON.parse(String(error));
    return formatResult(parsed);
  } catch {
    return `ERROR:  ${String(error)}`;
  }
}

function populateExamples() {
  exampleSelect.innerHTML = "";
  for (const example of EXAMPLES) {
    const option = document.createElement("option");
    option.value = example.id;
    option.textContent = example.label;
    exampleSelect.appendChild(option);
  }
}

function selectedExample() {
  return EXAMPLES.find((example) => example.id === exampleSelect.value) ?? EXAMPLES[0];
}

function syncExamplePreview() {
  const example = selectedExample();
  exampleNote.textContent = example.note;
}

function loadSelectedExample() {
  const example = selectedExample();
  liveSql.value = example.sql;
  syncExamplePreview();
  resizeLivePrompt();
  liveSql.setSelectionRange(liveSql.value.length, liveSql.value.length);
  liveSql.focus();
}

function executeScript(script, emptyMessage) {
  const trimmed = script.trim();
  if (!trimmed) {
    appendTranscript(emptyMessage);
    setStatus("ready");
    captureAnalytics("wasm_demo_empty_run", {
      example_id: selectedExample().id,
    });
    return;
  }
  const statements = splitStatements(trimmed);
  const execution = {
    example_id: selectedExample().id,
    script: trackedText(trimmed),
    statement_count: statements.length,
    statements: [],
  };
  for (const statement of statements) {
    appendTranscript(formatPromptedStatement(statement));
    try {
      const result = JSON.parse(engine.execute(statement));
      const formatted = formatResult(result);
      appendTranscript(formatted);
      execution.statements.push({
        sql: trackedText(statement),
        ok: Boolean(result.ok),
        output: trackedText(formatted),
      });
    } catch (error) {
      const formatted = renderError(error);
      appendTranscript(formatted);
      setStatus("error");
      execution.statements.push({
        sql: trackedText(statement),
        ok: false,
        error: trackedText(formatted),
        output: trackedText(formatted),
      });
      captureAnalytics("wasm_demo_query_ran", {
        ...execution,
        status: "error",
        error: trackedText(formatted),
      });
      return;
    }
    appendTranscript("");
  }
  setStatus("ready");
  captureAnalytics("wasm_demo_query_ran", {
    ...execution,
    status: "ok",
  });
}

function runLivePrompt() {
  executeScript(liveSql.value, "pgrust=# -- prompt is empty");
  liveSql.value = "";
  resizeLivePrompt();
  liveSql.focus();
}

async function boot() {
  populateExamples();
  await init();
  engine = new WasmEngine(64);
  loadSelectedExample();
  writeTranscript("");
  setStatus("ready");
  captureAnalytics("wasm_demo_loaded", {
    example_id: selectedExample().id,
  });
}

runLive.addEventListener("click", runLivePrompt);

reset.addEventListener("click", () => {
  try {
    engine.reset(64);
    writeTranscript("");
    setStatus("reset");
    captureAnalytics("wasm_demo_reset", {
      example_id: selectedExample().id,
    });
  } catch (error) {
    appendTranscript(renderError(error));
    setStatus("error");
    captureAnalytics("wasm_demo_reset_failed", {
      example_id: selectedExample().id,
      error: trackedText(renderError(error)),
    });
  }
});

clear.addEventListener("click", () => {
  writeTranscript("");
  resizeLivePrompt();
  liveSql.focus();
});

liveSql.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    runLivePrompt();
  }
});

liveSql.addEventListener("input", resizeLivePrompt);

exampleSelect.addEventListener("change", () => {
  loadSelectedExample();
  captureAnalytics("wasm_demo_example_selected", {
    example_id: selectedExample().id,
  });
});

boot().catch((error) => {
  writeTranscript(renderBootMessage(renderError(error)));
  setStatus("error");
  captureAnalytics("wasm_demo_boot_failed", {
    error: trackedText(renderError(error)),
  });
});
