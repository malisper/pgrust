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
    id: "upserts",
    label: "Upsert",
    note:
      "Shows `INSERT ... ON CONFLICT DO UPDATE` using both `excluded` values and the current table row.",
    sql: `create table if not exists upsert_demo (
  id int4 primary key,
  name text,
  note text
);
delete from upsert_demo;
insert into upsert_demo values
  (1, 'alice', 'alpha'),
  (2, 'ben', 'beta');
insert into upsert_demo (id, name, note) values
  (1, 'bob', 'fresh'),
  (3, 'cy', 'new')
on conflict (id) do update
set name = excluded.name,
    note = upsert_demo.name;
select id, name, note
from upsert_demo
order by id;`,
  },
  {
    id: "window-functions",
    label: "Window Functions",
    note:
      "A compact windowing example with `row_number`, `rank`, and a running sum partitioned by department.",
    sql: `create table if not exists window_demo (
  dept text,
  employee text,
  salary int4
);
delete from window_demo;
insert into window_demo values
  ('eng', 'Ada', 120),
  ('eng', 'Ben', 95),
  ('eng', 'Cy', 95),
  ('sales', 'Dia', 80),
  ('sales', 'Eli', 105);
select
  dept,
  employee,
  salary,
  row_number() over (partition by dept order by salary desc, employee) as row_number,
  rank() over (partition by dept order by salary desc) as salary_rank,
  sum(salary) over (partition by dept order by salary desc, employee) as running_total
from window_demo
order by dept, salary desc, employee;`,
  },
  {
    id: "json",
    label: "JSON",
    note:
      "Loads jsonb values and queries nested fields, containment, and scalar extraction.",
    sql: `create table if not exists json_demo (
  id int4,
  payload jsonb
);
delete from json_demo;
insert into json_demo values
  (1, '{"user":"ana","active":true,"tags":["sql","wasm"]}'),
  (2, '{"user":"ben","active":false,"tags":["planner"]}'),
  (3, '{"user":"cy","active":true,"tags":["executor","json"]}');
select
  id,
  payload ->> 'user' as user_name,
  payload @> '{"active": true}'::jsonb as active,
  payload -> 'tags' as tags
from json_demo
order by id;`,
  },
  {
    id: "foreign-keys",
    label: "Foreign Keys",
    note:
      "Shows that a child row cannot be inserted unless its referenced parent row already exists.",
    sql: `create table if not exists departments (
  id int4 primary key,
  name text
);
create table if not exists employees (
  id int4 primary key,
  department_id int4 references departments(id),
  name text
);
delete from employees;
delete from departments;
insert into departments values
  (1, 'engineering'),
  (2, 'sales'),
  (3, 'support');
insert into employees values (1, 1, 'Ada');
insert into employees values (2, 99, 'Orphan');`,
  },
  {
    id: "explain-analyze-joins",
    label: "EXPLAIN ANALYZE Joins",
    note:
      "Runs `EXPLAIN (ANALYZE, BUFFERS)` over a three-table join so you can inspect join order, row counts, and buffer activity.",
    sql: `create table if not exists join_big (
  id int4,
  note text
);
create table if not exists join_medium (
  id int4,
  category text
);
create table if not exists join_small (
  id int4,
  weight int4
);
delete from join_big;
delete from join_medium;
delete from join_small;
insert into join_big values
  (1, 'alpha'),
  (2, 'beta'),
  (3, 'gamma'),
  (4, 'delta');
insert into join_medium values
  (1, 'red'),
  (2, 'blue'),
  (3, 'red'),
  (4, 'green');
insert into join_small values
  (1, 10),
  (2, 20),
  (3, 30),
  (4, 40);
explain (analyze, buffers)
select b.id, b.note, m.category, s.weight
from join_big b
join join_medium m on b.id = m.id
join join_small s on m.id = s.id
where s.weight >= 20
order by b.id;`,
  },
  {
    id: "regular-expressions",
    label: "Regular Expressions",
    note:
      "Demonstrates regex matching, replacement, and substring extraction with PostgreSQL-style regexp functions.",
    sql: `create table if not exists regex_demo (
  input text
);
delete from regex_demo;
insert into regex_demo values
  ('Order-1001'),
  ('draft-note'),
  ('Order-2450'),
  ('invoice-77');
select
  input,
  regexp_like(input, '^Order-[0-9]+$') as is_order,
  regexp_replace(input, '[0-9]+', '###') as masked,
  regexp_substr(input, '[0-9]+') as digits
from regex_demo
order by input;`,
  },
  {
    id: "lisp-fibonacci",
    label: "Lisp Interpreter - Fibonacci Numbers",
    note:
      "Evaluates a tiny Lisp-style program encoded in JSONB. This example uses a recursive CTE plus JSONB state transitions to produce Fibonacci numbers.",
    sql: `with recursive loop as (
  select '{"stack": [{"type": "expr", "env": {"+": "+", "-": "-", "*": "*", "/": "/", ">": ">", "<": "<", "=": "=", "head": "head", "tail": "tail", "cons": "cons", "empty": "empty"}, "expr": [["lambda", ["f"], ["f", "f", 1, 0, 0]], ["lambda", ["self", "a", "b", "i"], ["if", [">", "i", 10], ["empty"], ["cons", "a", ["self", "self", ["+", "a", "b"], "a", ["+", "i", 1]]]]]]}]}'::jsonb as state
  union all
  select
    case
      when frame_type = 'expr'
      then case
        when jsonb_typeof(expr) = 'number'
        then jsonb_build_object('stack', stack - 0, 'result', expr)
        when jsonb_typeof(expr) = 'string'
        then jsonb_build_object('stack', stack - 0, 'result', env -> expr_string)
        when op_string = 'if'
        then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_if', 'expr', expr, 'env', env)) || (stack - 0))
        when op_string = 'lambda'
        then jsonb_build_object('stack', stack - 0, 'result', jsonb_build_object('args', arg1, 'body', arg2, 'env', env))
        else jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_args', 'left', expr, 'done', '[]'::jsonb, 'env', env)) || (stack - 0))
      end
      when frame_type = 'eval_args'
      then case
        when result is null and jsonb_array_length(args_left) = 0
        then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_call', 'expr', args_done, 'env', env)) || (stack - 0))
        when result is null
        then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', args_left -> 0, 'env', env), jsonb_build_object('type', 'eval_args', 'left', args_left - 0, 'done', args_done, 'env', env)) || stack - 0)
        else jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'eval_args', 'left', args_left, 'done', args_done || jsonb_build_array(result), 'env', env)) || (stack - 0))
      end
      when frame_type = 'eval_call'
      then case
        when op_string = '+'
        then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint + arg2::text::bigint)
        when op_string = '*'
        then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint * arg2::text::bigint)
        when op_string = '-'
        then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint - arg2::text::bigint)
        when op_string = '/'
        then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint / arg2::text::bigint)
        when op_string = '>'
        then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint > arg2::text::bigint)
        when op_string = '<'
        then jsonb_build_object('stack', stack - 0, 'result', arg1::text::bigint < arg2::text::bigint)
        when op_string = '='
        then jsonb_build_object('stack', stack - 0, 'result', arg1 = arg2)
        when op_string = 'head'
        then jsonb_build_object('stack', stack - 0, 'result', arg1 -> 0)
        when op_string = 'tail'
        then jsonb_build_object('stack', stack - 0, 'result', arg1 - 0)
        when op_string = 'cons'
        then jsonb_build_object('stack', stack - 0, 'result', jsonb_build_array(arg1) || arg2)
        when op_string = 'empty'
        then jsonb_build_object('stack', stack - 0, 'result', '[]'::jsonb)
        else jsonb_build_object(
          'stack',
          jsonb_build_array(
            jsonb_build_object(
              'type', 'expr',
              'expr', (op -> 'body'),
              'env', (op -> 'env') || jsonb_build_object(
                coalesce(op -> 'args' ->> 0, 'null'), arg1,
                coalesce(op -> 'args' ->> 1, 'null'), arg2,
                coalesce(op -> 'args' ->> 2, 'null'), arg3,
                coalesce(op -> 'args' ->> 3, 'null'), arg4
              )
            )
          ) || (stack - 0)
        )
      end
      when frame_type = 'eval_if'
      then case
        when result is null
        then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg1, 'env', env)) || stack)
        when result is not null and result::text::boolean
        then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg2, 'env', env)) || (stack - 0))
        when result is not null and not result::text::boolean
        then jsonb_build_object('stack', jsonb_build_array(jsonb_build_object('type', 'expr', 'expr', arg3, 'env', env)) || (stack - 0))
      end
    end
  from (
    select
      state -> 'stack' -> 0 ->> 'type' as frame_type,
      state -> 'stack' -> 0 -> 'expr' as expr,
      state -> 'stack' -> 0 ->> 'expr' as expr_string,
      state -> 'stack' -> 0 -> 'expr' -> 0 as op,
      state -> 'stack' -> 0 -> 'expr' ->> 0 as op_string,
      state -> 'stack' -> 0 -> 'expr' -> 1 as arg1,
      state -> 'stack' -> 0 -> 'expr' -> 2 as arg2,
      state -> 'stack' -> 0 -> 'expr' -> 3 as arg3,
      state -> 'stack' -> 0 -> 'expr' -> 4 as arg4,
      state -> 'stack' -> 0 -> 'left' as args_left,
      state -> 'stack' -> 0 -> 'done' as args_done,
      state -> 'stack' -> 0 -> 'env' as env,
      state -> 'result' as result,
      state -> 'stack' as stack
    from loop
  ) sub
)
select jsonb_pretty(state -> 'result')
from loop
where jsonb_array_length(state -> 'stack') = 0
limit 1;`,
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
