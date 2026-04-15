import init, { WasmEngine } from "./pkg/pgrust.js";

const output = document.getElementById("output");
const sql = document.getElementById("sql");
const run = document.getElementById("run");
const reset = document.getElementById("reset");

let engine;

function show(value) {
  output.textContent =
    typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

function splitStatements(script) {
  const statements = [];
  let current = "";
  let singleQuoted = false;
  let doubleQuoted = false;

  for (let i = 0; i < script.length; i++) {
    const ch = script[i];
    const prev = i > 0 ? script[i - 1] : "";

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

async function boot() {
  await init();
  engine = new WasmEngine(64);
  show({ ok: true, status: "ready" });
}

run.addEventListener("click", () => {
  try {
    const results = splitStatements(sql.value).map((statement) =>
      JSON.parse(engine.execute(statement)),
    );
    show(results.length === 1 ? results[0] : results);
  } catch (error) {
    try {
      show(JSON.parse(String(error)));
    } catch {
      show({ ok: false, error: String(error) });
    }
  }
});

reset.addEventListener("click", () => {
  engine.reset(64);
  show({ ok: true, status: "reset" });
});

boot().catch((error) => {
  show({ ok: false, error: String(error) });
});
