#!/usr/bin/env python3
"""Convert a CockroachDB logictest file into a sqllogictest subset.

This is intentionally lossy. It extracts the records that fit the generic
sqllogictest runner and skips Cockroach-specific directives or SQL shapes.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


SQLSTATE_RE = re.compile(r"pgcode\s+([0-9A-Z]{5})\b(.*)")
INDEX_HINT_RE = re.compile(r"@\{|\@\[[^\]]*\]|\@[A-Za-z_][A-Za-z0-9_]*")
INLINE_INDEX_RE = re.compile(r",\s*INDEX\s+(?:[A-Za-z_][A-Za-z0-9_]*\s*)?\([^)]*\)", re.IGNORECASE)
INLINE_UNIQUE_INDEX_RE = re.compile(
    r"\bunique\s+index\s+(?:[A-Za-z_][A-Za-z0-9_]*\s*)?\(",
    re.IGNORECASE,
)
FAMILY_CLAUSE_RE = re.compile(
    r",\s*FAMILY\s+(?:(?:\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*)\s*)?\([^)]*\)",
    re.IGNORECASE,
)
UNSUPPORTED_SQL_PATTERNS = [
    "@{",
    ":::",
    "create family",
    "crdb_internal.",
    "from [",
    "join [",
    "schema_locked",
    "st_makepointm(",
]
ALLOWED_QUERY_OPTIONS = {"rowsort", "valuesort", "nosort", "colnames"}


def should_skip_sql(sql: str) -> bool:
    lowered = sql.lower()
    if INDEX_HINT_RE.search(sql):
        return True
    if lowered.lstrip().startswith("delete ") and " order by " in lowered and " limit " in lowered:
        return True
    return any(pattern.lower() in lowered for pattern in UNSUPPORTED_SQL_PATTERNS)


def rewrite_sql(sql: str) -> str:
    rewritten = INLINE_UNIQUE_INDEX_RE.sub("unique (", sql)
    if rewritten.lstrip().upper().startswith("CREATE TABLE"):
        rewritten = INLINE_INDEX_RE.sub("", rewritten)
        rewritten = FAMILY_CLAUSE_RE.sub("", rewritten)
    return rewritten


def normalize_error_header(kind: str, header: str) -> str:
    match = SQLSTATE_RE.search(header)
    if match:
        return f"{kind} error ({match.group(1)})"

    header = re.sub(r"\bpq:\s*", "", header).strip()
    if "<" in header or ">" in header:
        return f"{kind} error"
    return f"{kind} error {header}".rstrip()


def parse_record(lines: list[str], start: int) -> tuple[int, dict[str, object] | None]:
    line = lines[start].rstrip("\n")
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return start + 1, None

    if stripped.startswith(("subtest ", "halt", "sleep ")):
        return start + 1, {"kind": "skip-meta", "reason": stripped}

    if stripped.startswith(("onlyif ", "skipif ")):
        return start + 1, {"kind": "skip-next", "reason": stripped}

    parts = stripped.split()
    if not parts or parts[0] not in {"statement", "query"}:
        return start + 1, {"kind": "skip-meta", "reason": stripped}

    kind = parts[0]
    if len(parts) < 2:
        return start + 1, {"kind": "skip-record", "reason": stripped}

    mode = parts[1]
    options = [part for part in parts[2:] if part in ALLOWED_QUERY_OPTIONS]
    header_tail = " ".join(parts[2:])
    i = start + 1

    sql_lines: list[str] = []
    expected_lines: list[str] = []
    separator_seen = False

    while i < len(lines):
        current = lines[i].rstrip("\n")
        if current == "----":
            separator_seen = True
            i += 1
            break
        if current.strip() == "":
            break
        sql_lines.append(current)
        i += 1

    if separator_seen:
        while i < len(lines):
            current = lines[i].rstrip("\n")
            if current.strip() == "":
                break
            expected_lines.append(current)
            i += 1

    while i < len(lines) and lines[i].strip() == "":
        i += 1

    record = {
        "kind": kind,
        "mode": mode,
        "options": options,
        "header_tail": header_tail,
        "sql": "\n".join(sql_lines).strip(),
        "expected": expected_lines,
    }
    return i, record


def convert_records(
    lines: list[str], source: Path, success_only: bool
) -> tuple[list[str], list[str]]:
    output: list[str] = [f"# Converted from {source}"]
    stats: list[str] = []

    i = 0
    skip_next = False
    converted = 0
    skipped = 0

    while i < len(lines):
        next_i, record = parse_record(lines, i)
        i = next_i
        if record is None:
            continue

        kind = record["kind"]
        if kind == "skip-next":
            skip_next = True
            skipped += 1
            stats.append(f"skip-next: {record['reason']}")
            continue
        if kind == "skip-meta":
            skipped += 1
            stats.append(f"skip-meta: {record['reason']}")
            continue

        sql = rewrite_sql(str(record["sql"]))
        if not sql:
            skipped += 1
            stats.append("skip-empty-record")
            continue

        if skip_next:
            skip_next = False
            skipped += 1
            stats.append(f"skip-directed: {sql.splitlines()[0]}")
            continue

        if should_skip_sql(sql):
            skipped += 1
            stats.append(f"skip-sql: {sql.splitlines()[0]}")
            continue

        if record["kind"] == "statement":
            if success_only and record["mode"] == "error":
                skipped += 1
                stats.append(f"skip-error-record: {sql.splitlines()[0]}")
                continue
            if record["mode"] == "ok":
                output.extend(["statement ok", sql, ""])
                converted += 1
                continue
            if record["mode"] == "count":
                count = str(record["header_tail"]).strip().split()[0] if record["header_tail"] else ""
                if count.isdigit():
                    output.extend([f"statement count {count}", sql, ""])
                    converted += 1
                    continue
            if record["mode"] == "error":
                output.extend([normalize_error_header("statement", record["header_tail"]), sql, ""])
                converted += 1
                continue

            skipped += 1
            stats.append(f"skip-statement-mode: {record['mode']}")
            continue

        if record["kind"] == "query":
            mode = str(record["mode"])
            options = [opt for opt in record["options"] if opt != "colnames"]
            expected = list(record["expected"])

            if "colnames" in record["options"] and expected:
                expected = expected[1:]

            if mode == "error":
                if success_only:
                    skipped += 1
                    stats.append(f"skip-error-record: {sql.splitlines()[0]}")
                    continue
                output.extend([normalize_error_header("query", record["header_tail"]), sql, ""])
                converted += 1
                continue

            header = f"query {mode}"
            if "rowsort" in options:
                header += " rowsort"
            elif "valuesort" in options:
                header += " valuesort"

            output.extend([header, sql, "----"])
            output.extend(expected)
            output.append("")
            converted += 1
            continue

        skipped += 1
        stats.append(f"skip-unknown: {record}")

    stats.insert(0, f"converted={converted}")
    stats.insert(1, f"skipped={skipped}")
    return output, stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--success-only",
        action="store_true",
        help="skip Cockroach records that expected an error before PostgreSQL materialization",
    )
    args = parser.parse_args()

    lines = args.input.read_text().splitlines(keepends=True)
    output, stats = convert_records(lines, args.input, args.success_only)
    args.output.write_text("\n".join(output).rstrip() + "\n")
    for line in stats:
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
