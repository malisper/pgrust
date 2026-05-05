#!/usr/bin/env python3
"""Build fast EXPLAIN-only regression fixtures from plan-diff queries."""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Block:
    kind: str
    text: str
    start: int
    end: int


@dataclass
class Target:
    test: str
    marker: str
    stmt: str
    norm: str


SETUP_KEYWORDS = {
    "alter",
    "analyze",
    "begin",
    "call",
    "checkpoint",
    "close",
    "cluster",
    "comment",
    "commit",
    "copy",
    "create",
    "deallocate",
    "declare",
    "delete",
    "discard",
    "do",
    "drop",
    "execute",
    "grant",
    "insert",
    "listen",
    "load",
    "lock",
    "merge",
    "move",
    "notify",
    "prepare",
    "reindex",
    "release",
    "reset",
    "revoke",
    "rollback",
    "savepoint",
    "security",
    "select",  # only when SELECT INTO / psql side-effect is detected.
    "set",
    "start",
    "truncate",
    "unlisten",
    "update",
    "vacuum",
    "with",  # only when data-modifying WITH is detected.
}

SQL_START_KEYWORDS = SETUP_KEYWORDS | {
    "explain",
    "fetch",
    "select",
    "show",
    "table",
    "values",
}

PLAN_OUTPUT_START_RE = re.compile(
    r"^\s*(?:"
    r"->|"
    r"Output:|Sort Key:|Group Key:|Filter:|Index Cond:|Hash Cond:|Merge Cond:|Join Filter:|"
    r"Rows Removed|Workers Planned:|Workers Launched:|One-Time Filter:|Disabled:|"
    r"Seq Scan\b|Index Scan\b|Index Only Scan\b|Bitmap\b|Tid Scan\b|"
    r"Nested Loop\b|Hash Join\b|Merge Join\b|Hash\b|"
    r"Aggregate\b|GroupAggregate\b|HashAggregate\b|Finalize Aggregate\b|Partial Aggregate\b|"
    r"Sort\b|Incremental Sort\b|Result\b|Limit\b|Append\b|Merge Append\b|Gather\b|Gather Merge\b|"
    r"Insert on\b|Update on\b|Delete on\b|Merge on\b|ModifyTable\b|"
    r"Unique\b|WindowAgg\b|ProjectSet\b|Subquery Scan\b|Function Scan\b|CTE Scan\b"
    r")",
    re.I,
)

KEPT_META = (
    "\\connect",
    "\\copy",
    "\\else",
    "\\elif",
    "\\endif",
    "\\getenv",
    "\\gexec",
    "\\gset",
    "\\if",
    "\\i",
    "\\include",
    "\\ir",
    "\\o",
    "\\pset",
    "\\set",
    "\\unset",
)


def remove_sql_comments(sql: str) -> str:
    out: list[str] = []
    i = 0
    state = "normal"
    dollar_tag = ""
    while i < len(sql):
        c = sql[i]
        n = sql[i + 1] if i + 1 < len(sql) else ""
        if state == "line_comment":
            if c == "\n":
                out.append(c)
                state = "normal"
            else:
                out.append(" ")
            i += 1
            continue
        if state == "block_comment":
            if c == "*" and n == "/":
                out.extend("  ")
                i += 2
                state = "normal"
            else:
                out.append("\n" if c == "\n" else " ")
                i += 1
            continue
        if state == "single":
            out.append(c)
            if c == "'" and n == "'":
                out.append(n)
                i += 2
            elif c == "'":
                state = "normal"
                i += 1
            else:
                i += 1
            continue
        if state == "double":
            out.append(c)
            if c == '"' and n == '"':
                out.append(n)
                i += 2
            elif c == '"':
                state = "normal"
                i += 1
            else:
                i += 1
            continue
        if state == "dollar":
            if sql.startswith(dollar_tag, i):
                out.append(dollar_tag)
                i += len(dollar_tag)
                state = "normal"
            else:
                out.append(c)
                i += 1
            continue

        if c == "-" and n == "-":
            out.extend("  ")
            i += 2
            state = "line_comment"
        elif c == "/" and n == "*":
            out.extend("  ")
            i += 2
            state = "block_comment"
        elif c == "'":
            out.append(c)
            i += 1
            state = "single"
        elif c == '"':
            out.append(c)
            i += 1
            state = "double"
        elif c == "$":
            m = re.match(r"\$[A-Za-z_][A-Za-z_0-9]*\$|\$\$", sql[i:])
            if m:
                dollar_tag = m.group(0)
                out.append(dollar_tag)
                i += len(dollar_tag)
                state = "dollar"
            else:
                out.append(c)
                i += 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def has_unquoted_semicolon(sql: str) -> bool:
    stripped = remove_sql_comments(sql)
    state = "normal"
    dollar_tag = ""
    i = 0
    while i < len(stripped):
        c = stripped[i]
        n = stripped[i + 1] if i + 1 < len(stripped) else ""
        if state == "single":
            if c == "'" and n == "'":
                i += 2
            elif c == "'":
                state = "normal"
                i += 1
            else:
                i += 1
            continue
        if state == "double":
            if c == '"' and n == '"':
                i += 2
            elif c == '"':
                state = "normal"
                i += 1
            else:
                i += 1
            continue
        if state == "dollar":
            if stripped.startswith(dollar_tag, i):
                i += len(dollar_tag)
                state = "normal"
            else:
                i += 1
            continue
        if c == "'":
            state = "single"
        elif c == '"':
            state = "double"
        elif c == "$":
            m = re.match(r"\$[A-Za-z_][A-Za-z_0-9]*\$|\$\$", stripped[i:])
            if m:
                dollar_tag = m.group(0)
                i += len(dollar_tag)
                state = "dollar"
                continue
        elif c == ";":
            return True
        i += 1
    return False


def normalize_sql(sql: str) -> str:
    kept_lines = []
    for line in sql.splitlines():
        if line.lstrip().startswith("\\"):
            continue
        kept_lines.append(line)
    stripped = remove_sql_comments("\n".join(kept_lines)).strip()
    stripped = re.sub(r";\s*\Z", "", stripped)
    return re.sub(r"\s+", " ", stripped).casefold()


def first_keyword(sql: str) -> str:
    stripped = remove_sql_comments(sql)
    m = re.search(r"[A-Za-z_][A-Za-z_0-9]*", stripped)
    return m.group(0).casefold() if m else ""


def start_keyword(line: str) -> str:
    m = re.match(r"\s*([A-Za-z_][A-Za-z_0-9]*)\b", remove_sql_comments(line))
    return m.group(1).casefold() if m else ""


def looks_like_sql_start(line: str) -> bool:
    stripped = line.lstrip()
    if not stripped:
        return False
    if PLAN_OUTPUT_START_RE.match(line):
        return False
    return bool(
        re.match(r"--($|[ \t])", stripped)
        or stripped.startswith("/*")
        or start_keyword(line) in SQL_START_KEYWORDS
    )


def is_copy_from_stdin(sql: str) -> bool:
    cleaned = normalize_sql(sql)
    return cleaned.startswith("copy ") and " from stdin" in cleaned


def split_blocks(text: str, *, consume_copy_data: bool = True) -> list[Block]:
    lines = text.splitlines()
    blocks: list[Block] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if line.lstrip().startswith("\\"):
            blocks.append(Block("meta", line + "\n", i, i))
            i += 1
            continue
        stripped = line.lstrip()
        if not looks_like_sql_start(line):
            i += 1
            continue

        start = i
        buf = [line]
        i += 1
        while i < len(lines) and not has_unquoted_semicolon("\n".join(buf)):
            if lines[i].lstrip().startswith("\\gset") or lines[i].lstrip().startswith("\\gexec"):
                buf.append(lines[i])
                i += 1
                break
            buf.append(lines[i])
            i += 1

        if consume_copy_data and is_copy_from_stdin("\n".join(buf)):
            probe = i
            while probe < len(lines) and not lines[probe].strip():
                probe += 1
            if probe < len(lines) and lines[probe].strip() != "\\." and looks_like_sql_start(lines[probe]):
                blocks.append(Block("sql", "\n".join(buf).rstrip() + "\n", start, i - 1))
                continue
            while i < len(lines):
                buf.append(lines[i])
                done = lines[i].strip() == "\\."
                i += 1
                if done:
                    break

        blocks.append(Block("sql", "\n".join(buf).rstrip() + "\n", start, i - 1))
    return blocks


def should_keep_meta(block: Block) -> bool:
    stripped = block.text.lstrip()
    return stripped.startswith(KEPT_META)


def should_keep_setup(block: Block) -> bool:
    if block.kind == "meta":
        return should_keep_meta(block)
    keyword = first_keyword(block.text)
    if keyword not in SETUP_KEYWORDS:
        return False
    cleaned = normalize_sql(block.text)
    if keyword == "select":
        return (
            bool(re.search(r"\binto\s+(?:temporary\s+|temp\s+|unlogged\s+)?(?:table\s+)?[A-Za-z_]", cleaned))
            or "\\gset" in block.text
            or "\\gexec" in block.text
            or "set_config(" in cleaned
        )
    if keyword == "with":
        return bool(re.search(r"\b(insert\s+into|update\s+.+\s+set|delete\s+from|merge\s+into)\b", cleaned))
    if keyword == "execute":
        return False
    return True


def parse_plan_targets(plan_query_sql: Path) -> dict[str, list[Target]]:
    lines = plan_query_sql.read_text(errors="replace").splitlines()
    targets: dict[str, list[Target]] = defaultdict(list)
    current_test = ""
    current_marker = ""
    current: list[str] = []

    def flush() -> None:
        nonlocal current
        if not current_test or not current_marker or not current:
            current = []
            return
        stmt_text = "\n".join(current).strip() + "\n"
        sql_blocks = [b for b in split_blocks(stmt_text) if b.kind == "sql"]
        sql_blocks = [b for b in sql_blocks if re.search(r"\bexplain\b|\bexplain_filter\b", b.text, re.I)]
        if sql_blocks:
            stmt = sql_blocks[-1].text.rstrip()
            targets[current_test].append(Target(current_test, current_marker, stmt, normalize_sql(stmt)))
        current = []

    for line in lines:
        if line.startswith("\\echo ==== "):
            flush()
            current_test = line.removeprefix("\\echo ==== ").removesuffix(" ====")
            current_marker = ""
        elif line.startswith("\\echo ---- plan-diff "):
            flush()
            current_marker = line.removeprefix("\\echo ")
        elif current_marker:
            current.append(line)
    flush()
    return dict(targets)


def block_output_ranges(blocks: list[Block], total_lines: int) -> dict[int, tuple[int, int]]:
    ranges = {}
    sql_blocks = [b for b in blocks if b.kind == "sql"]
    for idx, block in enumerate(sql_blocks):
        next_start = sql_blocks[idx + 1].start if idx + 1 < len(sql_blocks) else total_lines
        ranges[id(block)] = (block.end + 1, next_start)
    return ranges


def expected_outputs(expected_path: Path) -> dict[str, deque[str]]:
    text = expected_path.read_text(errors="replace")
    lines = text.splitlines()
    blocks = split_blocks(text, consume_copy_data=False)
    ranges = block_output_ranges(blocks, len(lines))
    out: dict[str, deque[str]] = defaultdict(deque)
    for block in blocks:
        if block.kind != "sql":
            continue
        norm = normalize_sql(block.text)
        if not norm:
            continue
        start, end = ranges[id(block)]
        output = "\n".join(lines[start:end]).rstrip()
        if output:
            output += "\n"
        out[norm].append(output)
    return out


def write_plan_fixture(
    test: str,
    source_path: Path,
    expected_path: Path,
    targets: list[Target],
    sql_out: Path,
    expected_out: Path,
) -> tuple[int, list[str]]:
    source_blocks = split_blocks(source_path.read_text(errors="replace"))
    expected_by_norm = expected_outputs(expected_path)
    targets_by_norm: dict[str, deque[Target]] = defaultdict(deque)
    for target in targets:
        targets_by_norm[target.norm].append(target)

    target_indices = [
        i for i, block in enumerate(source_blocks)
        if block.kind == "sql" and targets_by_norm.get(normalize_sql(block.text))
    ]
    warnings: list[str] = []
    if not target_indices:
        warnings.append(f"{test}: no plan targets matched source SQL")
        sql_out.write_text("\\set ON_ERROR_STOP off\n\\pset pager off\n", encoding="utf-8")
        expected_out.write_text("", encoding="utf-8")
        return 0, warnings

    last_target = max(target_indices)
    emitted = 0
    sql_parts = [
        f"-- Fast EXPLAIN-only fixture for {test}.sql.\n",
        "\\set ON_ERROR_STOP off\n",
        "\\pset pager off\n",
        "\\o /dev/null\n\n",
    ]
    expected_parts: list[str] = []

    for i, block in enumerate(source_blocks[: last_target + 1]):
        norm = normalize_sql(block.text) if block.kind == "sql" else ""
        target = targets_by_norm[norm].popleft() if block.kind == "sql" and targets_by_norm.get(norm) else None
        if target:
            emitted += 1
            sql_parts.append("\\o\n")
            sql_parts.append(f"\\echo {target.marker}\n")
            sql_parts.append(block.text.rstrip() + "\n")
            sql_parts.append("\\o /dev/null\n\n")

            expected_parts.append(target.marker + "\n")
            if expected_by_norm.get(norm):
                expected_parts.append(expected_by_norm[norm].popleft())
            else:
                warnings.append(f"{test}: expected output not found for {target.marker}")
            continue

        if should_keep_setup(block):
            sql_parts.append(block.text.rstrip() + "\n\n")

    for norm, pending in targets_by_norm.items():
        for target in pending:
            warnings.append(f"{test}: target not emitted from source SQL: {target.marker}")

    sql_parts.append("\\o\n")
    sql_out.write_text("".join(sql_parts), encoding="utf-8")
    expected_out.write_text("".join(expected_parts), encoding="utf-8")
    return emitted, warnings


def write_setup_fixture(test: str, source_path: Path, sql_out: Path) -> int:
    blocks = split_blocks(source_path.read_text(errors="replace"))
    kept = 0
    parts = [
        f"-- Setup-only fixture for dependency {test}.sql.\n",
        "\\set ON_ERROR_STOP off\n",
        "\\pset pager off\n",
        "\\o /dev/null\n\n",
    ]
    for block in blocks:
        if should_keep_setup(block):
            kept += 1
            parts.append(block.text.rstrip() + "\n\n")
    parts.append("\\o\n")
    sql_out.write_text("".join(parts), encoding="utf-8")
    return kept


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan-query-sql", required=True, type=Path)
    parser.add_argument("--pg-regress", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--test", action="append", default=[])
    parser.add_argument("--dependency-test", action="append", default=[])
    args = parser.parse_args()

    targets = parse_plan_targets(args.plan_query_sql)
    selected = args.test or sorted(targets)
    sql_dir = args.pg_regress / "sql"
    expected_dir = args.pg_regress / "expected"
    fixture_sql = args.out_dir / "sql"
    fixture_expected = args.out_dir / "expected"
    fixture_deps = args.out_dir / "deps"
    fixture_sql.mkdir(parents=True, exist_ok=True)
    fixture_expected.mkdir(parents=True, exist_ok=True)
    fixture_deps.mkdir(parents=True, exist_ok=True)

    manifest = ["kind\ttest\titems\tpath\n"]
    all_warnings: list[str] = []

    for dep in args.dependency_test:
        source = sql_dir / f"{dep}.sql"
        if not source.exists():
            all_warnings.append(f"{dep}: dependency source not found: {source}")
            continue
        out = fixture_deps / f"{dep}.sql"
        kept = write_setup_fixture(dep, source, out)
        manifest.append(f"dependency\t{dep}\t{kept}\t{out}\n")

    for test in selected:
        source = sql_dir / f"{test}.sql"
        expected = expected_dir / f"{test}.out"
        if not source.exists():
            all_warnings.append(f"{test}: source not found: {source}")
            continue
        if not expected.exists():
            all_warnings.append(f"{test}: expected not found: {expected}")
            continue
        emitted, warnings = write_plan_fixture(
            test,
            source,
            expected,
            targets.get(test, []),
            fixture_sql / f"{test}.sql",
            fixture_expected / f"{test}.out",
        )
        all_warnings.extend(warnings)
        manifest.append(f"test\t{test}\t{emitted}\t{fixture_sql / f'{test}.sql'}\n")

    (args.out_dir / "manifest.tsv").write_text("".join(manifest), encoding="utf-8")
    if all_warnings:
        (args.out_dir / "warnings.log").write_text("\n".join(all_warnings) + "\n", encoding="utf-8")
        for warning in all_warnings:
            print(f"warning: {warning}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
