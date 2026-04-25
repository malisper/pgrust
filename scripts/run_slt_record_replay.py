#!/usr/bin/env python3
"""Replay sqllogictest records independently against pgrust.

For each target record, this script creates a fresh pgrust database, replays
earlier state-building records as unchecked setup, then runs only the target
record with its original sqllogictest expectation.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


FIRST_FAILURE_RE = re.compile(r"Caused by:\n(?P<message>.*?)(?:\n\n|$)", re.S)
FAILURE_LOCATION_RE = re.compile(r"\bat (?P<path>/[^:\n]+):(?P<line>\d+)\b")
RECORD_HEADER_RE = re.compile(r"^(statement|query)\b")
STATEFUL_SQL_PREFIXES = {
    "ALTER",
    "CREATE",
    "DEALLOCATE",
    "DELETE",
    "DROP",
    "INSERT",
    "PREPARE",
    "RESET",
    "SET",
    "TRUNCATE",
    "UPDATE",
}


@dataclass(frozen=True)
class SltRecord:
    index: int
    line: int
    kind: str
    header: str
    sql: str
    text: str

    @property
    def expected_error(self) -> bool:
        return self.header.startswith("statement error") or self.header.startswith("query error")

    @property
    def first_sql_line(self) -> str:
        return self.sql.splitlines()[0] if self.sql else ""


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_slt(path: Path) -> list[SltRecord]:
    lines = path.read_text().splitlines()
    records: list[SltRecord] = []
    i = 0

    while i < len(lines):
        line = lines[i]
        if not RECORD_HEADER_RE.match(line):
            i += 1
            continue

        start = i
        header = line
        kind = line.split()[0]
        i += 1

        sql_lines: list[str] = []
        separator_seen = False
        while i < len(lines):
            current = lines[i]
            if current == "----":
                separator_seen = True
                i += 1
                break
            if current.strip() == "":
                break
            sql_lines.append(current)
            i += 1

        if separator_seen:
            while i < len(lines) and lines[i].strip() != "":
                i += 1

        block_end = i
        while i < len(lines) and lines[i].strip() == "":
            i += 1

        records.append(
            SltRecord(
                index=len(records) + 1,
                line=start + 1,
                kind=kind,
                header=header,
                sql="\n".join(sql_lines),
                text="\n".join(lines[start:block_end]).rstrip() + "\n",
            )
        )

    return records


def first_sql_keyword(sql: str) -> str:
    stripped = sql.lstrip()
    match = re.match(r"([A-Za-z_]+)", stripped)
    return match.group(1).upper() if match else ""


def is_setup_record(record: SltRecord) -> bool:
    if record.expected_error:
        return False
    return first_sql_keyword(record.sql) in STATEFUL_SQL_PREFIXES


def build_replay_file(records: list[SltRecord], target: SltRecord, output: Path) -> int:
    replay_lines = [
        f"# Replay for target record {target.index}, original line {target.line}",
        "# Earlier successful stateful records are run as unchecked setup.",
        "",
    ]

    setup_records = [record for record in records[: target.index - 1] if is_setup_record(record)]
    for record in setup_records:
        replay_lines.extend(
            [
                f"# setup record {record.index}, original line {record.line}",
                "statement ok",
                record.sql,
                "",
            ]
        )

    replay_lines.append(f"# target record {target.index}, original line {target.line}")
    target_line = len(replay_lines) + 1
    replay_lines.extend(target.text.rstrip().splitlines())
    replay_lines.append("")

    output.write_text("\n".join(replay_lines))
    return target_line


def extract_first_failure(output: str) -> str | None:
    match = FIRST_FAILURE_RE.search(output)
    if not match:
        return None
    lines = [line.rstrip() for line in match.group("message").splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines[:8]) if lines else None


def extract_failure_line(output: str, replay_file: Path) -> int | None:
    replay_resolved = replay_file.resolve()
    for match in FAILURE_LOCATION_RE.finditer(output):
        path = Path(match.group("path"))
        try:
            same_file = path.exists() and path.resolve() == replay_resolved
        except OSError:
            same_file = False
        if same_file:
            return int(match.group("line"))
    return None


def resolve_sqllogictest_bin(args: argparse.Namespace) -> Path | None:
    if args.sqllogictest_bin:
        return args.sqllogictest_bin
    if args.sqllogictest_dir:
        candidate = args.sqllogictest_dir / "target" / "debug" / "sqllogictest"
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def build_pgrust_once(skip_build: bool) -> None:
    if skip_build:
        return
    subprocess.run(
        ["cargo", "build", "--release", "--bin", "pgrust_server"],
        cwd=repo_root(),
        check=True,
    )


def run_target(
    script_dir: Path,
    replay_file: Path,
    target_line: int,
    target: SltRecord,
    args: argparse.Namespace,
    sqllogictest_bin: Path | None,
    port_allocator: PortAllocator,
) -> dict[str, object]:
    target_dir = args.results_dir / f"{target.index:04d}"
    target_dir.mkdir(parents=True, exist_ok=True)
    log_path = target_dir / "run.log"
    data_dir = target_dir / "data"
    port = port_allocator.choose(args.port_base + target.index)

    cmd = [
        str(script_dir / "run_sqllogictest.sh"),
        "--skip-build",
        "--port",
        str(port),
        "--files",
        str(replay_file),
        "--results-dir",
        str(target_dir / "sqllogictest"),
        "--data-dir",
        str(data_dir),
        "--junit-name",
        "",
    ]

    if args.sqllogictest_dir:
        cmd.extend(["--sqllogictest-dir", str(args.sqllogictest_dir)])
    if sqllogictest_bin:
        cmd.extend(["--sqllogictest-bin", str(sqllogictest_bin)])

    proc = subprocess.run(
        cmd,
        cwd=repo_root(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    log_path.write_text(proc.stdout)

    failure_line = extract_failure_line(proc.stdout, replay_file)
    if proc.returncode == 0:
        status = "pass"
    elif failure_line is not None and failure_line < target_line:
        status = "setup_failed"
    elif failure_line is not None:
        status = "fail"
    else:
        status = "unknown"

    if not args.keep_data:
        shutil.rmtree(data_dir, ignore_errors=True)

    return {
        "index": target.index,
        "line": target.line,
        "kind": target.kind,
        "header": target.header,
        "sql": target.sql,
        "status": status,
        "exit_code": proc.returncode,
        "replay_file": str(replay_file),
        "target_line": target_line,
        "port": port,
        "failure_line": failure_line,
        "log_path": str(log_path),
        "first_failure": extract_first_failure(proc.stdout),
    }


class PortAllocator:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._used: set[int] = set()

    def choose(self, preferred: int) -> int:
        with self._lock:
            port = preferred
            while port < 65535:
                if port not in self._used and port_is_available(port):
                    self._used.add(port)
                    return port
                port += 1
        raise RuntimeError(f"no available TCP port at or above {preferred}")


def port_is_available(port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.1)
            return sock.connect_ex(("127.0.0.1", port)) != 0
    except OSError:
        return False


def summarize(records: list[dict[str, object]]) -> dict[str, object]:
    totals = {
        "total": len(records),
        "pass": sum(1 for record in records if record["status"] == "pass"),
        "fail": sum(1 for record in records if record["status"] == "fail"),
        "setup_failed": sum(1 for record in records if record["status"] == "setup_failed"),
        "unknown": sum(1 for record in records if record["status"] == "unknown"),
    }
    asserted = totals["pass"] + totals["fail"]
    totals["asserted"] = asserted
    totals["pass_percent_of_asserted"] = round((totals["pass"] / asserted) * 100, 1) if asserted else None
    totals["pass_percent_of_total"] = (
        round((totals["pass"] / totals["total"]) * 100, 1) if totals["total"] else None
    )
    return totals


def main() -> int:
    started = time.monotonic()
    script_dir = Path(__file__).resolve().parent
    default_results = Path(tempfile.mkdtemp(prefix="pgrust_slt_record_replay."))

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, default=default_results)
    parser.add_argument("--sqllogictest-dir", type=Path)
    parser.add_argument("--sqllogictest-bin", type=Path)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--port-base", type=int, default=6450)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be >= 1")

    args.results_dir.mkdir(parents=True, exist_ok=True)
    replay_dir = args.results_dir / "replays"
    replay_dir.mkdir(parents=True, exist_ok=True)

    build_pgrust_once(args.skip_build)
    sqllogictest_bin = resolve_sqllogictest_bin(args)

    records = parse_slt(args.file)
    targets = records[: args.limit] if args.limit is not None else records
    port_allocator = PortAllocator()
    replay_jobs = []
    results: list[dict[str, object]] = []

    for target in targets:
        replay_file = replay_dir / f"{target.index:04d}.slt"
        target_line = build_replay_file(records, target, replay_file)
        replay_jobs.append((target, replay_file, target_line))

    if args.jobs == 1:
        for target, replay_file, target_line in replay_jobs:
            result = run_target(script_dir, replay_file, target_line, target, args, sqllogictest_bin, port_allocator)
            results.append(result)
            print_result(result, target, len(records))
    else:
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            futures = {
                executor.submit(
                    run_target,
                    script_dir,
                    replay_file,
                    target_line,
                    target,
                    args,
                    sqllogictest_bin,
                    port_allocator,
                ): target
                for target, replay_file, target_line in replay_jobs
            }
            for future in as_completed(futures):
                target = futures[future]
                result = future.result()
                results.append(result)
                print_result(result, target, len(records))

    results.sort(key=lambda item: item["index"])

    elapsed_seconds = round(time.monotonic() - started, 3)
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_file": str(args.file),
        "results_dir": str(args.results_dir),
        "metric": "independent_record_replay",
        "setup_policy": "replay earlier successful stateful records as statement ok",
        "jobs": args.jobs,
        "elapsed_seconds": elapsed_seconds,
        "records": results,
        "totals": summarize(results),
    }

    summary_path = args.results_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(f"summary: {summary_path}")
    print(
        "totals: "
        f"{summary['totals']['pass']} pass, "
        f"{summary['totals']['fail']} fail, "
        f"{summary['totals']['setup_failed']} setup_failed, "
        f"{summary['totals']['unknown']} unknown, "
        f"elapsed {elapsed_seconds}s"
    )
    return (
        0
        if summary["totals"]["fail"] == 0
        and summary["totals"]["setup_failed"] == 0
        and summary["totals"]["unknown"] == 0
        else 1
    )


def print_result(result: dict[str, object], target: SltRecord, total_records: int) -> None:
    print(
        f"[{target.index}/{total_records}] {result['status']} "
        f"{target.kind} line={target.line} sql={target.first_sql_line}",
        flush=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
