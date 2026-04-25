#!/usr/bin/env python3
"""Run a Cockroach logictest manifest through the pgrust sqllogictest wrapper."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


FIRST_FAILURE_RE = re.compile(r"Caused by:\n(?P<message>.*?)(?:\n\n|$)", re.S)
CONVERTED_RE = re.compile(r"^converted=(\d+)$", re.M)
SKIPPED_RE = re.compile(r"^skipped=(\d+)$", re.M)
MATERIALIZED_RE = re.compile(r"Materializing expected output with PostgreSQL")
CONVERTED_FILE_RE = re.compile(r"^Converted file:\s*(?P<path>.+)$", re.M)
FAILURE_LOCATION_RE = re.compile(r"\bat (?P<path>/[^:\n]+):(?P<line>\d+)\b")
RECORD_HEADER_RE = re.compile(r"^(statement|query)\b")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def read_manifest(path: Path) -> list[str]:
    tests: list[str] = []
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        tests.append(line)
    return tests


def run_one(
    script_dir: Path,
    test_name: str,
    args: argparse.Namespace,
    index: int,
) -> dict[str, object]:
    test_results = args.results_dir / f"{index:03d}-{safe_name(test_name)}"
    test_results.mkdir(parents=True, exist_ok=True)
    log_path = test_results / "run.log"
    data_dir = test_results / "pgrust-data"

    cmd = [
        str(script_dir / "run_cockroach_logic_test.sh"),
        "--test",
        test_name,
        "--results-dir",
        str(test_results / "sqllogictest"),
        "--data-dir",
        str(data_dir),
        "--port",
        str(args.base_port + index),
        "--postgres-port",
        str(args.postgres_base_port + index),
        "--keep-converted",
    ]

    if args.cockroach_dir:
        cmd.extend(["--cockroach-dir", str(args.cockroach_dir)])
    if args.sqllogictest_dir:
        cmd.extend(["--sqllogictest-dir", str(args.sqllogictest_dir)])
    if args.postgres_oracle:
        cmd.append("--postgres-oracle")
    if args.skip_build:
        cmd.append("--skip-build")

    env = os.environ.copy()
    proc = subprocess.run(cmd, cwd=repo_root(), text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    log_path.write_text(proc.stdout)

    converted_match = CONVERTED_RE.search(proc.stdout)
    skipped_match = SKIPPED_RE.search(proc.stdout)
    first_failure = extract_first_failure(proc.stdout)
    converted = int(converted_match.group(1)) if converted_match else None
    converted_path = extract_converted_path(proc.stdout)

    result = {
        "test": test_name,
        "status": "pass" if proc.returncode == 0 else "fail",
        "exit_code": proc.returncode,
        "converted": converted,
        "skipped": int(skipped_match.group(1)) if skipped_match else None,
        "postgres_oracle": args.postgres_oracle,
        "postgres_materialized": bool(MATERIALIZED_RE.search(proc.stdout)),
        "results_dir": str(test_results),
        "log_path": str(log_path),
        "first_failure": first_failure,
        "record_progress": record_progress(proc.stdout, converted, proc.returncode),
    }
    result["failure_class"] = classify_failure(first_failure, result.get("record_progress"))
    if args.record_replay:
        result["record_replay"] = run_record_replay(script_dir, test_results, converted_path, args, index)
    return result


def extract_first_failure(output: str) -> str | None:
    match = FIRST_FAILURE_RE.search(output)
    if not match:
        return None
    lines = [line.rstrip() for line in match.group("message").splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines[:8]) if lines else None


def record_progress(output: str, converted: int | None, exit_code: int) -> dict[str, object] | None:
    converted_path = extract_converted_path(output)
    if converted_path is None:
        return None

    records = read_slt_records(converted_path)
    total = len(records) or converted
    if total is None:
        return None

    if exit_code == 0:
        return {
            "metric": "prefix_before_first_failure",
            "converted_file": str(converted_path),
            "total_records": total,
            "prefix_passed_records": total,
            "prefix_pass_percent": 100.0,
            "failed_record_index": None,
            "failed_line": None,
            "failed_record": None,
        }

    failed_line = extract_failure_line(output, converted_path)
    if failed_line is None or not records:
        return {
            "metric": "prefix_before_first_failure",
            "converted_file": str(converted_path),
            "total_records": total,
            "prefix_passed_records": None,
            "prefix_pass_percent": None,
            "failed_record_index": None,
            "failed_line": failed_line,
            "failed_record": None,
        }

    failed_index = 0
    failed_record = None
    for index, record in enumerate(records, start=1):
        if record["line"] <= failed_line:
            failed_index = index
            failed_record = record
        else:
            break

    if failed_index == 0:
        prefix_passed = None
        percent = None
    else:
        prefix_passed = failed_index - 1
        percent = round((prefix_passed / total) * 100, 1) if total else None

    return {
        "metric": "prefix_before_first_failure",
        "converted_file": str(converted_path),
        "total_records": total,
        "prefix_passed_records": prefix_passed,
        "prefix_pass_percent": percent,
        "failed_record_index": failed_index or None,
        "failed_line": failed_line,
        "failed_record": failed_record,
    }


def extract_converted_path(output: str) -> Path | None:
    matches = CONVERTED_FILE_RE.findall(output)
    if not matches:
        return None
    path = Path(matches[-1].strip())
    return path if path.exists() else None


def extract_failure_line(output: str, converted_path: Path) -> int | None:
    converted_resolved = converted_path.resolve()
    for match in FAILURE_LOCATION_RE.finditer(output):
        path = Path(match.group("path"))
        try:
            same_file = path.exists() and path.resolve() == converted_resolved
        except OSError:
            same_file = False
        if same_file:
            return int(match.group("line"))
    return None


def read_slt_records(path: Path) -> list[dict[str, object]]:
    lines = path.read_text().splitlines()
    records: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        if not RECORD_HEADER_RE.match(line):
            continue
        records.append(
            {
                "index": len(records) + 1,
                "line": index + 1,
                "kind": line.split()[0],
                "header": line,
                "sql": extract_record_sql(lines, index + 1),
            }
        )
    return records


def extract_record_sql(lines: list[str], start: int) -> str:
    sql_lines: list[str] = []
    for line in lines[start:]:
        if line == "----" or line.strip() == "":
            break
        sql_lines.append(line.rstrip())
    return "\n".join(sql_lines)


def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def classify_failure(first_failure: str | None, progress: object) -> dict[str, object] | None:
    if first_failure is None:
        return None

    lowered = first_failure.lower()
    failed_record = progress.get("failed_record") if isinstance(progress, dict) else None
    failed_sql = failed_record.get("sql") if isinstance(failed_record, dict) else None

    if "query result mismatch" in lowered:
        category = "result_mismatch"
    elif "is expected to fail with error" in lowered and "but got error" in lowered:
        category = "postgres_expected_error_mismatch"
    elif "feature not supported" in lowered:
        category = "unsupported_feature"
    elif "connection refused" in lowered or "server did not become ready" in lowered:
        category = "harness_or_server_failure"
    elif "statement failed:" in lowered or "query failed:" in lowered:
        category = "unexpected_pgrust_error"
    elif "expected to fail" in lowered:
        category = "unexpected_success_or_error_shape"
    else:
        category = "unknown"

    return {
        "category": category,
        "sql": failed_sql,
        "likely_converter_artifact": likely_converter_artifact(failed_sql),
    }


def likely_converter_artifact(sql: str | None) -> bool:
    if not sql:
        return False
    upper = sql.upper()
    return any(
        marker in upper
        for marker in (
            "UNIQUE INDEX",
            " FAMILY ",
            "FAMILY (",
            " // ",
            "::STRING",
        )
    )


def run_record_replay(
    script_dir: Path,
    test_results: Path,
    converted_path: Path | None,
    args: argparse.Namespace,
    index: int,
) -> dict[str, object] | None:
    if converted_path is None:
        return None

    replay_results = test_results / "record-replay"
    replay_log = replay_results / "record-replay.log"
    replay_results.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(script_dir / "run_slt_record_replay.py"),
        "--file",
        str(converted_path),
        "--results-dir",
        str(replay_results),
        "--port-base",
        str(args.record_replay_port_base + (index * 3000)),
    ]
    if args.sqllogictest_dir:
        cmd.extend(["--sqllogictest-dir", str(args.sqllogictest_dir)])
    if args.skip_build:
        cmd.append("--skip-build")
    if args.record_replay_limit is not None:
        cmd.extend(["--limit", str(args.record_replay_limit)])
    if args.record_replay_jobs is not None:
        cmd.extend(["--jobs", str(args.record_replay_jobs)])
    if args.record_replay_expected_success_only:
        cmd.append("--expected-success-only")

    proc = subprocess.run(cmd, cwd=repo_root(), text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    replay_log.write_text(proc.stdout)
    summary_path = replay_results / "summary.json"
    if not summary_path.exists():
        return {
            "status": "error",
            "exit_code": proc.returncode,
            "log_path": str(replay_log),
            "summary_path": None,
        }

    summary = json.loads(summary_path.read_text())
    return {
        "status": "ok" if proc.returncode == 0 else "fail",
        "exit_code": proc.returncode,
        "log_path": str(replay_log),
        "summary_path": str(summary_path),
        "jobs": summary.get("jobs"),
        "elapsed_seconds": summary.get("elapsed_seconds"),
        "target_filter": summary.get("target_filter"),
        "source_total_records": summary.get("source_total_records"),
        "totals": summary["totals"],
    }


def main() -> int:
    started = time.monotonic()
    script_dir = Path(__file__).resolve().parent
    default_manifest = script_dir / "cockroach-logictest-presets" / "smoke.list"
    default_results = Path(tempfile.mkdtemp(prefix="pgrust_cockroach_logic_suite."))

    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=default_manifest)
    parser.add_argument("--results-dir", type=Path, default=default_results)
    parser.add_argument("--cockroach-dir", type=Path)
    parser.add_argument("--sqllogictest-dir", type=Path)
    parser.add_argument("--postgres-oracle", action="store_true", default=True)
    parser.add_argument("--no-postgres-oracle", action="store_false", dest="postgres_oracle")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--base-port", type=int, default=5450)
    parser.add_argument("--postgres-base-port", type=int, default=5550)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--record-replay", action="store_true")
    parser.add_argument("--record-replay-limit", type=int)
    parser.add_argument("--record-replay-jobs", type=int)
    parser.add_argument("--record-replay-expected-success-only", action="store_true")
    parser.add_argument("--record-replay-port-base", type=int, default=6450)
    args = parser.parse_args()

    tests = read_manifest(args.manifest)
    if args.limit is not None:
        tests = tests[: args.limit]

    args.results_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "manifest": str(args.manifest),
        "results_dir": str(args.results_dir),
        "postgres_oracle": args.postgres_oracle,
        "tests": [],
    }

    for index, test_name in enumerate(tests):
        print(f"[{index + 1}/{len(tests)}] {test_name}", flush=True)
        result = run_one(script_dir, test_name, args, index)
        summary["tests"].append(result)
        print(f"  {result['status']} converted={result['converted']} skipped={result['skipped']}", flush=True)
        progress = result.get("record_progress")
        if progress and progress.get("prefix_passed_records") is not None:
            print(
                "  record prefix: "
                f"{progress['prefix_passed_records']}/{progress['total_records']} "
                f"({progress['prefix_pass_percent']}%) before first failure",
                flush=True,
            )
        if result["first_failure"]:
            print("  first failure: " + str(result["first_failure"]).splitlines()[0], flush=True)
        failure_class = result.get("failure_class")
        if failure_class:
            artifact = " converter-artifact?" if failure_class.get("likely_converter_artifact") else ""
            print(f"  class: {failure_class['category']}{artifact}", flush=True)
        replay = result.get("record_replay")
        if replay and replay.get("totals"):
            totals = replay["totals"]
            print(
                "  independent replay: "
                f"{totals['pass']}/{totals['total']} pass total "
                f"({totals['pass_percent_of_total']}%), "
                f"{totals['setup_failed']} setup_failed, "
                f"{replay.get('elapsed_seconds')}s",
                flush=True,
            )

    passed = sum(1 for item in summary["tests"] if item["status"] == "pass")
    failed = len(summary["tests"]) - passed
    summary["totals"] = {
        "total": len(summary["tests"]),
        "passed": passed,
        "failed": failed,
        "converted": sum(item["converted"] or 0 for item in summary["tests"]),
        "skipped": sum(item["skipped"] or 0 for item in summary["tests"]),
    }
    add_record_progress_totals(summary)
    add_record_replay_totals(summary)
    add_failure_class_totals(summary)
    summary["elapsed_seconds"] = round(time.monotonic() - started, 3)

    summary_path = args.results_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(f"summary: {summary_path}")
    print(f"totals: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


def add_record_progress_totals(summary: dict[str, object]) -> None:
    tests = summary["tests"]
    measured = [
        item["record_progress"]
        for item in tests
        if item.get("record_progress")
        and item["record_progress"].get("prefix_passed_records") is not None
        and item["record_progress"].get("total_records") is not None
    ]
    prefix_passed = sum(item["prefix_passed_records"] for item in measured)
    total = sum(item["total_records"] for item in measured)
    summary["totals"]["record_prefix_measured_files"] = len(measured)
    summary["totals"]["record_prefix_passed"] = prefix_passed
    summary["totals"]["record_total"] = total
    summary["totals"]["record_prefix_pass_percent"] = round((prefix_passed / total) * 100, 1) if total else None


def add_record_replay_totals(summary: dict[str, object]) -> None:
    replay_items = [item["record_replay"] for item in summary["tests"] if item.get("record_replay")]
    replays = [item["totals"] for item in replay_items if item.get("totals")]
    if not replays:
        return

    total = sum(item["total"] for item in replays)
    passed = sum(item["pass"] for item in replays)
    failed = sum(item["fail"] for item in replays)
    setup_failed = sum(item["setup_failed"] for item in replays)
    unknown = sum(item["unknown"] for item in replays)
    asserted = passed + failed
    summary["totals"]["record_replay_measured_files"] = len(replays)
    summary["totals"]["record_replay_total"] = total
    summary["totals"]["record_replay_pass"] = passed
    summary["totals"]["record_replay_fail"] = failed
    summary["totals"]["record_replay_setup_failed"] = setup_failed
    summary["totals"]["record_replay_unknown"] = unknown
    summary["totals"]["record_replay_asserted"] = asserted
    summary["totals"]["record_replay_pass_percent_of_asserted"] = (
        round((passed / asserted) * 100, 1) if asserted else None
    )
    summary["totals"]["record_replay_pass_percent_of_total"] = round((passed / total) * 100, 1) if total else None
    summary["totals"]["record_replay_elapsed_seconds"] = round(
        sum(item.get("elapsed_seconds") or 0 for item in replay_items),
        3,
    )


def add_failure_class_totals(summary: dict[str, object]) -> None:
    classes: dict[str, int] = {}
    likely_converter_artifacts = 0
    for item in summary["tests"]:
        failure_class = item.get("failure_class")
        if not failure_class:
            continue
        category = failure_class["category"]
        classes[category] = classes.get(category, 0) + 1
        if failure_class.get("likely_converter_artifact"):
            likely_converter_artifacts += 1
    summary["totals"]["failure_classes"] = classes
    summary["totals"]["likely_converter_artifacts"] = likely_converter_artifacts


if __name__ == "__main__":
    raise SystemExit(main())
