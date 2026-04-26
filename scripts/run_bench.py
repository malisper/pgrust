#!/usr/bin/env python3
"""Run local pgrust/PostgreSQL benchmarks and emit JSON results."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
BENCH_SQL_DIR = REPO_ROOT / "bench" / "sql"
PGBENCH_WORKLOADS = {
    "scan-count": "pgbench_scan_count.sql",
    "point-select": "pgbench_point_select.sql",
    "range-select": "pgbench_range_select.sql",
    "activity-count": "pgbench_activity_count.sql",
    "feed-page": "pgbench_feed_page.sql",
    "top-touched": "pgbench_top_touched.sql",
    "event-join": "pgbench_event_join.sql",
    "insert-only": "pgbench_insert_only.sql",
    "read-write": "pgbench_read_write.sql",
    "mixed-oltp": "pgbench_mixed_oltp.sql",
}
TOUCHED_INDEX_WORKLOADS = {"activity-count", "top-touched"}
EVENT_INDEX_WORKLOADS = {"event-join"}


def default_cargo_target_dir() -> Path:
    repo_slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", REPO_ROOT.name).strip("-")
    pool_size_raw = os.environ.get("PGRUST_TARGET_POOL_SIZE", "8")
    try:
        pool_size = int(pool_size_raw)
    except ValueError as exc:
        raise SystemExit(
            f"PGRUST_TARGET_POOL_SIZE must be a positive integer, got: {pool_size_raw}"
        ) from exc
    if pool_size < 1:
        raise SystemExit(
            f"PGRUST_TARGET_POOL_SIZE must be a positive integer, got: {pool_size_raw}"
        )

    slot_raw = os.environ.get("PGRUST_TARGET_SLOT")
    if slot_raw is not None:
        try:
            slot = int(slot_raw)
        except ValueError as exc:
            raise SystemExit(
                f"PGRUST_TARGET_SLOT must be a non-negative integer, got: {slot_raw}"
            ) from exc
        if slot < 0 or slot >= pool_size:
            raise SystemExit(
                f"PGRUST_TARGET_SLOT must be between 0 and {pool_size - 1}, got: {slot_raw}"
            )
    else:
        slot_key = os.environ.get("PGRUST_TARGET_POOL_KEY", str(REPO_ROOT))
        slot_hash = hashlib.sha1(slot_key.encode("utf-8")).hexdigest()[:8]
        slot = int(slot_hash, 16) % pool_size

    pool_root = os.environ.get(
        "PGRUST_TARGET_POOL_DIR",
        str(Path("/tmp") / "pgrust-target-pool" / repo_slug),
    )
    return Path(pool_root) / str(slot)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local benchmark suites against pgrust and PostgreSQL."
    )
    parser.add_argument(
        "--suite",
        action="append",
        choices=["all", "select-wire", "pgbench", "pgbench-scan", "pgbench-like"],
        help="Benchmark suite to run. Repeatable. Default: all.",
    )
    parser.add_argument(
        "--pgbench-workload",
        action="append",
        choices=["all", *PGBENCH_WORKLOADS.keys()],
        help="pgbench workload to run. Repeatable. Default: all.",
    )
    parser.add_argument(
        "--engines",
        choices=["both", "pgrust", "postgres"],
        default="both",
        help="Wire-protocol engines to benchmark. Default: both.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        help="Directory to write benchmark artifacts into. Default: unique temp dir.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional explicit JSON path. Default: <results-dir>/summary.json.",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        help="Print a compact report for an existing summary JSON and exit.",
    )
    parser.add_argument(
        "--history-dir",
        type=Path,
        help="Optional local history directory. Records this run under <history-dir>/runs and updates index.json.",
    )
    parser.add_argument(
        "--history-label",
        help="Optional label stored with --history-dir entries, for example laptop-baseline.",
    )
    parser.add_argument(
        "--report-history",
        type=Path,
        help="Print a compact report for a local benchmark history directory and exit.",
    )
    parser.add_argument(
        "--history-limit",
        type=int,
        default=10,
        help="Number of recent history runs to show with --report-history. Default: 10.",
    )
    parser.add_argument(
        "--history-dashboard",
        type=Path,
        help="Write a standalone HTML dashboard when used with --report-history.",
    )
    parser.add_argument(
        "--check-history-regressions",
        type=Path,
        help="Check latest local history run for pgrust/PostgreSQL ratio regressions and exit.",
    )
    parser.add_argument(
        "--regression-threshold-percent",
        type=float,
        default=5.0,
        help="Percent change needed to flag a history regression. Default: 5.0.",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Do not print the compact report after a benchmark run.",
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        help="Cargo target dir for benchmark builds. Default: bounded target-dir pool under /tmp.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Do not rebuild pgrust_server / pgbench_like.",
    )
    parser.add_argument(
        "--skip-pgrust-server",
        action="store_true",
        help="Assume pgrust_server is already running on --pgrust-port.",
    )
    parser.add_argument(
        "--external-postgres",
        action="store_true",
        help="Use an already-running PostgreSQL server instead of a temporary local cluster.",
    )
    parser.add_argument(
        "--keep-pgrust-data",
        action="store_true",
        help="Preserve the temporary pgrust data dir.",
    )
    parser.add_argument(
        "--keep-postgres-data",
        action="store_true",
        help="Preserve the temporary PostgreSQL data dir.",
    )
    parser.add_argument(
        "--pgrust-port", type=int, default=5544, help="pgrust server port. Default: 5544."
    )
    parser.add_argument(
        "--postgres-port",
        type=int,
        default=int(os.environ.get("PGPORT", "5545")),
        help="PostgreSQL port. Default: $PGPORT or 5545.",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("PGHOST", "127.0.0.1"),
        help="Database host for both engines. Default: $PGHOST or 127.0.0.1.",
    )
    parser.add_argument(
        "--user",
        default=os.environ.get("PGUSER", "postgres"),
        help="Database user for both engines. Default: $PGUSER or postgres.",
    )
    parser.add_argument(
        "--password",
        default=os.environ.get("PGPASSWORD", "postgres"),
        help="Database password for both engines. Default: $PGPASSWORD or postgres.",
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("PGDATABASE", "postgres"),
        help="Database name for both engines. Default: $PGDATABASE or postgres.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=10000,
        help="Row count for scanbench-backed suites. Default: 10000.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=20,
        help="Iteration count for select-wire. Default: 20.",
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=1,
        help="Client count for select-wire / pgbench suites. Default: 1.",
    )
    parser.add_argument(
        "--pgbench-transactions",
        type=int,
        default=50,
        help="Transactions per client for pgbench workloads. Default: 50.",
    )
    parser.add_argument(
        "--pgbench-warmup-transactions",
        type=int,
        default=3,
        help="Warmup transactions per client before measured pgbench runs. Default: 3.",
    )
    parser.add_argument(
        "--reuse-pgbench-data",
        action="store_true",
        help="Reuse the same scanbench data across pgbench workloads instead of resetting per workload.",
    )
    parser.add_argument(
        "--pgbench-like-time",
        type=int,
        default=15,
        help="Duration in seconds for pgbench_like. Default: 15.",
    )
    parser.add_argument(
        "--pgbench-like-scale",
        type=int,
        default=1,
        help="Scale factor for pgbench_like. Default: 1.",
    )
    parser.add_argument(
        "--pgbench-like-clients",
        type=int,
        default=10,
        help="Client threads for pgbench_like. Default: 10.",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=16384,
        help="pgrust buffer pool pages. Default: 16384.",
    )
    return parser.parse_args()


@dataclass
class CommandResult:
    argv: list[str]
    returncode: int
    stdout: str
    stderr: str
    elapsed_ms: float


class BenchmarkRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.results_dir = (
            args.results_dir
            if args.results_dir
            else Path(tempfile.mkdtemp(prefix="pgrust_bench_results."))
        )
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.output_json = args.output_json or self.results_dir / "summary.json"
        self.artifacts_dir = self.results_dir / "artifacts"
        self.artifacts_dir.mkdir(exist_ok=True)
        self.pgrust_data_dir = self.results_dir / "pgrust-data"
        self.postgres_data_dir = self.results_dir / "postgres-data"
        self.cargo_target_dir = args.target_dir or default_cargo_target_dir()
        self.repo_target_dir = self.cargo_target_dir / "release"
        self.server_process: subprocess.Popen[str] | None = None
        self.postgres_started_by_runner = False
        self.server_started_by_runner = False

    def run(self) -> int:
        suites = self.resolve_suites()
        if self.requires_binary("pgbench") and shutil.which("pgbench") is None:
            raise SystemExit("pgbench not found on PATH")
        if shutil.which("psql") is None:
            raise SystemExit("psql not found on PATH")
        if self.requires_managed_postgres(suites) and shutil.which("initdb") is None:
            raise SystemExit("initdb not found on PATH")
        if self.requires_managed_postgres(suites) and shutil.which("pg_ctl") is None:
            raise SystemExit("pg_ctl not found on PATH")
        if self.requires_wire_suites(suites) and not self.args.skip_build:
            self.build_pgrust_binaries()
        elif "pgbench-like" in suites and not self.args.skip_build:
            self.build_pgbench_like()

        try:
            if self.requires_postgres_server(suites):
                self.start_postgres_server()
            if self.requires_pgrust_server(suites):
                self.start_pgrust_server()

            summary = {
                "created_at": utc_now(),
                "repo_root": str(REPO_ROOT),
                "git_branch": self.git_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
                "git_commit": self.git_output(["git", "rev-parse", "HEAD"]),
                "hostname": socket.gethostname(),
                "config": self.build_config(suites),
                "environment": self.build_environment(),
                "fairness": self.build_fairness_notes(suites),
                "results": [],
            }

            if "select-wire" in suites:
                summary["results"].extend(self.run_select_wire_suite())
            if "pgbench" in suites:
                summary["results"].extend(self.run_pgbench_suite())
            if "pgbench-like" in suites:
                summary["results"].append(self.run_pgbench_like_suite())
            summary["comparisons"] = build_comparisons(summary["results"])

            history_path = None
            if self.args.history_dir:
                history_path = record_history(
                    summary, self.args.history_dir, self.args.history_label
                )

            self.output_json.write_text(json.dumps(summary, indent=2) + "\n")
            print(f"Results dir: {self.results_dir}")
            print(f"Summary JSON: {self.output_json}")
            if history_path is not None:
                print(f"History JSON: {history_path}")
            if not self.args.no_report:
                print()
                print_report(summary)
            return 0
        finally:
            self.stop_pgrust_server()
            self.stop_postgres_server()
            if not self.args.keep_pgrust_data and self.server_started_by_runner:
                shutil.rmtree(self.pgrust_data_dir, ignore_errors=True)
            if not self.args.keep_postgres_data and self.postgres_started_by_runner:
                shutil.rmtree(self.postgres_data_dir, ignore_errors=True)

    def resolve_suites(self) -> list[str]:
        selected = self.args.suite or ["all"]
        if "all" in selected:
            return ["select-wire", "pgbench", "pgbench-like"]
        normalized = ["pgbench" if suite == "pgbench-scan" else suite for suite in selected]
        return list(dict.fromkeys(normalized))

    def resolve_pgbench_workloads(self) -> list[str]:
        selected = self.args.pgbench_workload or ["all"]
        if "all" in selected:
            return list(PGBENCH_WORKLOADS.keys())
        return list(dict.fromkeys(selected))

    def build_config(self, suites: list[str]) -> dict[str, Any]:
        return {
            "suites": suites,
            "engines": self.args.engines,
            "pgbench_workloads": self.resolve_pgbench_workloads(),
            "host": self.args.host,
            "user": self.args.user,
            "database": self.args.database,
            "target_dir": str(self.cargo_target_dir),
            "pgrust_port": self.args.pgrust_port,
            "postgres_port": self.args.postgres_port,
            "postgres_mode": "external" if self.args.external_postgres else "temporary",
            "rows": self.args.rows,
            "iterations": self.args.iterations,
            "clients": self.args.clients,
            "pgbench_transactions": self.args.pgbench_transactions,
            "pgbench_warmup_transactions": self.args.pgbench_warmup_transactions,
            "reuse_pgbench_data": self.args.reuse_pgbench_data,
            "pgbench_like_time": self.args.pgbench_like_time,
            "pgbench_like_scale": self.args.pgbench_like_scale,
            "pgbench_like_clients": self.args.pgbench_like_clients,
            "pool_size": self.args.pool_size,
        }

    def build_environment(self) -> dict[str, Any]:
        return {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": platform.python_version(),
            "tools": {
                "cargo": command_version(["cargo", "-V"]),
                "rustc": command_version(["rustc", "-V"]),
                "pgbench": command_version(["pgbench", "--version"]),
                "psql": command_version(["psql", "--version"]),
                "initdb": command_version(["initdb", "--version"]),
                "pg_ctl": command_version(["pg_ctl", "--version"]),
            },
            "binaries": self.binary_metadata(),
        }

    def binary_metadata(self) -> dict[str, Any]:
        metadata = {
            "cargo_target_dir": str(self.cargo_target_dir),
            "pgrust_server": file_metadata(self.repo_target_dir / "pgrust_server"),
            "pgbench_like": file_metadata(self.repo_target_dir / "pgbench_like"),
        }
        return metadata

    def build_fairness_notes(self, suites: list[str]) -> dict[str, Any]:
        return {
            "comparison_scope": "same host, same pgbench client, same SQL scripts, same row counts, same concurrency knobs",
            "postgres_mode": "external" if self.args.external_postgres else "temporary local cluster",
            "pgrust_server_mode": "external" if self.args.skip_pgrust_server else "temporary local server",
            "pgrust_build_profile": "release" if not self.args.skip_build else "prebuilt/unchanged",
            "pgbench_protocol": "simple",
            "pgbench_warmup_transactions": self.args.pgbench_warmup_transactions
            if "pgbench" in suites
            else None,
            "pgbench_data_reset": "reused across workloads"
            if self.args.reuse_pgbench_data
            else "reset before each workload",
            "caveats": [
                "PostgreSQL and pgrust are different implementations; this compares external workload behavior, not identical internals.",
                "Small local runs are useful smoke tests but not stable performance baselines.",
            ],
        }

    def requires_binary(self, name: str) -> bool:
        return any(
            name == "pgbench" and suite in {"pgbench"} for suite in self.resolve_suites()
        )

    def requires_wire_suites(self, suites: list[str]) -> bool:
        return any(suite in {"select-wire", "pgbench"} for suite in suites)

    def requires_pgrust_server(self, suites: list[str]) -> bool:
        if self.args.engines == "postgres":
            return False
        return any(suite in {"select-wire", "pgbench"} for suite in suites)

    def requires_postgres_server(self, suites: list[str]) -> bool:
        if self.args.engines == "pgrust":
            return False
        return any(suite in {"select-wire", "pgbench"} for suite in suites)

    def requires_managed_postgres(self, suites: list[str]) -> bool:
        return self.requires_postgres_server(suites) and not self.args.external_postgres

    def engine_list(self) -> list[str]:
        match self.args.engines:
            case "both":
                return ["pgrust", "postgres"]
            case "pgrust":
                return ["pgrust"]
            case "postgres":
                return ["postgres"]
        raise AssertionError("unreachable")

    def git_output(self, argv: list[str]) -> str:
        return (
            subprocess.run(
                argv,
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=True,
            )
            .stdout.strip()
        )

    def build_pgrust_binaries(self) -> None:
        self.run_command(
            ["cargo", "build", "--release", "--bin", "pgrust_server", "--bin", "pgbench_like"],
            artifact_stem="build_pgrust_bench",
            env=self.cargo_env(),
            check=True,
        )

    def build_pgbench_like(self) -> None:
        self.run_command(
            ["cargo", "build", "--release", "--bin", "pgbench_like"],
            artifact_stem="build_pgbench_like",
            env=self.cargo_env(),
            check=True,
        )

    def start_pgrust_server(self) -> None:
        if self.args.skip_pgrust_server:
            self.wait_for_ready("pgrust", self.args.pgrust_port, timeout_secs=10)
            return

        if self.server_process is not None:
            return

        if not self.args.keep_pgrust_data:
            shutil.rmtree(self.pgrust_data_dir, ignore_errors=True)
        self.pgrust_data_dir.mkdir(parents=True, exist_ok=True)
        server_bin = self.repo_target_dir / "pgrust_server"
        stdout_path = self.artifacts_dir / "pgrust_server.stdout.txt"
        stderr_path = self.artifacts_dir / "pgrust_server.stderr.txt"
        stdout_handle = stdout_path.open("w")
        stderr_handle = stderr_path.open("w")
        self.server_process = subprocess.Popen(
            [
                str(server_bin),
                "--dir",
                str(self.pgrust_data_dir),
                "--port",
                str(self.args.pgrust_port),
                "--pool-size",
                str(self.args.pool_size),
            ],
            cwd=REPO_ROOT,
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
            start_new_session=True,
        )
        self.server_started_by_runner = True
        self.wait_for_ready("pgrust", self.args.pgrust_port, timeout_secs=15)

    def start_postgres_server(self) -> None:
        if self.args.external_postgres:
            self.wait_for_ready("postgres", self.args.postgres_port, timeout_secs=10)
            return

        if not self.args.keep_postgres_data:
            shutil.rmtree(self.postgres_data_dir, ignore_errors=True)
        self.postgres_data_dir.mkdir(parents=True, exist_ok=True)
        initdb = self.run_command(
            [
                "initdb",
                "-A",
                "trust",
                "-U",
                self.args.user,
                "-D",
                str(self.postgres_data_dir),
            ],
            artifact_stem="postgres_initdb",
        )
        if initdb.returncode != 0:
            raise RuntimeError(f"initdb failed:\n{initdb.stderr}")

        log_path = self.artifacts_dir / "postgres.log"
        start = self.run_command(
            [
                "pg_ctl",
                "-D",
                str(self.postgres_data_dir),
                "-o",
                f"-p {self.args.postgres_port} -h {self.args.host}",
                "-l",
                str(log_path),
                "start",
            ],
            artifact_stem="postgres_pg_ctl_start",
        )
        if start.returncode != 0:
            raise RuntimeError(f"pg_ctl start failed:\n{start.stderr}")

        self.postgres_started_by_runner = True
        self.wait_for_ready("postgres", self.args.postgres_port, timeout_secs=15)

    def wait_for_ready(self, engine: str, port: int, timeout_secs: int) -> None:
        deadline = time.time() + timeout_secs
        while time.time() < deadline:
            result = subprocess.run(
                self.psql_argv(port) + ["-c", "SELECT 1"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return
            if self.server_process and self.server_process.poll() is not None:
                raise RuntimeError(f"{engine} server exited during startup")
            time.sleep(0.1)
        raise RuntimeError(f"{engine} server did not become ready on port {port}")

    def stop_pgrust_server(self) -> None:
        if self.server_process is None:
            return
        if self.server_process.poll() is None:
            os.killpg(self.server_process.pid, signal.SIGTERM)
            try:
                self.server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(self.server_process.pid, signal.SIGKILL)
                self.server_process.wait(timeout=5)
        self.server_process = None

    def stop_postgres_server(self) -> None:
        if not self.postgres_started_by_runner:
            return
        self.run_command(
            ["pg_ctl", "-D", str(self.postgres_data_dir), "-m", "fast", "stop"],
            artifact_stem="postgres_pg_ctl_stop",
        )

    def psql_argv(self, port: int) -> list[str]:
        return [
            "psql",
            "-w",
            "-h",
            self.args.host,
            "-p",
            str(port),
            "-U",
            self.args.user,
            "-d",
            self.args.database,
        ]

    def base_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["PGPASSWORD"] = self.args.password
        return env

    def cargo_env(self) -> dict[str, str]:
        env = self.base_env()
        env["CARGO_TARGET_DIR"] = str(self.cargo_target_dir)
        return env

    def run_command(
        self,
        argv: list[str],
        *,
        artifact_stem: str,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        check: bool = False,
    ) -> CommandResult:
        started = time.time()
        result = subprocess.run(
            argv,
            cwd=cwd or REPO_ROOT,
            env=env or self.base_env(),
            capture_output=True,
            text=True,
        )
        elapsed_ms = (time.time() - started) * 1000.0
        stdout_path = self.artifacts_dir / f"{artifact_stem}.stdout.txt"
        stderr_path = self.artifacts_dir / f"{artifact_stem}.stderr.txt"
        stdout_path.write_text(result.stdout)
        stderr_path.write_text(result.stderr)
        if check and result.returncode != 0:
            raise RuntimeError(
                f"command failed ({result.returncode}): {' '.join(argv)}\n{result.stderr}"
            )
        return CommandResult(
            argv=argv,
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            elapsed_ms=elapsed_ms,
        )

    def run_select_wire_suite(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for engine in self.engine_list():
            port = self.args.pgrust_port if engine == "pgrust" else self.args.postgres_port
            artifact = f"select_wire_{engine}"
            cmd = [
                "bash",
                "bench/bench_select_wire.sh",
                "--host",
                self.args.host,
                "--port",
                str(port),
                "--user",
                self.args.user,
                "--password",
                self.args.password,
                "--rows",
                str(self.args.rows),
                "--iterations",
                str(self.args.iterations),
                "--clients",
                str(self.args.clients),
                "--count",
            ]
            run = self.run_command(cmd, artifact_stem=artifact)
            parsed = parse_key_value_metrics(run.stdout)
            results.append(
                {
                    "suite": "select-wire",
                    "engine": engine,
                    "status": "ok" if run.returncode == 0 else "error",
                    "command": run.argv,
                    "elapsed_ms": round(run.elapsed_ms, 3),
                    "metrics": parsed,
                    "artifact_stdout": f"artifacts/{artifact}.stdout.txt",
                    "artifact_stderr": f"artifacts/{artifact}.stderr.txt",
                }
            )
        return results

    def run_pgbench_suite(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []

        for workload in self.resolve_pgbench_workloads():
            script = BENCH_SQL_DIR / PGBENCH_WORKLOADS[workload]
            for engine in self.engine_list():
                port = self.args.pgrust_port if engine == "pgrust" else self.args.postgres_port
                self.ensure_scanbench_loaded(port, engine, workload)
                artifact = f"pgbench_{workload.replace('-', '_')}_{engine}"
                warmup = None
                if self.args.pgbench_warmup_transactions > 0:
                    warmup = self.run_pgbench_command(
                        script=script,
                        port=port,
                        transactions=self.args.pgbench_warmup_transactions,
                        artifact_stem=f"{artifact}_warmup",
                        check=False,
                    )
                run = self.run_pgbench_command(
                    script=script,
                    port=port,
                    transactions=self.args.pgbench_transactions,
                    artifact_stem=artifact,
                    check=False,
                )
                metrics = parse_pgbench_output(run.stdout)
                item = {
                    "suite": "pgbench",
                    "workload": workload,
                    "engine": engine,
                    "status": "ok" if run.returncode == 0 else "error",
                    "command": run.argv,
                    "elapsed_ms": round(run.elapsed_ms, 3),
                    "metrics": metrics,
                    "artifact_stdout": f"artifacts/{artifact}.stdout.txt",
                    "artifact_stderr": f"artifacts/{artifact}.stderr.txt",
                }
                if warmup is not None:
                    item.update(
                        {
                            "warmup_status": "ok" if warmup.returncode == 0 else "error",
                            "warmup_command": warmup.argv,
                            "warmup_metrics": parse_pgbench_output(warmup.stdout),
                            "warmup_artifact_stdout": f"artifacts/{artifact}_warmup.stdout.txt",
                            "warmup_artifact_stderr": f"artifacts/{artifact}_warmup.stderr.txt",
                        }
                    )
                results.append(item)
        return results

    def run_pgbench_command(
        self,
        *,
        script: Path,
        port: int,
        transactions: int,
        artifact_stem: str,
        check: bool,
    ) -> CommandResult:
        cmd = [
            "pgbench",
            "-n",
            "-M",
            "simple",
            "-c",
            str(self.args.clients),
            "-j",
            str(self.args.clients),
            "-t",
            str(transactions),
            "-r",
            "-D",
            f"rows={self.args.rows}",
            "-f",
            str(script),
            "-h",
            self.args.host,
            "-p",
            str(port),
            "-U",
            self.args.user,
            self.args.database,
        ]
        return self.run_command(cmd, artifact_stem=artifact_stem, check=check)

    def ensure_scanbench_loaded(self, port: int, engine: str, workload: str) -> None:
        count_cmd = self.psql_argv(port) + [
            "-t",
            "-A",
            "-c",
            "SELECT count(*), count(touched) FROM scanbench;",
        ]
        result = subprocess.run(
            count_cmd,
            env=self.base_env(),
            capture_output=True,
            text=True,
        )
        existing = result.stdout.strip() if result.returncode == 0 else ""
        if self.args.reuse_pgbench_data and existing == f"{self.args.rows}|{self.args.rows}":
            return

        create_sql = (
            "DROP TABLE IF EXISTS scanbench_events;"
            "DROP TABLE IF EXISTS scanbench;"
            "CREATE TABLE scanbench (id int NOT NULL, payload text NOT NULL, touched int NOT NULL);"
            "CREATE TABLE scanbench_events (item_id int NOT NULL, event_type text NOT NULL);"
        )
        self.run_command(
            self.psql_argv(port) + ["-c", create_sql],
            artifact_stem=f"load_scanbench_create_{workload.replace('-', '_')}_{engine}",
            check=True,
        )

        sql_file = self.results_dir / f"scanbench_load_{workload}_{engine}.sql"
        with sql_file.open("w") as handle:
            handle.write("BEGIN;\n")
            for i in range(self.args.rows):
                touched = i % 10
                handle.write(
                    f"INSERT INTO scanbench (id, payload, touched) VALUES ({i + 1}, 'row-{i + 1}', {touched});\n"
                )
                handle.write(
                    f"INSERT INTO scanbench_events (item_id, event_type) VALUES ({i + 1}, 'seed');\n"
                )
            handle.write("COMMIT;\n")

        self.run_command(
            self.psql_argv(port) + ["-q", "-f", str(sql_file)],
            artifact_stem=f"load_scanbench_rows_{workload.replace('-', '_')}_{engine}",
            check=True,
        )
        self.run_command(
            self.psql_argv(port) + ["-c", "CREATE INDEX scanbench_id_idx ON scanbench (id);"],
            artifact_stem=f"load_scanbench_index_{workload.replace('-', '_')}_{engine}",
            check=True,
        )
        if workload in TOUCHED_INDEX_WORKLOADS:
            self.run_command(
                self.psql_argv(port)
                + ["-c", "CREATE INDEX scanbench_touched_idx ON scanbench (touched);"],
                artifact_stem=f"load_scanbench_touched_index_{workload.replace('-', '_')}_{engine}",
                check=True,
            )
        if workload in EVENT_INDEX_WORKLOADS:
            self.run_command(
                self.psql_argv(port)
                + [
                    "-c",
                    "CREATE INDEX scanbench_events_item_id_idx ON scanbench_events (item_id);",
                ],
                artifact_stem=f"load_scanbench_events_index_{workload.replace('-', '_')}_{engine}",
                check=True,
            )

    def run_pgbench_like_suite(self) -> dict[str, Any]:
        artifact = "pgbench_like"
        base_dir = self.results_dir / "pgbench_like_data"
        cmd = [
            str(self.repo_target_dir / "pgbench_like"),
            "--base-dir",
            str(base_dir),
            "--pool-size",
            str(self.args.pool_size),
            "--init",
            "--scale",
            str(self.args.pgbench_like_scale),
            "--clients",
            str(self.args.pgbench_like_clients),
            "--time",
            str(self.args.pgbench_like_time),
        ]
        run = self.run_command(cmd, artifact_stem=artifact)
        return {
            "suite": "pgbench-like",
            "engine": "pgrust-direct",
            "status": "ok" if run.returncode == 0 else "error",
            "command": run.argv,
            "elapsed_ms": round(run.elapsed_ms, 3),
            "metrics": parse_key_value_metrics(run.stdout),
            "artifact_stdout": f"artifacts/{artifact}.stdout.txt",
            "artifact_stderr": f"artifacts/{artifact}.stderr.txt",
        }


def parse_key_value_metrics(text: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower().replace(" ", "_")
        value = value.strip()
        if not value:
            continue
        parsed: Any = value
        if re.fullmatch(r"-?\d+", value):
            parsed = int(value)
        elif unit_match := re.fullmatch(r"(-?\d+(?:\.\d+)?)\s+(ms|s)", value):
            number = float(unit_match.group(1))
            unit = unit_match.group(2)
            key = f"{key}_{unit}"
            parsed = number
        else:
            try:
                parsed = float(value)
            except ValueError:
                parsed = value
        metrics[key] = parsed
    return metrics


def parse_pgbench_output(text: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    patterns = {
        "number_of_clients": r"number of clients:\s+(\d+)",
        "number_of_threads": r"number of threads:\s+(\d+)",
        "transactions_per_client": r"number of transactions actually processed:\s+(\d+)/(\d+)",
        "failed_transactions": r"number of failed transactions:\s+(\d+)",
        "latency_average_ms": r"latency average =\s+([\d.]+)\s+ms",
        "initial_connection_time_ms": r"initial connection time =\s+([\d.]+)\s+ms",
        "tps_including_connections": r"tps =\s+([\d.]+)\s+\(including connections establishing\)",
        "tps_excluding_connections": r"tps =\s+([\d.]+)\s+\(without initial connection time\)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if not match:
            continue
        if key == "transactions_per_client":
            metrics["transactions_processed"] = int(match.group(1))
            metrics["transactions_expected"] = int(match.group(2))
        elif key in {
            "number_of_clients",
            "number_of_threads",
            "failed_transactions",
        }:
            metrics[key] = int(match.group(1))
        else:
            metrics[key] = float(match.group(1))
    return metrics


def command_version(argv: list[str]) -> str | None:
    try:
        result = subprocess.run(argv, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        return None
    output = (result.stdout or result.stderr).strip()
    return output.splitlines()[0] if output else None


def file_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "size_bytes": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
    }


def build_comparisons(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str | None], dict[str, dict[str, Any]]] = {}
    for result in results:
        engine = result.get("engine")
        if engine not in {"pgrust", "postgres"}:
            continue
        key = (result.get("suite", ""), result.get("workload"))
        groups.setdefault(key, {})[engine] = result

    comparisons = []
    for (suite, workload), by_engine in groups.items():
        pgrust = by_engine.get("pgrust")
        postgres = by_engine.get("postgres")
        if not pgrust or not postgres:
            continue
        if pgrust.get("status") != "ok" or postgres.get("status") != "ok":
            continue

        pgrust_metrics = pgrust.get("metrics", {})
        postgres_metrics = postgres.get("metrics", {})
        comparison: dict[str, Any] = {"suite": suite}
        if workload is not None:
            comparison["workload"] = workload

        throughput_metric = first_metric(
            pgrust_metrics,
            postgres_metrics,
            ["tps_excluding_connections", "queries_per_sec", "rows_per_sec"],
        )
        if throughput_metric is not None:
            pgrust_value = pgrust_metrics[throughput_metric]
            postgres_value = postgres_metrics[throughput_metric]
            comparison.update(
                {
                    "throughput_metric": throughput_metric,
                    "pgrust_throughput": pgrust_value,
                    "postgres_throughput": postgres_value,
                    "pgrust_to_postgres_throughput_ratio": safe_ratio(
                        pgrust_value, postgres_value
                    ),
                }
            )

        latency_metric = first_metric(
            pgrust_metrics,
            postgres_metrics,
            ["latency_average_ms", "avg_ms_per_query", "avg_latency_ms"],
        )
        if latency_metric is not None:
            pgrust_value = pgrust_metrics[latency_metric]
            postgres_value = postgres_metrics[latency_metric]
            comparison.update(
                {
                    "latency_metric": latency_metric,
                    "pgrust_latency_ms": pgrust_value,
                    "postgres_latency_ms": postgres_value,
                    "pgrust_to_postgres_latency_ratio": safe_ratio(
                        pgrust_value, postgres_value
                    ),
                }
            )

        if len(comparison) > (2 if workload is not None else 1):
            comparisons.append(comparison)

    return comparisons


def record_history(
    summary: dict[str, Any], history_dir: Path, label: str | None
) -> Path:
    recorded_at = utc_now()
    history_dir.mkdir(parents=True, exist_ok=True)
    runs_dir = history_dir / "runs"
    runs_dir.mkdir(exist_ok=True)

    run_path = unique_history_path(runs_dir, summary)
    relative_run_path = run_path.relative_to(history_dir).as_posix()
    summary["history"] = {
        "recorded_at": recorded_at,
        "label": label,
        "path": relative_run_path,
    }
    run_path.write_text(json.dumps(summary, indent=2) + "\n")

    index_path = history_dir / "index.json"
    index = load_history_index(index_path)
    entry = build_history_entry(summary, relative_run_path, recorded_at, label)
    runs = [
        run
        for run in index.get("runs", [])
        if run.get("path") != relative_run_path
    ]
    runs.append(entry)
    runs.sort(key=lambda run: run.get("created_at") or "")
    index["runs"] = runs
    index_path.write_text(json.dumps(index, indent=2) + "\n")
    return run_path


def unique_history_path(runs_dir: Path, summary: dict[str, Any]) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    commit = str(summary.get("git_commit") or "unknown")[:12]
    stem = f"{timestamp}-{commit}"
    path = runs_dir / f"{stem}.json"
    suffix = 2
    while path.exists():
        path = runs_dir / f"{stem}-{suffix}.json"
        suffix += 1
    return path


def load_history_index(index_path: Path) -> dict[str, Any]:
    if not index_path.exists():
        return {"schema_version": 1, "runs": []}
    try:
        index = json.loads(index_path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"failed to parse benchmark history index {index_path}: {exc}")
    if not isinstance(index, dict):
        raise SystemExit(f"benchmark history index must be a JSON object: {index_path}")
    index.setdefault("schema_version", 1)
    index.setdefault("runs", [])
    return index


def load_history_runs(history_dir: Path) -> list[dict[str, Any]]:
    index_path = history_dir / "index.json"
    if not index_path.exists():
        raise SystemExit(f"benchmark history index not found: {index_path}")

    index = load_history_index(index_path)
    runs = index.get("runs", [])
    if not isinstance(runs, list):
        raise SystemExit(f"benchmark history index runs must be a list: {index_path}")
    return sorted(runs, key=lambda run: run.get("created_at") or "")


def build_history_entry(
    summary: dict[str, Any], relative_run_path: str, recorded_at: str, label: str | None
) -> dict[str, Any]:
    config = summary.get("config", {})
    environment = summary.get("environment", {})
    results = summary.get("results", [])
    return {
        "created_at": summary.get("created_at"),
        "recorded_at": recorded_at,
        "label": label,
        "path": relative_run_path,
        "git_branch": summary.get("git_branch"),
        "git_commit": summary.get("git_commit"),
        "hostname": summary.get("hostname"),
        "platform": environment.get("platform"),
        "machine": environment.get("machine"),
        "suites": config.get("suites", []),
        "engines": config.get("engines"),
        "pgbench_workloads": config.get("pgbench_workloads", []),
        "rows": config.get("rows"),
        "clients": config.get("clients"),
        "pgbench_transactions": config.get("pgbench_transactions"),
        "result_count": len(results),
        "ok_count": sum(1 for result in results if result.get("status") == "ok"),
        "error_count": sum(1 for result in results if result.get("status") != "ok"),
        "comparisons": summarize_history_comparisons(summary.get("comparisons", [])),
    }


def summarize_history_comparisons(
    comparisons: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    summarized = []
    for item in comparisons:
        row = {
            "suite": item.get("suite"),
            "workload": item.get("workload"),
            "throughput_metric": item.get("throughput_metric"),
            "throughput_ratio": item.get("pgrust_to_postgres_throughput_ratio"),
            "latency_metric": item.get("latency_metric"),
            "latency_ratio": item.get("pgrust_to_postgres_latency_ratio"),
        }
        summarized.append({key: value for key, value in row.items() if value is not None})
    return summarized


def print_history_report(history_dir: Path, limit: int) -> None:
    runs = load_history_runs(history_dir)
    if not runs:
        print("Benchmark History")
        print(f"  dir:  {history_dir}")
        print("  runs: 0")
        return

    recent = runs[-max(limit, 1) :]

    print("Benchmark History")
    print(f"  dir:  {history_dir}")
    print(f"  runs: {len(runs)}")
    print()
    print_history_run_table(recent)

    latest = runs[-1]
    latest_comparisons = latest.get("comparisons") or []
    if latest_comparisons:
        print()
        print_history_comparison_table(latest, runs[:-1])


def print_history_run_table(runs: list[dict[str, Any]]) -> None:
    rows = []
    for run in reversed(runs):
        rows.append(
            [
                short_time(run.get("created_at")),
                short_commit(run.get("git_commit")),
                run.get("label") or "-",
                ",".join(run.get("suites") or []),
                str(run.get("engines") or "-"),
                f"{run.get('ok_count', 0)}/{run.get('result_count', 0)}",
                str(run.get("error_count", 0)),
            ]
        )
    print_table(["created", "commit", "label", "suites", "engines", "ok", "errors"], rows)


def print_history_comparison_table(
    latest: dict[str, Any], previous_runs: list[dict[str, Any]]
) -> None:
    previous_by_key = latest_previous_comparisons(previous_runs)
    rows = []
    for item in latest.get("comparisons", []):
        previous_item = previous_by_key.get(comparison_key(item))
        rows.append(
            [
                comparison_label(item),
                format_ratio(item.get("throughput_ratio")),
                format_ratio(previous_item.get("throughput_ratio"))
                if previous_item
                else "-",
                format_percent_delta(
                    item.get("throughput_ratio"),
                    previous_item.get("throughput_ratio") if previous_item else None,
                ),
                format_ratio(item.get("latency_ratio")),
                format_ratio(previous_item.get("latency_ratio")) if previous_item else "-",
                format_percent_delta(
                    item.get("latency_ratio"),
                    previous_item.get("latency_ratio") if previous_item else None,
                ),
            ]
        )

    print("Latest Comparison Ratios")
    print_table(
        [
            "workload",
            "tps ratio",
            "prev tps",
            "tps delta",
            "lat ratio",
            "prev lat",
            "lat delta",
        ],
        rows,
    )


def comparison_key(item: dict[str, Any]) -> tuple[str | None, str | None]:
    return (item.get("suite"), item.get("workload"))


def comparison_label(item: dict[str, Any]) -> str:
    return item.get("workload") or item.get("suite") or "-"


def short_time(value: Any) -> str:
    if not isinstance(value, str) or len(value) < 16:
        return "-"
    return value[:16].replace("T", " ")


def short_commit(value: Any) -> str:
    if not isinstance(value, str) or not value:
        return "-"
    return value[:12]


def format_percent_delta(current: Any, previous: Any) -> str:
    if not is_number(current) or not is_number(previous) or previous == 0:
        return "-"
    return f"{((current / previous) - 1.0) * 100.0:+.1f}%"


def print_history_regression_check(history_dir: Path, threshold_percent: float) -> int:
    if threshold_percent <= 0:
        raise SystemExit("--regression-threshold-percent must be greater than 0")

    runs = load_history_runs(history_dir)
    if not runs:
        print("Benchmark Regression Check")
        print(f"  dir:       {history_dir}")
        print("  status:    no history")
        return 0

    latest = runs[-1]
    previous_runs = runs[:-1]
    rows, regression_count = build_regression_rows(
        latest, previous_runs, threshold_percent
    )

    print("Benchmark Regression Check")
    print(f"  dir:       {history_dir}")
    print(f"  latest:    {short_time(latest.get('created_at'))}")
    print(f"  commit:    {short_commit(latest.get('git_commit'))}")
    print(f"  threshold: {threshold_percent:.1f}%")
    print(f"  status:    {'fail' if regression_count else 'pass'}")

    if not rows:
        print()
        print("No comparable prior pgrust/PostgreSQL ratio data.")
        return 0

    print()
    print_table(
        ["workload", "metric", "current", "previous", "delta", "status"],
        rows,
    )
    return 1 if regression_count else 0


def build_regression_rows(
    latest: dict[str, Any],
    previous_runs: list[dict[str, Any]],
    threshold_percent: float,
) -> tuple[list[list[str]], int]:
    previous_by_key = latest_previous_comparisons(previous_runs)
    rows = []
    regression_count = 0
    for item in latest.get("comparisons", []):
        previous_item = previous_by_key.get(comparison_key(item))
        if previous_item is None:
            continue

        workload = comparison_label(item)
        throughput_row = regression_row(
            workload,
            "tps ratio",
            item.get("throughput_ratio"),
            previous_item.get("throughput_ratio"),
            threshold_percent,
            worse_when="lower",
        )
        latency_row = regression_row(
            workload,
            "lat ratio",
            item.get("latency_ratio"),
            previous_item.get("latency_ratio"),
            threshold_percent,
            worse_when="higher",
        )
        for row, is_regression in [throughput_row, latency_row]:
            if row is None:
                continue
            rows.append(row)
            regression_count += 1 if is_regression else 0

    return rows, regression_count


def latest_previous_comparisons(
    previous_runs: list[dict[str, Any]]
) -> dict[tuple[str | None, str | None], dict[str, Any]]:
    previous_by_key = {}
    for run in reversed(previous_runs):
        for item in run.get("comparisons", []):
            previous_by_key.setdefault(comparison_key(item), item)
    return previous_by_key


def regression_row(
    workload: str,
    metric: str,
    current: Any,
    previous: Any,
    threshold_percent: float,
    *,
    worse_when: str,
) -> tuple[list[str] | None, bool]:
    if not is_number(current) or not is_number(previous) or previous == 0:
        return None, False

    delta_percent = ((current / previous) - 1.0) * 100.0
    is_regression = (
        delta_percent <= -threshold_percent
        if worse_when == "lower"
        else delta_percent >= threshold_percent
    )
    return (
        [
            workload,
            metric,
            format_ratio(current),
            format_ratio(previous),
            f"{delta_percent:+.1f}%",
            "regression" if is_regression else "ok",
        ],
        is_regression,
    )


def write_history_dashboard(history_dir: Path, output_path: Path) -> None:
    runs = load_history_runs(history_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(build_history_dashboard_html(history_dir, runs))


def build_history_dashboard_html(
    history_dir: Path, runs: list[dict[str, Any]]
) -> str:
    generated_at = utc_now()
    latest = runs[-1] if runs else {}
    series = collect_history_series(runs)
    cards = "\n".join(render_series_card(key, points) for key, points in series.items())
    if not cards:
        cards = '<section class="empty">No pgrust/PostgreSQL comparison ratios yet.</section>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>pgrust Benchmark History</title>
  <style>
    :root {{
      --bg: #eef3ec;
      --ink: #182018;
      --muted: #5f6d5f;
      --card: rgba(255, 255, 248, 0.86);
      --line: #cfd8c8;
      --accent: #186f65;
      --accent-2: #b25a27;
      --shadow: 0 22px 70px rgba(42, 56, 41, 0.14);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "Trebuchet MS", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(24, 111, 101, 0.22), transparent 32rem),
        linear-gradient(135deg, #f7f3e8 0%, var(--bg) 48%, #dce8df 100%);
    }}
    main {{
      width: min(1160px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 48px 0 72px;
    }}
    header {{
      display: grid;
      gap: 18px;
      margin-bottom: 28px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(2.4rem, 6vw, 5.5rem);
      letter-spacing: -0.07em;
      line-height: 0.92;
    }}
    h2 {{
      margin: 0 0 14px;
      font-size: 1.05rem;
      letter-spacing: 0.02em;
    }}
    .meta-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
    }}
    .meta, .card, .empty {{
      background: var(--card);
      border: 1px solid rgba(207, 216, 200, 0.8);
      border-radius: 24px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }}
    .meta {{ padding: 16px 18px; }}
    .label {{
      display: block;
      color: var(--muted);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }}
    .value {{
      display: block;
      margin-top: 8px;
      font-size: 1.1rem;
      font-weight: 700;
      overflow-wrap: anywhere;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(330px, 1fr));
      gap: 18px;
      margin-top: 24px;
    }}
    .card {{ padding: 20px; }}
    .chart-grid {{
      display: grid;
      gap: 18px;
    }}
    .chart-title {{
      color: var(--muted);
      font-size: 0.84rem;
      font-weight: 700;
      margin: 8px 0;
      text-transform: uppercase;
      letter-spacing: 0.09em;
    }}
    svg {{
      display: block;
      width: 100%;
      height: auto;
      overflow: visible;
    }}
    .axis {{ stroke: var(--line); stroke-width: 1; }}
    .baseline {{ stroke: #8ea08d; stroke-width: 1; stroke-dasharray: 4 6; }}
    .throughput {{ fill: none; stroke: var(--accent); stroke-width: 4; }}
    .latency {{ fill: none; stroke: var(--accent-2); stroke-width: 4; }}
    .dot-throughput {{ fill: var(--accent); }}
    .dot-latency {{ fill: var(--accent-2); }}
    .tick {{
      fill: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 26px;
      background: rgba(255, 255, 248, 0.7);
      border-radius: 18px;
      overflow: hidden;
    }}
    th, td {{
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      white-space: nowrap;
    }}
    th {{
      color: var(--muted);
      font-size: 0.76rem;
      text-transform: uppercase;
      letter-spacing: 0.1em;
    }}
    tr:last-child td {{ border-bottom: 0; }}
    .empty {{
      padding: 24px;
      color: var(--muted);
    }}
    @media (max-width: 720px) {{
      main {{ width: min(100vw - 20px, 1160px); padding-top: 28px; }}
      .cards {{ grid-template-columns: 1fr; }}
      table {{ display: block; overflow-x: auto; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Benchmark History</h1>
      <div class="meta-grid">
        <div class="meta"><span class="label">History Dir</span><span class="value">{h(history_dir)}</span></div>
        <div class="meta"><span class="label">Runs</span><span class="value">{len(runs)}</span></div>
        <div class="meta"><span class="label">Latest Commit</span><span class="value">{h(short_commit(latest.get("git_commit")))}</span></div>
        <div class="meta"><span class="label">Generated</span><span class="value">{h(generated_at)}</span></div>
      </div>
    </header>
    <section>
      <h2>pgrust / PostgreSQL Ratios</h2>
      <p>Throughput above 1.0x means pgrust is faster. Latency below 1.0x means pgrust is faster.</p>
      <div class="cards">
        {cards}
      </div>
    </section>
    {render_history_runs_table(runs)}
  </main>
</body>
</html>
"""


def collect_history_series(
    runs: list[dict[str, Any]]
) -> dict[tuple[str | None, str | None], list[dict[str, Any]]]:
    series: dict[tuple[str | None, str | None], list[dict[str, Any]]] = {}
    for run in runs:
        for item in run.get("comparisons", []):
            key = comparison_key(item)
            series.setdefault(key, []).append(
                {
                    "created_at": run.get("created_at"),
                    "label": run.get("label"),
                    "git_commit": run.get("git_commit"),
                    "throughput_ratio": item.get("throughput_ratio"),
                    "latency_ratio": item.get("latency_ratio"),
                }
            )
    return dict(sorted(series.items(), key=lambda item: comparison_label_from_key(item[0])))


def render_series_card(
    key: tuple[str | None, str | None], points: list[dict[str, Any]]
) -> str:
    title = comparison_label_from_key(key)
    latest = points[-1] if points else {}
    return f"""<article class="card">
  <h2>{h(title)}</h2>
  <div class="meta-grid">
    <div><span class="label">Latest TPS Ratio</span><span class="value">{h(format_ratio(latest.get("throughput_ratio")))}</span></div>
    <div><span class="label">Latest Latency Ratio</span><span class="value">{h(format_ratio(latest.get("latency_ratio")))}</span></div>
  </div>
  <div class="chart-grid">
    <div>
      <div class="chart-title">Throughput ratio</div>
      {render_ratio_svg(points, "throughput_ratio", "throughput")}
    </div>
    <div>
      <div class="chart-title">Latency ratio</div>
      {render_ratio_svg(points, "latency_ratio", "latency")}
    </div>
  </div>
</article>"""


def render_ratio_svg(
    points: list[dict[str, Any]], metric: str, css_class: str
) -> str:
    values = [point.get(metric) for point in points if is_number(point.get(metric))]
    if not values:
        return '<div class="empty">No ratio data.</div>'

    width = 520
    height = 170
    pad_x = 38
    pad_y = 24
    min_value = min(values + [1.0])
    max_value = max(values + [1.0])
    if min_value == max_value:
        min_value *= 0.9
        max_value *= 1.1
    value_range = max_value - min_value
    usable_width = width - (pad_x * 2)
    usable_height = height - (pad_y * 2)

    plotted = []
    for index, point in enumerate(points):
        value = point.get(metric)
        if not is_number(value):
            continue
        x = pad_x + (usable_width * index / max(len(points) - 1, 1))
        y = height - pad_y - (((value - min_value) / value_range) * usable_height)
        plotted.append((x, y, value, point))

    line = " ".join(f"{x:.1f},{y:.1f}" for x, y, _, _ in plotted)
    baseline_y = height - pad_y - (((1.0 - min_value) / value_range) * usable_height)
    dots = "\n".join(
        f'<circle class="dot-{css_class}" cx="{x:.1f}" cy="{y:.1f}" r="4">'
        f"<title>{h(point_label(point, value))}</title></circle>"
        for x, y, value, point in plotted
    )
    polyline = (
        f'<polyline class="{css_class}" points="{line}"></polyline>' if len(plotted) > 1 else ""
    )
    return f"""<svg viewBox="0 0 {width} {height}" role="img" aria-label="{h(metric)} trend">
  <line class="axis" x1="{pad_x}" y1="{height - pad_y}" x2="{width - pad_x}" y2="{height - pad_y}"></line>
  <line class="baseline" x1="{pad_x}" y1="{baseline_y:.1f}" x2="{width - pad_x}" y2="{baseline_y:.1f}"></line>
  <text class="tick" x="4" y="{baseline_y + 4:.1f}">1.0x</text>
  <text class="tick" x="4" y="{pad_y + 4}">{h(format_ratio(max_value))}</text>
  <text class="tick" x="4" y="{height - pad_y + 4}">{h(format_ratio(min_value))}</text>
  {polyline}
  {dots}
</svg>"""


def point_label(point: dict[str, Any], value: float) -> str:
    pieces = [
        short_time(point.get("created_at")),
        format_ratio(value),
        short_commit(point.get("git_commit")),
    ]
    if point.get("label"):
        pieces.append(str(point["label"]))
    return " | ".join(piece for piece in pieces if piece != "-")


def comparison_label_from_key(key: tuple[str | None, str | None]) -> str:
    suite, workload = key
    return workload or suite or "-"


def render_history_runs_table(runs: list[dict[str, Any]]) -> str:
    if not runs:
        return ""
    rows = "\n".join(
        f"<tr><td>{h(short_time(run.get('created_at')))}</td>"
        f"<td>{h(short_commit(run.get('git_commit')))}</td>"
        f"<td>{h(run.get('label') or '-')}</td>"
        f"<td>{h(','.join(run.get('suites') or []))}</td>"
        f"<td>{h(run.get('engines') or '-')}</td>"
        f"<td>{run.get('ok_count', 0)}/{run.get('result_count', 0)}</td>"
        f"<td>{run.get('error_count', 0)}</td></tr>"
        for run in reversed(runs[-20:])
    )
    return f"""<section>
  <h2>Recent Runs</h2>
  <table>
    <thead><tr><th>Created</th><th>Commit</th><th>Label</th><th>Suites</th><th>Engines</th><th>OK</th><th>Errors</th></tr></thead>
    <tbody>
      {rows}
    </tbody>
  </table>
</section>"""


def h(value: Any) -> str:
    return html.escape(str(value), quote=True)


def first_metric(
    pgrust_metrics: dict[str, Any],
    postgres_metrics: dict[str, Any],
    candidates: list[str],
) -> str | None:
    for key in candidates:
        if is_number(pgrust_metrics.get(key)) and is_number(postgres_metrics.get(key)):
            return key
    return None


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator, 6)


def print_report(summary: dict[str, Any]) -> None:
    comparisons = summary.get("comparisons", [])
    results = summary.get("results", [])

    print("Benchmark Report")
    print(f"  created: {summary.get('created_at', 'unknown')}")
    print(f"  commit:  {str(summary.get('git_commit', 'unknown'))[:12]}")

    if comparisons:
        print()
        print_comparison_table(comparisons)

    standalone = [
        result
        for result in results
        if result.get("engine") not in {"pgrust", "postgres"}
        or result.get("status") != "ok"
    ]
    if standalone:
        print()
        print_standalone_table(standalone)


def print_comparison_table(comparisons: list[dict[str, Any]]) -> None:
    rows = []
    for item in comparisons:
        rows.append(
            [
                item.get("workload") or item.get("suite", ""),
                format_number(item.get("pgrust_throughput")),
                format_number(item.get("postgres_throughput")),
                format_ratio(item.get("pgrust_to_postgres_throughput_ratio")),
                format_number(item.get("pgrust_latency_ms")),
                format_number(item.get("postgres_latency_ms")),
                format_ratio(item.get("pgrust_to_postgres_latency_ratio")),
            ]
        )

    print_table(
        [
            "workload",
            "pgrust tps",
            "pg tps",
            "tps ratio",
            "pgrust ms",
            "pg ms",
            "lat ratio",
        ],
        rows,
    )


def print_standalone_table(results: list[dict[str, Any]]) -> None:
    rows = []
    for result in results:
        metrics = result.get("metrics", {})
        rows.append(
            [
                result.get("suite", ""),
                result.get("engine", ""),
                result.get("status", ""),
                format_number(
                    metrics.get("tps")
                    or metrics.get("tps_excluding_connections")
                    or metrics.get("queries_per_sec")
                ),
                format_number(
                    metrics.get("avg_latency_ms")
                    or metrics.get("latency_average_ms")
                    or metrics.get("avg_ms_per_query")
                ),
            ]
        )

    print_table(["suite", "engine", "status", "throughput", "latency ms"], rows)


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    widths = [len(header) for header in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    print("  " + "  ".join(header.ljust(widths[i]) for i, header in enumerate(headers)))
    print("  " + "  ".join("-" * width for width in widths))
    for row in rows:
        print("  " + "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))


def format_number(value: Any) -> str:
    if not is_number(value):
        return "-"
    if abs(value) >= 100:
        return f"{value:.1f}"
    if abs(value) >= 10:
        return f"{value:.2f}"
    return f"{value:.3f}"


def format_ratio(value: Any) -> str:
    if not is_number(value):
        return "-"
    return f"{value:.3f}x"


if __name__ == "__main__":
    args = parse_args()
    if args.report_json:
        print_report(json.loads(args.report_json.read_text()))
        sys.exit(0)
    if args.report_history:
        print_history_report(args.report_history, args.history_limit)
        if args.history_dashboard:
            write_history_dashboard(args.report_history, args.history_dashboard)
            print()
            print(f"History dashboard: {args.history_dashboard}")
        sys.exit(0)
    if args.check_history_regressions:
        sys.exit(
            print_history_regression_check(
                args.check_history_regressions, args.regression_threshold_percent
            )
        )
    if args.history_dashboard:
        raise SystemExit("--history-dashboard requires --report-history")

    runner = BenchmarkRunner(args)
    sys.exit(runner.run())
