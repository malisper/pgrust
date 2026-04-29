#!/usr/bin/env python3
"""Aggregate per-shard regression artifacts into one publishable results dir."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path


def rate_pct(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round((numerator * 100.0) / denominator, 2)


def copy_files(src_dir: Path, dest_dir: Path, shard_index: int) -> None:
    if not src_dir.exists():
        return

    dest_dir.mkdir(parents=True, exist_ok=True)
    for src in src_dir.iterdir():
        if not src.is_file():
            continue

        dest = dest_dir / src.name
        if dest.exists():
            dest = dest_dir / f"{src.stem}.shard{shard_index}{src.suffix}"
        shutil.copy2(src, dest)


def shard_index_from_dir(path: Path) -> int | None:
    try:
        return int(path.name.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None


def collect_memory_peaks(shard_dir: Path, shard_index: int) -> list[dict]:
    """Read a shard's per-test memory_peaks.jsonl, return enriched records.

    Returns [] when the file is missing, malformed, or when peak_rss_kb is 0
    (no signal). Each surviving record carries shard_index for downstream UI.
    """
    peaks_path = shard_dir / "memory_peaks.jsonl"
    if not peaks_path.exists():
        return []
    records = []
    for line in peaks_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        try:
            peak_kb = int(record.get("peak_rss_kb", 0))
        except (TypeError, ValueError):
            continue
        if peak_kb <= 0:
            continue
        records.append(
            {
                "test": str(record.get("test", "?")),
                "shard": shard_index,
                "worker": record.get("worker"),
                "peak_rss_kb": peak_kb,
                "peak_rss_mb": round(peak_kb / 1024, 1),
                "duration_sec": int(record.get("duration_sec", 0) or 0),
            }
        )
    return records


def main() -> int:
    if len(sys.argv) != 4:
        print(
            "usage: aggregate_regression_shards.py SHARD_ARTIFACT_DIR OUTPUT_DIR EXPECTED_SHARDS",
            file=sys.stderr,
        )
        return 2

    artifact_root = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    expected_shards = int(sys.argv[3])

    if output_dir.exists():
        shutil.rmtree(output_dir)
    for subdir in ("output", "diff", "status", "fixtures"):
        (output_dir / subdir).mkdir(parents=True, exist_ok=True)

    shard_dirs = {
        idx: path
        for path in artifact_root.glob("regression-shard-*")
        if path.is_dir() and (idx := shard_index_from_dir(path)) is not None
    }

    aggregate = {
        "status": "missing",
        "shards": {
            "expected": expected_shards,
            "present": len(shard_dirs),
            "missing": [],
            "details": [],
        },
        "tests": {
            "planned": 0,
            "total": 0,
            "passed": 0,
            "failed": 0,
            "errored": 0,
            "timed_out": 0,
            "pass_rate_pct": 0.0,
        },
        "queries": {
            "total": 0,
            "matched": 0,
            "mismatched": 0,
            "match_rate_pct": 0.0,
        },
        "memory_peaks": [],
    }
    all_memory_peaks: list[dict] = []

    for shard_index in range(expected_shards):
        shard_dir = shard_dirs.get(shard_index)
        if shard_dir is None:
            aggregate["shards"]["missing"].append(shard_index)
            aggregate["shards"]["details"].append(
                {"index": shard_index, "status": "missing"}
            )
            continue

        summary_path = shard_dir / "summary.json"
        if not summary_path.exists():
            aggregate["shards"]["details"].append(
                {"index": shard_index, "status": "missing_summary"}
            )
            continue

        summary = json.loads(summary_path.read_text())
        tests = summary.get("tests", {})
        queries = summary.get("queries", {})

        aggregate["shards"]["details"].append(
            {
                "index": shard_index,
                "status": summary.get("status", "unknown"),
                "tests_total": tests.get("total", 0),
                "queries_total": queries.get("total", 0),
            }
        )

        for key in ("planned", "total", "passed", "failed", "errored", "timed_out"):
            aggregate["tests"][key] += int(tests.get(key, 0))
        for key in ("total", "matched", "mismatched"):
            aggregate["queries"][key] += int(queries.get(key, 0))

        for subdir in ("output", "diff", "status", "fixtures"):
            copy_files(shard_dir / subdir, output_dir / subdir, shard_index)

        all_memory_peaks.extend(collect_memory_peaks(shard_dir, shard_index))

    statuses = [detail["status"] for detail in aggregate["shards"]["details"]]
    if not shard_dirs:
        aggregate["status"] = "missing"
    elif any(status != "completed" for status in statuses):
        aggregate["status"] = "partial"
    else:
        aggregate["status"] = "completed"

    aggregate["tests"]["pass_rate_pct"] = rate_pct(
        aggregate["tests"]["passed"], aggregate["tests"]["total"]
    )
    aggregate["queries"]["match_rate_pct"] = rate_pct(
        aggregate["queries"]["matched"], aggregate["queries"]["total"]
    )

    # Top per-test memory peaks. Sort by peak desc; cap at 10 records so the
    # summary stays small. Tests appearing multiple times across shards keep
    # only their highest peak (deduplicated by test name + worker tuple isn't
    # what we want — same test can run on different shards under retries, and
    # we want the worst-case the suite ever saw).
    by_test: dict[str, dict] = {}
    for rec in all_memory_peaks:
        key = rec["test"]
        if key not in by_test or rec["peak_rss_kb"] > by_test[key]["peak_rss_kb"]:
            by_test[key] = rec
    top_peaks = sorted(by_test.values(), key=lambda r: -r["peak_rss_kb"])[:10]
    aggregate["memory_peaks"] = top_peaks

    (output_dir / "summary.json").write_text(json.dumps(aggregate, indent=2) + "\n")
    (output_dir / "exit_code.txt").write_text("0\n" if shard_dirs else "1\n")

    manifest = [
        "Aggregated regression shard artifact bundle.",
        f"Source artifact root: {artifact_root}",
        f"Expected shards: {expected_shards}",
        f"Present shards: {len(shard_dirs)}",
        f"Missing shards: {aggregate['shards']['missing']}",
        "",
        "Included: summary.json, exit_code.txt, output/, diff/, status/, fixtures/",
    ]
    (output_dir / "artifact_manifest.txt").write_text("\n".join(manifest) + "\n")

    print(json.dumps(aggregate, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
