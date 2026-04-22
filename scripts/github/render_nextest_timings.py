#!/usr/bin/env python3

from __future__ import annotations

import argparse
import glob
import statistics
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TestCase:
    suite: str
    name: str
    seconds: float
    source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a markdown summary from nextest JUnit XML reports."
    )
    parser.add_argument("inputs", nargs="+", help="Glob patterns or XML files.")
    parser.add_argument(
        "--top",
        type=int,
        default=25,
        help="Number of slow tests to show.",
    )
    return parser.parse_args()


def expand_inputs(inputs: list[str]) -> list[Path]:
    files: list[Path] = []
    for item in inputs:
        matches = [Path(path) for path in glob.glob(item)]
        if matches:
            files.extend(matches)
            continue
        path = Path(item)
        if path.exists():
            files.append(path)
    deduped = sorted(set(files))
    return deduped


def parse_file(path: Path) -> list[TestCase]:
    root = ET.parse(path).getroot()
    cases: list[TestCase] = []
    for suite in root.iter("testsuite"):
        suite_name = suite.attrib.get("name", "<unknown suite>")
        for case in suite.findall("testcase"):
            name = case.attrib.get("name", "<unknown test>")
            seconds = float(case.attrib.get("time", "0") or 0.0)
            classname = case.attrib.get("classname")
            display_name = f"{classname}::{name}" if classname else name
            cases.append(
                TestCase(
                    suite=suite_name,
                    name=display_name,
                    seconds=seconds,
                    source=str(path),
                )
            )
    return cases


def render(cases: list[TestCase], files: list[Path], top: int) -> str:
    lines: list[str] = []
    lines.append("## Test Timing Summary")
    lines.append("")
    if not files:
        lines.append("No JUnit XML files were found.")
        return "\n".join(lines)
    if not cases:
        lines.append("JUnit XML files were found, but no test cases were present.")
        return "\n".join(lines)

    total_seconds = sum(case.seconds for case in cases)
    median_seconds = statistics.median(case.seconds for case in cases)
    mean_seconds = statistics.fmean(case.seconds for case in cases)
    lines.append(f"- Reports: {len(files)}")
    lines.append(f"- Tests: {len(cases)}")
    lines.append(f"- Total reported test time: {total_seconds:.2f}s")
    lines.append(f"- Mean test time: {mean_seconds:.3f}s")
    lines.append(f"- Median test time: {median_seconds:.3f}s")
    lines.append("")
    lines.append(f"### Slowest {min(top, len(cases))} Tests")
    lines.append("")
    lines.append("| Seconds | Test | Report |")
    lines.append("| ---: | --- | --- |")
    for case in sorted(cases, key=lambda item: item.seconds, reverse=True)[:top]:
        lines.append(
            f"| {case.seconds:.3f} | `{case.name}` | `{Path(case.source).name}` |"
        )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    files = expand_inputs(args.inputs)
    cases: list[TestCase] = []
    for path in files:
        cases.extend(parse_file(path))
    print(render(cases, files, args.top))
    return 0


if __name__ == "__main__":
    sys.exit(main())
