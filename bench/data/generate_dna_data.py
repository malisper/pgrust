#!/usr/bin/env python3
"""Generate FASTA-like DNA sequence data for the regex-redux benchmark.

Produces SQL to load DNA sequences into a table. Each row contains one line
of DNA sequence data (60 chars of [acgt]), matching the Benchmarks Game
FASTA format without headers or newlines.

Usage:
    python3 bench/data/generate_dna_data.py                # default 500K lines (~30MB)
    python3 bench/data/generate_dna_data.py --lines 100000 # smaller dataset
"""

import argparse
import random
import sys

TABLE = "dnabench"
BATCH_SIZE = 1000
LINE_LENGTH = 60  # standard FASTA line width


def generate_dna_lines(n, seed=42):
    """Generate n lines of random DNA sequence, 60 chars each."""
    rng = random.Random(seed)
    bases = "acgt"
    lines = []
    for _ in range(n):
        line = "".join(rng.choices(bases, k=LINE_LENGTH))
        lines.append(line)
    return lines


def sql_escape(s):
    return s.replace("'", "''")


def emit_sql(lines):
    print(f"DROP TABLE IF EXISTS {TABLE};")
    print(f"CREATE TABLE {TABLE} (id int NOT NULL, seq text NOT NULL);")
    print("BEGIN;")

    for batch_start in range(0, len(lines), BATCH_SIZE):
        batch = lines[batch_start : batch_start + BATCH_SIZE]
        values = []
        for i, line in enumerate(batch):
            row_id = batch_start + i
            values.append(f"({row_id},'{sql_escape(line)}')")
        print(f"INSERT INTO {TABLE} (id, seq) VALUES {','.join(values)};")

    print("COMMIT;")

    # Also emit the full concatenated sequence as a single row for whole-input matching
    full_seq = "".join(lines)
    print(f"DROP TABLE IF EXISTS {TABLE}_full;")
    print(f"CREATE TABLE {TABLE}_full (id int NOT NULL, seq text NOT NULL);")

    # Split the full sequence into chunks to avoid overly long SQL lines
    chunk_size = 100000
    print("BEGIN;")
    for i in range(0, len(full_seq), chunk_size):
        chunk = full_seq[i : i + chunk_size]
        print(f"INSERT INTO {TABLE}_full (id, seq) VALUES ({i // chunk_size},'{chunk}');")
    print("COMMIT;")

    print(f"-- Loaded {len(lines)} rows into {TABLE} ({len(full_seq)} bases total)", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Generate DNA regex benchmark data")
    parser.add_argument("--lines", type=int, default=500000,
                        help="Number of 60-char DNA lines (default: 500000, ~30MB)")
    args = parser.parse_args()

    print(f"-- Generating {args.lines} DNA sequence lines...", file=sys.stderr)
    lines = generate_dna_lines(args.lines)
    emit_sql(lines)


if __name__ == "__main__":
    main()
