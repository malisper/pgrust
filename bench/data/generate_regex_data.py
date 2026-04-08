#!/usr/bin/env python3
"""Generate SQL to load the mariomka regex-benchmark input text into a table.

Downloads input-text.txt from the mariomka/regex-benchmark repo and emits
CREATE TABLE + batched INSERT statements suitable for both pgrust and PostgreSQL.

Usage:
    python3 bench/data/generate_regex_data.py            # download + emit SQL
    python3 bench/data/generate_regex_data.py --no-download  # synthetic fallback only
"""

import argparse
import random
import string
import sys
import urllib.request

URL = "https://raw.githubusercontent.com/mariomka/regex-benchmark/master/input-text.txt"

TABLE = "regexbench"
BATCH_SIZE = 1000


def download_input_text():
    """Try to download the mariomka input text. Returns lines or None."""
    try:
        req = urllib.request.Request(URL, headers={"User-Agent": "pgrust-bench/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode("utf-8", errors="replace")
        lines = [l for l in data.splitlines() if l.strip()]
        if lines:
            return lines
    except Exception as e:
        print(f"-- Download failed: {e}", file=sys.stderr)
    return None


def generate_synthetic_lines(n=30000):
    """Generate synthetic text lines with embedded emails, URIs, and IPs."""
    rng = random.Random(42)
    lines = []
    words = ["the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
             "hello", "world", "data", "system", "network", "server", "client",
             "protocol", "request", "response", "error", "debug", "config"]
    tlds = ["com", "org", "net", "io", "dev"]
    schemes = ["http", "https", "ftp", "ssh"]

    for i in range(n):
        parts = [rng.choice(words) for _ in range(rng.randint(5, 20))]

        # ~20% of lines get an email
        if rng.random() < 0.2:
            user = "".join(rng.choices(string.ascii_lowercase, k=rng.randint(3, 10)))
            domain = "".join(rng.choices(string.ascii_lowercase, k=rng.randint(4, 8)))
            tld = rng.choice(tlds)
            parts.insert(rng.randint(0, len(parts)), f"{user}@{domain}.{tld}")

        # ~15% get a URI
        if rng.random() < 0.15:
            scheme = rng.choice(schemes)
            host = "".join(rng.choices(string.ascii_lowercase, k=6))
            tld = rng.choice(tlds)
            path = "/".join(rng.choices(words, k=rng.randint(1, 3)))
            parts.insert(rng.randint(0, len(parts)), f"{scheme}://{host}.{tld}/{path}")

        # ~10% get an IP
        if rng.random() < 0.10:
            ip = ".".join(str(rng.randint(0, 255)) for _ in range(4))
            parts.insert(rng.randint(0, len(parts)), ip)

        lines.append(" ".join(parts))

    return lines


def sql_escape(s):
    """Escape a string for SQL single-quoted literal."""
    return s.replace("'", "''").replace("\\", "\\\\")


def emit_sql(lines):
    """Print SQL statements to stdout."""
    print(f"DROP TABLE IF EXISTS {TABLE};")
    print(f"CREATE TABLE {TABLE} (id int NOT NULL, content text NOT NULL);")
    print("BEGIN;")

    for batch_start in range(0, len(lines), BATCH_SIZE):
        batch = lines[batch_start : batch_start + BATCH_SIZE]
        values = []
        for i, line in enumerate(batch):
            row_id = batch_start + i
            values.append(f"({row_id},'{sql_escape(line)}')")
        print(f"INSERT INTO {TABLE} (id, content) VALUES {','.join(values)};")

    print("COMMIT;")
    print(f"-- Loaded {len(lines)} rows into {TABLE}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Generate regex benchmark data SQL")
    parser.add_argument("--no-download", action="store_true",
                        help="Skip download, use synthetic data only")
    args = parser.parse_args()

    lines = None
    if not args.no_download:
        print("-- Downloading mariomka input text...", file=sys.stderr)
        lines = download_input_text()

    if lines is None:
        print("-- Using synthetic data", file=sys.stderr)
        lines = generate_synthetic_lines()

    emit_sql(lines)


if __name__ == "__main__":
    main()
