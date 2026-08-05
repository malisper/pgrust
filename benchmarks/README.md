# pgrust benchmark kit

This directory is the benchmark harness for pgrust's published performance
numbers — ClickBench versus ClickHouse, and OLTP (sysbench read-only,
pgbench read-write) versus C PostgreSQL 18.3.

It is the **exact kit used for the independent external review** of the
release-candidate numbers: the scripts were handed to an outside auditor,
with sole control of the benchmark machines, to reproduce our claims without
our involvement. They are published here as given, so you can run them
yourself. The only changes made for publication are sanitization: our
host addresses, instance/volume ids, ssh username, and time-limited binary
download links were replaced with `CHANGE-ME` placeholders (see `hosts.env`)
or redacted (`binaries/MANIFEST.tsv`). No benchmark logic, tuning flag, or
configuration was altered.

## Hardware of record

The published numbers were measured on AWS Graviton4 instances (us-east-2,
Amazon Linux 2023 arm64):

| box | shape | role |
|---|---|---|
| clickbench | **c8g.4xlarge** (16 vCPU / 32 GB), 500 GiB **gp2** EBS data volume | ClickBench sweep. gp2-500 is the storage class ClickBench's published c8g.4xlarge rows use; benchmarking on default gp3 costs ~12 points of combined score on the disk-bound axes (see `use-reference-storage.sh`) |
| oltp-server | **i8g.xlarge** (4 vCPU / 32 GB, ~872 GB instance-store NVMe) | database server, both arms (pgrust and C PostgreSQL 18.3). Matches PlanetScale's published baseline server shape |
| oltp-client | **c8g.2xlarge** (8 vCPU / 16 GB) | dedicated load driver, per the PlanetScale methodology (client CPU must not steal from the server) |

Provision your own three boxes, fill in `hosts.env`, and install the
server-side pieces with `server-plumbing/install-server-plumbing.sh` (OLTP
arms) and `clickbench-rig/install-clickbench-rig.sh` (ClickBench rig).

## What each piece is

| file | what it does |
|---|---|
| `hosts.env` | host inventory + knobs. **Fill in every `CHANGE-ME`**, then `source hosts.env` before anything else |
| `run-clickbench.sh` | the official ClickBench protocol: per query, stop server, drop OS page cache, restart, run 3 tries (try 1 = cold, min(2,3) = hot). Emits ClickBench-format `result.json` and scores it. Run on the clickbench box |
| `queries.sql` | the 43 ClickBench queries, byte-identical to upstream `postgresql/queries.sql` |
| `clickbench-rig/` | server lifecycle + data load for the ClickBench box (`install`, `start`, `stop`, `load`, `load-parquet`, `query`, `check`, `data-size`, `env.sh`) |
| `scorers/score-clickbench.py` | scores a `result.json` against the vendored leaderboard baselines (`baselines/`) with both the official metric and the combined formula |
| `run-oltp-ro.sh` | sysbench `oltp_read_only`, PlanetScale-exact flags (10 tables x 130M rows ≈ 300 GB, 600 s warmup, 3 x 300 s reps at 32 and 64 threads), both arms, prints the pgrust/C ratio table. Run on the oltp-client box |
| `run-oltp-rw.sh` | pgbench TPC-B-like read-write, both arms, same rep discipline. Run on the oltp-client box |
| `reinit-rw.sh` | rebuilds the pgbench read-write datadirs after they accumulate write history |
| `planetscale-methodology.md` | pinned copy of the OLTP methodology and its sources |
| `configs/` | the server configuration applied identically to both arms (see below) |
| `build-pgo.sh` | builds pgrust from source with the full PGO recipe, including a mechanical proof (`pgo/lint-training-overlap.sh`) that the training corpus shares no statement with the benchmark queries |
| `deploy-binary.sh` | copies a locally built binary to the oltp-server and installs it alongside (never over) the existing one, sha-verified |
| `update-binary.sh` | installs a published pgrust binary, verified against the sha256 trust anchors in `binaries/MANIFEST.tsv`. Download URLs are redacted in this public copy; use `--from PATH` or `--url URL` |
| `use-reference-storage.sh` | moves the ClickBench datadir onto the gp2 reference volume (and verifies the bank's identity first) |
| `use-parquet-bank.sh` | switches the ClickBench rig to the parquet-loaded bank |
| `server-plumbing/` | oltp-server side: `switch-arm.sh` (starts/stops the pgrust or C arm), config, and `LOADINFO.as-built.txt` documenting exactly how the OLTP datasets were loaded |
| `teardown.sh` | destroys the whole environment (instances, EIPs, security group, volumes). Nothing self-terminates; the boxes bill until you run this |

## Run order

```
0.  edit hosts.env (every CHANGE-ME), then on each box: source hosts.env

# clickbench box
1.  ./run-clickbench.sh --smoke        # 2 queries, proves the path (~3 min)
2.  ./run-clickbench.sh                # full 43-query sweep (~40-70 min incl. cold cycles)

# oltp-client box
3.  ./run-oltp-ro.sh --smoke           # ~5 min
4.  ./run-oltp-rw.sh --smoke           # ~5 min
5.  ./run-oltp-ro.sh                   # several hours
6.  ./run-oltp-rw.sh                   # several hours
```

For full provenance, build the binary yourself first (this is what the
external review did): `./build-pgo.sh --lint-only`, `./build-pgo.sh`
(2.5–5 h), `./deploy-binary.sh ~/audit/build/postgres`, then pass
`--binary` to the runners. The C arm is never affected by `--binary`, so a
shipped-vs-your-build comparison moves exactly one side. Every runner
records the measured binary's sha256 in its output so a number can never be
attributed to a binary that did not produce it.

Results land in `$RESULTS_DIR/<benchmark>-<timestamp>/` with the full
transcript, raw per-rep output, and scoring.

## Configuration and durability

Both OLTP arms run an **identical** configuration (`configs/oltp-common.conf`):
PlanetScale's published raw-Postgres frame (`shared_buffers=16GB`,
`work_mem=64MB`, etc.) plus two settings pgrust requires (`io_method=sync`,
`max_stack_depth=60000`), applied to both sides for parity. The ClickBench
config (`configs/clickbench.conf`) is ClickBench's own upstream PostgreSQL
formula, applied verbatim.

**Durability settings are stock.** Nothing in `configs/` touches `fsync`,
`synchronous_commit`, `full_page_writes`, or WAL durability — both engines
run with PostgreSQL's defaults (fsync on, synchronous commit on).
`initdb --no-locale --encoding=UTF8` on both arms; autovacuum on.

## What to expect

- **ClickBench** (c8g.4xlarge, reference gp2 storage): combined score around
  **0.93 measured** (~7% ahead of ClickHouse's published c8g.4xlarge row) —
  much faster cold, slightly faster hot, worse on load time and data size.
  On the default gp3 root volume the same binary reads ~12 combined points
  worse; that is the disk, not the engine — run
  `use-reference-storage.sh --check` before concluding anything.
- **OLTP read-only** (300 GB, i8g.xlarge): pgrust/C ratio in the
  **1.25–1.40x** band at 32–64 threads. These are PGO-build numbers; a
  non-PGO build reads 20–30 points lower.
- **OLTP read-write** (pgbench): **near parity** with C on this raw-instance
  shape (~1.03x measured). Higher read-write ratios we have published come
  from a different (Kubernetes, 100 GB) substrate and should not be expected
  here.

Per-rep spread matters: if reps within a cell differ by more than a few
percent, do not quote the ratio to three digits.

## Environment requirements

The scripts were written for and run on Amazon Linux 2023 (arm64): they
assume a GNU/Linux userland — bash 4+, GNU coreutils (`du -bs`, `du -sBG`),
GNU `time` (`/usr/bin/time -f`), gawk, `lsblk`, `/proc`. They are not
expected to run on macOS/BSD userlands. If you see odd `awk` or number-parse
errors, first check that `awk` is gawk and run under `LC_ALL=C` — a locale
with a comma decimal separator can break the numeric formatting the parsers
expect (the corpus generator pins `LC_ALL=C` itself; the runners inherit
your environment).
