# Baseline provenance

These are pinned copies of published ClickBench leaderboard rows for the
`c8g.4xlarge` machine class, taken from:

- Leaderboard: <https://benchmark.clickhouse.com/>
- Source repo: <https://github.com/ClickHouse/ClickBench>

They are vendored here rather than fetched at scoring time so that scoring is
reproducible offline and against a fixed snapshot — a leaderboard that moves
under you makes two runs incomparable for reasons that have nothing to do with
the engine.

| file | system | published |
|---|---|---|
| `clickhouse-c8g.json` | ClickHouse | 2026-07-19 |
| `gizmosql-c8g.json` | GizmoSQL | 2026-05-11 |
| `chdb-c8g.json` | chDB | see `date` field |
| `duckdb-c8g.json` | DuckDB | see `date` field |
| `umbra-c8g.json` | Umbra | see `date` field |
| `starrocks-c8g.json` | StarRocks | see `date` field |

Each file carries its own `system`, `date`, `machine`, `load_time`,
`data_size` and 43x3 `result` matrix, in ClickBench's own format.

ClickHouse is the primary comparison in our public statements. The others are
included so the result can be placed in the field rather than against a single
chosen opponent — `score-clickbench.py --all-baselines` scores against all of
them.

To refresh: re-download the corresponding rows from the leaderboard repo and
replace these files. Do not hand-edit them.
