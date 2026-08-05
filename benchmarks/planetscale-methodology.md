# PlanetScale Postgres benchmark methodology (pinned)

Sources (fetched 2026-07-04):
- https://planetscale.com/blog/benchmarking-postgres (methodology blog, 2025 "PlanetScale for Postgres" launch)
- https://planetscale.com/benchmarks (living methodology page)
- https://planetscale.com/benchmarks/aurora (example comparison page; thread counts, metrics)
- https://planetscale.xyz/benchmarks/instructions/tpcc500g (verbatim TPCC commands)
- https://planetscale.xyz/benchmarks/instructions/oltp300g (verbatim OLTP commands)
- https://planetscale.com/blog/benchmarking-postgres-17-vs-18 (their raw-Postgres-vs-Postgres methodology — closest analogue to our pgrust-vs-C run)
- Workload sources: github.com/akopytov/sysbench (oltp_common.lua / oltp_read_only.lua), github.com/Percona-Lab/sysbench-tpcc (tpcc.lua)

PlanetScale runs benchmarks with an internal tool ("Telescope"); it is not public.
The public reproduction instructions above are the authoritative commands. There is
no public repo with full automation or raw results.

## Workloads

### 1. TPCC (~500 GB)

Percona sysbench-tpcc (`tpcc.lua`), sysbench >= 1.0.2, `--db-driver=pgsql`.

Prepare:

    ./tpcc.lua --pgsql-host=$H --pgsql-port=$P --pgsql-user=$U --pgsql-password=$W \
      --pgsql-db=$D --tables=20 --scale=250 --use_fk=0 --threads=20 \
      --report-interval=1 --db-driver=pgsql prepare

Run (per thread count):

    ./tpcc.lua ... --tables=20 --scale=250 --use_fk=0 --time=300 \
      --threads=$THREADS --report-interval=1 --histogram=off --percentile=99 \
      --db-driver=pgsql run

- 20 table sets x scale 250 (= 5000 warehouses total), ~500 GB database.
- Duration 300 s per run. Thread counts published: 32 and 64.
- No FK constraints (`use_fk=0`). Standard TPC-C 5-transaction mix
  (new_order/payment/order_status/delivery/stock_level, sysbench-tpcc default
  weights ~45/43/4/4/4).

### 2. OLTP read-only (~300 GB)

sysbench built-in `oltp_read_only`, >= 1.0.2.

Prepare:

    sysbench oltp_read_only --pgsql-host=... --tables=10 --table-size=130000000 \
      --threads=2 --report-interval=1 --db-driver=pgsql prepare

Run:

    sysbench oltp_read_only ... --tables=10 --table-size=130000000 --time=300 \
      --threads=$THREADS --report-interval=1 --histogram=off --percentile=99 \
      --db-driver=pgsql run

- 10 tables x 130 M rows (~300 GB). 300 s per run.
- Default transaction (skip_trx=false): BEGIN; 10 point selects; 1 simple range;
  1 sum range; 1 order range; 1 distinct range (range_size=100 default); COMMIT.
- The 17-vs-18 blog (raw Postgres) additionally swept range_size in {100, 10000}
  and connections in {1, 10, 50}, 5 min runs after a 10-minute warmup, plus a
  point-selects-only variant. The comparison pages show a point-select variant too.

### 3. Query-path latency

`SELECT 1;` run 200 times in a row on a single connection; round-trip time per
query is measured (they plot the distribution / percentiles).

## Metrics reported

- QPS (OLTP pages) / TPS-equivalents from sysbench output, and p99 latency
  (`--percentile=99`). Comparison pages chart QPS and p99 per thread count.
- Latency benchmark: per-query round-trip times.

## Hardware / placement (as published)

- PlanetScale baseline server: "M-320" — 4 vCPU, 32 GB RAM, 937 GB local NVMe
  (i8g-class, i.e. Graviton4 + NVMe).
- Client: c6a.xlarge (4 vCPU/8 GB) on AWS us-east-1 (e2-standard-4 on GCP),
  same region as the server, separate machine.
- Managed-service comparisons: "Postgres configuration options left at each
  platform's defaults" except connection limits/timeouts.
- Their raw-Postgres runs (17 vs 18 blog) used r7i.2xlarge / i7i.2xlarge
  (8 vCPU, 64 GB) with shared_buffers=16GB, effective_cache_size=48GB,
  work_mem=64MB, effective_io_concurrency=200, max_parallel_workers=8.

## Exact SQL surface the workloads require

### sysbench oltp_read_only

Schema (pgsql driver):

    CREATE TABLE sbtest<N> (
      id SERIAL,
      k INTEGER DEFAULT '0' NOT NULL,
      c CHAR(120) DEFAULT '' NOT NULL,
      pad CHAR(60) DEFAULT '' NOT NULL,
      PRIMARY KEY (id));
    CREATE INDEX k_<N> ON sbtest<N>(k);

Load: multi-row bulk `INSERT INTO sbtest<N> (k, c, pad) VALUES (...),(...),...`.

Run-time statements (server-side prepared statements via libpq PQprepare /
extended protocol; sysbench pgsql driver prepares each template once per
connection):

    BEGIN / COMMIT
    SELECT c FROM sbtest<N> WHERE id=$1                                   (x10)
    SELECT c FROM sbtest<N> WHERE id BETWEEN $1 AND $2                    (x1)
    SELECT SUM(k) FROM sbtest<N> WHERE id BETWEEN $1 AND $2               (x1)
    SELECT c FROM sbtest<N> WHERE id BETWEEN $1 AND $2 ORDER BY c         (x1)
    SELECT DISTINCT c FROM sbtest<N> WHERE id BETWEEN $1 AND $2 ORDER BY c(x1)

(read-write variants, not used by oltp_read_only but part of the sysbench family:
`UPDATE ... SET k=k+1 WHERE id=$1`, `UPDATE ... SET c=$1 WHERE id=$2`,
`DELETE FROM ... WHERE id=$1`, `INSERT INTO ... (id,k,c,pad) VALUES ($1,$2,$3,$4)`.)

### Percona sysbench-tpcc (pgsql driver)

Schema: 9 tables per table-set (warehouse<N>, district<N>, customer<N>,
history<N>, orders<N>, new_orders<N>, order_line<N>, item<N>, stock<N>) using
smallint/int/varchar/char/decimal(x,y)/timestamp columns, composite primary
keys (e.g. `PRIMARY KEY (d_w_id, d_id)`), secondary indexes created after
load (`idx_customer`, `idx_orders`, `fkey_*`-named indexes), no FKs at
use_fk=0. Load is multi-row bulk INSERTs. Note: history has no PK; stock/
order_line are the big tables.

Run-time statements (interpolated literals, simple protocol; per transaction):

    BEGIN / COMMIT / ROLLBACK  (1% of new_order rolls back per spec)
    -- new_order
    SELECT c_discount, c_last, c_credit, w_tax FROM customer<N>, warehouse<N>
      WHERE w_id=.. AND c_w_id=.. AND c_d_id=.. AND c_id=..
    SELECT d_next_o_id, d_tax FROM district<N> WHERE d_w_id=.. AND d_id=.. FOR UPDATE
    UPDATE district<N> SET d_next_o_id=.. WHERE d_id=.. AND d_w_id=..
    INSERT INTO orders<N> (...) VALUES (...)
    INSERT INTO new_orders<N> (no_o_id,no_d_id,no_w_id) VALUES (...)
    SELECT i_price, i_name, i_data FROM item<N> WHERE i_id=..
    SELECT s_quantity, s_data, s_dist_XX s_dist FROM stock<N>
      WHERE s_i_id=.. AND s_w_id=.. FOR UPDATE
    UPDATE stock<N> SET s_quantity=.. WHERE s_i_id=.. AND s_w_id=..
    INSERT INTO order_line<N> (...) VALUES (...)
    -- payment
    UPDATE warehouse<N> SET w_ytd = w_ytd + .. WHERE w_id=..
    SELECT w_street_1,... FROM warehouse<N> WHERE w_id=..
    UPDATE district<N> SET d_ytd = d_ytd + .. WHERE d_w_id=.. AND d_id=..
    SELECT d_street_1,... FROM district<N> WHERE d_w_id=.. AND d_id=..
    SELECT count(c_id) namecnt FROM customer<N> WHERE c_w_id=.. AND c_d_id=.. AND c_last='..'
    SELECT c_id FROM customer<N> WHERE ... AND c_last='..' ORDER BY c_first
    SELECT c_first,...,c_since FROM customer<N> WHERE ... FOR UPDATE
    SELECT c_data FROM customer<N> WHERE ...
    UPDATE customer<N> SET c_balance=.., c_ytd_payment=.. [, c_data='..'] WHERE ...
    INSERT INTO history<N> (...) VALUES (...)
    -- order_status
    SELECT count(c_id) namecnt / SELECT c_balance,... (by last name, ORDER BY c_first)
    SELECT o_id, o_carrier_id, o_entry_d FROM orders<N> WHERE ... ORDER BY o_id DESC
    SELECT ol_i_id,... FROM order_line<N> WHERE ol_w_id=.. AND ol_d_id=.. AND ol_o_id=..
    -- delivery
    SELECT no_o_id FROM new_orders<N> WHERE no_d_id=.. AND no_w_id=..
      ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE
    DELETE FROM new_orders<N> WHERE no_o_id=.. AND no_d_id=.. AND no_w_id=..
    SELECT o_c_id FROM orders<N> WHERE o_id=.. AND o_d_id=.. AND o_w_id=..
    UPDATE orders<N> SET o_carrier_id=.. WHERE o_id=.. AND o_d_id=.. AND o_w_id=..
    UPDATE order_line<N> SET ol_delivery_d=NOW() WHERE ol_o_id=.. AND ol_d_id=.. AND ol_w_id=..
    SELECT SUM(ol_amount) sm FROM order_line<N> WHERE ol_o_id=.. AND ol_d_id=.. AND ol_w_id=..
    UPDATE customer<N> SET c_balance=c_balance+.., c_delivery_cnt=c_delivery_cnt+1 WHERE ...
    -- stock_level
    SELECT d_next_o_id FROM district<N> WHERE d_id=.. AND d_w_id=..
    SELECT COUNT(DISTINCT (s_i_id)) FROM order_line<N>, stock<N>
      WHERE ol_w_id=.. AND ol_d_id=.. AND ol_o_id < .. AND ol_o_id >= ..
      AND s_w_id=.. AND s_i_id=ol_i_id AND s_quantity < ..

### Latency benchmark

    SELECT 1;   (simple protocol, 200 iterations, single connection)

## Divergences we must note for our reproduction

- PlanetScale's headline numbers compare managed platforms; our run is the
  "raw Postgres vs raw Postgres" analogue (their 17-vs-18 setup) on identical
  hardware, which is the fair frame for pgrust-vs-C.
- Fleet standard is c8gd Graviton (arm64). PlanetScale M-320 is i8g (also
  Graviton + local NVMe, 4 vCPU/32 GB) — close; their clients were x86
  (c6a.xlarge). Same-instance-type pgrust-vs-C keeps the comparison fair.
- Dataset sizes (500 GB / 300 GB) are chosen to exceed RAM (32 GB) so the
  runs are storage-heavy on local NVMe. Matching that requires c8gd local
  NVMe and multi-hour prepare time.
