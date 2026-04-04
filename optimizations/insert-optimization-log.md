# Insert Optimization Log

**Goal:** Optimize single-transaction bulk insert performance.

**Benchmark:** `bench_insert --rows 100000`

**Comparison:** PostgreSQL 18 via Docker — 730k inserts/sec (single txn), 7,750 inserts/sec (auto-commit)

## Baseline: 232 inserts/sec (auto-commit)

Profiled with dtrace. **82% of time in syscalls:**
- 29% `close` — closing file descriptors
- 24% `__open` — opening files
- 18% `write` — writing data
- 11% `__fcntl` — fsync

Root cause: `TransactionManager::persist()` rewrites the entire BTreeMap to a
file (open + serialize + write + close) on every begin/commit/abort.

## Round 1: Fix CLOG persistence

### 1. Single-byte CLOG writes
- Replaced full-file rewrite with fixed-format file: 4-byte header + 1 byte per xid
- Keep file handle open persistently
- Status changes are `seek + write(1 byte)` — no open/close
- Impact: 232 → 58,000 inserts/sec

### 2. Single-transaction batching (benchmark fix)
- Changed benchmark to use BEGIN/COMMIT wrapping all inserts
- One WAL fsync at commit instead of one per row
- Impact: 58,000 → 62,000 inserts/sec

## Round 2: Eliminate per-insert syscalls

Re-profiled. Still 66% syscalls from storage manager:
- `smgr.open()` → `mkdir` per insert (creating directories)
- `smgr.nblocks()` → `stat` per insert (counting blocks via file metadata)
- `smgr.create()` → `create_new` open per insert (checking if file exists)
- `create_dir_all` → `stat` per insert (checking directory exists)

### 3. Cache opened relations (opened_rels HashSet)
- `smgr.open()` checks cache first, skips `mkdir`/`create_dir_all` if already opened
- Impact: 62,000 → 83,000 inserts/sec

### 4. Cache block counts (nblocks_cache HashMap)
- `smgr.nblocks()` returns cached count instead of calling `stat()`
- Updated on `extend()`, invalidated on `truncate`/`unlink`/`close`
- Impact: included in #3

### 5. Pre-open relations on Database startup
- `Database::open` iterates the catalog and calls `smgr.open()` + `smgr.create()`
  for all existing relations
- `CREATE TABLE` also opens/creates storage immediately
- Impact: 83,000 → 93,000 inserts/sec

### 6. Cache created forks (created_forks HashSet)
- `smgr.create()` returns `AlreadyExists` from cache instead of attempting
  `OpenOptions::create_new()` syscall
- Skips the `__open` syscall that was happening 100k times
- Impact: 93,000 → 99,000 inserts/sec

## Round 3: WAL optimizations

Re-profiled. Remaining costs:
- 27% `write` — one write() syscall per WAL record (100k syscalls)
- 4% `lseek` — seeking before each write
- 1% `fcntl` — fsync at commit
- ~9% pest parser — SQL parsing per insert

### 7. CRC32C checksums on WAL records
- Added CRC32C (hardware-accelerated) to each WAL record
- Enables detection of torn writes during recovery
- New format matches PostgreSQL's XLogRecord layout:
  xl_tot_len, xl_xid, xl_prev, xl_info, xl_rmid, xl_crc header
- Prerequisite for safe WAL write batching
- Impact: negligible overhead (~2% slower from CRC computation)

### 8. Remove per-write WAL seek
- WAL writer called `seek(SeekFrom::End(0))` before every `write_all()`
- Since we're the only writer and track position via `insert_lsn`, the file
  position is already at the end after the previous write
- Seek once in `WalWriter::new()` instead
- Impact: 99,000 → 105,000 inserts/sec

### 9. BufWriter for WAL batched writes
- Wrapped WAL `File` in `BufWriter` with 64KB buffer
- Instead of one `write()` syscall per ~8KB record, records accumulate
  in userspace buffer and flush in batches of ~8 records
- Reduces 100k `write()` syscalls to ~12.5k
- `BufWriter::flush()` called before fsync and on drop
- Impact: 105,000 → 140,000 inserts/sec

## Final Result

| Optimization | inserts/sec | vs baseline |
|---|---|---|
| Baseline (auto-commit, full-file CLOG) | 232 | 1x |
| Single-byte CLOG writes | 58,000 | 250x |
| Single transaction | 62,000 | 267x |
| smgr caching | 83,000 | 358x |
| Pre-open on startup | 93,000 | 401x |
| created_forks cache | 99,000 | 427x |
| Remove WAL seek | 105,000 | 453x |
| BufWriter WAL | **140,000** | **603x** |

**603x faster than baseline. 5x slower than PostgreSQL (730k/sec).**

## Remaining gap vs PostgreSQL

1. **Full page image WAL** — We write 8KB per insert (entire page). PG writes
   ~100B (row-level delta) after the first full page image per checkpoint.
   For 100k inserts: we write ~800MB of WAL, PG writes ~10MB.

2. **SQL parsing** — ~9% of time is pest parser parsing `INSERT INTO ... VALUES (...)`
   from scratch for every row. PG's DO block uses optimized internal execution.

3. **No group commit** — For auto-commit, PG batches fsyncs from concurrent
   transactions. We fsync synchronously per commit.

## Key Lessons

1. **Profile before optimizing.** The first profile showed 82% syscalls — no amount
   of code optimization would help without fixing the I/O pattern.
2. **Don't rewrite files on every operation.** The original CLOG rewrote the entire
   status file on every begin/commit. Single-byte writes are 250x faster.
3. **Cache filesystem metadata.** `stat()`, `mkdir()`, `open()` per insert add up.
   Cache "does this exist" in a HashSet.
4. **Batch I/O.** BufWriter turns 100k small writes into ~12.5k larger writes for free.
5. **Match PostgreSQL's architecture.** Reading PG's CLOG (clog.c) and WAL (xlog.c)
   code directly informed the right design: in-memory buffers with lazy flush.
