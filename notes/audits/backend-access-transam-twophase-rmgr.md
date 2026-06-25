# Audit: backend-access-transam-twophase-rmgr

- **C source**: `src/backend/access/transam/twophase_rmgr.c` (+ `src/include/access/twophase_rmgr.h`)
- **c2rust**: `c2rust-runs/backend-access-transam-twophase-rmgr/src/twophase_rmgr.rs`
- **Port**: `crates/backend-access-transam-twophase-rmgr/src/lib.rs`
- **Branch / commit audited**: `port/backend-access-transam-twophase-rmgr` @ e0e0a3a
- **Auditor**: independent re-derivation from C + c2rust; build `cargo build` clean; `cargo test -p backend-access-transam-twophase-rmgr` 8/8 pass.

## 1. Function inventory

`twophase_rmgr.c` is a tables-only translation unit. Reading the entire C file
and the entire c2rust rendering: there are **zero function definitions** (no
statics, no inlines; `RegisterTwoPhaseRecord`, declared in the header, is
defined in `twophase.c`, not this unit). The unit's contents are exactly four
`const TwoPhaseCallback[TWOPHASE_RM_MAX_ID + 1]` dispatch tables referencing
ten external callbacks. The c2rust output confirms: four `static` arrays of
`[TwoPhaseCallback; 5]` plus ten `extern "C"` declarations, nothing else.

The audit table below therefore enumerates every table slot (the unit's only
auditable artifacts) plus the header constants/typedef the port also carries.

## 2. Per-artifact comparison

### Constants and typedef (twophase_rmgr.h)

| Item | C value (verified in header) | Port | Verdict |
|---|---|---|---|
| `TWOPHASE_RM_END_ID` | 0 | `0u8` | MATCH |
| `TWOPHASE_RM_LOCK_ID` | 1 | `1u8` | MATCH |
| `TWOPHASE_RM_PGSTAT_ID` | 2 | `2u8` | MATCH |
| `TWOPHASE_RM_MULTIXACT_ID` | 3 | `3u8` | MATCH |
| `TWOPHASE_RM_PREDICATELOCK_ID` | 4 | `4u8` | MATCH |
| `TWOPHASE_RM_MAX_ID` | `= TWOPHASE_RM_PREDICATELOCK_ID` | same | MATCH |
| table length | `TWOPHASE_RM_MAX_ID + 1` = 5 | `NUM_TWOPHASE_RM = 5` | MATCH |
| `TwoPhaseCallback` | `void (*)(TransactionId, uint16, void *recdata, uint32 len)` | `fn(TransactionId, u16, &[u8]) -> PgResult<()>` | MATCH (idiomatic: `(recdata, len)` → `&[u8]`; `void` + `ereport(ERROR)` → `PgResult<()>`, repo convention per docs/types.md) |
| `TwoPhaseRmgrId` (`uint8`) | u8 | constants typed `u8` | MATCH |

### Table slots

C uses positional initializers; NULL slot = no callback for that rmgr in
that phase. Port uses `Option<TwoPhaseCallback>` with `None` for NULL and the
owner seam's `::call` for each named callback.

| Table | Slot | C entry | Port entry | Verdict |
|---|---|---|---|---|
| recover | 0 END | NULL | `None` | MATCH |
| recover | 1 Lock | `lock_twophase_recover` | `lock::lock_twophase_recover::call` | SEAMED |
| recover | 2 pgstat | NULL | `None` | MATCH |
| recover | 3 MultiXact | `multixact_twophase_recover` | `multixact::multixact_twophase_recover::call` | SEAMED |
| recover | 4 PredicateLock | `predicatelock_twophase_recover` | `predicate::predicatelock_twophase_recover::call` | SEAMED |
| postcommit | 0 END | NULL | `None` | MATCH |
| postcommit | 1 Lock | `lock_twophase_postcommit` | `lock::lock_twophase_postcommit::call` | SEAMED |
| postcommit | 2 pgstat | `pgstat_twophase_postcommit` | `pgstat::pgstat_twophase_postcommit::call` | SEAMED |
| postcommit | 3 MultiXact | `multixact_twophase_postcommit` | `multixact::multixact_twophase_postcommit::call` | SEAMED |
| postcommit | 4 PredicateLock | NULL | `None` | MATCH |
| postabort | 0 END | NULL | `None` | MATCH |
| postabort | 1 Lock | `lock_twophase_postabort` | `lock::lock_twophase_postabort::call` | SEAMED |
| postabort | 2 pgstat | `pgstat_twophase_postabort` | `pgstat::pgstat_twophase_postabort::call` | SEAMED |
| postabort | 3 MultiXact | `multixact_twophase_postabort` | `multixact::multixact_twophase_postabort::call` | SEAMED |
| postabort | 4 PredicateLock | NULL | `None` | MATCH |
| standby_recover | 0 END | NULL | `None` | MATCH |
| standby_recover | 1 Lock | `lock_twophase_standby_recover` | `lock::lock_twophase_standby_recover::call` | SEAMED |
| standby_recover | 2 pgstat | NULL | `None` | MATCH |
| standby_recover | 3 MultiXact | NULL | `None` | MATCH |
| standby_recover | 4 PredicateLock | NULL | `None` | MATCH |

Spot-check note: the historically risky cell — postabort slot 3 — was
re-verified character-by-character against the C (`multixact_twophase_postabort,`)
and the c2rust output (`Some(multixact_twophase_postabort ...)`); the port has
`multixact::multixact_twophase_postabort::call`. Correct (this is the slot the
port commit says src-idiomatic had wrong).

No control flow, error paths, or edge cases exist in this unit beyond the
NULL-vs-callback pattern; all 20 slots match.

### types-error (support crate added by this port)

Verified against `src/include/utils/elog.h` and `src/backend/utils/errcodes.txt`:

- Levels DEBUG5=10 … DEBUG1=14, LOG=15, LOG_SERVER_ONLY=16 (=COMMERROR),
  INFO=17, NOTICE=18, WARNING=19 (=PGWARNING), WARNING_CLIENT_ONLY=20,
  ERROR=21 (=PGERROR), FATAL=22, PANIC=23 — all match the header exactly.
- `pg_sixbit`/`pg_unsixbit`/`make_sqlstate` match `PGSIXBIT`/`PGUNSIXBIT`/
  `MAKE_SQLSTATE` bit-for-bit (shifts 0/6/12/18/24, mask 0x3F);
  `errcode_to_category`/`errcode_is_category` match the `(1 << 12) - 1` masks.
- `ERRCODE_SUCCESSFUL_COMPLETION`=00000, `ERRCODE_WARNING`=01000,
  `ERRCODE_INTERNAL_ERROR`=XX000 — match errcodes.txt.
- `default_sqlstate_for_level` mirrors elog.c's defaults (>=ERROR → XX000,
  >=WARNING → 01000, else 00000).
- `PgError`/`PgResult` are plain owned data, registered in docs/types.md.

## 3. Seam audit

This unit owns no seams (nothing in twophase_rmgr.c is called from elsewhere
in a cycle-forming way; the tables are consumed by twophase.c via a normal
dependency when that unit lands). Accordingly there is **no `init_seams()`**
in this crate and no `seams-init` change — consistent with the convention
(`seams-init` lists only crates that have an `init_seams()`).

The four new seam crates are owner-side declaration crates:

- `backend-storage-lmgr-lock-seams`: 4 seams (lock_twophase_{recover,
  postcommit,postabort,standby_recover}) — owner `storage/lmgr/lock.c`,
  unported. Declarations only.
- `backend-access-transam-multixact-seams`: 3 seams (multixact_twophase_
  {recover,postcommit,postabort}) — owner `access/transam/multixact.c`,
  unported. Declarations only.
- `backend-utils-activity-stat-seams`: 2 seams (pgstat_twophase_{postcommit,
  postabort}) — owner `utils/activity/pgstat_relation.c`, unported.
  Declarations only.
- `backend-storage-lmgr-predicate-seams`: 1 seam (predicatelock_twophase_
  recover) — owner `storage/lmgr/predicate.c`, unported. Declarations only.

Findings check:

- Each seam is justified: the owning units do not exist yet and (lock,
  multixact, pgstat, predicate are all large upstream subsystems) would form
  dependency cycles/forward deps if linked directly; the C file references
  these symbols purely across the link, exactly the seam pattern.
- Each table slot is a bare `::call` fn-pointer — zero marshaling, zero
  branching, zero computation in any seam path. Clean.
- `grep` for `set(` across the unit and the four seam crates: the only hit is
  inside the port's own `#[cfg(test)]` tests (installing a counting stub on
  `lock_twophase_postcommit` to prove dispatch, and verifying an uninstalled
  slot panics loudly). No production `set()` outside an owner. Clean.
- Uninstalled seams panic with their full path (seam-core `OnceLock` slot);
  no silent fallback. Verified by the unit's `uninstalled_slot_panics_loudly`
  test.

**Seam findings: none.**

## 4. Verdict

All 20 table slots and all constants `MATCH` (or `SEAMED` per the rules:
external callbacks delegated through thin, owner-named seam declarations);
support-crate constants verified against headers; zero seam findings; build
and tests green.

**PASS**
