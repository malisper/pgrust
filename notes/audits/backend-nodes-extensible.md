# Audit: backend-nodes-extensible

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Claude Fable 5
- Branch: `fix/report-backend-nodes-extensible`
- Unit C source: `src/backend/nodes/extensible.c` (carved out of the combined
  `backend-nodes-core` catalog unit)
- c2rust reference: `c2rust-runs/backend-nodes-core/src/extensible.rs`
- Port: `crates/backend-nodes-extensible/src/lib.rs`,
  `crates/types-extensible/src/lib.rs`,
  `crates/backend-nodes-extensible-seams/src/lib.rs`

## Re-port note (raw-pointer-overuse remediation)

This crate was flagged HIGH by the raw-pointer-overuse audit: the prior port
open-coded the C `HTAB` registries with a raw-pointer dynahash (`thread_local
*mut HTAB`, raw `hash_search` into `*mut ExtensibleNodeEntry`, `*const c_void`
method-table storage, `unsafe` derefs and a hand C-string `strlen`/key-buffer
copy — 7 `unsafe` sites), exactly the raw-ptr substrate pattern that SIGABRT'd
elsewhere. It is now re-ported from `src-idiomatic`'s SAFE model: each registry
is a `thread_local! RefCell<Vec<(String, M)>>` of owned, `Clone` method tables;
the map *is* the entry table (no `ExtensibleNodeEntry`, no pointer to deref).
`#![forbid(unsafe_code)]` holds in all three touched crates. **unsafe_before = 7,
unsafe_after = 0.** The dynahash dependency (and its `common-hashfn-seams` /
`backend-access-transam-xact-seams` test stand-ins, the SIGABRT vector) is gone.

The seam contract changed shape (C raw-ptr ABI → owned API): `&str` keys instead
of `*const c_char`, `&ExtensibleNodeMethods`/`&CustomScanMethods` for register,
`PgResult<Option<...>>` (cloned table / `None`) for get. The OOM behaviour is
preserved via `try_reserve` mapping to `ERRCODE_OUT_OF_MEMORY` (C's
`palloc`/dynahash abort surface), keeping the `PgResult` signature.
**Blast radius = 3 crates** (this crate, its seams crate, types-extensible); the
only other consumer of the seams crate, `backend-executor-nodeCustom`, uses the
custom-scan *provider* callbacks (`create_custom_scan_state`/…) which are
untouched, and no crate calls the four registry seam functions.

Audit performed independently: function inventory re-derived from the C source
and cross-checked against the full c2rust rendering; logic compared C ↔ c2rust ↔
Rust; constants verified against c2rust's decoded values (not from memory).

## 1. Function inventory

`extensible.c` defines exactly six functions (two file-local statics, four
exported). All six have a Rust counterpart; no other function definitions exist
in the file.

## 2. Per-function table

| # | C function (extensible.c) | C lines | Port location | Verdict | Notes |
|---|---------------------------|---------|---------------|---------|-------|
| 1 | `RegisterExtensibleNodeEntry` (static) | 38-69 | `register_extensible_node_entry` (lib.rs) | MATCH | Lazy `hash_create` when `*p_htable==NULL` ≡ empty `Vec` (no observable step beyond enabling storage); `strlen(extnodename) >= EXTNODENAME_MAX_LEN` → `elog(ERROR,...)` (`errmsg_internal`, no SQLSTATE — matches C `elog`); `HASH_ENTER` `found` ≡ `entries.iter().any(name==)` → `ereport(ERROR, ERRCODE_DUPLICATE_OBJECT, "...already exists")`; else store the cloned table (`try_reserve` guards growth → `ERRCODE_OUT_OF_MEMORY` instead of abort, behaviour-preserving). |
| 2 | `RegisterExtensibleNodeMethods` | 72-82 | `RegisterExtensibleNodeMethods` (lib.rs) | MATCH | Reads `methods.extnodename`, delegates to entry helper with label `"Extensible Node Methods"`, registry `extensible_node_methods`. |
| 3 | `RegisterCustomScanMethods` | 85-94 | `RegisterCustomScanMethods` (lib.rs) | MATCH | Reads `methods.CustomName`, label `"Custom Scan Methods"`, registry `custom_scan_methods`. |
| 4 | `GetExtensibleNodeEntry` (static) | 97-118 | `get_extensible_node_entry` (lib.rs) | MATCH | `entry=NULL`; if `htable!=NULL` `hash_search(HASH_FIND)` ≡ `entries.iter().find(name==)` (empty Vec ≡ NULL htable, both miss); `if (!entry)` → `missing_ok` returns `None` (C `NULL`) else `ereport(ERROR, ERRCODE_UNDEFINED_OBJECT, "...was not registered")`; else returns the cloned table (C `entry->extnodemethods`). |
| 5 | `GetExtensibleNodeMethods` | 121-131 | `GetExtensibleNodeMethods` (lib.rs) | MATCH | Delegates to entry helper on `extensible_node_methods`. |
| 6 | `GetCustomScanMethods` | 134-145 | `GetCustomScanMethods` (lib.rs) | MATCH | Delegates on `custom_scan_methods`. |

### Constants / errors verified

- `EXTNODENAME_MAX_LEN = 64` — matches c2rust (`pub const EXTNODENAME_MAX_LEN = 64`).
- `ERRCODE_DUPLICATE_OBJECT = 42710`, `ERRCODE_UNDEFINED_OBJECT = 42704` —
  types-error matches c2rust packed values.
- Error severities: both `ereport` paths are `ERROR`; the "too long" path is
  `elog(ERROR)` with no SQLSTATE — port uses `errmsg_internal` and sets no
  errcode, matching. Both error message strings reproduced verbatim from C.

### Edge-case notes

- **Lazy creation.** C's `if (*p_htable == NULL) hash_create(...)` has no
  observable effect beyond making the table available; an empty `Vec` is the
  faithful NULL-`HTAB` equivalent and the first `push` is the implicit creation.
- **String-key semantics.** C uses `HASH_STRINGS` (`string_hash`/`string_compare`,
  matching on the NUL-terminated leading bytes within `keysize`). The owned port
  compares whole `String`s by value, which is identical for keys < 64 bytes
  (longer keys are rejected upstream by the length guard) — no truncation/NUL
  semantics to model since the key is owned end-to-end.
- **`HASH_FIND` miss / `HASH_ENTER` found gate.** Miss → `None`/skip-assign as in
  C; on a duplicate the port errors before inserting; on a fresh insert it stores
  the table, matching `entry->extnodemethods = extnodemethods`.
- **Absent name.** C dereferences `methods->extnodename` unconditionally; the
  owned field is `Option<String>`, so `None` raises an internal error
  (`method_name`) rather than a NULL-deref/panic — behaviour-preserving for the
  always-present real case.

## 3. Seam and wiring audit

**Owned seam crate:** `backend-nodes-extensible-seams`. Declares four registry
seams (`RegisterExtensibleNodeMethods`, `RegisterCustomScanMethods`,
`GetExtensibleNodeMethods`, `GetCustomScanMethods`) — the inward boundary for the
copy/equal/out/read dispatch and the custom-scan executor — plus the unrelated
custom-scan PROVIDER callback seams (untouched by this re-port).

- `init_seams()` installs **all four** declared registry seams with nothing but
  `set()` calls. No uninstalled seam; no `set()` outside the owner.
- `seams-init::init_all()` calls `backend_nodes_extensible::init_seams()`; the
  recurrence-guard tests (`every_seam_installing_crate_is_wired_into_init_all`,
  `every_declared_seam_is_installed_by_its_owner`) pass.
- **No outward seams, no dynahash dependency** anymore: the crate's only deps are
  `types-error`, `types-extensible`, `backend-utils-error`, and its own seams
  crate. No marshal-and-delegate seam path; all six functions carry full in-crate
  logic. No `todo!()`/`unimplemented!()`.

## 4. Result

All six functions `MATCH`. `#![forbid(unsafe_code)]` holds; unsafe 7 → 0. Seam
ownership complete (4/4 declared registry seams installed by the owner's
`init_seams`, wired into `seams-init`). Gate: `cargo check --workspace` clean,
`cargo test -p backend-nodes-extensible` (7 tests) + `cargo test -p seams-init`
(2 guards) + `cargo check -p backend-executor-nodeCustom` all green.

**PASS.**
