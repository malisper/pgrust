# Audit: backend-commands-seclabel

Unit: `backend-commands-seclabel` (c_source: `src/backend/commands/seclabel.c`)
Branch: `port/backend-commands-seclabel`
Auditor: independent (re-derived from C + c2rust, not trusting port comments/build).

C source: 581 lines, 11 function definitions (enumerated below — every function gets a
row, including statics). c2rust run: `c2rust-runs/backend-commands-seclabel/src/seclabel.rs`.
Port: `crates/backend-commands-seclabel/src/lib.rs` + owned outward-seam crate
`crates/backend-commands-seclabel-seams/src/lib.rs`.

## Function inventory & verdicts

| # | C function (seclabel.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `SecLabelSupportsObjectType` (static, :36-105) | `SecLabelSupportsObjectType` (lib.rs:121) | MATCH | Exhaustive match over `ObjectType`; both true/false sets transcribed verbatim against the C switch (21 supported, 33 unsupported). No `default:` in C → exhaustive Rust match preserves the "compiler warns on new variant" property; trailing `return false` subsumed. Spot-checked all 54 variants against C lines 41-95: identical partition. |
| 2 | `ExecSecLabelStmt` (:114-217) | `ExecSecLabelStmt` (lib.rs:168) | MATCH | Provider-default logic (NIL→error, len!=1→error, else linitial), named-provider foreach+strcmp w/ not-loaded error, SecLabelSupportsObjectType gate, get_object_address(ShareUpdateExclusiveLock,missing_ok=false), check_object_ownership(GetUserId()), OBJECT_COLUMN relkind whitelist, provider hook veto, SetSecurityLabel, relation_close(NoLock). All three ereport SQLSTATEs match (INVALID_PARAMETER_VALUE x3, WRONG_OBJECT_TYPE x2). errloc line numbers (129/133/151/158/191) match C. |
| 3 | `GetSharedSecurityLabel` (static, :223-265) | `GetSharedSecurityLabel` (lib.rs:312) | MATCH | shseclabel_open(AccessShareLock); 3-key scan {objoid,classoid,provider}; single getnext; `if valid → heap_getattr(label); if !isnull → TextDatumGetCString`; close. `seclabel=None` init matches `char*=NULL`. |
| 4 | `GetSecurityLabel` (:271-322) | `GetSecurityLabel` (lib.rs:342) | MATCH | IsSharedRelation → delegate to shared helper; else 4-key scan {objoid,classoid,objsubid,provider} on pg_seclabel; same getnext/isnull/return shape. |
| 5 | `SetSharedSecurityLabel` (static, :328-396) | `SetSharedSecurityLabel` (lib.rs:379) | MATCH | values/nulls/replaces sized `Natts_pg_shseclabel`(=4); fills objoid,classoid,provider, label-iff-Some; RowExclusiveLock; find-one; found→(label None→delete / else replaces[label]=true,update); not-found+label→insert. The C `newtup==NULL` guard ≡ Rust `found` flag (faithful: found-and-updated vs not-found-insert; the delete branch leaves found=true so no insert, matching C where delete leaves newtup=NULL **but** label==NULL so the insert guard `label!=NULL` is false — same net behaviour). |
| 6 | `SetSecurityLabel` (:403-484) | `SetSecurityLabel` (lib.rs:436) | MATCH | IsSharedRelation→shared helper+return; values sized 5; fills objoid,classoid,objsubid,provider,label-iff-Some; 4-key find-one; identical upsert decision; insert iff !found && label.is_some(). |
| 7 | `DeleteSharedSecurityLabel` (:490-516) | `DeleteSharedSecurityLabel` (lib.rs:495) | MATCH | Always 2 keys {objoid,classoid}; RowExclusiveLock; delete-all loop behind seam; close. |
| 8 | `DeleteSecurityLabel` (:522-567) | `DeleteSecurityLabel` (lib.rs:510) | MATCH | IsSharedRelation→Assert(objsubid==0)+shared delete. C `Assert`→`debug_assert!` (faithful: Assert is debug-only). nkeys 3-vs-2 reproduced as `objsubid: Option<i32>` (Some iff objectSubId!=0). |
| 9 | `register_label_provider` (:569-581) | `register_label_provider` (lib.rs:539) | MATCH | C palloc+pstrdup in TopMemoryContext + lappend → push owned `LabelProvider{String,hook}` onto process-global Vec. Append-only, process lifetime → ownership exact (behaviour-preserving per repo global-state model). |
| 10 | `object_node` helper | lib.rs:554 | n/a (port-local) | Borrows `stmt->object` parser Node for get_object_address/check_object_ownership; None (malformed stmt) → hard panic, not a sentinel. Faithful to C deref of `stmt->object`. |
| 11 | `errloc` helper | lib.rs:109 | n/a (port-local) | ErrorLocation factory for the file's ereports; source path matches. |

## Constants verified against C headers (not memory)

`src/include/catalog/pg_seclabel.h` CATALOG order: objoid(1), classoid(2), objsubid(3),
provider(4), label(5); Natts=5. Port `NATTS_PG_SECLABEL=5`, ANUM_* 1..5 — MATCH.

`src/include/catalog/pg_shseclabel.h` CATALOG order: objoid(1), classoid(2), provider(3),
label(4); Natts=4 (no objsubid column — shared objects). Port `NATTS_PG_SHSECLABEL=4`,
ANUM_* 1..4 — MATCH. The shared catalog correctly omits objsubid in scan keys and tuple.

Lock modes: ShareUpdateExclusiveLock (get_object_address), AccessShareLock (Get*),
RowExclusiveLock (Set*/Delete*), NoLock (relation_close) — all MATCH.

## Seam audit

Owned seam crate (by c_source coverage): `backend-commands-seclabel-seams`. Its
declarations are **all OUTWARD** (cross-cycle callees in other subsystems): `get_user_id`
(miscinit), `cstring_get_text_datum`/`text_datum_get_cstring` (varlena), and the
pg_seclabel/pg_shseclabel catalog primitives `*_open`/`*_get_label`/`*_find_one`/
`*_delete`/`*_update`/`*_insert`/`*_delete_all` (table.h + genam systable scans + indexing
CatalogTuple* + heaptuple heap_form/modify). Per repo convention these are installed by
their real owners when they land; until then each panics with its seam path
(seam-and-panic). The crate's `init_seams()` is empty and **correct** — this unit owns no
inward-facing seams (nothing in the tree calls into seclabel across a cycle).

Seam-granularity check: the catalog scans (ScanKeyInit + systable_beginscan/getnext loop,
CatalogTupleInsert/Update/Delete, heap_modify_tuple) live behind the owner seams. This is
the established repo convention for catalog-scan primitives and is **identical** to the
just-merged sibling `backend-commands-comment` (`description_find_one`,
`description_get_description`, `description_delete_all`, etc.). The in-crate control flow
that decides *what* to do — the provider-default tree, the SecLabelSupportsObjectType gate,
the OBJECT_COLUMN relkind whitelist, the upsert delete/update/insert decision, the
3-vs-2-key choice, the IsSharedRelation routing — all remains in this crate. No branching,
node construction, or non-marshal computation lives in a seam call. No function body was
replaced by a "call somewhere else" (would be MISSING) — every C function's logic is
present in-crate; only genuine cross-subsystem callees cross seams.

Real (non-seam) direct calls: `IsSharedRelation` (backend-catalog-catalog) and
`errdetail_relkind_not_supported` (backend-catalog-pg-class) — both ported functions.
`get_object_address`/`check_object_ownership` cross the objectaddress owner's seam crate.

## Design conformance

- Allocating seams (`cstring_get_text_datum`, `*_open`, …) take `Mcx` and return
  `PgResult` — OK.
- `check_object_relabel_type` hook is `fn(&ObjectAddress, Option<&str>)->PgResult<()>`:
  failure surface mirrors the C hook's `ereport(ERROR)` (PgResult), no invented opacity,
  no `&'static mut`. OK.
- Process-global `LABEL_PROVIDER_LIST: Mutex<Vec<..>>` models a TopMemoryContext
  append-only list loaded at library init; lock is taken, values copied out, lock dropped
  before any `?`/ereport (no lock held across `?`). OK.
- No todo!/unimplemented!/own-logic stubs; no deferred/Err(unsupported) escapes.

## Gates

- `cargo check --workspace`: PASS (pre-existing unrelated warnings only).
- `cargo test -p backend-commands-seclabel`: PASS (2 tests).
- `cargo test -p seams-init`: PASS — `every_declared_seam_is_installed_by_its_owner` and
  `every_seam_installing_crate_is_wired_into_init_all` both green.

## Verdict: PASS

Every function MATCH or legitimately SEAMED (genuine cross-subsystem callee, thin
marshal+delegate). Zero MISSING/PARTIAL/DIVERGES. Zero seam findings. Constants verified
against headers. Wiring/guard green.
