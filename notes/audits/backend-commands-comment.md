# Audit: backend-commands-comment (comment.c)

Independent function-by-function audit of `port/backend-commands-comment`
(`c117bc0c`) against `src/backend/commands/comment.c` and
`../pgrust/c2rust-runs/backend-commands-comment/`.

## Function inventory

comment.c defines exactly 6 public functions (no statics, no inline helpers).
All 6 are confirmed present in the c2rust rendering and the Rust port.

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `CommentObject` | comment.c:39-131 | lib.rs:86-198 | MATCH | See below |
| `CreateComments` | comment.c:142-226 | lib.rs:207-260 | MATCH | See below |
| `CreateSharedComments` | comment.c:237-316 | lib.rs:266-307 | MATCH | See below |
| `DeleteComments` | comment.c:325-368 | lib.rs:316-327 | MATCH | See below |
| `DeleteSharedComments` | comment.c:373-404 | lib.rs:333-338 | MATCH | See below |
| `GetComment` | comment.c:409-459 | lib.rs:343-364 | MATCH | See below |

### CommentObject — MATCH
- COMMENT ON DATABASE dump work-around: `objtype == OBJECT_DATABASE` →
  `get_database_oid(name, missing_ok=true)`; on `!OidIsValid` emits
  `ereport(WARNING, ERRCODE_UNDEFINED_DATABASE, "database \"%s\" does not
  exist")` and returns `InvalidObjectAddress`. Matches c2rust (elevel 19 =
  WARNING, errcode decodes to `3D000`).
- `get_object_address(objtype, object, &relation, ShareUpdateExclusiveLock,
  missing_ok=false)` then `check_object_ownership(GetUserId(), objtype,
  address, object, relation)` — same order, same args.
- OBJECT_COLUMN integrity check: relkind whitelist `r/v/m/c/f/p`
  (`RELKIND_RELATION/VIEW/MATVIEW/COMPOSITE_TYPE/FOREIGN_TABLE/PARTITIONED_TABLE`)
  with `ereport(ERROR, ERRCODE_WRONG_OBJECT_TYPE, "cannot set comment on
  relation \"%s\"")` + `errdetail_relkind_not_supported(relkind)`. errcode
  decodes to `42809`, elevel 21 = ERROR. The C `switch` has only the
  OBJECT_COLUMN case + default-noop; the port's `match` mirrors it. The
  unconditional `relation` deref in the C is preserved by `relation.expect(...)`
  (get_object_address always opens the table for a column).
- Shared-vs-local routing: `OBJECT_DATABASE || OBJECT_TABLESPACE || OBJECT_ROLE`
  → `CreateSharedComments(objectId, classId, comment)`, else
  `CreateComments(objectId, classId, objectSubId, comment)`. Matches.
- `relation_close(relation, NoLock)` only `if relation != NULL`. Matches
  (`if let Some(rel)`).
- `errdetail_relkind_not_supported` is a real ported fn
  (`backend-catalog-pg-class`), called directly, not seamed.

### CreateComments / CreateSharedComments — MATCH
- Empty-string → NULL reduction (`comment != NULL && strlen==0`) ported as
  `reduce_empty` (`Some("") -> None`). Matches.
- `comment != NULL` array-fill branch: `nulls[i]=false, replaces[i]=true` for
  all attrs; `values[Anum-1]` = objoid/classoid/(objsubid)/description. Attr
  counts and 1-based Anums verified against catalog defs: pg_description has 4
  attrs (objoid=1, classoid=2, objsubid=3, description=4); pg_shdescription has
  3 (objoid=1, classoid=2, description=3). Port constants match exactly.
- Upsert structure: scan for one match; if found and comment NULL → delete; if
  found and comment non-NULL → modify+update; if not found and comment non-NULL
  → form+insert. The found-vs-not-found and delete-vs-upsert *decisions* are
  in-crate. The C's `newtuple == NULL` insert-guard is faithfully reproduced by
  the `None` arm (a match can never both update and insert). Matches.
- `table_open(...RowExclusiveLock)` / `table_close(...NoLock)`. Matches.

### DeleteComments — MATCH
- 3-vs-2 scan-key choice driven by `subid != 0` → ported as
  `objsubid: Option<i32>` (`Some` iff `subid != 0`). Delete-every-match loop
  behind `description_delete_all`. `table_open(RowExclusiveLock)` /
  `table_close(RowExclusiveLock)` (note: closes holding the lock, unlike
  CreateComments). Matches.

### DeleteSharedComments — MATCH
- Always 2 scan keys; delete-every-match; `table_close(RowExclusiveLock)`.
  Matches.

### GetComment — MATCH
- 3 scan keys; `comment = NULL`; first match → `heap_getattr(...description...,
  &isnull)`; `if !isnull` → `TextDatumGetCString`. `AccessShareLock` open and
  close. The `!isnull` branch and the single-match early stop are preserved.
  Returns `Option<String>` (C returns NULL-or-cstring). Matches.

## Seam audit

Owned seam crate: `backend-commands-comment-seams` (covers comment.c). All 28
declarations are installed by `backend_commands_comment::init_seams()` — except
that comment.c **owns no inward seam boundary**: nothing calls *into* comment.c
across a dependency cycle (its callers dependency.c/ruleutils.c are downstream).
`init_seams()` is correctly empty and is wired into `seams-init::init_all()`
(lib.rs:51). The seam crate holds only **outward** declarations, installed by
their real owners when they land. The `every_seam_installing_crate_is_wired_into
_init_all` recurrence guard passes.

Outward seams are all genuine cross-subsystem primitives the in-crate logic
marshals into and delegates to (mirror-PG-and-panic until owners land):
- objectaddress.c: `get_object_address`, `check_object_ownership`;
- dbcommands.c: `database_name` (`strVal` of opaque parser node);
- rel.h/relation.c: `relation_get_relkind`, `relation_get_relation_name`,
  `relation_close`;
- fmgr/varlena: `cstring_get_text_datum`, `text_datum_get_cstring`;
- pg_description/pg_shdescription catalog primitives (genam.c/heaptuple.c/
  indexing.c): `*_open`/`*_close`, `*_find_one`/`*_get_description`,
  `*_delete`/`*_update`/`*_insert`, `*_delete_all`.

The catalog-primitive seams carry only ScanKeyInit + systable index scan +
CatalogTuple{Delete,Update,Insert} + heap_{modify,form}_tuple — the
pg_description tuple ABI, which is not yet ported. None of comment.c's business
logic (empty→NULL reduction, found/not-found decision, delete-vs-upsert,
shared-vs-local routing, 3-vs-2 nkeys, relkind whitelist, !isnull) lives behind
a seam; all of it is in-crate. This is the standard composite-catalog-primitive
boundary, not body-replacement-by-seam.

## Design conformance
- No invented opacity: `DescriptionTupleId` wraps the real
  `ItemPointerData` (the C `&oldtuple->t_self`); relation crosses as its real
  `Oid` (relcache model). No stand-in handles.
- All allocating/erroring seams return `PgResult`. No shared statics, no
  ambient-global seams, no locks held across `?`.
- No `todo!()`/`unimplemented!()`, no own-logic stubs, no deferral-error stubs.

## Gates
- `cargo check --workspace`: PASS (warnings only, in unrelated crates).
- `cargo test -p backend-commands-comment`: PASS (17 passed).
- `cargo test -p seams-init`: PASS (2 passed, incl. recurrence guard).

## Verdict: PASS

Every function MATCH; zero seam findings; zero design-conformance findings.
