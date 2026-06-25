# Audit: backend-commands-define

**Verdict: PASS**
**Date:** 2026-06-13
**Model:** Claude Opus 4.8 (1M context)

Unit: `backend-commands-define` — C source `src/backend/commands/define.c`
(support routines for `DefElem` nodes). Audited independently against the C
source, the c2rust rendering (`c2rust-runs/backend-commands-define/src/define.rs`),
and the Rust port (`crates/backend-commands-define`,
`crates/backend-commands-define-seams`).

## 1. Function inventory

define.c defines exactly 10 top-level functions, no statics/inline helpers.
The c2rust run lists the same 10 (`#[no_mangle]` exports) plus the standard
header-derived inline shims (`DatumGetObjectId`/`CStringGetDatum`/etc.). Every
function gets a row.

| # | C function | C loc | Port loc | Verdict | Notes |
|---|-----------|-------|----------|---------|-------|
| 1 | `defGetString` | define.c:34-62 | lib.rs:42-65 | MATCH | nodeTag switch over owned `Node`; Integer→`ival.to_string()` (C `psprintf("%ld",(long)intVal)`, identical decimal), Float→`fval`, Boolean→"true"/"false", String→`sval`, TypeName→`typename_to_string_node` seam, List→`NameListToString` (direct), A_Star→"*", default→`unrecognized node type`. Allocates in `mcx`. |
| 2 | `defGetNumeric` | define.c:67-88 | lib.rs:69-79 | MATCH | NULL→"requires a numeric value"; Integer→`as f64`, Float→`floatVal`(=`atof`), default→same syntax error. |
| 3 | `defGetBoolean` | define.c:93-143 | lib.rs:82-115 | MATCH | NULL→`Ok(true)`; Integer 0→false,1→true,else fallthrough; non-Integer→`defGetString`-equivalent text then case-insensitive true/false/on/off; else "requires a Boolean value". See §2 note on the text helper. |
| 4 | `defGetInt32` | define.c:148-167 | lib.rs:118-126 | MATCH | NULL/default→"requires an integer value"; Integer→`ival` (C `(int32)intVal`). |
| 5 | `defGetInt64` | define.c:172-200 | lib.rs:129-141 | MATCH | Integer→`as i64`; Float→`int8in`=`pg_strtoint64` (hard error, see §2); default/NULL→"requires a numeric value". |
| 6 | `defGetObjectId` | define.c:205-233 | lib.rs:144-156 | MATCH | Integer→`as Oid` (C `(Oid)intVal`); Float→`oidin`=`uint32in_subr(s,false,"oid",None)` (see §2); default/NULL→"requires a numeric value". |
| 7 | `defGetQualifiedName` | define.c:238-262 | lib.rs:160-172 | MATCH | TypeName→`t.names`, List→cells, String→`list_make1`, default→"argument of %s must be a name". |
| 8 | `defGetTypeName` | define.c:270-292 | lib.rs:178-191 | MATCH | TypeName→clone, String→`makeTypeNameFromNameList` seam over `list_make1`, default→"argument of %s must be a type name". List intentionally not accepted (matches C). |
| 9 | `defGetTypeLength` | define.c:298-337 | lib.rs:196-233 | MATCH | Integer→`ival`; Float→"requires an integer value"; String "variable"→-1 else fallthrough; TypeName render=="variable"→-1 else fallthrough; List→fallthrough; default→unrecognized; trailing `ereport("invalid argument for %s: \"%s\"", defGetString(def))`. Fallthrough/error ordering preserved. |
| 10 | `errorConflictingDefElem` | define.c:370-377 | lib.rs:259-266 | MATCH | `parser_errposition` seam → `ereport(ERROR, ERRCODE_SYNTAX_ERROR, "conflicting or redundant options", errposition)`. |

## 2. Spot-check of load-bearing details

- **`%ld`/`intVal` width:** C `intVal` reads `Integer.ival` (`int`/i32). Port
  field `ival` is i32; `(long)`-cast `%ld` and `i32::to_string()` produce
  identical decimal text. MATCH.
- **`int8in` (defGetInt64 Float):** C `int8in` body is
  `pg_strtoint64_safe(num, fcinfo->context)`; reached via
  `DirectFunctionCall1` which builds an fcinfo with `context == NULL`, so the
  soft-error context is NULL → hard `ereport` on bad input. Port calls
  `pg_strtoint64` (the hard-error wrapper). MATCH.
- **`oidin` (defGetObjectId Float):** C `oidin` = `uint32in_subr(s, NULL,
  "oid", fcinfo->context)`; `context` NULL via DirectFunctionCall1. Port
  `uint32in_subr(s, false /* endloc NULL */, "oid", None /* escontext */)`.
  `false` endloc reproduces C `endloc == NULL` (full-string + trailing-garbage
  check); `None` escontext reproduces hard error. MATCH.
- **`floatVal`/`atof`:** `floatVal(v)` = `atof(Float->fval)`. The port's `atof`
  shrinks the prefix to the longest parseable `f64`, returning 0.0 when no
  valid prefix exists — C `atof` semantics (no error). Used only by
  defGetNumeric. MATCH.
- **`defGetBoolean` non-Integer text (lib.rs:382-392 `def_get_string_text`):**
  C calls full `defGetString(def)`, which for TypeName/List would invoke the
  context-needing renderers. defGetBoolean is only reachable for a boolean
  DefElem, whose grammar (`opt_boolean_or_string`) restricts the value node to
  Integer/String/TRUE_P(Boolean)/FALSE_P; the structural TypeName/List/A_Star
  forms cannot occur. Even if one did, its rendered text could never equal a
  boolean keyword, so C falls to the "requires a Boolean value" error; the
  port returns empty text for those forms and lands on the identical error.
  Behaviorally equivalent on every reachable input. MATCH.
- **`defGetStringList` cell validation:** C `IsA(str, String)` loop →
  port `cell.as_string().is_none()` check with the same
  "unexpected node type in name list" elog. Non-List arg → "unrecognized node
  type" (C `nodeTag(def->arg) != T_List`). MATCH.
- **`pg_strcasecmp`:** in-crate helper does ASCII case-insensitive equality
  (length-prefixed); the only define.c use is exact keyword comparison. MATCH.

## 3. Seam and wiring audit

**Owned seam crate (by C-source coverage):** `define.c` maps to exactly one
seam crate, `crates/backend-commands-define-seams`. It declares
`def_get_string` and `def_get_boolean` (both `PgResult` — they `ereport`
ERRCODE_SYNTAX_ERROR; `def_get_string` takes `Mcx<'mcx>` as it palloc's). Both
are installed by `backend_commands_define::init_seams()` (lib.rs:335-338), which
contains nothing but two `set()` calls. `seams-init::init_all` calls
`init_seams()` (seams-init/src/lib.rs:24). No uninstalled declaration; no `set()`
outside the owner. PASS.

The seam *implementations* `seam_def_get_string`/`seam_def_get_boolean` live in
the owning crate (correct — the nodeTag logic belongs in a crate, and it is in
the owner) and operate over the `DefElemArg` projection the caller marshals.
The seam declarations themselves are thin. No logic in the seam-crate path.

**Outward seam calls (justified cross-unit cycles, each thin marshal+delegate):**
- `typename_to_string_node` / parse-type-seams — `TypeNameToString`
  (parse_type.c, unported). Not in this unit's c_sources.
- `make_type_name_from_name_list` / makefuncs-seams — `makeTypeNameFromNameList`
  (makefuncs.c, unported). Not in this unit's c_sources.
- `parser_errposition` / parser-small1-seams — `parser_errposition`
  (parse_node.c, unported). Not in this unit's c_sources.
- `NameListToString` (backend-catalog-namespace) and `pg_strtoint64`/
  `uint32in_subr` (backend-utils-adt-numutils) are direct calls (those units are
  ported) — no seam needed.

No function body was replaced by a seam-to-elsewhere; all 10 bodies live here.

## 3b. Design conformance

- Allocating functions take `Mcx` and return `PgResult` (`defGetString`,
  `defGetTypeLength` error path, `seam_def_get_string`). Conforms.
- No invented opacity: operates on the real `types_parsenodes::Node` tree
  (Integer/Float/Boolean/String/TypeName/List/A_Star); the `DefElemArg`
  projection is the documented value-node projection for the cycle-breaking
  seam, not a stand-in handle. Conforms (types.md 6-7).
- No shared statics, no ambient-global seams, no locks across `?`, no registry
  side tables, no unledgered divergence markers. Error severities/SQLSTATEs
  match (all ERRCODE_SYNTAX_ERROR + the two `elog(ERROR)` unrecognized/unexpected
  node-type internal errors). Conforms.

## 4. Result

All 10 functions MATCH. Owned seam crate fully installed; zero seam findings;
zero design-conformance findings. Build + 11 unit tests pass
(`cargo test -p backend-commands-define`).

**PASS.**
