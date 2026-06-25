# Audit: backend-utils-adt-name (name.c)

Audited against `../pgrust/postgres-18.3/src/backend/utils/adt/name.c` and the
c2rust rendering. Status: PASS. Date 2026-06-15.

Model: `name` is pass-by-reference; its referent is the real
`types_tuple::heaptuple::NameData` (a `[u8; NAMEDATALEN]` NUL-padded fixed
buffer). Following the sibling `backend-utils-adt-char` port, these are plain
typed Rust functions — no fmgr/`Datum` marshalling layer. A `name` value
crosses as `&NameData`; a `cstring` as `&str`; binary I/O uses `StringInfo`.

## Function-by-function

Every C function defined in name.c is enumerated.

| C function | Status | Notes |
|---|---|---|
| `namein` | PASS | `len >= NAMEDATALEN` -> `pg_mbcliplen(s, len, NAMEDATALEN-1)` (mbutils seam, infallible i32); `palloc0` -> `NameData::default()` (all-zero); `memcpy(NameStr, s, len)`. Zero-pad preserved. |
| `nameout` | PASS | `pstrdup(NameStr(*s))` — `name_str()` (bytes to first NUL) into a `PgString` (UTF-8 lossy, ASCII-faithful). |
| `namerecv` | PASS | `pq_getmsgtext(buf, buf->len-buf->cursor, &nbytes)`; `nbytes >= NAMEDATALEN` -> `ereport(ERROR, ERRCODE_NAME_TOO_LONG, "identifier too long", errdetail "Identifier must be less than %d characters.")` (SQLSTATE 42622); `palloc0`+`memcpy`. `pfree(str)` is a no-op (owned). |
| `namesend` | PASS | `pq_begintypsend` + `pq_sendtext(NameStr, strlen)` + `pq_endtypsend` -> `Bytea`. |
| `namecmp` (static) | PASS | `collid == C_COLLATION_OID` (950) -> `strncmp(NameStr,NameStr,NAMEDATALEN)`; else `varstr_cmp(NameStr,strlen,NameStr,strlen,collid)`. strncmp helper compares as unsigned char, NUL/`n`-bounded. |
| `nameeq`/`namene`/`namelt`/`namele`/`namegt`/`namege` | PASS | `namecmp(...) {==,!=,<,<=,>,>=} 0`. Collation = `PG_GET_COLLATION()` -> passed-in `collid`. |
| `btnamecmp` | PASS | returns `namecmp(...)` int32. |
| `btnamesortsupport` | PASS | `varstr_sortsupport(ssup, NAMEOID, ssup->ssup_collation)`; the C MemoryContextSwitchTo(ssup_cxt) is modeled by varstr_sortsupport charging scratch to `ssup.ssup_cxt`. Returns the `VarStrSortSupport` decision (the comparator/abbrev function-pointer install is the sortsupport substrate, returned for caller dispatch — same shape as the sibling `bttextsortsupport`/`bytea_sortsupport` in varlena). |
| `namestrcpy` | PASS | delegates `NameData::namestrcpy` = `strncpy(NameStr, str, NAMEDATALEN)` + force trailing NUL + zero-pad (truncates source to NAMEDATALEN-1). |
| `namestrcmp` | PASS | both NULL -> 0; NULL name -> -1; NULL str -> 1 ("NULL < anything", verbatim); else `strncmp(NameStr, str, NAMEDATALEN)`. |
| `current_user` | PASS | `namein(GetUserNameFromId(GetUserId(), false))`; get_user_id + get_user_name_from_id(noerr=false) miscinit seams. noerr=false never returns None (raises ERROR, propagated via `?`); the `.expect` covers a contract-impossible None. |
| `session_user` | PASS | as current_user but `GetSessionUserId()`. |
| `current_schema` | PASS | `fetch_search_path(false)`; empty path (NIL) -> NULL (`Ok(None)`); `get_namespace_name(linitial_oid)`; missing namespace -> NULL; else `namein(nspname)`. `list_free` is a no-op (owned PgVec). |
| `current_schemas` | PASS | `fetch_search_path(PG_GETARG_BOOL(0))`; per-oid `get_namespace_name`, skip deleted (NULL); `namein` each; `construct_array_builtin(names, i, NAMEOID)` via the new `build_name_array` seam (NameData images). Result is the array varlena bytes (by-ref payload). |
| `nameconcatoid` | PASS | `snprintf(suffix, "_%u", oid)` = `format!("_{oid}")`; `namlen + suflen >= NAMEDATALEN` -> `pg_mbcliplen(NameStr, namlen, NAMEDATALEN-1-suflen)`; `palloc0` + `memcpy(name)` + `memcpy(suffix)`. Truncates name part, not suffix. |

## Constants / SQLSTATEs verified
- `NAMEDATALEN = 64`, `C_COLLATION_OID = 950`, `NAMEOID = 19` — from the typed
  `types_core`/`types_tuple` definitions.
- `ERRCODE_NAME_TOO_LONG = 42622`; errmsg "identifier too long", errdetail
  "Identifier must be less than 64 characters." — matches name.c verbatim.

## Cross-crate seams added (this change)
- `backend-catalog-namespace-seams::fetch_search_path` — installed by the
  namespace owner (already-ported `fetch_search_path`).
- `backend-utils-adt-arrayfuncs-seams::build_name_array` — installed by the
  arrayfuncs owner; builds a 1-D, no-null `NAMEOID` array varlena from
  `NameData` byte images, mirroring `construct_array`/`construct_md_array` for
  the fixed-length by-reference case (the existing `construct_array_builtin`
  Datum path cannot resolve a by-reference element from a bare pointer word —
  `datum_as_byte_window` is a stub — so a typed byte-image seam is the faithful
  bridge, matching the existing `build_text_array_nullable` precedent).

## residual_own_todos = 0
No todo!/unimplemented!. The only panics are: the contract-impossible-None
`.expect` in current_user/session_user, and the seam-and-panic boundaries for
the unported mbutils/lsyscache/miscinit owners (loud panic until they land).
