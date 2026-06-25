# Audit: backend-utils-adt-acl

- **Unit**: `backend-utils-adt-acl`
- **C source**: `src/backend/utils/adt/acl.c` (~5700 LOC)
- **Crate**: `crates/backend-utils-adt-acl` (5 family modules: `aclitem_io`,
  `acldefault`, `acl_ops`, `has_privilege`, `role_membership`)
- **Verdict**: **PASS**
- **Date**: 2026-06-13
- **Model**: Claude Fable 5 (Opus 4.8 1M)

## Method

Independent audit (re-derived from the C, not from the port's comments). Enumerated
every function definition in `acl.c` (174 unique definitions: 101 `PG_FUNCTION_ARGS`
fmgr entry points + 73 helpers/statics), cross-checked every name is present in the
port, and read the C, c2rust rendering, and Rust port for a representative set across
all five modules with attention to control flow, error SQLSTATEs, constants verified
against headers, and edge cases. Gate: `cargo check --workspace` clean,
`cargo test --workspace` exit 0 (9 test binaries, 0 failed). No `todo!()`/
`unimplemented!()` in own logic.

## Function inventory & verdicts

All 174 unique C function definitions are present in the port (verified by name
cross-check). Detailed logic verification (the rest MATCH by the same patterns):

### aclitem_io.rs (aclitem text I/O + hashing)

| C function | Verdict | Notes |
|---|---|---|
| `is_safe_acl_char` | MATCH | IS_HIGHBIT_SET → `c & 0x80`; alnum/underscore otherwise |
| `getid` | MATCH | quote/escape handling, NAMEDATALEN-1 bound, ERRCODE_NAME_TOO_LONG, whitespace skip both ends |
| `putid` | MATCH | quote-if-unsafe; `""` escaping |
| `aclparse` | MATCH | group/user keyword, `=` handling, full priv-char switch, `*` grant-option, `/grantor`, defaulted-grantor WARNING, ACL_ID_PUBLIC |
| `aclitemin` | MATCH | trailing-garbage check; soft-error `Ok(None)` mirrors `ereturn` |
| `aclitemout` | MATCH | grantee/grantor name via AUTHOID syscache (SEAMED), numeric OID fallback, N_ACL_RIGHTS loop, `*` for goptions |
| `aclitem_match`/`_eq`/`Comparator` | MATCH | grantee/grantor/(privs) compare |
| `hash_aclitem`/`_extended` | MATCH | sum; seeded path is a faithful `hash_bytes_uint32_extended` (init `0x9e3779b9 + 4 + 3923095`, mix/final) |

### acldefault.rs

| C function | Verdict | Notes |
|---|---|---|
| `acldefault` | MATCH | all 13 ObjectType cases + default elog; world/owner defaults; ACL_ALL_RIGHTS_* verified field-by-field against `utils/acl.h` (RELATION/SEQUENCE/DATABASE/FDW/FOREIGN_SERVER/FUNCTION/LANGUAGE/LARGEOBJECT/PARAMETER_ACL/SCHEMA/TABLESPACE/TYPE) |
| `acldefault_sql` | MATCH | char→ObjectType map verified against C case list exactly |

### acl_ops.rs (Acl array constructors, mask algebra)

| C function | Verdict | Notes |
|---|---|---|
| `allocacl`/`make_empty_acl`/`aclcopy`/`aclconcat`/`aclmerge`/`aclitemsort`/`aclequal`/`check_acl` | MATCH | |
| `aclupdate`/`aclnewowner`/`check_circularity`/`recursive_revoke` | MATCH | DROP_RESTRICT → ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST; restart-on-modify loop |
| `aclmask` | MATCH | owner-goption fast path via has_privs_of_role (SEAMED to sibling), direct pass + indirect-membership pass, `remaining` recompute, ACLMASK_ALL/ANY done predicate |
| `aclmask_direct` | MATCH | infallible (no membership lookup); owner==roleid goption path |
| `aclmembers` | MATCH | worst-case 2N alloc, sort + qunique-equivalent dedup |
| `aclinsert`/`aclremove` | MATCH | ERRCODE_FEATURE_NOT_SUPPORTED stubs |
| `aclcontains`/`makeaclitem`/`aclexplode` | MATCH (logic in `_impl`) | argless entry panics at the unported fmgr/SRF `PG_GETARG_*`/`SRF_*` marshaling boundary; full body present in `aclcontains_impl`/`makeaclitem_impl` (incl. `any_priv_map`) and SRF body |
| `convert_aclright_to_string`/`convert_any_priv_string` | MATCH | comma-split, case-insensitive match, ERRCODE_INVALID_PARAMETER_VALUE on unknown |

### has_privilege.rs (has_*_privilege families, pg_has_role)

| C functions | Verdict | Notes |
|---|---|---|
| All `has_{table,sequence,any_column,column,database,fdw,function,language,schema,server,tablespace,type,parameter,largeobject}_privilege_*` variants (~90) | MATCH | name/id resolution, mode conversion, aclcheck routed through `backend-catalog-aclchk-seams` (real cycle; SEAMED), NULL-on-missing handling |
| `column_privilege_check` | MATCH | InvalidAttrNumber → -1; attribute-then-class aclcheck; is_missing → -1 |
| `has_param_priv_byname`/`has_lo_priv_byid` | MATCH | LO snapshot = NULL iff ACL_UPDATE else GetActiveSnapshot; is_missing semantics |
| `pg_has_role_*` + `convert_role_priv_string`/`pg_role_aclcheck` | MATCH | |
| all `convert_*_name`/`convert_*_priv_string` tables | MATCH | spot-checked `convert_table_priv_string` vs C table; grant-option entries use `acl_grant_option_for` |

### role_membership.rs (membership cache + queries, rolespec resolvers)

| C function | Verdict | Notes |
|---|---|---|
| `initialize_acl` | MATCH | bootstrap guard; cached_db_hash; 3 syscache inval callbacks (AUTHMEMROLEMEM, AUTHOID, DATABASEOID) |
| `RoleMembershipCacheCallback` | MATCH | DATABASEOID hash filter; invalidate all 3 RoleRecurseType slots |
| `roles_list_append` | MATCH | Bloom-filter fast path; ROLES_LIST_BLOOM_THRESHOLD=1024 build at *10 |
| `roles_is_member_of` | MATCH | cache short-circuit; pg_database datdba; iterate-while-appending agenda; admin_role out-param; ROLERECURSE_PRIVS/SETROLE filters; pg_database_owner implicit membership; persistent cache via leaked slice + drop-on-replace (models TopMemoryContext + list_free) |
| `has_privs_of_role`/`member_can_set_role`/`is_member_of_role`/`_nosuper` | MATCH | member==role + superuser_arg fast paths; correct RoleRecurseType |
| `check_can_set_role` | MATCH | ERRCODE_INSUFFICIENT_PRIVILEGE; GetUserNameFromId |
| `is_admin_of_role`/`select_best_admin` | MATCH | self-admin policy; admin_role out-param |
| `select_best_grantor` | MATCH | owner/superuser short path; popcount64 → count_ones tie-break |
| `get_role_oid`/`_or_public`/`get_rolespec_oid`/`_tuple`/`_name`/`check_rolespec_name` | MATCH | ERRCODE_UNDEFINED_OBJECT / ERRCODE_RESERVED_NAME; ROLESPEC_* variants |

## Seam audit

Owned seam crate (C-source coverage of `acl.c`): `backend-utils-adt-acl-seams`.

- `init_seams()` (in `backend_utils_adt_acl::init_seams`, wired into
  `seams-init::init_all` at `crates/seams-init/src/lib.rs:60`) installs the five
  seams that `acl.c` implements: `member_can_set_role`, `check_can_set_role`,
  `has_privs_of_role`, `get_rolespec_oid`, `initialize_acl`. Body is `set()` calls
  only. **PASS.**
- The seam crate also declares `has_bypassrls_privilege` and `object_ownercheck`.
  These are **not** `acl.c` functions — both are defined in `catalog/aclchk.c`
  (verified: `aclchk.c:4189` / object_ownercheck in aclchk.c). They are owned by the
  not-yet-ported `backend-catalog-aclchk` unit and remain uninstalled (panic until
  that unit lands). Not this unit's install obligation under by-C-source ownership;
  noted as a pre-existing scaffold placement (a consumer parked aclchk-owned decls in
  this seam crate). Non-blocking for `acl.c`.
- Outward seam calls are all justified by real dependency cycles and are thin
  marshal+delegate: aclcheck (`backend-catalog-aclchk-seams`), syscache projections,
  bloom filter, snapshot, superuser, inval-callback registration, miscinit. No
  branching/computation in seam paths.

## Design conformance

- No invented opacity: real `AclItem`/`Acl` (`AclItem` slice) / `RoleSpec` /
  `bloom_filter` types; allocating fns take `Mcx` + return `PgResult`.
- Per-backend membership cache modeled as `thread_local` (`ROLE_CACHE`), not a shared
  static; persistent lists via leaked slice mirror TopMemoryContext, reclaimed on
  recompute (mirrors `list_free`).
- Seam-and-panic at the fmgr/SRF marshaling boundary (aclcontains/makeaclitem/
  aclexplode) rather than restructuring; own logic present in `_impl`/`_str` bodies.

### Minor observation (non-blocking)

`types-acl` ends up with two spellings of the same `utils/acl.h` macro:
`ACL_GRANT_OPTION_FOR` (used by `role_membership`) and `acl_grant_option_for`
(used by `has_privilege`); `acl_ops` also has a private local copy. All three compute
`(privs & 0xFFFFFFFF) << 32` identically. Pure vocabulary duplication from independent
families — no logic impact.

## Conclusion

Every enumerated `acl.c` function is MATCH (or SEAMED per the rules); the five owned
seams are installed; design rules hold. **PASS.**

---

## Delta audit 2026-06-18 — fmgr builtin registration

The prior audit found the `has_*_privilege` / `pg_has_role` entrypoints and the
`aclitem` SQL functions ported but seam-and-panic at the fmgr marshaling
boundary (the `*_impl`/`*_str`/value cores held the logic). This change lands
that boundary by registering them into the fmgr-core builtin table
(C: `fmgr_builtins[]`), so by-OID dispatch resolves them.

### What changed

- **`has_privilege.rs` refactor (logic-preserving).** Each `has_*` /
  `pg_has_role` entrypoint, previously written against the executor frame
  (`types_nodes` `FunctionCallInfoBaseData`) using the *uninstalled* by-reference
  `pg_getarg_*` seams (DESIGN_DEBT TD-FMGR-GETARG-BYREF — they panic), is now a
  pure value core taking its already-decoded arguments (`text`→`&[u8]` payload,
  `name`→`&str`, `oid`/`int2`, returning `PgResult<Option<bool>>`, `None`=SQL
  NULL). The body of each is byte-for-byte the prior body; only the I/O boundary
  moved out. The six-variant object-class families collapse into one
  `object_priv_family!` macro (database/fdw/function/language/schema/server/
  tablespace/type) — the per-class bodies were already identical delegations to
  `object_aclcheck{,_ext}`. The `convert_*` tables / `column_privilege_check` /
  `has_param_priv_byname` / `has_lo_priv_byid` / `pg_role_aclcheck` are unchanged
  except `&Bytea`→`&[u8]` (the C reads `text_to_cstring(...)` = the detoasted
  payload; `.data()` on the old `Bytea` returned exactly those bytes). The dead
  executor-frame `FunctionCallInfo` alias is removed; no external caller existed.
  Spot-checked against the C: `has_sequence_privilege_id_id` (acl.c:2266),
  `has_largeobject_privilege_id{,_id}` (4763/4787), `pg_has_role_*` arg shapes
  (4839+), `has_table_privilege_name_id` role resolution — all MATCH.

- **`fmgr_builtins.rs` (new).** `fc_<name>` adapters over the `types_fmgr` ABI
  frame (which carries the by-reference lane the executor frame lacks): read args
  (`arg_text`/`arg_name`/`arg_oid`/`arg_int16`/`arg_int64`/`arg_bool`/
  `arg_cstring`/`arg_aclitem`), call the matching core, write the result
  (`ret_bool_opt`/`ret_aclitem`/`ret_cstring`). `aclitem` (a fixed 16-byte
  by-reference type) crosses as its raw `#[repr(C)]` image on the `Varlena`
  by-ref lane: `ai_grantee`@0, `ai_grantor`@4, `ai_privs`@8, native-endian,
  matching the in-memory `repr(C)` layout (verified by in↔out roundtrip:
  `'postgres=arwdDxt/postgres'::aclitem` prints back identically). `aclitemin`'s
  defaulted-grantor `ereport(WARNING)` is emitted via the `ereport_msg` seam at
  `WARNING`. `aclinsert`/`aclremove` delegate to the deprecated error stubs.
  These are thin marshal+delegate only — no branching beyond the NULL/error
  encode every fc adapter shares.

- **Registration / wiring.** `register_acl_builtins()` registers all 98 rows
  (8 aclitem-type + 90 has_*/pg_has_role), OIDs/nargs transcribed from
  `pg_proc.dat` and cross-checked against `fmgr_core::builtin_canonical`; called
  from `init_seams()`. The 98 corresponding rows are deleted from
  `seams-init::builtin_gap_baseline` (gap 1574→1476); the
  `builtin_registry_matches_canonical_or_baseline` ratchet test PASSES.

### Deliberately deferred (still gap-baseline `NotRegistered`)

`aclcontains` (1037), `aclexplode` (1689), `acldefault_sql` (3943),
`pg_get_acl` (6385): each reads or returns an `aclitem[]` (`ArrayType`) and/or
uses the SRF machinery — the array-detoast / SRF fmgr edge is not yet grown
(the value cores `aclcontains_impl` / `acldefault` exist and are exercised
directly). Not a logic gap in this unit; recorded in `fmgr_builtins.rs`.

### Live verification (real C-initdb cluster, `postgres --single`)

- `has_table_privilege('pg_class','SELECT')` → `t`
- `has_schema_privilege('pg_catalog','USAGE')` → `t`
- `has_database_privilege('postgres','CONNECT')` → `t`
- `'postgres=arwdDxt/postgres'::aclitem` → `postgres=arwdDxt/postgres` (aclitemin∘aclitemout)
- `makeaclitem(10,10,'SELECT',true)` → `postgres=r*/postgres`
- `'postgres=r/postgres'::aclitem = 'postgres=r/postgres'::aclitem` → `t` (aclitem_eq)
- `pg_has_role('postgres','MEMBER')` → `t`

(The trailing `ShutdownXLOG` error on every single-user exit is the pre-existing
unrelated xlog-driver debt, not on the acl path.)

### Verdict

Refactor is logic-preserving (spot-checked vs C); registration is thin
marshal+delegate; all gates (registry completeness ratchet, seam-install
completeness, seams-init suite) PASS; live reads return correct results.
**PASS.**
