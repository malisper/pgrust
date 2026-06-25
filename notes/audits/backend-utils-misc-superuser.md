# Audit: backend-utils-misc-superuser

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Branch:** `port/backend-utils-misc-superuser`
- **Unit C source:** `src/backend/utils/misc/superuser.c` (107 lines)
- **Port location:** `crates/backend-utils-misc-more/src/superuser.rs` (the unit's
  superuser.c logic was ported as part of `backend-utils-misc-more`; this branch
  wires the inward seams).

This is an independent, function-by-function re-derivation from the C source and
the c2rust rendering (`pgrust/c2rust-runs/backend-utils-misc-more/src/superuser.rs`).

## Function inventory & verdicts

Every function defined in `superuser.c`:

| C function | C location | Port location | Verdict | Notes |
|---|---|---|---|---|
| `superuser(void)` | superuser.c:45-49 | superuser.rs:30-32 | MATCH | `superuser_arg(get_user_id::call())`. `GetUserId()` delegated outward to `backend-utils-init-miscinit-seams::get_user_id` (real dep: user id is per-backend session state owned by miscinit). |
| `superuser_arg(Oid)` | superuser.c:55-96 | superuser.rs:35-62 | MATCH | All five behavioral steps reproduced in-crate (see detail below). |
| `RoleidCallback(Datum,int,uint32)` | superuser.c:102-107 (static) | superuser.rs:66-68 | MATCH | Sets `LAST_ROLEID = InvalidOid`; args ignored, exactly as C. Registered as the syscache callback function pointer. |
| `OidIsValid` (macro, inlined) | c.h | superuser.rs:71-74 | MATCH | `object_id != INVALID_OID`. |

### `superuser_arg` detail (logic parity)

1. **Cache hit** — C: `if (OidIsValid(last_roleid) && last_roleid == roleid) return last_roleid_is_super;`. Rust: identical guard over `LAST_ROLEID`/`LAST_ROLEID_IS_SUPER`. MATCH.
2. **Escape path** — C: `if (!IsUnderPostmaster && roleid == BOOTSTRAP_SUPERUSERID) return true;`. Rust: `if !IsUnderPostmaster() && roleid == BOOTSTRAP_SUPERUSERID { return Ok(true); }`. `IsUnderPostmaster` read via `backend-utils-init-small::globals::IsUnderPostmaster()` (per-backend global). MATCH.
3. **Catalog lookup** — C: `SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid))` → on `HeapTupleIsValid`, read `Form_pg_authid->rolsuper` + `ReleaseSysCache`; else `result = false`. Rust: `search_authid_rolsuper::call(roleid)?.unwrap_or(false)` — the `Option::None` case (cache miss / `!HeapTupleIsValid`) maps to `false`, matching C exactly. The syscache search + `GETSTRUCT->rolsuper` projection + `ReleaseSysCache` are owned by the syscache unit (outward seam, see below). MATCH.
4. **Callback registration** — C: first-time-only `CacheRegisterSyscacheCallback(AUTHOID, RoleidCallback, (Datum)0)` guarded by `roleid_callback_registered`. Rust: `if !ROLEID_CALLBACK_REGISTERED ... inval_seam::call(AUTHOID, RoleidCallback, Datum::null())?; set(true)`. Guard, AUTHOID, fn pointer, zero Datum all match. MATCH.
5. **Cache store** — C: `last_roleid = roleid; last_roleid_is_super = result;`. Rust: identical. MATCH.

## Constants (verified against headers/types crates, not memory)

| Constant | C value | Rust source | OK |
|---|---|---|---|
| `AUTHOID` | 11 (`SysCacheIdentifier`, c2rust:112) | `types-syscache::AUTHOID = 11` | ✓ |
| `BOOTSTRAP_SUPERUSERID` | 10 (`pg_authid_d.h`) | `types-core::catalog::BOOTSTRAP_SUPERUSERID = 10` | ✓ |
| `InvalidOid` | 0 | `types-core::INVALID_OID = InvalidOid = 0` | ✓ |

## Per-backend state

`last_roleid`, `last_roleid_is_super`, `roleid_callback_registered` are C `static`
(per-backend) globals. The port renders them as `thread_local!` `Cell`s — correct;
no shared statics for per-backend globals (design rule satisfied).

## Seam audit

**Owned seam crates for this unit (by C-source coverage of superuser.c):**
`crates/backend-utils-misc-superuser-seams` only. It declares:
- `superuser() -> PgResult<bool>`
- `superuser_arg(Oid) -> PgResult<bool>`

Both are installed by `backend_utils_misc_more::init_seams()`
(`crates/backend-utils-misc-more/src/lib.rs:48-49`), and that `init_seams()` is
registered in `seams-init::init_all()` (`crates/seams-init/src/lib.rs:71`). No
owned seam declaration is left uninstalled. ✓

`PgResult` return type is justified: the lookup path can `ereport(ERROR)` through
the syscache, so the failure surface is carried on `Err` (seam-signatures rule). ✓

**Outward seams consumed by the port** (each a real cross-unit dependency, thin
delegate, declared/installed by the owning unit — not this unit's concern):
- `backend-utils-init-miscinit-seams::get_user_id` — `GetUserId()`, per-backend session id owned by miscinit.
- `backend-utils-cache-inval-seams::cache_register_syscache_callback` — `CacheRegisterSyscacheCallback`, owned by cache/inval.
- `backend-utils-cache-syscache-seams::search_authid_rolsuper` — the `SearchSysCache1(AUTHOID)` + `GETSTRUCT->rolsuper` + `ReleaseSysCache` projection, owned by the syscache unit. The in-crate side is pure marshalling (`?.unwrap_or(false)`); no branching/computation hidden in the port's seam use.

No function body was replaced by a seam call to "elsewhere" — all of superuser.c's
own control flow (cache hit, escape path, false-on-miss, register-once, store)
lives in this crate. ✓

## Design conformance (§3b)

- No invented opacity: uses real `Oid`/`Datum` types (types.md rules 6-7). ✓
- No allocation in this unit → no `Mcx` needed; `PgResult` present where the C can error. ✓
- Per-backend globals are `thread_local`, not shared statics. ✓
- No ambient-global seams, no locks across `?`, no registry-shaped side tables. ✓
- No unledgered divergence markers. ✓

## Result

All 3 C functions (+ inlined `OidIsValid`) **MATCH**. Zero seam findings. Zero
design-conformance findings. Build: `cargo build -p backend-utils-misc-more`
succeeds. **PASS.**
