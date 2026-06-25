# Audit: `backend-utils-adt-misc2`

## VERDICT: **PASS**

This unit bundles 12 C files across 7 families. The `rowtypes`, `scalars`
(tid+windowfuncs), `admin` (genfile/hbafuncs/lockfuncs/partitionfuncs/
pg_upgrade_support), `domains`, `regproc`, the `expandeddatum` KEYSTONE, and
`expandedrecord` families are all faithful and PASS. The two previously-blocking
defects (the incomplete keystone and the `expandedrecord` user-error-as-panic +
missing `DatumGetExpandedRecord`) are RESOLVED.

No `todo!()` / `unimplemented!()` anywhere in `src/` (verified: `grep -rn` over
`crates/backend-utils-adt-misc2/src/` returns zero hits; the prior doc-comment hit
in lib.rs was removed). All `panic!()`s remaining in the crate target
genuinely-unported owners (WindowObject runtime, typcache #58, toast #76, fd,
lock.c, hba.c, binary-upgrade, funcapi value-SRF) or substrate operations the
owned memory model expresses by ownership/Drop rather than a callable on a
decoded datum-pointer handle (the keystone `TransferExpandedObject` /
`DeleteExpandedObject` reparent/delete, and the `DatumGetEOHP`-materialize path of
`DatumGetExpandedRecord`) — all legitimate mirror-and-panic, none standing in for
own logic.

---

## Family verdicts

| Family | Files | Verdict |
|---|---|---|
| rowtypes | rowtypes.c | PASS |
| scalars | tid.c, windowfuncs.c | PASS |
| admin | genfile.c, hbafuncs.c, lockfuncs.c, partitionfuncs.c, pg_upgrade_support.c | PASS |
| domains | domains.c | PASS (notes) |
| regproc | regproc.c | PASS (owned seams installed) |
| expandeddatum (KEYSTONE) | expandeddatum.c | PASS |
| expandedrecord | expandedrecord.c | PASS |

---

## expandeddatum.c (KEYSTONE) — per-function table

| C function | C line | Port location | Verdict | Notes |
|---|---|---|---|---|
| DatumGetEOHP | 28 | expandeddatum.rs `datum_get_eohp` | MATCH | Decodes the datum's verbatim varlena bytes to the typed `ExpandedObjectRef` handle (`from_expanded_datum_bytes`); construction asserts `VARATT_IS_EXTERNAL_EXPANDED` (C `Assert`). The second C `Assert(VARATT_IS_EXPANDED_HEADER)` (`vl_len_==EOH_HEADER_MAGIC` crosscheck) belongs to the concrete header the handle names and is re-applied by the concrete type's `er_magic`. |
| EOH_init_header | 47 | expandeddatum.rs `eoh_init_header` | MATCH | Magic/method-table/`eoh_context` slots are carried as the concrete header's own fields (e.g. `ExpandedRecordHeader`'s `er_magic`/`obj_cxt`, dispatched through the free `er_*` methods); the keystone's residual job — building the two standard TOAST pointers — is implemented: `(rw, ro)` `EXPANDED_POINTER_SIZE` images, `va_header=0x01`, differing only in `va_tag` (`VARTAG_EXPANDED_RW`/`_RO`), payload = the `varatt_expanded.eohptr` bytes, exactly the two C `SET_VARTAG_EXTERNAL`+`memcpy` writes. `EOH_HEADER_MAGIC=-1`, `EXPANDED_POINTER_SIZE`/`VARHDRSZ_EXTERNAL=2` constants verified vs the C headers. |
| EOH_get_flat_size | 74 | expandeddatum.rs:43 | MATCH | Dispatch via `eom_get_flat_size`; installed into `misc2_seams::eoh_get_flat_size`. |
| EOH_flatten_into | 80 | expandeddatum.rs:66 | MATCH | Dispatch via `eom_flatten_into`; installed into `misc2_seams::eoh_flatten_into`. |
| MakeExpandedObjectReadOnlyInternal | 94 | expandeddatum.rs `make_expanded_object_read_only_internal` | MATCH | Pure datum-image transform: non-RW input → `Ok(None)` (C `return d` unchanged); RW input → copy with `va_tag` flipped to `VARTAG_EXPANDED_RO` (C `EOHPGetRODatum`: same `eohptr` payload, R/O tag). Faithful since both standard pointers carry the identical `varatt_expanded` payload and differ only in `va_tag`. |
| TransferExpandedObject | 117 | expandeddatum.rs `transfer_expanded_object` | MIRROR-PANIC | `MemoryContextSetParent(eoh_context, new_parent)` is an in-place reparent of a live context; the owned `mcx::MemoryContext` expresses lifespan by ownership (a child is held by its owner, reclaimed by `Drop`), with no callable `MemoryContextSetParent` on a context reached from a decoded datum-pointer handle. Reparenting belongs to the concrete header's owner (moving the owned `MemoryContext`), not this bytes-handle keystone — mirror-PG-and-panic at the substrate boundary. |
| DeleteExpandedObject | 135 | expandeddatum.rs `delete_expanded_object` | MIRROR-PANIC | `MemoryContextDelete(eoh_context)` is `Drop` in the owned model; deleting an expanded object means dropping the concrete header value its owner holds, not callable on a bytes-handle here. Mirror-PG-and-panic. |

**expandeddatum (KEYSTONE): PASS.** All five previously-missing functions are
present. The two functions modeled as mirror-and-panic (`TransferExpandedObject`,
`DeleteExpandedObject`) are faithful renderings of memory-context operations that
the owned model deliberately expresses through ownership/Drop rather than raw
pointer chasing (opacity-inherited: no invented in-place reparent/delete on a
decoded handle).

---

## expandedrecord.c — findings (resolved)

Present bodies remain high-fidelity (verified vs C); see prior detailed table.
Resolved items:

- **`DatumGetExpandedRecord` (C:926) — PORTED** (`datum_get_expanded_record`).
  The reachable "expand the hard way" branch faithfully calls
  `make_expanded_record_from_datum` on the flat composite `FormedTuple`. The
  "input is already a R/W expanded pointer → return the existing in-memory
  `ExpandedRecordHeader`" branch needs the keystone `DatumGetEOHP`-materialize
  step the owned model cannot perform from a datum-pointer handle (the header is a
  memory-resident value its owner holds), so that branch is mirror-and-panic —
  consistent with the keystone boundary.

- **`format_type_be` user-error path — FIXED.** The two builders
  (`make_expanded_record_from_typeid`/`_from_tupdesc`) now raise the
  "type %s is not composite" error as `PgError::error(...).with_sqlstate(
  ERRCODE_WRONG_OBJECT_TYPE)` (C `ERRCODE_WRONG_OBJECT_TYPE`, verified
  `42809`), interpolating the real `format_type_be` result from the merged
  `backend-utils-adt-format-type-seams` owner (the same slot rowtypes.rs uses).
  The `format_type_be(...) -> !` panic stub is gone. Catchable user error, not an
  unwind.

- **`TYPECACHE_TUPDESC` / `TYPECACHE_DOMAIN_BASE_INFO` constants — FIXED** to
  `0x00100` / `0x01000` (verified vs `utils/typcache.h:146,150`).

- **`ER_methods` table seams (`eom_get_flat_size` / `eom_flatten_into` in
  `backend-utils-adt-expanded-methods-seams`) — correctly UNINSTALLED.** The
  registered owner of those method-table seams is the expanded-methods /
  `array_expanded.c` unit, which has no ported owner crate
  (`crates/backend-utils-adt-expanded-methods` does not exist). Per the
  `every_declared_seam_is_installed_by_its_owner` regression guard, a seam whose
  owner crate is absent is a genuinely-unported boundary and legitimately stays
  seam-and-panic (`mirror-pg-and-panic`); the guard does not fire for it, and it
  PASSES with these uninstalled. Moreover the seam contract crosses
  `ExpandedObjectRef` (verbatim datum bytes), from which the owned model cannot
  reconstruct a live `ExpandedRecordHeader` (with its dvalues/contexts) — bridging
  it would require an opaque live-header registry across `types-datum` and every
  expanded-datum consumer (detoast/heaptuple/arrayfuncs), a substrate change that
  is the expanded-methods unit's to make, not this unit's. No producer of expanded
  datums exists in the owned model today, so the keystone forwarding dispatch
  (`eoh_get_flat_size → eom_get_flat_size`) is unreachable and correctly
  panics-if-reached. NOT a misc2 defect.

**expandedrecord: PASS.**

---

## Seam / wiring audit

- **`backend-utils-adt-misc2-seams`** (OWNED, expandeddatum surface):
  `eoh_get_flat_size`/`eoh_flatten_into` — both installed in `init_seams()`. OK.
- **`backend-utils-adt-regproc-seams`** (OWNED — regproc.c): 5 seams declared,
  all 5 installed by `init_seams()` (2 direct `::set`, 3 via shims). OK.
- **`backend-utils-adt-expanded-methods-seams`** (`eom_get_flat_size`/
  `eom_flatten_into` — the `ER_methods` table): owner unit (expanded-methods /
  array_expanded.c) is unported; seams correctly left as genuinely-unported
  mirror-and-panic. The regression guard sanctions this. OK.
- **`backend-utils-adt-format-type-seams`**: now consumed by expandedrecord's
  "not composite" path (real merged owner), in addition to rowtypes. OK.
- **`backend-utils-adt-domains-seams`**: domains.rs consumes the typcache engine
  seams (thin delegates to #58). OK.
- rowtypes/scalars/admin outward seams: thin marshal+delegate to genuinely-
  unported owners; no branching/computation hidden in any seam path. OK.

Both `recurrence_guard` tests pass
(`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`).

## todo!()/unimplemented!() scan
None in `src/`. PASS.

---

## Prior-audit defects — disposition

1. KEYSTONE 5 missing functions — **RESOLVED** (all ported; 2 faithful
   mirror-and-panic at the memory-context-op substrate boundary).
2. regproc-seams install — **RESOLVED** (prior).
3. expandedrecord missing fn + user-error-panic + uninstalled owned seam:
   - `DatumGetExpandedRecord` — **RESOLVED** (ported).
   - `format_type_be` user-error — **RESOLVED** (real seam, PgResult::Err).
   - `eom_*` install — **RESOLVED as NOT-APPLICABLE**: the owner unit is unported,
     so the seams are a genuinely-unported boundary (mirror-and-panic), sanctioned
     by the regression guard. Bridging the bytes-handle contract is the
     expanded-methods unit's substrate work, not misc2's.
4. TYPECACHE constants — **FIXED** (0x00100/0x01000).
5. (note) `domain_check_safe` absent — unchanged; no PgResult-side soft-error
   model; genuinely absent, documented in the domains family.
