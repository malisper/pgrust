# Adjudication package: IsSystemClass temp-toast arm (aclcheck lane)

Proof lane aclcheck, 2026-07-30. Divergence candidate #A1 from the WAVE 9
runqueue (was UNRESOLVED when the previous runner died mid-queue). Evidence
package only — options below, NO ruling.

## Claim

`has_table_privilege` (and every route through
`aclchk::pg_class_aclmask_ext`) diverges from C PostgreSQL for **write-class
privileges on the current session's own temp TOAST tables**: C denies them
to non-superusers (system-catalog write strip), pgrust grants them.

## Mechanism

C REL_18_STABLE, aclchk.c `pg_class_aclmask_ext`:

    if ((mask & (ACL_INSERT|ACL_UPDATE|ACL_DELETE|ACL_TRUNCATE|ACL_USAGE)) &&
        IsSystemClass(table_oid, classForm) && ... && !superuser_arg(roleid))
        mask &= ~(...);

with catalog.c `IsSystemClass -> IsToastClass -> IsToastNamespace`:

    return (namespaceId == PG_TOAST_NAMESPACE) ||
        isTempToastNamespace(namespaceId);

`isTempToastNamespace` (namespace.c) is TRUE only for the CURRENT session's
own temp toast namespace (`myTempToastNamespace`), so the C strip is
**session-local state**, not a pure function of the catalog row.

pgrust `crates/backend/catalog/aclchk/src/lib.rs:401-405` inlines a reduced
IsSystemClass with a stale comment ("IsSystemClass (catalog.c, unported)"):

    let is_system_class =
        relnamespace == PG_TOAST_NAMESPACE || table_oid < FirstUnpinnedObjectId;

— no temp-toast arm. NOTE the comment is stale two ways: catalog.c IS now
ported, and `catalog::IsSystemClass`
(crates/backend/catalog/catalog/src/lib.rs:76) DOES carry the arm via
`namespace_seams::is_temp_toast_namespace` (installed by
`catalog_namespace::isTempToastNamespace`). Only the aclchk inline lacks it.
`rg is_temp_toast_namespace crates/backend/catalog/aclchk` = no hits.
(stale-mechanism-comment law: the comment names a C mechanism the code
doesn't implement — it predicted this defect.)

## Ground truth (real binaries, identical SQL, 2026-07-30)

Session: role `tt_owner` (non-superuser), `CREATE TEMP TABLE tt(x text)`,
toast rel = `pg_toast_temp_N.pg_toast_<oid>`, owner tt_owner, relacl NULL.
`has_table_privilege('tt_owner', <toast oid>, <priv>)`:

| cell (owning session)      | C PG 18.4 (docker postgres:18, Linux-aarch64) | pgrust v0.2 (docker malisper/pgrust:v0.2) |
|----------------------------|---------------------------------------------|-------------------------------------------|
| INSERT / UPDATE / DELETE / TRUNCATE | **f f f f** | **t t t t**  ← DIVERGES |
| SELECT / MAINTAIN          | t t (not in strip mask)                     | t t (agree)                                |
| pg_toast perm-table control: INSERT / SELECT | f / t             | f / t (agree — PG_TOAST_NAMESPACE arm exists) |

Session-locality (C PG 18.4): the SAME oid probed from a DIFFERENT session
(same role, and superuser session) returns INSERT = **t** — C's answer to
`has_table_privilege` on a temp toast table depends on which session asks.
pgrust answers t from every session (no session-local arm at all), so the
divergence surface is exactly: **asking session = owning session, write-class
privileges**. Raw transcripts: this lane's scratchpad
(pg18_own_session.txt / pgrust_own_session.txt); SQL reproduced in this file's
git history and trivially re-runnable.

## Formal witness (proofs/aclcheck, committed cbecce9944)

- `diag_c_probe_strip` GREEN 8.9s: vendored-C model under concrete state
  {found, relkind 'r', relnamespace 16384, owner=grantee=roleid=100, stored
  ACL INSERT, temp-toast flag RAISED, non-superuser} returns mask **0**
  (strip fires); flag lowered returns **1** (control).
- `probe_system_class_temp_toast_core` EXPECTED-FAIL 3s: shipped
  `aclchk::pg_class_aclmask` under the same state returns **ACL_INSERT**;
  failing property `m == 0`, Ok-arm cover SATISFIED, Err-arm UNREACHABLE.
- `diag_seam_visibility` GREEN: rig soundness (seam writes cross the
  goto-link boundary; C IsSystemClass sees the flag).
- Full-pipeline `probe_system_class_temp_toast` (fc wrapper + priv-string
  parse): wall(symex, timeout-450s) — superseded by _core.

## User-visible surface

Small but real, and security-flavored in the "reports privileges you don't
have" direction (pgrust over-REPORTS and, if the same mask path guards DML,
over-GRANTS write access to the session's own temp toast table — a relation
the same session owner can already rewrite indirectly through its parent
temp table, which bounds the practical exposure). Also affects UPDATE/DELETE/
TRUNCATE/USAGE via the same mask, and `has_table_privilege`'s answer parity
for tools that introspect toast relations.

## Options (no ruling)

(a) Fix aclchk inline: add the temp-toast arm by calling the ALREADY-PORTED
    `catalog::IsSystemClass` (or `namespace_seams::is_temp_toast_namespace`)
    from `pg_class_aclmask_ext` — C-parity, one-line-ish; needs the syscache
    relnamespace read it already does. Restores session-local behavior
    including the C quirk (different answers per asking session).
(b) Keep current behavior and document: pgrust's answer is
    session-independent (arguably more consistent than C's session-local
    strip); diverges from C on the owning-session cells above.
(c) Broader arm: strip for ANY pg_toast_temp_* namespace (not just the
    current session's) — session-independent AND always-deny; diverges from
    C in the OTHER direction (cross-session probes would flip t->f).

Candidate default is (a) (C-exactness doctrine); recorded as candidate only.

## Rig findings recorded in passing (family-relevant)

1. Mixed C+Rust probe variants (~1300 checks) produced
   nondeterministic-across-edits C-side reads (cerr!=0, then cout!=0)
   despite a GREEN 7-assert pre-call state audit of every strip conjunct;
   the identical C call in a 759-check harness proves green. Suspect
   object-bits/goto-link interaction at scale — NOT diagnosed. Family eq_*
   harnesses are of the affected scale: treat any surprising eq_* FAILED as
   suspect-rig FIRST (replay against a single-side oracle harness).
2. `assert!(false)` in an arm later proven UNREACHABLE was reported as the
   failing property in two runs (Err(Box<PgError>) payload/discriminant
   corruption class, already a KNOWN Kani defect in the skill). Cover-based
   arm witnessing is the honest pattern.
