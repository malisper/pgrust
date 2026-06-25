# Audit: backend-catalog-objectaddress

C source: `src/backend/catalog/objectaddress.c` (PG 18.3, 6260 lines).
Unit: NEEDS_DECOMP, assembled from F0 (keystone) + F1/F2/F3/F4 family branches.

Verdict: **needs-decomp** (clean for the filled families; the resolution
ENGINE is a sanctioned mirror-and-panic residual — a genuine cross-subsystem
seam-and-panic, not absent own-logic). `cargo check --workspace` = exit 0;
`cargo test -p backend-catalog-objectaddress` and `-p seams-init` pass.

## Method

Re-derived the function inventory from the C directly (not the port comments).
For each function: read the C, compared to the Rust port; verified static-table
row counts, error predicates, control flow, and constant maps against the C
headers. Auditors do not trust a green build.

## Function inventory + verdicts

| C fn (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `ObjectProperty[]` (119) | `tables.rs:70` (37 rows) | MATCH | 37 rows, 1:1 with C (verified row count + class_oid/oid_catcache_id/attnum_namespace fields). |
| `ObjectTypeMap[]` (652) | `tables.rs:615` (59 rows) | MATCH | 59 (name, ObjectType) pairs, 1:1 with C. |
| `get_object_property_data` (2755) | `properties.rs` | MATCH | linear `ObjectProperty[]` scan keyed on `class_id`; C `prop_last` 1-entry cache omitted (behaviour-preserving — pure perf). Misses raise the same "unrecognized class %u" error. |
| `is_objectclass_supported` (2738) | `properties.rs` | MATCH | scan for a row whose `class_oid == class_id`. |
| the 13 `ObjectProperty[]` accessors (2629-2737) | `properties.rs` | MATCH | each returns the projected field of `get_object_property_data`. |
| `get_object_type` (via lsyscache relkind dig) | `properties.rs` | MATCH | `get_rel_relkind` seam + `get_relkind_objtype`. |
| `get_object_namespace` (2573) | `resolve.rs:240` | MATCH | property lookup → `attnum_namespace == InvalidAttrNumber` early `InvalidOid`; `SearchSysCache1`; cache-miss `elog(ERROR "cache lookup failed for cache %d oid %u")`; `SysCacheGetAttrNotNull`. Mcx-less seam ⇒ transient local context (no value escapes — faithful). |
| `read_objtype_from_string` (2609) | `resolve.rs:284` | MATCH | `ObjectTypeMap[]` scan; `ERRCODE_INVALID_PARAMETER_VALUE` "unrecognized object type" on miss. |
| `get_relkind_objtype` (6186) | `resolve.rs:299` | MATCH | total switch incl. TOASTVALUE→TABLE and default→TABLE; cannot ereport. |
| `get_catalog_object_by_oid` (2790) | `resolve.rs:317` | SEAMED | delegates to `backend_catalog_indexing_seams::get_catalog_object_by_oid` (the systable-scan primitive lives in the indexing owner — real dep cycle); thin marshal. |
| `get_catalog_object_by_oid_extended` (2803) | `resolve.rs:329` | SEAMED | same primitive with `locktuple`. |
| `getObjectDescription` (2912, ~41 class arms) | `description.rs` (1241 lines, 41 arms) | MATCH | per-class description text assembled in-crate; catalog rows cross as thin GETSTRUCT projections via syscache-seams (cast/amop/amproc/rewrite/trigger/auth_member/default_acl/policy/publication/constraint/attrdef/...). Formatting/branching is all in-crate. |
| `getObjectDescriptionOids` (4086) | `description.rs` | MATCH | builds an ObjectAddress and calls getObjectDescription. |
| `getRelationDescription` / `getOpFamilyDescription` (4103/4178) | `description.rs` | MATCH | in-crate helpers. |
| `getObjectTypeDescription` (4497, 41 arms) | `type_description.rs` (249 lines, 41 arms) | MATCH | per-class type label; relation/constraint/procedure disambiguation helpers in-crate. |
| `getRelationTypeDescription`/`getConstraintTypeDescription`/`getProcedureTypeDescription` (4687/4750/4787) | `type_description.rs` | MATCH | |
| `getObjectIdentity` (4824) | `identity.rs` | MATCH | wraps getObjectIdentityParts. |
| `getObjectIdentityParts` (4839, ~41 arms) | `identity.rs` (1200 lines, 68 arms incl. nested) | MATCH | per-class identity assembly + names/args out-params; catalog rows via syscache-seams identity projections. |
| `getOpFamilyIdentity`/`getRelationIdentity` (6053/6097) | `identity.rs` | MATCH | in-crate helpers. |
| `getPublicationSchemaInfo` (2864) | `description.rs`/`identity.rs` | MATCH | via publication_namespace seams. |
| `pg_describe_object` (4220) | `fmgr_sql.rs:119` | MATCH | pinned-OID guard (`!OidIsValid(classid) && !OidIsValid(objid)` ⇒ NULL); F1 getObjectDescription; `cstring_to_text` subsumed by the `PgString` value model. |
| `pg_identify_object_as_address` (4365) | `fmgr_sql.rs:192` | MATCH | F2 type-desc + F3 identity-parts; returns the deconstructed name/arg `text[]` columns directly (value boundary); never-NULL type asserted. |
| `get_object_address` (923) | `resolve.rs:46` | **residual mirror-and-panic** | see below. |
| `get_object_address_rv` (1225) | `resolve.rs:60` | **residual** | |
| 13 `get_object_address_*` helpers (1247-1962) | `resolve.rs:77-197` | **residual** | |
| `check_object_ownership` (2391) | `resolve.rs:208` | **residual** | |
| `object_ownercheck` | `resolve.rs:234` | **residual** | |
| `pg_get_object_address` (2109) | `fmgr_sql.rs:96` | **residual** | gated on parser node-construction lane + array Datum lane. |
| `pg_identify_object` (4248) | `fmgr_sql.rs:159` | **residual** | gated on catalog-tuple read lane (heap_getattr schema/name + quote_identifier). |
| `pg_get_acl` (4426) | `fmgr_sql.rs:256` | **residual** | gated on aclitem[] array Datum payload lane. |
| `textarray_to_strvaluelist` (2083) / `strlist_to_textarray` (6131) | `fmgr_sql.rs:53/70` | **residual** | gated on the array Datum element-bytes payload lane (header-only `types_array::ArrayType`). |

## Residual: the resolution ENGINE (sanctioned seam-and-panic)

`get_object_address[_rv]`, the 13 `get_object_address_*` helpers,
`check_object_ownership`, and `object_ownercheck` panic loudly with a
documented rationale. This is a genuine cross-subsystem block, not absent
own-logic:

- They fan out to ~40 distinct cross-crate lookup callees. ~25 require NEW seam
  declarations and 6 require NEW `-seams` crates that do not exist
  (pg-cast / pg-transform / pg-statistic-ext / pg-parameter-acl /
  rewrite-rewritesupport / pg-amop/amproc/publication).
- Several EXISTING owner seams have signatures divergent from what
  objectaddress.c needs (get_opclass_oid name-list vs (amid,opcname,nsp),
  get_collation_oid `&[&str]`, lookup_type_name_oid `&ParseTypeName`,
  typename_type_id declared in two crates, format_type_be needs mcx+PgString,
  lock_shared_object LockGuard objsubid:u16, get_foreign_server_by_name returns
  ForeignServer not Oid).

Reconciling those contracts is a coordinated cross-crate seam-contract pass that
directly overlaps the active seam-reconcile lane (#112); doing it inside an
F0-only assembly risks workspace-wide E0428/contract-divergence breakage. Per
the project's repeated "mirror-pg-and-panic" / "contract-divergence STOP"
guidance this is the correct deferral: the panics are mirror-and-panic into
unbuilt/divergent owner contracts, NOT own-logic stubs. No `todo!()`,
`unimplemented!()`, or unwired own-logic exists anywhere in the crate.

The SQL-leg residuals (`pg_get_object_address`, `pg_identify_object`,
`pg_get_acl`, `textarray_to_strvaluelist`, `strlist_to_textarray`) are likewise
gated on lanes that do not exist yet (the array Datum element-bytes payload —
`types_array::ArrayType` is header-only; the parser node-construction lane; the
catalog-tuple read+quoting lane).

## Seam audit

Owned inward seam crate: `backend-catalog-objectaddress-seams` (5 PINNED
decls). All 5 are installed by `init_seams()` (which is nothing but `set()`
calls) and `seams-init::init_all()` calls `backend_catalog_objectaddress::init_seams()`.
No `set()` outside the owner. Outward seams (syscache/lsyscache/indexing/
namespace/regproc/format-type/ruleutils/dbcommands/tablespace/extension/
miscinit/foreign/largeobject/misc2/user/pg-constraint) are thin GETSTRUCT
projections or scan primitives — no branching/node-construction in a seam path.

Assembly note: the F1+F3 merge unioned two disjoint seam sets into
`backend-utils-cache-syscache-seams` (description-row projections + identity
projections); shared seams deduped (auth_member_member_role,
parameter_acl_name, transform_type_lang, user_mapping_user_server,
event_trigger_name, cast_source_target). A git auto-merge silently duplicated
`format_type_extended` in `backend-utils-adt-format-type-seams` (identical
signature) — removed the duplicate (E0428 guard caught it).

## Gate

- `cargo check --workspace`: exit 0 (warnings only).
- `cargo test -p backend-catalog-objectaddress`: pass.
- `cargo test -p seams-init` (both recurrence guards): pass.
