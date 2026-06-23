#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
// PgError is a large error type shared across the whole tree; boxing it would
// diverge from every sibling crate's Result shape.
#![allow(clippy::result_large_err)]
// Several drivers faithfully take the same wide parameter set as the C callee.
#![allow(clippy::too_many_arguments)]

//! Faithful port of `backend/commands/publicationcmds.c` — CREATE / ALTER /
//! DROP PUBLICATION and its supporting validation (PostgreSQL 18.3).
//!
//! This is the repo (per-owner-seam) model, not the centralized-`rt::` model of
//! the `src-idiomatic` reference. Catalog reads, the `Get*` relation/schema
//! helpers, `pub_collist_validate`, `get_top_most_ancestor_in_publication`,
//! `get_pub_partition_option_relations`, `check_and_fetch_column_list`,
//! `GetPublication`, `is_schema_publication`, the two catalog mutators
//! (`publication_add_relation` / `publication_add_schema`) all come from the
//! ported `backend-catalog-pg-publication` crate (via its `-seams`). The
//! pg_publication tuple insert / update / owner-change and the three
//! `Remove*ById` deletes are written inline in this crate (mirroring the
//! `pg_publication.c` mechanics: `GetNewOidWithIndex` + `heap_form_tuple` /
//! `heap_modify_tuple` + `CatalogTuple{Insert,Update,Delete}`).
//!
//! The six inward seams (`AlterPublicationOwner_oid`, `RemovePublicationById`,
//! `RemovePublicationRelById`, `RemovePublicationSchemaById`,
//! `AlterPublicationOwner`, `InvalidatePubRelSyncCache`) are reached from
//! `backend-catalog-dependency` (the per-class drop handlers) and from
//! `backend-catalog-pg-shdepend` (REASSIGN OWNED) — direct dependency cycles.
//! Their seam contracts are `Mcx`-free; each installer wrapper spins up a fresh
//! `MemoryContext` and runs the `Mcx`-taking implementation in it (the
//! established bridging idiom, cf. `backend-commands-foreigncmds::init_seams`),
//! because `mcx` deliberately has no ambient current context.

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;

use mcx::{Mcx, PgBox, PgString, PgVec};

use utils_error::{ereport, PgResult};
use types_error::{
    PgError, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_UNDEFINED_SCHEMA, ERROR, WARNING,
};

use types_acl::acl::{ACLCHECK_NOT_OWNER, ACLCHECK_OK, ACL_CREATE, AclResult};
use types_catalog::catalog::{
    DATABASE_RELATION_ID, NAMESPACE_RELATION_ID, RELATION_RELATION_ID,
};
use types_catalog::catalog_dependency::{InvalidObjectAddress, ObjectAddress};
use types_catalog::pg_publication::{
    Anum_pg_publication_namespace_oid, Anum_pg_publication_oid, Anum_pg_publication_pubdelete,
    Anum_pg_publication_pubgencols, Anum_pg_publication_pubinsert, Anum_pg_publication_pubname,
    Anum_pg_publication_puballtables, Anum_pg_publication_pubowner, Anum_pg_publication_pubtruncate,
    Anum_pg_publication_pubupdate, Anum_pg_publication_pubviaroot, Anum_pg_publication_rel_oid,
    Anum_pg_publication_rel_prattrs, Anum_pg_publication_rel_prqual, Anum_pg_publication_rel_prrelid,
    Natts_pg_publication, PublicationActions, PublicationNamespaceObjectIndexId,
    PublicationNamespaceRelationId, PublicationObjectIndexId, PublicationPartOpt,
    PublicationRelObjectIndexId, PublicationRelRelationId, PublicationRelationId,
};
use types_core::catalog::FirstNormalObjectId;
use types_core::primitive::{AttrNumber, InvalidOid, Oid, ParseLoc};
use nodes::ddlnodes::{
    AlterPublicationStmt, CreatePublicationStmt, DefElem, PublicationObjSpec, PublicationTable,
    AP_AddObjects, AP_DropObjects, AP_SetObjects, PUBLICATIONOBJ_TABLE,
    PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA, PUBLICATIONOBJ_TABLES_IN_SCHEMA,
};
use nodes::nodes::{ntag, Node};
use nodes::parsenodes::{DROP_CASCADE, ObjectType};
use nodes::parsestmt::{ParseExprKind, ParseState};
use nodes::primnodes::Expr;
use types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock,
};
use types_tuple::access::RELKIND_PARTITIONED_TABLE;
use types_tuple::heaptuple::{Datum, FormedTuple};

use heaptuple::{heap_deform_tuple, heap_form_tuple, heap_modify_tuple};
use scankey::ScanKeyInit;
use transam_xact::CommandCounterIncrement;
use aclchk::{object_aclcheck, object_ownercheck};
use catalog_catalog::GetNewOidWithIndex;
use dependency::performDeletion;
use indexing::keystone::{CatalogTupleDelete, CatalogTupleInsert, CatalogTupleUpdate};
use pg_shdepend::{changeDependencyOnOwner, recordDependencyOnOwner};
use define_seams::DefElemArg;
use nodes_core::nodefuncs::check_functions_in_node;
use nodes_core::node_walker::expression_tree_walker;
use equalfuncs::equal_node;
use name::namein;
use inval::cache_invalidate::{
    CacheInvalidateRelSync, CacheInvalidateRelSyncAll, CacheInvalidateRelcacheAll,
    CacheInvalidateRelcacheByRelid,
};
use cache_syscache::{ReleaseSysCache, SearchSysCache1, SearchSysCache2, SysCacheGetAttr};
use miscinit::GetUserId;
use pgstrcasecmp::pg_strcasecmp;

use cache::syscache::SysCacheKey;
use datum::Datum as KeyDatum;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_syscache::syscache_ids::{PUBLICATIONOID, PUBLICATIONRELMAP};

// Cross-subsystem externals reached through their owners' -seams crates.
use genam_seams as genam;
use transam_xlog_seams as xlog_seams;
use objectaccess_seams as objaccess;
use pg_inherits_seams as inherits_seams;
use pg_publication_seams as pubcat;
use dbcommands_seams as dbcommands_seams;
use event_trigger_seams as event_trigger_seams;
use tablespace_globals_seams as globals_seams;
use read_seams as read_seams;
use parser_analyze_seams as analyze_seams;
use rewritehandler_seams as rewrite_seams;
use acl_seams as acl_seams;
use varlena_seams as varlena_seams;
use lsyscache_seams as lsyscache;
use miscinit_seams as miscinit_seams;
use aclchk_seams as aclchk_seams;
use postgres_seams as postgres_seams;

mod inward;

/// `REPLICA_IDENTITY_FULL` (`pg_class.h`) — `relreplident` is read as `u8`.
const REPLICA_IDENTITY_FULL: u8 = b'f';

/// `MAX_RELCACHE_INVAL_MSGS` (`commands/publicationcmds.h`).
const MAX_RELCACHE_INVAL_MSGS: usize = 4096;

/// `FirstLowInvalidHeapAttributeNumber` (`access/sysattr.h`) = -7.
const FirstLowInvalidHeapAttributeNumber: i32 = -7;

/// `PUBLISH_GENCOLS_NONE` ('n') / `PUBLISH_GENCOLS_STORED` ('s')
/// (`catalog/pg_publication.h`).
const PUBLISH_GENCOLS_NONE: i8 = b'n' as i8;
const PUBLISH_GENCOLS_STORED: i8 = b's' as i8;

/// `PROVOLATILE_IMMUTABLE` ('i') (`catalog/pg_proc.h`).
const PROVOLATILE_IMMUTABLE: u8 = b'i';

/// `ObjectAddressSet(addr, class, object)`.
fn object_address_set(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `OidIsValid(oid)`.
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

// ---------------------------------------------------------------------------
// Owned analogues of the C `List *` helpers used by this file.
// ---------------------------------------------------------------------------

fn list_member_oid(list: &[Oid], datum: Oid) -> bool {
    list.contains(&datum)
}

fn list_append_unique_oid(list: &mut Vec<Oid>, datum: Oid) {
    if !list.contains(&datum) {
        list.push(datum);
    }
}

fn list_concat_unique_oid(mut list1: Vec<Oid>, list2: &[Oid]) -> Vec<Oid> {
    for &o in list2 {
        if !list1.contains(&o) {
            list1.push(o);
        }
    }
    list1
}

fn list_difference_oid(list1: &[Oid], list2: &[Oid]) -> Vec<Oid> {
    list1.iter().copied().filter(|o| !list2.contains(o)).collect()
}

/// `bms_is_member(x, set)` over the `Vec<i32>` bitmap representation.
fn bms_is_member(x: i32, set: &[i32]) -> bool {
    set.contains(&x)
}

/// Set equality (order-insensitive), with a NULL old bitmap equal to a
/// NULL/empty new bitmap (`bms_equal` accounting for the C `NULL`).
fn bms_equal_opt(old: Option<&[i32]>, new: &[i32]) -> bool {
    match old {
        None => new.is_empty(),
        Some(o) => o.len() == new.len() && o.iter().all(|x| new.contains(x)),
    }
}

// ---------------------------------------------------------------------------
// Small varlena / scan-key / NameData helpers (copied from pg_publication.rs).
// ---------------------------------------------------------------------------

/// `CStringGetTextDatum(s)` — a `text` varlena Datum (4-byte header + payload).
fn cstring_to_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    const VARHDRSZ: usize = 4;
    let payload = s.as_bytes();
    let total = VARHDRSZ + payload.len();
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    let vl_len: u32 = (total as u32) << 2;
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    buf[VARHDRSZ..].copy_from_slice(payload);
    Ok(Datum::ByRef(buf))
}

/// `NameStr(name)` — read a NUL-padded `NameData` image as a `&str`.
fn name_str(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).unwrap_or("")
}

/// `namein(s)` image — a `NAMEDATALEN`-byte NUL-padded `NameData` Datum.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    use types_core::fmgr::NAMEDATALEN;
    let mut image: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, NAMEDATALEN as usize)?;
    let src = s.as_bytes();
    let take = core::cmp::min(src.len(), (NAMEDATALEN as usize) - 1);
    for &b in &src[..take] {
        image.push(b);
    }
    while image.len() < NAMEDATALEN as usize {
        image.push(0);
    }
    Ok(Datum::ByRef(image))
}

/// `ObjectIdGetDatum` as a syscache key.
fn oid_cache_key(value: Oid) -> SysCacheKey<'static> {
    SysCacheKey::Value(KeyDatum::from_oid(value))
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: i32, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    use types_core::fmgr::F_OIDEQ;
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno as AttrNumber,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

/// `int16` element values of an on-disk `int2vector` varlena image
/// (`pg_publication_rel.prattrs`). The varlena header is 4 bytes normally but ONE
/// byte for a short-packed stored image (C reaches it through `DatumGetArrayTypeP`
/// / `PG_DETOAST_DATUM`, which un-packs short→4B), so the struct content begins at
/// [`arr_content_off`] (`VARDATA_ANY`): reading `dim1` at a fixed 4-byte offset on
/// a short-packed vector mis-reads the count. No-op while packing is off.
fn int2vector_elems(bytes: &[u8]) -> Vec<i16> {
    let c = arr_content_off(bytes);
    // dim1 lives 12 bytes into the struct content; data 20 bytes in.
    let header = c + 20;
    if bytes.len() < header {
        return Vec::new();
    }
    let nelems = i32::from_ne_bytes([bytes[c + 12], bytes[c + 13], bytes[c + 14], bytes[c + 15]]);
    if nelems < 0 {
        return Vec::new();
    }
    let nelems = nelems as usize;
    let need = header + nelems * 2;
    if bytes.len() < need {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(nelems);
    for i in 0..nelems {
        let off = header + i * 2;
        out.push(i16::from_ne_bytes([bytes[off], bytes[off + 1]]));
    }
    out
}

/// The byte offset of an `int2vector` image's struct content (the `ndim` field)
/// past its varlena length header — `VARDATA_ANY`-style. ONE byte for a short
/// (low-bit-set, non-external) header, else `VARHDRSZ` (4). No-op while
/// `SHORT_VARLENA_PACKING` is off (every stored image is 4-byte).
fn arr_content_off(image: &[u8]) -> usize {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => 1,
        _ => 4,
    }
}

/// Deform a freshly scanned tuple into its `(value, isnull)` columns.
fn deform<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<PgVec<'mcx, (Datum<'mcx>, bool)>> {
    let desc = rel.rd_att_clone_in(mcx)?;
    heap_deform_tuple(mcx, &tup.tuple, &desc, &tup.data)
}

// ---------------------------------------------------------------------------
// In-crate `PublicationRelInfo` carrying the walkable WHERE-clause node.
// ---------------------------------------------------------------------------

/// `typedef struct PublicationRelInfo` (`catalog/pg_publication.h`). The
/// row-filter WHERE clause is carried as the walkable `Node::Expr(...)` so this
/// crate can run the row-filter walks; the `publication_add_relation` seam takes
/// it as `Option<&Node>`. The column list is carried as the original parser
/// `Node` list (`String`/`ColumnRef` names) so the seam can validate it.
struct PublicationRelInfo<'mcx> {
    /// `Relation relation` — the (already opened) target relation.
    relation: rel::Relation<'mcx>,
    /// `Node *whereClause` — the row-filter expression (`None` if none).
    whereClause: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `List *columns` — the column-list name nodes (empty if none).
    columns: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
}

/// Clone a column-name `Node` list into `mcx`.
fn clone_columns<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[PgBox<'mcx, Node<'mcx>>],
) -> PgResult<PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>> {
    let mut out: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> = mcx::vec_with_capacity_in(mcx, src.len())?;
    for c in src {
        out.push(mcx::alloc_in(mcx, c.clone_in(mcx)?)?);
    }
    Ok(out)
}

/* ===========================================================================
 * parse_publication_options / defGetGeneratedColsOption (publicationcmds.c)
 * ======================================================================== */

/// Result of [`parse_publication_options`].
struct PublicationOptions {
    publish_given: bool,
    pubactions: PublicationActions,
    publish_via_partition_root_given: bool,
    publish_via_partition_root: bool,
    publish_generated_columns_given: bool,
    publish_generated_columns: i8,
}

/// Project a `DefElem`'s value node into the `DefElemArg` the define.c value
/// accessors switch on (`nodeTag(def->arg)`). Mirrors `defGetString`'s full
/// node switch (define.c): a bare-identifier value arrives as a `T_TypeName`
/// (grammar `def_arg: func_type`) and a qualified name as a `T_List`; both
/// render to their textual form. A `_ => AStar` catch-all would collapse those
/// to `"*"`.
fn defel_arg(defel: &DefElem) -> PgResult<Option<DefElemArg>> {
    let Some(node) = defel.arg.as_deref() else {
        return Ok(None);
    };
    Ok(Some(match node.node_tag() {
        ntag::T_Integer => DefElemArg::Integer(node.expect_integer().ival as i64),
        ntag::T_Float => DefElemArg::Float(node.expect_float().fval.as_str().to_string()),
        ntag::T_Boolean => DefElemArg::Boolean(node.expect_boolean().boolval),
        ntag::T_String => DefElemArg::String(node.expect_string().sval.as_str().to_string()),
        ntag::T_TypeName => DefElemArg::TypeName(defel_type_name_to_string(node.expect_typename())?),
        ntag::T_List => DefElemArg::List(defel_name_list_to_string(node.expect_list())?),
        ntag::T_A_Star => DefElemArg::AStar,
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized node type: {}", other))
                .into_error())
        }
    }))
}

/// `TypeNameToString(typeName)` for the `defGetString` `T_TypeName` case
/// (parse_type.c). A reloption-style `def->arg` `TypeName` is always a parsed
/// identifier carrying `names`.
fn defel_type_name_to_string(tn: &nodes::rawnodes::TypeName<'_>) -> PgResult<String> {
    if tn.names.is_empty() {
        return Err(ereport(ERROR)
            .errmsg_internal("DefElem TypeName carries no name")
            .into_error());
    }
    let mut out = String::new();
    for (i, name) in tn.names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node: &Node = name;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("unrecognized node type: {}", other))
                    .into_error())
            }
        }
    }
    if tn.pct_type {
        out.push_str("%TYPE");
    }
    if !tn.arrayBounds.is_empty() {
        out.push_str("[]");
    }
    Ok(out)
}

/// `NameListToString(names)` (namespace.c) for the `defGetString` `T_List` case.
fn defel_name_list_to_string(names: &[nodes::nodes::NodePtr<'_>]) -> PgResult<String> {
    let mut out = String::new();
    for (i, name) in names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node: &Node = name;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            ntag::T_A_Star => out.push('*'),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("unrecognized node type: {}", other))
                    .into_error())
            }
        }
    }
    Ok(out)
}

/// `defGetString(def)` (define.c).
fn defGetString(mcx: Mcx<'_>, defel: &DefElem) -> PgResult<String> {
    let s = define_seams::def_get_string::call(
        mcx,
        defel.defname.as_deref().unwrap_or("").to_string(),
        defel_arg(defel)?,
    )?;
    Ok(s.as_str().to_string())
}

/// `defGetBoolean(def)` (define.c).
fn defGetBoolean(defel: &DefElem) -> PgResult<bool> {
    define_seams::def_get_boolean::call(
        defel.defname.as_deref().unwrap_or("").to_string(),
        defel_arg(defel)?,
    )
}

/// `defGetGeneratedColsOption(def)` (publicationcmds.c:2118-2142).
fn defGetGeneratedColsOption(mcx: Mcx<'_>, def: &DefElem) -> PgResult<i8> {
    let mut sval = String::new();

    /* A parameter value is required. */
    if def.arg.is_some() {
        sval = defGetString(mcx, def)?;

        if pg_strcasecmp(sval.as_bytes(), b"none") == 0 {
            return Ok(PUBLISH_GENCOLS_NONE);
        }
        if pg_strcasecmp(sval.as_bytes(), b"stored") == 0 {
            return Ok(PUBLISH_GENCOLS_STORED);
        }
    }

    let defname = def.defname.as_deref().unwrap_or("");
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!(
            "invalid value for publication parameter \"{defname}\": \"{sval}\""
        ))
        .errdetail(format!("Valid values are \"{}\" and \"{}\".", "none", "stored"))
        .into_error())
}

/// `errorConflictingDefElem(defel, pstate)` (define.c:371). Attaches
/// `parser_errposition(pstate, defel->location)` so psql renders the `LINE n:
/// ...` source-context with a caret under the redundant option.
fn error_conflicting_def_elem(pstate: &ParseState<'_>, location: ParseLoc) -> PgResult<PgError> {
    let cursorpos = small1_seams::parser_errposition::call(pstate, location)?;
    Ok(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .errposition(cursorpos)
        .into_error())
}

/// `parse_publication_options` (publicationcmds.c:77-177).
fn parse_publication_options<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    options: &[PgBox<'mcx, Node<'mcx>>],
) -> PgResult<PublicationOptions> {
    let mut publish_given = false;
    let mut publish_via_partition_root_given = false;
    let mut publish_generated_columns_given = false;

    /* defaults */
    let mut pubactions = PublicationActions {
        pubinsert: true,
        pubupdate: true,
        pubdelete: true,
        pubtruncate: true,
    };
    let mut publish_via_partition_root = false;
    let mut publish_generated_columns = PUBLISH_GENCOLS_NONE;

    /* Parse options */
    for defel_node in options {
        let defel = match defel_node.node_tag() {
            ntag::T_DefElem => defel_node.expect_defelem(),
            _ => {
                return Err(PgError::error(
                    "publication option list element is not a DefElem",
                ))
            }
        };
        let dn = defel.defname.as_deref().unwrap_or("");

        if dn == "publish" {
            if publish_given {
                return Err(error_conflicting_def_elem(pstate, defel.location)?);
            }

            /*
             * If publish option was given only the explicitly listed actions
             * should be published.
             */
            pubactions.pubinsert = false;
            pubactions.pubupdate = false;
            pubactions.pubdelete = false;
            pubactions.pubtruncate = false;

            publish_given = true;

            let publish = defGetString(mcx, defel)?;

            let publish_list = match varlena_seams::split_identifier_string::call(
                mcx,
                publish.as_str(),
                ',',
            )? {
                Some(l) => l,
                None => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!("invalid list syntax in parameter \"{}\"", "publish"))
                        .into_error());
                }
            };

            /* Process the option list. */
            for publish_opt in publish_list.iter() {
                let publish_opt = publish_opt.as_str();
                if publish_opt == "insert" {
                    pubactions.pubinsert = true;
                } else if publish_opt == "update" {
                    pubactions.pubupdate = true;
                } else if publish_opt == "delete" {
                    pubactions.pubdelete = true;
                } else if publish_opt == "truncate" {
                    pubactions.pubtruncate = true;
                } else {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!(
                            "unrecognized value for publication option \"{}\": \"{publish_opt}\"",
                            "publish"
                        ))
                        .into_error());
                }
            }
        } else if dn == "publish_via_partition_root" {
            if publish_via_partition_root_given {
                return Err(error_conflicting_def_elem(pstate, defel.location)?);
            }
            publish_via_partition_root_given = true;
            publish_via_partition_root = defGetBoolean(defel)?;
        } else if dn == "publish_generated_columns" {
            if publish_generated_columns_given {
                return Err(error_conflicting_def_elem(pstate, defel.location)?);
            }
            publish_generated_columns_given = true;
            publish_generated_columns = defGetGeneratedColsOption(mcx, defel)?;
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized publication parameter: \"{dn}\""))
                .into_error());
        }
    }

    Ok(PublicationOptions {
        publish_given,
        pubactions,
        publish_via_partition_root_given,
        publish_via_partition_root,
        publish_generated_columns_given,
        publish_generated_columns,
    })
}

/* ===========================================================================
 * ObjectsInPublicationToOids (publicationcmds.c:183-230)
 * ======================================================================== */

/// `ObjectsInPublicationToOids` — split a `PublicationObjSpec` list into a list
/// of `PublicationTable` (`rels`) and a deduplicated schema OID list.
fn ObjectsInPublicationToOids<'mcx>(
    mcx: Mcx<'mcx>,
    pubobjspec_list: &[PgBox<'mcx, Node<'mcx>>],
    rels: &mut Vec<PgBox<'mcx, PublicationTable<'mcx>>>,
    schemas: &mut Vec<Oid>,
) -> PgResult<()> {
    if pubobjspec_list.is_empty() {
        return Ok(());
    }

    for pubobj_node in pubobjspec_list {
        let pubobj: &PublicationObjSpec = match pubobj_node.node_tag() {
            ntag::T_PublicationObjSpec => pubobj_node.expect_publicationobjspec(),
            _ => {
                return Err(PgError::error(
                    "publication object list element is not a PublicationObjSpec",
                ))
            }
        };
        match pubobj.pubobjtype {
            PUBLICATIONOBJ_TABLE => {
                /* *rels = lappend(*rels, pubobj->pubtable) */
                if let Some(t) = &pubobj.pubtable {
                    rels.push(mcx::alloc_in(mcx, t.clone_in(mcx)?)?);
                }
            }
            PUBLICATIONOBJ_TABLES_IN_SCHEMA => {
                let name = pubobj.name.as_deref().unwrap_or("");
                let schemaid = catalog_namespace::get_namespace_oid(name, false)?;
                list_append_unique_oid(schemas, schemaid);
            }
            PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA => {
                /* nothing valid in search_path? */
                let search_path = catalog_namespace::fetch_search_path(mcx, false)?;
                let schemaid = match search_path.first() {
                    Some(&oid) => oid,
                    None => {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_UNDEFINED_SCHEMA)
                            .errmsg("no schema has been selected for CURRENT_SCHEMA")
                            .into_error())
                    }
                };
                list_append_unique_oid(schemas, schemaid);
            }
            other => {
                /* shouldn't happen */
                return Err(PgError::error(format!(
                    "invalid publication object type {}",
                    other as i32
                )));
            }
        }
    }

    Ok(())
}

/* ===========================================================================
 * contain_invalid_rfcolumn_walker / pub_rf_contains_invalid_column
 * ======================================================================== */

/// `rf_context` (publicationcmds.c:56-63). Carries an `Mcx` for the
/// `get_attname` lookup the pubviaroot path performs (the C function reads it
/// from the ambient context; this repo has no ambient context).
struct RfContext<'mcx> {
    mcx: Mcx<'mcx>,
    bms_replident: Vec<i32>,
    pubviaroot: bool,
    relid: Oid,
    parentid: Oid,
}

/// `contain_invalid_rfcolumn_walker(node, context)` (publicationcmds.c:236-267).
fn contain_invalid_rfcolumn_walker(
    node: &Node,
    context: &RfContext,
    err: &RefCell<Option<PgError>>,
) -> bool {
    let mcx = context.mcx;
    if let Some(var) = node.as_var() {
        let mut attnum = var.varattno;

        /*
         * If pubviaroot is true, we are validating the row filter of the parent
         * table, but the bitmap contains the replica identity information of the
         * child table.  So get the column number of the child table as parent
         * and child column order could be different.
         */
        if context.pubviaroot {
            match lsyscache::get_attname::call(mcx, context.parentid, attnum, false) {
                Ok(colname) => {
                    let cn = colname.as_deref().unwrap_or("");
                    match lsyscache::get_attnum::call(context.relid, cn) {
                        Ok(n) => attnum = n,
                        Err(e) => {
                            *err.borrow_mut() = Some(e);
                            return true;
                        }
                    }
                }
                Err(e) => {
                    *err.borrow_mut() = Some(e);
                    return true;
                }
            }
        }

        if !bms_is_member(
            attnum as i32 - FirstLowInvalidHeapAttributeNumber,
            &context.bms_replident,
        ) {
            return true;
        }
    }

    expression_tree_walker(node, &mut |n| {
        contain_invalid_rfcolumn_walker(n, context, err)
    })
}

/// `pub_rf_contains_invalid_column(pubid, relation, ancestors, pubviaroot)`
/// (publicationcmds.c:275-343).
pub fn pub_rf_contains_invalid_column<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    relation: &rel::Relation<'mcx>,
    ancestors: &[Oid],
    pubviaroot: bool,
) -> PgResult<bool> {
    let relid = relation.rd_id;
    let mut publish_as_relid = relid;
    let mut result = false;

    /*
     * FULL means all columns are in the REPLICA IDENTITY, so all columns are
     * allowed in the row filter and we can skip the validation.
     */
    if relation.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
        return Ok(false);
    }

    /*
     * For a partition, if pubviaroot is true, find the topmost ancestor that is
     * published via this publication.
     */
    if pubviaroot && relation.rd_rel.relispartition {
        let (topmost, _level) =
            pubcat::GetTopMostAncestorInPublication::call(mcx, pubid, ancestors)?;
        publish_as_relid = topmost;
        if !oid_is_valid(publish_as_relid) {
            publish_as_relid = relid;
        }
    }

    let rftuple = SearchSysCache2(
        mcx,
        PUBLICATIONRELMAP,
        oid_cache_key(publish_as_relid),
        oid_cache_key(pubid),
    )?;

    let Some(rftuple) = rftuple else {
        return Ok(false);
    };

    let (rfdatum, rfisnull) =
        SysCacheGetAttr(mcx, PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prqual)?;

    if !rfisnull {
        /* Remember columns that are part of the REPLICA IDENTITY */
        let bms = relation_get_replident_attrs(mcx, relation)?;

        let context = RfContext {
            mcx,
            bms_replident: bms,
            pubviaroot,
            parentid: publish_as_relid,
            relid,
        };

        let qual_str = text_datum_str(&rfdatum);
        let rfnode = read_seams::string_to_node::call(mcx, &qual_str)?;

        let err: RefCell<Option<PgError>> = RefCell::new(None);
        result = contain_invalid_rfcolumn_walker(&rfnode, &context, &err);
        if let Some(e) = err.into_inner() {
            ReleaseSysCache(rftuple);
            return Err(e);
        }
    }

    ReleaseSysCache(rftuple);

    Ok(result)
}

/* ===========================================================================
 * pub_contains_invalid_column (publicationcmds.c:361-497)
 * ======================================================================== */

/// `pub_contains_invalid_column(...)` (publicationcmds.c:361-497).
pub fn pub_contains_invalid_column<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    relation: &rel::Relation<'mcx>,
    ancestors: &[Oid],
    pubviaroot: bool,
    pubgencols_type: i8,
) -> PgResult<(bool, bool, bool)> {
    let relid = relation.rd_id;
    let mut publish_as_relid = relid;

    let mut invalid_column_list = false;
    let mut invalid_gen_col = false;

    if pubviaroot && relation.rd_rel.relispartition {
        let (topmost, _level) =
            pubcat::GetTopMostAncestorInPublication::call(mcx, pubid, ancestors)?;
        publish_as_relid = topmost;
        if !oid_is_valid(publish_as_relid) {
            publish_as_relid = relid;
        }
    }

    /* Fetch the column list */
    let pub_ = pubcat::GetPublication::call(mcx, pubid)?;
    let (has_columns, columns_bms) =
        pubcat::check_and_fetch_column_list::call(mcx, pubid, pub_.alltables, publish_as_relid, None)?;
    let columns: Vec<i32> = bitmapset_members(columns_bms.as_deref());

    if relation.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
        /* With REPLICA IDENTITY FULL, no column list is allowed. */
        invalid_column_list = has_columns;

        if pubgencols_type != PUBLISH_GENCOLS_STORED
            && relation_has_generated_stored(relation)
        {
            invalid_gen_col = true;
        }

        /* Virtual generated columns are not supported at all. */
        if relation_has_generated_virtual(relation) {
            invalid_gen_col = true;
        }

        if invalid_gen_col && invalid_column_list {
            return Ok((true, invalid_column_list, invalid_gen_col));
        }
    }

    /* Remember columns that are part of the REPLICA IDENTITY */
    let idattrs = relation_get_replident_attrs(mcx, relation)?;

    for &x in idattrs.iter() {
        let mut attnum = (x + FirstLowInvalidHeapAttributeNumber) as i16;
        let attgenerated = tupdesc_attgenerated(relation, attnum);

        if !has_columns {
            if attgenerated == types_tuple::access::ATTRIBUTE_GENERATED_STORED as i8
                && pubgencols_type != PUBLISH_GENCOLS_STORED
            {
                invalid_gen_col = true;
                break;
            }
            if attgenerated == types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL as i8 {
                invalid_gen_col = true;
                break;
            }
            /* Skip validating the column list since it is not defined */
            continue;
        }

        if pubviaroot {
            let colname = lsyscache::get_attname::call(mcx, relid, attnum, false)?;
            let cn = colname.as_deref().unwrap_or("");
            attnum = lsyscache::get_attnum::call(publish_as_relid, cn)?;
        }

        invalid_column_list |= !bms_is_member(attnum as i32, &columns);

        if invalid_column_list && invalid_gen_col {
            break;
        }
    }

    Ok((
        invalid_column_list || invalid_gen_col,
        invalid_column_list,
        invalid_gen_col,
    ))
}

/* ===========================================================================
 * InvalidatePubRelSyncCache (publicationcmds.c:505-536)
 * ======================================================================== */

/// `InvalidatePubRelSyncCache(pubid, puballtables)` (publicationcmds.c:505-536).
pub fn InvalidatePubRelSyncCache<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    puballtables: bool,
) -> PgResult<()> {
    if puballtables {
        CacheInvalidateRelSyncAll()?;
    } else {
        let relids = pubcat::GetPublicationRelations::call(mcx, pubid, PublicationPartOpt::All)?;
        let schemarelids =
            pubcat::GetAllSchemaPublicationRelations::call(mcx, pubid, PublicationPartOpt::All)?;

        let mut combined: Vec<Oid> = relids.iter().copied().collect();
        combined = list_concat_unique_oid(combined, schemarelids.as_slice());

        /* Invalidate the relsyncache */
        for relid in combined {
            CacheInvalidateRelSync(relid)?;
        }
    }

    Ok(())
}

/* ===========================================================================
 * check_simple_rowfilter_expr_walker / contain_mutable_or_user_functions_checker
 * ======================================================================== */

/// `contain_mutable_or_user_functions_checker(func_id, context)`
/// (publicationcmds.c:539-544).
fn contain_mutable_or_user_functions_checker(func_id: Oid) -> bool {
    match lsyscache::func_volatile::call(func_id) {
        Ok(v) => v != PROVOLATILE_IMMUTABLE || func_id >= FirstNormalObjectId,
        Err(_) => true,
    }
}

/// `check_simple_rowfilter_expr_walker(node, pstate)` (publicationcmds.c:584-684).
fn check_simple_rowfilter_expr_walker(
    node: &Node,
    pstate: &ParseState<'_>,
    err: &RefCell<Option<PgError>>,
) -> bool {
    let mut errdetail_msg: Option<&'static str> = None;

    /*
     * In the owned `Expr` model the C `switch(nodeTag(node))` over expression
     * tags is a match on the `Expr` value carried by `Node::Expr`.
     */
    if let Some(expr) = node.as_expr() {
        match expr {
            Expr::Var(var) => {
                /* System columns are not allowed. */
                if var.varattno < 0 {
                    errdetail_msg = Some("System columns are not allowed.");
                }
            }
            /*
             * T_OpExpr / T_DistinctExpr / T_NullIfExpr share the `OpExpr *`
             * cast in C; the owned tree gives each its own variant (all with an
             * `opno`).
             */
            Expr::OpExpr(op) | Expr::DistinctExpr(op) | Expr::NullIfExpr(op) => {
                if op.opno >= FirstNormalObjectId {
                    errdetail_msg = Some("User-defined operators are not allowed.");
                }
            }
            Expr::ScalarArrayOpExpr(sa) => {
                if sa.opno >= FirstNormalObjectId {
                    errdetail_msg = Some("User-defined operators are not allowed.");
                }
                /*
                 * We don't need to check the hashfuncid and negfuncid of
                 * ScalarArrayOpExpr as those functions are only built for a
                 * subquery.
                 */
            }
            Expr::RowCompareExpr(rc) => {
                for &opid in rc.opnos.iter() {
                    if opid >= FirstNormalObjectId {
                        errdetail_msg = Some("User-defined operators are not allowed.");
                        break;
                    }
                }
            }
            Expr::Const(_)
            | Expr::FuncExpr(_)
            | Expr::BoolExpr(_)
            | Expr::RelabelType(_)
            | Expr::CollateExpr(_)
            | Expr::CaseExpr(_)
            | Expr::CaseTestExpr(_)
            | Expr::ArrayExpr(_)
            | Expr::RowExpr(_)
            | Expr::CoalesceExpr(_)
            | Expr::MinMaxExpr(_)
            | Expr::XmlExpr(_)
            | Expr::NullTest(_)
            | Expr::BooleanTest(_) => { /* OK, supported */ }
            _ => {
                errdetail_msg = Some(
                    "Only columns, constants, built-in operators, built-in data types, built-in collations, and immutable built-in functions are allowed.",
                );
            }
        }

        /*
         * For all the supported nodes, if we haven't already found a problem,
         * check the types, functions, and collations used in it.
         */
        if errdetail_msg.is_none() {
            let typid = match nodeFuncs_seams::expr_type_info::call(expr) {
                Ok(info) => info.typid,
                Err(e) => {
                    *err.borrow_mut() = Some(e);
                    return true;
                }
            };
            if typid >= FirstNormalObjectId {
                errdetail_msg = Some("User-defined types are not allowed.");
            } else {
                /*
                 * `check_functions_in_node` takes `&mut Expr`; we only read here,
                 * so scan a clone for the function-OID check.
                 */
                let mut expr_copy = expr.clone();
                match check_functions_in_node(
                    &mut expr_copy,
                    &mut contain_mutable_or_user_functions_checker,
                ) {
                    Ok(true) => {
                        errdetail_msg =
                            Some("User-defined or built-in mutable functions are not allowed.");
                    }
                    Ok(false) => {
                        let coll = nodeFuncs_seams::exprCollation::call(expr);
                        // C: exprInputCollation(node). The owned tree carries the
                        // field on the `Expr`, so resolve it via the `_expr`
                        // form (the `_node` seam is the erased-Node stub used by
                        // the funcapi polymorphic resolver, which always returns
                        // InvalidOid for a non-FmgrInfo node).
                        let incoll =
                            nodeFuncs_seams::expr_input_collation_expr::call(expr);
                        if coll >= FirstNormalObjectId || incoll >= FirstNormalObjectId {
                            errdetail_msg = Some("User-defined collations are not allowed.");
                        }
                    }
                    Err(e) => {
                        *err.borrow_mut() = Some(e);
                        return true;
                    }
                }
            }
        }

        if let Some(msg) = errdetail_msg {
            // C: parser_errposition(pstate, exprLocation(node)) — convert the
            // expression's byte offset into the 1-based character cursor psql
            // renders as the `LINE n: ...` caret (0 if no source text).
            let location = nodeFuncs_seams::exprLocation::call(expr);
            let cursorpos =
                match small1_seams::parser_errposition::call(pstate, location) {
                    Ok(p) => p,
                    Err(e) => {
                        *err.borrow_mut() = Some(e);
                        return true;
                    }
                };
            let e = ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("invalid publication WHERE expression")
                .errdetail_internal(msg)
                .errposition(cursorpos)
                .into_error();
            *err.borrow_mut() = Some(e);
            return true;
        }
    }

    expression_tree_walker(node, &mut |n| {
        check_simple_rowfilter_expr_walker(n, pstate, err)
    })
}

/// `check_simple_rowfilter_expr(node, pstate)` (publicationcmds.c:691-695).
fn check_simple_rowfilter_expr(node: &Node, pstate: &ParseState<'_>) -> PgResult<()> {
    let err: RefCell<Option<PgError>> = RefCell::new(None);
    check_simple_rowfilter_expr_walker(node, pstate, &err);
    match err.into_inner() {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/* ===========================================================================
 * TransformPubWhereClauses (publicationcmds.c:706-767)
 * ======================================================================== */

/// `TransformPubWhereClauses(tables, queryString, pubviaroot)`
/// (publicationcmds.c:706-767).
fn TransformPubWhereClauses<'mcx>(
    mcx: Mcx<'mcx>,
    tables: &mut [PublicationRelInfo<'mcx>],
    query_string: Option<&str>,
    pubviaroot: bool,
) -> PgResult<()> {
    for pri in tables.iter_mut() {
        if pri.whereClause.is_none() {
            continue;
        }

        /*
         * If the publication doesn't publish changes via the root partitioned
         * table, the partition's row filter will be used. So disallow using
         * WHERE clause on partitioned table in this case.
         */
        if !pubviaroot && pri.relation.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "cannot use publication WHERE clause for relation \"{}\"",
                    pri.relation.name()
                ))
                .errdetail(format!(
                    "WHERE clause cannot be used for a partitioned table when {} is false.",
                    "publish_via_partition_root"
                ))
                .into_error());
        }

        /*
         * A fresh pstate is required so that we only have "this" table in its
         * rangetable.
         */
        let mut pstate = analyze_seams::make_parsestate::call(mcx, None)?;
        if let Some(qs) = query_string {
            pstate.p_sourcetext = Some(PgString::from_str_in(qs, mcx)?);
        }
        let nsitem = parser_relation::addRangeTableEntryForRelation(
            mcx,
            &mut pstate,
            &pri.relation,
            AccessShareLock,
            None,
            false,
            false,
        )?;
        parser_relation::addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;

        /* whereClause is Some(); copyObject + transform it. */
        let clause = pri.whereClause.take().expect("whereClause is Some");
        let clause_copy: Node<'mcx> = clause.clone_in(mcx)?;
        let whereclause =
            clause::transformWhereClause(
                mcx,
                &mut pstate,
                Some(clause_copy),
                ParseExprKind::EXPR_KIND_WHERE,
                "PUBLICATION WHERE",
            )?;

        /* Fix up collation information (on the Expr). The parser-arena `'static`
         * clause is brought into `mcx` for the in-place collation pass (pstate+
         * expr share one invariant `'mcx`), then erased back to the parser-arena
         * `'static` the `expand_generated_columns_in_expr` seam expects. */
        let mut whereclause: Option<nodes::primnodes::Expr<'mcx>> = match whereclause {
            Some(e) => Some(e.clone_in(mcx)?),
            None => None,
        };
        if let Some(expr) = whereclause.as_mut() {
            parse_collate::assign_expr_collations(Some(&pstate), expr)?;
        }
        let whereclause: Option<nodes::primnodes::Expr<'static>> =
            whereclause.map(|e| e.erase_lifetime());

        /*
         * `expand_generated_columns_in_expr(whereclause, rel, 1)` —
         * rewriteHandler.c. Owner-seam (declared in rewritehandler-seams).
         */
        let whereclause = rewrite_seams::expand_generated_columns_in_expr::call(
            mcx,
            whereclause,
            pri.relation.rd_id,
            1,
        )?;

        /* Re-wrap the transformed Expr as a walkable Node for storage. */
        let wherenode: Option<PgBox<'mcx, Node<'mcx>>> = match whereclause {
            Some(expr) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, expr.clone_in(mcx)?)?)?),
            None => None,
        };

        /*
         * We allow only simple expressions in row filters. See
         * check_simple_rowfilter_expr_walker.
         */
        if let Some(n) = wherenode.as_deref() {
            check_simple_rowfilter_expr(n, &pstate)?;
        }

        pri.whereClause = wherenode;
    }

    Ok(())
}

/* ===========================================================================
 * CheckPubRelationColumnList (publicationcmds.c:780-826)
 * ======================================================================== */

/// `CheckPubRelationColumnList(pubname, tables, publish_schema, pubviaroot)`
/// (publicationcmds.c:780-826).
fn CheckPubRelationColumnList<'mcx>(
    mcx: Mcx<'mcx>,
    pubname: &str,
    tables: &[PublicationRelInfo<'mcx>],
    publish_schema: bool,
    pubviaroot: bool,
) -> PgResult<()> {
    for pri in tables.iter() {
        if pri.columns.is_empty() {
            continue;
        }

        /* Disallow specifying column list if any schema is in the publication. */
        if publish_schema {
            let nspname = lsyscache::get_namespace_name::call(mcx, pri.relation.rd_rel.relnamespace)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "cannot use column list for relation \"{}.{}\" in publication \"{pubname}\"",
                    nspname.as_deref().unwrap_or(""),
                    pri.relation.name()
                ))
                .errdetail("Column lists cannot be specified in publications containing FOR TABLES IN SCHEMA elements.")
                .into_error());
        }

        /*
         * Disallow using a column list on the partitioned table if not
         * publishing via root.
         */
        if !pubviaroot && pri.relation.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            let nspname = lsyscache::get_namespace_name::call(mcx, pri.relation.rd_rel.relnamespace)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "cannot use column list for relation \"{}.{}\" in publication \"{pubname}\"",
                    nspname.as_deref().unwrap_or(""),
                    pri.relation.name()
                ))
                .errdetail(format!(
                    "Column lists cannot be specified for partitioned tables when {} is false.",
                    "publish_via_partition_root"
                ))
                .into_error());
        }
    }

    Ok(())
}

/* ===========================================================================
 * OpenTableList / CloseTableList / LockSchemaList (publicationcmds.c:1660-1853)
 * ======================================================================== */

/// `RangeVar` of a `PublicationTable.relation` node.
fn pubtable_rangevar<'a, 'mcx>(
    t: &'a PublicationTable<'mcx>,
) -> PgResult<&'a nodes::rawnodes::RangeVar<'mcx>> {
    match t.relation.as_deref().and_then(|n| n.as_rangevar()) {
        Some(rv) => Ok(rv),
        None => Err(PgError::error(
            "PublicationTable.relation is not a RangeVar",
        )),
    }
}

/// Bridge the parser-model `RangeVar<'mcx>` to the `table_open` substrate's
/// `types_tuple::access::RangeVar` (the relation-open layer predates the node
/// model and carries the plain owned struct).
fn to_access_rangevar(rv: &nodes::rawnodes::RangeVar<'_>) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_deref().map(|s| s.to_string()),
        schemaname: rv.schemaname.as_deref().map(|s| s.to_string()),
        relname: rv.relname.as_deref().unwrap_or("").to_string(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// `OpenTableList(tables)` (publicationcmds.c:1660-1805).
fn OpenTableList<'mcx>(
    mcx: Mcx<'mcx>,
    tables: &[PgBox<'mcx, PublicationTable<'mcx>>],
) -> PgResult<Vec<PublicationRelInfo<'mcx>>> {
    let mut relids: Vec<Oid> = Vec::new();
    let mut rels: Vec<PublicationRelInfo<'mcx>> = Vec::new();
    let mut relids_with_rf: Vec<Oid> = Vec::new();
    let mut relids_with_collist: Vec<Oid> = Vec::new();

    /* Open, share-lock, and check all the explicitly-specified relations. */
    for t in tables {
        let recurse = pubtable_rangevar(t)?.inh;
        let t_where = t.where_clause.is_some();
        let t_columns = !t.columns.is_empty();

        /* Allow query cancel in case this takes a long time */
        check_for_interrupts()?;

        let rel = {
            let rv = to_access_rangevar(pubtable_rangevar(t)?);
            table::table_openrv(mcx, &rv, ShareUpdateExclusiveLock)?
        };
        let myrelid = rel.rd_id;

        /* The walkable Node row filter (parser raw expr). */
        let resolved_where: Option<PgBox<'mcx, Node<'mcx>>> = match &t.where_clause {
            Some(w) => Some(mcx::alloc_in(mcx, w.clone_in(mcx)?)?),
            None => None,
        };

        /*
         * Filter out duplicates if user specifies "foo, foo".
         */
        if list_member_oid(&relids, myrelid) {
            /* Disallow duplicate tables if there are any with row filters. */
            if t_where || list_member_oid(&relids_with_rf, myrelid) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!(
                        "conflicting or redundant WHERE clauses for table \"{}\"",
                        rel.name()
                    ))
                    .into_error());
            }

            /* Disallow duplicate tables if there are any with column lists. */
            if t_columns || list_member_oid(&relids_with_collist, myrelid) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!(
                        "conflicting or redundant column lists for table \"{}\"",
                        rel.name()
                    ))
                    .into_error());
            }

            table::table_close(rel, ShareUpdateExclusiveLock)?;
            continue;
        }

        let is_partitioned = rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;
        rels.push(PublicationRelInfo {
            relation: rel,
            whereClause: resolved_where,
            columns: clone_columns(mcx, &t.columns)?,
        });
        relids.push(myrelid);

        if t_where {
            relids_with_rf.push(myrelid);
        }
        if t_columns {
            relids_with_collist.push(myrelid);
        }

        /*
         * Add children of this rel, if requested. A partitioned table can't
         * have any inheritance children other than its partitions.
         */
        if recurse && !is_partitioned {
            let children = inherits_seams::find_all_inheritors::call(
                mcx,
                myrelid,
                ShareUpdateExclusiveLock,
            )?;

            for childrelid in children.iter().copied() {
                /* Allow query cancel in case this takes a long time */
                check_for_interrupts()?;

                /* Skip duplicates if user specified both parent and child. */
                if list_member_oid(&relids, childrelid) {
                    if childrelid != myrelid
                        && (t_where || list_member_oid(&relids_with_rf, childrelid))
                    {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_DUPLICATE_OBJECT)
                            .errmsg(format!(
                                "conflicting or redundant WHERE clauses for table \"{}\"",
                                lsyscache::get_rel_name::call(mcx, childrelid)?
                                    .as_deref()
                                    .unwrap_or("")
                            ))
                            .into_error());
                    }

                    if childrelid != myrelid
                        && (t_columns || list_member_oid(&relids_with_collist, childrelid))
                    {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_DUPLICATE_OBJECT)
                            .errmsg(format!(
                                "conflicting or redundant column lists for table \"{}\"",
                                lsyscache::get_rel_name::call(mcx, childrelid)?
                                    .as_deref()
                                    .unwrap_or("")
                            ))
                            .into_error());
                    }

                    continue;
                }

                /* find_all_inheritors already got lock */
                let crel = table::table_open(mcx, childrelid, NoLock)?;
                let cwhere: Option<PgBox<'mcx, Node<'mcx>>> = match &t.where_clause {
                    Some(w) => Some(mcx::alloc_in(mcx, w.clone_in(mcx)?)?),
                    None => None,
                };
                rels.push(PublicationRelInfo {
                    relation: crel,
                    /* child inherits WHERE clause + column list from parent */
                    whereClause: cwhere,
                    columns: clone_columns(mcx, &t.columns)?,
                });
                relids.push(childrelid);

                if t_where {
                    relids_with_rf.push(childrelid);
                }
                if t_columns {
                    relids_with_collist.push(childrelid);
                }
            }
        }
    }

    Ok(rels)
}

/// `CloseTableList(rels)` (publicationcmds.c:1810-1824).
fn CloseTableList<'mcx>(rels: Vec<PublicationRelInfo<'mcx>>) -> PgResult<()> {
    for pub_rel in rels {
        table::table_close(pub_rel.relation, NoLock)?;
    }
    Ok(())
}

/// `LockSchemaList(schemalist)` (publicationcmds.c:1830-1853).
fn LockSchemaList<'mcx>(mcx: Mcx<'mcx>, schemalist: &[Oid]) -> PgResult<()> {
    for &schemaid in schemalist {
        /* Allow query cancel in case this takes a long time */
        check_for_interrupts()?;
        lmgr::LockDatabaseObject(
            NAMESPACE_RELATION_ID,
            schemaid,
            0,
            AccessShareLock,
        )?;

        /*
         * It is possible that by the time we acquire the lock on schema,
         * concurrent DDL has removed it.
         */
        if lsyscache::get_namespace_name::call(mcx, schemaid)?.is_none() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_SCHEMA)
                .errmsg(format!("schema with OID {schemaid} does not exist"))
                .into_error());
        }
    }

    Ok(())
}

/* ===========================================================================
 * PublicationAddTables / DropTables / AddSchemas / DropSchemas
 * ======================================================================== */

/// `PublicationAddTables(pubid, rels, if_not_exists, stmt)`
/// (publicationcmds.c:1858-1887).
fn PublicationAddTables<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    rels: &[PublicationRelInfo<'mcx>],
    if_not_exists: bool,
    stmt: Option<&AlterPublicationStmt<'mcx>>,
) -> PgResult<()> {
    for pub_rel in rels {
        let rel = &pub_rel.relation;

        /* Must be owner of the table or superuser. */
        if !object_ownercheck(mcx, RELATION_RELATION_ID, rel.rd_id, GetUserId())? {
            return Err(aclcheck_error_str(
                ACLCHECK_NOT_OWNER,
                ObjectType::Table,
                rel.name(),
            ));
        }

        let where_node: Option<&Node<'mcx>> = pub_rel.whereClause.as_deref();
        let columns: Option<&[PgBox<'mcx, Node<'mcx>>]> = if pub_rel.columns.is_empty() {
            None
        } else {
            Some(pub_rel.columns.as_slice())
        };

        let obj = pubcat::publication_add_relation::call(
            mcx,
            pubid,
            rel,
            where_node,
            columns,
            if_not_exists,
        )?;
        if let Some(stmt) = stmt {
            event_trigger_seams::event_trigger_collect_simple_command_publication::call(
                obj,
                InvalidObjectAddress,
                stmt.clone_in(mcx)?,
            )?;

            objaccess::invoke_object_post_create_hook::call(
                PublicationRelRelationId,
                obj.objectId,
                0,
            )?;
        }
    }

    Ok(())
}

/// `PublicationDropTables(pubid, rels, missing_ok)` (publicationcmds.c:1892-1932).
fn PublicationDropTables<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    rels: &[PublicationRelInfo<'mcx>],
    missing_ok: bool,
) -> PgResult<()> {
    for pubrel in rels {
        let rel = &pubrel.relation;
        let relid = rel.rd_id;

        if !pubrel.columns.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("column list must not be specified in ALTER PUBLICATION ... DROP")
                .into_error());
        }

        let prid = publication_rel_map_oid(mcx, relid, pubid)?;
        if !oid_is_valid(prid) {
            if missing_ok {
                continue;
            }
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "relation \"{}\" is not part of the publication",
                    rel.name()
                ))
                .into_error());
        }

        if pubrel.whereClause.is_some() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("cannot use a WHERE clause when removing a table from a publication")
                .into_error());
        }

        let obj = object_address_set(PublicationRelRelationId, prid);
        performDeletion(mcx, &obj, DROP_CASCADE, 0)?;
    }

    Ok(())
}

/// `PublicationAddSchemas(pubid, schemas, if_not_exists, stmt)`
/// (publicationcmds.c:1937-1960).
fn PublicationAddSchemas<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    schemas: &[Oid],
    if_not_exists: bool,
    stmt: Option<&AlterPublicationStmt<'mcx>>,
) -> PgResult<()> {
    for &schemaid in schemas {
        let obj = pubcat::publication_add_schema::call(mcx, pubid, schemaid, if_not_exists)?;
        if let Some(stmt) = stmt {
            event_trigger_seams::event_trigger_collect_simple_command_publication::call(
                obj,
                InvalidObjectAddress,
                stmt.clone_in(mcx)?,
            )?;

            objaccess::invoke_object_post_create_hook::call(
                PublicationNamespaceRelationId,
                obj.objectId,
                0,
            )?;
        }
    }

    Ok(())
}

/// `PublicationDropSchemas(pubid, schemas, missing_ok)`
/// (publicationcmds.c:1965-1994).
fn PublicationDropSchemas<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    schemas: &[Oid],
    missing_ok: bool,
) -> PgResult<()> {
    for &schemaid in schemas {
        let psid = publication_namespace_map_oid(mcx, schemaid, pubid)?;
        if !oid_is_valid(psid) {
            if missing_ok {
                continue;
            }
            let nspname = lsyscache::get_namespace_name::call(mcx, schemaid)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "tables from schema \"{}\" are not part of the publication",
                    nspname.as_deref().unwrap_or("")
                ))
                .into_error());
        }

        let obj = object_address_set(PublicationNamespaceRelationId, psid);
        performDeletion(mcx, &obj, DROP_CASCADE, 0)?;
    }

    Ok(())
}

/* ===========================================================================
 * InvalidatePublicationRels (publicationcmds.c:1177-1193)
 * ======================================================================== */

/// `InvalidatePublicationRels(relids)` (publicationcmds.c:1177-1193).
pub fn InvalidatePublicationRels(relids: &[Oid]) -> PgResult<()> {
    /*
     * We don't want to send too many individual messages, at some point it's
     * cheaper to just reset whole relcache.
     */
    if relids.len() < MAX_RELCACHE_INVAL_MSGS {
        for &relid in relids {
            CacheInvalidateRelcacheByRelid(relid)?;
        }
    } else {
        CacheInvalidateRelcacheAll()?;
    }

    Ok(())
}

/* ===========================================================================
 * CreatePublication (publicationcmds.c:831-975)
 * ======================================================================== */

/// `CreatePublication(pstate, stmt)` (publicationcmds.c:831-975).
pub fn CreatePublication<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    stmt: &CreatePublicationStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let pubname = stmt.pubname.as_deref().unwrap_or("");

    /* must have CREATE privilege on database */
    let my_database_id = globals_seams::MyDatabaseId::call()?;
    let aclresult = object_aclcheck(mcx, DATABASE_RELATION_ID, my_database_id, GetUserId(), ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        let dbname = dbcommands_seams::get_database_name::call(mcx, my_database_id)?;
        return Err(aclcheck_error_str(
            aclresult,
            ObjectType::Database,
            dbname.as_deref().unwrap_or(""),
        ));
    }

    /* FOR ALL TABLES requires superuser */
    if stmt.for_all_tables && !miscinit_seams::superuser::call(mcx)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to create FOR ALL TABLES publication")
            .into_error());
    }

    let rel = table::table_open(mcx, PublicationRelationId, RowExclusiveLock)?;

    /* Check if name is used */
    let puboid = lsyscache::get_publication_oid::call(pubname, true)?;
    if oid_is_valid(puboid) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("publication \"{pubname}\" already exists"))
            .into_error());
    }

    /* Form a tuple. */
    let mut values: [Datum<'mcx>; Natts_pg_publication] = core::array::from_fn(|_| Datum::null());
    let nulls = [false; Natts_pg_publication];
    let idx = |attno: i32| (attno - 1) as usize;

    /* validate the name (namein length check), then store the NameData image */
    let _ = namein(pubname)?;
    values[idx(Anum_pg_publication_pubname)] = name_datum(mcx, pubname)?;
    values[idx(Anum_pg_publication_pubowner)] = Datum::from_oid(GetUserId());

    let opts = parse_publication_options(mcx, pstate, &stmt.options)?;

    let puboid = GetNewOidWithIndex(&rel, PublicationObjectIndexId, Anum_pg_publication_oid as AttrNumber)?;
    values[idx(Anum_pg_publication_oid)] = Datum::from_oid(puboid);
    values[idx(Anum_pg_publication_puballtables)] = Datum::from_bool(stmt.for_all_tables);
    values[idx(Anum_pg_publication_pubinsert)] = Datum::from_bool(opts.pubactions.pubinsert);
    values[idx(Anum_pg_publication_pubupdate)] = Datum::from_bool(opts.pubactions.pubupdate);
    values[idx(Anum_pg_publication_pubdelete)] = Datum::from_bool(opts.pubactions.pubdelete);
    values[idx(Anum_pg_publication_pubtruncate)] = Datum::from_bool(opts.pubactions.pubtruncate);
    values[idx(Anum_pg_publication_pubviaroot)] = Datum::from_bool(opts.publish_via_partition_root);
    values[idx(Anum_pg_publication_pubgencols)] = Datum::from_char(opts.publish_generated_columns);

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)
        .map_err(|e| PgError::error(format!("heap_form_tuple failed: {e:?}")))?;

    /* Insert tuple into catalog. */
    CatalogTupleInsert(mcx, &rel, &mut tup)?;

    recordDependencyOnOwner(PublicationRelationId, puboid, GetUserId())?;

    let myself = object_address_set(PublicationRelationId, puboid);

    /* Make the changes visible. */
    CommandCounterIncrement()?;

    /* Associate objects with the publication. */
    if stmt.for_all_tables {
        /* Invalidate relcache so that publication info is rebuilt. */
        CacheInvalidateRelcacheAll()?;
    } else {
        let mut relations: Vec<PgBox<'mcx, PublicationTable<'mcx>>> = Vec::new();
        let mut schemaidlist: Vec<Oid> = Vec::new();
        ObjectsInPublicationToOids(mcx, &stmt.pubobjects, &mut relations, &mut schemaidlist)?;

        /* FOR TABLES IN SCHEMA requires superuser */
        if !schemaidlist.is_empty() && !miscinit_seams::superuser::call(mcx)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("must be superuser to create FOR TABLES IN SCHEMA publication")
                .into_error());
        }

        if !relations.is_empty() {
            let mut rels = OpenTableList(mcx, &relations)?;
            let source = pstate.p_sourcetext.as_deref();
            TransformPubWhereClauses(mcx, &mut rels, source, opts.publish_via_partition_root)?;

            CheckPubRelationColumnList(
                mcx,
                pubname,
                &rels,
                !schemaidlist.is_empty(),
                opts.publish_via_partition_root,
            )?;

            PublicationAddTables(mcx, puboid, &rels, true, None)?;
            CloseTableList(rels)?;
        }

        if !schemaidlist.is_empty() {
            /*
             * Schema lock is held until the publication is created to prevent
             * concurrent schema deletion.
             */
            LockSchemaList(mcx, &schemaidlist)?;
            PublicationAddSchemas(mcx, puboid, &schemaidlist, true, None)?;
        }
    }

    table::table_close(rel, RowExclusiveLock)?;

    objaccess::invoke_object_post_create_hook::call(PublicationRelationId, puboid, 0)?;

    if xlog_seams::wal_level::call() != wal::xlog_consts::WalLevel::Logical {
        ereport(WARNING)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("\"wal_level\" is insufficient to publish logical changes")
            .errhint("Set \"wal_level\" to \"logical\" before creating subscriptions.")
            .finish(errloc("CreatePublication"))?;
    }

    Ok(myself)
}

/* ===========================================================================
 * AlterPublicationOptions (publicationcmds.c:980-1172)
 * ======================================================================== */

/// `AlterPublicationOptions(pstate, stmt, rel, tup)` (publicationcmds.c:980-1172).
fn AlterPublicationOptions<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    stmt: &AlterPublicationStmt<'mcx>,
    rel: &rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<()> {
    let pubname = stmt.pubname.as_deref().unwrap_or("");

    let opts = parse_publication_options(mcx, pstate, &stmt.options)?;

    let pub_oid = pubform_oid(mcx, rel, tup)?;
    let pubform_puballtables = pubform_bool(mcx, rel, tup, Anum_pg_publication_puballtables)?;

    let mut root_relids: Vec<Oid> = Vec::new();

    /*
     * If the publication doesn't publish changes via the root partitioned
     * table, disallow WHERE clause and column lists on partitioned tables.
     */
    if !pubform_puballtables
        && opts.publish_via_partition_root_given
        && !opts.publish_via_partition_root
    {
        /* Lock the publication so nobody else can do anything with it. */
        lmgr::LockDatabaseObject(
            PublicationRelationId,
            pub_oid,
            0,
            AccessShareLock,
        )?;

        let rr = pubcat::GetPublicationRelations::call(mcx, pub_oid, PublicationPartOpt::Root)?;
        root_relids = rr.iter().copied().collect();

        for &relid in root_relids.iter() {
            /*
             * Beware: we don't have lock on the relations, so cope silently with
             * the cache lookups returning NULL.
             */
            let rftuple = match SearchSysCache2(
                mcx,
                PUBLICATIONRELMAP,
                oid_cache_key(relid),
                oid_cache_key(pub_oid),
            )? {
                Some(t) => t,
                None => continue,
            };

            let (_q, qnull) =
                SysCacheGetAttr(mcx, PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prqual)?;
            let (_a, anull) =
                SysCacheGetAttr(mcx, PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prattrs)?;
            let has_rowfilter = !qnull;
            let has_collist = !anull;
            if !has_rowfilter && !has_collist {
                ReleaseSysCache(rftuple);
                continue;
            }

            let relkind = lsyscache::get_rel_relkind::call(relid)?;
            if relkind != RELKIND_PARTITIONED_TABLE {
                ReleaseSysCache(rftuple);
                continue;
            }
            let relname = match lsyscache::get_rel_name::call(mcx, relid)? {
                Some(n) => n.as_str().to_string(),
                None => {
                    /* table concurrently dropped */
                    ReleaseSysCache(rftuple);
                    continue;
                }
            };

            ReleaseSysCache(rftuple);

            if has_rowfilter {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "cannot set parameter \"{}\" to false for publication \"{pubname}\"",
                        "publish_via_partition_root"
                    ))
                    .errdetail(format!(
                        "The publication contains a WHERE clause for partitioned table \"{relname}\", which is not allowed when \"{}\" is false.",
                        "publish_via_partition_root"
                    ))
                    .into_error());
            }
            /* Assert(has_collist); */
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "cannot set parameter \"{}\" to false for publication \"{pubname}\"",
                    "publish_via_partition_root"
                ))
                .errdetail(format!(
                    "The publication contains a column list for partitioned table \"{relname}\", which is not allowed when \"{}\" is false.",
                    "publish_via_partition_root"
                ))
                .into_error());
        }
    }

    /*
     * Everything ok, form a new tuple (heap_modify_tuple of the columns whose
     * *_given flag is set, CatalogTupleUpdate, CommandCounterIncrement).
     */
    let mut repl_values: [Datum<'mcx>; Natts_pg_publication] = core::array::from_fn(|_| Datum::null());
    let repl_nulls = [false; Natts_pg_publication];
    let mut replaces = [false; Natts_pg_publication];
    let idx = |attno: i32| (attno - 1) as usize;

    if opts.publish_given {
        repl_values[idx(Anum_pg_publication_pubinsert)] = Datum::from_bool(opts.pubactions.pubinsert);
        replaces[idx(Anum_pg_publication_pubinsert)] = true;
        repl_values[idx(Anum_pg_publication_pubupdate)] = Datum::from_bool(opts.pubactions.pubupdate);
        replaces[idx(Anum_pg_publication_pubupdate)] = true;
        repl_values[idx(Anum_pg_publication_pubdelete)] = Datum::from_bool(opts.pubactions.pubdelete);
        replaces[idx(Anum_pg_publication_pubdelete)] = true;
        repl_values[idx(Anum_pg_publication_pubtruncate)] = Datum::from_bool(opts.pubactions.pubtruncate);
        replaces[idx(Anum_pg_publication_pubtruncate)] = true;
    }

    if opts.publish_via_partition_root_given {
        repl_values[idx(Anum_pg_publication_pubviaroot)] = Datum::from_bool(opts.publish_via_partition_root);
        replaces[idx(Anum_pg_publication_pubviaroot)] = true;
    }

    if opts.publish_generated_columns_given {
        repl_values[idx(Anum_pg_publication_pubgencols)] = Datum::from_char(opts.publish_generated_columns);
        replaces[idx(Anum_pg_publication_pubgencols)] = true;
    }

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut newtup = heap_modify_tuple(mcx, tup, &tupdesc, &repl_values, &repl_nulls, &replaces)
        .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;

    /* Update the catalog. */
    let otid = newtup.tuple.t_self;
    CatalogTupleUpdate(mcx, rel, otid, &mut newtup)?;

    CommandCounterIncrement()?;

    let pub_oid = pubform_oid(mcx, rel, &newtup)?;
    let pubform_puballtables = pubform_bool(mcx, rel, &newtup, Anum_pg_publication_puballtables)?;

    /* Invalidate the relcache. */
    if pubform_puballtables {
        CacheInvalidateRelcacheAll()?;
    } else {
        let mut relids: Vec<Oid>;

        /*
         * For any partitioned tables contained in the publication, we must
         * invalidate all partitions, not just those explicitly mentioned.
         */
        if root_relids.is_empty() {
            let rr =
                pubcat::GetPublicationRelations::call(mcx, pub_oid, PublicationPartOpt::All)?;
            relids = rr.iter().copied().collect();
        } else {
            relids = Vec::new();
            for &root in root_relids.iter() {
                let base: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
                let got = pubcat::GetPubPartitionOptionRelations::call(
                    mcx,
                    base,
                    PublicationPartOpt::All,
                    root,
                )?;
                for r in got.iter() {
                    relids.push(*r);
                }
            }
        }

        let schemarelids =
            pubcat::GetAllSchemaPublicationRelations::call(mcx, pub_oid, PublicationPartOpt::All)?;
        relids = list_concat_unique_oid(relids, schemarelids.as_slice());

        InvalidatePublicationRels(&relids)?;
    }

    let obj = object_address_set(PublicationRelationId, pub_oid);
    event_trigger_seams::event_trigger_collect_simple_command_publication::call(
        obj,
        InvalidObjectAddress,
        stmt.clone_in(mcx)?,
    )?;

    objaccess::invoke_object_post_alter_hook::call(PublicationRelationId, pub_oid, 0)?;

    Ok(())
}

/* ===========================================================================
 * AlterPublicationTables (publicationcmds.c:1198-1353)
 * ======================================================================== */

/// `AlterPublicationTables(stmt, tup, tables, queryString, publish_schema)`
/// (publicationcmds.c:1198-1353).
fn AlterPublicationTables<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    stmt: &AlterPublicationStmt<'mcx>,
    tup: &FormedTuple<'mcx>,
    tables: &[PgBox<'mcx, PublicationTable<'mcx>>],
    query_string: Option<&str>,
    mut publish_schema: bool,
) -> PgResult<()> {
    let pubname = stmt.pubname.as_deref().unwrap_or("");
    let pubid = pubform_oid(mcx, rel, tup)?;
    let pubviaroot = pubform_bool(mcx, rel, tup, Anum_pg_publication_pubviaroot)?;
    let action = stmt.action;

    /*
     * Nothing to do if no objects, except in SET.
     */
    if tables.is_empty() && action != AP_SetObjects {
        return Ok(());
    }

    let mut rels = OpenTableList(mcx, tables)?;

    if action == AP_AddObjects {
        TransformPubWhereClauses(mcx, &mut rels, query_string, pubviaroot)?;

        publish_schema |= pubcat::is_schema_publication::call(mcx, pubid)?;

        CheckPubRelationColumnList(mcx, pubname, &rels, publish_schema, pubviaroot)?;

        PublicationAddTables(mcx, pubid, &rels, false, Some(stmt))?;
    } else if action == AP_DropObjects {
        PublicationDropTables(mcx, pubid, &rels, false)?;
    } else {
        /* AP_SetObjects */
        let oldrelids_v = pubcat::GetPublicationRelations::call(mcx, pubid, PublicationPartOpt::Root)?;
        let oldrelids: Vec<Oid> = oldrelids_v.iter().copied().collect();
        let mut delrels: Vec<PublicationRelInfo<'mcx>> = Vec::new();

        TransformPubWhereClauses(mcx, &mut rels, query_string, pubviaroot)?;

        CheckPubRelationColumnList(mcx, pubname, &rels, publish_schema, pubviaroot)?;

        /*
         * To recreate the relation list for the publication, look for existing
         * relations that do not need to be dropped.
         */
        for oldrelid in oldrelids {
            let mut found = false;
            let mut oldrelwhereclause: Option<PgBox<'mcx, Node<'mcx>>> = None;
            let mut oldcolumns: Option<Vec<i32>> = None;

            /* look up the cache for the old relmap */
            let rftuple = SearchSysCache2(
                mcx,
                PUBLICATIONRELMAP,
                oid_cache_key(oldrelid),
                oid_cache_key(pubid),
            )?;

            if let Some(rftuple) = rftuple {
                /* Load the WHERE clause for this table. */
                let (qdatum, qnull) =
                    SysCacheGetAttr(mcx, PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prqual)?;
                if !qnull {
                    let s = text_datum_str(&qdatum);
                    oldrelwhereclause = Some(read_seams::string_to_node::call(mcx, &s)?);
                }

                /* Transform the int2vector column list to a bitmap. */
                let (adatum, anull) =
                    SysCacheGetAttr(mcx, PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prattrs)?;
                if !anull {
                    let elems = int2vector_elems(adatum.as_ref_bytes());
                    oldcolumns = Some(elems.into_iter().map(|e| e as i32).collect());
                }

                ReleaseSysCache(rftuple);
            }

            for newpubrel in rels.iter() {
                let newrelid = newpubrel.relation.rd_id;

                /*
                 * Validate the column list.  If the column list or WHERE clause
                 * changes, then the validation done here will be duplicated
                 * inside PublicationAddTables().
                 */
                let newcols_opt: Option<&[PgBox<'mcx, Node<'mcx>>]> = if newpubrel.columns.is_empty()
                {
                    None
                } else {
                    Some(newpubrel.columns.as_slice())
                };
                let newcols_bms = match newcols_opt {
                    Some(cols) => {
                        pubcat::pub_collist_validate::call(mcx, &newpubrel.relation, cols)?
                    }
                    None => None,
                };
                let newcolumns: Vec<i32> = bitmapset_members(newcols_bms.as_deref());

                /*
                 * Check if any of the new set of relations matches with the
                 * existing relations in the publication.
                 */
                if newrelid == oldrelid
                    && nodes_equal(
                        oldrelwhereclause.as_deref(),
                        newpubrel.whereClause.as_deref(),
                    )
                    && bms_equal_opt(oldcolumns.as_deref(), &newcolumns)
                {
                    found = true;
                    break;
                }
            }

            /* Add the non-matched relations to a list so they can be dropped. */
            if !found {
                let oldrel = table::table_open(
                    mcx,
                    oldrelid,
                    ShareUpdateExclusiveLock,
                )?;
                delrels.push(PublicationRelInfo {
                    relation: oldrel,
                    whereClause: None,
                    columns: mcx::vec_with_capacity_in(mcx, 0)?,
                });
            }
        }

        /* And drop them. */
        PublicationDropTables(mcx, pubid, &delrels, true)?;

        /*
         * Don't bother calculating the difference for adding, we'll catch and
         * skip existing ones when doing catalog update.
         */
        PublicationAddTables(mcx, pubid, &rels, true, Some(stmt))?;

        CloseTableList(delrels)?;
    }

    CloseTableList(rels)?;

    Ok(())
}

/// `equal(a, b)` over two row-filter `Node`s (`equalfuncs.c`).  In C `equal(NULL,
/// NULL)` is true; otherwise `equal_node` does the structural comparison.
fn nodes_equal(a: Option<&Node>, b: Option<&Node>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_node(x, y),
        _ => false,
    }
}

/* ===========================================================================
 * AlterPublicationSchemas (publicationcmds.c:1360-1438)
 * ======================================================================== */

/// `AlterPublicationSchemas(stmt, tup, schemaidlist)` (publicationcmds.c:1360-1438).
fn AlterPublicationSchemas<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    stmt: &AlterPublicationStmt<'mcx>,
    tup: &FormedTuple<'mcx>,
    schemaidlist: &[Oid],
) -> PgResult<()> {
    let pubname = stmt.pubname.as_deref().unwrap_or("");
    let pubid = pubform_oid(mcx, rel, tup)?;
    let action = stmt.action;

    /* Nothing to do if no objects, except in SET. */
    if schemaidlist.is_empty() && action != AP_SetObjects {
        return Ok(());
    }

    /*
     * Schema lock is held until the publication is altered to prevent
     * concurrent schema deletion.
     */
    LockSchemaList(mcx, schemaidlist)?;
    if action == AP_AddObjects {
        let reloids = pubcat::GetPublicationRelations::call(mcx, pubid, PublicationPartOpt::Root)?;

        for relid in reloids.iter().copied() {
            let coltuple = match SearchSysCache2(
                mcx,
                PUBLICATIONRELMAP,
                oid_cache_key(relid),
                oid_cache_key(pubid),
            )? {
                Some(t) => t,
                None => continue,
            };

            /*
             * Disallow adding schema if column list is already part of the
             * publication.  See CheckPubRelationColumnList.
             */
            let (_a, anull) =
                SysCacheGetAttr(mcx, PUBLICATIONRELMAP, &coltuple, Anum_pg_publication_rel_prattrs)?;
            if !anull {
                ReleaseSysCache(coltuple);
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("cannot add schema to publication \"{pubname}\""))
                    .errdetail("Schemas cannot be added if any tables that specify a column list are already part of the publication.")
                    .into_error());
            }

            ReleaseSysCache(coltuple);
        }

        PublicationAddSchemas(mcx, pubid, schemaidlist, false, Some(stmt))?;
    } else if action == AP_DropObjects {
        PublicationDropSchemas(mcx, pubid, schemaidlist, false)?;
    } else {
        /* AP_SetObjects */
        let oldschemaids_v = pubcat::GetPublicationSchemas::call(mcx, pubid)?;
        let oldschemaids: Vec<Oid> = oldschemaids_v.iter().copied().collect();

        /* Identify which schemas should be dropped */
        let delschemas = list_difference_oid(&oldschemaids, schemaidlist);

        /*
         * Schema lock is held until the publication is altered to prevent
         * concurrent schema deletion.
         */
        LockSchemaList(mcx, &delschemas)?;

        /* And drop them */
        PublicationDropSchemas(mcx, pubid, &delschemas, true)?;

        /*
         * Don't bother calculating the difference for adding, we'll catch and
         * skip existing ones when doing catalog update.
         */
        PublicationAddSchemas(mcx, pubid, schemaidlist, true, Some(stmt))?;
    }

    Ok(())
}

/* ===========================================================================
 * CheckAlterPublication (publicationcmds.c:1444-1474)
 * ======================================================================== */

/// `CheckAlterPublication(stmt, tup, tables, schemaidlist)`
/// (publicationcmds.c:1444-1474).
fn CheckAlterPublication<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    stmt: &AlterPublicationStmt<'mcx>,
    tup: &FormedTuple<'mcx>,
    has_tables: bool,
    has_schemas: bool,
) -> PgResult<()> {
    let action = stmt.action;
    let puballtables = pubform_bool(mcx, rel, tup, Anum_pg_publication_puballtables)?;

    if (action == AP_AddObjects || action == AP_SetObjects)
        && has_schemas
        && !miscinit_seams::superuser::call(mcx)?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to add or set schemas")
            .into_error());
    }

    /* Check that user is allowed to manipulate the publication tables in schema. */
    if has_schemas && puballtables {
        let pubname = pubform_pubname(mcx, rel, tup)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("publication \"{pubname}\" is defined as FOR ALL TABLES"))
            .errdetail("Schemas cannot be added to or dropped from FOR ALL TABLES publications.")
            .into_error());
    }

    /* Check that user is allowed to manipulate the publication tables. */
    if has_tables && puballtables {
        let pubname = pubform_pubname(mcx, rel, tup)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("publication \"{pubname}\" is defined as FOR ALL TABLES"))
            .errdetail("Tables cannot be added to or dropped from FOR ALL TABLES publications.")
            .into_error());
    }

    Ok(())
}

/* ===========================================================================
 * AlterPublication (publicationcmds.c:1482-1547)
 * ======================================================================== */

/// `AlterPublication(pstate, stmt)` (publicationcmds.c:1482-1547).
pub fn AlterPublication<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    stmt: &AlterPublicationStmt<'mcx>,
) -> PgResult<()> {
    let pubname = stmt.pubname.as_deref().unwrap_or("");

    let rel = table::table_open(mcx, PublicationRelationId, RowExclusiveLock)?;

    let pubid = lsyscache::get_publication_oid::call(pubname, true)?;
    if !oid_is_valid(pubid) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("publication \"{pubname}\" does not exist"))
            .into_error());
    }

    let mut tup = match SearchSysCache1(mcx, PUBLICATIONOID, oid_cache_key(pubid))? {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("publication \"{pubname}\" does not exist"))
                .into_error())
        }
    };

    let pub_oid = pubform_oid(mcx, &rel, &tup)?;

    /* must be owner */
    if !object_ownercheck(mcx, PublicationRelationId, pub_oid, GetUserId())? {
        ReleaseSysCache(tup);
        table::table_close(rel, RowExclusiveLock)?;
        return Err(aclcheck_error_str(
            ACLCHECK_NOT_OWNER,
            ObjectType::Publication,
            pubname,
        ));
    }

    if !stmt.options.is_empty() {
        AlterPublicationOptions(mcx, pstate, stmt, &rel, &tup)?;
    } else {
        let mut relations: Vec<PgBox<'mcx, PublicationTable<'mcx>>> = Vec::new();
        let mut schemaidlist: Vec<Oid> = Vec::new();
        let pubid = pub_oid;

        ObjectsInPublicationToOids(mcx, &stmt.pubobjects, &mut relations, &mut schemaidlist)?;

        CheckAlterPublication(
            mcx,
            &rel,
            stmt,
            &tup,
            !relations.is_empty(),
            !schemaidlist.is_empty(),
        )?;

        ReleaseSysCache(tup);

        /* Lock the publication so nobody else can do anything with it. */
        lmgr::LockDatabaseObject(
            PublicationRelationId,
            pubid,
            0,
            AccessExclusiveLock,
        )?;

        /*
         * It is possible that by the time we acquire the lock on publication,
         * concurrent DDL has removed it.
         */
        tup = match SearchSysCache1(mcx, PUBLICATIONOID, oid_cache_key(pubid))? {
            Some(t) => t,
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("publication \"{pubname}\" does not exist"))
                    .into_error())
            }
        };

        let source = pstate.p_sourcetext.as_deref();
        AlterPublicationTables(
            mcx,
            &rel,
            stmt,
            &tup,
            &relations,
            source,
            !schemaidlist.is_empty(),
        )?;
        AlterPublicationSchemas(mcx, &rel, stmt, &tup, &schemaidlist)?;
    }

    /* Cleanup. */
    ReleaseSysCache(tup);
    table::table_close(rel, RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * RemovePublicationRelById / RemovePublicationById / RemovePublicationSchemaById
 * ======================================================================== */

/// `RemovePublicationRelById(proid)` (publicationcmds.c:1552-1588).
///
/// The C uses `SearchSysCache1(PUBLICATIONREL, proid)`, but this repo has no
/// by-oid `PUBLICATIONREL` syscache; mirror the lookup as a `systable` index
/// scan on `pg_publication_rel_oid_index` instead.
pub fn RemovePublicationRelById<'mcx>(mcx: Mcx<'mcx>, proid: Oid) -> PgResult<()> {
    let rel =
        table::table_open(mcx, PublicationRelRelationId, RowExclusiveLock)?;

    let keys = [oid_key(Anum_pg_publication_rel_oid, proid)?];
    let mut scan =
        genam::systable_beginscan::call(&rel, PublicationRelObjectIndexId, true, None, &keys)?;

    let tup = match genam::systable_getnext::call(mcx, scan.desc_mut())? {
        Some(t) => t,
        None => {
            scan.end()?;
            table::table_close(rel, RowExclusiveLock)?;
            return Err(PgError::error(format!(
                "cache lookup failed for publication table {proid}"
            )));
        }
    };

    let cols = deform(mcx, &rel, &tup)?;
    let prrelid = cols[(Anum_pg_publication_rel_prrelid - 1) as usize].0.as_oid();

    /*
     * Invalidate relcache so that publication info is rebuilt.  For partitioned
     * tables, invalidate all partitions in the hierarchies.
     */
    let base: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    let relids =
        pubcat::GetPubPartitionOptionRelations::call(mcx, base, PublicationPartOpt::All, prrelid)?;
    let relids_v: Vec<Oid> = relids.iter().copied().collect();

    InvalidatePublicationRels(&relids_v)?;

    let tid = tup.tuple.t_self;
    CatalogTupleDelete(mcx, &rel, tid)?;

    scan.end()?;
    table::table_close(rel, RowExclusiveLock)?;

    Ok(())
}

/// `RemovePublicationById(pubid)` (publicationcmds.c:1593-1617).
pub fn RemovePublicationById<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, PublicationRelationId, RowExclusiveLock)?;

    let tup = match SearchSysCache1(mcx, PUBLICATIONOID, oid_cache_key(pubid))? {
        Some(t) => t,
        None => {
            table::table_close(rel, RowExclusiveLock)?;
            return Err(PgError::error(format!(
                "cache lookup failed for publication {pubid}"
            )));
        }
    };

    /* Invalidate relcache so that publication info is rebuilt. */
    if pubform_bool(mcx, &rel, &tup, Anum_pg_publication_puballtables)? {
        CacheInvalidateRelcacheAll()?;
    }

    let tid = tup.tuple.t_self;
    CatalogTupleDelete(mcx, &rel, tid)?;

    ReleaseSysCache(tup);

    table::table_close(rel, RowExclusiveLock)?;

    Ok(())
}

/// `RemovePublicationSchemaById(psoid)` (publicationcmds.c:1622-1653).
///
/// The C uses `SearchSysCache1(PUBLICATIONNAMESPACE, psoid)`, but this repo has
/// no by-oid `PUBLICATIONNAMESPACE` syscache; mirror as a `systable` index scan
/// on `pg_publication_namespace_oid_index`.
pub fn RemovePublicationSchemaById<'mcx>(mcx: Mcx<'mcx>, psoid: Oid) -> PgResult<()> {
    let rel = table::table_open(
        mcx,
        PublicationNamespaceRelationId,
        RowExclusiveLock,
    )?;

    let keys = [oid_key(Anum_pg_publication_namespace_oid, psoid)?];
    let mut scan = genam::systable_beginscan::call(
        &rel,
        PublicationNamespaceObjectIndexId,
        true,
        None,
        &keys,
    )?;

    let tup = match genam::systable_getnext::call(mcx, scan.desc_mut())? {
        Some(t) => t,
        None => {
            scan.end()?;
            table::table_close(rel, RowExclusiveLock)?;
            return Err(PgError::error(format!(
                "cache lookup failed for publication schema {psoid}"
            )));
        }
    };

    let cols = deform(mcx, &rel, &tup)?;
    // Anum_pg_publication_namespace_pnnspid = 3.
    let pnnspid = cols[2].0.as_oid();

    /*
     * Invalidate relcache so that publication info is rebuilt.  See
     * RemovePublicationRelById for why we need to consider all the partitions.
     */
    let schema_rels =
        pubcat::GetSchemaPublicationRelations::call(mcx, pnnspid, PublicationPartOpt::All)?;
    let schema_rels_v: Vec<Oid> = schema_rels.iter().copied().collect();
    InvalidatePublicationRels(&schema_rels_v)?;

    let tid = tup.tuple.t_self;
    CatalogTupleDelete(mcx, &rel, tid)?;

    scan.end()?;
    table::table_close(rel, RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * AlterPublicationOwner_internal / AlterPublicationOwner(_oid)
 * ======================================================================== */

/// `AlterPublicationOwner_internal(rel, tup, newOwnerId)`
/// (publicationcmds.c:1999-2052).
fn AlterPublicationOwner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
    new_owner_id: Oid,
) -> PgResult<()> {
    let form_oid = pubform_oid(mcx, rel, tup)?;
    let form_owner = pubform_oid_attr(mcx, rel, tup, Anum_pg_publication_pubowner)?;
    let form_puballtables = pubform_bool(mcx, rel, tup, Anum_pg_publication_puballtables)?;

    if form_owner == new_owner_id {
        return Ok(());
    }

    if !miscinit_seams::superuser::call(mcx)? {
        /* Must be owner */
        if !object_ownercheck(mcx, PublicationRelationId, form_oid, GetUserId())? {
            let pubname = pubform_pubname(mcx, rel, tup)?;
            return Err(aclcheck_error_str(
                ACLCHECK_NOT_OWNER,
                ObjectType::Publication,
                &pubname,
            ));
        }

        /* Must be able to become new owner */
        acl_seams::check_can_set_role::call(GetUserId(), new_owner_id)?;

        /* New owner must have CREATE privilege on database */
        let my_database_id = globals_seams::MyDatabaseId::call()?;
        let aclresult =
            object_aclcheck(mcx, DATABASE_RELATION_ID, my_database_id, new_owner_id, ACL_CREATE)?;
        if aclresult != ACLCHECK_OK {
            let dbname = dbcommands_seams::get_database_name::call(mcx, my_database_id)?;
            return Err(aclcheck_error_str(
                aclresult,
                ObjectType::Database,
                dbname.as_deref().unwrap_or(""),
            ));
        }

        if form_puballtables && !miscinit_seams::superuser_arg::call(new_owner_id)? {
            let pubname = pubform_pubname(mcx, rel, tup)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "permission denied to change owner of publication \"{pubname}\""
                ))
                .errhint("The owner of a FOR ALL TABLES publication must be a superuser.")
                .into_error());
        }

        if !miscinit_seams::superuser_arg::call(new_owner_id)?
            && pubcat::is_schema_publication::call(mcx, form_oid)?
        {
            let pubname = pubform_pubname(mcx, rel, tup)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "permission denied to change owner of publication \"{pubname}\""
                ))
                .errhint("The owner of a FOR TABLES IN SCHEMA publication must be a superuser.")
                .into_error());
        }
    }

    /*
     * form->pubowner = newOwnerId; CatalogTupleUpdate; changeDependencyOnOwner;
     * InvokeObjectPostAlterHook.
     */
    let mut repl_values: [Datum<'mcx>; Natts_pg_publication] = core::array::from_fn(|_| Datum::null());
    let repl_nulls = [false; Natts_pg_publication];
    let mut replaces = [false; Natts_pg_publication];
    let idx = (Anum_pg_publication_pubowner - 1) as usize;
    repl_values[idx] = Datum::from_oid(new_owner_id);
    replaces[idx] = true;

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut newtup = heap_modify_tuple(mcx, tup, &tupdesc, &repl_values, &repl_nulls, &replaces)
        .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;

    let otid = newtup.tuple.t_self;
    CatalogTupleUpdate(mcx, rel, otid, &mut newtup)?;

    /* Update owner dependency reference */
    changeDependencyOnOwner(PublicationRelationId, form_oid, new_owner_id)?;

    objaccess::invoke_object_post_alter_hook::call(PublicationRelationId, form_oid, 0)?;

    Ok(())
}

/// `AlterPublicationOwner(name, newOwnerId)` (publicationcmds.c:2057-2087).
pub fn AlterPublicationOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    new_owner_id: Oid,
) -> PgResult<ObjectAddress> {
    let rel = table::table_open(mcx, PublicationRelationId, RowExclusiveLock)?;

    let pubid = lsyscache::get_publication_oid::call(name, true)?;
    if !oid_is_valid(pubid) {
        table::table_close(rel, RowExclusiveLock)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("publication \"{name}\" does not exist"))
            .into_error());
    }

    let tup = match SearchSysCache1(mcx, PUBLICATIONOID, oid_cache_key(pubid))? {
        Some(t) => t,
        None => {
            table::table_close(rel, RowExclusiveLock)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("publication \"{name}\" does not exist"))
                .into_error());
        }
    };

    AlterPublicationOwner_internal(mcx, &rel, &tup, new_owner_id)?;

    let address = object_address_set(PublicationRelationId, pubid);

    ReleaseSysCache(tup);

    table::table_close(rel, RowExclusiveLock)?;

    Ok(address)
}

/// `AlterPublicationOwner_oid(pubid, newOwnerId)` (publicationcmds.c:2092-2112).
pub fn AlterPublicationOwner_oid<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, PublicationRelationId, RowExclusiveLock)?;

    let tup = match SearchSysCache1(mcx, PUBLICATIONOID, oid_cache_key(pubid))? {
        Some(t) => t,
        None => {
            table::table_close(rel, RowExclusiveLock)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("publication with OID {pubid} does not exist"))
                .into_error());
        }
    };

    AlterPublicationOwner_internal(mcx, &rel, &tup, new_owner_id)?;

    ReleaseSysCache(tup);

    table::table_close(rel, RowExclusiveLock)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers reading a pg_publication tuple's fixed columns (GETSTRUCT analog).
// ---------------------------------------------------------------------------

fn pubform_oid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<Oid> {
    pubform_oid_attr(mcx, rel, tup, Anum_pg_publication_oid)
}

fn pubform_oid_attr<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
    attno: i32,
) -> PgResult<Oid> {
    let cols = deform(mcx, rel, tup)?;
    Ok(cols[(attno - 1) as usize].0.as_oid())
}

fn pubform_bool<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
    attno: i32,
) -> PgResult<bool> {
    let cols = deform(mcx, rel, tup)?;
    Ok(cols[(attno - 1) as usize].0.as_bool())
}

fn pubform_pubname<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<String> {
    let cols = deform(mcx, rel, tup)?;
    Ok(name_str(cols[(Anum_pg_publication_pubname - 1) as usize].0.as_ref_bytes()).to_string())
}

// ---------------------------------------------------------------------------
// publication_rel / publication_namespace map-oid lookups.
// ---------------------------------------------------------------------------

/// `GetSysCacheOid2(PUBLICATIONRELMAP, ..., relid, pubid)` — the pg_publication_rel
/// OID for (relid, pubid), or InvalidOid.
fn publication_rel_map_oid<'mcx>(mcx: Mcx<'mcx>, relid: Oid, pubid: Oid) -> PgResult<Oid> {
    let tup = SearchSysCache2(
        mcx,
        PUBLICATIONRELMAP,
        oid_cache_key(relid),
        oid_cache_key(pubid),
    )?;
    match tup {
        Some(t) => {
            let (v, _isnull) =
                SysCacheGetAttr(mcx, PUBLICATIONRELMAP, &t, Anum_pg_publication_rel_oid)?;
            let oid = v.as_oid();
            ReleaseSysCache(t);
            Ok(oid)
        }
        None => Ok(InvalidOid),
    }
}

/// `GetSysCacheOid2(PUBLICATIONNAMESPACEMAP, ..., schemaid, pubid)` — the
/// pg_publication_namespace OID for (schemaid, pubid), or InvalidOid.
fn publication_namespace_map_oid<'mcx>(mcx: Mcx<'mcx>, schemaid: Oid, pubid: Oid) -> PgResult<Oid> {
    use types_syscache::syscache_ids::PUBLICATIONNAMESPACEMAP;
    let tup = SearchSysCache2(
        mcx,
        PUBLICATIONNAMESPACEMAP,
        oid_cache_key(schemaid),
        oid_cache_key(pubid),
    )?;
    match tup {
        Some(t) => {
            let (v, _isnull) =
                SysCacheGetAttr(mcx, PUBLICATIONNAMESPACEMAP, &t, Anum_pg_publication_namespace_oid)?;
            let oid = v.as_oid();
            ReleaseSysCache(t);
            Ok(oid)
        }
        None => Ok(InvalidOid),
    }
}

// ---------------------------------------------------------------------------
// Relation index-attribute / generated-column helpers (RelationGetIndexAttrBitmap
// / generated-column inspection on the opened relation's tupdesc).
// ---------------------------------------------------------------------------

/// `RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY)` —
/// returns the set of replica-identity columns, offset by
/// `FirstLowInvalidHeapAttributeNumber` (the C bitmap convention).
fn relation_get_replident_attrs<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &rel::Relation<'mcx>,
) -> PgResult<Vec<i32>> {
    let bms = relcache_seams::relation_get_index_attr_bitmap::call(
        mcx,
        relation,
        relcache_entry::IndexAttrBitmapKind::Identity,
    )?;
    Ok(bitmapset_members(bms.as_deref()))
}

/// `tupdesc->attrs[attnum-1].attgenerated` for an opened relation.
fn tupdesc_attgenerated<'mcx>(relation: &rel::Relation<'mcx>, attnum: i16) -> i8 {
    if attnum < 1 {
        return 0;
    }
    let i = (attnum - 1) as usize;
    if i >= relation.rd_att.natts as usize {
        return 0;
    }
    relation.rd_att.attr(i).attgenerated
}

/// Whether any (non-dropped) column of the relation is a STORED generated column.
fn relation_has_generated_stored<'mcx>(relation: &rel::Relation<'mcx>) -> bool {
    let desc = &relation.rd_att;
    for i in 0..(desc.natts as usize) {
        let att = desc.attr(i);
        if !att.attisdropped && att.attgenerated == types_tuple::access::ATTRIBUTE_GENERATED_STORED as i8 {
            return true;
        }
    }
    false
}

/// Whether any (non-dropped) column of the relation is a VIRTUAL generated column.
fn relation_has_generated_virtual<'mcx>(relation: &rel::Relation<'mcx>) -> bool {
    let desc = &relation.rd_att;
    for i in 0..(desc.natts as usize) {
        let att = desc.attr(i);
        if !att.attisdropped && att.attgenerated == types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL as i8
        {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Misc small helpers.
// ---------------------------------------------------------------------------

/// Enumerate the members of an optional `Bitmapset` (ascending) into a `Vec`.
fn bitmapset_members(bms: Option<&nodes::bitmapset::Bitmapset<'_>>) -> Vec<i32> {
    let mut out = Vec::new();
    let mut i = -1;
    loop {
        i = nodes_core::bitmapset::bms_next_member(bms, i);
        if i < 0 {
            break;
        }
        out.push(i);
    }
    out
}

/// `TextDatumGetCString(datum)` — the cstring payload of a `text` varlena Datum
/// (skipping the 4-byte length header).
fn text_datum_str(datum: &Datum<'_>) -> String {
    let bytes = datum.as_ref_bytes();
    // VARDATA_ANY: the source is a stored catalog `text` attribute
    // (SysCacheGetAttr on pg_publication_rel.prqual), which arrives short-headed
    // (1-byte, low-bit-set) once SHORT_VARLENA_PACKING is on; skip ONE byte for
    // it, else the ordinary 4-byte VARHDRSZ. A fixed 4-byte strip would drop
    // three payload bytes. No-op while the flag is off.
    let data = match bytes.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &bytes[1..],
        Some(_) if bytes.len() >= 4 => &bytes[4..],
        _ => return String::new(),
    };
    String::from_utf8_lossy(data).into_owned()
}

/// `aclcheck_error(aclerr, objtype, objectname)` (aclchk.c) — the C is
/// `pg_noreturn`; this helper always returns the resulting `PgError`.
fn aclcheck_error_str(aclerr: AclResult, objtype: ObjectType, objectname: &str) -> PgError {
    match aclchk_seams::aclcheck_error::call(aclerr, objtype, Some(objectname.to_string())) {
        Ok(()) => ereport(ERROR)
            .errmsg_internal("aclcheck_error seam returned without raising")
            .into_error(),
        Err(e) => e,
    }
}

/// `CHECK_FOR_INTERRUPTS()` — process any pending interrupt (may `ereport`).
fn check_for_interrupts() -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()
}

/// `ereport` location helper for `publicationcmds.c`.
fn errloc(funcname: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(
        "../src/backend/commands/publicationcmds.c",
        0,
        funcname,
    )
}

/// Install this unit's six inward seams. The seam contracts are `Mcx`-free; each
/// wrapper spins a fresh `MemoryContext` to obtain an `Mcx` for the ported body
/// (the established bridging idiom, cf. `backend-commands-foreigncmds`).
/// Outward-seam adapter for `CreatePublication` (utility.c:1845,
/// `T_CreatePublicationStmt`): downcast the arena
/// [`nodes::nodes::Node`] and run the ported body.
fn create_publication_seam<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &nodes::nodes::Node<'mcx>,
) -> PgResult<ObjectAddress> {
    let cps = match stmt.as_createpublicationstmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "create_publication_seam: statement is not a CreatePublicationStmt",
            ))
        }
    };
    CreatePublication(mcx, pstate, cps)
}

/// Outward-seam adapter for `AlterPublication` (utility.c:1849,
/// `T_AlterPublicationStmt`).
fn alter_publication_seam<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &nodes::nodes::Node<'mcx>,
) -> PgResult<()> {
    let aps = match stmt.as_alterpublicationstmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "alter_publication_seam: statement is not an AlterPublicationStmt",
            ))
        }
    };
    AlterPublication(mcx, pstate, aps)
}

pub fn init_seams() {
    inward::init_seams();

    // utility.c `ProcessUtilitySlow` dispatches CREATE/ALTER PUBLICATION through
    // tcop-utility-out-seams.
    utility_out_seams::create_publication::set(create_publication_seam);
    utility_out_seams::alter_publication::set(alter_publication_seam);
}
