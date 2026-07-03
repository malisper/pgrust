// DefineIndex + ComputeIndexAttrs + CheckPredicate + ChooseIndex*Name*
// (indexcmds.c), btree/hash lane. Loud: CONCURRENTLY, INCLUDE, named
// opclasses/collations, WITH options, TABLESPACE, exclusion/WITHOUT OVERLAPS,
// partitioned tables, non-btree/hash AMs.
use catalog_index::{
    IndexCreateExtra, BTREE_AM_OID,
    INDEX_CREATE_ADD_CONSTRAINT, INDEX_CREATE_IS_PRIMARY,
};
use datum::Datum;
use execindexing::IndexInfo;
use mcx::{Mcx, PgString, PgVec};
use types_core::{
    AttrNumber, InvalidOid, Oid, RegProcedure, INDEX_MAX_KEYS, NAMEDATALEN,
    RELATION_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INDETERMINATE_COLLATION,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::rawnodes::{IndexElem, IndexStmt, SortByDir, SortByNulls};
use types_rel::{Relation, ShareLock, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use crate::GetDefaultOpClass;

const NamespaceRelationId: Oid = 2615;
const ClassNameNspIndexId: Oid = 2663;
const Anum_pg_class_relname: AttrNumber = 2;
const Anum_pg_class_relnamespace: AttrNumber = 3;
const F_NAMEEQ: RegProcedure = 62;
const F_OIDEQ: RegProcedure = 184;
const ACL_CREATE: u64 = 1 << 9;
const INDOPTION_DESC: i16 = 1 << 0;
const INDOPTION_NULLS_FIRST: i16 = 1 << 1;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: indexcmds {what}")
}

#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

pub(crate) fn define_index_for_alter<'mcx>(
    mcx: Mcx<'mcx>,
    table_id: Oid,
    stmt_node: types_nodes::Node<'mcx>,
    skip_build: bool,
) -> PgResult<Oid> {
    let stmt = stmt_node.as_variant::<IndexStmt>().expect("IndexStmt");
    DefineIndex(mcx, table_id, stmt, InvalidOid, true, true, false, skip_build, false)
}

pub fn DefineIndex<'mcx>(
    mcx: Mcx<'mcx>,
    tableId: Oid,
    stmt: &IndexStmt<'mcx>,
    indexRelationId: Oid,
    is_alter_table: bool,
    check_rights: bool,
    check_not_in_use: bool,
    skip_build: bool,
    quiet: bool,
) -> PgResult<Oid> {
    let _ = (is_alter_table, quiet);
    if stmt.concurrent {
        unported("DefineIndex: CONCURRENTLY");
    }
    if stmt.reset_default_tblspc {
        unported("DefineIndex: reset_default_tblspc");
    }
    if !stmt.excludeOpNames.is_nil() || stmt.iswithoutoverlaps {
        unported("DefineIndex: exclusion / WITHOUT OVERLAPS constraints");
    }
    if !stmt.indexIncludingParams.is_nil() {
        unported("DefineIndex: INCLUDE columns");
    }
    if !stmt.options.is_nil() {
        unported("DefineIndex: WITH reloptions");
    }
    if stmt.tableSpace.is_some() {
        unported("DefineIndex: TABLESPACE");
    }
    if stmt.deferrable || stmt.initdeferred {
        unported("DefineIndex: DEFERRABLE constraint indexes");
    }
    if stmt.oldNumber != 0 || skip_build {
        unported("DefineIndex: skip_build / oldNumber reuse");
    }
    // Closed-set AM name resolution (C: get_index_am_oid + GetIndexAmRoutine).
    let (accessMethodId, amname, amcanorder, amcanunique, amcanmulticol) =
        match stmt.accessMethod {
            Some("btree") => (BTREE_AM_OID, "btree", true, true, true),
            Some("hash") => (catalog_index::HASH_AM_OID, "hash", false, false, false),
            Some("gin") => (catalog_index::GIN_AM_OID, "gin", false, false, true),
            Some("gist") => (catalog_index::GIST_AM_OID, "gist", false, false, true),
            Some("spgist") => (types_core::SPGIST_AM_OID, "spgist", false, false, true),
            Some("brin") => (types_core::BRIN_AM_OID, "brin", false, false, true),
            other => unported(&format!("DefineIndex: access method {other:?} (AMNAME lookup)")),
        };
    if stmt.unique && !amcanunique {
        return Err(err(
            format!("access method \"{amname}\" does not support unique indexes"),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    if let Some(wc) = stmt.whereClause {
        CheckPredicate(mcx, wc)?;
    }

    let mut root_save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    let numberOfKeyAttributes = stmt.indexParams.len();
    let numberOfAttributes = numberOfKeyAttributes;
    if numberOfKeyAttributes > 1 && !amcanmulticol {
        return Err(err(
            format!("access method \"{amname}\" does not support multicolumn indexes"),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }
    if numberOfAttributes == 0 {
        return Err(err("must specify at least one column".into(), ERRCODE_INVALID_OBJECT_DEFINITION));
    }
    if numberOfAttributes > INDEX_MAX_KEYS as usize {
        return Err(err(
            format!("cannot use more than {INDEX_MAX_KEYS} columns in an index"),
            ERRCODE_TOO_MANY_COLUMNS,
        ));
    }

    let rel = table::table_open(mcx, tableId, ShareLock)?;
    let (root_save_userid, _) = miscinit::GetUserIdAndSecContext();
    let guard = miscinit::SecContextGuard::security_restricted(rel.rd_rel.relowner);

    let namespaceId = rel.rd_rel.relnamespace;

    match rel.rd_rel.relkind {
        RELKIND_RELATION | RELKIND_MATVIEW => {}
        RELKIND_PARTITIONED_TABLE => unported("DefineIndex: partitioned tables"),
        _ => {
            return Err(err(
                format!("cannot create index on relation \"{}\"", rel.name()),
                ERRCODE_WRONG_OBJECT_TYPE,
            ))
        }
    }

    if check_not_in_use {
        catalog_heap::CheckTableNotInUse(&rel, "CREATE INDEX")?;
    }

    if check_rights && !miscinit_seams::is_bootstrap_processing_mode::call() {
        let aclresult = aclchk_seams::object_aclcheck::call(
            NamespaceRelationId,
            namespaceId,
            root_save_userid,
            ACL_CREATE,
        )?;
        if aclresult != 0 {
            let nspname = lsyscache::get_namespace_name(mcx, namespaceId)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(err(
                format!("permission denied for schema {nspname}"),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    }

    if rel.rd_rel.relisshared {
        unported("DefineIndex: shared relations");
    }
    let tablespaceId = InvalidOid; // GetDefaultTablespace (DefineRelation precedent)

    let indexColNames = ChooseIndexColumnNames(mcx, &stmt.indexParams)?;
    let name_storage;
    let indexRelationName: &str = match stmt.idxname {
        Some(n) => n,
        None => {
            name_storage = if stmt.primary {
                ChooseRelationName(mcx, rel.name(), None, "pkey", namespaceId, true)?
            } else if stmt.isconstraint {
                let addition = ChooseIndexNameAddition(mcx, &indexColNames)?;
                ChooseRelationName(mcx, rel.name(), Some(addition.as_str()), "key", namespaceId, true)?
            } else {
                ChooseIndexName(mcx, rel.name(), namespaceId, &indexColNames)?
            };
            name_storage.as_str()
        }
    };

    let mut indexInfo = IndexInfo {
        ii_NumIndexAttrs: numberOfAttributes as i32,
        ii_AmCache: None,
        ii_NumIndexKeyAttrs: numberOfKeyAttributes as i32,
        ii_IndexAttrNumbers: [0; INDEX_MAX_KEYS as usize],
        ii_Expressions: types_nodes::NodeList::nil(),
        ii_ExpressionsState: PgVec::new_in(mcx),
        ii_Predicate: clauses::make_ands_implicit(mcx, stmt.whereClause)?,
        ii_PredicateState: None,
        ii_Unique: stmt.unique,
        ii_NullsNotDistinct: stmt.nulls_not_distinct,
        ii_ReadyForInserts: true,
        ii_Summarizing: false,
        ii_Concurrent: false,
        ii_BrokenHotChain: false,
        ii_UniqueOps: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueProcs: [0; INDEX_MAX_KEYS as usize],
        ii_UniqueStrats: [0; INDEX_MAX_KEYS as usize],
    };

    let mut collationIds = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut opclassIds = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut coloptions = [0i16; INDEX_MAX_KEYS as usize];
    ComputeIndexAttrs(
        mcx,
        &rel,
        &mut indexInfo,
        &mut collationIds,
        &mut opclassIds,
        &mut coloptions,
        &stmt.indexParams,
        stmt.isconstraint,
        accessMethodId,
        amname,
        amcanorder,
        &mut root_save_nestlevel,
    )?;

    if stmt.primary {
        catalog_index::index_check_primary_key(mcx, &rel, &indexInfo, is_alter_table)?;
    }

    for i in 0..numberOfAttributes {
        if indexInfo.ii_IndexAttrNumbers[i] < 0 {
            return Err(err(
                "index creation on system columns is not supported".into(),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
    }
    if !indexInfo.ii_Expressions.is_nil() || !indexInfo.ii_Predicate.is_nil() {
        let mut check = |list: &types_nodes::NodeList<'mcx>| -> PgResult<()> {
            for e in list.iter() {
                for v in vars::pull_var_clause(mcx, e, 0)?.iter() {
                    if v.as_var().expect("pull_var_clause").varattno < 0 {
                        return Err(err(
                            "index creation on system columns is not supported".into(),
                            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                        ));
                    }
                }
            }
            Ok(())
        };
        check(&indexInfo.ii_Expressions)?;
        check(&indexInfo.ii_Predicate)?;
        // attgenerated is 0 on every ported lane, so the virtual-generated
        // column error arm is dead.
    }

    let mut colname_refs: PgVec<'_, &str> = PgVec::new_in(mcx);
    for n in indexColNames.iter() {
        colname_refs.push(n.as_str());
    }

    let indexRelationId = catalog_index::index_create(
        mcx,
        &rel,
        indexRelationName,
        indexRelationId,
        &mut indexInfo,
        &colname_refs,
        accessMethodId,
        tablespaceId,
        &collationIds[..numberOfAttributes],
        &opclassIds[..numberOfAttributes],
        &coloptions[..numberOfAttributes],
        &IndexCreateExtra {
            flags: (if stmt.primary { INDEX_CREATE_IS_PRIMARY } else { 0 })
                | (if stmt.isconstraint { INDEX_CREATE_ADD_CONSTRAINT } else { 0 }),
            constr_flags: 0,
            allow_system_table_mods: false,
            is_internal: !check_rights,
        },
    )?;

    guc::AtEOXact_GUC(false, root_save_nestlevel);
    guard.restore();

    rel.close(types_rel::NoLock)?;
    Ok(indexRelationId)
}

// ResolveOpClass (indexcmds.c), named-opclass arm; the NIL arm stays inline
// in ComputeIndexAttrs.
fn ResolveOpClass(
    opclass: &types_nodes::NodeList<'_>,
    attrType: Oid,
    accessMethodName: &str,
    accessMethodId: Oid,
) -> PgResult<Oid> {
    let mut names: [&str; 4] = [""; 4];
    let nnames = opclass.len();
    if nnames == 0 || nnames > 3 {
        unported("ResolveOpClass: improper qualified opclass name");
    }
    for (i, n) in opclass.iter().enumerate() {
        names[i] = n.as_string().expect("opclass holds Strings").sval;
    }
    let (schemaname, opcname) = catalog_namespace::DeconstructQualifiedName(&names[..nnames])?;

    let opClassId = if let Some(schemaname) = schemaname {
        let namespaceId = catalog_namespace::LookupExplicitNamespace(schemaname, false)?;
        syscache_seams::lookup_pg_opclass_oid_by_name::call(
            accessMethodId,
            opcname,
            namespaceId,
        )?
    } else {
        catalog_namespace::OpclassnameGetOpcid(accessMethodId, opcname)?
    };
    if opClassId == InvalidOid {
        return Err(err(
            format!(
                "operator class \"{}\" does not exist for access method \"{}\"",
                if schemaname.is_some() { names[..nnames].join(".") } else { opcname.to_string() },
                accessMethodName
            ),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    }

    let Some(shape) = syscache_seams::lookup_pg_opclass_shape::call(opClassId)? else {
        return Err(err(
            format!(
                "operator class \"{}\" does not exist for access method \"{}\"",
                names[..nnames].join("."),
                accessMethodName
            ),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    };
    if !coerce::IsBinaryCoercible(attrType, shape.opcintype)? {
        return Err(err(
            format!(
                "operator class \"{}\" does not accept data type {}",
                names[..nnames].join("."),
                format_type::format_type_be(attrType)?
            ),
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    Ok(opClassId)
}

#[allow(clippy::too_many_arguments)]
fn ComputeIndexAttrs<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
    collationIds: &mut [Oid],
    opclassIds: &mut [Oid],
    coloptions: &mut [i16],
    attList: &types_nodes::NodeList<'mcx>,
    isconstraint: bool,
    accessMethodId: Oid,
    amname: &str,
    amcanorder: bool,
    ddl_save_nestlevel: &mut i32,
) -> PgResult<()> {
    for (attn, node) in attList.iter().enumerate() {
        let attribute = node
            .as_variant::<IndexElem>()
            .unwrap_or_else(|| panic!("IndexElem expected in indexParams"));
        if !attribute.opclassopts.is_nil() {
            unported("ComputeIndexAttrs: opclass options (attoptions)");
        }
        let (atttype, attcollation) = if let Some(name) = attribute.name {
            let desc = rel.descr();
            let mut found = None;
            for i in 0..desc.natts as usize {
                let att = desc.attr(i);
                if !att.attisdropped && att.attname.name_str() == name.as_bytes() {
                    found = Some(*att);
                    break;
                }
            }
            // C SearchSysCacheAttName resolves system columns to negative
            // attnums; DefineIndex then rejects them with 0A000.
            if found.is_none() {
                if let Some(sysatt) = catalog_heap::SystemAttributeByName(name) {
                    found = Some(*sysatt);
                }
            }
            let Some(attform) = found else {
                let msg = if isconstraint {
                    format!("column \"{name}\" named in key does not exist")
                } else {
                    format!("column \"{name}\" does not exist")
                };
                return Err(err(msg, ERRCODE_UNDEFINED_COLUMN));
            };
            indexInfo.ii_IndexAttrNumbers[attn] = attform.attnum;
            (attform.atttypid, attform.attcollation)
        } else {
            // Expression column. Top-level CollateExpr stripping is dead:
            // COLLATE stays loud upstream (no transformed CollateExpr node).
            let expr = attribute.expr.expect("IndexElem without name or expr");
            let atttype = nodes_core::expr_type(expr);
            let attcollation = nodes_core::expr_collation(expr);
            if let Some(var) = expr.as_var() {
                if var.varattno != 0 {
                    indexInfo.ii_IndexAttrNumbers[attn] = var.varattno;
                } else {
                    push_index_expression(mcx, indexInfo, attn, expr)?;
                }
            } else {
                push_index_expression(mcx, indexInfo, attn, expr)?;
            }
            (atttype, attcollation)
        };
        let mut attcollation = attcollation;
        // COLLATE clause overrides either leg's collation (indexcmds.c:2050-2062,
        // resolved before the collatable check).
        if !attribute.collation.is_nil() {
            guc::AtEOXact_GUC(false, *ddl_save_nestlevel);
            let resolved = catalog_namespace::get_collation_oid_list(&attribute.collation, false);
            *ddl_save_nestlevel = guc::NewGUCNestLevel();
            guc::RestrictSearchPath()?;
            attcollation = resolved?;
        }

        if lsyscache::type_is_collatable(atttype)? {
            if attcollation == InvalidOid {
                return Err(Box::new(
                    (*err(
                        "could not determine which collation to use for index expression".into(),
                        ERRCODE_INDETERMINATE_COLLATION,
                    ))
                    .with_hint("Use the COLLATE clause to set the collation explicitly."),
                ));
            }
        } else if attcollation != InvalidOid {
            return Err(err(
                format!(
                    "collations are not supported by type {}",
                    format_type::format_type_be(atttype)?
                ),
                ERRCODE_DATATYPE_MISMATCH,
            ));
        }
        collationIds[attn] = attcollation;

        // Opclass (and collation above) resolve under the DDL owner's original
        // search path: the RestrictSearchPath nest level pops around the
        // lookup (indexcmds.c ComputeIndexAttrs, ddl_save_nestlevel dance).
        guc::AtEOXact_GUC(false, *ddl_save_nestlevel);
        let resolved = if !attribute.opclass.is_nil() {
            ResolveOpClass(&attribute.opclass, atttype, amname, accessMethodId)
        } else {
            GetDefaultOpClass(atttype, accessMethodId)
        };
        *ddl_save_nestlevel = guc::NewGUCNestLevel();
        guc::RestrictSearchPath()?;
        opclassIds[attn] = resolved?;
        if attribute.opclass.is_nil() {
            if opclassIds[attn] == InvalidOid {
                return Err(err(
                    format!(
                        "data type {} has no default operator class for access method \"{amname}\"",
                        format_type::format_type_be(atttype)?
                    ),
                    ERRCODE_UNDEFINED_OBJECT,
                ));
            }
        }

        coloptions[attn] = 0;
        if amcanorder {
            if attribute.ordering == SortByDir::SORTBY_DESC {
                coloptions[attn] |= INDOPTION_DESC;
            }
            match attribute.nulls_ordering {
                SortByNulls::SORTBY_NULLS_DEFAULT => {
                    if attribute.ordering == SortByDir::SORTBY_DESC {
                        coloptions[attn] |= INDOPTION_NULLS_FIRST;
                    }
                }
                SortByNulls::SORTBY_NULLS_FIRST => coloptions[attn] |= INDOPTION_NULLS_FIRST,
                SortByNulls::SORTBY_NULLS_LAST => {}
            }
        } else {
            if attribute.ordering != SortByDir::SORTBY_DEFAULT {
                return Err(err(
                    format!("access method \"{amname}\" does not support ASC/DESC options"),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
            if attribute.nulls_ordering != SortByNulls::SORTBY_NULLS_DEFAULT {
                return Err(err(
                    format!(
                        "access method \"{amname}\" does not support NULLS FIRST/LAST options"
                    ),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
        }
    }
    Ok(())
}

fn push_index_expression<'mcx>(
    mcx: Mcx<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
    attn: usize,
    expr: types_nodes::Node<'mcx>,
) -> PgResult<()> {
    indexInfo.ii_IndexAttrNumbers[attn] = 0;
    indexInfo.ii_Expressions.lappend(mcx, expr)?;
    if clauses::contain_mutable_functions_after_planning(mcx, expr)? {
        return Err(err(
            "functions in index expression must be marked IMMUTABLE".into(),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }
    Ok(())
}

// CheckPredicate (indexcmds.c).
fn CheckPredicate<'mcx>(mcx: Mcx<'mcx>, predicate: types_nodes::Node<'mcx>) -> PgResult<()> {
    if clauses::contain_mutable_functions_after_planning(mcx, predicate)? {
        return Err(err(
            "functions in index predicate must be marked IMMUTABLE".into(),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }
    Ok(())
}

// ChooseIndexName, non-constraint arm (pkey/key/excl labels ride the
// constraint lane).
fn ChooseIndexName<'mcx>(
    mcx: Mcx<'mcx>,
    tabname: &str,
    namespaceId: Oid,
    colnames: &[PgString<'mcx>],
) -> PgResult<PgString<'mcx>> {
    let addition = ChooseIndexNameAddition(mcx, colnames)?;
    ChooseRelationName(mcx, tabname, Some(addition.as_str()), "idx", namespaceId, false)
}

fn ChooseIndexNameAddition<'mcx>(
    mcx: Mcx<'mcx>,
    colnames: &[PgString<'mcx>],
) -> PgResult<PgString<'mcx>> {
    let mut buf = PgString::new_in(mcx);
    for name in colnames {
        if !buf.is_empty() {
            buf.try_push_str("_")?;
        }
        buf.try_push_str(name.as_str())?;
        if buf.len() >= NAMEDATALEN as usize {
            unported("ChooseIndexNameAddition: name truncation");
        }
    }
    Ok(buf)
}

fn ChooseIndexColumnNames<'mcx>(
    mcx: Mcx<'mcx>,
    indexElems: &types_nodes::NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for node in indexElems.iter() {
        let ielem = node.as_variant::<IndexElem>().expect("IndexElem");
        let origname = ielem
            .indexcolname
            .or(ielem.name)
            .unwrap_or("expr");
        let mut curname = PgString::from_str_in(origname, mcx)?;
        let mut i = 1;
        while result.iter().any(|n| n.as_str() == curname.as_str()) {
            if origname.len() + 10 >= NAMEDATALEN as usize {
                unported("ChooseIndexColumnNames: mbcliplen truncation");
            }
            curname = PgString::from_str_in(origname, mcx)?;
            use core::fmt::Write;
            write!(curname, "{i}").expect("suffix");
            i += 1;
        }
        result.push(curname);
    }
    Ok(result)
}

// ChooseRelationName. C divergence: probes pg_class under the transaction
// snapshot, not a dirty snapshot (single-backend lane).
fn ChooseRelationName<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
    namespaceid: Oid,
    isconstraint: bool,
) -> PgResult<PgString<'mcx>> {
    let pgclassrel = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    let mut pass = 0;
    let mut modlabel = PgString::from_str_in(label, mcx)?;
    let relname = loop {
        let relname = make_object_name(mcx, name1, name2, modlabel.as_str())?;
        let cname = name_arg(mcx, relname.as_str())?;
        let keys = [
            eq_key(Anum_pg_class_relname, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)),
            eq_key(Anum_pg_class_relnamespace, F_OIDEQ, Datum::from_oid(namespaceid)),
        ];
        let mut scan =
            genam::systable_beginscan(mcx, &pgclassrel, ClassNameNspIndexId, true, None, &keys)?;
        let mut collides = genam::systable_getnext(mcx, &mut scan)?.is_some();
        genam::systable_endscan(mcx, scan)?;
        if !collides && isconstraint {
            collides = constraint_name_exists(mcx, relname.as_str(), namespaceid)?;
        }
        if !collides {
            break relname;
        }
        pass += 1;
        modlabel = PgString::from_str_in(label, mcx)?;
        use core::fmt::Write;
        write!(modlabel, "{pass}").expect("label suffix");
    };
    pgclassrel.close(types_rel::AccessShareLock)?;
    Ok(relname)
}

// ConstraintNameExists (pg_constraint.c).
fn constraint_name_exists(mcx: Mcx<'_>, name: &str, namespaceid: Oid) -> PgResult<bool> {
    let conrel = table::table_open(
        mcx,
        types_core::CONSTRAINT_RELATION_ID,
        types_rel::AccessShareLock,
    )?;
    let cname = name_arg(mcx, name)?;
    let keys = [
        eq_key(2, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)),
        eq_key(3, F_OIDEQ, Datum::from_oid(namespaceid)),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &conrel,
        types_core::CONSTRAINT_NAME_NSP_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let found = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    conrel.close(types_rel::AccessShareLock)?;
    Ok(found)
}

// makeObjectName without the truncation lane (loud on overflow).
fn make_object_name<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
) -> PgResult<PgString<'mcx>> {
    let mut s = PgString::from_str_in(name1, mcx)?;
    if let Some(n2) = name2 {
        s.try_push_str("_")?;
        s.try_push_str(n2)?;
    }
    s.try_push_str("_")?;
    s.try_push_str(label)?;
    assert!(
        s.len() < NAMEDATALEN as usize,
        "makeObjectName (indexcmds.c): identifier truncation unported ({:?})",
        s.as_str()
    );
    Ok(s)
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "makeObjectName truncation unported: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}
