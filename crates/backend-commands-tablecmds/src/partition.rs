//! `backend/commands/tablecmds.c` — the partition-key (partspec) branch of
//! `DefineRelation` (tablecmds.c:1210-1249): `transformPartitionSpec` /
//! `ComputePartitionAttrs` / `StorePartitionKey`.
//!
//! Ported faithfully with the same error codes / messages / SQLSTATE as
//! PostgreSQL 18.3. The catalog write (`StorePartitionKey`) lives in
//! `backend-catalog-heap` (its C home, `catalog/heap.c`); this file owns the
//! parse-analysis + per-column attribute computation that drives it.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Oid, OidIsValid};
use types_error::PgResult;
use types_nodes::ddlnodes::{PartitionElem, PartitionSpec};
use types_nodes::nodes::Node;
use types_nodes::partition::PartitionStrategy;
use types_nodes::primnodes::Expr;
use types_nodes::parsestmt::ParseExprKind;

use backend_utils_error::ereport;
use crate::helpers::here;
use types_error::{
    ERRCODE_DATATYPE_MISMATCH, ERRCODE_INDETERMINATE_COLLATION, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_TOO_MANY_COLUMNS, ERRCODE_UNDEFINED_COLUMN, ERROR,
};

/* PARTITION_MAX_KEYS (pg_partitioned_table.h). */
const PARTITION_MAX_KEYS: usize = 32;
/* pg_am OIDs (pg_am_d.h). */
const HASH_AM_OID: Oid = 405;
use types_core::catalog::BTREE_AM_OID;
/* FirstLowInvalidHeapAttributeNumber (sysattr.h). */
const FirstLowInvalidHeapAttributeNumber: i32 = -7;
/* BITS_PER_BITMAPWORD (nodes/bitmapset.h): 64 on LP64. */
const BITS_PER_BITMAPWORD: i32 = 64;

use types_pathnodes::{Bitmapset, Relids};

/// `bms_is_member(x, a)` over the `types_pathnodes::Bitmapset` word storage
/// `pull_varattnos` produces (distinct from the `types_nodes` planner-relids
/// set the nodes-core ops use).
fn bms_is_member(x: i32, a: Option<&Bitmapset>) -> bool {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let Some(a) = a else { return false };
    let wnum = (x / BITS_PER_BITMAPWORD) as usize;
    if wnum >= a.words.len() {
        return false;
    }
    a.words[wnum] & (1u64 << (x % BITS_PER_BITMAPWORD)) != 0
}

/// `bms_next_member(a, prevbit)` (nodes/bitmapset.c).
fn bms_next_member(a: Option<&Bitmapset>, prevbit: i32) -> i32 {
    let Some(a) = a else { return -2 };
    let nwords = a.words.len();
    let prevbit = prevbit + 1;
    let mut wordnum = (prevbit / BITS_PER_BITMAPWORD) as usize;
    if wordnum >= nwords {
        return -2;
    }
    let mut mask = (!0u64) << (prevbit % BITS_PER_BITMAPWORD);
    loop {
        let w = a.words[wordnum] & mask;
        if w != 0 {
            return (wordnum as i32) * BITS_PER_BITMAPWORD + w.trailing_zeros() as i32;
        }
        wordnum += 1;
        if wordnum >= nwords {
            return -2;
        }
        mask = !0u64;
    }
}

/// `bms_add_member(a, x)` (nodes/bitmapset.c) — grows `words` as needed.
fn bms_add_member(a: Relids, x: i32) -> Relids {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let wnum = (x / BITS_PER_BITMAPWORD) as usize;
    let mut bms = a.unwrap_or_else(|| Box::new(Bitmapset { words: Vec::new() }));
    if wnum >= bms.words.len() {
        bms.words.resize(wnum + 1, 0);
    }
    bms.words[wnum] |= 1u64 << (x % BITS_PER_BITMAPWORD);
    Some(bms)
}

/// `bms_add_range(a, lower, upper)` (nodes/bitmapset.c).
fn bms_add_range(mut a: Relids, lower: i32, upper: i32) -> Relids {
    if upper < lower {
        return a;
    }
    let mut x = lower;
    while x <= upper {
        a = bms_add_member(a, x);
        x += 1;
    }
    a
}

/// `bms_del_member(a, x)` (nodes/bitmapset.c).
fn bms_del_member(a: Relids, x: i32) -> Relids {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let Some(mut bms) = a else { return None };
    let wnum = (x / BITS_PER_BITMAPWORD) as usize;
    if wnum < bms.words.len() {
        bms.words[wnum] &= !(1u64 << (x % BITS_PER_BITMAPWORD));
    }
    if bms.words.iter().all(|&w| w == 0) {
        return None;
    }
    Some(bms)
}

/// `transformPartitionSpec(rel, partspec)` (tablecmds.c:19727) — transform the
/// raw partition-key expressions into executable expression trees. Returns a
/// new, owned `PartitionSpec` (the C `copyObject`-based "avoid scribbling on
/// the input" contract). The strategy column-count check for LIST is here.
fn transformPartitionSpec<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::RelationData<'mcx>,
    partspec: &PartitionSpec<'mcx>,
) -> PgResult<PartitionSpec<'mcx>> {
    /* Check valid number of columns for strategy. */
    if partspec.strategy == PartitionStrategy::List && partspec.partParams.len() != 1 {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("cannot use \"list\" partition strategy with more than one column")
            .finish(here("transformPartitionSpec"))
            .map(|()| unreachable!());
    }

    /*
     * Create a dummy ParseState and insert the target relation as its sole
     * rangetable entry.  We need a ParseState for transformExpr.
     */
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;
    let nsitem = backend_parser_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        rel,
        types_storage::lock::AccessShareLock,
        None,
        false,
        true,
    )?;
    backend_parser_relation::addNSItemToQuery(mcx, &mut pstate, nsitem, true, true, true)?;

    /* take care of any partition expressions */
    let mut new_params: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> =
        mcx::vec_with_capacity_in(mcx, partspec.partParams.len())?;
    for l in partspec.partParams.iter() {
        let pelem = match &**l {
            Node::PartitionElem(pe) => pe,
            other => unreachable!("partParams element is not a PartitionElem: {}", other.node_tag()),
        };

        /* Copy, to avoid scribbling on the input. */
        let mut new_elem = pelem.clone_in(mcx)?;

        if let Some(expr_node) = pelem.expr.as_deref() {
            /* Now do parse transformation of the expression. */
            let transformed = backend_parser_parse_expr::transformExpr(
                &mut pstate,
                Some(expr_node.clone_in(mcx)?),
                ParseExprKind::EXPR_KIND_PARTITION_EXPRESSION,
            )?;
            /* we have to fix its collations too */
            let mut transformed_expr = transformed.expect("transformExpr of a non-NULL partition expression");
            backend_parser_parse_collate::assign_expr_collations(Some(&pstate), &mut transformed_expr)?;
            new_elem.expr = Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, transformed_expr))?);
        }

        new_params.push(mcx::alloc_in(mcx, Node::PartitionElem(new_elem))?);
    }

    Ok(PartitionSpec {
        strategy: partspec.strategy,
        partParams: new_params,
        location: partspec.location,
    })
}

/// `ComputePartitionAttrs(pstate, rel, partParams, ...)` (tablecmds.c:19785) —
/// compute the per-column `partattrs` / `partexprs` / `partopclass` /
/// `partcollation` arrays from the (already parse-analyzed) `PartitionElem`s.
#[allow(clippy::type_complexity)]
fn ComputePartitionAttrs<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::RelationData<'mcx>,
    part_params: &[types_nodes::nodes::NodePtr<'mcx>],
    strategy: PartitionStrategy,
) -> PgResult<(
    Vec<AttrNumber>,
    Vec<Expr>,
    Vec<Oid>,
    Vec<Oid>,
)> {
    let relid = rel.rd_id;
    let mut partattrs: Vec<AttrNumber> = Vec::with_capacity(part_params.len());
    let mut partexprs: Vec<Expr> = Vec::new();
    let mut partopclass: Vec<Oid> = Vec::with_capacity(part_params.len());
    let mut partcollation: Vec<Oid> = Vec::with_capacity(part_params.len());

    let am_oid = if strategy == PartitionStrategy::Hash {
        HASH_AM_OID
    } else {
        BTREE_AM_OID
    };
    let am_name = if am_oid == HASH_AM_OID { "hash" } else { "btree" };

    for (attn, pp) in part_params.iter().enumerate() {
        let pelem = match &**pp {
            Node::PartitionElem(pe) => pe,
            other => unreachable!("partParams element is not a PartitionElem: {}", other.node_tag()),
        };

        let atttype: Oid;
        let mut attcollation: Oid;

        if let Some(name) = pelem.name.as_ref().map(|s| s.as_str()) {
            /* Simple attribute reference. */
            let atttuple =
                backend_utils_cache_syscache::SearchSysCacheAttName(mcx, relid, name)?;
            if atttuple.is_none() {
                return ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!(
                        "column \"{name}\" named in partition key does not exist"
                    ))
                    .finish(here("ComputePartitionAttrs"))
                    .map(|()| unreachable!());
            }
            if let Some(t) = atttuple {
                backend_utils_cache_syscache::ReleaseSysCache(t);
            }

            let attnum = backend_utils_cache_lsyscache::attribute::get_attnum(relid, name)?;

            if attnum <= 0 {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "cannot use system column \"{name}\" in partition key"
                    ))
                    .finish(here("ComputePartitionAttrs"))
                    .map(|()| unreachable!());
            }

            let attgenerated =
                backend_utils_cache_lsyscache::attribute::get_attgenerated(relid, attnum)?;
            if attgenerated != 0 {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("cannot use generated column in partition key")
                    .errdetail(format!("Column \"{name}\" is a generated column."))
                    .finish(here("ComputePartitionAttrs"))
                    .map(|()| unreachable!());
            }

            let (typid, _typmod, collid) =
                backend_utils_cache_lsyscache::attribute::get_atttypetypmodcoll(relid, attnum)?;
            partattrs.push(attnum);
            atttype = typid;
            attcollation = collid;
        } else {
            /* Expression. */
            let expr_node = pelem
                .expr
                .as_deref()
                .expect("PartitionElem with no name must have an expr");
            let expr = match expr_node {
                Node::Expr(e) => e,
                other => unreachable!("partition expr is not an Expr: {}", other.node_tag()),
            };
            atttype = backend_nodes_core::nodefuncs::expr_type(Some(expr))?;
            attcollation = backend_nodes_core::nodefuncs::expr_collation(Some(expr))?;

            /* The expression must be of a storable type. */
            let partattname = format!("{}", attn + 1);
            let mut containing: Vec<Oid> = Vec::new();
            backend_catalog_heap::CheckAttributeType(
                mcx,
                &partattname,
                atttype,
                attcollation,
                &mut containing,
                backend_catalog_heap::CHKATYPE_IS_PARTKEY,
            )?;

            /* Strip any top-level COLLATE clause. */
            let mut stripped: Expr = expr.clone_in(mcx)?;
            while let Expr::CollateExpr(ce) = &stripped {
                let inner = ce
                    .arg
                    .as_deref()
                    .expect("CollateExpr.arg is NOT NULL");
                stripped = inner.clone_in(mcx)?;
            }
            let stripped_node = Node::mk_expr(mcx, stripped.clone_in(mcx)?);

            /* Examine all columns in the partition key expression. */
            let mut expr_attrs = backend_optimizer_util_vars::var::pull_varattnos(
                &stripped_node,
                1,
                None,
            );
            /* whole-row reference => all columns */
            let whole_row = 0 - FirstLowInvalidHeapAttributeNumber;
            if bms_is_member(whole_row, expr_attrs.as_deref()) {
                let lo = 1 - FirstLowInvalidHeapAttributeNumber;
                let hi = rel.rd_att.attrs.len() as i32 - FirstLowInvalidHeapAttributeNumber;
                expr_attrs = bms_add_range(expr_attrs, lo, hi);
                expr_attrs = bms_del_member(expr_attrs, whole_row);
            }

            let mut i: i32 = -1;
            loop {
                i = bms_next_member(expr_attrs.as_deref(), i);
                if i < 0 {
                    break;
                }
                let attno = i + FirstLowInvalidHeapAttributeNumber;
                if attno < 0 {
                    return ereport(ERROR)
                        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                        .errmsg("partition key expressions cannot contain system column references")
                        .finish(here("ComputePartitionAttrs"))
                        .map(|()| unreachable!());
                }
                if attno > 0 {
                    let attgen = backend_utils_cache_lsyscache::attribute::get_attgenerated(
                        relid,
                        attno as AttrNumber,
                    )?;
                    if attgen != 0 {
                        let cname = backend_utils_cache_lsyscache::attribute::get_attname(
                            mcx,
                            relid,
                            attno as AttrNumber,
                            false,
                        )?;
                        return ereport(ERROR)
                            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                            .errmsg("cannot use generated column in partition key")
                            .errdetail(format!(
                                "Column \"{}\" is a generated column.",
                                cname.as_ref().map(|s| s.as_str()).unwrap_or("")
                            ))
                            .finish(here("ComputePartitionAttrs"))
                            .map(|()| unreachable!());
                    }
                }
            }

            if let Expr::Var(v) = &stripped {
                if v.varattno > 0 {
                    /* User wrote "(column)" — treat like simple attribute. */
                    partattrs.push(v.varattno);
                    /* fall through to collation handling below */
                    finish_collation(
                        mcx,
                        pelem,
                        atttype,
                        &mut attcollation,
                    )?;
                    partcollation.push(attcollation);
                    partopclass.push(resolve_opclass(
                        mcx, pelem, atttype, am_oid, am_name, strategy,
                    )?);
                    continue;
                }
            }

            /* marks the column as expression */
            partattrs.push(0);

            /*
             * Preprocess the expression (expression_planner) before checking
             * for mutability.
             */
            let planned = backend_optimizer_plan_planner::expression_planner(mcx, stripped.clone_in(mcx)?)?;

            if backend_optimizer_util_clauses::grounded::contain_mutable_functions(Some(&planned))? {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("functions in partition key expression must be marked IMMUTABLE")
                    .finish(here("ComputePartitionAttrs"))
                    .map(|()| unreachable!());
            }

            if let Expr::Const(_) = &planned {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("cannot use constant expression as partition key")
                    .finish(here("ComputePartitionAttrs"))
                    .map(|()| unreachable!());
            }

            /* Save the (un-planned) stripped expression for storage. */
            partexprs.push(stripped);
        }

        /* Apply collation override + collatability check. */
        finish_collation(mcx, pelem, atttype, &mut attcollation)?;
        partcollation.push(attcollation);

        /* Identify the appropriate operator class. */
        partopclass.push(resolve_opclass(mcx, pelem, atttype, am_oid, am_name, strategy)?);
    }

    Ok((partattrs, partexprs, partopclass, partcollation))
}

/// The collation-override + collatability-consistency tail shared by the
/// column and expression branches of `ComputePartitionAttrs`.
fn finish_collation<'mcx>(
    mcx: Mcx<'mcx>,
    pelem: &PartitionElem<'mcx>,
    atttype: Oid,
    attcollation: &mut Oid,
) -> PgResult<()> {
    /* Apply collation override if any. */
    if !pelem.collation.is_empty() {
        let namelist = nodelist_to_namelist(&pelem.collation);
        *attcollation =
            backend_catalog_namespace::get_collation_oid(mcx, &namelist, false)?;
    }

    /* Check we have a collation iff it's a collatable type. */
    if backend_utils_cache_lsyscache::type_::type_is_collatable(atttype)? {
        if !OidIsValid(*attcollation) {
            return ereport(ERROR)
                .errcode(ERRCODE_INDETERMINATE_COLLATION)
                .errmsg("could not determine which collation to use for partition expression")
                .errhint("Use the COLLATE clause to set the collation explicitly.")
                .finish(here("ComputePartitionAttrs"))
                .map(|()| unreachable!());
        }
    } else if OidIsValid(*attcollation) {
        return ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "collations are not supported by type {}",
                backend_utils_adt_format_type::format_type_be(mcx, atttype)?
            ))
            .finish(here("ComputePartitionAttrs"))
            .map(|()| unreachable!());
    }
    Ok(())
}

/// Identify the operator class for a partition column (default or explicit).
fn resolve_opclass<'mcx>(
    mcx: Mcx<'mcx>,
    pelem: &PartitionElem<'mcx>,
    atttype: Oid,
    am_oid: Oid,
    am_name: &str,
    strategy: PartitionStrategy,
) -> PgResult<Oid> {
    if pelem.opclass.is_empty() {
        let opclass = backend_commands_indexcmds::opclass::GetDefaultOpClass(atttype, am_oid)?;
        if !OidIsValid(opclass) {
            let (errmsg, hint) = if strategy == PartitionStrategy::Hash {
                (
                    format!(
                        "data type {} has no default operator class for access method \"hash\"",
                        backend_utils_adt_format_type::format_type_be(mcx, atttype)?
                    ),
                    "You must specify a hash operator class or define a default hash operator class for the data type.",
                )
            } else {
                (
                    format!(
                        "data type {} has no default operator class for access method \"btree\"",
                        backend_utils_adt_format_type::format_type_be(mcx, atttype)?
                    ),
                    "You must specify a btree operator class or define a default btree operator class for the data type.",
                )
            };
            return ereport(ERROR)
                .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(errmsg)
                .errhint(hint)
                .finish(here("ComputePartitionAttrs"))
                .map(|()| unreachable!());
        }
        Ok(opclass)
    } else {
        backend_commands_indexcmds::opclass::ResolveOpClass(
            mcx,
            &pelem.opclass,
            atttype,
            am_name,
            am_oid,
        )
    }
}

/// Flatten a `List` of `String` nodes (a qualified collation name) into the
/// `&[Option<String>]` `NameList` `get_collation_oid` expects.
fn nodelist_to_namelist<'mcx>(
    nodes: &[types_nodes::nodes::NodePtr<'mcx>],
) -> Vec<Option<String>> {
    nodes
        .iter()
        .map(|n| match &**n {
            Node::String(s) => Some(s.sval.as_str().to_string()),
            _ => None,
        })
        .collect()
}

/// `DefineRelation`'s `partitioned` partition-key block (tablecmds.c:1210-1249):
/// `transformPartitionSpec` / `ComputePartitionAttrs` / `StorePartitionKey`.
pub fn define_relation_partspec<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::RelationData<'mcx>,
    partspec: &PartitionSpec<'mcx>,
    _query_string: Option<&str>,
) -> PgResult<()> {
    let partnatts = partspec.partParams.len();

    /* Protect fixed-size arrays here and in executor. */
    if partnatts > PARTITION_MAX_KEYS {
        return ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(format!(
                "cannot partition using more than {PARTITION_MAX_KEYS} columns"
            ))
            .finish(here("define_relation_partspec"))
            .map(|()| unreachable!());
    }

    /*
     * We need to transform the raw parsetrees corresponding to partition
     * expressions into executable expression trees.
     */
    let newspec = transformPartitionSpec(mcx, rel, partspec)?;

    let (partattrs, partexprs, partopclass, partcollation) =
        ComputePartitionAttrs(mcx, rel, &newspec.partParams, newspec.strategy)?;

    /* Assemble the partexprs List node (None if no expressions). */
    let partexprs_node = if partexprs.is_empty() {
        None
    } else {
        let mut cells: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> =
            mcx::vec_with_capacity_in(mcx, partexprs.len())?;
        for e in partexprs.into_iter() {
            cells.push(mcx::alloc_in(mcx, Node::mk_expr(mcx, e))?);
        }
        Some(Node::mk_list(mcx, cells))
    };

    backend_catalog_heap::StorePartitionKey(
        mcx,
        rel,
        newspec.strategy as i8,
        partnatts as i16,
        &partattrs,
        partexprs_node.as_ref(),
        &partopclass,
        &partcollation,
    )?;

    Ok(())
}
