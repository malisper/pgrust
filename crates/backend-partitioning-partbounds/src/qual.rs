//! `get_qual_from_partbound` and the per-strategy partition-constraint
//! generators (`get_qual_for_hash` / `get_qual_for_list` / `get_qual_for_range`)
//! plus their helpers (`make_partition_op_expr` / `get_partition_operator` /
//! `get_range_key_properties` / `get_range_nulltest`) — `partbounds.c`.
//!
//! Faithful 1:1 port of PostgreSQL 18.3 `partbounds.c`. These build the
//! implicit-AND list of `Expr` constraint clauses describing the set of rows a
//! partition (given its parent and bound spec) accepts. They are reached from:
//!
//!   * `RelationGetPartitionQual` / `generate_partition_qual` (partcache.c) via
//!     the installed `qual_from_partbound` seam (reads `relpartbound`), and
//!   * `ATExecAttachPartition` (tablecmds.c), which calls `get_qual_from_partbound`
//!     directly on the to-be-attached bound.
//!
//! The C `get_qual_for_range` uses a throwaway `EState` + `ExecInitExpr` to test
//! whether a leading lower/upper bound pair are *equal* (so it can emit a single
//! `keyCol = val` clause instead of the OR pair). The executor cannot be a
//! dependency of this low-level crate, so we compute the same predicate by the
//! partition key's btree 3-way comparison support function (`partsupfunc[i]`,
//! the same FmgrInfo the routing search uses): the bounds are equal iff the
//! support function returns 0. This is exactly the semantics of the
//! `BTEqualStrategyNumber` operator the C evaluates, with no behavioral
//! difference for the immutable Const bounds a range partition carries.

use mcx::{Mcx, PgBox};
use types_core::primitive::{Oid, OidIsValid};

/// `RECORDOID` (pg_type.h) — the pseudo-type for an anonymous record.
const RECORDOID: Oid = 2249;
use types_error::{PgError, PgResult};
use types_nodes::ddlnodes::{PartitionBoundSpec, PartitionRangeDatum};
use types_nodes::nodes::Node;
use types_nodes::partition::{
    PartitionDescData, PartitionKeyData, PartitionRangeDatumKind, PartitionStrategy,
};
use types_nodes::primnodes::{Const, Expr};

use backend_nodes_core::makefuncs::{
    make_ands_explicit, make_bool_const, make_bool_expr, make_const, make_is_not_null,
    make_opclause, make_relabel_type, make_var,
};
use types_nodes::primnodes::{ArrayExpr, BoolExprType, CoercionForm, NullTest, NullTestType, ScalarArrayOpExpr};

use backend_utils_cache_lsyscache_seams as lsyscache;

use crate::call_cmp;

/* StrategyNumber constants (access/stratnum.h). */
const BTLessStrategyNumber: u16 = 1;
const BTLessEqualStrategyNumber: u16 = 2;
const BTEqualStrategyNumber: u16 = 3;
const BTGreaterEqualStrategyNumber: u16 = 4;
const BTGreaterStrategyNumber: u16 = 5;

const BOOLOID: Oid = 16;
const InvalidOid: Oid = 0;
/// `INT4OID` (pg_type.dat).
const INT4OID: Oid = 23;
/// `OIDOID` (pg_type.dat).
const OIDOID: Oid = 26;

/// The unified `Datum` carrier `make_const` consumes (matches `makefuncs.rs`).
use types_tuple::backend_access_common_heaptuple::Datum;

/// `IsPolymorphicType(typid)` (pg_type.h) — true for the pseudo-types that
/// accept any input type. Mirrors the macro's OID list.
fn is_polymorphic_type(typid: Oid) -> bool {
    const ANYELEMENTOID: u32 = 2283;
    const ANYARRAYOID: u32 = 2277;
    const ANYNONARRAYOID: u32 = 2776;
    const ANYENUMOID: u32 = 3500;
    const ANYRANGEOID: u32 = 3831;
    const ANYMULTIRANGEOID: u32 = 4537;
    const ANYCOMPATIBLEOID: u32 = 5077;
    const ANYCOMPATIBLEARRAYOID: u32 = 5078;
    const ANYCOMPATIBLENONARRAYOID: u32 = 5079;
    const ANYCOMPATIBLERANGEOID: u32 = 5080;
    const ANYCOMPATIBLEMULTIRANGEOID: u32 = 5081;
    matches!(
        typid,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

/// Wrap a freshly built `Expr` as a `Node` (the implicit-AND list element type
/// the qual seam returns; the elements are always `Expr` leaves), allocating the
/// opaque node in `mcx`.
fn node<'mcx>(mcx: Mcx<'mcx>, e: Expr<'mcx>) -> PgResult<Node<'mcx>> {
    Node::mk_expr(mcx, e)
}

/// Extract the `Const` carried by a `PartitionRangeDatum`/`spec->listdatums`
/// node pointer (a `Node::Expr(Expr::Const)`), cloned into `mcx`.
fn const_of<'mcx>(mcx: Mcx<'mcx>, n: &Node<'_>) -> PgResult<Const<'mcx>> {
    // `Const.constvalue` is the by-ref `Datum` carrier; deep-clone it into `mcx`
    // (`copyObject(Const)`) so the returned `Const<'mcx>` is tied to the mcx
    // lifetime and independent of the input node's arena — a derived `.clone()`
    // would copy the by-ref `Datum` pointer into a node that may outlive the
    // source bound spec (a UAF the borrow checker now rejects).
    let c = n
        .as_const()
        .ok_or_else(|| elog("partition bound datum is not a Const"))?;
    Ok(Const {
        consttype: c.consttype,
        consttypmod: c.consttypmod,
        constcollid: c.constcollid,
        constlen: c.constlen,
        constvalue: c.constvalue.clone_in(mcx)?,
        constisnull: c.constisnull,
        constbyval: c.constbyval,
        location: c.location,
    })
}

fn elog(msg: &str) -> PgError {
    backend_utils_error::ereport(types_error::ERROR)
        .errmsg_internal(msg.to_string())
        .into_error()
}

/// `make_partition_op_expr(key, keynum, strategy, arg1, arg2)` (partbounds.c:3868).
/// Build the (possibly OR-combined / ScalarArrayOp) operator clause for one
/// partition key column.
fn make_partition_op_expr<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData<'_>,
    keynum: usize,
    strategy: u16,
    arg1: Expr<'mcx>,
    arg2_elems: MakeOpArg<'mcx>,
) -> PgResult<Option<Expr<'mcx>>> {
    // operoid = get_partition_operator(key, keynum, strategy, &need_relabel);
    let (operoid, need_relabel) = get_partition_operator(key, keynum, strategy)?;

    // Possibly wrap the non-Const operand in a RelabelType.
    let arg1_is_const = matches!(arg1, Expr::Const(_));
    let arg1 = if !arg1_is_const
        && (need_relabel || key.partcollation[keynum] != key.parttypcoll[keynum])
    {
        make_relabel_type(
            arg1,
            key.partopcintype[keynum],
            -1,
            key.partcollation[keynum],
            CoercionForm::COERCE_EXPLICIT_CAST,
        )
    } else {
        arg1
    };

    let result = match key.strategy {
        PartitionStrategy::List => {
            let elems = match arg2_elems {
                MakeOpArg::Elems(v) => v,
                MakeOpArg::Single(_) => return Err(elog("LIST make_partition_op_expr needs elems")),
            };
            let nelems = elems.len();
            debug_assert!(nelems >= 1);
            debug_assert!(keynum == 0);

            // type_is_array(typid) == (get_element_type(typid) != InvalidOid).
            let is_array = lsyscache::get_element_type::call(key.parttypid[keynum])?.is_some();
            if nelems > 1 && !is_array {
                // Construct leftop = ANY (ARRAY[...]).
                let array_typeid = lsyscache::get_array_type::call(key.parttypid[keynum])?
                    .unwrap_or(InvalidOid);
                let arrexpr = ArrayExpr {
                    array_typeid,
                    array_collid: key.parttypcoll[keynum],
                    element_typeid: key.parttypid[keynum],
                    elements: elems.into_iter().collect(),
                    multidims: false,
                    location: -1,
                };
                let opfuncid = lsyscache::get_opcode::call(operoid)?;
                let saopexpr = ScalarArrayOpExpr {
                    opno: operoid,
                    opfuncid,
                    hashfuncid: InvalidOid,
                    negfuncid: InvalidOid,
                    useOr: true,
                    inputcollid: key.partcollation[keynum],
                    args: vec![arg1, Expr::ArrayExpr(arrexpr)],
                    location: -1,
                };
                Some(Expr::ScalarArrayOpExpr(saopexpr))
            } else {
                // OR of per-element opclauses.
                let mut elemops: Vec<Expr> = Vec::with_capacity(nelems);
                for elem in elems.into_iter() {
                    let elemop = make_opclause(
                        operoid,
                        BOOLOID,
                        false,
                        arg1.clone(),
                        Some(elem),
                        InvalidOid,
                        key.partcollation[keynum],
                    );
                    elemops.push(elemop);
                }
                Some(if elemops.len() > 1 {
                    make_bool_expr(BoolExprType::OR_EXPR, elemops, -1)
                } else {
                    elemops.into_iter().next().unwrap()
                })
            }
        }
        PartitionStrategy::Range => {
            let arg2 = match arg2_elems {
                MakeOpArg::Single(e) => e,
                MakeOpArg::Elems(_) => return Err(elog("RANGE make_partition_op_expr needs a single arg")),
            };
            Some(make_opclause(
                operoid,
                BOOLOID,
                false,
                arg1,
                Some(arg2),
                InvalidOid,
                key.partcollation[keynum],
            ))
        }
        PartitionStrategy::Hash => {
            // Assert(false) in C — hash never builds op-exprs this way.
            return Err(elog("make_partition_op_expr: unexpected HASH strategy"));
        }
    };
    let _ = mcx;
    Ok(result)
}

/// Argument carrier for `make_partition_op_expr` (a single Expr for RANGE, a
/// list of Const elems for LIST).
enum MakeOpArg<'mcx> {
    Single(Expr<'mcx>),
    Elems(Vec<Expr<'mcx>>),
}

/// `get_partition_operator(key, col, strategy, &need_relabel)` (partbounds.c:3832).
/// Look up the btree operator OID for the partitioning column at `strategy`.
fn get_partition_operator(
    key: &PartitionKeyData<'_>,
    col: usize,
    strategy: u16,
) -> PgResult<(Oid, bool)> {
    let operoid = lsyscache::get_opfamily_member::call(
        key.partopfamily[col],
        key.partopcintype[col],
        key.partopcintype[col],
        strategy as i16,
    )?;
    if !OidIsValid(operoid) {
        return Err(elog(&format!(
            "missing operator {}({},{}) in partition opfamily {}",
            strategy, key.partopcintype[col], key.partopcintype[col], key.partopfamily[col],
        )));
    }

    let need_relabel = key.parttypid[col] != key.partopcintype[col]
        && key.partopcintype[col] != RECORDOID
        && !is_polymorphic_type(key.partopcintype[col]);

    Ok((operoid, need_relabel))
}

/// The `keyCol` expression for partition key column `keynum`: a `Var` when the
/// column is a plain attribute, else a copy of the corresponding partexpr.
/// `partexprs_idx` tracks the position into `key.partexprs` (advanced when an
/// expression column is consumed). Mirrors the `partexprs_item` cursor logic of
/// the C helpers.
fn key_col<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData<'_>,
    keynum: usize,
    partexprs_idx: &mut usize,
) -> PgResult<Expr<'mcx>> {
    if key.partattrs[keynum] != 0 {
        Ok(Expr::Var(make_var(
            1,
            key.partattrs[keynum],
            key.parttypid[keynum],
            key.parttypmod[keynum],
            key.parttypcoll[keynum],
            0,
        )))
    } else {
        let e = key
            .partexprs
            .get(*partexprs_idx)
            .ok_or_else(|| elog("wrong number of partition key expressions"))?;
        let cloned = e.clone_in(mcx)?;
        *partexprs_idx += 1;
        Ok(cloned)
    }
}

/// `get_range_nulltest(key)` (partbounds.c:4676) — one `IS NOT NULL` per key
/// column.
fn get_range_nulltest<'mcx>(mcx: Mcx<'mcx>, key: &PartitionKeyData<'_>) -> PgResult<Vec<Expr<'mcx>>> {
    let mut result = Vec::new();
    let mut partexprs_idx = 0usize;
    for i in 0..(key.partnatts as usize) {
        let kc = key_col(mcx, key, i, &mut partexprs_idx)?;
        result.push(make_is_not_null(kc));
    }
    Ok(result)
}

/// `get_qual_for_hash(parent, spec)` (partbounds.c:3983) — the hash partition
/// constraint is always `satisfies_hash_partition(parentoid, modulus,
/// remainder, key...)`. Building this needs the parent OID + the per-key
/// fmgr-call argument expansion; it is reached only for HASH parents.
fn get_qual_for_hash<'mcx>(
    mcx: Mcx<'mcx>,
    parent_relid: Oid,
    key: &PartitionKeyData<'_>,
    spec: &PartitionBoundSpec<'_>,
) -> PgResult<Vec<Expr<'mcx>>> {
    // Fixed arguments: the parent relation OID, the modulus, and the remainder,
    // all as immutable Const nodes.
    //
    // relidConst = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
    //                        ObjectIdGetDatum(RelationGetRelid(parent)),
    //                        false, true);
    let relid_const = make_const(
        mcx,
        OIDOID,
        -1,
        InvalidOid,
        core::mem::size_of::<Oid>() as i32,
        Datum::from_oid(parent_relid),
        false,
        true,
    )?;

    // modulusConst = makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
    //                          Int32GetDatum(spec->modulus), false, true);
    let modulus_const = make_const(
        mcx,
        INT4OID,
        -1,
        InvalidOid,
        core::mem::size_of::<i32>() as i32,
        Datum::from_i32(spec.modulus),
        false,
        true,
    )?;

    // remainderConst = makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
    //                            Int32GetDatum(spec->remainder), false, true);
    let remainder_const = make_const(
        mcx,
        INT4OID,
        -1,
        InvalidOid,
        core::mem::size_of::<i32>() as i32,
        Datum::from_i32(spec.remainder),
        false,
        true,
    )?;

    // args = list_make3(relidConst, modulusConst, remainderConst);
    let mut args: Vec<Expr> = vec![
        Expr::Const(relid_const),
        Expr::Const(modulus_const),
        Expr::Const(remainder_const),
    ];

    // Add an argument for each key column.
    let mut partexprs_idx = 0usize;
    for i in 0..(key.partnatts as usize) {
        // Left operand: a Var for a plain attribute, else a copy of the partexpr.
        let key_col = if key.partattrs[i] != 0 {
            Expr::Var(make_var(
                1,
                key.partattrs[i],
                key.parttypid[i],
                key.parttypmod[i],
                key.parttypcoll[i],
                0,
            ))
        } else {
            let e = key
                .partexprs
                .get(partexprs_idx)
                .ok_or_else(|| elog("wrong number of partition key expressions"))?;
            let cloned = e.clone_in(mcx)?;
            partexprs_idx += 1;
            cloned
        };
        args.push(key_col);
    }

    // fexpr = makeFuncExpr(F_SATISFIES_HASH_PARTITION, BOOLOID, args,
    //                      InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
    const F_SATISFIES_HASH_PARTITION: Oid = 5028;
    let fexpr = backend_nodes_core::makefuncs::make_func_expr(
        F_SATISFIES_HASH_PARTITION,
        BOOLOID,
        args,
        InvalidOid,
        InvalidOid,
        CoercionForm::COERCE_EXPLICIT_CALL,
    );

    // return list_make1(fexpr);
    Ok(vec![fexpr])
}

/// `get_qual_for_list(parent, spec)` (partbounds.c:4066).
fn get_qual_for_list<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData<'_>,
    spec: &PartitionBoundSpec<'_>,
    parent_partdesc: Option<&PartitionDescData<'_>>,
) -> PgResult<Vec<Expr<'mcx>>> {
    // Only single-column list partitioning is supported.
    debug_assert!(key.partnatts == 1);

    let mut partexprs_idx = 0usize;
    let key_col = key_col(mcx, key, 0, &mut partexprs_idx)?;

    let mut elems: Vec<Expr> = Vec::new();
    let mut list_has_null = false;

    if spec.is_default {
        // For the default list partition, collect datums for all the *other*
        // partitions; the constraint checks that the key equals none of them.
        // C reads them from `RelationGetPartitionDesc(parent, false)->boundinfo`,
        // threaded in here as `parent_partdesc` (the partdesc crate depends on
        // this one, so we cannot reach it directly).
        let pdesc = parent_partdesc.ok_or_else(|| {
            elog("default LIST partition constraint requires the parent's PartitionDesc")
        })?;

        let mut ndatums = 0usize;
        if let Some(boundinfo) = pdesc.boundinfo.as_ref() {
            ndatums = boundinfo.ndatums as usize;
            // partition_bound_accepts_nulls(boundinfo) == (null_index != -1).
            if boundinfo.null_index != -1 {
                list_has_null = true;
            }
        }

        // If default is the only partition, there need not be any partition
        // constraint on it.
        if ndatums == 0 && !list_has_null {
            return Ok(Vec::new());
        }

        if let Some(boundinfo) = pdesc.boundinfo.as_ref() {
            for i in 0..ndatums {
                // Construct Const from the known-not-null datum, copying the
                // value so the result outlives the relcache entry.
                let row = boundinfo.datums.get(i).ok_or_else(|| {
                    elog("default LIST partition: boundinfo datum index out of range")
                })?;
                let src = row.first().ok_or_else(|| {
                    elog("default LIST partition: boundinfo datum row is empty")
                })?;
                let copied = backend_utils_adt_scalar_seams::datum_copy::call(
                    mcx,
                    src,
                    key.parttypbyval[0],
                    key.parttyplen[0],
                )?;
                let val = make_const(
                    mcx,
                    key.parttypid[0],
                    key.parttypmod[0],
                    key.parttypcoll[0],
                    key.parttyplen[0] as i32,
                    copied,
                    false,
                    key.parttypbyval[0],
                )?;
                elems.push(Expr::Const(val));
            }
        }
    } else {
        // Consts for the allowed values, excluding nulls.
        for n in spec.listdatums.iter() {
            let c = const_of(mcx, n)?;
            if c.constisnull {
                list_has_null = true;
            } else {
                elems.push(Expr::Const(c));
            }
        }
    }

    let opexpr = if !elems.is_empty() {
        make_partition_op_expr(mcx, key, 0, BTEqualStrategyNumber, key_col.clone(), MakeOpArg::Elems(elems))?
    } else {
        None
    };

    let mut result = if !list_has_null {
        // "col IS NOT NULL" ANDed with the main expression.
        let nulltest = make_is_not_null(key_col);
        match opexpr {
            Some(op) => vec![nulltest, op],
            None => vec![nulltest],
        }
    } else {
        // "col IS NULL" OR'd with the main expression.
        let nulltest = Expr::NullTest(NullTest {
            arg: Some(Box::new(key_col)),
            nulltesttype: NullTestType::IS_NULL,
            argisrow: false,
            location: -1,
        });
        match opexpr {
            Some(op) => vec![make_bool_expr(BoolExprType::OR_EXPR, vec![nulltest, op], -1)],
            None => vec![nulltest],
        }
    };

    // Applying NOT to a constraint expression inverts the row set here because
    // the partition constraints we construct never evaluate to NULL (NOT NULL
    // would be NULL otherwise).
    if spec.is_default {
        let ands = make_ands_explicit(result);
        let not = make_bool_expr(BoolExprType::NOT_EXPR, vec![ands], -1);
        result = vec![not];
    }

    Ok(result)
}

/// `get_range_key_properties(key, keynum, ldatum, udatum, &partexprs_item,
/// &keyCol, &lower_val, &upper_val)` (partbounds.c:4632).
fn get_range_key_properties<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData<'_>,
    keynum: usize,
    ldatum: &PartitionRangeDatum<'_>,
    udatum: &PartitionRangeDatum<'_>,
    partexprs_idx: &mut usize,
) -> PgResult<(Expr<'mcx>, Option<Const<'mcx>>, Option<Const<'mcx>>)> {
    let key_col = key_col(mcx, key, keynum, partexprs_idx)?;

    let lower_val = if ldatum.kind == PartitionRangeDatumKind::Value {
        let v = ldatum.value.as_ref().ok_or_else(|| elog("range lower datum has no value"))?;
        Some(const_of(mcx, v)?)
    } else {
        None
    };
    let upper_val = if udatum.kind == PartitionRangeDatumKind::Value {
        let v = udatum.value.as_ref().ok_or_else(|| elog("range upper datum has no value"))?;
        Some(const_of(mcx, v)?)
    } else {
        None
    };

    Ok((key_col, lower_val, upper_val))
}

/// Read a `PartitionRangeDatum` out of a bound's `lowerdatums`/`upperdatums`
/// node-pointer list.
fn range_datum<'a, 'mcx>(n: &'a Node<'mcx>) -> PgResult<&'a PartitionRangeDatum<'mcx>> {
    n.as_partitionrangedatum()
        .ok_or_else(|| elog("range bound element is not a PartitionRangeDatum"))
}

/// `get_qual_for_range(parent, spec, for_default)` (partbounds.c:4275).
///
/// The default-partition recursion (`spec->is_default`) and multi-column
/// trailing-OR construction are ported in full; the leading equal-bounds
/// optimization uses the btree comparison support function in place of the C's
/// throwaway-EState `ExecInitExpr` (see module header).
fn get_qual_for_range<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData<'_>,
    spec: &PartitionBoundSpec<'_>,
    for_default: bool,
    parent_partdesc: Option<&PartitionDescData<'_>>,
) -> PgResult<Vec<Expr<'mcx>>> {
    if spec.is_default {
        // The default range partition holds everything NOT contained in the
        // non-default siblings: OR each sibling's constraint, AND in the
        // per-key NOT NULL tests, then negate the whole thing. C reads the
        // sibling OIDs from `RelationGetPartitionDesc(parent, false)->oids`
        // (threaded in here as `parent_partdesc`) and each sibling's bound spec
        // from its `relpartbound` catalog row.
        let pdesc = parent_partdesc.ok_or_else(|| {
            elog("default RANGE partition constraint requires the parent's PartitionDesc")
        })?;

        let mut or_expr_args: Vec<Expr> = Vec::new();

        for k in 0..(pdesc.nparts as usize) {
            let inhrelid = *pdesc.oids.get(k).ok_or_else(|| {
                elog("default RANGE partition: partdesc oid index out of range")
            })?;

            // SearchSysCache1(RELOID, inhrelid) + SysCacheGetAttrNotNull(
            //   relpartbound) + stringToNode/castNode(PartitionBoundSpec).
            let text = backend_utils_cache_syscache_seams::pg_class_relpartbound_text::call(
                inhrelid,
            )?
            .ok_or_else(|| {
                elog(&format!(
                    "missing relpartbound for partition relation {inhrelid}"
                ))
            })?;
            let bnode = backend_nodes_read_seams::string_to_node::call(mcx, &text)?;
            let bspec: PgBox<'mcx, PartitionBoundSpec<'mcx>> =
                match PgBox::into_inner(bnode).into_partitionboundspec() {
                    Some(spec) => mcx::alloc_in(mcx, spec)?,
                    None => return Err(elog("expected PartitionBoundSpec")),
                };

            if !bspec.is_default {
                // part_qual = get_qual_for_range(parent, bspec, true). The
                // sibling is non-default, so this recursion never re-enters the
                // default branch — no partdesc needed (pass None).
                let part_qual = get_qual_for_range(mcx, key, &bspec, true, None)?;

                // AND the sibling's constraint clauses and add to or_expr_args.
                or_expr_args.push(if part_qual.len() > 1 {
                    make_bool_expr(BoolExprType::AND_EXPR, part_qual, -1)
                } else {
                    part_qual
                        .into_iter()
                        .next()
                        .ok_or_else(|| elog("non-default sibling produced an empty qual"))?
                });
            }
        }

        let mut result: Vec<Expr> = Vec::new();
        if !or_expr_args.is_empty() {
            // Combine the non-default constraints with OR, AND in the per-key
            // NOT NULL tests (omitted from each sibling arm to avoid useless
            // repetition), then negate: the default holds everything NOT in the
            // siblings.
            let mut and_args = get_range_nulltest(mcx, key)?;
            and_args.push(if or_expr_args.len() > 1 {
                make_bool_expr(BoolExprType::OR_EXPR, or_expr_args, -1)
            } else {
                or_expr_args.into_iter().next().unwrap()
            });
            let other_parts_constr = make_bool_expr(BoolExprType::AND_EXPR, and_args, -1);
            result.push(make_bool_expr(
                BoolExprType::NOT_EXPR,
                vec![other_parts_constr],
                -1,
            ));
        }

        return Ok(result);
    }

    let mut result: Vec<Expr> = Vec::new();

    // For the non-default partition, prepend the per-column IS NOT NULL tests.
    if !for_default {
        result = get_range_nulltest(mcx, key)?;
    }

    let lower = &spec.lowerdatums;
    let upper = &spec.upperdatums;
    let ncols = lower.len();
    debug_assert!(ncols == upper.len());

    let mut partexprs_idx = 0usize;
    let mut partexprs_idx_saved = 0usize;

    // Phase 1: emit `keyCol = val` for each leading column whose lower and upper
    // bounds are equal.
    let mut i = 0usize;
    while i < ncols {
        partexprs_idx_saved = partexprs_idx;

        let ldatum = range_datum(&lower[i])?;
        let udatum = range_datum(&upper[i])?;
        let (key_col, lower_val, upper_val) =
            get_range_key_properties(mcx, key, i, ldatum, udatum, &mut partexprs_idx)?;

        // If either is MINVALUE/MAXVALUE, treat as unequal.
        let (lv, uv) = match (&lower_val, &upper_val) {
            (Some(lv), Some(uv)) => (lv, uv),
            _ => break,
        };

        // bounds equal? compare via the btree CMP support function (== 0).
        let collation = key.partcollation[i];
        let cmp = call_cmp(
            &key.partsupfunc[i],
            collation,
            lv.constvalue.clone(),
            uv.constvalue.clone(),
        )?;
        if cmp != 0 {
            break;
        }

        // The last key column's bounds can't be equal (empty range).
        if i == (key.partnatts as usize) - 1 {
            return Err(elog("invalid range bound specification"));
        }

        // keyCol = lower_val
        if let Some(op) = make_partition_op_expr(
            mcx,
            key,
            i,
            BTEqualStrategyNumber,
            key_col,
            MakeOpArg::Single(Expr::Const(lower_val.unwrap())),
        )? {
            result.push(op);
        }

        i += 1;
    }

    // Phase 2: OR-arms for the remaining columns (the function-header tree).
    let start = i;
    let num_or_arms = (key.partnatts as usize) - i;
    let mut current_or_arm = 0usize;
    let mut lower_or_arms: Vec<Expr> = Vec::new();
    let mut upper_or_arms: Vec<Expr> = Vec::new();
    let mut need_next_lower_arm = true;
    let mut need_next_upper_arm = true;

    while current_or_arm < num_or_arms {
        let mut lower_or_arm_args: Vec<Expr> = Vec::new();
        let mut upper_or_arm_args: Vec<Expr> = Vec::new();

        let mut j = start;
        partexprs_idx = partexprs_idx_saved;

        // Iterate columns from `start` (== i) to the end of the bounds.
        let mut col = start;
        while col < ncols {
            let ldatum = range_datum(&lower[col])?;
            let udatum = range_datum(&upper[col])?;
            let ldatum_next = if col + 1 < ncols {
                Some(range_datum(&lower[col + 1])?)
            } else {
                None
            };
            let udatum_next = if col + 1 < ncols {
                Some(range_datum(&upper[col + 1])?)
            } else {
                None
            };

            let (key_col, lower_val, upper_val) =
                get_range_key_properties(mcx, key, j, ldatum, udatum, &mut partexprs_idx)?;

            if need_next_lower_arm {
                if let Some(lv) = &lower_val {
                    let strategy = if j - i < current_or_arm {
                        BTEqualStrategyNumber
                    } else if j == (key.partnatts as usize) - 1
                        || ldatum_next
                            .map(|d| d.kind == PartitionRangeDatumKind::MinValue)
                            .unwrap_or(false)
                    {
                        BTGreaterEqualStrategyNumber
                    } else {
                        BTGreaterStrategyNumber
                    };
                    if let Some(op) = make_partition_op_expr(
                        mcx,
                        key,
                        j,
                        strategy,
                        key_col.clone(),
                        MakeOpArg::Single(Expr::Const(lv.clone())),
                    )? {
                        lower_or_arm_args.push(op);
                    }
                }
            }

            if need_next_upper_arm {
                if let Some(uv) = &upper_val {
                    let strategy = if j - i < current_or_arm {
                        BTEqualStrategyNumber
                    } else if udatum_next
                        .map(|d| d.kind == PartitionRangeDatumKind::MaxValue)
                        .unwrap_or(false)
                    {
                        BTLessEqualStrategyNumber
                    } else {
                        BTLessStrategyNumber
                    };
                    if let Some(op) = make_partition_op_expr(
                        mcx,
                        key,
                        j,
                        strategy,
                        key_col.clone(),
                        MakeOpArg::Single(Expr::Const(uv.clone())),
                    )? {
                        upper_or_arm_args.push(op);
                    }
                }
            }

            j += 1;
            if j - i > current_or_arm {
                // Stop this arm if the next column to consider is unbounded.
                if lower_val.is_none()
                    || ldatum_next
                        .map(|d| d.kind != PartitionRangeDatumKind::Value)
                        .unwrap_or(true)
                {
                    need_next_lower_arm = false;
                }
                if upper_val.is_none()
                    || udatum_next
                        .map(|d| d.kind != PartitionRangeDatumKind::Value)
                        .unwrap_or(true)
                {
                    need_next_upper_arm = false;
                }
                break;
            }
            col += 1;
        }

        if !lower_or_arm_args.is_empty() {
            lower_or_arms.push(if lower_or_arm_args.len() > 1 {
                make_bool_expr(BoolExprType::AND_EXPR, lower_or_arm_args, -1)
            } else {
                lower_or_arm_args.into_iter().next().unwrap()
            });
        }
        if !upper_or_arm_args.is_empty() {
            upper_or_arms.push(if upper_or_arm_args.len() > 1 {
                make_bool_expr(BoolExprType::AND_EXPR, upper_or_arm_args, -1)
            } else {
                upper_or_arm_args.into_iter().next().unwrap()
            });
        }

        if !need_next_lower_arm && !need_next_upper_arm {
            break;
        }
        current_or_arm += 1;
    }

    // Generate the OR expressions for lower and upper bounds.
    if !lower_or_arms.is_empty() {
        result.push(if lower_or_arms.len() > 1 {
            make_bool_expr(BoolExprType::OR_EXPR, lower_or_arms, -1)
        } else {
            lower_or_arms.into_iter().next().unwrap()
        });
    }
    if !upper_or_arms.is_empty() {
        result.push(if upper_or_arms.len() > 1 {
            make_bool_expr(BoolExprType::OR_EXPR, upper_or_arms, -1)
        } else {
            upper_or_arms.into_iter().next().unwrap()
        });
    }

    // As in C: an empty result means TRUE (or, for the default recursion, the
    // nulltest list).
    if result.is_empty() {
        result = if for_default {
            get_range_nulltest(mcx, key)?
        } else {
            vec![Expr::Const(make_bool_const(true, false))]
        };
    }

    Ok(result)
}

/// `get_qual_from_partbound(parent, spec)` (partbounds.c:249) — dispatch by the
/// parent's partition strategy. `key` is the parent's `RelationGetPartitionKey`.
///
/// Returns the implicit-AND list of `Expr` clauses.
pub fn get_qual_from_partbound<'mcx>(
    mcx: Mcx<'mcx>,
    parent_relid: Oid,
    key: &PartitionKeyData<'_>,
    spec: &PartitionBoundSpec<'_>,
    parent_partdesc: Option<&PartitionDescData<'_>>,
) -> PgResult<Vec<Expr<'mcx>>> {
    match key.strategy {
        PartitionStrategy::Hash => get_qual_for_hash(mcx, parent_relid, key, spec),
        PartitionStrategy::List => get_qual_for_list(mcx, key, spec, parent_partdesc),
        PartitionStrategy::Range => {
            get_qual_for_range(mcx, key, spec, false, parent_partdesc)
        }
    }
}

/// Install body for the `qual_from_partbound` seam (partcache.c's
/// `generate_partition_qual` leg): read `relpartbound` for `relid`, parse it to a
/// `PartitionBoundSpec`, look up the parent's partition key, then
/// `get_qual_from_partbound`. Returns the implicit-AND qual list as `Node`s.
pub fn qual_from_partbound_seam<'mcx, 'p>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    parent: &types_rel::RelationData<'p>,
) -> PgResult<mcx::PgVec<'mcx, Node<'mcx>>> {
    // datum = SysCacheGetAttr(RELOID, relid, relpartbound, &isnull); if isnull,
    // my_qual stays NIL.
    let text = backend_utils_cache_syscache_seams::pg_class_relpartbound_text::call(relid)?;
    let mut out = mcx::PgVec::new_in(mcx);
    let Some(text) = text else {
        return Ok(out);
    };

    // bound = castNode(PartitionBoundSpec, stringToNode(...));
    let bound_node = backend_nodes_read_seams::string_to_node::call(mcx, &text)?;
    let bound: PgBox<'mcx, PartitionBoundSpec<'mcx>> =
        match PgBox::into_inner(bound_node).into_partitionboundspec() {
            Some(spec) => mcx::alloc_in(mcx, spec)?,
            None => return Err(elog("invalid relpartbound: stringToNode did not yield a PartitionBoundSpec")),
        };

    // key = RelationGetPartitionKey(parent); the parent is already open and
    // locked by the caller (generate_partition_qual holds it across this seam),
    // so we re-acquire a NoLock handle to obtain an aliasable `Relation` for the
    // partition-key lookup, then drop it (NoLock release is a no-op).
    use types_storage::lock::NoLock;
    let parent_rel =
        backend_access_common_relation_seams::relation_open::call(mcx, parent.rd_id, NoLock)?;
    let key = backend_utils_cache_partcache_seams::relation_get_partition_key::call(
        mcx,
        parent_rel.alias(),
    )?
    .ok_or_else(|| elog("get_qual_from_partbound: parent has no partition key"))?;

    // For a DEFAULT partition the constraint is the negation of all siblings'
    // bounds, which C reads from `RelationGetPartitionDesc(parent, false)`. Fetch
    // it only when needed (the partdesc crate depends on this one, so the call
    // crosses the partdesc inward seam). `omit_detached=false` matches C.
    let pdesc = if bound.is_default {
        Some(backend_partitioning_partdesc_seams::relation_get_partition_desc::call(
            mcx,
            &parent_rel.alias(),
            false,
        )?)
    } else {
        None
    };

    let exprs = get_qual_from_partbound(mcx, parent.rd_id, &key, &bound, pdesc.as_deref())?;
    parent_rel.close(NoLock)?;
    for e in exprs {
        out.push(node(mcx, e)?);
    }
    Ok(out)
}
