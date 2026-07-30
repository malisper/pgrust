// get_qual_from_partbound family, satisfies_hash_partition, and
// check_default_partition_contents (partbounds.c).
use datum::Datum;
use mcx::Mcx;
use types_core::{
    InvalidOid, Oid, ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID,
    ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID,
    ANYMULTIRANGEOID, ANYNONARRAYOID, ANYRANGEOID, BOOLOID, INT4OID, OIDOID, RECORDOID,
};
use types_error::{
    PgError, PgResult, ERRCODE_CHECK_VIOLATION, ERRCODE_INVALID_PARAMETER_VALUE, ERROR,
};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo, PGFunction};
use types_nodes::primnodes::{
    ArrayExpr, BoolExpr, BoolExprType, Const, FuncExpr, NullTest, NullTestType, OpExpr,
    RelabelType, ScalarArrayOpExpr, Var,
};
use types_nodes::rawnodes::{PartitionBoundSpec, PartitionRangeDatum, PartitionRangeDatumKind};
use types_nodes::CoercionForm;
use types_nodes::{Node, NodeList};
use types_rel::{
    AccessExclusiveLock, AccessShareLock, NoLock, Relation, RELKIND_FOREIGN_TABLE,
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};

use partcache::PartitionKeyData;

use crate::{
    datum_copy, hash_combine64, PartitionBoundInfoData, HASH_PARTITION_SEED,
    PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE,
};

const BTLessStrategyNumber: i16 = 1;
const BTLessEqualStrategyNumber: i16 = 2;
const BTEqualStrategyNumber: i16 = 3;
const BTGreaterEqualStrategyNumber: i16 = 4;
const BTGreaterStrategyNumber: i16 = 5;

const RELOID: i32 = cache_syscache::cacheinfo::RELOID;
const Anum_pg_class_relpartbound: i32 = 34;
pub const F_SATISFIES_HASH_PARTITION: Oid = 5028;

pub fn get_qual_from_partbound<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData,
    parent_oid: Oid,
    boundinfo: Option<&PartitionBoundInfoData<'_>>,
    part_oids: &[Oid],
    spec: &PartitionBoundSpec<'_>,
) -> PgResult<NodeList<'mcx>> {
    match key.strategy as u8 {
        PARTITION_STRATEGY_HASH => {
            assert!(spec.strategy == PARTITION_STRATEGY_HASH);
            get_qual_for_hash(mcx, key, parent_oid, spec)
        }
        PARTITION_STRATEGY_LIST => {
            assert!(spec.strategy == PARTITION_STRATEGY_LIST || spec.is_default);
            get_qual_for_list(mcx, key, boundinfo, spec)
        }
        PARTITION_STRATEGY_RANGE => {
            assert!(spec.strategy == PARTITION_STRATEGY_RANGE || spec.is_default);
            get_qual_for_range(mcx, key, part_oids, spec, false)
        }
        other => panic!("unexpected partition strategy: {}", other as char),
    }
}

fn is_polymorphic_type(typid: Oid) -> bool {
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

fn get_partition_operator(
    key: &PartitionKeyData,
    col: usize,
    strategy: i16,
) -> PgResult<(Oid, bool)> {
    let operoid = lsyscache::get_opfamily_member(
        key.partopfamily[col],
        key.partopcintype[col],
        key.partopcintype[col],
        strategy,
    )?;
    if operoid == InvalidOid {
        return Err(Box::new(PgError::new(
            ERROR,
            format!(
                "missing operator {strategy}({},{}) in partition opfamily {}",
                key.partopcintype[col], key.partopcintype[col], key.partopfamily[col]
            ),
        )));
    }
    let need_relabel = key.parttypid[col] != key.partopcintype[col]
        && key.partopcintype[col] != RECORDOID
        && !is_polymorphic_type(key.partopcintype[col]);
    Ok((operoid, need_relabel))
}

fn make_key_var<'mcx>(mcx: Mcx<'mcx>, key: &PartitionKeyData, i: usize) -> PgResult<Node<'mcx>> {
    let attno = key.partattrs[i];
    if attno == 0 {
        // C walks partexprs with a ListCell cursor; the i-th expression key is
        // preceded by exprno zero entries in partattrs.
        let exprno = key.partattrs[..i].iter().filter(|&&a| a == 0).count();
        let expr = key
            .partexprs
            .iter()
            .nth(exprno)
            .unwrap_or_else(|| panic!("wrong number of partition key expressions"));
        // copyObject: qual trees outlive this call and must not alias the
        // cache's partexprs.
        return copyfuncs::copy_object(mcx, expr);
    }
    Node::mk(
        mcx,
        Var {
            varno: 1,
            varattno: attno,
            vartype: key.parttypid[i],
            vartypmod: key.parttypmod[i],
            varcollid: key.parttypcoll[i],
            varnosyn: 1,
            varattnosyn: attno,
            ..Default::default()
        },
    )
}

// copyObject over a bound Const: the datum image is copied into `mcx`.
fn copy_const_node<'mcx>(mcx: Mcx<'mcx>, node: Node<'_>) -> PgResult<Node<'mcx>> {
    let c = node.as_variant::<Const>().expect("partition bound datum is not a Const");
    let mut copy = *c;
    if !copy.constisnull {
        copy.constvalue = datum_copy(mcx, copy.constvalue, copy.constbyval, copy.constlen as i16)?;
    }
    Node::mk(mcx, copy)
}

fn make_opclause<'mcx>(
    mcx: Mcx<'mcx>,
    opno: Oid,
    arg1: Node<'mcx>,
    arg2: Node<'mcx>,
    inputcollid: Oid,
) -> PgResult<Node<'mcx>> {
    let mut args = NodeList::nil();
    args.lappend(mcx, arg1)?;
    args.lappend(mcx, arg2)?;
    Node::mk(
        mcx,
        OpExpr {
            opno,
            // fix_opfuncids applied at build time.
            opfuncid: lsyscache::get_opcode(opno)?,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: InvalidOid,
            inputcollid,
            args,
            location: -1,
        },
    )
}

fn make_bool_expr<'mcx>(
    mcx: Mcx<'mcx>,
    boolop: BoolExprType,
    args: NodeList<'mcx>,
) -> PgResult<Node<'mcx>> {
    Node::mk(mcx, BoolExpr { boolop, args, location: -1 })
}

fn make_bool_const<'mcx>(mcx: Mcx<'mcx>, value: bool, isnull: bool) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        Const {
            consttype: BOOLOID,
            consttypmod: -1,
            constcollid: InvalidOid,
            constlen: 1,
            constvalue: Datum::from_bool(value),
            constisnull: isnull,
            constbyval: true,
            location: -1,
        },
    )
}

enum OpArg<'mcx> {
    Scalar(Node<'mcx>),
    List(NodeList<'mcx>),
}

fn make_partition_op_expr<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData,
    keynum: usize,
    strategy: i16,
    arg1: Node<'mcx>,
    arg2: OpArg<'mcx>,
) -> PgResult<Node<'mcx>> {
    let (operoid, need_relabel) = get_partition_operator(key, keynum, strategy)?;
    let mut arg1 = arg1;
    if arg1.as_variant::<Const>().is_none()
        && (need_relabel || key.partcollation[keynum] != key.parttypcoll[keynum])
    {
        arg1 = Node::mk(
            mcx,
            RelabelType {
                arg: arg1,
                resulttype: key.partopcintype[keynum],
                resulttypmod: -1,
                resultcollid: key.partcollation[keynum],
                relabelformat: CoercionForm::COERCE_EXPLICIT_CAST,
                location: -1,
            },
        )?;
    }
    match key.strategy as u8 {
        PARTITION_STRATEGY_LIST => {
            let OpArg::List(elems) = arg2 else {
                panic!("make_partition_op_expr: list strategy takes an element list")
            };
            let nelems = elems.len();
            assert!(nelems >= 1);
            assert!(keynum == 0);
            let type_is_array = lsyscache::get_element_type(key.parttypid[keynum])? != InvalidOid;
            if nelems > 1 && !type_is_array {
                let arrexpr = Node::mk(
                    mcx,
                    ArrayExpr {
                        array_typeid: lsyscache::get_array_type(key.parttypid[keynum])?,
                        array_collid: key.parttypcoll[keynum],
                        element_typeid: key.parttypid[keynum],
                        elements: elems,
                        multidims: false,
                        list_start: -1,
                        list_end: -1,
                        location: -1,
                    },
                )?;
                let mut args = NodeList::nil();
                args.lappend(mcx, arg1)?;
                args.lappend(mcx, arrexpr)?;
                Node::mk(
                    mcx,
                    ScalarArrayOpExpr {
                        opno: operoid,
                        opfuncid: lsyscache::get_opcode(operoid)?,
                        hashfuncid: InvalidOid,
                        negfuncid: InvalidOid,
                        useOr: true,
                        inputcollid: key.partcollation[keynum],
                        args,
                        location: -1,
                    },
                )
            } else {
                let mut elemops = NodeList::nil();
                for elem in elems.iter() {
                    elemops.lappend(
                        mcx,
                        make_opclause(mcx, operoid, arg1, elem, key.partcollation[keynum])?,
                    )?;
                }
                if nelems > 1 {
                    make_bool_expr(mcx, BoolExprType::OR_EXPR, elemops)
                } else {
                    Ok(elemops.nth(0))
                }
            }
        }
        PARTITION_STRATEGY_RANGE => {
            let OpArg::Scalar(a2) = arg2 else {
                panic!("make_partition_op_expr: range strategy takes a scalar")
            };
            make_opclause(mcx, operoid, arg1, a2, key.partcollation[keynum])
        }
        other => panic!("make_partition_op_expr: unexpected strategy {}", other as char),
    }
}

fn get_qual_for_hash<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData,
    parent_oid: Oid,
    spec: &PartitionBoundSpec<'_>,
) -> PgResult<NodeList<'mcx>> {
    let mut args = NodeList::nil();
    args.lappend(
        mcx,
        Node::mk(
            mcx,
            Const {
                consttype: OIDOID,
                consttypmod: -1,
                constcollid: InvalidOid,
                constlen: 4,
                constvalue: Datum::from_oid(parent_oid),
                constisnull: false,
                constbyval: true,
                location: -1,
            },
        )?,
    )?;
    for v in [spec.modulus, spec.remainder] {
        args.lappend(
            mcx,
            Node::mk(
                mcx,
                Const {
                    consttype: INT4OID,
                    consttypmod: -1,
                    constcollid: InvalidOid,
                    constlen: 4,
                    constvalue: Datum::from_i32(v),
                    constisnull: false,
                    constbyval: true,
                    location: -1,
                },
            )?,
        )?;
    }
    for i in 0..key.partnatts as usize {
        args.lappend(mcx, make_key_var(mcx, key, i)?)?;
    }
    let fexpr = Node::mk(
        mcx,
        FuncExpr {
            funcid: F_SATISFIES_HASH_PARTITION,
            funcresulttype: BOOLOID,
            funcretset: false,
            funcvariadic: false,
            funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
            funccollid: InvalidOid,
            inputcollid: InvalidOid,
            args,
            location: -1,
        },
    )?;
    NodeList::make1(mcx, fexpr)
}

fn get_qual_for_list<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData,
    boundinfo: Option<&PartitionBoundInfoData<'_>>,
    spec: &PartitionBoundSpec<'_>,
) -> PgResult<NodeList<'mcx>> {
    assert!(key.partnatts == 1);
    let key_col = make_key_var(mcx, key, 0)?;
    let mut elems = NodeList::nil();
    let mut list_has_null = false;

    if spec.is_default {
        let mut ndatums = 0;
        if let Some(b) = boundinfo {
            ndatums = b.ndatums;
            if b.accepts_nulls() {
                list_has_null = true;
            }
        }
        if ndatums == 0 && !list_has_null {
            return Ok(NodeList::nil());
        }
        let b = boundinfo.expect("ndatums > 0");
        for i in 0..ndatums {
            let val = Node::mk(
                mcx,
                Const {
                    consttype: key.parttypid[0],
                    consttypmod: key.parttypmod[0],
                    constcollid: key.parttypcoll[0],
                    constlen: key.parttyplen[0] as i32,
                    constvalue: datum_copy(
                        mcx,
                        b.datum(i, 0),
                        key.parttypbyval[0],
                        key.parttyplen[0],
                    )?,
                    constisnull: false,
                    constbyval: key.parttypbyval[0],
                    location: -1,
                },
            )?;
            elems.lappend(mcx, val)?;
        }
    } else {
        for cell in spec.listdatums.iter() {
            let val = cell.as_variant::<Const>().expect("list bound datum is not a Const");
            if val.constisnull {
                list_has_null = true;
            } else {
                elems.lappend(mcx, copy_const_node(mcx, cell)?)?;
            }
        }
    }

    let opexpr = if !elems.is_nil() {
        Some(make_partition_op_expr(
            mcx,
            key,
            0,
            BTEqualStrategyNumber,
            key_col,
            OpArg::List(elems),
        )?)
    } else {
        None
    };

    let mut result = NodeList::nil();
    if !list_has_null {
        let nulltest = Node::mk(
            mcx,
            NullTest {
                arg: Some(key_col),
                nulltesttype: NullTestType::IS_NOT_NULL,
                argisrow: false,
                location: -1,
            },
        )?;
        result.lappend(mcx, nulltest)?;
        if let Some(op) = opexpr {
            result.lappend(mcx, op)?;
        }
    } else {
        let nulltest = Node::mk(
            mcx,
            NullTest {
                arg: Some(key_col),
                nulltesttype: NullTestType::IS_NULL,
                argisrow: false,
                location: -1,
            },
        )?;
        if let Some(op) = opexpr {
            let mut or_args = NodeList::nil();
            or_args.lappend(mcx, nulltest)?;
            or_args.lappend(mcx, op)?;
            result.lappend(mcx, make_bool_expr(mcx, BoolExprType::OR_EXPR, or_args)?)?;
        } else {
            result.lappend(mcx, nulltest)?;
        }
    }

    if spec.is_default {
        // The constraints built here never evaluate to NULL, so negation is
        // exact (C's note in get_qual_for_list).
        let anded = make_ands_explicit(mcx, result)?;
        let not = make_bool_expr(mcx, BoolExprType::NOT_EXPR, NodeList::make1(mcx, anded)?)?;
        result = NodeList::make1(mcx, not)?;
    }
    Ok(result)
}

// text varlena -> &str, inline images only (relpartbound is written inline).
fn text_to_str<'mcx>(mcx: ::mcx::Mcx<'mcx>, d: Datum) -> &'mcx str {
    let p = d.as_usize() as *const u8;
    // SAFETY: syscache text attribute; toasted/compressed images are loud.
    unsafe {
        let b0 = *p;
        let (len, off) = if b0 & 0x01 != 0 {
            if b0 == 0x01 {
                panic!("partbounds: toasted relpartbound unported");
            }
            ((((b0 as usize) >> 1) & 0x7F) - 1, 1)
        } else {
            let w = u32::from_ne_bytes(core::slice::from_raw_parts(p, 4).try_into().unwrap());
            if w & 0x02 != 0 {
                let total = ::types_tuple::varatt::varsize_any(p);
                let raw = core::slice::from_raw_parts(p, total);
                let flat = ::detoast_seams::detoast_attr::call(mcx, raw)
                    .expect("detoast relpartbound");
                let (ptr, len) = (flat.as_ptr(), flat.len());
                core::mem::forget(flat);
                // detoast_attr returns the full 4-byte-header image; the
                // payload follows. Arena-backed until mcx reset; forget only
                // skips the vec's own dealloc.
                let s = core::slice::from_raw_parts(ptr.add(4), len - 4);
                return core::str::from_utf8(s).expect("non-UTF-8 relpartbound");
            }
            ((w as usize >> 2) - 4, 4)
        };
        core::str::from_utf8(core::slice::from_raw_parts(p.add(off), len))
            .expect("non-UTF-8 relpartbound")
    }
}

pub fn read_boundspec<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<&'mcx PartitionBoundSpec<'mcx>> {
    Ok(read_boundspec_opt(mcx, relid)?
        .unwrap_or_else(|| panic!("missing relpartbound for relation {relid}")))
}

// NULL relpartbound is legal: index partitions carry relispartition without
// a bound (C generate_partition_qual reads the attr with isnull).
pub fn read_boundspec_opt<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Option<&'mcx PartitionBoundSpec<'mcx>>> {
    let tuple = cache_syscache::SearchSysCache1(
        RELOID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(relid)),
    )?
    .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let (d, isnull) =
        cache_syscache::SysCacheGetAttr(RELOID, &tuple, Anum_pg_class_relpartbound)?;
    if isnull {
        cache_syscache::ReleaseSysCache(tuple);
        return Ok(None);
    }
    let node = readfuncs::stringToNode(mcx, text_to_str(mcx, d))?;
    cache_syscache::ReleaseSysCache(tuple);
    Ok(Some(
        node.as_variant::<PartitionBoundSpec>()
            .unwrap_or_else(|| panic!("expected PartitionBoundSpec")),
    ))
}

fn get_range_key_properties<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData,
    keynum: usize,
    ldatum: &PartitionRangeDatum<'_>,
    udatum: &PartitionRangeDatum<'_>,
) -> PgResult<(Node<'mcx>, Option<Node<'mcx>>, Option<Node<'mcx>>)> {
    let key_col = make_key_var(mcx, key, keynum)?;
    let lower_val = if ldatum.kind == PartitionRangeDatumKind::Value {
        Some(copy_const_node(mcx, ldatum.value.expect("PartitionRangeDatum value"))?)
    } else {
        None
    };
    let upper_val = if udatum.kind == PartitionRangeDatumKind::Value {
        Some(copy_const_node(mcx, udatum.value.expect("PartitionRangeDatum value"))?)
    } else {
        None
    };
    Ok((key_col, lower_val, upper_val))
}

fn get_range_nulltest<'mcx>(mcx: Mcx<'mcx>, key: &PartitionKeyData) -> PgResult<NodeList<'mcx>> {
    let mut result = NodeList::nil();
    for i in 0..key.partnatts as usize {
        let key_col = make_key_var(mcx, key, i)?;
        result.lappend(
            mcx,
            Node::mk(
                mcx,
                NullTest {
                    arg: Some(key_col),
                    nulltesttype: NullTestType::IS_NOT_NULL,
                    argisrow: false,
                    location: -1,
                },
            )?,
        )?;
    }
    Ok(result)
}

fn get_qual_for_range<'mcx>(
    mcx: Mcx<'mcx>,
    key: &PartitionKeyData,
    part_oids: &[Oid],
    spec: &PartitionBoundSpec<'_>,
    for_default: bool,
) -> PgResult<NodeList<'mcx>> {
    if spec.is_default {
        let mut or_expr_args = NodeList::nil();
        for &inhrelid in part_oids {
            let bspec = read_boundspec(mcx, inhrelid)?;
            if bspec.is_default {
                continue;
            }
            let part_qual = get_qual_for_range(mcx, key, part_oids, bspec, true)?;
            or_expr_args.lappend(
                mcx,
                if part_qual.len() > 1 {
                    make_bool_expr(mcx, BoolExprType::AND_EXPR, part_qual)?
                } else {
                    part_qual.nth(0)
                },
            )?;
        }
        let mut result = NodeList::nil();
        if !or_expr_args.is_nil() {
            let mut and_args = get_range_nulltest(mcx, key)?;
            and_args.lappend(
                mcx,
                if or_expr_args.len() > 1 {
                    make_bool_expr(mcx, BoolExprType::OR_EXPR, or_expr_args)?
                } else {
                    or_expr_args.nth(0)
                },
            )?;
            let other_parts_constr = make_bool_expr(mcx, BoolExprType::AND_EXPR, and_args)?;
            let not = make_bool_expr(
                mcx,
                BoolExprType::NOT_EXPR,
                NodeList::make1(mcx, other_parts_constr)?,
            )?;
            result = NodeList::make1(mcx, not)?;
        }
        return Ok(result);
    }

    let mut result =
        if !for_default { get_range_nulltest(mcx, key)? } else { NodeList::nil() };

    let lowers: Vec<&PartitionRangeDatum<'_>> = spec
        .lowerdatums
        .iter()
        .map(|n| n.as_variant::<PartitionRangeDatum>().expect("PartitionRangeDatum"))
        .collect();
    let uppers: Vec<&PartitionRangeDatum<'_>> = spec
        .upperdatums
        .iter()
        .map(|n| n.as_variant::<PartitionRangeDatum>().expect("PartitionRangeDatum"))
        .collect();
    let npairs = lowers.len().min(uppers.len());
    let partnatts = key.partnatts as usize;

    let mut i = 0usize;
    while i < npairs {
        let (key_col, lower_val, upper_val) =
            get_range_key_properties(mcx, key, i, lowers[i], uppers[i])?;
        let (Some(lv), Some(_uv)) = (lower_val, upper_val) else { break };
        let lc = lv.as_variant::<Const>().expect("Const");
        let uc = upper_val.unwrap().as_variant::<Const>().expect("Const");
        // C evaluates the btree = operator via the executor here; the
        // partsupfunc comparator == 0 is equivalent for btree opfamilies.
        if key.cmp(i, lc.constvalue, uc.constvalue)? != 0 {
            break;
        }
        if i == partnatts - 1 {
            return Err(Box::new(PgError::new(
                ERROR,
                "invalid range bound specification".to_string(),
            )));
        }
        result.lappend(
            mcx,
            make_partition_op_expr(mcx, key, i, BTEqualStrategyNumber, key_col, OpArg::Scalar(lv))?,
        )?;
        i += 1;
    }

    let start = i;
    let num_or_arms = partnatts - i;
    let mut current_or_arm = 0usize;
    let mut lower_or_arms = NodeList::nil();
    let mut upper_or_arms = NodeList::nil();
    let mut need_next_lower_arm = true;
    let mut need_next_upper_arm = true;
    while current_or_arm < num_or_arms {
        let mut lower_or_arm_args = NodeList::nil();
        let mut upper_or_arm_args = NodeList::nil();
        let mut j = i;
        for idx in start..npairs {
            let ldatum = lowers[idx];
            let udatum = uppers[idx];
            let ldatum_next = lowers.get(idx + 1).copied();
            let udatum_next = uppers.get(idx + 1).copied();
            let (key_col, lower_val, upper_val) =
                get_range_key_properties(mcx, key, j, ldatum, udatum)?;

            if need_next_lower_arm {
                if let Some(lv) = lower_val {
                    let strategy = if j - i < current_or_arm {
                        BTEqualStrategyNumber
                    } else if j == partnatts - 1
                        || ldatum_next
                            .is_some_and(|d| d.kind == PartitionRangeDatumKind::Minvalue)
                    {
                        BTGreaterEqualStrategyNumber
                    } else {
                        BTGreaterStrategyNumber
                    };
                    lower_or_arm_args.lappend(
                        mcx,
                        make_partition_op_expr(mcx, key, j, strategy, key_col, OpArg::Scalar(lv))?,
                    )?;
                }
            }
            if need_next_upper_arm {
                if let Some(uv) = upper_val {
                    let strategy = if j - i < current_or_arm {
                        BTEqualStrategyNumber
                    } else if udatum_next
                        .is_some_and(|d| d.kind == PartitionRangeDatumKind::Maxvalue)
                    {
                        BTLessEqualStrategyNumber
                    } else {
                        BTLessStrategyNumber
                    };
                    upper_or_arm_args.lappend(
                        mcx,
                        make_partition_op_expr(mcx, key, j, strategy, key_col, OpArg::Scalar(uv))?,
                    )?;
                }
            }

            j += 1;
            if j - i > current_or_arm {
                if lower_val.is_none()
                    || !ldatum_next.is_some_and(|d| d.kind == PartitionRangeDatumKind::Value)
                {
                    need_next_lower_arm = false;
                }
                if upper_val.is_none()
                    || !udatum_next.is_some_and(|d| d.kind == PartitionRangeDatumKind::Value)
                {
                    need_next_upper_arm = false;
                }
                break;
            }
        }
        if !lower_or_arm_args.is_nil() {
            lower_or_arms.lappend(
                mcx,
                if lower_or_arm_args.len() > 1 {
                    make_bool_expr(mcx, BoolExprType::AND_EXPR, lower_or_arm_args)?
                } else {
                    lower_or_arm_args.nth(0)
                },
            )?;
        }
        if !upper_or_arm_args.is_nil() {
            upper_or_arms.lappend(
                mcx,
                if upper_or_arm_args.len() > 1 {
                    make_bool_expr(mcx, BoolExprType::AND_EXPR, upper_or_arm_args)?
                } else {
                    upper_or_arm_args.nth(0)
                },
            )?;
        }
        if !need_next_lower_arm && !need_next_upper_arm {
            break;
        }
        current_or_arm += 1;
    }
    if !lower_or_arms.is_nil() {
        result.lappend(
            mcx,
            if lower_or_arms.len() > 1 {
                make_bool_expr(mcx, BoolExprType::OR_EXPR, lower_or_arms)?
            } else {
                lower_or_arms.nth(0)
            },
        )?;
    }
    if !upper_or_arms.is_nil() {
        result.lappend(
            mcx,
            if upper_or_arms.len() > 1 {
                make_bool_expr(mcx, BoolExprType::OR_EXPR, upper_or_arms)?
            } else {
                upper_or_arms.nth(0)
            },
        )?;
    }

    if result.is_nil() {
        result = if for_default {
            get_range_nulltest(mcx, key)?
        } else {
            NodeList::make1(mcx, make_bool_const(mcx, true, false)?)?
        };
    }
    Ok(result)
}

// map_partition_varattnos (catalog/partition.c): translate fromrel-numbered
// Vars to to_rel attnos; whole-row Vars become ConvertRowtypeExpr(to_rel row).
pub fn map_partition_varattnos<'mcx>(
    mcx: Mcx<'mcx>,
    expr: NodeList<'mcx>,
    fromrel_varno: i32,
    to_rel: &Relation<'_>,
    from_rel: &Relation<'_>,
) -> PgResult<NodeList<'mcx>> {
    if expr.is_nil() {
        return Ok(expr);
    }
    let attmap = tupdesc::build_attrmap_by_name(mcx, to_rel.descr(), from_rel.descr())?;
    let node = Node::mk_list(mcx, expr)?;
    let (mapped, _found_whole_row) = rewrite_manip::map_variable_attnos(
        mcx,
        node,
        fromrel_varno,
        0,
        &attmap,
        to_rel.rd_rel.reltype,
    )?;
    Ok(mapped.as_list().expect("List").clone_in(mcx)?)
}

// make_ands_explicit (makefuncs.c).
pub fn make_ands_explicit<'mcx>(mcx: Mcx<'mcx>, exprs: NodeList<'mcx>) -> PgResult<Node<'mcx>> {
    match exprs.len() {
        0 => make_bool_const(mcx, true, false),
        1 => Ok(exprs.nth(0)),
        _ => make_bool_expr(mcx, BoolExprType::AND_EXPR, exprs),
    }
}

// make_ands_implicit (makefuncs.c).
fn make_ands_implicit<'mcx>(mcx: Mcx<'mcx>, clause: Node<'mcx>) -> PgResult<NodeList<'mcx>> {
    if let Some(b) = clause.as_variant::<BoolExpr>() {
        if b.boolop == BoolExprType::AND_EXPR {
            let mut out = NodeList::nil();
            for n in b.args.iter() {
                out.lappend(mcx, n)?;
            }
            return Ok(out);
        }
    }
    if let Some(c) = clause.as_variant::<Const>() {
        if !c.constisnull && c.consttype == BOOLOID && c.constvalue.as_bool() {
            return Ok(NodeList::nil());
        }
    }
    NodeList::make1(mcx, clause)
}

// get_proposed_default_constraint (catalog/partition.c). C also runs
// canonicalize_qual after simplification; skipped — consumers here evaluate
// the expression directly, so canonical form is not load-bearing.
pub fn get_proposed_default_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    new_part_constraints: NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let def = make_ands_explicit(mcx, new_part_constraints)?;
    let not = make_bool_expr(mcx, BoolExprType::NOT_EXPR, NodeList::make1(mcx, def)?)?;
    let simplified = clauses_seams::eval_const_expressions::call(mcx, not)?;
    make_ands_implicit(mcx, simplified)
}

// check_default_partition_contents (partbounds.c). C first tries
// PartConstraintImpliedByRelConstraint to skip the scan; no predicate_implied_by
// here, so the default partition is always scanned (DEBUG1-only divergence).
pub fn check_default_partition_contents<'mcx>(
    mcx: Mcx<'mcx>,
    parent: &Relation<'mcx>,
    default_rel: &Relation<'mcx>,
    key: &PartitionKeyData,
    boundinfo: Option<&PartitionBoundInfoData<'_>>,
    part_oids: &[Oid],
    new_spec: &PartitionBoundSpec<'_>,
) -> PgResult<()> {
    let new_part_constraints = if new_spec.strategy == PARTITION_STRATEGY_LIST {
        get_qual_for_list(mcx, key, boundinfo, new_spec)?
    } else {
        get_qual_for_range(mcx, key, part_oids, new_spec, false)?
    };
    let def_part_constraints = get_proposed_default_constraint(mcx, new_part_constraints)?;
    let def_part_constraints =
        map_partition_varattnos(mcx, def_part_constraints, 1, default_rel, parent)?;

    let default_relid = default_rel.rd_id;
    let mut all_parts: Vec<Oid> = Vec::new();
    if default_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        for &oid in pg_inherits::find_all_inheritors(mcx, default_relid, AccessExclusiveLock)?.iter()
        {
            all_parts.push(oid);
        }
    } else {
        all_parts.push(default_relid);
    }

    for &part_relid in &all_parts {
        let opened;
        let part_rel: &Relation<'mcx> = if part_relid != default_relid {
            opened = Some(table::table_open(mcx, part_relid, NoLock)?);
            opened.as_ref().unwrap()
        } else {
            opened = None;
            default_rel
        };

        if part_rel.rd_rel.relkind != RELKIND_RELATION {
            if part_rel.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
                panic!("partbounds: foreign-table partitions unported");
            }
            if let Some(r) = opened {
                r.close(NoLock)?;
            }
            continue;
        }

        let this_constraints = if part_relid != default_relid {
            map_partition_varattnos(
                mcx,
                def_part_constraints.clone_in(mcx)?,
                1,
                part_rel,
                default_rel,
            )?
        } else {
            def_part_constraints.clone_in(mcx)?
        };
        let constraint = make_ands_explicit(mcx, this_constraints)?;
        let planned = clauses_seams::eval_const_expressions::call(mcx, constraint)?;

        let mut state = execexpr::exec_init_expr(mcx, Some(planned), execexpr::ParamBind::NONE)?
            .expect("partition constraint expr");
        // By-ref call results land in the statement mcx (C: per-tuple
        // econtext reset each row).
        state.arm_result_mcx(mcx);
        let mut slot = tableam::table_slot_create(mcx, part_rel)?;
        let snapshot = snapmgr::GetLatestSnapshot()?;
        let snapshot = snapmgr::RegisterSnapshot(Some(&snapshot))?.expect("registered snapshot");
        let mut scan = tableam::table_beginscan(
            mcx,
            part_rel,
            Some(snapshot.clone()),
            0,
            mcx::PgVec::new_in(mcx),
        )?;
        while tableam::table_scan_getnextslot(
            mcx,
            &mut scan,
            types_scan::ScanDirection::ForwardScanDirection,
            &mut slot,
        )? {
            let mut slots =
                execexpr::EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
            let r = execexpr::exec_eval_expr(&mut state, &mut slots)?;
            // ExecCheck: NULL passes.
            if !r.isnull && !r.value.as_bool() {
                tableam::table_endscan(scan)?;
                snapmgr::UnregisterSnapshot(Some(&snapshot));
                return Err(default_violated(mcx, default_rel));
            }
        }
        tableam::table_endscan(scan)?;
        snapmgr::UnregisterSnapshot(Some(&snapshot));
        if let Some(r) = opened {
            // Keep the lock until commit.
            r.close(NoLock)?;
        }
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn default_violated(mcx: Mcx<'_>, default_rel: &Relation<'_>) -> Box<PgError> {
    let table = default_rel.name().to_string();
    let schema = lsyscache::misc::get_namespace_name(mcx, default_rel.rd_rel.relnamespace)
        .ok()
        .flatten()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    Box::new(
        PgError::new(
            ERROR,
            format!(
                "updated partition constraint for default partition \"{table}\" would be \
                 violated by some row"
            ),
        )
        .with_sqlstate(ERRCODE_CHECK_VIOLATION)
        .with_schema_name(schema)
        .with_table_name(table),
    )
}

// satisfies_hash_partition fn_extra memo (C ColumnsHashData).
struct ColumnsHashData {
    relid: Oid,
    nkeys: usize,
    // InvalidOid means the fixed-arity (non-variadic) call form; otherwise
    // this is the element type of the VARIADIC "any" array argument, and
    // partcollid/partsupfunc carry a single entry (C: my_extra->partsupfunc[0]).
    variadic_type: Oid,
    variadic_typlen: i16,
    variadic_typbyval: bool,
    variadic_typalign: i8,
    // std Vec justified: rides FmgrInfo.fn_extra, same
    // open-set slot the C fn_mcxt allocation fills.
    partcollid: Vec<Oid>,
    partsupfunc: Vec<FmgrInfo>,
}

pub fn fc_satisfies_hash_partition(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    if fcinfo.args[0].isnull || fcinfo.args[1].isnull || fcinfo.args[2].isnull {
        return Ok(Datum::from_bool(false));
    }
    let parent_id = fcinfo.args[0].value.as_oid();
    let modulus = fcinfo.args[1].value.as_i32();
    let remainder = fcinfo.args[2].value.as_i32();
    if modulus <= 0 {
        return Err(hash_param_error(
            "modulus for hash partition must be an integer value greater than zero",
        ));
    }
    if remainder < 0 {
        return Err(hash_param_error(
            "remainder for hash partition must be an integer value greater than or equal to zero",
        ));
    }
    if remainder >= modulus {
        return Err(hash_param_error("remainder for hash partition must be less than modulus"));
    }
    let flinfo = flinfo.expect("satisfies_hash_partition: NULL flinfo");
    // C's call-form dispatch: get_fn_expr_variadic(fcinfo->flinfo).
    let is_variadic = funcapi::get_fn_expr_variadic(Some(flinfo));

    let stale =
        flinfo.fn_extra_ref::<ColumnsHashData>().map_or(true, |x| x.relid != parent_id);
    if stale {
        let mcx = fcinfo.result_mcx();
        let parent = table::table_open(mcx, parent_id, AccessShareLock)?;
        // partcache::RelationGetPartitionKey only searches pg_partitioned_table
        // when relkind is RELKIND_PARTITIONED_TABLE (C partcache.c:51-58);
        // otherwise C hands back NULL without a syscache lookup. Mirror that
        // gate here so a non-partitioned or child-partition relid reaches the
        // SQL error below instead of the "cache lookup failed" internal panic.
        let key = if parent.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            Some(partcache::RelationGetPartitionKey(&parent)?)
        } else {
            None
        };
        let key = match key {
            Some(key) if key.strategy as u8 == PARTITION_STRATEGY_HASH => key,
            _ => {
                let name = lsyscache::get_rel_name(mcx, parent_id)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                parent.close(NoLock)?;
                return Err(hash_param_error_owned(format!(
                    "\"{name}\" is not a hash partitioned table"
                )));
            }
        };

        let extra = if !is_variadic {
            let nargs = fcinfo.nargs as usize - 3;
            if key.partnatts as usize != nargs {
                return Err(hash_param_error_owned(format!(
                    "number of partitioning columns ({}) does not match number of partition \
                     keys provided ({nargs})",
                    key.partnatts
                )));
            }
            let mut partcollid = Vec::with_capacity(nargs);
            let mut partsupfunc = Vec::with_capacity(nargs);
            for j in 0..nargs {
                let argtype = funcapi::get_fn_expr_argtype(Some(flinfo), j + 3);
                let parttypid = key.parttypid[j];
                if argtype != parttypid && !coerce::IsBinaryCoercible(argtype, parttypid)? {
                    return Err(hash_param_error_owned(format!(
                        "column {} of the partition key has type {}, but supplied value is of \
                         type {}",
                        j + 1,
                        format_type::format_type_be(parttypid)?,
                        format_type::format_type_be(argtype)?,
                    )));
                }
                partcollid.push(key.partcollation[j]);
                partsupfunc.push(key.partsupfunc[j].borrow().clone());
            }
            ColumnsHashData {
                relid: parent_id,
                nkeys: nargs,
                variadic_type: InvalidOid,
                variadic_typlen: 0,
                variadic_typbyval: false,
                variadic_typalign: 0,
                partcollid,
                partsupfunc,
            }
        } else {
            let variadic_type = variadic_array_elemtype(fcinfo)?;
            let (typlen, typbyval, typalign) = lsyscache::get_typlenbyvalalign(variadic_type)?;
            for j in 0..key.partnatts as usize {
                let parttypid = key.parttypid[j];
                if parttypid != variadic_type {
                    return Err(hash_param_error_owned(format!(
                        "column {} of the partition key has type \"{}\", but supplied value is \
                         of type \"{}\"",
                        j + 1,
                        format_type::format_type_be(parttypid)?,
                        format_type::format_type_be(variadic_type)?,
                    )));
                }
            }
            ColumnsHashData {
                relid: parent_id,
                nkeys: key.partnatts as usize,
                variadic_type,
                variadic_typlen: typlen,
                variadic_typbyval: typbyval,
                variadic_typalign: typalign,
                partcollid: vec![key.partcollation[0]],
                partsupfunc: vec![key.partsupfunc[0].borrow().clone()],
            }
        };
        flinfo.set_fn_extra(extra);
        // Hold the lock until commit.
        parent.close(NoLock)?;
    }

    let my = flinfo.fn_extra_mut::<ColumnsHashData>().expect("just built");
    let seed = Datum::from_u64(HASH_PARTITION_SEED);
    let mut row_hash: u64 = 0;

    if my.variadic_type == InvalidOid {
        for i in 0..my.nkeys {
            let argno = i + 3;
            if fcinfo.args[argno].isnull {
                continue;
            }
            let hash = invoke_hash_support(
                fcinfo,
                &mut my.partsupfunc[i],
                my.partcollid[i],
                fcinfo.args[argno].value,
                seed,
            )?;
            row_hash = hash_combine64(row_hash, hash);
        }
    } else {
        let (datums, isnull) = deconstruct_variadic_array(
            fcinfo,
            my.variadic_typlen,
            my.variadic_typbyval,
            my.variadic_typalign,
        )?;
        if datums.len() != my.nkeys {
            return Err(hash_param_error_owned(format!(
                "number of partitioning columns ({}) does not match number of partition keys \
                 provided ({})",
                my.nkeys,
                datums.len()
            )));
        }
        for i in 0..datums.len() {
            if isnull[i] {
                continue;
            }
            let hash = invoke_hash_support(
                fcinfo,
                &mut my.partsupfunc[0],
                my.partcollid[0],
                datums[i],
                seed,
            )?;
            row_hash = hash_combine64(row_hash, hash);
        }
    }
    Ok(Datum::from_bool(row_hash % modulus as u64 == remainder as u64))
}

// Custom hash opclasses (SQL-function support procs) allocate by-ref
// intermediates through the frame's result mcx.
fn invoke_hash_support(
    fcinfo: &FunctionCallInfoBaseData,
    supfunc: &mut FmgrInfo,
    collid: Oid,
    val: Datum,
    seed: Datum,
) -> PgResult<u64> {
    let mut call = LocalFcinfo::<2>::new(collid);
    // SAFETY: the outer frame's armed context outlives this inner call.
    unsafe { call.set_result_mcx(fcinfo.result_mcx_detached()) };
    call.set_arg(0, val);
    call.set_arg(1, seed);
    let hash = supfunc.invoke(&mut call)?;
    assert!(!call.isnull, "partition hash support function returned NULL");
    Ok(hash.as_u64())
}

// Raw array bytes for the VARIADIC "any" call form's sole trailing argument
// (C: PG_GETARG_ARRAYTYPE_P(3)).
fn variadic_array_bytes<'mcx>(fcinfo: &FunctionCallInfoBaseData) -> PgResult<&'mcx [u8]> {
    // SAFETY: the outer frame's armed context outlives this array's readers.
    let mcx: Mcx<'mcx> = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: a live varlena array Datum readable through its VARSIZE_ANY.
    let p = unsafe { fcinfo.arg_ptr(3) };
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    Ok(detoast_seams::detoast_attr::call(mcx, raw)?.leak())
}

fn variadic_array_elemtype(fcinfo: &FunctionCallInfoBaseData) -> PgResult<Oid> {
    Ok(arrayfuncs::arr_elemtype(variadic_array_bytes(fcinfo)?))
}

fn deconstruct_variadic_array<'mcx>(
    fcinfo: &FunctionCallInfoBaseData,
    typlen: i16,
    typbyval: bool,
    typalign: i8,
) -> PgResult<(mcx::PgVec<'mcx, Datum>, mcx::PgVec<'mcx, bool>)> {
    // SAFETY: the outer frame's armed context outlives this array's readers.
    let mcx: Mcx<'mcx> = unsafe { fcinfo.result_mcx_detached() };
    let flat = variadic_array_bytes(fcinfo)?;
    arrayfuncs::deconstruct_array(mcx, flat, typlen as i32, typbyval, typalign as u8, true)
}

#[track_caller]
#[cold]
#[inline(never)]
fn hash_param_error(msg: &'static str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg.to_string()).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn hash_param_error_owned(msg: String) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: false, func }
}

pub const PARTBOUNDS_BUILTINS: &[FmgrBuiltin] =
    &[b(F_SATISFIES_HASH_PARTITION, "satisfies_hash_partition", 4, fc_satisfies_hash_partition)];
