//! outfuncs.c nodeToString for the node sets stored in pg_attrdef.adbin /
//! pg_constraint.conbin (DEFAULT/CHECK corpus), pg_trigger.tgqual, and
//! pg_rewrite.ev_action (view SELECT-rule Query trees). Every other node tag
//! is a loud panic naming the C writer. Output is byte-compatible with C 18.3
//! nodeToString (write_location_fields=false: every location renders as -1).

#![allow(non_snake_case)]

use core::fmt::Write;

use datum::Datum;
use mcx::{Mcx, PgString};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::{IntList, NodeList, OidList};
use types_nodes::parsenodes::{Query, RTEKind, RTEPermissionInfo, RangeTblEntry, SortGroupClause};
use types_nodes::primnodes::{
    Aggref, Alias, BoolExpr, BoolExprType, CoerceToDomain, CoerceToDomainValue, CoerceViaIO,
    Const, FromExpr, FuncExpr, JoinExpr, NamedArgExpr, NullTest, OpExpr, RangeTblRef, RelabelType,
    ScalarArrayOpExpr, SubLink, TargetEntry, Var,
};
use types_nodes::rawnodes::{PartitionBoundSpec, PartitionRangeDatum};
use types_nodes::{Boolean, Float, Integer, Node, NodeTag};

pub fn nodeToString<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<PgString<'mcx>> {
    let mut out = PgString::new_in(mcx);
    out_node(&mut out, node)?;
    Ok(out)
}

// Query reachable only as RangeTblEntry.subquery's &Query (no node handle).
pub fn queryToString<'mcx>(mcx: Mcx<'mcx>, q: &Query<'_>) -> PgResult<PgString<'mcx>> {
    let mut out = PgString::new_in(mcx);
    out_query(&mut out, q)?;
    Ok(out)
}

macro_rules! w {
    ($out:expr, $($arg:tt)*) => {
        write!($out, $($arg)*).expect("outfuncs append")
    };
}

fn out_node(out: &mut PgString<'_>, node: Node<'_>) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => out_var(out, node.as_variant::<Var>().expect("Var")),
        NodeTag::T_Const => out_const(out, node.as_variant::<Const>().expect("Const")),
        NodeTag::T_OpExpr => out_op_expr(out, node.as_variant::<OpExpr>().expect("OpExpr"))?,
        NodeTag::T_FuncExpr => {
            out_func_expr(out, node.as_variant::<FuncExpr>().expect("FuncExpr"))?
        }
        NodeTag::T_NamedArgExpr => {
            out_named_arg_expr(out, node.as_variant::<NamedArgExpr>().expect("NamedArgExpr"))?
        }
        NodeTag::T_BoolExpr => {
            out_bool_expr(out, node.as_variant::<BoolExpr>().expect("BoolExpr"))?
        }
        NodeTag::T_NullTest => {
            out_null_test(out, node.as_variant::<NullTest>().expect("NullTest"))?
        }
        NodeTag::T_RelabelType => {
            out_relabel_type(out, node.as_variant::<RelabelType>().expect("RelabelType"))?
        }
        NodeTag::T_List => out_list(out, node.as_list().expect("List"))?,
        NodeTag::T_CoerceViaIO => {
            out_coerce_via_io(out, node.as_variant::<CoerceViaIO>().expect("CoerceViaIO"))?
        }
        NodeTag::T_CoerceToDomain => out_coerce_to_domain(
            out,
            node.as_variant::<CoerceToDomain>().expect("CoerceToDomain"),
        )?,
        NodeTag::T_CoerceToDomainValue => {
            let v = node.as_variant::<CoerceToDomainValue>().expect("CoerceToDomainValue");
            w!(
                out,
                "{{COERCETODOMAINVALUE :typeId {} :typeMod {} :collation {} :location -1}}",
                v.typeId, v.typeMod, v.collation
            );
        }
        NodeTag::T_SQLValueFunction => {
            let v = node
                .as_variant::<types_nodes::primnodes::SQLValueFunction>()
                .expect("SQLValueFunction");
            w!(
                out,
                "{{SQLVALUEFUNCTION :op {} :type {} :typmod {} :location -1}}",
                v.op as u32, v.r#type, v.typmod
            );
        }
        NodeTag::T_ScalarArrayOpExpr => out_scalar_array_op_expr(
            out,
            node.as_variant::<ScalarArrayOpExpr>().expect("ScalarArrayOpExpr"),
        )?,
        NodeTag::T_PartitionBoundSpec => out_partition_bound_spec(
            out,
            node.as_variant::<PartitionBoundSpec>().expect("PartitionBoundSpec"),
        )?,
        NodeTag::T_PartitionRangeDatum => out_partition_range_datum(
            out,
            node.as_variant::<PartitionRangeDatum>().expect("PartitionRangeDatum"),
        )?,
        NodeTag::T_BooleanTest => {
            let bt = node
                .as_variant::<types_nodes::primnodes::BooleanTest>()
                .expect("BooleanTest");
            w!(out, "{{BOOLEANTEST :arg ");
            out_opt_node(out, bt.arg)?;
            w!(out, " :booltesttype {} :location -1}}", bt.booltesttype as u32);
        }
        NodeTag::T_SetToDefault => {
            let d = node
                .as_variant::<types_nodes::primnodes::SetToDefault>()
                .expect("SetToDefault");
            w!(
                out,
                "{{SETTODEFAULT :typeId {} :typeMod {} :collation {} :location -1}}",
                d.typeId, d.typeMod, d.collation
            );
        }
        NodeTag::T_Query => out_query(out, node.as_variant::<Query>().expect("Query"))?,
        NodeTag::T_RangeTblEntry => {
            out_range_tbl_entry(out, node.as_variant::<RangeTblEntry>().expect("RangeTblEntry"))?
        }
        NodeTag::T_RTEPermissionInfo => out_rte_permission_info(
            out,
            node.as_variant::<RTEPermissionInfo>().expect("RTEPermissionInfo"),
        ),
        NodeTag::T_Alias => out_alias(out, node.as_variant::<Alias>().expect("Alias"))?,
        NodeTag::T_FromExpr => {
            out_from_expr(out, node.as_variant::<FromExpr>().expect("FromExpr"))?
        }
        NodeTag::T_JoinExpr => {
            out_join_expr(out, node.as_variant::<JoinExpr>().expect("JoinExpr"))?
        }
        NodeTag::T_RangeTblRef => {
            out_range_tbl_ref(out, node.as_variant::<RangeTblRef>().expect("RangeTblRef"))
        }
        NodeTag::T_TargetEntry => {
            out_target_entry(out, node.as_variant::<TargetEntry>().expect("TargetEntry"))?
        }
        NodeTag::T_SortGroupClause => out_sort_group_clause(
            out,
            node.as_variant::<SortGroupClause>().expect("SortGroupClause"),
        ),
        NodeTag::T_Aggref => out_aggref(out, node.as_variant::<Aggref>().expect("Aggref"))?,
        NodeTag::T_SubLink => out_sub_link(out, node.as_variant::<SubLink>().expect("SubLink"))?,
        NodeTag::T_IntList => out_int_list(out, node.as_int_list().expect("IntList")),
        NodeTag::T_OidList => out_oid_list(out, node.as_oid_list().expect("OidList")),
        NodeTag::T_String => out_string_node(out, node.as_string().expect("String").sval),
        NodeTag::T_Integer => {
            w!(out, "{}", node.as_variant::<Integer>().expect("Integer").ival)
        }
        NodeTag::T_Float => w!(out, "{}", node.as_variant::<Float>().expect("Float").fval),
        NodeTag::T_Boolean => {
            out_bool(out, node.as_variant::<Boolean>().expect("Boolean").boolval)
        }
        other => panic!(
            "outNode (outfuncs.c): {other:?} write arm unported (DEFAULT/CHECK + view \
             SELECT-rule sets)"
        ),
    }
    Ok(())
}

fn out_list(out: &mut PgString<'_>, list: &NodeList<'_>) -> PgResult<()> {
    if list.is_nil() {
        w!(out, "<>");
        return Ok(());
    }
    w!(out, "(");
    for (i, item) in list.iter().enumerate() {
        if i > 0 {
            w!(out, " ");
        }
        out_node(out, item)?;
    }
    w!(out, ")");
    Ok(())
}

fn out_bitmapset(out: &mut PgString<'_>, bms: &Bitmapset<'_>) {
    w!(out, "(b");
    for m in bms.iter() {
        w!(out, " {m}");
    }
    w!(out, ")");
}

fn out_bool(out: &mut PgString<'_>, b: bool) {
    w!(out, "{}", if b { "true" } else { "false" });
}

fn out_var(out: &mut PgString<'_>, v: &Var<'_>) {
    w!(
        out,
        "{{VAR :varno {} :varattno {} :vartype {} :vartypmod {} :varcollid {} :varnullingrels ",
        v.varno, v.varattno, v.vartype, v.vartypmod, v.varcollid
    );
    out_bitmapset(out, &v.varnullingrels);
    w!(
        out,
        " :varlevelsup {} :varreturningtype {} :varnosyn {} :varattnosyn {} :location -1}}",
        v.varlevelsup, v.varreturningtype as u32, v.varnosyn, v.varattnosyn
    );
}

fn out_const(out: &mut PgString<'_>, c: &Const) {
    w!(
        out,
        "{{CONST :consttype {} :consttypmod {} :constcollid {} :constlen {} :constbyval ",
        c.consttype, c.consttypmod, c.constcollid, c.constlen
    );
    out_bool(out, c.constbyval);
    w!(out, " :constisnull ");
    out_bool(out, c.constisnull);
    w!(out, " :location -1 :constvalue ");
    if c.constisnull {
        w!(out, "<>");
    } else {
        out_datum(out, c.constvalue, c.constlen, c.constbyval);
    }
    w!(out, "}}");
}

// _outDatum (outfuncs.c) prints bytes as `char`, unsigned on aarch64 Linux —
// the byte-compare oracle; readfuncs accepts either signedness.
fn out_datum(out: &mut PgString<'_>, value: Datum, typlen: i32, typbyval: bool) {
    if typbyval {
        let bytes = value.as_usize().to_le_bytes();
        w!(out, "{} [ ", typlen as u32);
        for b in bytes {
            w!(out, "{b} ");
        }
        w!(out, "]");
        return;
    }
    let p = value.as_usize() as *const u8;
    if p.is_null() {
        w!(out, "0 [ ]");
        return;
    }
    let length = match typlen {
        l if l > 0 => l as usize,
        -1 => {
            // SAFETY: byref const datum points at a live in-line varlena.
            unsafe { varlena_size(p) }
        }
        -2 => {
            // cstring (unknown-type Consts): NUL included, as C's strlen+1.
            let mut n = 0usize;
            // SAFETY: byref cstring datum points at a live NUL-terminated string.
            while unsafe { *p.add(n) } != 0 {
                n += 1;
            }
            n + 1
        }
        other => panic!("_outDatum (outfuncs.c): typlen {other} unported"),
    };
    if length == 0 {
        w!(out, "0 [ ]");
        return;
    }
    w!(out, "{length} [ ");
    for i in 0..length {
        // SAFETY: length derived from the datum's own size.
        let b = unsafe { *p.add(i) };
        w!(out, "{b} ");
    }
    w!(out, "]");
}

// VARSIZE_ANY over a plain (parser-built, never toasted) varlena image.
unsafe fn varlena_size(p: *const u8) -> usize {
    // SAFETY: caller guarantees a live varlena header at p.
    let b0 = unsafe { *p };
    if b0 & 0x01 != 0 {
        (b0 as usize) >> 1
    } else {
        // SAFETY: 4-byte header form.
        let word = unsafe { core::ptr::read_unaligned(p as *const u32) };
        (word as usize) >> 2
    }
}

fn out_op_expr(out: &mut PgString<'_>, o: &OpExpr<'_>) -> PgResult<()> {
    w!(
        out,
        "{{OPEXPR :opno {} :opfuncid {} :opresulttype {} :opretset ",
        o.opno, o.opfuncid, o.opresulttype
    );
    out_bool(out, o.opretset);
    w!(out, " :opcollid {} :inputcollid {} :args ", o.opcollid, o.inputcollid);
    out_list(out, &o.args)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_func_expr(out: &mut PgString<'_>, f: &FuncExpr<'_>) -> PgResult<()> {
    w!(out, "{{FUNCEXPR :funcid {} :funcresulttype {} :funcretset ", f.funcid, f.funcresulttype);
    out_bool(out, f.funcretset);
    w!(out, " :funcvariadic ");
    out_bool(out, f.funcvariadic);
    w!(
        out,
        " :funcformat {} :funccollid {} :inputcollid {} :args ",
        f.funcformat as u32, f.funccollid, f.inputcollid
    );
    out_list(out, &f.args)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_named_arg_expr(out: &mut PgString<'_>, n: &NamedArgExpr<'_>) -> PgResult<()> {
    w!(out, "{{NAMEDARGEXPR :arg ");
    out_node(out, n.arg)?;
    w!(out, " :name ");
    out_str(out, n.name);
    w!(out, " :argnumber {} :location -1}}", n.argnumber);
    Ok(())
}

fn out_bool_expr(out: &mut PgString<'_>, b: &BoolExpr<'_>) -> PgResult<()> {
    let opstr = match b.boolop {
        BoolExprType::AND_EXPR => "and",
        BoolExprType::OR_EXPR => "or",
        BoolExprType::NOT_EXPR => "not",
    };
    w!(out, "{{BOOLEXPR :boolop {opstr} :args ");
    out_list(out, &b.args)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_null_test(out: &mut PgString<'_>, n: &NullTest<'_>) -> PgResult<()> {
    w!(out, "{{NULLTEST :arg ");
    match n.arg {
        Some(arg) => out_node(out, arg)?,
        None => w!(out, "<>"),
    }
    w!(out, " :nulltesttype {} :argisrow ", n.nulltesttype as u32);
    out_bool(out, n.argisrow);
    w!(out, " :location -1}}");
    Ok(())
}

fn out_coerce_to_domain(out: &mut PgString<'_>, c: &CoerceToDomain<'_>) -> PgResult<()> {
    w!(out, "{{COERCETODOMAIN :arg ");
    out_node(out, c.arg)?;
    w!(
        out,
        " :resulttype {} :resulttypmod {} :resultcollid {} :coercionformat {} :location -1}}",
        c.resulttype, c.resulttypmod, c.resultcollid, c.coercionformat as u32
    );
    Ok(())
}

fn out_partition_bound_spec(
    out: &mut PgString<'_>,
    b: &PartitionBoundSpec<'_>,
) -> PgResult<()> {
    w!(out, "{{PARTITIONBOUNDSPEC :strategy ");
    if b.strategy == 0 {
        w!(out, "<>");
    } else {
        w!(out, "{}", b.strategy as char);
    }
    w!(out, " :is_default ");
    out_bool(out, b.is_default);
    w!(out, " :modulus {} :remainder {} :listdatums ", b.modulus, b.remainder);
    out_list(out, &b.listdatums)?;
    w!(out, " :lowerdatums ");
    out_list(out, &b.lowerdatums)?;
    w!(out, " :upperdatums ");
    out_list(out, &b.upperdatums)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_partition_range_datum(
    out: &mut PgString<'_>,
    d: &PartitionRangeDatum<'_>,
) -> PgResult<()> {
    w!(out, "{{PARTITIONRANGEDATUM :kind {} :value ", d.kind as i32);
    match d.value {
        Some(v) => out_node(out, v)?,
        None => w!(out, "<>"),
    }
    w!(out, " :location -1}}");
    Ok(())
}

fn out_relabel_type(out: &mut PgString<'_>, r: &RelabelType<'_>) -> PgResult<()> {
    w!(out, "{{RELABELTYPE :arg ");
    out_node(out, r.arg)?;
    w!(
        out,
        " :resulttype {} :resulttypmod {} :resultcollid {} :relabelformat {} :location -1}}",
        r.resulttype, r.resulttypmod, r.resultcollid, r.relabelformat as u32
    );
    Ok(())
}

fn out_coerce_via_io(out: &mut PgString<'_>, c: &CoerceViaIO<'_>) -> PgResult<()> {
    w!(out, "{{COERCEVIAIO :arg ");
    out_node(out, c.arg)?;
    w!(
        out,
        " :resulttype {} :resultcollid {} :coerceformat {} :location -1}}",
        c.resulttype, c.resultcollid, c.coerceformat as u32
    );
    Ok(())
}

// outToken (outfuncs.c): backslash-escape anything read.c treats specially.
fn out_token(out: &mut PgString<'_>, s: &str) {
    if s.is_empty() {
        w!(out, "\"\"");
        return;
    }
    let b = s.as_bytes();
    if b[0] == b'<'
        || b[0] == b'"'
        || b[0].is_ascii_digit()
        || ((b[0] == b'+' || b[0] == b'-')
            && b.len() > 1
            && (b[1].is_ascii_digit() || b[1] == b'.'))
    {
        w!(out, "\\");
    }
    for c in s.chars() {
        if matches!(c, ' ' | '\n' | '\t' | '(' | ')' | '{' | '}' | '\\') {
            w!(out, "\\");
        }
        w!(out, "{c}");
    }
}

fn out_str(out: &mut PgString<'_>, s: Option<&str>) {
    match s {
        None => w!(out, "<>"),
        Some(s) => out_token(out, s),
    }
}

// outChar (outfuncs.c): '\0' keeps its traditional <> encoding.
fn out_char(out: &mut PgString<'_>, c: u8) {
    if c == 0 {
        w!(out, "<>");
        return;
    }
    let buf = [c];
    out_token(out, core::str::from_utf8(&buf).expect("outChar ascii"));
}

// _outString (outfuncs.c): always quoted, content escaped via outToken.
fn out_string_node(out: &mut PgString<'_>, s: &str) {
    w!(out, "\"");
    if !s.is_empty() {
        out_token(out, s);
    }
    w!(out, "\"");
}

fn out_opt_node(out: &mut PgString<'_>, n: Option<Node<'_>>) -> PgResult<()> {
    match n {
        None => {
            w!(out, "<>");
            Ok(())
        }
        Some(n) => out_node(out, n),
    }
}

fn out_int_list(out: &mut PgString<'_>, l: &IntList<'_>) {
    if l.is_nil() {
        w!(out, "<>");
        return;
    }
    w!(out, "(i");
    for v in l.iter() {
        w!(out, " {v}");
    }
    w!(out, ")");
}

fn out_oid_list(out: &mut PgString<'_>, l: &OidList<'_>) {
    if l.is_nil() {
        w!(out, "<>");
        return;
    }
    w!(out, "(o");
    for v in l.iter() {
        w!(out, " {v}");
    }
    w!(out, ")");
}

fn out_alias(out: &mut PgString<'_>, a: &Alias<'_>) -> PgResult<()> {
    w!(out, "{{ALIAS :aliasname ");
    out_str(out, a.aliasname);
    w!(out, " :colnames ");
    out_list(out, &a.colnames)?;
    w!(out, "}}");
    Ok(())
}

fn out_opt_alias(out: &mut PgString<'_>, a: Option<&Alias<'_>>) -> PgResult<()> {
    match a {
        None => {
            w!(out, "<>");
            Ok(())
        }
        Some(a) => out_alias(out, a),
    }
}

fn out_query(out: &mut PgString<'_>, q: &Query<'_>) -> PgResult<()> {
    w!(
        out,
        "{{QUERY :commandType {} :querySource {} :canSetTag ",
        q.commandType as u32, q.querySource as u32
    );
    out_bool(out, q.canSetTag);
    w!(out, " :utilityStmt ");
    out_opt_node(out, q.utilityStmt)?;
    w!(out, " :resultRelation {} :hasAggs ", q.resultRelation);
    out_bool(out, q.hasAggs);
    w!(out, " :hasWindowFuncs ");
    out_bool(out, q.hasWindowFuncs);
    w!(out, " :hasTargetSRFs ");
    out_bool(out, q.hasTargetSRFs);
    w!(out, " :hasSubLinks ");
    out_bool(out, q.hasSubLinks);
    w!(out, " :hasDistinctOn ");
    out_bool(out, q.hasDistinctOn);
    w!(out, " :hasRecursive ");
    out_bool(out, q.hasRecursive);
    w!(out, " :hasModifyingCTE ");
    out_bool(out, q.hasModifyingCTE);
    w!(out, " :hasForUpdate ");
    out_bool(out, q.hasForUpdate);
    w!(out, " :hasRowSecurity ");
    out_bool(out, q.hasRowSecurity);
    w!(out, " :hasGroupRTE ");
    out_bool(out, q.hasGroupRTE);
    w!(out, " :isReturn ");
    out_bool(out, q.isReturn);
    w!(out, " :cteList ");
    out_list(out, &q.cteList)?;
    w!(out, " :rtable ");
    out_list(out, &q.rtable)?;
    w!(out, " :rteperminfos ");
    out_list(out, &q.rteperminfos)?;
    w!(out, " :jointree ");
    match q.jointree {
        None => w!(out, "<>"),
        Some(f) => out_from_expr(out, f)?,
    }
    w!(out, " :mergeActionList ");
    out_list(out, &q.mergeActionList)?;
    w!(out, " :mergeTargetRelation {} :mergeJoinCondition ", q.mergeTargetRelation);
    out_opt_node(out, q.mergeJoinCondition)?;
    w!(out, " :targetList ");
    out_list(out, &q.targetList)?;
    w!(out, " :override {} :onConflict ", q.r#override as u32);
    out_opt_node(out, q.onConflict)?;
    w!(out, " :returningOldAlias ");
    out_str(out, q.returningOldAlias);
    w!(out, " :returningNewAlias ");
    out_str(out, q.returningNewAlias);
    w!(out, " :returningList ");
    out_list(out, &q.returningList)?;
    w!(out, " :groupClause ");
    out_list(out, &q.groupClause)?;
    w!(out, " :groupDistinct ");
    out_bool(out, q.groupDistinct);
    w!(out, " :groupingSets ");
    out_list(out, &q.groupingSets)?;
    w!(out, " :havingQual ");
    out_opt_node(out, q.havingQual)?;
    w!(out, " :windowClause ");
    out_list(out, &q.windowClause)?;
    w!(out, " :distinctClause ");
    out_list(out, &q.distinctClause)?;
    w!(out, " :sortClause ");
    out_list(out, &q.sortClause)?;
    w!(out, " :limitOffset ");
    out_opt_node(out, q.limitOffset)?;
    w!(out, " :limitCount ");
    out_opt_node(out, q.limitCount)?;
    w!(out, " :limitOption {} :rowMarks ", q.limitOption as u32);
    out_list(out, &q.rowMarks)?;
    w!(out, " :setOperations ");
    out_opt_node(out, q.setOperations)?;
    w!(out, " :constraintDeps ");
    out_oid_list(out, &q.constraintDeps);
    w!(out, " :withCheckOptions ");
    out_list(out, &q.withCheckOptions)?;
    w!(out, " :stmt_location -1 :stmt_len -1}}");
    Ok(())
}

fn out_range_tbl_entry(out: &mut PgString<'_>, r: &RangeTblEntry<'_>) -> PgResult<()> {
    w!(out, "{{RANGETBLENTRY :alias ");
    out_opt_alias(out, r.alias)?;
    w!(out, " :eref ");
    out_opt_alias(out, r.eref)?;
    w!(out, " :rtekind {}", r.rtekind as u32);
    match r.rtekind {
        RTEKind::RTE_RELATION => {
            w!(out, " :relid {} :inh ", r.relid);
            out_bool(out, r.inh);
            w!(out, " :relkind ");
            out_char(out, r.relkind);
            w!(
                out,
                " :rellockmode {} :perminfoindex {} :tablesample ",
                r.rellockmode, r.perminfoindex
            );
            out_opt_node(out, r.tablesample)?;
        }
        RTEKind::RTE_SUBQUERY => {
            w!(out, " :subquery ");
            match r.subquery {
                None => w!(out, "<>"),
                Some(q) => out_query(out, q)?,
            }
            w!(out, " :security_barrier ");
            out_bool(out, r.security_barrier);
            w!(out, " :relid {} :inh ", r.relid);
            out_bool(out, r.inh);
            w!(out, " :relkind ");
            out_char(out, r.relkind);
            w!(
                out,
                " :rellockmode {} :perminfoindex {}",
                r.rellockmode, r.perminfoindex
            );
        }
        RTEKind::RTE_JOIN => {
            w!(
                out,
                " :jointype {} :joinmergedcols {} :joinaliasvars ",
                r.jointype as u32, r.joinmergedcols
            );
            out_list(out, &r.joinaliasvars)?;
            w!(out, " :joinleftcols ");
            out_int_list(out, &r.joinleftcols);
            w!(out, " :joinrightcols ");
            out_int_list(out, &r.joinrightcols);
            w!(out, " :join_using_alias ");
            out_opt_alias(out, r.join_using_alias)?;
        }
        RTEKind::RTE_VALUES => {
            w!(out, " :values_lists ");
            out_list(out, &r.values_lists)?;
            w!(out, " :coltypes ");
            out_oid_list(out, &r.coltypes);
            w!(out, " :coltypmods ");
            out_int_list(out, &r.coltypmods);
            w!(out, " :colcollations ");
            out_oid_list(out, &r.colcollations);
        }
        RTEKind::RTE_GROUP => {
            w!(out, " :groupexprs ");
            out_list(out, &r.groupexprs)?;
        }
        other => panic!(
            "_outRangeTblEntry (outfuncs.c): {other:?} arm unported (view SELECT-rule set)"
        ),
    }
    w!(out, " :lateral ");
    out_bool(out, r.lateral);
    w!(out, " :inFromCl ");
    out_bool(out, r.inFromCl);
    w!(out, " :securityQuals ");
    out_list(out, &r.securityQuals)?;
    w!(out, "}}");
    Ok(())
}

fn out_rte_permission_info(out: &mut PgString<'_>, p: &RTEPermissionInfo<'_>) {
    w!(out, "{{RTEPERMISSIONINFO :relid {} :inh ", p.relid);
    out_bool(out, p.inh);
    w!(
        out,
        " :requiredPerms {} :checkAsUser {} :selectedCols ",
        p.requiredPerms, p.checkAsUser
    );
    out_bitmapset(out, &p.selectedCols);
    w!(out, " :insertedCols ");
    out_bitmapset(out, &p.insertedCols);
    w!(out, " :updatedCols ");
    out_bitmapset(out, &p.updatedCols);
    w!(out, "}}");
}

fn out_from_expr(out: &mut PgString<'_>, f: &FromExpr<'_>) -> PgResult<()> {
    w!(out, "{{FROMEXPR :fromlist ");
    out_list(out, &f.fromlist)?;
    w!(out, " :quals ");
    out_opt_node(out, f.quals)?;
    w!(out, "}}");
    Ok(())
}

fn out_join_expr(out: &mut PgString<'_>, j: &JoinExpr<'_>) -> PgResult<()> {
    w!(out, "{{JOINEXPR :jointype {} :isNatural ", j.jointype as u32);
    out_bool(out, j.isNatural);
    w!(out, " :larg ");
    out_node(out, j.larg)?;
    w!(out, " :rarg ");
    out_node(out, j.rarg)?;
    w!(out, " :usingClause ");
    out_list(out, &j.usingClause)?;
    w!(out, " :join_using_alias ");
    out_opt_alias(out, j.join_using_alias)?;
    w!(out, " :quals ");
    out_opt_node(out, j.quals)?;
    w!(out, " :alias ");
    out_opt_alias(out, j.alias)?;
    w!(out, " :rtindex {}}}", j.rtindex);
    Ok(())
}

fn out_range_tbl_ref(out: &mut PgString<'_>, r: &RangeTblRef) {
    w!(out, "{{RANGETBLREF :rtindex {}}}", r.rtindex);
}

fn out_target_entry(out: &mut PgString<'_>, t: &TargetEntry<'_>) -> PgResult<()> {
    w!(out, "{{TARGETENTRY :expr ");
    out_node(out, t.expr)?;
    w!(out, " :resno {} :resname ", t.resno);
    out_str(out, t.resname);
    w!(
        out,
        " :ressortgroupref {} :resorigtbl {} :resorigcol {} :resjunk ",
        t.ressortgroupref, t.resorigtbl, t.resorigcol
    );
    out_bool(out, t.resjunk);
    w!(out, "}}");
    Ok(())
}

fn out_sort_group_clause(out: &mut PgString<'_>, s: &SortGroupClause) {
    w!(
        out,
        "{{SORTGROUPCLAUSE :tleSortGroupRef {} :eqop {} :sortop {} :reverse_sort ",
        s.tleSortGroupRef, s.eqop, s.sortop
    );
    out_bool(out, s.reverse_sort);
    w!(out, " :nulls_first ");
    out_bool(out, s.nulls_first);
    w!(out, " :hashable ");
    out_bool(out, s.hashable);
    w!(out, "}}");
}

fn out_aggref(out: &mut PgString<'_>, a: &Aggref<'_>) -> PgResult<()> {
    w!(
        out,
        "{{AGGREF :aggfnoid {} :aggtype {} :aggcollid {} :inputcollid {} :aggtranstype {} \
         :aggargtypes ",
        a.aggfnoid, a.aggtype, a.aggcollid, a.inputcollid, a.aggtranstype
    );
    out_oid_list(out, &a.aggargtypes);
    w!(out, " :aggdirectargs ");
    out_list(out, &a.aggdirectargs)?;
    w!(out, " :args ");
    out_list(out, &a.args)?;
    w!(out, " :aggorder ");
    out_list(out, &a.aggorder)?;
    w!(out, " :aggdistinct ");
    out_list(out, &a.aggdistinct)?;
    w!(out, " :aggfilter ");
    out_opt_node(out, a.aggfilter)?;
    w!(out, " :aggstar ");
    out_bool(out, a.aggstar);
    w!(out, " :aggvariadic ");
    out_bool(out, a.aggvariadic);
    w!(out, " :aggkind ");
    out_char(out, a.aggkind as u8);
    w!(out, " :aggpresorted ");
    out_bool(out, a.aggpresorted);
    w!(
        out,
        " :agglevelsup {} :aggsplit {} :aggno {} :aggtransno {} :location -1}}",
        a.agglevelsup, a.aggsplit, a.aggno, a.aggtransno
    );
    Ok(())
}

fn out_sub_link(out: &mut PgString<'_>, s: &SubLink<'_>) -> PgResult<()> {
    w!(
        out,
        "{{SUBLINK :subLinkType {} :subLinkId {} :testexpr ",
        s.subLinkType as u32, s.subLinkId
    );
    out_opt_node(out, s.testexpr)?;
    w!(out, " :operName ");
    out_list(out, &s.operName)?;
    w!(out, " :subselect ");
    out_node(out, s.subselect)?;
    w!(out, " :location -1}}");
    Ok(())
}

fn out_scalar_array_op_expr(out: &mut PgString<'_>, s: &ScalarArrayOpExpr<'_>) -> PgResult<()> {
    w!(
        out,
        "{{SCALARARRAYOPEXPR :opno {} :opfuncid {} :hashfuncid {} :negfuncid {} :useOr ",
        s.opno, s.opfuncid, s.hashfuncid, s.negfuncid
    );
    out_bool(out, s.useOr);
    w!(out, " :inputcollid {} :args ", s.inputcollid);
    out_list(out, &s.args)?;
    w!(out, " :location -1}}");
    Ok(())
}

#[cfg(test)]
mod tests;
