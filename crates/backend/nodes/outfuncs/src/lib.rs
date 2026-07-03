//! outfuncs.c minimal arm: nodeToString for the expression node set stored in
//! pg_attrdef.adbin / pg_constraint.conbin / pg_trigger.tgqual. Every other
//! node tag is a loud panic naming the C writer. Output is byte-compatible
//! with C 18.3 nodeToString (locations stripped to -1, WRITE_LOCATION_FIELD).

#![allow(non_snake_case)]

use core::fmt::Write;

use datum::Datum;
use mcx::{Mcx, PgString};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::NodeList;
use types_nodes::primnodes::{
    BoolExpr, BoolExprType, CoerceToDomain, CoerceToDomainValue, CoerceViaIO, Const, FuncExpr,
    NullTest, OpExpr, RelabelType, Var,
};
use types_nodes::rawnodes::{PartitionBoundSpec, PartitionRangeDatum};
use types_nodes::{Node, NodeTag};

pub fn nodeToString<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<PgString<'mcx>> {
    let mut out = PgString::new_in(mcx);
    out_node(&mut out, node)?;
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
        NodeTag::T_BoolExpr => {
            out_bool_expr(out, node.as_variant::<BoolExpr>().expect("BoolExpr"))?
        }
        NodeTag::T_NullTest => {
            out_null_test(out, node.as_variant::<NullTest>().expect("NullTest"))?
        }
        NodeTag::T_RelabelType => {
            out_relabel_type(out, node.as_variant::<RelabelType>().expect("RelabelType"))?
        }
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
        NodeTag::T_PartitionBoundSpec => out_partition_bound_spec(
            out,
            node.as_variant::<PartitionBoundSpec>().expect("PartitionBoundSpec"),
        )?,
        NodeTag::T_PartitionRangeDatum => out_partition_range_datum(
            out,
            node.as_variant::<PartitionRangeDatum>().expect("PartitionRangeDatum"),
        )?,
        other => panic!(
            "outNode (outfuncs.c): {other:?} write arm unported (DEFAULT/CHECK expr set only)"
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

#[cfg(test)]
mod tests;
