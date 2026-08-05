use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult, ERROR};
use types_nodes::Node;
use types_rel::Relation;
use types_tuple::HeapTupleData;

use crate::ColStats;

pub const StatisticRelationId: Oid = 2619;

pub const Natts_pg_statistic: usize = 31;
pub const Anum_pg_statistic_starelid: usize = 1;
pub const Anum_pg_statistic_staattnum: usize = 2;
pub const Anum_pg_statistic_stainherit: usize = 3;
pub const Anum_pg_statistic_stanullfrac: usize = 4;
pub const Anum_pg_statistic_stawidth: usize = 5;
pub const Anum_pg_statistic_stadistinct: usize = 6;
pub const Anum_pg_statistic_stakind1: usize = 7;
pub const Anum_pg_statistic_staop1: usize = 12;
pub const Anum_pg_statistic_stacoll1: usize = 17;
pub const Anum_pg_statistic_stanumbers1: usize = 22;
pub const Anum_pg_statistic_stavalues1: usize = 27;
pub const STATISTIC_NUM_SLOTS: usize = 5;

const ROW_EXCLUSIVE_LOCK: types_rel::LOCKMODE = 3;

pub struct ExprStatsRow<'mcx> {
    pub stanullfrac: f32,
    pub stawidth: i32,
    pub stadistinct: f32,
    pub stakind: [i16; STATISTIC_NUM_SLOTS],
    pub staop: [Oid; STATISTIC_NUM_SLOTS],
    pub stacoll: [Oid; STATISTIC_NUM_SLOTS],
    pub stanumbers: [Option<PgVec<'mcx, u8>>; STATISTIC_NUM_SLOTS],
    pub stavalues: [Option<PgVec<'mcx, u8>>; STATISTIC_NUM_SLOTS],
}

// C's compute_expr_stats + serialize_expr_stats inputs live in analyze.c's
// VacAttrStats; the analyze crate implements this (extended_stats.c's
// compute_stats fn-pointer boundary).
pub trait ExprStatsCompute<'mcx> {
    fn compute(
        &mut self,
        mcx: Mcx<'mcx>,
        onerel: &Relation<'mcx>,
        exprs: &[Node<'mcx>],
        stattarget: i32,
        rows: &[HeapTupleData<'_>],
    ) -> PgResult<PgVec<'mcx, Option<ExprStatsRow<'mcx>>>>;
}

fn varlena_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: varlena header declares the image length.
    unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            2 + types_tuple::varatt::vartag_size(*p.add(1))
        } else if b0 & 0x01 != 0 {
            ((b0 as usize) >> 1) & 0x7F
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    }
}

fn text_str<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<&'mcx str> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null text datum into a live catalog tuple.
    let b0 = unsafe { *p };
    let src: &[u8] = if b0 == 0x01 || (b0 & 0x03) == 0x02 {
        &detoast::detoast_attr(mcx, varlena_image(d))?.leak()[4..]
    } else if b0 & 0x01 != 0 {
        let len = ((b0 as usize) >> 1) & 0x7F;
        // SAFETY: short varlena header declares len bytes including itself.
        unsafe { core::slice::from_raw_parts(p.add(1), len - 1) }
    } else {
        &varlena_image(d)[4..]
    };
    let mut copied: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, src.len())?;
    mcx::vec_append_bytes(&mut copied, src)?;
    Ok(core::str::from_utf8(copied.leak()).expect("stxexprs is UTF-8"))
}

// fetch_statentries_for_relation's expression decode (extended_stats.c):
// stringToNode + eval_const_expressions + fix_opfuncids.
pub(crate) fn decode_stxexprs<'mcx>(
    mcx: Mcx<'mcx>,
    d: Datum,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    let s = text_str(mcx, d)?;
    let node = readfuncs::stringToNode(mcx, s)?;
    let node = clauses_seams::eval_const_expressions::call(mcx, node)?;
    nodes_core::fix_opfuncids(node)?;
    let list = node.as_list().expect("stxexprs is a List");
    let mut out: PgVec<'mcx, Node<'mcx>> = mcx::vec_with_capacity_in(mcx, list.len())?;
    out.extend(list.iter());
    Ok(out)
}

// examine_expression (extended_stats.c), ColStats form: only the type-shape
// fields the multivariate builders read.
pub(crate) fn expr_col_stats(expr: Node<'_>, stattarget: i32) -> PgResult<ColStats> {
    let atttypid = nodes_core::node_funcs::expr_type(expr);
    let attrcollid = nodes_core::node_funcs::expr_collation(expr);
    let (typlen, typbyval, _typalign) = lsyscache::get_typlenbyvalalign(atttypid)?;
    Ok(ColStats {
        tupattnum: 0,
        attstattarget: stattarget,
        attrtypid: atttypid,
        attrcollid,
        typlen,
        typbyval,
    })
}

// make_build_data's expression evaluation (extended_stats.c): results land in
// the build context, as C's non-switching ExecEvalExpr does.
pub(crate) fn eval_exprs<'mcx, 'b>(
    mcx: Mcx<'mcx>,
    bmcx: Mcx<'b>,
    onerel: &Relation<'mcx>,
    exprs: &[Node<'mcx>],
    rows: &[HeapTupleData<'_>],
    values: &mut PgVec<'b, PgVec<'b, Datum>>,
    nulls: &mut PgVec<'b, PgVec<'b, bool>>,
) -> PgResult<()> {
    let mut states: PgVec<'mcx, mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>> =
        PgVec::new_in(mcx);
    for &e in exprs {
        let mut st = execexpr::exec_init_expr(mcx, Some(e), execexpr::ParamBind::NONE)?
            .expect("statistics expression");
        // SAFETY: bmcx outlives the StatsBuildData consuming these datums;
        // the build context is reset only after the builders finish.
        unsafe { st.arm_result_mcx_raw(bmcx) };
        states.push(st);
    }
    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        types_slot::TupleSlotKind::HeapTuple,
        Some(onerel.rd_att.clone()),
    );
    let base = values.len();
    for _ in exprs {
        values.push(mcx::vec_with_capacity_in(bmcx, rows.len())?);
        nulls.push(mcx::vec_with_capacity_in(bmcx, rows.len())?);
    }
    for row in rows {
        // SAFETY: the sample image outlives this loop; the reborrow mirrors
        // C's shouldFree=false ExecStoreHeapTuple.
        let tuple = unsafe {
            HeapTupleData::from_raw_parts(
                core::ptr::from_ref(row.t_data()).cast::<u8>(),
                row.t_len,
                row.t_self,
                row.t_tableOid,
            )
        };
        exectuples::exec_store_heap_tuple(&mut slot, mcx, tuple);
        for (j, st) in states.iter_mut().enumerate() {
            let nd = {
                let mut slots =
                    execexpr::EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
                execexpr::exec_eval_expr(&mut **st, &mut slots)?
            };
            values[base + j].push(if nd.isnull { Datum::null() } else { nd.value });
            nulls[base + j].push(nd.isnull);
        }
    }
    Ok(())
}

// serialize_expr_stats (extended_stats.c): a pg_statistic[] of formed
// pg_statistic tuples, one per expression, invalid stats as NULL elements.
pub(crate) fn serialize_expr_stats<'b>(
    mcx: Mcx<'b>,
    exprstats: &[Option<ExprStatsRow<'_>>],
) -> PgResult<PgVec<'b, u8>> {
    let sd = table::table_open(mcx, StatisticRelationId, ROW_EXCLUSIVE_LOCK)?;
    let typ_oid = lsyscache::get_rel_type_id(StatisticRelationId)?;
    if typ_oid == 0 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "relation \"pg_statistic\" does not have a composite type".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    let mut elems: PgVec<'b, Datum> = mcx::vec_with_capacity_in(mcx, exprstats.len())?;
    let mut elem_nulls: PgVec<'b, bool> = mcx::vec_with_capacity_in(mcx, exprstats.len())?;
    for row in exprstats {
        let Some(row) = row else {
            elems.push(Datum::null());
            elem_nulls.push(true);
            continue;
        };
        let mut values = [Datum::null(); Natts_pg_statistic];
        let mut nulls = [false; Natts_pg_statistic];
        values[Anum_pg_statistic_starelid - 1] = Datum::from_oid(0);
        values[Anum_pg_statistic_staattnum - 1] = Datum::from_i16(0);
        values[Anum_pg_statistic_stainherit - 1] = Datum::from_bool(false);
        values[Anum_pg_statistic_stanullfrac - 1] = Datum::from_f32(row.stanullfrac);
        values[Anum_pg_statistic_stawidth - 1] = Datum::from_i32(row.stawidth);
        values[Anum_pg_statistic_stadistinct - 1] = Datum::from_f32(row.stadistinct);
        for k in 0..STATISTIC_NUM_SLOTS {
            values[Anum_pg_statistic_stakind1 - 1 + k] = Datum::from_i16(row.stakind[k]);
            values[Anum_pg_statistic_staop1 - 1 + k] = Datum::from_oid(row.staop[k]);
            values[Anum_pg_statistic_stacoll1 - 1 + k] = Datum::from_oid(row.stacoll[k]);
        }
        for k in 0..STATISTIC_NUM_SLOTS {
            let i = Anum_pg_statistic_stanumbers1 - 1 + k;
            match &row.stanumbers[k] {
                Some(img) => values[i] = Datum::from_usize(img.as_ptr() as usize),
                None => nulls[i] = true,
            }
        }
        for k in 0..STATISTIC_NUM_SLOTS {
            let i = Anum_pg_statistic_stavalues1 - 1 + k;
            match &row.stavalues[k] {
                Some(img) => values[i] = Datum::from_usize(img.as_ptr() as usize),
                None => nulls[i] = true,
            }
        }
        let stup = heaptuple::heap_form_tuple(mcx, sd.descr(), &values, &nulls)?;
        elems.push(heaptuple::heap_copy_tuple_as_datum(mcx, stup.as_tuple(), sd.descr())?);
        elem_nulls.push(false);
    }

    table::table_close(sd, ROW_EXCLUSIVE_LOCK)?;

    let dims = [elems.len() as i32];
    let lbs = [1i32];
    arrayfuncs::construct_md_array(
        mcx,
        &elems,
        Some(&elem_nulls),
        1,
        &dims,
        &lbs,
        typ_oid,
        -1,
        false,
        b'd',
    )
}
