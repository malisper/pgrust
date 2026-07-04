use datum::Datum;
use funcapi::TypeFuncClass;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_fmgr::{byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_rel::{AccessShareLock, RELKIND_HAS_PARTITIONS};

pub static PARTITIONFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 3423,
        name: "pg_partition_tree",
        nargs: 1,
        strict: true,
        retset: true,
        func: fc_pg_partition_tree,
    },
    FmgrBuiltin {
        foid: 3424,
        name: "pg_partition_root",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_partition_root,
    },
    FmgrBuiltin {
        foid: 3425,
        name: "pg_partition_ancestors",
        nargs: 1,
        strict: true,
        retset: true,
        func: fc_pg_partition_ancestors,
    },
];

fn check_rel_can_be_partition(relid: Oid) -> PgResult<bool> {
    let Some(reltup) = syscache_seams::lookup_pg_class_ls_shape::call(relid)? else {
        return Ok(false);
    };
    Ok(reltup.relispartition || RELKIND_HAS_PARTITIONS(reltup.relkind as u8))
}

struct TreeRows {
    tuples: Vec<Vec<u8>>,
}

fn collect_tree_rows(flinfo: &FmgrInfo, fcinfo: &Fcinfo, rootrelid: Oid) -> PgResult<TreeRows> {
    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != TypeFuncClass::Composite {
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    let desc = resolved.result_tuple_desc.expect("composite result carries a tupdesc");

    let partitions = pg_inherits::find_all_inheritors(mcx, rootrelid, AccessShareLock)?;
    let mut tuples = Vec::with_capacity(partitions.len());
    for &relid in partitions.iter() {
        let relkind = lookup_relkind(relid)?;
        let ancestors = pg_inherits::get_partition_ancestors(mcx, relid)?;

        let mut values = [Datum::null(); 4];
        let mut nulls = [false; 4];
        values[0] = Datum::from_oid(relid);
        let parentid = ancestors.first().copied().unwrap_or(InvalidOid);
        if parentid != InvalidOid {
            values[1] = Datum::from_oid(parentid);
        } else {
            nulls[1] = true;
        }
        values[2] = Datum::from_bool(!RELKIND_HAS_PARTITIONS(relkind));
        let mut level = 0i32;
        if relid != rootrelid {
            for &a in ancestors.iter() {
                level += 1;
                if a == rootrelid {
                    break;
                }
            }
        }
        values[3] = Datum::from_i32(level);

        let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &nulls)?;
        tuples.push(tuple.image().to_vec());
    }
    Ok(TreeRows { tuples })
}

fn lookup_relkind(relid: Oid) -> PgResult<u8> {
    Ok(syscache_seams::lookup_pg_class_ls_shape::call(relid)?
        .map_or(0, |reltup| reltup.relkind as u8))
}

pub fn fc_pg_partition_tree(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_partition_tree: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let rootrelid = fcinfo.arg_oid(0);
        let rows = if check_rel_can_be_partition(rootrelid)? {
            collect_tree_rows(flinfo, fcinfo, rootrelid)?
        } else {
            TreeRows { tuples: Vec::new() }
        };
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("pg_partition_tree: rows set at first call")
        .downcast_ref::<TreeRows>()
        .expect("pg_partition_tree: user_fctx is TreeRows");
    match rows.tuples.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

pub fn fc_pg_partition_root(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let relid = fcinfo.arg_oid(0);
    if !check_rel_can_be_partition(relid)? {
        return Ok(fcinfo.return_null());
    }
    let ancestors = pg_inherits::get_partition_ancestors(fcinfo.result_mcx(), relid)?;
    match ancestors.last() {
        None => Ok(Datum::from_oid(relid)),
        Some(&rootrelid) => Ok(Datum::from_oid(rootrelid)),
    }
}

struct AncestorRows {
    oids: Vec<Oid>,
}

pub fn fc_pg_partition_ancestors(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_partition_ancestors: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let relid = fcinfo.arg_oid(0);
        let mut oids = Vec::new();
        if check_rel_can_be_partition(relid)? {
            oids.push(relid);
            oids.extend(
                pg_inherits::get_partition_ancestors(fcinfo.result_mcx(), relid)?
                    .iter()
                    .copied(),
            );
        }
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(AncestorRows { oids }));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("pg_partition_ancestors: rows set at first call")
        .downcast_ref::<AncestorRows>()
        .expect("pg_partition_ancestors: user_fctx is AncestorRows");
    match rows.oids.get(idx) {
        Some(&oid) => Ok(funcapi::srf_return_next(flinfo, fcinfo, Datum::from_oid(oid))),
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}
