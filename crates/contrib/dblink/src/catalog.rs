// Local-catalog helpers that touch no remote connection (dblink.c
// dblink_get_pkey + dblink_build_sql_*). These read the LOCAL relation and
// build SQL text for later remote execution.
use datum::Datum;
use mcx::Mcx;
use types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_CARDINALITY_VIOLATION,
    ERRCODE_INVALID_PARAMETER_VALUE,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_rel::{AccessShareLock, Relation};
use types_scan::scankey::{ScanKeyData, BTEqualStrategyNumber};

const INDEX_INDRELID_INDEX_ID: types_core::Oid = 2678;
const ANUM_PG_INDEX_INDRELID: i32 = 2;
const ANUM_PG_INDEX_INDNKEYATTS: i32 = 4;
const ANUM_PG_INDEX_INDISPRIMARY: i32 = 7;
const ANUM_PG_INDEX_INDKEY: i32 = 16;

fn get_rel_from_relname<'mcx>(
    mcx: Mcx<'mcx>,
    rawname: &str,
    lockmode: i32,
    aclmode: u64,
) -> PgResult<Relation<'mcx>> {
    let names = varlena::textToQualifiedNameList(mcx, rawname)?;
    let parts: Vec<&str> = names.iter().map(String::as_str).collect();
    let (catalogname, schemaname, relname) = match parts.as_slice() {
        [r] => (None, None, *r),
        [s, r] => (None, Some(*s), *r),
        [c, s, r] => (Some(*c), Some(*s), *r),
        _ => {
            return Err(Box::new(
                PgError::error(format!("improper relation name (too many dotted names): {rawname}"))
                    .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
            ))
        }
    };
    let rv = rel_vocab::RangeVar {
        catalogname,
        schemaname,
        relname,
        inh: true,
        relpersistence: types_core::catalog::RELPERSISTENCE_PERMANENT,
        location: -1,
    };
    let rel = table::table_openrv(mcx, &rv, lockmode)?;
    let aclresult = aclchk::pg_class_aclcheck(rel.rd_id, miscinit::GetUserId(), aclmode)?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NO_PRIV,
            tablecmds::get_relkind_objtype(rel.rd_rel.relkind as u8),
            rel.name(),
        )?;
    }
    Ok(rel)
}

fn oid_scankey(attno: i32, oid: types_core::Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as i16;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

fn getattr(
    tup: &types_tuple::HeapTupleData<'_>,
    attnum: i32,
    desc: &types_tuple::TupleDescData<'_>,
) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: fixed-position catalog column under pg_index's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
    (d, isnull)
}

// int2vector datum body: i16 elements start after the 24-byte header; dim1
// (element count) sits at offset 16. int2vector is never toasted.
fn int2vector_values(p: *const u8) -> Vec<i16> {
    // SAFETY: p is a live int2vector image (catalog-typed arg).
    let dim1 = unsafe { core::ptr::read_unaligned(p.add(16).cast::<i32>()) };
    let mut v = Vec::with_capacity(dim1.max(0) as usize);
    for i in 0..dim1.max(0) as usize {
        // SAFETY: dim1 elements follow the header contiguously.
        v.push(unsafe { core::ptr::read_unaligned(p.add(24 + 2 * i).cast::<i16>()) });
    }
    v
}

// get_pkey_attnames: primary-key column names, or (0, empty) if none.
fn get_pkey_attnames<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<(i16, Vec<String>)> {
    let index_rel = table::table_open(mcx, types_core::INDEX_RELATION_ID, AccessShareLock)?;
    let key = oid_scankey(ANUM_PG_INDEX_INDRELID, rel.rd_id);
    let mut scan =
        genam::systable_beginscan(mcx, &index_rel, INDEX_INDRELID_INDEX_ID, true, None, &[key])?;
    let tupdesc = rel.descr();
    let mut result = (0i16, Vec::new());
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let idesc = index_rel.descr();
        if !getattr(tup, ANUM_PG_INDEX_INDISPRIMARY, idesc).0.as_bool() {
            continue;
        }
        let indnkeyatts = getattr(tup, ANUM_PG_INDEX_INDNKEYATTS, idesc).0.as_i16();
        if indnkeyatts > 0 {
            let indkey_ptr = getattr(tup, ANUM_PG_INDEX_INDKEY, idesc).0.as_usize() as *const u8;
            let indkey = int2vector_values(indkey_ptr);
            let mut names = Vec::with_capacity(indnkeyatts as usize);
            for i in 0..indnkeyatts as usize {
                let attno = indkey[i];
                let name = tupdesc.attr(attno as usize - 1).attname.name_str();
                names.push(String::from_utf8_lossy(name).into_owned());
            }
            result = (indnkeyatts, names);
        }
        break;
    }
    genam::systable_endscan(mcx, scan)?;
    table::table_close(index_rel, AccessShareLock)?;
    Ok(result)
}

fn generate_relation_name<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<String> {
    let nspname = if catalog_namespace::RelationIsVisible(rel.rd_id)? {
        None
    } else {
        get_namespace_name(mcx, rel.namespace())?
    };
    Ok(ruleutils::quote_qualified_identifier(nspname.as_deref(), rel.name()))
}

fn get_namespace_name(mcx: Mcx<'_>, nspid: types_core::Oid) -> PgResult<Option<String>> {
    Ok(lsyscache::get_namespace_name(mcx, nspid)?
        .map(|s| String::from_utf8_lossy(s.as_bytes()).into_owned()))
}

fn quote_ident_str(mcx: Mcx<'_>, name: &[u8]) -> PgResult<String> {
    let q = quote::quote_identifier(mcx, name)?;
    Ok(String::from_utf8_lossy(q.as_bytes()).into_owned())
}

// quote_literal_cstr (quote.c): E-prefix whenever a backslash forces
// doubled-backslash escaping; double ' and \.
fn quote_literal_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    if s.contains('\\') {
        out.push('E');
    }
    out.push('\'');
    for c in s.chars() {
        if c == '\'' || c == '\\' {
            out.push(c);
        }
        out.push(c);
    }
    out.push('\'');
    out
}

// get_text_array_contents: text[] -> Vec of element strings (None = NULL).
fn get_text_array_contents(mcx: Mcx<'_>, image: &[u8]) -> PgResult<Vec<Option<String>>> {
    let (datums, nulls) =
        arrayfuncs::deconstruct_array_builtin(mcx, image, types_core::TEXTOID, true)?;
    let mut out = Vec::with_capacity(datums.len());
    for (i, d) in datums.iter().enumerate() {
        if nulls[i] {
            out.push(None);
        } else {
            // SAFETY: text element datum from the array image.
            let pv = unsafe { types_fmgr::datum_varlena_packed(*d, mcx)? };
            out.push(Some(String::from_utf8_lossy(pv.data()).into_owned()));
        }
    }
    Ok(out)
}

// validate_pkattnums: 1-based logical attnums -> 0-based physical, dropped
// columns skipped; count clamped to the vector length.
fn validate_pkattnums(
    rel: &Relation<'_>,
    pkattnums: &[i16],
    pknumatts_arg: i32,
) -> PgResult<Vec<usize>> {
    let tupdesc = rel.descr();
    let natts = tupdesc.natts;
    let pknumatts = pknumatts_arg.min(pkattnums.len() as i32);
    if pknumatts <= 0 {
        return Err(Box::new(
            PgError::error("number of key attributes must be > 0")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    let mut out = Vec::with_capacity(pknumatts as usize);
    for &pk in pkattnums.iter().take(pknumatts as usize) {
        let pkattnum = pk as i32;
        if pkattnum <= 0 || pkattnum > natts {
            return Err(invalid_attnum(pkattnum));
        }
        let mut lnum = 0i32;
        let mut found = None;
        for j in 0..natts as usize {
            if tupdesc.attr(j).attisdropped {
                continue;
            }
            lnum += 1;
            if lnum == pkattnum {
                found = Some(j);
                break;
            }
        }
        match found {
            Some(j) => out.push(j),
            None => return Err(invalid_attnum(pkattnum)),
        }
    }
    Ok(out)
}

#[track_caller]
#[cold]
fn invalid_attnum(n: i32) -> Box<PgError> {
    Box::new(
        PgError::error(format!("invalid attribute number {n}"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

fn get_attnum_pk_pos(pkattnums: &[usize], key: usize) -> Option<usize> {
    pkattnums.iter().position(|&p| p == key)
}

// get_tuple_of_interest: SELECT the local row matching src_pkattvals via SPI,
// copy it out (readable after SPI_finish). None = no qualifying row.
fn get_tuple_of_interest<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    pkattnums: &[usize],
    src_pkattvals: &[Option<String>],
) -> PgResult<Option<heaptuple::HeapTuple<'mcx>>> {
    spi::SPI_connect()?;
    let tupdesc = rel.descr();
    let natts = tupdesc.natts as usize;
    let relname = generate_relation_name(mcx, rel)?;

    let mut sql = String::from("SELECT ");
    for i in 0..natts {
        if i > 0 {
            sql.push_str(", ");
        }
        let att = tupdesc.attr(i);
        if att.attisdropped {
            sql.push_str("NULL");
        } else {
            sql.push_str(&quote_ident_str(mcx, att.attname.name_str())?);
        }
    }
    sql.push_str(&format!(" FROM {relname} WHERE "));
    for (i, &pkidx) in pkattnums.iter().enumerate() {
        if i > 0 {
            sql.push_str(" AND ");
        }
        sql.push_str(&quote_ident_str(mcx, tupdesc.attr(pkidx).attname.name_str())?);
        match &src_pkattvals[i] {
            Some(v) => sql.push_str(&format!(" = {}", quote_literal_str(v))),
            None => sql.push_str(" IS NULL"),
        }
    }

    let ret = spi::SPI_exec(&sql, 0)?;
    let processed = spi::SPI_processed();
    if ret == spi::SPI_OK_SELECT && processed > 1 {
        spi::SPI_finish()?;
        return Err(Box::new(
            PgError::error("source criteria matched more than one record")
                .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION),
        ));
    }
    let copied = if ret == spi::SPI_OK_SELECT && processed == 1 {
        spi::SPI_tuptable().and_then(|h| {
            spi::tuptable_with(h, |d| d.vals.first().and_then(|t| heaptuple::heap_copytuple(mcx, t).ok()))
        })
    } else {
        None
    };
    spi::SPI_finish()?;
    Ok(copied)
}

fn tuple_value(
    mcx: Mcx<'_>,
    tuple: &types_tuple::HeapTupleData<'_>,
    tupdesc: &types_tuple::TupleDescData<'_>,
    attnum: i32,
) -> PgResult<Option<String>> {
    Ok(spi::SPI_getvalue(mcx, tuple, tupdesc, attnum)?
        .map(|b| String::from_utf8_lossy(b).into_owned()))
}

fn get_sql_insert(
    mcx: Mcx<'_>,
    rel: &Relation<'_>,
    pkattnums: &[usize],
    src_pkattvals: &[Option<String>],
    tgt_pkattvals: &[Option<String>],
) -> PgResult<String> {
    let relname = generate_relation_name(mcx, rel)?;
    let tupdesc = rel.descr();
    let natts = tupdesc.natts as usize;
    let tuple = get_tuple_of_interest(mcx, rel, pkattnums, src_pkattvals)?
        .ok_or_else(source_not_found)?;

    let mut buf = format!("INSERT INTO {relname}(");
    let mut need_comma = false;
    for i in 0..natts {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            continue;
        }
        if need_comma {
            buf.push(',');
        }
        buf.push_str(&quote_ident_str(mcx, att.attname.name_str())?);
        need_comma = true;
    }
    buf.push_str(") VALUES(");
    need_comma = false;
    for i in 0..natts {
        if tupdesc.attr(i).attisdropped {
            continue;
        }
        if need_comma {
            buf.push(',');
        }
        let val = match get_attnum_pk_pos(pkattnums, i) {
            Some(k) => tgt_pkattvals[k].clone(),
            None => tuple_value(mcx, &tuple, tupdesc, i as i32 + 1)?,
        };
        match val {
            Some(v) => buf.push_str(&quote_literal_str(&v)),
            None => buf.push_str("NULL"),
        }
        need_comma = true;
    }
    buf.push(')');
    Ok(buf)
}

fn get_sql_delete(
    mcx: Mcx<'_>,
    rel: &Relation<'_>,
    pkattnums: &[usize],
    tgt_pkattvals: &[Option<String>],
) -> PgResult<String> {
    let relname = generate_relation_name(mcx, rel)?;
    let tupdesc = rel.descr();
    let mut buf = format!("DELETE FROM {relname} WHERE ");
    for (i, &pkidx) in pkattnums.iter().enumerate() {
        if i > 0 {
            buf.push_str(" AND ");
        }
        buf.push_str(&quote_ident_str(mcx, tupdesc.attr(pkidx).attname.name_str())?);
        match &tgt_pkattvals[i] {
            Some(v) => buf.push_str(&format!(" = {}", quote_literal_str(v))),
            None => buf.push_str(" IS NULL"),
        }
    }
    Ok(buf)
}

fn get_sql_update(
    mcx: Mcx<'_>,
    rel: &Relation<'_>,
    pkattnums: &[usize],
    src_pkattvals: &[Option<String>],
    tgt_pkattvals: &[Option<String>],
) -> PgResult<String> {
    let relname = generate_relation_name(mcx, rel)?;
    let tupdesc = rel.descr();
    let natts = tupdesc.natts as usize;
    let tuple = get_tuple_of_interest(mcx, rel, pkattnums, src_pkattvals)?
        .ok_or_else(source_not_found)?;

    let mut buf = format!("UPDATE {relname} SET ");
    let mut need_comma = false;
    for i in 0..natts {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            continue;
        }
        if need_comma {
            buf.push_str(", ");
        }
        buf.push_str(&format!("{} = ", quote_ident_str(mcx, att.attname.name_str())?));
        let val = match get_attnum_pk_pos(pkattnums, i) {
            Some(k) => tgt_pkattvals[k].clone(),
            None => tuple_value(mcx, &tuple, tupdesc, i as i32 + 1)?,
        };
        match val {
            Some(v) => buf.push_str(&quote_literal_str(&v)),
            None => buf.push_str("NULL"),
        }
        need_comma = true;
    }
    buf.push_str(" WHERE ");
    for (i, &pkidx) in pkattnums.iter().enumerate() {
        if i > 0 {
            buf.push_str(" AND ");
        }
        buf.push_str(&quote_ident_str(mcx, tupdesc.attr(pkidx).attname.name_str())?);
        match &tgt_pkattvals[i] {
            Some(v) => buf.push_str(&format!(" = {}", quote_literal_str(v))),
            None => buf.push_str(" IS NULL"),
        }
    }
    Ok(buf)
}

#[track_caller]
#[cold]
fn source_not_found() -> Box<PgError> {
    Box::new(
        PgError::error("source row not found").with_sqlstate(ERRCODE_CARDINALITY_VIOLATION),
    )
}

fn array_len_mismatch(which: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{which} key array length must match number of key attributes"))
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
    )
}

// --- fmgr entry points ---

pub fn fc_dblink_get_pkey(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("dblink_get_pkey: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: strict text arg.
    let relname = unsafe { fcinfo.arg_varlena_packed(0)? };
    let rel = get_rel_from_relname(mcx, &String::from_utf8_lossy(relname.data()), AccessShareLock, adt_acl::ACL_SELECT)?;
    let (nkeyatts, names) = get_pkey_attnames(mcx, &rel)?;
    table::table_close(rel, AccessShareLock)?;

    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    for (i, name) in names.iter().enumerate().take(nkeyatts.max(0) as usize) {
        let pos = Datum::from_i32(i as i32 + 1);
        let col = types_fmgr::varlena_result(varlena::cstring_to_text(mcx, name.as_bytes())?);
        srf.putvalues(&[pos, col], &[false, false])?;
    }
    Ok(srf.finish(fcinfo))
}

fn build_args<'mcx>(fcinfo: &Fcinfo, rel: &Relation<'mcx>) -> PgResult<(Vec<usize>, i32)> {
    // SAFETY: int2vector by-ref arg 1.
    let pkattnums = int2vector_values(unsafe { fcinfo.arg_ptr(1) });
    let pknumatts_arg = fcinfo.arg_i32(2);
    let physical = validate_pkattnums(rel, &pkattnums, pknumatts_arg)?;
    let n = physical.len() as i32;
    Ok((physical, n))
}

pub fn fc_dblink_build_sql_insert(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: strict text arg 0; text[] args 3,4.
    let relname = unsafe { fcinfo.arg_varlena_packed(0)? };
    let rel = get_rel_from_relname(mcx, &String::from_utf8_lossy(relname.data()), AccessShareLock, adt_acl::ACL_SELECT)?;
    let (pkattnums, pknumatts) = build_args(fcinfo, &rel)?;
    let src = get_text_array_contents(mcx, unsafe { fcinfo.arg_varlena_packed(3)? }.image())?;
    if src.len() as i32 != pknumatts {
        table::table_close(rel, AccessShareLock)?;
        return Err(array_len_mismatch("source"));
    }
    let tgt = get_text_array_contents(mcx, unsafe { fcinfo.arg_varlena_packed(4)? }.image())?;
    if tgt.len() as i32 != pknumatts {
        table::table_close(rel, AccessShareLock)?;
        return Err(array_len_mismatch("target"));
    }
    let sql = get_sql_insert(mcx, &rel, &pkattnums, &src, &tgt)?;
    table::table_close(rel, AccessShareLock)?;
    Ok(crate::text_result(mcx, &sql)?)
}

pub fn fc_dblink_build_sql_delete(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: strict text arg 0; text[] arg 3.
    let relname = unsafe { fcinfo.arg_varlena_packed(0)? };
    let rel = get_rel_from_relname(mcx, &String::from_utf8_lossy(relname.data()), AccessShareLock, adt_acl::ACL_SELECT)?;
    let (pkattnums, pknumatts) = build_args(fcinfo, &rel)?;
    let tgt = get_text_array_contents(mcx, unsafe { fcinfo.arg_varlena_packed(3)? }.image())?;
    if tgt.len() as i32 != pknumatts {
        table::table_close(rel, AccessShareLock)?;
        return Err(array_len_mismatch("target"));
    }
    let sql = get_sql_delete(mcx, &rel, &pkattnums, &tgt)?;
    table::table_close(rel, AccessShareLock)?;
    Ok(crate::text_result(mcx, &sql)?)
}

pub fn fc_dblink_build_sql_update(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: strict text arg 0; text[] args 3,4.
    let relname = unsafe { fcinfo.arg_varlena_packed(0)? };
    let rel = get_rel_from_relname(mcx, &String::from_utf8_lossy(relname.data()), AccessShareLock, adt_acl::ACL_SELECT)?;
    let (pkattnums, pknumatts) = build_args(fcinfo, &rel)?;
    let src = get_text_array_contents(mcx, unsafe { fcinfo.arg_varlena_packed(3)? }.image())?;
    if src.len() as i32 != pknumatts {
        table::table_close(rel, AccessShareLock)?;
        return Err(array_len_mismatch("source"));
    }
    let tgt = get_text_array_contents(mcx, unsafe { fcinfo.arg_varlena_packed(4)? }.image())?;
    if tgt.len() as i32 != pknumatts {
        table::table_close(rel, AccessShareLock)?;
        return Err(array_len_mismatch("target"));
    }
    let sql = get_sql_update(mcx, &rel, &pkattnums, &src, &tgt)?;
    table::table_close(rel, AccessShareLock)?;
    Ok(crate::text_result(mcx, &sql)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_literal_matches_c() {
        assert_eq!(quote_literal_str("plain"), "'plain'");
        assert_eq!(quote_literal_str("O'Brien"), "'O''Brien'");
        // backslash forces the E'' prefix and doubled backslash.
        assert_eq!(quote_literal_str("a\\b"), "E'a\\\\b'");
        assert_eq!(quote_literal_str("x'\\y"), "E'x''\\\\y'");
    }

    #[test]
    fn int2vector_decode() {
        // header: vl_len_, ndim=1, dataoffset=0, elemtype=INT2, dim1=3,
        // lbound1=0, then i16[3] = {2, 4, 6}.
        let mut buf = vec![0u8; 24 + 6];
        buf[16..20].copy_from_slice(&3i32.to_ne_bytes()); // dim1
        buf[24..26].copy_from_slice(&2i16.to_ne_bytes());
        buf[26..28].copy_from_slice(&4i16.to_ne_bytes());
        buf[28..30].copy_from_slice(&6i16.to_ne_bytes());
        assert_eq!(int2vector_values(buf.as_ptr()), vec![2, 4, 6]);
    }
}
