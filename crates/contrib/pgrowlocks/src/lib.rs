//! `contrib/pgrowlocks` — list the rows of a table currently locked by open
//! transactions, decoding the lock kind from each row header's infomask and
//! expanding multixact lockers into per-member xid/mode/pid.
//!
//! C builds each row as C strings through BuildTupleFromCStrings: the same
//! strings go through the declared result columns' input functions here.

#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use std::ffi::CString;

use types_core::{catalog, Oid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_WRONG_OBJECT_TYPE};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_tuple::tupdesc::TupleDescData;
use types_rel::pg_class::{RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};
use types_storage::multixact::{MultiXactMember, MultiXactStatus};
use types_tuple::htup::{
    HeapTupleData, HeapTupleHeaderData, HEAP_KEYS_UPDATED, HEAP_LOCKED_UPGRADED,
    HEAP_XMAX_IS_EXCL_LOCKED, HEAP_XMAX_IS_KEYSHR_LOCKED, HEAP_XMAX_IS_MULTI,
    HEAP_XMAX_IS_SHR_LOCKED, HEAP_XMAX_LOCK_ONLY,
};

const LIBRARY: &str = "pgrowlocks";

// pg_authid.dat ROLE_PG_STAT_SCAN_TABLES.
const ROLE_PG_STAT_SCAN_TABLES: types_core::Oid = 3377;

fn single_locker_mode(infomask: u16, infomask2: u16) -> &'static str {
    if infomask & HEAP_XMAX_LOCK_ONLY != 0 {
        if HEAP_XMAX_IS_SHR_LOCKED(infomask) {
            "For Share"
        } else if HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) {
            "For Key Share"
        } else if HEAP_XMAX_IS_EXCL_LOCKED(infomask) {
            if infomask2 & HEAP_KEYS_UPDATED != 0 {
                "For Update"
            } else {
                "For No Key Update"
            }
        } else {
            // neither keyshare nor exclusive bit is set
            "transient upgrade status"
        }
    } else if infomask2 & HEAP_KEYS_UPDATED != 0 {
        "Update"
    } else {
        "No Key Update"
    }
}

fn mode_name(status: MultiXactStatus) -> &'static str {
    match status {
        MultiXactStatus::MultiXactStatusUpdate => "Update",
        MultiXactStatus::MultiXactStatusNoKeyUpdate => "No Key Update",
        MultiXactStatus::MultiXactStatusForUpdate => "For Update",
        MultiXactStatus::MultiXactStatusForNoKeyUpdate => "For No Key Update",
        MultiXactStatus::MultiXactStatusForShare => "For Share",
        MultiXactStatus::MultiXactStatusForKeyShare => "For Key Share",
    }
}

// TupleDescGetAttInMetadata: each declared column's input function, once;
// a dropped column has none.
fn att_in_metadata(tupdesc: &TupleDescData<'_>) -> PgResult<Vec<Option<(FmgrInfo, Oid, i32)>>> {
    let natts = tupdesc.natts as usize;
    let mut atts = Vec::with_capacity(natts);
    for i in 0..natts {
        let att = tupdesc.attr(i);
        atts.push(if att.attisdropped {
            None
        } else {
            let (infunc, typioparam) = lsyscache::getTypeInputInfo(att.atttypid)?;
            Some((fmgr_core::fmgr_info(infunc)?, typioparam, att.atttypmod))
        });
    }
    Ok(atts)
}

// BuildTupleFromCStrings: the strings through the columns' input functions;
// dropped columns are NULL.
fn build_tuple_from_cstrings(
    mcx: Mcx<'_>,
    attinmeta: &mut [Option<(FmgrInfo, Oid, i32)>],
    values: &[String],
) -> PgResult<(Vec<Datum>, Vec<bool>)> {
    let n = attinmeta.len();
    let mut datums = vec![Datum::null(); n];
    let mut nulls = vec![false; n];
    for (i, att) in attinmeta.iter_mut().enumerate() {
        match (att, values.get(i)) {
            (Some((flinfo, typioparam, typmod)), Some(s)) => {
                let cstr = CString::new(s.as_str()).expect("pgrowlocks: interior NUL");
                datums[i] =
                    types_fmgr::input_function_call(flinfo, Some(&cstr), *typioparam, *typmod, mcx)?;
            }
            _ => nulls[i] = true,
        }
    }
    Ok((datums, nulls))
}

// textToQualifiedNameList + makeRangeVarFromNameList + relation_openrv.
fn relation_open_by_text_arg<'m>(
    mcx: Mcx<'m>,
    fcinfo: &Fcinfo,
    i: usize,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<types_rel::Relation<'m>> {
    // SAFETY: arg i is a non-null text (STRICT).
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    let rawname = String::from_utf8_lossy(v.data()).into_owned();
    let encoding = if mbutils_seams::get_database_encoding::is_installed() {
        mbutils_seams::get_database_encoding::call()
    } else {
        wchar::PG_SQL_ASCII
    };
    // C textToQualifiedNameList: false/NIL → 42602; quoted-empty `""` is kept.
    let names = match varlena::split_identifier_string(mcx, &rawname, b'.', encoding)? {
        Some(names) if !names.is_empty() => names,
        _ => {
            return Err(Box::new(
                PgError::error("invalid name syntax")
                    .with_sqlstate(types_error::ERRCODE_INVALID_NAME),
            ));
        }
    };
    let (catalogname, schemaname, relname) = match names.as_slice() {
        [r] => (None, None, r.as_str()),
        [s, r] => (None, Some(s.as_str()), r.as_str()),
        [c, s, r] => (Some(c.as_str()), Some(s.as_str()), r.as_str()),
        _ => {
            // namespace.c:3578 formats this with NameListToString(names): the
            // parsed list (downcased / unquoted / whitespace-trimmed) joined by
            // '.', never the raw text argument.
            return Err(Box::new(
                PgError::error(format!(
                    "improper relation name (too many dotted names): {}",
                    names.join(".")
                ))
                .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
            ))
        }
    };
    let rv = rel_vocab::RangeVar {
        catalogname,
        schemaname,
        relname,
        inh: true,
        relpersistence: catalog::RELPERSISTENCE_PERMANENT,
        location: -1,
    };
    relation::relation_openrv(mcx, &rv, lockmode)
}

fn fc_pgrowlocks(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pgrowlocks: resolved FmgrInfo required");
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    let mut attinmeta = att_in_metadata(&srf.tupdesc)?;

    let rel = relation_open_by_text_arg(mcx, fcinfo, 0, types_rel::AccessShareLock)?;

    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Err(Box::new(
            PgError::error(format!("\"{}\" is a partitioned table", rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail("Partitioned tables do not contain rows.".to_string()),
        ));
    } else if rel.rd_rel.relkind != RELKIND_RELATION {
        return Err(Box::new(
            PgError::error(format!("\"{}\" is not a table", rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    } else if rel.rd_rel.relam != tableam::HEAP_TABLE_AM_OID {
        return Err(Box::new(
            PgError::error("only heap AM is supported")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    // Must have SELECT on the table or be in pg_stat_scan_tables.
    let user = miscinit::GetUserId();
    let mut aclresult =
        aclchk::pg_class_aclcheck(rel.rd_id, user, types_nodes::parsenodes::ACL_SELECT as u64)?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclresult = if adt_acl::has_privs_of_role(user, ROLE_PG_STAT_SCAN_TABLES)? {
            aclchk::ACLCHECK_OK
        } else {
            aclchk::ACLCHECK_NO_PRIV
        };
    }
    if aclresult != aclchk::ACLCHECK_OK {
        aclchk::aclcheck_error(
            aclresult,
            tablecmds::get_relkind_objtype(rel.rd_rel.relkind),
            rel.name(),
        )?;
    }

    let snapshot = snapmgr::GetActiveSnapshot();
    let curcid = xact::GetCurrentCommandId(false)?;
    let scan = tableam::table_beginscan(mcx, &rel, Some(snapshot), 0, mcx::PgVec::new_in(mcx))?;
    let tableam::TableScanDesc::Heap(mut hscan) = scan else {
        unreachable!("heap AM checked above");
    };

    loop {
        let (t_len, t_self, t_table_oid, hdr) =
            match heapam::heap_getnext(&mut hscan, types_scan::sdir::ScanDirection::ForwardScanDirection)? {
                Some(t) => (t.t_len, t.t_self, t.t_tableOid, t.header_ptr()),
                None => break,
            };

        // A buffer lock must be held to call HeapTupleSatisfiesUpdate.
        let buf = hscan.rs_cbuf.as_ref().expect("current scan buffer").buffer();
        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_SHARE)?;

        // SAFETY: the tuple image lives in the pinned current buffer.
        let mut htup =
            unsafe { HeapTupleData::from_raw_parts(hdr, t_len, t_self, t_table_oid) };
        let htsu = heapam_visibility::HeapTupleSatisfiesUpdate(&mut htup, curcid, buf)?;
        // SAFETY: header in the locked, pinned buffer.
        let (xmax, infomask, infomask2) = unsafe {
            let h = &*hdr.cast::<HeapTupleHeaderData>();
            (h.xmax_raw(), h.t_infomask, h.t_infomask2)
        };

        if htsu != tableam_vocab::TM_Result::TM_BeingModified {
            bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_UNLOCK)?;
            continue;
        }

        let blkno = ((t_self.ip_blkid.bi_hi as u32) << 16) | t_self.ip_blkid.bi_lo as u32;
        let is_multi = infomask & HEAP_XMAX_IS_MULTI != 0;

        let (xids_s, modes_s, pids_s) = if is_multi {
            let allow_old = HEAP_LOCKED_UPGRADED(infomask);
            let mut members: Vec<MultiXactMember> = Vec::new();
            let nmembers = multixact::GetMultiXactIdMembers(xmax, allow_old, false, &mut |m| {
                members.extend_from_slice(m)
            })?;
            if nmembers == -1 {
                ("{0}".to_string(), "{transient upgrade status}".to_string(), "{0}".to_string())
            } else {
                let xids: Vec<String> = members.iter().map(|m| m.xid.to_string()).collect();
                let modes: Vec<&str> = members.iter().map(|m| mode_name(m.status)).collect();
                let pids: Vec<String> = members
                    .iter()
                    .map(|m| procarray::BackendXidGetPid(m.xid).to_string())
                    .collect();
                (
                    format!("{{{}}}", xids.join(",")),
                    format!("{{{}}}", modes.join(",")),
                    format!("{{{}}}", pids.join(",")),
                )
            }
        } else {
            (
                format!("{{{xmax}}}"),
                format!("{{{}}}", single_locker_mode(infomask, infomask2)),
                format!("{{{}}}", procarray::BackendXidGetPid(xmax)),
            )
        };

        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_UNLOCK)?;

        let values = [
            format!("({blkno},{})", t_self.ip_posid),
            xmax.to_string(),
            (if is_multi { "true" } else { "false" }).to_string(),
            xids_s,
            modes_s,
            pids_s,
        ];
        let (datums, nulls) = build_tuple_from_cstrings(mcx, &mut attinmeta, &values)?;
        srf.putvalues(&datums, &nulls)?;
    }

    heapam::heap_endscan(hscan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(srf.finish(fcinfo))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "pgrowlocks" => fc_pgrowlocks,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_tuple::htup::{HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_SHR_LOCK};

    #[test]
    fn single_locker_mode_arms() {
        // pgrowlocks.c lock-mode decode over the C infomask combinations.
        assert_eq!(single_locker_mode(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_SHR_LOCK, 0), "For Share");
        assert_eq!(
            single_locker_mode(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK, 0),
            "For Key Share"
        );
        assert_eq!(
            single_locker_mode(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK, HEAP_KEYS_UPDATED),
            "For Update"
        );
        assert_eq!(
            single_locker_mode(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK, 0),
            "For No Key Update"
        );
        assert_eq!(single_locker_mode(HEAP_XMAX_LOCK_ONLY, 0), "transient upgrade status");
        assert_eq!(single_locker_mode(0, HEAP_KEYS_UPDATED), "Update");
        assert_eq!(single_locker_mode(0, 0), "No Key Update");
    }

    #[test]
    fn multixact_mode_names() {
        use MultiXactStatus::*;
        assert_eq!(mode_name(MultiXactStatusUpdate), "Update");
        assert_eq!(mode_name(MultiXactStatusNoKeyUpdate), "No Key Update");
        assert_eq!(mode_name(MultiXactStatusForUpdate), "For Update");
        assert_eq!(mode_name(MultiXactStatusForNoKeyUpdate), "For No Key Update");
        assert_eq!(mode_name(MultiXactStatusForShare), "For Share");
        assert_eq!(mode_name(MultiXactStatusForKeyShare), "For Key Share");
    }
}
