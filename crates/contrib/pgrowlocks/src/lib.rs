//! `contrib/pgrowlocks` — list the rows of a table currently locked by open
//! transactions, decoding the lock kind from each row header's infomask and
//! expanding multixact lockers into per-member xid/mode/pid.
//!
//! C builds each row as C strings through BuildTupleFromCStrings; the same
//! values are built here as typed datums (tid, xid, bool, xid[], text[],
//! int4[]) — array_out re-quotes space-containing mode names identically.

#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::{catalog, TransactionId, INT4OID, TEXTOID, XIDOID};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_WRONG_OBJECT_TYPE};
use types_fmgr::{byref_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_rel::pg_class::{RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};
use types_storage::multixact::{MultiXactMember, MultiXactStatus};
use types_tuple::htup::{
    HeapTupleData, HeapTupleHeaderData, HEAP_KEYS_UPDATED, HEAP_LOCKED_UPGRADED,
    HEAP_XMAX_IS_EXCL_LOCKED, HEAP_XMAX_IS_KEYSHR_LOCKED, HEAP_XMAX_IS_MULTI,
    HEAP_XMAX_IS_SHR_LOCKED, HEAP_XMAX_LOCK_ONLY,
};
use types_tuple::itemptr::ItemPointerData;

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

fn tid_datum(mcx: Mcx<'_>, ip: ItemPointerData) -> PgResult<Datum> {
    let mut img = [0u8; 6];
    img[0..2].copy_from_slice(&ip.ip_blkid.bi_hi.to_ne_bytes());
    img[2..4].copy_from_slice(&ip.ip_blkid.bi_lo.to_ne_bytes());
    img[4..6].copy_from_slice(&ip.ip_posid.to_ne_bytes());
    byref_result(mcx, &img)
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

fn xid_array(mcx: Mcx<'_>, xids: &[TransactionId]) -> PgResult<Datum> {
    let elems: Vec<Datum> = xids.iter().map(|&x| Datum::from_transaction_id(x)).collect();
    let image = datum::array_build::construct_array_image(mcx, &elems, XIDOID, 4, true, b'i')?;
    byref_result(mcx, &image)
}

fn text_array(mcx: Mcx<'_>, texts: &[&str]) -> PgResult<Datum> {
    let mut elems = Vec::with_capacity(texts.len());
    for t in texts {
        elems.push(text_datum(mcx, t)?);
    }
    let image = datum::array_build::construct_array_image(mcx, &elems, TEXTOID, -1, false, b'i')?;
    byref_result(mcx, &image)
}

fn int4_array(mcx: Mcx<'_>, vals: &[i32]) -> PgResult<Datum> {
    let elems: Vec<Datum> = vals.iter().map(|&v| Datum::from_i32(v)).collect();
    let image = datum::array_build::construct_array_image(mcx, &elems, INT4OID, 4, true, b'i')?;
    byref_result(mcx, &image)
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
    let names = varlena::split_identifier_string(mcx, &rawname, b'.', encoding)?
        .filter(|l| !l.is_empty())
        .ok_or_else(|| {
            Box::new(
                PgError::error("invalid name syntax")
                    .with_sqlstate(types_error::ERRCODE_INVALID_NAME),
            )
        })?;
    let (catalogname, schemaname, relname) = match names.as_slice() {
        [r] => (None, None, r.as_str()),
        [s, r] => (None, Some(s.as_str()), r.as_str()),
        [c, s, r] => (Some(c.as_str()), Some(s.as_str()), r.as_str()),
        _ => {
            return Err(Box::new(
                PgError::error(format!(
                    "improper relation name (too many dotted names): {rawname}"
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

        let locked_row = tid_datum(mcx, t_self)?;
        let locker = Datum::from_transaction_id(xmax);
        let is_multi = infomask & HEAP_XMAX_IS_MULTI != 0;

        let (xids_d, modes_d, pids_d) = if is_multi {
            let allow_old = HEAP_LOCKED_UPGRADED(infomask);
            let mut members: Vec<MultiXactMember> = Vec::new();
            let nmembers = multixact::GetMultiXactIdMembers(xmax, allow_old, false, &mut |m| {
                members.extend_from_slice(m)
            })?;
            if nmembers == -1 {
                (
                    xid_array(mcx, &[0])?,
                    text_array(mcx, &["transient upgrade status"])?,
                    int4_array(mcx, &[0])?,
                )
            } else {
                let xids: Vec<TransactionId> = members.iter().map(|m| m.xid).collect();
                let modes: Vec<&str> = members.iter().map(|m| mode_name(m.status)).collect();
                let pids: Vec<i32> =
                    members.iter().map(|m| procarray::BackendXidGetPid(m.xid)).collect();
                (xid_array(mcx, &xids)?, text_array(mcx, &modes)?, int4_array(mcx, &pids)?)
            }
        } else {
            let mode = single_locker_mode(infomask, infomask2);
            (
                xid_array(mcx, &[xmax])?,
                text_array(mcx, &[mode])?,
                int4_array(mcx, &[procarray::BackendXidGetPid(xmax)])?,
            )
        };

        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_UNLOCK)?;

        let values = [
            locked_row,
            locker,
            Datum::from_bool(is_multi),
            xids_d,
            modes_d,
            pids_d,
        ];
        srf.putvalues(&values, &[false; 6])?;
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
