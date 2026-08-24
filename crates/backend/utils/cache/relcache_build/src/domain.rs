use mcx::{Mcx, MemoryContext, PgVec};
use typcache_seams::DomainCheckRow;
use types_core::{Oid, CONSTRAINT_RELATION_ID};
use types_error::PgResult;
use types_rel::AccessShareLock;

const ConstraintTypidIndexId: Oid = 2666;
const Anum_pg_constraint_conname: i32 = 2;
const Anum_pg_constraint_contype: i32 = 4;
const Anum_pg_constraint_contypid: i32 = 10;
const Anum_pg_constraint_conbin: i32 = 28;
const CONSTRAINT_CHECK: i8 = b'c' as i8;

pub(crate) fn scan_domain_check_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    contypid: Oid,
) -> PgResult<PgVec<'mcx, DomainCheckRow<'mcx>>> {
    let cx = MemoryContext::new("DomainConstraintScan");
    let smcx = cx.mcx();
    let rel = table::table_open(smcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [crate::oid_key(Anum_pg_constraint_contypid, contypid)];
    let mut scan = genam::systable_beginscan(smcx, &rel, ConstraintTypidIndexId, true, None, &keys)?;
    let mut out: PgVec<'mcx, DomainCheckRow<'mcx>> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        let td = rel.descr();
        if crate::req(td, tup, Anum_pg_constraint_contype)?.as_i8() != CONSTRAINT_CHECK {
            continue;
        }
        let conname = crate::name_from(tup, crate::req(td, tup, Anum_pg_constraint_conname)?);
        let (conbin, isnull) = crate::getattr(td, tup, Anum_pg_constraint_conbin);
        if isnull {
            panic!(
                "domain constraint \"{}\" has NULL conbin",
                String::from_utf8_lossy(conname.name_str())
            );
        }
        let text = crate::attrs::text_str(mcx, smcx, conbin)?;
        // SAFETY: text_str validated UTF-8; leak re-borrows the same bytes.
        let conbin: &str = unsafe { core::str::from_utf8_unchecked(text.into_bytes().leak()) };
        out.push(DomainCheckRow { conname, conbin });
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(out)
}
