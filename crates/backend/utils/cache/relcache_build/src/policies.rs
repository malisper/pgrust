use mcx::{Mcx, MemoryContext, PgVec};
use relcache_build_seams::PgPolicyShape;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_rel::AccessShareLock;
use types_tuple::{HeapTupleData, TupleDescData};

use crate::rules::text_attr;
use crate::{getattr, oid_key, req};

const POLICY_RELATION_ID: Oid = 3256;
const POLICY_POLRELID_POLNAME_INDEX_ID: Oid = 3258;
const Anum_pg_policy_polname: i32 = 2;
const Anum_pg_policy_polrelid: i32 = 3;
const Anum_pg_policy_polcmd: i32 = 4;
const Anum_pg_policy_polpermissive: i32 = 5;
const Anum_pg_policy_polroles: i32 = 6;
const Anum_pg_policy_polqual: i32 = 7;
const Anum_pg_policy_polwithcheck: i32 = 8;

const OIDOID: Oid = 26;

fn name_datum_str<'a>(d: datum::Datum) -> &'a str {
    // SAFETY: a non-null pg_policy name column is a 64-byte NameData image.
    let bytes = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 64) };
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(64);
    core::str::from_utf8(&bytes[..len]).expect("non-UTF-8 name in pg_policy")
}

fn opt_text_attr<'mcx>(
    mcx: Mcx<'mcx>,
    td: &TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
    attno: i32,
) -> PgResult<Option<&'mcx str>> {
    let (_, isnull) = getattr(td, tup, attno);
    if isnull {
        return Ok(None);
    }
    Ok(Some(text_attr(mcx, td, tup, attno)?))
}

// deconstruct_array_builtin(polroles, OIDOID) — 1-D no-null oid[] only
// (polroles is BKI_FORCE_NOT_NULL and written by CreatePolicy without nulls).
fn oid_array_attr<'mcx>(
    mcx: Mcx<'mcx>,
    td: &TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
    attno: i32,
) -> PgResult<&'mcx [Oid]> {
    let d = req(td, tup, attno)?;
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum addresses in-tuple bytes; length is
    // taken from its own header before slicing.
    let raw = unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            detoast::varsize_any(core::slice::from_raw_parts(p, 2))
        } else if b0 & 0x01 != 0 {
            ((b0 >> 1) & 0x7F) as usize
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    };
    let image = detoast::detoast_attr(mcx, raw)?;
    let a = &image[datum::varlena::VARHDRSZ..];
    let word = |off: usize| u32::from_ne_bytes([a[off], a[off + 1], a[off + 2], a[off + 3]]);
    let ndim = word(0);
    let dataoffset = word(4);
    let elemtype = word(8);
    if ndim != 1 || dataoffset != 0 || elemtype != OIDOID {
        return Err(Box::new(PgError::error(format!(
            "unexpected pg_policy.polroles array shape (ndim {ndim}, elemtype {elemtype})"
        ))));
    }
    let nelems = word(12) as usize;
    let mut out: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, nelems)?;
    for i in 0..nelems {
        out.push(word(20 + 4 * i));
    }
    Ok(out.leak())
}

// RelationBuildRowSecurity's pg_policy scan half (policy.c): polrelid equality
// over PolicyPolrelidPolnameIndexId, polname order.
pub(crate) fn scan_pg_policy<'mcx>(
    mcx: Mcx<'mcx>,
    polrelid: Oid,
) -> PgResult<PgVec<'mcx, PgPolicyShape<'mcx>>> {
    let cx = MemoryContext::new("ScanPgPolicy");
    let scan_mcx = cx.mcx();
    let rel = table::table_open(scan_mcx, POLICY_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_policy_polrelid, polrelid)];
    let mut scan = genam::systable_beginscan(
        scan_mcx,
        &rel,
        POLICY_POLRELID_POLNAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let mut rows: PgVec<'mcx, PgPolicyShape<'mcx>> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(scan_mcx, &mut scan)? {
        let td = rel.descr();
        rows.push(PgPolicyShape {
            polname: {
                let s = name_datum_str(req(td, tup, Anum_pg_policy_polname)?);
                let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
                core::str::from_utf8(bytes).expect("pg_policy name is UTF-8")
            },
            polcmd: req(td, tup, Anum_pg_policy_polcmd)?.as_u8(),
            polpermissive: req(td, tup, Anum_pg_policy_polpermissive)?.as_bool(),
            polroles: oid_array_attr(mcx, td, tup, Anum_pg_policy_polroles)?,
            polqual: opt_text_attr(mcx, td, tup, Anum_pg_policy_polqual)?,
            polwithcheck: opt_text_attr(mcx, td, tup, Anum_pg_policy_polwithcheck)?,
        });
    }
    genam::systable_endscan(scan_mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(rows)
}
