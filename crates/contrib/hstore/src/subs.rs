//! hstore_subs.c execution bodies, installed on hstore_subs_seams; the
//! transform/exec plumbing lives in parse_expr::subscripts and execexpr.

use datum::{Datum, NullableDatum};
use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_tuple::varatt;

use crate::repr::{build_hstore, find_key, HstoreView, Pair};
use crate::{check_key_len, check_val_len};

// DatumGetTextPP payload (short or 4B header; already detoasted upstream).
fn text_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live inline text varlena datum.
    unsafe {
        if varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p) {
            core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            )
        } else {
            core::slice::from_raw_parts(p.add(4), varatt::varsize_4b(p) - 4)
        }
    }
}

fn hstore_view<'a>(mcx: Mcx<'a>, d: Datum) -> PgResult<HstoreView<'a>> {
    let img = crate::gist::detoasted_image(mcx, d)?;
    Ok(HstoreView::from_vardata(&img[4..]))
}

pub(crate) fn fetch<'m>(mcx: Mcx<'m>, source: Datum, key: Datum) -> PgResult<NullableDatum> {
    let hs = hstore_view(mcx, source)?;
    let key = text_payload(key);
    match find_key(&hs, None, key) {
        Some(idx) if !hs.val_isnull(idx) => {
            let t = varlena::cstring_to_text(mcx, hs.val(idx))?;
            Ok(NullableDatum { value: types_fmgr::varlena_result(t), isnull: false })
        }
        _ => Ok(NullableDatum::null()),
    }
}

pub(crate) fn assign<'m>(
    mcx: Mcx<'m>,
    source: NullableDatum,
    key: Datum,
    replace: NullableDatum,
) -> PgResult<Datum> {
    let key = text_payload(key).to_vec();
    check_key_len(key.len())?;
    let val = if replace.isnull {
        None
    } else {
        let v = text_payload(replace.value).to_vec();
        check_val_len(v.len())?;
        Some(v)
    };
    let p = Pair { key, val, needfree: false };

    let img = if source.isnull {
        build_hstore(&[p])
    } else {
        // hstore_concat with the single new pair (s2 wins ties).
        let hs = hstore_view(mcx, source.value)?;
        let one = build_hstore(&[p]);
        let s2 = HstoreView::from_vardata(&one[4..]);
        build_hstore(&crate::concat_pairs(&hs, &s2))
    };
    let mut out: mcx::PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, img.len())?;
    mcx::vec_append_bytes(&mut out, &img)?;
    let d = Datum::from_usize(out.as_ptr() as usize);
    core::mem::forget(out);
    Ok(d)
}

// The handler function exists only for CREATE EXTENSION's C-symbol
// validation and pg_type.typsubscript resolution; the parse/exec plumbing
// resolves it by proname, never through fmgr.
pub fn fc_hstore_subscript_handler(
    _f: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Err(Box::new(PgError::error(
        "hstore: hstore_subscript_handler reached through a raw fmgr call — \
         subscripting dispatches by proname in parse_expr/execexpr",
    )))
}
