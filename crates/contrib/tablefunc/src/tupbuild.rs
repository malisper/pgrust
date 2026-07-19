//! `funcapi.c` `TupleDescGetAttInMetadata` / `BuildTupleFromCStrings`, the
//! slice tablefunc needs (no other consumer yet, so it lives here rather than
//! in the shared funcapi crate). Resolves each column's input function once
//! into a carrier; `build` produces the `(values, isnull)` pair the
//! MaterializedSRF tuplestore consumes (C forms a heaptuple + puttuple; the
//! tuplestore does the forming here).

use core::ffi::CStr;

use datum::Datum;
use mcx::Mcx;
use types_error::PgResult;
use types_fmgr::FmgrInfo;
use types_tuple::TupleDescData;

struct AttIn {
    // `None` for a dropped column: never fed a value (C zeroes its in_funcs).
    in_func: Option<FmgrInfo>,
    typ_ioparam: types_core::Oid,
    atttypmod: i32,
}

pub struct AttInMetadata {
    atts: Vec<AttIn>,
}

impl AttInMetadata {
    pub fn new(tupdesc: &TupleDescData<'_>) -> PgResult<AttInMetadata> {
        let natts = tupdesc.natts as usize;
        let mut atts = Vec::with_capacity(natts);
        for att in &tupdesc.attrs[..natts] {
            if att.attisdropped {
                atts.push(AttIn { in_func: None, typ_ioparam: types_core::InvalidOid, atttypmod: -1 });
                continue;
            }
            let (typinput, typioparam) = lsyscache::typ::getTypeInputInfo(att.atttypid)?;
            atts.push(AttIn {
                in_func: Some(fmgr_core::fmgr_info(typinput)?),
                typ_ioparam: typioparam,
                atttypmod: att.atttypmod,
            });
        }
        Ok(AttInMetadata { atts })
    }

    pub fn natts(&self) -> usize {
        self.atts.len()
    }

    // C BuildTupleFromCStrings: each `values[i]` is a NUL-terminated cstring
    // or None (SQL NULL). Returns the (datum, isnull) columns.
    pub fn build(
        &mut self,
        mcx: Mcx<'_>,
        values: &[Option<&[u8]>],
    ) -> PgResult<(Vec<Datum>, Vec<bool>)> {
        let natts = self.natts();
        let mut out = Vec::with_capacity(natts);
        let mut isnull = Vec::with_capacity(natts);
        for (i, att) in self.atts.iter_mut().enumerate() {
            let Some(in_func) = att.in_func.as_mut() else {
                out.push(Datum::null());
                isnull.push(true);
                continue;
            };
            // C calls InputFunctionCall even for NULL (non-strict input fns —
            // domain_in — must see the NULL); isnull is values[i] == NULL.
            let mut buf;
            let cstr = match values[i] {
                Some(bytes) => {
                    buf = Vec::with_capacity(bytes.len() + 1);
                    buf.extend_from_slice(bytes);
                    buf.push(0);
                    Some(CStr::from_bytes_with_nul(&buf).expect("appended single NUL"))
                }
                None => None,
            };
            let d = types_fmgr::input_function_call(
                in_func,
                cstr,
                att.typ_ioparam,
                att.atttypmod,
                mcx,
            )?;
            out.push(d);
            isnull.push(values[i].is_none());
        }
        Ok((out, isnull))
    }
}
