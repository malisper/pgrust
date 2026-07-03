// No conversion proc is ported, so the client!=server arm is driven by a
// fixture proc with the C conversion-proc ABI (writes a NUL-terminated image
// into the worst-case dest buffer, returns the consumed source-byte count);
// it needs raw-pointer writes, which pqformat's forbid(unsafe_code) pushes
// into this integration-test crate.
use datum::Datum;
use pqformat::{pq_getmsgstring, pq_getmsgtext, PqString};
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use wchar::{PG_LATIN1, PG_UTF8};

fn upper_conv(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let src = fcinfo.args[2].value.as_usize() as *const u8;
    let dst = fcinfo.args[3].value.as_usize() as *mut u8;
    let len = fcinfo.args[4].value.as_i32() as usize;
    // SAFETY: convert_with_proc's contract — `len` readable source bytes,
    // `len * MAX_CONVERSION_GROWTH + 1` writable dest bytes.
    unsafe {
        for i in 0..len {
            *dst.add(i) = (*src.add(i)).to_ascii_uppercase();
        }
        *dst.add(len) = 0;
    }
    Ok(Datum::from_i32(len as i32))
}

#[test]
fn getmsg_converts_when_encodings_differ() {
    xact_seams::is_transaction_state::set(|| true);
    namespace_seams::find_default_conversion_proc::set(|_, _| Ok(4242));
    fmgr_seams::fmgr_info::set(|oid| Ok(FmgrInfo::new(upper_conv, oid, 6, true, false)));
    mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
    mbutils::InitializeClientEncoding().unwrap();
    assert_eq!(mbutils::PrepareClientEncoding(PG_LATIN1).unwrap(), 0);
    assert_eq!(mbutils::SetClientEncoding(PG_LATIN1).unwrap(), 0);

    let ctx = mcx::MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut msg =
        stringinfo::StringInfo::from_vec(mcx::slice_in(mcx, b"abc\0def").unwrap()).unwrap();
    let s = pq_getmsgstring(mcx, &mut msg).unwrap();
    assert!(matches!(s, PqString::Converted(_)));
    assert_eq!(s.as_bytes(), b"ABC");
    drop(s);
    let t = pq_getmsgtext(mcx, &mut msg, 3).unwrap();
    assert_eq!(&t[..], b"DEF");
}
