//! Native demonstration that the pg_server_to_client seam model is
//! LOAD-BEARING for the json_send theorems: with a skewed (non-identity)
//! seam, the shipped json_send output diverges from the C shim's
//! identity-conversion image. Own test binary = own process, so the
//! seam::set here cannot collide with native_check's identity install.
use proof_json_text as _;
use std::os::raw::c_int;

extern "C" {
    fn pg_json_send(vardata: *const u8, len: c_int, out: *mut u8) -> c_int;
}

#[test]
fn seam_skew_diverges() {
    mbutils_seams::pg_server_to_client::set(|mcx, s| {
        let mut v = mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
        mcx::vec_append_bytes(&mut v, b"X")?;
        mcx::vec_append_bytes(&mut v, s)?;
        Ok(Some(v))
    });
    let ctx = mcx::MemoryContext::new_bump("seam-skew");
    let buf = [b'1'];
    let mut c_out = [0u8; 8];
    let c_total = unsafe { pg_json_send(buf.as_ptr(), 1, c_out.as_mut_ptr()) } as usize;
    let b = adt_json::json_send(ctx.mcx(), &buf).expect("json_send");
    assert_ne!(
        b.as_bytes(),
        &c_out[..c_total],
        "skewed seam must change the wire image"
    );
}
