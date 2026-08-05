// Native differential for the box_poly image FAILED (playback walled):
// shipped fc_box_poly vs the vendored C driver's dump
// (scratchpad boxpoly_c.txt regenerated per run by boxpoly_c_driver.c).
use datum::{Datum, NullableDatum};
use types_fmgr::LocalFcinfo;

#[test]
fn dump_rust_side() {
    let sp: [f64; 9] = [
        0.0, -0.0, 1.0, -1.0, 1e300, -1e300, f64::INFINITY, f64::NEG_INFINITY, f64::NAN,
    ];
    for (a, &hx) in sp.iter().enumerate() {
        for (b, &hy) in sp.iter().enumerate() {
            for (c, &lx) in sp.iter().enumerate() {
                for (d, &ly) in sp.iter().enumerate() {
                    let mut ib = [0u8; 32];
                    ib[0..8].copy_from_slice(&hx.to_ne_bytes());
                    ib[8..16].copy_from_slice(&hy.to_ne_bytes());
                    ib[16..24].copy_from_slice(&lx.to_ne_bytes());
                    ib[24..32].copy_from_slice(&ly.to_ne_bytes());
                    let ctx = mcx::MemoryContext::new_bump("boxpoly-native");
                    let mut f = LocalFcinfo::<1>::new(0);
                    unsafe { f.set_result_mcx(ctx.mcx()) };
                    f.args[0] = NullableDatum::value(Datum::from_usize(ib.as_ptr() as usize));
                    let dres = (adt_geo::builtins::fc_box_poly)(None, &mut f).unwrap();
                    let img = unsafe {
                        core::slice::from_raw_parts(dres.as_usize() as *const u8, 104)
                    };
                    let hex: String = img.iter().map(|x| format!("{:02x}", x)).collect();
                    println!("{} {} {} {} {}", a, b, c, d, hex);
                    core::mem::forget(ctx);
                }
            }
        }
    }
}
