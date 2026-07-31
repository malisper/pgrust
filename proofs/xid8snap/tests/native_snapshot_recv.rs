//! Native differential replay for the pg_snapshot_recv wire cells
//! (w3-sendrecv lane). The Kani cell eq_snapshot_recv_d20n0 reported
//! VERIFICATION FAILED under kissat (2026-07-30) with the counterexample
//! undecoded (default solver is a CNF memory wall on the Ok-image
//! plane). Per the 3:1 artifact law this test replays the SAME dual
//! execution concretely over a structured + randomized sweep of frames
//! to find (or rule out) a concrete witness.
//!
//! C side: vendored pgc_pg_snapshot_recv (c/pg_xid8snap.c, REL_18
//! bodies + documented shims), linked natively via build.rs.
//! Rust side: shipped xid8funcs::snapshot_recv over a real StringInfo.

use std::os::raw::c_int;

#[link(name = "pg_xid8snap", kind = "static")]
extern "C" {
    fn pgc_pg_snapshot_recv(
        data: *const u8,
        dlen: i32,
        cursor: *mut i32,
        outbuf: *mut u8,
        outlen: *mut i32,
    ) -> c_int;
}

const PGC_ERR_PROTOCOL: c_int = 4;
const PGC_ERR_BADFORMAT: c_int = 22;

fn be_frame(nxip: u32, xmin: u64, xmax: u64, xips: &[u64], extra: usize) -> Vec<u8> {
    let mut f = Vec::new();
    f.extend_from_slice(&nxip.to_be_bytes());
    f.extend_from_slice(&xmin.to_be_bytes());
    f.extend_from_slice(&xmax.to_be_bytes());
    for &x in xips {
        f.extend_from_slice(&x.to_be_bytes());
    }
    f.extend(std::iter::repeat(0xEE).take(extra));
    f
}

fn replay(frame: &[u8]) {
    let mut ccur: i32 = 0;
    let mut cout = [0u8; 128];
    let mut coutlen: i32 = 0;
    let cst = unsafe {
        pgc_pg_snapshot_recv(
            frame.as_ptr(),
            frame.len() as i32,
            &mut ccur,
            cout.as_mut_ptr(),
            &mut coutlen,
        )
    };

    let ctx = mcx::MemoryContext::new_bump("native-snap-recv");
    let mut si = stringinfo::StringInfo::with_capacity_in(ctx.mcx(), frame.len() + 2)
        .expect("alloc");
    si.append_bytes(frame).expect("append");
    match xid8funcs::snapshot_recv(ctx.mcx(), &mut si) {
        Ok(v) => {
            let img = v.as_bytes();
            assert_eq!(cst, 0, "C errored ({cst}) where Rust accepted; frame={frame:02x?}");
            assert_eq!(
                img.len(),
                coutlen as usize,
                "image length mismatch; frame={frame:02x?} c_img={:02x?} r_img={img:02x?}",
                &cout[..coutlen as usize]
            );
            assert_eq!(
                img,
                &cout[..coutlen as usize],
                "image bytes mismatch; frame={frame:02x?}"
            );
            assert_eq!(si.cursor, ccur as usize, "cursor mismatch; frame={frame:02x?}");
            core::mem::forget(v);
        }
        Err(e) => {
            match cst {
                PGC_ERR_PROTOCOL => assert_eq!(
                    e.sqlstate,
                    types_error::ERRCODE_PROTOCOL_VIOLATION,
                    "err-class mismatch; frame={frame:02x?}"
                ),
                PGC_ERR_BADFORMAT => assert_eq!(
                    e.sqlstate,
                    types_error::ERRCODE_INVALID_BINARY_REPRESENTATION,
                    "err-class mismatch; frame={frame:02x?}"
                ),
                0 => panic!("Rust errored where C accepted; frame={frame:02x?}"),
                other => panic!("unexpected C status {other}; frame={frame:02x?}"),
            }
        }
    }
    core::mem::forget(si);
    core::mem::forget(ctx);
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

#[test]
fn native_differential_snapshot_recv() {
    // interesting u64 planes: invalid (0..3 low-word with epoch 0),
    // first-normal boundary, epoch boundaries, max
    let vals: &[u64] = &[
        0,
        1,
        2,
        3,
        4,
        (1u64 << 32) - 1,
        1u64 << 32,
        (1u64 << 32) + 2,
        (1u64 << 32) + 3,
        u64::MAX - 1,
        u64::MAX,
        5,
        9,
    ];
    // nxip=0 exact frames (the d20n0 plane), full cross product
    for &a in vals {
        for &b in vals {
            replay(&be_frame(0, a, b, &[], 0));
        }
    }
    // trailing-junk plane (d36n0: dlen 36, nxip=0)
    replay(&be_frame(0, 5, 9, &[], 16));
    // nxip=1/2 planes incl. dup, out-of-range, unordered
    for &a in &[4u64, 5, 1 << 32] {
        for &b in &[9u64, (1 << 32) + 3] {
            for &x1 in vals {
                replay(&be_frame(1, a, b, &[x1], 0));
                for &x2 in &[x1, x1.wrapping_add(1), 5, u64::MAX] {
                    replay(&be_frame(2, a, b, &[x1, x2], 0));
                }
            }
        }
    }
    // randomized sweep
    let mut rng = Rng(0xD1B54A32D192ED03);
    for _ in 0..200_000 {
        let r = rng.next();
        let nxip = (r & 3) as u32;
        let band = |v: u64| match v & 3 {
            0 => v & 0xF,                       // tiny / invalid plane
            1 => (v & 0xF) | (1 << 32),         // epoch-1 plane
            2 => v,                             // full random
            _ => u64::MAX - (v & 0xF),          // top plane
        };
        let xmin = band(rng.next());
        let xmax = band(rng.next());
        let xips: Vec<u64> = (0..nxip).map(|_| band(rng.next())).collect();
        let extra = (r >> 8) as usize & 7;
        let f = be_frame(nxip, xmin, xmax, &xips, extra);
        // also truncated variants (protocol-err plane)
        let cut = (r >> 16) as usize % (f.len() + 1);
        replay(&f[..cut]);
        replay(&f);
    }
}
