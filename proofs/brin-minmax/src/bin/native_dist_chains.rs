//! Native differential for the byte-chain distances whose fully-symbolic
//! Kani harnesses wall the local 450s budget under load (w2-brin
//! 2026-07-30): macaddr8 (both solvers walled) and uuid (16-deep chain);
//! macaddr and inet v4/v6 ride along as free cross-checks of their proved
//! harnesses. Census-grade only — recorded as `tested(differential)`,
//! never `proved` (TRIAGE verdict discipline); the walled harnesses stay
//! staged fleet-bound.
//!
//! Same shipped wrappers (dispatch-table lookup) vs the SAME vendored C
//! (pg_brin_multi_dist.c, build.rs cc link, -fwrapv). Pairs are drawn
//! random then lex-ordered (the C caller contract fence the harnesses
//! assume); inet pairs are ordered by masked first address, mirroring the
//! harness fence.
//!
//! Run: cargo run --release --bin native_dist_chains

use proof_brin_minmax as _; // bundle the vendored C archive (build.rs cc link)

use datum::Datum;
use proof_support::fcinfo::call2;
use types_fmgr::PGFunction;

extern "C" {
    fn pg_dist_uuid(d1: *const u8, d2: *const u8) -> f64;
    fn pg_dist_macaddr(a: *const u8, b: *const u8) -> f64;
    fn pg_dist_macaddr8(a: *const u8, b: *const u8) -> f64;
    fn pg_dist_inet(
        fam_a: u8,
        bits_a: u8,
        addr_a: *const u8,
        fam_b: u8,
        bits_b: u8,
        addr_b: *const u8,
    ) -> f64;
}

fn builtin(oid: u32) -> PGFunction {
    let t = brin_minmax_multi::MINMAX_MULTI_BUILTINS;
    t.iter().find(|e| e.foid == oid).expect("oid registered").func
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
    fn bytes<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        for chunk in out.chunks_mut(8) {
            let w = self.next().to_ne_bytes();
            chunk.copy_from_slice(&w[..chunk.len()]);
        }
        out
    }
}

fn fixed_pair_check<const N: usize>(
    name: &str,
    fc: PGFunction,
    cfn: unsafe extern "C" fn(*const u8, *const u8) -> f64,
    iters: u64,
    rng: &mut Rng,
) -> u64 {
    let mut diffs = 0u64;
    let mut run = |a: [u8; N], b: [u8; N]| {
        let (a, b) = if a <= b { (a, b) } else { (b, a) }; // lex fence
        let r = call2(
            fc,
            Datum::from_usize(a.as_ptr() as usize),
            Datum::from_usize(b.as_ptr() as usize),
        )
        .expect("distance infallible")
        .as_f64();
        let c = unsafe { cfn(a.as_ptr(), b.as_ptr()) };
        if r.to_bits() != c.to_bits() {
            diffs += 1;
            eprintln!("DIFF {name} a={a:02x?} b={b:02x?} rust={r:?} c={c:?}");
        }
    };
    // boundary spots: all-0, all-ff, single-byte steps at each position
    let zero = [0u8; N];
    let ff = [0xffu8; N];
    run(zero, zero);
    run(zero, ff);
    run(ff, ff);
    for i in 0..N {
        let mut hi = zero;
        hi[i] = 0xff;
        run(zero, hi);
        let mut one = zero;
        one[i] = 1;
        run(one, hi);
    }
    for _ in 0..iters {
        run(rng.bytes::<N>(), rng.bytes::<N>());
    }
    println!("{name}: {} checked, {diffs} diffs", iters + 3 + 2 * N as u64);
    diffs
}

#[repr(C, align(8))]
struct InetImg([u8; 24]);

fn inet_img<const LEN: usize>(family: u8, bits: u8, addr: &[u8; LEN]) -> InetImg {
    let mut img = InetImg([0u8; 24]);
    let total: u32 = (4 + 2 + LEN) as u32;
    img.0[0..4].copy_from_slice(&(total << 2).to_ne_bytes());
    img.0[4] = family;
    img.0[5] = bits;
    img.0[6..6 + LEN].copy_from_slice(addr);
    img
}

fn masked<const LEN: usize>(addr: &[u8; LEN], bits: u8) -> [u8; LEN] {
    let mut out = *addr;
    for (i, o) in out.iter_mut().enumerate() {
        let nbits = (bits as i32 - (i as i32) * 8).max(0);
        if nbits < 8 {
            *o &= (0xFFu32 << (8 - nbits)) as u8;
        }
    }
    out
}

fn inet_check<const LEN: usize>(
    name: &str,
    fam: u8,
    maxbits: u8,
    iters: u64,
    rng: &mut Rng,
) -> u64 {
    let fc = builtin(4636);
    let mut diffs = 0u64;
    for _ in 0..iters {
        let mut aa: [u8; LEN] = rng.bytes::<LEN>();
        let mut ab: [u8; LEN] = rng.bytes::<LEN>();
        let mut ba = (rng.next() % (maxbits as u64 + 1)) as u8;
        let mut bb = (rng.next() % (maxbits as u64 + 1)) as u8;
        if masked(&aa, ba) > masked(&ab, bb) {
            core::mem::swap(&mut aa, &mut ab);
            core::mem::swap(&mut ba, &mut bb);
        }
        let ia = inet_img::<LEN>(fam, ba, &aa);
        let ib = inet_img::<LEN>(fam, bb, &ab);
        let r = call2(
            fc,
            Datum::from_usize(ia.0.as_ptr() as usize),
            Datum::from_usize(ib.0.as_ptr() as usize),
        )
        .expect("dist_inet infallible")
        .as_f64();
        let mut ca = [0u8; 16];
        let mut cb = [0u8; 16];
        ca[..LEN].copy_from_slice(&aa);
        cb[..LEN].copy_from_slice(&ab);
        let c = unsafe { pg_dist_inet(fam, ba, ca.as_ptr(), fam, bb, cb.as_ptr()) };
        if r.to_bits() != c.to_bits() {
            diffs += 1;
            eprintln!("DIFF {name} a={aa:02x?}/{ba} b={ab:02x?}/{bb} rust={r:?} c={c:?}");
        }
    }
    println!("{name}: {iters} checked, {diffs} diffs");
    diffs
}

fn main() {
    let mut rng = Rng(0xA5A5_1DE4_2026_0730);
    let mut total = 0u64;
    total += fixed_pair_check::<8>(
        "dist_macaddr8",
        builtin(4635),
        pg_dist_macaddr8,
        4_000_000,
        &mut rng,
    );
    total += fixed_pair_check::<16>("dist_uuid", builtin(4628), pg_dist_uuid, 4_000_000, &mut rng);
    total += fixed_pair_check::<6>(
        "dist_macaddr",
        builtin(4634),
        pg_dist_macaddr,
        2_000_000,
        &mut rng,
    );
    total += inet_check::<4>("dist_inet_v4", 2, 32, 2_000_000, &mut rng);
    total += inet_check::<16>("dist_inet_v6", 3, 128, 2_000_000, &mut rng);
    if total == 0 {
        println!("VERDICT: byte-chain distance parity holds (0 diffs)");
    } else {
        println!("VERDICT: UNEXPECTED — {total} diffs, investigate");
        std::process::exit(1);
    }
}
