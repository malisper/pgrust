// Randomized differential: len4 (50M samples) + long strings (100k x up to 4KB)
// exercising the C ESCAPE_JSON_FLUSH_AFTER=512 path.
extern "C" {
    fn pg_run_escape_json_with_len(s: *const u8, len: i32, out: *mut u8) -> i32;
}
struct Rng(u64);
impl Rng { fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 } }
fn main() {
    let ctx = mcx::MemoryContext::new("n");
    let mut rng = Rng(0x9e3779b97f4a7c15);
    let mut cout = vec![0u8; 64];
    let mut diverge = 0u64;
    for _ in 0..50_000_000u64 {
        let r = rng.next();
        let s = r.to_le_bytes();
        let s = &s[..4];
        let mut buf = stringinfo::StringInfo::new_in(ctx.mcx()).unwrap();
        adt_json::escape_json(&mut buf, s).unwrap();
        let n = unsafe { pg_run_escape_json_with_len(s.as_ptr(), 4, cout.as_mut_ptr()) } as usize;
        if buf.as_bytes() != &cout[..n] { diverge += 1;
            if diverge < 5 { println!("LEN4 DIVERGE in={:02x?} rust={:02x?} c={:02x?}", s, buf.as_bytes(), &cout[..n]); } }
    }
    println!("len4 50M random done, diverge={}", diverge);
    // long strings: heavy in special chars to stress per-byte/flush interleave
    let mut cout = vec![0u8; 40960];
    for it in 0..100_000u64 {
        let len = 1 + (rng.next() % 4096) as usize;
        let mut s = vec![0u8; len];
        for b in s.iter_mut() {
            let r = rng.next();
            *b = if r % 5 == 0 { (r >> 8) as u8 % 0x40 } else { (r >> 8) as u8 };
        }
        let mut buf = stringinfo::StringInfo::new_in(ctx.mcx()).unwrap();
        adt_json::escape_json(&mut buf, &s).unwrap();
        let n = unsafe { pg_run_escape_json_with_len(s.as_ptr(), len as i32, cout.as_mut_ptr()) } as usize;
        if buf.as_bytes() != &cout[..n] { diverge += 1; println!("LONG DIVERGE iter={} len={}", it, len); }
    }
    println!("long done, total diverge={}", diverge);
}
