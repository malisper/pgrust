// Exhaustive native differential: shipped Rust escape_json vs vendored C
// escape_json_with_len for all len-2 (65536) and len-3 (16.7M) inputs.
extern "C" {
    fn pg_run_escape_json_with_len(s: *const u8, len: i32, out: *mut u8) -> i32;
}
fn main() {
    let ctx = mcx::MemoryContext::new("n");
    let mut cout = [0u8; 64];
    let mut diverge = 0u64;
    // len 2
    for a in 0..=255u8 { for b in 0..=255u8 {
        let s = [a, b];
        let mut buf = stringinfo::StringInfo::new_in(ctx.mcx()).unwrap();
        adt_json::escape_json(&mut buf, &s).unwrap();
        let n = unsafe { pg_run_escape_json_with_len(s.as_ptr(), 2, cout.as_mut_ptr()) } as usize;
        if buf.as_bytes() != &cout[..n] { diverge += 1;
            println!("LEN2 DIVERGE in={:02x}{:02x} rust={:02x?} c={:02x?}", a, b, buf.as_bytes(), &cout[..n]); }
    }}
    println!("len2 done, diverge={}", diverge);
    // len 3
    for a in 0..=255u8 { for b in 0..=255u8 { for c in 0..=255u8 {
        let s = [a, b, c];
        let mut buf = stringinfo::StringInfo::new_in(ctx.mcx()).unwrap();
        adt_json::escape_json(&mut buf, &s).unwrap();
        let n = unsafe { pg_run_escape_json_with_len(s.as_ptr(), 3, cout.as_mut_ptr()) } as usize;
        if buf.as_bytes() != &cout[..n] { diverge += 1;
            if diverge < 20 { println!("LEN3 DIVERGE in={:02x}{:02x}{:02x} rust={:02x?} c={:02x?}", a, b, c, buf.as_bytes(), &cout[..n]); } }
    }}}
    println!("len3 done, total diverge={}", diverge);
}
