// Native ground-truth census: shipped adt_json::escape_json over all 256 single bytes.
fn main() {
    let ctx = mcx::MemoryContext::new("n");
    for b in 0u16..256 {
        let s = [b as u8];
        let mut buf = stringinfo::StringInfo::new_in(ctx.mcx()).unwrap();
        adt_json::escape_json(&mut buf, &s).unwrap();
        let hex: String = buf.as_bytes().iter().map(|x| format!("{:02x}", x)).collect();
        println!("R {:02x} {}", b, hex);
    }
}
