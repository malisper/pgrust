use mcx::{Allocator, MemoryContext};
use std::{alloc::Layout, time::Instant};
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let size: usize = args[1].parse().unwrap();
    let cap: usize = args[2].parse().unwrap();
    let iters: usize = args[3].parse().unwrap();
    let parent = MemoryContext::new_bump("parent");
    let mut ctx = parent.new_child_bump_with_max_block_size("probe", cap);
    let layout = Layout::from_size_align(size, 8).unwrap();
    let start = Instant::now();
    for _ in 0..iters / 65536 {
        let mcx = ctx.mcx();
        for _ in 0..65536 {
            let p = Allocator::allocate(&mcx, layout).unwrap();
            // SAFETY: every measured allocation has at least one writable byte.
            unsafe { (p.as_ptr() as *mut u8).write_volatile(1); }
            let mut address = p.as_ptr() as *mut u8 as usize;
            // SAFETY: the empty barrier preserves the pointer without touching memory.
            unsafe { std::arch::asm!("/* {0} */", inout(reg) address, options(nomem, nostack, preserves_flags)); }
            let _ = address;
        }
        ctx.reset();
    }
    println!("ns_per_alloc={:.6}", start.elapsed().as_nanos() as f64 / (iters / 65536 * 65536) as f64);
}
