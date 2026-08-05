//! Tie-order equivalence against verbatim C: compiles the vendored
//! lib/sort_template.h driver (cref/qsort_ref.c) with the system C compiler
//! at test runtime and checks that pg_qsort produces the bit-identical
//! permutation — including equal-key order — over randomized dup-heavy
//! inputs and the algorithm's size boundaries (n<7 insertion sort, n>7
//! med3, n>40 med3-of-9, presorted early-out).
//!
//! Runtime compilation (not build.rs/cc) keeps the pg_qsort crate dep-free
//! and keeps `cargo check --target aarch64-unknown-linux-gnu` from needing a
//! cross C toolchain. Both dev laptops and the fleet have cc.

use std::io::Write;
use std::process::{Command, Stdio};

fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 33
}

fn cases() -> Vec<Vec<i32>> {
    let mut out = Vec::new();
    let mut rng = 0xc0ffee_u64;
    for n in [
        0usize, 1, 2, 3, 5, 6, 7, 8, 13, 20, 39, 40, 41, 47, 64, 100, 341, 1000, 4096, 20000,
    ] {
        for &kmod in &[2u64, 3, 7, 16, 100, 1_000_000] {
            out.push((0..n).map(|_| (lcg(&mut rng) % kmod) as i32).collect());
        }
        out.push((0..n).map(|i| (i / 3) as i32).collect()); // presorted with ties
        out.push((0..n).rev().map(|i| (i / 3) as i32).collect()); // reverse
        out.push(vec![42i32; n]); // all-equal
        // Organ-pipe and sawtooth (classic B&M adversary shapes).
        out.push((0..n).map(|i| std::cmp::min(i, n - i.min(n)) as i32).collect());
        out.push((0..n).map(|i| (i % 13) as i32).collect());
    }
    out
}

#[test]
fn tie_order_matches_verbatim_c() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("cref");
    let exe = std::env::temp_dir().join(format!("pg_qsort_cref_{}", std::process::id()));
    let cc = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
    let status = Command::new(&cc)
        .arg("-O2")
        .arg("-o")
        .arg(&exe)
        .arg(dir.join("qsort_ref.c"))
        .arg(format!("-I{}", dir.display()))
        .status()
        .expect("system C compiler (cc) must be available for the tie-order gate");
    assert!(status.success(), "compiling cref/qsort_ref.c failed");

    for (ci, keys) in cases().iter().enumerate() {
        // C side.
        let mut input = format!("{}\n", keys.len());
        for k in keys {
            input.push_str(&format!("{}\n", k));
        }
        let mut child = Command::new(&exe)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        let c_perm: Vec<i32> = String::from_utf8(out.stdout)
            .unwrap()
            .split_ascii_whitespace()
            .map(|s| s.parse().unwrap())
            .collect();

        // Rust side: same element shape, same comparator.
        let mut v: Vec<(i32, i32)> = keys
            .iter()
            .enumerate()
            .map(|(i, &k)| (k, i as i32))
            .collect();
        pg_qsort::pg_qsort(&mut v, |a, b| (a.0 > b.0) as i32 - (a.0 < b.0) as i32);
        let r_perm: Vec<i32> = v.iter().map(|e| e.1).collect();

        assert_eq!(
            c_perm, r_perm,
            "tie-order divergence vs verbatim C on case {} (n={})",
            ci,
            keys.len()
        );
    }
    let _ = std::fs::remove_file(&exe);
}
