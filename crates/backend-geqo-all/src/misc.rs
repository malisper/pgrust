//! `geqo_misc.c` — misc printout and debug stuff (`#ifdef GEQO_DEBUG`).
//!
//! These routines are compiled only under `GEQO_DEBUG` in C and exist purely to
//! print pool/edge-table state for debugging. The C `FILE *` / `fprintf` sink
//! becomes any [`core::fmt::Write`] sink (the crate is `#![no_std]`, so there is
//! no `stdout`); the *computation* (averages, best/worst/mean selection) is
//! ported 1:1. They are not reached by the production `geqo()` path.

use crate::erx::Edge;
use crate::pool::Pool;
use core::fmt::Write;

/// `avg_pool(pool)` — average `worth` over the pool. Divides by `size` before
/// summing (not after) to avoid overflow when the pool holds many `DBL_MAX`.
fn avg_pool(pool: &Pool) -> f64 {
    if pool.size <= 0 {
        panic!("pool_size is zero");
    }

    let mut cumulative = 0.0;
    for i in 0..pool.size as usize {
        cumulative += pool.data[i].worth / pool.size as f64;
    }

    cumulative
}

/// `print_pool(fp, pool, start, stop)` — print pool entries `[start, stop)`
/// (clamped to valid bounds) with their tours and worth.
pub fn print_pool<W: Write>(fp: &mut W, pool: &Pool, mut start: i32, mut stop: i32) {
    /* be extra careful that start and stop are valid inputs */
    if start < 0 {
        start = 0;
    }
    if stop > pool.size {
        stop = pool.size;
    }

    if start + stop > pool.size {
        start = 0;
        stop = pool.size;
    }

    for i in start as usize..stop as usize {
        let _ = write!(fp, "{})\t", i);
        for j in 0..pool.string_length as usize {
            let _ = write!(fp, "{} ", pool.data[i].string[j]);
        }
        let _ = writeln!(fp, "{}", pool.data[i].worth);
    }
}

/// `print_gen(fp, pool, generation)` — printout of best/worst/mean/avg worth for
/// a generation.
pub fn print_gen<W: Write>(fp: &mut W, pool: &Pool, generation: i32) {
    /* Get index to lowest ranking gene in population. */
    /* Use 2nd to last since last is buffer. */
    let lowest = if pool.size > 1 { pool.size - 2 } else { 0 };

    let _ = writeln!(
        fp,
        "{:5} | Best: {}  Worst: {}  Mean: {}  Avg: {}",
        generation,
        pool.data[0].worth,
        pool.data[lowest as usize].worth,
        pool.data[(pool.size / 2) as usize].worth,
        avg_pool(pool)
    );
}

/// `print_edge_table(fp, edge_table, num_gene)` — printout of the ERX edge
/// table.
pub fn print_edge_table<W: Write>(fp: &mut W, edge_table: &[Edge], num_gene: i32) {
    let _ = writeln!(fp, "\nEDGE TABLE");

    for i in 1..=num_gene as usize {
        let _ = write!(fp, "{} :", i);
        for j in 0..edge_table[i].unused_edges as usize {
            let _ = write!(fp, " {}", edge_table[i].edge_list[j]);
        }
        let _ = writeln!(fp);
    }

    let _ = writeln!(fp);
}
