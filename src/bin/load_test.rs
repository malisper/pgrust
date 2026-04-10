use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use pgrust::pgrust::database::Database;
use pgrust::executor::{StatementResult, Value};
use pgrust::ClientId;

struct LoadTestConfig {
    num_writer_threads: usize,
    num_reader_threads: usize,
    inserts_per_writer: usize,
    selects_per_reader: usize,
    pool_size: usize,
}

struct ThreadResult {
    role: &'static str,
    thread_id: usize,
    ops: usize,
    elapsed: Duration,
    errors: usize,
}

fn main() {
    let config = LoadTestConfig {
        num_writer_threads: 4,
        num_reader_threads: 4,
        inserts_per_writer: 200,
        selects_per_reader: 500,
        pool_size: 128,
    };

    let base_dir = std::env::temp_dir().join(format!(
        "pgrust_loadtest_{}_{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base_dir);

    println!("=== PGRUST LOAD TEST ===");
    println!("Base dir: {}", base_dir.display());
    println!(
        "Writers: {}  ({} inserts each)",
        config.num_writer_threads, config.inserts_per_writer
    );
    println!(
        "Readers: {}  ({} selects each)",
        config.num_reader_threads, config.selects_per_reader
    );
    println!("Buffer pool size: {}", config.pool_size);
    println!();

    let db = Database::open(&base_dir, config.pool_size).unwrap();

    // Schema setup
    db.execute(
        0,
        "create table events (id int4 not null, thread_id int4 not null, seq int4 not null)",
    )
    .unwrap();

    // Seed a few rows so readers always have something to scan.
    for i in 1..=5 {
        db.execute(
            0,
            &format!("insert into events (id, thread_id, seq) values ({i}, 0, {i})"),
        )
        .unwrap();
    }

    let total_errors = Arc::new(AtomicU64::new(0));
    let total_writer_ops = Arc::new(AtomicU64::new(0));
    let total_reader_ops = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = Vec::new();

    // Writer threads
    for t in 0..config.num_writer_threads {
        let db = db.clone();
        let total_errors = total_errors.clone();
        let total_writer_ops = total_writer_ops.clone();
        let n = config.inserts_per_writer;
        handles.push(thread::spawn(move || {
            let client_id = (t + 1) as ClientId;
            let mut errors = 0usize;
            let thread_start = Instant::now();

            for i in 0..n {
                let id = t as i32 * 10000 + i as i32;
                let sql = format!(
                    "insert into events (id, thread_id, seq) values ({id}, {t}, {i})"
                );
                match db.execute(client_id, &sql) {
                    Ok(_) => {
                        total_writer_ops.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        errors += 1;
                        total_errors.fetch_add(1, Ordering::Relaxed);
                        if errors <= 3 {
                            eprintln!("  writer-{t} error #{errors}: {e:?}");
                        }
                    }
                }
            }

            ThreadResult {
                role: "writer",
                thread_id: t,
                ops: n - errors,
                elapsed: thread_start.elapsed(),
                errors,
            }
        }));
    }

    // Reader threads
    for t in 0..config.num_reader_threads {
        let db = db.clone();
        let total_errors = total_errors.clone();
        let total_reader_ops = total_reader_ops.clone();
        let n = config.selects_per_reader;
        handles.push(thread::spawn(move || {
            let client_id = (t + 100) as ClientId;
            let mut errors = 0usize;
            let thread_start = Instant::now();

            for _ in 0..n {
                match db.execute(client_id, "select count(*) from events") {
                    Ok(StatementResult::Query { rows, .. }) => {
                        total_reader_ops.fetch_add(1, Ordering::Relaxed);
                        if let Some(row) = rows.first() {
                            if let Some(Value::Int32(count)) = row.first() {
                                assert!(*count >= 5, "expected at least 5 rows, got {count}");
                            }
                        }
                    }
                    Ok(_) => {
                        errors += 1;
                        total_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        errors += 1;
                        total_errors.fetch_add(1, Ordering::Relaxed);
                        if errors <= 3 {
                            eprintln!("  reader-{t} error #{errors}: {e:?}");
                        }
                    }
                }
            }

            ThreadResult {
                role: "reader",
                thread_id: t,
                ops: n - errors,
                elapsed: thread_start.elapsed(),
                errors,
            }
        }));
    }

    let results: Vec<ThreadResult> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let wall_time = start.elapsed();

    println!("--- Per-thread results ---");
    println!(
        "{:<10} {:<6} {:>8} {:>8} {:>12} {:>10}",
        "role", "tid", "ops", "errors", "elapsed_ms", "ops/sec"
    );
    for r in &results {
        let ops_per_sec = if r.elapsed.as_secs_f64() > 0.0 {
            r.ops as f64 / r.elapsed.as_secs_f64()
        } else {
            0.0
        };
        println!(
            "{:<10} {:<6} {:>8} {:>8} {:>12.1} {:>10.0}",
            r.role,
            r.thread_id,
            r.ops,
            r.errors,
            r.elapsed.as_secs_f64() * 1000.0,
            ops_per_sec,
        );
    }

    let total_w = total_writer_ops.load(Ordering::Relaxed);
    let total_r = total_reader_ops.load(Ordering::Relaxed);
    let total_e = total_errors.load(Ordering::Relaxed);
    let total_ops = total_w + total_r;

    println!();
    println!("--- Aggregate ---");
    println!("Wall time:       {:.1} ms", wall_time.as_secs_f64() * 1000.0);
    println!("Total inserts:   {total_w}");
    println!("Total selects:   {total_r}");
    println!("Total errors:    {total_e}");
    println!(
        "Throughput:      {:.0} ops/sec (wall)",
        total_ops as f64 / wall_time.as_secs_f64()
    );

    // Verify final row count
    let expected_inserts =
        5 + config.num_writer_threads * config.inserts_per_writer - total_e as usize;
    match db.execute(0, "select count(*) from events").unwrap() {
        StatementResult::Query { rows, .. } => {
            let count = match &rows[0][0] {
                Value::Int32(n) => *n,
                other => panic!("unexpected value: {other:?}"),
            };
            println!();
            println!("--- Consistency check ---");
            println!("Expected rows:   {expected_inserts}");
            println!("Actual rows:     {count}");
            if count == expected_inserts as i32 {
                println!("PASS");
            } else {
                println!("FAIL: row count mismatch!");
                std::process::exit(1);
            }
        }
        other => {
            eprintln!("Unexpected result from count: {other:?}");
            std::process::exit(1);
        }
    }

    // Buffer pool stats
    let stats = db.pool.usage_stats();
    println!();
    println!("--- Buffer pool stats ---");
    println!("Shared hits:     {}", stats.shared_hit);
    println!("Shared reads:    {}", stats.shared_read);
    println!("Shared written:  {}", stats.shared_written);

    let _ = std::fs::remove_dir_all(&base_dir);
}
