//! Differential SAMPLER for adt_cash::cash_numeric (money->numeric) and
//! adt_cash::numeric_cash (numeric->money), oracled against a REAL
//! PostgreSQL 18.3 (docker container pg183-lane0b). Orchestrated by
//! ../../cash_numeric_sample.sh:
//!
//!   gen <dir> [N1] [N2]  - write plane1.txt (i64 cents/line) and
//!                          plane2.txt (numeric literal/line); deterministic
//!                          xorshift64* seed => reproducible corpus.
//!   check1 <tsv>         - read "t \t pg_numeric_text" rows produced by PG
//!                          from plane1.txt (money built via int8 arithmetic,
//!                          cast ::numeric::text under lc_monetary=C) and
//!                          compare against cash_numeric(t) -> numeric_out.
//!   check2 <tsv>         - read "s \t result" rows where result is PG's
//!                          (s::numeric::money)::numeric::text or
//!                          "ERR:<sqlstate>"; compare against
//!                          numeric_in(s) -> numeric_cash -> cash_numeric ->
//!                          numeric_out (or the pgrust error sqlstate).
//!
//! Any mismatch prints the exact input and both sides, and the process exits
//! nonzero at the end (comparisons are exact text / exact sqlstate).

use std::io::{BufRead, BufWriter, Write};

fn pgrust_cash_numeric_text(t: i64) -> String {
    let img = adt_cash::cash_numeric(t).expect("cash_numeric is infallible under C locale");
    // fc-wrapper plane (same convention as the *_diff drivers): route the
    // same input through fc_cash_numeric / fc_numeric_cash on a native
    // LocalFcinfo frame and assert wrapper ≡ core.
    {
        use datum::Datum;
        let ctx = mcx::MemoryContext::new("cashnum-fc");
        let mut f = types_fmgr::LocalFcinfo::<1>::new(0);
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = datum::NullableDatum::value(Datum::from_i64(t));
        let d = adt_cash::builtins::fc_cash_numeric(None, &mut f)
            .expect("fc_cash_numeric is infallible under C locale");
        let wbytes = unsafe {
            let p = d.as_usize() as *const u8;
            std::slice::from_raw_parts(p, img.as_bytes().len())
        };
        assert!(wbytes == img.as_bytes(), "fc_cash_numeric vs core t={t}");
        // fc_numeric_cash: numeric image arg (by-ref datum) -> cents.
        let mut f2 = types_fmgr::LocalFcinfo::<1>::new(0);
        unsafe { f2.set_result_mcx(ctx.mcx()) };
        f2.args[0] =
            datum::NullableDatum::value(Datum::from_usize(img.as_bytes().as_ptr() as usize));
        match adt_cash::builtins::fc_numeric_cash(None, &mut f2) {
            Ok(d2) => assert!(d2.as_i64() == t, "fc_numeric_cash roundtrip t={t}"),
            Err(e) => panic!("fc_numeric_cash errored on roundtrip t={t}: {}", e.message),
        }
    }
    let mut out = Vec::new();
    adt_numeric::numeric_out_into(img.num(), &mut out);
    String::from_utf8(out).unwrap()
}

/// numeric_in(s) -> numeric_cash -> cents -> cash_numeric -> text, or
/// "ERR:<sqlstate>" mirroring the plane-2 PG oracle verdict format.
fn pgrust_plane2(s: &str) -> String {
    let parsed = match adt_numeric::numeric_in(s, -1, None) {
        Ok(img) => img.expect("hard-error numeric_in never returns None"),
        Err(e) => return err_verdict(&e),
    };
    match adt_cash::numeric_cash(parsed.num()) {
        Ok(cents) => pgrust_cash_numeric_text(cents),
        Err(e) => err_verdict(&e),
    }
}

fn err_verdict(e: &types_error::PgError) -> String {
    let c = types_error::unpack_sqlstate(e.sqlstate());
    format!("ERR:{}", std::str::from_utf8(&c).unwrap())
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545f4914f6cdd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn gen_plane1(path: &str, n_total: u64) {
    let mut w = BufWriter::new(std::fs::File::create(path).unwrap());
    let mut count: u64 = 0;
    let emit = |w: &mut BufWriter<std::fs::File>, t: i64| {
        writeln!(w, "{t}").unwrap();
    };
    // 1) exhaustive |t| <= 200000
    for t in -200_000i64..=200_000 {
        emit(&mut w, t);
        count += 1;
    }
    // 2) powers of 10 +- neighborhood (offsets -100..=100), both signs
    let mut p: i64 = 1;
    for _ in 0..19 {
        for off in -100i64..=100 {
            emit(&mut w, p.wrapping_add(off));
            emit(&mut w, (-p).wrapping_add(off));
            count += 2;
        }
        p = p.saturating_mul(10);
    }
    // 3) exact i64 bounds (+-92233720368547758.08/.07) and 100-neighborhoods
    for k in 0..=100i64 {
        emit(&mut w, i64::MIN + k);
        emit(&mut w, i64::MAX - k);
        count += 2;
    }
    // 4) random spread over the full i64 range up to n_total
    let mut rng = Rng(0x1234_5678_9abc_def1);
    while count < n_total {
        emit(&mut w, rng.next() as i64);
        count += 1;
    }
    w.flush().unwrap();
    eprintln!("plane1: {count} inputs -> {path}");
}

fn gen_plane2(path: &str, n_total: u64) {
    let mut w = BufWriter::new(std::fs::File::create(path).unwrap());
    let mut rng = Rng(0xfeed_beef_cafe_f00d);
    let mut count: u64 = 0;
    let emit = |w: &mut BufWriter<std::fs::File>, s: &str| {
        writeln!(w, "{s}").unwrap();
    };

    // Curated seeds: money bounds (cents = i64 bounds after *100 + round),
    // rounding half-edges at the bound, specials.
    let bound_frac = [
        ".07", ".08", ".075", ".074", ".0749", ".0751", ".0749999999", ".0750000001", ".085",
        ".084999999", ".0850000001", ".08499999999999999999", ".00", "",
    ];
    for f in bound_frac {
        emit(&mut w, &format!("92233720368547758{f}"));
        emit(&mut w, &format!("-92233720368547758{f}"));
        count += 2;
    }
    for s in [
        "NaN", "nan", "Infinity", "-Infinity", "+infinity", "inf", "-inf", "0", "-0", "0.000",
        "0.005", "-0.005", "0.004999999999999999999999", "1.005", "-1.005", "2.675",
        "92233720368547759", "-92233720368547759", "92233720368547758", "-92233720368547758",
        "1e30", "-1e30", "1e-30", "9.9999e17", "1.7014118346046923e38",
    ] {
        emit(&mut w, s);
        count += 1;
    }

    let mut buf = String::new();
    while count < n_total {
        buf.clear();
        let cat = rng.below(100);
        if rng.below(2) == 0 {
            buf.push('-');
        }
        if cat < 20 {
            // random integer, 1..19 digits
            let d = 1 + rng.below(19);
            push_digits(&mut rng, &mut buf, d);
        } else if cat < 55 {
            // integer.frac with 1-4 decimal places (covers exact + rounding)
            let d = 1 + rng.below(18);
            push_digits(&mut rng, &mut buf, d);
            buf.push('.');
            let f = 1 + rng.below(4);
            push_digits_lead0(&mut rng, &mut buf, f);
        } else if cat < 75 {
            // rounding-edge: dollars + .xx5 / .xxx5 / .xx4999... / .xx5000...1
            let d = 1 + rng.below(17);
            push_digits(&mut rng, &mut buf, d);
            buf.push('.');
            push_digits_lead0(&mut rng, &mut buf, 2);
            match rng.below(4) {
                0 => buf.push('5'),
                1 => {
                    buf.push('4');
                    for _ in 0..rng.below(20) {
                        buf.push('9');
                    }
                }
                2 => {
                    buf.push('5');
                    for _ in 0..rng.below(20) {
                        buf.push('0');
                    }
                    buf.push('1');
                }
                _ => {
                    buf.push('0');
                    buf.push('5');
                }
            }
        } else if cat < 85 {
            // huge (mostly out of money range): 18..28 digit integers
            let d = 18 + rng.below(11);
            push_digits(&mut rng, &mut buf, d);
            if rng.below(2) == 0 {
                buf.push('.');
                let n = 1 + rng.below(3);
                push_digits_lead0(&mut rng, &mut buf, n);
            }
        } else if cat < 93 {
            // tiny: 0.000...0d with up to 30 leading zeros
            buf.push_str("0.");
            for _ in 0..rng.below(30) {
                buf.push('0');
            }
            let n = 1 + rng.below(4);
            push_digits_lead0(&mut rng, &mut buf, n);
        } else {
            // scientific notation
            let n = 1 + rng.below(3);
            push_digits(&mut rng, &mut buf, n);
            if rng.below(2) == 0 {
                buf.push('.');
                let n = 1 + rng.below(6);
                push_digits_lead0(&mut rng, &mut buf, n);
            }
            buf.push('e');
            let e = rng.below(71) as i64 - 35;
            buf.push_str(&e.to_string());
        }
        emit(&mut w, &buf);
        count += 1;
    }
    w.flush().unwrap();
    eprintln!("plane2: {count} inputs -> {path}");
}

fn push_digits(rng: &mut Rng, buf: &mut String, n: u64) {
    buf.push((b'1' + rng.below(9) as u8) as char);
    for _ in 1..n {
        buf.push((b'0' + rng.below(10) as u8) as char);
    }
}

fn push_digits_lead0(rng: &mut Rng, buf: &mut String, n: u64) {
    for _ in 0..n {
        buf.push((b'0' + rng.below(10) as u8) as char);
    }
}

fn check1(tsv: &str) -> (u64, u64) {
    let f = std::fs::File::open(tsv).unwrap_or_else(|e| panic!("open {tsv}: {e}"));
    let mut rows = 0u64;
    let mut bad = 0u64;
    for line in std::io::BufReader::new(f).lines() {
        let line = line.unwrap();
        let (ts, pg) = line.split_once('\t').expect("plane1 row must be t<TAB>text");
        let t: i64 = ts.parse().unwrap();
        let ours = pgrust_cash_numeric_text(t);
        if ours != pg {
            bad += 1;
            println!("PLANE1 MISMATCH t={t}: pgrust={ours} pg={pg}");
        }
        rows += 1;
    }
    (rows, bad)
}

fn check2(tsv: &str) -> (u64, u64) {
    let f = std::fs::File::open(tsv).unwrap_or_else(|e| panic!("open {tsv}: {e}"));
    let mut rows = 0u64;
    let mut bad = 0u64;
    for line in std::io::BufReader::new(f).lines() {
        let line = line.unwrap();
        let (s, pg) = line.split_once('\t').expect("plane2 row must be s<TAB>result");
        let ours = pgrust_plane2(s);
        if ours != pg {
            bad += 1;
            println!("PLANE2 MISMATCH s={s}: pgrust={ours} pg={pg}");
        }
        rows += 1;
    }
    (rows, bad)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("gen") => {
            let dir = args.get(2).expect("gen <dir> [N1] [N2]");
            let n1: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10_000_000);
            let n2: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
            gen_plane1(&format!("{dir}/plane1.txt"), n1);
            gen_plane2(&format!("{dir}/plane2.txt"), n2);
        }
        Some("check1") => {
            let (rows, bad) = check1(args.get(2).expect("check1 <tsv>"));
            println!("plane1 (cash_numeric): {rows} rows compared, {bad} mismatches");
            std::process::exit(if bad == 0 { 0 } else { 1 });
        }
        Some("check2") => {
            let (rows, bad) = check2(args.get(2).expect("check2 <tsv>"));
            println!("plane2 (numeric_cash): {rows} rows compared, {bad} mismatches");
            std::process::exit(if bad == 0 { 0 } else { 1 });
        }
        _ => {
            eprintln!("usage: cash_numeric_sample gen <dir> [N1] [N2] | check1 <tsv> | check2 <tsv>");
            std::process::exit(2);
        }
    }
}
