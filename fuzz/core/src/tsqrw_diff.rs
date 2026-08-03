//! tsqrw_diff: differential fuzz driver — shipped Rust `adt_tsquery_rewrite`
//! vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_tsqrw_io.c + csrc/tsqrw/*). Crate under test:
//! crates/backend/utils/adt/tsquery_rewrite.
//!
//! ORACLE PLANE (p1-lanef's plan, fuzz/README-TODO-tsqrw_diff.md): tsquery
//! VARLENA IMAGES, never text. The three tsquery arguments are built
//! RUST-side (adt_tsquery_core::io::tsquery_in_core over three sub-texts
//! of the fuzz input) and the SAME image bytes feed both sides, so the C
//! PARSER — and with it the text-search dictionary/GUC cache — never
//! enters this target (parser parity is tsquery_core_diff's plane over
//! the same corpus). Result images compared byte-for-byte in the
//! zero-header convention + Ok/Err verdict (the only in-scope ereport is
//! qtn2qt's 54000 "tsquery is too large", unreachable under the input
//! cap; the verdict plane still carries it).
//!
//! Rust sides compared per exec:
//!   - fc_tsquery_rewrite (oid 3684) — the shipped wrapper, full lib.rs
//!     surface (prepared_tree/findsubquery/finish_tree incl. the empty-
//!     query/ex copy path and the empty-substitution deletion path);
//!   - the pure findsubquery path (qt2qtn -> qtn_ternary -> qtn_sort ->
//!     findsubquery -> qtn_binary -> qtn2qt), replicating the wrapper body
//!     over the same images — asserted equal to BOTH the fc result and
//!     the C oracle, so a wrapper-vs-core drift is its own failure mode.
//!
//! Input layout: [selector(ignored beyond routing)][s1][s2][text...]:
//! s1/s2 split the text into three parts (query, ex, subs); each part
//! parses standard-mode softly, any parse failure leaves the domain
//! (tsquery_core_diff owns parser divergence). Empty parts are IN-domain:
//! they exercise the size()==0 copy/deletion arms.
//!
//! SKIPPED rows (NAMED CARVE, adopted from p1-lanef's adjudication):
//! fc_tsquery_rewrite_query (oid 3685, lib.rs:234-328) — SPI executor
//! state (SPI_connect/prepare/cursor over a user query); the C
//! counterpart marks the same boundary with #include "executor/spi.h"
//! (tsquery_rewrite.c:18). Recorded as the crate's exception row.
//!
//! Stack-depth seam: same class + cap as tsquery_core_diff (module header
//! there); total input <= 2048 bytes.

use adt_tsquery_core::io::tsquery_in_core;
use adt_tsquery_core::parse::build_query_image;
use adt_tsquery_core::util::{qt2qtn, qtn2qt, qtn_binary, qtn_sort, qtn_ternary};
use adt_tsquery_rewrite::findsubquery;
use adt_tsvector_core::query::TsQueryRef;
use datum::{Datum, NullableDatum};
use types_error::{PgResult, SoftErrorContext};
use types_fmgr::{LocalFcinfo, PGFunction, PackedVarlena};

extern "C" {
    fn pg_diff_tsquery_rewrite(
        img_query: *const u8,
        len_query: i32,
        img_ex: *const u8,
        len_ex: i32,
        img_subs: *const u8,
        len_subs: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
}

/// Same stack-depth-seam cap as tsquery_core_diff.
const MAX_INPUT: usize = 2048;
/// Rewrite never grows past |query| + |subs| * matches; parse of <=2KiB
/// text keeps every image far under this.
const OUT_CAP: usize = 1 << 20;

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — verbatim from
// tsquery_core_diff.rs).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over non-null args; returns (result, isnull flag).
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// Payload bytes of a varlena result Datum (image bytes AFTER the header).
fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena,
    // read before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// Copy a zero-header image and stamp the 4B uncompressed varlena header.
fn stamp_header(img: &[u8]) -> Vec<u8> {
    let len = img.len() as u32;
    #[cfg(target_endian = "little")]
    let word = len << 2;
    #[cfg(target_endian = "big")]
    let word = len & 0x3FFF_FFFF;
    let mut v = img.to_vec();
    v[..4].copy_from_slice(&word.to_ne_bytes());
    v
}

fn tsq_ref(img: &[u8]) -> TsQueryRef<'_> {
    TsQueryRef { payload: &img[4..] }
}

/// Parse one sub-text standard-mode softly; None = out of domain.
fn parse_soft<'m>(m: mcx::Mcx<'m>, text: &[u8]) -> Option<mcx::PgVec<'m, u8>> {
    let mut esc = SoftErrorContext::new(false);
    match tsquery_in_core(m, text, Some(&mut esc)) {
        Ok(Some(img)) => Some(img),
        _ => None,
    }
}

/// The pure-path replica of fc_tsquery_rewrite's body (lib.rs): returns the
/// result image in the zero-header convention.
fn pure_rewrite<'m>(
    m: mcx::Mcx<'m>,
    query: &[u8],
    ex: &[u8],
    subs: &[u8],
) -> PgResult<mcx::PgVec<'m, u8>> {
    let (q, e, s) = (tsq_ref(query), tsq_ref(ex), tsq_ref(subs));
    if q.size() == 0 || e.size() == 0 {
        let mut img = mcx::vec_with_capacity_in(m, query.len())?;
        mcx::vec_append_bytes(&mut img, query)?;
        return Ok(img);
    }
    let mut tree = qt2qtn(m, q, 0)?;
    qtn_ternary(&mut tree)?;
    qtn_sort(&mut tree)?;
    let mut qex = qt2qtn(m, e, 0)?;
    qtn_ternary(&mut qex)?;
    qtn_sort(&mut qex)?;
    let qsubs = if s.size() != 0 { Some(qt2qtn(m, s, 0)?) } else { None };
    match findsubquery(m, tree, &qex, qsubs.as_ref())? {
        Some(mut t) => {
            qtn_binary(m, &mut t)?;
            qtn2qt(m, &t)
        }
        None => build_query_image(m, &[], &[]),
    }
}

// ---------------------------------------------------------------------------
// Dispatch (single arm)
// ---------------------------------------------------------------------------

/// dofindsubquery crosses the CHECK_FOR_INTERRUPTS seam (its upstream body
/// does the same call); the fuzz process has no interrupt sources, so the
/// production semantics ARE the no-op Ok. set() is install-once and panics
/// on a second install (another module may have won the race) — guarded.
fn setup() {
    use std::sync::Once;
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        if !postgres_seams::check_for_interrupts::is_installed() {
            let _ = std::panic::catch_unwind(|| {
                postgres_seams::check_for_interrupts::set(|| Ok(()));
            });
        }
    });
}

pub fn tsqrw_diff(data: &[u8]) {
    setup();
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if data.len() > MAX_INPUT {
        return;
    }
    let Some((&[_sel, s1, s2], text)) = data.split_first_chunk::<3>() else {
        return;
    };
    if text.contains(&0) || core::str::from_utf8(text).is_err() {
        return; // cstring + pg_verify_mbstr boundary (tsquery_core_diff header)
    }
    let c1 = s1 as usize % (text.len() + 1);
    let c2 = c1 + (s2 as usize % (text.len() - c1 + 1));
    let (tq, tex, tsubs) = (&text[..c1], &text[c1..c2], &text[c2..]);

    let cx = mcx::MemoryContext::new("tsqrw_fuzz");
    let m = cx.mcx();
    let (Some(iq), Some(iex), Some(isubs)) =
        (parse_soft(m, tq), parse_soft(m, tex), parse_soft(m, tsubs))
    else {
        return;
    };

    // C oracle.
    let mut obuf = vec![0u8; OUT_CAP];
    let mut olen = 0i32;
    // SAFETY: images/obuf live; caps passed.
    let cst = unsafe {
        pg_diff_tsquery_rewrite(
            iq.as_ptr(),
            iq.len() as i32,
            iex.as_ptr(),
            iex.len() as i32,
            isubs.as_ptr(),
            isubs.len() as i32,
            obuf.as_mut_ptr(),
            OUT_CAP as i32,
            &mut olen,
        )
    };
    let cerr = unsafe { pg_diff_errcode_get() };

    // Shipped fc wrapper.
    let (aq, aex, asubs) = (stamp_header(&iq), stamp_header(&iex), stamp_header(&isubs));
    let (r, _) = fc_call::<3>(
        adt_tsquery_rewrite::fc_tsquery_rewrite,
        m,
        [
            Datum::from_usize(aq.as_ptr() as usize),
            Datum::from_usize(aex.as_ptr() as usize),
            Datum::from_usize(asubs.as_ptr() as usize),
        ],
    );

    // Pure path.
    let pure = pure_rewrite(m, &iq, &iex, &isubs);

    match (r, pure) {
        (Ok(d), Ok(p)) => {
            let fc_img = read_varlena_data(d);
            assert!(
                cst == 0 && &obuf[4..olen as usize] == fc_img,
                "tsquery_rewrite DIVERGENCE q={:?} ex={:?} subs={:?}: C=(st {cst} err {cerr} len {olen}) fc len {}",
                String::from_utf8_lossy(tq),
                String::from_utf8_lossy(tex),
                String::from_utf8_lossy(tsubs),
                fc_img.len(),
            );
            assert!(
                &p.as_slice()[4..] == fc_img,
                "tsquery_rewrite fc-vs-pure DIVERGENCE q={:?} ex={:?} subs={:?}",
                String::from_utf8_lossy(tq),
                String::from_utf8_lossy(tex),
                String::from_utf8_lossy(tsubs),
            );
        }
        (Err(e), Err(pe)) => {
            assert!(
                cst == 1 && e.sqlstate == pe.sqlstate,
                "tsquery_rewrite error-shape DIVERGENCE q={:?} ex={:?} subs={:?}: C=(st {cst} err {cerr}) fc {:?} pure {:?}",
                String::from_utf8_lossy(tq),
                String::from_utf8_lossy(tex),
                String::from_utf8_lossy(tsubs),
                e.sqlstate,
                pe.sqlstate,
            );
        }
        (r, p) => panic!(
            "tsquery_rewrite fc/pure verdict DIVERGENCE q={:?} ex={:?} subs={:?}: fc ok={} pure ok={}",
            String::from_utf8_lossy(tq),
            String::from_utf8_lossy(tex),
            String::from_utf8_lossy(tsubs),
            r.is_ok(),
            p.is_ok(),
        ),
    };
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        // 32MiB stack: deep-nesting seeds recurse the QTN web, which fits
        // the fuzz binary's 8MiB main stack but not the 2MiB default
        // test-thread stack under debug frame sizes (post boundary-audit
        // merge the PgResult-threaded frames are deeper still; the 16MiB
        // sibling convention in tsquery_core_diff, doubled for margin).
        std::thread::Builder::new()
            .stack_size(32 << 20)
            .spawn(|| {
                let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/tsqrw_diff");
                let mut n = 0;
                for e in std::fs::read_dir(dir).expect("corpus/tsqrw_diff missing") {
                    let p = e.unwrap().path();
                    if p.is_file() {
                        tsqrw_diff(&std::fs::read(&p).unwrap());
                        n += 1;
                    }
                }
                assert!(n >= 30, "expected >=30 seeds, found {n}");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// Known-answer shapes through the full diff driver (any C-vs-Rust
    /// disagreement asserts inside). Layout: [sel][s1][s2][text]; s1/s2
    /// split text into (query, ex, subs).
    #[test]
    fn arms_smoke() {
        // leaf substitution: q="cat & dog" ex="cat" subs="rat" -> rat & dog.
        tsqrw_diff(b"\x00\x09\x03cat & dogcatrat");
        // deletion (empty subs): q="cat & dog" ex="cat" subs="".
        tsqrw_diff(b"\x00\x09\x03cat & dogcat");
        // whole-tree void -> empty result: q="cat" ex="cat" subs="".
        tsqrw_diff(b"\x00\x03\x03catcat");
        // AND-subset match: q="a & b & c" ex="a & b" subs="x".
        tsqrw_diff(b"\x00\x09\x05a & b & ca & bx");
        // empty ex -> copy of query.
        tsqrw_diff(b"\x00\x03\x00catrat");
        // empty query -> copy (empty image).
        tsqrw_diff(b"\x00\x00\x03catrat");
        // OR-node substitution with NOT in subs.
        tsqrw_diff(b"\x00\x09\x09cat | dogcat | dog!rat");
        // phrase node (no subset matching on OP_PHRASE, exact only).
        tsqrw_diff(b"\x00\x0b\x0bcat <-> dogcat <-> dograt");
        // parse failure leaves the domain silently (still must not panic).
        tsqrw_diff(b"\x00\x05\x03cat &catrat");
    }
}

#[cfg(test)]
mod tie_probe {
    use super::*;

    fn drive(q: &str, ex: &str, subs: &str) -> Result<(), String> {
        let mut input = vec![0u8, q.len() as u8, ex.len() as u8];
        input.extend_from_slice(q.as_bytes());
        input.extend_from_slice(ex.as_bytes());
        input.extend_from_slice(subs.as_bytes());
        std::panic::catch_unwind(|| tsqrw_diff(&input))
            .map_err(|e| format!("{q:?} -> {:?}", e.downcast_ref::<String>()))
    }

    /// Payload-distinct sort ties (same lexeme, different weight/prefix =>
    /// qtnode_compare==0) through QTNSort. Was a CONFIRMED pgrust-bug (116
    /// diverging shapes, docker-18.3-adjudicated, FINDINGS-qsort-tie.md);
    /// FIXED by the shipped qtn_sort index proxy over the canonical
    /// crates/_support/pg_qsort::pg_qsort_arg (tsquery_core/src/util.rs,
    /// task #135) — this test now asserts C==Rust across the whole probe
    /// grid, byte-exact against the vendored sort_template oracle.
    #[test]
    fn tie_order_probe() {
        let mut fails = Vec::new();
        let words = ["b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"];
        for n in [5usize, 6, 7, 8, 9, 12, 13] {
            for pos in 0..n.saturating_sub(1) {
                for (t1, t2) in [("a:A", "a:B"), ("a:B", "a:A"), ("a:*", "a"), ("a", "a:CD")] {
                    let mut kids: Vec<String> =
                        words[..n - 2].iter().map(|s| s.to_string()).collect();
                    kids.insert(pos, t1.to_string());
                    kids.insert(pos + 1, t2.to_string());
                    // also a shuffled placement: tie pair split apart
                    let q1 = kids.join(" | ");
                    if let Err(e) = drive(&q1, "q", "r") {
                        fails.push(e);
                    }
                    let mut split = kids.clone();
                    let last = split.remove(pos + 1);
                    split.push(last);
                    let q2 = split.join(" | ");
                    if let Err(e) = drive(&q2, "q", "r") {
                        fails.push(e);
                    }
                }
            }
        }
        for f in &fails[..fails.len().min(2)] {
            eprintln!("TIE DIVERGENCE: {f}");
        }
        assert!(fails.is_empty(), "{} tie divergences", fails.len());
    }
}

#[cfg(test)]
mod tie_probe2 {
    use super::*;

    /// Decode one witness: print C image vs Rust image as text via
    /// tsquery_out_core for docker adjudication (FINDINGS-qsort-tie.md).
    /// Kept #[ignore]: a manual decode aid, not an assertion (the parity
    /// assertion is tie_order_probe).
    #[test]
    #[ignore]
    fn tie_case_detail() {
        setup();
        let _serial = crate::c_oracle_serial();
        let q = "b | c | d | a:A | e | f | a:B";
        let cx = mcx::MemoryContext::new("probe");
        let m = cx.mcx();
        let iq = parse_soft(m, q.as_bytes()).unwrap();
        let iex = parse_soft(m, b"q").unwrap();
        let isubs = parse_soft(m, b"r").unwrap();
        let mut obuf = vec![0u8; OUT_CAP];
        let mut olen = 0i32;
        let cst = unsafe {
            pg_diff_tsquery_rewrite(
                iq.as_ptr(), iq.len() as i32,
                iex.as_ptr(), iex.len() as i32,
                isubs.as_ptr(), isubs.len() as i32,
                obuf.as_mut_ptr(), OUT_CAP as i32, &mut olen,
            )
        };
        assert_eq!(cst, 0);
        let cimg = &obuf[..olen as usize];
        let rimg = pure_rewrite(m, &iq, &iex, &isubs).unwrap();
        let ctext = adt_tsquery_core::io::tsquery_out_core(m, tsq_ref(cimg)).unwrap();
        let rtext = adt_tsquery_core::io::tsquery_out_core(m, tsq_ref(&rimg)).unwrap();
        eprintln!("C   : {}", String::from_utf8_lossy(&ctext[..ctext.len()-1]));
        eprintln!("Rust: {}", String::from_utf8_lossy(&rtext[..rtext.len()-1]));
    }
}
