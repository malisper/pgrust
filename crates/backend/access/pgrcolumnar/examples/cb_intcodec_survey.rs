//! Integer/timestamp codec survey (intcodec lane, phase 1 — evidence only).
//!
//! For each fixed-width (int/date/timestamp) column of a pgrcolumnar part:
//! current on-disk footprint (chunk payload + metadata), encoding/codec
//! histogram, measured decode cost of the CURRENT path (decode_granule:
//! frame decompress + widen), and OFFLINE simulated sizes for candidate
//! codecs applied per granule (the reader's random-access unit):
//!
//!   delta   : d[0]=v[0]; d[i]=v[i]-v[i-1]; FOR over deltas at byte width
//!             {1,2,4,8}; plain / +LZ4 / +ZSTD-1
//!   ddelta  : second-order deltas, same byte-width FOR; plain / +LZ4
//!   t64     : per-64-row block FOR + bit-packing (T64-style crop);
//!             plain / +LZ4
//!
//! Compressed candidates carry the v6 8-byte granule frame header; plain
//! candidates carry a 24-byte per-granule meta (first val, base, width).
//! Sizes are exact encodes of the real column data, not entropy estimates.
//!
//! Usage: cb_intcodec_survey <part-file> <ncols> [names-file] [col ...]
//!   names-file: one column name per line, in attribute order.

use pgrcolumnar::format::{Codec, Encoding};
use pgrcolumnar::reader::Part;
use std::time::Instant;

fn bytes_width(range: u128) -> usize {
    if range <= u8::MAX as u128 {
        1
    } else if range <= u16::MAX as u128 {
        2
    } else if range <= u32::MAX as u128 {
        4
    } else {
        8
    }
}

// FOR-encode `vals` at minimal byte width. Returns the packed image.
fn for_pack(vals: &[i64]) -> Vec<u8> {
    if vals.is_empty() {
        return Vec::new();
    }
    let min = *vals.iter().min().unwrap();
    let max = *vals.iter().max().unwrap();
    let w = bytes_width((max as i128 - min as i128) as u128);
    let mut out = Vec::with_capacity(vals.len() * w);
    for &v in vals {
        let d = (v as i128 - min as i128) as u128;
        out.extend_from_slice(&d.to_le_bytes()[..w]);
    }
    out
}

fn deltas(vals: &[i64]) -> Vec<i64> {
    let mut d = Vec::with_capacity(vals.len());
    let mut prev = 0i64;
    for (i, &v) in vals.iter().enumerate() {
        d.push(if i == 0 { 0 } else { v.wrapping_sub(prev) });
        prev = v;
    }
    d
}

// T64-style: per 64-value block, FOR + bit-pack to the block's bit range.
// Size model: 8B block min + 1B bits + packed bits (exact packing).
fn t64_pack(vals: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for blk in vals.chunks(64) {
        let min = *blk.iter().min().unwrap();
        let max = *blk.iter().max().unwrap();
        let range = (max as i128 - min as i128) as u128;
        let bits = (128 - range.leading_zeros()) as usize;
        out.extend_from_slice(&min.to_le_bytes());
        out.push(bits as u8);
        let mut acc: u128 = 0;
        let mut nacc = 0usize;
        for &v in blk {
            let d = (v as i128 - min as i128) as u128;
            acc |= d << nacc;
            nacc += bits;
            while nacc >= 8 {
                out.push(acc as u8);
                acc >>= 8;
                nacc -= 8;
            }
        }
        if nacc > 0 {
            out.push(acc as u8);
        }
    }
    out
}

const FRAME_HDR: usize = 8; // v6 granule frame: u32 raw_len | u32 comp_len
const PLAIN_META: usize = 24; // first val + base + width bookkeeping

struct Cand {
    plain: u64,
    lz4: u64,
    zstd: u64,
}

fn cand(image: &[u8]) -> Cand {
    let lz4 = lz4_flex::compress(image).len();
    let zstd = zstd::bulk::compress(image, 1).map(|v| v.len()).unwrap_or(usize::MAX);
    Cand {
        plain: (image.len() + PLAIN_META) as u64,
        lz4: (lz4.min(image.len()) + FRAME_HDR + PLAIN_META) as u64,
        zstd: (zstd.min(image.len()) + FRAME_HDR + PLAIN_META) as u64,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("usage: cb_intcodec_survey <part> <ncols> [names] [col ...]");
    let ncols: usize = args.next().expect("ncols required").parse().expect("bad ncols");
    let names: Vec<String> = args
        .next()
        .map(|f| {
            std::fs::read_to_string(f)
                .expect("names file")
                .lines()
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let only: Vec<usize> = args.map(|a| a.parse().expect("bad col index")).collect();

    let part = Part::open(&path, ncols).expect("open failed").expect("no committed footer");
    println!(
        "part {path}: {} rows, {} RGs, {} cols",
        part.total_rows(),
        part.rgs.len(),
        part.ncols
    );
    println!(
        "col\tname\trows\tenc\tcodec\tplain_b\tstored_b\tdecode_ms\tdelta\tdelta_lz4\tdelta_zstd\tdd\tdd_lz4\tt64\tt64_lz4"
    );

    let mut out: Vec<datum::Datum> = Vec::new();
    let mut dict: Vec<datum::Datum> = Vec::new();
    let mut arena: Vec<u64> = Vec::new();

    for c in 0..ncols {
        if !only.is_empty() && !only.contains(&c) {
            continue;
        }
        // Skip text columns.
        let is_int = (0..part.rgs.len()).all(|rg| {
            matches!(
                part.chunk(rg, c).hdr.encoding,
                Encoding::Raw | Encoding::For | Encoding::Const | Encoding::DeltaFor
            )
        });
        if !is_int {
            continue;
        }

        let mut rows: u64 = 0;
        let mut plain_b: u64 = 0;
        let mut stored_b: u64 = 0;
        let mut enc_hist: std::collections::BTreeMap<String, u32> = Default::default();
        let mut codec_hist: std::collections::BTreeMap<&'static str, u32> = Default::default();
        let mut sizes = [0u64; 7]; // delta, delta_lz4, delta_zstd, dd, dd_lz4, t64, t64_lz4
        let mut decode_ns: u128 = 0;

        for rg in 0..part.rgs.len() {
            let cv = part.chunk(rg, c);
            let nrows = part.rgs[rg].nrows as usize;
            rows += nrows as u64;
            let enc = format!(
                "{:?}:{}",
                cv.hdr.encoding,
                if cv.hdr.encoding == Encoding::Const { 0 } else { cv.hdr.width }
            );
            *enc_hist.entry(enc).or_default() += 1;
            *codec_hist
                .entry(match cv.hdr.frame_codec() {
                    Codec::None => "none",
                    Codec::Lz4 => "lz4",
                    Codec::Zstd => "zstd",
                })
                .or_default() += 1;
            plain_b += (nrows * 8) as u64;
            stored_b += cv.hdr.payload_len;

            let ng = cv.hdr.ngranules as usize;
            // Timed current-path decode (3 reps min, includes decompress).
            let mut best = u128::MAX;
            for _ in 0..3 {
                let t = Instant::now();
                for g in 0..ng {
                    dict.clear();
                    cv.decode_granule(g, &mut out, &mut dict, &mut arena);
                    std::hint::black_box(out.last());
                }
                best = best.min(t.elapsed().as_nanos());
            }
            decode_ns += best;

            // Candidate simulation per granule.
            for g in 0..ng {
                dict.clear();
                cv.decode_granule(g, &mut out, &mut dict, &mut arena);
                let vals: Vec<i64> = out.iter().map(|d| d.as_i64()).collect();
                if vals.iter().all(|&v| v == vals[0]) {
                    // Const-collapsible: every candidate stores meta only.
                    for s in sizes.iter_mut() {
                        *s += PLAIN_META as u64;
                    }
                    continue;
                }
                let d1 = deltas(&vals);
                let img = for_pack(&d1);
                let cd = cand(&img);
                sizes[0] += cd.plain;
                sizes[1] += cd.lz4;
                sizes[2] += cd.zstd;
                let d2 = deltas(&d1);
                let img2 = for_pack(&d2);
                let cdd = cand(&img2);
                sizes[3] += cdd.plain;
                sizes[4] += cdd.lz4;
                let t = t64_pack(&vals);
                let ct = cand(&t);
                sizes[5] += ct.plain;
                sizes[6] += ct.lz4;
            }
        }

        let name = names.get(c).map(|s| s.as_str()).unwrap_or("?");
        let ench: Vec<String> = enc_hist.iter().map(|(k, v)| format!("{k}x{v}")).collect();
        let cdh: Vec<String> = codec_hist.iter().map(|(k, v)| format!("{k}x{v}")).collect();
        println!(
            "{c}\t{name}\t{rows}\t{}\t{}\t{plain_b}\t{stored_b}\t{:.1}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            ench.join(","),
            cdh.join(","),
            decode_ns as f64 / 1e6,
            sizes[0],
            sizes[1],
            sizes[2],
            sizes[3],
            sizes[4],
            sizes[5],
            sizes[6],
        );
    }
}
