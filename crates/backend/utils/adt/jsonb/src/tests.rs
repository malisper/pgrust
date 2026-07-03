//! Differential tests vs PostgreSQL 18.3 goldens (fixtures/gen_goldens.py,
//! C-collation UTF8 database). golden_docs payload_hex is the on-disk datum
//! payload captured via pageinspect — the byte-exact serialized-form gate.

use std::sync::Once;

use crate::build::item_to_jsonb_image;
use crate::container::JsonbItem;
use crate::getfield::{self, PathResult};
use crate::io;
use crate::ops;
use mbutils::SetDatabaseEncoding;
use mcx::{Mcx, MemoryContext};
use types_error::SoftErrorContext;
use wchar::PG_UTF8;

fn setup() {
    let _ = SetDatabaseEncoding(PG_UTF8);
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        mbutils::init_seams();
        // The golden database is C collation: memcmp semantics.
        pg_locale_seams::varstr_cmp_locale::set(|_collid, a, b| {
            Ok(varlena::varstrfastcmp_c(a, b))
        });
    });
}

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0, "odd hex: {s}");
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|c| format!("{c:02x}")).collect()
}

fn jsonb_image(mcx: Mcx<'_>, doc: &[u8]) -> Vec<u8> {
    io::jsonb_in(mcx, doc, None)
        .unwrap_or_else(|e| {
            panic!(
                "jsonb_in failed on {:?}: {}",
                String::from_utf8_lossy(doc),
                e.message()
            )
        })
        .expect("hard path returns Some")[..]
        .to_vec()
}

struct DocRow {
    input: Vec<u8>,
    out: Vec<u8>,
    typeof_: String,
    hash: i32,
    hash_ext0: i64,
    hash_ext42: i64,
    payload: Vec<u8>,
}

fn golden_docs() -> Vec<DocRow> {
    include_str!("../fixtures/golden_docs.tsv")
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            DocRow {
                input: unhex(f[0]),
                out: unhex(f[1]),
                typeof_: f[2].to_string(),
                hash: f[3].parse().unwrap(),
                hash_ext0: f[4].parse().unwrap(),
                hash_ext42: f[5].parse().unwrap(),
                payload: unhex(f[6]),
            }
        })
        .collect()
}

#[test]
fn on_disk_bytes_match_c() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    for (i, row) in golden_docs().iter().enumerate() {
        let img = jsonb_image(mcx, &row.input);
        assert_eq!(
            hex(&img[4..]),
            hex(&row.payload),
            "doc {i}: {:?}",
            String::from_utf8_lossy(&row.input)
        );
    }
}

#[test]
fn out_text_matches_c() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    for (i, row) in golden_docs().iter().enumerate() {
        let img = jsonb_image(mcx, &row.input);
        let mut out = io::jsonb_out(mcx, &img[4..]).unwrap()[..].to_vec();
        assert_eq!(out.pop(), Some(0));
        assert_eq!(
            String::from_utf8_lossy(&out),
            String::from_utf8_lossy(&row.out),
            "doc {i}"
        );
    }
}

#[test]
fn typeof_matches_c() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    for row in golden_docs() {
        let img = jsonb_image(mcx, &row.input);
        assert_eq!(io::container_type_name(&img[4..]), row.typeof_);
    }
}

#[test]
fn hash_matches_c() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    for (i, row) in golden_docs().iter().enumerate() {
        let img = jsonb_image(mcx, &row.input);
        let p = &img[4..];
        assert_eq!(ops::jsonb_hash(mcx, p).unwrap() as i32, row.hash, "doc {i} hash");
        assert_eq!(
            ops::jsonb_hash_extended(mcx, p, 0).unwrap() as i64,
            row.hash_ext0,
            "doc {i} hash_ext0"
        );
        assert_eq!(
            ops::jsonb_hash_extended(mcx, p, 42).unwrap() as i64,
            row.hash_ext42,
            "doc {i} hash_ext42"
        );
    }
}

#[test]
fn btree_order_matches_c() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let images: Vec<Vec<u8>> = golden_docs()
        .iter()
        .map(|r| jsonb_image(mcx, &r.input))
        .collect();
    let mut idx: Vec<usize> = (0..images.len()).collect();
    idx.sort_by(|&a, &b| {
        ops::compare_containers(mcx, &images[a][4..], &images[b][4..])
            .unwrap()
            .cmp(&0)
            .then(a.cmp(&b))
    });
    let expected: Vec<usize> = include_str!("../fixtures/golden_order.tsv")
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.parse().unwrap())
        .collect();
    assert_eq!(idx, expected);
}

#[test]
fn pairwise_cmp_matches_c() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let images: Vec<Vec<u8>> = golden_docs()
        .iter()
        .map(|r| jsonb_image(mcx, &r.input))
        .collect();
    for l in include_str!("../fixtures/golden_cmp.tsv").lines() {
        if l.is_empty() {
            continue;
        }
        let f: Vec<&str> = l.split('\t').collect();
        let (a, b): (usize, usize) = (f[0].parse().unwrap(), f[1].parse().unwrap());
        let want: i32 = f[2].parse().unwrap();
        let got = ops::compare_containers(mcx, &images[a][4..], &images[b][4..]).unwrap();
        assert_eq!(got.signum(), want, "cmp({a},{b})");
    }
}

#[test]
fn containment_matches_c() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let images: Vec<Vec<u8>> = golden_docs()
        .iter()
        .map(|r| jsonb_image(mcx, &r.input))
        .collect();
    for l in include_str!("../fixtures/golden_contains.tsv").lines() {
        if l.starts_with('#') || l.is_empty() {
            continue;
        }
        let f: Vec<&str> = l.split('\t').collect();
        let i: usize = f[0].parse().unwrap();
        let probe = jsonb_image(mcx, &unhex(f[1]));
        let contains = ops::jsonb_contains(mcx, &images[i][4..], &probe[4..]).unwrap();
        let contained = ops::jsonb_contains(mcx, &probe[4..], &images[i][4..]).unwrap();
        assert_eq!(contains, f[2] == "t", "doc {i} @> {}", f[1]);
        assert_eq!(contained, f[3] == "t", "doc {i} <@ {}", f[1]);
    }
}

#[test]
fn exists_matches_c() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let images: Vec<Vec<u8>> = golden_docs()
        .iter()
        .map(|r| jsonb_image(mcx, &r.input))
        .collect();
    for l in include_str!("../fixtures/golden_exists.tsv").lines() {
        if l.starts_with('#') || l.is_empty() {
            continue;
        }
        let f: Vec<&str> = l.split('\t').collect();
        let i: usize = f[0].parse().unwrap();
        let key = unhex(f[1]);
        assert_eq!(
            ops::exists_key(&images[i][4..], &key),
            f[2] == "t",
            "doc {i} ? {:?}",
            String::from_utf8_lossy(&key)
        );
    }
}

fn image_out_text(mcx: Mcx<'_>, image: &[u8]) -> Vec<u8> {
    let mut out = io::jsonb_out(mcx, &image[4..]).unwrap()[..].to_vec();
    assert_eq!(out.pop(), Some(0));
    out
}

// The fixture paths are simple unquoted array literals: {a,b, 1}.
fn parse_path(spec: &[u8]) -> Vec<Vec<u8>> {
    let s = std::str::from_utf8(spec).unwrap();
    let inner = s.strip_prefix('{').unwrap().strip_suffix('}').unwrap();
    if inner.is_empty() {
        return Vec::new();
    }
    inner.split(',').map(|e| e.trim().as_bytes().to_vec()).collect()
}

#[test]
fn getfield_matches_c() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let docs = golden_docs();
    let images: Vec<Vec<u8>> = docs.iter().map(|r| jsonb_image(mcx, &r.input)).collect();
    for l in include_str!("../fixtures/golden_getfield.tsv").lines() {
        if l.starts_with('#') || l.is_empty() {
            continue;
        }
        let f: Vec<&str> = l.split('\t').collect();
        let (kind, i, arg) = (f[0], f[1].parse::<usize>().unwrap(), unhex(f[2]));
        let payload = &images[i][4..];
        let (got_jsonb, got_text): (Option<Vec<u8>>, Option<Vec<u8>>) = match kind {
            "k" => (
                getfield::object_field(mcx, payload, &arg)
                    .unwrap()
                    .map(|v| image_out_text(mcx, &v)),
                getfield::object_field_text(mcx, payload, &arg)
                    .unwrap()
                    .map(|t| t.data().to_vec()),
            ),
            "i" => {
                let ix: i32 = std::str::from_utf8(&arg).unwrap().parse().unwrap();
                (
                    getfield::array_element(mcx, payload, ix)
                        .unwrap()
                        .map(|v| image_out_text(mcx, &v)),
                    getfield::array_element_text(mcx, payload, ix)
                        .unwrap()
                        .map(|t| t.data().to_vec()),
                )
            }
            "p" => {
                let path_elems = parse_path(&arg);
                let path: Vec<&[u8]> = path_elems.iter().map(|v| &v[..]).collect();
                let g = |as_text: bool| -> Option<Vec<u8>> {
                    match getfield::get_element(mcx, payload, &path, as_text).unwrap() {
                        PathResult::Null => None,
                        PathResult::Jsonb(v) => Some(image_out_text(mcx, &v)),
                        PathResult::Text(t) => Some(t.data().to_vec()),
                        PathResult::Input => Some(image_out_text(
                            mcx,
                            &item_to_jsonb_image(mcx, JsonbItem::Binary(payload)).unwrap(),
                        )),
                    }
                };
                (g(false), g(true))
            }
            _ => unreachable!(),
        };
        let want_jsonb = (f[3] != "N").then(|| unhex(f[3]));
        let want_text = (f[4] != "N").then(|| unhex(f[4]));
        let show = |v: &Option<Vec<u8>>| {
            v.as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_else(|| "NULL".into())
        };
        assert_eq!(
            show(&got_jsonb),
            show(&want_jsonb),
            "doc {i} {kind} -> {:?}",
            String::from_utf8_lossy(&arg)
        );
        assert_eq!(
            show(&got_text),
            show(&want_text),
            "doc {i} {kind} ->> {:?}",
            String::from_utf8_lossy(&arg)
        );
    }
}

#[test]
fn unicode_zero_rejected_22p05() {
    setup();
    let ctx = MemoryContext::new("t");
    let err = io::jsonb_in(ctx.mcx(), b"\"a\\u0000b\"", None).expect_err("must fail");
    assert_eq!(err.message(), "unsupported Unicode escape sequence");
    assert_eq!(err.detail().unwrap(), "\\u0000 cannot be converted to text.");
}

#[test]
fn surrogate_errors_match_c() {
    setup();
    let ctx = MemoryContext::new("t");
    let err = io::jsonb_in(ctx.mcx(), b"\"\\ude00\"", None).expect_err("lone low surrogate");
    assert_eq!(
        err.detail().unwrap(),
        "Unicode low surrogate must follow a high surrogate."
    );
    let err = io::jsonb_in(ctx.mcx(), b"\"\\ud83d\\ud83d\"", None).expect_err("two highs");
    assert_eq!(
        err.detail().unwrap(),
        "Unicode high surrogate must not follow a high surrogate."
    );
    let err = io::jsonb_in(ctx.mcx(), b"\"\\ud83dx\"", None).expect_err("unpaired high");
    assert_eq!(
        err.detail().unwrap(),
        "Unicode low surrogate must follow a high surrogate."
    );
}

#[test]
fn soft_error_absorbs() {
    setup();
    let ctx = MemoryContext::new("t");
    let mut esc = SoftErrorContext::new(true);
    let r = io::jsonb_in(ctx.mcx(), b"{bad", Some(&mut esc)).unwrap();
    assert!(r.is_none());
    assert!(esc.error_occurred());
}

#[test]
fn recv_send_round_trip() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let img = jsonb_image(mcx, b"{\"a\": [1, 2.50], \"b\": \"x\"}");
    let sent = io::jsonb_send(mcx, &img[4..]).unwrap();
    let wire = sent.data().to_vec();
    assert_eq!(wire[0], 1);
    let mut buf = stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&wire).unwrap();
    let img2 = io::jsonb_recv(mcx, &mut buf).unwrap();
    assert_eq!(hex(&img[..]), hex(&img2[..]));
}
