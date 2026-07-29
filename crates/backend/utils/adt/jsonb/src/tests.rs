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

// Non-allocating core for proofs/jsonb-probe cmp family: the fixed-stack walk
// must agree with the allocating walk everywhere it engages, and the deep
// fallback must preserve unbounded-nesting behavior.
#[test]
fn fixed_cmp_core_matches_allocating_path() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let images: Vec<Vec<u8>> = golden_docs()
        .iter()
        .map(|r| jsonb_image(mcx, &r.input))
        .collect();
    for a in 0..images.len() {
        for b in 0..images.len() {
            let alloc = ops::compare_containers(mcx, &images[a][4..], &images[b][4..]).unwrap();
            let fixed = ops::compare_containers_fixed::<{ ops::CMP_FIXED_DEPTH }>(
                &images[a][4..],
                &images[b][4..],
            )
            .expect("golden docs are within the fixed depth cap")
            .unwrap();
            assert_eq!(fixed, alloc, "cmp({a},{b})");
        }
    }

    // Depth > CMP_FIXED_DEPTH: fixed core abstains (None), the public entry
    // point still answers via the allocating fallback.
    let deep = |inner: &str| {
        let depth = ops::CMP_FIXED_DEPTH + 8;
        let doc = format!("{}{}{}", "[".repeat(depth), inner, "]".repeat(depth));
        jsonb_image(mcx, doc.as_bytes())
    };
    let (d1, d2) = (deep("1"), deep("2"));
    assert!(ops::compare_containers_fixed::<{ ops::CMP_FIXED_DEPTH }>(&d1[4..], &d2[4..])
        .is_none());
    assert_eq!(ops::compare_containers(mcx, &d1[4..], &d1[4..]).unwrap(), 0);
    assert!(ops::compare_containers(mcx, &d1[4..], &d2[4..]).unwrap() < 0);
    assert!(ops::compare_containers(mcx, &d2[4..], &d1[4..]).unwrap() > 0);
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

// Bad version byte is a clean C-parity error (elog ERROR, XX000), not a
// panic: the byte is client-reachable (binary param / COPY BINARY).
#[test]
fn recv_bad_version_is_clean_error() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut buf = stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&[0x02]).unwrap();
    let err = io::jsonb_recv(mcx, &mut buf).unwrap_err();
    assert!(err.message().contains("unsupported jsonb version number 2"), "{err:?}");
}

// On-disk byte identity of the mutation family: golden_mutations.tsv carries
// the pageinspect-captured datum payloads of C 18.3 evaluating each case.
#[test]
fn mutations_match_c_on_disk() {
    setup();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let data = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/golden_mutations.tsv"
    ))
    .unwrap();
    for line in data.lines().filter(|l| !l.starts_with('#') && !l.is_empty()) {
        let cols: Vec<&str> = line.split('\t').collect();
        let (op, target, a1, a2, a3, expected) = (
            cols[0],
            unhex(cols[1]),
            unhex(cols[2]),
            unhex(cols[3]),
            cols[4],
            unhex(cols[5]),
        );
        let image = jsonb_image(mcx, &target);
        let payload: &[u8] = mcx::slice_in(mcx, &image[4..]).unwrap().leak();
        let path: Vec<Option<&[u8]>> = if matches!(op, "del_keys" | "del_path" | "set" | "insert")
        {
            a1.split(|b| *b == b',')
                .map(|e| Some(mcx::slice_in(mcx, e).unwrap().leak() as &[u8]))
                .collect()
        } else {
            Vec::new()
        };
        let newval_image;
        let newval = if matches!(op, "set" | "insert") {
            newval_image = jsonb_image(mcx, &a2);
            let p: &[u8] = mcx::slice_in(mcx, &newval_image[4..]).unwrap().leak();
            Some(match crate::io::extract_scalar(p) {
                Some(v) => v,
                None => JsonbItem::Binary(p),
            })
        } else {
            None
        };
        let flag = a3 == "true";
        let result = match op {
            "concat" => {
                let other = jsonb_image(mcx, &a1);
                let op2: &[u8] = mcx::slice_in(mcx, &other[4..]).unwrap().leak();
                crate::mutate::concat(mcx, payload, op2)
            }
            "del_key" => crate::mutate::delete_key(mcx, payload, &a1),
            "del_idx" => crate::mutate::delete_idx(
                mcx,
                payload,
                std::str::from_utf8(&a1).unwrap().parse().unwrap(),
            ),
            "del_keys" => {
                let keys: Vec<&[u8]> = path.iter().map(|p| p.unwrap()).collect();
                crate::mutate::delete_keys(mcx, payload, &keys)
            }
            "del_path" => crate::mutate::set_path(
                mcx,
                payload,
                &crate::mutate::SetPathArgs {
                    path: &path,
                    newval: None,
                    op_type: crate::mutate::JB_PATH_DELETE,
                },
            ),
            "set" => crate::mutate::set_path(
                mcx,
                payload,
                &crate::mutate::SetPathArgs {
                    path: &path,
                    newval,
                    op_type: if flag {
                        crate::mutate::JB_PATH_CREATE
                    } else {
                        crate::mutate::JB_PATH_REPLACE
                    },
                },
            ),
            "insert" => crate::mutate::set_path(
                mcx,
                payload,
                &crate::mutate::SetPathArgs {
                    path: &path,
                    newval,
                    op_type: if flag {
                        crate::mutate::JB_PATH_INSERT_AFTER
                    } else {
                        crate::mutate::JB_PATH_INSERT_BEFORE
                    },
                },
            ),
            other => panic!("unknown op {other}"),
        }
        .unwrap_or_else(|e| {
            panic!(
                "{op} on {:?} failed: {}",
                String::from_utf8_lossy(&target),
                e.message()
            )
        });
        assert_eq!(
            hex(&result[4..]),
            hex(&expected),
            "{op} {} {} {} {}",
            String::from_utf8_lossy(&target),
            String::from_utf8_lossy(&a1),
            String::from_utf8_lossy(&a2),
            a3,
        );
    }
}

#[test]
fn gin_jsonpath_extraction_shapes() {
    setup();
    let ctx = MemoryContext::new_bump("test");
    let mcx = ctx.mcx();
    let jp = |s: &[u8]| {
        adt_jsonpath::path::jsonpath_in(mcx, s, None)
            .unwrap()
            .expect("valid jsonpath")
    };
    let key_of = |d: datum::Datum| {
        let p = d.as_usize() as *const u8;
        unsafe {
            let len = types_tuple::varatt::varsize_4b(p);
            std::slice::from_raw_parts(p.add(4), len - 4).to_vec()
        }
    };

    use crate::gin::*;
    use gin_vocab::{JSP_GIN_AND, JSP_GIN_ENTRY, JSP_GIN_OR};

    // lax '$.tag == "x"': AND(key tag, OR(x-as-key, x-as-value)).
    let image = jp(b"$.tag == \"x\"");
    let (entries, ops) =
        extract_jsp_query(mcx, &image[4..], JsonbJsonpathPredicateStrategyNumber, false).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(
        ops.iter().map(|o| (o.kind, o.val)).collect::<Vec<_>>(),
        vec![
            (JSP_GIN_AND, 2),
            (JSP_GIN_ENTRY, 0),
            (JSP_GIN_OR, 2),
            (JSP_GIN_ENTRY, 1),
            (JSP_GIN_ENTRY, 2)
        ]
    );
    assert_eq!(key_of(entries[0]), b"\x01tag");
    assert_eq!(key_of(entries[1]), b"\x01x");
    assert_eq!(key_of(entries[2]), b"\x05x");

    // strict '$.tag == "x"': AND(key tag, x-as-value).
    let image = jp(b"strict $.tag == \"x\"");
    let (entries, ops) =
        extract_jsp_query(mcx, &image[4..], JsonbJsonpathPredicateStrategyNumber, false).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(ops[0].kind, JSP_GIN_AND);
    assert_eq!(key_of(entries[1]), b"\x05x");

    // '$.a != 1' is not extractable: full-scan signal.
    let image = jp(b"$.a != 1");
    let (entries, ops) =
        extract_jsp_query(mcx, &image[4..], JsonbJsonpathPredicateStrategyNumber, false).unwrap();
    assert!(entries.is_empty() && ops.is_empty());

    // path_ops '$.a.b == 5': one hash-chain entry.
    let image = jp(b"$.a.b == 5");
    let (entries, ops) =
        extract_jsp_query(mcx, &image[4..], JsonbJsonpathPredicateStrategyNumber, true).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(ops.len(), 1);
    // Same chain as gin_extract_jsonb_path over {"a": {"b": 5}}.
    let doc = jsonb_image(mcx, b"{\"a\": {\"b\": 5}}");
    let doc_entries = gin_extract_jsonb_path(mcx, &doc[4..]).unwrap();
    assert_eq!(doc_entries.len(), 1);
    assert_eq!(entries[0].as_usize(), doc_entries[0].as_usize());

    // path_ops EXISTS ('$.a') extracts nothing.
    let image = jp(b"$.a");
    let (entries, _) =
        extract_jsp_query(mcx, &image[4..], JsonbJsonpathExistsStrategyNumber, true).unwrap();
    assert!(entries.is_empty());

    // Plain EXISTS ('$.a.b', statement of the 2nd kind) extracts nothing
    // in jsonb_ops either — C skips it to not confuse the optimizer.
    let image = jp(b"$.a.b");
    let (entries, _) =
        extract_jsp_query(mcx, &image[4..], JsonbJsonpathExistsStrategyNumber, false).unwrap();
    assert!(entries.is_empty());

    // EXISTS with an equality filter extracts the key chain + scalar.
    let image = jp(b"$.a ? (@.b == 1)");
    let (entries, ops) =
        extract_jsp_query(mcx, &image[4..], JsonbJsonpathExistsStrategyNumber, false).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!((ops[0].kind, ops[0].val), (JSP_GIN_AND, 3));
    assert_eq!(key_of(entries[0]), b"\x01b");
    assert_eq!(key_of(entries[1]), b"\x01a");
    assert_eq!(key_of(entries[2]), b"\x041");
}

#[test]
fn gin_jsonpath_execute_ops() {
    use crate::gin::execute_jsp_gin_ops;
    use gin_vocab::{JspGinOp, JSP_GIN_AND, JSP_GIN_ENTRY, JSP_GIN_OR};
    let op = |kind, val| JspGinOp { kind, val };
    // AND(e0, OR(e1, e2)).
    let ops = [
        op(JSP_GIN_AND, 2),
        op(JSP_GIN_ENTRY, 0),
        op(JSP_GIN_OR, 2),
        op(JSP_GIN_ENTRY, 1),
        op(JSP_GIN_ENTRY, 2),
    ];
    assert_eq!(execute_jsp_gin_ops(&ops, &[1, 0, 1], false), 1);
    assert_eq!(execute_jsp_gin_ops(&ops, &[1, 0, 0], false), 0);
    assert_eq!(execute_jsp_gin_ops(&ops, &[0, 1, 1], false), 0);
    assert_eq!(execute_jsp_gin_ops(&ops, &[1, 2, 0], true), 2);
    assert_eq!(execute_jsp_gin_ops(&ops, &[2, 1, 0], true), 2);
    assert_eq!(execute_jsp_gin_ops(&ops, &[1, 1, 0], true), 1);
    assert_eq!(execute_jsp_gin_ops(&ops, &[0, 1, 1], true), 0);
}

// json_populate_type (populate.rs) over a fixed catalog fixture:
// int4/text/json/jsonb/int4[].
mod populate {
    use super::{jsonb_image, setup};
    use crate::populate::{json_populate_type, ColumnIoData};
    use datum::Datum;
    use mcx::{Mcx, MemoryContext};
    use std::sync::Once;
    use types_core::catalog::{INT4OID, JSONBOID, JSONOID, TEXTOID};
    use types_core::Oid;
    use types_error::PgResult;
    use types_fmgr::{ErrorSaveNode, FmgrInfo, PackedVarlena};

    const INT4ARRAYOID: Oid = 1007;
    const F_INT4IN: Oid = 42;
    const F_TEXTIN: Oid = 46;
    const F_JSON_IN: Oid = 321;
    const F_ARRAY_IN: Oid = 750;
    const F_JSONB_IN: Oid = 3806;

    fn populate_setup() {
        setup();
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            syscache_seams::pg_type_typtype::set(|_typid| Ok(Some(b'b' as i8)));
            syscache_seams::pg_type_element_shape::set(|typid| {
                Ok((typid == INT4ARRAYOID).then_some(syscache_seams::PgTypeElementShape {
                    typelem: INT4OID,
                    typsubscript: lsyscache::F_ARRAY_SUBSCRIPT_HANDLER,
                }))
            });
            syscache_seams::pg_type_io_shape::set(|typid| {
                let shape = |typinput, typelem, typlen, typbyval| syscache_seams::PgTypeIoShape {
                    oid: typid,
                    typinput,
                    typoutput: 0,
                    typreceive: 0,
                    typsend: 0,
                    typmodin: 0,
                    typmodout: 0,
                    typelem,
                    typlen,
                    typbyval,
                    typalign: b'i' as i8,
                    typdelim: b',' as i8,
                    typisdefined: true,
                };
                Ok(match typid {
                    INT4OID => Some(shape(F_INT4IN, 0, 4, true)),
                    TEXTOID => Some(shape(F_TEXTIN, 0, -1, false)),
                    JSONOID => Some(shape(F_JSON_IN, 0, -1, false)),
                    JSONBOID => Some(shape(F_JSONB_IN, 0, -1, false)),
                    INT4ARRAYOID => Some(shape(F_ARRAY_IN, INT4OID, -1, false)),
                    _ => None,
                })
            });
            syscache_seams::lookup_pg_type_shape::set(|typid| {
                Ok((typid == INT4OID).then_some(types_tuple::PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typstorage: b'p' as i8,
                    typcollation: 0,
                }))
            });
            fmgr_seams::fmgr_info::set(|oid| {
                Ok(match oid {
                    F_INT4IN => FmgrInfo::new(adt_int::builtins::fc_int4in, oid, 1, true, false),
                    F_TEXTIN => FmgrInfo::new(varlena::builtins::fc_textin, oid, 1, true, false),
                    F_JSON_IN => {
                        FmgrInfo::new(adt_json::builtins::fc_json_in, oid, 1, true, false)
                    }
                    F_JSONB_IN => {
                        FmgrInfo::new(crate::builtins::fc_jsonb_in, oid, 1, true, false)
                    }
                    F_ARRAY_IN => {
                        FmgrInfo::new(arrayfuncs::builtins::fc_array_in, oid, 3, true, false)
                    }
                    other => panic!("populate tests: unexpected fmgr_info oid {other}"),
                })
            });
        });
    }

    fn populate<'mcx>(
        mcx: Mcx<'mcx>,
        cache: &mut Option<ColumnIoData<'mcx>>,
        doc: &[u8],
        typid: Oid,
        omit_quotes: bool,
        escontext: Option<&mut ErrorSaveNode>,
    ) -> PgResult<(Datum, bool)> {
        let img = jsonb_image(mcx, doc);
        let d = Datum::from_usize(img.as_ptr() as usize);
        let mut isnull = false;
        // SAFETY: `img` is a live 4B-header jsonb varlena for the whole call.
        let res = unsafe {
            json_populate_type(
                d, JSONBOID, typid, -1, cache, mcx, mcx, &mut isnull, omit_quotes, escontext,
            )?
        };
        Ok((res, isnull))
    }

    fn varlena_data(d: Datum) -> Vec<u8> {
        // SAFETY: tests pass live non-null varlena datums.
        let pv = unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) };
        pv.data().to_vec()
    }

    #[test]
    fn scalar_text_from_string() {
        populate_setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut cache = None;
        let (d, isnull) =
            populate(mcx, &mut cache, br#""hello""#, TEXTOID, true, None).unwrap();
        assert!(!isnull);
        assert_eq!(varlena_data(d), b"hello");
        // Same cache, quotes kept: text carries the serialized json literal.
        let (d, isnull) =
            populate(mcx, &mut cache, br#""hello""#, TEXTOID, false, None).unwrap();
        assert!(!isnull);
        assert_eq!(varlena_data(d), br#""hello""#);
    }

    #[test]
    fn scalar_int4_from_number() {
        populate_setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut cache = None;
        let (d, isnull) = populate(mcx, &mut cache, b"42", INT4OID, false, None).unwrap();
        assert!(!isnull);
        assert_eq!(d.as_i32(), 42);
    }

    #[test]
    fn scalar_int4_soft_error() {
        populate_setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut cache = None;
        let mut esc = ErrorSaveNode::new(true);
        let (d, isnull) =
            populate(mcx, &mut cache, br#""nope""#, INT4OID, true, Some(&mut esc)).unwrap();
        assert!(isnull);
        assert_eq!(d.as_usize(), 0);
        assert!(esc.ctx.error_occurred());
    }

    #[test]
    fn jsonb_round_trip() {
        populate_setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut cache = None;
        let doc = br#"{"a": [1, "x", null]}"#;
        let (d, isnull) = populate(mcx, &mut cache, doc, JSONBOID, false, None).unwrap();
        assert!(!isnull);
        assert_eq!(varlena_data(d), jsonb_image(mcx, doc)[4..].to_vec());
    }

    #[test]
    fn json_from_scalar_string_keeps_quotes() {
        populate_setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut cache = None;
        let (d, isnull) = populate(mcx, &mut cache, br#""x""#, JSONOID, false, None).unwrap();
        assert!(!isnull);
        assert_eq!(varlena_data(d), br#""x""#);
    }

    #[test]
    fn array_int4_from_json_array() {
        populate_setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut cache = None;
        let (d, isnull) = populate(mcx, &mut cache, b"[3, 4]", INT4ARRAYOID, false, None).unwrap();
        assert!(!isnull);
        let p = d.as_usize() as *const u8;
        // SAFETY: live array varlena image of arr_size bytes.
        let img = unsafe {
            core::slice::from_raw_parts(p, arrayfuncs::foundation::arr_size(
                core::slice::from_raw_parts(p, 8),
            ))
        };
        assert_eq!(arrayfuncs::foundation::arr_ndim(img), 1);
        assert_eq!(arrayfuncs::foundation::arr_dim(img, 0), 2);
        let (elems, nulls) =
            arrayfuncs::deconstruct_array_builtin(mcx, img, INT4OID, true).unwrap();
        assert_eq!(nulls.as_slice(), &[false, false]);
        assert_eq!(elems.iter().map(|e| e.as_i32()).collect::<Vec<_>>(), vec![3, 4]);
    }

    #[test]
    fn array_int4_two_dims() {
        populate_setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut cache = None;
        let (d, isnull) =
            populate(mcx, &mut cache, b"[[1, 2], [3, 4]]", INT4ARRAYOID, false, None).unwrap();
        assert!(!isnull);
        let p = d.as_usize() as *const u8;
        // SAFETY: live array varlena image of arr_size bytes.
        let img = unsafe {
            core::slice::from_raw_parts(p, arrayfuncs::foundation::arr_size(
                core::slice::from_raw_parts(p, 8),
            ))
        };
        assert_eq!(arrayfuncs::foundation::arr_ndim(img), 2);
        assert_eq!(arrayfuncs::foundation::arr_dim(img, 0), 2);
        assert_eq!(arrayfuncs::foundation::arr_dim(img, 1), 2);
        let (elems, _nulls) =
            arrayfuncs::deconstruct_array_builtin(mcx, img, INT4OID, true).unwrap();
        assert_eq!(
            elems.iter().map(|e| e.as_i32()).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn array_expected_json_array_errors() {
        populate_setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        // Hard: no escontext.
        let mut cache = None;
        let err = populate(mcx, &mut cache, br#"{"a": 1}"#, INT4ARRAYOID, false, None)
            .unwrap_err();
        assert_eq!(err.message(), "expected JSON array");
        // Soft: escontext armed.
        let mut cache = None;
        let mut esc = ErrorSaveNode::new(true);
        let (d, isnull) =
            populate(mcx, &mut cache, b"12", INT4ARRAYOID, false, Some(&mut esc)).unwrap();
        assert!(isnull);
        assert_eq!(d.as_usize(), 0);
        assert_eq!(
            esc.ctx.take_error().expect("soft error saved").message(),
            "expected JSON array"
        );
    }

    #[test]
    fn array_mismatched_dimensions_error() {
        populate_setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut cache = None;
        let err = populate(mcx, &mut cache, b"[[1], [2, 3]]", INT4ARRAYOID, false, None)
            .unwrap_err();
        assert_eq!(err.message(), "malformed JSON array");
    }
}

mod iterate_tests {
    use super::{jsonb_image, setup};
    use crate::iterate::*;
    use mcx::MemoryContext;
    use types_error::PgResult;

    fn collect(doc: &[u8], flags: u32) -> Vec<String> {
        setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let img = jsonb_image(mcx, doc);
        let mut out = Vec::new();
        iterate_jsonb_values(mcx, &img[4..], flags, &mut |e| {
            out.push(String::from_utf8_lossy(e).into_owned());
            Ok(())
        })
        .unwrap();
        out
    }

    fn collect_json(doc: &[u8], flags: u32) -> Vec<String> {
        setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut out = Vec::new();
        iterate_json_values(mcx, doc, flags, &mut |e| {
            out.push(String::from_utf8_lossy(e).into_owned());
            Ok(())
        })
        .unwrap();
        out
    }

    const DOC: &[u8] = br#"{"a": "x", "b": [1, true, "y"], "c": {"d": null}}"#;

    #[test]
    fn jsonb_iterate_flags_select_lanes() {
        assert_eq!(collect(DOC, JTI_STRING), vec!["x", "y"]);
        assert_eq!(collect(DOC, JTI_NUMERIC), vec!["1"]);
        assert_eq!(collect(DOC, JTI_BOOL), vec!["true"]);
        assert_eq!(collect(DOC, JTI_KEY), vec!["a", "b", "c", "d"]);
        assert_eq!(
            collect(DOC, JTI_ALL),
            vec!["a", "x", "b", "1", "true", "y", "c", "d"]
        );
    }

    #[test]
    fn json_iterate_flags_select_lanes() {
        assert_eq!(collect_json(DOC, JTI_STRING), vec!["x", "y"]);
        assert_eq!(collect_json(DOC, JTI_NUMERIC), vec!["1"]);
        assert_eq!(collect_json(DOC, JTI_BOOL), vec!["true"]);
        assert_eq!(collect_json(DOC, JTI_KEY), vec!["a", "b", "c", "d"]);
    }

    fn flags_of(doc: &[u8]) -> PgResult<u32> {
        setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let img = jsonb_image(mcx, doc);
        parse_jsonb_index_flags(mcx, &img[4..])
    }

    #[test]
    fn parse_index_flags_values_and_errors() {
        assert_eq!(flags_of(br#"["string"]"#).unwrap(), JTI_STRING);
        assert_eq!(flags_of(br#""all""#).unwrap(), JTI_ALL);
        assert_eq!(
            flags_of(br#"["key", "NUMERIC", "boolean"]"#).unwrap(),
            JTI_KEY | JTI_NUMERIC | JTI_BOOL
        );
        assert_eq!(
            flags_of(br#"{"a": 1}"#).unwrap_err().message(),
            "wrong flag type, only arrays and scalars are allowed"
        );
        assert_eq!(
            flags_of(br#"[1]"#).unwrap_err().message(),
            "flag array element is not a string"
        );
        assert_eq!(
            flags_of(br#"["strings"]"#).unwrap_err().message(),
            "wrong flag in flag array: \"strings\""
        );
    }

    #[test]
    fn transform_jsonb_rewrites_string_values_only() {
        setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let img = jsonb_image(mcx, br#"{"a": "x", "b": [1, "y"], "c": true}"#);
        let payload: &[u8] = mcx::slice_in(mcx, &img[4..]).unwrap().leak();
        let out = transform_jsonb_string_values(mcx, payload, &mut |s| {
            let mut v = mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
            mcx::vec_append_bytes(&mut v, b"<")?;
            mcx::vec_append_bytes(&mut v, s)?;
            Ok(v.leak())
        })
        .unwrap();
        let expect = jsonb_image(mcx, br#"{"a": "<x", "b": [1, "<y"], "c": true}"#);
        assert_eq!(&out[..], &expect[..]);
    }

    #[test]
    fn transform_jsonb_raw_scalar_round_trip() {
        setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let img = jsonb_image(mcx, br#""s""#);
        let payload: &[u8] = mcx::slice_in(mcx, &img[4..]).unwrap().leak();
        let out = transform_jsonb_string_values(mcx, payload, &mut |s| {
            Ok(mcx::slice_in(mcx, s).unwrap().leak())
        })
        .unwrap();
        assert_eq!(&out[..], &img[..]);
    }

    #[test]
    fn transform_json_reescapes_and_preserves_layout() {
        setup();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let out = transform_json_string_values(
            mcx,
            br#"{"a": "x", "b": [1, "y", null], "c": false}"#,
            &mut |s| {
                let mut v = mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
                mcx::vec_append_bytes(&mut v, b"<")?;
                mcx::vec_append_bytes(&mut v, s)?;
                Ok(v)
            },
        )
        .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&out),
            r#"{"a":"<x","b":[1,"<y",null],"c":false}"#
        );
    }
}
