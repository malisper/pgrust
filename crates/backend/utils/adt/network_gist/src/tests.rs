use super::*;
use ::adt_network::{network_cmp_internal, network_in, network_overlap, network_sub, network_subeq, network_sup, network_supeq};

fn v(s: &str) -> InetValue {
    network_in(s, false, None).unwrap().unwrap()
}

fn corpus() -> Vec<InetValue> {
    [
        "0.0.0.0/0",
        "10.0.0.0/8",
        "10.1.0.0/16",
        "10.1.2.0/24",
        "10.1.2.3",
        "10.1.2.3/8",
        "10.1.3.0/24",
        "10.128.0.0/9",
        "192.168.1.0/24",
        "192.168.1.5",
        "192.168.1.255",
        "255.255.255.255",
        "127.0.0.1",
        "::/0",
        "::1",
        "2001:db8::/32",
        "2001:db8::1",
        "2001:db8:0:1::/64",
        "2001:db8:0:1::5",
        "2001:db8:8000::/33",
        "fe80::/10",
        "fe80::1",
        "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "::ffff:10.4.3.2",
    ]
    .iter()
    .map(|s| v(s))
    .collect()
}

fn leaf_key_image(ip: &InetValue) -> ([u8; 20], usize) {
    let r = ip.iref();
    gk_image(r.family, r.bits as i32, r.maxbits() as i32, r.addr)
}

fn gk<'a>(img: &'a ([u8; 20], usize)) -> GkRef<'a> {
    GkRef::from_image(&img.0[..img.1])
}

const ALL_STRATEGIES: [u16; 11] = [
    INETSTRAT_OVERLAPS,
    INETSTRAT_EQ,
    INETSTRAT_NE,
    INETSTRAT_LT,
    INETSTRAT_LE,
    INETSTRAT_GT,
    INETSTRAT_GE,
    INETSTRAT_SUB,
    INETSTRAT_SUBEQ,
    INETSTRAT_SUP,
    INETSTRAT_SUPEQ,
];

fn oracle(k: &InetValue, q: &InetValue, strategy: u16) -> bool {
    let (k, q) = (k.iref(), q.iref());
    match strategy {
        INETSTRAT_OVERLAPS => network_overlap(k, q),
        INETSTRAT_EQ => network_cmp_internal(k, q) == 0,
        INETSTRAT_NE => network_cmp_internal(k, q) != 0,
        INETSTRAT_LT => network_cmp_internal(k, q) < 0,
        INETSTRAT_LE => network_cmp_internal(k, q) <= 0,
        INETSTRAT_GT => network_cmp_internal(k, q) > 0,
        INETSTRAT_GE => network_cmp_internal(k, q) >= 0,
        INETSTRAT_SUB => network_sub(k, q),
        INETSTRAT_SUBEQ => network_subeq(k, q),
        INETSTRAT_SUP => network_sup(k, q),
        INETSTRAT_SUPEQ => network_supeq(k, q),
        _ => unreachable!(),
    }
}

#[test]
fn gk_image_layout_matches_c() {
    let ip = v("192.168.1.0/24");
    let (img, len) = leaf_key_image(&ip);
    assert_eq!(len, 8);
    assert_eq!(img[0], (8 << 1) | 1);
    assert_eq!(&img[1..8], &[2, 24, 32, 192, 168, 1, 0]);

    let ip6 = v("2001:db8::/32");
    let (img, len) = leaf_key_image(&ip6);
    assert_eq!(len, 20);
    assert_eq!(img[0], (20 << 1) | 1);
    assert_eq!(img[1], 3);
    assert_eq!(img[2], 32);
    assert_eq!(img[3], 128);
    assert_eq!(&img[4..8], &[0x20, 0x01, 0x0d, 0xb8]);
    assert_eq!(&img[8..20], &[0u8; 12]);
}

#[test]
fn gk_image_masks_partial_byte() {
    // commonbits=9 over a nonzero addr: bits past 9 zeroed.
    let addr = [0xff, 0xff, 0xff, 0xff];
    let (img, _) = gk_image(2, 9, 9, &addr);
    assert_eq!(&img[4..8], &[0xff, 0x80, 0, 0]);
}

#[test]
fn leaf_consistent_matches_operators() {
    let c = corpus();
    for k in &c {
        let img = leaf_key_image(k);
        let key = gk(&img);
        for q in &c {
            for s in ALL_STRATEGIES {
                assert_eq!(
                    consistent_internal(key, q.iref(), s, true),
                    oracle(k, q, s),
                    "key={k:?} query={q:?} strategy={s}"
                );
            }
        }
    }
}

#[test]
fn inner_consistent_never_misses() {
    // Union of any subset must descend whenever some member leaf matches.
    let c = corpus();
    for i in 0..c.len() {
        for j in i..c.len() {
            let (a, b) = (&c[i], &c[j]);
            let ia = leaf_key_image(a);
            let ib = leaf_key_image(b);
            let p = calc_inet_union_params([gk(&ia), gk(&ib)].into_iter());
            let family = if p.minfamily != p.maxfamily {
                0
            } else {
                p.minfamily
            };
            let u = gk_image(family, p.minbits, p.commonbits, gk(&ia).addr());
            let ukey = gk(&u);
            for q in &c {
                for s in ALL_STRATEGIES {
                    let some_leaf = oracle(a, q, s) || oracle(b, q, s);
                    if some_leaf {
                        assert!(
                            consistent_internal(ukey, q.iref(), s, false),
                            "false negative: a={a:?} b={b:?} q={q:?} strategy={s}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn union_params_multi_family_zeroes() {
    let a = leaf_key_image(&v("10.0.0.1"));
    let b = leaf_key_image(&v("::1"));
    let p = calc_inet_union_params([gk(&a), gk(&b)].into_iter());
    assert_eq!((p.minfamily, p.maxfamily), (2, 3));
    assert_eq!((p.minbits, p.commonbits), (0, 0));
}

#[test]
fn common_bits_capped_equals_bitncommon() {
    let mut s = 0x1234_5678_9abc_def0u64;
    let mut lcg = || {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        s
    };
    for _ in 0..5000 {
        let mut a = [0u8; 16];
        let mut b = [0u8; 16];
        for i in 0..16 {
            a[i] = (lcg() >> 56) as u8;
            b[i] = if lcg() & 3 == 0 { (lcg() >> 56) as u8 } else { a[i] };
        }
        for &sz in &[4usize, 16] {
            let n = (lcg() % (sz as u64 * 8 + 1)) as i32;
            assert_eq!(
                common_bits_capped(&a[..sz], &b[..sz], n),
                bitncommon(&a[..sz], &b[..sz], n),
                "a={a:?} b={b:?} n={n}"
            );
        }
    }
}
