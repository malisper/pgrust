use super::*;
use ::types_error::make_sqlstate;

// corpus.tsv: \x01-separated rows captured from live PostgreSQL 18.3
// (Homebrew); OK rows carry the exact wire text, ERR rows
// SQLSTATE \x01 message \x01 detail.
const CORPUS: &str = include_str!("corpus.tsv");

fn parse(s: &str, is_cidr: bool) -> PgResult<InetValue> {
    Ok(network_in(s, is_cidr, None)?.expect("hard error path returns Some"))
}

fn out(v: &InetValue, is_cidr: bool) -> String {
    let mut buf = [0u8; INET_OUT_BUFLEN];
    let len = network_out_into(v.iref(), is_cidr, &mut buf).unwrap();
    String::from_utf8(buf[..len].to_vec()).unwrap()
}

// The corpus probe captures value-typed results through ::text (network_show).
fn show(v: &InetValue) -> String {
    text_of(v, network_show_into)
}

fn text_of(v: &InetValue, f: fn(InetRef<'_>, &mut [u8]) -> PgResult<usize>) -> String {
    let mut buf = [0u8; INET_OUT_BUFLEN];
    let len = f(v.iref(), &mut buf).unwrap();
    String::from_utf8(buf[..len].to_vec()).unwrap()
}

struct Expected<'a> {
    ok: Option<&'a str>,
    sqlstate: &'a str,
    message: &'a str,
    detail: &'a str,
}

fn check(line: &str, expected: &Expected<'_>, got: PgResult<String>) -> Option<String> {
    match (expected.ok, got) {
        (Some(want), Ok(g)) => (g != want).then(|| format!("{line}: got {g:?} want {want:?}")),
        (Some(want), Err(e)) => Some(format!("{line}: got error {e} want {want:?}")),
        (None, Ok(g)) => Some(format!("{line}: got {g:?} want error {}", expected.message)),
        (None, Err(e)) => {
            let want_state = make_sqlstate(expected.sqlstate.as_bytes().try_into().unwrap());
            let mut bad = e.sqlstate() != want_state || e.message() != expected.message;
            if !expected.detail.is_empty() {
                bad |= e.detail() != Some(expected.detail);
            }
            bad.then(|| {
                format!(
                    "{line}: got error {:?}/{} detail {:?} want {}/{} detail {:?}",
                    e.sqlstate(),
                    e.message(),
                    e.detail(),
                    expected.sqlstate,
                    expected.message,
                    expected.detail
                )
            })
        }
    }
}

fn bool_s(b: bool) -> String {
    (if b { "true" } else { "false" }).to_string()
}

#[test]
fn differential_corpus_vs_live_pg() {
    let mut n = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for line in CORPUS.lines() {
        let f: Vec<&str> = line.split('\x01').collect();
        let (tag, rest) = (f[0], &f[1..]);
        let status_at: usize = match tag {
            "IN" | "FN" => 2,
            "FN2" | "OP" | "OPI" => 3,
            _ => panic!("bad corpus tag {tag}"),
        };
        let args = &rest[..status_at];
        let expected = if rest[status_at] == "OK" {
            Expected {
                ok: Some(rest[status_at + 1]),
                sqlstate: "",
                message: "",
                detail: "",
            }
        } else {
            Expected {
                ok: None,
                sqlstate: rest[status_at + 1],
                message: rest[status_at + 2],
                detail: rest.get(status_at + 3).copied().unwrap_or(""),
            }
        };

        let got: PgResult<String> = match tag {
            "IN" => {
                let is_cidr = args[0] == "cidr";
                parse(args[1], is_cidr).map(|v| out(&v, is_cidr))
            }
            "FN" => {
                let (fname, v) = (args[0], args[1]);
                let is_cidr = fname == "abbrev_cidr";
                parse(v, is_cidr).and_then(|v| {
                    Ok(match fname {
                        "host" => text_of(&v, network_host_into),
                        "text" => text_of(&v, network_show_into),
                        "abbrev" => text_of(&v, inet_abbrev_into),
                        "abbrev_cidr" => text_of(&v, cidr_abbrev_into),
                        "masklen" => (v.bits as i32).to_string(),
                        "family" => network_family(v.iref()).to_string(),
                        "network" => show(&network_network(v.iref())),
                        "netmask" => show(&network_netmask(v.iref())),
                        "broadcast" => show(&network_broadcast(v.iref())),
                        "hostmask" => show(&network_hostmask(v.iref())),
                        "to_cidr" => show(&inet_to_cidr(v.iref())?),
                        "not" => show(&inetnot(v.iref())),
                        "hashinet" => (hashinet_bytes(v.iref()) as i32).to_string(),
                        _ => panic!("bad FN {fname}"),
                    })
                })
            }
            "FN2" => {
                let (fname, v, k) = (args[0], args[1], args[2].parse::<i32>().unwrap());
                parse(v, false).and_then(|v| match fname {
                    "set_masklen" => Ok(show(&inet_set_masklen(v.iref(), k)?)),
                    "set_masklen_cidr" => {
                        let c = network_network(v.iref());
                        Ok(show(&cidr_set_masklen(c.iref(), k)?))
                    }
                    _ => panic!("bad FN2 {fname}"),
                })
            }
            "OP" => {
                let (opname, a, b) = (args[0], args[1], args[2]);
                parse(a, false).and_then(|a| {
                    let b = parse(b, false)?;
                    let (a, b) = (a.iref(), b.iref());
                    Ok(match opname {
                        "cmp" => network_cmp_internal(a, b).to_string(),
                        "lt" => bool_s(network_cmp_internal(a, b) < 0),
                        "le" => bool_s(network_cmp_internal(a, b) <= 0),
                        "eq" => bool_s(network_cmp_internal(a, b) == 0),
                        "ge" => bool_s(network_cmp_internal(a, b) >= 0),
                        "gt" => bool_s(network_cmp_internal(a, b) > 0),
                        "ne" => bool_s(network_cmp_internal(a, b) != 0),
                        "sub" => bool_s(network_sub(a, b)),
                        "subeq" => bool_s(network_subeq(a, b)),
                        "sup" => bool_s(network_sup(a, b)),
                        "supeq" => bool_s(network_supeq(a, b)),
                        "overlap" => bool_s(network_overlap(a, b)),
                        "same_family" => bool_s(inet_same_family(a, b)),
                        "merge" => show(&inet_merge(a, b)?),
                        "mi" => inetmi(a, b)?.to_string(),
                        "and" => show(&inetand(a, b)?),
                        "or" => show(&inetor(a, b)?),
                        _ => panic!("bad OP {opname}"),
                    })
                })
            }
            "OPI" => {
                let (opname, a) = (args[0], args[1]);
                parse(a, false).and_then(|a| match opname {
                    "pl" => {
                        let addend = args[2].parse::<i64>().unwrap();
                        Ok(show(&internal_inetpl(a.iref(), addend)?))
                    }
                    "mi8" => {
                        let addend = args[2].parse::<i64>().unwrap();
                        Ok(show(&internal_inetpl(a.iref(), addend.wrapping_neg())?))
                    }
                    "mi" => {
                        let b = parse(args[2], false)?;
                        Ok(inetmi(a.iref(), b.iref())?.to_string())
                    }
                    _ => panic!("bad OPI {opname}"),
                })
            }
            _ => unreachable!(),
        };
        if let Some(msg) = check(line, &expected, got) {
            failures.push(msg.replace('\x01', "|"));
        }
        n += 1;
    }
    assert!(
        failures.is_empty(),
        "{} of {n} corpus rows diverged:\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert!(n >= 1400, "corpus unexpectedly small: {n}");
}

#[test]
fn soft_error_lane() {
    let mut soft = SoftErrorContext::new(true);
    assert!(network_in("junk", false, Some(&mut soft)).unwrap().is_none());
    assert!(soft.error_occurred());

    let mut soft = SoftErrorContext::new(true);
    assert!(network_in("192.168.1.5/24", true, Some(&mut soft))
        .unwrap()
        .is_none());
    assert!(soft.error_occurred());
}

#[test]
fn recv_send_roundtrip_and_wire_image() {
    let ctx = mcx::MemoryContext::new("network-test");
    let mcx = ctx.mcx();
    for (s, is_cidr) in [
        ("192.168.1.5/24", false),
        ("1.2.3.4", false),
        ("2001:db8::1/64", false),
        ("10.0.0.0/8", true),
        ("2001:db8::/32", true),
    ] {
        let v = parse(s, is_cidr).unwrap();
        let sent = network_send(mcx, v.iref(), is_cidr).unwrap();
        let payload = sent.data().to_vec();
        assert_eq!(payload[0], v.family);
        assert_eq!(payload[1], v.bits);
        assert_eq!(payload[2], is_cidr as u8);
        assert_eq!(payload[3] as usize, v.addrsize());
        assert_eq!(&payload[4..], &v.ipaddr[..v.addrsize()]);

        let mut si = stringinfo::StringInfo::with_capacity_in(mcx, payload.len()).unwrap();
        si.append_bytes(&payload).unwrap();
        let back = network_recv(&mut si, is_cidr).unwrap();
        assert_eq!(back, v);
    }
}

#[test]
fn recv_error_arms() {
    let ctx = mcx::MemoryContext::new("network-test");
    let mcx = ctx.mcx();
    let mut run = |bytes: &[u8], is_cidr: bool| -> PgResult<InetValue> {
        let mut si = stringinfo::StringInfo::with_capacity_in(mcx, bytes.len()).unwrap();
        si.append_bytes(bytes).unwrap();
        network_recv(&mut si, is_cidr)
    };
    let e = run(&[9, 0, 0, 4, 1, 2, 3, 4], false).unwrap_err();
    assert_eq!(
        e.message(),
        "invalid address family in external \"inet\" value"
    );
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_BINARY_REPRESENTATION);
    let e = run(&[2, 33, 0, 4, 1, 2, 3, 4], false).unwrap_err();
    assert_eq!(e.message(), "invalid bits in external \"inet\" value");
    let e = run(&[2, 24, 0, 3, 1, 2, 3], true).unwrap_err();
    assert_eq!(e.message(), "invalid length in external \"cidr\" value");
    let e = run(&[2, 24, 1, 4, 1, 2, 3, 4], true).unwrap_err();
    assert_eq!(e.message(), "invalid external \"cidr\" value");
    assert_eq!(e.detail(), Some("Value has bits set to right of mask."));
}

#[test]
fn hash_extended_matches_live_pg() {
    // SELECT hashinetextended(x, seed) on live PostgreSQL 18.3.
    let cases: [(&str, u64, i64); 3] = [
        ("1.2.3.4", 0, -8863782565210921970),
        ("1.2.3.4", 42, 7449071704568358571),
        ("2001:db8::1/64", 7, 1748326417020274813),
    ];
    for (s, seed, want) in cases {
        let v = parse(s, false).unwrap();
        assert_eq!(hashinet_bytes_extended(v.iref(), seed) as i64, want, "{s}");
    }
}

#[test]
fn sort_order_matches_live_pg_order_by() {
    let inputs = [
        "255.255.255.255/32",
        "10.0.0.0/8",
        "1.2.3.4",
        "192.168.1.0/24",
        "0.0.0.0/0",
        "192.168.1.0/25",
        "192.168.1.128/25",
        "::1",
        "::",
        "2001:db8::/32",
        "2001:db8::1",
        "10.1.2.3/8",
        "10.1.2.3",
        "::ffff:1.2.3.4",
        "128.0.0.0/1",
    ];
    let mut vals: Vec<InetValue> = inputs.iter().map(|s| parse(s, false).unwrap()).collect();
    vals.sort_by(|a, b| network_cmp_internal(a.iref(), b.iref()).cmp(&0));
    // SELECT inet_out(v) ... ORDER BY v on live PostgreSQL 18.3.
    let want = [
        "0.0.0.0/0",
        "1.2.3.4",
        "10.0.0.0/8",
        "10.1.2.3/8",
        "10.1.2.3",
        "128.0.0.0/1",
        "192.168.1.0/24",
        "192.168.1.0/25",
        "192.168.1.128/25",
        "255.255.255.255",
        "::",
        "::1",
        "::ffff:1.2.3.4",
        "2001:db8::/32",
        "2001:db8::1",
    ];
    let got: Vec<String> = vals.iter().map(|v| out(v, false)).collect();
    assert_eq!(got, want);
}
