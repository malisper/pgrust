//! Differential fuzz drivers: adt_char ("char") and adt_bool vs vendored
//! PostgreSQL C (csrc/pg_char.c, csrc/pg_bool.c — Stamp 18.3, 62d6c7d3df).
//!
//! Campaign lane: proofs/p1-lane0a (100%-coverage phase 1). One composite
//! target per crate, sibling functions behind a selector byte (the
//! float_in_diff pattern). Every case is compared on the three planes:
//! value bits/bytes (incl. exact output images), error-vs-no-error, and
//! errcode class. Any mismatch panics -> libFuzzer crash artifact = the
//! divergence reproducer.
//!
//! Besides the value cores, each op also drives the shipped fmgr wrappers
//! (`fc_*` in the crates' builtins.rs) through `LocalFcinfo`, asserting the
//! wrapper agrees with its own core — so the Datum packing/unpacking lines
//! execute under the same differential campaign.

use std::ffi::{c_char, CString};

use datum::{Datum, VarlenaRef};
use mcx::MemoryContext;
use stringinfo::StringInfo;
use types_error::{
    SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_fmgr::{direct_function_call1_coll_in, AggStateNode, LocalFcinfo};

extern "C" {
    fn pg_diff_errcode_get() -> i32;

    // csrc/pg_char.c
    fn pg_diff_charin(ch: *const c_char) -> c_char;
    fn pg_diff_charout(ch: c_char, out5: *mut c_char) -> i32;
    fn pg_diff_chareq(a: c_char, b: c_char) -> i32;
    fn pg_diff_charne(a: c_char, b: c_char) -> i32;
    fn pg_diff_charlt(a: c_char, b: c_char) -> i32;
    fn pg_diff_charle(a: c_char, b: c_char) -> i32;
    fn pg_diff_chargt(a: c_char, b: c_char) -> i32;
    fn pg_diff_charge(a: c_char, b: c_char) -> i32;
    fn pg_diff_chartoi4(a: c_char) -> i32;
    fn pg_diff_i4tochar(arg1: i32, out: *mut c_char) -> i32;
    fn pg_diff_text_char(data: *const c_char, len: i32) -> c_char;
    fn pg_diff_char_text(arg1: c_char, out4: *mut c_char) -> i32;

    // csrc/pg_bool.c
    fn pg_diff_parse_bool_with_len(value: *const c_char, len: usize, result: *mut i32) -> i32;
    fn pg_diff_boolin(in_str: *const c_char, result: *mut i32) -> i32;
    fn pg_diff_boolout(b: i32, out2: *mut c_char);
    fn pg_diff_boolrecv(ext: i32) -> i32;
    fn pg_diff_booltext(arg1: i32, out8: *mut c_char) -> i32;
    fn pg_diff_booleq(a: i32, b: i32) -> i32;
    fn pg_diff_boolne(a: i32, b: i32) -> i32;
    fn pg_diff_boollt(a: i32, b: i32) -> i32;
    fn pg_diff_boolgt(a: i32, b: i32) -> i32;
    fn pg_diff_boolle(a: i32, b: i32) -> i32;
    fn pg_diff_boolge(a: i32, b: i32) -> i32;
    fn pg_diff_booland_statefunc(a: i32, b: i32) -> i32;
    fn pg_diff_boolor_statefunc(a: i32, b: i32) -> i32;
    fn pg_diff_bool_accum(state_isnull: i32, state: *mut CBoolAggState, val_isnull: i32, val: i32);
    fn pg_diff_bool_accum_inv(
        state_isnull: i32,
        state: *mut CBoolAggState,
        val_isnull: i32,
        val: i32,
    ) -> i32;
    fn pg_diff_bool_alltrue(state_isnull: i32, state: *const CBoolAggState) -> i32;
    fn pg_diff_bool_anytrue(state_isnull: i32, state: *const CBoolAggState) -> i32;
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CBoolAggState {
    aggcount: i64,
    aggtrue: i64,
}

fn c_errcode() -> i32 {
    unsafe { pg_diff_errcode_get() }
}

/// Same small-int classes as csrc (pg_float_io.c convention).
const C_ERR_INVALID_TEXT: i32 = 1; /* 22P02 */
const C_ERR_OUT_OF_RANGE: i32 = 2; /* 22003 */

/// The fixed pq_begintypsend/pq_endtypsend bytea image for a 1-byte send
/// payload: 4-byte varlena header (VARHDRSZ + 1 = 5, 4B aligned form) then
/// the byte. charsend/boolsend have no per-value logic beyond this framing
/// (their C bodies are a single pq_sendbyte), so the expected image is
/// constructed here rather than vendoring pqformat (SHIM 5 in pg_char.c).
fn expected_send1_image(payload: u8) -> [u8; 5] {
    let mut img = [0u8; 5];
    // SET_VARSIZE 4B header: little-endian on this platform, len<<2 form.
    let hdr = ((5u32) << 2).to_ne_bytes();
    img[..4].copy_from_slice(&hdr);
    img[4] = payload;
    img
}

// ---------------------------------------------------------------------------
// Target: char_diff — adt/char vs vendored char.c.
// ---------------------------------------------------------------------------
//
// Input layout: [selector][payload...]:
//   sel % 6 == 0: charin/fc_charin — payload = cstring text (NUL-free).
//   sel % 6 == 1: out family on payload[0] as the char value: charout,
//                 fc_charout, char_text, fc_char_text, charsend,
//                 fc_charsend, text_char(charout image) round-trip.
//   sel % 6 == 2: comparison ops on payload[0], payload[1] + fc_ wrappers.
//   sel % 6 == 3: chartoi4 (payload[0]) and i4tochar (payload[0..4] le i32)
//                 incl. the 22003 error plane + fc_ wrappers.
//   sel % 6 == 4: text_char on the raw payload (any bytes, incl. NUL and
//                 non-UTF8 — text payloads are arbitrary) + fc_text_char.
//   sel % 6 == 5: charrecv over a StringInfo of the payload + wire parity.

pub fn char_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    if payload.len() > 1024 {
        return;
    }
    match sel % 6 {
        0 => char_in_case(payload),
        1 => char_out_case(payload),
        2 => char_cmp_case(payload),
        3 => char_int_case(payload),
        4 => char_textin_case(payload),
        _ => char_recv_case(payload),
    }
}

fn char_in_case(payload: &[u8]) {
    // The shipped entry receives a parser-produced cstring: NUL-free.
    if payload.contains(&0) {
        return;
    }
    let cs = CString::new(payload).unwrap();
    let cval = unsafe { pg_diff_charin(cs.as_ptr()) } as i8;
    let r = adt_char::charin(payload);
    assert!(
        r == cval,
        "charin DIVERGENCE input={payload:?}: C={cval} Rust={r}"
    );

    // fmgr wrapper: cstring datum -> char datum.
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_usize(cs.as_ptr() as usize));
    let d = adt_char::builtins::fc_charin(None, &mut fcinfo).unwrap();
    assert!(
        d.as_char() == cval,
        "fc_charin DIVERGENCE input={payload:?}: C={cval} Rust={}",
        d.as_char()
    );
}

fn char_out_case(payload: &[u8]) {
    let Some(&b0) = payload.first() else {
        return;
    };
    let ch = b0 as i8;

    // charout image.
    let mut cbuf = [0 as c_char; 5];
    let clen = unsafe { pg_diff_charout(b0 as c_char, cbuf.as_mut_ptr()) } as usize;
    let cimg: Vec<u8> = cbuf[..clen].iter().map(|&c| c as u8).collect();
    let mut rbuf = [0u8; 4];
    let rlen = adt_char::charout(ch, &mut rbuf);
    assert!(
        rbuf[..rlen] == cimg[..],
        "charout DIVERGENCE ch={b0:#04x}: C={cimg:?} Rust={:?}",
        &rbuf[..rlen]
    );

    // fc_charout returns a NUL-terminated cstring datum.
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_char(ch));
    let d = adt_char::builtins::fc_charout(None, &mut fcinfo).unwrap();
    // SAFETY: fc_charout returns a NUL-terminated cstring datum.
    let s = unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const c_char) };
    assert!(
        s.to_bytes() == &cimg[..],
        "fc_charout DIVERGENCE ch={b0:#04x}: C={cimg:?} Rust={:?}",
        s.to_bytes()
    );

    // char_text payload image.
    let mut ctbuf = [0 as c_char; 4];
    let ctlen = unsafe { pg_diff_char_text(b0 as c_char, ctbuf.as_mut_ptr()) } as usize;
    let ctimg: Vec<u8> = ctbuf[..ctlen].iter().map(|&c| c as u8).collect();
    let ctx = MemoryContext::new_bump("char_diff");
    let v = adt_char::char_text(ctx.mcx(), ch).unwrap();
    assert!(
        v.data() == &ctimg[..],
        "char_text DIVERGENCE ch={b0:#04x}: C={ctimg:?} Rust={:?}",
        v.data()
    );

    // fc_char_text through the armed result mcx.
    let d = direct_function_call1_coll_in(
        adt_char::builtins::fc_char_text,
        0,
        ctx.mcx(),
        Datum::from_char(ch),
    )
    .unwrap();
    // SAFETY: fc_char_text returns a live 4B-header text varlena in ctx.
    let vr = unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    assert!(
        vr.data() == &ctimg[..],
        "fc_char_text DIVERGENCE ch={b0:#04x}: C={ctimg:?} Rust={:?}",
        vr.data()
    );

    // text_char(charout image) round-trip: C matches C, Rust matches Rust,
    // and both agree (charin's backwards-compat first-byte rule makes every
    // charout image a fixed point).
    let c_rt = unsafe { pg_diff_text_char(cbuf.as_ptr(), clen as i32) } as i8;
    let r_rt = adt_char::text_char(&rbuf[..rlen]);
    assert!(
        c_rt == r_rt && r_rt == ch,
        "char out/in round-trip DIVERGENCE ch={b0:#04x}: C={c_rt} Rust={r_rt}"
    );

    // charsend wire image (fixed framing; see expected_send1_image).
    let b = adt_char::charsend(ctx.mcx(), ch).unwrap();
    let expect = expected_send1_image(b0);
    assert!(
        b.as_bytes() == expect,
        "charsend IMAGE DIVERGENCE ch={b0:#04x}: expected={expect:?} Rust={:?}",
        b.as_bytes()
    );
    let d = direct_function_call1_coll_in(
        adt_char::builtins::fc_charsend,
        0,
        ctx.mcx(),
        Datum::from_char(ch),
    )
    .unwrap();
    // SAFETY: fc_charsend returns a live 4B-header bytea in ctx.
    let vr = unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    assert!(
        vr.data() == [b0],
        "fc_charsend DIVERGENCE ch={b0:#04x}: payload={:?}",
        vr.data()
    );
}

fn char_cmp_case(payload: &[u8]) {
    if payload.len() < 2 {
        return;
    }
    let (a, b) = (payload[0] as i8, payload[1] as i8);
    let (ca, cb) = (payload[0] as c_char, payload[1] as c_char);
    type RustCmp = fn(i8, i8) -> bool;
    type CCmp = unsafe extern "C" fn(c_char, c_char) -> i32;
    type FcCmp = types_fmgr::PGFunction;
    let table: &[(&str, RustCmp, CCmp, FcCmp)] = &[
        ("chareq", adt_char::chareq, pg_diff_chareq, adt_char::builtins::fc_chareq),
        ("charne", adt_char::charne, pg_diff_charne, adt_char::builtins::fc_charne),
        ("charlt", adt_char::charlt, pg_diff_charlt, adt_char::builtins::fc_charlt),
        ("charle", adt_char::charle, pg_diff_charle, adt_char::builtins::fc_charle),
        ("chargt", adt_char::chargt, pg_diff_chargt, adt_char::builtins::fc_chargt),
        ("charge", adt_char::charge, pg_diff_charge, adt_char::builtins::fc_charge),
    ];
    for (name, rf, cf, fc) in table {
        let rv = rf(a, b);
        let cv = unsafe { cf(ca, cb) } != 0;
        assert!(rv == cv, "{name} DIVERGENCE a={a} b={b}: C={cv} Rust={rv}");
        let mut fcinfo = LocalFcinfo::<2>::new(0);
        fcinfo.set_arg(0, Datum::from_char(a));
        fcinfo.set_arg(1, Datum::from_char(b));
        let d = fc(None, &mut fcinfo).unwrap();
        assert!(
            d.as_bool() == cv,
            "fc_{name} DIVERGENCE a={a} b={b}: C={cv} Rust={}",
            d.as_bool()
        );
    }
}

fn char_int_case(payload: &[u8]) {
    let Some(&b0) = payload.first() else {
        return;
    };
    let ch = b0 as i8;
    let cv = unsafe { pg_diff_chartoi4(b0 as c_char) };
    let rv = adt_char::chartoi4(ch);
    assert!(cv == rv, "chartoi4 DIVERGENCE ch={b0:#04x}: C={cv} Rust={rv}");
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_char(ch));
    let d = adt_char::builtins::fc_chartoi4(None, &mut fcinfo).unwrap();
    assert!(d.as_i32() == cv, "fc_chartoi4 DIVERGENCE ch={b0:#04x}");

    if payload.len() < 4 {
        return;
    }
    let arg = i32::from_le_bytes(payload[..4].try_into().unwrap());
    let mut cout: c_char = 0;
    let cerr = unsafe { pg_diff_i4tochar(arg, &mut cout) };
    let cerrcode = c_errcode();
    match adt_char::i4tochar(arg) {
        Ok(r) => assert!(
            cerr == 0 && r == cout as i8,
            "i4tochar DIVERGENCE arg={arg}: C=(err {cerr}, {cout}) Rust=Ok({r})"
        ),
        Err(e) => {
            let rclass = if e.sqlstate() == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
                C_ERR_OUT_OF_RANGE
            } else {
                99
            };
            assert!(
                cerr == 1 && cerrcode == rclass,
                "i4tochar DIVERGENCE arg={arg}: C=(err {cerr} code {cerrcode}) Rust=Err({rclass})"
            );
        }
    }
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_i32(arg));
    let fr = adt_char::builtins::fc_i4tochar(None, &mut fcinfo);
    assert!(
        fr.is_ok() == (cerr == 0),
        "fc_i4tochar error-plane DIVERGENCE arg={arg}"
    );
    if let Ok(d) = fr {
        assert!(d.as_char() == cout as i8, "fc_i4tochar value DIVERGENCE arg={arg}");
    }
}

fn char_textin_case(payload: &[u8]) {
    // text payloads are arbitrary bytes (text_char reads VARDATA/len).
    let len = payload.len().min(512);
    let bytes = &payload[..len];
    let cval = unsafe { pg_diff_text_char(bytes.as_ptr().cast(), len as i32) } as i8;
    let rval = adt_char::text_char(bytes);
    assert!(
        cval == rval,
        "text_char DIVERGENCE input={bytes:?}: C={cval} Rust={rval}"
    );

    // fc_text_char over a real text varlena.
    let ctx = MemoryContext::new_bump("char_diff");
    let v = varlena::cstring_to_text(ctx.mcx(), bytes).unwrap();
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_usize(v.as_bytes().as_ptr() as usize));
    let d = adt_char::builtins::fc_text_char(None, &mut fcinfo).unwrap();
    assert!(
        d.as_char() == cval,
        "fc_text_char DIVERGENCE input={bytes:?}: C={cval} Rust={}",
        d.as_char()
    );
}

fn char_recv_case(payload: &[u8]) {
    // charrecv's body is pq_getmsgbyte: consume 1 byte or raise. The wire
    // framing is pqformat's, not char.c's (SHIM 5); expected behavior is
    // stated inline: byte 0 of the message on success, error iff empty.
    let ctx = MemoryContext::new_bump("char_diff");
    let n = payload.len().min(64);
    let mut si = StringInfo::with_capacity_in(ctx.mcx(), n + 1).unwrap();
    si.append_bytes(&payload[..n]).unwrap();
    match adt_char::charrecv(&mut si) {
        Ok(v) => {
            assert!(
                n >= 1 && v == payload[0] as i8 && si.cursor == 1,
                "charrecv DIVERGENCE payload={payload:?}: got {v} cursor={}",
                si.cursor
            );
        }
        Err(_) => assert!(n == 0, "charrecv errored on non-empty message"),
    }

    // fc_charrecv through the recv ABI (arg0 = live StringInfo pointer).
    if n >= 1 {
        let mut si = StringInfo::with_capacity_in(ctx.mcx(), n + 1).unwrap();
        si.append_bytes(&payload[..n]).unwrap();
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        fcinfo.set_arg(0, Datum::from_usize(&mut si as *mut StringInfo as usize));
        let d = adt_char::builtins::fc_charrecv(None, &mut fcinfo).unwrap();
        assert!(
            d.as_char() == payload[0] as i8,
            "fc_charrecv DIVERGENCE payload={payload:?}: got {}",
            d.as_char()
        );
    }

    // hashchar wrappers: EXECUTION-ONLY coverage pinned against the shipped
    // hashfn kernel (kernel differential proofs live in proofs/hash-rows;
    // char.c has no hash body to diff — hashchar lives in hashfn.c).
    let ch = payload.first().copied().unwrap_or(0) as i8;
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_char(ch));
    let d = adt_char::builtins::fc_hashchar(None, &mut fcinfo).unwrap();
    assert!(
        d.as_u32() == hashfn::hash_bytes_uint32(ch as i32 as u32),
        "fc_hashchar skew vs kernel: ch={ch}"
    );
    let seed = payload.get(1).copied().unwrap_or(0) as i64;
    let mut fcinfo = LocalFcinfo::<2>::new(0);
    fcinfo.set_arg(0, Datum::from_char(ch));
    fcinfo.set_arg(1, Datum::from_i64(seed));
    let d = adt_char::builtins::fc_hashcharextended(None, &mut fcinfo).unwrap();
    assert!(
        d.as_u64() == hashfn::hash_bytes_uint32_extended(ch as i32 as u32, seed as u64),
        "fc_hashcharextended skew vs kernel: ch={ch} seed={seed}"
    );
}

// ---------------------------------------------------------------------------
// Target: bool_diff — adt/bool vs vendored bool.c.
// ---------------------------------------------------------------------------
//
// Input layout: [selector][payload...]:
//   sel % 5 == 0: boolin hard + soft error paths + fc_boolin — payload =
//                 cstring text (UTF-8, NUL-free; the shipped entry is &str).
//   sel % 5 == 1: parse_bool_with_len on raw bytes (incl. non-UTF8/NUL) +
//                 parse_bool when NUL-free UTF-8.
//   sel % 5 == 2: out family on payload[0]&1: boolout, fc_boolout,
//                 booltext, fc_booltext, boolsend, fc_boolsend, boolrecv
//                 byte semantics + fc_boolrecv-equivalent wire parity.
//   sel % 5 == 3: comparison + statefunc ops on payload[0]&1, payload[1]&1
//                 + fc_ wrappers.
//   sel % 5 == 4: aggregate sequence: bool_accum / bool_accum_inv /
//                 bool_alltrue / bool_anytrue driven op-by-op from payload
//                 bytes against the verbatim C state machine, plus the
//                 fc_bool_accum aggcontext path and the two error arms
//                 (inv-on-NULL parity; non-agg-context pinned message).

pub fn bool_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    // init_seams is real startup surface (scalar_seams registration); run it
    // once and pin that the installed seam answers like parse_bool.
    static SEAMS: std::sync::Once = std::sync::Once::new();
    SEAMS.call_once(|| {
        adt_bool::init_seams();
        assert!(scalar_seams::parse_bool::call("yes") == Some(true), "installed seam skew");
    });
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    if payload.len() > 1024 {
        return;
    }
    match sel % 5 {
        0 => bool_in_case(payload),
        1 => bool_parse_case(payload),
        2 => bool_out_case(payload),
        3 => bool_cmp_case(payload),
        _ => bool_agg_case(payload),
    }
}

fn bool_in_case(payload: &[u8]) {
    if payload.contains(&0) {
        return;
    }
    let Ok(s) = std::str::from_utf8(payload) else {
        return;
    };
    let cs = CString::new(payload).unwrap();
    let mut cres: i32 = 0;
    let cerr = unsafe { pg_diff_boolin(cs.as_ptr(), &mut cres) };
    let cerrcode = c_errcode();

    // Hard path.
    match adt_bool::boolin(s, None) {
        Ok(v) => assert!(
            cerr == 0 && (cres != 0) == v,
            "boolin DIVERGENCE input={s:?}: C=(err {cerr}, {cres}) Rust=Ok({v})"
        ),
        Err(e) => {
            let rclass = if e.sqlstate() == ERRCODE_INVALID_TEXT_REPRESENTATION {
                C_ERR_INVALID_TEXT
            } else {
                99
            };
            assert!(
                cerr == 1 && cerrcode == rclass,
                "boolin DIVERGENCE input={s:?}: C=(err {cerr} code {cerrcode}) Rust=Err({rclass} {})",
                e.message()
            );
        }
    }

    // Soft path: same verdict, no Err.
    let mut soft = SoftErrorContext::new(false);
    let sv = adt_bool::boolin(s, Some(&mut soft)).expect("soft boolin never hard-errors");
    if cerr == 0 {
        assert!(
            !soft.error_occurred() && sv == (cres != 0),
            "boolin(soft) DIVERGENCE input={s:?}: C ok {cres} Rust=({sv}, err={})",
            soft.error_occurred()
        );
    } else {
        assert!(
            soft.error_occurred(),
            "boolin(soft) DIVERGENCE input={s:?}: C err, Rust ok({sv})"
        );
    }

    // fmgr wrapper (hard path only: no soft context armed).
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_usize(cs.as_ptr() as usize));
    let fr = adt_bool::builtins::fc_boolin(None, &mut fcinfo);
    assert!(
        fr.is_ok() == (cerr == 0),
        "fc_boolin error-plane DIVERGENCE input={s:?}"
    );
    if let Ok(d) = fr {
        assert!(d.as_bool() == (cres != 0), "fc_boolin value DIVERGENCE input={s:?}");
    }
}

fn bool_parse_case(payload: &[u8]) {
    // Raw-byte plane: parse_bool_with_len takes arbitrary bytes on both
    // sides (C reads through pg_strncasecmp, which stops at NUL; the Rust
    // port mirrors that exactly). C requires a readable *value even for
    // len 0 (it reads the cstring NUL there) — the sentinel provides it.
    let len = payload.len().min(512);
    let mut cbytes = payload[..len].to_vec();
    cbytes.push(0);
    let mut cres: i32 = 0;
    let cok =
        unsafe { pg_diff_parse_bool_with_len(cbytes.as_ptr().cast(), len, &mut cres) } != 0;
    let r = adt_bool::parse_bool_with_len(&payload[..len]);
    match r {
        Some(v) => assert!(
            cok && v == (cres != 0),
            "parse_bool_with_len DIVERGENCE input={:?}: C=({cok},{cres}) Rust=Some({v})",
            &payload[..len]
        ),
        None => assert!(
            !cok,
            "parse_bool_with_len DIVERGENCE input={:?}: C=({cok},{cres}) Rust=None",
            &payload[..len]
        ),
    }

    // &str plane when representable.
    if !payload[..len].contains(&0) {
        if let Ok(s) = std::str::from_utf8(&payload[..len]) {
            assert!(
                adt_bool::parse_bool(s) == r,
                "parse_bool/&str DIVERGENCE input={s:?}"
            );
        }
    }
}

fn bool_out_case(payload: &[u8]) {
    let Some(&b0) = payload.first() else {
        return;
    };
    let v = b0 & 1 != 0;

    // boolout single-byte image.
    let mut cbuf = [0 as c_char; 2];
    unsafe { pg_diff_boolout(v as i32, cbuf.as_mut_ptr()) };
    let r = adt_bool::boolout(v);
    assert!(
        cbuf[0] as u8 == r && cbuf[1] == 0,
        "boolout DIVERGENCE v={v}: C={} Rust={r}",
        cbuf[0] as u8
    );
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_bool(v));
    let d = adt_bool::builtins::fc_boolout(None, &mut fcinfo).unwrap();
    // SAFETY: fc_boolout returns a NUL-terminated cstring datum.
    let s = unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const c_char) };
    assert!(s.to_bytes() == [r], "fc_boolout DIVERGENCE v={v}");

    // booltext image.
    let mut ctbuf = [0 as c_char; 8];
    let ctlen = unsafe { pg_diff_booltext(v as i32, ctbuf.as_mut_ptr()) } as usize;
    let ctimg: Vec<u8> = ctbuf[..ctlen].iter().map(|&c| c as u8).collect();
    let ctx = MemoryContext::new_bump("bool_diff");
    let t = adt_bool::booltext(ctx.mcx(), v).unwrap();
    assert!(
        t.data() == &ctimg[..],
        "booltext DIVERGENCE v={v}: C={ctimg:?} Rust={:?}",
        t.data()
    );
    let d = direct_function_call1_coll_in(
        adt_bool::builtins::fc_booltext,
        0,
        ctx.mcx(),
        Datum::from_bool(v),
    )
    .unwrap();
    // SAFETY: fc_booltext returns a live 4B-header text varlena in ctx.
    let vr = unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    assert!(vr.data() == &ctimg[..], "fc_booltext DIVERGENCE v={v}");

    // boolsend wire image (C body is pq_sendbyte(arg1 ? 1 : 0)).
    let b = adt_bool::boolsend(ctx.mcx(), v).unwrap();
    let expect = expected_send1_image(v as u8);
    assert!(
        b.as_bytes() == expect,
        "boolsend IMAGE DIVERGENCE v={v}: expected={expect:?} Rust={:?}",
        b.as_bytes()
    );
    let d = direct_function_call1_coll_in(
        adt_bool::builtins::fc_boolsend,
        0,
        ctx.mcx(),
        Datum::from_bool(v),
    )
    .unwrap();
    // SAFETY: fc_boolsend returns a live 4B-header bytea in ctx.
    let vr = unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    assert!(vr.data() == [v as u8], "fc_boolsend DIVERGENCE v={v}");

    // boolrecv byte semantics: any nonzero byte is true.
    let ext = payload.get(1).copied().unwrap_or(b0);
    let cr = unsafe { pg_diff_boolrecv(ext as i32) } != 0;
    let mut si = StringInfo::with_capacity_in(ctx.mcx(), 2).unwrap();
    si.append_bytes(&[ext]).unwrap();
    let rr = adt_bool::boolrecv(&mut si).unwrap();
    assert!(cr == rr, "boolrecv DIVERGENCE ext={ext}: C={cr} Rust={rr}");
    let empty_err = {
        let mut si0 = StringInfo::with_capacity_in(ctx.mcx(), 1).unwrap();
        adt_bool::boolrecv(&mut si0).is_err()
    };
    assert!(empty_err, "boolrecv on empty message must error");

    // fc_boolrecv through the recv ABI (arg0 = live StringInfo pointer).
    let mut si = StringInfo::with_capacity_in(ctx.mcx(), 2).unwrap();
    si.append_bytes(&[ext]).unwrap();
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_usize(&mut si as *mut StringInfo as usize));
    let d = adt_bool::builtins::fc_boolrecv(None, &mut fcinfo).unwrap();
    assert!(d.as_bool() == cr, "fc_boolrecv DIVERGENCE ext={ext}: C={cr} Rust={}", d.as_bool());

    // hashbool wrappers + cores: EXECUTION-ONLY coverage pinned against the
    // shipped hashfn kernel (the hash kernels' differential proofs live in
    // proofs/hash-rows; there is no bool.c hash counterpart to diff here).
    let hv = adt_bool::hashbool(v);
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_bool(v));
    let d = adt_bool::builtins::fc_hashbool(None, &mut fcinfo).unwrap();
    assert!(d.as_u32() == hv, "fc_hashbool skew vs core: v={v}");
    let seed = u64::from(ext) << 1;
    let hx = adt_bool::hashboolextended(v, seed);
    let mut fcinfo = LocalFcinfo::<2>::new(0);
    fcinfo.set_arg(0, Datum::from_bool(v));
    fcinfo.set_arg(1, Datum::from_u64(seed));
    let d = adt_bool::builtins::fc_hashboolextended(None, &mut fcinfo).unwrap();
    assert!(d.as_u64() == hx, "fc_hashboolextended skew vs core: v={v} seed={seed}");
}

fn bool_cmp_case(payload: &[u8]) {
    if payload.len() < 2 {
        return;
    }
    let (a, b) = (payload[0] & 1 != 0, payload[1] & 1 != 0);
    type RustCmp = fn(bool, bool) -> bool;
    type CCmp = unsafe extern "C" fn(i32, i32) -> i32;
    type FcCmp = types_fmgr::PGFunction;
    let table: &[(&str, RustCmp, CCmp, FcCmp)] = &[
        ("booleq", adt_bool::booleq, pg_diff_booleq, adt_bool::builtins::fc_booleq),
        ("boolne", adt_bool::boolne, pg_diff_boolne, adt_bool::builtins::fc_boolne),
        ("boollt", adt_bool::boollt, pg_diff_boollt, adt_bool::builtins::fc_boollt),
        ("boolgt", adt_bool::boolgt, pg_diff_boolgt, adt_bool::builtins::fc_boolgt),
        ("boolle", adt_bool::boolle, pg_diff_boolle, adt_bool::builtins::fc_boolle),
        ("boolge", adt_bool::boolge, pg_diff_boolge, adt_bool::builtins::fc_boolge),
        (
            "booland_statefunc",
            adt_bool::booland_statefunc,
            pg_diff_booland_statefunc,
            adt_bool::builtins::fc_booland_statefunc,
        ),
        (
            "boolor_statefunc",
            adt_bool::boolor_statefunc,
            pg_diff_boolor_statefunc,
            adt_bool::builtins::fc_boolor_statefunc,
        ),
    ];
    for (name, rf, cf, fc) in table {
        let rv = rf(a, b);
        let cv = unsafe { cf(a as i32, b as i32) } != 0;
        assert!(rv == cv, "{name} DIVERGENCE a={a} b={b}: C={cv} Rust={rv}");
        let mut fcinfo = LocalFcinfo::<2>::new(0);
        fcinfo.set_arg(0, Datum::from_bool(a));
        fcinfo.set_arg(1, Datum::from_bool(b));
        let d = fc(None, &mut fcinfo).unwrap();
        assert!(d.as_bool() == cv, "fc_{name} DIVERGENCE a={a} b={b}");
    }
}

fn bool_agg_case(payload: &[u8]) {
    use adt_bool::BoolAggState;

    // Core-level state machine parity, op-by-op. Byte bits: 0 = accum vs
    // accum_inv, 1 = value NULL flag, 2 = value.
    let ops = &payload[..payload.len().min(64)];
    let mut rstate: Option<BoolAggState> = None;
    let mut cstate = CBoolAggState::default();
    let mut cstate_null = true;
    for &op in ops {
        let inverse = op & 1 != 0;
        let val = if op & 2 != 0 { None } else { Some(op & 4 != 0) };
        let (cvn, cv) = match val {
            None => (1, 0),
            Some(v) => (0, v as i32),
        };
        if inverse {
            let cerr = unsafe {
                pg_diff_bool_accum_inv(cstate_null as i32, &mut cstate, cvn, cv)
            };
            match adt_bool::bool_accum_inv(rstate, val) {
                Ok(st) => {
                    assert!(cerr == 0, "bool_accum_inv DIVERGENCE ops={ops:?}: C err, Rust ok");
                    rstate = Some(st);
                }
                Err(_) => {
                    assert!(
                        cerr != 0 && c_errcode() == 5,
                        "bool_accum_inv DIVERGENCE ops={ops:?}: C ok, Rust err"
                    );
                    return; /* both errored; sequence over */
                }
            }
        } else {
            unsafe { pg_diff_bool_accum(cstate_null as i32, &mut cstate, cvn, cv) };
            cstate_null = false;
            rstate = Some(adt_bool::bool_accum(rstate, val));
        }
        let r = rstate.as_ref().unwrap();
        assert!(
            r.aggcount == cstate.aggcount && r.aggtrue == cstate.aggtrue,
            "BoolAggState DIVERGENCE ops={ops:?}: C=({},{}) Rust=({},{})",
            cstate.aggcount,
            cstate.aggtrue,
            r.aggcount,
            r.aggtrue
        );
    }

    // Finals (incl. the NULL-state and zero-count NULL returns).
    let call_finals = |st: Option<&BoolAggState>, cnull: bool, cst: &CBoolAggState| {
        let call = |any: bool| -> i32 {
            match if any {
                adt_bool::bool_anytrue(st)
            } else {
                adt_bool::bool_alltrue(st)
            } {
                None => -1,
                Some(v) => v as i32,
            }
        };
        let c_all = unsafe { pg_diff_bool_alltrue(cnull as i32, cst) };
        let c_any = unsafe { pg_diff_bool_anytrue(cnull as i32, cst) };
        assert!(
            call(false) == c_all && call(true) == c_any,
            "bool final DIVERGENCE: C=({c_all},{c_any}) Rust=({},{})",
            call(false),
            call(true)
        );
    };
    call_finals(rstate.as_ref(), cstate_null, &cstate);
    call_finals(None, true, &CBoolAggState::default());

    // fmgr aggregate plumbing: replay the same op sequence through
    // fc_bool_accum/fc_bool_accum_inv with a real AggStateNode, then the
    // finals; state Datum threads through as C's PG_RETURN_POINTER does.
    let mut node = AggStateNode::new(MemoryContext::new_bump("bool-aggctx"));
    let mut st = Datum::null();
    let mut st_null = true;
    for &op in ops {
        let inverse = op & 1 != 0;
        let val = if op & 2 != 0 { None } else { Some(op & 4 != 0) };
        let mut fcinfo = LocalFcinfo::<2>::new(0);
        fcinfo.context = node.fm_node_ptr();
        if !st_null {
            fcinfo.set_arg(0, st);
        } else {
            fcinfo.set_arg_null(0);
        }
        match val {
            Some(v) => fcinfo.set_arg(1, Datum::from_bool(v)),
            None => fcinfo.set_arg_null(1),
        }
        let r = if inverse {
            adt_bool::builtins::fc_bool_accum_inv(None, &mut fcinfo)
        } else {
            Ok(adt_bool::builtins::fc_bool_accum(None, &mut fcinfo).unwrap())
        };
        match r {
            Ok(d) => {
                st = d;
                st_null = false;
            }
            Err(_) => {
                assert!(st_null, "fc_bool_accum_inv errored with non-NULL state");
                break;
            }
        }
    }
    if !st_null {
        // SAFETY: the state datum is the aggcontext-lived BoolAggState the
        // transfn chain returned.
        let got = unsafe { *(st.as_usize() as *const BoolAggState) };
        let want = rstate.unwrap_or_default();
        // The fc chain stops at the first inv-on-NULL error like the core
        // chain does, so states agree whenever both ran to completion.
        if rstate.is_some() {
            assert!(
                got == want,
                "fc agg state DIVERGENCE ops={ops:?}: fc=({},{}) core=({},{})",
                got.aggcount,
                got.aggtrue,
                want.aggcount,
                want.aggtrue
            );
        }
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        fcinfo.set_arg(0, st);
        let all = adt_bool::builtins::fc_bool_alltrue(None, &mut fcinfo).unwrap();
        let all = if fcinfo.isnull { -1 } else { all.as_bool() as i32 };
        let c_all = unsafe { pg_diff_bool_alltrue(0, &cstate) };
        if rstate.is_some() {
            assert!(all == c_all, "fc_bool_alltrue DIVERGENCE ops={ops:?}");
        }
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        fcinfo.set_arg(0, st);
        let any = adt_bool::builtins::fc_bool_anytrue(None, &mut fcinfo).unwrap();
        let any = if fcinfo.isnull { -1 } else { any.as_bool() as i32 };
        let c_any = unsafe { pg_diff_bool_anytrue(0, &cstate) };
        if rstate.is_some() {
            assert!(any == c_any, "fc_bool_anytrue DIVERGENCE ops={ops:?}");
        }
    }

    // Deterministic first-op arms through the fc chain: inv on NULL state
    // (C elog parity, bool.c bool_accum_inv NULL-state ereport) and the two
    // finals on NULL state (C returns SQL NULL).
    {
        let mut agg = AggStateNode::new(MemoryContext::new_bump("bool-aggctx-inv"));
        let mut fcinfo = LocalFcinfo::<2>::new(0);
        fcinfo.context = agg.fm_node_ptr();
        fcinfo.set_arg_null(0);
        fcinfo.set_arg(1, Datum::from_bool(true));
        let e = adt_bool::builtins::fc_bool_accum_inv(None, &mut fcinfo)
            .expect_err("fc_bool_accum_inv on NULL state must error");
        assert!(
            e.message() == "bool_accum_inv called with NULL state",
            "inv-on-NULL message drifted: {}",
            e.message()
        );
        for anyf in [false, true] {
            let f = if anyf {
                adt_bool::builtins::fc_bool_anytrue
            } else {
                adt_bool::builtins::fc_bool_alltrue
            };
            let mut fcinfo = LocalFcinfo::<1>::new(0);
            fcinfo.set_arg_null(0);
            let d = f(None, &mut fcinfo).unwrap();
            let _ = d;
            assert!(fcinfo.isnull, "agg final on NULL state must be SQL NULL");
        }
    }

    // Non-aggregate-context arm: pinned message parity with C's
    // AggCheckCallContext elog (environment check, message asserted here).
    let mut fcinfo = LocalFcinfo::<2>::new(0);
    fcinfo.set_arg_null(0);
    fcinfo.set_arg(1, Datum::from_bool(true));
    let e = adt_bool::builtins::fc_bool_accum(None, &mut fcinfo).unwrap_err();
    assert!(
        e.message() == "aggregate function called in non-aggregate context",
        "fc_bool_accum non-agg-context message drifted: {}",
        e.message()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOOL_STR_CORPUS: &[&str] = &[
        "true", "TRUE", "tRuE", "t", "tr", "tru", "yes", "y", "ye", "on", "ON", "oN", "1",
        "false", "FALSE", "f", "fa", "fal", "fals", "no", "n", "off", "of", "0", " true ",
        "\t\n on \r", "  0  ", "o", "O", "", " ", "truex", "truee", "yess", "offf", "onn",
        "11", "00", "2", "tru e", "-", "x", "\u{e9}", "TrUe   ", "   fAlSe",
    ];

    #[test]
    fn bool_in_corpus() {
        let _serial = crate::c_oracle_serial();
        for s in BOOL_STR_CORPUS {
            for sel in [0u8, 1] {
                let mut d = vec![sel];
                d.extend_from_slice(s.as_bytes());
                bool_diff(&d);
            }
        }
    }

    #[test]
    fn bool_parse_raw_bytes() {
        let _serial = crate::c_oracle_serial();
        // Non-UTF8 and interior-NUL shapes only reach the _with_len plane.
        for raw in [
            &b"t\xff"[..],
            &b"\xfftrue"[..],
            &b"t\x00rue"[..],
            &b"on\x00x"[..],
            &b"\x00"[..],
            &b"off\xc3"[..],
        ] {
            let mut d = vec![1u8];
            d.extend_from_slice(raw);
            bool_diff(&d);
        }
    }

    #[test]
    fn bool_out_cmp_agg_sweep() {
        let _serial = crate::c_oracle_serial();
        for b0 in [0u8, 1, 2, 255] {
            for b1 in [0u8, 1, 3] {
                bool_diff(&[2, b0, b1]);
                bool_diff(&[3, b0, b1]);
            }
        }
        // agg sequences: all op-bit combos, incl. inv-on-NULL first (op&1).
        for ops in [
            &[0u8, 4, 4, 1, 5][..],
            &[4, 4, 4][..],
            &[2, 2][..],
            &[1][..],
            &[5, 4][..],
            &[][..],
            &[4, 5, 1, 0, 2, 6, 7, 3][..],
        ] {
            let mut d = vec![4u8];
            d.extend_from_slice(ops);
            bool_diff(&d);
        }
    }

    #[test]
    fn char_corpus() {
        let _serial = crate::c_oracle_serial();
        // charin text shapes.
        for s in [
            "", "a", "A", "\\", "\\0", "\\00", "\\000", "\\377", "\\400", "\\777", "\\778",
            "\\080", "\\123", "ab", "abc", "abcd", "\\1234", "\u{e9}", "\x7f",
        ] {
            let mut d = vec![0u8];
            d.extend_from_slice(s.as_bytes());
            char_diff(&d);
        }
        // out/cmp/int/text/recv sweeps over interesting byte values.
        for b in [0u8, 1, 0x41, 0x7f, 0x80, 0xff, 0xc3] {
            char_diff(&[1, b]);
            char_diff(&[2, b, b.wrapping_add(1)]);
            char_diff(&[3, b, 0, 0, 0]);
            char_diff(&[5, b]);
        }
        for arg in [-129i32, -128, -1, 0, 127, 128, i32::MIN, i32::MAX] {
            let mut d = vec![3u8, 0];
            d.extend_from_slice(&arg.to_le_bytes());
            // payload[0] is the chartoi4 byte; then 4 le bytes for i4tochar.
            let mut d2 = vec![3u8];
            d2.extend_from_slice(&arg.to_le_bytes());
            char_diff(&d2);
            let _ = d;
        }
        // text_char raw bytes (incl. NUL + non-UTF8).
        for raw in [&b""[..], &b"\x00"[..], &b"\\123"[..], &b"\\12\x00"[..], &b"\xff\xfe"[..]] {
            let mut d = vec![4u8];
            d.extend_from_slice(raw);
            char_diff(&d);
        }
        char_diff(&[5]); /* empty recv message */
    }

    /// Comparator-is-load-bearing witness: a deliberately skewed "oracle"
    /// value must make the char comparison fail.
    #[test]
    #[should_panic(expected = "charin DIVERGENCE")]
    fn char_comparator_must_fail_on_skew() {
        let _serial = crate::c_oracle_serial();
        let payload = b"a";
        let cs = CString::new(&payload[..]).unwrap();
        let cval = unsafe { pg_diff_charin(cs.as_ptr()) } as i8;
        let skewed = cval.wrapping_add(1);
        let r = adt_char::charin(payload);
        assert!(
            r == skewed,
            "charin DIVERGENCE input={payload:?}: C={skewed} Rust={r}"
        );
    }

    /// Same witness for the bool comparator, on the error plane.
    #[test]
    #[should_panic(expected = "boolin DIVERGENCE")]
    fn bool_comparator_must_fail_on_skew() {
        let _serial = crate::c_oracle_serial();
        let cs = CString::new("true").unwrap();
        let mut cres: i32 = 0;
        let cerr = unsafe { pg_diff_boolin(cs.as_ptr(), &mut cres) };
        // Skew: pretend C errored.
        let cerr = 1 - cerr.min(1);
        match adt_bool::boolin("true", None) {
            Ok(v) => assert!(
                cerr == 0 && (cres != 0) == v,
                "boolin DIVERGENCE input=\"true\": C=(err {cerr}, {cres}) Rust=Ok({v})"
            ),
            Err(_) => unreachable!(),
        }
    }
}
