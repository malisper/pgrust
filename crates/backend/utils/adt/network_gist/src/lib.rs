//! network_gist.c: GiST inet_ops. GistInetKey rides its C on-disk image —
//! 1B short varlena header, family/minbits/commonbits, 4/16-byte address.
#![allow(non_upper_case_globals)]

use ::adt_network::{bitncmp, bitncommon, InetRef, InetValue, PGSQL_AF_INET6};
use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use ::types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};

const INETSTRAT_OVERLAPS: u16 = 3;
const INETSTRAT_EQ: u16 = 18;
const INETSTRAT_NE: u16 = 19;
const INETSTRAT_LT: u16 = 20;
const INETSTRAT_LE: u16 = 21;
const INETSTRAT_GT: u16 = 22;
const INETSTRAT_GE: u16 = 23;
const INETSTRAT_SUB: u16 = 24;
const INETSTRAT_SUBEQ: u16 = 25;
const INETSTRAT_SUP: u16 = 26;
const INETSTRAT_SUPEQ: u16 = 27;

const GK_HDR: usize = 1;
const GK_ADDR_OFF: usize = 4;

fn ip_family_maxbits(family: u8) -> i32 {
    if family == PGSQL_AF_INET6 {
        128
    } else {
        32
    }
}

fn family_addrsize(family: u8) -> usize {
    if family == PGSQL_AF_INET6 {
        16
    } else {
        4
    }
}

/// Borrowed view of a GistInetKey image — a bare pointer like C's
/// DatumGetInetKeyP; field offsets are the fixed C struct layout, never the
/// varlena header (byte counts derive from the family field).
#[derive(Clone, Copy)]
pub struct GkRef<'a>(*const u8, core::marker::PhantomData<&'a [u8]>);

impl<'a> GkRef<'a> {
    pub fn from_image(img: &'a [u8]) -> GkRef<'a> {
        debug_assert!(img.len() >= GK_ADDR_OFF + 4);
        GkRef(img.as_ptr(), core::marker::PhantomData)
    }

    // SAFETY: d points at a live GistInetKey image (1B-header varlena we
    // wrote via gk_image; the gist core round-trips it byte-identically).
    unsafe fn at(d: Datum) -> GkRef<'a> {
        let p = d.as_usize() as *const u8;
        debug_assert!(::types_tuple::varatt::varatt_is_1b(p));
        GkRef(p, core::marker::PhantomData)
    }

    fn family(self) -> u8 {
        // SAFETY: construction contract — a live GistInetKey image.
        unsafe { *self.0.add(GK_HDR) }
    }
    fn minbits(self) -> i32 {
        // SAFETY: as family().
        unsafe { *self.0.add(GK_HDR + 1) as i32 }
    }
    fn commonbits(self) -> i32 {
        // SAFETY: as family().
        unsafe { *self.0.add(GK_HDR + 2) as i32 }
    }
    fn addr(self) -> &'a [u8] {
        // SAFETY: image carries addrsize() address bytes at GK_ADDR_OFF.
        unsafe { core::slice::from_raw_parts(self.0.add(GK_ADDR_OFF), self.addrsize()) }
    }
    fn addrsize(self) -> usize {
        family_addrsize(self.family())
    }
    fn maxbits(self) -> i32 {
        ip_family_maxbits(self.family())
    }
}

/// build_inet_union_key: 1B-header image, address zero-padded past
/// commonbits (C pallocs zeroed and masks the last partial byte).
pub fn gk_image(family: u8, minbits: i32, commonbits: i32, addr: &[u8]) -> ([u8; 20], usize) {
    let mut img = [0u8; 20];
    let len = GK_ADDR_OFF + family_addrsize(family);
    img[0] = ((len << 1) | 1) as u8;
    img[GK_HDR] = family;
    img[GK_HDR + 1] = minbits as u8;
    img[GK_HDR + 2] = commonbits as u8;
    let nbytes = (commonbits as usize + 7) / 8;
    img[GK_ADDR_OFF..GK_ADDR_OFF + nbytes].copy_from_slice(&addr[..nbytes]);
    if commonbits % 8 != 0 {
        img[GK_ADDR_OFF + commonbits as usize / 8] &= !(0xFFu8 >> (commonbits % 8));
    }
    (img, len)
}

// SAFETY: gist fmgr protocol — arg pointers live for the call.
unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    &*(fcinfo.arg(i).as_usize() as *const GISTENTRY)
}

fn entry_result(fcinfo: &Fcinfo, e: &GISTENTRY) -> PgResult<Datum> {
    let bytes = unsafe {
        core::slice::from_raw_parts(
            (e as *const GISTENTRY).cast::<u8>(),
            core::mem::size_of::<GISTENTRY>(),
        )
    };
    byref_result(fcinfo.result_mcx(), bytes)
}

fn fc_inet_gist_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let ent = unsafe { entry_arg(fcinfo, 0) };
    let query_pv = unsafe { fcinfo.arg_varlena_packed(1) }?;
    let query = InetRef::from_payload(query_pv.data());
    let strategy = fcinfo.arg(2).as_u16();
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: key datum per protocol; recheck out-param live in caller frame.
    let key = unsafe { GkRef::at(ent.key) };
    unsafe { *recheck = false };
    Ok(Datum::from_bool(consistent_internal(
        key,
        query,
        strategy,
        ent.page_is_leaf,
    )))
}

pub fn consistent_internal(key: GkRef<'_>, query: InetRef<'_>, strategy: u16, leaf: bool) -> bool {
    if key.family() == 0 {
        debug_assert!(!leaf);
        return true;
    }

    if key.family() != query.family {
        return match strategy {
            INETSTRAT_LT | INETSTRAT_LE => key.family() < query.family,
            INETSTRAT_GE | INETSTRAT_GT => key.family() > query.family,
            INETSTRAT_NE => true,
            _ => false,
        };
    }

    let qbits = query.bits as i32;
    match strategy {
        INETSTRAT_SUB => {
            if leaf && key.minbits() <= qbits {
                return false;
            }
        }
        INETSTRAT_SUBEQ => {
            if leaf && key.minbits() < qbits {
                return false;
            }
        }
        INETSTRAT_SUPEQ | INETSTRAT_EQ => {
            if key.minbits() > qbits {
                return false;
            }
        }
        INETSTRAT_SUP => {
            if key.minbits() >= qbits {
                return false;
            }
        }
        _ => {}
    }

    let minbits = key.commonbits().min(key.minbits()).min(qbits);
    let order = bitncmp(key.addr(), query.addr, minbits);

    match strategy {
        INETSTRAT_SUB | INETSTRAT_SUBEQ | INETSTRAT_OVERLAPS | INETSTRAT_SUPEQ
        | INETSTRAT_SUP => return order == 0,
        INETSTRAT_LT | INETSTRAT_LE => {
            if order > 0 {
                return false;
            }
            if order < 0 || !leaf {
                return true;
            }
        }
        INETSTRAT_EQ => {
            if order != 0 {
                return false;
            }
            if !leaf {
                return true;
            }
        }
        INETSTRAT_GE | INETSTRAT_GT => {
            if order < 0 {
                return false;
            }
            if order > 0 || !leaf {
                return true;
            }
        }
        INETSTRAT_NE => {
            if order != 0 || !leaf {
                return true;
            }
        }
        _ => {}
    }

    debug_assert!(leaf);

    match strategy {
        INETSTRAT_LT | INETSTRAT_LE => {
            if key.minbits() < qbits {
                return true;
            }
            if key.minbits() > qbits {
                return false;
            }
        }
        INETSTRAT_EQ => {
            if key.minbits() != qbits {
                return false;
            }
        }
        INETSTRAT_GE | INETSTRAT_GT => {
            if key.minbits() > qbits {
                return true;
            }
            if key.minbits() < qbits {
                return false;
            }
        }
        INETSTRAT_NE => {
            if key.minbits() != qbits {
                return true;
            }
        }
        _ => {}
    }

    let order = bitncmp(key.addr(), query.addr, key.maxbits());

    match strategy {
        INETSTRAT_LT => order < 0,
        INETSTRAT_LE => order <= 0,
        INETSTRAT_EQ => order == 0,
        INETSTRAT_GE => order >= 0,
        INETSTRAT_GT => order > 0,
        INETSTRAT_NE => order != 0,
        _ => panic!("unknown strategy for inet GiST"),
    }
}

struct UnionParams {
    minfamily: u8,
    maxfamily: u8,
    minbits: i32,
    commonbits: i32,
}

fn calc_inet_union_params<'a>(keys: impl Iterator<Item = GkRef<'a>>) -> UnionParams {
    let mut it = keys;
    let first = it.next().expect("union of zero keys");
    let mut p = UnionParams {
        minfamily: first.family(),
        maxfamily: first.family(),
        minbits: first.minbits(),
        commonbits: first.commonbits(),
    };
    let addr = first.addr();
    for tmp in it {
        p.minfamily = p.minfamily.min(tmp.family());
        p.maxfamily = p.maxfamily.max(tmp.family());
        p.minbits = p.minbits.min(tmp.minbits());
        p.commonbits = p.commonbits.min(tmp.commonbits());
        if p.commonbits > 0 {
            p.commonbits = bitncommon(addr, tmp.addr(), p.commonbits);
        }
    }
    if p.minfamily != p.maxfamily {
        p.minbits = 0;
        p.commonbits = 0;
    }
    p
}

fn fc_inet_gist_union(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let n = entryvec.n as usize;
    let p = calc_inet_union_params(
        entryvec.vector[..n]
            .iter()
            .map(|e| unsafe { GkRef::at(e.key) }),
    );
    let family = if p.minfamily != p.maxfamily {
        0
    } else {
        p.minfamily
    };
    // SAFETY: as above.
    let addr = unsafe { GkRef::at(entryvec.vector[0].key) }.addr();
    let (img, len) = gk_image(family, p.minbits, p.commonbits, addr);
    byref_result(fcinfo.result_mcx(), &img[..len])
}

fn fc_inet_gist_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if !entry.leafkey {
        return Ok(fcinfo.arg(0));
    }
    let key = if entry.key.as_usize() != 0 {
        // SAFETY: non-null leaf key is an inet varlena (never external).
        let pv = unsafe { ::types_fmgr::PackedVarlena::from_ptr(entry.key.as_usize() as *const u8) };
        let ip = InetRef::from_payload(pv.data());
        let mut img = [0u8; 20];
        let len = GK_ADDR_OFF + ip.addrsize();
        img[0] = ((len << 1) | 1) as u8;
        img[GK_HDR] = ip.family;
        img[GK_HDR + 1] = ip.bits;
        img[GK_HDR + 2] = ip.maxbits();
        img[GK_ADDR_OFF..GK_ADDR_OFF + ip.addrsize()]
            .copy_from_slice(&ip.addr[..ip.addrsize()]);
        byref_result(fcinfo.result_mcx(), &img[..len])?
    } else {
        Datum::null()
    };
    let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
    entry_result(fcinfo, &retval)
}

fn fc_inet_gist_fetch(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let key = unsafe { GkRef::at(entry.key) };
    let mut ipaddr = [0u8; 16];
    ipaddr[..key.addrsize()].copy_from_slice(&key.addr()[..key.addrsize()]);
    let v = InetValue {
        family: key.family(),
        bits: key.minbits() as u8,
        ipaddr,
    };
    let (img, len) = v.image();
    let dst = byref_result(fcinfo.result_mcx(), &img[..len])?;
    let retval = GISTENTRY::init(dst, entry.offset, false, entry.page_is_leaf);
    entry_result(fcinfo, &retval)
}

fn fc_inet_gist_penalty(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origent = unsafe { entry_arg(fcinfo, 0) };
    let newent = unsafe { entry_arg(fcinfo, 1) };
    let penalty = fcinfo.arg(2).as_usize() as *mut f32;
    let orig = unsafe { GkRef::at(origent.key) };
    let new = unsafe { GkRef::at(newent.key) };
    // SAFETY: penalty out-param live in the caller frame.
    unsafe { *penalty = penalty_internal(orig, new) };
    Ok(fcinfo.arg(2))
}

// bitncommon == min(n, leading bits in common); the xor/clz form drops the
// per-bit reduction loop (penalty is per-tuple on gist build/insert descent).
fn common_bits_capped(l: &[u8], r: &[u8], n: i32) -> i32 {
    let mut bits = 0i32;
    for (x, y) in l.iter().zip(r) {
        let d = x ^ y;
        if d != 0 {
            bits += d.leading_zeros() as i32;
            break;
        }
        bits += 8;
    }
    bits.min(n)
}

pub fn penalty_internal(orig: GkRef<'_>, new: GkRef<'_>) -> f32 {
    if orig.family() == new.family() {
        if orig.minbits() <= new.minbits() {
            let commonbits = common_bits_capped(
                orig.addr(),
                new.addr(),
                orig.commonbits().min(new.commonbits()),
            );
            if commonbits > 0 {
                1.0f32 / commonbits as f32
            } else {
                2.0
            }
        } else {
            3.0
        }
    } else {
        4.0
    }
}

fn fc_inet_gist_picksplit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };
    let maxoff = (entryvec.n - 1) as usize;
    // SAFETY: as above.
    let key_at = |i: usize| unsafe { GkRef::at(entryvec.vector[i].key) };

    v.spl_left = Vec::with_capacity(maxoff + 1);
    v.spl_right = Vec::with_capacity(maxoff + 1);

    let p = calc_inet_union_params((1..=maxoff).map(key_at));

    if p.minfamily != p.maxfamily {
        for i in 1..=maxoff {
            if key_at(i).family() != p.maxfamily {
                v.spl_left.push(i as u16);
            } else {
                v.spl_right.push(i as u16);
            }
        }
    } else {
        let maxbits = ip_family_maxbits(p.minfamily);
        let mut commonbits = p.commonbits;
        while commonbits < maxbits {
            let bitbyte = commonbits as usize / 8;
            let bitmask = 0x80u8 >> (commonbits % 8);
            v.spl_left.clear();
            v.spl_right.clear();
            for i in 1..=maxoff {
                if key_at(i).addr()[bitbyte] & bitmask == 0 {
                    v.spl_left.push(i as u16);
                } else {
                    v.spl_right.push(i as u16);
                }
            }
            if !v.spl_left.is_empty() && !v.spl_right.is_empty() {
                break;
            }
            commonbits += 1;
        }
        if commonbits >= maxbits {
            v.spl_left.clear();
            v.spl_right.clear();
            for i in 1..=maxoff / 2 {
                v.spl_left.push(i as u16);
            }
            for i in maxoff / 2 + 1..=maxoff {
                v.spl_right.push(i as u16);
            }
        }
    }

    let pl = calc_inet_union_params(v.spl_left.iter().map(|&o| key_at(o as usize)));
    let lf = if pl.minfamily != pl.maxfamily {
        0
    } else {
        pl.minfamily
    };
    let (img, len) = gk_image(
        lf,
        pl.minbits,
        pl.commonbits,
        key_at(v.spl_left[0] as usize).addr(),
    );
    v.spl_ldatum = byref_result(fcinfo.result_mcx(), &img[..len])?;

    let pr = calc_inet_union_params(v.spl_right.iter().map(|&o| key_at(o as usize)));
    let rf = if pr.minfamily != pr.maxfamily {
        0
    } else {
        pr.minfamily
    };
    let (img, len) = gk_image(
        rf,
        pr.minbits,
        pr.commonbits,
        key_at(v.spl_right[0] as usize).addr(),
    );
    v.spl_rdatum = byref_result(fcinfo.result_mcx(), &img[..len])?;

    Ok(fcinfo.arg(1))
}

fn fc_inet_gist_same(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol; args 0/1 are key datums.
    let left = unsafe { GkRef::at(fcinfo.arg(0)) };
    let right = unsafe { GkRef::at(fcinfo.arg(1)) };
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    let r = left.family() == right.family()
        && left.minbits() == right.minbits()
        && left.commonbits() == right.commonbits()
        && left.addr()[..left.addrsize()] == right.addr()[..left.addrsize()];
    // SAFETY: result out-param live in the caller frame.
    unsafe { *result = r };
    Ok(fcinfo.arg(2))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

pub const NETWORK_GIST_BUILTINS: &[FmgrBuiltin] = &[
    b(3553, "inet_gist_consistent", 5, fc_inet_gist_consistent),
    b(3554, "inet_gist_union", 2, fc_inet_gist_union),
    b(3555, "inet_gist_compress", 1, fc_inet_gist_compress),
    b(3557, "inet_gist_penalty", 3, fc_inet_gist_penalty),
    b(3558, "inet_gist_picksplit", 2, fc_inet_gist_picksplit),
    b(3559, "inet_gist_same", 3, fc_inet_gist_same),
    b(3573, "inet_gist_fetch", 1, fc_inet_gist_fetch),
];

#[cfg(test)]
mod tests;
