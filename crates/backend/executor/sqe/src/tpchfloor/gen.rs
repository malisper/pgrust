//! Deterministic TPC-H-shaped data generator (dbgen stand-in — spec
//! cardinalities, spec key sparsity/formulas where the queries feel them:
//! orderkey 8-of-32 sparsity, custkey thirds rule, the partsupp supplier
//! formula, p_name color words, l_extendedprice = qty * p_retailprice).
//! Values are NOT bit-identical to dbgen (comments/addresses are short
//! synthetics); correctness is floor-vs-oracle over THIS data, byte-law.
//! SF100 CI cluster generation swaps dbgen in behind the same ingest seam.

use super::date_days;

pub const SEGMENTS: [&str; 5] =
    ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"];
pub const PRIORITIES: [&str; 5] =
    ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
pub const INSTRUCT: [&str; 4] =
    ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"];
pub const MODES: [&str; 7] = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];
pub const CONTAINERS: [&str; 8] =
    ["SM CASE", "SM BOX", "MED BAG", "MED BOX", "LG CASE", "LG BOX", "JUMBO PACK", "WRAP JAR"];
pub const TYPES: [&str; 6] = [
    "STANDARD ANODIZED TIN",
    "SMALL PLATED COPPER",
    "MEDIUM POLISHED BRASS",
    "ECONOMY BURNISHED STEEL",
    "PROMO BRUSHED NICKEL",
    "LARGE PLATED STEEL",
];

/// The dbgen 92-word color list (p_name = 5 of these; Q9's '%green%').
pub const COLORS: [&str; 92] = [
    "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
    "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
    "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
    "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
    "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime",
    "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin",
    "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink",
    "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon",
    "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan",
    "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow",
];

pub const NATIONS: [(&str, u8); 25] = [
    ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 4),
    ("ETHIOPIA", 0), ("FRANCE", 3), ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2),
    ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2), ("JORDAN", 4), ("KENYA", 0),
    ("MOROCCO", 0), ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3),
    ("SAUDI ARABIA", 4), ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
];
pub const REGIONS: [&str; 5] = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];

#[inline(always)]
fn splitmix(x: &mut u64) -> u64 {
    *x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

pub struct Rng(u64);
impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }
    #[inline(always)]
    pub fn next(&mut self) -> u64 {
        splitmix(&mut self.0)
    }
    /// Uniform in [lo, hi] inclusive.
    #[inline(always)]
    pub fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % (hi - lo + 1) as u64) as i64
    }
}

// ---------------------------------------------------------------------------
// The in-memory dataset (struct-of-vecs; formulaic text derived on demand
// so SF1 stays a few hundred MB).
// ---------------------------------------------------------------------------

pub struct Customers {
    pub n: u32,
    pub nationkey: Vec<u8>,
    pub segment: Vec<u8>, // index into SEGMENTS
    pub acctbal: Vec<i32>,
}

pub struct Orders {
    pub orderkey: Vec<i64>,
    pub custkey: Vec<u32>,
    pub status: Vec<u8>, // b'F' | b'O' | b'P'
    pub totalprice: Vec<i64>,
    pub orderdate: Vec<i32>,
    pub priority: Vec<u8>,
    pub clerk: Vec<u32>,
}

pub struct Lineitems {
    pub orderkey: Vec<i64>,
    pub partkey: Vec<u32>,
    pub suppkey: Vec<u32>,
    pub linenumber: Vec<u8>,
    pub quantity_c: Vec<i32>,
    pub extprice_c: Vec<i64>,
    pub discount: Vec<u8>, // hundredths 0..10
    pub tax: Vec<u8>,      // hundredths 0..8
    pub returnflag: Vec<u8>,
    pub linestatus: Vec<u8>,
    pub shipdate: Vec<i32>,
    pub commitdate: Vec<i32>,
    pub receiptdate: Vec<i32>,
    pub instruct: Vec<u8>,
    pub mode: Vec<u8>,
}

pub struct Parts {
    pub n: u32,
    pub name_words: Vec<[u8; 5]>, // indices into COLORS
    pub mfgr: Vec<u8>,            // 1..5
    pub brand: Vec<u8>,           // 11..55
    pub ptype: Vec<u8>,
    pub size: Vec<u8>,
    pub container: Vec<u8>,
    pub retail_c: Vec<i32>,
}

pub struct Partsupps {
    pub partkey: Vec<u32>,
    pub suppkey: Vec<u32>,
    pub availqty: Vec<i32>,
    pub supplycost_c: Vec<i32>,
    /// The 4 (unique) suppliers of part pk = supp4[pk-1] — lineitem picks
    /// from here so every (l_partkey, l_suppkey) has exactly one ps row.
    pub supp4: Vec<[u32; 4]>,
}

pub struct Suppliers {
    pub n: u32,
    pub nationkey: Vec<u8>,
    pub acctbal: Vec<i32>,
}

pub struct Tpch {
    pub cust: Customers,
    pub ord: Orders,
    pub li: Lineitems,
    pub part: Parts,
    pub ps: Partsupps,
    pub supp: Suppliers,
}

// Formulaic text (shared by ingest, oracle and reference — data
// derivation, not query logic).
pub fn c_name(k: u32) -> String {
    format!("Customer#{k:09}")
}
pub fn s_name(k: u32) -> String {
    format!("Supplier#{k:09}")
}
pub fn clerk_name(k: u32) -> String {
    format!("Clerk#{k:09}")
}
pub fn p_name(words: [u8; 5]) -> String {
    words.iter().map(|&w| COLORS[w as usize]).collect::<Vec<_>>().join(" ")
}
pub fn address(tag: char, k: u32) -> String {
    format!("{tag}addr-{k}")
}
pub fn phone(nation: u8, k: u32) -> String {
    format!("{}-{:07}", 10 + nation as u32, k % 10_000_000)
}
pub fn comment(tag: char, k: u64) -> String {
    format!("{tag}-comment-{k}")
}
/// dbgen retail price formula, cents.
pub fn retail_cents(partkey: u32) -> i32 {
    let pk = partkey as i64;
    (90_000 + (pk / 10) % 20_001 + 100 * (pk % 1_000)) as i32
}
/// dbgen partsupp supplier formula: the i-th (0..4) supplier of a part.
pub fn ps_suppkey(partkey: u32, i: u32, scount: u32) -> u32 {
    let s = scount as u64;
    let pk = partkey as u64 - 1;
    ((pk + i as u64 * (s / 4 + pk / s)) % s + 1) as u32
}
/// Spec orderkey sparsity: the first 8 keys of every 32.
pub fn orderkey_of(i: u64) -> i64 {
    ((i / 8) * 32 + i % 8 + 1) as i64
}

pub fn generate(sf: f64) -> Tpch {
    let scount = ((10_000.0 * sf) as u32).max(4);
    let pcount = ((200_000.0 * sf) as u32).max(20);
    let ccount = ((150_000.0 * sf) as u32).max(15);
    let ocount = ((1_500_000.0 * sf) as u64).max(150);

    // customer
    let mut rng = Rng::new(0xC0);
    let mut cust = Customers {
        n: ccount,
        nationkey: Vec::with_capacity(ccount as usize),
        segment: Vec::with_capacity(ccount as usize),
        acctbal: Vec::with_capacity(ccount as usize),
    };
    for _ in 0..ccount {
        cust.nationkey.push((rng.next() % 25) as u8);
        cust.segment.push((rng.next() % 5) as u8);
        cust.acctbal.push(rng.range(-99_999, 999_999) as i32);
    }

    // supplier
    let mut rng = Rng::new(0x5);
    let mut supp = Suppliers {
        n: scount,
        nationkey: Vec::with_capacity(scount as usize),
        acctbal: Vec::with_capacity(scount as usize),
    };
    for _ in 0..scount {
        supp.nationkey.push((rng.next() % 25) as u8);
        supp.acctbal.push(rng.range(-99_999, 999_999) as i32);
    }

    // part
    let mut rng = Rng::new(0xBA);
    let mut part = Parts {
        n: pcount,
        name_words: Vec::with_capacity(pcount as usize),
        mfgr: Vec::with_capacity(pcount as usize),
        brand: Vec::with_capacity(pcount as usize),
        ptype: Vec::with_capacity(pcount as usize),
        size: Vec::with_capacity(pcount as usize),
        container: Vec::with_capacity(pcount as usize),
        retail_c: Vec::with_capacity(pcount as usize),
    };
    for pk in 1..=pcount {
        let mut w = [0u8; 5];
        for x in w.iter_mut() {
            *x = (rng.next() % 92) as u8;
        }
        part.name_words.push(w);
        let m = 1 + (rng.next() % 5) as u8;
        part.mfgr.push(m);
        part.brand.push(m * 10 + 1 + (rng.next() % 5) as u8);
        part.ptype.push((rng.next() % TYPES.len() as u64) as u8);
        part.size.push(1 + (rng.next() % 50) as u8);
        part.container.push((rng.next() % CONTAINERS.len() as u64) as u8);
        part.retail_c.push(retail_cents(pk));
    }

    // partsupp: 4 suppliers per part, spec formula.
    let mut rng = Rng::new(0x125);
    let n_ps = pcount as usize * 4;
    let mut ps = Partsupps {
        partkey: Vec::with_capacity(n_ps),
        suppkey: Vec::with_capacity(n_ps),
        availqty: Vec::with_capacity(n_ps),
        supplycost_c: Vec::with_capacity(n_ps),
        supp4: Vec::with_capacity(pcount as usize),
    };
    for pk in 1..=pcount {
        // Spec formula, then linear-probe to uniqueness (the formula can
        // collide at tiny supplier counts; TPC-H requires 4 distinct).
        let mut four = [0u32; 4];
        for i in 0..4 {
            let mut k = ps_suppkey(pk, i, scount);
            while four[..i as usize].contains(&k) {
                k = k % scount + 1;
            }
            four[i as usize] = k;
            ps.partkey.push(pk);
            ps.suppkey.push(k);
            ps.availqty.push(rng.range(1, 9_999) as i32);
            ps.supplycost_c.push(rng.range(100, 100_000) as i32);
        }
        ps.supp4.push(four);
    }

    // orders + lineitem, generated together (totalprice needs the lines).
    let start = date_days(1992, 1, 1);
    let current = date_days(1995, 6, 17);
    let odate_span = date_days(1998, 8, 2) - start; // ENDDATE - 151 days
    let mut rng = Rng::new(0x0DE5);
    let est_li = ocount as usize * 4;
    let mut ord = Orders {
        orderkey: Vec::with_capacity(ocount as usize),
        custkey: Vec::with_capacity(ocount as usize),
        status: Vec::with_capacity(ocount as usize),
        totalprice: Vec::with_capacity(ocount as usize),
        orderdate: Vec::with_capacity(ocount as usize),
        priority: Vec::with_capacity(ocount as usize),
        clerk: Vec::with_capacity(ocount as usize),
    };
    let mut li = Lineitems {
        orderkey: Vec::with_capacity(est_li),
        partkey: Vec::with_capacity(est_li),
        suppkey: Vec::with_capacity(est_li),
        linenumber: Vec::with_capacity(est_li),
        quantity_c: Vec::with_capacity(est_li),
        extprice_c: Vec::with_capacity(est_li),
        discount: Vec::with_capacity(est_li),
        tax: Vec::with_capacity(est_li),
        returnflag: Vec::with_capacity(est_li),
        linestatus: Vec::with_capacity(est_li),
        shipdate: Vec::with_capacity(est_li),
        commitdate: Vec::with_capacity(est_li),
        receiptdate: Vec::with_capacity(est_li),
        instruct: Vec::with_capacity(est_li),
        mode: Vec::with_capacity(est_li),
    };
    let nthirds = (ccount as u64 * 2 / 3).max(1); // custkeys not divisible by 3
    for i in 0..ocount {
        let okey = orderkey_of(i);
        let j = rng.next() % nthirds;
        let custkey = (j / 2 * 3 + 1 + j % 2) as u32; // 1,2,4,5,7,8,...
        let odate = start + rng.range(0, odate_span as i64 - 1) as i32;
        let nlines = 1 + (rng.next() % 7) as u8;
        let mut total_c = 0i64;
        let (mut all_f, mut all_o) = (true, true);
        for ln in 1..=nlines {
            let pk = 1 + (rng.next() % pcount as u64) as u32;
            let sk = ps.supp4[pk as usize - 1][(rng.next() % 4) as usize];
            let qty = rng.range(1, 50);
            let ext_c = qty * retail_cents(pk) as i64;
            let disc = (rng.next() % 11) as u8;
            let tax = (rng.next() % 9) as u8;
            let ship = odate + rng.range(1, 121) as i32;
            let commit = odate + rng.range(30, 90) as i32;
            let receipt = ship + rng.range(1, 30) as i32;
            let rf = if receipt <= current {
                if rng.next() % 2 == 0 { b'R' } else { b'A' }
            } else {
                b'N'
            };
            let ls = if ship > current { b'O' } else { b'F' };
            all_f &= ls == b'F';
            all_o &= ls == b'O';
            total_c += ext_c * (100 + tax as i64) * (100 - disc as i64) / 10_000;
            li.orderkey.push(okey);
            li.partkey.push(pk);
            li.suppkey.push(sk);
            li.linenumber.push(ln);
            li.quantity_c.push((qty * 100) as i32);
            li.extprice_c.push(ext_c);
            li.discount.push(disc);
            li.tax.push(tax);
            li.returnflag.push(rf);
            li.linestatus.push(ls);
            li.shipdate.push(ship);
            li.commitdate.push(commit);
            li.receiptdate.push(receipt);
            li.instruct.push((rng.next() % INSTRUCT.len() as u64) as u8);
            li.mode.push((rng.next() % MODES.len() as u64) as u8);
        }
        ord.orderkey.push(okey);
        ord.custkey.push(custkey);
        ord.status.push(if all_f {
            b'F'
        } else if all_o {
            b'O'
        } else {
            b'P'
        });
        ord.totalprice.push(total_c);
        ord.orderdate.push(odate);
        ord.priority.push((rng.next() % 5) as u8);
        ord.clerk.push(1 + (rng.next() % (scount as u64 / 10 + 1)) as u32);
    }

    Tpch { cust, ord, li, part, ps, supp }
}
