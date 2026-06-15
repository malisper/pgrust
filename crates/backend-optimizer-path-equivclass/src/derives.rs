//! equivclass.c — the EC derived-clause lookup family over the EmId arena.
//! `ec_members` holds [`EmId`] handles; `RestrictInfo::left_em`/`right_em` hold
//! the same handles, so the pointer identity the C `ec_derives_hash` simplehash
//! keys on is faithfully expressed as `EmId` equality. The hash is modelled as
//! PostgreSQL's `simplehash` (open addressing, linear probing, power-of-two
//! sizing, 0.9 fill-factor grow); the only observable behaviour is key→rinfo
//! lookup.

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use types_pathnodes::{
    DerivesHash, ECDerivesEntry, ECDerivesKey, EcId, EmId, PlannerInfo, RinfoId,
};

/// `EC_DERIVES_HASH_THRESHOLD` (equivclass.c) — switch to hash lookup once the
/// derived-clause list reaches this length.
const EC_DERIVES_HASH_THRESHOLD: usize = 32;

const SH_STATUS_EMPTY: u32 = 0x00;
const SH_STATUS_IN_USE: u32 = 0x01;
const SH_FILLFACTOR: f64 = 0.9;

/* ----- simplehash core ----------------------------------------------- */

fn hash_key(key: &ECDerivesKey) -> u32 {
    /* FNV-1a over the three (canonicalised) handle slots. The exact hash is
     * not observable; only the lookup result is, and that is determined by key
     * equality (handled by the probe loop). */
    let mut h: u32 = 0x811c_9dc5;
    let mut mix = |v: u32| {
        h ^= v;
        h = h.wrapping_mul(0x0100_0193);
    };
    mix(key.em1.map_or(0, |id| id.0.wrapping_add(1)));
    mix(key.em2.map_or(0, |id| id.0.wrapping_add(1)));
    mix(key.parent_ec.map_or(0, |id| id.0.wrapping_add(1)));
    h
}

fn derives_update_parameters(tb: &mut DerivesHash, newsize: u64) {
    let want = newsize.max(2);
    let size = want.next_power_of_two();
    tb.size = size;
    tb.sizemask = (size - 1) as u32;
    tb.grow_threshold = ((size as f64) * SH_FILLFACTOR) as u32;
    tb.data = Vec::new();
    tb.data.resize(size as usize, ECDerivesEntry::default());
}

/// `derives_create(ctx, nelements, private)` (simplehash SH_CREATE).
fn derives_create(nelements: u64) -> Box<DerivesHash> {
    let mut tb = DerivesHash::default();
    let size = ((nelements as f64) / SH_FILLFACTOR) as u64;
    derives_update_parameters(&mut tb, size);
    Box::new(tb)
}

fn derives_grow(tb: &mut DerivesHash) {
    let old = core::mem::take(&mut tb.data);
    derives_update_parameters(tb, tb.size * 2);
    tb.members = 0;
    for e in old {
        if e.status == SH_STATUS_IN_USE {
            let slot = derives_insert(tb, e.key);
            slot.rinfo = e.rinfo;
        }
    }
}

/// `derives_insert(tb, key, &found)` (simplehash SH_INSERT) — return the slot
/// (existing or freshly inserted) for `key`.
fn derives_insert(tb: &mut DerivesHash, key: ECDerivesKey) -> &mut ECDerivesEntry {
    if tb.members >= tb.grow_threshold {
        derives_grow(tb);
    }
    let mask = tb.sizemask;
    let mut bucket = hash_key(&key) & mask;
    loop {
        let status = tb.data[bucket as usize].status;
        if status == SH_STATUS_EMPTY {
            tb.members += 1;
            let e = &mut tb.data[bucket as usize];
            e.status = SH_STATUS_IN_USE;
            e.key = key;
            return e;
        }
        if tb.data[bucket as usize].key == key {
            return &mut tb.data[bucket as usize];
        }
        bucket = (bucket + 1) & mask;
    }
}

/// `derives_lookup(tb, key)` (simplehash SH_LOOKUP).
fn derives_lookup(tb: &DerivesHash, key: &ECDerivesKey) -> Option<RinfoId> {
    if tb.size == 0 {
        return None;
    }
    let mask = tb.sizemask;
    let mut bucket = hash_key(key) & mask;
    loop {
        let e = &tb.data[bucket as usize];
        match e.status {
            SH_STATUS_EMPTY => return None,
            _ if e.key == *key => return e.rinfo,
            _ => bucket = (bucket + 1) & mask,
        }
    }
}

/* ----- fill_ec_derives_key (equivclass.c:3754) ----------------------- */

/// `fill_ec_derives_key(key, leftem, rightem, parent_ec)` — canonicalise the EM
/// pair. The C key orders the two EM *pointers*; the arena equivalent orders by
/// [`EmId`] index. A const-EM lookup passes `rightem = None`, which stores the
/// non-const EM in `em2` with `em1 = None`.
pub fn fill_ec_derives_key(
    leftem: EmId,
    rightem: Option<EmId>,
    parent_ec: Option<EcId>,
) -> ECDerivesKey {
    let (em1, em2) = match rightem {
        None => (None, Some(leftem)),
        Some(r) if leftem.index() < r.index() => (Some(leftem), Some(r)),
        Some(r) => (Some(r), Some(leftem)),
    };
    ECDerivesKey {
        em1,
        em2,
        parent_ec,
    }
}

/* ----- ec_*_derives_hash (equivclass.c:3672, 3788) ------------------- */

/// `ec_add_clause_to_derives_hash(ec, rinfo)` (equivclass.c:3788).
pub fn ec_add_clause_to_derives_hash(root: &mut PlannerInfo, ec: EcId, rinfo: RinfoId) {
    let left_em = root
        .rinfo(rinfo)
        .left_em
        .expect("derived clause must have a left_em");
    let right_em = root
        .rinfo(rinfo)
        .right_em
        .expect("derived clause must have a right_em");
    let parent_ec = root.rinfo(rinfo).parent_ec;

    /* Constants are always on the RHS; LHS is never constant. */
    debug_assert!(!root.em(left_em).em_is_const);
    /* Clauses containing a constant are never redundant, so parent_ec unset. */
    debug_assert!(parent_ec.is_none() || !root.em(right_em).em_is_const);

    let rightem = if root.em(right_em).em_is_const {
        None
    } else {
        Some(right_em)
    };
    let key = fill_ec_derives_key(left_em, rightem, parent_ec);

    let hash = root
        .ec_mut(ec)
        .ec_derives_hash
        .as_mut()
        .expect("ec_add_clause_to_derives_hash: ec_derives_hash not built");
    let entry = derives_insert(hash, key);
    debug_assert!(entry.rinfo.is_none());
    entry.rinfo = Some(rinfo);
}

/// `ec_build_derives_hash(root, ec)` (equivclass.c:3672).
pub fn ec_build_derives_hash(root: &mut PlannerInfo, ec: EcId) {
    debug_assert!(root.ec(ec).ec_derives_hash.is_none());

    let n = root.ec(ec).ec_derives_list.len();
    let hash = derives_create(n as u64);
    root.ec_mut(ec).ec_derives_hash = Some(hash);

    let clauses = root.ec(ec).ec_derives_list.clone();
    for rinfo in clauses {
        ec_add_clause_to_derives_hash(root, ec, rinfo);
    }
}

/* ----- ec_add/clear_derived_clause(s) (equivclass.c:3702..3841) ------ */

/// `ec_add_derived_clause(ec, clause)` (equivclass.c:3702).
pub fn ec_add_derived_clause(root: &mut PlannerInfo, ec: EcId, clause: RinfoId) {
    debug_assert!(!root.em(
        root.rinfo(clause).left_em.expect("clause has left_em")
    )
    .em_is_const);
    debug_assert!(
        root.rinfo(clause).parent_ec.is_none()
            || !root
                .em(root.rinfo(clause).right_em.expect("clause has right_em"))
                .em_is_const
    );

    root.ec_mut(ec).ec_derives_list.push(clause);
    if root.ec(ec).ec_derives_hash.is_some() {
        ec_add_clause_to_derives_hash(root, ec, clause);
    }
}

/// `ec_add_derived_clauses(ec, clauses)` (equivclass.c:3732).
pub fn ec_add_derived_clauses(root: &mut PlannerInfo, ec: EcId, clauses: &[RinfoId]) {
    root.ec_mut(ec).ec_derives_list.extend_from_slice(clauses);
    if root.ec(ec).ec_derives_hash.is_some() {
        for &rinfo in clauses {
            ec_add_clause_to_derives_hash(root, ec, rinfo);
        }
    }
}

/// `ec_clear_derived_clauses(ec)` (equivclass.c:3830).
pub fn ec_clear_derived_clauses(root: &mut PlannerInfo, ec: EcId) {
    let e = root.ec_mut(ec);
    e.ec_derives_list = Vec::new();
    e.ec_derives_hash = None;
}

/* ----- ec_search_*_clause_for_ems (equivclass.c:3854, 3892) ---------- */

/// `ec_search_derived_clause_for_ems(root, ec, leftem, rightem, parent_ec)`
/// (equivclass.c:3892).
pub fn ec_search_derived_clause_for_ems(
    root: &mut PlannerInfo,
    ec: EcId,
    leftem: EmId,
    rightem: Option<EmId>,
    parent_ec: Option<EcId>,
) -> Option<RinfoId> {
    /* switch to hash lookup when the list grows "too long" */
    if root.ec(ec).ec_derives_hash.is_none()
        && root.ec(ec).ec_derives_list.len() >= EC_DERIVES_HASH_THRESHOLD
    {
        ec_build_derives_hash(root, ec);
    }

    if root.ec(ec).ec_derives_hash.is_some() {
        let key = fill_ec_derives_key(leftem, rightem, parent_ec);
        let hash = root.ec(ec).ec_derives_hash.as_deref().unwrap();
        if let Some(rinfo) = derives_lookup(hash, &key) {
            debug_assert!(
                rightem.is_some()
                    || root
                        .rinfo(rinfo)
                        .right_em
                        .is_some_and(|r| root.em(r).em_is_const)
            );
            return Some(rinfo);
        }
        return None;
    }

    /* fallback: linear search over ec_derives_list */
    let clauses = root.ec(ec).ec_derives_list.clone();
    for rinfo in clauses {
        let r_left = root.rinfo(rinfo).left_em;
        let r_right = root.rinfo(rinfo).right_em;
        let r_parent = root.rinfo(rinfo).parent_ec;

        /* special case: lookup by non-const EM alone */
        if rightem.is_none() && r_left == Some(leftem) {
            debug_assert!(r_right.is_some_and(|r| root.em(r).em_is_const));
            return Some(rinfo);
        }
        if r_left == Some(leftem) && r_right == rightem && r_parent == parent_ec {
            return Some(rinfo);
        }
        if r_left == rightem && r_right == Some(leftem) && r_parent == parent_ec {
            return Some(rinfo);
        }
    }

    None
}

/// `ec_search_clause_for_ems(root, ec, leftem, rightem, parent_ec)`
/// (equivclass.c:3854).
pub fn ec_search_clause_for_ems(
    root: &mut PlannerInfo,
    ec: EcId,
    leftem: EmId,
    rightem: Option<EmId>,
    parent_ec: Option<EcId>,
) -> Option<RinfoId> {
    /* check original source clauses */
    let sources = root.ec(ec).ec_sources.clone();
    for rinfo in sources {
        let r_left = root.rinfo(rinfo).left_em;
        let r_right = root.rinfo(rinfo).right_em;
        let r_parent = root.rinfo(rinfo).parent_ec;
        if r_left == Some(leftem) && r_right == rightem && r_parent == parent_ec {
            return Some(rinfo);
        }
        if r_left == rightem && r_right == Some(leftem) && r_parent == parent_ec {
            return Some(rinfo);
        }
    }

    /* not found in ec_sources; search derived clauses */
    ec_search_derived_clause_for_ems(root, ec, leftem, rightem, parent_ec)
}

/// `find_derived_clause_for_ec_member(root, ec, em)` (equivclass.c:2804).
pub fn find_derived_clause_for_ec_member(
    root: &mut PlannerInfo,
    ec: EcId,
    em: EmId,
) -> Option<RinfoId> {
    debug_assert!(root.ec(ec).ec_has_const);
    debug_assert!(!root.em(em).em_is_const);
    ec_search_derived_clause_for_ems(root, ec, em, None, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `fill_ec_derives_key` canonicalises the EM pair by arena index (the C
    /// pointer-address ordering analogue): `em1` is always the lower of the two,
    /// and the const-EM lookup (rightem=None) stores the sole EM in `em2`.
    #[test]
    fn fill_ec_derives_key_canonical_order() {
        let a = EmId(3);
        let b = EmId(7);
        let pec = Some(EcId(2));

        let k1 = fill_ec_derives_key(a, Some(b), pec);
        let k2 = fill_ec_derives_key(b, Some(a), pec);
        /* commutative: both orderings produce the same key */
        assert_eq!(k1, k2);
        assert_eq!(k1.em1, Some(a));
        assert_eq!(k1.em2, Some(b));
        assert_eq!(k1.parent_ec, pec);

        /* const-EM lookup: em1 None, the EM goes to em2 */
        let kc = fill_ec_derives_key(a, None, None);
        assert_eq!(kc.em1, None);
        assert_eq!(kc.em2, Some(a));
    }

    /// The open-addressing simplehash round-trips inserted keys, survives a
    /// grow, and reports `None` for absent keys.
    #[test]
    fn derives_hash_insert_lookup_grow() {
        let mut tb = derives_create(4);
        for i in 0..200u32 {
            let key = ECDerivesKey {
                em1: Some(EmId(i)),
                em2: Some(EmId(i + 1)),
                parent_ec: None,
            };
            let slot = derives_insert(&mut tb, key);
            slot.rinfo = Some(RinfoId(i));
        }
        for i in 0..200u32 {
            let key = ECDerivesKey {
                em1: Some(EmId(i)),
                em2: Some(EmId(i + 1)),
                parent_ec: None,
            };
            assert_eq!(derives_lookup(&tb, &key), Some(RinfoId(i)));
        }
        let absent = ECDerivesKey {
            em1: Some(EmId(9999)),
            em2: Some(EmId(10000)),
            parent_ec: None,
        };
        assert_eq!(derives_lookup(&tb, &absent), None);
    }
}
