use core::cmp::Ordering;

pub const MAP_OLD_GUC_NAMES: &[(&str, &str)] = &[
    ("sort_mem", "work_mem"),
    ("vacuum_mem", "maintenance_work_mem"),
    ("ssl_ecdh_curve", "ssl_groups"),
];

// ASCII-only downcasing, deliberately not strcasecmp: stable across setlocale().
pub fn guc_name_compare(namea: &str, nameb: &str) -> Ordering {
    let a = namea.as_bytes();
    let b = nameb.as_bytes();
    let n = a.len().min(b.len());
    for i in 0..n {
        let cha = a[i].to_ascii_lowercase();
        let chb = b[i].to_ascii_lowercase();
        if cha != chb {
            return cha.cmp(&chb);
        }
    }
    a.len().cmp(&b.len())
}

#[inline]
pub fn guc_name_eq(namea: &str, nameb: &str) -> bool {
    guc_name_compare(namea, nameb) == Ordering::Equal
}

pub fn guc_name_hash(name: &str) -> u32 {
    let mut result: u32 = 0;
    for &b in name.as_bytes() {
        result = result.rotate_left(5);
        result ^= b.to_ascii_lowercase() as u32;
    }
    result
}

pub fn fold_name(name: &str) -> String {
    name.bytes().map(|b| b.to_ascii_lowercase() as char).collect()
}

pub fn convert_guc_name_for_parameter_acl(name: &str) -> String {
    let mut canonical = name;
    for (old, new) in MAP_OLD_GUC_NAMES {
        if guc_name_eq(name, old) {
            canonical = new;
            break;
        }
    }
    fold_name(canonical)
}

// guc_name_hash-compatible BuildHasher over pre-folded keys (the guc_hashtab
// hash/match pair).
#[derive(Clone, Copy, Default)]
pub struct GucNameHasherBuilder;

pub struct GucNameHasher(u32);

impl std::hash::Hasher for GucNameHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            if b == 0xff {
                continue;
            }
            self.0 = self.0.rotate_left(5);
            self.0 ^= b.to_ascii_lowercase() as u32;
        }
    }
    fn finish(&self) -> u64 {
        self.0 as u64
    }
}

impl std::hash::BuildHasher for GucNameHasherBuilder {
    type Hasher = GucNameHasher;
    fn build_hasher(&self) -> GucNameHasher {
        GucNameHasher(0)
    }
}
