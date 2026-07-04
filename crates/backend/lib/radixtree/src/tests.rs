use super::*;
use std::collections::BTreeMap;

fn test_ctx() -> MemoryContext {
    MemoryContext::new("test_radix_tree")
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

#[test]
fn test_empty() {
    let ctx = test_ctx();
    let mut tree: RadixTree<u64> = RadixTree::create(&ctx).unwrap();

    assert!(tree.find(0).is_none());
    assert!(tree.find(1).is_none());
    assert!(tree.find(u64::MAX).is_none());
    assert!(!tree.delete(0));
    assert_eq!(tree.num_keys(), 0);

    let mut iter = tree.begin_iterate();
    assert!(iter.next().is_none());
}

// test_radixtree.c: keys big enough to grow into each size class.
const NODE_CLASS_NKEYS: [usize; 5] = [2, 15, 30, 60, 256];

fn test_basic(nkeys: usize, shift: i32, asc: bool) {
    let ctx = test_ctx();
    let mut tree: RadixTree<u64> = RadixTree::create(&ctx).unwrap();

    let keys: Vec<u64> = (0..nkeys)
        .map(|i| if asc { (i as u64) << shift } else { ((nkeys - 1 - i) as u64) << shift })
        .collect();

    for &key in &keys {
        assert!(!tree.set(key, &key).unwrap());
    }
    for &key in &keys {
        assert_eq!(*tree.find(key).unwrap(), key);
    }
    for &key in &keys {
        let update = key + 1;
        assert!(tree.set(key, &update).unwrap());
    }
    for &key in &keys {
        assert!(tree.delete(key));
        assert!(!tree.set(key, &key).unwrap());
    }
    for &key in &keys {
        assert_eq!(*tree.find(key).unwrap(), key);
    }

    let mut iter = tree.begin_iterate();
    for i in 0..nkeys {
        let expected = if asc { keys[i] } else { keys[nkeys - 1 - i] };
        let (iterkey, iterval) = iter.next().unwrap();
        assert_eq!(iterkey, expected);
        assert_eq!(*iterval, expected);
    }
    assert!(iter.next().is_none());
    drop(iter);

    for &key in &keys {
        assert!(tree.delete(key));
    }
    for &key in &keys {
        assert!(tree.find(key).is_none());
    }
    assert_eq!(tree.num_keys(), 0);
}

#[test]
fn test_node_classes() {
    for &nkeys in &NODE_CLASS_NKEYS {
        for shift in [0, 8, RT_MAX_SHIFT] {
            test_basic(nkeys, shift, true);
            test_basic(nkeys, shift, false);
        }
    }
}

#[test]
fn test_random() {
    let ctx = test_ctx();
    let mut tree: RadixTree<u64> = RadixTree::create(&ctx).unwrap();

    // Limit memory use by limiting the key space (test_radixtree.c).
    let filter: u64 = (0x07 << 24) | (0xFF << 16) | 0xFF;
    let num_keys = if cfg!(miri) { 2_000 } else { 100_000 };
    let seed = 0x1234_5678_9ABC_DEF0u64;

    let mut prng = SplitMix64(seed);
    let mut keys = Vec::with_capacity(num_keys);
    for _ in 0..num_keys {
        let key = prng.next() & filter;
        keys.push(key);
        tree.set(key, &key).unwrap();
    }

    for &key in &keys {
        assert_eq!(*tree.find(key).unwrap(), key);
    }

    keys.sort_unstable();

    for i in 0..num_keys - 1 {
        if keys[i + 1] == keys[i] || keys[i + 1] == keys[i] + 1 {
            continue;
        }
        assert!(tree.find(keys[i] + 1).is_none());
    }
    for key in 0..keys[0].min(if cfg!(miri) { 500 } else { 10_000 }) {
        assert!(tree.find(key).is_none());
    }
    for i in 1..(if cfg!(miri) { 500 } else { 10_000u64 }) {
        assert!(tree.find(keys[num_keys - 1] + i).is_none());
    }

    let mut iter = tree.begin_iterate();
    for i in 0..num_keys {
        if i < num_keys - 1 && keys[i + 1] == keys[i] {
            continue;
        }
        let (iterkey, iterval) = iter.next().unwrap();
        assert_eq!(iterkey, keys[i]);
        assert_eq!(*iterval, keys[i]);
    }
    assert!(iter.next().is_none());
    drop(iter);

    let mut prng = SplitMix64(seed);
    for _ in 0..num_keys {
        let key = prng.next() & filter;
        tree.delete(key);
    }
    assert_eq!(tree.num_keys(), 0);
}

#[test]
fn test_dense_shrink_chain() {
    let ctx = test_ctx();
    let mut tree: RadixTree<u64> = RadixTree::create(&ctx).unwrap();

    let base = 0xAB00u64;
    for i in 0..256u64 {
        assert!(!tree.set(base + i, &i).unwrap());
    }
    // Delete descending: walks node256 -> node48 -> node16 -> node4 shrink
    // boundaries; verify survivors at every step.
    for deleted in (1..=256u64).rev() {
        assert!(tree.delete(base + deleted - 1));
        for i in 0..deleted - 1 {
            assert_eq!(*tree.find(base + i).unwrap(), i);
        }
        assert!(tree.find(base + deleted - 1).is_none());
    }
    assert_eq!(tree.num_keys(), 0);

    // Re-grow after full emptying (root-empty fast path in set()).
    let key = 0xDEAD_BEEF_0000u64;
    assert!(!tree.set(key, &key).unwrap());
    assert_eq!(*tree.find(key).unwrap(), key);
}

#[test]
fn test_model_random_ops() {
    for seed in [1u64, 42, 0xFEED] {
        let ctx = test_ctx();
        let mut tree: RadixTree<u64> = RadixTree::create(&ctx).unwrap();
        let mut model: BTreeMap<u64, u64> = BTreeMap::new();
        let mut prng = SplitMix64(seed);

        for step in 0..(if cfg!(miri) { 1_200 } else { 30_000usize }) {
            let r = prng.next();
            let key = match r % 4 {
                0 => prng.next() % 512,
                1 => (prng.next() % 256) << 16,
                2 => prng.next() % (1 << 40),
                _ => match prng.next() % 8 {
                    0 => u64::MAX,
                    1 => u64::MAX - 1,
                    v => prng.next() >> (v % 60),
                },
            };
            if r % 10 < 6 {
                let val = prng.next();
                let found = tree.set(key, &val).unwrap();
                let model_found = model.insert(key, val).is_some();
                assert_eq!(found, model_found, "set found mismatch at step {step}");
            } else if r % 10 < 9 {
                let deleted = tree.delete(key);
                let model_deleted = model.remove(&key).is_some();
                assert_eq!(deleted, model_deleted, "delete mismatch at step {step}");
            } else {
                assert_eq!(tree.find(key).copied(), model.get(&key).copied());
            }

            if step % 5_000 == 4_999 {
                assert_eq!(tree.num_keys() as usize, model.len());
                let mut iter = tree.begin_iterate();
                for (&mk, &mv) in model.iter() {
                    let (k, v) = iter.next().expect("iterator ended early");
                    assert_eq!((k, *v), (mk, mv));
                }
                assert!(iter.next().is_none());
                assert!(tree.memory_usage() > 0);
            }
        }

        assert_eq!(tree.num_keys() as usize, model.len());
        for (&mk, &mv) in model.iter() {
            assert_eq!(tree.find(mk).copied(), Some(mv));
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Big([u64; 3]);

// SAFETY: fixed-size plain bytes; 24 bytes forces single-value leaves.
unsafe impl RtValue for Big {}

#[test]
fn test_large_fixed_value_leaves() {
    let ctx = test_ctx();
    let mut tree: RadixTree<Big> = RadixTree::create(&ctx).unwrap();

    for i in 0..1000u64 {
        let v = Big([i, i * 2, i * 3]);
        assert!(!tree.set(i * 7, &v).unwrap());
    }
    for i in 0..1000u64 {
        let v = tree.find(i * 7).unwrap();
        assert_eq!(v.0, [i, i * 2, i * 3]);
    }
    // Update in place (same size leaf is reused).
    let v = Big([9, 9, 9]);
    assert!(tree.set(0, &v).unwrap());
    assert_eq!(tree.find(0).unwrap().0, [9, 9, 9]);
    for i in 0..1000u64 {
        assert!(tree.delete(i * 7));
    }
    assert_eq!(tree.num_keys(), 0);
}

#[repr(C)]
#[derive(Clone, Copy)]
struct VarHdr {
    flags: u8,
    nwords: i8,
    fill: [u16; 3],
}

const _: () = assert!(size_of::<VarHdr>() == 8);

// SAFETY: header prefix of a varlen image (BlocktableEntry shape); bit 0 of
// flags is always set, doubling as the embedded tag.
unsafe impl RtValue for VarHdr {
    const VARLEN: bool = true;
    const RUNTIME_EMBEDDABLE: bool = true;

    fn value_size(&self) -> usize {
        size_of::<VarHdr>() + self.nwords as usize * 8
    }
}

#[repr(C, align(8))]
struct VarImage {
    hdr: VarHdr,
    words: [u64; 8],
}

impl VarImage {
    fn new(nwords: usize, fill: u64) -> VarImage {
        let mut img = VarImage {
            hdr: VarHdr { flags: 1, nwords: nwords as i8, fill: [0; 3] },
            words: [0; 8],
        };
        for w in 0..nwords {
            img.words[w] = fill.wrapping_add(w as u64);
        }
        img
    }
}

unsafe fn check_var(tree: &RadixTree<VarHdr>, key: u64, nwords: usize, fill: u64) {
    let p = tree.find_ptr(key).expect("missing varlen key").as_ptr();
    assert_eq!((*p).flags & 1, 1);
    assert_eq!((*p).nwords as usize, nwords);
    let words = p.cast::<u8>().add(size_of::<VarHdr>()).cast::<u64>();
    for w in 0..nwords {
        assert_eq!(ptr::read_unaligned(words.add(w)), fill.wrapping_add(w as u64));
    }
}

#[test]
fn test_varlen_runtime_embeddable() {
    let ctx = test_ctx();
    let mut tree: RadixTree<VarHdr> = RadixTree::create(&ctx).unwrap();

    unsafe {
        // Embedded (8 bytes) -> leaf (3 words) -> bigger leaf (5 words) ->
        // same-size update (leaf reuse) -> back to embedded -> delete.
        let img = VarImage::new(0, 0);
        assert!(!tree.set_ptr(7, (&img as *const VarImage).cast()).unwrap());
        check_var(&tree, 7, 0, 0);

        let img = VarImage::new(3, 100);
        assert!(tree.set_ptr(7, (&img as *const VarImage).cast()).unwrap());
        check_var(&tree, 7, 3, 100);

        let img = VarImage::new(5, 200);
        assert!(tree.set_ptr(7, (&img as *const VarImage).cast()).unwrap());
        check_var(&tree, 7, 5, 200);

        let img = VarImage::new(5, 300);
        assert!(tree.set_ptr(7, (&img as *const VarImage).cast()).unwrap());
        check_var(&tree, 7, 5, 300);

        let img = VarImage::new(0, 0);
        assert!(tree.set_ptr(7, (&img as *const VarImage).cast()).unwrap());
        check_var(&tree, 7, 0, 0);

        // Populate a spread of keys with varying sizes; verify via iteration.
        for i in 0..500u64 {
            let img = VarImage::new((i % 8) as usize, i);
            tree.set_ptr(i * 3, (&img as *const VarImage).cast()).unwrap();
        }
        let mut iter = tree.begin_iterate();
        let mut seen = 0u64;
        while let Some((k, p)) = iter.next_ptr() {
            if k == 7 {
                continue;
            }
            assert_eq!(k % 3, 0);
            let i = k / 3;
            assert_eq!((*p.as_ptr()).nwords as usize, (i % 8) as usize);
            seen += 1;
        }
        assert_eq!(seen, 500);
        drop(iter);

        for i in 0..500u64 {
            assert!(tree.delete(i * 3));
        }
        assert!(tree.delete(7));
        assert_eq!(tree.num_keys(), 0);
    }
}

#[test]
fn test_memory_usage_tracks_growth() {
    let ctx = test_ctx();
    let mut tree: RadixTree<u64> = RadixTree::create(&ctx).unwrap();
    let initial = tree.memory_usage();
    assert!(initial > 0);

    for i in 0..(if cfg!(miri) { 3_000 } else { 50_000u64 }) {
        tree.set(i, &i).unwrap();
    }
    let grown = tree.memory_usage();
    assert!(grown > initial, "memory_usage did not grow: {initial} -> {grown}");
}

#[test]
fn test_shared_tree_threads() {
    let shared: SharedRadixTree<u64> = SharedRadixTree::create().unwrap();
    let nthreads = 4u64;
    let per_thread = if cfg!(miri) { 300 } else { 5_000u64 };

    std::thread::scope(|s| {
        for t in 0..nthreads {
            let shared = &shared;
            s.spawn(move || {
                for i in 0..per_thread {
                    let key = t * per_thread + i;
                    let mut tree = shared.lock_exclusive();
                    assert!(!tree.set(key, &(key * 10)).unwrap());
                }
            });
        }
    });

    {
        let tree = shared.lock_share();
        assert_eq!(tree.num_keys(), (nthreads * per_thread) as i64);
        let mut iter = tree.begin_iterate();
        for key in 0..nthreads * per_thread {
            let (k, v) = iter.next().unwrap();
            assert_eq!(k, key);
            assert_eq!(*v, key * 10);
        }
        assert!(iter.next().is_none());
    }

    std::thread::scope(|s| {
        for t in 0..nthreads {
            let shared = &shared;
            s.spawn(move || {
                let tree = shared.lock_share();
                for i in 0..per_thread {
                    let key = t * per_thread + i;
                    assert_eq!(*tree.find(key).unwrap(), key * 10);
                }
            });
        }
    });

    assert!(shared.memory_usage() > 0);

    {
        let mut tree = shared.lock_exclusive();
        for key in 0..nthreads * per_thread {
            assert!(tree.delete(key));
        }
        assert_eq!(tree.num_keys(), 0);
    }
}
