//! ginbulk.c: build accumulator. C's rbtree becomes hash-dedup + one sort at
//! dump — the dumped entry order (sorted by ginCompareEntries) and each
//! entry's TID list are identical to C's in-order rbtree walk. Dedup is by
//! key VALUE bytes; a compareFn-equal but byte-unequal key pair would diverge
//! from C, so the dump sort panics loudly if it ever sees adjacent equal keys.

use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, PgFxHashMap, PgVec};
use ::types_error::PgResult;
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::varatt;

use crate::util::ginCompareAttEntries;
use ::types_core::OffsetNumber;

const DEF_NPTR: usize = 5;
const DEF_NENTRY: usize = 2048;
const SIZEOF_C_ENTRY_ACCUMULATOR: usize = 56;

/// GetMemoryChunkSpace over C's default aset: pow2 bucket incl. the 8-byte
/// chunk header below the 8KB chunk limit, header + MAXALIGN above. Drives
/// only the maintenance_work_mem dump boundary.
fn chunk_space(sz: usize) -> usize {
    let s = sz + 8;
    if s <= 8 * 1024 {
        s.next_power_of_two().max(16)
    } else {
        MAXALIGN(sz) + 8
    }
}

#[derive(Clone, Copy)]
struct KeyRef {
    attnum: OffsetNumber,
    category: GinNullCategory,
    /// Value bytes of the copied key (accumulator-owned; null for categories).
    ptr: *const u8,
    len: u32,
}

impl KeyRef {
    fn bytes(&self) -> &[u8] {
        if self.ptr.is_null() {
            return &[];
        }
        // SAFETY: accumulator-owned copy, live until reset.
        unsafe { core::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}

impl PartialEq for KeyRef {
    fn eq(&self, other: &Self) -> bool {
        self.attnum == other.attnum
            && self.category == other.category
            && self.bytes() == other.bytes()
    }
}
impl Eq for KeyRef {}
impl core::hash::Hash for KeyRef {
    fn hash<H: core::hash::Hasher>(&self, h: &mut H) {
        self.attnum.hash(h);
        self.category.hash(h);
        self.bytes().hash(h);
    }
}

pub struct EntryAcc<'s> {
    pub attnum: OffsetNumber,
    pub key: Datum,
    pub category: GinNullCategory,
    should_sort: bool,
    pub list: PgVec<'s, ItemPointerData>,
}

pub struct BuildAccumulator<'s> {
    mcx: Mcx<'s>,
    state: GinState,
    map: PgFxHashMap<'s, KeyRef, u32>,
    entries: PgVec<'s, EntryAcc<'s>>,
    pub allocated_memory: usize,
    dump_order: PgVec<'s, u32>,
    dump_pos: usize,
}

fn key_value_bytes(col: &GinColState, key: Datum) -> (*const u8, u32) {
    if col.key_byval {
        // By-value keys hash as the 8-byte word; KeyRef points nowhere.
        (core::ptr::null(), 0)
    } else if col.key_len == -1 {
        // SAFETY: non-null varlena key datum.
        unsafe {
            let p = key.as_usize() as *const u8;
            if !crate::opclass::inline_image(key) {
                // Compressed keys (pending-list cleanup re-accumulates keys
                // that GinFormTuple stored compressed) hash by their whole
                // image; identical keys compress identically, so grouping is
                // unchanged, and the dump-order sort compares detoasted.
                (p, varatt::varsize_any(p) as u32)
            } else if varatt::varatt_is_1b(p) {
                (
                    p.add(varatt::VARHDRSZ_SHORT),
                    (varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT) as u32,
                )
            } else {
                (p.add(4), (varatt::varsize_4b(p) - 4) as u32)
            }
        }
    } else {
        (key.as_usize() as *const u8, col.key_len as u32)
    }
}

impl<'s> BuildAccumulator<'s> {
    /// ginInitBA.
    pub fn new(mcx: Mcx<'s>, state: GinState) -> Self {
        BuildAccumulator {
            mcx,
            state,
            map: PgFxHashMap::with_hasher_in(Default::default(), mcx),
            entries: PgVec::new_in(mcx),
            allocated_memory: 0,
            dump_order: mcx::vec_new_in(mcx),
            dump_pos: 0,
        }
    }

    /// getDatumCopy: copy a by-ref key into the accumulator context.
    fn key_copy(&mut self, attnum: OffsetNumber, key: Datum) -> PgResult<Datum> {
        let col = self.state.col(attnum);
        if col.key_byval {
            return Ok(key);
        }
        let len = if col.key_len == -1 {
            // SAFETY: non-null varlena key datum.
            unsafe { varatt::varsize_any(key.as_usize() as *const u8) }
        } else {
            col.key_len as usize
        };
        let mut buf: PgVec<'s, u8> = mcx::vec_with_capacity_in(self.mcx, len)?;
        // SAFETY: len bytes of the live key image.
        crate::vec_append(&mut buf, unsafe {
            core::slice::from_raw_parts(key.as_usize() as *const u8, len)
        })?;
        self.allocated_memory += chunk_space(len);
        let p = buf.as_ptr();
        core::mem::forget(buf);
        Ok(Datum::from_usize(p as usize))
    }

    /// ginInsertBAEntry.
    fn insert_entry(
        &mut self,
        heapptr: &ItemPointerData,
        attnum: OffsetNumber,
        key: Datum,
        category: GinNullCategory,
    ) -> PgResult<()> {
        let key_byval = self.state.col(attnum).key_byval;
        let (ptr, len) = if category == GIN_CAT_NORM_KEY {
            key_value_bytes(self.state.col(attnum), key)
        } else {
            (core::ptr::null(), 0)
        };
        let probe = KeyRef {
            attnum,
            category,
            ptr: if key_byval && category == GIN_CAT_NORM_KEY {
                // By-value word: hash the datum bytes through a stable copy.
                core::ptr::null()
            } else {
                ptr
            },
            len,
        };

        // By-value keys fold the word into the map key via a synthetic ref.
        let byval_word;
        let probe = if key_byval && category == GIN_CAT_NORM_KEY {
            byval_word = key.as_u64().to_ne_bytes();
            KeyRef {
                attnum,
                category,
                ptr: byval_word.as_ptr(),
                len: 8,
            }
        } else {
            probe
        };

        if let Some(&idx) = self.map.get(&probe) {
            // ginCombineData.
            let e = &mut self.entries[idx as usize];
            if e.list.len() == e.list.capacity() {
                self.allocated_memory -= chunk_space(e.list.capacity() * 6);
                e.list.try_reserve_exact(e.list.capacity()).map_err(|_| crate::oom(e.list.capacity() * 6))?;
                self.allocated_memory += chunk_space(e.list.capacity() * 6);
            }
            if !e.should_sort {
                let res = ginCompareItemPointers(&e.list[e.list.len() - 1], heapptr);
                debug_assert!(res != 0);
                if res > 0 {
                    e.should_sort = true;
                }
            }
            e.list.push(*heapptr);
            return Ok(());
        }

        // New entry: permanent key copy + fresh list.
        let key_copied = if category == GIN_CAT_NORM_KEY {
            self.key_copy(attnum, key)?
        } else {
            Datum::null()
        };
        let mut list: PgVec<'s, ItemPointerData> =
            mcx::vec_with_capacity_in(self.mcx, DEF_NPTR)?;
        list.push(*heapptr);
        self.allocated_memory += chunk_space(DEF_NPTR * 6);
        if self.entries.len() % DEF_NENTRY == 0 {
            // C's entryallocator chunks.
            self.allocated_memory += chunk_space(DEF_NENTRY * SIZEOF_C_ENTRY_ACCUMULATOR);
        }

        let idx = self.entries.len() as u32;
        self.entries.push(EntryAcc {
            attnum,
            key: key_copied,
            category,
            should_sort: false,
            list,
        });
        let stored = if category == GIN_CAT_NORM_KEY {
            let (p, l) = if key_byval {
                // Point at the entry's stored 8-byte word (stable: Datum is
                // stored by value; use a copy in the accumulator arena).
                let mut w: PgVec<'s, u8> = mcx::vec_with_capacity_in(self.mcx, 8)?;
                crate::vec_append(&mut w, &key_copied.as_u64().to_ne_bytes())?;
                let p = w.as_ptr();
                core::mem::forget(w);
                (p, 8u32)
            } else {
                key_value_bytes(self.state.col(attnum), key_copied)
            };
            KeyRef {
                attnum,
                category,
                ptr: p,
                len: l,
            }
        } else {
            KeyRef {
                attnum,
                category,
                ptr: core::ptr::null(),
                len: 0,
            }
        };
        self.map.insert(stored, idx);
        Ok(())
    }

    /// ginInsertBAEntries (hash map: insertion order is irrelevant).
    pub fn insert_entries(
        &mut self,
        heapptr: &ItemPointerData,
        attnum: OffsetNumber,
        entries: &[Datum],
        categories: &[GinNullCategory],
    ) -> PgResult<()> {
        for (i, key) in entries.iter().enumerate() {
            self.insert_entry(heapptr, attnum, *key, categories[i])?;
        }
        Ok(())
    }

    /// ginBeginBAScan: sort the dump order by ginCompareEntries.
    pub fn begin_scan(&mut self) -> PgResult<()> {
        self.dump_order.clear();
        self.dump_order
            .try_reserve(self.entries.len())
            .map_err(|_| crate::oom(self.entries.len() * 4))?;
        for i in 0..self.entries.len() as u32 {
            self.dump_order.push(i);
        }
        let entries = &self.entries;
        let state = self.state;
        self.dump_order.sort_by(|&a, &b| {
            let ea = &entries[a as usize];
            let eb = &entries[b as usize];
            let c = ginCompareAttEntries(
                &state,
                ea.attnum,
                ea.key,
                ea.category,
                eb.attnum,
                eb.key,
                eb.category,
            );
            if c == 0 {
                // Byte-dedup missed a compareFn-equal pair: silent index
                // corruption vs C — refuse.
                panic!("gin build accumulator: compareFn-equal keys with distinct byte images");
            }
            c.cmp(&0)
        });
        self.dump_pos = 0;
        Ok(())
    }

    /// ginGetBAEntry.
    pub fn next_entry(
        &mut self,
    ) -> Option<(OffsetNumber, Datum, GinNullCategory, &[ItemPointerData])> {
        if self.dump_pos >= self.dump_order.len() {
            return None;
        }
        let idx = self.dump_order[self.dump_pos] as usize;
        self.dump_pos += 1;
        let e = &mut self.entries[idx];
        if e.should_sort && e.list.len() > 1 {
            e.list.sort_by(|a, b| {
                let r = ginCompareItemPointers(a, b);
                debug_assert!(r != 0);
                r.cmp(&0)
            });
        }
        Some((e.attnum, e.key, e.category, e.list.as_slice()))
    }

    pub fn nentries(&self) -> usize {
        self.entries.len()
    }
}
