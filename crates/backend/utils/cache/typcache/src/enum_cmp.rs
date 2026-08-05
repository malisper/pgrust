// typcache.c enum comparison support: load_enum_cache_data /
// compare_values_of_enum / enum_known_sorted / find_enumitem.
use core::cmp::Ordering;

use mcx::PgVec;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE, ERROR};

use crate::{lookup_type_cache, with_state, TypeCacheEntry};

const TYPTYPE_ENUM: i8 = b'e' as i8;

#[derive(Clone, Copy)]
pub(crate) struct EnumItem {
    enum_oid: Oid,
    sort_order: f32,
}

pub(crate) struct EnumData {
    bitmap_base: Oid,
    // Bit k set => OID bitmap_base+k is in the known-sorted subset (the C
    // Bitmapset; offsets are < 8192 by construction).
    sorted_words: PgVec<'static, u64>,
    // Sorted by enum_oid for binary search.
    items: PgVec<'static, EnumItem>,
}

impl EnumData {
    fn known_sorted(&self, arg: Oid) -> bool {
        if arg < self.bitmap_base {
            return false;
        }
        let offset = (arg - self.bitmap_base) as usize;
        self.sorted_words
            .get(offset / 64)
            .is_some_and(|w| w & (1u64 << (offset % 64)) != 0)
    }

    fn find(&self, arg: Oid) -> Option<EnumItem> {
        self.items
            .binary_search_by(|it| it.enum_oid.cmp(&arg))
            .ok()
            .map(|i| self.items[i])
    }
}

pub fn compare_values_of_enum(tcache: &TypeCacheEntry, arg1: Oid, arg2: Oid) -> PgResult<i32> {
    if arg1 == arg2 {
        return Ok(0);
    }

    if tcache.enum_data.borrow().is_none() {
        load_enum_cache_data(tcache)?;
    }

    {
        let data = tcache.enum_data.borrow();
        let data = data.as_ref().expect("enumData loaded above");
        if data.known_sorted(arg1) && data.known_sorted(arg2) {
            return Ok(if arg1 < arg2 { -1 } else { 1 });
        }
        if let (Some(item1), Some(item2)) = (data.find(arg1), data.find(arg2)) {
            return Ok(order_cmp(item1, item2));
        }
    }

    // One or both values missing: the enum changed under us — reload once.
    load_enum_cache_data(tcache)?;
    // Borrow released before value_not_found: format_type_be re-enters caches.
    let (item1, item2) = {
        let data = tcache.enum_data.borrow();
        let data = data.as_ref().expect("enumData reloaded above");
        (data.find(arg1), data.find(arg2))
    };
    let item1 = item1.ok_or_else(|| value_not_found(arg1, tcache.type_id))?;
    let item2 = item2.ok_or_else(|| value_not_found(arg2, tcache.type_id))?;
    Ok(order_cmp(item1, item2))
}

fn order_cmp(item1: EnumItem, item2: EnumItem) -> i32 {
    match item1.sort_order.partial_cmp(&item2.sort_order) {
        Some(Ordering::Less) => -1,
        Some(Ordering::Greater) => 1,
        _ => 0,
    }
}

fn load_enum_cache_data(tcache: &TypeCacheEntry) -> PgResult<()> {
    if tcache.typtype() != TYPTYPE_ENUM {
        return Err(not_an_enum(tcache.type_id)?);
    }

    let mcx = with_state(|st| st.mcx);
    let mut items: PgVec<'static, EnumItem> = PgVec::new_in(mcx);
    for (enum_oid, sort_order) in pg_enum_seams::scan_enum_members::call(mcx, tcache.type_id)?.iter() {
        items.push(EnumItem { enum_oid: *enum_oid, sort_order: *sort_order });
    }
    items.sort_unstable_by_key(|it| it.enum_oid);
    let numitems = items.len();

    // The enum's initial OIDs were assigned in order; find the longest sorted
    // subsequence (offset-bounded at 8192) so those compare by bare OID.
    let mut bitmap_base = types_core::InvalidOid;
    let mut bitmap: PgVec<'static, u64> = PgVec::new_in(mcx);
    let mut bm_size = 1usize;

    let mut start_pos = 0usize;
    while numitems > 0 && start_pos < numitems - 1 {
        let mut this_bitmap: PgVec<'static, u64> = PgVec::new_in(mcx);
        set_bit(&mut this_bitmap, 0);
        let mut this_bm_size = 1usize;
        let start_oid = items[start_pos].enum_oid;
        let mut prev_order = items[start_pos].sort_order;

        for i in start_pos + 1..numitems {
            let offset = items[i].enum_oid - start_oid;
            if offset >= 8192 {
                break;
            }
            if items[i].sort_order > prev_order {
                prev_order = items[i].sort_order;
                set_bit(&mut this_bitmap, offset as usize);
                this_bm_size += 1;
            }
        }

        if this_bm_size > bm_size {
            bitmap_base = start_oid;
            bitmap = this_bitmap;
            bm_size = this_bm_size;
        }

        if bm_size >= numitems - start_pos - 1 {
            break;
        }
        start_pos += 1;
    }

    *tcache.enum_data.borrow_mut() =
        Some(EnumData { bitmap_base, sorted_words: bitmap, items });
    Ok(())
}

fn set_bit(words: &mut PgVec<'static, u64>, bit: usize) {
    let word = bit / 64;
    if words.len() <= word {
        words.resize(word + 1, 0);
    }
    words[word] |= 1u64 << (bit % 64);
}

fn compare_values_of_enum_by_oid(type_id: Oid, arg1: Oid, arg2: Oid) -> PgResult<i32> {
    let entry = lookup_type_cache(type_id, 0)?;
    compare_values_of_enum(&entry, arg1, arg2)
}

pub(crate) fn install_seam() {
    typcache_seams::compare_values_of_enum::set(compare_values_of_enum_by_oid);
}

#[cold]
#[inline(never)]
fn not_an_enum(type_id: Oid) -> PgResult<Box<PgError>> {
    let name = format_type::format_type_be(type_id)?;
    Ok(Box::new(
        PgError::new(ERROR, format!("{name} is not an enum"))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn value_not_found(arg: Oid, type_id: Oid) -> Box<PgError> {
    let name = format_type::format_type_be(type_id).unwrap_or_else(|_| "???".into());
    Box::new(PgError::new(
        ERROR,
        format!("enum value {arg} not found in cache for enum {name}"),
    ))
}
