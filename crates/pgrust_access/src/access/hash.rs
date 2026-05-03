pub use pgrust_nodes::access::HashOptions;
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::relcache::IndexRelCacheEntry;
use pgrust_storage::page::bufpage::{
    ITEM_ID_SIZE, ItemIdFlags, MAXALIGN, PageError, SIZE_OF_PAGE_HEADER_DATA, max_align,
    page_get_item, page_get_item_id, page_get_max_offset_number, page_header, page_init,
    page_special, page_special_mut,
};
use pgrust_storage::smgr::BLCKSZ;

use crate::access::htup::AttributeCompression;
use crate::access::itup::IndexTupleData;
use crate::nbtree::tuple::{decode_key_payload, encode_key_payload};
use crate::{AccessError, AccessResult, AccessScalarServices};

pub const HASH_METAPAGE: u32 = 0;
pub const HASH_MAGIC: u32 = 0x0644_0640;
pub const HASH_VERSION: u32 = 4;
pub const HASH_PAGE_ID: u16 = 0xFF80;
pub const HASH_DEFAULT_FILLFACTOR: u16 = 75;
pub const HASH_STANDARD_PROC: i16 = 1;
pub const HASH_MAX_BUCKETS: usize = 1024;
pub const HASH_SPLITPOINTS: usize = 32;
pub const HASH_INVALID_BLOCK: u32 = u32::MAX;

pub const LH_UNUSED_PAGE: u16 = 0;
pub const LH_OVERFLOW_PAGE: u16 = 1 << 0;
pub const LH_BUCKET_PAGE: u16 = 1 << 1;
pub const LH_BITMAP_PAGE: u16 = 1 << 2;
pub const LH_META_PAGE: u16 = 1 << 3;
pub const LH_BUCKET_BEING_POPULATED: u16 = 1 << 4;
pub const LH_BUCKET_BEING_SPLIT: u16 = 1 << 5;
pub const LH_BUCKET_NEEDS_SPLIT_CLEANUP: u16 = 1 << 6;
pub const LH_PAGE_HAS_DEAD_TUPLES: u16 = 1 << 7;
pub const LH_PAGE_TYPE: u16 = LH_OVERFLOW_PAGE | LH_BUCKET_PAGE | LH_BITMAP_PAGE | LH_META_PAGE;

pub const HASH_SPECIAL_SIZE: usize = 16;
pub const HASH_PAGE_CONTENT_OFFSET: usize =
    (SIZE_OF_PAGE_HEADER_DATA + (MAXALIGN - 1)) & !(MAXALIGN - 1);
const HASH_META_DATA_SIZE: usize = 4 * 6 + 8 + HASH_SPLITPOINTS * 4 + HASH_MAX_BUCKETS * 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashPageOpaqueData {
    pub hasho_prevblkno: u32,
    pub hasho_nextblkno: u32,
    pub hasho_bucket: u32,
    pub hasho_flag: u16,
    pub hasho_page_id: u16,
}

impl HashPageOpaqueData {
    pub fn new(bucket: u32, flags: u16) -> Self {
        Self {
            hasho_prevblkno: HASH_INVALID_BLOCK,
            hasho_nextblkno: HASH_INVALID_BLOCK,
            hasho_bucket: bucket,
            hasho_flag: flags,
            hasho_page_id: HASH_PAGE_ID,
        }
    }

    pub fn is_meta(self) -> bool {
        self.hasho_flag & LH_PAGE_TYPE == LH_META_PAGE
    }

    pub fn is_bucket(self) -> bool {
        self.hasho_flag & LH_PAGE_TYPE == LH_BUCKET_PAGE
    }

    pub fn is_overflow(self) -> bool {
        self.hasho_flag & LH_PAGE_TYPE == LH_OVERFLOW_PAGE
    }

    pub fn is_unused(self) -> bool {
        self.hasho_flag & LH_PAGE_TYPE == LH_UNUSED_PAGE
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashMetaPageData {
    pub hashm_magic: u32,
    pub hashm_version: u32,
    pub hashm_maxbucket: u32,
    pub hashm_highmask: u32,
    pub hashm_lowmask: u32,
    pub hashm_ffactor: u32,
    pub hashm_ntuples: u64,
    pub hashm_spares: [u32; HASH_SPLITPOINTS],
    pub hashm_bucket_blocks: [u32; HASH_MAX_BUCKETS],
}

impl HashMetaPageData {
    pub fn new(bucket_count: u32, fillfactor: u16) -> Self {
        let bucket_count = bucket_count.clamp(1, HASH_MAX_BUCKETS as u32);
        let bucket_count = bucket_count
            .next_power_of_two()
            .min(HASH_MAX_BUCKETS as u32);
        let maxbucket = bucket_count.saturating_sub(1);
        let highmask = bucket_count.saturating_sub(1);
        let lowmask = highmask >> 1;
        let mut bucket_blocks = [HASH_INVALID_BLOCK; HASH_MAX_BUCKETS];
        for bucket in 0..bucket_count {
            bucket_blocks[bucket as usize] = bucket.saturating_add(1);
        }
        Self {
            hashm_magic: HASH_MAGIC,
            hashm_version: HASH_VERSION,
            hashm_maxbucket: maxbucket,
            hashm_highmask: highmask,
            hashm_lowmask: lowmask,
            hashm_ffactor: u32::from(fillfactor),
            hashm_ntuples: 0,
            hashm_spares: [0; HASH_SPLITPOINTS],
            hashm_bucket_blocks: bucket_blocks,
        }
    }

    pub fn bucket_count(&self) -> u32 {
        self.hashm_maxbucket.saturating_add(1)
    }

    pub fn bucket_for_hash(&self, hash: u32) -> u32 {
        let mut bucket = hash & self.hashm_highmask;
        if bucket > self.hashm_maxbucket {
            bucket &= self.hashm_lowmask;
        }
        bucket
    }

    pub fn bucket_block(&self, bucket: u32) -> Option<u32> {
        self.hashm_bucket_blocks
            .get(bucket as usize)
            .copied()
            .filter(|block| *block != HASH_INVALID_BLOCK)
    }

    pub fn target_tuples_per_bucket(&self) -> u64 {
        let fillfactor = self.hashm_ffactor.clamp(10, 100) as u64;
        ((fillfactor * 64) / 100).max(8)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashPageError {
    Page(PageError),
    Corrupt(&'static str),
}

impl From<PageError> for HashPageError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

pub fn hash_page_init(page: &mut [u8; BLCKSZ], bucket: u32, flags: u16) {
    page_init(page, HASH_SPECIAL_SIZE);
    let opaque = HashPageOpaqueData::new(bucket, flags);
    hash_page_set_opaque(page, opaque).expect("new hash page should have special space");
}

pub fn hash_page_get_opaque(page: &[u8; BLCKSZ]) -> Result<HashPageOpaqueData, HashPageError> {
    let special = page_special(page)?;
    if special.len() < HASH_SPECIAL_SIZE {
        return Err(HashPageError::Corrupt("hash special space too small"));
    }
    let opaque = HashPageOpaqueData {
        hasho_prevblkno: u32::from_le_bytes(special[0..4].try_into().unwrap()),
        hasho_nextblkno: u32::from_le_bytes(special[4..8].try_into().unwrap()),
        hasho_bucket: u32::from_le_bytes(special[8..12].try_into().unwrap()),
        hasho_flag: u16::from_le_bytes(special[12..14].try_into().unwrap()),
        hasho_page_id: u16::from_le_bytes(special[14..16].try_into().unwrap()),
    };
    if opaque.hasho_page_id != HASH_PAGE_ID {
        return Err(HashPageError::Corrupt("invalid hash page id"));
    }
    Ok(opaque)
}

pub fn hash_page_set_opaque(
    page: &mut [u8; BLCKSZ],
    opaque: HashPageOpaqueData,
) -> Result<(), HashPageError> {
    let special = page_special_mut(page)?;
    if special.len() < HASH_SPECIAL_SIZE {
        return Err(HashPageError::Corrupt("hash special space too small"));
    }
    special[0..4].copy_from_slice(&opaque.hasho_prevblkno.to_le_bytes());
    special[4..8].copy_from_slice(&opaque.hasho_nextblkno.to_le_bytes());
    special[8..12].copy_from_slice(&opaque.hasho_bucket.to_le_bytes());
    special[12..14].copy_from_slice(&opaque.hasho_flag.to_le_bytes());
    special[14..16].copy_from_slice(&opaque.hasho_page_id.to_le_bytes());
    Ok(())
}

pub fn hash_opclass_for_first_key(meta: &IndexRelCacheEntry) -> Option<u32> {
    meta.indclass.first().copied()
}

pub fn hash_fillfactor_from_meta(meta: &IndexRelCacheEntry) -> u16 {
    meta.hash_options
        .map(|options| options.fillfactor)
        .unwrap_or(HASH_DEFAULT_FILLFACTOR)
}

pub fn hash_build_bucket_count(index_tuple_count: usize, fillfactor: u16) -> u32 {
    let target = HashMetaPageData::new(1, fillfactor).target_tuples_per_bucket() as usize;
    let desired = index_tuple_count.div_ceil(target).max(2);
    desired.min(HASH_MAX_BUCKETS) as u32
}

pub fn encode_hash_tuple_payload(
    desc: &RelationDesc,
    key_values: &[Value],
    hash: u32,
    default_toast_compression: AttributeCompression,
    services: &dyn AccessScalarServices,
) -> AccessResult<Vec<u8>> {
    let key_payload = encode_key_payload(desc, key_values, default_toast_compression, services)?;
    let mut payload = Vec::with_capacity(4 + key_payload.len());
    payload.extend_from_slice(&hash.to_le_bytes());
    payload.extend_from_slice(&key_payload);
    Ok(payload)
}

pub fn hash_tuple_hash(tuple: &IndexTupleData) -> AccessResult<u32> {
    if tuple.payload.len() < 4 {
        return Err(AccessError::Corrupt("hash tuple payload too short"));
    }
    Ok(u32::from_le_bytes(tuple.payload[0..4].try_into().unwrap()))
}

pub fn hash_tuple_key_values(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
    services: &dyn AccessScalarServices,
) -> AccessResult<Vec<Value>> {
    if tuple.payload.len() < 4 {
        return Err(AccessError::Corrupt("hash tuple payload too short"));
    }
    decode_key_payload(desc, &tuple.payload[4..], services)
}

pub fn hash_page_items(page: &[u8; BLCKSZ]) -> AccessResult<Vec<IndexTupleData>> {
    let mut items = Vec::new();
    let max_offset = page_get_max_offset_number(page).map_err(hash_page_access_error)?;
    for offset in 1..=max_offset {
        let item_id = page_get_item_id(page, offset).map_err(hash_page_access_error)?;
        if item_id.lp_flags != ItemIdFlags::Normal {
            continue;
        }
        let bytes = page_get_item(page, offset).map_err(hash_page_access_error)?;
        items.push(IndexTupleData::parse(bytes).map_err(|err| {
            AccessError::Scalar(format!("hash index tuple parse failed: {err:?}"))
        })?);
    }
    Ok(items)
}

pub fn hash_page_has_space(page: &[u8; BLCKSZ], tuple: &IndexTupleData) -> AccessResult<bool> {
    let header = page_header(page).map_err(hash_page_access_error)?;
    let needed = max_align(tuple.size()) + ITEM_ID_SIZE;
    Ok(header.free_space() >= needed)
}

pub fn hash_page_has_items(page: &[u8; BLCKSZ]) -> AccessResult<bool> {
    Ok(page_get_max_offset_number(page).map_err(hash_page_access_error)? > 0)
}

pub fn hash_split_needed(meta: &HashMetaPageData) -> bool {
    meta.bucket_count() < HASH_MAX_BUCKETS as u32
        && meta.hashm_ntuples > u64::from(meta.bucket_count()) * meta.target_tuples_per_bucket()
}

fn hash_page_access_error(err: PageError) -> AccessError {
    AccessError::Scalar(format!("hash slotted page error: {err:?}"))
}

pub fn hash_metapage_init(page: &mut [u8; BLCKSZ], meta: &HashMetaPageData) {
    hash_page_init(page, 0, LH_META_PAGE);
    hash_metapage_set(page, meta).expect("hash metapage should fit");
}

pub fn hash_metapage_set(
    page: &mut [u8; BLCKSZ],
    meta: &HashMetaPageData,
) -> Result<(), HashPageError> {
    let offset = HASH_PAGE_CONTENT_OFFSET;
    if offset + HASH_META_DATA_SIZE > BLCKSZ - HASH_SPECIAL_SIZE {
        return Err(HashPageError::Corrupt("hash metapage data too large"));
    }
    let mut pos = offset;
    for value in [
        meta.hashm_magic,
        meta.hashm_version,
        meta.hashm_maxbucket,
        meta.hashm_highmask,
        meta.hashm_lowmask,
        meta.hashm_ffactor,
    ] {
        page[pos..pos + 4].copy_from_slice(&value.to_le_bytes());
        pos += 4;
    }
    page[pos..pos + 8].copy_from_slice(&meta.hashm_ntuples.to_le_bytes());
    pos += 8;
    for value in meta.hashm_spares {
        page[pos..pos + 4].copy_from_slice(&value.to_le_bytes());
        pos += 4;
    }
    for value in meta.hashm_bucket_blocks {
        page[pos..pos + 4].copy_from_slice(&value.to_le_bytes());
        pos += 4;
    }
    page[12..14].copy_from_slice(&(pos as u16).to_le_bytes());
    Ok(())
}

pub fn hash_metapage_data(page: &[u8; BLCKSZ]) -> Result<HashMetaPageData, HashPageError> {
    let opaque = hash_page_get_opaque(page)?;
    if !opaque.is_meta() {
        return Err(HashPageError::Corrupt("hash metapage has wrong page type"));
    }
    let mut pos = HASH_PAGE_CONTENT_OFFSET;
    let read_u32 = |page: &[u8; BLCKSZ], pos: &mut usize| -> u32 {
        let value = u32::from_le_bytes(page[*pos..*pos + 4].try_into().unwrap());
        *pos += 4;
        value
    };
    let hashm_magic = read_u32(page, &mut pos);
    let hashm_version = read_u32(page, &mut pos);
    if hashm_magic != HASH_MAGIC {
        return Err(HashPageError::Corrupt("invalid hash metapage magic"));
    }
    if hashm_version != HASH_VERSION {
        return Err(HashPageError::Corrupt("invalid hash metapage version"));
    }
    let hashm_maxbucket = read_u32(page, &mut pos);
    let hashm_highmask = read_u32(page, &mut pos);
    let hashm_lowmask = read_u32(page, &mut pos);
    let hashm_ffactor = read_u32(page, &mut pos);
    let hashm_ntuples = u64::from_le_bytes(page[pos..pos + 8].try_into().unwrap());
    pos += 8;
    let mut hashm_spares = [0; HASH_SPLITPOINTS];
    for value in &mut hashm_spares {
        *value = read_u32(page, &mut pos);
    }
    let mut hashm_bucket_blocks = [HASH_INVALID_BLOCK; HASH_MAX_BUCKETS];
    for value in &mut hashm_bucket_blocks {
        *value = read_u32(page, &mut pos);
    }
    Ok(HashMetaPageData {
        hashm_magic,
        hashm_version,
        hashm_maxbucket,
        hashm_highmask,
        hashm_lowmask,
        hashm_ffactor,
        hashm_ntuples,
        hashm_spares,
        hashm_bucket_blocks,
    })
}

pub const fn hash_page_fillfactor_reserve(fillfactor: u16) -> usize {
    BLCKSZ * (100usize.saturating_sub(fillfactor as usize)) / 100
}

pub const fn hash_page_payload_limit(fillfactor: u16) -> usize {
    BLCKSZ - HASH_SPECIAL_SIZE - MAXALIGN - hash_page_fillfactor_reserve(fillfactor)
}
