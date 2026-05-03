use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::catalog::CatalogError;
use crate::backend::storage::smgr::BLCKSZ;
use crate::include::access::brin_internal::BrinTupleLocation;

pub(crate) use pgrust_access::brin::revmap::BrinRevmap;

pub(crate) fn brin_revmap_initialize(
    index_pages: &[[u8; BLCKSZ]],
) -> Result<BrinRevmap, CatalogError> {
    pgrust_access::brin::revmap::brin_revmap_initialize(index_pages).map_err(map_access_error)
}

pub(crate) fn brin_revmap_extend(
    index_pages: &mut Vec<[u8; BLCKSZ]>,
    revmap: &mut BrinRevmap,
    heap_blk: u32,
) -> Result<(), CatalogError> {
    pgrust_access::brin::revmap::brin_revmap_extend(index_pages, revmap, heap_blk)
        .map_err(map_access_error)
}

pub(crate) fn brin_revmap_get_location(
    index_pages: &[[u8; BLCKSZ]],
    revmap: &BrinRevmap,
    heap_blk: u32,
) -> Result<BrinTupleLocation, CatalogError> {
    pgrust_access::brin::revmap::brin_revmap_get_location(index_pages, revmap, heap_blk)
        .map_err(map_access_error)
}

pub(crate) fn brin_revmap_set_location(
    index_pages: &mut Vec<[u8; BLCKSZ]>,
    revmap: &mut BrinRevmap,
    heap_blk: u32,
    location: BrinTupleLocation,
) -> Result<(), CatalogError> {
    pgrust_access::brin::revmap::brin_revmap_set_location(index_pages, revmap, heap_blk, location)
        .map_err(map_access_error)
}

pub(crate) fn brin_revmap_desummarize_range(
    index_pages: &mut Vec<[u8; BLCKSZ]>,
    revmap: &mut BrinRevmap,
    heap_blk: u32,
) -> Result<bool, CatalogError> {
    pgrust_access::brin::revmap::brin_revmap_desummarize_range(index_pages, revmap, heap_blk)
        .map_err(map_access_error)
}

pub(crate) fn brin_revmap_get_tuple_bytes<'a>(
    index_pages: &'a [[u8; BLCKSZ]],
    revmap: &BrinRevmap,
    heap_blk: u32,
) -> Result<Option<(BrinTupleLocation, &'a [u8])>, CatalogError> {
    pgrust_access::brin::revmap::brin_revmap_get_tuple_bytes(index_pages, revmap, heap_blk)
        .map_err(map_access_error)
}
