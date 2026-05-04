use crate::access::htup::{
    AttributeDesc, HEAP_HASNULL, HEAP_HASVARWIDTH, HEAP_NATTS_MASK, HeapTuple, HeapTupleHeaderData,
    ItemPointerData, SIZEOF_HEAP_TUPLE_HEADER, TupleError, TupleValue,
};
use crate::varatt::{
    compressed_inline_total_size, external_varlena_size, is_compressed_inline_datum,
};
use pgrust_storage::page::bufpage::{
    OffsetNumber, max_align, page_add_item, page_get_item, page_get_item_id, page_init,
};
use pgrust_storage::smgr::BLCKSZ;

impl HeapTupleHeaderData {
    pub fn new(natts: u16, null_bitmap: Vec<u8>) -> Self {
        let has_nulls = !null_bitmap.is_empty();
        let bitmap_len = if has_nulls { null_bitmap.len() } else { 0 };
        let hoff = max_align(SIZEOF_HEAP_TUPLE_HEADER + bitmap_len) as u8;
        Self {
            xmin: 0,
            xmax: 0,
            cid_or_xvac: 0,
            ctid: ItemPointerData::default(),
            infomask2: natts,
            infomask: if has_nulls { HEAP_HASNULL } else { 0 },
            hoff,
            null_bitmap,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; usize::from(self.hoff)];
        buf[0..4].copy_from_slice(&self.xmin.to_le_bytes());
        buf[4..8].copy_from_slice(&self.xmax.to_le_bytes());
        buf[8..12].copy_from_slice(&self.cid_or_xvac.to_le_bytes());
        buf[12..16].copy_from_slice(&self.ctid.block_number.to_le_bytes());
        buf[16..18].copy_from_slice(&self.ctid.offset_number.to_le_bytes());
        buf[18..20].copy_from_slice(&self.infomask2.to_le_bytes());
        buf[20..22].copy_from_slice(&self.infomask.to_le_bytes());
        buf[22] = self.hoff;
        let bitmap_end = SIZEOF_HEAP_TUPLE_HEADER + self.null_bitmap.len();
        if bitmap_end <= buf.len() {
            buf[SIZEOF_HEAP_TUPLE_HEADER..bitmap_end].copy_from_slice(&self.null_bitmap);
        }
        buf
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, TupleError> {
        if bytes.len() < SIZEOF_HEAP_TUPLE_HEADER {
            return Err(TupleError::HeaderTooShort);
        }
        let hoff = bytes[22];
        if usize::from(hoff) < SIZEOF_HEAP_TUPLE_HEADER || usize::from(hoff) > bytes.len() {
            return Err(TupleError::InvalidHeaderOffset);
        }
        let infomask2 = u16::from_le_bytes([bytes[18], bytes[19]]);
        let infomask = u16::from_le_bytes([bytes[20], bytes[21]]);
        let bitmap_len = if infomask & HEAP_HASNULL != 0 {
            usize::from(infomask2 & HEAP_NATTS_MASK).div_ceil(8)
        } else {
            0
        };
        Ok(Self {
            xmin: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            xmax: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            cid_or_xvac: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            ctid: ItemPointerData {
                block_number: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
                offset_number: u16::from_le_bytes([bytes[16], bytes[17]]),
            },
            infomask2,
            infomask,
            hoff,
            null_bitmap: bytes[SIZEOF_HEAP_TUPLE_HEADER..SIZEOF_HEAP_TUPLE_HEADER + bitmap_len]
                .to_vec(),
        })
    }
}

impl HeapTuple {
    pub fn new_raw(natts: u16, data: Vec<u8>) -> Self {
        Self {
            header: HeapTupleHeaderData::new(natts, Vec::new()),
            data,
        }
    }

    pub fn new_raw_with_null_bitmap(natts: u16, null_bitmap: Vec<u8>, data: Vec<u8>) -> Self {
        Self {
            header: HeapTupleHeaderData::new(natts, null_bitmap),
            data,
        }
    }

    pub fn serialized_len(&self) -> usize {
        usize::from(self.header.hoff) + self.data.len()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = self.header.serialize();
        bytes.extend_from_slice(&self.data);
        bytes
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, TupleError> {
        let header = HeapTupleHeaderData::parse(bytes)?;
        Ok(Self {
            data: bytes[usize::from(header.hoff)..].to_vec(),
            header,
        })
    }

    pub fn from_values(desc: &[AttributeDesc], values: &[TupleValue]) -> Result<Self, TupleError> {
        if desc.len() != values.len() {
            return Err(TupleError::WrongValueCount {
                expected: desc.len(),
                actual: values.len(),
            });
        }
        if desc.len() > usize::from(HEAP_NATTS_MASK) {
            return Err(TupleError::AttributeCountTooLarge(desc.len()));
        }

        let has_nulls = values.iter().any(|v| matches!(v, TupleValue::Null));
        let mut null_bitmap = if has_nulls {
            vec![0u8; bitmap_len(desc.len() as u16)]
        } else {
            Vec::new()
        };

        let mut infomask = 0u16;
        let mut data = Vec::new();

        for (i, (attr, value)) in desc.iter().zip(values.iter()).enumerate() {
            match value {
                TupleValue::Null => {
                    if !attr.nullable {
                        return Err(TupleError::NullNotAllowed {
                            attnum: i + 1,
                            name: attr.name.clone(),
                        });
                    }
                }
                TupleValue::Bytes(bytes) | TupleValue::EncodedVarlena(bytes) => {
                    if has_nulls {
                        null_bitmap[i / 8] |= 1 << (i % 8);
                    }

                    match attr.attlen {
                        len if len > 0 => {
                            let expected = len as usize;
                            if bytes.len() != expected {
                                return Err(TupleError::InvalidValueLength {
                                    attnum: i + 1,
                                    name: attr.name.clone(),
                                    expected,
                                    actual: bytes.len(),
                                });
                            }
                            let aligned = attr.attalign.align_offset(data.len());
                            if aligned > data.len() {
                                data.resize(aligned, 0);
                            }
                            data.extend_from_slice(bytes);
                        }
                        -1 => {
                            infomask |= HEAP_HASVARWIDTH;
                            match value {
                                TupleValue::EncodedVarlena(_) => {
                                    if bytes.is_empty() {
                                        return Err(TupleError::InvalidValueLength {
                                            attnum: i + 1,
                                            name: attr.name.clone(),
                                            expected: 1,
                                            actual: 0,
                                        });
                                    }
                                    if bytes[0] & 0x01 == 0 {
                                        let aligned = attr.attalign.align_offset(data.len());
                                        if aligned > data.len() {
                                            data.resize(aligned, 0);
                                        }
                                    }
                                    data.extend_from_slice(bytes);
                                }
                                TupleValue::Bytes(_) => {
                                    let total_len_1b = 1 + bytes.len();
                                    if total_len_1b <= 127 {
                                        data.push((total_len_1b as u8) << 1 | 0x01);
                                        data.extend_from_slice(bytes);
                                    } else {
                                        let aligned = attr.attalign.align_offset(data.len());
                                        if aligned > data.len() {
                                            data.resize(aligned, 0);
                                        }
                                        let total_len = (4 + bytes.len()) as u32;
                                        data.extend_from_slice(&(total_len << 2).to_le_bytes());
                                        data.extend_from_slice(bytes);
                                    }
                                }
                                TupleValue::Null => unreachable!("handled above"),
                            }
                        }
                        -2 => {
                            infomask |= HEAP_HASVARWIDTH;
                            data.extend_from_slice(bytes);
                            data.push(0);
                        }
                        other => {
                            return Err(TupleError::UnsupportedAttributeType {
                                attnum: i + 1,
                                name: attr.name.clone(),
                                attlen: other,
                            });
                        }
                    }
                }
            }
        }

        if has_nulls {
            infomask |= HEAP_HASNULL;
        }

        let mut header = HeapTupleHeaderData::new(desc.len() as u16, null_bitmap);
        header.infomask |= infomask;

        Ok(Self { header, data })
    }

    pub fn deform<'a>(
        &'a self,
        desc: &[AttributeDesc],
    ) -> Result<Vec<Option<&'a [u8]>>, TupleError> {
        let natts = usize::from(self.header.infomask2 & HEAP_NATTS_MASK);
        if natts > desc.len() {
            return Err(TupleError::WrongValueCount {
                expected: desc.len(),
                actual: natts,
            });
        }

        let mut values = Vec::with_capacity(natts);
        let mut off = 0usize;

        for (i, attr) in desc.iter().take(natts).enumerate() {
            let is_null =
                self.header.infomask & HEAP_HASNULL != 0 && att_isnull(i, &self.header.null_bitmap);
            if is_null {
                values.push(None);
                continue;
            }

            match attr.attlen {
                len if len > 0 => {
                    off = attr.attalign.align_offset(off);
                    let end = off + len as usize;
                    values.push(Some(&self.data[off..end]));
                    off = end;
                }
                -1 => {
                    if let Some(size) = external_varlena_size(&self.data[off..]) {
                        let end = off + size;
                        values.push(Some(&self.data[off..end]));
                        off = end;
                    } else if self.data[off] & 0x01 != 0 {
                        let total_len = (self.data[off] >> 1) as usize;
                        let start = off + 1;
                        let end = off + total_len;
                        values.push(Some(&self.data[start..end]));
                        off = end;
                    } else {
                        off = attr.attalign.align_offset(off);
                        let raw = u32::from_le_bytes([
                            self.data[off],
                            self.data[off + 1],
                            self.data[off + 2],
                            self.data[off + 3],
                        ]);
                        let total_len = (raw >> 2) as usize;
                        let end = off + total_len;
                        if is_compressed_inline_datum(&self.data[off..end]) {
                            values.push(Some(&self.data[off..end]));
                        } else {
                            let start = off + 4;
                            values.push(Some(&self.data[start..end]));
                        }
                        off = end;
                    }
                }
                -2 => {
                    let mut end = off;
                    while self.data[end] != 0 {
                        end += 1;
                    }
                    values.push(Some(&self.data[off..end]));
                    off = end + 1;
                }
                other => {
                    return Err(TupleError::UnsupportedAttributeType {
                        attnum: i + 1,
                        name: attr.name.clone(),
                        attlen: other,
                    });
                }
            }
        }

        Ok(values)
    }
}

pub fn deform_raw<'a>(
    bytes: &'a [u8],
    desc: &[AttributeDesc],
) -> Result<Vec<Option<&'a [u8]>>, TupleError> {
    if bytes.len() < SIZEOF_HEAP_TUPLE_HEADER {
        return Err(TupleError::HeaderTooShort);
    }
    let hoff = bytes[22];
    if usize::from(hoff) < SIZEOF_HEAP_TUPLE_HEADER || usize::from(hoff) > bytes.len() {
        return Err(TupleError::InvalidHeaderOffset);
    }
    let infomask2 = u16::from_le_bytes([bytes[18], bytes[19]]);
    let infomask = u16::from_le_bytes([bytes[20], bytes[21]]);
    let natts = usize::from(infomask2 & HEAP_NATTS_MASK);
    if natts > desc.len() {
        return Err(TupleError::WrongValueCount {
            expected: desc.len(),
            actual: natts,
        });
    }

    let null_bitmap = if infomask & HEAP_HASNULL != 0 {
        &bytes[SIZEOF_HEAP_TUPLE_HEADER..]
    } else {
        &[] as &[u8]
    };
    let data = &bytes[usize::from(hoff)..];

    let mut values = Vec::with_capacity(natts);
    let mut off = 0usize;

    for (i, attr) in desc.iter().take(natts).enumerate() {
        let is_null = infomask & HEAP_HASNULL != 0 && att_isnull(i, null_bitmap);
        if is_null {
            values.push(None);
            continue;
        }

        match attr.attlen {
            len if len > 0 => {
                off = attr.attalign.align_offset(off);
                let end = off + len as usize;
                values.push(Some(&data[off..end]));
                off = end;
            }
            -1 => {
                if let Some(size) = external_varlena_size(&data[off..]) {
                    let end = off + size;
                    values.push(Some(&data[off..end]));
                    off = end;
                } else if data[off] & 0x01 != 0 {
                    let total_len = (data[off] >> 1) as usize;
                    let start = off + 1;
                    let end = off + total_len;
                    values.push(Some(&data[start..end]));
                    off = end;
                } else {
                    off = attr.attalign.align_offset(off);
                    let total_len = compressed_inline_total_size(&data[off..])
                        .ok_or(TupleError::HeaderTooShort)?;
                    let end = off + total_len;
                    if is_compressed_inline_datum(&data[off..end]) {
                        values.push(Some(&data[off..end]));
                    } else {
                        let start = off + 4;
                        values.push(Some(&data[start..end]));
                    }
                    off = end;
                }
            }
            -2 => {
                let mut end = off;
                while data[end] != 0 {
                    end += 1;
                }
                values.push(Some(&data[off..end]));
                off = end + 1;
            }
            other => {
                return Err(TupleError::UnsupportedAttributeType {
                    attnum: i + 1,
                    name: attr.name.clone(),
                    attlen: other,
                });
            }
        }
    }

    Ok(values)
}

pub fn heap_page_init(page: &mut [u8; BLCKSZ]) {
    page_init(page, 0);
}

pub fn heap_page_add_tuple(
    page: &mut [u8; BLCKSZ],
    block_number: u32,
    tuple: &HeapTuple,
) -> Result<OffsetNumber, TupleError> {
    let mut stored = tuple.clone();
    stored.header.ctid = ItemPointerData {
        block_number,
        offset_number: 0,
    };
    let offset = page_add_item(page, &stored.serialize())?;
    let item_bytes = page_get_item(page, offset)?;
    let mut parsed = HeapTuple::parse(item_bytes)?;
    parsed.header.ctid.offset_number = offset;
    let rewritten = parsed.serialize();

    let item_id = page_get_item_id(page, offset)?;
    let start = usize::from(item_id.lp_off);
    let end = start + usize::from(item_id.lp_len);
    page[start..end].copy_from_slice(&rewritten);
    Ok(offset)
}

pub fn heap_page_get_tuple(
    page: &[u8; BLCKSZ],
    offset: OffsetNumber,
) -> Result<HeapTuple, TupleError> {
    Ok(HeapTuple::parse(page_get_item(page, offset)?)?)
}

pub fn heap_page_get_ctid(
    page: &[u8; BLCKSZ],
    offset: OffsetNumber,
) -> Result<ItemPointerData, TupleError> {
    let bytes = page_get_item(page, offset)?;
    if bytes.len() < 18 {
        return Err(TupleError::HeaderTooShort);
    }
    Ok(ItemPointerData {
        block_number: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        offset_number: u16::from_le_bytes([bytes[16], bytes[17]]),
    })
}

pub fn heap_page_replace_tuple(
    page: &mut [u8; BLCKSZ],
    offset: OffsetNumber,
    tuple: &HeapTuple,
) -> Result<(), TupleError> {
    let item_id = page_get_item_id(page, offset)?;
    let start = usize::from(item_id.lp_off);
    let existing_infomask = u16::from_le_bytes([page[start + 20], page[start + 21]]);
    let layout_bits = HEAP_HASNULL | HEAP_HASVARWIDTH;
    let infomask = (tuple.header.infomask & !layout_bits) | (existing_infomask & layout_bits);

    page[start..start + 4].copy_from_slice(&tuple.header.xmin.to_le_bytes());
    page[start + 4..start + 8].copy_from_slice(&tuple.header.xmax.to_le_bytes());
    page[start + 8..start + 12].copy_from_slice(&tuple.header.cid_or_xvac.to_le_bytes());
    page[start + 12..start + 16].copy_from_slice(&tuple.header.ctid.block_number.to_le_bytes());
    page[start + 16..start + 18].copy_from_slice(&tuple.header.ctid.offset_number.to_le_bytes());
    page[start + 20..start + 22].copy_from_slice(&infomask.to_le_bytes());
    Ok(())
}

fn bitmap_len(natts: u16) -> usize {
    usize::from(natts).div_ceil(8)
}

pub fn att_isnull(attnum: usize, bits: &[u8]) -> bool {
    (bits[attnum >> 3] & (1 << (attnum & 0x07))) == 0
}
