use datum::Datum;
use detoast::VarattExternal;
use heaptuple::heap_form_tuple;
use mcx::PgVec;
use types_core::Oid;
use types_error::PgResult;
use types_rel::RelationData;
use types_tuple::heap_deform_tuple;

use crate::{
    dl_iter, rb_error, ChangeId, ListHead, ReorderBuffer, ReorderBufferChangeData, TxnId,
};

pub struct ReorderBufferToastEnt {
    pub chunk_id: Oid,
    pub last_chunk_seq: i32,
    pub num_chunks: usize,
    pub size: usize,
    pub(crate) chunks: ListHead,
    pub reconstructed: Option<PgVec<'static, u8>>,
}

const VARHDRSZ: usize = 4;
const VARHDRSZ_SHORT: usize = 1;
const VARHDRSZ_EXTERNAL: usize = 2;
const VARTAG_INDIRECT: u8 = 1;
const VARTAG_EXPANDED_RO: u8 = 2;
const VARTAG_EXPANDED_RW: u8 = 3;
const VARTAG_ONDISK: u8 = 18;

fn vartag_size(tag: u8) -> usize {
    match tag {
        VARTAG_INDIRECT => 8,
        VARTAG_EXPANDED_RO | VARTAG_EXPANDED_RW => 8,
        VARTAG_ONDISK => 16,
        _ => panic!("unrecognized TOAST vartag {tag}"),
    }
}

fn is_1b_e(b: &[u8]) -> bool {
    b[0] == 0x01
}

fn is_1b(b: &[u8]) -> bool {
    b[0] & 0x01 == 0x01
}

fn is_4b(b: &[u8]) -> bool {
    b[0] & 0x01 == 0x00
}

fn is_4b_u(b: &[u8]) -> bool {
    b[0] & 0x03 == 0x00
}

fn varsize_4b(b: &[u8]) -> usize {
    (u32::from_ne_bytes([b[0], b[1], b[2], b[3]]) >> 2) as usize
}

fn varsize_short(b: &[u8]) -> usize {
    ((b[0] >> 1) & 0x7F) as usize
}

// varattrib_4b header word (varatt.h, little-endian build).
fn varsize_4b_word(len: usize, compressed: bool) -> [u8; 4] {
    (((len as u32) << 2) | if compressed { 0x02 } else { 0x00 }).to_ne_bytes()
}

/// # Safety: `p` points at a live varlena image readable for its full length.
pub(crate) unsafe fn varlena_image<'a>(p: *const u8) -> &'a [u8] {
    let b0 = *p;
    let len = if b0 == 0x01 {
        VARHDRSZ_EXTERNAL + vartag_size(*p.add(1))
    } else if b0 & 0x01 == 0x01 {
        ((b0 >> 1) & 0x7F) as usize
    } else {
        let w = core::ptr::read_unaligned(p as *const u32);
        (w >> 2) as usize
    };
    core::slice::from_raw_parts(p, len)
}

impl ReorderBuffer {
    // ReorderBufferToastAppendChunk; caller already unlinked `cid` from its
    // transaction's change list.
    pub fn toast_append_chunk(
        &mut self,
        txn: TxnId,
        relation: &RelationData<'static>,
        cid: ChangeId,
    ) -> PgResult<()> {
        debug_assert!(catalog::IsToastRelation(relation));
        self.toast_append_chunk_with_desc(txn, &relation.rd_att, cid)
    }

    pub(crate) fn toast_append_chunk_with_desc(
        &mut self,
        txn: TxnId,
        desc: &types_tuple::TupleDescData<'static>,
        cid: ChangeId,
    ) -> PgResult<()> {
        let natts = desc.natts as usize;
        let mut values = vec![Datum::from_usize(0); natts];
        let mut isnull = vec![false; natts];
        {
            let change = self.change(cid);
            let ReorderBufferChangeData::Tp { newtuple: Some(newtup), .. } = &change.data else {
                unreachable!("toast chunk change carries a new tuple");
            };
            heap_deform_tuple(newtup.as_tuple(), desc, &mut values, &mut isnull);
        }
        debug_assert!(!isnull[0] && !isnull[1] && !isnull[2]);
        let chunk_id = values[0].as_usize() as u32;
        let chunk_seq = values[1].as_usize() as u32 as i32;

        // SAFETY: deform yielded a by-ref datum into the live newtuple image.
        let chunk = unsafe { varlena_image(values[2].as_usize() as *const u8) };

        let chunksize = if is_4b_u(chunk) {
            varsize_4b(chunk) - VARHDRSZ
        } else if is_1b(chunk) && !is_1b_e(chunk) {
            varsize_short(chunk) - VARHDRSZ_SHORT
        } else {
            return Err(rb_error("unexpected type of toast chunk".into()));
        };

        let mut hash = self.txn_mut(txn).toast_hash.take().unwrap_or_else(|| {
            mcx::PgFxHashMap::with_hasher_in(Default::default(), crate::rb_mcx())
        });

        match hash.get(&chunk_id).map(|e| e.last_chunk_seq + 1) {
            Some(expected) => {
                if chunk_seq != expected {
                    self.txn_mut(txn).toast_hash = Some(hash);
                    return Err(rb_error(format!(
                        "got sequence entry {chunk_seq} for toast chunk {chunk_id} instead of seq {expected}"
                    )));
                }
            }
            None => {
                if chunk_seq != 0 {
                    self.txn_mut(txn).toast_hash = Some(hash);
                    return Err(rb_error(format!(
                        "got sequence entry {chunk_seq} for toast chunk {chunk_id} instead of seq 0"
                    )));
                }
                hash.insert(
                    chunk_id,
                    ReorderBufferToastEnt {
                        chunk_id,
                        last_chunk_seq: 0,
                        num_chunks: 0,
                        size: 0,
                        chunks: ListHead::EMPTY,
                        reconstructed: None,
                    },
                );
            }
        }
        let ent = hash.get_mut(&chunk_id).expect("present");

        ent.size += chunksize;
        ent.last_chunk_seq = chunk_seq;
        ent.num_chunks += 1;
        let mut list = ent.chunks;
        crate::dl_push_tail(&mut self.changes, &mut list, cid, |c| &mut c.node);
        ent.chunks = list;

        self.txn_mut(txn).toast_hash = Some(hash);
        Ok(())
    }

    // ReorderBufferToastReplace. DIVERGENCE: C rebuilds changed toasted
    // columns as VARTAG_INDIRECT pointers at the reconstructed chunks; this
    // port inlines the reconstructed (possibly still compressed) varlena into
    // the rebuilt tuple — same detoasted value, no unsafe pointer datum.
    pub fn toast_replace(
        &mut self,
        txn: TxnId,
        relation: &RelationData<'static>,
        cid: ChangeId,
    ) -> PgResult<()> {
        if self.txn(txn).toast_hash.is_none() {
            return Ok(());
        }

        let toast_rel = relcache::RelationIdGetRelation(relation.rd_rel.reltoastrelid)?
            .ok_or_else(|| {
                rb_error(format!(
                    "could not open toast relation with OID {} (base relation \"{}\")",
                    relation.rd_rel.reltoastrelid,
                    relation.name(),
                ))
            })?;
        self.toast_replace_with_descs(txn, &relation.rd_att, &toast_rel.rd_att, cid)
    }

    pub(crate) fn toast_replace_with_descs(
        &mut self,
        txn: TxnId,
        desc: &types_tuple::TupleDescData<'static>,
        toast_desc: &types_tuple::TupleDescData<'static>,
        cid: ChangeId,
    ) -> PgResult<()> {
        if self.txn(txn).toast_hash.is_none() {
            return Ok(());
        }
        let old_size = self.change_size(cid);
        let natts = desc.natts as usize;
        let toast_natts = toast_desc.natts as usize;

        let mut values = vec![Datum::from_usize(0); natts];
        let mut isnull = vec![false; natts];
        let (t_self, t_table_oid) = {
            let change = self.change(cid);
            let ReorderBufferChangeData::Tp { newtuple: Some(newtup), .. } = &change.data else {
                unreachable!("toast replace requires a new tuple");
            };
            heap_deform_tuple(newtup.as_tuple(), desc, &mut values, &mut isnull);
            (newtup.t_self, newtup.t_tableOid)
        };

        let mut hash = self.txn_mut(txn).toast_hash.take().expect("checked above");
        let mut replaced_any = false;

        for natt in 0..natts {
            let attr = desc.attr(natt);
            if attr.attnum < 0 || attr.attisdropped || attr.attlen != -1 || isnull[natt] {
                continue;
            }

            // SAFETY: deform yielded a by-ref datum into the live newtuple image.
            let varlena = unsafe { varlena_image(values[natt].as_usize() as *const u8) };
            if !is_1b_e(varlena) {
                continue;
            }
            let toast_pointer = VarattExternal::from_image(varlena);
            let Some(ent) = hash.get_mut(&toast_pointer.va_valueid) else {
                continue;
            };

            let mut reconstructed: PgVec<'static, u8> =
                mcx::vec_with_capacity_in(self.mcx, toast_pointer.va_rawsize as usize)?;
            reconstructed.extend_from_slice(&[0u8; VARHDRSZ]);

            let mut data_done = 0usize;
            for chunk_cid in dl_iter(&self.changes, ent.chunks, |c| c.node) {
                let mut cvalues = vec![Datum::from_usize(0); toast_natts];
                let mut cisnull = vec![false; toast_natts];
                let cchange = self.change(chunk_cid);
                let ReorderBufferChangeData::Tp { newtuple: Some(ctup), .. } = &cchange.data
                else {
                    unreachable!("toast chunk change carries a new tuple");
                };
                heap_deform_tuple(ctup.as_tuple(), toast_desc, &mut cvalues, &mut cisnull);
                debug_assert!(!cisnull[2]);
                // SAFETY: deform yielded a by-ref datum into the live chunk image.
                let chunk = unsafe { varlena_image(cvalues[2].as_usize() as *const u8) };
                debug_assert!(is_4b(chunk));
                mcx::vec_append_bytes(&mut reconstructed, &chunk[VARHDRSZ..varsize_4b(chunk)])?;
                data_done += varsize_4b(chunk) - VARHDRSZ;
            }
            debug_assert_eq!(data_done as u32, toast_pointer.extsize());

            let header = varsize_4b_word(data_done + VARHDRSZ, toast_pointer.is_compressed());
            reconstructed[..VARHDRSZ].copy_from_slice(&header);

            ent.reconstructed = Some(reconstructed);
            values[natt] = Datum::from_usize(
                ent.reconstructed.as_ref().expect("just set").as_ptr() as usize,
            );
            replaced_any = true;
        }

        self.txn_mut(txn).toast_hash = Some(hash);

        if replaced_any {
            let mut formed = heap_form_tuple(self.mcx, desc, &values, &isnull)?;
            formed.t_self = t_self;
            formed.t_tableOid = t_table_oid;
            match &mut self.change_mut(cid).data {
                ReorderBufferChangeData::Tp { newtuple, .. } => *newtuple = Some(formed),
                _ => unreachable!("toast replace requires Tp data"),
            }
        }

        self.change_memory_update(Some(cid), None, false, old_size);
        let new_size = self.change_size(cid);
        self.change_memory_update(Some(cid), None, true, new_size);
        Ok(())
    }

    pub fn toast_reset(&mut self, txn: TxnId) {
        let Some(hash) = self.txn_mut(txn).toast_hash.take() else {
            return;
        };
        for (_, mut ent) in hash {
            ent.reconstructed = None;
            let chunks: Vec<ChangeId> =
                dl_iter(&self.changes, ent.chunks, |c| c.node).collect();
            let mut list = ent.chunks;
            for chunk_cid in chunks {
                crate::dl_delete(&mut self.changes, &mut list, chunk_cid, |c| &mut c.node);
                self.free_change(chunk_cid, true);
            }
        }
    }
}
