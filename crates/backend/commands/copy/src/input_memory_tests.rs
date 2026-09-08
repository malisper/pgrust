use std::rc::Rc;

use datum::Datum;
use mcx::{MemoryContext, PgVec};
use types_slot::TupleSlotKind;
use types_tuple::{CompactAttribute, FormData_pg_attribute, TupleDescData};

#[test]
fn buffered_copy_values_survive_input_context_resets() {
    let statement = MemoryContext::new("copy-buffer-test");
    let mcx = statement.mcx();
    let mut input = MemoryContext::new_bump("copy-input-test");
    let att = FormData_pg_attribute {
        attnum: 1,
        attlen: -1,
        attbyval: false,
        attalign: types_tuple::TYPALIGN_INT,
        attstorage: types_tuple::TYPSTORAGE_EXTENDED,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    attrs.push(att);
    let mut compact_attrs = PgVec::new_in(mcx);
    compact_attrs.push(CompactAttribute::populate_from(&att));
    let desc = Rc::new(TupleDescData {
        natts: 1, tdtypeid: 0, tdtypmod: -1, tdrefcount: -1,
        constr: None, attrs, compact_attrs,
    });
    for kind in [TupleSlotKind::Virtual, TupleSlotKind::HeapTuple, TupleSlotKind::BufferHeapTuple] {
        let mut slots = Vec::new();
        for i in 0..8 {
            input.reset();
            let mut image = PgVec::new_in(input.mcx());
            let len = 128 + i * 17;
            mcx::vec_append_bytes(&mut image,
                &types_tuple::varatt::set_varsize_4b_word((len + 4) as u32).to_ne_bytes()).unwrap();
            image.resize(len + 4, b'a' + i as u8);
            let value = Datum::from_usize(image.as_ptr() as usize);
            std::mem::forget(image);
            let mut slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc.clone()));
            slot.base_mut().tts_values[0] = value;
            slot.base_mut().tts_isnull[0] = false;
            exectuples::exec_store_virtual_tuple(&mut slot);
            exectuples::exec_materialize_slot(&mut slot, mcx).unwrap();
            slots.push(slot);
        }
        input.reset();
        for (i, slot) in slots.iter_mut().enumerate() {
            let mut isnull = true;
            let value = exectuples::slot_getattr(slot, 1, &mut isnull);
            assert!(!isnull);
            // SAFETY: materialization owns this image independently of the reset input context.
            let bytes = unsafe {
                let ptr = value.as_usize() as *const u8;
                let header = if types_tuple::varatt::varatt_is_1b(ptr) { 1 } else { 4 };
                std::slice::from_raw_parts(ptr.add(header), types_tuple::varatt::varsize_any(ptr) - header)
            };
            assert_eq!(bytes.len(), 128 + i * 17);
            assert!(bytes.iter().all(|&b| b == b'a' + i as u8));
            exectuples::exec_clear_tuple(slot, mcx);
        }
    }
}
