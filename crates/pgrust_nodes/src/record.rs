use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use crate::datum::{RecordDescriptor, RecordFieldDesc};
use crate::parsenodes::SqlType;

#[derive(Default)]
struct AnonymousRecordRegistry {
    next_typmod: i32,
    by_key: HashMap<Vec<RecordFieldDesc>, RecordDescriptor>,
    by_typmod: HashMap<i32, RecordDescriptor>,
}

fn registry() -> &'static RwLock<AnonymousRecordRegistry> {
    static REGISTRY: OnceLock<RwLock<AnonymousRecordRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        RwLock::new(AnonymousRecordRegistry {
            next_typmod: 1,
            ..AnonymousRecordRegistry::default()
        })
    })
}

pub fn assign_anonymous_record_descriptor(fields: Vec<(String, SqlType)>) -> RecordDescriptor {
    let key: Vec<RecordFieldDesc> = fields
        .into_iter()
        .map(|(name, sql_type)| RecordFieldDesc { name, sql_type })
        .collect();
    if let Some(existing) = registry().read().unwrap().by_key.get(&key) {
        return existing.clone();
    }

    let mut registry = registry().write().unwrap();
    if let Some(existing) = registry.by_key.get(&key) {
        return existing.clone();
    }

    let typmod = registry.next_typmod;
    registry.next_typmod += 1;
    let descriptor = RecordDescriptor {
        type_oid: pgrust_core::RECORD_TYPE_OID,
        typrelid: 0,
        typmod,
        fields: key.clone(),
    };
    registry.by_typmod.insert(typmod, descriptor.clone());
    registry.by_key.insert(key, descriptor.clone());
    descriptor
}

pub fn lookup_anonymous_record_descriptor(typmod: i32) -> Option<RecordDescriptor> {
    if typmod < 0 {
        return None;
    }
    registry().read().unwrap().by_typmod.get(&typmod).cloned()
}

pub fn register_anonymous_record_descriptor(descriptor: &RecordDescriptor) {
    if descriptor.typrelid != 0 || descriptor.typmod < 0 {
        return;
    }

    let key = descriptor.fields.clone();
    let mut registry = registry().write().unwrap();
    registry
        .by_typmod
        .entry(descriptor.typmod)
        .or_insert_with(|| descriptor.clone());
    registry
        .by_key
        .entry(key)
        .or_insert_with(|| descriptor.clone());
    registry.next_typmod = registry.next_typmod.max(descriptor.typmod + 1);
}
