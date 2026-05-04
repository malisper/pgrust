use std::collections::HashMap;
use std::sync::OnceLock;

use parking_lot::RwLock;

static ROLE_SETTINGS: OnceLock<RwLock<HashMap<(u32, u32), HashMap<String, String>>>> =
    OnceLock::new();

fn role_settings_store() -> &'static RwLock<HashMap<(u32, u32), HashMap<String, String>>> {
    ROLE_SETTINGS.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn store_role_setting(database_oid: u32, role_oid: u32, name: String, value: Option<String>) {
    let mut settings = role_settings_store().write();
    let role_settings = settings.entry((database_oid, role_oid)).or_default();
    if let Some(value) = value {
        role_settings.insert(name, value);
    } else {
        role_settings.remove(&name);
    }
}

pub fn role_settings(database_oid: u32, role_oid: u32) -> HashMap<String, String> {
    role_settings_store()
        .read()
        .get(&(database_oid, role_oid))
        .cloned()
        .unwrap_or_default()
}
