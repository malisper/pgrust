use pgrust_catalog_data::PgConversionRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversionEntry {
    pub oid: u32,
    pub name: String,
    pub namespace_oid: u32,
    pub for_encoding: String,
    pub to_encoding: String,
    pub function_name: String,
    pub is_default: bool,
    pub owner_oid: u32,
    pub comment: Option<String>,
}

pub fn conversion_row_from_entry(entry: &ConversionEntry) -> PgConversionRow {
    PgConversionRow {
        oid: entry.oid,
        conname: entry.name.clone(),
        connamespace: entry.namespace_oid,
        conowner: entry.owner_oid,
        conforencoding: conversion_encoding_code(&entry.for_encoding),
        contoencoding: conversion_encoding_code(&entry.to_encoding),
        conproc: 0,
        condefault: entry.is_default,
    }
}

pub fn conversion_encoding_code(name: &str) -> i32 {
    match name.to_ascii_uppercase().as_str() {
        "UTF8" | "UTF-8" => 6,
        "LATIN1" | "ISO88591" | "ISO-8859-1" => 8,
        _ => -1,
    }
}

pub fn conversion_object_name(raw_name: &str) -> String {
    raw_name
        .rsplit_once('.')
        .map(|(_, name)| name.to_ascii_lowercase())
        .unwrap_or_else(|| raw_name.to_ascii_lowercase())
}
