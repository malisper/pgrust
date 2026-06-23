//! Wiring of the *non-libxml2* cross-subsystem dependency seams that
//! `utils/adt/xml.c` reaches through `backend-utils-adt-xml-libxml-seams`.
//!
//! These are NOT libxml2 calls — they are the syscache/lsyscache lookups, the
//! namespace resolution, and the utils/mb encoding conversions xml.c performs.
//! Their real owners are already ported (`backend-utils-cache-lsyscache`,
//! `backend-catalog-namespace`, `backend-utils-mb-mbutils`); here we install the
//! libxml-seams slots from those owners. This is the only crate that both
//! consumes the libxml-seams contract and can name those owners without a
//! dependency cycle.
//!
//! The libxml-seams signatures are owned/`Mcx`-free (`-> PgResult<String>`,
//! `-> i32`, …) while the owner functions are `Mcx`-bound and may return
//! `Option`/`PgString`/`PgVec`. We bridge with a short-lived scratch
//! `MemoryContext` per call (the C side likewise allocates the result in the
//! caller's context and the caller copies it onward) and copy the result out to
//! an owned value before the scratch context drops.
//!
//! Seams whose owner body does not yet exist, or whose contract needs a
//! composite catalog lookup we cannot yet express (`output_function_call`,
//! `deconstruct_array`, `detoast_bytea`, `sqlchar_to_unicode`,
//! `get_database_name`, the SPI family, `relation_info`/`relation_columns`/
//! `type_info`), are intentionally left uninstalled — they stay
//! loud-panicking until their substrate lands (mirror-pg-and-panic).

use alloc::string::String;
use alloc::vec::Vec;

use ::mcx::MemoryContext;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT};

use xml_libxml_seams as seam;

/// `get_typtype(typeoid) == 'd'` (pg_type.typtype TYPTYPE_DOMAIN).
const TYPTYPE_DOMAIN: u8 = b'd';

/// Run `f` with a fresh scratch [`MemoryContext`]; the closure copies whatever
/// it needs out before the context drops.
fn with_scratch<R>(f: impl for<'mcx> FnOnce(::mcx::Mcx<'mcx>) -> R) -> R {
    let cx = MemoryContext::new("xml dep-seam scratch");
    f(cx.mcx())
}

/// Install the cross-subsystem (non-libxml2) dependency seams from their real
/// owners. Called from `init_seams()`.
pub fn install() {
    // --- syscache / lsyscache ---

    // C: `get_namespace_name(nspid)`.
    seam::get_namespace_name::set(|nspid: Oid| -> PgResult<String> {
        with_scratch(|mcx| {
            match lsyscache::namespace_range_index_pubsub::get_namespace_name(
                mcx, nspid,
            )? {
                Some(s) => Ok(s.as_str().to_string()),
                None => Err(PgError::error(alloc::format!(
                    "cache lookup failed for namespace {nspid}"
                ))),
            }
        })
    });

    // C: `get_rel_name(relid)`.
    seam::get_rel_name::set(|relid: Oid| -> PgResult<String> {
        with_scratch(|mcx| {
            match lsyscache::relation::get_rel_name(mcx, relid)? {
                Some(s) => Ok(s.as_str().to_string()),
                None => Err(PgError::error(alloc::format!(
                    "cache lookup failed for relation {relid}"
                ))),
            }
        })
    });

    // C: `getBaseTypeAndTypmod(typeoid, &typmod)`.
    seam::get_base_type_and_typmod::set(|typeoid: Oid, _typmod: i32| -> PgResult<(Oid, i32)> {
        lsyscache::type_::get_base_type_and_typmod(typeoid)
    });

    // C: `get_typtype(typeoid) == TYPTYPE_DOMAIN`.
    seam::is_domain::set(|typeoid: Oid| -> PgResult<bool> {
        let typtype = lsyscache::type_::get_typtype(typeoid)?;
        Ok(typtype == TYPTYPE_DOMAIN)
    });

    // C: `SearchSysCache1(TYPEOID, typeoid)` + GETSTRUCT(Form_pg_type) read of
    // (typname, typnamespace, typtype) for `map_sql_type_to_xml_name`. A cache
    // miss is the C `elog(ERROR, "cache lookup failed for type %u")`.
    seam::type_info::set(|typeoid: Oid| -> PgResult<types_xml::TypeInfo> {
        match syscache_seams::pg_type_form::call(typeoid)? {
            Some(form) => Ok(types_xml::TypeInfo {
                typname: String::from_utf8_lossy(form.typname.name_str()).into_owned(),
                typnamespace: form.typnamespace,
                is_domain: form.typtype as u8 == TYPTYPE_DOMAIN,
            }),
            None => Err(PgError::error(alloc::format!(
                "cache lookup failed for type {typeoid}"
            ))),
        }
    });

    // C: `rel = table_open(relid, AccessShareLock)`; iterate the tuple
    // descriptor reading (attname, atttypid, attisdropped) for every column
    // (dropped columns included; `map_sql_*` filters on `is_dropped`);
    // `table_close(rel, NoLock)`.
    seam::relation_columns::set(|relid: Oid| -> PgResult<Vec<types_xml::RelationColumn>> {
        use ::types_storage::lock::{AccessShareLock, NoLock};
        with_scratch(|mcx| {
            let rel = table::table_open(mcx, relid, AccessShareLock)?;
            let cols = {
                let tupdesc = &*rel.rd_att;
                let natts = tupdesc.natts as usize;
                let mut cols: Vec<types_xml::RelationColumn> = Vec::with_capacity(natts);
                for i in 0..natts {
                    let att = tupdesc.attr(i);
                    cols.push(types_xml::RelationColumn {
                        attname: String::from_utf8_lossy(att.attname.name_str()).into_owned(),
                        atttypid: att.atttypid,
                        is_dropped: att.attisdropped,
                    });
                }
                cols
            };
            table::table_close(rel, NoLock)?;
            Ok(cols)
        })
    });

    // C: `SearchSysCache1(RELOID, relid)` -> `Form_pg_class` (relname,
    // relnamespace). `map_sql_table_to_xmlschema` only reads relname +
    // relnamespace from this tuple; the `columns` field of `RelationInfo` is
    // unused on that path (columns flow in separately), so we leave it empty.
    seam::relation_info::set(|relid: Oid| -> PgResult<types_xml::RelationInfo> {
        with_scratch(|mcx| {
            let relname =
                match lsyscache::relation::get_rel_name(mcx, relid)? {
                    Some(name) => name.to_string(),
                    None => {
                        return Err(PgError::error(alloc::format!(
                            "cache lookup failed for relation {relid}"
                        )))
                    }
                };
            let relnamespace = lsyscache::relation::get_rel_namespace(relid)?;
            Ok(types_xml::RelationInfo {
                relname,
                relnamespace,
                columns: Vec::new(),
            })
        })
    });

    // --- namespace ---

    // C: `LookupExplicitNamespace(name, false)`.
    seam::lookup_namespace::set(|name: &str| -> PgResult<Oid> {
        catalog_namespace::LookupExplicitNamespace(name, false)
    });

    // --- utils/mb encoding ---

    // C: `pg_get_client_encoding()`.
    seam::client_encoding::set(|| -> i32 { mbutils::pg_get_client_encoding() });

    // C: `GetDatabaseEncoding()`.
    seam::get_database_encoding::set(|| -> i32 { mbutils::GetDatabaseEncoding() });

    // C: `pg_unicode_to_server(u, buf)`.
    seam::unicode_to_server::set(|codepoint: u32| -> PgResult<Vec<u8>> {
        with_scratch(|mcx| {
            let v = mbutils::pg_unicode_to_server(mcx, codepoint)?;
            Ok(v.as_slice().to_vec())
        })
    });

    // C: `pg_any_to_server(s, len, encoding)`.
    seam::any_to_server::set(|bytes: &[u8], encoding: i32| -> PgResult<Vec<u8>> {
        with_scratch(|mcx| {
            match mbutils::pg_any_to_server(mcx, bytes, encoding)? {
                Some(v) => Ok(v.as_slice().to_vec()),
                // C returns the input pointer unchanged when no conversion is
                // needed (same encoding); mirror by echoing the input bytes.
                None => Ok(bytes.to_vec()),
            }
        })
    });

    // C: `pg_server_to_any(s, len, encoding)`.
    seam::server_to_any::set(|bytes: &[u8], encoding: i32| -> PgResult<Vec<u8>> {
        with_scratch(|mcx| {
            match mbutils::pg_server_to_any(mcx, bytes, encoding)? {
                Some(v) => Ok(v.as_slice().to_vec()),
                None => Ok(bytes.to_vec()),
            }
        })
    });

    // C: `pg_server_to_client(str, len)`.
    seam::server_to_client::set(|bytes: &[u8]| -> PgResult<Vec<u8>> {
        with_scratch(|mcx| {
            match mbutils::pg_server_to_client(mcx, bytes)? {
                Some(v) => Ok(v.as_slice().to_vec()),
                None => Ok(bytes.to_vec()),
            }
        })
    });

    // C: `pg_mblen(mbstr)`.
    seam::pg_mblen::set(|bytes: &[u8]| -> PgResult<i32> {
        Ok(mbutils::pg_mblen(bytes))
    });

    // C: `sqlchar_to_unicode(s)` (xml.c:2336, static) — the Unicode codepoint of
    // the first server-encoding character of `s`: convert the leading char to
    // UTF-8, then decode that UTF-8 to a single wide char (codepoint).
    seam::sqlchar_to_unicode::set(|s: &[u8]| -> PgResult<u32> {
        use ::types_wchar::encoding::PG_UTF8;
        with_scratch(|mcx| {
            // pg_server_to_any(s, pg_mblen_cstr(s), PG_UTF8)
            let len = mbutils::pg_mblen(s) as usize;
            let leading = &s[..len.min(s.len())];
            let utf8: Vec<u8> =
                match mbutils::pg_server_to_any(mcx, leading, PG_UTF8 as i32)? {
                    Some(v) => v.as_slice().to_vec(),
                    None => leading.to_vec(),
                };
            // pg_encoding_mb2wchar_with_len(PG_UTF8, utf8string,
            //     ret, pg_encoding_mblen(PG_UTF8, utf8string)); return ret[0].
            // `utf8` is exactly the one converted character, so its byte length
            // is `pg_encoding_mblen(PG_UTF8, utf8string)`.
            let ret = mbutils::pg_encoding_mb2wchar_with_len(
                mcx, PG_UTF8, &utf8, utf8.len() as i32,
            )?;
            Ok(ret.first().copied().unwrap_or(0) as u32)
        })
    });

    let _ = ERRCODE_UNDEFINED_OBJECT; // reserved for cache-miss SQLSTATE refinement
}
