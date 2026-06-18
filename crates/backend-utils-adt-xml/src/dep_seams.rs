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

use mcx::MemoryContext;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT};

use backend_utils_adt_xml_libxml_seams as seam;

/// `get_typtype(typeoid) == 'd'` (pg_type.typtype TYPTYPE_DOMAIN).
const TYPTYPE_DOMAIN: u8 = b'd';

/// Run `f` with a fresh scratch [`MemoryContext`]; the closure copies whatever
/// it needs out before the context drops.
fn with_scratch<R>(f: impl for<'mcx> FnOnce(mcx::Mcx<'mcx>) -> R) -> R {
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
            match backend_utils_cache_lsyscache::namespace_range_index_pubsub::get_namespace_name(
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
            match backend_utils_cache_lsyscache::relation::get_rel_name(mcx, relid)? {
                Some(s) => Ok(s.as_str().to_string()),
                None => Err(PgError::error(alloc::format!(
                    "cache lookup failed for relation {relid}"
                ))),
            }
        })
    });

    // C: `getBaseTypeAndTypmod(typeoid, &typmod)`.
    seam::get_base_type_and_typmod::set(|typeoid: Oid, _typmod: i32| -> PgResult<(Oid, i32)> {
        backend_utils_cache_lsyscache::type_::get_base_type_and_typmod(typeoid)
    });

    // C: `get_typtype(typeoid) == TYPTYPE_DOMAIN`.
    seam::is_domain::set(|typeoid: Oid| -> PgResult<bool> {
        let typtype = backend_utils_cache_lsyscache::type_::get_typtype(typeoid)?;
        Ok(typtype == TYPTYPE_DOMAIN)
    });

    // --- namespace ---

    // C: `LookupExplicitNamespace(name, false)`.
    seam::lookup_namespace::set(|name: &str| -> PgResult<Oid> {
        backend_catalog_namespace::LookupExplicitNamespace(name, false)
    });

    // --- utils/mb encoding ---

    // C: `pg_get_client_encoding()`.
    seam::client_encoding::set(|| -> i32 { backend_utils_mb_mbutils::pg_get_client_encoding() });

    // C: `GetDatabaseEncoding()`.
    seam::get_database_encoding::set(|| -> i32 { backend_utils_mb_mbutils::GetDatabaseEncoding() });

    // C: `pg_unicode_to_server(u, buf)`.
    seam::unicode_to_server::set(|codepoint: u32| -> PgResult<Vec<u8>> {
        with_scratch(|mcx| {
            let v = backend_utils_mb_mbutils::pg_unicode_to_server(mcx, codepoint)?;
            Ok(v.as_slice().to_vec())
        })
    });

    // C: `pg_any_to_server(s, len, encoding)`.
    seam::any_to_server::set(|bytes: &[u8], encoding: i32| -> PgResult<Vec<u8>> {
        with_scratch(|mcx| {
            match backend_utils_mb_mbutils::pg_any_to_server(mcx, bytes, encoding)? {
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
            match backend_utils_mb_mbutils::pg_server_to_any(mcx, bytes, encoding)? {
                Some(v) => Ok(v.as_slice().to_vec()),
                None => Ok(bytes.to_vec()),
            }
        })
    });

    // C: `pg_server_to_client(str, len)`.
    seam::server_to_client::set(|bytes: &[u8]| -> PgResult<Vec<u8>> {
        with_scratch(|mcx| {
            match backend_utils_mb_mbutils::pg_server_to_client(mcx, bytes)? {
                Some(v) => Ok(v.as_slice().to_vec()),
                None => Ok(bytes.to_vec()),
            }
        })
    });

    // C: `pg_mblen(mbstr)`.
    seam::pg_mblen::set(|bytes: &[u8]| -> PgResult<i32> {
        Ok(backend_utils_mb_mbutils::pg_mblen(bytes))
    });

    let _ = ERRCODE_UNDEFINED_OBJECT; // reserved for cache-miss SQLSTATE refinement
}
