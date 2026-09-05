//! pg_import_system_collations (collationcmds.c:836-1054) with its static
//! subroutines normalize_libc_locale_name, cmpaliases (a byte-order sort),
//! get_icu_locale_comment (pg_locale::icu_locale_display_name_ascii) and
//! create_collation_from_locale. The libc leg is C's READ_LOCALE_A_OUTPUT
//! (every non-Windows build); the ICU leg follows pg_locale's runtime-bound
//! libicu, absent = a C build without USE_ICU. The Windows
//! EnumSystemLocalesEx leg (ENUM_SYSTEM_LOCALE) has no target here.

use elog::ereport;
use mcx::Mcx;
use pg_collation::CollationForm;
use pg_locale::{COLLPROVIDER_ICU, COLLPROVIDER_LIBC};
use types_core::{Oid, OidIsValid, COLLATION_RELATION_ID};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG1, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_UNDEFINED_SCHEMA, ERROR, WARNING,
};

// pg_locale.h:25.
const LOCALE_NAME_BUFLEN: usize = 128;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/commands/collationcmds.c", line, func)
}

/// normalize_libc_locale_name (collationcmds.c:625-648): strip encoding tags
/// such as ".utf8" ("en_US.utf8" -> "en_US", "br_FR.iso885915@euro" ->
/// "br_FR@euro"); Some(new name) only when a different name was generated.
pub(crate) fn normalize_libc_locale_name(old: &str) -> Option<String> {
    let o = old.as_bytes();
    let mut new: Vec<u8> = Vec::with_capacity(o.len());
    let mut changed = false;
    let mut i = 0;
    while i < o.len() {
        if o[i] == b'.' {
            // skip over encoding tag such as ".utf8" or ".UTF-8"
            i += 1;
            while i < o.len() && (o[i].is_ascii_alphanumeric() || o[i] == b'-') {
                i += 1;
            }
            changed = true;
        } else {
            new.push(o[i]);
            i += 1;
        }
    }
    changed.then(|| String::from_utf8_lossy(&new).into_owned())
}

fn create_libc_collation<'mcx>(
    mcx: Mcx<'mcx>,
    collname: &str,
    nspid: Oid,
    enc: i32,
    locale: &str,
) -> PgResult<Oid> {
    let version = pg_locale::get_collation_actual_version(COLLPROVIDER_LIBC, locale)?;
    pg_collation::CollationCreate(
        mcx,
        collname,
        nspid,
        miscinit::GetUserId(),
        &CollationForm {
            collprovider: COLLPROVIDER_LIBC,
            collisdeterministic: true,
            collencoding: enc,
            collcollate: Some(locale),
            collctype: Some(locale),
            colllocale: None,
            collicurules: None,
            collversion: version.as_deref(),
        },
        true,
        true,
    )
}

/// create_collation_from_locale (collationcmds.c:727-786): returns the
/// locale's encoding, or -1 when it is not valid for creating a collation;
/// bumps `nvalid` for a usable locale and `ncreated` when a row was made.
fn create_collation_from_locale<'mcx>(
    mcx: Mcx<'mcx>,
    locale: &str,
    nspid: Oid,
    nvalid: &mut i32,
    ncreated: &mut i32,
) -> PgResult<i32> {
    let skip = |line: i32, why: &str| -> PgResult<i32> {
        ereport(DEBUG1)
            .errmsg_internal(format!("skipping locale with {why}: \"{locale}\""))
            .finish(loc(line, "create_collation_from_locale"))?;
        Ok(-1)
    };
    // Locale names not made of ASCII letters need the locale itself to be
    // interpreted; C filters them out.
    if !locale.is_ascii() {
        return skip(741, "non-ASCII name");
    }
    let enc = pg_locale::pg_get_encoding_from_locale(Some(locale), false)?;
    if enc < 0 {
        return skip(748, "unrecognized encoding");
    }
    if !wchar::pg_valid_be_encoding(enc) {
        return skip(753, "client-only encoding");
    }
    if enc == wchar::PG_SQL_ASCII {
        return Ok(-1); // C/POSIX are already in the catalog
    }

    // count valid locales found in operating system
    *nvalid += 1;

    // Create a collation named the same as the locale, quietly doing nothing
    // if it already exists ("locale -a" can report a name more than once).
    let collid = create_libc_collation(mcx, locale, nspid, enc, locale)?;
    if OidIsValid(collid) {
        *ncreated += 1;
        // Must do CCI between inserts to handle duplicates correctly
        xact::CommandCounterIncrement()?;
    }

    Ok(enc)
}

/// pg_import_system_collations (collationcmds.c:836-1054): add known system
/// collations to pg_collation under `nspid`; returns the number created.
pub fn pg_import_system_collations<'mcx>(mcx: Mcx<'mcx>, nspid: Oid) -> PgResult<i32> {
    let mut ncreated: i32 = 0;

    if !superuser::superuser()? {
        return Err(Box::new(
            PgError::error("must be superuser to import system collations")
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }

    if !cache_syscache::SearchSysCacheExists(
        cache_syscache::cacheinfo::NAMESPACEOID,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(nspid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )? {
        return Err(Box::new(
            PgError::error(format!("schema with OID {nspid} does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_SCHEMA),
        ));
    }

    // Load collations known to libc, using "locale -a" to enumerate them.
    {
        let mut nvalid: i32 = 0;
        // (localename, alias, enc) — the aliases are saved up and added after
        // the whole "locale -a" output was read, since one might conflict
        // with a name seen later.
        let mut aliases: Vec<(String, String, i32)> = Vec::new();

        let locale_a_handle = fd::OpenPipeStream("locale -a", "r")?;
        if locale_a_handle < 0 {
            ereport(ERROR)
                .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg("could not execute command \"locale -a\": %m")
                .finish(loc(870, "pg_import_system_collations"))?;
        }

        let mut localebuf = [0u8; LOCALE_NAME_BUFLEN];
        loop {
            // fgets(localebuf, sizeof(localebuf), ...): NULL (EOF or error)
            // ends the loop.
            let len = match fd::PipeStreamGets(locale_a_handle, &mut localebuf) {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            let line = &localebuf[..len];
            if line.last() != Some(&b'\n') {
                ereport(DEBUG1)
                    .errmsg_internal(format!(
                        "skipping locale with too-long name: \"{}\"",
                        String::from_utf8_lossy(line)
                    ))
                    .finish(loc(884, "pg_import_system_collations"))?;
                continue;
            }
            let locale = String::from_utf8_lossy(&line[..len - 1]).into_owned();

            let enc = create_collation_from_locale(mcx, &locale, nspid, &mut nvalid, &mut ncreated)?;
            if enc < 0 {
                continue;
            }

            // Generate aliases such as "en_US" in addition to "en_US.utf8"
            // for ease of use (collation names are unique per encoding only).
            if let Some(alias) = normalize_libc_locale_name(&locale) {
                aliases.push((locale, alias, enc));
            }
        }

        // The return value is not checked: a missing "locale" command is
        // supported (the WARNING below covers it).
        fd::ClosePipeStream(locale_a_handle)?;

        // Sort by locale name so that among several names with the same base
        // name and encoding ("en_US.utf8" vs "en_US.utf-8") a deterministic
        // one wins: first in ASCII order (cmpaliases = strcmp on localename).
        aliases.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

        // Now add aliases, ignoring any that match pre-existing entries.
        for (locale, alias, enc) in &aliases {
            let collid = create_libc_collation(mcx, alias, nspid, *enc, locale)?;
            if OidIsValid(collid) {
                ncreated += 1;
                xact::CommandCounterIncrement()?;
            }
        }

        // Give a warning if "locale -a" seems to be malfunctioning.
        if nvalid == 0 {
            ereport(WARNING)
                .errmsg("no usable system locales were found")
                .finish(loc(956, "pg_import_system_collations"))?;
        }
    }

    // Load collations known to ICU: uloc_countAvailable()/uloc_getAvailable()
    // (the full set of language+region combinations, unlike ucol_*), with
    // the root locale ("") sneaked in first.
    if let Some(ids) = pg_locale::icu_available_locale_ids() {
        for name in core::iter::once(String::new()).chain(ids) {
            // At ERROR the failure arm raised; None is not reachable here.
            let Some(langtag) = pg_locale::icu_language_tag(&name, ERROR)? else {
                continue;
            };

            // Be paranoid about not allowing any non-ASCII strings into
            // pg_collation.
            if !langtag.is_ascii() {
                continue;
            }

            let version = pg_locale::get_collation_actual_version(COLLPROVIDER_ICU, &langtag)?;
            let collid = pg_collation::CollationCreate(
                mcx,
                &format!("{langtag}-x-icu"),
                nspid,
                miscinit::GetUserId(),
                &CollationForm {
                    collprovider: COLLPROVIDER_ICU,
                    collisdeterministic: true,
                    collencoding: -1,
                    collcollate: None,
                    collctype: None,
                    colllocale: Some(&langtag),
                    collicurules: None,
                    collversion: version.as_deref(),
                },
                true,
                true,
            )?;
            if OidIsValid(collid) {
                ncreated += 1;
                xact::CommandCounterIncrement()?;

                if let Some(icucomment) = pg_locale::icu_locale_display_name_ascii(&name) {
                    commands_comment::CreateComments(
                        mcx,
                        collid,
                        COLLATION_RELATION_ID,
                        0,
                        Some(&icucomment),
                    )?;
                }
            }
        }
    }

    Ok(ncreated)
}

#[cfg(test)]
mod tests {
    use super::normalize_libc_locale_name;

    #[test]
    fn normalize_libc_locale_name_matches_c() {
        // collationcmds.c:619-624 examples.
        assert_eq!(normalize_libc_locale_name("en_US.utf8").as_deref(), Some("en_US"));
        assert_eq!(normalize_libc_locale_name("en_US.UTF-8").as_deref(), Some("en_US"));
        assert_eq!(
            normalize_libc_locale_name("br_FR.iso885915@euro").as_deref(),
            Some("br_FR@euro")
        );
        // No encoding tag: no new name.
        assert_eq!(normalize_libc_locale_name("en_US"), None);
        assert_eq!(normalize_libc_locale_name("C"), None);
        // A trailing dot still counts as a change (C sets changed on '.').
        assert_eq!(normalize_libc_locale_name("en_US.").as_deref(), Some("en_US"));
    }
}
