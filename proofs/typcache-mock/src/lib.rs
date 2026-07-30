//! In-process typcache/catalog mock for BUILT-IN types — test/fuzz/proof only.
//!
//! Serves the catalog seams `typcache::lookup_type_cache` (and the lsyscache
//! getters under it) read, from tables GENERATED out of the vendored
//! PostgreSQL catalog .dat files (`generate.pl` -> `src/generated.rs`). For
//! built-in types these are THE REAL catalog rows — pg_type/pg_opclass/
//! pg_amop/pg_amproc/pg_operator/pg_range/pg_cast contents are static initdb
//! data; fidelity_check.py diffs every generated row against a live
//! `malisper/pgrust:v0.2` server (byte-identical, 0 mismatches, 2026-07-30).
//!
//! HONEST SCOPE (what this mock can and cannot do):
//! - Built-in base types, their arrays, ranges and multiranges: full
//!   TYPECACHE_* flag coverage except TUPDESC (composite lane is unported in
//!   typcache itself and loud).
//! - User-defined types (enums, domains, named composites, user ranges):
//!   NOT served — those rows are per-database state, not static catalog
//!   data. Lookups of unknown oids fail exactly like a missing pg_type row
//!   ("type with OID n does not exist"); the enum/domain-constraint scan
//!   seams stay uninstalled, so reaching them panics loudly rather than
//!   returning fabricated data.
//! - `syscache_hash_value_typeoid` is a deterministic stand-in (golden-ratio
//!   multiply, same as typcache's own unit tests). It only routes inval
//!   callbacks, which no in-process test fires; it never affects entry
//!   contents.
//!
//! `get_default_opclass` is a transcription of C GetDefaultOpClass
//! (indexcmds.c) + IsBinaryCoercible[WithCast] (parse_coerce.c) over the
//! generated tables — including the binary-coercion legs (varchar->text,
//! array->anyarray, range->anyrange, ...) that make e.g. `_int4`'s default
//! btree opclass `array_ops`.
//!
//! NEVER SHIPPED: this crate lives outside the workspace, nothing in the
//! server depends on it, and `install()` would panic (seam double-install)
//! inside any binary that boots the real catalog stack.

mod generated;

pub use generated::{
    GenPgOperator, GenPgType, PG_AM, PG_AMOP, PG_AMPROC, PG_CAST, PG_OPCLASS, PG_OPERATOR,
    PG_RANGE, PG_TYPE,
};

use std::sync::Once;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_tuple::NameData;

pub const BTREE_AM_OID: Oid = 403;
pub const HASH_AM_OID: Oid = 405;
const PG_CATALOG_NAMESPACE: Oid = 11;

fn typ(oid: Oid) -> Option<&'static GenPgType> {
    generated::PG_TYPE.binary_search_by_key(&oid, |t| t.oid).ok().map(|i| &generated::PG_TYPE[i])
}

fn typ_by_name(name: &str) -> &'static GenPgType {
    generated::PG_TYPE
        .iter()
        .find(|t| t.typname == name)
        .unwrap_or_else(|| panic!("generated PG_TYPE has no {name}"))
}

fn name_data(s: &str) -> NameData {
    let mut n = NameData::default();
    n.namestrcpy(s);
    n
}

// ---- C transcriptions (over the generated tables) --------------------------

/// getBaseType (lsyscache.c): domains reduce to base. No built-in domains
/// exist, but keep the loop so the transcription stays C-shaped.
fn get_base_type(mut typid: Oid) -> Oid {
    while let Some(t) = typ(typid) {
        if t.typtype != b'd' {
            break;
        }
        typid = 0; // no built-in domains: typbasetype is not carried; unreachable
        break;
    }
    typid
}

fn type_is_array(typid: Oid) -> bool {
    // C get_element_type: typelem when typlen == -1 ("true" varlena array).
    typ(typid).is_some_and(|t| t.typlen == -1 && t.typelem != 0)
}

fn is_complex(typid: Oid) -> bool {
    typ(typid).is_some_and(|t| t.typrelid != 0)
}

fn is_complex_array(typid: Oid) -> bool {
    typ(typid).is_some_and(|t| t.typlen == -1 && t.typelem != 0 && is_complex(t.typelem))
}

fn typtype_is(typid: Oid, tt: u8) -> bool {
    typ(typid).is_some_and(|t| t.typtype == tt)
}

/// IsBinaryCoercibleWithCast (parse_coerce.c), returns the bool only.
fn is_binary_coercible(srctype: Oid, targettype: Oid) -> bool {
    let anyoid = typ_by_name("any").oid;
    let anyelement = typ_by_name("anyelement").oid;
    let anycompatible = typ_by_name("anycompatible").oid;
    let anyarray = typ_by_name("anyarray").oid;
    let anycompatiblearray = typ_by_name("anycompatiblearray").oid;
    let anynonarray = typ_by_name("anynonarray").oid;
    let anycompatiblenonarray = typ_by_name("anycompatiblenonarray").oid;
    let anyenum = typ_by_name("anyenum").oid;
    let anyrange = typ_by_name("anyrange").oid;
    let anycompatiblerange = typ_by_name("anycompatiblerange").oid;
    let anymultirange = typ_by_name("anymultirange").oid;
    let anycompatiblemultirange = typ_by_name("anycompatiblemultirange").oid;
    let record = typ_by_name("record").oid;
    let recordarray = typ_by_name("_record").oid;

    if srctype == targettype {
        return true;
    }
    if targettype == anyoid || targettype == anyelement || targettype == anycompatible {
        return true;
    }
    let srctype = if srctype != InvalidOid { get_base_type(srctype) } else { srctype };
    if srctype == targettype {
        return true;
    }
    if (targettype == anyarray || targettype == anycompatiblearray) && type_is_array(srctype) {
        return true;
    }
    if (targettype == anynonarray || targettype == anycompatiblenonarray)
        && !type_is_array(srctype)
    {
        return true;
    }
    if targettype == anyenum && typtype_is(srctype, b'e') {
        return true;
    }
    if (targettype == anyrange || targettype == anycompatiblerange) && typtype_is(srctype, b'r') {
        return true;
    }
    if (targettype == anymultirange || targettype == anycompatiblemultirange)
        && typtype_is(srctype, b'm')
    {
        return true;
    }
    if targettype == record && is_complex(srctype) {
        return true;
    }
    if targettype == recordarray && is_complex_array(srctype) {
        return true;
    }
    // Else look in pg_cast: binary method + implicit context.
    generated::PG_CAST
        .binary_search_by_key(&(srctype, targettype), |c| (c.0, c.1))
        .ok()
        .is_some_and(|i| {
            let c = &generated::PG_CAST[i];
            c.4 == b'b' && c.3 == b'i'
        })
}

/// IsPreferredType (parse_coerce.c).
fn is_preferred_type(category: u8, typid: Oid) -> bool {
    typ(typid).is_some_and(|t| (category == t.typcategory || category == 0) && t.typispreferred)
}

/// GetDefaultOpClass (indexcmds.c) transcription over PG_OPCLASS.
pub fn get_default_opclass(type_id: Oid, am_id: Oid) -> PgResult<Oid> {
    let type_id = get_base_type(type_id);
    let tcategory = typ(type_id).map(|t| t.typcategory).unwrap_or(0);

    let mut result = InvalidOid;
    let mut nexact = 0;
    let mut ncompatible = 0;
    let mut ncompatiblepreferred = 0;

    for &(oid, opcmethod, _fam, opcintype, _key, opcdefault) in generated::PG_OPCLASS {
        if opcmethod != am_id || !opcdefault {
            continue;
        }
        if opcintype == type_id {
            nexact += 1;
            result = oid;
        } else if nexact == 0 && is_binary_coercible(type_id, opcintype) {
            if is_preferred_type(tcategory, opcintype) {
                ncompatiblepreferred += 1;
                result = oid;
            } else if ncompatiblepreferred == 0 {
                ncompatible += 1;
                result = oid;
            }
        }
    }
    if nexact > 1 {
        return Err(Box::new(PgError::error(format!(
            "there are multiple default operator classes for data type {type_id}"
        ))));
    }
    if nexact == 1 || ncompatiblepreferred == 1 || (ncompatiblepreferred == 0 && ncompatible == 1)
    {
        return Ok(result);
    }
    Ok(InvalidOid)
}

// ---- seam servers ----------------------------------------------------------

fn typcache_shape(typid: Oid) -> PgResult<Option<syscache_seams::PgTypeTypcacheShape>> {
    Ok(typ(typid).map(|t| syscache_seams::PgTypeTypcacheShape {
        typname: name_data(t.typname),
        typlen: t.typlen,
        typbyval: t.typbyval,
        typalign: t.typalign as i8,
        typstorage: t.typstorage as i8,
        typtype: t.typtype as i8,
        typisdefined: t.typisdefined,
        typrelid: t.typrelid,
        typsubscript: t.typsubscript,
        typelem: t.typelem,
        typarray: t.typarray,
        typcollation: t.typcollation,
    }))
}

fn amop_by_strategy(opfamily: Oid, lefttype: Oid, righttype: Oid, strategy: i16) -> PgResult<Oid> {
    Ok(generated::PG_AMOP
        .binary_search_by_key(&(opfamily, lefttype, righttype, strategy), |a| {
            (a.0, a.1, a.2, a.3)
        })
        .ok()
        .map(|i| generated::PG_AMOP[i].5)
        .unwrap_or(InvalidOid))
}

fn amproc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: i16) -> PgResult<Oid> {
    Ok(generated::PG_AMPROC
        .binary_search_by_key(&(opfamily, lefttype, righttype, procnum), |a| {
            (a.0, a.1, a.2, a.3)
        })
        .ok()
        .map(|i| generated::PG_AMPROC[i].4)
        .unwrap_or(InvalidOid))
}

fn operator_shape(opno: Oid) -> PgResult<Option<syscache_seams::PgOperatorShape>> {
    Ok(generated::PG_OPERATOR.binary_search_by_key(&opno, |o| o.oid).ok().map(|i| {
        let o = &generated::PG_OPERATOR[i];
        syscache_seams::PgOperatorShape {
            oprnamespace: PG_CATALOG_NAMESPACE,
            oprleft: o.oprleft,
            oprright: o.oprright,
            oprresult: o.oprresult,
            oprcom: o.oprcom,
            oprnegate: o.oprnegate,
            oprcode: o.oprcode,
            oprrest: o.oprrest,
            oprjoin: o.oprjoin,
            oprcanmerge: o.oprcanmerge,
            oprcanhash: o.oprcanhash,
        }
    }))
}

fn range_shape(rngtypid: Oid) -> PgResult<Option<syscache_seams::PgRangeShape>> {
    Ok(generated::PG_RANGE.binary_search_by_key(&rngtypid, |r| r.0).ok().map(|i| {
        let r = &generated::PG_RANGE[i];
        syscache_seams::PgRangeShape {
            rngsubtype: r.1,
            rngmultitypid: r.2,
            rngcollation: r.3,
            rngsubopc: r.4,
            rngcanonical: r.5,
            rngsubdiff: r.6,
        }
    }))
}

static INSTALL: Once = Once::new();

/// Install the mock behind every catalog seam typcache (and the lsyscache
/// getters it uses) reads, plus the real fmgr + typcache seam consumers.
/// Idempotent; panics if a competing installer already claimed a seam.
pub fn install() {
    INSTALL.call_once(|| {
        use syscache_seams as s;
        fmgr_core::init_seams();
        typcache::init_seams();
        detoast::init_seams();
        // In-process tests are always in normal (non-bootstrap) processing.
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);

        s::lookup_pg_type_typcache_shape::set(typcache_shape);
        s::lookup_pg_type_shape::set(|typid| {
            Ok(typ(typid).map(|t| types_tuple::PgTypeShape {
                typlen: t.typlen,
                typbyval: t.typbyval,
                typalign: t.typalign as i8,
                typstorage: t.typstorage as i8,
                typcollation: t.typcollation,
            }))
        });
        s::pg_type_isdefined::set(|typid| Ok(typ(typid).map(|t| t.typisdefined)));
        s::pg_type_typtype::set(|typid| Ok(typ(typid).map(|t| t.typtype as i8)));
        s::pg_type_category::set(|typid| {
            Ok(typ(typid).map(|t| (t.typcategory as i8, t.typispreferred)))
        });
        s::pg_type_typrelid::set(|typid| Ok(typ(typid).map(|t| t.typrelid)));
        s::pg_type_element_shape::set(|typid| {
            Ok(typ(typid).map(|t| syscache_seams::PgTypeElementShape {
                typelem: t.typelem,
                typsubscript: t.typsubscript,
            }))
        });
        s::pg_type_typarray::set(|typid| Ok(typ(typid).map(|t| t.typarray)));
        s::pg_type_base_shape::set(|typid| {
            Ok(typ(typid).map(|t| syscache_seams::PgTypeBaseShape {
                typtype: t.typtype as i8,
                typbasetype: InvalidOid, // no built-in domains
                typtypmod: -1,
                typelem: t.typelem,
                typsubscript: t.typsubscript,
            }))
        });
        s::pg_type_domain_shape::set(|typid| {
            Ok(typ(typid).map(|t| syscache_seams::PgTypeDomainShape {
                typname: name_data(t.typname),
                typnamespace: PG_CATALOG_NAMESPACE,
                typtype: t.typtype as i8,
                typnotnull: false,
                typbasetype: InvalidOid,
            }))
        });
        s::pg_type_io_shape::set(|typid| {
            Ok(typ(typid).map(|t| syscache_seams::PgTypeIoShape {
                oid: t.oid,
                typinput: t.typinput,
                typoutput: t.typoutput,
                typreceive: t.typreceive,
                typsend: t.typsend,
                typmodin: t.typmodin,
                typmodout: t.typmodout,
                typelem: t.typelem,
                typlen: t.typlen,
                typbyval: t.typbyval,
                typalign: t.typalign as i8,
                typdelim: t.typdelim as i8,
                typisdefined: t.typisdefined,
            }))
        });
        // Deterministic stand-in; routes inval only (see module docs).
        s::syscache_hash_value_typeoid::set(|typid| Ok(typid.wrapping_mul(0x9e37_79b1)));
        s::lookup_pg_opclass_shape::set(|opclass| {
            Ok(generated::PG_OPCLASS.binary_search_by_key(&opclass, |o| o.0).ok().map(|i| {
                let o = &generated::PG_OPCLASS[i];
                syscache_seams::PgOpclassShape {
                    opcmethod: o.1,
                    opcfamily: o.2,
                    opcintype: o.3,
                    opckeytype: o.4,
                }
            }))
        });
        s::lookup_pg_amop_by_strategy::set(amop_by_strategy);
        s::lookup_pg_amproc::set(amproc);
        s::lookup_pg_operator_shape::set(operator_shape);
        s::lookup_pg_range_shape::set(range_shape);
        s::lookup_pg_range_by_multirange::set(|mr| {
            Ok(generated::PG_RANGE.iter().find(|r| r.2 == mr).map(|r| r.0))
        });
        indexcmds_seams::get_default_opclass::set(get_default_opclass);
    });
}
