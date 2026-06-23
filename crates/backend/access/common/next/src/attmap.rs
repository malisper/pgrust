//! `backend/access/common/attmap.c` — attribute mapping support.
//!
//! Build and manage attribute mappings by comparing input and output
//! `TupleDesc`s. Used by DDL on inheritance/partition trees and by the tuple
//! conversion routines in `tupconvert.rs`.
//!
//! The C `AttrMap` is a `palloc`'d struct with a trailing `AttrNumber[]` and an
//! `int maplen`. The repo's [`AttrMap`] carries just the `attnums` vector;
//! `maplen` is `attnums.len()`. A C `NULL` return ("no runtime conversion
//! needed") is `Ok(None)`.

use mcx::{vec_with_capacity_in, alloc_in, Mcx, PgBox};
use types_core::primitive::AttrNumber;
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH};
use types_tuple::attmap::AttrMap;
use types_tuple::heaptuple::{NameData, TupleDescData};

use adt_format_type::{format_type_be, format_type_with_typemod};

/// `make_attrmap(maplen)` (attmap.c) — allocate an attribute map in `mcx`
/// (C: `palloc0` of the struct + a `maplen`-element `AttrNumber[]`). The
/// `attnums` vector is reserved with the fallible allocator and zero-filled to
/// `maplen` entries.
pub fn make_attrmap<'mcx>(mcx: Mcx<'mcx>, maplen: i32) -> PgResult<PgBox<'mcx, AttrMap<'mcx>>> {
    // A negative maplen is nonsensical (the C `sizeof * maplen` would compute a
    // huge size); guard so the `as usize` cast cannot wrap.
    if maplen < 0 {
        return Err(PgError::error("make_attrmap: negative map length"));
    }
    let n = maplen as usize;
    let mut attnums = vec_with_capacity_in::<AttrNumber>(mcx, n)?;
    attnums.resize(n, 0);
    alloc_in(mcx, AttrMap { attnums })
}

/// `free_attrmap(map)` (attmap.c) — release an attribute map. In the owned
/// model the map and its `attnums` are freed by dropping the value; taking it
/// by value reproduces the C `pfree(map->attnums); pfree(map);`.
pub fn free_attrmap(map: PgBox<'_, AttrMap<'_>>) {
    drop(map);
}

/// `build_attrmap_by_position(indesc, outdesc, msg)` (attmap.c) — a bare
/// attribute map for tuple conversion, matching input and output columns by
/// position. Dropped columns are ignored in both, marked as 0. Returns
/// `Ok(None)` (C `NULL`) when the map is a one-to-one match and no runtime
/// conversion is needed.
///
/// `msg` (the errdetail "Returned/expected rowtype" messages) speak of
/// `indesc` as the "returned" rowtype and `outdesc` as the "expected" rowtype.
pub fn build_attrmap_by_position<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    msg: &str,
) -> PgResult<Option<PgBox<'mcx, AttrMap<'mcx>>>> {
    // The length is the number of attributes of the expected rowtype (it
    // includes dropped attributes in its count).
    let n = outdesc.natts;
    let mut attr_map = make_attrmap(mcx, n)?;

    let mut j: i32 = 0; // j is next physical input attribute
    let mut nincols: i32 = 0; // these count non-dropped attributes
    let mut noutcols: i32 = 0;
    let mut same = true;
    for i in 0..n {
        let outatt = outdesc.attr(i as usize);

        if outatt.attisdropped {
            continue; // attr_map.attnums[i] is already 0
        }
        noutcols += 1;
        while j < indesc.natts {
            let inatt = indesc.attr(j as usize);

            if inatt.attisdropped {
                j += 1;
                continue;
            }
            nincols += 1;

            // Found matching column, now check type.
            if outatt.atttypid != inatt.atttypid
                || (outatt.atttypmod != inatt.atttypmod && outatt.atttypmod >= 0)
            {
                return Err(datatype_mismatch_internal(
                    msg,
                    format!(
                        "Returned type {} does not match expected type {} in column \"{}\" (position {}).",
                        format_type_with_typemod(mcx, inatt.atttypid, inatt.atttypmod)?.as_str(),
                        format_type_with_typemod(mcx, outatt.atttypid, outatt.atttypmod)?.as_str(),
                        name_str(&outatt.attname),
                        noutcols
                    ),
                ));
            }
            attr_map.attnums[i as usize] = (j + 1) as AttrNumber;
            j += 1;
            break;
        }
        if attr_map.attnums[i as usize] == 0 {
            same = false; // we'll complain below
        }
    }

    // Check for unused input columns.
    while j < indesc.natts {
        if indesc.compact_attr(j as usize).attisdropped {
            j += 1;
            continue;
        }
        nincols += 1;
        same = false; // we'll complain below
        j += 1;
    }

    // Report column count mismatch using the non-dropped-column counts.
    if !same {
        return Err(datatype_mismatch_internal(
            msg,
            format!(
                "Number of returned columns ({nincols}) does not match expected column count ({noutcols})."
            ),
        ));
    }

    // Check if the map has a one-to-one match.
    if check_attrmap_match(indesc, outdesc, &attr_map) {
        // Runtime conversion is not needed.
        free_attrmap(attr_map);
        return Ok(None);
    }

    Ok(Some(attr_map))
}

/// `build_attrmap_by_name(indesc, outdesc, missing_ok)` (attmap.c) — a bare
/// attribute map for tuple conversion, matching input and output columns by
/// name. Dropped columns are ignored in both. If `missing_ok`, an `outdesc`
/// column not present in `indesc` is not an error (its `attnums[]` entry stays
/// 0).
pub fn build_attrmap_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    missing_ok: bool,
) -> PgResult<PgBox<'mcx, AttrMap<'mcx>>> {
    let outnatts = outdesc.natts;
    let innatts = indesc.natts;

    let mut attr_map = make_attrmap(mcx, outnatts)?;
    let mut nextindesc: i32 = -1;
    for i in 0..outnatts {
        let outatt = outdesc.attr(i as usize);

        if outatt.attisdropped {
            continue; // attr_map.attnums[i] is already 0
        }
        let attname = outatt.attname;
        let atttypid = outatt.atttypid;
        let atttypmod = outatt.atttypmod;

        // Search for an attribute with the same name in indesc. A partitioned
        // table likely has the attributes in the same order as the partition,
        // so the search is optimized for that case; `nextindesc` tracks the
        // starting point so dropped columns in one but not the other relation
        // are skipped while leaving the cursor in the right place.
        for _j in 0..innatts {
            nextindesc += 1;
            if nextindesc >= innatts {
                nextindesc = 0;
            }

            let inatt = indesc.attr(nextindesc as usize);
            if inatt.attisdropped {
                continue;
            }
            if name_str(&attname) == name_str(&inatt.attname) {
                // Found it, check type.
                if atttypid != inatt.atttypid || atttypmod != inatt.atttypmod {
                    return Err(datatype_mismatch(
                        "could not convert row type",
                        format!(
                            "Attribute \"{}\" of type {} does not match corresponding attribute of type {}.",
                            name_str(&attname),
                            format_type_be(mcx, outdesc.tdtypeid)?.as_str(),
                            format_type_be(mcx, indesc.tdtypeid)?.as_str()
                        ),
                    ));
                }
                attr_map.attnums[i as usize] = inatt.attnum;
                break;
            }
        }
        if attr_map.attnums[i as usize] == 0 && !missing_ok {
            return Err(datatype_mismatch(
                "could not convert row type",
                format!(
                    "Attribute \"{}\" of type {} does not exist in type {}.",
                    name_str(&attname),
                    format_type_be(mcx, outdesc.tdtypeid)?.as_str(),
                    format_type_be(mcx, indesc.tdtypeid)?.as_str()
                ),
            ));
        }
    }
    Ok(attr_map)
}

/// `build_attrmap_by_name_if_req(indesc, outdesc, missing_ok)` (attmap.c) —
/// the mapping created by [`build_attrmap_by_name`], or `Ok(None)` (C `NULL`)
/// if no conversion is required.
pub fn build_attrmap_by_name_if_req<'mcx>(
    mcx: Mcx<'mcx>,
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    missing_ok: bool,
) -> PgResult<Option<PgBox<'mcx, AttrMap<'mcx>>>> {
    // Verify compatibility and prepare the attribute-number map.
    let attr_map = build_attrmap_by_name(mcx, indesc, outdesc, missing_ok)?;

    // Check if the map has a one-to-one match.
    if check_attrmap_match(indesc, outdesc, &attr_map) {
        // Runtime conversion is not needed.
        free_attrmap(attr_map);
        Ok(None)
    } else {
        Ok(Some(attr_map))
    }
}

/// `check_attrmap_match(indesc, outdesc, attrMap)` (attmap.c) — is the map a
/// one-to-one match, in which case no tuple conversion is needed?
fn check_attrmap_match(
    indesc: &TupleDescData<'_>,
    outdesc: &TupleDescData<'_>,
    attr_map: &AttrMap<'_>,
) -> bool {
    // No match if attribute numbers are not the same.
    if indesc.natts != outdesc.natts {
        return false;
    }

    let maplen = attr_map.attnums.len();
    for i in 0..maplen {
        let inatt = indesc.compact_attr(i);

        // If the input column has a missing attribute, we need a conversion.
        if inatt.atthasmissing {
            return false;
        }

        if attr_map.attnums[i] == (i + 1) as AttrNumber {
            continue;
        }

        let outatt = outdesc.compact_attr(i);

        // If it's a dropped column and the corresponding input column is also
        // dropped, no conversion is needed -- but attlen and attalignby must
        // agree.
        if attr_map.attnums[i] == 0
            && inatt.attisdropped
            && inatt.attlen == outatt.attlen
            && inatt.attalignby == outatt.attalignby
        {
            continue;
        }

        return false;
    }

    true
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// `NameStr(name)` decoded to a Rust `&str` for messages and name comparison.
/// `NameData` is fixed-size NUL-padded; `name_str()` returns the bytes up to
/// the first NUL, UTF-8-lossy decoded for display.
fn name_str(name: &NameData) -> alloc::borrow::Cow<'_, str> {
    String::from_utf8_lossy(name.name_str())
}

/// `ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH), errmsg_internal("%s",
/// _(msg)), errdetail(...))` — the `build_attrmap_by_position` sites, where
/// `msg` is caller-supplied (already translatable) and goes through
/// `errmsg_internal`.
fn datatype_mismatch_internal(msg: &str, detail: String) -> PgError {
    PgError::error(msg.to_string())
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
        .with_detail(detail)
}

/// `ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("could not
/// convert row type"), errdetail(...))` — the `build_attrmap_by_name` sites,
/// where the message is a fixed translatable literal.
fn datatype_mismatch(msg: &'static str, detail: String) -> PgError {
    PgError::error(msg)
        .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
        .with_detail(detail)
}

extern crate alloc;
