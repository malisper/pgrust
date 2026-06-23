//! `backend-access-common-tupdesc` — the tuple-descriptor support code of
//! `src/backend/access/common/tupdesc.c` over the owned, `mcx`-allocated
//! [`::types_tuple::heaptuple::TupleDescData`] vocabulary.
//!
//! # Owned-value / `mcx` model vs. the C ABI
//!
//! PG18's `TupleDescData` is a single `palloc`'d chunk with two trailing
//! flexible arrays — `compact_attrs[natts]` and `FormData_pg_attribute[natts]`
//! — reached by the `TupleDescAttr` / `TupleDescCompactAttr` macros. The
//! owned [`TupleDescData`] carries `compact_attrs: PgVec<CompactAttribute>` and
//! `attrs: PgVec<FormData_pg_attribute>` kept in lock-step, accessed via
//! `attr(i)` / `attr_mut(i)` / `compact_attr(i)`. Descriptors are allocated in
//! the caller's [`Mcx`]; there is no `palloc`, pointer arithmetic, or
//! `ATTRIBUTE_FIXED_PART_SIZE` flat-copy. Constraint payloads (`TupleConstr`,
//! `AttrDefault`, `AttrMissing`, `ConstrCheck`) are owned values; the
//! constraint deep-copy (`CreateTupleDescCopyConstr`) and missing-value
//! `datumCopy` / `datumIsEqual` reduce to `Datum::clone_in` / `PartialEq`.
//!
//! # Refcount / resource-owner lifecycle (`Incr`/`DecrTupleDescRefCount`,
//! `FreeTupleDesc`, the `ResOwner*` callbacks)
//!
//! Ref-counted descriptors live in the relcache/typcache, are pinned through
//! `CurrentResourceOwner`, and are freed by `pfree` when the count drops to
//! zero. In the owned model a descriptor's storage is reclaimed when its `mcx`
//! resets; there is no `palloc`/`pfree` and no global `CurrentResourceOwner`
//! here. [`FreeTupleDesc`] therefore validates the refcount invariant
//! (`Assert(tdrefcount <= 0)`) and drops the owned value; the resource-owner
//! pin/unpin machinery (`ResourceOwnerRemember/Forget`, the callbacks) belongs
//! to the not-yet-ported `utils/resowner.c` owner and is intentionally absent
//! rather than faked behind an invented owner handle.
//!
//! # Debug-only C `Assert`s
//!
//! Several C `Assert()`s guard cases that cannot occur for well-formed callers
//! (attribute numbers in `[1, natts]`, `attdim` in `[0, PG_INT16_MAX]`,
//! `natts >= 0`). In a release C build these compile away and bad input is
//! undefined behaviour; here they are fail-fast [`PgResult`] errors
//! (`ERRCODE_INTERNAL_ERROR`) so the safe API never reaches undefined
//! behaviour. The happy path is identical.

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;

use ::mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_core::primitive::{AttrNumber, InvalidOid};
use ::types_core::{Oid, FLOAT8PASSBYVAL};
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::nodes::nodes::Node;
use types_tuple::tupdesc::PgTypeInfo;
use ::types_tuple::heaptuple::{
    AttrDefault, AttrMissing, CompactAttribute, ConstrCheck, FormData_pg_attribute, NameData,
    TupleConstr, TupleDescData, ALIGNOF_DOUBLE, ALIGNOF_INT, ALIGNOF_SHORT, ATTNULLABLE_UNKNOWN,
    ATTNULLABLE_UNRESTRICTED, ATTNULLABLE_VALID, BOOLOID, DEFAULT_COLLATION_OID, INT4OID, INT8OID,
    InvalidCompressionMethod, OIDOID, PG_INT16_MAX, RECORDOID, TEXTARRAYOID, TEXTOID, TYPALIGN_CHAR,
    TYPALIGN_DOUBLE, TYPALIGN_INT, TYPALIGN_SHORT, TYPSTORAGE_EXTENDED, TYPSTORAGE_PLAIN,
};

const NAMEDATALEN: usize = ::types_core::NAMEDATALEN as usize;

/// `CreateTemplateTupleDesc(natts)` (`access/common/tupdesc.c`).
///
/// Allocate an empty tuple descriptor for `natts` columns in `mcx`. The tuple
/// type ID information is initially set for an anonymous record type
/// (`RECORDOID`, typmod `-1`); the caller can overwrite this if needed. The
/// descriptor is not reference-counted (`tdrefcount == -1`). Both the
/// `compact_attrs` and the full `attrs` arrays are grown to `natts`
/// zeroed/default entries up front (C: the equivalent trailing flexible
/// arrays).
pub fn CreateTemplateTupleDesc<'mcx>(mcx: Mcx<'mcx>, natts: i32) -> PgResult<TupleDescData<'mcx>> {
    // Assert(natts >= 0).
    if natts < 0 {
        return internal_error("CreateTemplateTupleDesc: negative attribute count");
    }
    let n = natts as usize;
    let mut compact_attrs: PgVec<'mcx, CompactAttribute> = vec_with_capacity_in(mcx, n)?;
    let mut attrs: PgVec<'mcx, FormData_pg_attribute> = vec_with_capacity_in(mcx, n)?;
    for _ in 0..n {
        compact_attrs.push(CompactAttribute::default());
        attrs.push(FormData_pg_attribute::default());
    }
    Ok(TupleDescData {
        natts,
        tdtypeid: RECORDOID,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs,
        attrs,
    })
}

/// `CreateTupleDesc(natts, attrs)` (`access/common/tupdesc.c`).
///
/// Allocate a new `TupleDesc` by copying a given `Form_pg_attribute` array.
/// Tuple type ID information is initially set for an anonymous record type;
/// the caller can overwrite. C `memcpy`s `ATTRIBUTE_FIXED_PART_SIZE` of each
/// attribute; the owned analogue is an element copy.
pub fn CreateTupleDesc<'mcx>(
    mcx: Mcx<'mcx>,
    attrs: &[FormData_pg_attribute],
) -> PgResult<TupleDescData<'mcx>> {
    let natts = attrs.len() as i32;
    let mut desc = CreateTemplateTupleDesc(mcx, natts)?;
    for i in 0..natts as usize {
        *desc.attr_mut(i) = attrs[i];
        populate_compact_attribute(&mut desc, i)?;
    }
    Ok(desc)
}

/// `populate_compact_attribute(tupdesc, attnum)` (`access/common/tupdesc.c`).
///
/// Fill in the `CompactAttribute` element for the given (0-based) attribute
/// number from its `Form_pg_attribute`. Must be called whenever a change is
/// made to a `Form_pg_attribute` in the descriptor.
pub fn populate_compact_attribute(tupdesc: &mut TupleDescData<'_>, attnum: usize) -> PgResult<()> {
    let dst = populate_compact_attribute_internal(&tupdesc.attrs[attnum])?;
    tupdesc.compact_attrs[attnum] = dst;
    Ok(())
}

/// `populate_compact_attribute_internal(src, dst)` (`access/common/tupdesc.c`).
///
/// Derive a `CompactAttribute` from a `Form_pg_attribute`. The struct is zeroed
/// first (here: built fresh), `attcacheoff` is `-1`, the nullability is decided
/// from `attnotnull` + `IsCatalogRelationOid`, and the alignment character is
/// mapped to a byte count.
fn populate_compact_attribute_internal(
    src: &FormData_pg_attribute,
) -> PgResult<CompactAttribute> {
    // Assign nullability status for this column. Assuming a not-null constraint
    // exists, at this point we don't know if it is valid, so we assign UNKNOWN
    // unless the table is a catalog, in which case we know it's valid.
    let attnullability = if !src.attnotnull {
        ATTNULLABLE_UNRESTRICTED
    } else if catalog_seams::is_catalog_relation_oid::call(src.attrelid) {
        ATTNULLABLE_VALID
    } else {
        ATTNULLABLE_UNKNOWN
    };

    let attalignby = match src.attalign {
        TYPALIGN_INT => ALIGNOF_INT,
        TYPALIGN_CHAR => 1, // sizeof(char)
        TYPALIGN_DOUBLE => ALIGNOF_DOUBLE,
        TYPALIGN_SHORT => ALIGNOF_SHORT,
        other => {
            // C: elog(ERROR, "invalid attalign value: %c", src->attalign).
            return internal_error(format!("invalid attalign value: {}", other as u8 as char));
        }
    };

    Ok(CompactAttribute {
        attcacheoff: -1,
        attlen: src.attlen,
        attbyval: src.attbyval,
        attispackable: src.attstorage != TYPSTORAGE_PLAIN,
        atthasmissing: src.atthasmissing,
        attisdropped: src.attisdropped,
        attgenerated: src.attgenerated != 0,
        attnullability,
        attalignby,
    })
}

/// `CreateTupleDescCopy(tupdesc)` (`access/common/tupdesc.c`).
///
/// Create a new `TupleDesc` in `mcx` by copying from an existing one — a flat
/// copy: **constraints and defaults are not copied**, and the per-attribute
/// fields associated with them (`attnotnull` / `atthasdef` / `atthasmissing` /
/// `attidentity` / `attgenerated`) are cleared on the copy. The tuple type
/// identification (`tdtypeid` / `tdtypmod`) *is* copied.
pub fn CreateTupleDescCopy<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<TupleDescData<'mcx>> {
    let mut desc = CreateTemplateTupleDesc(mcx, tupdesc.natts)?;

    // Flat-copy the attribute array.
    for i in 0..desc.natts as usize {
        *desc.attr_mut(i) = tupdesc.attrs[i];
    }

    // Since we're not copying constraints and defaults, clear fields associated
    // with them.
    for i in 0..desc.natts as usize {
        clear_constraint_fields(desc.attr_mut(i));
        populate_compact_attribute(&mut desc, i)?;
    }

    desc.tdtypeid = tupdesc.tdtypeid;
    desc.tdtypmod = tupdesc.tdtypmod;
    Ok(desc)
}

/// `CreateTupleDescTruncatedCopy(tupdesc, natts)` (`access/common/tupdesc.c`).
///
/// Like [`CreateTupleDescCopy`], but the copy keeps only the first `natts`
/// attributes (`index_truncate_tuple`'s key-truncation helper). Constraints and
/// defaults are not copied, and the per-attribute fields associated with them
/// are cleared, exactly as in [`CreateTupleDescCopy`].
pub fn CreateTupleDescTruncatedCopy<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
    natts: i32,
) -> PgResult<TupleDescData<'mcx>> {
    debug_assert!(natts <= tupdesc.natts);
    let mut desc = CreateTemplateTupleDesc(mcx, natts)?;

    // Flat-copy the (truncated) attribute array.
    for i in 0..desc.natts as usize {
        *desc.attr_mut(i) = tupdesc.attrs[i];
    }

    for i in 0..desc.natts as usize {
        clear_constraint_fields(desc.attr_mut(i));
        populate_compact_attribute(&mut desc, i)?;
    }

    desc.tdtypeid = tupdesc.tdtypeid;
    desc.tdtypmod = tupdesc.tdtypmod;
    Ok(desc)
}

/// `CreateTupleDescCopyConstr(tupdesc)` (`access/common/tupdesc.c`).
///
/// Create a new `TupleDesc` in `mcx` by copying from an existing one **including
/// its constraints and defaults**. Each attribute's `CompactAttribute` is
/// re-derived and its `attnullability` is preserved from the source (because
/// `populate_compact_attribute` recomputes it from `IsCatalogRelationOid`,
/// which can differ from the source's recorded validity). The constraint
/// payload — default expressions, missing values (deep-copied via
/// `datumCopy`/`Datum::clone_in`), and CHECK clauses — is deep-copied.
pub fn CreateTupleDescCopyConstr<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<TupleDescData<'mcx>> {
    let mut desc = CreateTemplateTupleDesc(mcx, tupdesc.natts)?;

    // Flat-copy the attribute array.
    for i in 0..desc.natts as usize {
        *desc.attr_mut(i) = tupdesc.attrs[i];
    }

    for i in 0..desc.natts as usize {
        populate_compact_attribute(&mut desc, i)?;
        desc.compact_attrs[i].attnullability = tupdesc.compact_attrs[i].attnullability;
    }

    // Copy the TupleConstr data structure, if any.
    if let Some(constr) = tupdesc.constr.as_deref() {
        let mut defval: PgVec<'mcx, AttrDefault<'mcx>> =
            vec_with_capacity_in(mcx, constr.defval.len())?;
        for d in constr.defval.iter() {
            defval.push(d.clone_in(mcx)?);
        }

        let mut missing: PgVec<'mcx, AttrMissing<'mcx>> =
            vec_with_capacity_in(mcx, constr.missing.len())?;
        for m in constr.missing.iter() {
            // C: copies the whole AttrMissing array, then datumCopy's each
            // present by-reference value. The owned AttrMissing::clone_in does
            // both (Datum::clone_in == datumCopy); a not-present entry
            // carries no live payload to copy.
            missing.push(m.clone_in(mcx)?);
        }

        let mut check: PgVec<'mcx, ConstrCheck<'mcx>> =
            vec_with_capacity_in(mcx, constr.check.len())?;
        for c in constr.check.iter() {
            check.push(c.clone_in(mcx)?);
        }

        let cpy = TupleConstr {
            defval,
            check,
            missing,
            num_defval: constr.num_defval,
            num_check: constr.num_check,
            has_not_null: constr.has_not_null,
            has_generated_stored: constr.has_generated_stored,
            has_generated_virtual: constr.has_generated_virtual,
        };
        desc.constr = Some(alloc_in(mcx, cpy)?);
    }

    desc.tdtypeid = tupdesc.tdtypeid;
    desc.tdtypmod = tupdesc.tdtypmod;
    Ok(desc)
}

/// `TupleDescCopy(dst, src)` (`access/common/tupdesc.c`).
///
/// Copy a tuple descriptor into a caller-supplied `dst` (C: shared memory).
/// **Constraints and defaults are not copied**; the per-attribute fields
/// associated with them are cleared, `constr` is set to `None`, and the
/// destination is marked not-ref-counted (`tdrefcount = -1`). `dst` must have
/// `dst.natts == src.natts` (C requires `dst` sized for `TupleDescSize(src)`).
pub fn TupleDescCopy(dst: &mut TupleDescData<'_>, src: &TupleDescData<'_>) -> PgResult<()> {
    if dst.natts != src.natts {
        return internal_error("TupleDescCopy: destination natts mismatch");
    }
    // Flat-copy the header (type id) and attribute array.
    dst.tdtypeid = src.tdtypeid;
    dst.tdtypmod = src.tdtypmod;
    for i in 0..dst.natts as usize {
        *dst.attr_mut(i) = src.attrs[i];
    }
    for i in 0..dst.natts as usize {
        clear_constraint_fields(dst.attr_mut(i));
        populate_compact_attribute(dst, i)?;
    }
    dst.constr = None;
    // Assume the destination is not to be ref-counted. (Copying the source's
    // refcount would be wrong in any case.)
    dst.tdrefcount = -1;
    Ok(())
}

/// `TupleDescCopyEntry(dst, dstAttno, src, srcAttno)`
/// (`access/common/tupdesc.c`).
///
/// Copy a single attribute structure from one descriptor to another (1-based
/// attribute numbers). **Constraints and defaults are not copied**: the copied
/// attribute's `attnum` is set to `dstAttno` and its constraint/default/
/// identity/generated fields are cleared.
pub fn TupleDescCopyEntry(
    dst: &mut TupleDescData<'_>,
    dst_attno: AttrNumber,
    src: &TupleDescData<'_>,
    src_attno: AttrNumber,
) -> PgResult<()> {
    let src_index = attno_to_index(src_attno, src.natts)?;
    let dst_index = attno_to_index(dst_attno, dst.natts)?;

    *dst.attr_mut(dst_index) = src.attrs[src_index];

    {
        let dst_att = dst.attr_mut(dst_index);
        dst_att.attnum = dst_attno;
        // since we're not copying constraints or defaults, clear these
        clear_constraint_fields(dst_att);
    }

    populate_compact_attribute(dst, dst_index)
}

/// `FreeTupleDesc(tupdesc)` (`access/common/tupdesc.c`).
///
/// Free a `TupleDesc` including all substructure. In C this `pfree`s the
/// constraint payload (`adbin`/`ccname`/`ccbin` strings, present by-reference
/// missing values, then the arrays) and the descriptor itself. In the owned
/// model dropping the value reclaims every owned allocation, so this only
/// enforces the C invariant (`Assert(tdrefcount <= 0)` — explicit freeing of
/// un-refcounted descriptors) and consumes the value.
pub fn FreeTupleDesc(tupdesc: TupleDescData<'_>) -> PgResult<()> {
    if tupdesc.tdrefcount > 0 {
        return internal_error("FreeTupleDesc: descriptor is still referenced");
    }
    // Dropping `tupdesc` reclaims constr/defval/missing/check + attrs.
    drop(tupdesc);
    Ok(())
}

/// `IncrTupleDescRefCount(tupdesc)` (`access/common/tupdesc.c`).
///
/// Increment the reference count of a refcounted descriptor. C also logs the
/// reference in `CurrentResourceOwner` (`ResourceOwnerEnlarge` +
/// `ResourceOwnerRememberTupleDesc`); that machinery is owned by the
/// not-yet-ported `utils/resowner.c`, so the resource-owner side is absent
/// here — the count itself is maintained. `Assert(tdrefcount >= 0)`.
pub fn IncrTupleDescRefCount(tupdesc: &mut TupleDescData<'_>) -> PgResult<()> {
    if tupdesc.tdrefcount < 0 {
        return internal_error("IncrTupleDescRefCount: descriptor is not refcounted");
    }
    tupdesc.tdrefcount += 1;
    Ok(())
}

/// `DecrTupleDescRefCount(tupdesc)` (`access/common/tupdesc.c`).
///
/// Decrement the reference count and free the descriptor if no more references
/// remain. C removes the corresponding `CurrentResourceOwner` reference
/// (absent here, see [`IncrTupleDescRefCount`]). Returns the descriptor back to
/// the caller when references remain (`Some`) or consumes and frees it when the
/// count reaches zero (`None`). `Assert(tdrefcount > 0)`.
pub fn DecrTupleDescRefCount(
    mut tupdesc: TupleDescData<'_>,
) -> PgResult<Option<TupleDescData<'_>>> {
    if tupdesc.tdrefcount <= 0 {
        return internal_error("DecrTupleDescRefCount: descriptor is not referenced");
    }
    tupdesc.tdrefcount -= 1;
    if tupdesc.tdrefcount == 0 {
        FreeTupleDesc(tupdesc)?;
        Ok(None)
    } else {
        Ok(Some(tupdesc))
    }
}

/// `equalTupleDescs(tupdesc1, tupdesc2)` (`access/common/tupdesc.c`).
///
/// Compare two `TupleDesc`s for logical equality (`tdtypmod` and `tdrefcount`
/// are not checked).
pub fn equalTupleDescs(tupdesc1: &TupleDescData<'_>, tupdesc2: &TupleDescData<'_>) -> bool {
    if tupdesc1.natts != tupdesc2.natts {
        return false;
    }
    if tupdesc1.tdtypeid != tupdesc2.tdtypeid {
        return false;
    }

    for i in 0..tupdesc1.natts as usize {
        let attr1 = tupdesc1.attr(i);
        let attr2 = tupdesc2.attr(i);

        // We do not need to check every single field: attrelid and attnum can
        // be disregarded. We intentionally ignore atthasmissing.
        if attr1.attname.name_str() != attr2.attname.name_str() {
            return false;
        }
        if attr1.atttypid != attr2.atttypid {
            return false;
        }
        if attr1.attlen != attr2.attlen {
            return false;
        }
        if attr1.attndims != attr2.attndims {
            return false;
        }
        if attr1.atttypmod != attr2.atttypmod {
            return false;
        }
        if attr1.attbyval != attr2.attbyval {
            return false;
        }
        if attr1.attalign != attr2.attalign {
            return false;
        }
        if attr1.attstorage != attr2.attstorage {
            return false;
        }
        if attr1.attcompression != attr2.attcompression {
            return false;
        }
        if attr1.attnotnull != attr2.attnotnull {
            return false;
        }

        // When the column has a not-null constraint, also consider its validity
        // aspect, which only manifests in CompactAttribute->attnullability.
        if attr1.attnotnull && tupdesc1.compact_attr(i).attnullability
            != tupdesc2.compact_attr(i).attnullability
        {
            return false;
        }
        if attr1.atthasdef != attr2.atthasdef {
            return false;
        }
        if attr1.attidentity != attr2.attidentity {
            return false;
        }
        if attr1.attgenerated != attr2.attgenerated {
            return false;
        }
        if attr1.attisdropped != attr2.attisdropped {
            return false;
        }
        if attr1.attislocal != attr2.attislocal {
            return false;
        }
        if attr1.attinhcount != attr2.attinhcount {
            return false;
        }
        if attr1.attcollation != attr2.attcollation {
            return false;
        }
    }

    match (tupdesc1.constr.as_deref(), tupdesc2.constr.as_deref()) {
        (Some(constr1), Some(constr2)) => {
            if constr1.has_not_null != constr2.has_not_null {
                return false;
            }
            if constr1.has_generated_stored != constr2.has_generated_stored {
                return false;
            }
            if constr1.has_generated_virtual != constr2.has_generated_virtual {
                return false;
            }
            if constr1.num_defval != constr2.num_defval {
                return false;
            }
            // We assume both AttrDefault arrays are in adnum order.
            for i in 0..constr1.num_defval as usize {
                let d1 = &constr1.defval[i];
                let d2 = &constr2.defval[i];
                if d1.adnum != d2.adnum {
                    return false;
                }
                if opt_str(&d1.adbin) != opt_str(&d2.adbin) {
                    return false;
                }
            }
            // missing
            let m1_present = !constr1.missing.is_empty();
            let m2_present = !constr2.missing.is_empty();
            if m1_present {
                if !m2_present {
                    return false;
                }
                for i in 0..tupdesc1.natts as usize {
                    let mv1 = &constr1.missing[i];
                    let mv2 = &constr2.missing[i];
                    if mv1.am_present != mv2.am_present {
                        return false;
                    }
                    if mv1.am_present {
                        // C: datumIsEqual(.., attbyval, attlen). The owned
                        // Datum's PartialEq is exactly that comparison
                        // (by-value word equality / by-reference byte identity).
                        if mv1.am_value != mv2.am_value {
                            return false;
                        }
                    }
                }
            } else if m2_present {
                return false;
            }

            if constr1.num_check != constr2.num_check {
                return false;
            }
            // We rely on the ConstrCheck entries being sorted by name.
            for i in 0..constr1.num_check as usize {
                let c1 = &constr1.check[i];
                let c2 = &constr2.check[i];
                if !(opt_str(&c1.ccname) == opt_str(&c2.ccname)
                    && opt_str(&c1.ccbin) == opt_str(&c2.ccbin)
                    && c1.ccenforced == c2.ccenforced
                    && c1.ccvalid == c2.ccvalid
                    && c1.ccnoinherit == c2.ccnoinherit)
                {
                    return false;
                }
            }
            true
        }
        (None, None) => true,
        _ => false,
    }
}

/// `equalRowTypes(tupdesc1, tupdesc2)` (`access/common/tupdesc.c`).
///
/// Whether two descriptors have equal row types — only the fields relevant to
/// row types (name, type, typmod, collation, dropped-ness), ignoring physical
/// storage and table-column metadata. `tdtypmod` is deliberately not checked
/// (so `typcache.c` can match a cached record type to a requested type).
pub fn equalRowTypes(tupdesc1: &TupleDescData<'_>, tupdesc2: &TupleDescData<'_>) -> bool {
    if tupdesc1.natts != tupdesc2.natts {
        return false;
    }
    if tupdesc1.tdtypeid != tupdesc2.tdtypeid {
        return false;
    }

    for i in 0..tupdesc1.natts as usize {
        let attr1 = tupdesc1.attr(i);
        let attr2 = tupdesc2.attr(i);

        if attr1.attname.name_str() != attr2.attname.name_str() {
            return false;
        }
        if attr1.atttypid != attr2.atttypid {
            return false;
        }
        if attr1.atttypmod != attr2.atttypmod {
            return false;
        }
        if attr1.attcollation != attr2.attcollation {
            return false;
        }
        // Record types derived from tables could have dropped fields.
        if attr1.attisdropped != attr2.attisdropped {
            return false;
        }
    }
    true
}

/// `hashRowType(desc)` (`access/common/tupdesc.c`).
///
/// A hash compatible with [`equalRowTypes`]: combine the attribute count, the
/// composite type ID, and each attribute's `atttypid`. The per-value mixing
/// (`hash_uint32` = `hash_bytes_uint32`) is owned by `common/hashfn.c` (seam);
/// `hash_combine` is a pure inline ported in-crate.
pub fn hashRowType(desc: &TupleDescData<'_>) -> u32 {
    let mut s = hash_combine(0, hash_uint32(desc.natts as u32));
    s = hash_combine(s, hash_uint32(desc.tdtypeid));
    for i in 0..desc.natts as usize {
        s = hash_combine(s, hash_uint32(desc.attr(i).atttypid));
    }
    s
}

/// `TupleDescInitEntry(...)` (`access/common/tupdesc.c`).
///
/// Initialise a single attribute. The type-dependent fields come from the
/// `pg_type` row fetched through the syscache (`search_type_attr_info` seam).
/// `attcollation` is set to the type's default; insert a nondefault collation
/// afterwards via [`TupleDescInitEntryCollation`]. If `attributeName` is
/// `None`, the attname is set to the empty (all-NUL) string.
///
/// C writes the attribute fields first and only then calls `SearchSysCache1`,
/// raising `cache lookup failed for type %u` on a miss *after* those writes.
/// This port does the lookup first and errors before touching the descriptor;
/// the reorder is observationally equivalent (C's partial entry is discarded by
/// the caller on the error path, and the happy path reaches the same state).
pub fn TupleDescInitEntry(
    desc: &mut TupleDescData<'_>,
    attributeNumber: AttrNumber,
    attributeName: Option<&str>,
    oidtypeid: Oid,
    typmod: i32,
    attdim: i32,
) -> PgResult<()> {
    let info = syscache_seams::search_type_attr_info::call(oidtypeid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for type {oidtypeid}")))?;
    let index = init_entry_common(desc, attributeNumber, attributeName, oidtypeid, typmod, attdim, info)?;
    populate_compact_attribute(desc, index)
}

/// `TupleDescInitBuiltinEntry(...)` (`access/common/tupdesc.c`).
///
/// Initialise a single attribute without catalog access. Only the limited range
/// of builtin types `tupdesc.c` hard-codes is supported (TEXT/TEXT[], BOOL,
/// INT4, INT8, OID); any other type raises `unsupported type %u`. Unlike
/// [`TupleDescInitEntry`], an attribute name is required.
pub fn TupleDescInitBuiltinEntry(
    desc: &mut TupleDescData<'_>,
    attributeNumber: AttrNumber,
    attributeName: &str,
    oidtypeid: Oid,
    typmod: i32,
    attdim: i32,
) -> PgResult<()> {
    let info = builtin_type_info(oidtypeid)?;
    let index = init_entry_common(
        desc,
        attributeNumber,
        Some(attributeName),
        oidtypeid,
        typmod,
        attdim,
        info,
    )?;
    populate_compact_attribute(desc, index)
}

/// `TupleDescInitEntryCollation(desc, attributeNumber, collationid)`
/// (`access/common/tupdesc.c`). Assign a nondefault collation to a previously
/// initialised attribute.
pub fn TupleDescInitEntryCollation(
    desc: &mut TupleDescData<'_>,
    attributeNumber: AttrNumber,
    collationid: Oid,
) -> PgResult<()> {
    let index = attno_to_index(attributeNumber, desc.natts)?;
    desc.attr_mut(index).attcollation = collationid;
    Ok(())
}

/// `BuildDescFromLists(names, types, typmods, collations)`
/// (`access/common/tupdesc.c`).
///
/// Build a `TupleDesc` in `mcx` from parallel column-name / type-OID / typmod /
/// collation-OID lists (for functions returning RECORD). No constraints are
/// generated. The four lists must be the same length.
pub fn BuildDescFromLists<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[&str],
    types: &[Oid],
    typmods: &[i32],
    collations: &[Oid],
) -> PgResult<TupleDescData<'mcx>> {
    let natts = names.len() as i32;
    if types.len() as i32 != natts || typmods.len() as i32 != natts || collations.len() as i32 != natts
    {
        return internal_error("BuildDescFromLists: list length mismatch");
    }

    let mut desc = CreateTemplateTupleDesc(mcx, natts)?;
    for i in 0..natts as usize {
        let attnum = (i + 1) as AttrNumber;
        TupleDescInitEntry(&mut desc, attnum, Some(names[i]), types[i], typmods[i], 0)?;
        TupleDescInitEntryCollation(&mut desc, attnum, collations[i])?;
    }
    Ok(desc)
}

/// `TupleDescGetDefault(tupdesc, attnum)` (`access/common/tupdesc.c`).
///
/// Get the default expression (or `None` if none) for the given attribute
/// number. The stored `adbin` (a `nodeToString` rendering) is reconstructed via
/// `stringToNode` (`nodes/read.c` seam, allocated in `mcx`).
pub fn TupleDescGetDefault<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
    attnum: AttrNumber,
) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    if let Some(constr) = tupdesc.constr.as_deref() {
        for i in 0..constr.num_defval as usize {
            let attrdef = &constr.defval[i];
            if attrdef.adnum == attnum {
                if let Some(adbin) = attrdef.adbin.as_ref() {
                    let node = read_seams::string_to_node::call(mcx, adbin.as_str())?;
                    return Ok(Some(node));
                }
                return Ok(None);
            }
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The shared body of [`TupleDescInitEntry`] / [`TupleDescInitBuiltinEntry`]:
/// write every attribute field except the compact-attr cache (filled by the
/// caller). Returns the 0-based index of the touched attribute.
#[allow(clippy::too_many_arguments)]
fn init_entry_common(
    desc: &mut TupleDescData<'_>,
    attributeNumber: AttrNumber,
    attributeName: Option<&str>,
    oidtypeid: Oid,
    typmod: i32,
    attdim: i32,
    info: PgTypeInfo,
) -> PgResult<usize> {
    // Assert(attdim >= 0); Assert(attdim <= PG_INT16_MAX).
    if attdim < 0 || attdim > PG_INT16_MAX {
        return internal_error("TupleDescInitEntry: attribute dimension out of range");
    }
    let index = attno_to_index(attributeNumber, desc.natts)?;
    let att = desc.attr_mut(index);

    att.attrelid = 0; // dummy value

    // C: if attributeName == NULL -> zero the name; else namestrcpy. The
    // "re-use the att's own attname buffer" fast path (a caller passing
    // NameStr(att->attname)) cannot be expressed by a &str aliasing the buffer
    // we're about to write; for any distinct name the copy matches C.
    match attributeName {
        Some(name) => namestrcpy(&mut att.attname, name),
        None => att.attname = NameData::default(),
    }

    att.atttypmod = typmod;
    att.attnum = attributeNumber;
    att.attndims = attdim as i16;
    att.attnotnull = false;
    att.atthasdef = false;
    att.atthasmissing = false;
    att.attidentity = 0;
    att.attgenerated = 0;
    att.attisdropped = false;
    att.attislocal = true;
    att.attinhcount = 0;
    // variable-length fields are not present in tupledescs

    att.atttypid = oidtypeid;
    att.attlen = info.typlen;
    att.attbyval = info.typbyval;
    att.attalign = info.typalign;
    att.attstorage = info.typstorage;
    att.attcompression = InvalidCompressionMethod;
    att.attcollation = info.typcollation;
    Ok(index)
}

/// The hard-coded builtin-type table from `TupleDescInitBuiltinEntry`.
fn builtin_type_info(oidtypeid: Oid) -> PgResult<PgTypeInfo> {
    match oidtypeid {
        TEXTOID | TEXTARRAYOID => Ok(PgTypeInfo {
            typlen: -1,
            typbyval: false,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_EXTENDED,
            typcollation: DEFAULT_COLLATION_OID,
        }),
        BOOLOID => Ok(PgTypeInfo {
            typlen: 1,
            typbyval: true,
            typalign: TYPALIGN_CHAR,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        INT4OID => Ok(PgTypeInfo {
            typlen: 4,
            typbyval: true,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        INT8OID => Ok(PgTypeInfo {
            typlen: 8,
            // C: att->attbyval = FLOAT8PASSBYVAL (1 on the 64-bit target ABI).
            typbyval: FLOAT8PASSBYVAL != 0,
            typalign: TYPALIGN_DOUBLE,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        OIDOID => Ok(PgTypeInfo {
            typlen: 4,
            typbyval: true,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: InvalidOid,
        }),
        // C: default: elog(ERROR, "unsupported type %u", oidtypeid).
        _ => internal_error(format!("unsupported type {oidtypeid}")),
    }
}

/// Clear the per-attribute constraint/default/identity/generated fields (the C
/// "since we're not copying constraints and defaults" reset).
fn clear_constraint_fields(att: &mut FormData_pg_attribute) {
    att.attnotnull = false;
    att.atthasdef = false;
    att.atthasmissing = false;
    att.attidentity = 0; // '\0'
    att.attgenerated = 0; // '\0'
}

/// `Assert(attributeNumber >= 1); Assert(attributeNumber <= desc->natts)` plus
/// the conversion to a 0-based index. Out-of-range is a fail-fast error here
/// rather than C's release-build UB.
fn attno_to_index(attno: AttrNumber, natts: i32) -> PgResult<usize> {
    if attno < 1 || (attno as i32) > natts {
        return internal_error("TupleDesc attribute number out of range");
    }
    Ok((attno - 1) as usize)
}

/// `namestrcpy(&att->attname, src)` (`utils/adt/name.c`): zero the fixed-size
/// `NameData` buffer, then copy at most `NAMEDATALEN - 1` bytes, always leaving
/// a NUL terminator.
fn namestrcpy(dst: &mut NameData, src: &str) {
    dst.data.fill(0);
    let bytes = src.as_bytes();
    let len = bytes.len().min(NAMEDATALEN - 1);
    dst.data[..len].copy_from_slice(&bytes[..len]);
}

/// `hash_combine(a, b)` (`common/hashfn.h`) — combine two 32-bit hashes with
/// decent bit mixing (boost-style). Pure inline; no external dependency.
#[inline]
fn hash_combine(mut a: u32, b: u32) -> u32 {
    a ^= b
        .wrapping_add(0x9e37_79b9)
        .wrapping_add(a << 6)
        .wrapping_add(a >> 2);
    a
}

/// `hash_uint32(k)` (`common/hashfn.h`) — `hash_bytes_uint32(k)` (owned by
/// `common/hashfn.c`, routed through its seam).
#[inline]
fn hash_uint32(k: u32) -> u32 {
    hashfn_seams::hash_bytes_uint32::call(k)
}

/// Borrow the bytes of an `Option<PgString>` as `&[u8]` for comparison (`None`
/// reads as empty; C only reaches `strcmp` on present strings).
fn opt_str<'a>(s: &'a Option<::mcx::PgString<'_>>) -> &'a [u8] {
    match s {
        Some(s) => s.as_bytes(),
        None => &[],
    }
}

/// Build an `ERRCODE_INTERNAL_ERROR` `PgError` (the `elog(ERROR, ...)` path).
fn internal_error<T, S: Into<alloc::string::String>>(msg: S) -> PgResult<T> {
    Err(PgError::error(msg.into()).with_sqlstate(ERRCODE_INTERNAL_ERROR))
}

/// Install this unit's owned seams. Wired into `seams-init::init_all()`.
pub fn init_seams() {
    tupdesc_seams::hash_row_type::set(hashRowType);
    tupdesc_seams::equal_row_types::set(equalRowTypes);
    tupdesc_seams::create_tupledesc_copy::set(create_tupledesc_copy_seam);
    tupdesc_seams::create_tuple_desc_copy::set(CreateTupleDescCopy);
    tupdesc_seams::tuple_desc_init_entry_collation::set(
        TupleDescInitEntryCollation,
    );
    tupdesc_seams::tuple_desc_copy_entry::set(TupleDescCopyEntry);
    tupdesc_seams::tuple_desc_init_entry::set(TupleDescInitEntry);
    tupdesc_seams::create_tuple_desc::set(CreateTupleDesc);

    // The `tupdesc.c` constructor surface other crates reach across the cycle
    // through `toastdesc-seams` (value-typed `TupleDescData`).
    toastdesc_seams::create_template_tuple_desc::set(CreateTemplateTupleDesc);
    toastdesc_seams::tuple_desc_init_entry::set(tuple_desc_init_entry_seam);
    toastdesc_seams::build_desc_from_lists::set(build_desc_from_lists_seam);
}

/// Adapter for the `toastdesc-seams` `tuple_desc_init_entry` (always-present
/// name) over [`TupleDescInitEntry`] (`Option<&str>` name).
fn tuple_desc_init_entry_seam(
    desc: &mut TupleDescData<'_>,
    attribute_number: AttrNumber,
    attribute_name: &str,
    oidtypeid: Oid,
    typmod: i32,
    attdim: i32,
) -> PgResult<()> {
    TupleDescInitEntry(
        desc,
        attribute_number,
        Some(attribute_name),
        oidtypeid,
        typmod,
        attdim,
    )
}

/// Adapter for the `toastdesc-seams` `build_desc_from_lists` (names as
/// `PgString`) over [`BuildDescFromLists`] (names as `&str`).
fn build_desc_from_lists_seam<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[::mcx::PgString<'_>],
    types: &[Oid],
    typmods: &[i32],
    collations: &[Oid],
) -> PgResult<::types_tuple::heaptuple::TupleDesc<'mcx>> {
    let name_refs: alloc::vec::Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let desc = BuildDescFromLists(mcx, &name_refs, types, typmods, collations)?;
    Ok(Some(alloc_in(mcx, desc)?))
}

/// Adapter for `create_tupledesc_copy` (returns a `PgBox` in `mcx`, as the
/// typcache/subplan consumers expect).
fn create_tupledesc_copy_seam<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>> {
    let desc = CreateTupleDescCopy(mcx, tupdesc)?;
    alloc_in(mcx, desc)
}

#[cfg(test)]
mod tests;
