//! `MergeAttributes` (tablecmds.c:2546) — build the merged column descriptor
//! list from a `CREATE TABLE`'s explicit columns plus inherited parents.
//!
//! The non-inheritance / non-partition path (`supers == NIL`, not a partition)
//! is ported faithfully: column-count limit, the explicit-column duplicate-name
//! check (with the typed-table `is_from_type` merge), then return the column
//! list with empty inherited-constraint / inherited-not-null lists.
//!
//! The inheritance branch (`supers != NIL`) and the partition branch
//! (`is_partition`) are NOT yet ported: they bottom out on relcache parent
//! reads (`table_open` over arbitrary parents), `AttrMap` / `TupleConstr`
//! projection, and the `MergeInheritedAttribute` / `MergeCheckConstraint`
//! helpers. They panic with a precise handoff.

use backend_utils_error::ereport;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::{PgResult, ERRCODE_DUPLICATE_COLUMN, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_UNDEFINED_COLUMN, ERROR};
use types_nodes::rawnodes::ColumnDef;
use types_tuple::heaptuple::MaxHeapAttributeNumber;

use backend_commands_tablecmds_seams::MergeAttributesResult;

use crate::helpers::here;

/// `MergeAttributes(columns, supers, relpersistence, is_partition,
/// &supconstr, &supnotnulls)`.
///
/// `columns` is destructively rewritten (duplicate typed-table column options
/// are merged into the type-derived column and removed).
pub fn merge_attributes<'mcx>(
    mcx: Mcx<'mcx>,
    mut columns: PgVec<'mcx, ColumnDef<'mcx>>,
    supers: &[types_core::primitive::Oid],
    _relpersistence: u8,
    is_partition: bool,
) -> PgResult<MergeAttributesResult<'mcx>> {
    /*
     * Check for and reject tables with too many columns. We perform this check
     * relatively early to avoid overflowing an AttrNumber, and because the
     * dedup pass below is O(n^2).
     */
    if columns.len() as i32 > MaxHeapAttributeNumber {
        return ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(format!(
                "tables can have at most {MaxHeapAttributeNumber} columns"
            ))
            .finish(here("MergeAttributes"))
            .map(|()| unreachable!());
    }

    /*
     * Check for duplicate names in the explicit list of attributes.
     *
     * Index-based looping (not foreach) because the inner loop may delete the
     * element it is examining. Any deletion happens beyond the outer index, so
     * the outer index never needs adjustment.
     */
    let mut coldefpos = 0usize;
    while coldefpos < columns.len() {
        if !is_partition && columns[coldefpos].typeName.is_none() {
            /*
             * Typed-table column option that does not belong to a column from
             * the type. (Columns from the type come first in the list. We omit
             * this check for partition column lists, processed separately.)
             */
            let colname = colname_of(&columns[coldefpos]);
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!("column \"{colname}\" does not exist"))
                .finish(here("MergeAttributes"))
                .map(|()| unreachable!());
        }

        let mut restpos = coldefpos + 1;
        while restpos < columns.len() {
            if colname_of(&columns[coldefpos]) == colname_of(&columns[restpos]) {
                if columns[coldefpos].is_from_type {
                    /* merge the column options into the column from the type */
                    let restdef = columns.remove(restpos);
                    let coldef = &mut columns[coldefpos];
                    coldef.is_not_null = restdef.is_not_null;
                    coldef.raw_default = restdef.raw_default;
                    coldef.cooked_default = restdef.cooked_default;
                    coldef.constraints = restdef.constraints;
                    coldef.is_from_type = false;
                    /* restpos now points at the next element; do not advance */
                } else {
                    let colname = colname_of(&columns[restpos]);
                    return ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_COLUMN)
                        .errmsg(format!("column \"{colname}\" specified more than once"))
                        .finish(here("MergeAttributes"))
                        .map(|()| unreachable!());
                }
            } else {
                restpos += 1;
            }
        }
        coldefpos += 1;
    }

    /*
     * The partition and legacy-inheritance merge paths are not yet ported.
     * They require reading arbitrary parent relations (table_open over
     * `supers`), AttrMap / TupleConstr projection, and the
     * MergeInheritedAttribute / MergeCheckConstraint helpers. The plain
     * CREATE TABLE path (no supers, no partition) needs none of that.
     */
    if is_partition {
        panic!(
            "MergeAttributes: partition column-merge path not yet ported \
             (is_partition=true); only the plain CREATE TABLE path is ported"
        );
    }
    if !supers.is_empty() {
        panic!(
            "MergeAttributes: legacy-inheritance merge path not yet ported \
             ({} parent(s) in `supers`); only the no-inheritance path is ported",
            supers.len()
        );
    }

    /*
     * No parents and not a partition: there are no inherited constraints or
     * not-null constraints, and no bogus-default conflicts are possible.
     * Return the (deduped) explicit column list as-is.
     */
    let old_constraints = vec_with_capacity_in(mcx, 0)?;
    let old_notnulls = vec_with_capacity_in(mcx, 0)?;

    Ok(MergeAttributesResult {
        columns,
        old_constraints,
        old_notnulls,
    })
}

/// The column name of a `ColumnDef`. A `ColumnDef` produced by the parser
/// always carries a name; an absent name is treated as empty (matching C's
/// `strcmp` against a possibly-empty string).
fn colname_of<'a>(col: &'a ColumnDef<'_>) -> &'a str {
    col.colname.as_ref().map(|s| s.as_str()).unwrap_or("")
}
