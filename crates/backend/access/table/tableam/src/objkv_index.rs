//! Entries go into the *same commit object* as the row changes they describe.
//! That is the whole design: no second commit point, so a crash cannot leave
//! index and table disagreeing, and two transactions inserting one value into
//! a unique index collide in the conflict detector that already exists.
//!
//! Key format and reasoning: `objkv::index_key`. What it cannot order is
//! refused at CREATE INDEX rather than mis-sorted at read time.

use ::datum::Datum;
use ::mcx::Mcx;
use ::objkv::commit::Op;
use ::objkv::index_key::{self, Col, ColOpt};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_rel::Relation;

use crate::objkv_am;

// Type OIDs the encoding orders correctly. Anything else is refused.
const BOOLOID: Oid = 16;
const INT8OID: Oid = 20;
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
/// `name`: fixed 64 bytes, C collation by definition. The catalogs key on it.
const NAMEOID: Oid = 19;
/// `oid`: unsigned. Nearly every catalog index starts with one.
const OIDOID: Oid = 26;
/// `"char"`: one byte, unsigned. pg_class.relkind and its kin.
const CHAROID: Oid = 18;
const INT2VECTOROID: Oid = 22;
const OIDVECTOROID: Oid = 30;
/// The oid aliases: an oid with another input function, ordering as oid does.
const REGPROCOID: Oid = 24;
const REGPROCEDUREOID: Oid = 2202;
const REGOPEROID: Oid = 2203;
const REGOPERATOROID: Oid = 2204;
const REGCLASSOID: Oid = 2205;
const REGTYPEOID: Oid = 2206;
const REGCOLLATIONOID: Oid = 4191;
const REGCONFIGOID: Oid = 3734;
const REGDICTIONARYOID: Oid = 3769;
const REGNAMESPACEOID: Oid = 4089;
const REGROLEOID: Oid = 4096;
/// What a `name` looks like inside an index: `name_ops` sets `opckeytype =
/// cstring`, so the declared type is cstring though the value is NameData.
const CSTRINGOID: Oid = 2275;
const TEXTOID: Oid = 25;
const BPCHAROID: Oid = 1042;
const VARCHAROID: Oid = 1043;
const DATEOID: Oid = 1082;
const TIMESTAMPOID: Oid = 1114;
const TIMESTAMPTZOID: Oid = 1184;
const UUIDOID: Oid = 2950;
const FLOAT4OID: Oid = 700;
const FLOAT8OID: Oid = 701;

const C_COLLATION_OID: Oid = 950;

fn refuse(what: String) -> Box<PgError> {
    Box::new(
        PgError::error(what).with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub fn supports_type(typid: Oid) -> bool {
    matches!(
        typid,
        BOOLOID
            | INT2OID
            | INT4OID
            | INT8OID
            | OIDOID
            | CHAROID
            | INT2VECTOROID
            | OIDVECTOROID
            | REGPROCOID
            | REGPROCEDUREOID
            | REGOPEROID
            | REGOPERATOROID
            | REGCLASSOID
            | REGTYPEOID
            | REGCOLLATIONOID
            | REGCONFIGOID
            | REGDICTIONARYOID
            | REGNAMESPACEOID
            | REGROLEOID
            | NAMEOID
            | CSTRINGOID
            | DATEOID
            | TIMESTAMPOID
            | TIMESTAMPTZOID
            | UUIDOID
            | TEXTOID
            | VARCHAROID
            | BPCHAROID
            | FLOAT4OID
            | FLOAT8OID
    )
}

/// Only C does. Any other needs an ICU sort key, and those change between ICU
/// versions: baking one into an immutable key means an upgrade reorders it.
pub fn supports_collation(collation: Oid) -> bool {
    collation == 0 || collation == C_COLLATION_OID
}

#[derive(Debug, PartialEq)]
enum Owned {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Oid(u32),
    Char(u8),
    Vector(Vec<u64>),
    Name(Vec<u8>),
    Uuid([u8; 16]),
    Text(Vec<u8>),
    /// `char(n)`, still padded; the encoder trims it.
    Bpchar(Vec<u8>),
    Float8(f64),
}

impl Owned {
    fn as_col(&self) -> Col<'_> {
        match self {
            Owned::Null => Col::Null,
            Owned::Bool(b) => Col::Bool(*b),
            Owned::Int2(v) => Col::Int2(*v),
            Owned::Int4(v) => Col::Int4(*v),
            Owned::Int8(v) => Col::Int8(*v),
            Owned::Oid(v) => Col::Oid(*v),
            Owned::Char(c) => Col::Char(*c),
            Owned::Vector(v) => Col::Vector(v),
            Owned::Name(n) => Col::Name(n),
            Owned::Uuid(u) => Col::Uuid(u),
            Owned::Text(t) => Col::Text(t),
            Owned::Bpchar(t) => Col::Bpchar(t),
            Owned::Float8(v) => Col::Float8(*v),
        }
    }
}

/// One-dimensional, no nulls, standard header -- which is what makes reading
/// them this directly safe.
fn vector_elems(d: Datum, wide: bool) -> Vec<u64> {
    const HEADER: usize = 4 + 4 + 4 + 4 + 4 + 4;
    let p = d.as_usize() as *const u8;
    // SAFETY: a non-NULL vector datum addresses a complete array header.
    let n = unsafe { core::ptr::read_unaligned(p.add(16) as *const i32) }.max(0) as usize;
    let width = if wide { 4 } else { 2 };
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        // SAFETY: n is the array's own dimension; elements follow the header
        // contiguously with no null bitmap.
        let e = unsafe {
            let at = p.add(HEADER + i * width);
            if wide {
                core::ptr::read_unaligned(at as *const u32) as u64
            } else {
                core::ptr::read_unaligned(at as *const i16) as u64
            }
        };
        out.push(e);
    }
    out
}

/// Detoasted first: a value can arrive compressed or out of line, and reading
/// its header as text produces a wrong key rather than an error.
fn varlena_bytes(mcx: Mcx<'_>, d: Datum) -> PgResult<Vec<u8>> {
    let p = d.as_usize() as *const u8;
    let total = varsize_any(p);
    // SAFETY: a non-NULL varlena datum whose header describes its own size.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    let flat = ::detoast_seams::detoast_attr::call(mcx, raw)?;
    let len = u32::from_ne_bytes([flat[0], flat[1], flat[2], flat[3]]) as usize >> 2;
    Ok(flat[4..len].to_vec())
}

fn varsize_any(p: *const u8) -> usize {
    // SAFETY: p addresses a live varlena header.
    unsafe {
        let b0 = *p;
        if b0 == 0x01 {
            2 + match *p.add(1) {
                1 | 2 | 3 => 8,
                18 => 16,
                other => panic!("unrecognized TOAST vartag {other}"),
            }
        } else if b0 & 0x01 != 0 {
            (b0 as usize >> 1) & 0x7f
        } else {
            let w = u32::from_ne_bytes(
                core::slice::from_raw_parts(p, 4).try_into().expect("4 bytes"),
            );
            (w >> 2) as usize
        }
    }
}

/// The elements of an `= ANY (array)` argument, NULLs dropped.
///
/// A NULL element can never equal anything, so it matches no row and leaving
/// it in would encode as the null tag and match rows that have no value.
pub fn array_elements<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<Vec<Datum>> {
    let p = d.as_usize() as *const u8;
    let total = varsize_any(p);
    // SAFETY: a non-NULL array datum whose header describes its own size.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    let flat = ::detoast_seams::detoast_attr::call(mcx, raw)?;
    let img = &flat[..];
    let elemtype = ::arrayfuncs::arr_elemtype(img);
    let (elmlen, elmbyval, elmalign) = ::lsyscache::get_typlenbyvalalign(elemtype)?;
    let (values, nulls) =
        ::arrayfuncs::deconstruct_array(mcx, img, elmlen as i32, elmbyval, elmalign as u8, true)?;
    Ok(values
        .iter()
        .zip(nulls.iter())
        .filter(|(_, n)| !**n)
        .map(|(v, _)| *v)
        .collect())
}

fn owned_col(mcx: Mcx<'_>, typid: Oid, d: Datum, isnull: bool) -> PgResult<Owned> {
    if isnull {
        return Ok(Owned::Null);
    }
    Ok(match typid {
        BOOLOID => Owned::Bool(d.as_bool()),
        INT2OID => Owned::Int2(d.as_i16()),
        INT4OID | DATEOID => Owned::Int4(d.as_i32()),
        INT8OID | TIMESTAMPOID | TIMESTAMPTZOID => Owned::Int8(d.as_i64()),
        OIDOID
        | REGPROCOID
        | REGPROCEDUREOID
        | REGOPEROID
        | REGOPERATOROID
        | REGCLASSOID
        | REGTYPEOID
        | REGCOLLATIONOID
        | REGCONFIGOID
        | REGDICTIONARYOID
        | REGNAMESPACEOID
        | REGROLEOID => Owned::Oid(d.as_u32()),
        CHAROID => Owned::Char(d.as_u8()),
        // Widened, so the key is one shape and a float8 bound needs no rounding.
        FLOAT4OID => Owned::Float8(d.as_f32() as f64),
        FLOAT8OID => Owned::Float8(d.as_f64()),
        INT2VECTOROID | OIDVECTOROID => Owned::Vector(vector_elems(d, typid == OIDVECTOROID)),
        // Both spellings: a `name` is NAMEDATALEN bytes NUL-padded, and the cstring
        // an index declares is the same pointer read to its first NUL.
        NAMEOID | CSTRINGOID => Owned::Name(name_bytes(d)?),
        UUIDOID => {
            let p = d.as_usize() as *const u8;
            // SAFETY: a uuid datum points at exactly 16 bytes.
            let mut u = [0u8; 16];
            u.copy_from_slice(unsafe { core::slice::from_raw_parts(p, 16) });
            Owned::Uuid(u)
        }
        TEXTOID | VARCHAROID => Owned::Text(varlena_bytes(mcx, d)?),
        // Compared blank-trimmed by bpcharcmp, whatever its padding; the
        // encoder trims. Both an entry and a bound come through here, so
        // `c = 'a'` (typmod -1, unpadded) meets the padded entry for 'a'.
        BPCHAROID => Owned::Bpchar(varlena_bytes(mcx, d)?),
        other => {
            return Err(refuse(format!(
                "objkv indexes cannot order type with OID {other}"
            )))
        }
    })
}

/// Read to the NUL, never past NAMEDATALEN: a value with no NUL is not a name,
/// and reading on would walk off the allocation.
fn name_bytes(d: Datum) -> PgResult<Vec<u8>> {
    const MAX: usize = ::types_core::fmgr::NAMEDATALEN as usize;
    let p = d.as_usize() as *const u8;
    // SAFETY: a name datum addresses NAMEDATALEN bytes containing a NUL.
    let bytes = unsafe { core::slice::from_raw_parts(p, MAX) };
    match bytes.iter().position(|&b| b == 0) {
        Some(end) => Ok(bytes[..end].to_vec()),
        None => Err(refuse(format!(
            "objkv indexes: a name value with no terminator in {MAX} bytes"
        ))),
    }
}

/// Whether column `attno` (1-based) can be handed back from the entry alone.
///
/// Every key column is in the entry, so this is only about whether the bytes
/// can be turned back into the value exactly. `objkv_btree` takes no INCLUDE
/// columns (`amcaninclude` is off), so past `indnkeyatts` there is nothing.
pub fn returnable(index: &Relation<'_>, attno: i32) -> bool {
    let nkeys = index.rd_index.as_ref().map_or(0, |i| i.indnkeyatts as i32);
    attno >= 1 && attno <= nkeys && returnable_type(index.rd_att.attr(attno as usize - 1).atttypid)
}

/// The vector types have no decoder yet. `char(n)` is stored trimmed, and
/// its padding cannot be put back without the typmod and the encoding's
/// character widths, so a padded value is fetched from the row instead.
fn returnable_type(typid: Oid) -> bool {
    !matches!(typid, INT2VECTOROID | OIDVECTOROID | BPCHAROID)
}

/// The key columns of one entry, as values.
///
/// The exact inverse of what was written. Nothing is read from the table, and
/// nothing needs to be: an entry is retired with the row change that
/// invalidates it, so what it says is what the row says.
pub fn decode_entry<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    tuple: &[u8],
) -> PgResult<(Vec<Datum>, Vec<bool>)> {
    let nkeys = index.rd_index.as_ref().map_or(0, |i| i.indnkeyatts as usize);
    let spans = ::objkv::index_key::column_spans(tuple, &widths(index, nkeys)?)
        .ok_or_else(|| refuse("objkv indexes: unreadable index entry".to_string()))?;

    let mut values = Vec::with_capacity(nkeys);
    let mut isnull = Vec::with_capacity(nkeys);
    for (i, &(a, b)) in spans.iter().enumerate() {
        let typid = index.rd_att.attr(i).atttypid;
        // A column reported unreturnable is one the planner never asks for;
        // it is left NULL so the columns beside it can still be handed back.
        if ::objkv::index_key::is_null_tag(tuple[a]) || !returnable_type(typid) {
            values.push(Datum::null());
            isnull.push(true);
            continue;
        }
        let body = ::objkv::index_key::column_body(tuple[a], &tuple[a + 1..b]);
        values.push(datum_from_key(mcx, typid, &body)?);
        isnull.push(false);
    }
    Ok((values, isnull))
}

/// A copy in the caller's context, since the key it was read from is scratch.
fn planted<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<Datum> {
    let mut v: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, bytes.len())?;
    ::mcx::vec_append_bytes(&mut v, bytes)?;
    Ok(Datum::from_usize(v.leak().as_ptr() as usize))
}

fn datum_from_key<'mcx>(mcx: Mcx<'mcx>, typid: Oid, body: &[u8]) -> PgResult<Datum> {
    use ::objkv::index_key::{decode_float, decode_int, decode_string, decode_uint};
    let bad = || refuse(format!("objkv indexes: unreadable value of type {typid}"));
    Ok(match typid {
        BOOLOID => Datum::from_bool(body.first().copied().ok_or_else(bad)? != 0),
        CHAROID => Datum::from_u8(body.first().copied().ok_or_else(bad)?),
        INT2OID => Datum::from_i16(decode_int(body) as i16),
        INT4OID | DATEOID => Datum::from_i32(decode_int(body) as i32),
        INT8OID | TIMESTAMPOID | TIMESTAMPTZOID => Datum::from_i64(decode_int(body)),
        OIDOID | REGPROCOID | REGPROCEDUREOID | REGOPEROID | REGOPERATOROID | REGCLASSOID
        | REGTYPEOID | REGCOLLATIONOID | REGCONFIGOID | REGDICTIONARYOID | REGNAMESPACEOID
        | REGROLEOID => Datum::from_u32(decode_uint(body) as u32),
        // Widened on the way in, so narrowing here is the value it started as.
        FLOAT4OID => Datum::from_f32(decode_float(body.try_into().map_err(|_| bad())?) as f32),
        FLOAT8OID => Datum::from_f64(decode_float(body.try_into().map_err(|_| bad())?)),
        UUIDOID => planted(mcx, body)?,
        // NAMEDATALEN bytes, NUL-padded: the padding is not part of the value,
        // so it was trimmed on the way in and has to be put back.
        NAMEOID | CSTRINGOID => {
            const MAX: usize = ::types_core::fmgr::NAMEDATALEN as usize;
            let text = decode_string(body).ok_or_else(bad)?;
            let mut padded = vec![0u8; MAX];
            let n = text.len().min(MAX - 1);
            padded[..n].copy_from_slice(&text[..n]);
            planted(mcx, &padded)?
        }
        TEXTOID | VARCHAROID => {
            let text = decode_string(body).ok_or_else(bad)?;
            let v = ::varlena::cstring_to_text(mcx, &text)?;
            Datum::from_usize(v.into_image().leak().as_ptr() as usize)
        }
        other => return Err(refuse(format!("objkv indexes cannot return type {other}"))),
    })
}

fn cols_of(
    mcx: Mcx<'_>,
    index: &Relation<'_>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<Vec<Owned>> {
    let nkeys = index.rd_index.as_ref().map_or(0, |i| i.indnkeyatts as usize);
    let mut out = Vec::with_capacity(nkeys);
    for i in 0..nkeys {
        let typid = index.rd_att.attr(i).atttypid;
        let collation = index.rd_indcollation.get(i).copied().unwrap_or(0);
        if matches!(typid, TEXTOID | VARCHAROID | BPCHAROID) && !supports_collation(collation) {
            return Err(refuse(format!(
                "objkv indexes support only the C collation; column {} of index \"{}\" uses another",
                i + 1,
                index.name()
            )));
        }
        out.push(owned_col(mcx, typid, values[i], isnull[i])?);
    }
    Ok(out)
}

/// Each key column's direction, from pg_index.indoption.
fn opts_of(index: &Relation<'_>) -> Vec<ColOpt> {
    const INDOPTION_DESC: i16 = 1 << 0;
    const INDOPTION_NULLS_FIRST: i16 = 1 << 1;
    let nkeys = index.rd_index.as_ref().map_or(0, |i| i.indnkeyatts as usize);
    (0..nkeys)
        .map(|i| {
            let o = index.rd_indoption.get(i).copied().unwrap_or(0);
            ColOpt { desc: o & INDOPTION_DESC != 0, nulls_first: o & INDOPTION_NULLS_FIRST != 0 }
        })
        .collect()
}

fn is_unique(index: &Relation<'_>) -> bool {
    index.rd_index.as_ref().is_some_and(|i| i.indisunique)
}

fn index_id(index: &Relation<'_>) -> u32 {
    index.rd_id
}

fn as_pg_error(e: std::io::Error) -> Box<PgError> {
    Box::new(PgError::error(format!("objkv index: {e}")))
}

/// The entry key `rowid`'s current row would produce, or None if the row is
/// gone or a column cannot be re-derived.
fn row_entry_key(
    mcx: Mcx<'_>,
    index: &Relation<'_>,
    heap: &Relation<'_>,
    scope: u32,
    rowid: u64,
) -> PgResult<Option<Vec<u8>>> {
    // The scope is passed in, not derived from the heap: a shared catalog is
    // described in scope 0 and in every database, and asking the heap which it
    // 'naturally' belongs to is how a critical system index comes back empty.
    let Some(image) = objkv_am::fetch_row(scope, objkv_am::relid(heap), rowid, ::objkv::key::LATEST)?
    else {
        return Ok(None); // the row is gone; the entry is a leftover
    };
    let Some(ind) = index.rd_index.as_ref() else {
        return Ok(None);
    };
    let nkeys = ind.indnkeyatts as usize;

    let mut slot = ::exectuples::make_tuple_table_slot(
        mcx,
        ::types_slot::TupleSlotKind::HeapTuple,
        Some(heap.rd_att.clone()),
    );
    objkv_am::store_image(mcx, &mut slot, &image, objkv_am::tid_of(rowid))?;

    let mut values = vec![Datum::null(); nkeys];
    let mut isnull = vec![false; nkeys];
    let plain = !ind.has_indpred && ind.indkey[..nkeys].iter().all(|&a| a > 0);
    if plain {
        for i in 0..nkeys {
            values[i] = ::exectuples::slot_getattr(&mut slot, ind.indkey[i] as i32, &mut isnull[i]);
        }
    } else {
        // An expression or a predicate: the executor evaluates it, above
        // this crate, through the seam. A row a partial index does not
        // cover has no entry to retire.
        let natts = ind.indnatts as usize;
        let mut all = vec![Datum::null(); natts];
        let mut all_null = vec![false; natts];
        if !::tableam_seams::objkv_index_row_datum::call(mcx, index, &mut slot, &mut all, &mut all_null)? {
            return Ok(None);
        }
        values.copy_from_slice(&all[..nkeys]);
        isnull.copy_from_slice(&all_null[..nkeys]);
    }
    let owned = cols_of(mcx, index, &values, &isnull)?;
    let cols: Vec<Col> = owned.iter().map(Owned::as_col).collect();
    Ok(Some(
        index_key::entry_key_with(
            objkv_am::scope(index),
            index_id(index),
            &cols,
            &opts_of(index),
            rowid,
            is_unique(index),
        )
        .map_err(as_pg_error)?,
    ))
}

/// One entry under an explicit scope, unchecked: the lift copies an already
/// consistent catalog into scope 0 while the heap relation belongs to a
/// database, so a check here reports duplicates that do not exist.
pub fn insert_unchecked(
    mcx: Mcx<'_>,
    index: &Relation<'_>,
    scope: u32,
    values: &[Datum],
    isnull: &[bool],
    rowid: u64,
) -> PgResult<()> {
    note_table_of(index);
    let owned = cols_of(mcx, index, values, isnull)?;
    let cols: Vec<Col> = owned.iter().map(Owned::as_col).collect();
    let key = index_key::entry_key_with(scope, index_id(index), &cols, &opts_of(index), rowid, is_unique(index))
        .map_err(as_pg_error)?;
    objkv_am::stage(key, Op::Put(index_key::payload(rowid)));
    Ok(())
}

/// One entry beside its row change; both reach the bucket in one object.
///
/// `report_duplicate` is `checkUnique == UNIQUE_CHECK_PARTIAL`: a speculative
/// insert (`INSERT ... ON CONFLICT`) and a deferred constraint both want the
/// duplicate reported rather than raised, so the caller can withdraw the row
/// or queue the recheck. Returns whether the entry satisfies the constraint.
pub fn insert(
    mcx: Mcx<'_>,
    index: &Relation<'_>,
    heap: &Relation<'_>,
    values: &[Datum],
    isnull: &[bool],
    rowid: u64,
    report_duplicate: bool,
) -> PgResult<bool> {
    note_table_of(index);
    let owned = cols_of(mcx, index, values, isnull)?;
    let cols: Vec<Col> = owned.iter().map(Owned::as_col).collect();
    let unique = is_unique(index);
    let key = index_key::entry_key_with(objkv_am::scope(index), index_id(index), &cols, &opts_of(index), rowid, unique)
        .map_err(as_pg_error)?;

    // A NULL is never a duplicate, and is keyed with its rowid for that reason.
    if unique && !cols.iter().any(Col::is_null) {
        let duplicate = match objkv_am::staged_op(&key) {
            // Ours, invisible to the store check below -- unless it names the
            // row being written, which is an updated catalog row's own entry.
            Some(Op::Put(v)) => index_key::rowid_of(&key, &v) != Some(rowid),
            Some(Op::Delete) => false,
            None => match first_match(index, &cols, unique)? {
                // A candidate counts only if its row still carries this value.
                Some(id) if id != rowid => {
                    row_entry_key(mcx, index, heap, objkv_am::scope(index), id)?.as_deref()
                        == Some(key.as_slice())
                }
                _ => false,
            },
        };
        if duplicate {
            if report_duplicate {
                // Staged anyway, as nbtree inserts the tuple anyway: the row
                // is withdrawn or the recheck fires, and either way that path
                // removes the entry with it.
                objkv_am::stage(key, Op::Put(index_key::payload(rowid)));
                return Ok(false);
            }
            return Err(Box::new(
                PgError::error(format!(
                    "duplicate key value violates unique constraint \"{}\"",
                    index.name()
                ))
                .with_sqlstate(::types_error::ERRCODE_UNIQUE_VIOLATION),
            ));
        }
    }

    objkv_am::stage(key, Op::Put(index_key::payload(rowid)));
    Ok(true)
}

fn first_match(index: &Relation<'_>, cols: &[Col<'_>], unique: bool) -> PgResult<Option<u64>> {
    let prefix = index_key::seek_prefix_with(objkv_am::scope(index), index_id(index), cols, &opts_of(index), unique)
        .map_err(as_pg_error)?;
    let (seq, view) = objkv_am::with_db(|db| (db.current_seq(), db.view()))?;
    let found = ::objkv::index::lookup(&view, &prefix, ::objkv::key::LATEST);
    // ...and record when now was, or an insert-only transaction validates
    // against nothing and the duplicate slips through.
    objkv_am::observe_read_at(seq);
    Ok(found.map_err(as_pg_error)?.first().copied())
}

/// One condition from the scan key array, on a key column of the index.
pub struct Cond {
    /// Zero-based key column.
    pub col: usize,
    pub strategy: u16,
    /// One value, or the elements of an `IN` list. Empty matches nothing.
    pub values: Vec<Datum>,
    pub isnull: bool,
    /// The type of `value`, when the operator's right-hand type differs from
    /// the column's. 0 means they are the same.
    pub subtype: Oid,
    /// The condition arrived as `= ANY (array)`, so `values` holds the whole
    /// list and any one of them matching is a match.
    pub isarray: bool,
    /// `IS NULL` or `IS NOT NULL` rather than a comparison against a value.
    ///
    /// A NULL encodes as one byte above every real value, so the two are an
    /// equality and a less-than against that byte. The distinction from an
    /// ordinary NULL argument matters: `x > NULL` matches nothing, while
    /// `x IS NOT NULL` matches almost everything.
    pub nulltest: bool,
}

const BT_LESS: u16 = 1;
const BT_LESS_EQUAL: u16 = 2;
const BT_EQUAL: u16 = 3;
const BT_GREATER_EQUAL: u16 = 4;
const BT_GREATER: u16 = 5;

/// Bytewise, which is the point of the encoding: no operator is needed.
fn passes(have: &[u8], want: &[u8], strategy: u16) -> bool {
    match strategy {
        BT_LESS => have < want,
        BT_LESS_EQUAL => have <= want,
        BT_EQUAL => have == want,
        BT_GREATER_EQUAL => have >= want,
        BT_GREATER => have > want,
        _ => false,
    }
}

/// The integer widths. A bound of one against a column of another is the
/// commonest cross-type comparison there is -- `bigint_col = 42` hands over
/// an int4 -- and is refitted to the column's width in `refit`.
const INTS: &[Oid] = &[INT2OID, INT4OID, INT8OID];

/// Whether a value of type `b` can be compared against a column of type `a`
/// once read and, for the integers, refitted.
///
/// The string types differ only in how they are stored, the oid aliases are
/// an oid with another input function, and both float widths ride one key.
/// The integer widths do not encode alike -- an eight-byte bound against a
/// four-byte column would sort wrongly in silence -- so a bound of one width
/// is restated at the column's before it is encoded. Everything else must
/// match exactly.
fn same_encoding(a: Oid, b: Oid) -> bool {
    const STRINGS: &[Oid] = &[NAMEOID, CSTRINGOID, TEXTOID, VARCHAROID, BPCHAROID];
    const FLOATS: &[Oid] = &[FLOAT4OID, FLOAT8OID];
    const OIDS: &[Oid] = &[
        OIDOID, REGPROCOID, REGPROCEDUREOID, REGOPEROID, REGOPERATOROID, REGCLASSOID,
        REGTYPEOID, REGCOLLATIONOID, REGCONFIGOID, REGDICTIONARYOID, REGNAMESPACEOID,
        REGROLEOID,
    ];
    a == b
        || [STRINGS, OIDS, FLOATS, INTS]
            .iter()
            .any(|set| set.contains(&a) && set.contains(&b))
}

fn int_value(o: &Owned) -> Option<i64> {
    match o {
        Owned::Int2(v) => Some(*v as i64),
        Owned::Int4(v) => Some(*v as i64),
        Owned::Int8(v) => Some(*v),
        _ => None,
    }
}

/// Where an integer lands against a column of a given width.
#[derive(Debug, PartialEq)]
enum Fit {
    Exact(Owned),
    /// Under the column's smallest value.
    Below,
    /// Over its largest.
    Above,
}

fn fit_int(v: i64, col_type: Oid) -> Fit {
    let out_of_range = || if v < 0 { Fit::Below } else { Fit::Above };
    match col_type {
        INT2OID => i16::try_from(v).map_or_else(|_| out_of_range(), |v| Fit::Exact(Owned::Int2(v))),
        INT4OID => i32::try_from(v).map_or_else(|_| out_of_range(), |v| Fit::Exact(Owned::Int4(v))),
        _ => Fit::Exact(Owned::Int8(v)),
    }
}

/// The column's smallest and largest values.
fn int_extremes(col_type: Oid) -> (Owned, Owned) {
    match col_type {
        INT2OID => (Owned::Int2(i16::MIN), Owned::Int2(i16::MAX)),
        INT4OID => (Owned::Int4(i32::MIN), Owned::Int4(i32::MAX)),
        _ => (Owned::Int8(i64::MIN), Owned::Int8(i64::MAX)),
    }
}

/// A comparison against integers of another width, restated at the column's
/// width -- what nbtree's cross-type operators (`int84eq`, `int24lt`, ...)
/// do without saying so. A literal past the column's range has no encoding,
/// but the comparison still has an answer: `int2_col = 100000` is false for
/// every row, and `int2_col < 100000` is true for every row with a value,
/// which is `int2_col <= 32767`. A list is `ANY`, so `x < ANY(3, 100000)`
/// is `x < 100000` and one literal over the top makes the whole list every
/// value. `None` means no row can satisfy the condition.
fn refit(strategy: u16, values: Vec<Owned>, col_type: Oid, read_as: Oid) -> Option<(u16, Vec<Owned>)> {
    if col_type == read_as || !INTS.contains(&col_type) || !INTS.contains(&read_as) {
        return Some((strategy, values));
    }
    let fits: Vec<Fit> = values.iter().filter_map(int_value).map(|v| fit_int(v, col_type)).collect();
    let (min, max) = int_extremes(col_type);
    let exact = |fits: Vec<Fit>| -> Vec<Owned> {
        fits.into_iter().filter_map(|f| if let Fit::Exact(o) = f { Some(o) } else { None }).collect()
    };
    let (strategy, kept) = match strategy {
        BT_LESS | BT_LESS_EQUAL if fits.contains(&Fit::Above) => (BT_LESS_EQUAL, vec![max]),
        BT_GREATER | BT_GREATER_EQUAL if fits.contains(&Fit::Below) => (BT_GREATER_EQUAL, vec![min]),
        // Whatever is left out of range matches nothing under this strategy:
        // `x = 100000`, `x < -100000`, `x > 100000`.
        _ => (strategy, exact(fits)),
    };
    if kept.is_empty() { None } else { Some((strategy, kept)) }
}

/// One condition with its values read, refitted and encoded: what the scan
/// is planned from. Values are encoded as the column is stored, so a
/// descending column's list is in key order, the reverse of value order;
/// the strategy stays in value order until `oriented` turns it.
struct Probe {
    col: usize,
    strategy: u16,
    nulltest: bool,
    /// Sorted and deduplicated. Never empty: an empty list is answered
    /// before a probe is built.
    values: Vec<Vec<u8>>,
}

/// A comparison against a column that is stored descending reads the other
/// way in key order. A null test is against the tag, which is not inverted;
/// only where the tag sits changes, and only "everything that is not it"
/// cares which side that is.
fn oriented(p: &Probe, opt: ColOpt) -> u16 {
    if p.nulltest {
        return if opt.nulls_first && p.strategy == BT_LESS { BT_GREATER } else { p.strategy };
    }
    if !opt.desc {
        return p.strategy;
    }
    match p.strategy {
        BT_LESS => BT_GREATER,
        BT_LESS_EQUAL => BT_GREATER_EQUAL,
        BT_GREATER_EQUAL => BT_LESS_EQUAL,
        BT_GREATER => BT_LESS,
        other => other,
    }
}

fn is_range(strategy: u16) -> bool {
    matches!(strategy, BT_LESS | BT_LESS_EQUAL | BT_GREATER_EQUAL | BT_GREATER)
}

/// How many bytes each key column takes once encoded, so an entry key can be
/// split back into columns.
fn widths(index: &Relation<'_>, nkeys: usize) -> PgResult<Vec<index_key::Width>> {
    use index_key::Width;
    (0..nkeys)
        .map(|i| match index.rd_att.attr(i).atttypid {
            BOOLOID | CHAROID => Ok(Width::Fixed(1)),
            INT2OID => Ok(Width::Fixed(2)),
            INT4OID | DATEOID => Ok(Width::Fixed(4)),
            INT8OID | TIMESTAMPOID | TIMESTAMPTZOID | FLOAT4OID | FLOAT8OID => {
                Ok(Width::Fixed(8))
            }
            OIDOID | REGPROCOID | REGPROCEDUREOID | REGOPEROID | REGOPERATOROID
            | REGCLASSOID | REGTYPEOID | REGCOLLATIONOID | REGCONFIGOID
            | REGDICTIONARYOID | REGNAMESPACEOID | REGROLEOID => Ok(Width::Fixed(4)),
            UUIDOID => Ok(Width::Fixed(16)),
            INT2VECTOROID | OIDVECTOROID => Ok(Width::Vector),
            NAMEOID | CSTRINGOID | TEXTOID | VARCHAROID | BPCHAROID => Ok(Width::Str),
            other => Err(refuse(format!("objkv indexes cannot order type with OID {other}"))),
        })
        .collect()
}

/// The encoded tuple an entry key carries, un-hexed. The key is the index
/// prefix, the tuple in hex, then for a non-unique index a slash and row id.
fn tuple_of(key: &[u8], prefix_len: usize) -> Option<Vec<u8>> {
    let rest = key.get(prefix_len..)?;
    let hex = match rest.iter().position(|&b| b == b'/') {
        Some(i) => &rest[..i],
        None => rest,
    };
    index_key::unhex(hex)
}

/// Where a scan starts and stops. Half-open with no inclusive flag: "greater
/// than v" moves the bound past every key carrying v, "up to and including v"
/// moves it the other way. `0xff` is above every byte a key can hold.
struct Bounds {
    lo: Vec<u8>,
    hi: Vec<u8>,
}

impl Bounds {
    /// Every key under `prefix`.
    fn under(prefix: &[u8]) -> Bounds {
        let mut hi = prefix.to_vec();
        hi.push(0xff);
        Bounds { lo: prefix.to_vec(), hi }
    }

    fn tighten(&mut self, bound: Vec<u8>, strategy: u16) {
        let mut past = bound.clone();
        past.push(0xff);
        match strategy {
            BT_GREATER_EQUAL => self.lo = self.lo.clone().max(bound),
            BT_GREATER => self.lo = self.lo.clone().max(past),
            BT_LESS => self.hi = self.hi.clone().min(bound),
            BT_LESS_EQUAL => self.hi = self.hi.clone().min(past),
            _ => {}
        }
    }
}

/// Where the scan reads, and which probes it still has to check per key.
struct ScanPlan {
    bounds: Bounds,
    /// Indexes into the probes the bounds do not fully express.
    residual: Vec<usize>,
}

fn opt_at(opts: &[ColOpt], col: usize) -> ColOpt {
    opts.get(col).copied().unwrap_or_default()
}

/// Turns the probes into a key range and a per-key check. Pure: what it
/// decides can be checked without an index in hand.
fn plan_scan(index_prefix: Vec<u8>, opts: &[ColOpt], probes: &[Probe], nkeys: usize) -> ScanPlan {
    // Leading equalities become the seek prefix. A column with no single-value
    // equality stops it: everything past that point is answered by where the
    // scan starts and stops, or by reading the value back out of the key. A
    // list of values is not one place to seek to, so it stops the prefix too
    // and is checked against the key instead.
    let mut prefix = index_prefix;
    let mut in_prefix = vec![false; probes.len()];
    let mut eq = 0;
    while eq < nkeys {
        let Some(i) = probes
            .iter()
            .position(|p| p.col == eq && p.strategy == BT_EQUAL && p.values.len() == 1)
        else {
            break;
        };
        index_key::hex_into(&probes[i].values[0], &mut prefix);
        in_prefix[i] = true;
        eq += 1;
    }

    let mut bounds = Bounds::under(&prefix);
    // A range on the bounded column is over values, and the column's NULLs
    // sit at one end of its key range or the other. `x > 40` bounded below
    // only would read up through them; keep the window on the value side of
    // the null tag. A null test is a comparison against the tag itself and
    // says which side it wants.
    if probes.iter().any(|p| p.col == eq && is_range(p.strategy) && !p.nulltest) {
        let mut tag = prefix.clone();
        if opt_at(opts, eq).nulls_first {
            index_key::hex_into(&[index_key::TAG_NULL_FIRST], &mut tag);
            bounds.tighten(tag, BT_GREATER);
        } else {
            index_key::hex_into(&[index_key::TAG_NULL], &mut tag);
            bounds.tighten(tag, BT_LESS);
        }
    }
    for p in probes.iter().filter(|p| p.col == eq) {
        if is_range(p.strategy) {
            // A list bounds the scan at its loosest value: `x < ANY(...)` is
            // `x < the largest` and `x > ANY(...)` is `x > the smallest`. The
            // planner refuses ALL (`match_saopclause_to_indexcol`), so ANY is
            // the only reading. The values are sorted, and the encoding orders
            // bytewise, so the ends of the list are those two values.
            let strategy = oriented(p, opt_at(opts, eq));
            let pick = match strategy {
                BT_LESS | BT_LESS_EQUAL => p.values.last(),
                _ => p.values.first(),
            };
            if let Some(v) = pick {
                let mut bound = prefix.clone();
                index_key::hex_into(v, &mut bound);
                bounds.tighten(bound, strategy);
            }
        }
        // A list on the first unbounded column: read from its lowest value to
        // its highest and check membership on the way through. Fewer seeks
        // than one per value, and the scan stays in key order.
        if p.strategy == BT_EQUAL && p.values.len() > 1 {
            for (v, strategy) in [(p.values.first(), BT_GREATER_EQUAL), (p.values.last(), BT_LESS_EQUAL)] {
                let mut bound = prefix.clone();
                index_key::hex_into(v.expect("non-empty"), &mut bound);
                bounds.tighten(bound, strategy);
            }
        }
    }

    // Everything the prefix and the bounds do not express exactly is checked
    // against the key: a second condition on a prefix column (`id = 5 AND id
    // > 7`, `id = 5 AND id = 6`), a list, anything on a later column. A
    // single-value range on the bounded column is the bound itself and needs
    // no second look. Nothing is dropped -- a dropped condition is a wrong
    // row with no error, and no recheck above catches it.
    let residual = (0..probes.len())
        .filter(|&i| {
            let p = &probes[i];
            !in_prefix[i] && !(p.col == eq && is_range(p.strategy) && p.values.len() == 1)
        })
        .collect();
    ScanPlan { bounds, residual }
}

/// Whether an entry's tuple satisfies every residual probe.
fn admits(
    tuple: &[u8],
    widths: &[index_key::Width],
    probes: &[Probe],
    residual: &[usize],
    opts: &[ColOpt],
) -> bool {
    let Some(spans) = index_key::column_spans(tuple, widths) else {
        return false;
    };
    residual.iter().all(|&i| {
        let p = &probes[i];
        spans.get(p.col).is_some_and(|&(a, b)| {
            // A NULL compares as neither above nor below a value, whatever
            // its tag's byte says. Only a null test can admit it.
            if !p.nulltest && index_key::is_null_tag(tuple[a]) {
                return false;
            }
            let strategy = oriented(p, opt_at(opts, p.col));
            p.values.iter().any(|want| passes(&tuple[a..b], want, strategy))
        })
    })
}

/// How many entries a scan reads at a time. A range can cover the whole index
/// while the query wants ten rows from it; the size trades round trips against
/// wasted reading.
const WINDOW: usize = 512;

/// A scan that has nothing to return and nowhere left to read.
fn finish_empty(scan: &mut ::objkv::index::ScanState) {
    scan.rows.clear();
    scan.keys.clear();
    scan.last = None;
    scan.pos = 0;
    scan.resume = None;
    scan.started = true;
}

/// Fills the scan's next window of rowids; the walk afterwards is pure memory.
pub fn load_scan(
    mcx: Mcx<'_>,
    index: &Relation<'_>,
    scan: &mut ::objkv::index::ScanState,
    conds: &[Cond],
    snapshot: u64,
) -> PgResult<()> {
    let nkeys = index.rd_index.as_ref().map_or(0, |i| i.indnkeyatts as usize);
    let scope = objkv_am::scope(index);
    let unique = is_unique(index);

    // A comparison against NULL is unknown, never true -- `x = ANY(NULL)`, or
    // a runtime key whose parameter turned out to be NULL. nbtree reaches the
    // same verdict in _bt_preprocess_keys and never descends the tree; without
    // this the NULL would encode to its sort byte and the scan would seek to
    // it, answering `x = NULL` with the rows where x IS NULL. A null *test*
    // carries the flag too, and means the opposite, so it is excluded here.
    if conds.iter().any(|c| c.isnull && !c.nulltest) {
        finish_empty(scan);
        return Ok(());
    }

    let opts = opts_of(index);
    let mut probes = Vec::with_capacity(conds.len());
    for c in conds {
        if c.col >= nkeys {
            return Err(refuse(format!(
                "objkv indexes: a condition on column {} of a {nkeys}-column index",
                c.col + 1
            )));
        }
        let col_type = index.rd_att.attr(c.col).atttypid;
        // Read the value as the operator's type, then check it can be brought
        // to the column's encoding -- a text bound on a name column is fine,
        // one on an integer column is not, and would silently mis-order.
        let read_as = if c.subtype != 0 { c.subtype } else { col_type };
        if !same_encoding(col_type, read_as) {
            return Err(refuse(format!(
                "objkv indexes: comparing a column of type {col_type} against a value of type {read_as}"
            )));
        }
        let (strategy, vals) = if c.nulltest || c.isnull {
            (c.strategy, vec![Owned::Null])
        } else {
            let mut vals = Vec::with_capacity(c.values.len());
            for v in &c.values {
                vals.push(owned_col(mcx, read_as, *v, false)?);
            }
            match refit(c.strategy, vals, col_type, read_as) {
                Some(fitted) => fitted,
                // No row can satisfy it: `int2_col = 100000`.
                None => {
                    finish_empty(scan);
                    return Ok(());
                }
            }
        };
        // An empty list -- `x = ANY('{}')` -- matches nothing.
        if vals.is_empty() {
            finish_empty(scan);
            return Ok(());
        }
        let opt = opt_at(&opts, c.col);
        let mut values: Vec<Vec<u8>> =
            vals.iter().map(|o| index_key::encode_with(&[o.as_col()], &[opt])).collect();
        values.sort();
        values.dedup();
        probes.push(Probe { col: c.col, strategy, nulltest: c.nulltest, values });
    }
    let ScanPlan { mut bounds, residual } =
        plan_scan(index_key::index_prefix(scope, index_id(index), unique), &opts, &probes, nkeys);

    // Carry on from where the last window stopped -- the bottom of the range
    // reading forwards, the top of it reading backwards.
    if let Some(from) = scan.resume.as_ref() {
        if scan.backward {
            bounds.hi = bounds.hi.clone().min(from.clone());
        } else {
            bounds.lo = bounds.lo.clone().max(from.clone());
        }
    }
    // Entries written before the last TRUNCATE describe rows that are no
    // longer in the table. The table's rows are filtered the same way; without
    // this an index scan would answer from entries alone and bring them back.
    let emptied = if objkv_am::staged_empty_mark_for(scope, index_id(index)) > 0 {
        u64::MAX
    } else {
        objkv_am::emptied_at(scope, index_id(index), snapshot)?.unwrap_or(0)
    };
    let view = objkv_am::view()?;
    let (durable, resume) = if scan.backward {
        view.scan_window_back_at(&bounds.lo, &bounds.hi, snapshot, WINDOW)
    } else {
        view.scan_window_stamped_at(&bounds.lo, &bounds.hi, snapshot, WINDOW)
    }
    .map_err(as_pg_error)?;
    scan.resume = resume;
    scan.started = true;
    let mut merged: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = durable
        .into_iter()
        .filter(|(_, _, seq)| *seq >= emptied)
        .map(|(k, v, _)| (k, v))
        .collect();
    // This transaction's own entries, which are not in the store yet. Without
    // them a statement cannot find what an earlier statement in the same
    // transaction wrote -- and once the catalogs are objkv rows, that is
    // CREATE TABLE failing to find the table it just created.
    if !objkv_am::reading_the_past() {
        let (slo, shi) = match (&scan.resume, scan.backward) {
            (Some(r), false) => (bounds.lo.clone(), r.clone()),
            (Some(r), true) => (r.clone(), bounds.hi.clone()),
            (None, _) => (bounds.lo.clone(), bounds.hi.clone()),
        };
        let since = objkv_am::staged_empty_mark_for(scope, index_id(index));
        for (k, (ord, op)) in objkv_am::staged_range(&slo, &shi) {
            if ord <= since {
                merged.remove(&k);
                continue;
            }
            match op {
                Op::Put(v) => {
                    merged.insert(k, v);
                }
                Op::Delete => {
                    merged.remove(&k);
                }
            }
        }
    }

    let candidate_count = merged.len();
    // The columns come out of the key for two reasons -- checking a condition
    // the bounds could not express, and handing values back to a query that
    // wants nothing else. Once either is needed the work is the same.
    let need_tuple = !residual.is_empty() || scan.want_keys;
    let w = if need_tuple { widths(index, nkeys)? } else { Vec::new() };
    let plen = index_key::index_prefix(scope, index_id(index), unique).len();
    let mut rows = Vec::with_capacity(merged.len());
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for (key, payload) in merged.iter() {
        let decoded = if need_tuple { tuple_of(key, plen) } else { None };
        if need_tuple && decoded.is_none() {
            continue;
        }
        if !residual.is_empty()
            && !admits(decoded.as_deref().expect("checked above"), &w, &probes, &residual, &opts)
        {
            continue;
        }
        if let Some(id) = index_key::rowid_of(key, payload) {
            rows.push(id);
            if scan.want_keys {
                keys.push(decoded.unwrap_or_default());
            }
        }
    }

    if std::env::var_os("PGRUST_OBJKV_TRACE").is_some() {
        eprintln!(
            "OBJKVTRACE index_scan index={} scope={scope} lo={} hi={} candidates={} kept={}",
            index_id(index),
            String::from_utf8_lossy(&bounds.lo),
            String::from_utf8_lossy(&bounds.hi),
            candidate_count,
            rows.len()
        );
    }
    scan.rows = rows;
    scan.keys = keys;
    scan.last = None;
    scan.pos = 0;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use index_key::{encode_with, entry_key_with, index_prefix, Width};

    fn probe(col: usize, strategy: u16, values: &[Col<'_>]) -> Probe {
        let mut values: Vec<Vec<u8>> = values.iter().map(|c| encode_with(&[*c], &[])).collect();
        values.sort();
        values.dedup();
        Probe { col, strategy, nulltest: false, values }
    }

    fn within(bounds: &Bounds, key: &[u8]) -> bool {
        bounds.lo.as_slice() <= key && key < bounds.hi.as_slice()
    }

    #[test]
    fn a_second_condition_on_a_prefix_column_is_checked_not_dropped() {
        // `WHERE id = 5 AND id > 7`: the equality becomes the seek prefix, so
        // the bounds admit the id = 5 entry. The contradiction is only seen
        // if the range is applied to the key -- Postgres answers no rows.
        let opts = [ColOpt::default()];
        let probes = [probe(0, BT_EQUAL, &[Col::Int4(5)]), probe(0, BT_GREATER, &[Col::Int4(7)])];
        let plan = plan_scan(index_prefix(1, 2, false), &opts, &probes, 1);
        let five = entry_key_with(1, 2, &[Col::Int4(5)], &opts, 9, false).unwrap();
        assert!(within(&plan.bounds, &five), "the prefix seeks to id = 5");
        assert_eq!(plan.residual, vec![1], "the range is applied per key, not dropped");
        let tuple = encode_with(&[Col::Int4(5)], &opts);
        assert!(!admits(&tuple, &[Width::Fixed(4)], &probes, &plan.residual, &opts), "5 > 7 is false");

        // The same shape with a satisfiable second condition still admits.
        let probes = [probe(0, BT_EQUAL, &[Col::Int4(5)]), probe(0, BT_GREATER, &[Col::Int4(3)])];
        let plan = plan_scan(index_prefix(1, 2, false), &opts, &probes, 1);
        assert_eq!(plan.residual, vec![1]);
        assert!(admits(&tuple, &[Width::Fixed(4)], &probes, &plan.residual, &opts));

        // Two equalities: `id = 5 AND id = 6`.
        let probes = [probe(0, BT_EQUAL, &[Col::Int4(5)]), probe(0, BT_EQUAL, &[Col::Int4(6)])];
        let plan = plan_scan(index_prefix(1, 2, false), &opts, &probes, 1);
        assert_eq!(plan.residual, vec![1]);
        assert!(!admits(&tuple, &[Width::Fixed(4)], &probes, &plan.residual, &opts));

        // An equality and a list that excludes it: `tag = 'a' AND tag IN ('b', 'c')`,
        // with the list first in the scan keys.
        let probes = [
            probe(0, BT_EQUAL, &[Col::Text(b"b"), Col::Text(b"c")]),
            probe(0, BT_EQUAL, &[Col::Text(b"a")]),
        ];
        let plan = plan_scan(index_prefix(1, 2, false), &opts, &probes, 1);
        assert_eq!(plan.residual, vec![0], "the single value is the prefix; the list is checked");
        let a = encode_with(&[Col::Text(b"a")], &opts);
        assert!(!admits(&a, &[Width::Str], &probes, &plan.residual, &opts));
    }

    #[test]
    fn a_single_range_on_the_bounded_column_is_the_bound_itself() {
        // `id = 5 AND n > 7` on (id, n): the range is fully expressed by the
        // bounds and needs no per-key check, while a range on a later column
        // does.
        let opts = [ColOpt::default(), ColOpt::default(), ColOpt::default()];
        let probes = [
            probe(0, BT_EQUAL, &[Col::Int4(5)]),
            probe(1, BT_GREATER, &[Col::Int4(7)]),
            probe(2, BT_LESS, &[Col::Int4(1)]),
        ];
        let plan = plan_scan(index_prefix(1, 2, false), &opts, &probes, 3);
        assert_eq!(plan.residual, vec![2]);
        let seven = entry_key_with(1, 2, &[Col::Int4(5), Col::Int4(7), Col::Int4(0)], &opts, 1, false).unwrap();
        let eight = entry_key_with(1, 2, &[Col::Int4(5), Col::Int4(8), Col::Int4(0)], &opts, 1, false).unwrap();
        assert!(!within(&plan.bounds, &seven) && within(&plan.bounds, &eight));
    }

    #[test]
    fn integer_bounds_of_another_width_are_refitted_to_the_column() {
        // `int2_col = 5` hands over an int4; it must encode as the column does.
        assert_eq!(
            refit(BT_EQUAL, vec![Owned::Int4(5)], INT2OID, INT4OID),
            Some((BT_EQUAL, vec![Owned::Int2(5)]))
        );
        assert_eq!(
            refit(BT_EQUAL, vec![Owned::Int4(42)], INT8OID, INT4OID),
            Some((BT_EQUAL, vec![Owned::Int8(42)]))
        );
        assert_eq!(
            refit(BT_LESS, vec![Owned::Int2(-3)], INT8OID, INT2OID),
            Some((BT_LESS, vec![Owned::Int8(-3)]))
        );
        // Same width, or not integers at all: untouched.
        assert_eq!(
            refit(BT_EQUAL, vec![Owned::Int4(5)], INT4OID, INT4OID),
            Some((BT_EQUAL, vec![Owned::Int4(5)]))
        );
        assert_eq!(
            refit(BT_EQUAL, vec![Owned::Text(b"x".to_vec())], NAMEOID, TEXTOID),
            Some((BT_EQUAL, vec![Owned::Text(b"x".to_vec())]))
        );
    }

    #[test]
    fn an_out_of_range_literal_still_has_an_answer() {
        // `int2_col = 100000` matches nothing.
        assert_eq!(refit(BT_EQUAL, vec![Owned::Int4(100_000)], INT2OID, INT4OID), None);
        // `int2_col < 100000` matches every value: it is `<= 32767`.
        assert_eq!(
            refit(BT_LESS, vec![Owned::Int4(100_000)], INT2OID, INT4OID),
            Some((BT_LESS_EQUAL, vec![Owned::Int2(i16::MAX)]))
        );
        assert_eq!(
            refit(BT_LESS_EQUAL, vec![Owned::Int8(1 << 40)], INT4OID, INT8OID),
            Some((BT_LESS_EQUAL, vec![Owned::Int4(i32::MAX)]))
        );
        // `int2_col > 100000` matches nothing; `int2_col > -100000` everything.
        assert_eq!(refit(BT_GREATER, vec![Owned::Int4(100_000)], INT2OID, INT4OID), None);
        assert_eq!(refit(BT_GREATER_EQUAL, vec![Owned::Int4(100_000)], INT2OID, INT4OID), None);
        assert_eq!(
            refit(BT_GREATER, vec![Owned::Int4(-100_000)], INT2OID, INT4OID),
            Some((BT_GREATER_EQUAL, vec![Owned::Int2(i16::MIN)]))
        );
        assert_eq!(refit(BT_LESS, vec![Owned::Int4(-100_000)], INT2OID, INT4OID), None);
        // The edges themselves fit.
        assert_eq!(
            refit(BT_EQUAL, vec![Owned::Int8(i32::MAX as i64)], INT4OID, INT8OID),
            Some((BT_EQUAL, vec![Owned::Int4(i32::MAX)]))
        );
        assert_eq!(refit(BT_EQUAL, vec![Owned::Int8(i32::MAX as i64 + 1)], INT4OID, INT8OID), None);
    }

    #[test]
    fn a_list_keeps_what_fits_and_loosens_to_everything_when_one_does_not() {
        // `int2_col IN (1, 100000)` is `int2_col = 1`.
        assert_eq!(
            refit(BT_EQUAL, vec![Owned::Int4(1), Owned::Int4(100_000)], INT2OID, INT4OID),
            Some((BT_EQUAL, vec![Owned::Int2(1)]))
        );
        // `int2_col < ANY(3, 100000)` is `int2_col < 100000`, which is every value.
        assert_eq!(
            refit(BT_LESS, vec![Owned::Int4(3), Owned::Int4(100_000)], INT2OID, INT4OID),
            Some((BT_LESS_EQUAL, vec![Owned::Int2(i16::MAX)]))
        );
        // `int2_col > ANY(3, 100000)` is `int2_col > 3`; the impossible one drops.
        assert_eq!(
            refit(BT_GREATER, vec![Owned::Int4(3), Owned::Int4(100_000)], INT2OID, INT4OID),
            Some((BT_GREATER, vec![Owned::Int2(3)]))
        );
    }

    #[test]
    fn a_refitted_bound_encodes_at_the_columns_width() {
        // The point of refitting: the probe must compare bytewise against a
        // two-byte column, and an int4 encoding never could.
        let (_, vals) = refit(BT_EQUAL, vec![Owned::Int4(5)], INT2OID, INT4OID).unwrap();
        let probe = encode_with(&[vals[0].as_col()], &[]);
        assert_eq!(probe, encode_with(&[Col::Int2(5)], &[]));
        assert_eq!(probe.len(), 3);
    }
}

/// Stages a delete for every index entry describing `rowid` as it stands now.
///
/// Nothing else removes one: Postgres leaves that to vacuum, which reads pages
/// objkv does not have, so an entry outlived the value it described and every
/// scan fetched each candidate row to find out. That fetch was most of what an
/// index scan cost. Must run before the row change is staged, since the keys
/// are read off the row as it is now.
pub fn retire_entries(mcx: Mcx<'_>, heap: &Relation<'_>, rowid: u64) -> PgResult<()> {
    if !heap.rd_rel.relhasindex {
        return Ok(());
    }
    let scope = objkv_am::scope(heap);
    let indexes = ::relcache_seams::relation_get_index_list::call(mcx, heap.rd_id)?;
    for &oid in indexes.iter() {
        // relation_open, not table_open: the latter refuses anything that is
        // not a table, and these are indexes.
        let index = ::relation_seams::relation_open::call(
            mcx,
            oid,
            ::types_storage::lock::AccessShareLock,
        )?;
        let ours = ::tableam_vocab::is_objkv_index_am_oid(index.rd_rel.relam);
        // A row a partial index does not cover has no entry, and
        // row_entry_key says so by returning None.
        // Not `?`: `row_entry_key` fails on a refused collation or type, on a
        // malformed name, and on any object-store error, and returning through
        // it would leave the relcache reference held -- which assertion builds
        // report even though the abort releases it.
        let key = match if ours { row_entry_key(mcx, &index, heap, scope, rowid) } else { Ok(None) }
        {
            Ok(key) => key,
            Err(e) => {
                let _ = index.close(::types_storage::lock::AccessShareLock);
                return Err(e);
            }
        };
        index.close(::types_storage::lock::AccessShareLock)?;
        if let Some(key) = key {
            objkv_am::stage(key, Op::Delete);
        }
    }
    Ok(())
}

/// Tells the collector which table this index belongs to. It cannot look the
/// fact up while holding the storage lock a catalog read would take, so it is
/// recorded wherever an entry is written -- writing entries is what creates
/// the garbage. Recording it on the scan path alone missed the case that
/// matters: an UPDATE with no WHERE writes entries and never reads the index.
fn note_table_of(index: &Relation<'_>) {
    if let Some(i) = index.rd_index.as_ref() {
        objkv_am::note_index_table(index.rd_id, i.indrelid);
    }
}
