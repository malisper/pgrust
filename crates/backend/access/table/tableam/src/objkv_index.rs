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
const DEFAULT_COLLATION_OID: Oid = 100;

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
        TEXTOID | VARCHAROID | BPCHAROID => Owned::Text(varlena_bytes(mcx, d)?),
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
/// can be turned back into the value exactly. The vector types are the ones
/// that cannot yet; INCLUDE columns are not in the key at all.
pub fn returnable(index: &Relation<'_>, attno: i32) -> bool {
    let nkeys = index.rd_index.as_ref().map_or(0, |i| i.indnkeyatts as i32);
    if attno < 1 || attno > nkeys {
        return false;
    }
    !matches!(
        index.rd_att.attr(attno as usize - 1).atttypid,
        INT2VECTOROID | OIDVECTOROID
    )
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
        if ::objkv::index_key::is_null_tag(tuple[a]) {
            values.push(Datum::null());
            isnull.push(true);
            continue;
        }
        let body = ::objkv::index_key::column_body(tuple[a], &tuple[a + 1..b]);
        values.push(datum_from_key(mcx, index.rd_att.attr(i).atttypid, &body)?);
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
        TEXTOID | VARCHAROID | BPCHAROID => {
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

/// A comparison against a column that is stored descending reads the other
/// way in key order. A null test is against the tag, which is not inverted;
/// only where the tag sits changes, and only "everything that is not it"
/// cares which side that is.
fn oriented(c: &Cond, opt: ColOpt) -> u16 {
    if c.nulltest {
        return if opt.nulls_first && c.strategy == BT_LESS { BT_GREATER } else { c.strategy };
    }
    if !opt.desc {
        return c.strategy;
    }
    match c.strategy {
        BT_LESS => BT_GREATER,
        BT_LESS_EQUAL => BT_GREATER_EQUAL,
        BT_GREATER_EQUAL => BT_LESS_EQUAL,
        BT_GREATER => BT_LESS,
        other => other,
    }
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

fn row_columns(
    mcx: Mcx<'_>,
    index: &Relation<'_>,
    heap: &Relation<'_>,
    scope: u32,
    rowid: u64,
) -> PgResult<Option<Vec<Owned>>> {
    let Some(image) = objkv_am::fetch_row(scope, objkv_am::relid(heap), rowid, ::objkv::key::LATEST)?
    else {
        return Ok(None);
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

    let mut out = Vec::with_capacity(nkeys);
    for i in 0..nkeys {
        let attno = ind.indkey[i];
        if attno <= 0 {
            return Ok(None);
        }
        let mut isnull = false;
        let d = ::exectuples::slot_getattr(&mut slot, attno as i32, &mut isnull);
        let typid = index.rd_att.attr(i).atttypid;
        // Owned, and borrowed by the caller. Box::leak's 'static was literal:
        // one leak per column per candidate, on every catalog scan.
        out.push(owned_col(mcx, typid, d, isnull)?);
    }
    Ok(Some(out))
}

pub fn lookup(
    index: &Relation<'_>,
    cols: &[Col<'_>],
    snapshot: u64,
) -> PgResult<Vec<u64>> {
    let scope = objkv_am::scope(index);
    let prefix = index_key::seek_prefix_with(scope, index_id(index), cols, &opts_of(index), is_unique(index))
        .map_err(as_pg_error)?;
    // Entries from before the last TRUNCATE describe rows that are gone. This
    // is the duplicate-key check too, so without the filter an emptied table
    // refuses to take back a value it no longer holds.
    let emptied = if objkv_am::staged_empty_mark_for(scope, index_id(index)) > 0 {
        u64::MAX
    } else {
        objkv_am::emptied_at(scope, index_id(index), snapshot)?.unwrap_or(0)
    };
    let mut hi = prefix.clone();
    hi.push(0xff);
    let durable = objkv_am::view()?
        .scan_window_stamped_at(&prefix, &hi, snapshot, usize::MAX)
        .map_err(as_pg_error)?
        .0;
    let mut merged: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = durable
        .into_iter()
        .filter(|(_, _, seq)| *seq >= emptied)
        .map(|(k, v, _)| (k, v))
        .collect();
    // Our own entries, not in the store yet: without them a statement cannot
    // find what an earlier one in the same transaction wrote, which is CREATE
    // TABLE failing to find the table it just made. Present-tense reads only --
    // uncommitted writes have no sequence number and belong to now.
    if !objkv_am::reading_the_past() {
        let since = objkv_am::staged_empty_mark_for(scope, index_id(index));
        for (k, (ord, op)) in objkv_am::staged_prefix(&prefix) {
            if ord <= since {
                merged.remove(&k);
                continue;
            }
            match op {
                ::objkv::commit::Op::Put(v) => {
                    merged.insert(k, v);
                }
                ::objkv::commit::Op::Delete => {
                    merged.remove(&k);
                }
            }
        }
    }
    Ok(merged
        .iter()
        .filter_map(|(k, v)| ::objkv::index_key::rowid_of(k, v))
        .collect())
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

/// Whether two types encode to the same bytes, so a value of one can be
/// compared against a column of the other.
///
/// The string types differ only in how they are stored, and the oid aliases
/// are an oid with another input function. Everything else must match: an
/// eight-byte bound against a four-byte column would sort wrongly in silence.
fn same_encoding(a: Oid, b: Oid) -> bool {
    const STRINGS: &[Oid] = &[NAMEOID, CSTRINGOID, TEXTOID, VARCHAROID, BPCHAROID];
    const FLOATS: &[Oid] = &[FLOAT4OID, FLOAT8OID];
    const OIDS: &[Oid] = &[
        OIDOID, REGPROCOID, REGPROCEDUREOID, REGOPEROID, REGOPERATOROID, REGCLASSOID,
        REGTYPEOID, REGCOLLATIONOID, REGCONFIGOID, REGDICTIONARYOID, REGNAMESPACEOID,
        REGROLEOID,
    ];
    a == b
        || [STRINGS, OIDS, FLOATS]
            .iter()
            .any(|set| set.contains(&a) && set.contains(&b))
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
    if hex.len() % 2 != 0 {
        return None;
    }
    let digit = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
    hex.chunks(2)
        .map(|p| Some(digit(p[0])? << 4 | digit(p[1])?))
        .collect()
}

/// Where a scan starts and stops. Half-open with no inclusive flag: "greater
/// than v" moves the bound past every key carrying v, "up to and including v"
/// moves it the other way. `0xff` is above every byte a key can hold.
struct Bounds {
    lo: Vec<u8>,
    hi: Vec<u8>,
}

impl Bounds {
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

/// How many entries a scan reads at a time. A range can cover the whole index
/// while the query wants ten rows from it; the size trades round trips against
/// wasted reading.
const WINDOW: usize = 512;

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
        scan.rows.clear();
        scan.keys.clear();
        scan.last = None;
        scan.pos = 0;
        scan.resume = None;
        scan.started = true;
        return Ok(());
    }

    let mut owned = Vec::with_capacity(conds.len());
    for c in conds {
        if c.col >= nkeys {
            return Err(refuse(format!(
                "objkv indexes: a condition on column {} of a {nkeys}-column index",
                c.col + 1
            )));
        }
        let col_type = index.rd_att.attr(c.col).atttypid;
        // Read the value as the operator's type, then check it encodes the
        // same way the column does -- a text bound on a name column is fine,
        // one on an integer column is not, and would silently mis-order.
        let read_as = if c.subtype != 0 { c.subtype } else { col_type };
        if !same_encoding(col_type, read_as) {
            return Err(refuse(format!(
                "objkv indexes: comparing a column of type {col_type} against a value of type {read_as}"
            )));
        }
        let mut vals = Vec::with_capacity(c.values.len().max(1));
        if c.isnull {
            vals.push(owned_col(mcx, read_as, Datum::null(), true)?);
        } else {
            for v in &c.values {
                vals.push(owned_col(mcx, read_as, *v, false)?);
            }
        }
        owned.push(vals);
    }
    // Each condition's values, encoded and in order, so a membership test on a
    // key is a run of byte comparisons.
    // Encoded as the column is stored, so a descending column's list comes
    // out in key order, which is the reverse of value order; the strategies
    // are turned around to match, in `oriented`.
    let opts = opts_of(index);
    let opt = |col: usize| opts.get(col).copied().unwrap_or_default();
    let encoded: Vec<Vec<Vec<u8>>> = owned
        .iter()
        .zip(conds)
        .map(|(vs, c)| {
            let mut e: Vec<Vec<u8>> = vs
                .iter()
                .map(|o| ::objkv::index_key::encode_with(&[o.as_col()], &[opt(c.col)]))
                .collect();
            e.sort();
            e.dedup();
            e
        })
        .collect();

    // Leading equalities become the seek prefix. A column with no equality
    // stops it: everything past that point is answered by where the scan
    // starts and stops, or by reading the value back out of the key.
    let mut eq_cols: Vec<Col> = Vec::new();
    for col in 0..nkeys {
        // A list of values is not one place to seek to, so the prefix stops
        // here and the list is checked against the key instead.
        match conds
            .iter()
            .enumerate()
            .find(|(i, c)| c.col == col && c.strategy == BT_EQUAL && owned[*i].len() == 1)
            .map(|(i, _)| i)
        {
            Some(i) => eq_cols.push(owned[i][0].as_col()),
            None => break,
        }
    }
    let eq = eq_cols.len();
    let prefix = index_key::seek_prefix_with(scope, index_id(index), &eq_cols, &opts, unique)
        .map_err(as_pg_error)?;

    let mut bounds = Bounds { lo: prefix.clone(), hi: { let mut h = prefix.clone(); h.push(0xff); h } };
    // A range on the bounded column is over values, and the column's NULLs
    // sit at one end of its key range or the other. `x > 40` bounded below
    // only would read up through them; keep the window on the value side of
    // the null tag. A null test is a comparison against the tag itself and
    // says which side it wants.
    if conds.iter().any(|c| c.col == eq && is_range(c.strategy) && !c.nulltest && !c.isnull) {
        let mut tag = prefix.clone();
        if opt(eq).nulls_first {
            hex_onto(&[::objkv::index_key::TAG_NULL_FIRST], &mut tag);
            bounds.tighten(tag, BT_GREATER);
        } else {
            hex_onto(&[::objkv::index_key::TAG_NULL], &mut tag);
            bounds.tighten(tag, BT_LESS);
        }
    }
    for (i, c) in conds.iter().enumerate() {
        // A NULL is not ordered against anything, so a range against one
        // matches no row -- and must not be turned into a byte bound.
        if c.col == eq && is_range(c.strategy) {
            // A list bounds the scan at its loosest value: `x < ANY(...)` is
            // `x < the largest` and `x > ANY(...)` is `x > the smallest`. The
            // planner refuses ALL (`match_saopclause_to_indexcol`), so ANY is
            // the only reading. `encoded` is sorted, and the encoding orders
            // bytewise, so the ends of the list are those two values. The
            // condition stays in `residual` as well: the bound narrows the
            // read, the per-key check is what decides.
            let strategy = oriented(c, opt(c.col));
            let pick = match strategy {
                BT_LESS | BT_LESS_EQUAL => encoded[i].last(),
                _ => encoded[i].first(),
            };
            if let Some(bound_val) = pick {
                let mut bound = prefix.clone();
                hex_onto(bound_val, &mut bound);
                bounds.tighten(bound, strategy);
            }
        }
        // A list on the first unbounded column: read from its lowest value to
        // its highest and check membership on the way through. Fewer seeks
        // than one per value, and the scan stays in key order.
        if c.col == eq && c.strategy == BT_EQUAL && encoded[i].len() > 1 {
            for (v, strategy) in [
                (encoded[i].first(), BT_GREATER_EQUAL),
                (encoded[i].last(), BT_LESS_EQUAL),
            ] {
                let mut bound = prefix.clone();
                hex_onto(v.expect("non-empty"), &mut bound);
                bounds.tighten(bound, strategy);
            }
        }
    }

    // Conditions the bounds cannot express: on a column past the one the scan
    // is bounded by, or a range against NULL. Checked against the key itself.
    let residual: Vec<usize> = conds
        .iter()
        .enumerate()
        .filter(|(i, c)| {
            // A single-value range is exactly the bound above. A list of them
            // is not: the bound covers the loosest value, and only a check per
            // key can tell which entries the rest of the list admits.
            c.col > eq
                || (c.col == eq && (!is_range(c.strategy) || encoded[*i].len() > 1))
        })
        .map(|(i, _)| i)
        .collect();

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
    let plen = prefix_len(scope, index_id(index), unique);
    let mut rows = Vec::with_capacity(merged.len());
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for (key, payload) in merged.iter() {
        let decoded = if need_tuple { tuple_of(key, plen) } else { None };
        if need_tuple && decoded.is_none() {
            continue;
        }
        if !residual.is_empty() {
            let tuple = decoded.as_deref().expect("checked above");
            let Some(spans) = ::objkv::index_key::column_spans(tuple, &w) else {
                continue;
            };
            if !residual.iter().all(|&i| {
                let c = &conds[i];
                spans.get(c.col).is_some_and(|&(a, b)| {
                    // A NULL compares as neither above nor below a value,
                    // whatever its tag's byte says. Only a null test can
                    // admit it.
                    if !c.nulltest && ::objkv::index_key::is_null_tag(tuple[a]) {
                        return false;
                    }
                    let strategy = oriented(c, opt(c.col));
                    encoded[i].iter().any(|want| passes(&tuple[a..b], want, strategy))
                })
            }) {
                continue;
            }
        }
        if let Some(id) = ::objkv::index_key::rowid_of(key, payload) {
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
            String::from_utf8_lossy(&bounds.hi[..bounds.hi.len() - 1]),
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

fn prefix_len(scope: u32, index: u32, unique: bool) -> usize {
    index_key::index_prefix(scope, index, unique).len()
}

fn hex_onto(bytes: &[u8], out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize]);
        out.push(HEX[(b & 0xf) as usize]);
    }
}

fn heap_of(index: &Relation<'_>) -> PgResult<Oid> {
    index
        .rd_index
        .as_ref()
        .map(|i| i.indrelid)
        .ok_or_else(|| refuse(format!("relation \"{}\" is not an index", index.name())))
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
