use super::*;
use ::arrayfuncs::construct::{construct_md_array, deconstruct_array};
use ::arrayfuncs::foundation::arr_lbound;
use ::mcx::MemoryContext;
use ::types_core::INT4OID;
use ::types_error::PgResult;
use ::types_fmgr::FunctionCallInfoBaseData as Fcinfo;

fn int4_meta() -> ElemMeta {
    ElemMeta { element_type: INT4OID, typlen: 4, typbyval: true, typalign: b'i' }
}

fn int4_arr<'m>(mcx: Mcx<'m>, elems: &[Option<i32>], lb: i32) -> PgVec<'m, u8> {
    let dv: Vec<Datum> =
        elems.iter().map(|e| Datum::from_i32(e.unwrap_or(0))).collect();
    let nulls: Vec<bool> = elems.iter().map(|e| e.is_none()).collect();
    let dims = [elems.len() as i32];
    let lbs = [lb];
    construct_md_array(mcx, &dv, Some(&nulls), 1, &dims, &lbs, INT4OID, 4, true, b'i').unwrap()
}

fn int4_arr_md<'m>(
    mcx: Mcx<'m>,
    elems: &[Option<i32>],
    dims: &[i32],
    lbs: &[i32],
) -> PgVec<'m, u8> {
    let dv: Vec<Datum> = elems.iter().map(|e| Datum::from_i32(e.unwrap_or(0))).collect();
    let nulls: Vec<bool> = elems.iter().map(|e| e.is_none()).collect();
    construct_md_array(mcx, &dv, Some(&nulls), dims.len() as i32, dims, lbs, INT4OID, 4, true, b'i')
        .unwrap()
}

fn to_int4(mcx: Mcx<'_>, image: &[u8]) -> Vec<Option<i32>> {
    let (elems, nulls) = deconstruct_array(mcx, image, 4, true, b'i', true).unwrap();
    elems
        .iter()
        .zip(nulls.iter())
        .map(|(d, &n)| if n { None } else { Some(d.as_i32()) })
        .collect()
}

fn dims_of(image: &[u8]) -> (i32, Vec<i32>, Vec<i32>) {
    let (nd, dims, lbs) = read_dims_lbounds(image);
    (nd, dims[..nd as usize].to_vec(), lbs[..nd as usize].to_vec())
}

fn fc_int4eq(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(fcinfo.arg(0).as_i32() == fcinfo.arg(1).as_i32()))
}

fn int4eq_finfo() -> FmgrInfo {
    FmgrInfo::new(fc_int4eq, 65, 2, true, false)
}

#[test]
fn append_flat() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr(mcx, &[Some(1), Some(2)], 1);
    let out = array_append_internal(mcx, &a, Datum::from_i32(3), false, &int4_meta()).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(1), Some(2), Some(3)]);
    assert_eq!(dims_of(&out), (1, vec![3], vec![1]));
}

#[test]
fn append_keeps_lower_bound_and_null_elem() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr(mcx, &[Some(7)], -2);
    let out = array_append_internal(mcx, &a, Datum::null(), true, &int4_meta()).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(7), None]);
    assert_eq!(dims_of(&out), (1, vec![2], vec![-2]));
}

#[test]
fn append_onto_empty() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = construct_empty_array(mcx, INT4OID).unwrap();
    let out = array_append_internal(mcx, &a, Datum::from_i32(9), false, &int4_meta()).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(9)]);
    assert_eq!(dims_of(&out), (1, vec![1], vec![1]));
}

#[test]
fn append_multidim_rejected() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr_md(mcx, &[Some(1), Some(2), Some(3), Some(4)], &[2, 2], &[1, 1]);
    let err = array_append_internal(mcx, &a, Datum::from_i32(5), false, &int4_meta()).unwrap_err();
    assert_eq!(err.message(), "argument must be empty or one-dimensional array");
}

#[test]
fn append_index_overflow() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    // lb+dim == i32::MAX is the largest constructible array; appending must
    // fail (in the bounds re-check, as in C's array_set_element).
    let a = int4_arr(mcx, &[Some(1)], i32::MAX - 1);
    assert!(array_append_internal(mcx, &a, Datum::from_i32(2), false, &int4_meta()).is_err());
}

#[test]
fn prepend_restores_lower_bound() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr(mcx, &[Some(1), Some(2)], 5);
    let out = array_prepend_internal(mcx, &a, Datum::from_i32(0), false, &int4_meta()).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(0), Some(1), Some(2)]);
    assert_eq!(dims_of(&out), (1, vec![3], vec![5]));
}

#[test]
fn prepend_onto_empty_and_overflow() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = construct_empty_array(mcx, INT4OID).unwrap();
    let out = array_prepend_internal(mcx, &a, Datum::from_i32(4), false, &int4_meta()).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(4)]);
    assert_eq!(dims_of(&out), (1, vec![1], vec![1]));

    let b = int4_arr(mcx, &[Some(1)], i32::MIN);
    let err = array_prepend_internal(mcx, &b, Datum::from_i32(2), false, &int4_meta()).unwrap_err();
    assert_eq!(err.message(), "integer out of range");
}

#[test]
fn cat_same_dims() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr(mcx, &[Some(1), None], 1);
    let b = int4_arr(mcx, &[Some(3)], 7);
    let out = array_cat_internal(mcx, &a, &b).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(1), None, Some(3)]);
    assert_eq!(dims_of(&out), (1, vec![3], vec![1]));
}

#[test]
fn cat_empty_identities() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let e = construct_empty_array(mcx, INT4OID).unwrap();
    let a = int4_arr(mcx, &[Some(1)], 1);
    assert_eq!(to_int4(mcx, &array_cat_internal(mcx, &e, &a).unwrap()), vec![Some(1)]);
    assert_eq!(to_int4(mcx, &array_cat_internal(mcx, &a, &e).unwrap()), vec![Some(1)]);
    let ee = array_cat_internal(mcx, &e, &e).unwrap();
    assert_eq!(dims_of(&ee).0, 0);
}

#[test]
fn cat_outer_inner() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let md = int4_arr_md(mcx, &[Some(1), Some(2), Some(3), Some(4)], &[2, 2], &[1, 1]);
    let row = int4_arr(mcx, &[Some(5), Some(6)], 1);
    let out = array_cat_internal(mcx, &md, &row).unwrap();
    assert_eq!(dims_of(&out), (2, vec![3, 2], vec![1, 1]));
    assert_eq!(
        to_int4(mcx, &out),
        vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
    );
    let out2 = array_cat_internal(mcx, &row, &md).unwrap();
    assert_eq!(dims_of(&out2), (2, vec![3, 2], vec![1, 1]));
    assert_eq!(
        to_int4(mcx, &out2),
        vec![Some(5), Some(6), Some(1), Some(2), Some(3), Some(4)]
    );
}

#[test]
fn cat_dim_mismatch_errors() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr(mcx, &[Some(1)], 1);
    let md3 = int4_arr_md(
        mcx,
        &[Some(1), Some(2), Some(3), Some(4), Some(5), Some(6), Some(7), Some(8)],
        &[2, 2, 2],
        &[1, 1, 1],
    );
    let err = array_cat_internal(mcx, &a, &md3).unwrap_err();
    assert_eq!(err.message(), "cannot concatenate incompatible arrays");
    assert_eq!(
        err.detail().unwrap(),
        "Arrays of 1 and 3 dimensions are not compatible for concatenation."
    );

    let m1 = int4_arr_md(mcx, &[Some(1), Some(2)], &[1, 2], &[1, 1]);
    let m2 = int4_arr_md(mcx, &[Some(1), Some(2), Some(3)], &[1, 3], &[1, 1]);
    let err = array_cat_internal(mcx, &m1, &m2).unwrap_err();
    assert_eq!(
        err.detail().unwrap(),
        "Arrays with differing element dimensions are not compatible for concatenation."
    );

    let row3 = int4_arr(mcx, &[Some(1), Some(2), Some(3)], 1);
    let err = array_cat_internal(mcx, &m1, &row3).unwrap_err();
    assert_eq!(
        err.detail().unwrap(),
        "Arrays with differing dimensions are not compatible for concatenation."
    );
}

#[test]
fn position_basics() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr(mcx, &[Some(10), None, Some(30), Some(10)], 1);
    let meta = int4_meta();
    let mut eq = int4eq_finfo();

    let s = |searched: Option<i32>, min: Option<i32>| PositionSearch {
        searched: searched.map(Datum::from_i32).unwrap_or(Datum::null()),
        null_search: searched.is_none(),
        collation: 0,
        position_min: min,
    };
    assert_eq!(
        array_position_internal(mcx, &a, &s(Some(30), None), &meta, &mut eq).unwrap(),
        Some(3)
    );
    assert_eq!(
        array_position_internal(mcx, &a, &s(Some(10), Some(2)), &meta, &mut eq).unwrap(),
        Some(4)
    );
    assert_eq!(array_position_internal(mcx, &a, &s(None, None), &meta, &mut eq).unwrap(), Some(2));
    assert_eq!(array_position_internal(mcx, &a, &s(Some(99), None), &meta, &mut eq).unwrap(), None);

    let lb = int4_arr(mcx, &[Some(5), Some(6)], -3);
    assert_eq!(
        array_position_internal(mcx, &lb, &s(Some(6), None), &meta, &mut eq).unwrap(),
        Some(-2)
    );
}

#[test]
fn positions_accumulates() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr(mcx, &[Some(1), Some(2), Some(1), None, Some(1)], 1);
    let meta = int4_meta();
    let mut eq = int4eq_finfo();
    let s = PositionSearch {
        searched: Datum::from_i32(1),
        null_search: false,
        collation: 0,
        position_min: None,
    };
    let out = array_positions_internal(mcx, &a, &s, &meta, &mut eq).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(1), Some(3), Some(5)]);

    let s_null = PositionSearch {
        searched: Datum::null(),
        null_search: true,
        collation: 0,
        position_min: None,
    };
    let out = array_positions_internal(mcx, &a, &s_null, &meta, &mut eq).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(4)]);

    let s_miss = PositionSearch {
        searched: Datum::from_i32(42),
        null_search: false,
        collation: 0,
        position_min: None,
    };
    let out = array_positions_internal(mcx, &a, &s_miss, &meta, &mut eq).unwrap();
    assert_eq!(dims_of(&out).0, 0);
}

#[test]
fn agg_array_accumulate() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    const INT4_ARRAY: Oid = 1007;
    let a = int4_arr(mcx, &[Some(1), Some(2)], 1);
    let b = int4_arr(mcx, &[Some(3), None], 1);
    let st = accum_array_result_arr(
        mcx,
        Some(init_array_result_arr(mcx, INT4_ARRAY, INT4OID).unwrap()),
        Some(&a),
        INT4_ARRAY,
    )
    .unwrap();
    let st = accum_array_result_arr(mcx, Some(st), Some(&b), INT4_ARRAY).unwrap();
    let out = make_array_result_arr(mcx, &st).unwrap();
    assert_eq!(dims_of(&out), (2, vec![2, 2], vec![1, 1]));
    assert_eq!(to_int4(mcx, &out), vec![Some(1), Some(2), Some(3), None]);
}

#[test]
fn agg_array_error_arms() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    const INT4_ARRAY: Oid = 1007;
    let init = || init_array_result_arr(mcx, INT4_ARRAY, INT4OID).unwrap();

    let err = accum_array_result_arr(mcx, Some(init()), None, INT4_ARRAY).err().unwrap();
    assert_eq!(err.message(), "cannot accumulate null arrays");

    let empty = construct_empty_array(mcx, INT4OID).unwrap();
    let err =
        accum_array_result_arr(mcx, Some(init()), Some(&empty), INT4_ARRAY).err().unwrap();
    assert_eq!(err.message(), "cannot accumulate empty arrays");

    let a = int4_arr(mcx, &[Some(1), Some(2)], 1);
    let c = int4_arr(mcx, &[Some(1)], 1);
    let st = accum_array_result_arr(mcx, Some(init()), Some(&a), INT4_ARRAY).unwrap();
    let err = accum_array_result_arr(mcx, Some(st), Some(&c), INT4_ARRAY).err().unwrap();
    assert_eq!(err.message(), "cannot accumulate arrays of different dimensionality");

    let empty_state = init();
    let out = make_array_result_arr(mcx, &empty_state).unwrap();
    assert_eq!(dims_of(&out).0, 0);
}

#[test]
fn agg_array_late_nulls() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    const INT4_ARRAY: Oid = 1007;
    let a = int4_arr(mcx, &[Some(1), Some(2)], 1);
    let b = int4_arr(mcx, &[None, Some(4)], 1);
    let init = init_array_result_arr(mcx, INT4_ARRAY, INT4OID).unwrap();
    let st = accum_array_result_arr(mcx, Some(init), Some(&a), INT4_ARRAY).unwrap();
    let st = accum_array_result_arr(mcx, Some(st), Some(&b), INT4_ARRAY).unwrap();
    let out = make_array_result_arr(mcx, &st).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(1), Some(2), None, Some(4)]);
}

// v_pagg_test's array_agg(ARRAY[x]) shape: the null bitmap is allocated for
// aitems = 256 (32 bytes) and item 256 flushes its last byte exactly —
// array_bitmap_copy must not touch the byte past the bitmap there.
#[test]
fn agg_array_bitmap_fills_exact_capacity() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    const INT4_ARRAY: Oid = 1007;
    let mut st = init_array_result_arr(mcx, INT4_ARRAY, INT4OID).unwrap();
    for i in 0..257 {
        let elem = if i % 4 == 0 { None } else { Some(i as i32) };
        let img = int4_arr(mcx, &[elem], 1);
        st = accum_array_result_arr(mcx, Some(st), Some(&img), INT4_ARRAY).unwrap();
    }
    let out = make_array_result_arr(mcx, &st).unwrap();
    assert_eq!(dims_of(&out), (2, vec![257, 1], vec![1, 1]));
    let vals = to_int4(mcx, &out);
    assert_eq!(vals.len(), 257);
    for (i, v) in vals.iter().enumerate() {
        assert_eq!(*v, if i % 4 == 0 { None } else { Some(i as i32) });
    }
}

#[test]
fn trim_array_slices() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr(mcx, &[Some(1), Some(2), Some(3)], 1);
    let out = trim_array_internal(mcx, &a, 1, 4, b'i').unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(1), Some(2)]);
    let out = trim_array_internal(mcx, &a, 3, 4, b'i').unwrap();
    assert_eq!(dims_of(&out).0, 0);

    let err = trim_array_internal(mcx, &a, 4, 4, b'i').unwrap_err();
    assert_eq!(err.message(), "number of elements to trim must be between 0 and 3");
    let err = trim_array_internal(mcx, &a, -1, 4, b'i').unwrap_err();
    assert_eq!(err.message(), "number of elements to trim must be between 0 and 3");
}

#[test]
fn shuffle_is_a_permutation() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    ::pg_prng::global_prng(|p| p.seed(42));
    let src: Vec<Option<i32>> = (0..17).map(Some).collect();
    let a = int4_arr(mcx, &src, 3);
    let out = array_shuffle_n(mcx, &a, 17, true, &int4_meta()).unwrap();
    let mut got = to_int4(mcx, &out);
    assert_eq!(dims_of(&out), (1, vec![17], vec![3]));
    got.sort();
    let mut want = src.clone();
    want.sort();
    assert_eq!(got, want);
}

#[test]
fn sample_shapes() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    ::pg_prng::global_prng(|p| p.seed(7));
    let a = int4_arr(mcx, &[Some(1), Some(2), Some(3), Some(4)], 5);
    let out = array_shuffle_n(mcx, &a, 2, false, &int4_meta()).unwrap();
    assert_eq!(dims_of(&out), (1, vec![2], vec![1]));
    let out = array_shuffle_n(mcx, &a, 0, false, &int4_meta()).unwrap();
    assert_eq!(dims_of(&out).0, 0);
}

#[test]
fn shuffle_multidim_keeps_items() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    ::pg_prng::global_prng(|p| p.seed(11));
    let a = int4_arr_md(mcx, &[Some(1), Some(2), Some(3), Some(4)], &[2, 2], &[1, 1]);
    let out = array_shuffle_n(mcx, &a, 2, true, &int4_meta()).unwrap();
    assert_eq!(dims_of(&out), (2, vec![2, 2], vec![1, 1]));
    let got = to_int4(mcx, &out);
    assert!(
        got == vec![Some(1), Some(2), Some(3), Some(4)]
            || got == vec![Some(3), Some(4), Some(1), Some(2)]
    );
}

#[test]
fn reverse_preserves_bounds_and_items() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr(mcx, &[Some(1), None, Some(3)], 4);
    let out = array_reverse_n(mcx, &a, &int4_meta()).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(3), None, Some(1)]);
    assert_eq!(dims_of(&out), (1, vec![3], vec![4]));

    let md = int4_arr_md(mcx, &[Some(1), Some(2), Some(3), Some(4)], &[2, 2], &[1, 1]);
    let out = array_reverse_n(mcx, &md, &int4_meta()).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(3), Some(4), Some(1), Some(2)]);
}

fn sort_int4s<'m>(
    mcx: Mcx<'m>,
    array: &[u8],
    desc: bool,
    nulls_first: bool,
) -> PgVec<'m, u8> {
    array_sort_with(mcx, array, &int4_meta(), None, |items| {
        let mut v: Vec<NullableDatum> = items.to_vec();
        v.sort_by(|a, b| {
            use core::cmp::Ordering;
            match (a.isnull, b.isnull) {
                (true, true) => Ordering::Equal,
                (true, false) => if nulls_first { Ordering::Less } else { Ordering::Greater },
                (false, true) => if nulls_first { Ordering::Greater } else { Ordering::Less },
                (false, false) => {
                    let o = a.value.as_i32().cmp(&b.value.as_i32());
                    if desc { o.reverse() } else { o }
                }
            }
        });
        ::mcx::slice_in(mcx, &v)
    })
    .unwrap()
}

#[test]
fn sort_scalars() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr(mcx, &[Some(3), None, Some(1), Some(2)], 4);

    let out = sort_int4s(mcx, &a, false, false);
    assert_eq!(to_int4(mcx, &out), vec![Some(1), Some(2), Some(3), None]);
    assert_eq!(arr_lbound(&out, 0), 4);

    let out = sort_int4s(mcx, &a, true, true);
    assert_eq!(to_int4(mcx, &out), vec![None, Some(3), Some(2), Some(1)]);
}

#[test]
fn sort_subarrays() {
    let ctx = MemoryContext::new_bump("t");
    let mcx = ctx.mcx();
    let a = int4_arr_md(mcx, &[Some(3), Some(4), Some(1), Some(2)], &[2, 2], &[5, 1]);
    let sorter = |items: &[NullableDatum]| {
        let mut v: Vec<NullableDatum> = items.to_vec();
        let first = |nd: &NullableDatum| {
            let p = nd.value.as_usize() as *const u8;
            let img = unsafe { core::slice::from_raw_parts(p, varsize_any(p)) };
            to_int4(mcx, img)[0]
        };
        v.sort_by_key(first);
        ::mcx::slice_in(mcx, &v)
    };
    let out = array_sort_with(mcx, &a, &int4_meta(), Some(1007), sorter).unwrap();
    assert_eq!(to_int4(mcx, &out), vec![Some(1), Some(2), Some(3), Some(4)]);
    assert_eq!(dims_of(&out), (2, vec![2, 2], vec![5, 1]));
}

#[test]
fn cmp_helpers_present() {
    assert_eq!(ARRAY_LT_OP, 1072);
    assert_eq!(ARRAY_GT_OP, 1073);
    assert_eq!(F_BTARRAYCMP, 382);
}

mod agg_array_serial {
    use super::*;
    use crate::{
        clone_array_build_state_arr, combine_array_build_state_arr,
        deserialize_array_build_state_arr, serialize_array_build_state_arr,
    };

    const INT4_ARRAY: Oid = 1007;

    fn state_of<'m>(mcx: Mcx<'m>, arrays: &[&[Option<i32>]]) -> ArrayBuildStateArr<'m> {
        let mut st = init_array_result_arr(mcx, INT4_ARRAY, INT4OID).unwrap();
        for a in arrays {
            let img = int4_arr(mcx, a, 1);
            st = accum_array_result_arr(mcx, Some(st), Some(&img), INT4_ARRAY).unwrap();
        }
        st
    }

    // Hand-derived from the C wire layout: elemtype/arrtype/nbytes (i32 BE),
    // data raw, abytes/aitems (i32 BE), [nullbitmap], nitems/ndims (i32 BE),
    // whole dims/lbs arrays raw native.
    #[test]
    fn serialize_golden_int4() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let st = state_of(mcx, &[&[Some(1), Some(2)]]);
        let out = serialize_array_build_state_arr(mcx, &st).unwrap();
        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(&23u32.to_be_bytes());
        expected.extend_from_slice(&1007u32.to_be_bytes());
        expected.extend_from_slice(&8u32.to_be_bytes());
        expected.extend_from_slice(&1i32.to_ne_bytes());
        expected.extend_from_slice(&2i32.to_ne_bytes());
        // abytes: pg_nextpower2_32(max(1024, 8 + 1)) = 1024; aitems: 0.
        expected.extend_from_slice(&1024u32.to_be_bytes());
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&2u32.to_be_bytes());
        expected.extend_from_slice(&2u32.to_be_bytes());
        let mut dims = [0i32; MAXDIM];
        dims[0] = 1;
        dims[1] = 2;
        let mut lbs = [0i32; MAXDIM];
        lbs[0] = 1;
        lbs[1] = 1;
        for d in dims {
            expected.extend_from_slice(&d.to_ne_bytes());
        }
        for l in lbs {
            expected.extend_from_slice(&l.to_ne_bytes());
        }
        assert_eq!(out.data(), &expected[..]);
    }

    #[test]
    fn roundtrip_with_null_elements() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let st = state_of(mcx, &[&[Some(1), Some(2)], &[Some(3), None]]);
        // aitems: pg_nextpower2_32(max(256, 4 + 1)) = 256.
        assert_eq!(st.aitems, 256);
        let img = serialize_array_build_state_arr(mcx, &st).unwrap();
        let back = deserialize_array_build_state_arr(mcx, img.data()).unwrap();
        assert_eq!(back.array_type, INT4_ARRAY);
        assert_eq!(back.element_type, INT4OID);
        assert_eq!((back.abytes, back.aitems), (st.abytes, st.aitems));
        assert_eq!((back.nbytes, back.nitems, back.ndims), (st.nbytes, st.nitems, st.ndims));
        let out = make_array_result_arr(mcx, &back).unwrap();
        assert_eq!(dims_of(&out), (2, vec![2, 2], vec![1, 1]));
        assert_eq!(to_int4(mcx, &out), vec![Some(1), Some(2), Some(3), None]);
    }

    #[test]
    fn roundtrip_no_nulls() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let st = state_of(mcx, &[&[Some(5)], &[Some(6)]]);
        let img = serialize_array_build_state_arr(mcx, &st).unwrap();
        let back = deserialize_array_build_state_arr(mcx, img.data()).unwrap();
        assert!(back.nullbitmap.is_none());
        let out = make_array_result_arr(mcx, &back).unwrap();
        assert_eq!(to_int4(mcx, &out), vec![Some(5), Some(6)]);
    }

    #[test]
    fn combine_clone_and_append() {
        let agg = MemoryContext::new_bump("agg");
        let aggmcx = agg.mcx();
        let s1 = {
            let worker = MemoryContext::new_bump("w1");
            let wmcx = worker.mcx();
            let s2 = state_of(wmcx, &[&[Some(1), Some(2)]]);
            clone_array_build_state_arr(aggmcx, &s2).unwrap()
        };
        // Worker context dropped: the clone owns its buffers.
        let mut s1 = s1;
        assert_eq!((s1.nitems, s1.ndims, s1.dims[0]), (2, 2, 1));

        let s3 = state_of(aggmcx, &[&[Some(3), None]]);
        combine_array_build_state_arr(&mut s1, &s3).unwrap();
        assert_eq!((s1.nitems, s1.dims[0]), (4, 2));
        // First nulls arrive via combine: aitems = pg_nextpower2_32(max(256, 5)).
        assert_eq!(s1.aitems, 256);
        let out = make_array_result_arr(aggmcx, &s1).unwrap();
        assert_eq!(to_int4(aggmcx, &out), vec![Some(1), Some(2), Some(3), None]);
    }

    #[test]
    fn combine_dimensionality_mismatch() {
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let mut s1 = state_of(mcx, &[&[Some(1), Some(2)]]);
        let s2 = state_of(mcx, &[&[Some(1)]]);
        let err = combine_array_build_state_arr(&mut s1, &s2).err().unwrap();
        assert_eq!(err.message(), "cannot accumulate arrays of different dimensionality");
    }
}
