use super::*;
use datum::{set_varsize_4b, Datum, VARHDRSZ};
use mcx::MemoryContext;
use std::sync::Once;
use types_core::{INT4OID, TEXTOID};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_portal::params::{ParamExternData, PARAM_FLAG_CONST};
use types_tuple::{PgTypeShape, TYPALIGN_INT, TYPSTORAGE_EXTENDED, TYPSTORAGE_PLAIN};

const F_INT4OUT: types_core::Oid = 43;
const F_TEXTOUT: types_core::Oid = 47;

std::thread_local! {
    static OUT: core::cell::RefCell<Vec<u8>> = const { core::cell::RefCell::new(Vec::new()) };
}

fn cstring_out(bytes: &[u8]) -> Datum {
    OUT.with(|c| {
        let mut b = c.borrow_mut();
        b.clear();
        b.extend_from_slice(bytes);
        b.push(0);
        Datum::from_usize(b.as_ptr() as usize)
    })
}

fn fake_int4out(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> types_error::PgResult<Datum> {
    Ok(cstring_out(fc.arg_i32(0).to_string().as_bytes()))
}

fn fake_textout(_f: Option<&mut FmgrInfo>, fc: &mut Fcinfo) -> types_error::PgResult<Datum> {
    // SAFETY: test fixtures carry inline uncompressed varlena images.
    let data = unsafe { fc.arg_varlena_packed(0) }.unwrap().data().to_vec();
    Ok(cstring_out(&data))
}

fn type_shape(typid: types_core::Oid) -> Option<PgTypeShape> {
    match typid {
        INT4OID => Some(PgTypeShape {
            typlen: 4,
            typbyval: true,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: 0,
        }),
        TEXTOID => Some(PgTypeShape {
            typlen: -1,
            typbyval: false,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_EXTENDED,
            typcollation: 100,
        }),
        _ => None,
    }
}

fn install() {
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        use syscache_seams as s;
        s::lookup_pg_type_shape::set(|typid| Ok(type_shape(typid)));
        s::pg_type_io_shape::set(|typid| {
            let out = match typid {
                INT4OID => F_INT4OUT,
                TEXTOID => F_TEXTOUT,
                _ => return Ok(None),
            };
            let sh = type_shape(typid).unwrap();
            Ok(Some(s::PgTypeIoShape {
                oid: typid,
                typinput: 1,
                typoutput: out,
                typreceive: 1,
                typsend: 1,
                typmodin: 0,
                typmodout: 0,
                typelem: 0,
                typlen: sh.typlen,
                typbyval: sh.typbyval,
                typalign: sh.typalign,
                typdelim: b',' as i8,
                typisdefined: true,
            }))
        });
        fmgr_seams::fmgr_info::set(|foid| {
            let f = match foid {
                F_INT4OUT => fake_int4out,
                F_TEXTOUT => fake_textout,
                other => panic!("test fmgr_info: unexpected function {other}"),
            };
            Ok(FmgrInfo::new(f, foid, 1, true, false))
        });
    });
}

fn text_image(payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::from(set_varsize_4b(VARHDRSZ + payload.len()));
    v.extend_from_slice(payload);
    v
}

fn int_param(v: i32) -> ParamExternData {
    ParamExternData {
        value: Datum::from_i32(v),
        isnull: false,
        pflags: PARAM_FLAG_CONST,
        ptype: INT4OID,
    }
}

#[test]
fn copy_deep_copies_byref() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let img = text_image(b"deep");
    let params = [
        int_param(7),
        ParamExternData {
            value: Datum::from_usize(img.as_ptr() as usize),
            isnull: false,
            pflags: 0,
            ptype: TEXTOID,
        },
        ParamExternData { value: Datum::null(), isnull: true, pflags: 0, ptype: TEXTOID },
        ParamExternData { value: Datum::from_i32(9), isnull: false, pflags: 0, ptype: 0 },
    ];
    let copy = copy_param_list(mcx, &params).unwrap();
    assert_eq!(copy.len(), 4);
    assert_eq!(copy[0].value.as_i32(), 7);
    assert_eq!(copy[0].pflags, PARAM_FLAG_CONST);
    assert_ne!(copy[1].value.as_usize(), params[1].value.as_usize());
    let copied =
        unsafe { core::slice::from_raw_parts(copy[1].value.as_usize() as *const u8, img.len()) };
    assert_eq!(copied, &img[..]);
    assert!(copy[2].isnull);
    assert_eq!(copy[3].value.as_i32(), 9);
}

#[test]
fn serialize_layout_estimate_and_restore() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let img = text_image(b"xy");
    let params = [
        int_param(42),
        ParamExternData {
            value: Datum::from_usize(img.as_ptr() as usize),
            isnull: false,
            pflags: 0,
            ptype: TEXTOID,
        },
        ParamExternData { value: Datum::null(), isnull: true, pflags: 3, ptype: TEXTOID },
        ParamExternData { value: Datum::from_i32(-1), isnull: false, pflags: 0, ptype: 0 },
    ];
    let mut out = mcx::PgVec::new_in(mcx);
    serialize_param_list(&params, &mut out).unwrap();
    assert_eq!(out.len(), estimate_param_list_space(&params).unwrap());

    let mut expect = Vec::new();
    expect.extend_from_slice(&4i32.to_ne_bytes());
    expect.extend_from_slice(&INT4OID.to_ne_bytes());
    expect.extend_from_slice(&(PARAM_FLAG_CONST).to_ne_bytes());
    expect.extend_from_slice(&(-1i32).to_ne_bytes());
    expect.extend_from_slice(&(Datum::from_i32(42).as_usize() as u64).to_ne_bytes());
    expect.extend_from_slice(&TEXTOID.to_ne_bytes());
    expect.extend_from_slice(&0u16.to_ne_bytes());
    expect.extend_from_slice(&(img.len() as i32).to_ne_bytes());
    expect.extend_from_slice(&img);
    expect.extend_from_slice(&TEXTOID.to_ne_bytes());
    expect.extend_from_slice(&3u16.to_ne_bytes());
    expect.extend_from_slice(&(-2i32).to_ne_bytes());
    expect.extend_from_slice(&0u32.to_ne_bytes());
    expect.extend_from_slice(&0u16.to_ne_bytes());
    expect.extend_from_slice(&(-1i32).to_ne_bytes());
    expect.extend_from_slice(&(Datum::from_i32(-1).as_usize() as u64).to_ne_bytes());
    assert_eq!(&out[..], &expect[..]);

    let mut cur: &[u8] = &out;
    let restored = restore_param_list(mcx, &mut cur).unwrap();
    assert!(cur.is_empty());
    assert_eq!(restored.len(), 4);
    assert_eq!(restored[0].value.as_i32(), 42);
    assert_eq!(restored[0].ptype, INT4OID);
    assert_eq!(restored[0].pflags, PARAM_FLAG_CONST);
    let rimg = unsafe {
        core::slice::from_raw_parts(restored[1].value.as_usize() as *const u8, img.len())
    };
    assert_eq!(rimg, &img[..]);
    assert!(restored[2].isnull && restored[2].pflags == 3);
    assert_eq!(restored[3].value.as_i32(), -1);
    assert_eq!(restored[3].ptype, 0);
}

#[test]
fn log_string_formats() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let img = text_image(b"it's");
    let params = [
        int_param(5),
        ParamExternData { value: Datum::null(), isnull: true, pflags: 0, ptype: TEXTOID },
        ParamExternData {
            value: Datum::from_usize(img.as_ptr() as usize),
            isnull: false,
            pflags: 0,
            ptype: TEXTOID,
        },
    ];
    let s = build_param_log_string(mcx, &params, None, -1).unwrap().unwrap();
    assert_eq!(s.as_str(), "$1 = '5', $2 = NULL, $3 = 'it''s'");

    let long = text_image(b"abcdefgh");
    let params = [ParamExternData {
        value: Datum::from_usize(long.as_ptr() as usize),
        isnull: false,
        pflags: 0,
        ptype: TEXTOID,
    }];
    let s = build_param_log_string(mcx, &params, None, 3).unwrap().unwrap();
    assert_eq!(s.as_str(), "$1 = 'abc...'");

    let known: [Option<&str>; 1] = [Some("known'v")];
    let s = build_param_log_string(mcx, &params, Some(&known), -1).unwrap().unwrap();
    assert_eq!(s.as_str(), "$1 = 'known''v'");
}
