use super::*;
use ::datum::varlena::set_varsize_4b;
use ::mcx::MemoryContext;
use ::types_fmgr::FunctionCallInfoBaseData;
use ::types_portal::{
    CachedPlanHandle, ParamListHandle, PortalCleanupHook, PortalData, PortalStatus,
    PortalStrategy, QueryCompletion, QueryDescHandle, QueryEnvHandle, StmtListHandle,
    TuplestoreHandle, CMDTAG_UNKNOWN,
};
use ::types_resowner::ResourceOwner;
use ::types_slot::{TupleSlotKind, TupleTableSlot, VirtualTupleTableSlot};
use ::types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TYPALIGN_INT};
use std::cell::{Cell, RefCell};
use std::sync::Once;

const INT4OUT: Oid = 43;
const TEXTOUT: Oid = 47;
const INT4SEND: Oid = 2407;
const INT4OID: Oid = 23;
const TEXTOID: Oid = 25;
const DOMAINOID: Oid = 99923;

thread_local! {
    static SENT: RefCell<Vec<(u8, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
    static CONVERT: Cell<bool> = const { Cell::new(false) };
    static CONVERT_CALLS: Cell<u32> = const { Cell::new(0) };
    static FMGR_INFO_CALLS: Cell<u32> = const { Cell::new(0) };
    static TLIST: RefCell<Vec<TargetEntrySummary>> = const { RefCell::new(Vec::new()) };
}

fn int4out_fn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let s = format!("{}\0", fcinfo.arg(0).as_i32());
    Ok(Datum::from_usize(
        Box::leak(s.into_boxed_str()).as_ptr() as usize
    ))
}

// Test "textout": the datum already is a NUL-terminated cstring.
fn textout_fn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    Ok(fcinfo.arg(0))
}

fn int4send_fn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let mut img = Vec::with_capacity(8);
    img.extend_from_slice(&set_varsize_4b(8));
    img.extend_from_slice(&fcinfo.arg(0).as_i32().to_be_bytes());
    Ok(Datum::from_usize(
        Box::leak(img.into_boxed_slice()).as_ptr() as usize
    ))
}

fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        pqcomm_seams::pq_putmessage::set(|msgtype, body| {
            SENT.with(|s| s.borrow_mut().push((msgtype, body.to_vec())));
            Ok(0)
        });
        mbutils_seams::pg_server_to_client::set(|mcx, s| {
            CONVERT_CALLS.with(|c| c.set(c.get() + 1));
            if CONVERT.with(|c| c.get()) {
                let upper: Vec<u8> = s.iter().map(|b| b.to_ascii_uppercase()).collect();
                Ok(Some(mcx::slice_in(mcx, &upper)?))
            } else {
                Ok(None)
            }
        });
        mbutils_seams::server_to_client_conversion_needed::set(|| CONVERT.with(|c| c.get()));
        pquery_seams::fetch_portal_target_list::set(|mcx, _portal| {
            TLIST.with(|t| mcx::slice_in(mcx, &t.borrow()))
        });
        lsyscache_seams::get_type_output_info::set(|oid| match oid {
            INT4OID | DOMAINOID => Ok((INT4OUT, false)),
            TEXTOID => Ok((TEXTOUT, true)),
            _ => panic!("get_type_output_info: unexpected oid {oid}"),
        });
        lsyscache_seams::get_type_binary_output_info::set(|oid| match oid {
            INT4OID => Ok((INT4SEND, false)),
            _ => panic!("get_type_binary_output_info: unexpected oid {oid}"),
        });
        lsyscache_seams::get_base_type_and_typmod::set(|typid, typmod| match typid {
            DOMAINOID => Ok((INT4OID, 7)),
            _ => Ok((typid, typmod)),
        });
        fmgr_seams::fmgr_info::set(|oid| {
            FMGR_INFO_CALLS.with(|c| c.set(c.get() + 1));
            match oid {
                INT4OUT => Ok(FmgrInfo::new(int4out_fn, INT4OUT, 1, true, false)),
                TEXTOUT => Ok(FmgrInfo::new(textout_fn, TEXTOUT, 1, true, false)),
                INT4SEND => Ok(FmgrInfo::new(int4send_fn, INT4SEND, 1, true, false)),
                _ => panic!("fmgr_info: unexpected oid {oid}"),
            }
        });
    });
}

fn setup() -> MemoryContext {
    install_fixtures();
    SENT.with(|s| s.borrow_mut().clear());
    CONVERT.with(|c| c.set(false));
    CONVERT_CALLS.with(|c| c.set(0));
    FMGR_INFO_CALLS.with(|c| c.set(0));
    TLIST.with(|t| t.borrow_mut().clear());
    MemoryContext::new("printtup-test")
}

fn sent() -> Vec<(u8, Vec<u8>)> {
    SENT.with(|s| s.borrow().clone())
}

fn int4_attr(i: i16) -> FormData_pg_attribute {
    let mut attname = NameData::default();
    attname.namestrcpy(&format!("c{}", i + 1));
    FormData_pg_attribute {
        attname,
        attnum: i + 1,
        atttypid: INT4OID,
        atttypmod: -1,
        attlen: 4,
        attbyval: true,
        attalign: TYPALIGN_INT,
        ..Default::default()
    }
}

fn make_desc<'mcx>(
    mcx: Mcx<'mcx>,
    atts: Vec<FormData_pg_attribute>,
) -> Rc<TupleDescData<'mcx>> {
    let mut attrs = mcx::PgVec::new_in(mcx);
    let mut compact = mcx::PgVec::new_in(mcx);
    let natts = atts.len() as i32;
    for att in atts {
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn int4_desc(mcx: Mcx<'_>, n: i16) -> Rc<TupleDescData<'_>> {
    make_desc(mcx, (0..n).map(int4_attr).collect())
}

fn make_slot<'mcx>(
    mcx: Mcx<'mcx>,
    desc: Rc<TupleDescData<'mcx>>,
    values: &[(Datum, bool)],
) -> SlotData<'mcx> {
    let mut base = TupleTableSlot::new_in(mcx, TupleSlotKind::Virtual);
    base.set_descriptor(mcx, desc);
    for (i, &(v, isnull)) in values.iter().enumerate() {
        base.tts_values[i] = v;
        base.tts_isnull[i] = isnull;
    }
    base.tts_nvalid = values.len() as i16;
    base.mark_not_empty();
    SlotData::Virtual(VirtualTupleTableSlot {
        base,
        data: mcx::PgVec::new_in(mcx),
    })
}

fn make_portal<'mcx>(mcx: Mcx<'mcx>, formats: &[i16]) -> Portal<'mcx> {
    let mut fv = mcx::PgVec::new_in(mcx);
    for &f in formats {
        fv.push(f);
    }
    Portal::new(PortalData {
        name: mcx::PgString::new_in(mcx),
        prepStmtName: None,
        portalContext: None,
        plansource: ::types_portal::PlanSourceHandle::NULL,
        planContext: core::ptr::null_mut(),
        resowner: ResourceOwner::default(),
        cleanup: PortalCleanupHook::None,
        createSubid: 0,
        activeSubid: 0,
        createLevel: 0,
        sourceText: None,
        commandTag: CMDTAG_UNKNOWN,
        qc: QueryCompletion::default(),
        stmts: StmtListHandle::NULL,
        cplan: CachedPlanHandle::NULL,
        portalParams: ParamListHandle::NULL,
        queryEnv: QueryEnvHandle::NULL,
        strategy: PortalStrategy::default(),
        cursorOptions: 0,
        status: PortalStatus::default(),
        portalPinned: false,
        autoHeld: false,
        queryDesc: QueryDescHandle::NULL,
        tupDesc: None,
        formats: fv,
        portalSnapshot: None,
        holdStore: TuplestoreHandle::NULL,
        holdContext: None,
        holdSnapshot: None,
        atStart: true,
        atEnd: false,
        portalPos: 0,
        creation_time: 0,
        visible: false,
        // WS-CA wave-10 (cursors inc-2): mechanical literal completion only.
        cursorStoreArmed: false,
        cursorStore: TuplestoreHandle::NULL,
        cursorFillExhausted: false,
        currentOfEligible: None,
        cursorCaptureBatch: false,
        cursorTidStore: TuplestoreHandle::NULL,
    })
}

fn remote_receiver<'mcx>(mcx: Mcx<'mcx>, formats: &[i16]) -> DrPrinttup<'mcx> {
    let mut dr = printtup_create_DR(CommandDest::Remote);
    SetRemoteDestReceiverParams(&mut dr, make_portal(mcx, formats));
    dr
}

fn expect_rowdesc_col(name: &[u8], typid: Oid, attlen: i16, typmod: i32, format: i16) -> Vec<u8> {
    expect_rowdesc_col_origin(name, 0, 0, typid, attlen, typmod, format)
}

fn expect_rowdesc_col_origin(
    name: &[u8],
    resorigtbl: Oid,
    resorigcol: i16,
    typid: Oid,
    attlen: i16,
    typmod: i32,
    format: i16,
) -> Vec<u8> {
    let mut v = name.to_vec();
    v.push(0);
    v.extend_from_slice(&resorigtbl.to_be_bytes());
    v.extend_from_slice(&(resorigcol as u16).to_be_bytes());
    v.extend_from_slice(&typid.to_be_bytes());
    v.extend_from_slice(&(attlen as u16).to_be_bytes());
    v.extend_from_slice(&(typmod as u32).to_be_bytes());
    v.extend_from_slice(&(format as u16).to_be_bytes());
    v
}

fn counted(s: &[u8]) -> Vec<u8> {
    let mut v = (s.len() as u32).to_be_bytes().to_vec();
    v.extend_from_slice(s);
    v
}

#[test]
fn startup_sends_row_description_for_dest_remote_only() {
    let ctx = setup();
    let desc = int4_desc(ctx.mcx(), 2);

    let mut dr = remote_receiver(ctx.mcx(), &[]);
    assert!(dr.sendDescrip);
    dr.startup(1, &desc).unwrap();
    let msgs = sent();
    assert_eq!(msgs.len(), 1);
    let (t, body) = &msgs[0];
    assert_eq!(*t, b'T');
    let mut expect = 2u16.to_be_bytes().to_vec();
    expect.extend_from_slice(&expect_rowdesc_col(b"c1", INT4OID, 4, -1, 0));
    expect.extend_from_slice(&expect_rowdesc_col(b"c2", INT4OID, 4, -1, 0));
    assert_eq!(body, &expect);

    // DestRemoteExecute: no T message.
    let mut dr = printtup_create_DR(CommandDest::RemoteExecute);
    SetRemoteDestReceiverParams(&mut dr, make_portal(ctx.mcx(), &[]));
    assert!(!dr.sendDescrip);
    dr.startup(1, &desc).unwrap();
    assert_eq!(sent().len(), 1);
}

#[test]
fn row_description_skips_resjunk_and_zero_fills_missing_tlist() {
    let ctx = setup();
    TLIST.with(|t| {
        *t.borrow_mut() = vec![
            TargetEntrySummary {
                resjunk: true,
                resorigtbl: 111,
                resorigcol: 9,
            },
            TargetEntrySummary {
                resjunk: false,
                resorigtbl: 16384,
                resorigcol: 2,
            },
        ]
    });
    let desc = int4_desc(ctx.mcx(), 2);
    let mut dr = remote_receiver(ctx.mcx(), &[]);
    dr.startup(1, &desc).unwrap();

    let (_, body) = &sent()[0];
    let mut expect = 2u16.to_be_bytes().to_vec();
    expect.extend_from_slice(&expect_rowdesc_col_origin(b"c1", 16384, 2, INT4OID, 4, -1, 0));
    expect.extend_from_slice(&expect_rowdesc_col(b"c2", INT4OID, 4, -1, 0));
    assert_eq!(body, &expect);
}

#[test]
fn row_description_resolves_domain_base_type_and_formats() {
    let ctx = setup();
    let mut att = int4_attr(0);
    att.atttypid = DOMAINOID;
    att.atttypmod = -1;
    let desc = make_desc(ctx.mcx(), vec![att]);
    let mut dr = remote_receiver(ctx.mcx(), &[1]);
    dr.startup(1, &desc).unwrap();

    let (_, body) = &sent()[0];
    let mut expect = 1u16.to_be_bytes().to_vec();
    expect.extend_from_slice(&expect_rowdesc_col(b"c1", INT4OID, 4, 7, 1));
    assert_eq!(body, &expect);
}

#[test]
fn datarow_text_output_with_nulls() {
    let ctx = setup();
    let desc = int4_desc(ctx.mcx(), 4);
    let mut slot = make_slot(
        ctx.mcx(),
        Rc::clone(&desc),
        &[
            (Datum::from_i32(1), false),
            (Datum::from_i32(-42), false),
            (Datum::null(), true),
            (Datum::from_i32(i32::MAX), false),
        ],
    );
    let mut dr = remote_receiver(ctx.mcx(), &[]);
    dr.startup(1, &desc).unwrap();
    // RowDescription's attname sends go through the conversion seam; the
    // hoist claim is about the row loop only.
    let calls_after_startup = CONVERT_CALLS.with(|c| c.get());
    assert!(dr.receive_slot(&mut slot).unwrap());

    let msgs = sent();
    let (t, body) = &msgs[1];
    assert_eq!(*t, b'D');
    let mut expect = 4u16.to_be_bytes().to_vec();
    expect.extend_from_slice(&counted(b"1"));
    expect.extend_from_slice(&counted(b"-42"));
    expect.extend_from_slice(&(-1i32).to_be_bytes());
    expect.extend_from_slice(&counted(b"2147483647"));
    assert_eq!(body, &expect);
    // Conversion hoisted: encodings match, so no per-attribute seam calls.
    assert_eq!(CONVERT_CALLS.with(|c| c.get()), calls_after_startup);
}

#[test]
fn datarow_binary_and_mixed_formats() {
    let ctx = setup();
    let desc = int4_desc(ctx.mcx(), 2);
    let values = [
        (Datum::from_i32(0x0102_0304), false),
        (Datum::from_i32(-1), false),
    ];

    let mut slot = make_slot(ctx.mcx(), Rc::clone(&desc), &values);
    let mut dr = remote_receiver(ctx.mcx(), &[1, 1]);
    dr.startup(1, &desc).unwrap();
    dr.receive_slot(&mut slot).unwrap();
    let mut expect = 2u16.to_be_bytes().to_vec();
    expect.extend_from_slice(&counted(&[1, 2, 3, 4]));
    expect.extend_from_slice(&counted(&[0xFF, 0xFF, 0xFF, 0xFF]));
    assert_eq!(sent()[1], (b'D', expect));

    let mut slot = make_slot(ctx.mcx(), Rc::clone(&desc), &values);
    let mut dr = remote_receiver(ctx.mcx(), &[0, 1]);
    dr.startup(1, &desc).unwrap();
    dr.receive_slot(&mut slot).unwrap();
    let mut expect = 2u16.to_be_bytes().to_vec();
    expect.extend_from_slice(&counted(b"16909060"));
    expect.extend_from_slice(&counted(&[0xFF, 0xFF, 0xFF, 0xFF]));
    assert_eq!(sent()[3], (b'D', expect));
}

#[test]
fn attr_info_prepared_once_per_descriptor() {
    let ctx = setup();
    let desc = int4_desc(ctx.mcx(), 2);
    let values = [(Datum::from_i32(7), false), (Datum::from_i32(8), false)];
    let mut slot = make_slot(ctx.mcx(), Rc::clone(&desc), &values);
    let mut dr = remote_receiver(ctx.mcx(), &[]);
    dr.startup(1, &desc).unwrap();

    dr.receive_slot(&mut slot).unwrap();
    dr.receive_slot(&mut slot).unwrap();
    assert_eq!(FMGR_INFO_CALLS.with(|c| c.get()), 2);

    // Descriptor identity change forces re-derivation (C: attrinfo pointer test).
    let desc2 = int4_desc(ctx.mcx(), 2);
    let mut slot2 = make_slot(ctx.mcx(), desc2, &values);
    dr.receive_slot(&mut slot2).unwrap();
    assert_eq!(FMGR_INFO_CALLS.with(|c| c.get()), 4);
    assert_eq!(sent().len(), 4);
}

#[test]
fn text_output_converts_when_encodings_differ() {
    let ctx = setup();
    CONVERT.with(|c| c.set(true));
    let mut att = int4_attr(0);
    att.atttypid = TEXTOID;
    att.attlen = -1;
    att.attbyval = false;
    let desc = make_desc(ctx.mcx(), vec![att]);

    let text = b"hello\0";
    let mut slot = make_slot(
        ctx.mcx(),
        Rc::clone(&desc),
        &[(Datum::from_usize(text.as_ptr() as usize), false)],
    );
    let mut dr = printtup_create_DR(CommandDest::RemoteExecute);
    SetRemoteDestReceiverParams(&mut dr, make_portal(ctx.mcx(), &[]));
    dr.startup(1, &desc).unwrap();
    dr.receive_slot(&mut slot).unwrap();

    let mut expect = 1u16.to_be_bytes().to_vec();
    expect.extend_from_slice(&counted(b"HELLO"));
    assert_eq!(sent()[0], (b'D', expect));
    assert_eq!(CONVERT_CALLS.with(|c| c.get()), 1);
}

#[test]
fn unsupported_format_code_is_rejected() {
    let ctx = setup();
    let desc = int4_desc(ctx.mcx(), 1);
    let mut slot = make_slot(ctx.mcx(), Rc::clone(&desc), &[(Datum::from_i32(1), false)]);
    let mut dr = remote_receiver(ctx.mcx(), &[2]);
    dr.startup(1, &desc).unwrap();
    let err = dr.receive_slot(&mut slot).unwrap_err();
    assert_eq!(err.message(), "unsupported format code: 2");
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
}

#[test]
fn shutdown_releases_buffer_and_attr_info() {
    let ctx = setup();
    let desc = int4_desc(ctx.mcx(), 1);
    let mut slot = make_slot(ctx.mcx(), Rc::clone(&desc), &[(Datum::from_i32(1), false)]);
    let mut dr = remote_receiver(ctx.mcx(), &[]);
    dr.startup(1, &desc).unwrap();
    dr.receive_slot(&mut slot).unwrap();
    dr.shutdown();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = dr.receive_slot(&mut slot);
    }))
    .is_err());
}

// The retained-scratch contract: statement N reuses statement 1's wire-buffer
// capacity and the scratch context never grows.
#[test]
fn scratch_stays_flat_across_statement_cycles() {
    let ctx = setup();
    let desc = int4_desc(ctx.mcx(), 2);
    let values = [(Datum::from_i32(7), false), (Datum::from_i32(8), false)];
    let mut cycle = || {
        let mut slot = make_slot(ctx.mcx(), Rc::clone(&desc), &values);
        let mut dr = remote_receiver(ctx.mcx(), &[]);
        dr.startup(1, &desc).unwrap();
        dr.receive_slot(&mut slot).unwrap();
        dr.shutdown();
    };
    cycle();
    let scratch = scratch_mcx().context();
    let used_after_first = scratch.used();
    let peak_after_first = scratch.peak();
    for _ in 0..(if cfg!(miri) { 20 } else { 500 }) {
        cycle();
    }
    assert_eq!(scratch.used(), used_after_first, "printtup scratch grew");
    assert_eq!(scratch.peak(), peak_after_first, "printtup scratch peak grew");
    assert!(WIRE_BUF.with(|c| {
        let buf = c.take();
        let pooled = buf.is_some();
        c.set(buf);
        pooled
    }));
}
