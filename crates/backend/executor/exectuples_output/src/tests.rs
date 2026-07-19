use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Once;

use datum::{Datum, VarlenaRef};
use mcx::{Mcx, MemoryContext};
use tcop_dest::DestReceiver;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use types_portal::{
    CachedPlanHandle, ParamListHandle, Portal, PortalCleanupHook, PortalData, PortalStatus,
    PortalStrategy, QueryCompletion, QueryDescHandle, QueryEnvHandle, StmtListHandle,
    TuplestoreHandle, CMDTAG_UNKNOWN,
};
use types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};

use crate::*;

const TEXTOID: u32 = 25;
const TEXTOUT: u32 = 47;

thread_local! {
    static SENT: RefCell<Vec<(u8, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
}

fn textout_fn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    // SAFETY: test datum is a live 4B-header text varlena.
    let v = unsafe { VarlenaRef::from_ptr(fcinfo.arg(0).as_usize() as *const u8) };
    let mut s = v.data().to_vec();
    s.push(0);
    Ok(Datum::from_usize(
        Box::leak(s.into_boxed_slice()).as_ptr() as usize
    ))
}

fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        pqcomm_seams::pq_putmessage::set(|msgtype, body| {
            SENT.with(|s| s.borrow_mut().push((msgtype, body.to_vec())));
            Ok(0)
        });
        mbutils_seams::server_to_client_conversion_needed::set(|| false);
        mbutils_seams::pg_server_to_client::set(|_, _| Ok(None));
        lsyscache_seams::get_type_output_info::set(|oid| match oid {
            TEXTOID => Ok((TEXTOUT, true)),
            _ => panic!("get_type_output_info: unexpected oid {oid}"),
        });
        fmgr_seams::fmgr_info::set(|oid| match oid {
            TEXTOUT => Ok(FmgrInfo::new(textout_fn, TEXTOUT, 1, true, false)),
            _ => panic!("fmgr_info: unexpected oid {oid}"),
        });
    });
}

fn text_desc(mcx: Mcx<'_>) -> Rc<TupleDescData<'_>> {
    let mut attname = NameData::default();
    attname.namestrcpy("QUERY PLAN");
    let att = FormData_pg_attribute {
        attname,
        attnum: 1,
        atttypid: TEXTOID,
        atttypmod: -1,
        attlen: -1,
        attbyval: false,
        attalign: types_tuple::TYPALIGN_INT,
        ..Default::default()
    };
    let mut attrs = mcx::PgVec::new_in(mcx);
    let mut compact = mcx::PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn make_portal(mcx: Mcx<'_>) -> Portal<'_> {
    Portal::new(PortalData {
        name: mcx::PgString::new_in(mcx),
        prepStmtName: None,
        portalContext: None,
        plansource: ::types_portal::PlanSourceHandle::NULL,
        planContext: core::ptr::null_mut(),
        resowner: Default::default(),
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
        formats: mcx::PgVec::new_in(mcx),
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

fn data_rows() -> Vec<Vec<u8>> {
    SENT.with(|s| {
        s.borrow()
            .iter()
            .filter(|(t, _)| *t == b'D')
            .map(|(_, b)| b.clone())
            .collect()
    })
}

fn row_text(body: &[u8]) -> String {
    // 'D' body: int16 natts, then per column int32 len + bytes.
    assert_eq!(i16::from_be_bytes([body[0], body[1]]), 1);
    let len = i32::from_be_bytes([body[2], body[3], body[4], body[5]]) as usize;
    String::from_utf8(body[6..6 + len].to_vec()).unwrap()
}

#[test]
fn multiline_text_output_sends_one_row_per_line() {
    install_fixtures();
    SENT.with(|s| s.borrow_mut().clear());
    let ctx = MemoryContext::new("tupoutput-test");
    let mcx = ctx.mcx();

    let mut dr = printtup::printtup_create_DR(types_dest::CommandDest::RemoteExecute);
    printtup::SetRemoteDestReceiverParams(&mut dr, make_portal(mcx));
    let mut dest = DestReceiver::PrintTup(dr);

    let mut tstate = begin_tup_output_tupdesc(mcx, &mut dest, text_desc(mcx)).unwrap();
    do_text_output_multiline(&mut tstate, mcx, "Result\n  Output: 1\n").unwrap();
    do_text_output_oneline(&mut tstate, mcx, "one line").unwrap();
    end_tup_output(tstate).unwrap();

    let rows = data_rows();
    assert_eq!(rows.len(), 3);
    assert_eq!(row_text(&rows[0]), "Result");
    assert_eq!(row_text(&rows[1]), "  Output: 1");
    assert_eq!(row_text(&rows[2]), "one line");
}

#[test]
fn multiline_without_trailing_newline_emits_last_line() {
    install_fixtures();
    SENT.with(|s| s.borrow_mut().clear());
    let ctx = MemoryContext::new("tupoutput-test");
    let mcx = ctx.mcx();

    let mut dest = DestReceiver::DoNothing;
    let mut tstate = begin_tup_output_tupdesc(mcx, &mut dest, text_desc(mcx)).unwrap();
    do_text_output_multiline(&mut tstate, mcx, "a\nb").unwrap();
    end_tup_output(tstate).unwrap();
}
