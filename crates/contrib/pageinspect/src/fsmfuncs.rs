//! fsmfuncs.c — fsm_page_contents.

use crate::*;

pub(crate) fn fc_fsm_page_contents(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    require_superuser("raw page")?;

    let page = RawPage::arg(fcinfo, 0)?;
    let b = page.bytes();
    if page_is_new(b) {
        return Ok(fcinfo.return_null());
    }

    // FSMPageData sits at PageGetContents: fp_next_slot (int), then fp_nodes.
    let contents = SizeOfPageHeaderData;
    let fp_next_slot = r_u32(b, contents) as i32;
    let fp_nodes = &b[contents + 4..];

    let mut out = String::new();
    for i in 0..freespace::NODES_PER_PAGE as usize {
        if fp_nodes[i] != 0 {
            out.push_str(&format!("{}: {}\n", i, fp_nodes[i]));
        }
    }
    out.push_str(&format!("fp_next_slot: {fp_next_slot}\n"));

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    text_datum(mcx, out.as_bytes())
}
