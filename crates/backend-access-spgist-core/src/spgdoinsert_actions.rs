// Included into spgdoinsert.rs (shares its `use`s). The remaining inner-tuple
// actions and the spgdoinsert driver from spgdoinsert.c.

// ===========================================================================
// checkAllTheSame (static)
// ===========================================================================

/// `checkAllTheSame(in, out, tooBig, &includeNew)` (spgdoinsert.c:598) — detect
/// the case where the opclass' picksplit assigned all leaf tuples to one node,
/// and if so override the result to spread them across 8 nodes so progress is
/// guaranteed. Returns `(allTheSame, includeNew)`.
fn checkAllTheSame(
    in_: &spgPickSplitIn<'_>,
    out: &mut spgPickSplitOut<'_>,
    too_big: bool,
) -> PgResult<(bool, bool)> {
    let n_tuples = in_.nTuples() as usize;
    let mut include_new = true;

    if in_.nTuples() <= 1 {
        return Ok((false, include_new)); // nothing to do
    }

    let the_node = out.mapTuplesToNodes[0];
    let limit = if too_big { n_tuples - 1 } else { n_tuples };
    for i in 1..limit {
        if out.mapTuplesToNodes[i] != the_node {
            return Ok((false, include_new)); // not all the same
        }
    }

    // Yup, it's all-the-same. Override the picksplit to use 8 nodes.
    if too_big && out.mapTuplesToNodes[n_tuples - 1] != the_node {
        include_new = false;
    }

    out.nNodes = 8;

    // Make a fake set of node assignments.
    for i in 0..n_tuples {
        out.mapTuplesToNodes[i] = (i % out.nNodes as usize) as i32;
    }

    // Same for the labels, if any.
    if let Some(labels) = out.nodeLabels.as_ref() {
        let the_label = labels[the_node as usize].clone();
        let mut new_labels = Vec::with_capacity(out.nNodes as usize);
        for _ in 0..out.nNodes {
            new_labels.push(the_label.clone());
        }
        out.nodeLabels = Some(new_labels);
    }

    Ok((true, include_new))
}

// ===========================================================================
// doPickSplit (static)
// ===========================================================================

/// `doPickSplit(index, state, current, parent, newLeafTuple, level, isNulls,
/// isNew)` (spgdoinsert.c:676) — replace the leaf page contents (or the root
/// page) with a new inner tuple and redistribute the leaf tuples across child
/// nodes (and possibly across leaf pages). Returns true if `newLeafTuple` was
/// actually inserted in this call (else the caller must descend the new inner
/// tuple and retry).
fn doPickSplit<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    state: &mut SpGistState<'mcx>,
    current: &mut SPPageDesc,
    parent: &mut SPPageDesc,
    new_leaf_tuple: &PgVec<'mcx, u8>,
    level: i32,
    is_nulls: bool,
    is_new: bool,
) -> PgResult<bool> {
    let mut inserted_new = false;

    let cur_page = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
    let max = PageGetMaxOffsetNumber(&PageRef::new(&cur_page)?);

    // Collect the old leaf tuples and their datums.
    let mut in_datums: Vec<Datum<'mcx>> = Vec::with_capacity((max + 1) as usize);
    let mut old_leafs: Vec<Vec<u8>> = Vec::with_capacity((max + 1) as usize);
    let mut to_delete: Vec<OffsetNumber> = Vec::new();
    let mut space_to_delete: usize = 0;

    let is_root = SpGistBlockIsRoot(current.blkno);
    if is_root {
        // Root page: leaf tuples are unchained, scan all offsets.
        let mut i = FirstOffsetNumber;
        while i <= max {
            let it_off = {
                let pr = PageRef::new(&cur_page)?;
                let iid = PageGetItemId(&pr, i)?;
                iid.lp_off() as usize
            };
            let it = &cur_page[it_off..];
            debug_assert_eq!(lt_tupstate(it), SPGIST_LIVE);
            let datum = if is_nulls {
                Datum::null()
            } else {
                lt_datum(mcx, state, it)?
            };
            in_datums.push(datum);
            old_leafs.push(it[..lt_size(it)].to_vec());
            to_delete.push(i);
            space_to_delete += lt_size(it) + SIZEOF_ITEM_ID_DATA;
            i += 1;
        }
    } else {
        // Normal leaf page: walk the chain.
        let mut i = current.offnum;
        while i != InvalidOffsetNumber {
            let it_off = {
                let pr = PageRef::new(&cur_page)?;
                let iid = PageGetItemId(&pr, i)?;
                iid.lp_off() as usize
            };
            let it = &cur_page[it_off..];
            let st = lt_tupstate(it);
            if st == SPGIST_LIVE {
                let datum = if is_nulls {
                    Datum::null()
                } else {
                    lt_datum(mcx, state, it)?
                };
                in_datums.push(datum);
                old_leafs.push(it[..lt_size(it)].to_vec());
                to_delete.push(i);
                // Will be replaced by a dead tuple, not deleted: saves
                // size - SGDTSIZE.
                debug_assert!(lt_size(it) >= SGDTSIZE);
                space_to_delete += lt_size(it) - SGDTSIZE;
            } else if st == SPGIST_DEAD {
                debug_assert_eq!(lt_get_next_offset(it), InvalidOffsetNumber);
                to_delete.push(i);
                // We don't bother to include it in the picksplit input.
            } else {
                return Err(elog_error("unexpected SPGiST tuple state".into()));
            }
            i = lt_get_next_offset(it);
        }
    }
    let n_to_delete = to_delete.len();
    let n_to_insert_collected = old_leafs.len(); // == in.nTuples (without new)

    // Build the picksplit input: nTuples real ones, plus the incoming tuple
    // (appended without bumping the count for the opclass).
    let new_leaf_datum = if is_nulls {
        Datum::null()
    } else {
        lt_datum(mcx, state, new_leaf_tuple)?
    };
    old_leafs.push(new_leaf_tuple[..lt_size(new_leaf_tuple)].to_vec());

    let in_ = spgPickSplitIn {
        datums: {
            let mut v = in_datums.clone();
            v.push(new_leaf_datum);
            v
        },
        level,
    };
    let n_tuples = in_.nTuples() as usize; // collected + 1

    let mut out = spgPickSplitOut::default();

    // Dispatch to the opclass picksplit, or do a dummy split for nulls.
    if !is_nulls {
        let proc_oid =
            relcache::index_getprocid::call(index, 1, SPGIST_PICKSPLIT_PROC as u16)?;
        backend_access_spg_core_seams::spg_picksplit::call(mcx, proc_oid, &in_, &mut out)?;
    } else {
        // Nulls tree: dummy split into a single node.
        out.hasPrefix = false;
        out.nNodes = 1;
        out.nodeLabels = None;
        out.mapTuplesToNodes = alloc::vec![0i32; n_tuples];
    }

    // Form new leaf tuples for each input value (key column replaced).
    let leaf_desc: &TupleDescData<'mcx> = state
        .leafTupDesc
        .as_ref()
        .expect("doPickSplit: leafTupDesc is NULL");
    let natts = leaf_desc.natts as usize;
    let mut new_leafs: Vec<PgVec<'mcx, u8>> = Vec::with_capacity(n_tuples);
    let mut total_leaf_sizes: usize = 0;
    for i in 0..n_tuples {
        // Default datums/isnulls: deform the old leaf if it has INCLUDE columns.
        let mut leaf_datums: Vec<Datum<'mcx>> = alloc::vec![Datum::null(); natts];
        let mut leaf_isnulls: Vec<bool> = alloc::vec![true; natts];
        if natts > 1 {
            let cols = spgDeformLeafTuple(mcx, &old_leafs[i], leaf_desc, is_nulls)?;
            for (j, (d, n)) in cols.iter().enumerate() {
                leaf_datums[j] = d.clone_in(mcx)?;
                leaf_isnulls[j] = *n;
            }
        }
        if !is_nulls {
            leaf_datums[spgKeyColumn as usize] = out.leafTupleDatums[i].clone_in(mcx)?;
            leaf_isnulls[spgKeyColumn as usize] = false;
        } else {
            leaf_datums[spgKeyColumn as usize] = Datum::null();
            leaf_isnulls[spgKeyColumn as usize] = true;
        }
        // Re-fetch heapPtr from the old leaf image (offset 6, 6 bytes).
        let heap_ptr = leaf_heap_ptr(&old_leafs[i]);
        let lt =
            spgFormLeafTuple(mcx, state, &heap_ptr, &leaf_datums, &leaf_isnulls)?;
        total_leaf_sizes += lt.len() + SIZEOF_ITEM_ID_DATA;
        new_leafs.push(lt);
    }

    // checkAllTheSame may override the picksplit.
    let too_big = total_leaf_sizes > SPGIST_PAGE_CAPACITY;
    let (all_the_same, include_new) = checkAllTheSame(&in_, &mut out, too_big)?;

    let max_to_include = if include_new {
        n_tuples
    } else {
        // Exclude the new tuple from this split round.
        total_leaf_sizes -= new_leafs[n_tuples - 1].len() + SIZEOF_ITEM_ID_DATA;
        n_tuples - 1
    };

    // Build the new inner tuple.
    let mut nodes: Vec<PgVec<'mcx, u8>> = Vec::with_capacity(out.nNodes as usize);
    for i in 0..out.nNodes as usize {
        let (label, labelisnull) = match out.nodeLabels.as_ref() {
            Some(labels) => (labels[i].clone(), false),
            None => (Datum::null(), true),
        };
        nodes.push(spgFormNodeTuple(mcx, state, &label, labelisnull)?);
    }
    let prefix = out.prefixDatum.clone().unwrap_or_else(Datum::null);
    let mut inner_tuple = spgFormInnerTuple(mcx, state, out.hasPrefix, &prefix, &nodes)?;
    it_set_all_the_same(&mut inner_tuple, all_the_same);

    // Per-node leaf-size accumulation (for the leaf page distribution).
    let mut leaf_sizes: Vec<usize> = alloc::vec![0usize; out.nNodes as usize];
    for i in 0..max_to_include {
        let n = out.mapTuplesToNodes[i];
        if n < 0 || n >= out.nNodes {
            return Err(elog_error(format!(
                "inconsistent result of SPGiST picksplit function (node {n})"
            )));
        }
        leaf_sizes[n as usize] += new_leafs[i].len() + SIZEOF_ITEM_ID_DATA;
    }

    // Choose the inner-tuple buffer.
    let mut init_inner = false;
    let inner_tuple_size = inner_tuple.len();
    let parent_free_ok = if buffer_is_valid(parent.buffer) && !SpGistBlockIsRoot(parent.blkno) {
        let ppage = bufmgr::buffer_get_page::call(mcx, parent.buffer)?;
        SpGistPageGetFreeSpace(&ppage, 1)? >= inner_tuple_size + SIZEOF_ITEM_ID_DATA
    } else {
        false
    };
    let new_inner_buffer: Buffer = if parent_free_ok {
        parent.buffer
    } else if buffer_is_valid(parent.buffer) {
        let flags = GBUF_INNER_PARITY(parent.blkno + 1) | if is_nulls { GBUF_NULLS } else { 0 };
        SpGistGetBuffer(
            mcx,
            index,
            flags,
            (inner_tuple_size + SIZEOF_ITEM_ID_DATA) as i32,
            &mut init_inner,
        )?
    } else {
        InvalidBuffer
    };

    // Compute current page free space (after the deletions we will do).
    let current_free_space: usize = if !is_root {
        PageGetExactFreeSpace(&PageRef::new(&cur_page)?) + space_to_delete
    } else {
        0
    };

    // Choose how to distribute leaf tuples across pages.
    let mut init_dest = false;
    let mut leaf_page_select: Vec<u8> = alloc::vec![0u8; n_tuples];
    let mut n_to_insert: usize;
    let new_leaf_buffer: Buffer;

    if total_leaf_sizes <= current_free_space {
        // All on the current page.
        new_leaf_buffer = InvalidBuffer;
        n_to_insert = max_to_include;
        if include_new {
            n_to_insert += 1;
            inserted_new = true;
        }
        for x in leaf_page_select.iter_mut().take(n_to_insert) {
            *x = 0;
        }
    } else if n_tuples == 1 && total_leaf_sizes > SPGIST_PAGE_CAPACITY {
        // Single long value that doesn't fit even alone: caller must suffix it.
        debug_assert!(include_new);
        new_leaf_buffer = InvalidBuffer;
        n_to_insert = 0;
    } else {
        // Need a second leaf page.
        let flags = GBUF_LEAF | if is_nulls { GBUF_NULLS } else { 0 };
        new_leaf_buffer = SpGistGetBuffer(
            mcx,
            index,
            flags,
            core::cmp::min(total_leaf_sizes, SPGIST_PAGE_CAPACITY) as i32,
            &mut init_dest,
        )?;

        let mut node_page_select: Vec<u8> = alloc::vec![0u8; out.nNodes as usize];

        // Try assigning, including the new tuple; if it doesn't fit, retry
        // without it.
        let mut leaf_sizes_work = leaf_sizes.clone();
        let assign = |sizes: &[usize], sel: &mut [u8]| -> bool {
            let mut curspace = current_free_space as i64;
            let mut newspace = SPGIST_PAGE_CAPACITY as i64;
            for (i, &sz) in sizes.iter().enumerate() {
                if (sz as i64) <= curspace {
                    sel[i] = 0;
                    curspace -= sz as i64;
                } else {
                    sel[i] = 1;
                    newspace -= sz as i64;
                }
            }
            curspace >= 0 && newspace >= 0
        };

        let ok = assign(&leaf_sizes_work, &mut node_page_select);
        if ok {
            n_to_insert = max_to_include;
            if include_new {
                n_to_insert += 1;
                inserted_new = true;
            }
        } else if include_new {
            // Exclude the new tuple from this split, redo the assignment.
            let new_node = out.mapTuplesToNodes[n_tuples - 1] as usize;
            leaf_sizes_work[new_node] -=
                new_leafs[n_tuples - 1].len() + SIZEOF_ITEM_ID_DATA;
            let ok2 = assign(&leaf_sizes_work, &mut node_page_select);
            if !ok2 {
                return Err(elog_error(
                    "failed to divide leaf tuple groups across pages".into(),
                ));
            }
            n_to_insert = n_tuples - 1;
        } else {
            return Err(elog_error(
                "failed to divide leaf tuple groups across pages".into(),
            ));
        }

        // Expand the per-node selection to per-tuple.
        for i in 0..n_to_insert {
            leaf_page_select[i] = node_page_select[out.mapTuplesToNodes[i] as usize];
        }
    }

    // --- begin WAL record prep ---
    let mut x_n_delete: u16 = 0;
    let mut init_src = is_new;
    let x_stores_nulls = is_nulls;
    let x_is_root_split = is_root;

    let mut leafdata: Vec<u8> = Vec::with_capacity(total_leaf_sizes);

    miscinit::start_crit_section::call();

    // Delete the old tuples from the current page (skip for root split — the
    // page is reinitialized below).
    let mut redirect_tuple_pos = InvalidOffsetNumber;
    if !is_root {
        let n_placeholder = {
            let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
            opaque_n_placeholder(&pg)
        };
        let cur_max = {
            let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
            PageGetMaxOffsetNumber(&PageRef::new(&pg)?)
        };
        if state.isBuild && (n_to_delete + n_placeholder as usize) == cur_max as usize {
            // Just reinitialize the page.
            SpGistInitBuffer(
                current.buffer,
                SPGIST_LEAF | if is_nulls { SPGIST_NULLS } else { 0 },
            )?;
            init_src = true;
        } else if is_new {
            // Don't expose a freshly-init'd buffer as a backup block; nothing
            // to delete.
            debug_assert_eq!(n_to_delete, 0);
        } else {
            x_n_delete = n_to_delete as u16;
            if !state.isBuild {
                if n_to_delete > 0 {
                    redirect_tuple_pos = to_delete[0];
                }
                bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
                    spgPageIndexMultiDelete(
                        state,
                        pg,
                        &to_delete,
                        SPGIST_REDIRECT,
                        SPGIST_PLACEHOLDER,
                        SPGIST_METAPAGE_BLKNO,
                        FirstOffsetNumber,
                    )
                })?;
            } else {
                bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
                    spgPageIndexMultiDelete(
                        state,
                        pg,
                        &to_delete,
                        SPGIST_PLACEHOLDER,
                        SPGIST_PLACEHOLDER,
                        InvalidBlockNumber,
                        InvalidOffsetNumber,
                    )
                })?;
            }
        }
    }

    // Place the leaf tuples + set the node downlinks (held in `nodes` images,
    // copied into the inner tuple at the end).
    let mut start_offsets = [InvalidOffsetNumber, InvalidOffsetNumber];
    let mut to_insert: Vec<OffsetNumber> = alloc::vec![InvalidOffsetNumber; n_to_insert];
    // Mutable node t_tid downlinks, indexed by node number.
    let mut node_tids: Vec<Option<(BlockNumber, OffsetNumber)>> =
        alloc::vec![None; out.nNodes as usize];

    for i in 0..n_to_insert {
        let it_img = &new_leafs[i];
        let sel = leaf_page_select[i] as usize;
        let leaf_buffer = if sel == 1 { new_leaf_buffer } else { current.buffer };
        let leaf_block = bufmgr::buffer_get_block_number::call(leaf_buffer);
        let n = out.mapTuplesToNodes[i] as usize;

        // Chain into the existing node downlink, if any.
        let mut it_to_add = it_img.clone();
        if let Some((blk, off)) = node_tids[n] {
            debug_assert_eq!(blk, leaf_block);
            lt_set_next_offset(&mut it_to_add, off);
        } else {
            lt_set_next_offset(&mut it_to_add, InvalidOffsetNumber);
        }

        let it_len = it_to_add.len();
        let mut newoffset = InvalidOffsetNumber;
        let mut so = start_offsets[sel];
        bufmgr::with_buffer_page::call(leaf_buffer, &mut |pg: &mut [u8]| {
            newoffset = SpGistPageAddNewItem(
                state,
                pg,
                &it_to_add,
                it_len as Size,
                Some(&mut so),
                false,
            )?;
            Ok(())
        })?;
        start_offsets[sel] = so;
        to_insert[i] = newoffset;
        node_tids[n] = Some((leaf_block, newoffset));
        leafdata.extend_from_slice(&it_to_add);
    }

    // Write the computed node downlinks back into the inner-tuple image.
    {
        let node_offs = node_offsets(&inner_tuple);
        for (n, tid) in node_tids.iter().enumerate() {
            if let Some((blk, off)) = *tid {
                node_set_t_tid(&mut inner_tuple[node_offs[n]..], blk, off);
            }
        }
    }

    if buffer_is_valid(new_leaf_buffer) {
        bufmgr::mark_buffer_dirty::call(new_leaf_buffer);
    }

    let save_current = *current;

    // Store the inner tuple — three cases.
    let mut x_offnum_inner;
    let mut x_inner_is_parent = false;
    let mut x_offnum_parent = InvalidOffsetNumber;
    let mut x_node_i: u16 = 0;

    if new_inner_buffer == parent.buffer && buffer_is_valid(new_inner_buffer) {
        // (a) Inner goes to parent page.
        current.blkno = parent.blkno;
        current.buffer = parent.buffer;
        let inner_img = inner_tuple.clone();
        let inner_len = inner_img.len();
        let mut off = InvalidOffsetNumber;
        bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
            off = SpGistPageAddNewItem(state, pg, &inner_img, inner_len as Size, None, false)?;
            Ok(())
        })?;
        current.offnum = off;
        x_offnum_inner = off;
        x_inner_is_parent = true;
        x_offnum_parent = parent.offnum;
        x_node_i = parent.node as u16;
        saveNodeLink(parent, current.blkno, current.offnum)?;
        if redirect_tuple_pos != InvalidOffsetNumber {
            setRedirectionTuple(&save_current, redirect_tuple_pos, current.blkno, current.offnum)?;
        }
        bufmgr::mark_buffer_dirty::call(save_current.buffer);
    } else if buffer_is_valid(parent.buffer) {
        // (b) Inner on a new page.
        debug_assert!(buffer_is_valid(new_inner_buffer));
        current.buffer = new_inner_buffer;
        current.blkno = bufmgr::buffer_get_block_number::call(current.buffer);
        let inner_img = inner_tuple.clone();
        let inner_len = inner_img.len();
        let mut off = InvalidOffsetNumber;
        bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
            off = SpGistPageAddNewItem(state, pg, &inner_img, inner_len as Size, None, false)?;
            Ok(())
        })?;
        current.offnum = off;
        x_offnum_inner = off;
        bufmgr::mark_buffer_dirty::call(current.buffer);
        x_inner_is_parent = parent.buffer == current.buffer;
        x_offnum_parent = parent.offnum;
        x_node_i = parent.node as u16;
        saveNodeLink(parent, current.blkno, current.offnum)?;
        if redirect_tuple_pos != InvalidOffsetNumber {
            setRedirectionTuple(&save_current, redirect_tuple_pos, current.blkno, current.offnum)?;
        }
        bufmgr::mark_buffer_dirty::call(save_current.buffer);
    } else {
        // (c) Root page split.
        debug_assert!(is_root && redirect_tuple_pos == InvalidOffsetNumber);
        SpGistInitBuffer(current.buffer, if is_nulls { SPGIST_NULLS } else { 0 })?;
        init_inner = true;
        x_inner_is_parent = false;
        let inner_img = inner_tuple.clone();
        let inner_len = inner_img.len();
        let mut off = InvalidOffsetNumber;
        bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
            let mut pm = PageMut::new(pg)?;
            off = PageAddItemExtended(&mut pm, &inner_img, InvalidOffsetNumber, 0)?;
            Ok(())
        })?;
        if off != FirstOffsetNumber {
            return Err(elog_error(format!(
                "failed to add item of size {inner_len} to SPGiST index page"
            )));
        }
        x_offnum_inner = off;
        current.offnum = off;
        x_offnum_parent = InvalidOffsetNumber;
        x_node_i = 0;
        bufmgr::mark_buffer_dirty::call(current.buffer);
    }
    let _ = &mut x_offnum_inner;

    // --- WAL ---
    if relation_needs_wal(index) && !state.isBuild {
        let xlrec = spgxlogPickSplit {
            isRootSplit: x_is_root_split,
            nDelete: x_n_delete,
            nInsert: n_to_insert as u16,
            offnumInner: x_offnum_inner,
            initInner: init_inner,
            storesNulls: x_stores_nulls,
            innerIsParent: x_inner_is_parent,
            offnumParent: x_offnum_parent,
            nodeI: x_node_i,
        };
        let state_src = store_state(state);

        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_data::call(&xlrec.to_bytes(init_src, init_dest, &state_src))?;
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&to_delete[..x_n_delete as usize]))?;
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&to_insert))?;
        xloginsert::xlog_register_data::call(&leaf_page_select[..n_to_insert])?;
        xloginsert::xlog_register_data::call(&inner_tuple[..inner_tuple.len()])?;
        xloginsert::xlog_register_data::call(&leafdata)?;

        // Old leaf page (only if not root split).
        if buffer_is_valid(save_current.buffer) && !is_root {
            let mut flags = REGBUF_STANDARD;
            if init_src {
                flags |= REGBUF_WILL_INIT;
            }
            xloginsert::xlog_register_buffer::call(0, save_current.buffer, flags)?;
        }
        // New leaf page.
        if buffer_is_valid(new_leaf_buffer) {
            let mut flags = REGBUF_STANDARD;
            if init_dest {
                flags |= REGBUF_WILL_INIT;
            }
            xloginsert::xlog_register_buffer::call(1, new_leaf_buffer, flags)?;
        }
        // Inner page.
        {
            let mut flags = REGBUF_STANDARD;
            if init_inner {
                flags |= REGBUF_WILL_INIT;
            }
            xloginsert::xlog_register_buffer::call(2, current.buffer, flags)?;
        }
        // Parent page, if different from inner page.
        if buffer_is_valid(parent.buffer) && parent.buffer != current.buffer {
            xloginsert::xlog_register_buffer::call(3, parent.buffer, REGBUF_STANDARD)?;
        }

        let recptr = xloginsert::xlog_insert_record::call(RM_SPGIST_ID, XLOG_SPGIST_PICKSPLIT)?;

        if buffer_is_valid(new_leaf_buffer) {
            bufmgr::page_set_lsn::call(new_leaf_buffer, recptr)?;
        }
        if buffer_is_valid(save_current.buffer) && !is_root {
            bufmgr::page_set_lsn::call(save_current.buffer, recptr)?;
        }
        bufmgr::page_set_lsn::call(current.buffer, recptr)?;
        if buffer_is_valid(parent.buffer) && parent.buffer != current.buffer {
            bufmgr::page_set_lsn::call(parent.buffer, recptr)?;
        }
    }

    miscinit::end_crit_section::call();

    // Release the extra buffers we acquired.
    if buffer_is_valid(new_leaf_buffer) {
        SpGistSetLastUsedPage(mcx, index, new_leaf_buffer)?;
        bufmgr::unlock_release_buffer::call(new_leaf_buffer);
    }
    if buffer_is_valid(save_current.buffer) && save_current.buffer != current.buffer {
        SpGistSetLastUsedPage(mcx, index, save_current.buffer)?;
        bufmgr::unlock_release_buffer::call(save_current.buffer);
    }

    let _ = (n_to_insert_collected, parent);
    Ok(inserted_new)
}

/// Read a leaf tuple's heapPtr (ItemPointerData @6) from its on-disk image.
fn leaf_heap_ptr(tup: &[u8]) -> ItemPointerData {
    ItemPointerData {
        ip_blkid: types_tuple::heaptuple::BlockIdData {
            bi_hi: u16::from_ne_bytes([tup[6], tup[7]]),
            bi_lo: u16::from_ne_bytes([tup[8], tup[9]]),
        },
        ip_posid: u16::from_ne_bytes([tup[10], tup[11]]),
    }
}

// ===========================================================================
// spgMatchNodeAction (static)
// ===========================================================================

/// `spgMatchNodeAction(index, state, innerTuple, current, parent, nodeN)`
/// (spgdoinsert.c:1458) — descend into the `nodeN`'th child of the inner tuple
/// at `current`: make `current` the parent, and point `current` at the child.
fn spgMatchNodeAction<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    inner_tuple: &[u8],
    current: &mut SPPageDesc,
    parent: &mut SPPageDesc,
    node_n: i32,
) -> PgResult<()> {
    // Release the previous parent buffer, if any (and not the same as current).
    if buffer_is_valid(parent.buffer) && parent.buffer != current.buffer {
        SpGistSetLastUsedPage(mcx, index, parent.buffer)?;
        bufmgr::unlock_release_buffer::call(parent.buffer);
    }

    // current becomes the new parent.
    *parent = *current;
    parent.node = node_n;

    // Find the node and its downlink.
    let node_offs = node_offsets(inner_tuple);
    if node_n < 0 || node_n as usize >= node_offs.len() {
        return Err(elog_error(format!(
            "failed to find requested node {node_n} in SPGiST inner tuple"
        )));
    }
    let node = &inner_tuple[node_offs[node_n as usize]..];
    let tid = node_t_tid(node);
    if ItemPointerIsValid(Some(&tid)) {
        current.blkno = ItemPointerGetBlockNumber(&tid);
        current.offnum = ItemPointerGetOffsetNumber(&tid);
    } else {
        current.blkno = InvalidBlockNumber;
        current.offnum = InvalidOffsetNumber;
    }
    current.buffer = InvalidBuffer;
    Ok(())
}

// ===========================================================================
// spgAddNodeAction (static)
// ===========================================================================

/// `spgAddNodeAction(index, state, innerTuple, current, parent, nodeN, nodeLabel)`
/// (spgdoinsert.c:1512) — add a node to the inner tuple at `current` (in place if
/// it fits, else by moving the inner tuple to a new page leaving a redirect).
fn spgAddNodeAction<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    state: &mut SpGistState<'mcx>,
    inner_tuple: &[u8],
    current: &mut SPPageDesc,
    parent: &mut SPPageDesc,
    node_n: i32,
    node_label: &Datum<'_>,
) -> PgResult<()> {
    {
        let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
        debug_assert!(!SpGistPageStoresNulls(&pg));
    }
    let old_size = it_size(inner_tuple);
    let new_inner_tuple = addNode(mcx, state, inner_tuple, node_label, node_n)?;
    let new_size = new_inner_tuple.len();

    let mut xlrec = spgxlogAddNode {
        offnum: current.offnum,
        offnumNew: InvalidOffsetNumber,
        newPage: false,
        parentBlk: -1,
        offnumParent: InvalidOffsetNumber,
        nodeI: 0,
    };
    let state_src = store_state(state);

    let free_space = {
        let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
        PageGetExactFreeSpace(&PageRef::new(&pg)?)
    };

    if free_space >= new_size - old_size {
        // Replace in place.
        let cur_off = current.offnum;
        miscinit::start_crit_section::call();
        bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
            let mut pm = PageMut::new(pg)?;
            PageIndexTupleDelete(&mut pm, cur_off)?;
            let added = {
                let mut pm2 = PageMut::new(pg)?;
                PageAddItemExtended(&mut pm2, &new_inner_tuple, cur_off, 0)?
            };
            if added != cur_off {
                return Err(elog_error(format!(
                    "failed to add item of size {new_size} to SPGiST index page"
                )));
            }
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(current.buffer);

        if relation_needs_wal(index) && !state.isBuild {
            xloginsert::xlog_begin_insert::call()?;
            xloginsert::xlog_register_data::call(&xlrec.to_bytes(&state_src))?;
            xloginsert::xlog_register_data::call(&new_inner_tuple[..new_size])?;
            xloginsert::xlog_register_buffer::call(0, current.buffer, REGBUF_STANDARD)?;
            let recptr = xloginsert::xlog_insert_record::call(RM_SPGIST_ID, XLOG_SPGIST_ADD_NODE)?;
            bufmgr::page_set_lsn::call(current.buffer, recptr)?;
        }
        miscinit::end_crit_section::call();
    } else {
        // Move inner tuple to another page.
        if SpGistBlockIsRoot(current.blkno) {
            return Err(elog_error("cannot enlarge root tuple any more".into()));
        }
        debug_assert!(buffer_is_valid(parent.buffer));

        let save_current = *current;
        xlrec.offnumParent = parent.offnum;
        xlrec.nodeI = parent.node as u16;

        let flags = GBUF_INNER_PARITY(current.blkno);
        let mut new_page = false;
        let new_buffer = SpGistGetBuffer(
            mcx,
            index,
            flags,
            (new_size + SIZEOF_ITEM_ID_DATA) as i32,
            &mut new_page,
        )?;
        xlrec.newPage = new_page;
        current.buffer = new_buffer;
        current.blkno = bufmgr::buffer_get_block_number::call(current.buffer);
        if current.blkno == save_current.blkno {
            return Err(elog_error(
                "SPGiST new buffer shouldn't be same as old buffer".into(),
            ));
        }

        xlrec.parentBlk = if parent.buffer == save_current.buffer {
            0
        } else if parent.buffer == current.buffer {
            1
        } else {
            2
        };

        miscinit::start_crit_section::call();
        let inner_len = new_size;
        let mut off = InvalidOffsetNumber;
        bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
            off = SpGistPageAddNewItem(state, pg, &new_inner_tuple, inner_len as Size, None, false)?;
            Ok(())
        })?;
        current.offnum = off;
        xlrec.offnumNew = off;
        bufmgr::mark_buffer_dirty::call(current.buffer);

        saveNodeLink(parent, current.blkno, current.offnum)?;

        // Replace the old inner tuple with a dead (redirect/placeholder) tuple.
        let dead_state = if state.isBuild {
            SPGIST_PLACEHOLDER
        } else {
            SPGIST_REDIRECT
        };
        let (dt_blk, dt_off) = if state.isBuild {
            (InvalidBlockNumber, InvalidOffsetNumber)
        } else {
            (current.blkno, current.offnum)
        };
        spgFormDeadTuple(state, dead_state, dt_blk, dt_off);
        let dead = state
            .deadTupleStorage
            .as_ref()
            .expect("spgAddNodeAction: deadTupleStorage is NULL")
            .clone();
        let sc_off = save_current.offnum;
        bufmgr::with_buffer_page::call(save_current.buffer, &mut |pg: &mut [u8]| {
            let mut pm = PageMut::new(pg)?;
            PageIndexTupleDelete(&mut pm, sc_off)?;
            let added = {
                let mut pm2 = PageMut::new(pg)?;
                PageAddItemExtended(&mut pm2, &dead, sc_off, 0)?
            };
            if added != sc_off {
                return Err(elog_error(
                    "failed to add item of size to SPGiST index page".into(),
                ));
            }
            // Bump the relevant counter.
            if dead_state == SPGIST_PLACEHOLDER {
                set_opaque_n_placeholder(pg, opaque_n_placeholder(pg) + 1);
            } else {
                set_opaque_n_redirection(pg, opaque_n_redirection(pg) + 1);
            }
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(save_current.buffer);

        if relation_needs_wal(index) && !state.isBuild {
            xloginsert::xlog_begin_insert::call()?;
            xloginsert::xlog_register_buffer::call(0, save_current.buffer, REGBUF_STANDARD)?;
            let mut flags = REGBUF_STANDARD;
            if xlrec.newPage {
                flags |= REGBUF_WILL_INIT;
            }
            xloginsert::xlog_register_buffer::call(1, current.buffer, flags)?;
            if xlrec.parentBlk == 2 {
                xloginsert::xlog_register_buffer::call(2, parent.buffer, REGBUF_STANDARD)?;
            }
            xloginsert::xlog_register_data::call(&xlrec.to_bytes(&state_src))?;
            xloginsert::xlog_register_data::call(&new_inner_tuple[..new_size])?;
            let recptr = xloginsert::xlog_insert_record::call(RM_SPGIST_ID, XLOG_SPGIST_ADD_NODE)?;
            bufmgr::page_set_lsn::call(current.buffer, recptr)?;
            bufmgr::page_set_lsn::call(parent.buffer, recptr)?;
            bufmgr::page_set_lsn::call(save_current.buffer, recptr)?;
        }
        miscinit::end_crit_section::call();

        // Release the old buffer if not retained as parent/current.
        if save_current.buffer != current.buffer && save_current.buffer != parent.buffer {
            SpGistSetLastUsedPage(mcx, index, save_current.buffer)?;
            bufmgr::unlock_release_buffer::call(save_current.buffer);
        }
    }
    Ok(())
}

// ===========================================================================
// spgSplitNodeAction (static)
// ===========================================================================

/// `spgSplitNodeAction(index, state, innerTuple, current, out)`
/// (spgdoinsert.c:1714) — implement the `spgSplitTuple` action: replace the
/// inner tuple at `current` with a shorter-prefix tuple, and chain a "postfix"
/// inner tuple (carrying the original nodes) below it.
fn spgSplitNodeAction<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    state: &mut SpGistState<'mcx>,
    inner_tuple: &[u8],
    current: &mut SPPageDesc,
    out: &spgChooseOut<'mcx>,
) -> PgResult<()> {
    {
        let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
        debug_assert!(!SpGistPageStoresNulls(&pg));
    }

    let split = match &out.result {
        spgChooseOutResult::SplitTuple(s) => s,
        _ => unreachable!("spgSplitNodeAction called for non-split result"),
    };

    if split.prefixNNodes < 1 || split.prefixNNodes > SGITMAXNNODES as i32 {
        return Err(elog_error(format!(
            "invalid number of prefix nodes: {}",
            split.prefixNNodes
        )));
    }
    if split.childNodeN < 0 || split.childNodeN >= split.prefixNNodes {
        return Err(elog_error(format!(
            "invalid child node number: {}",
            split.childNodeN
        )));
    }

    // Build the prefix tuple (the new upper inner tuple).
    let mut prefix_nodes: Vec<PgVec<'mcx, u8>> = Vec::with_capacity(split.prefixNNodes as usize);
    for i in 0..split.prefixNNodes as usize {
        let (label, labelisnull) = match split.prefixNodeLabels.as_ref() {
            Some(labels) => (labels[i].clone(), false),
            None => (Datum::null(), true),
        };
        prefix_nodes.push(spgFormNodeTuple(mcx, state, &label, labelisnull)?);
    }
    let prefix_tuple = spgFormInnerTuple(
        mcx,
        state,
        split.prefixHasPrefix,
        &split.prefixPrefixDatum,
        &prefix_nodes,
    )?;
    if prefix_tuple.len() > it_size(inner_tuple) {
        return Err(elog_error("SPGiST inner tuple size error".into()));
    }

    // Build the postfix tuple carrying the original nodes.
    let orig_node_offs = node_offsets(inner_tuple);
    let mut postfix_nodes: Vec<PgVec<'mcx, u8>> = Vec::with_capacity(orig_node_offs.len());
    for &noff in &orig_node_offs {
        let nsz = node_tuple_size(&inner_tuple[noff..]);
        let mut buf = mcx::vec_with_capacity_in(mcx, nsz)?;
        buf.extend_from_slice(&inner_tuple[noff..noff + nsz]);
        postfix_nodes.push(buf);
    }
    let mut postfix_tuple = spgFormInnerTuple(
        mcx,
        state,
        split.postfixHasPrefix,
        &split.postfixPrefixDatum,
        &postfix_nodes,
    )?;
    it_set_all_the_same(&mut postfix_tuple, it_all_the_same(inner_tuple));

    let mut xlrec = spgxlogSplitTuple {
        offnumPrefix: InvalidOffsetNumber,
        offnumPostfix: InvalidOffsetNumber,
        newPage: false,
        postfixBlkSame: false,
    };

    // Decide whether the postfix tuple needs a new page.
    let need_new_page = {
        if SpGistBlockIsRoot(current.blkno) {
            true
        } else {
            let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
            SpGistPageGetFreeSpace(&pg, 1)? + it_size(inner_tuple)
                < prefix_tuple.len() + postfix_tuple.len() + SIZEOF_ITEM_ID_DATA
        }
    };
    let mut new_page = false;
    let new_buffer = if need_new_page {
        let flags = GBUF_INNER_PARITY(current.blkno + 1);
        SpGistGetBuffer(
            mcx,
            index,
            flags,
            (postfix_tuple.len() + SIZEOF_ITEM_ID_DATA) as i32,
            &mut new_page,
        )?
    } else {
        InvalidBuffer
    };
    xlrec.newPage = new_page;

    miscinit::start_crit_section::call();

    // Replace the old inner tuple with the prefix tuple.
    let cur_off = current.offnum;
    let mut prefix_for_page = prefix_tuple.clone();
    let prefix_len = prefix_for_page.len();
    bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
        let mut pm = PageMut::new(pg)?;
        PageIndexTupleDelete(&mut pm, cur_off)?;
        let added = {
            let mut pm2 = PageMut::new(pg)?;
            PageAddItemExtended(&mut pm2, &prefix_for_page, cur_off, 0)?
        };
        if added != cur_off {
            return Err(elog_error(format!(
                "failed to add item of size {prefix_len} to SPGiST index page"
            )));
        }
        Ok(())
    })?;
    xlrec.offnumPrefix = cur_off;

    // Place the postfix tuple.
    let postfix_len = postfix_tuple.len();
    let (postfix_blkno, postfix_offset);
    if !buffer_is_valid(new_buffer) {
        postfix_blkno = current.blkno;
        let mut off = InvalidOffsetNumber;
        bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
            off = SpGistPageAddNewItem(state, pg, &postfix_tuple, postfix_len as Size, None, false)?;
            Ok(())
        })?;
        postfix_offset = off;
        xlrec.offnumPostfix = off;
        xlrec.postfixBlkSame = true;
    } else {
        postfix_blkno = bufmgr::buffer_get_block_number::call(new_buffer);
        let mut off = InvalidOffsetNumber;
        bufmgr::with_buffer_page::call(new_buffer, &mut |pg: &mut [u8]| {
            off = SpGistPageAddNewItem(state, pg, &postfix_tuple, postfix_len as Size, None, false)?;
            Ok(())
        })?;
        postfix_offset = off;
        xlrec.offnumPostfix = off;
        bufmgr::mark_buffer_dirty::call(new_buffer);
        xlrec.postfixBlkSame = false;
    }

    // Set the prefix tuple's child downlink (on the in-memory image for WAL and
    // on the page).
    spgUpdateNodeLink(&mut prefix_for_page, split.childNodeN, postfix_blkno, postfix_offset)?;
    bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
        let item_off = {
            let pr = PageRef::new(pg)?;
            let iid = PageGetItemId(&pr, cur_off)?;
            iid.lp_off() as usize
        };
        spgUpdateNodeLink(&mut pg[item_off..], split.childNodeN, postfix_blkno, postfix_offset)
    })?;
    bufmgr::mark_buffer_dirty::call(current.buffer);

    if relation_needs_wal(index) && !state.isBuild {
        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_data::call(&xlrec.to_bytes())?;
        xloginsert::xlog_register_data::call(&prefix_for_page[..prefix_len])?;
        xloginsert::xlog_register_data::call(&postfix_tuple[..postfix_len])?;

        xloginsert::xlog_register_buffer::call(0, current.buffer, REGBUF_STANDARD)?;
        if buffer_is_valid(new_buffer) {
            let mut flags = REGBUF_STANDARD;
            if xlrec.newPage {
                flags |= REGBUF_WILL_INIT;
            }
            xloginsert::xlog_register_buffer::call(1, new_buffer, flags)?;
        }

        let recptr = xloginsert::xlog_insert_record::call(RM_SPGIST_ID, XLOG_SPGIST_SPLIT_TUPLE)?;
        bufmgr::page_set_lsn::call(current.buffer, recptr)?;
        if buffer_is_valid(new_buffer) {
            bufmgr::page_set_lsn::call(new_buffer, recptr)?;
        }
    }

    miscinit::end_crit_section::call();

    if buffer_is_valid(new_buffer) {
        SpGistSetLastUsedPage(mcx, index, new_buffer)?;
        bufmgr::unlock_release_buffer::call(new_buffer);
    }
    Ok(())
}

// ===========================================================================
// spgdoinsert (exported driver)
// ===========================================================================

/// `spgdoinsert(index, state, heapPtr, datums, isnulls)` (spgdoinsert.c:1913) —
/// insert one heap tuple's index entry into the SP-GiST index. Returns true on
/// success, false if the caller must retry (a concurrency conflict or a pending
/// non-cancel interrupt was hit).
pub fn spgdoinsert<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    state: &mut SpGistState<'mcx>,
    heap_ptr: &ItemPointerData,
    datums: &[Datum<'mcx>],
    isnulls: &[bool],
) -> PgResult<bool> {
    let mut result = true;
    let isnull = isnulls[spgKeyColumn as usize];
    let mut level: i32 = 0;
    let mut num_no_progress_cycles: i32 = 0;

    let natts = state
        .leafTupDesc
        .as_ref()
        .expect("spgdoinsert: leafTupDesc is NULL")
        .natts as usize;

    // Resolve the choose proc once (only needed for non-null keys).
    let choose_proc: RegProcedure = if !isnull {
        relcache::index_getprocid::call(index, 1, SPGIST_CHOOSE_PROC as u16)?
    } else {
        types_core::primitive::InvalidOid
    };

    // Prepare the leaf datum array (key column compressed/detoasted + INCLUDE).
    let mut leaf_datums: Vec<Datum<'mcx>> = alloc::vec![Datum::null(); natts];

    if !isnull {
        let compress_proc =
            relcache::index_getprocid::call(index, 1, SPGIST_COMPRESS_PROC as u16)?;
        if OidIsValid(compress_proc) {
            let coll = index_collation(index, spgKeyColumn as usize);
            leaf_datums[spgKeyColumn as usize] =
                backend_utils_fmgr_fmgr_seams::function_call1_coll_datum::call(
                    mcx,
                    compress_proc,
                    coll,
                    datums[spgKeyColumn as usize].clone_in(mcx)?,
                )?;
        } else {
            debug_assert_eq!(state.attLeafType.type_, state.attType.type_);
            if state.attType.attlen == -1 {
                // Detoast the varlena key value.
                let detoasted = backend_access_common_detoast_seams::detoast_attr::call(
                    mcx,
                    datums[spgKeyColumn as usize].as_ref_bytes(),
                )?;
                leaf_datums[spgKeyColumn as usize] = Datum::ByRef(detoasted);
            } else {
                leaf_datums[spgKeyColumn as usize] =
                    datums[spgKeyColumn as usize].clone_in(mcx)?;
            }
        }
    } else {
        leaf_datums[spgKeyColumn as usize] = Datum::null();
    }

    // INCLUDE columns.
    {
        let leaf_desc = state.leafTupDesc.as_ref().unwrap();
        for i in spgFirstIncludeColumn as usize..natts {
            if !isnulls[i] {
                if leaf_desc.attr(i).attlen == -1 {
                    let detoasted = backend_access_common_detoast_seams::detoast_attr::call(
                        mcx,
                        datums[i].as_ref_bytes(),
                    )?;
                    leaf_datums[i] = Datum::ByRef(detoasted);
                } else {
                    leaf_datums[i] = datums[i].clone_in(mcx)?;
                }
            } else {
                leaf_datums[i] = Datum::null();
            }
        }
    }

    let leaf_size = {
        let leaf_desc = state.leafTupDesc.as_ref().unwrap();
        SpGistGetLeafTupleSize(leaf_desc, &leaf_datums, isnulls)? as usize + SIZEOF_ITEM_ID_DATA
    };
    if leaf_size > SPGIST_PAGE_CAPACITY && (isnull || !state.config.longValuesOK) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "index row size {} exceeds maximum {} for index \"{}\"",
                leaf_size - SIZEOF_ITEM_ID_DATA,
                SPGIST_PAGE_CAPACITY - SIZEOF_ITEM_ID_DATA,
                rel_name(index)
            ))
            .into_error());
    }
    let mut leaf_size = leaf_size;
    let mut best_leaf_size = leaf_size;

    let mut current = SPPageDesc {
        blkno: if isnull {
            SPGIST_NULL_BLKNO
        } else {
            SPGIST_ROOT_BLKNO
        },
        buffer: InvalidBuffer,
        offnum: FirstOffsetNumber,
        node: -1,
    };
    let mut parent = SPPageDesc::invalid();

    backend_access_transam_parallel_rt_seams::check_for_interrupts::call()?;

    loop {
        let mut is_new = false;

        if interrupt_pending() {
            result = false;
            break;
        }

        // Acquire the current buffer.
        if current.blkno == InvalidBlockNumber {
            let flags = GBUF_LEAF | if isnull { GBUF_NULLS } else { 0 };
            current.buffer = SpGistGetBuffer(
                mcx,
                index,
                flags,
                core::cmp::min(leaf_size, SPGIST_PAGE_CAPACITY) as i32,
                &mut is_new,
            )?;
            current.blkno = bufmgr::buffer_get_block_number::call(current.buffer);
        } else if !buffer_is_valid(parent.buffer) {
            current.buffer = bufmgr::read_buffer::call(index, current.blkno)?;
            bufmgr::lock_buffer::call(current.buffer, BUFFER_LOCK_EXCLUSIVE)?;
        } else if current.blkno != parent.blkno {
            current.buffer = bufmgr::read_buffer::call(index, current.blkno)?;
            if !bufmgr::conditional_lock_buffer::call(current.buffer)? {
                // Deadlock avoidance: drop both buffers, retry.
                bufmgr::release_buffer::call(current.buffer);
                bufmgr::lock_buffer::call(parent.buffer, BUFFER_LOCK_UNLOCK)?;
                bufmgr::unlock_release_buffer::call(parent.buffer);
                return Ok(false);
            }
        } else {
            // Same page as parent.
            current.buffer = parent.buffer;
        }

        // Check the page's nulls flag matches.
        let (page_is_leaf, page_stores_nulls) = {
            let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
            (SpGistPageIsLeaf(&pg), SpGistPageStoresNulls(&pg))
        };
        if isnull != page_stores_nulls {
            return Err(elog_error(format!(
                "SPGiST index page {} has wrong nulls flag",
                current.blkno
            )));
        }

        if page_is_leaf {
            // Leaf page: try to place the leaf tuple.
            let mut leaf_tuple =
                spgFormLeafTuple(mcx, state, heap_ptr, &leaf_datums, isnulls)?;
            let lt_sz = leaf_tuple.len();

            let free = {
                let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
                SpGistPageGetFreeSpace(&pg, 1)?
            };
            if lt_sz + SIZEOF_ITEM_ID_DATA <= free {
                addLeafTuple(
                    mcx, index, state, &mut leaf_tuple, &mut current, &parent, isnull, is_new,
                )?;
                break;
            }

            // Doesn't fit: try moveLeafs, else doPickSplit.
            let (size_to_split, n_to_split) = {
                let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
                checkSplitConditions(state, &pg, &current)?
            };
            if size_to_split < SPGIST_PAGE_CAPACITY / 2
                && n_to_split < 64
                && lt_sz + SIZEOF_ITEM_ID_DATA + size_to_split <= SPGIST_PAGE_CAPACITY
            {
                debug_assert!(!is_new);
                moveLeafs(
                    mcx, index, state, &mut current, &parent, &mut leaf_tuple, isnull,
                )?;
                break;
            } else if doPickSplit(
                mcx,
                index,
                state,
                &mut current,
                &mut parent,
                &leaf_tuple,
                level,
                isnull,
                is_new,
            )? {
                break;
            }
            // doPickSplit returned false: current is now an inner tuple; fall
            // through to process it.
            debug_assert!({
                let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
                !SpGistPageIsLeaf(&pg)
            });
        }

        // Inner page (or doPickSplit produced an inner tuple): process it.
        'inner: loop {
            if interrupt_pending() {
                result = false;
                break;
            }

            let inner_tuple: Vec<u8> = {
                let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
                let pr = PageRef::new(&pg)?;
                let iid = PageGetItemId(&pr, current.offnum)?;
                let it = PageGetItem(&pr, &iid)?;
                it[..it_size(it)].to_vec()
            };

            let all_the_same = it_all_the_same(&inner_tuple);
            let n_nodes = it_n_nodes(&inner_tuple) as i32;

            let mut out = if !isnull {
                let in_ = spgChooseIn {
                    datum: datums[spgKeyColumn as usize].clone_in(mcx)?,
                    leafDatum: leaf_datums[spgKeyColumn as usize].clone_in(mcx)?,
                    level,
                    allTheSame: all_the_same,
                    hasPrefix: it_prefix_size(&inner_tuple) > 0,
                    prefixDatum: it_datum(mcx, state, &inner_tuple)?,
                    nNodes: n_nodes,
                    nodeLabels: spgExtractNodeLabels(mcx, state, &inner_tuple)?
                        .map(|v| v.to_vec()),
                };
                let mut out = spgChooseOut {
                    result: spgChooseOutResult::MatchNode(
                        types_spgist::spgChooseOutMatchNode {
                            nodeN: 0,
                            levelAdd: 0,
                            restDatum: Datum::null(),
                        },
                    ),
                };
                backend_access_spg_core_seams::spg_choose::call(mcx, choose_proc, &in_, &mut out)?;
                out
            } else {
                // Force a match into a random subnode.
                spgChooseOut {
                    result: spgChooseOutResult::MatchNode(
                        types_spgist::spgChooseOutMatchNode {
                            nodeN: 0,
                            levelAdd: 0,
                            restDatum: Datum::null(),
                        },
                    ),
                }
            };

            // allTheSame fixups.
            if all_the_same {
                match &out.result {
                    spgChooseOutResult::AddNode(_) => {
                        return Err(elog_error(
                            "cannot add a node to an allTheSame inner tuple".into(),
                        ));
                    }
                    spgChooseOutResult::MatchNode(_) => {
                        let r =
                            pg_prng::global_prng(|p| p.u64_range(0, (n_nodes - 1) as u64)) as i32;
                        out.result = spgChooseOutResult::MatchNode(
                            types_spgist::spgChooseOutMatchNode {
                                nodeN: r,
                                levelAdd: 0,
                                restDatum: Datum::null(),
                            },
                        );
                    }
                    _ => {}
                }
            }

            match out.resultType() {
                spgChooseResultType::spgMatchNode => {
                    let m = match out.result {
                        spgChooseOutResult::MatchNode(m) => m,
                        _ => unreachable!(),
                    };
                    spgMatchNodeAction(
                        mcx, index, &inner_tuple, &mut current, &mut parent, m.nodeN,
                    )?;
                    level += m.levelAdd;
                    if !isnull {
                        leaf_datums[spgKeyColumn as usize] = m.restDatum.clone_in(mcx)?;
                        leaf_size = {
                            let leaf_desc = state.leafTupDesc.as_ref().unwrap();
                            SpGistGetLeafTupleSize(leaf_desc, &leaf_datums, isnulls)? as usize
                                + SIZEOF_ITEM_ID_DATA
                        };
                        if leaf_size > SPGIST_PAGE_CAPACITY {
                            if state.config.longValuesOK && !isnull {
                                if leaf_size < best_leaf_size {
                                    best_leaf_size = leaf_size;
                                    num_no_progress_cycles = 0;
                                } else {
                                    num_no_progress_cycles += 1;
                                    if num_no_progress_cycles >= 10 {
                                        return Err(long_value_error(index, leaf_size));
                                    }
                                }
                            } else {
                                return Err(long_value_error(index, leaf_size));
                            }
                        }
                    }
                    break 'inner; // re-acquire current and loop
                }
                spgChooseResultType::spgAddNode => {
                    let a = match out.result {
                        spgChooseOutResult::AddNode(a) => a,
                        _ => unreachable!(),
                    };
                    // C errors if the inner tuple has no node labels
                    // (`in.nodeLabels == NULL`).
                    if node_labels_absent(&inner_tuple) {
                        return Err(elog_error(
                            "cannot add a node to an inner tuple without node labels".into(),
                        ));
                    }
                    spgAddNodeAction(
                        mcx,
                        index,
                        state,
                        &inner_tuple,
                        &mut current,
                        &mut parent,
                        a.nodeN,
                        &a.nodeLabel,
                    )?;
                    // goto process_inner_tuple — re-read current's inner tuple.
                    continue 'inner;
                }
                spgChooseResultType::spgSplitTuple => {
                    spgSplitNodeAction(mcx, index, state, &inner_tuple, &mut current, &out)?;
                    continue 'inner;
                }
            }
        }
        if !result {
            break;
        }
    }

    // Release held buffers (beware current == parent same buffer).
    if buffer_is_valid(current.buffer) {
        SpGistSetLastUsedPage(mcx, index, current.buffer)?;
        bufmgr::unlock_release_buffer::call(current.buffer);
    }
    if buffer_is_valid(parent.buffer) && parent.buffer != current.buffer {
        SpGistSetLastUsedPage(mcx, index, parent.buffer)?;
        bufmgr::unlock_release_buffer::call(parent.buffer);
    }

    // A pending query-cancel is thrown here; otherwise we return false to retry.
    backend_access_transam_parallel_rt_seams::check_for_interrupts::call()?;

    Ok(result)
}

/// `INTERRUPTS_PENDING_CONDITION()` (miscadmin.h) via the globals seam.
fn interrupt_pending() -> bool {
    backend_utils_init_small_seams::interrupt_pending::call()
}

/// The collation for index column `col` (`index->rd_indcollation[col]`).
fn index_collation(index: &Relation<'_>, col: usize) -> Oid {
    index
        .rd_indcollation
        .get(col)
        .copied()
        .unwrap_or(types_core::primitive::InvalidOid)
}

/// The "index row size exceeds maximum" ereport for the long-value retry path.
fn long_value_error(index: &Relation<'_>, leaf_size: usize) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .errmsg(format!(
            "index row size {} exceeds maximum {} for index \"{}\"",
            leaf_size - SIZEOF_ITEM_ID_DATA,
            SPGIST_PAGE_CAPACITY - SIZEOF_ITEM_ID_DATA,
            rel_name(index)
        ))
        .into_error()
}

/// Whether an inner tuple's node labels are all NULL (the C `in.nodeLabels ==
/// NULL` test): true if the first node has the NULL flag.
fn node_labels_absent(inner: &[u8]) -> bool {
    let offs = node_offsets(inner);
    if offs.is_empty() {
        return true;
    }
    node_tuple_has_nulls(&inner[offs[0]..])
}
