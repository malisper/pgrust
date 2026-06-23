//! Port of `src/backend/access/spgist/spgtextproc.c` (PostgreSQL 18.3) — the
//! SP-GiST `text_ops` radix-tree (compressed trie) opclass.
//!
//! In a `text_ops` SP-GiST index, inner tuples can have a prefix which is the
//! common prefix of all strings indexed under that tuple. The node labels
//! represent the next byte of the string(s) after the prefix. The leaf value
//! is the suffix remaining after the inner-tuple prefixes and node labels are
//! stripped off; reconstruct the full string by concatenating, root to leaf,
//! the inner prefixes and node labels, then appending the leaf datum.
//!
//! Special node labels: `-1` means "no more bytes after the prefix-so-far" and
//! `-2` is the dummy label used when splitting an `allTheSame` tuple. Neither
//! contributes to the reconstructed string. (A node label of `0` is also
//! accepted on read for backwards compatibility but never written for new
//! entries.)
//!
//! Every function in the C file is ported 1:1 — identical control flow, branch
//! order, strategy switch arms, message text and SQLSTATE:
//!
//!   * [`spg_text_config`]            (config, `spgtextproc.c:95`)
//!   * [`form_text_datum`]            (static, `spgtextproc.c:112`)
//!   * [`common_prefix`]             (static, `spgtextproc.c:137`)
//!   * [`search_char`]              (static, `spgtextproc.c:157`)
//!   * [`spg_text_choose`]            (choose, `spgtextproc.c:183`)
//!   * [`spg_text_picksplit`]         (picksplit, `spgtextproc.c:332`)
//!   * [`spg_text_inner_consistent`]  (inner consistent, `spgtextproc.c:425`)
//!   * [`spg_text_leaf_consistent`]   (leaf consistent, `spgtextproc.c:573`)
//!
//! ## Idiomatic carriers
//!
//! The C fmgr entry points receive `Pointer`s to the typed `spg*In`/`spg*Out`
//! structs and `palloc` their output arrays. As in the sibling point opclasses
//! (`backend-access-spg-quadtree` / `-kdtree`), the bodies here operate
//! directly on the owned [`spgist`] vocabulary structs, with the `text`
//! payloads carried inside [`::types_tuple::Datum::ByRef`] varlena images and the
//! `int16` node labels inside [`::types_tuple::Datum::ByVal`]. "Allocate an output
//! array" becomes "fill an owned `Vec`".
//!
//! `formTextDatum` builds a `text` varlena image (short or long header, exactly
//! as C) into an `::mcx::PgVec<u8>` wrapped in `Datum::ByRef`; `DatumGetTextPP`
//! becomes the local [`vardata_any`] header-stripping reader. The two cross-file
//! support calls — `DirectFunctionCall2Coll(text_starts_with, ...)` and
//! `varstr_cmp` (varlena.c) — are reached directly through the leaf
//! `::varlena::comparison` bodies; `collate_is_c` /
//! `collation_is_deterministic` come from the `pg-locale` seams.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::spgist::{
    spgChooseIn, spgChooseOut, spgChooseOutAddNode, spgChooseOutMatchNode,
    spgChooseOutResult, spgChooseOutSplitTuple, spgConfigOut, spgInnerConsistentIn,
    spgInnerConsistentOut, spgLeafConsistentIn, spgLeafConsistentOut, spgPickSplitIn,
    spgPickSplitOut,
};
use types_tuple::heaptuple::Datum;

use ::varlena::comparison::{text_starts_with, varstr_cmp};
use pg_locale_seams as locale_seams;

// ===========================================================================
// Support-procedure OIDs (catalog/pg_proc.dat) — the `text_ops` opclass.
// ===========================================================================

/// `F_SPG_TEXT_CONFIG` — `spg_text_config` (pg_proc.dat oid 4027).
pub const F_SPG_TEXT_CONFIG: Oid = 4027;
/// `F_SPG_TEXT_CHOOSE` — `spg_text_choose` (pg_proc.dat oid 4028).
pub const F_SPG_TEXT_CHOOSE: Oid = 4028;
/// `F_SPG_TEXT_PICKSPLIT` — `spg_text_picksplit` (pg_proc.dat oid 4029).
pub const F_SPG_TEXT_PICKSPLIT: Oid = 4029;
/// `F_SPG_TEXT_INNER_CONSISTENT` — `spg_text_inner_consistent` (pg_proc.dat oid 4030).
pub const F_SPG_TEXT_INNER_CONSISTENT: Oid = 4030;
/// `F_SPG_TEXT_LEAF_CONSISTENT` — `spg_text_leaf_consistent` (pg_proc.dat oid 4031).
pub const F_SPG_TEXT_LEAF_CONSISTENT: Oid = 4031;

// ===========================================================================
// Catalog OIDs (catalog/pg_type.h) and strategy numbers (access/stratnum.h).
// ===========================================================================

/// `TEXTOID` (pg_type.h).
const TEXTOID: Oid = 25;
/// `INT2OID` (pg_type.h).
const INT2OID: Oid = 21;

/// `BTLessStrategyNumber` (access/stratnum.h).
const BTLessStrategyNumber: u16 = 1;
/// `BTLessEqualStrategyNumber` (access/stratnum.h).
const BTLessEqualStrategyNumber: u16 = 2;
/// `BTEqualStrategyNumber` (access/stratnum.h).
const BTEqualStrategyNumber: u16 = 3;
/// `BTGreaterEqualStrategyNumber` (access/stratnum.h).
const BTGreaterEqualStrategyNumber: u16 = 4;
/// `BTGreaterStrategyNumber` (access/stratnum.h).
const BTGreaterStrategyNumber: u16 = 5;
/// `RTPrefixStrategyNumber` (access/stratnum.h).
const RTPrefixStrategyNumber: u16 = 28;

/// `SPG_STRATEGY_ADDITION` (spgtextproc.c:82): collation-aware text strategies
/// equal the btree strategy plus 10.
const SPG_STRATEGY_ADDITION: u16 = 10;

/// `SPG_IS_COLLATION_AWARE_STRATEGY(s)` (spgtextproc.c:83).
#[inline]
fn spg_is_collation_aware_strategy(s: u16) -> bool {
    s > SPG_STRATEGY_ADDITION && s != RTPrefixStrategyNumber
}

/// `SPGIST_MAX_PREFIX_LENGTH` (spgtextproc.c:70): `Max(BLCKSZ - 258*16 - 100,
/// 32)`. With the default `BLCKSZ == 8192`: `Max(8192 - 4128 - 100, 32) ==
/// 3964`.
const BLCKSZ: i32 = 8192;
const SPGIST_MAX_PREFIX_LENGTH: i32 = {
    let v = BLCKSZ - 258 * 16 - 100;
    if v > 32 {
        v
    } else {
        32
    }
};

// ===========================================================================
// varlena helpers — the C `DatumGetTextPP` / `formTextDatum` / `VARDATA_ANY`
// macros over the owned `Datum::ByRef` text image.
// ===========================================================================

/// `VARHDRSZ` (`c.h`).
const VARHDRSZ: usize = 4;
/// `VARHDRSZ_SHORT` (`varatt.h`).
const VARHDRSZ_SHORT: usize = 1;
/// `VARATT_SHORT_MAX` (`varatt.h`) — `0x7F`.
const VARATT_SHORT_MAX: usize = 0x7F;

/// `VARDATA_ANY(text) .. VARSIZE_ANY_EXHDR(text)` (varatt.h): the payload bytes
/// of a `text` varlena image, stripping the short (1-byte) or long (4-byte)
/// header. SP-GiST inner/leaf values are produced by [`form_text_datum`] and the
/// scankey query args are detoasted-packed by the executor before reaching here,
/// so only the inline short/long forms occur (mirror C `DatumGetTextPP`, which
/// for an already-packed datum is a no-op).
fn vardata_any(image: &[u8]) -> &[u8] {
    let header = image[0];
    if header & 0x01 == 0x01 {
        // VARATT_IS_1B: short 1-byte-header datum. total = (header >> 1) & 0x7F.
        let total = ((header >> 1) & 0x7F) as usize;
        &image[VARHDRSZ_SHORT..total]
    } else {
        // VARATT_IS_4B_U: plain 4-byte-header datum. (Compressed/external inline
        // forms never occur for SP-GiST text inner/leaf values.)
        let word = u32::from_le_bytes([image[0], image[1], image[2], image[3]]);
        let total = ((word >> 2) & 0x3FFF_FFFF) as usize;
        &image[VARHDRSZ..total]
    }
}

/// `DatumGetTextPP(datum)` → its payload bytes.
#[inline]
fn datum_text_payload<'a>(d: &'a Datum<'_>) -> &'a [u8] {
    vardata_any(d.as_ref_bytes())
}

/// `DatumGetPointer(datum) == NULL` — a NULL/absent reconstructed value. The
/// owned model carries an absent value as the zero by-value word (`Datum::null()
/// == ByVal(0)`); a present text value is always a `ByRef` image.
#[inline]
fn datum_is_null(d: &Datum<'_>) -> bool {
    *d == Datum::null()
}

/// `formTextDatum(data, datalen)` (spgtextproc.c:112) — form a `text` datum from
/// the given not-necessarily-null-terminated bytes, using the short varlena
/// header format if possible.
fn form_text_datum<'mcx>(mcx: Mcx<'mcx>, data: &[u8]) -> PgResult<Datum<'mcx>> {
    let datalen = data.len();
    let image: alloc::vec::Vec<u8> = if datalen + VARHDRSZ_SHORT <= VARATT_SHORT_MAX {
        // SET_VARSIZE_SHORT(p, datalen + VARHDRSZ_SHORT): the 1-byte header is
        // `(len << 1) | 0x01` (SET_VARSIZE_1B, varatt.h:237). `total` is bounded
        // by VARATT_SHORT_MAX (0x7F) on this branch, so the shifted length fits.
        let total = datalen + VARHDRSZ_SHORT;
        let mut v = alloc::vec::Vec::with_capacity(total);
        v.push(((total as u8) << 1) | 0x01);
        v.extend_from_slice(data);
        v
    } else {
        // SET_VARSIZE(p, datalen + VARHDRSZ): the 4-byte length word is
        // `total << 2` (the low two bits stay 0 == VARATT_IS_4B_U).
        let total = datalen + VARHDRSZ;
        let mut v = alloc::vec::Vec::with_capacity(total);
        v.extend_from_slice(&((total as u32) << 2).to_le_bytes());
        v.extend_from_slice(data);
        v
    };
    Ok(Datum::ByRef(::mcx::slice_in(mcx, &image)?))
}

/// Build a long-header (`SET_VARSIZE`) `text` of length `datalen`, payload
/// uninitialised-then-filled. Used by `spg_text_inner_consistent`, which always
/// emits long-format reconstructed values. Returns the mutable image so the
/// caller can poke the payload and adjust the header (`SET_VARSIZE`) afterward.
fn alloc_long_text(maxlen: usize) -> alloc::vec::Vec<u8> {
    let total = VARHDRSZ + maxlen;
    let mut v = alloc::vec![0u8; total];
    let word = (total as u32) << 2;
    v[..VARHDRSZ].copy_from_slice(&word.to_le_bytes());
    v
}

/// `SET_VARSIZE(p, total)` over a long-header image: write the 4-byte length
/// word. `total` is the full image length including the 4-byte header.
#[inline]
fn set_varsize(image: &mut [u8], total: usize) {
    let word = (total as u32) << 2;
    image[..VARHDRSZ].copy_from_slice(&word.to_le_bytes());
}

// ===========================================================================
// spg_text_config (spgtextproc.c:95)
// ===========================================================================

/// `spg_text_config(cfgin, cfg)` — fill the opclass config output.
pub fn spg_text_config(_cfgin: &::spgist::spgConfigIn, cfg: &mut spgConfigOut) {
    cfg.prefixType = TEXTOID;
    cfg.labelType = INT2OID;
    cfg.canReturnData = true;
    // suffixing will shorten long values.
    cfg.longValuesOK = true;
}

// ===========================================================================
// commonPrefix / searchChar (spgtextproc.c:137 / :157)
// ===========================================================================

/// `commonPrefix(a, b, lena, lenb)` (spgtextproc.c:137) — length of the common
/// prefix of byte slices `a` and `b`.
fn common_prefix(a: &[u8], b: &[u8]) -> i32 {
    let mut i = 0usize;
    let n = a.len().min(b.len());
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i as i32
}

/// `searchChar(nodeLabels, nNodes, c, &i)` (spgtextproc.c:157) — binary search
/// the int16-datum node-label array for `c`. On success returns `(true, idx)`;
/// on failure returns `(false, insert_pos)`.
fn search_char(node_labels: &[Datum<'_>], n_nodes: i32, c: i16) -> (bool, i32) {
    let mut stop_low = 0i32;
    let mut stop_high = n_nodes;
    while stop_low < stop_high {
        let stop_middle = (stop_low + stop_high) >> 1;
        let middle = node_labels[stop_middle as usize].as_i16();
        if c < middle {
            stop_high = stop_middle;
        } else if c > middle {
            stop_low = stop_middle + 1;
        } else {
            return (true, stop_middle);
        }
    }
    (false, stop_high)
}

// ===========================================================================
// spg_text_choose (spgtextproc.c:183)
// ===========================================================================

/// `spg_text_choose(in, out)` — decide how to place a new value.
pub fn spg_text_choose<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgChooseIn<'mcx>,
    out: &mut spgChooseOut<'mcx>,
) -> PgResult<()> {
    let in_str = datum_text_payload(&in_.datum);
    let in_size = in_str.len() as i32;
    let mut common_len = 0i32;
    #[allow(unused_assignments)]
    let mut prefix_size = 0i32;
    let node_char: i16;

    // Check for prefix match, set nodeChar to first byte after prefix.
    if in_.hasPrefix {
        let prefix_payload = datum_text_payload(&in_.prefixDatum);
        let prefix_str = prefix_payload;
        prefix_size = prefix_str.len() as i32;

        // commonLen = commonPrefix(inStr + in->level, prefixStr,
        //                          inSize - in->level, prefixSize);
        let in_tail = &in_str[(in_.level as usize)..];
        common_len = common_prefix(in_tail, prefix_str);

        if common_len == prefix_size {
            if in_size - in_.level > common_len {
                node_char =
                    in_str[(in_.level + common_len) as usize] as i16; // unsigned char
            } else {
                node_char = -1;
            }
        } else {
            // Must split tuple because incoming value doesn't match prefix.
            let prefix_has_prefix;
            let prefix_prefix_datum;
            if common_len == 0 {
                prefix_has_prefix = false;
                prefix_prefix_datum = Datum::null();
            } else {
                prefix_has_prefix = true;
                prefix_prefix_datum =
                    form_text_datum(mcx, &prefix_str[..common_len as usize])?;
            }

            let prefix_node_labels =
                alloc::vec![Datum::from_i16(prefix_str[common_len as usize] as i16)];

            let postfix_has_prefix;
            let postfix_prefix_datum;
            if prefix_size - common_len == 1 {
                postfix_has_prefix = false;
                postfix_prefix_datum = Datum::null();
            } else {
                postfix_has_prefix = true;
                postfix_prefix_datum = form_text_datum(
                    mcx,
                    &prefix_str[(common_len + 1) as usize..prefix_size as usize],
                )?;
            }

            out.result = spgChooseOutResult::SplitTuple(spgChooseOutSplitTuple {
                prefixHasPrefix: prefix_has_prefix,
                prefixPrefixDatum: prefix_prefix_datum,
                prefixNNodes: 1,
                prefixNodeLabels: Some(prefix_node_labels),
                childNodeN: 0,
                postfixHasPrefix: postfix_has_prefix,
                postfixPrefixDatum: postfix_prefix_datum,
            });
            return Ok(());
        }
    } else if in_size > in_.level {
        node_char = in_str[in_.level as usize] as i16;
    } else {
        node_char = -1;
    }

    // Look up nodeChar in the node label array.
    let node_labels = in_
        .nodeLabels
        .as_deref()
        .unwrap_or(&[]);
    let (found, i) = search_char(node_labels, in_.nNodes, node_char);
    if found {
        // Descend to existing node.
        let mut level_add = common_len;
        if node_char >= 0 {
            level_add += 1;
        }
        let rest_datum = if in_size - in_.level - level_add > 0 {
            let start = (in_.level + level_add) as usize;
            let len = (in_size - in_.level - level_add) as usize;
            form_text_datum(mcx, &in_str[start..start + len])?
        } else {
            form_text_datum(mcx, &[])?
        };
        out.result = spgChooseOutResult::MatchNode(spgChooseOutMatchNode {
            nodeN: i,
            levelAdd: level_add,
            restDatum: rest_datum,
        });
    } else if in_.allTheSame {
        // Can't use AddNode action, so split the tuple. The upper tuple keeps the
        // same prefix and uses a dummy node label -2 for the lower tuple; the
        // lower tuple has no prefix and the original node labels.
        out.result = spgChooseOutResult::SplitTuple(spgChooseOutSplitTuple {
            prefixHasPrefix: in_.hasPrefix,
            prefixPrefixDatum: in_.prefixDatum.clone(),
            prefixNNodes: 1,
            prefixNodeLabels: Some(alloc::vec![Datum::from_i16(-2)]),
            childNodeN: 0,
            postfixHasPrefix: false,
            postfixPrefixDatum: Datum::null(),
        });
    } else {
        // Add a node for the not-previously-seen nodeChar value.
        out.result = spgChooseOutResult::AddNode(spgChooseOutAddNode {
            nodeLabel: Datum::from_i16(node_char),
            nodeN: i,
        });
    }

    Ok(())
}

// ===========================================================================
// spg_text_picksplit (spgtextproc.c:332)
// ===========================================================================

/// `spgNodePtr` (spgtextproc.c:87) — sort entry for picksplit.
#[derive(Clone)]
struct SpgNodePtr {
    /// The original datum (`d`).
    d: usize,
    /// The original tuple index (`i`).
    i: i32,
    /// The label byte / -1 (`c`).
    c: i16,
}

/// `spg_text_picksplit(in, out)` — split a set of leaf tuples into child nodes.
pub fn spg_text_picksplit<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgPickSplitIn<'mcx>,
    out: &mut spgPickSplitOut<'mcx>,
) -> PgResult<()> {
    let n_tuples = in_.nTuples();
    let text0 = datum_text_payload(&in_.datums[0]);

    // Identify longest common prefix, if any.
    let mut common_len = text0.len() as i32;
    let mut i = 1i32;
    while i < n_tuples && common_len > 0 {
        let texti = datum_text_payload(&in_.datums[i as usize]);
        let tmp = common_prefix(text0, texti);
        if tmp < common_len {
            common_len = tmp;
        }
        i += 1;
    }

    // Limit the prefix length so the resulting inner tuple fits on a page.
    if common_len > SPGIST_MAX_PREFIX_LENGTH {
        common_len = SPGIST_MAX_PREFIX_LENGTH;
    }

    // Set node prefix to that string, if it's not empty.
    if common_len == 0 {
        out.hasPrefix = false;
        out.prefixDatum = None;
    } else {
        out.hasPrefix = true;
        out.prefixDatum = Some(form_text_datum(mcx, &text0[..common_len as usize])?);
    }

    // Extract the node label (first non-common byte) from each value.
    let mut nodes: alloc::vec::Vec<SpgNodePtr> = alloc::vec::Vec::with_capacity(n_tuples as usize);
    for i in 0..n_tuples {
        let texti = datum_text_payload(&in_.datums[i as usize]);
        let c = if common_len < texti.len() as i32 {
            texti[common_len as usize] as i16 // unsigned char
        } else {
            -1 // use -1 if string is all common
        };
        nodes.push(SpgNodePtr {
            d: i as usize, // index into in_.datums (== nodes[].d points at datum i)
            i,
            c,
        });
    }

    // Sort by label value (stable; cmpNodePtr is pg_cmp_s16). qsort is not
    // stable, but the picksplit result only depends on grouping equal labels
    // together and on producing labels in ascending order, both of which a
    // stable sort satisfies identically.
    nodes.sort_by(|a, b| a.c.cmp(&b.c));

    // Emit results.
    out.nNodes = 0;
    let mut node_labels: alloc::vec::Vec<Datum<'mcx>> =
        alloc::vec::Vec::with_capacity(n_tuples as usize);
    let mut map_tuples_to_nodes = alloc::vec![0i32; n_tuples as usize];
    let mut leaf_tuple_datums: alloc::vec::Vec<Datum<'mcx>> =
        alloc::vec![Datum::null(); n_tuples as usize];

    for i in 0..n_tuples as usize {
        let texti = datum_text_payload(&in_.datums[nodes[i].d]);

        if i == 0 || nodes[i].c != nodes[i - 1].c {
            node_labels.push(Datum::from_i16(nodes[i].c));
            out.nNodes += 1;
        }

        let leaf_d = if common_len < texti.len() as i32 {
            let start = (common_len + 1) as usize;
            let len = (texti.len() as i32 - common_len - 1) as usize;
            form_text_datum(mcx, &texti[start..start + len])?
        } else {
            form_text_datum(mcx, &[])?
        };

        leaf_tuple_datums[nodes[i].i as usize] = leaf_d;
        map_tuples_to_nodes[nodes[i].i as usize] = out.nNodes - 1;
    }

    out.nodeLabels = Some(node_labels);
    out.mapTuplesToNodes = map_tuples_to_nodes;
    out.leafTupleDatums = leaf_tuple_datums;
    Ok(())
}

// ===========================================================================
// spg_text_inner_consistent (spgtextproc.c:425)
// ===========================================================================

/// `spg_text_inner_consistent(in, out)` — decide which child nodes to descend.
pub fn spg_text_inner_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgInnerConsistentIn<'mcx>,
    out: &mut spgInnerConsistentOut<'mcx>,
) -> PgResult<()> {
    // bool collate_is_c = pg_newlocale_from_collation(PG_GET_COLLATION())
    //                       ->collate_is_c;
    // PG_GET_COLLATION() is the index collation, carried on every scankey's
    // sk_collation; collate_is_c is only consulted inside the per-key loop, where
    // sk_collation[j] is exactly that collation.
    let collation = in_.scankeys.first().map(|k| k.sk_collation).unwrap_or(0);
    let collate_is_c = if collation != 0 {
        locale_seams::collation_is_c::call(collation)?
    } else {
        false
    };

    // Reconstruct values represented at this tuple: parent data + this tuple's
    // prefix (if any) + the node label (if non-dummy). in->level is the length
    // of the previously reconstructed value.
    let reconstructed_value: Option<&[u8]> = if datum_is_null(&in_.reconstructedValue) {
        None
    } else {
        Some(datum_text_payload(&in_.reconstructedValue))
    };
    debug_assert!(match reconstructed_value {
        None => in_.level == 0,
        Some(rv) => (rv as &[u8]).len() as i32 == in_.level,
    });

    let mut max_reconstr_len = in_.level + 1;
    let prefix_text: Option<&[u8]> = if in_.hasPrefix {
        Some(datum_text_payload(&in_.prefixDatum))
    } else {
        None
    };
    let prefix_size = prefix_text.map(|p| p.len() as i32).unwrap_or(0);
    if in_.hasPrefix {
        max_reconstr_len += prefix_size;
    }

    // reconstrText = palloc(VARHDRSZ + maxReconstrLen);
    // SET_VARSIZE(reconstrText, VARHDRSZ + maxReconstrLen);
    let mut reconstr_text = alloc_long_text(max_reconstr_len as usize);

    if in_.level != 0 {
        let rv = reconstructed_value.expect("level>0 requires reconstructedValue");
        reconstr_text[VARHDRSZ..VARHDRSZ + in_.level as usize]
            .copy_from_slice(&rv[..in_.level as usize]);
    }
    if prefix_size != 0 {
        let p = prefix_text.unwrap();
        let off = VARHDRSZ + in_.level as usize;
        reconstr_text[off..off + prefix_size as usize].copy_from_slice(&p[..prefix_size as usize]);
    }
    // The last byte of reconstrText is filled in per-node below.

    let node_labels = in_.nodeLabels.as_deref().unwrap_or(&[]);

    out.nodeNumbers = alloc::vec::Vec::with_capacity(in_.nNodes as usize);
    out.levelAdds = alloc::vec::Vec::with_capacity(in_.nNodes as usize);
    out.reconstructedValues = alloc::vec::Vec::with_capacity(in_.nNodes as usize);
    out.traversalValues = alloc::vec::Vec::new();
    out.distances = alloc::vec::Vec::new();
    out.nNodes = 0;

    for i in 0..in_.nNodes {
        let node_char = node_labels[i as usize].as_i16();
        let this_len;
        let mut res = true;

        // If nodeChar is a dummy value, don't include it in data.
        if node_char <= 0 {
            this_len = max_reconstr_len - 1;
        } else {
            reconstr_text[VARHDRSZ + (max_reconstr_len - 1) as usize] = node_char as u8;
            this_len = max_reconstr_len;
        }

        for j in 0..in_.scankeys.len() {
            let mut strategy = in_.scankeys[j].sk_strategy;
            // If it's a collation-aware operator but the collation is C, treat it
            // as non-collation-aware. With non-C collation we must traverse the
            // whole tree, so there's no point checking here.
            if spg_is_collation_aware_strategy(strategy) {
                if collate_is_c {
                    strategy -= SPG_STRATEGY_ADDITION;
                } else {
                    continue;
                }
            }

            let in_text = datum_text_payload(&in_.scankeys[j].sk_argument);
            let in_size = in_text.len() as i32;

            // r = memcmp(VARDATA(reconstrText), VARDATA_ANY(inText),
            //            Min(inSize, thisLen));
            let cmp_len = in_size.min(this_len) as usize;
            let lhs = &reconstr_text[VARHDRSZ..VARHDRSZ + cmp_len];
            let rhs = &in_text[..cmp_len];
            let r = mem_cmp(lhs, rhs);

            match strategy {
                BTLessStrategyNumber | BTLessEqualStrategyNumber => {
                    if r > 0 {
                        res = false;
                    }
                }
                BTEqualStrategyNumber => {
                    if r != 0 || in_size < this_len {
                        res = false;
                    }
                }
                BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => {
                    if r < 0 {
                        res = false;
                    }
                }
                RTPrefixStrategyNumber => {
                    if r != 0 {
                        res = false;
                    }
                }
                _ => {
                    return Err(PgError::error(format!(
                        "unrecognized strategy number: {}",
                        in_.scankeys[j].sk_strategy
                    )));
                }
            }

            if !res {
                break; // no need to consider remaining conditions
            }
        }

        if res {
            out.nodeNumbers.push(i);
            out.levelAdds.push(this_len - in_.level);
            // SET_VARSIZE(reconstrText, VARHDRSZ + thisLen); datumCopy(...).
            let total = VARHDRSZ + this_len as usize;
            let mut copy = reconstr_text[..total].to_vec();
            set_varsize(&mut copy, total);
            out.reconstructedValues
                .push(Datum::ByRef(::mcx::slice_in(mcx, &copy)?));
            out.nNodes += 1;
        }
    }

    Ok(())
}

// ===========================================================================
// spg_text_leaf_consistent (spgtextproc.c:573)
// ===========================================================================

/// `spg_text_leaf_consistent(in, out)` — test the leaf datum against the
/// scankeys. Returns the C `DatumGetBool(result)`.
pub fn spg_text_leaf_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgLeafConsistentIn<'mcx>,
    out: &mut spgLeafConsistentOut<'mcx>,
) -> PgResult<bool> {
    let level = in_.level;

    // All tests are exact.
    out.recheck = false;

    let leaf_value = datum_text_payload(&in_.leafDatum);
    let leaf_len = leaf_value.len() as i32;

    // As above, in->reconstructedValue isn't toasted or short.
    let reconstr_value: Option<&[u8]> = if datum_is_null(&in_.reconstructedValue) {
        None
    } else {
        Some(datum_text_payload(&in_.reconstructedValue))
    };
    debug_assert!(match reconstr_value {
        None => level == 0,
        Some(rv) => (rv as &[u8]).len() as i32 == level,
    });

    // Reconstruct the full string represented by this leaf tuple.
    let full_len = level + leaf_len;
    // full_value: the bytes that all comparisons read.
    let full_value: alloc::vec::Vec<u8>;
    if leaf_len == 0 && level > 0 {
        // fullValue = VARDATA(reconstrValue); out->leafValue = reconstrValue.
        let rv = reconstr_value.expect("level>0 requires reconstructedValue");
        full_value = rv.to_vec();
        out.leafValue = Some(in_.reconstructedValue.clone());
    } else {
        // fullText = palloc(VARHDRSZ + fullLen); SET_VARSIZE; memcpy parts.
        let mut buf = alloc::vec::Vec::with_capacity(full_len as usize);
        if level != 0 {
            let rv = reconstr_value.expect("level>0 requires reconstructedValue");
            buf.extend_from_slice(&rv[..level as usize]);
        }
        if leaf_len > 0 {
            buf.extend_from_slice(leaf_value);
        }
        full_value = buf;
        out.leafValue = Some(form_long_text_datum(mcx, &full_value)?);
    }

    // Perform the required comparison(s).
    let mut res = true;
    for j in 0..in_.scankeys.len() {
        let mut strategy = in_.scankeys[j].sk_strategy;
        let query = datum_text_payload(&in_.scankeys[j].sk_argument);
        let query_len = query.len() as i32;
        let collid = in_.scankeys[j].sk_collation;

        if strategy == RTPrefixStrategyNumber {
            // If level >= length of query then reconstrValue must begin with the
            // query (prefix) string, so we don't need to check it again.
            res = (level >= query_len) || {
                // DirectFunctionCall2Coll(text_starts_with, PG_GET_COLLATION(),
                //                         out->leafValue, query)
                // text_starts_with takes payload bytes; out->leafValue payload is
                // full_value, query payload is `query`.
                text_starts_with(&full_value, query, collid)?
            };

            if !res {
                break; // no need to consider remaining conditions
            }
            continue;
        }

        let r;
        if spg_is_collation_aware_strategy(strategy) {
            // Collation-aware comparison.
            strategy -= SPG_STRATEGY_ADDITION;
            r = varstr_cmp(&full_value, query, collid)?;
        } else {
            // Non-collation-aware comparison.
            let cmp_len = query_len.min(full_len) as usize;
            let mut rr = mem_cmp(&full_value[..cmp_len], &query[..cmp_len]);
            if rr == 0 {
                if query_len > full_len {
                    rr = -1;
                } else if query_len < full_len {
                    rr = 1;
                }
            }
            r = rr;
        }

        res = match strategy {
            BTLessStrategyNumber => r < 0,
            BTLessEqualStrategyNumber => r <= 0,
            BTEqualStrategyNumber => r == 0,
            BTGreaterEqualStrategyNumber => r >= 0,
            BTGreaterStrategyNumber => r > 0,
            _ => {
                return Err(PgError::error(format!(
                    "unrecognized strategy number: {}",
                    in_.scankeys[j].sk_strategy
                )));
            }
        };

        if !res {
            break; // no need to consider remaining conditions
        }
    }

    Ok(res)
}

/// Build a long-header `text` datum image from payload bytes (always 4-byte
/// header, as the C `palloc(VARHDRSZ + fullLen) + SET_VARSIZE` leaf path).
fn form_long_text_datum<'mcx>(mcx: Mcx<'mcx>, data: &[u8]) -> PgResult<Datum<'mcx>> {
    let total = VARHDRSZ + data.len();
    let mut image = alloc::vec::Vec::with_capacity(total);
    image.extend_from_slice(&((total as u32) << 2).to_le_bytes());
    image.extend_from_slice(data);
    Ok(Datum::ByRef(::mcx::slice_in(mcx, &image)?))
}

/// `memcmp(a, b, n)` 3-way result over equal-length slices (`-1`/`0`/`1`),
/// mirroring C `memcmp`'s sign convention.
#[inline]
fn mem_cmp(a: &[u8], b: &[u8]) -> i32 {
    match a.cmp(b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

extern crate alloc;

/// This crate installs nothing of its own; the SP-GiST opclass dispatcher
/// (`backend-access-spg-quadtree`, the single installer) routes the
/// `F_SPG_TEXT_*` support-proc OIDs to these bodies. The empty `init_seams()`
/// satisfies the `seams-init` recurrence guard.
pub fn init_seams() {}
