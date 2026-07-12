// AArch64 word emitter: the production execexpr jitq Emitter lineage
// (literal pool + label fixups + patch_branch), ported from the proven
// batchexec POC (poc/batchexec/src/jit/emit.rs) and trimmed to the encodings
// the lanestitch bodies need, plus the float-comparator additions. Every
// non-trivial encoding is pinned against clang in the test table below.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Label(pub u32);

#[derive(Clone, Copy)]
#[allow(dead_code)]
pub enum Cond {
    Eq = 0,
    Ne = 1,
    // Unsigned relations (oid comparators); HS/LO are CS/CC. After an fcmp
    // they double as the float NaN-ordering conds: unordered sets
    // NZCV=0011, so HI/HS read true (PG float.h: a NaN lhs vs a non-NaN
    // const passes gt/ge) while MI/LS read false (lt/le fail on NaN lhs).
    Hs = 2,
    Lo = 3,
    Mi = 4,
    Pl = 5,
    Hi = 8,
    Ls = 9,
    Ge = 10,
    Lt = 11,
    Gt = 12,
    Le = 13,
    Vs = 6,
    Vc = 7,
}

impl Cond {
    /// AArch64 condition inversion (encoding ^ 1 pairs EQ/NE, GE/LT, GT/LE,
    /// HS/LO, HI/LS, MI/PL, VS/VC).
    pub fn inv(self) -> Cond {
        match self {
            Cond::Eq => Cond::Ne,
            Cond::Ne => Cond::Eq,
            Cond::Ge => Cond::Lt,
            Cond::Lt => Cond::Ge,
            Cond::Gt => Cond::Le,
            Cond::Le => Cond::Gt,
            Cond::Hs => Cond::Lo,
            Cond::Lo => Cond::Hs,
            Cond::Hi => Cond::Ls,
            Cond::Ls => Cond::Hi,
            Cond::Mi => Cond::Pl,
            Cond::Pl => Cond::Mi,
            Cond::Vs => Cond::Vc,
            Cond::Vc => Cond::Vs,
        }
    }
}

pub struct Emitter {
    pub code: Vec<u32>,
    fixups: Vec<(usize, Label)>,
    bound: Vec<Option<usize>>,
    lits: Vec<u64>,
    lit_uses: Vec<(usize, u32)>,
}

impl Emitter {
    pub fn new() -> Emitter {
        Emitter {
            code: Vec::with_capacity(256),
            fixups: Vec::new(),
            bound: Vec::new(),
            lits: Vec::new(),
            lit_uses: Vec::new(),
        }
    }

    pub fn new_label(&mut self) -> Label {
        self.bound.push(None);
        Label(self.bound.len() as u32 - 1)
    }

    pub fn bind(&mut self, l: Label) {
        debug_assert!(self.bound[l.0 as usize].is_none(), "label bound twice");
        self.bound[l.0 as usize] = Some(self.code.len());
    }

    pub fn raw(&mut self, w: u32) {
        self.code.push(w);
    }

    // LDR Xt, <literal>: 64-bit constant from the pool appended after code.
    pub fn ldr_lit(&mut self, rt: u32, v: u64) {
        let id = match self.lits.iter().position(|&l| l == v) {
            Some(i) => i as u32,
            None => {
                self.lits.push(v);
                (self.lits.len() - 1) as u32
            }
        };
        self.lit_uses.push((self.code.len(), id));
        self.code.push(0x5800_0000 | rt);
    }

    pub fn ldr_x(&mut self, rt: u32, rn: u32, off: u32) {
        debug_assert!(off % 8 == 0 && off / 8 <= 4095);
        self.raw(0xF940_0000 | ((off / 8) << 10) | (rn << 5) | rt);
    }

    pub fn str_x(&mut self, rt: u32, rn: u32, off: u32) {
        debug_assert!(off % 8 == 0 && off / 8 <= 4095);
        self.raw(0xF900_0000 | ((off / 8) << 10) | (rn << 5) | rt);
    }

    pub fn ldrb(&mut self, rt: u32, rn: u32, off: u32) {
        debug_assert!(off <= 4095);
        self.raw(0x3940_0000 | (off << 10) | (rn << 5) | rt);
    }

    pub fn strb(&mut self, rt: u32, rn: u32, off: u32) {
        debug_assert!(off <= 4095);
        self.raw(0x3900_0000 | (off << 10) | (rn << 5) | rt);
    }

    // LDR Xt, [Xn, Xm, LSL #3]
    pub fn ldr_x_idx3(&mut self, rt: u32, rn: u32, rm: u32) {
        self.raw(0xF860_7800 | (rm << 16) | (rn << 5) | rt);
    }

    // STR Xt, [Xn, Xm, LSL #3]
    pub fn str_x_idx3(&mut self, rt: u32, rn: u32, rm: u32) {
        self.raw(0xF820_7800 | (rm << 16) | (rn << 5) | rt);
    }

    // LDRB Wt, [Xn, Xm] (X index, no extend)
    pub fn ldrb_idx(&mut self, rt: u32, rn: u32, rm: u32) {
        self.raw(0x3860_6800 | (rm << 16) | (rn << 5) | rt);
    }

    pub fn movz_w(&mut self, rd: u32, imm16: u32) {
        debug_assert!(imm16 <= 0xFFFF);
        self.raw(0x5280_0000 | (imm16 << 5) | rd);
    }

    // MOVZ Wd, #imm16, LSL #16
    pub fn movz_w_hw1(&mut self, rd: u32, imm16: u32) {
        debug_assert!(imm16 <= 0xFFFF);
        self.raw(0x52A0_0000 | (imm16 << 5) | rd);
    }

    pub fn movz_x(&mut self, rd: u32, imm16: u32) {
        debug_assert!(imm16 <= 0xFFFF);
        self.raw(0xD280_0000 | (imm16 << 5) | rd);
    }

    // MOVN Xd, #imm16: xd = !imm16 (imm16=0 -> -1).
    pub fn movn_x(&mut self, rd: u32, imm16: u32) {
        self.raw(0x9280_0000 | (imm16 << 5) | rd);
    }

    pub fn mov_x(&mut self, rd: u32, rm: u32) {
        self.raw(0xAA00_03E0 | (rm << 16) | rd);
    }

    pub fn add_x_imm(&mut self, rd: u32, rn: u32, imm12: u32) {
        debug_assert!(imm12 <= 4095);
        self.raw(0x9100_0000 | (imm12 << 10) | (rn << 5) | rd);
    }

    pub fn add_x(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x8B00_0000 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn cmp_w_imm(&mut self, rn: u32, imm12: u32) {
        debug_assert!(imm12 <= 4095);
        self.raw(0x7100_001F | (imm12 << 10) | (rn << 5));
    }

    // CMN Wn, #imm12 (compare against -imm).
    pub fn cmn_w_imm(&mut self, rn: u32, imm12: u32) {
        debug_assert!(imm12 <= 4095);
        self.raw(0x3100_001F | (imm12 << 10) | (rn << 5));
    }

    pub fn cmp_w_w(&mut self, rn: u32, rm: u32) {
        self.raw(0x6B00_001F | (rm << 16) | (rn << 5));
    }

    pub fn cmp_x_x(&mut self, rn: u32, rm: u32) {
        self.raw(0xEB00_001F | (rm << 16) | (rn << 5));
    }

    // CMP Xn, Wm SXTW (int4mul product check).
    pub fn cmp_x_w_sxtw(&mut self, rn: u32, rm: u32) {
        self.raw(0xEB20_C01F | (rm << 16) | (rn << 5));
    }

    pub fn orr_w(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x2A00_0000 | (rm << 16) | (rn << 5) | rd);
    }

    // BIC Xd, Xn, Xm
    pub fn bic_x(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x8A20_0000 | (rm << 16) | (rn << 5) | rd);
    }

    // AND Xd, Xn, #63
    pub fn and_x_63(&mut self, rd: u32, rn: u32) {
        self.raw(0x9240_1400 | (rn << 5) | rd);
    }

    // LSR Xd, Xn, #6
    pub fn lsr_x_6(&mut self, rd: u32, rn: u32) {
        self.raw(0xD346_FC00 | (rn << 5) | rd);
    }

    // LSL Xd, Xn, Xm (LSLV)
    pub fn lslv_x(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x9AC0_2000 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn cset_x(&mut self, rd: u32, cond: Cond) {
        // CSINC Xd, XZR, XZR, inv(cond).
        self.raw(0x9A9F_07E0 | (((cond as u32) ^ 1) << 12) | rd);
    }

    pub fn sxtw(&mut self, xd: u32, wn: u32) {
        self.raw(0x9340_7C00 | (wn << 5) | xd);
    }

    pub fn adds_w(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x2B00_0000 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn subs_w(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x6B00_0000 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn smull(&mut self, xd: u32, wn: u32, wm: u32) {
        self.raw(0x9B20_7C00 | (wm << 16) | (wn << 5) | xd);
    }

    // SDIV Wd, Wn, Wm
    pub fn sdiv_w(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x1AC0_0C00 | (rm << 16) | (rn << 5) | rd);
    }

    // ADD Xd, Xn, Xm, LSL #3 (Datum lane element address).
    pub fn add_x_lsl3(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x8B00_0C00 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn sub_x_imm(&mut self, rd: u32, rn: u32, imm12: u32) {
        debug_assert!(imm12 <= 4095);
        self.raw(0xD100_0000 | (imm12 << 10) | (rn << 5) | rd);
    }

    pub fn sub_x(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0xCB00_0000 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn and_x(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x8A00_0000 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn cmp_x_imm(&mut self, rn: u32, imm12: u32) {
        debug_assert!(imm12 <= 4095);
        self.raw(0xF100_001F | (imm12 << 10) | (rn << 5));
    }

    pub fn rbit_x(&mut self, rd: u32, rn: u32) {
        self.raw(0xDAC0_0000 | (rn << 5) | rd);
    }

    pub fn clz_x(&mut self, rd: u32, rn: u32) {
        self.raw(0xDAC0_1000 | (rn << 5) | rd);
    }

    pub fn orr_x(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0xAA00_0000 | (rm << 16) | (rn << 5) | rd);
    }

    // ---- NEON / FP (SIMD + float-comparator stencils). Encodings
    // clang-verified; see the test table.

    // LDP Qt, Qt2, [Xn, #imm] (imm 16-scaled).
    pub fn ldp_q(&mut self, rt: u32, rt2: u32, rn: u32, imm: u32) {
        debug_assert!(imm % 16 == 0 && imm / 16 <= 63);
        self.raw(0xAD40_0000 | ((imm / 16) << 15) | (rt2 << 10) | (rn << 5) | rt);
    }

    // LDR Dt, [Xn, Xm] (8 isnull bytes).
    pub fn ldr_d_idx(&mut self, rt: u32, rn: u32, rm: u32) {
        self.raw(0xFC60_6800 | (rm << 16) | (rn << 5) | rt);
    }

    pub fn dup_2d_x(&mut self, rd: u32, rn: u32) {
        self.raw(0x4E08_0C00 | (rn << 5) | rd);
    }

    pub fn fmov_d_x(&mut self, vd: u32, rn: u32) {
        self.raw(0x9E67_0000 | (rn << 5) | vd);
    }

    pub fn cmeq_2d(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x6EE0_8C00 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn cmgt_2d(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x4EE0_3400 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn cmge_2d(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x4EE0_3C00 | (rm << 16) | (rn << 5) | rd);
    }

    // CMHI/CMHS Vd.2d: unsigned 2x64 compares (the oid SIMD arm — exact
    // under the both-operands-sign-extended contract: lanes by deform,
    // konsts canonicalized at translation).
    pub fn cmhi_2d(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x6EE0_3400 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn cmhs_2d(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x6EE0_3C00 | (rm << 16) | (rn << 5) | rd);
    }

    // ---- Float comparators (NaN-ordering stencils). The scalar tail and
    // the NEON arm both promote f32 operands to f64 (exact,
    // order-preserving — btfloat48cmp precedent).

    // FMOV Sd, Wn (raw f32 bit pattern from the datum's low word).
    pub fn fmov_s_w(&mut self, vd: u32, rn: u32) {
        self.raw(0x1E27_0000 | (rn << 5) | vd);
    }

    // FCVT Dd, Sn (f32 -> f64 promotion).
    pub fn fcvt_d_s(&mut self, vd: u32, vn: u32) {
        self.raw(0x1E22_C000 | (vn << 5) | vd);
    }

    // FCMP Dn, Dm (sets NZCV; unordered = 0011).
    pub fn fcmp_d(&mut self, vn: u32, vm: u32) {
        self.raw(0x1E60_2000 | (vm << 16) | (vn << 5));
    }

    // NEON float compares, 2x64 lanes (ordered-true / unordered-false).
    pub fn fcmeq_2d(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x4E60_E400 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn fcmgt_2d(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x6EE0_E400 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn fcmge_2d(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x6E60_E400 | (rm << 16) | (rn << 5) | rd);
    }

    // XTN Vd.2s, Vn.2d (low words of two datums: raw f32 patterns).
    pub fn xtn_2s_2d(&mut self, rd: u32, rn: u32) {
        self.raw(0x0EA1_2800 | (rn << 5) | rd);
    }

    // FCVTL Vd.2d, Vn.2s (two f32 -> two f64, exact).
    pub fn fcvtl_2d_2s(&mut self, rd: u32, rn: u32) {
        self.raw(0x0E61_7800 | (rn << 5) | rd);
    }

    // MVN Vd.16b, Vn.16b (mask complement for the not-of-fcm relations).
    pub fn not_16b(&mut self, rd: u32, rn: u32) {
        self.raw(0x6E20_5800 | (rn << 5) | rd);
    }

    // ORR Vd.16b, Vn.16b, Vm.16b (isnan-mask OR for the float gt/ge arm).
    pub fn orr_16b(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x4EA0_1C00 | (rm << 16) | (rn << 5) | rd);
    }

    // MVN Vd.8b, Vn.8b
    pub fn not_8b(&mut self, rd: u32, rn: u32) {
        self.raw(0x2E20_5800 | (rn << 5) | rd);
    }

    pub fn uzp1_4s(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x4E80_1800 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn uzp1_8h(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x4E40_1800 | (rm << 16) | (rn << 5) | rd);
    }

    // XTN Vd.8b, Vn.8h
    pub fn xtn_8b(&mut self, rd: u32, rn: u32) {
        self.raw(0x0E21_2800 | (rn << 5) | rd);
    }

    // CMEQ Vd.8b, Vn.8b, #0
    pub fn cmeq0_8b(&mut self, rd: u32, rn: u32) {
        self.raw(0x0E20_9800 | (rn << 5) | rd);
    }

    pub fn and_8b(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x0E20_1C00 | (rm << 16) | (rn << 5) | rd);
    }

    pub fn orr_8b(&mut self, rd: u32, rn: u32, rm: u32) {
        self.raw(0x0EA0_1C00 | (rm << 16) | (rn << 5) | rd);
    }

    // ADDV Bd, Vn.8b (movemask horizontal add)
    pub fn addv_b_8b(&mut self, rd: u32, rn: u32) {
        self.raw(0x0E31_B800 | (rn << 5) | rd);
    }

    pub fn umov_w_b0(&mut self, rd: u32, vn: u32) {
        self.raw(0x0E01_3C00 | (vn << 5) | rd);
    }

    pub fn ret(&mut self) {
        self.raw(0xD65F_03C0);
    }

    pub fn b(&mut self, l: Label) {
        self.fixups.push((self.code.len(), l));
        self.raw(0x1400_0000);
    }

    pub fn b_cond(&mut self, cond: Cond, l: Label) {
        self.fixups.push((self.code.len(), l));
        self.raw(0x5400_0000 | cond as u32);
    }

    pub fn cbz_w(&mut self, rt: u32, l: Label) {
        self.fixups.push((self.code.len(), l));
        self.raw(0x3400_0000 | rt);
    }

    pub fn cbnz_w(&mut self, rt: u32, l: Label) {
        self.fixups.push((self.code.len(), l));
        self.raw(0x3500_0000 | rt);
    }

    pub fn cbz_x(&mut self, rt: u32, l: Label) {
        self.fixups.push((self.code.len(), l));
        self.raw(0xB400_0000 | rt);
    }

    /// Appends the literal pool, resolves every literal load and label fixup.
    /// Returns the finished word stream.
    pub fn finish(mut self) -> Vec<u32> {
        if self.code.len() % 2 != 0 {
            self.raw(0xD503_201F); // nop: 8-align the pool
        }
        let pool_pos = self.code.len();
        let lits = core::mem::take(&mut self.lits);
        for v in &lits {
            self.code.push(*v as u32);
            self.code.push((*v >> 32) as u32);
        }
        for (pos, id) in core::mem::take(&mut self.lit_uses) {
            let target = pool_pos + id as usize * 2;
            let delta = (target - pos) as u32;
            self.code[pos] |= (delta & 0x7_FFFF) << 5;
        }
        for (pos, l) in core::mem::take(&mut self.fixups) {
            let target = self.bound[l.0 as usize].expect("unbound label at finish");
            patch_branch(&mut self.code, pos, target);
        }
        self.code
    }
}

fn patch_branch(code: &mut [u32], pos: usize, target: usize) {
    let delta = (target as i64 - pos as i64) as i32;
    let w = code[pos];
    code[pos] = match w >> 24 {
        0x14 => 0x1400_0000 | ((delta as u32) & 0x03FF_FFFF),
        // b.cond / cbz / cbnz: imm19 at [23:5].
        _ => (w & 0xFF00_001F) | (((delta as u32) & 0x7FFFF) << 5),
    };
}

// Every word below is the clang -arch arm64 assembly of the commented
// mnemonic (objdump-verified) — the disassembler reference for the SIMD
// and float stencil encodings.
#[cfg(test)]
mod tests {
    use super::Emitter;

    #[test]
    fn stencil_encodings_match_clang() {
        let cases: &[(fn(&mut Emitter), u32, &str)] = &[
            (|e| e.add_x_lsl3(5, 2, 3), 0x8B030C45, "add x5, x2, x3, lsl #3"),
            (|e| e.sub_x_imm(5, 2, 1), 0xD1000445, "sub x5, x2, #1"),
            (|e| e.sub_x(5, 2, 3), 0xCB030045, "sub x5, x2, x3"),
            (|e| e.and_x(5, 2, 3), 0x8A030045, "and x5, x2, x3"),
            (|e| e.cmp_x_imm(2, 64), 0xF101005F, "cmp x2, #64"),
            (|e| e.rbit_x(5, 2), 0xDAC00045, "rbit x5, x2"),
            (|e| e.clz_x(5, 2), 0xDAC01045, "clz x5, x2"),
            (|e| e.orr_x(5, 2, 3), 0xAA030045, "orr x5, x2, x3"),
            (|e| e.ldp_q(2, 3, 4, 0), 0xAD400C82, "ldp q2, q3, [x4]"),
            (|e| e.ldp_q(2, 3, 4, 32), 0xAD410C82, "ldp q2, q3, [x4, #32]"),
            (|e| e.ldr_d_idx(2, 4, 3), 0xFC636882, "ldr d2, [x4, x3]"),
            (|e| e.dup_2d_x(2, 3), 0x4E080C62, "dup v2.2d, x3"),
            (|e| e.fmov_d_x(2, 3), 0x9E670062, "fmov d2, x3"),
            (|e| e.cmeq_2d(2, 3, 4), 0x6EE48C62, "cmeq v2.2d, v3.2d, v4.2d"),
            (|e| e.cmgt_2d(2, 3, 4), 0x4EE43462, "cmgt v2.2d, v3.2d, v4.2d"),
            (|e| e.cmge_2d(2, 3, 4), 0x4EE43C62, "cmge v2.2d, v3.2d, v4.2d"),
            (|e| e.cmhi_2d(2, 3, 4), 0x6EE43462, "cmhi v2.2d, v3.2d, v4.2d"),
            (|e| e.cmhs_2d(2, 3, 4), 0x6EE43C62, "cmhs v2.2d, v3.2d, v4.2d"),
            (|e| e.fmov_s_w(0, 11), 0x1E270160, "fmov s0, w11"),
            (|e| e.fcvt_d_s(0, 0), 0x1E22C000, "fcvt d0, s0"),
            (|e| e.fcmp_d(0, 1), 0x1E612000, "fcmp d0, d1"),
            (|e| e.fcmeq_2d(2, 3, 3), 0x4E63E462, "fcmeq v2.2d, v3.2d, v3.2d"),
            (|e| e.fcmgt_2d(2, 3, 24), 0x6EF8E462, "fcmgt v2.2d, v3.2d, v24.2d"),
            (|e| e.fcmge_2d(2, 24, 3), 0x6E63E702, "fcmge v2.2d, v24.2d, v3.2d"),
            (|e| e.xtn_2s_2d(2, 3), 0x0EA12862, "xtn v2.2s, v3.2d"),
            (|e| e.fcvtl_2d_2s(2, 3), 0x0E617862, "fcvtl v2.2d, v3.2s"),
            (|e| e.not_16b(2, 3), 0x6E205862, "mvn v2.16b, v3.16b"),
            (|e| e.orr_16b(2, 3, 4), 0x4EA41C62, "orr v2.16b, v3.16b, v4.16b"),
            (|e| e.not_8b(2, 3), 0x2E205862, "mvn v2.8b, v3.8b"),
            (|e| e.uzp1_4s(2, 3, 4), 0x4E841862, "uzp1 v2.4s, v3.4s, v4.4s"),
            (|e| e.uzp1_8h(2, 3, 4), 0x4E441862, "uzp1 v2.8h, v3.8h, v4.8h"),
            (|e| e.xtn_8b(2, 3), 0x0E212862, "xtn v2.8b, v3.8h"),
            (|e| e.cmeq0_8b(2, 3), 0x0E209862, "cmeq v2.8b, v3.8b, #0"),
            (|e| e.and_8b(2, 3, 4), 0x0E241C62, "and v2.8b, v3.8b, v4.8b"),
            (|e| e.orr_8b(2, 3, 4), 0x0EA41C62, "orr v2.8b, v3.8b, v4.8b"),
            (|e| e.addv_b_8b(2, 3), 0x0E31B862, "addv b2, v3.8b"),
            (|e| e.umov_w_b0(2, 3), 0x0E013C62, "umov w2, v3.b[0]"),
        ];
        for (emit, want, asm) in cases {
            let mut e = Emitter::new();
            emit(&mut e);
            assert_eq!(e.code, vec![*want], "{asm}");
        }
    }
}
