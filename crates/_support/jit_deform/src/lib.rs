// Copy-and-patch JIT tuple deform: docs/optimizations/jit-deform.md.
// SAFETY: kernel contract — sound iff `tp` is the data area of a live
// NULL-FREE tuple matching the emission-time tupdesc prefix with natts >=
// ncols, `values` >= ncols cells, row-ABI `isnull` >= ncols bytes / soa-ABI
// stride addresses ncols cells. Miri cannot run generated code;
// fuzz_parity_vs_interpreter is the evidence standard.

use std::rc::Rc;

use datum::Datum;
use types_tuple::{CompactAttribute, TupleDescData};

/// Row ABI: x0 = tuple data, x1 = values (stride 8), x2 = isnull bytes.
pub type DeformRowFn = unsafe extern "C" fn(*const u8, *mut Datum, *mut u8);
/// SoA ABI: x0 = tuple data, x1 = &values[row] col 0, x2 = byte stride.
pub type DeformSoaFn = unsafe extern "C" fn(*const u8, *mut Datum, usize);

/// Columns 0..n all have `attlen > 0`: the widest prefix a kernel can cover.
pub fn fixed_prefix(atts: &[CompactAttribute]) -> usize {
    atts.iter().take_while(|a| a.attlen > 0).count()
}

/// AIO-style availability gate + A/B kill switch; fallback = interpreter.
pub fn available() -> bool {
    #[cfg(target_arch = "aarch64")]
    {
        static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        !*OFF.get_or_init(|| {
            matches!(std::env::var("PGRUST_JIT_DEFORM").as_deref(), Ok("0") | Ok("off"))
        })
    }
    #[cfg(not(target_arch = "aarch64"))]
    false
}

pub struct DeformKernel {
    // Pins the emission-time tupdesc: identity compare can never ABA.
    desc: Rc<TupleDescData<'static>>,
    code: imp::CodeAlloc,
    row_off: u32,
    soa_off: u32,
    ncols: u16,
    end_off: u32,
}

impl DeformKernel {
    #[inline]
    pub fn ncols(&self) -> u16 {
        self.ncols
    }

    /// deform_internal's resume `off` at attnum == ncols (null-free walk).
    #[inline]
    pub fn end_off(&self) -> u32 {
        self.end_off
    }

    /// The kernel is valid only for the exact descriptor it was emitted from.
    #[inline]
    pub fn matches(&self, desc: &TupleDescData<'_>) -> bool {
        core::ptr::eq(
            Rc::as_ptr(&self.desc).cast::<u8>(),
            (desc as *const TupleDescData<'_>).cast::<u8>(),
        )
    }

    /// # Safety
    /// Crate-level contract, row ABI.
    #[inline]
    pub unsafe fn row(&self, tp: *const u8, values: *mut Datum, isnull: *mut u8) {
        let f: DeformRowFn =
            unsafe { core::mem::transmute(self.code.base().add(self.row_off as usize)) };
        unsafe { f(tp, values, isnull) }
    }

    /// # Safety
    /// Crate-level contract, SoA ABI.
    #[inline]
    pub unsafe fn soa(&self, tp: *const u8, col0: *mut Datum, stride: usize) {
        let f: DeformSoaFn =
            unsafe { core::mem::transmute(self.code.base().add(self.soa_off as usize)) };
        unsafe { f(tp, col0, stride) }
    }
}

/// None = refused (shape, arch, kill switch, arena full): callers fail open.
pub fn install(desc: &Rc<TupleDescData<'static>>, ncols: usize) -> Option<Rc<DeformKernel>> {
    if !available() {
        return None;
    }
    imp::install(desc, ncols)
}

// Arena reuse surface for other copy-and-patch emitters (jit-qual lane):
// same thread-local chunked W^X arena, whole-kernel granularity, fail-open.
pub struct CodeBlock {
    code: imp::CodeAlloc,
    len: usize,
}

impl CodeBlock {
    #[inline]
    pub fn base(&self) -> *const u8 {
        self.code.base()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// None = refused (arch, arena full): callers fail open to their interpreter.
pub fn install_code(words: &[u32]) -> Option<CodeBlock> {
    let code = imp::alloc_code(words)?;
    Some(CodeBlock { code, len: words.len() * 4 })
}

// C JitInstrumentation slice we can attribute (created_functions +
// generation time; inlining/optimization/emission are LLVM phases that stay
// zero under copy-and-patch). Lives here so executor state crates can carry
// it without depending on the emitters.
#[derive(Clone, Copy, Default)]
pub struct JitInstrumentation {
    pub created_functions: i32,
    pub generation_nanos: u64,
}

mcx::forget_safe_nodrop!(JitInstrumentation);

#[cfg(not(target_arch = "aarch64"))]
mod imp {
    pub(crate) struct CodeAlloc;
    impl CodeAlloc {
        pub(crate) fn base(&self) -> *const u8 {
            unreachable!("no kernels are installed off-aarch64")
        }
    }
    pub(crate) fn alloc_code(_words: &[u32]) -> Option<CodeAlloc> {
        None
    }
    pub(crate) fn install(
        _desc: &std::rc::Rc<types_tuple::TupleDescData<'static>>,
        _ncols: usize,
    ) -> Option<std::rc::Rc<super::DeformKernel>> {
        None
    }
}

#[cfg(target_arch = "aarch64")]
mod imp {
    use super::DeformKernel;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use types_tuple::{CompactAttribute, TupleDescData};

    // 64KiB chunks, 512KiB/thread cap (kernels are ~16-300B); full = fail open.
    const CHUNK_BYTES: usize = 64 * 1024;
    const MAX_CHUNKS: usize = 8;

    struct Chunk {
        base: *mut u8,
        used: Cell<usize>,
        live: Cell<usize>,
    }

    impl Drop for Chunk {
        fn drop(&mut self) {
            // SAFETY: chunk-owned mapping; kernels hold the Rc, no entry outlives it.
            unsafe { libc::munmap(self.base.cast(), CHUNK_BYTES) };
        }
    }

    pub(crate) struct CodeAlloc {
        chunk: Rc<Chunk>,
        off: usize,
    }

    impl CodeAlloc {
        #[inline]
        pub(crate) fn base(&self) -> *const u8 {
            // SAFETY: off + kernel length <= CHUNK_BYTES (bump bound).
            unsafe { self.chunk.base.add(self.off) }
        }
    }

    impl Drop for CodeAlloc {
        fn drop(&mut self) {
            let live = self.chunk.live.get() - 1;
            self.chunk.live.set(live);
            // Whole-chunk reuse only: bump arenas cannot free mid-chunk.
            if live == 0 {
                self.chunk.used.set(0);
            }
        }
    }

    thread_local! {
        static ARENA: RefCell<Vec<Rc<Chunk>>> = const { RefCell::new(Vec::new()) };
    }

    fn map_chunk() -> Option<Rc<Chunk>> {
        // SAFETY: fresh private mapping; RX at rest, RW only in the copy window.
        let base = unsafe {
            libc::mmap(
                core::ptr::null_mut(),
                CHUNK_BYTES,
                libc::PROT_READ | libc::PROT_EXEC,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return None;
        }
        Some(Rc::new(Chunk { base: base.cast(), used: Cell::new(0), live: Cell::new(0) }))
    }

    pub(crate) fn alloc_code(words: &[u32]) -> Option<CodeAlloc> {
        let len = words.len() * 4;
        debug_assert!(len <= CHUNK_BYTES);
        ARENA.with(|arena| {
            let mut arena = arena.borrow_mut();
            let chunk = match arena.iter().find(|c| c.used.get() + len <= CHUNK_BYTES) {
                Some(c) => Rc::clone(c),
                None => {
                    if arena.len() >= MAX_CHUNKS {
                        return None;
                    }
                    let c = map_chunk()?;
                    arena.push(Rc::clone(&c));
                    c
                }
            };
            let off = chunk.used.get();
            // SAFETY: in-bounds chunk slice; temporal W^X (no kernel can be
            // mid-flight — the arena's one thread is here) + icache flush.
            unsafe {
                let rc = libc::mprotect(
                    chunk.base.cast(),
                    CHUNK_BYTES,
                    libc::PROT_READ | libc::PROT_WRITE,
                );
                if rc != 0 {
                    return None;
                }
                core::ptr::copy_nonoverlapping(
                    words.as_ptr().cast::<u8>(),
                    chunk.base.add(off),
                    len,
                );
                let rc = libc::mprotect(
                    chunk.base.cast(),
                    CHUNK_BYTES,
                    libc::PROT_READ | libc::PROT_EXEC,
                );
                assert!(rc == 0, "mprotect(RX) failed on the deform-JIT arena");
                flush_icache(chunk.base.add(off), len);
            }
            chunk.used.set(off + len);
            chunk.live.set(chunk.live.get() + 1);
            Some(CodeAlloc { chunk, off })
        })
    }

    unsafe fn flush_icache(start: *mut u8, len: usize) {
        #[cfg(target_os = "macos")]
        {
            extern "C" {
                fn sys_icache_invalidate(start: *mut core::ffi::c_void, len: usize);
            }
            unsafe { sys_icache_invalidate(start.cast(), len) };
        }
        #[cfg(not(target_os = "macos"))]
        unsafe {
            let base = start as usize & !63;
            let end = start as usize + len;
            let mut p = base;
            while p < end {
                core::arch::asm!("dc cvau, {0}", in(reg) p);
                p += 64;
            }
            core::arch::asm!("dsb ish");
            let mut p = base;
            while p < end {
                core::arch::asm!("ic ivau, {0}", in(reg) p);
                p += 64;
            }
            core::arch::asm!("dsb ish", "isb");
        }
    }

    struct Emitter {
        code: Vec<u32>,
        ok: bool,
    }

    impl Emitter {
        fn new() -> Emitter {
            Emitter { code: Vec::with_capacity(96), ok: true }
        }

        // fetch_att Datum semantics: byval sign-extends by width, byref = address.
        fn load_att(&mut self, rt: u32, rn: u32, off: u32, attlen: i16, attbyval: bool) {
            let w = match (attbyval, attlen) {
                (true, 8) => self.scaled(0xF940_0000, off, 8),
                (true, 4) => self.scaled(0xB980_0000, off, 4),
                (true, 2) => self.scaled(0x7980_0000, off, 2),
                (true, 1) => self.scaled(0x3980_0000, off, 1),
                (false, l) if l > 0 => {
                    if off > 4095 {
                        self.ok = false;
                    }
                    0x9100_0000 | (off << 10)
                }
                _ => {
                    self.ok = false;
                    0
                }
            };
            self.code.push(w | (rn << 5) | rt);
        }

        fn scaled(&mut self, base: u32, off: u32, size: u32) -> u32 {
            if off % size != 0 || off / size > 4095 {
                self.ok = false;
                return base;
            }
            base | ((off / size) << 10)
        }

        fn str64(&mut self, rt: u32, rn: u32, off: u32) {
            let w = self.scaled(0xF900_0000, off, 8);
            self.code.push(w | (rn << 5) | rt);
        }

        // Zero EXACTLY n isnull bytes (slot arrays are natts long, no overrun).
        fn zero_bytes(&mut self, rn: u32, n: usize) {
            let mut off = 0usize;
            while off + 8 <= n {
                let w = self.scaled(0xF900_0000, off as u32, 8);
                self.code.push(w | (rn << 5) | 31);
                off += 8;
            }
            if off + 4 <= n {
                let w = self.scaled(0xB900_0000, off as u32, 4);
                self.code.push(w | (rn << 5) | 31);
                off += 4;
            }
            if off + 2 <= n {
                let w = self.scaled(0x7900_0000, off as u32, 2);
                self.code.push(w | (rn << 5) | 31);
                off += 2;
            }
            if off < n {
                let w = self.scaled(0x3900_0000, off as u32, 1);
                self.code.push(w | (rn << 5) | 31);
            }
        }

        fn ret(&mut self) {
            self.code.push(0xD65F_03C0);
        }
    }

    // Offsets: the nominal aligned chain, deform_internal's null-free walk.
    fn emit_row(atts: &[CompactAttribute], ncols: usize) -> Option<(Vec<u32>, u32)> {
        let mut e = Emitter::new();
        e.zero_bytes(2, ncols);
        let end = emit_loads(&mut e, atts, ncols, |e, rt, c| e.str64(rt, 1, (c * 8) as u32));
        e.ret();
        e.ok.then_some((e.code, end))
    }

    fn emit_soa(atts: &[CompactAttribute], ncols: usize) -> Option<Vec<u32>> {
        let mut e = Emitter::new();
        emit_loads(&mut e, atts, ncols, |e, rt, _| {
            e.str64(rt, 1, 0);
            // add x1, x1, x2
            e.code.push(0x8B02_0021);
        });
        e.ret();
        e.ok.then_some(e.code)
    }

    fn emit_loads(
        e: &mut Emitter,
        atts: &[CompactAttribute],
        ncols: usize,
        mut store: impl FnMut(&mut Emitter, u32, usize),
    ) -> u32 {
        let mut off = 0usize;
        for (c, a) in atts[..ncols].iter().enumerate() {
            off = off.next_multiple_of(a.attalignby.max(1) as usize);
            let rt = 3 + (c as u32 % 4);
            e.load_att(rt, 0, off as u32, a.attlen, a.attbyval);
            store(e, rt, c);
            off += a.attlen as usize;
        }
        off as u32
    }

    pub(crate) fn install(
        desc: &Rc<TupleDescData<'static>>,
        ncols: usize,
    ) -> Option<Rc<DeformKernel>> {
        let atts: &[CompactAttribute] = &desc.compact_attrs;
        if ncols == 0 || ncols > super::fixed_prefix(atts) || ncols > u16::MAX as usize {
            return None;
        }
        let (row, end_off) = emit_row(atts, ncols)?;
        let soa = emit_soa(atts, ncols)?;
        let mut words = row;
        let soa_off = (words.len() * 4) as u32;
        words.extend_from_slice(&soa);
        let code = alloc_code(&words)?;
        Some(Rc::new(DeformKernel {
            desc: Rc::clone(desc),
            code,
            row_off: 0,
            soa_off,
            ncols: ncols as u16,
            end_off,
        }))
    }
}

#[cfg(all(test, target_arch = "aarch64"))]
mod tests;
