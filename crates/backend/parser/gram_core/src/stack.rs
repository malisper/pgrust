use core::alloc::Layout;
use core::ptr::NonNull;

use mcx::{Allocator, Mcx};
use types_error::{PgError, PgResult};

use crate::tables::YYMAXDEPTH;
use crate::yystype::YYSTYPE;

// The three bison stacks; initial arrays live in yyparse's frame (yyssa/
// yyvsa/yylsa), overflow relocates into one arena block (YYSTACK_RELOCATE).
// Value slots move: written once by a push, read at most once ($n or the
// default $$=$n) before the pop — LALR enforces it — so reads are ptr::read.
#[derive(Clone, Copy)]
pub(crate) struct Stacks<'mcx> {
    vs: NonNull<YYSTYPE<'mcx>>,
    ls: NonNull<i32>,
    ss: NonNull<i16>,
    cap: usize,
    arena_backed: bool,
    // Set to the pre-reduce sp before each action call ($n accessors).
    sp: usize,
}

fn layout(cap: usize) -> (Layout, usize, usize) {
    let vs = Layout::array::<YYSTYPE>(cap).expect("stack layout");
    let (l1, ls_off) = vs.extend(Layout::array::<i32>(cap).unwrap()).unwrap();
    let (l2, ss_off) = l1.extend(Layout::array::<i16>(cap + 1).unwrap()).unwrap();
    (l2, ls_off, ss_off)
}

impl<'mcx> Stacks<'mcx> {
    // SAFETY: the arrays must hold at least cap (states: cap + 1) elements
    // and outlive every use of this view (yyparse's frame does).
    pub(crate) unsafe fn from_frame(
        vs: *mut YYSTYPE<'mcx>,
        ls: *mut i32,
        ss: *mut i16,
        cap: usize,
    ) -> Stacks<'mcx> {
        // SAFETY: caller contract.
        unsafe {
            Stacks {
                vs: NonNull::new_unchecked(vs),
                ls: NonNull::new_unchecked(ls),
                ss: NonNull::new_unchecked(ss),
                cap,
                arena_backed: false,
                sp: 0,
            }
        }
    }

    fn alloc(mcx: Mcx<'mcx>, cap: usize) -> PgResult<Stacks<'mcx>> {
        const { assert!(!core::mem::needs_drop::<YYSTYPE<'static>>()) };
        let (layout, ls_off, ss_off) = layout(cap);
        let block = mcx
            .allocate(layout)
            .map_err(|_| Box::new(mcx.oom(layout.size())))?;
        let base = block.cast::<u8>();
        // SAFETY: offsets are within the just-allocated block.
        unsafe {
            Ok(Stacks {
                vs: base.cast(),
                ls: base.add(ls_off).cast(),
                ss: base.add(ss_off).cast(),
                cap,
                arena_backed: true,
                sp: 0,
            })
        }
    }

    // SAFETY (all accessors): i must address a live slot (< cap; for values,
    // written and not yet consumed).
    #[inline(always)]
    pub(crate) unsafe fn take_val(&mut self, i: usize) -> YYSTYPE<'mcx> {
        debug_assert!(i < self.cap);
        unsafe { self.vs.add(i).read() }
    }

    #[inline(always)]
    pub(crate) unsafe fn write_val(&mut self, i: usize, v: YYSTYPE<'mcx>, l: i32) {
        debug_assert!(i < self.cap);
        unsafe {
            self.vs.add(i).write(v);
            self.ls.add(i).write(l);
        }
    }

    #[inline(always)]
    pub(crate) unsafe fn write_loc(&mut self, i: usize, l: i32) {
        debug_assert!(i < self.cap);
        unsafe { self.ls.add(i).write(l) }
    }

    #[inline(always)]
    pub(crate) unsafe fn loc(&self, i: usize) -> i32 {
        debug_assert!(i < self.cap);
        unsafe { self.ls.add(i).read() }
    }

    #[inline(always)]
    pub(crate) unsafe fn state(&self, i: usize) -> i16 {
        debug_assert!(i <= self.cap);
        unsafe { self.ss.add(i).read() }
    }

    #[inline(always)]
    pub(crate) unsafe fn write_state(&mut self, i: usize, s: i16) {
        debug_assert!(i <= self.cap);
        unsafe { self.ss.add(i).write(s) }
    }

    // Guarantee indices < new_sp writable; live values = new_sp - 1 at every
    // call site (shift: new_sp = sp + 1; reduce: new_sp = base + 1).
    #[inline(always)]
    pub(crate) fn ensure(&mut self, mcx: Mcx<'mcx>, new_sp: usize) -> PgResult<()> {
        if new_sp >= self.cap {
            self.grow(mcx, new_sp - 1)?;
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, mcx: Mcx<'mcx>, live: usize) -> PgResult<()> {
        if self.cap >= YYMAXDEPTH {
            return Err(Box::new(PgError::error("memory exhausted")));
        }
        let new_cap = (self.cap * 2).clamp(crate::parse::YYINITDEPTH, YYMAXDEPTH);
        let mut new = Stacks::alloc(mcx, new_cap)?;
        // SAFETY: live values/locations and live+1 states are initialized.
        unsafe {
            core::ptr::copy_nonoverlapping(self.vs.as_ptr(), new.vs.as_ptr(), live);
            core::ptr::copy_nonoverlapping(self.ls.as_ptr(), new.ls.as_ptr(), live);
            core::ptr::copy_nonoverlapping(self.ss.as_ptr(), new.ss.as_ptr(), live + 1);
            // The initial frame-local stacks must not be deallocated.
            if self.arena_backed {
                let (old_layout, ..) = layout(self.cap);
                mcx.deallocate(self.vs.cast(), old_layout);
            }
        }
        new.sp = self.sp;
        *self = new;
        Ok(())
    }

    #[inline(always)]
    pub(crate) fn set_sp(&mut self, sp: usize) {
        self.sp = sp;
    }

    // The action's $n / @n (moves; single read per slot).
    #[inline(always)]
    pub(crate) fn v(&mut self, yylen: usize, n: usize) -> YYSTYPE<'mcx> {
        let i = self.sp - yylen + (n - 1);
        // SAFETY: n <= yylen <= sp, so i < sp; slot unread since its push.
        unsafe { self.take_val(i) }
    }

    #[inline(always)]
    pub(crate) fn l(&self, yylen: usize, n: usize) -> i32 {
        // SAFETY: as v().
        unsafe { self.loc(self.sp - yylen + (n - 1)) }
    }
}
