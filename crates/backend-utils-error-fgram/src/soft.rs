use crate::{ErrorLocation, ErrorStackFrame, PgError, PgResult};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SoftErrorContext {
    details_wanted: bool,
    error_occurred: bool,
    error: Option<PgError>,
}

impl SoftErrorContext {
    pub fn new(details_wanted: bool) -> Self {
        Self {
            details_wanted,
            error_occurred: false,
            error: None,
        }
    }

    pub fn details_wanted(&self) -> bool {
        self.details_wanted
    }

    pub fn error_occurred(&self) -> bool {
        self.error_occurred
    }

    pub fn error(&self) -> Option<&PgError> {
        self.error.as_ref()
    }

    pub fn take_error(&mut self) -> Option<PgError> {
        self.error.take()
    }

    pub fn save(&mut self, error: PgError) {
        self.error_occurred = true;
        self.error = Some(error);
    }

    pub fn mark_error_occurred(&mut self) {
        self.error_occurred = true;
    }
}

pub fn errsave(context: Option<&mut SoftErrorContext>, error: PgError) -> PgResult<()> {
    if let Some(context) = context {
        context.save(error);
        Ok(())
    } else {
        Err(error)
    }
}

#[derive(Debug)]
pub enum ErrSaveStart<'a> {
    Skipped,
    Active(ErrSaveFrame<'a>),
}

#[derive(Debug)]
pub struct ErrSaveFrame<'a> {
    context: Option<&'a mut SoftErrorContext>,
    frame: ErrorStackFrame,
}

impl<'a> ErrSaveFrame<'a> {
    pub fn errcode(&mut self, sqlstate: crate::SqlState) -> PgResult<&mut Self> {
        self.frame.errcode(sqlstate)?;
        Ok(self)
    }

    pub fn errmsg(&mut self, message: impl Into<String>) -> PgResult<&mut Self> {
        self.frame.errmsg(message)?;
        Ok(self)
    }

    pub fn errmsg_internal(&mut self, message: impl Into<String>) -> PgResult<&mut Self> {
        self.frame.errmsg_internal(message)?;
        Ok(self)
    }

    pub fn errdetail(&mut self, detail: impl Into<String>) -> PgResult<&mut Self> {
        self.frame.errdetail(detail)?;
        Ok(self)
    }

    pub fn errdetail_log(&mut self, detail_log: impl Into<String>) -> PgResult<&mut Self> {
        self.frame.errdetail_log(detail_log)?;
        Ok(self)
    }

    pub fn errhint(&mut self, hint: impl Into<String>) -> PgResult<&mut Self> {
        self.frame.errhint(hint)?;
        Ok(self)
    }

    pub fn errcontext_msg(&mut self, context: impl Into<String>) -> PgResult<&mut Self> {
        self.frame.errcontext_msg(context)?;
        Ok(self)
    }

    pub fn errposition(&mut self, cursor_position: i32) -> PgResult<&mut Self> {
        self.frame.errposition(cursor_position)?;
        Ok(self)
    }

    pub fn internalerrposition(&mut self, internal_position: i32) -> PgResult<&mut Self> {
        self.frame.internalerrposition(internal_position)?;
        Ok(self)
    }

    pub fn internalerrquery(&mut self, query: impl Into<String>) -> PgResult<&mut Self> {
        self.frame.internalerrquery(query)?;
        Ok(self)
    }

    /// `err_generic_string(field, str)` on the in-flight soft error — mirrors
    /// C's `err_generic_string`, which mutates the current errordata regardless
    /// of the hard/soft path. Used by helpers like `errdatatype`.
    pub fn err_generic_string(
        &mut self,
        field: crate::ErrorField,
        value: impl Into<String>,
    ) -> PgResult<&mut Self> {
        self.frame.err_generic_string(field, value)?;
        Ok(self)
    }

    pub fn set_errcontext_domain(&mut self, domain: impl Into<String>) -> PgResult<&mut Self> {
        self.frame.set_errcontext_domain(domain)?;
        Ok(self)
    }

    pub fn errhidestmt(&mut self, hide_statement: bool) -> PgResult<&mut Self> {
        self.frame.errhidestmt(hide_statement)?;
        Ok(self)
    }

    pub fn errhidecontext(&mut self, hide_context: bool) -> PgResult<&mut Self> {
        self.frame.errhidecontext(hide_context)?;
        Ok(self)
    }

    pub fn finish(self, location: ErrorLocation) -> PgResult<()> {
        let Self { context, frame } = self;
        let error = frame.finish(location)?;
        match context {
            Some(context) => {
                context.save(error);
                Ok(())
            }
            None => Err(error),
        }
    }
}

pub fn errsave_start<'a>(
    context: Option<&'a mut SoftErrorContext>,
    domain: Option<&str>,
) -> PgResult<ErrSaveStart<'a>> {
    match context {
        Some(context) => {
            context.mark_error_occurred();
            if !context.details_wanted() {
                return Ok(ErrSaveStart::Skipped);
            }
            Ok(ErrSaveStart::Active(ErrSaveFrame {
                context: Some(context),
                frame: crate::stack::soft_error_frame(domain)?,
            }))
        }
        None => Ok(ErrSaveStart::Active(ErrSaveFrame {
            context: None,
            frame: ErrorStackFrame::push(crate::ERROR, domain)?,
        })),
    }
}
