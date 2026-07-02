seam_core::seam!(
    // load_hba() (hba.c); false = parse/load failure (already logged).
    pub fn load_hba() -> bool
);

seam_core::seam!(
    // load_ident() (hba.c).
    pub fn load_ident() -> bool
);

seam_core::seam!(
    // hba_authname(auth_method) (hba.c).
    pub fn hba_authname(auth_method: types_core::init::UserAuth) -> &'static str
);
