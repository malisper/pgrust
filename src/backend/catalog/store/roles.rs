use crate::backend::catalog::catalog::CatalogError;
use crate::backend::catalog::role_memberships::{
    NewRoleMembership, grant_membership as grant_role_membership_row,
    revoke_role_membership as delete_role_membership_row,
    revoke_role_membership_option as update_role_membership_row,
};
use crate::backend::catalog::roles::{
    RoleAttributes, alter_role_attributes as alter_role_row, create_role as create_role_row,
    drop_roles as drop_role_rows, rename_role as rename_role_row,
};
use crate::include::catalog::{BootstrapCatalogKind, PgAuthIdRow, PgAuthMembersRow};

use super::CatalogStore;

impl CatalogStore {
    pub fn create_role(
        &mut self,
        role_name: &str,
        attrs: &RoleAttributes,
    ) -> Result<PgAuthIdRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = create_role_row(
            &mut catalog.authids,
            &mut catalog.next_oid,
            role_name,
            attrs,
        )?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthId])?;
        self.catalog = catalog.clone();
        self.control.next_oid = catalog.next_oid;
        self.control.next_rel_number = catalog.next_rel_number;
        Ok(row)
    }

    pub fn rename_role(
        &mut self,
        role_name: &str,
        new_name: &str,
    ) -> Result<PgAuthIdRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = rename_role_row(&mut catalog.authids, role_name, new_name)?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthId])?;
        self.catalog = catalog.clone();
        Ok(row)
    }

    pub fn alter_role_attributes(
        &mut self,
        role_name: &str,
        attrs: &RoleAttributes,
    ) -> Result<PgAuthIdRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = alter_role_row(&mut catalog.authids, role_name, attrs)?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthId])?;
        self.catalog = catalog.clone();
        Ok(row)
    }

    pub fn drop_role(&mut self, role_name: &str) -> Result<PgAuthIdRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let removed = drop_role_rows(&mut catalog.authids, &[role_name.to_string()])?;
        let removed_row = removed
            .into_iter()
            .next()
            .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
        catalog.auth_members.retain(|row| {
            row.roleid != removed_row.oid
                && row.member != removed_row.oid
                && row.grantor != removed_row.oid
        });
        self.persist_catalog_kinds(
            &catalog,
            &[
                BootstrapCatalogKind::PgAuthId,
                BootstrapCatalogKind::PgAuthMembers,
            ],
        )?;
        self.catalog = catalog.clone();
        Ok(removed_row)
    }

    pub fn grant_role_membership(
        &mut self,
        membership: &NewRoleMembership,
    ) -> Result<PgAuthMembersRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = grant_role_membership_row(
            &mut catalog.auth_members,
            &mut catalog.next_oid,
            membership,
        )?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthMembers])?;
        self.catalog = catalog.clone();
        self.control.next_oid = catalog.next_oid;
        Ok(row)
    }

    pub fn update_role_membership_options(
        &mut self,
        roleid: u32,
        member: u32,
        grantor: u32,
        admin_option: bool,
        inherit_option: bool,
        set_option: bool,
    ) -> Result<PgAuthMembersRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = update_role_membership_row(
            &mut catalog.auth_members,
            roleid,
            member,
            grantor,
            admin_option,
            inherit_option,
            set_option,
        )?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthMembers])?;
        self.catalog = catalog.clone();
        Ok(row)
    }

    pub fn revoke_role_membership(
        &mut self,
        roleid: u32,
        member: u32,
        grantor: u32,
    ) -> Result<PgAuthMembersRow, CatalogError> {
        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = delete_role_membership_row(&mut catalog.auth_members, roleid, member, grantor)?;
        self.persist_catalog_kinds(&catalog, &[BootstrapCatalogKind::PgAuthMembers])?;
        self.catalog = catalog.clone();
        Ok(row)
    }
}
