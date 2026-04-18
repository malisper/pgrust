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
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::include::catalog::{BootstrapCatalogKind, PgAuthIdRow, PgAuthMembersRow};

use super::{CatalogStore, CatalogStoreMode};

impl CatalogStore {
    pub fn create_role(
        &mut self,
        role_name: &str,
        attrs: &RoleAttributes,
    ) -> Result<PgAuthIdRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut authids = self.catcache()?.authid_rows();
            let mut control = self.control_state()?;
            let row = create_role_row(&mut authids, &mut control.next_oid, role_name, attrs)?;
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows::default(),
                &PhysicalCatalogRows {
                    authids: vec![row.clone()],
                    ..PhysicalCatalogRows::default()
                },
                &[BootstrapCatalogKind::PgAuthId],
            )?;
            self.control = control;
            return Ok(row);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = create_role_row(
            &mut catalog.authids,
            &mut catalog.next_oid,
            role_name,
            attrs,
        )?;
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows::default(),
            &PhysicalCatalogRows {
                authids: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            &[BootstrapCatalogKind::PgAuthId],
        )?;
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
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut authids = self.catcache()?.authid_rows();
            let control = self.control_state()?;
            let old_row = authids
                .iter()
                .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
                .cloned()
                .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
            let row = rename_role_row(&mut authids, role_name, new_name)?;
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows {
                    authids: vec![old_row],
                    ..PhysicalCatalogRows::default()
                },
                &PhysicalCatalogRows {
                    authids: vec![row.clone()],
                    ..PhysicalCatalogRows::default()
                },
                &[BootstrapCatalogKind::PgAuthId],
            )?;
            self.control = control;
            return Ok(row);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let old_row = catalog
            .authids
            .iter()
            .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
        let row = rename_role_row(&mut catalog.authids, role_name, new_name)?;
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows {
                authids: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            &PhysicalCatalogRows {
                authids: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            &[BootstrapCatalogKind::PgAuthId],
        )?;
        self.catalog = catalog.clone();
        Ok(row)
    }

    pub fn alter_role_attributes(
        &mut self,
        role_name: &str,
        attrs: &RoleAttributes,
    ) -> Result<PgAuthIdRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut authids = self.catcache()?.authid_rows();
            let control = self.control_state()?;
            let old_row = authids
                .iter()
                .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
                .cloned()
                .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
            let row = alter_role_row(&mut authids, role_name, attrs)?;
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows {
                    authids: vec![old_row],
                    ..PhysicalCatalogRows::default()
                },
                &PhysicalCatalogRows {
                    authids: vec![row.clone()],
                    ..PhysicalCatalogRows::default()
                },
                &[BootstrapCatalogKind::PgAuthId],
            )?;
            self.control = control;
            return Ok(row);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let old_row = catalog
            .authids
            .iter()
            .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
        let row = alter_role_row(&mut catalog.authids, role_name, attrs)?;
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows {
                authids: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            &PhysicalCatalogRows {
                authids: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            &[BootstrapCatalogKind::PgAuthId],
        )?;
        self.catalog = catalog.clone();
        Ok(row)
    }

    pub fn drop_role(&mut self, role_name: &str) -> Result<PgAuthIdRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let catcache = self.catcache()?;
            let mut authids = catcache.authid_rows();
            let auth_members = catcache.auth_members_rows();
            let control = self.control_state()?;
            let removed = drop_role_rows(&mut authids, &[role_name.to_string()])?;
            let removed_row = removed
                .into_iter()
                .next()
                .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
            let removed_members = auth_members
                .iter()
                .filter(|row| {
                    row.roleid == removed_row.oid
                        || row.member == removed_row.oid
                        || row.grantor == removed_row.oid
                })
                .cloned()
                .collect::<Vec<_>>();
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows {
                    authids: vec![removed_row.clone()],
                    auth_members: removed_members,
                    ..PhysicalCatalogRows::default()
                },
                &PhysicalCatalogRows::default(),
                &[
                    BootstrapCatalogKind::PgAuthId,
                    BootstrapCatalogKind::PgAuthMembers,
                ],
            )?;
            self.control = control;
            return Ok(removed_row);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let removed = drop_role_rows(&mut catalog.authids, &[role_name.to_string()])?;
        let removed_row = removed
            .into_iter()
            .next()
            .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
        let removed_members = catalog
            .auth_members
            .iter()
            .filter(|row| {
                row.roleid == removed_row.oid
                    || row.member == removed_row.oid
                    || row.grantor == removed_row.oid
            })
            .cloned()
            .collect::<Vec<_>>();
        catalog.auth_members.retain(|row| {
            row.roleid != removed_row.oid
                && row.member != removed_row.oid
                && row.grantor != removed_row.oid
        });
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows {
                authids: vec![removed_row.clone()],
                auth_members: removed_members,
                ..PhysicalCatalogRows::default()
            },
            &PhysicalCatalogRows::default(),
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
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut auth_members = self.catcache()?.auth_members_rows();
            let mut control = self.control_state()?;
            let row =
                grant_role_membership_row(&mut auth_members, &mut control.next_oid, membership)?;
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows::default(),
                &PhysicalCatalogRows {
                    auth_members: vec![row.clone()],
                    ..PhysicalCatalogRows::default()
                },
                &[BootstrapCatalogKind::PgAuthMembers],
            )?;
            self.control = control;
            return Ok(row);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = grant_role_membership_row(
            &mut catalog.auth_members,
            &mut catalog.next_oid,
            membership,
        )?;
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows::default(),
            &PhysicalCatalogRows {
                auth_members: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            &[BootstrapCatalogKind::PgAuthMembers],
        )?;
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
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut auth_members = self.catcache()?.auth_members_rows();
            let control = self.control_state()?;
            let old_row = auth_members
                .iter()
                .find(|row| row.roleid == roleid && row.member == member && row.grantor == grantor)
                .cloned()
                .ok_or_else(|| CatalogError::UnknownTable(roleid.to_string()))?;
            let row = update_role_membership_row(
                &mut auth_members,
                roleid,
                member,
                grantor,
                admin_option,
                inherit_option,
                set_option,
            )?;
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows {
                    auth_members: vec![old_row],
                    ..PhysicalCatalogRows::default()
                },
                &PhysicalCatalogRows {
                    auth_members: vec![row.clone()],
                    ..PhysicalCatalogRows::default()
                },
                &[BootstrapCatalogKind::PgAuthMembers],
            )?;
            self.control = control;
            return Ok(row);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let old_row = catalog
            .auth_members
            .iter()
            .find(|row| row.roleid == roleid && row.member == member && row.grantor == grantor)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(roleid.to_string()))?;
        let row = update_role_membership_row(
            &mut catalog.auth_members,
            roleid,
            member,
            grantor,
            admin_option,
            inherit_option,
            set_option,
        )?;
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows {
                auth_members: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            &PhysicalCatalogRows {
                auth_members: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            &[BootstrapCatalogKind::PgAuthMembers],
        )?;
        self.catalog = catalog.clone();
        Ok(row)
    }

    pub fn revoke_role_membership(
        &mut self,
        roleid: u32,
        member: u32,
        grantor: u32,
    ) -> Result<PgAuthMembersRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut auth_members = self.catcache()?.auth_members_rows();
            let control = self.control_state()?;
            let row = delete_role_membership_row(&mut auth_members, roleid, member, grantor)?;
            self.persist_catalog_row_changes_with_control(
                control.next_oid,
                control.next_rel_number,
                &PhysicalCatalogRows {
                    auth_members: vec![row.clone()],
                    ..PhysicalCatalogRows::default()
                },
                &PhysicalCatalogRows::default(),
                &[BootstrapCatalogKind::PgAuthMembers],
            )?;
            self.control = control;
            return Ok(row);
        }

        let mut catalog = self.catalog_snapshot_with_control()?;
        let row = delete_role_membership_row(&mut catalog.auth_members, roleid, member, grantor)?;
        self.persist_catalog_row_changes(
            &catalog,
            &PhysicalCatalogRows {
                auth_members: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            &PhysicalCatalogRows::default(),
            &[BootstrapCatalogKind::PgAuthMembers],
        )?;
        self.catalog = catalog.clone();
        Ok(row)
    }
}
