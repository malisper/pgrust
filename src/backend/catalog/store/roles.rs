use crate::backend::catalog::catalog::CatalogError;
use crate::backend::catalog::persistence::{
    delete_catalog_rows_subset_mvcc, insert_catalog_rows_subset_mvcc,
};
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
use crate::backend::utils::cache::catcache::CatCache;
use crate::include::catalog::{BootstrapCatalogKind, PgAuthIdRow, PgAuthMembersRow};

#[cfg(test)]
use super::CatalogStoreMode;
use super::{CatalogMutationEffect, CatalogStore, CatalogWriteContext};

fn role_catalog_effect(kinds: &[BootstrapCatalogKind]) -> CatalogMutationEffect {
    CatalogMutationEffect {
        touched_catalogs: kinds.to_vec(),
        ..CatalogMutationEffect::default()
    }
}

fn visible_role_catcache_for_ctx(
    store: &CatalogStore,
    ctx: &CatalogWriteContext,
) -> Result<CatCache, CatalogError> {
    // Read the visible role rows without carrying the txns read lock into the
    // later MVCC insert/delete helpers, which may need to reacquire it.
    let snapshot = ctx
        .txns
        .read()
        .snapshot_for_command(ctx.xid, ctx.cid)
        .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
    let txns = ctx.txns.read();
    store.catcache_with_snapshot(&ctx.pool, &txns, &snapshot, ctx.client_id)
}

impl CatalogStore {
    #[cfg(test)]
    pub fn create_role_direct(
        &mut self,
        role_name: &str,
        attrs: &RoleAttributes,
    ) -> Result<PgAuthIdRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut authids = self.catalog.authids.clone();
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
            self.catalog.authids = authids;
            self.catalog.next_oid = control.next_oid;
            self.catalog.next_rel_number = control.next_rel_number;
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

    #[cfg(test)]
    pub fn rename_role_direct(
        &mut self,
        role_name: &str,
        new_name: &str,
    ) -> Result<PgAuthIdRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut authids = self.catalog.authids.clone();
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
            self.catalog.authids = authids;
            self.catalog.next_oid = control.next_oid;
            self.catalog.next_rel_number = control.next_rel_number;
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

    #[cfg(test)]
    pub fn alter_role_attributes_direct(
        &mut self,
        role_name: &str,
        attrs: &RoleAttributes,
    ) -> Result<PgAuthIdRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut authids = self.catalog.authids.clone();
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
            self.catalog.authids = authids;
            self.catalog.next_oid = control.next_oid;
            self.catalog.next_rel_number = control.next_rel_number;
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

    #[cfg(test)]
    pub fn drop_role_direct(&mut self, role_name: &str) -> Result<PgAuthIdRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut authids = self.catalog.authids.clone();
            let auth_members = self.catalog.auth_members.clone();
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
            self.catalog.authids = authids;
            self.catalog.auth_members.retain(|row| {
                row.roleid != removed_row.oid
                    && row.member != removed_row.oid
                    && row.grantor != removed_row.oid
            });
            self.catalog.next_oid = control.next_oid;
            self.catalog.next_rel_number = control.next_rel_number;
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

    #[cfg(test)]
    pub fn grant_role_membership_direct(
        &mut self,
        membership: &NewRoleMembership,
    ) -> Result<PgAuthMembersRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut auth_members = self.catalog.auth_members.clone();
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
            self.catalog.auth_members = auth_members;
            self.catalog.next_oid = control.next_oid;
            self.catalog.next_rel_number = control.next_rel_number;
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

    #[cfg(test)]
    pub fn update_role_membership_options_direct(
        &mut self,
        roleid: u32,
        member: u32,
        grantor: u32,
        admin_option: bool,
        inherit_option: bool,
        set_option: bool,
    ) -> Result<PgAuthMembersRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut auth_members = self.catalog.auth_members.clone();
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
            self.catalog.auth_members = auth_members;
            self.catalog.next_oid = control.next_oid;
            self.catalog.next_rel_number = control.next_rel_number;
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

    #[cfg(test)]
    pub fn revoke_role_membership_direct(
        &mut self,
        roleid: u32,
        member: u32,
        grantor: u32,
    ) -> Result<PgAuthMembersRow, CatalogError> {
        if matches!(&self.mode, CatalogStoreMode::Durable { .. }) {
            let mut auth_members = self.catalog.auth_members.clone();
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
            self.catalog.auth_members = auth_members;
            self.catalog.next_oid = control.next_oid;
            self.catalog.next_rel_number = control.next_rel_number;
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

    pub fn create_role_mvcc(
        &mut self,
        role_name: &str,
        attrs: &RoleAttributes,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgAuthIdRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_role_catcache_for_ctx(self, ctx)?;
        let mut authids = catcache.authid_rows();
        let mut next_oid = self.allocate_next_oid(0)?;
        let row = create_role_row(&mut authids, &mut next_oid, role_name, attrs)?;
        let kinds = [BootstrapCatalogKind::PgAuthId];
        let rows = PhysicalCatalogRows {
            authids: vec![row.clone()],
            ..PhysicalCatalogRows::default()
        };
        insert_catalog_rows_subset_mvcc(ctx, &rows, self.scope_db_oid(), &kinds)?;
        Ok((row, role_catalog_effect(&kinds)))
    }

    pub fn rename_role_mvcc(
        &mut self,
        role_name: &str,
        new_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgAuthIdRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_role_catcache_for_ctx(self, ctx)?;
        let mut authids = catcache.authid_rows();
        let old_row = authids
            .iter()
            .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
        let row = rename_role_row(&mut authids, role_name, new_name)?;
        let kinds = [BootstrapCatalogKind::PgAuthId];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                authids: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                authids: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        Ok((row, role_catalog_effect(&kinds)))
    }

    pub fn alter_role_attributes_mvcc(
        &mut self,
        role_name: &str,
        attrs: &RoleAttributes,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgAuthIdRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_role_catcache_for_ctx(self, ctx)?;
        let mut authids = catcache.authid_rows();
        let old_row = authids
            .iter()
            .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(role_name.to_string()))?;
        let row = alter_role_row(&mut authids, role_name, attrs)?;
        let kinds = [BootstrapCatalogKind::PgAuthId];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                authids: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                authids: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        Ok((row, role_catalog_effect(&kinds)))
    }

    pub fn drop_role_mvcc(
        &mut self,
        role_name: &str,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgAuthIdRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_role_catcache_for_ctx(self, ctx)?;
        let mut authids = catcache.authid_rows();
        let auth_members = catcache.auth_members_rows();
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
        let kinds = [
            BootstrapCatalogKind::PgAuthId,
            BootstrapCatalogKind::PgAuthMembers,
        ];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                authids: vec![removed_row.clone()],
                auth_members: removed_members,
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        Ok((removed_row, role_catalog_effect(&kinds)))
    }

    pub fn grant_role_membership_mvcc(
        &mut self,
        membership: &NewRoleMembership,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgAuthMembersRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_role_catcache_for_ctx(self, ctx)?;
        let mut auth_members = catcache.auth_members_rows();
        let mut next_oid = self.allocate_next_oid(0)?;
        let row = grant_role_membership_row(&mut auth_members, &mut next_oid, membership)?;
        let kinds = [BootstrapCatalogKind::PgAuthMembers];
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                auth_members: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        Ok((row, role_catalog_effect(&kinds)))
    }

    pub fn update_role_membership_options_mvcc(
        &mut self,
        roleid: u32,
        member: u32,
        grantor: u32,
        admin_option: bool,
        inherit_option: bool,
        set_option: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgAuthMembersRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_role_catcache_for_ctx(self, ctx)?;
        let mut auth_members = catcache.auth_members_rows();
        let old_row = auth_members
            .iter()
            .find(|row| row.roleid == roleid && row.member == member && row.grantor == grantor)
            .cloned()
            .ok_or_else(|| CatalogError::UnknownTable(format!("{roleid}/{member}/{grantor}")))?;
        let row = update_role_membership_row(
            &mut auth_members,
            roleid,
            member,
            grantor,
            admin_option,
            inherit_option,
            set_option,
        )?;
        let kinds = [BootstrapCatalogKind::PgAuthMembers];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                auth_members: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        insert_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                auth_members: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        Ok((row, role_catalog_effect(&kinds)))
    }

    pub fn revoke_role_membership_mvcc(
        &mut self,
        roleid: u32,
        member: u32,
        grantor: u32,
        ctx: &CatalogWriteContext,
    ) -> Result<(PgAuthMembersRow, CatalogMutationEffect), CatalogError> {
        let catcache = visible_role_catcache_for_ctx(self, ctx)?;
        let mut auth_members = catcache.auth_members_rows();
        let row = delete_role_membership_row(&mut auth_members, roleid, member, grantor)?;
        let kinds = [BootstrapCatalogKind::PgAuthMembers];
        delete_catalog_rows_subset_mvcc(
            ctx,
            &PhysicalCatalogRows {
                auth_members: vec![row.clone()],
                ..PhysicalCatalogRows::default()
            },
            self.scope_db_oid(),
            &kinds,
        )?;
        Ok((row, role_catalog_effect(&kinds)))
    }
}
