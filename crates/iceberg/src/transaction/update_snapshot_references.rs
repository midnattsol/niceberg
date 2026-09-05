// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::{FormatVersion, MAIN_BRANCH, SnapshotReference, SnapshotRetention};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::util::snapshot::ancestors_of;
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// Updates snapshot branches and tags without writing snapshot or manifest files.
///
/// Mutations require format version V2 or later, since V1 does not persist references.
/// An earlier format upgrade action in the same transaction satisfies this requirement.
///
/// Calls compose in order within this action, but only the final reference values
/// are committed. Snapshots must exist in the original table and at commit time.
/// Earlier transaction actions must not change references touched by this action;
/// put dependent reference operations in a single action instead.
///
/// On refresh, changes to any touched reference (including type and retention)
/// cause a non-retryable conflict rather than rebasing the operation. Catalog
/// requirements assert the original table UUID and reference heads or absence.
/// They cannot detect same-head type/retention changes after refresh, or assert
/// the full metadata version.
pub struct UpdateSnapshotReferencesAction {
    original: Table,
    desired: BTreeMap<String, Option<SnapshotReference>>,
}

impl UpdateSnapshotReferencesAction {
    pub(crate) fn new(table: &Table) -> Self {
        Self {
            original: table.clone(),
            desired: BTreeMap::new(),
        }
    }

    /// Create an absent branch or tag with its typed retention policy.
    /// `main` may only be a branch. Existing references are never overwritten.
    pub fn create_ref(
        mut self,
        name: impl Into<String>,
        reference: SnapshotReference,
    ) -> Result<Self> {
        let name = name.into();
        if name.is_empty() || self.reference(&name).is_some() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Reference name is empty or already exists: {name}"),
            ));
        }
        if name == MAIN_BRANCH && !reference.is_branch() {
            return Err(Error::new(ErrorKind::DataInvalid, "main must be a branch"));
        }
        let valid_retention = match reference.retention {
            SnapshotRetention::Branch {
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                max_ref_age_ms,
            } => {
                min_snapshots_to_keep.is_none_or(|value| value > 0)
                    && max_snapshot_age_ms.is_none_or(|value| value > 0)
                    && max_ref_age_ms.is_none_or(|value| value > 0)
            }
            SnapshotRetention::Tag { max_ref_age_ms } => {
                max_ref_age_ms.is_none_or(|value| value > 0)
            }
        };
        if !valid_retention {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Reference retention values must be positive",
            ));
        }
        self.check_snapshot(reference.snapshot_id)?;
        self.desired.insert(name, Some(reference));
        Ok(self)
    }

    /// Remove an existing branch or tag. Removing `main` is not allowed.
    pub fn remove_ref(mut self, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if name == MAIN_BRANCH || self.reference(&name).is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Cannot remove main or a missing reference: {name}"),
            ));
        }
        self.desired.insert(name, None);
        Ok(self)
    }

    /// Atomically rename an existing branch or tag to an absent name, preserving retention.
    /// Neither name may be `main`.
    pub fn rename_ref(
        mut self,
        source: impl Into<String>,
        destination: impl Into<String>,
    ) -> Result<Self> {
        let source = source.into();
        let destination = destination.into();
        if source == MAIN_BRANCH
            || destination == MAIN_BRANCH
            || destination.is_empty()
            || self.reference(&destination).is_some()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot rename main, or rename to an empty or existing reference",
            ));
        }
        let reference = self.reference(&source).cloned().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Reference does not exist: {source}"),
            )
        })?;
        self.desired.insert(source, None);
        self.desired.insert(destination, Some(reference));
        Ok(self)
    }

    /// Advance an existing branch to a descendant snapshot (or its current head).
    /// The branch's retention policy is preserved; tags cannot be advanced.
    pub fn fast_forward_branch(self, name: impl Into<String>, snapshot_id: i64) -> Result<Self> {
        let name = name.into();
        let reference = self.reference(&name).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Reference does not exist: {name}"),
            )
        })?;
        if !ancestors_of(&self.original.metadata_ref(), snapshot_id)
            .any(|snapshot| snapshot.snapshot_id() == reference.snapshot_id)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Snapshot {snapshot_id} is not a descendant of branch {name}"),
            ));
        }
        self.set_branch_snapshot(name, snapshot_id)
    }

    /// Set an existing branch to any existing snapshot, preserving retention.
    /// Unlike fast-forward, this permits rollback or switching lineage. Tags are rejected.
    pub fn set_branch_snapshot(
        mut self,
        name: impl Into<String>,
        snapshot_id: i64,
    ) -> Result<Self> {
        let name = name.into();
        let mut reference = self.reference(&name).cloned().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Reference does not exist: {name}"),
            )
        })?;
        if !reference.is_branch() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Reference is not a branch: {name}"),
            ));
        }
        self.check_snapshot(snapshot_id)?;
        reference.snapshot_id = snapshot_id;
        self.desired.insert(name, Some(reference));
        Ok(self)
    }

    fn reference(&self, name: &str) -> Option<&SnapshotReference> {
        match self.desired.get(name) {
            Some(reference) => reference.as_ref(),
            None => self.original.metadata().refs.get(name),
        }
    }

    fn check_snapshot(&self, snapshot_id: i64) -> Result<()> {
        if self
            .original
            .metadata()
            .snapshot_by_id(snapshot_id)
            .is_none()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unknown snapshot: {snapshot_id}"),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionAction for UpdateSnapshotReferencesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let original = self.original.metadata();
        if table.identifier() != self.original.identifier()
            || table.metadata().uuid() != original.uuid()
        {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "Snapshot reference action belongs to a different table",
            ));
        }
        let mut requirements = vec![TableRequirement::UuidMatch {
            uuid: original.uuid(),
        }];
        let mut updates = vec![];
        for (name, desired) in &self.desired {
            let expected = original.refs.get(name);
            if table.metadata().refs.get(name) != expected {
                return Err(Error::new(
                    ErrorKind::CatalogCommitConflicts,
                    format!("Snapshot reference changed since action creation: {name}"),
                ));
            }
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: name.clone(),
                snapshot_id: expected.map(|reference| reference.snapshot_id),
            });
            if let Some(reference) = desired
                && table
                    .metadata()
                    .snapshot_by_id(reference.snapshot_id)
                    .is_none()
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Unknown snapshot: {}", reference.snapshot_id),
                ));
            }
            if desired.as_ref() == expected {
                continue;
            }
            updates.push(match desired {
                Some(reference) => TableUpdate::SetSnapshotRef {
                    ref_name: name.clone(),
                    reference: reference.clone(),
                },
                None => TableUpdate::RemoveSnapshotRef {
                    ref_name: name.clone(),
                },
            });
        }
        if !updates.is_empty() && table.metadata().format_version() < FormatVersion::V2 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Snapshot reference mutations require format version V2 or later",
            ));
        }
        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MockCatalog;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{Operation, Snapshot, Summary, TableMetadata};
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, TableCommit, TableCreation};

    const OLD: i64 = 3051729675574597004;
    const HEAD: i64 = 3055729675574597004;

    fn make_v1_table() -> Table {
        let snapshot = Snapshot::builder()
            .with_snapshot_id(OLD)
            .with_sequence_number(0)
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_manifest_list("/snapshot.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: Default::default(),
            })
            .build();
        Transaction::update_table_metadata(crate::transaction::tests::make_v1_table(), &[
            TableUpdate::AddSnapshot { snapshot },
        ])
        .unwrap()
    }

    fn make_v2_table() -> Table {
        // TableCommit requires a versioned metadata filename, unlike the fixture placeholder.
        crate::transaction::tests::make_v2_table().with_metadata_location(
            "s3://bucket/test/location/metadata/00000-9c12d441-03fe-4693-9a96-a0705ddf69c1.metadata.json".to_string(),
        )
    }

    fn branch(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference::new(
            snapshot_id,
            SnapshotRetention::branch(Some(3), Some(1000), Some(2000)),
        )
    }

    fn tag(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference::new(snapshot_id, SnapshotRetention::Tag {
            max_ref_age_ms: Some(3000),
        })
    }

    fn with_ref(table: &Table, name: &str, reference: SnapshotReference) -> Table {
        Transaction::update_table_metadata(table.clone(), &[TableUpdate::SetSnapshotRef {
            ref_name: name.to_string(),
            reference,
        }])
        .unwrap()
    }

    async fn commit_of(table: &Table, action: UpdateSnapshotReferencesAction) -> TableCommit {
        let mut commit = Arc::new(action).commit(table).await.unwrap();
        TableCommit::builder()
            .ident(table.identifier().clone())
            .updates(commit.take_updates())
            .requirements(commit.take_requirements())
            .build()
    }

    #[tokio::test]
    async fn v1_mutation_rejected_without_catalog_update() {
        let table = make_v1_table();
        let reference = tag(OLD);
        let refreshed = table.clone();
        let mut catalog = MockCatalog::new();
        catalog.expect_load_table().times(1).returning_st(move |_| {
            let table = refreshed.clone();
            Box::pin(async move { Ok(table) })
        });
        catalog.expect_update_table().times(0);

        let tx = Transaction::new(&table);
        let error = tx
            .update_snapshot_references()
            .create_ref("tag", reference)
            .unwrap()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(!error.retryable());
        assert!(
            error
                .message()
                .contains("require format version V2 or later")
        );
    }

    #[tokio::test]
    async fn v1_upgrade_then_create_tag_survives_metadata_roundtrip() {
        let table = make_v1_table().with_metadata_location(
            "s3://bucket/test/location/metadata/00000-9c12d441-03fe-4693-9a96-a0705ddf69c1.metadata.json".to_string(),
        );
        let reference = tag(OLD);
        let refreshed = table.clone();
        let current = table.clone();
        let mut catalog = MockCatalog::new();
        catalog.expect_load_table().times(1).returning_st(move |_| {
            let table = refreshed.clone();
            Box::pin(async move { Ok(table) })
        });
        catalog
            .expect_update_table()
            .times(1)
            .returning_st(move |commit| {
                let table = current.clone();
                Box::pin(async move {
                    let table = commit.apply(table)?;
                    let metadata: TableMetadata =
                        serde_json::from_str(&serde_json::to_string(table.metadata()).unwrap())
                            .unwrap();
                    Ok(table.with_metadata(Arc::new(metadata)))
                })
            });

        let tx = Transaction::new(&table);
        let tx = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V2)
            .apply(tx)
            .unwrap();
        let table = tx
            .update_snapshot_references()
            .create_ref("tag", reference.clone())
            .unwrap()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        assert_eq!(table.metadata().format_version(), FormatVersion::V2);
        assert_eq!(table.metadata().refs.get("tag"), Some(&reference));
    }

    #[tokio::test]
    async fn typed_creation_and_normalized_batch() {
        let table = make_v2_table();
        let action = Transaction::new(&table)
            .update_snapshot_references()
            .create_ref("temporary", branch(OLD))
            .unwrap()
            .fast_forward_branch("temporary", HEAD)
            .unwrap()
            .rename_ref("temporary", "branch")
            .unwrap()
            .create_ref("tag", tag(OLD))
            .unwrap();
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        assert_eq!(commit.take_updates(), vec![
            TableUpdate::SetSnapshotRef {
                ref_name: "branch".into(),
                reference: branch(HEAD)
            },
            TableUpdate::SetSnapshotRef {
                ref_name: "tag".into(),
                reference: tag(OLD)
            },
        ]);
        assert_eq!(commit.take_requirements(), vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid()
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "branch".into(),
                snapshot_id: None
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "tag".into(),
                snapshot_id: None
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "temporary".into(),
                snapshot_id: None
            },
        ]);
    }

    #[tokio::test]
    async fn rename_is_atomic_and_asserts_both_original_heads() {
        for reference in [branch(OLD), tag(OLD)] {
            let table = with_ref(&make_v2_table(), "source", reference.clone());
            let action = || {
                Transaction::new(&table)
                    .update_snapshot_references()
                    .rename_ref("source", "destination")
                    .unwrap()
            };
            let updated = commit_of(&table, action())
                .await
                .apply(table.clone())
                .unwrap();
            assert!(!updated.metadata().refs.contains_key("source"));
            assert_eq!(updated.metadata().refs.get("destination"), Some(&reference));
            for raced in [
                with_ref(&table, "source", branch(HEAD)),
                with_ref(&table, "destination", tag(HEAD)),
            ] {
                let before = raced.metadata().clone();
                let error = commit_of(&table, action())
                    .await
                    .apply(raced.clone())
                    .unwrap_err();
                assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
                assert!(error.retryable());
                assert_eq!(raced.metadata(), &before);
            }
        }
    }

    #[tokio::test]
    async fn refresh_rejects_full_ref_changes_without_catalog_update() {
        let table = with_ref(&make_v2_table(), "branch", branch(OLD));
        for reference in [
            branch(HEAD),
            SnapshotReference::new(OLD, SnapshotRetention::branch(None, None, None)),
            tag(OLD),
        ] {
            let refreshed = with_ref(&table, "branch", reference);
            let mut catalog = MockCatalog::new();
            catalog.expect_load_table().times(1).returning_st(move |_| {
                let table = refreshed.clone();
                Box::pin(async move { Ok(table) })
            });
            catalog.expect_update_table().times(0);
            let tx = Transaction::new(&table);
            let tx = tx
                .update_snapshot_references()
                .fast_forward_branch("branch", HEAD)
                .unwrap()
                .apply(tx)
                .unwrap();
            let error = tx.commit(&catalog).await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
            assert!(!error.retryable());
        }
    }

    #[tokio::test]
    async fn refresh_rejects_created_or_removed_refs_and_wrong_table() {
        let table = make_v2_table();
        let action = Transaction::new(&table)
            .update_snapshot_references()
            .create_ref("new", tag(OLD))
            .unwrap();
        let error = Arc::new(action)
            .commit(&with_ref(&table, "new", tag(HEAD)))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(!error.retryable());
        let referenced = with_ref(&table, "gone", tag(OLD));
        let action = Transaction::new(&referenced)
            .update_snapshot_references()
            .remove_ref("gone")
            .unwrap();
        assert!(Arc::new(action).commit(&table).await.is_err());
        let other = Transaction::update_table_metadata(table.clone(), &[TableUpdate::AssignUuid {
            uuid: uuid::Uuid::new_v4(),
        }])
        .unwrap();
        let action = Transaction::new(&table)
            .update_snapshot_references()
            .create_ref("new", tag(OLD))
            .unwrap();
        assert!(Arc::new(action).commit(&other).await.is_err());
    }

    #[tokio::test]
    async fn catalog_race_is_not_rebased_on_retry() {
        let table = with_ref(&make_v2_table(), "source", branch(OLD));
        let raced = with_ref(&table, "source", branch(HEAD));
        let mut catalog = MockCatalog::new();
        let mut sequence = mockall::Sequence::new();
        let original = table.clone();
        catalog
            .expect_load_table()
            .times(1)
            .in_sequence(&mut sequence)
            .returning_st(move |_| {
                let table = original.clone();
                Box::pin(async move { Ok(table) })
            });
        let concurrent = raced.clone();
        catalog
            .expect_update_table()
            .times(1)
            .in_sequence(&mut sequence)
            .returning_st(move |commit| {
                let table = concurrent.clone();
                Box::pin(async move { commit.apply(table) })
            });
        catalog
            .expect_load_table()
            .times(1)
            .in_sequence(&mut sequence)
            .returning_st(move |_| {
                let table = raced.clone();
                Box::pin(async move { Ok(table) })
            });
        let tx = Transaction::new(&table);
        let error = tx
            .update_snapshot_references()
            .rename_ref("source", "dest")
            .unwrap()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(!error.retryable());
        assert!(error.message().contains("changed since action creation"));
    }

    #[tokio::test]
    async fn unrelated_refresh_is_allowed_but_protocol_cannot_assert_retention() {
        let table = with_ref(&make_v2_table(), "source", branch(OLD));
        let action = || {
            Transaction::new(&table)
                .update_snapshot_references()
                .rename_ref("source", "dest")
                .unwrap()
        };
        let refreshed = with_ref(&table, "unrelated", tag(HEAD));
        assert!(Arc::new(action()).commit(&refreshed).await.is_ok());
        let raced = with_ref(&table, "source", tag(OLD));
        // Standard requirements compare heads, not the full reference value.
        assert!(commit_of(&table, action()).await.apply(raced).is_ok());
    }

    #[tokio::test]
    async fn target_snapshot_must_still_exist_at_refresh() {
        let table = make_v2_table();
        let action = Transaction::new(&table)
            .update_snapshot_references()
            .create_ref("tag", tag(OLD))
            .unwrap();
        let refreshed =
            Transaction::update_table_metadata(table.clone(), &[TableUpdate::RemoveSnapshots {
                snapshot_ids: vec![OLD],
            }])
            .unwrap();
        let error = Arc::new(action).commit(&refreshed).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn branch_moves_preserve_retention_and_validate_ancestry() {
        let table = with_ref(&make_v2_table(), "branch", branch(OLD));
        let action = Transaction::new(&table)
            .update_snapshot_references()
            .fast_forward_branch("branch", HEAD)
            .unwrap();
        let updated = commit_of(&table, action)
            .await
            .apply(table.clone())
            .unwrap();
        assert_eq!(updated.metadata().refs.get("branch"), Some(&branch(HEAD)));
        assert_eq!(updated.metadata().current_snapshot_id(), Some(HEAD));
        let tx = Transaction::new(&updated);
        assert!(
            tx.update_snapshot_references()
                .fast_forward_branch("branch", OLD)
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .fast_forward_branch("branch", HEAD)
                .is_ok()
        );
        let reset = tx
            .update_snapshot_references()
            .set_branch_snapshot("branch", OLD)
            .unwrap();
        let updated = commit_of(&updated, reset).await.apply(updated).unwrap();
        assert_eq!(updated.metadata().refs.get("branch"), Some(&branch(OLD)));
        let reset = Transaction::new(&updated)
            .update_snapshot_references()
            .set_branch_snapshot(MAIN_BRANCH, OLD)
            .unwrap();
        let updated = commit_of(&updated, reset).await.apply(updated).unwrap();
        assert_eq!(updated.metadata().current_snapshot_id(), Some(OLD));

        let divergent = Snapshot::builder()
            .with_snapshot_id(123)
            .with_sequence_number(35)
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_manifest_list("/divergent.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: Default::default(),
            })
            .build();
        let table = Transaction::update_table_metadata(updated, &[TableUpdate::AddSnapshot {
            snapshot: divergent,
        }])
        .unwrap();
        let tx = Transaction::new(&table);
        assert!(
            tx.update_snapshot_references()
                .fast_forward_branch("branch", 123)
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .set_branch_snapshot("branch", 123)
                .is_ok()
        );
    }

    #[test]
    fn invalid_operations() {
        let table = with_ref(&make_v2_table(), "tag", tag(OLD));
        let tx = Transaction::new(&table);
        assert!(
            tx.update_snapshot_references()
                .create_ref("tag", branch(HEAD))
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .create_ref("", tag(OLD))
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .create_ref("missing", tag(-123))
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .create_ref(
                    "invalid",
                    SnapshotReference::new(OLD, SnapshotRetention::Tag {
                        max_ref_age_ms: Some(0)
                    })
                )
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .remove_ref(MAIN_BRANCH)
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .remove_ref("missing")
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .rename_ref(MAIN_BRANCH, "other")
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .rename_ref("tag", MAIN_BRANCH)
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .rename_ref("tag", "tag")
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .rename_ref("missing", "other")
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .set_branch_snapshot("tag", HEAD)
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .fast_forward_branch("tag", HEAD)
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .set_branch_snapshot("missing", HEAD)
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .set_branch_snapshot(MAIN_BRANCH, -123)
                .is_err()
        );
        assert!(
            tx.update_snapshot_references()
                .fast_forward_branch(MAIN_BRANCH, -123)
                .is_err()
        );
        let empty =
            Transaction::update_table_metadata(table.clone(), &[TableUpdate::RemoveSnapshotRef {
                ref_name: MAIN_BRANCH.into(),
            }])
            .unwrap();
        assert!(
            Transaction::new(&empty)
                .update_snapshot_references()
                .create_ref(MAIN_BRANCH, tag(OLD))
                .is_err()
        );
    }

    #[tokio::test]
    async fn dependent_actions_fail_instead_of_asserting_intermediate_heads() {
        let table = with_ref(&make_v2_table(), "branch", branch(OLD));
        let tx = Transaction::new(&table);
        let tx = tx
            .update_snapshot_references()
            .set_branch_snapshot("branch", HEAD)
            .unwrap()
            .apply(tx)
            .unwrap();
        let mut tx = tx
            .update_snapshot_references()
            .rename_ref("branch", "renamed")
            .unwrap()
            .apply(tx)
            .unwrap();
        let mut catalog = MockCatalog::new();
        catalog.expect_load_table().returning_st(move |_| {
            let table = table.clone();
            Box::pin(async move { Ok(table) })
        });
        catalog.expect_update_table().times(0);
        let error = tx.do_commit(&catalog).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(!error.retryable());
    }

    #[tokio::test]
    async fn memory_catalog_roundtrip() {
        let catalog = new_memory_catalog().await;
        let fixture = make_v2_table();
        let ident = fixture.identifier();
        catalog
            .create_namespace(ident.namespace(), Default::default())
            .await
            .unwrap();
        let table = catalog
            .create_table(
                ident.namespace(),
                TableCreation::builder()
                    .name(ident.name().to_string())
                    .schema((**fixture.metadata().current_schema()).clone())
                    .build(),
            )
            .await
            .unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        let snapshots = [(OLD, None), (HEAD, Some(OLD))]
            .into_iter()
            .enumerate()
            .map(|(i, (id, parent))| TableUpdate::AddSnapshot {
                snapshot: Snapshot::builder()
                    .with_snapshot_id(id)
                    .with_parent_snapshot_id(parent)
                    .with_sequence_number(i as i64 + 1)
                    .with_timestamp_ms(now + i as i64)
                    .with_schema_id(table.metadata().current_schema_id())
                    .with_manifest_list(format!("/snapshot-{id}.avro"))
                    .with_summary(Summary {
                        operation: Operation::Append,
                        additional_properties: Default::default(),
                    })
                    .build(),
            })
            .collect();
        let table = catalog
            .update_table(
                TableCommit::builder()
                    .ident(ident.clone())
                    .updates(snapshots)
                    .requirements(vec![])
                    .build(),
            )
            .await
            .unwrap();
        let tx = Transaction::new(&table);
        let table = tx
            .update_snapshot_references()
            .create_ref(MAIN_BRANCH, branch(HEAD))
            .unwrap()
            .create_ref("branch", branch(OLD))
            .unwrap()
            .create_ref("tag", tag(OLD))
            .unwrap()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let tx = Transaction::new(&table);
        let table = tx
            .update_snapshot_references()
            .fast_forward_branch("branch", HEAD)
            .unwrap()
            .rename_ref("branch", "renamed")
            .unwrap()
            .remove_ref("tag")
            .unwrap()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let loaded = catalog.load_table(ident).await.unwrap();
        assert_eq!(loaded.metadata(), table.metadata());
        assert_eq!(loaded.metadata().refs.get("renamed"), Some(&branch(HEAD)));
        assert!(!loaded.metadata().refs.contains_key("branch"));
        assert!(!loaded.metadata().refs.contains_key("tag"));
        assert_eq!(loaded.metadata().current_snapshot_id(), Some(HEAD));
    }
}
