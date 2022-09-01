//! Timeline synchronization logic to delete a bulk of timeline's remote files from the remote storage.

use std::path::Path;

use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use tracing::{debug, error, info};

use crate::storage_sync::{SyncQueue, SyncTask};
use remote_storage::{GenericRemoteStorage, RemoteStorage};
use utils::zid::ZTenantTimelineId;

use super::{LayersDeletion, SyncData};

/// Attempts to remove the timleline layers from the remote storage.
/// If the task had not adjusted the metadata before, the deletion will fail.
pub(super) async fn delete_timeline_layers<'a>(
    storage: &'a GenericRemoteStorage,
    sync_queue: &SyncQueue,
    sync_id: ZTenantTimelineId,
    mut delete_data: SyncData<LayersDeletion>,
) -> bool {
    if !delete_data.data.deletion_registered {
        error!("Cannot delete timeline layers before the deletion metadata is not registered, reenqueueing");
        delete_data.retries += 1;
        sync_queue.push(sync_id, SyncTask::Delete(delete_data));
        return false;
    }

    if delete_data.data.layers_to_delete.is_empty() {
        info!("No layers to delete, skipping");
        return true;
    }

    let layers_to_delete = delete_data
        .data
        .layers_to_delete
        .drain()
        .collect::<Vec<_>>();
    debug!("Layers to delete: {layers_to_delete:?}");
    info!("Deleting {} timeline layers", layers_to_delete.len());

    let mut delete_tasks = layers_to_delete
        .into_iter()
        .map(|local_layer_path| async {
            match match storage {
                GenericRemoteStorage::Local(storage) => {
                    remove_storage_object(storage, &local_layer_path).await
                }
                GenericRemoteStorage::S3(storage) => {
                    remove_storage_object(storage, &local_layer_path).await
                }
            } {
                Ok(()) => Ok(local_layer_path),
                Err(e) => Err((e, local_layer_path)),
            }
        })
        .collect::<FuturesUnordered<_>>();

    let mut errored = false;
    while let Some(deletion_result) = delete_tasks.next().await {
        match deletion_result {
            Ok(local_layer_path) => {
                debug!(
                    "Successfully deleted layer {} for timeline {sync_id}",
                    local_layer_path.display()
                );
                delete_data.data.deleted_layers.insert(local_layer_path);
            }
            Err((e, local_layer_path)) => {
                errored = true;
                error!(
                    "Failed to delete layer {} for timeline {sync_id}: {e:?}",
                    local_layer_path.display()
                );
                delete_data.data.layers_to_delete.insert(local_layer_path);
            }
        }
    }

    if errored {
        debug!("Reenqueuing failed delete task for timeline {sync_id}");
        delete_data.retries += 1;
        sync_queue.push(sync_id, SyncTask::Delete(delete_data));
    } else {
        info!("Successfully deleted all layers");
    }
    errored
}

async fn remove_storage_object<P, S>(storage: &S, local_layer_path: &Path) -> anyhow::Result<()>
where
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let storage_path = storage
        .remote_object_id(local_layer_path)
        .with_context(|| {
            format!(
                "Failed to get the layer storage path for local path '{}'",
                local_layer_path.display()
            )
        })?;

    storage.delete(&storage_path).await.with_context(|| {
        format!(
            "Failed to delete remote layer from storage at '{:?}'",
            storage_path
        )
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, num::NonZeroUsize};

    use itertools::Itertools;
    use tempfile::tempdir;
    use tokio::fs;
    use utils::lsn::Lsn;

    use crate::{
        layered_repository::repo_harness::{RepoHarness, TIMELINE_ID},
        storage_sync::test_utils::{create_local_timeline, dummy_metadata},
    };
    use remote_storage::{LocalFs, RemoteStorage};

    use super::*;

    #[tokio::test]
    async fn delete_timeline_negative() -> anyhow::Result<()> {
        let harness = RepoHarness::create("delete_timeline_negative")?;
        let sync_queue = SyncQueue::new(NonZeroUsize::new(100).unwrap());
        let sync_id = ZTenantTimelineId::new(harness.tenant_id, TIMELINE_ID);
        let storage = GenericRemoteStorage::Local(LocalFs::new(
            tempdir()?.path().to_path_buf(),
            harness.conf.workdir.clone(),
        )?);

        let deleted = delete_timeline_layers(
            &storage,
            &sync_queue,
            sync_id,
            SyncData {
                retries: 1,
                data: LayersDeletion {
                    deleted_layers: HashSet::new(),
                    layers_to_delete: HashSet::new(),
                    deletion_registered: false,
                },
            },
        )
        .await;

        assert!(
            !deleted,
            "Should not start the deletion for task with delete metadata unregistered"
        );

        Ok(())
    }

    #[tokio::test]
    async fn delete_timeline() -> anyhow::Result<()> {
        let harness = RepoHarness::create("delete_timeline")?;
        let sync_queue = SyncQueue::new(NonZeroUsize::new(100).unwrap());

        let sync_id = ZTenantTimelineId::new(harness.tenant_id, TIMELINE_ID);
        let layer_files = ["a", "b", "c", "d"];
        let storage = GenericRemoteStorage::Local(LocalFs::new(
            tempdir()?.path().to_path_buf(),
            harness.conf.workdir.clone(),
        )?);

        let local_storage = storage.as_local().unwrap();

        let current_retries = 3;
        let metadata = dummy_metadata(Lsn(0x30));
        let local_timeline_path = harness.timeline_path(&TIMELINE_ID);
        let timeline_upload =
            create_local_timeline(&harness, TIMELINE_ID, &layer_files, metadata.clone()).await?;
        for local_path in timeline_upload.layers_to_upload {
            let remote_path = local_storage.remote_object_id(&local_path)?;
            let remote_parent_dir = remote_path.parent().unwrap();
            if !remote_parent_dir.exists() {
                fs::create_dir_all(&remote_parent_dir).await?;
            }
            fs::copy(&local_path, &remote_path).await?;
        }
        assert_eq!(
            local_storage
                .list()
                .await?
                .into_iter()
                .map(|remote_path| local_storage.local_path(&remote_path).unwrap())
                .filter_map(|local_path| { Some(local_path.file_name()?.to_str()?.to_owned()) })
                .sorted()
                .collect::<Vec<_>>(),
            layer_files
                .iter()
                .map(|layer_str| layer_str.to_string())
                .sorted()
                .collect::<Vec<_>>(),
            "Expect to have all layer files remotely before deletion"
        );

        let deleted = delete_timeline_layers(
            &storage,
            &sync_queue,
            sync_id,
            SyncData {
                retries: current_retries,
                data: LayersDeletion {
                    deleted_layers: HashSet::new(),
                    layers_to_delete: HashSet::from([
                        local_timeline_path.join("a"),
                        local_timeline_path.join("c"),
                        local_timeline_path.join("something_different"),
                    ]),
                    deletion_registered: true,
                },
            },
        )
        .await;
        assert!(deleted, "Should be able to delete timeline files");

        assert_eq!(
            local_storage
                .list()
                .await?
                .into_iter()
                .map(|remote_path| local_storage.local_path(&remote_path).unwrap())
                .filter_map(|local_path| { Some(local_path.file_name()?.to_str()?.to_owned()) })
                .sorted()
                .collect::<Vec<_>>(),
            vec!["b".to_string(), "d".to_string()],
            "Expect to have only non-deleted files remotely"
        );

        Ok(())
    }
}
