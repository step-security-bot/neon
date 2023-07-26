import os
import shutil
from contextlib import closing

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    RemoteStorageKind,
    available_s3_storages,
)
from fixtures.pg_version import PgVersion


def add_pgdir_prefix(pgversion, files):
    return [f"pg_install/v{pgversion}/" + x for x in files]


# Cleaning up downloaded files is important for local tests
# or else one test could reuse the files from another test or another test run
def cleanup(cleanup_files, cleanup_folders):
    for file in cleanup_files:
        try:
            os.remove(file)
            log.info(f"removed file {file}")
        except Exception as err:
            log.info(f"error removing file {file}: {err}")

    for folder in cleanup_folders:
        try:
            shutil.rmtree(folder)
            log.info(f"removed folder {folder}")
        except Exception as err:
            log.info(f"error removing folder {folder}: {err}")


# Test downloading remote extension.
@pytest.mark.parametrize("remote_storage_kind", available_s3_storages())
def test_remote_extensions(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    pg_version: PgVersion,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_extensions",
        enable_remote_extensions=True,
    )
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_remote_extensions", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None  # satisfy mypy
    assert env.remote_storage_client is not None  # satisfy mypy

    # For MOCK_S3 we upload test files.
    # For REAL_S3 we use the files already in the bucket
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        log.info("Uploading test files to mock bucket")
        os.chdir("test_runner/regress/data/extension_test")
        for path in os.walk("."):
            prefix, _, files = path
            for file in files:
                # the [2:] is to remove the leading "./"
                full_path = os.path.join(prefix, file)[2:]

                with open(full_path, "rb") as f:
                    log.info(f"UPLOAD {full_path} to ext/{full_path}")
                    env.remote_storage_client.upload_fileobj(
                        f,
                        env.ext_remote_storage.bucket_name,
                        f"ext/{full_path}",
                    )
        os.chdir("../../../..")
    # Start a compute node and check that it can download the extensions
    # and use them to CREATE EXTENSION and LOAD
    endpoint = env.endpoints.create_start(
        "test_remote_extensions",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
        # config_lines=["log_min_messages=debug3"],
    )
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            # Check that appropriate control files were downloaded
            cur.execute("SELECT * FROM pg_available_extensions")
            all_extensions = [x[0] for x in cur.fetchall()]
            log.info(all_extensions)
            assert "anon" in all_extensions
            assert "kq_imcx" in all_extensions

            # TODO: check that we cant't download custom extensions for other tenant ids

            # check that we can download private extension
            # this is expected to fail because we don't have the pgcrypto extension
            try:
                cur.execute("CREATE EXTENSION anon")
            except Exception as err:
                log.info("error creating anon extension")
                assert "pgcrypto" in str(err), "unexpected error creating anon extension"

    cleanup_files = add_pgdir_prefix(
        pg_version,
        [
            "lib/postgresql/anon.so",
            "share/postgresql/extension/anon.control",
        ],
    )
    cleanup_folders = add_pgdir_prefix(
        pg_version, ["share/postgresql/extension/anon", "download_extensions"]
    )
    cleanup(cleanup_files, cleanup_folders)


"""
# Test downloading remote library.
@pytest.mark.parametrize("remote_storage_kind", available_s3_storages())
def test_remote_library(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    pg_version: PgVersion,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_library",
        enable_remote_extensions=True,
    )
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_remote_library", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None  # satisfy mypy
    assert env.remote_storage_client is not None  # satisfy mypy

    # For MOCK_S3 we upload test files.
    # For REAL_S3 we use the files already in the bucket
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        log.info("Uploading test files to mock bucket")
        os.chdir("test_runner/regress/data/extension_test")
        for path in os.walk("."):
            prefix, _, files = path
            for file in files:
                # the [2:] is to remove the leading "./"
                full_path = os.path.join(prefix, file)[2:]

                with open(full_path, "rb") as f:
                    log.info(f"UPLOAD {full_path} to ext/{full_path}")
                    env.remote_storage_client.upload_fileobj(
                        f,
                        env.ext_remote_storage.bucket_name,
                        f"ext/{full_path}",
                    )
        os.chdir("../../../..")
    # Start a compute node and check that it can download the extensions
    # and use them to run LOAD library
    endpoint = env.endpoints.create_start(
        "test_remote_library",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
        # config_lines=["log_min_messages=debug3"],
    )
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            # try to load library
            try:
                cur.execute("LOAD 'anon'")
            except Exception as err:
                log.info(f"error loading anon library: {err}")
                raise AssertionError("unexpected error loading anon library") from err

            # TODO test library which name is different from extension name

    cleanup_files = add_pgdir_prefix(
        pg_version,
        [
            "lib/postgresql/anon.so",
            "share/postgresql/extension/anon.control",
        ],
    )
    cleanup_folders = add_pgdir_prefix(
        pg_version, ["share/postgresql/extension/anon", "download_extensions"]
    )
    cleanup(cleanup_files, cleanup_folders)
"""
