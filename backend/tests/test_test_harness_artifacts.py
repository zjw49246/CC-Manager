from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from backend.services.test_harness_artifacts import (
    TestHarnessArtifactError as ArtifactError,
    TestHarnessArtifactQuotaError as ArtifactQuotaError,
    TestHarnessArtifactStore as ArtifactStore,
)


PNG = b"\x89PNG\r\n\x1a\nimage-evidence"


def _store(tmp_path, **overrides) -> ArtifactStore:
    values = {
        "max_file_bytes": 64,
        "max_run_bytes": 128,
        "max_task_bytes": 256,
        "max_total_bytes": 512,
        "retention_days": 2,
        **overrides,
    }
    return ArtifactStore(tmp_path / "evidence", **values)


def test_archive_survives_source_removal_and_reopens_by_relative_key(tmp_path):
    store = _store(tmp_path)
    source = tmp_path / "source.png"
    source.write_bytes(PNG)

    archived = store.archive(
        source,
        task_id=7,
        run_id="a" * 32,
        attempt_id="b" * 32,
        name="final.png",
    )
    assert not os.path.isabs(archived.storage_key)
    assert archived.storage_key.startswith("runs/task-7/")
    source.unlink()

    restarted = _store(tmp_path)
    opened = restarted.open(
        archived.storage_key,
        expected_sha256=archived.sha256,
        expected_size=archived.byte_size,
    )
    try:
        assert b"".join(opened.chunks()) == PNG
    finally:
        opened.close()


def test_archive_rejects_symlink_and_invalid_png(tmp_path):
    store = _store(tmp_path)
    real = tmp_path / "real.png"
    real.write_bytes(PNG)
    link = tmp_path / "link.png"
    link.symlink_to(real)

    with pytest.raises(ArtifactError, match="opened safely"):
        store.archive(
            link,
            task_id=1,
            run_id="a" * 32,
            attempt_id="b" * 32,
            name="final.png",
        )

    invalid = tmp_path / "invalid.png"
    invalid.write_bytes(b"not a png")
    with pytest.raises(ArtifactError, match="valid PNG"):
        store.archive(
            invalid,
            task_id=1,
            run_id="a" * 32,
            attempt_id="b" * 32,
            name="final.png",
        )


def test_open_detects_tampering_without_reopening_an_unchecked_path(tmp_path):
    store = _store(tmp_path)
    source = tmp_path / "source.png"
    source.write_bytes(PNG)
    archived = store.archive(
        source,
        task_id=1,
        run_id="a" * 32,
        attempt_id="b" * 32,
        name="final.png",
    )
    archived.path.write_bytes(PNG + b"tampered")

    with pytest.raises(ArtifactError, match="size does not match|integrity"):
        store.open(
            archived.storage_key,
            expected_sha256=archived.sha256,
            expected_size=archived.byte_size,
        )


def test_rearchive_repairs_tampered_content_addressed_destination(tmp_path):
    store = _store(tmp_path)
    source = tmp_path / "source.png"
    source.write_bytes(PNG)
    archived = store.archive(
        source,
        task_id=1,
        run_id="a" * 32,
        attempt_id="b" * 32,
        name="final.png",
    )
    archived.path.write_bytes(PNG[:-1] + b"X")

    repaired = store.archive(
        source,
        task_id=1,
        run_id="a" * 32,
        attempt_id="b" * 32,
        name="final.png",
    )

    assert repaired.storage_key == archived.storage_key
    opened = store.open(
        repaired.storage_key,
        expected_sha256=repaired.sha256,
        expected_size=repaired.byte_size,
    )
    try:
        assert b"".join(opened.chunks()) == PNG
    finally:
        opened.close()


def test_file_and_job_quotas_fail_closed(tmp_path):
    store = _store(
        tmp_path,
        max_file_bytes=16,
        max_run_bytes=32,
        max_task_bytes=64,
        max_total_bytes=128,
    )
    source = tmp_path / "source.png"
    source.write_bytes(PNG)
    with pytest.raises(ArtifactQuotaError, match="exceeds"):
        store.archive(
            source,
            task_id=1,
            run_id="a" * 32,
            attempt_id="b" * 32,
            name="final.png",
        )

    job_dir = store.create_job_dir("c" * 32)
    with pytest.raises(ArtifactQuotaError, match="exceeds"):
        store.ensure_job_capacity(job_dir, "report.md", 17)


def test_orphan_and_expired_job_cleanup_is_scoped_to_managed_root(tmp_path):
    store = _store(tmp_path)
    source = tmp_path / "source.png"
    source.write_bytes(PNG)
    archived = store.archive(
        source,
        task_id=1,
        run_id="a" * 32,
        attempt_id="b" * 32,
        name="final.png",
    )
    old = (datetime.now(timezone.utc) - timedelta(days=3)).timestamp()
    os.utime(archived.path, (old, old))
    assert store.cleanup_orphan_archives(set()) == 1
    assert not archived.path.exists()

    job_dir = store.create_job_dir("c" * 32)
    job_dir.joinpath("report.md").write_text("report", encoding="utf-8")
    os.utime(job_dir, (old, old))
    outside = tmp_path / "outside.txt"
    outside.write_text("keep", encoding="utf-8")
    assert store.cleanup_job_dirs(now=datetime.now(timezone.utc)) == 1
    assert not job_dir.exists()
    assert outside.read_text(encoding="utf-8") == "keep"


def test_job_cleanup_reclaims_oldest_eligible_staging_at_quota_equality(tmp_path):
    store = _store(
        tmp_path,
        max_file_bytes=8,
        max_run_bytes=8,
        max_task_bytes=8,
        max_total_bytes=8,
    )
    oldest = store.create_job_dir("1" * 32)
    oldest.joinpath("report.md").write_bytes(b"12345678")
    newer = store.jobs_root / ("2" * 32)
    newer.mkdir(mode=0o700)
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).timestamp()
    os.utime(oldest, (old, old))

    assert store.total_bytes() == store.max_total_bytes
    assert store.cleanup_job_dirs(active_job_ids={newer.name}) == 1
    assert not oldest.exists()
    assert newer.exists()


def test_required_admission_capacity_reclaims_oldest_unprotected_staging(tmp_path):
    store = _store(
        tmp_path,
        max_file_bytes=8,
        max_run_bytes=8,
        max_task_bytes=16,
        max_total_bytes=16,
    )
    oldest = store.create_job_dir("1" * 32)
    store.write_job_bytes(oldest, "report.md", b"a" * 8)
    protected = store.create_job_dir("2" * 32)
    store.write_job_bytes(protected, "report.md", b"b" * 8)
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).timestamp()
    new = (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp()
    os.utime(oldest, (old, old))
    os.utime(protected, (new, new))

    assert store.cleanup_job_dirs(
        active_job_ids={protected.name},
        required_free_bytes=1,
    ) == 1
    assert not oldest.exists()
    assert protected.exists()
    assert store.total_bytes() < store.max_total_bytes


def test_shared_root_lock_makes_job_quota_check_and_write_atomic(tmp_path):
    limits = {
        "max_file_bytes": 6,
        "max_run_bytes": 8,
        "max_task_bytes": 8,
        "max_total_bytes": 8,
    }
    first_store = _store(tmp_path, **limits)
    second_store = _store(tmp_path, **limits)
    first_dir = first_store.create_job_dir("1" * 32)
    second_dir = second_store.create_job_dir("2" * 32)
    barrier = threading.Barrier(2)

    def write(store, job_dir, name):
        barrier.wait()
        try:
            store.write_job_bytes(job_dir, name, b"123456")
        except ArtifactQuotaError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = [
            executor.submit(
                write,
                first_store,
                first_dir,
                "report.md",
            ),
            executor.submit(
                write,
                second_store,
                second_dir,
                "telemetry.json",
            ),
        ]

    assert sorted(future.result() for future in outcomes) == [False, True]
    assert first_store.total_bytes() == 6


@pytest.mark.parametrize("quota_scope", ["run", "task", "total"])
def test_concurrent_archives_cannot_overshoot_scoped_quota(tmp_path, quota_scope):
    limits = {
        "max_file_bytes": 6,
        "max_run_bytes": 6 if quota_scope != "run" else 8,
        "max_task_bytes": 8 if quota_scope == "task" else 16,
        "max_total_bytes": 8 if quota_scope == "total" else 64,
    }
    if quota_scope == "total":
        limits["max_task_bytes"] = 6
    first_store = _store(tmp_path, **limits)
    second_store = _store(tmp_path, **limits)
    first_source = tmp_path / "first.md"
    second_source = tmp_path / "second.md"
    first_source.write_bytes(b"123456")
    second_source.write_bytes(b"abcdef")
    barrier = threading.Barrier(2)

    first_identity = {
        "task_id": 7,
        "run_id": "a" * 32,
        "attempt_id": "1" * 32,
    }
    if quota_scope == "run":
        second_identity = {
            "task_id": 7,
            "run_id": "a" * 32,
            "attempt_id": "2" * 32,
        }
    elif quota_scope == "task":
        second_identity = {
            "task_id": 7,
            "run_id": "b" * 32,
            "attempt_id": "2" * 32,
        }
    else:
        second_identity = {
            "task_id": 8,
            "run_id": "b" * 32,
            "attempt_id": "2" * 32,
        }

    def archive(store, source, identity):
        barrier.wait()
        try:
            store.archive(source, name="report.md", **identity)
        except ArtifactQuotaError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = [
            executor.submit(
                archive,
                first_store,
                first_source,
                first_identity,
            ),
            executor.submit(
                archive,
                second_store,
                second_source,
                second_identity,
            ),
        ]

    assert sorted(future.result() for future in outcomes) == [False, True]
    assert first_store.total_bytes() == 6
