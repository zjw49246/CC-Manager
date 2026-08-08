from __future__ import annotations

import os
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
