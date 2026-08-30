"""Git-object binding tests for immutable pre-PR review subjects."""

from __future__ import annotations

import base64
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest

from backend.services import code_review_subject as subject_module
from backend.services.code_review_subject import (
    MAX_PATCH_BYTES,
    MAX_REVIEW_FILE_BYTES,
    CodeReviewSubjectError,
    GitCommandError,
    RepositoryStateError,
    SubjectChangedError,
    capture_commit_range_subject,
    verify_commit_range_subject,
)


def _git(repo: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", os.fspath(repo), *args],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
        check=False,
        env={**os.environ, "LC_ALL": "C", "GIT_TERMINAL_PROMPT": "0"},
    )
    if result.returncode:
        raise AssertionError(
            f"git {' '.join(args)} failed: "
            f"{result.stderr.decode('utf-8', errors='replace')}"
        )
    return result.stdout


def _init_repo(path: Path) -> Path:
    path.mkdir()
    subprocess.run(
        ["git", "init", "-q", "-b", "main", os.fspath(path)],
        check=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
    )
    _git(path, "config", "user.name", "CCM Tests")
    _git(path, "config", "user.email", "ccm-tests@example.invalid")
    return path


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD").decode("ascii").strip()


def _two_commit_repo(tmp_path: Path, name: str = "repo") -> tuple[Path, str, str]:
    repo = _init_repo(tmp_path / name)
    (repo / "AGENTS.md").write_text("Review exact state transitions.\n")
    (repo / "app.py").write_text("value = 1\n")
    base = _commit(repo, "base")
    (repo / "app.py").write_text("value = 2\n")
    head = _commit(repo, "head")
    return repo, base, head


def _objects_by_utf8_path(captured) -> dict[str, object]:
    return {
        item.path.utf8: item
        for item in captured.files
        if item.path.utf8 is not None
    }


def test_capture_and_verify_bind_raw_patch_tree_files_and_guidance(tmp_path: Path):
    repo = _init_repo(tmp_path / "repo")
    (repo / "AGENTS.md").write_text("Root guidance from committed HEAD.\n")
    (repo / "src").mkdir()
    (repo / "src" / "CLAUDE.md").write_text("Nested exact guidance.\n")
    (repo / "src" / "old.py").write_text(
        "def answer():\n"
        "    value = 1\n"
        "    return value\n"
    )
    base = _commit(repo, "base")

    _git(repo, "mv", "src/old.py", "src/renamed.py")
    (repo / "src" / "renamed.py").write_text(
        "def answer():\n"
        "    value = 2\n"
        "    return value\n"
    )
    (repo / "binary.dat").write_bytes(b"a\x00b\xff")
    (repo / "non-utf8.txt").write_bytes(b"plain-ish-\xff\n")
    os.symlink("/etc/passwd", repo / "external-link")
    (repo / "--no-index").write_text("argv, never shell\n")
    head = _commit(repo, "review subject")

    repo_alias = tmp_path / "repo-alias"
    repo_alias.symlink_to(repo, target_is_directory=True)
    captured = capture_commit_range_subject(
        repo_alias,
        base,
        expected_head_sha=head,
    )

    expected_patch = _git(
        repo,
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--binary",
        "--full-index",
        "--find-renames",
        base,
        head,
        "--",
    )
    assert captured.repo_path == os.fspath(repo.resolve())
    assert captured.patch == expected_patch
    assert captured.patch_sha256 == hashlib.sha256(expected_patch).hexdigest()
    assert captured.head_tree_sha == _git(
        repo, "rev-parse", f"{head}^{{tree}}"
    ).decode("ascii").strip()
    assert captured.detached_head is False
    assert captured.head_ref == "main"
    assert any(item.status.startswith("R") for item in captured.changed_paths)

    objects = _objects_by_utf8_path(captured)
    assert objects["binary.dat"].content == b"a\x00b\xff"
    assert objects["binary.dat"].content_kind == "binary"
    assert objects["non-utf8.txt"].content == b"plain-ish-\xff\n"
    assert objects["non-utf8.txt"].content_kind == "non_utf8"
    assert objects["external-link"].mode == "120000"
    assert objects["external-link"].content == b"/etc/passwd"
    assert objects["external-link"].content_kind == "symlink_target_utf-8"
    assert b"root:" not in objects["external-link"].content
    assert objects["--no-index"].content == b"argv, never shell\n"

    guidance = {item.path.utf8: item.content for item in captured.guidance}
    assert guidance == {
        "AGENTS.md": b"Root guidance from committed HEAD.\n",
        "src/CLAUDE.md": b"Nested exact guidance.\n",
    }
    material = captured.prompt_material()
    assert material["subject"] == captured.subject.as_dict()
    assert captured.prompt_guidance()["subject"] == captured.subject.as_dict()
    rendered = json.dumps(material, ensure_ascii=False).encode("utf-8")
    assert b"base64" in rendered
    assert material["patch_hash_semantics"] == "sha256(raw git-diff stdout bytes)"
    assert verify_commit_range_subject(repo, captured) == captured.subject


@pytest.mark.parametrize("dirty_kind", ["tracked", "untracked"])
def test_capture_rejects_tracked_and_untracked_dirt(tmp_path: Path, dirty_kind: str):
    repo, base, head = _two_commit_repo(tmp_path)
    if dirty_kind == "tracked":
        (repo / "app.py").write_text("dirty = True\n")
    else:
        (repo / "untracked.txt").write_text("not ignored\n")

    with pytest.raises(RepositoryStateError, match="clean"):
        capture_commit_range_subject(repo, base, expected_head_sha=head)


def test_capture_rejects_non_git_and_nonancestor_ranges(tmp_path: Path):
    not_repo = tmp_path / "plain"
    not_repo.mkdir()
    with pytest.raises(CodeReviewSubjectError, match="Git|git"):
        capture_commit_range_subject(not_repo, "a" * 40)

    repo = _init_repo(tmp_path / "repo")
    (repo / "root.txt").write_text("root\n")
    root = _commit(repo, "root")
    _git(repo, "checkout", "-q", "-b", "side", root)
    (repo / "side.txt").write_text("side\n")
    side = _commit(repo, "side")
    _git(repo, "checkout", "-q", "main")
    (repo / "main.txt").write_text("main\n")
    main = _commit(repo, "main")

    with pytest.raises(CodeReviewSubjectError, match="not an ancestor"):
        capture_commit_range_subject(repo, side, expected_head_sha=main)


@pytest.mark.parametrize(
    "base, expected, message",
    [
        ("abc", None, "40-byte SHA"),
        ("f" * 40, None, "Git command failed"),
        (None, "ABC", "40-byte SHA"),
    ],
)
def test_capture_rejects_invalid_or_unresolvable_full_shas(
    tmp_path: Path,
    base: str | None,
    expected: str | None,
    message: str,
):
    repo, real_base, _head = _two_commit_repo(tmp_path)
    with pytest.raises(CodeReviewSubjectError, match=message):
        capture_commit_range_subject(
            repo,
            real_base if base is None else base,
            expected_head_sha=expected,
        )


def test_capture_rejects_equal_base_and_head(tmp_path: Path):
    repo, _base, head = _two_commit_repo(tmp_path)
    with pytest.raises(CodeReviewSubjectError, match="must be different"):
        capture_commit_range_subject(repo, head, expected_head_sha=head)


@pytest.mark.parametrize(
    "marker,is_directory",
    [
        ("MERGE_HEAD", False),
        ("CHERRY_PICK_HEAD", False),
        ("BISECT_START", False),
        ("rebase-merge", True),
    ],
)
def test_capture_rejects_intermediate_git_operations(
    tmp_path: Path,
    marker: str,
    is_directory: bool,
):
    repo, base, head = _two_commit_repo(tmp_path)
    marker_raw = _git(repo, "rev-parse", "--git-path", marker).rstrip(b"\r\n")
    marker_path = Path(os.fsdecode(marker_raw))
    if not marker_path.is_absolute():
        marker_path = repo / marker_path
    if is_directory:
        marker_path.mkdir(parents=True)
    else:
        marker_path.parent.mkdir(parents=True, exist_ok=True)
        marker_path.write_text(head + "\n")

    with pytest.raises(RepositoryStateError, match="intermediate"):
        capture_commit_range_subject(repo, base, expected_head_sha=head)


def test_detached_head_is_allowed_but_explicit(tmp_path: Path):
    repo, base, head = _two_commit_repo(tmp_path)
    _git(repo, "checkout", "-q", "--detach", head)

    captured = capture_commit_range_subject(repo, base, expected_head_sha=head)

    assert captured.detached_head is True
    assert captured.head_ref is None
    assert verify_commit_range_subject(repo, captured) == captured.subject


def test_verify_rejects_head_change_tree_or_patch_mismatch_and_dirt(tmp_path: Path):
    repo, base, _head = _two_commit_repo(tmp_path)
    captured = capture_commit_range_subject(repo, base)

    with pytest.raises(SubjectChangedError, match="tree"):
        verify_commit_range_subject(
            repo,
            replace(captured, head_tree_sha="0" * 40),
        )
    with pytest.raises(SubjectChangedError, match="patch"):
        verify_commit_range_subject(
            repo,
            replace(captured, patch_sha256="0" * 64),
        )

    (repo / "app.py").write_text("dirty after capture\n")
    with pytest.raises(RepositoryStateError, match="clean"):
        verify_commit_range_subject(repo, captured)
    _git(repo, "restore", "app.py")

    (repo / "next.txt").write_text("new head\n")
    _commit(repo, "advance")
    with pytest.raises(SubjectChangedError, match="HEAD"):
        verify_commit_range_subject(repo, captured)


def test_large_and_unsafe_named_files_are_bounded_and_never_path_read(
    tmp_path: Path,
):
    repo = _init_repo(tmp_path / "--repo-option-looking")
    (repo / "seed.txt").write_text("seed\n")
    base = _commit(repo, "base")

    large = b"x" * (MAX_REVIEW_FILE_BYTES + 1)
    (repo / "large.bin").write_bytes(large)
    strange_name = "line\nbreak -- .. marker.txt"
    (repo / strange_name).write_text("safe object data\n")
    os.symlink("../../outside-secret", repo / "traversal-link")

    raw_repo = os.fsencode(repo)
    invalid_name = b"invalid-\xff.txt"
    fd = os.open(os.path.join(raw_repo, invalid_name), os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        os.write(fd, b"non-utf8-content-\xfe")
    finally:
        os.close(fd)
    head = _commit(repo, "edge paths")

    captured = capture_commit_range_subject(repo, base, expected_head_sha=head)
    objects = {item.path.raw: item for item in captured.files}
    assert objects[b"large.bin"].content is None
    assert objects[b"large.bin"].byte_length == len(large)
    assert objects[b"large.bin"].omitted_reason == (
        "per-file review material limit exceeded"
    )
    assert objects[os.fsencode(strange_name)].content == b"safe object data\n"
    assert objects[b"traversal-link"].content == b"../../outside-secret"
    assert objects[invalid_name].content == b"non-utf8-content-\xfe"
    invalid_material = objects[invalid_name].as_material()
    assert invalid_material["path"]["utf8"] is None
    assert base64.b64decode(invalid_material["path"]["base64"]) == invalid_name
    # JSON serialization must never receive a surrogate from the raw filename.
    json.dumps(captured.prompt_material(), ensure_ascii=False).encode("utf-8")


def test_git_stdout_limits_apply_before_large_output_is_retained(tmp_path: Path):
    repo = _init_repo(tmp_path / "repo")
    payload = b"bounded-reader\n" * 8192
    (repo / "large.txt").write_bytes(payload)
    _commit(repo, "large object")
    object_id = _git(repo, "rev-parse", "HEAD:large.txt").decode().strip()

    with pytest.raises(GitCommandError, match="stdout exceeds the 1024-byte limit"):
        subject_module._run_git(
            repo,
            ["cat-file", "blob", object_id],
            max_stdout_bytes=1024,
        )


@pytest.mark.parametrize(
    ("limit_name", "expected_limit"),
    [
        ("MAX_PATCH_BYTES", 16),
        ("MAX_CHANGED_PATH_DATA_BYTES", 8),
        ("MAX_TREE_DATA_BYTES", 8),
    ],
)
def test_capture_fails_closed_when_each_bulk_git_stream_exceeds_its_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit_name: str,
    expected_limit: int,
):
    repo, base, head = _two_commit_repo(tmp_path)
    monkeypatch.setattr(subject_module, limit_name, expected_limit)

    with pytest.raises(
        GitCommandError,
        match=rf"stdout exceeds the {expected_limit}-byte limit",
    ):
        capture_commit_range_subject(repo, base, expected_head_sha=head)


def test_patch_has_a_dedicated_bound_above_small_git_output_limit(tmp_path: Path):
    repo = _init_repo(tmp_path / "repo")
    (repo / "seed.txt").write_text("seed\n")
    base = _commit(repo, "base")
    (repo / "too-large.txt").write_bytes(b"x" * (MAX_PATCH_BYTES + 4096))
    head = _commit(repo, "large patch")

    with pytest.raises(
        GitCommandError,
        match=rf"stdout exceeds the {MAX_PATCH_BYTES}-byte limit",
    ):
        capture_commit_range_subject(repo, base, expected_head_sha=head)


def test_capture_checks_serialized_material_against_structured_prompt_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    repo, base, head = _two_commit_repo(tmp_path)
    monkeypatch.setattr(subject_module, "MAX_STRUCTURED_PROMPT_SECTION_BYTES", 128)

    with pytest.raises(CodeReviewSubjectError, match="review material exceeds"):
        capture_commit_range_subject(repo, base, expected_head_sha=head)


def test_git_environment_forces_no_lazy_fetch_and_no_external_attributes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    repo, base, head = _two_commit_repo(tmp_path)
    seen_environments: list[dict[str, str]] = []
    original_popen = subject_module.subprocess.Popen

    def recording_popen(*args, **kwargs):
        seen_environments.append(dict(kwargs["env"]))
        return original_popen(*args, **kwargs)

    monkeypatch.setenv("GIT_NO_LAZY_FETCH", "0")
    monkeypatch.setenv("GIT_ATTR_NOSYSTEM", "0")
    monkeypatch.setattr(subject_module.subprocess, "Popen", recording_popen)

    capture_commit_range_subject(repo, base, expected_head_sha=head)

    assert seen_environments
    assert all(env["GIT_NO_LAZY_FETCH"] == "1" for env in seen_environments)
    assert all(env["GIT_ATTR_NOSYSTEM"] == "1" for env in seen_environments)
    assert all(env["GIT_CONFIG_NOSYSTEM"] == "1" for env in seen_environments)
    assert all(env["GIT_CONFIG_GLOBAL"] == os.devnull for env in seen_environments)
    isolated = [env for env in seen_environments if "GIT_DIR" in env]
    assert isolated
    assert all(env["GIT_ATTR_SOURCE"] == head for env in isolated)


def test_external_attributes_and_repo_diff_config_cannot_change_exact_patch(
    tmp_path: Path,
):
    repo, base, head = _two_commit_repo(tmp_path)
    info_attributes_raw = _git(
        repo,
        "rev-parse",
        "--git-path",
        "info/attributes",
    ).rstrip(b"\r\n")
    info_attributes = Path(os.fsdecode(info_attributes_raw))
    if not info_attributes.is_absolute():
        info_attributes = repo / info_attributes
    info_attributes.parent.mkdir(parents=True, exist_ok=True)
    info_attributes.write_text("*.py -diff\n")
    global_attributes = tmp_path / "global-attributes"
    global_attributes.write_text("*.py -diff\n")
    _git(repo, "config", "core.attributesFile", os.fspath(global_attributes))
    _git(repo, "config", "diff.algorithm", "histogram")

    contaminated = _git(
        repo,
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--binary",
        "--full-index",
        "--find-renames",
        base,
        head,
        "--",
    )
    captured = capture_commit_range_subject(repo, base, expected_head_sha=head)

    assert b"GIT binary patch" in contaminated
    assert b"@@ -1 +1 @@" in captured.patch
    assert b"GIT binary patch" not in captured.patch


def test_info_attributes_drift_during_capture_cannot_change_patch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    repo, base, head = _two_commit_repo(tmp_path)
    info_attributes_raw = _git(
        repo,
        "rev-parse",
        "--git-path",
        "info/attributes",
    ).rstrip(b"\r\n")
    info_attributes = Path(os.fsdecode(info_attributes_raw))
    if not info_attributes.is_absolute():
        info_attributes = repo / info_attributes
    info_attributes.parent.mkdir(parents=True, exist_ok=True)
    original_patch_bytes = subject_module._patch_bytes

    def patch_while_attributes_move(*args, **kwargs):
        info_attributes.write_text("*.py -diff\n")
        patch = original_patch_bytes(*args, **kwargs)
        info_attributes.write_text("*.py diff=host-only-driver\n")
        return patch

    monkeypatch.setattr(subject_module, "_patch_bytes", patch_while_attributes_move)
    captured = capture_commit_range_subject(repo, base, expected_head_sha=head)

    assert b"@@ -1 +1 @@" in captured.patch
    assert b"GIT binary patch" not in captured.patch


def test_committed_attributes_are_bound_to_exact_head_tree(tmp_path: Path):
    repo = _init_repo(tmp_path / "repo")
    (repo / ".gitattributes").write_text("*.py -diff\n")
    (repo / "app.py").write_text("value = 1\n")
    base = _commit(repo, "base")
    (repo / "app.py").write_text("value = 2\n")
    head = _commit(repo, "head")

    captured = capture_commit_range_subject(repo, base, expected_head_sha=head)

    assert b"GIT binary patch" in captured.patch


def test_head_metadata_race_is_rejected_after_material_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    repo, base, head = _two_commit_repo(tmp_path)
    original_patch_bytes = subject_module._patch_bytes

    def patch_then_advance(*args, **kwargs):
        patch = original_patch_bytes(*args, **kwargs)
        (repo / "advanced.txt").write_text("new head during capture\n")
        _commit(repo, "racing head")
        return patch

    monkeypatch.setattr(subject_module, "_patch_bytes", patch_then_advance)

    with pytest.raises(SubjectChangedError, match="HEAD metadata changed"):
        capture_commit_range_subject(repo, base, expected_head_sha=head)


def test_verify_recomputes_subject_and_rejects_tampered_captured_material(
    tmp_path: Path,
):
    repo, base, _head = _two_commit_repo(tmp_path)
    captured = capture_commit_range_subject(repo, base)

    with pytest.raises(SubjectChangedError, match="patch bytes"):
        replace(captured, patch=captured.patch + b"tampered").prompt_material()
    with pytest.raises(SubjectChangedError, match="captured review material"):
        verify_commit_range_subject(
            repo,
            replace(captured, changed_paths=()),
        )
    with pytest.raises(SubjectChangedError, match="captured review material"):
        verify_commit_range_subject(
            repo,
            replace(captured, guidance=()),
        )


def test_verify_retains_intermediate_operation_gate(tmp_path: Path):
    repo, base, head = _two_commit_repo(tmp_path)
    captured = capture_commit_range_subject(repo, base, expected_head_sha=head)
    marker_raw = _git(repo, "rev-parse", "--git-path", "MERGE_HEAD").rstrip(
        b"\r\n"
    )
    marker = Path(os.fsdecode(marker_raw))
    if not marker.is_absolute():
        marker = repo / marker
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(head + "\n")

    with pytest.raises(RepositoryStateError, match="intermediate"):
        verify_commit_range_subject(repo, captured)
