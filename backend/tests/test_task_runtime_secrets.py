import json
import os
import stat

import pytest

from backend.config import settings
from backend.services import task_runtime_secrets
from backend.services.task_runtime_secrets import (
    TaskRuntimeSecretError,
    create_private_task_temp_dir,
    create_private_output,
    create_private_runtime_temp_dir,
    remove_private_file,
    remove_private_scope,
    write_private_json,
)


def _new_task_temp(**overrides):
    values = {
        "task_id": 41,
        "task_incarnation_id": "a" * 32,
        "retry_count": 0,
        "turn_generation": 1,
    }
    values.update(overrides)
    return create_private_task_temp_dir(**values)


def test_task_temp_is_short_private_and_generation_unique():
    first = _new_task_temp()
    second = _new_task_temp(turn_generation=2)
    try:
        assert first.path != second.path
        assert first.path == first.path.resolve(strict=True)
        assert len(os.fsencode(first.path)) <= 60
        assert stat.S_IMODE(first.path.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(first.path.stat().st_mode) == 0o700
        first.assert_valid()
        second.assert_valid()
    finally:
        first.cleanup()
        second.cleanup()


def test_generic_runtime_temp_uses_positive_owner_and_durable_generation():
    first = create_private_runtime_temp_dir(
        runtime_namespace="plan-run",
        owner_id=73,
        generation_components={
            "run_generation": 4,
            "version": "b" * 32,
        },
    )
    second = create_private_runtime_temp_dir(
        runtime_namespace="plan-run",
        owner_id=73,
        generation_components={
            "run_generation": 5,
            "version": "b" * 32,
        },
    )
    try:
        assert first.path != second.path
        assert len(os.fsencode(first.path)) <= 60
        assert stat.S_IMODE(first.path.stat().st_mode) == 0o700
        first.assert_valid()
        second.assert_valid()
    finally:
        first.cleanup()
        second.cleanup()


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "runtime_namespace": "plan-run",
            "owner_id": -1,
            "generation_components": {"generation": 1},
        },
        {
            "runtime_namespace": "Plan",
            "owner_id": 1,
            "generation_components": {"generation": 1},
        },
        {
            "runtime_namespace": "plan-run",
            "owner_id": 1,
            "generation_components": {},
        },
        {
            "runtime_namespace": "plan-run",
            "owner_id": 1,
            "generation_components": {"generation": True},
        },
        {
            "runtime_namespace": "plan-run",
            "owner_id": 1,
            "generation_components": {"generation": "../../escape"},
        },
    ],
)
def test_generic_runtime_temp_rejects_ambiguous_identity(kwargs):
    with pytest.raises(ValueError):
        create_private_runtime_temp_dir(**kwargs)


def test_task_temp_cleanup_restores_leaf_and_child_chmod_zero():
    scratch = _new_task_temp()
    child = scratch.path / "nested"
    child.mkdir()
    payload = child / "payload"
    payload.write_text("private", encoding="utf-8")
    child.chmod(0o000)
    scratch.path.chmod(0o000)

    scratch.cleanup()

    assert scratch.cleaned
    assert not scratch.path.exists()


def test_task_temp_cleanup_unlinks_symlink_without_following_target(tmp_path):
    scratch = _new_task_temp()
    target = tmp_path / "outside"
    target.mkdir()
    marker = target / "keep"
    marker.write_text("unchanged", encoding="utf-8")
    (scratch.path / "outside-link").symlink_to(
        target,
        target_is_directory=True,
    )

    scratch.cleanup()

    assert marker.read_text(encoding="utf-8") == "unchanged"
    assert not scratch.path.exists()


def test_task_temp_cleanup_refuses_replacement_symlink(tmp_path):
    scratch = _new_task_temp()
    original = scratch.path
    target = tmp_path / "outside"
    target.mkdir()
    original.rmdir()
    original.symlink_to(target, target_is_directory=True)
    try:
        with pytest.raises(TaskRuntimeSecretError, match="identity changed"):
            scratch.cleanup()
        assert target.is_dir()
    finally:
        original.unlink()


def test_task_temp_bound_runtime_owns_cleanup():
    scratch = _new_task_temp()
    scratch.bind_to_runtime()

    assert scratch.cleanup_if_unbound() is False
    assert scratch.path.is_dir()

    scratch.cleanup()
    assert not scratch.path.exists()


def test_task_temp_cleanup_depth_limit_is_bounded(monkeypatch):
    scratch = _new_task_temp()
    current = scratch.path
    for index in range(4):
        current = current / f"d{index}"
        current.mkdir()
    monkeypatch.setattr(
        task_runtime_secrets,
        "_TASK_TEMP_MAX_CLEANUP_DEPTH",
        1,
    )
    with pytest.raises(TaskRuntimeSecretError, match="depth limit"):
        scratch.cleanup()
    assert scratch.path.exists()

    monkeypatch.setattr(
        task_runtime_secrets,
        "_TASK_TEMP_MAX_CLEANUP_DEPTH",
        128,
    )
    scratch.cleanup()
    assert not scratch.path.exists()


def test_task_temp_cleanup_entry_limit_is_bounded(monkeypatch):
    scratch = _new_task_temp()
    for index in range(4):
        (scratch.path / f"f{index}").write_text("x", encoding="utf-8")
    monkeypatch.setattr(
        task_runtime_secrets,
        "_TASK_TEMP_MAX_CLEANUP_ENTRIES",
        2,
    )
    with pytest.raises(TaskRuntimeSecretError, match="entry limit"):
        scratch.cleanup()
    assert scratch.path.exists()

    monkeypatch.setattr(
        task_runtime_secrets,
        "_TASK_TEMP_MAX_CLEANUP_ENTRIES",
        10_000,
    )
    scratch.cleanup()
    assert not scratch.path.exists()


def test_private_runtime_json_has_private_directory_and_file_modes(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "runtime-secrets"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))

    target = write_private_json(
        "task",
        41,
        "mcp.json",
        {"token": "scoped"},
    )

    assert json.loads(target.read_text(encoding="utf-8")) == {
        "token": "scoped"
    }
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE(target.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(target.stat().st_mode) == 0o600

    write_private_json("task", 41, "mcp.json", {"token": "rotated"})
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "token": "rotated"
    }
    assert not list(target.parent.glob("*.tmp"))


def test_private_runtime_root_rejects_symlink_ancestor(tmp_path, monkeypatch):
    real = tmp_path / "real"
    real.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(linked / "runtime"),
    )

    with pytest.raises(TaskRuntimeSecretError, match="symlink ancestor"):
        write_private_json("task", 5, "mcp.json", {"ok": True})


def test_private_runtime_cleanup_refuses_unexpected_entries(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "runtime-secrets"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))
    target = write_private_json("task", 7, "mcp.json", {"ok": True})
    unexpected = target.parent / "nested"
    unexpected.mkdir()

    with pytest.raises(TaskRuntimeSecretError, match="Unexpected entry"):
        remove_private_scope("task", 7)

    unexpected.rmdir()
    remove_private_scope("task", 7)
    assert not target.parent.exists()


def test_private_runtime_single_file_cleanup_preserves_siblings(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "runtime-secrets"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))
    first = write_private_json("monitor", 8, "mcp-1.json", {"generation": 1})
    second = write_private_json("monitor", 8, "mcp-2.json", {"generation": 2})

    remove_private_file("monitor", 8, "mcp-1.json")

    assert not first.exists()
    assert second.exists()
    assert stat.S_IMODE(os.lstat(second).st_mode) == 0o600


def test_private_output_is_random_exclusive_private_and_inode_safe(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "runtime"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))
    output = create_private_output("monitor", 91, "agent-output")
    path = output.path

    assert path.parent == root / "monitor-91"
    assert path.name.startswith("agent-output-")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    output._stream.write(b"model output")
    output.close()
    assert not path.exists()


@pytest.mark.parametrize("collision_kind", ["symlink", "hardlink", "regular"])
def test_private_output_collision_never_opens_attacker_path(
    tmp_path,
    monkeypatch,
    collision_kind,
):
    root = tmp_path / "runtime"
    scope = root / "sub-agent-92"
    scope.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    scope.chmod(0o700)
    victim = tmp_path / "victim"
    victim.write_bytes(b"do-not-truncate")
    collision = scope / "agent-output-fixed.log"
    if collision_kind == "symlink":
        collision.symlink_to(victim)
    elif collision_kind == "hardlink":
        os.link(victim, collision)
    else:
        collision.write_bytes(b"preexisting")
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))
    monkeypatch.setattr(
        "backend.services.task_runtime_secrets.secrets.token_hex",
        lambda _size: "fixed",
    )

    with pytest.raises(FileExistsError):
        create_private_output("sub-agent", 92, "agent-output")

    assert victim.read_bytes() == b"do-not-truncate"


def test_private_output_cleanup_refuses_replaced_path(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "runtime"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(root))
    output = create_private_output("monitor", 93, "agent-output")
    path = output.path
    victim = tmp_path / "victim"
    victim.write_bytes(b"unchanged")
    path.unlink()
    path.symlink_to(victim)

    with pytest.raises(TaskRuntimeSecretError, match="changed before cleanup"):
        output.close()

    assert victim.read_bytes() == b"unchanged"
