import json
import os
import stat
from pathlib import Path

import pytest

from backend.services import codex_pool as codex_pool_module
from backend.services import codex_session_migration as migration_module
from backend.services.codex_session_migration import (
    AmbiguousCodexSessionError,
    CodexRolloutMigrationMetadataError,
    CodexSessionConflictError,
    CodexSessionMigrationError,
    CodexSessionNotFoundError,
    InvalidCodexSessionIdError,
    find_codex_rollout_session,
    migrate_codex_rollout_session,
    read_rollout_migration_marker,
    rollout_migration_sidecar_path,
)


def _write_rollout(
    codex_home: Path,
    session_id: str,
    *,
    date: tuple[str, str, str] = ("2026", "07", "21"),
    content: str = '{"type":"session_meta"}\n',
) -> Path:
    directory = codex_home / "sessions" / date[0] / date[1] / date[2]
    directory.mkdir(parents=True)
    path = directory / f"rollout-2026-07-21T10-20-30-{session_id}.jsonl"
    path.write_text(content, encoding="utf-8")
    return path


def _stage_rollout_reservation(
    source: Path,
    target: Path,
    *,
    exclusive: bool,
) -> tuple[Path, Path, Path]:
    """Leave the marker/rollout pair at the pre-publication crash boundary."""

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary, copied_bytes = migration_module._copy_source_to_temporary_file(
        source,
        target,
    )
    marker_temporary = migration_module._prepare_rollout_migration_marker(
        target,
        temporary,
        copied_bytes,
    )
    if exclusive:
        migration_module._install_rollout_migration_marker_exclusive(
            target,
            marker_temporary,
        )
    else:
        migration_module._install_rollout_migration_marker(target, marker_temporary)
    return target, temporary, marker_temporary


def _rewrite_reservation_owner(
    target: Path,
    marker_temporary: Path,
    *,
    pid: int,
    start_ticks: int,
    **updates: object,
) -> None:
    sidecar = rollout_migration_sidecar_path(target)
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    payload["reservation_owner"] = {"pid": pid, "start_ticks": start_ticks}
    payload.update(updates)
    marker_temporary.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    marker_temporary.chmod(0o600)


def _quota_event(used_percent: int, timestamp: str) -> str:
    return json.dumps({
        "timestamp": timestamp,
        "payload": {
            "type": "token_count",
            "rate_limits": {
                "primary": {
                    "used_percent": used_percent,
                    "window_minutes": 300,
                    "resets_at": 1_800_000_000,
                },
                "secondary": None,
                "plan_type": "pro",
                "rate_limit_reached_type": None,
                "credits": {"has_credits": False},
            },
        },
    }) + "\n"


def _usage_limit_event(timestamp: str) -> str:
    return json.dumps({
        "timestamp": timestamp,
        "type": "event_msg",
        "payload": {
            "type": "task_complete",
            "error": {
                "message": "You've hit your usage limit. Try again later.",
                "codex_error_info": "usage_limit_exceeded",
            },
        },
    }) + "\n"


def test_migrate_copies_only_rollout_at_same_relative_path(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000001"
    source_home = tmp_path / "source-account"
    target_home = tmp_path / "target-account"
    source = _write_rollout(source_home, session_id)
    source.chmod(0o640)
    (source_home / "auth.json").write_text('{"secret":"must-not-copy"}', encoding="utf-8")

    target = migrate_codex_rollout_session(session_id, source_home, target_home)

    expected = target_home / source.relative_to(source_home)
    assert target == expected.resolve()
    assert target.read_bytes() == source.read_bytes()
    assert source.exists()
    assert not source.samefile(target)
    assert not (target_home / "auth.json").exists()
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    sidecar = rollout_migration_sidecar_path(target)
    assert sidecar.is_file()
    assert stat.S_IMODE(sidecar.stat().st_mode) == 0o600
    assert read_rollout_migration_marker(target) == target.stat().st_size
    for directory in [
        target_home,
        target_home / "sessions",
        target_home / "sessions" / "2026",
        target_home / "sessions" / "2026" / "07",
        target.parent,
    ]:
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700


def test_existing_identical_target_is_idempotent_and_not_replaced(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000002"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="same\n")
    target = _write_rollout(target_home, session_id, content="same\n")
    target_inode = target.stat().st_ino
    target.chmod(0o640)

    result = migrate_codex_rollout_session(session_id, source_home, target_home)

    assert result == target.resolve()
    assert result.stat().st_ino == target_inode
    assert stat.S_IMODE(result.stat().st_mode) == 0o640
    assert not source.samefile(result)
    assert read_rollout_migration_marker(result) == result.stat().st_size


def test_round_trip_updates_old_target_atomically_with_backup(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000003"
    home_a = tmp_path / "account-a"
    home_b = tmp_path / "account-b"
    rollout_a = _write_rollout(home_a, session_id, content="turn-1\n")

    rollout_b = migrate_codex_rollout_session(session_id, home_a, home_b)
    with rollout_b.open("a", encoding="utf-8") as stream:
        stream.write("turn-2\n")
    old_a_inode = rollout_a.stat().st_ino

    result = migrate_codex_rollout_session(session_id, home_b, home_a)

    backup = rollout_a.with_name(rollout_a.name + ".pre-migration.bak")
    assert result == rollout_a.resolve()
    assert result.read_text(encoding="utf-8") == "turn-1\nturn-2\n"
    assert result.stat().st_ino != old_a_inode
    assert backup.read_text(encoding="utf-8") == "turn-1\n"
    assert stat.S_IMODE(backup.stat().st_mode) == 0o600
    assert rollout_b.exists()
    assert not rollout_b.samefile(result)
    assert not backup.samefile(result)


def test_newer_target_is_preserved_without_backup(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000006"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    _write_rollout(source_home, session_id, content="turn-1\n")
    target = _write_rollout(target_home, session_id, content="turn-1\nturn-2\n")
    target_inode = target.stat().st_ino

    result = migrate_codex_rollout_session(session_id, source_home, target_home)

    assert result.read_text(encoding="utf-8") == "turn-1\nturn-2\n"
    assert result.stat().st_ino == target_inode
    assert list(target.parent.glob(target.name + ".pre-migration*.bak")) == []
    assert read_rollout_migration_marker(result) == len("turn-1\n".encode())


def test_diverged_target_is_preserved_and_reported(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000007"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    _write_rollout(source_home, session_id, content="common\nsource\n")
    target = _write_rollout(target_home, session_id, content="common\ntarget\n")
    before = target.read_bytes()

    with pytest.raises(CodexSessionConflictError, match="diverged content"):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert target.read_bytes() == before


def test_missing_session_raises_explicit_error(tmp_path: Path):
    with pytest.raises(CodexSessionNotFoundError, match="not found"):
        migrate_codex_rollout_session("missing-session", tmp_path / "source", tmp_path / "target")


def test_multiple_source_rollouts_raise_ambiguous_error(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000004"
    source_home = tmp_path / "source"
    _write_rollout(source_home, session_id, date=("2026", "07", "20"))
    _write_rollout(source_home, session_id, date=("2026", "07", "21"))

    with pytest.raises(AmbiguousCodexSessionError, match="multiple rollouts"):
        find_codex_rollout_session(session_id, source_home)


@pytest.mark.parametrize("session_id", ["../escape", "bad*glob", "", "space id"])
def test_invalid_session_id_is_rejected(session_id: str, tmp_path: Path):
    with pytest.raises(InvalidCodexSessionIdError):
        migrate_codex_rollout_session(session_id, tmp_path / "source", tmp_path / "target")


def test_existing_hardlink_is_not_accepted_as_a_copy(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000005"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id)
    target = target_home / source.relative_to(source_home)
    target.parent.mkdir(parents=True)
    os.link(source, target)

    with pytest.raises(CodexSessionConflictError, match="aliases the source"):
        migrate_codex_rollout_session(session_id, source_home, target_home)


def test_migrated_terminal_does_not_pollute_destination_quota(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000008"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    _write_rollout(
        source_home,
        session_id,
        content=_usage_limit_event("2026-08-18T19:40:04Z"),
    )

    target = migrate_codex_rollout_session(session_id, source_home, target_home)

    assert read_rollout_migration_marker(target) == target.stat().st_size
    assert codex_pool_module._read_quota_from_rollout(str(target_home)) is None


def test_destination_events_appended_after_migration_remain_quota_evidence(
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000009"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(
        source_home,
        session_id,
        content=_usage_limit_event("2026-08-18T19:40:04Z"),
    )
    target = migrate_codex_rollout_session(session_id, source_home, target_home)
    copied_size = source.stat().st_size
    with target.open("a", encoding="utf-8") as stream:
        stream.write(_quota_event(37, "2026-08-18T19:41:04Z"))

    quota = codex_pool_module._read_quota_from_rollout(str(target_home))

    assert read_rollout_migration_marker(target) == copied_size
    assert quota is not None
    assert quota["primary_used_percent"] == 37


def test_target_extending_source_marks_only_source_prefix_as_foreign(
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000010"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source_content = _usage_limit_event("2026-08-18T19:40:04Z")
    _write_rollout(source_home, session_id, content=source_content)
    target = _write_rollout(
        target_home,
        session_id,
        content=source_content + _quota_event(42, "2026-08-18T19:42:04Z"),
    )
    target_inode = target.stat().st_ino

    result = migrate_codex_rollout_session(session_id, source_home, target_home)
    quota = codex_pool_module._read_quota_from_rollout(str(target_home))

    assert result.stat().st_ino == target_inode
    assert read_rollout_migration_marker(result) == len(source_content.encode())
    assert quota is not None
    assert quota["primary_used_percent"] == 42


def test_target_extending_source_rejects_damaged_existing_marker(
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000013"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    _write_rollout(source_home, session_id, content="foreign\n")
    target = migrate_codex_rollout_session(session_id, source_home, target_home)
    with target.open("a", encoding="utf-8") as stream:
        stream.write("native\n")
    before = target.read_bytes()
    sidecar = rollout_migration_sidecar_path(target)
    sidecar.write_text("{damaged\n", encoding="utf-8")
    sidecar.chmod(0o600)

    with pytest.raises(CodexRolloutMigrationMetadataError):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert target.read_bytes() == before
    assert sidecar.read_text(encoding="utf-8") == "{damaged\n"


def test_round_trip_preserves_destination_marker_and_native_suffix(
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000020"
    source_home = tmp_path / "source"
    destination_home = tmp_path / "destination"
    round_trip_home = tmp_path / "round-trip"
    source = _write_rollout(
        source_home,
        session_id,
        content=_usage_limit_event("2026-08-18T19:40:04Z"),
    )
    destination = migrate_codex_rollout_session(
        session_id,
        source_home,
        destination_home,
    )
    foreign_prefix = read_rollout_migration_marker(destination)
    with destination.open("a", encoding="utf-8") as stream:
        stream.write(_quota_event(72, "2026-08-18T19:49:04Z"))

    round_trip = migrate_codex_rollout_session(
        session_id,
        destination_home,
        round_trip_home,
    )
    result = migrate_codex_rollout_session(
        session_id,
        round_trip_home,
        destination_home,
    )
    quota = codex_pool_module._read_quota_from_rollout(str(destination_home))

    assert foreign_prefix is not None
    assert read_rollout_migration_marker(result) == foreign_prefix
    assert quota is not None
    assert quota["primary_used_percent"] == 72
    assert round_trip.exists()


def test_new_copy_marker_uses_copied_bytes_before_destination_append(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000014"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(
        source_home,
        session_id,
        content=_usage_limit_event("2026-08-18T19:40:04Z"),
    )
    source_size = source.stat().st_size
    target = target_home / source.relative_to(source_home)
    real_link = os.link

    def append_after_rollout_link(source_path, target_path, **kwargs):
        result = real_link(source_path, target_path, **kwargs)
        if Path(target_path) == target:
            with target.open("a", encoding="utf-8") as stream:
                stream.write(_quota_event(61, "2026-08-18T19:44:04Z"))
        return result

    monkeypatch.setattr(migration_module.os, "link", append_after_rollout_link)

    target = migrate_codex_rollout_session(session_id, source_home, target_home)
    quota = codex_pool_module._read_quota_from_rollout(str(target_home))

    assert read_rollout_migration_marker(target) == source_size
    assert quota is not None
    assert quota["primary_used_percent"] == 61


def test_new_copy_marker_is_visible_before_rollout_to_quota_scanner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000019"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(
        source_home,
        session_id,
        content=_usage_limit_event("2026-08-18T19:40:04Z"),
    )
    target = target_home / source.relative_to(source_home)
    real_link = os.link
    observations: list[dict | None] = []

    def observe_after_rollout_link(source_path, target_path, **kwargs):
        result = real_link(source_path, target_path, **kwargs)
        if Path(target_path) == target:
            observations.append(
                codex_pool_module._read_quota_from_rollout(str(target_home))
            )
        return result

    monkeypatch.setattr(migration_module.os, "link", observe_after_rollout_link)

    migrate_codex_rollout_session(session_id, source_home, target_home)

    assert observations == [None]


def test_replacement_marker_uses_copied_bytes_before_destination_append(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000015"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source_content = "{}\n" + _usage_limit_event("2026-08-18T19:40:04Z")
    source = _write_rollout(source_home, session_id, content=source_content)
    target = _write_rollout(target_home, session_id, content="{}\n")
    source_size = source.stat().st_size
    real_replace = os.replace

    def append_after_rollout_replace(source_path, target_path):
        real_replace(source_path, target_path)
        if Path(target_path) == target:
            with target.open("a", encoding="utf-8") as stream:
                stream.write(_quota_event(62, "2026-08-18T19:45:04Z"))

    monkeypatch.setattr(migration_module.os, "replace", append_after_rollout_replace)

    result = migrate_codex_rollout_session(session_id, source_home, target_home)
    quota = codex_pool_module._read_quota_from_rollout(str(target_home))

    assert read_rollout_migration_marker(result) == source_size
    assert quota is not None
    assert quota["primary_used_percent"] == 62


def test_replacement_sidecar_failure_preserves_old_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000016"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    _write_rollout(source_home, session_id, content="old\nnew\n")
    target = _write_rollout(target_home, session_id, content="old\n")
    old_inode = target.stat().st_ino
    real_replace = os.replace

    def fail_marker_install(source_path, target_path):
        if Path(target_path) == rollout_migration_sidecar_path(target):
            raise OSError("simulated marker install failure")
        return real_replace(source_path, target_path)

    monkeypatch.setattr(migration_module.os, "replace", fail_marker_install)

    with pytest.raises(CodexSessionMigrationError, match="migration sidecar"):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert target.stat().st_ino == old_inode
    assert target.read_text(encoding="utf-8") == "old\n"
    assert not rollout_migration_sidecar_path(target).exists()


def test_rollout_replace_failure_leaves_old_target_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000017"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    target_content = _quota_event(63, "2026-08-18T19:46:04Z")
    _write_rollout(source_home, session_id, content=target_content + "{}\n")
    target = _write_rollout(target_home, session_id, content=target_content)
    old_inode = target.stat().st_ino
    real_replace = os.replace

    def fail_rollout_replace(source_path, target_path):
        if Path(target_path) == target:
            raise OSError("simulated rollout replace failure")
        return real_replace(source_path, target_path)

    monkeypatch.setattr(migration_module.os, "replace", fail_rollout_replace)

    with pytest.raises(CodexSessionMigrationError):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert target.stat().st_ino == old_inode
    assert rollout_migration_sidecar_path(target).is_file()
    assert codex_pool_module._read_quota_from_rollout(str(target_home)) is None


def test_quota_scan_uses_open_rollout_when_path_is_replaced(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000018"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="{}\n")
    target = migrate_codex_rollout_session(session_id, source_home, target_home)
    with target.open("a", encoding="utf-8") as stream:
        stream.write(_quota_event(71, "2026-08-18T19:47:04Z"))

    replacement = target.with_name(target.name + ".replacement")
    replacement.write_bytes(
        source.read_bytes() + _quota_event(99, "2026-08-18T19:48:04Z").encode()
    )
    replacement.chmod(0o600)
    real_iterator = codex_pool_module._iter_rollout_lines_reverse_stream
    replaced = False

    def replace_path_before_read(stream, **kwargs):
        nonlocal replaced
        if not replaced:
            os.replace(replacement, target)
            replaced = True
        yield from real_iterator(stream, **kwargs)

    monkeypatch.setattr(
        codex_pool_module,
        "_iter_rollout_lines_reverse_stream",
        replace_path_before_read,
    )

    quota = codex_pool_module._read_quota_from_rollout(str(target_home))

    assert replaced is True
    assert quota is not None
    assert quota["primary_used_percent"] == 71


@pytest.mark.parametrize(
    "damage",
    ["invalid_json", "public_permissions", "inode_mismatch"],
)
def test_invalid_migration_sidecar_makes_rollout_quota_ineligible(
    damage: str,
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000011"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    _write_rollout(source_home, session_id, content="{}\n")
    target = migrate_codex_rollout_session(session_id, source_home, target_home)
    with target.open("a", encoding="utf-8") as stream:
        stream.write(_quota_event(51, "2026-08-18T19:43:04Z"))

    if damage == "invalid_json":
        rollout_migration_sidecar_path(target).write_text("{not-json\n", encoding="utf-8")
        rollout_migration_sidecar_path(target).chmod(0o600)
    elif damage == "public_permissions":
        rollout_migration_sidecar_path(target).chmod(0o644)
    else:
        replacement = target.with_name(target.name + ".replacement")
        replacement.write_bytes(target.read_bytes())
        replacement.chmod(0o600)
        os.replace(replacement, target)

    assert codex_pool_module._read_quota_from_rollout(str(target_home)) is None


def test_reverse_iterator_after_offset_keeps_only_complete_suffix_lines(
    tmp_path: Path,
):
    rollout = tmp_path / "rollout.jsonl"
    rollout.write_bytes(b"foreign\nnative-one\nnative-two\n")

    at_boundary = list(codex_pool_module._iter_rollout_lines_reverse_after(
        rollout,
        len(b"foreign\n"),
        chunk_size=3,
    ))
    inside_line = list(codex_pool_module._iter_rollout_lines_reverse_after(
        rollout,
        len(b"foreign\nnat"),
        chunk_size=3,
    ))

    assert [line for line in at_boundary if line] == [b"native-two", b"native-one"]
    assert [line for line in inside_line if line] == [b"native-two"]


def test_sidecar_write_failure_is_atomic_and_removes_unmarked_copy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000012"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id)
    target = target_home / source.relative_to(source_home)

    real_link = os.link
    sidecar = rollout_migration_sidecar_path(target)

    def fail_sidecar_link(source_path, target_path, **kwargs):
        if Path(target_path) == sidecar:
            raise OSError("simulated sidecar install failure")
        return real_link(source_path, target_path, **kwargs)

    monkeypatch.setattr(migration_module.os, "link", fail_sidecar_link)

    with pytest.raises(CodexSessionMigrationError, match="migration sidecar"):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert not target.exists()
    assert not rollout_migration_sidecar_path(target).exists()
    assert list(target.parent.glob(f".{target.name}.ccm-migration.json.*.tmp")) == []


def test_dead_marker_only_reservation_is_recovered_and_retry_succeeds(
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000020"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="foreign\n")
    target = target_home / source.relative_to(source_home)
    _, rollout_temporary, marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=True,
    )
    _rewrite_reservation_owner(
        target,
        marker_temporary,
        pid=999_999_999,
        start_ticks=1,
    )

    result = migrate_codex_rollout_session(session_id, source_home, target_home)

    assert result == target
    assert result.read_text(encoding="utf-8") == "foreign\n"
    assert read_rollout_migration_marker(result) == result.stat().st_size
    assert not rollout_temporary.exists()
    assert not marker_temporary.exists()


def test_live_marker_only_reservation_remains_fail_closed(
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000021"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="foreign\n")
    target = target_home / source.relative_to(source_home)
    _, rollout_temporary, marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=True,
    )
    start_ticks = migration_module._linux_process_start_ticks(os.getpid())
    assert start_ticks is not None
    _rewrite_reservation_owner(
        target,
        marker_temporary,
        pid=os.getpid(),
        start_ticks=start_ticks,
    )

    with pytest.raises(CodexRolloutMigrationMetadataError, match="still alive"):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert not target.exists()
    assert rollout_migration_sidecar_path(target).is_file()
    assert rollout_temporary.is_file()
    assert marker_temporary.is_file()


def test_existing_rollout_with_stale_sidecar_is_not_auto_recovered(
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000022"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="old\nnew\n")
    target = _write_rollout(target_home, session_id, content="old\n")
    _, rollout_temporary, _marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=False,
    )
    marker_temporary = rollout_migration_sidecar_path(target)
    _rewrite_reservation_owner(
        target,
        marker_temporary,
        pid=999_999_999,
        start_ticks=1,
    )

    with pytest.raises(CodexRolloutMigrationMetadataError, match="no longer matches"):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert target.read_text(encoding="utf-8") == "old\n"
    assert rollout_migration_sidecar_path(target).is_file()
    assert rollout_temporary.is_file()
    assert marker_temporary.is_file()


def test_crashed_new_target_reservation_is_recovered_before_retry(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000021"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="foreign\n")
    target = target_home / source.relative_to(source_home)
    _, rollout_temporary, marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=True,
    )
    _rewrite_reservation_owner(
        target,
        marker_temporary,
        pid=999_999_999,
        start_ticks=1,
    )

    result = migrate_codex_rollout_session(session_id, source_home, target_home)

    assert result.read_text(encoding="utf-8") == "foreign\n"
    assert not rollout_temporary.exists()
    assert not marker_temporary.exists()
    assert read_rollout_migration_marker(result) == result.stat().st_size


def test_crashed_replacement_reservation_remains_fail_closed(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000022"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="old\nnew\n")
    target = _write_rollout(target_home, session_id, content="old\n")
    _, rollout_temporary, marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=False,
    )
    _rewrite_reservation_owner(
        target,
        marker_temporary,
        pid=999_999_999,
        start_ticks=1,
    )

    with pytest.raises(CodexRolloutMigrationMetadataError, match="no longer matches"):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert target.read_text(encoding="utf-8") == "old\n"
    assert rollout_temporary.exists()
    assert marker_temporary.exists()
    assert not target.with_name(target.name + ".pre-migration.bak").exists()


def test_live_reservation_owner_prevents_orphan_cleanup(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000023"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="foreign\n")
    target = target_home / source.relative_to(source_home)
    _, rollout_temporary, marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=True,
    )

    with pytest.raises(CodexRolloutMigrationMetadataError, match="still alive"):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert not target.exists()
    assert rollout_temporary.exists()
    assert marker_temporary.exists()
    assert rollout_migration_sidecar_path(target).exists()


def test_unavailable_owner_identity_prevents_orphan_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000027"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="foreign\n")
    target = target_home / source.relative_to(source_home)
    _, rollout_temporary, marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=True,
    )
    monkeypatch.setattr(
        migration_module,
        "_linux_process_start_ticks",
        lambda pid: None,
    )

    with pytest.raises(
        CodexRolloutMigrationMetadataError,
        match="identity is unavailable",
    ):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert not target.exists()
    assert rollout_temporary.exists()
    assert marker_temporary.exists()


def test_unknown_staging_state_prevents_orphan_cleanup(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000028"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="foreign\n")
    target = target_home / source.relative_to(source_home)
    _, rollout_temporary, marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=True,
    )
    _rewrite_reservation_owner(
        target,
        marker_temporary,
        pid=999_999_999,
        start_ticks=1,
        staging_state="committed",
    )

    with pytest.raises(
        CodexRolloutMigrationMetadataError,
        match="not a recoverable staging marker",
    ):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert not target.exists()
    assert rollout_temporary.exists()
    assert marker_temporary.exists()


def test_pid_reuse_start_tick_mismatch_is_recovered_as_orphan(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000024"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="foreign\n")
    target = target_home / source.relative_to(source_home)
    _, rollout_temporary, marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=True,
    )
    current_ticks = migration_module._linux_process_start_ticks(os.getpid())
    if current_ticks is None:
        pytest.skip("Linux process start ticks are unavailable")
    _rewrite_reservation_owner(
        target,
        marker_temporary,
        pid=os.getpid(),
        start_ticks=current_ticks + 1,
    )

    result = migrate_codex_rollout_session(session_id, source_home, target_home)

    assert result.exists()
    assert not rollout_temporary.exists()
    assert not marker_temporary.exists()


def test_malformed_orphan_sidecar_remains_fail_closed(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000025"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="foreign\n")
    target = target_home / source.relative_to(source_home)
    _, rollout_temporary, marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=True,
    )
    marker_temporary.write_text("{not-json\n", encoding="utf-8")
    marker_temporary.chmod(0o600)

    with pytest.raises(CodexRolloutMigrationMetadataError, match="valid JSON"):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert rollout_temporary.exists()
    assert marker_temporary.exists()
    assert rollout_migration_sidecar_path(target).exists()


def test_recovery_never_removes_unrelated_staging_file(tmp_path: Path):
    session_id = "019f0000-aaaa-bbbb-cccc-000000000026"
    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source = _write_rollout(source_home, session_id, content="foreign\n")
    target = target_home / source.relative_to(source_home)
    _, rollout_temporary, marker_temporary = _stage_rollout_reservation(
        source,
        target,
        exclusive=True,
    )
    unrelated = target.parent / f".{target.name}.unrelated.tmp"
    unrelated.write_text("do-not-delete\n", encoding="utf-8")
    unrelated.chmod(0o600)
    _rewrite_reservation_owner(
        target,
        marker_temporary,
        pid=999_999_999,
        start_ticks=1,
        staging_rollout_name=unrelated.name,
    )

    with pytest.raises(CodexRolloutMigrationMetadataError, match="identity"):
        migrate_codex_rollout_session(session_id, source_home, target_home)

    assert unrelated.read_text(encoding="utf-8") == "do-not-delete\n"
    assert rollout_temporary.exists()
    assert marker_temporary.exists()
