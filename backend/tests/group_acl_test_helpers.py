"""Focused helpers for final group-membership effect-fence regressions."""

from sqlalchemy import delete

from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.user_group import UserGroup, UserGroupMember


async def grant_group_project_access(
    session_factory,
    *,
    project_id: int,
    user_id: int,
    shared_by: int = 999,
) -> int:
    async with session_factory() as db:
        group = UserGroup(
            name=f"effect-project-{project_id}-user-{user_id}",
            created_by=shared_by,
        )
        db.add(group)
        await db.flush()
        db.add_all(
            [
                UserGroupMember(group_id=group.id, user_id=user_id),
                TeamProjectShare(
                    project_id=project_id,
                    target_type="group",
                    target_id=group.id,
                    shared_by=shared_by,
                ),
            ]
        )
        await db.commit()
        return group.id


async def grant_group_task_chat_access(
    session_factory,
    *,
    task_id: int,
    user_id: int,
    shared_by: int = 999,
) -> int:
    async with session_factory() as db:
        group = UserGroup(
            name=f"effect-task-{task_id}-user-{user_id}",
            created_by=shared_by,
        )
        db.add(group)
        await db.flush()
        db.add_all(
            [
                UserGroupMember(group_id=group.id, user_id=user_id),
                TeamTaskShare(
                    task_id=task_id,
                    target_type="group",
                    target_id=group.id,
                    permission="chat",
                    shared_by=shared_by,
                ),
            ]
        )
        await db.commit()
        return group.id


def revoke_group_membership_at_effect_fence(
    monkeypatch,
    *,
    on_call: int = 1,
) -> dict[str, int | bool]:
    """Make revocation win immediately before one final membership lock.

    The delete runs in the effect transaction so SQLite can deterministically
    model the state that a separately committed revocation exposes to the
    post-wait ACL query without deadlocking on its database-wide writer lock.
    """

    import backend.api.deps as deps

    original = deps._lock_user_group_membership_authority
    state: dict[str, int | bool] = {"calls": 0, "revoked": False}

    async def revoke_then_lock(user_id: int, db) -> None:
        state["calls"] = int(state["calls"]) + 1
        if state["calls"] == on_call:
            await db.execute(
                delete(UserGroupMember).where(
                    UserGroupMember.user_id == user_id
                )
            )
            state["revoked"] = True
        await original(user_id, db)

    monkeypatch.setattr(
        deps,
        "_lock_user_group_membership_authority",
        revoke_then_lock,
    )
    return state
