from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

import backend.database as database_module
import backend.main as main_module
import backend.services.shared_proxy as shared_proxy_module
from backend.api.shared import (
    ReceiveSharePayload,
    _start_relay_and_backfill,
    receive_share,
)
from backend.api.shared_access import shared_config
from backend.models.task import Task
from backend.models.task_share import SharedTaskReceived, TaskShare


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner_attention_tag", "stale_shadow_tag"),
    [
        ("需要人工确认", None),
        (None, "旧标签"),
    ],
)
async def test_owner_config_backfill_mirrors_attention_tag_to_new_share_shadow(
    session_factory,
    monkeypatch,
    owner_attention_tag,
    stale_shadow_tag,
):
    async with session_factory() as db:
        owner_task = Task(
            title="Owner task",
            description="shared task",
            status="completed",
            attention_tag=owner_attention_tag,
        )
        db.add(owner_task)
        await db.flush()
        db.add(TaskShare(
            task_id=owner_task.id,
            shared_to_open_id="receiver-open-id",
            shared_to_name="Receiver",
            shared_to_ccm_url="https://receiver.example",
            share_token="attention-token",
            status="active",
        ))
        await db.commit()
        owner_task_id = owner_task.id

    monkeypatch.setattr(main_module, "shared_relay", None)
    async with session_factory() as db:
        response = await receive_share(ReceiveSharePayload(
            owner_ccm_url="https://owner.example",
            owner_name="Owner",
            owner_feishu_open_id="owner-open-id",
            remote_task_id=owner_task_id,
            share_token="attention-token",
            task_title="Owner task",
            task_description="shared task",
            project_name=None,
        ), db)
        assert response == {"ok": True}
        received = (
            await db.execute(
                select(SharedTaskReceived).where(
                    SharedTaskReceived.owner_ccm_url
                    == "https://owner.example",
                    SharedTaskReceived.remote_task_id == owner_task_id,
                )
            )
        ).scalar_one()
        shadow = await db.get(Task, received.local_task_id)
        assert shadow.shared_from_id == received.id
        assert shadow.execution_user_id is None
        assert shadow.execution_user_role == "member"
        assert shadow.execution_mode == "sandbox"
        assert shadow.execution_principal_kind == "system"
        shadow.attention_tag = stale_shadow_tag
        await db.commit()
        shadow_task_id = shadow.id

    async with session_factory() as db:
        owner_config = await shared_config(
            owner_task_id,
            "attention-token",
            db,
        )
    assert owner_config["attention_tag"] == owner_attention_tag

    proxy_config = AsyncMock(return_value=owner_config)
    monkeypatch.setattr(shared_proxy_module, "proxy_config", proxy_config)
    monkeypatch.setattr(database_module, "async_session", session_factory)
    relay = SimpleNamespace(
        backfill_history=AsyncMock(),
        start_relay=AsyncMock(),
    )

    await _start_relay_and_backfill(relay, received)

    proxy_config.assert_awaited_once_with(
        "https://owner.example",
        owner_task_id,
        "attention-token",
    )
    relay.backfill_history.assert_awaited_once_with(received)
    relay.start_relay.assert_awaited_once_with(received)
    async with session_factory() as db:
        mirrored_shadow = await db.get(Task, shadow_task_id)
        assert mirrored_shadow.attention_tag == owner_attention_tag
