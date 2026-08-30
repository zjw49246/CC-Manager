import asyncio

import pytest
from sqlalchemy import select

from backend.models.global_settings import GlobalSettings


@pytest.mark.asyncio
async def test_capacity_defaults_to_environment_and_persists_override(
    client,
    session_factory,
):
    from backend.config import settings
    from backend.main import dispatcher

    original_override = dispatcher._max_concurrent_instances_override
    try:
        dispatcher.configure_capacity_override(None)
        response = await client.get("/api/settings/capacity")
        assert response.status_code == 200
        assert response.json()["max_concurrent_instances"] == settings.max_concurrent_instances
        assert response.json()["configured_override"] is None

        response = await client.put(
            "/api/settings/capacity",
            json={"max_concurrent_instances": 3},
        )
        assert response.status_code == 200
        assert response.json()["max_concurrent_instances"] == 3
        assert dispatcher.max_concurrent_instances == 3

        async with session_factory() as db:
            row = await db.scalar(select(GlobalSettings).where(GlobalSettings.id == 1))
            assert row is not None
            assert row.max_concurrent_instances == 3

        response = await client.put(
            "/api/settings/capacity",
            json={"max_concurrent_instances": None},
        )
        assert response.status_code == 200
        assert response.json()["configured_override"] is None
        assert dispatcher.max_concurrent_instances == settings.max_concurrent_instances
    finally:
        dispatcher._max_concurrent_instances_override = original_override


@pytest.mark.asyncio
async def test_capacity_rejects_unsafe_values(client):
    for value in (0, -1, 65, 1.5):
        response = await client.put(
            "/api/settings/capacity",
            json={"max_concurrent_instances": value},
        )
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_capacity_updates_serialize_db_and_dispatcher(
    client,
    session_factory,
    monkeypatch,
):
    from backend.main import dispatcher

    original_override = dispatcher._max_concurrent_instances_override
    original_apply = dispatcher.apply_capacity_override
    first_apply_entered = asyncio.Event()
    release_first_apply = asyncio.Event()
    second_apply_entered = asyncio.Event()
    applied: list[int | None] = []
    requests: list[asyncio.Task] = []

    async def controlled_apply(override: int | None) -> None:
        applied.append(override)
        if override == 3:
            first_apply_entered.set()
            await release_first_apply.wait()
        elif override == 5:
            second_apply_entered.set()
        await original_apply(override)

    monkeypatch.setattr(dispatcher, "apply_capacity_override", controlled_apply)
    try:
        first_request = asyncio.create_task(
            client.put(
                "/api/settings/capacity",
                json={"max_concurrent_instances": 3},
            )
        )
        requests.append(first_request)
        await asyncio.wait_for(first_apply_entered.wait(), timeout=2)

        second_request = asyncio.create_task(
            client.put(
                "/api/settings/capacity",
                json={"max_concurrent_instances": 5},
            )
        )
        requests.append(second_request)

        # The second request cannot commit while the first is paused between
        # its durable commit and runtime application.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(second_apply_entered.wait(), timeout=0.1)
        async with session_factory() as db:
            row = await db.get(GlobalSettings, 1)
            assert row is not None
            assert row.max_concurrent_instances == 3
        assert applied == [3]

        release_first_apply.set()
        first_response, second_response = await asyncio.gather(
            first_request,
            second_request,
        )

        assert first_response.status_code == 200
        assert first_response.json()["configured_override"] == 3
        assert first_response.json()["max_concurrent_instances"] == 3
        assert second_response.status_code == 200
        assert second_response.json()["configured_override"] == 5
        assert second_response.json()["max_concurrent_instances"] == 5
        assert applied == [3, 5]
        assert second_apply_entered.is_set()
        assert dispatcher.max_concurrent_instances == 5
        async with session_factory() as db:
            row = await db.get(GlobalSettings, 1)
            assert row is not None
            assert row.max_concurrent_instances == 5
    finally:
        release_first_apply.set()
        if requests:
            await asyncio.gather(*requests, return_exceptions=True)
        dispatcher._max_concurrent_instances_override = original_override
