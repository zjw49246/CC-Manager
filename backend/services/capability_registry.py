"""In-process registry for capability executor adapters.

The core intentionally ships without a registered production executor. Plan
and code-review adapters register in later integration changes, keeping this
first migration deployable with all effects disabled.
"""

from dataclasses import dataclass, field
from copy import deepcopy
import re
from typing import Protocol


CAPABILITY_KEY_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")


class CapabilityRegistryError(ValueError):
    pass


class CapabilityExecutor(Protocol):
    async def ensure_started(self, *args, **kwargs): ...

    async def observe(self, *args, **kwargs): ...

    async def recover(self, *args, **kwargs): ...

    async def cancel(self, *args, **kwargs): ...


@dataclass(frozen=True, slots=True)
class CapabilityDefinition:
    capability_key: str
    executor_kind: str
    executor_config: dict = field(default_factory=dict)
    policy_snapshot: dict = field(default_factory=dict)
    max_attempts: int = 1
    executor: CapabilityExecutor | None = None

    def __post_init__(self) -> None:
        if not CAPABILITY_KEY_RE.fullmatch(self.capability_key):
            raise CapabilityRegistryError("Invalid capability key")
        if not self.executor_kind or len(self.executor_kind) > 64:
            raise CapabilityRegistryError("Invalid capability executor kind")
        if self.max_attempts < 1:
            raise CapabilityRegistryError("max_attempts must be at least 1")
        object.__setattr__(self, "executor_config", deepcopy(self.executor_config))
        object.__setattr__(self, "policy_snapshot", deepcopy(self.policy_snapshot))


_REGISTRY: dict[str, CapabilityDefinition] = {}


def register_capability(
    definition: CapabilityDefinition,
    *,
    replace: bool = False,
) -> None:
    if definition.capability_key in _REGISTRY and not replace:
        raise CapabilityRegistryError(
            f"Capability {definition.capability_key!r} is already registered"
        )
    _REGISTRY[definition.capability_key] = definition


def unregister_capability(capability_key: str) -> None:
    _REGISTRY.pop(capability_key, None)


def resolve_capability(capability_key: str) -> CapabilityDefinition | None:
    return _REGISTRY.get(capability_key)


def registered_capabilities() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))
