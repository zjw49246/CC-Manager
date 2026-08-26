"""Project readiness gate for user-facing executable Task creation.

Background clones are asynchronous: a Project may legitimately be targeted
while still ``pending``/``cloning``/``initializing`` (its Tasks wait in the
queue behind ``project_ready_dispatch_predicate``). A Project whose clone
already **failed** is different — ``local_path`` points at a directory that
does not exist and every launch would burn retry budget on a bare
``[Errno 2]``. Reject those at creation time with an actionable message.

Deliberately NOT part of ``task_creation.stage_task_record``: that boundary is
shared with entry points that must stay exempt (migration import, shared
shadows, Harness children, PR-review internals).
"""


class ProjectNotDispatchableError(Exception):
    """The target Project's clone failed; creating executable Tasks is refused."""

    def __init__(self, project_name: str, error_message: str | None):
        detail = f"Project '{project_name}' clone failed"
        if error_message:
            detail += f": {error_message}"
        detail += " — fix the Git settings and re-clone the project before creating tasks"
        super().__init__(detail)
        self.detail = detail


def require_project_dispatchable(project) -> None:
    """Raise when an executable Task targets a Project in ``error`` state.

    ``pending``/``cloning``/``initializing`` are allowed — the dispatch queue
    holds those Tasks until the clone completes.
    """
    if project is not None and project.status == "error":
        raise ProjectNotDispatchableError(project.name, project.error_message)
