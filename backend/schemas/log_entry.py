from datetime import datetime

from pydantic import BaseModel


class LogEntryResponse(BaseModel):
    id: int
    instance_id: int
    task_id: int | None
    event_type: str
    role: str | None
    content: str | None
    tool_name: str | None
    tool_input: str | None
    tool_output: str | None
    item_id: str | None
    # This diagnostic needs the owning Task/Instance provider, which is not
    # available on a bare LogEntry ORM row. Endpoints derive it explicitly.
    protocol_anomaly: str | None = None
    is_error: bool
    timestamp: datetime

    model_config = {"from_attributes": True}
