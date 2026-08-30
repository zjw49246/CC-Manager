from datetime import datetime

from sqlalchemy import Integer, String, DateTime, Boolean, JSON
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.elements import ColumnElement

from backend.database import Base


# Tags with this prefix are Manager-owned resource identities, not
# presentation labels.  Public Project creation/update remains admin-only;
# consumers must nevertheless treat these Projects as non-shareable internal
# implementation records.
INTERNAL_PROJECT_TAG_PREFIX = "ccm:internal:"


def project_is_internal(project: object) -> bool:
    tags = getattr(project, "tags", None)
    return bool(
        isinstance(tags, (list, tuple, set))
        and any(
            isinstance(tag, str) and tag.startswith(INTERNAL_PROJECT_TAG_PREFIX)
            for tag in tags
        )
    )


class _EmptyJSONArrayServerDefault(ColumnElement):
    """Dialect-safe empty JSON array used by metadata ``create_all``."""

    type = JSON()
    inherit_cache = True


@compiles(_EmptyJSONArrayServerDefault)
def _compile_empty_json_array_default(_element, _compiler, **_kwargs):
    return "'[]'"


@compiles(_EmptyJSONArrayServerDefault, "mysql")
def _compile_mysql_empty_json_array_default(_element, _compiler, **_kwargs):
    # MySQL 8 rejects bare literal defaults for JSON columns.  JSON_ARRAY() is
    # accepted when parenthesized as a default expression.
    return "(JSON_ARRAY())"


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    worker_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)  # NULL = local
    git_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    has_remote: Mapped[bool] = mapped_column(Boolean, default=False)
    local_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    default_branch: Mapped[str] = mapped_column(String(100), default="main")
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, cloning, ready, error
    error_message: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    show_in_selector: Mapped[bool] = mapped_column(Boolean, default=True, server_default="1")
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False, server_default="0", index=True)
    tags: Mapped[list] = mapped_column(
        JSON,
        default=list,
        nullable=False,
        server_default=_EmptyJSONArrayServerDefault(),
    )
    env_files: Mapped[list] = mapped_column(
        JSON,
        default=list,
        nullable=False,
        server_default=_EmptyJSONArrayServerDefault(),
    )
    # Trusted, manager-owned commands used to launch an isolated preview of a
    # Task worktree.  Repository files are never executed as preview config.
    preview_config: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # Git identity (commit author)
    git_author_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    git_author_email: Mapped[str | None] = mapped_column(String(200), nullable=True)
    # Git credentials
    git_credential_type: Mapped[str | None] = mapped_column(String(20), nullable=True)  # "ssh" | "https" | None
    git_ssh_key_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    git_https_username: Mapped[str | None] = mapped_column(String(200), nullable=True)
    git_https_token: Mapped[str | None] = mapped_column(String(500), nullable=True)
    badge_color: Mapped[str | None] = mapped_column(String(20), nullable=True)  # color key for task list badge
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
