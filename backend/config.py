from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///./claude_manager.db"
    openai_api_key: str = ""
    auth_token: str = ""
    max_concurrent_instances: int = 8   # hard cap on live local instances (idle + running)
    min_idle_instances: int = 2         # auto top-up idle instances (capped by max_concurrent)
    claude_binary: str = "claude"
    codex_binary: str = "codex"
    # Keep one Codex app-server process and run task turns over JSON-RPC.
    # Startup/protocol failures automatically fall back to `codex exec`.
    codex_app_server_enabled: bool = True
    codex_app_server_request_timeout: float = 30.0
    # Inject task-scoped required ccm_skills into Codex main tasks on both
    # app-server and the MCP-equivalent exec fallback.  Keep the environment
    # override as an emergency rollback switch.
    codex_main_mcp_enabled: bool = True
    default_provider: str = "codex"
    provider_options: str = "claude,codex"
    default_model: str = "claude-opus-4-6"
    model_options: str = "default,claude-opus-5,claude-sonnet-5,claude-sonnet-5[1m],claude-fable-5,claude-fable-5[1m],claude-opus-4-6,claude-opus-4-6[1m],claude-opus-4-7,claude-opus-4-7[1m],claude-opus-4-8,claude-opus-4-8[1m],claude-sonnet-4-6,claude-sonnet-4-6[1m],claude-haiku-4-5"  # comma-separated
    default_codex_model: str = "gpt-5.6-sol"
    # GPT-5.6 是三个模型（sol/terra/luna），无裸 "gpt-5.6" ID（Codex 服务端模型列表实证）
    codex_model_options: str = "default,gpt-5.6-sol,gpt-5.6-terra,gpt-5.6-luna,gpt-5.5,gpt-5.4,gpt-5.4-mini,gpt-5.3-codex-spark"  # comma-separated
    # 基线档位（gpt-5.5 及更早）；gpt-5.6 系列的 max/ultra 见 services/codex_models.py
    codex_effort_options: str = "low,medium,high,xhigh"
    default_codex_goal_evaluator_model: str = "gpt-5.4-mini"
    default_effort: str = "medium"
    effort_options: str = "low,medium,high,xhigh,max"  # comma-separated
    host: str = "0.0.0.0"
    port: int = 8000
    # Optional fixed Manager-local origin for MCP servers/hooks. When empty,
    # the live ASGI listening address is learned from incoming requests.
    internal_api_base_url: str = ""
    # Public base URL of this deployment (e.g. https://ccm.example.com),
    # used to display the GitHub webhook URL on the PR Monitor page.
    public_base_url: str = ""
    workspace_dir: str = "~/Projects"
    auto_start_dispatcher: bool = True
    merge_push_retries: int = 3
    auto_push_to_origin: bool = True
    task_timeout_seconds: int = 7200  # 2 hours
    # 会话上下文利用率达到该比例即自动摘要+换新 session。超大 context 的请求
    # 在服务端易挂起（2026-07-08 task 22/27 连环 stall 均发生在 ~90% 区间），
    # 故不要设回 0.9 让 session 在重灾区长时间工作。
    context_compact_threshold: float = 0.80
    default_goal_evaluator_model: str = "claude-haiku-4-5"
    goal_evaluation_timeout: int = 120
    # --- Independent Plan Agent pipeline ---
    # Both stages have an explicit primary/fallback route. A Plan Task stores
    # the resolved snapshot so queued work cannot drift with later defaults.
    plan_planner_provider: str = "claude"
    plan_planner_model: str = "claude-fable-5"
    plan_planner_effort: str = "high"
    plan_planner_fallback_provider: str = "codex"
    plan_planner_fallback_model: str = "gpt-5.6-terra"
    plan_planner_fallback_effort: str = "xhigh"
    plan_reviewer_enabled: bool = True
    plan_reviewer_provider: str = "codex"
    plan_reviewer_model: str = "gpt-5.6-sol"
    plan_reviewer_effort: str = "xhigh"
    plan_reviewer_fallback_provider: str = "claude"
    plan_reviewer_fallback_model: str = "claude-sonnet-5"
    plan_reviewer_fallback_effort: str = "high"
    plan_max_revision_cycles: int = 2
    # Pause/resume rounds per PlanRun. This never limits questions per request.
    plan_max_interactions: int = 3
    plan_planner_timeout: int = 1800
    plan_reviewer_timeout: int = 900
    # Once a Codex Reviewer has started streaming agent/reasoning deltas, a
    # longer silent interval indicates a stuck Responses stream. Initial
    # xhigh reasoning is deliberately governed by plan_reviewer_timeout.
    plan_reviewer_delta_idle_timeout: int = 90
    # Abort only pathological JSON whitespace outside string values. Long Plan
    # content and reasoning remain governed by the normal route timeout.
    plan_structured_output_whitespace_limit: int = 4_096
    plan_transcript_max_chars: int = 60_000
    plan_step_output_max_chars: int = 200_000
    git_ssh_key_path: str = ""  # Instance-level SSH key, fallback when project has none
    # Browser-uploaded SSH Profile keys. Files are private host credentials;
    # only opaque one-time upload tokens are returned to the frontend.
    ssh_key_storage_dir: str = "~/.ccm/ssh-keys"
    ssh_sftp_max_concurrency: int = 8
    ssh_sftp_queue_timeout_seconds: float = 5.0
    ssh_sftp_operation_timeout_seconds: float = 30.0
    ssh_sftp_download_timeout_seconds: float = 120.0
    ssh_sftp_channel_timeout_seconds: float = 15.0
    # Task-scoped MCP credentials and Claude security settings live outside
    # world-readable/shared temporary directories. Agent sandboxes deny this
    # complete root after the trusted CLI has loaded its own files.
    task_runtime_secret_dir: str = "~/.ccm/task-runtime-secrets"

    # --- Distributed workers (docs/plans/elastic-worker-design.md) ---
    worker_enabled: bool = True
    worker_cloud_provider: str = "aws"  # 目前仅 aws
    worker_ssh_key_path: str = ""       # Manager 自己密钥对的私钥 .pem 路径
    worker_ssh_user: str = "ubuntu"
    worker_remote_dir: str = "/home/ubuntu/ccm"  # Worker 上 CCM 部署目录
    worker_deploy_source_dir: str = "."          # rsync 部署源（Manager 本地仓库根）
    # Worker EC2 固定配置（非空时覆盖 Manager 自身配置，避免随 Manager 升级漂移）
    worker_instance_type: str = ""       # e.g. "t3.medium"
    worker_image_id: str = ""            # AMI ID
    worker_subnet_id: str = ""           # VPC subnet
    worker_security_group_ids: str = ""  # 逗号分隔的安全组 ID
    worker_key_name: str = ""            # SSH key pair name

    # --- Team CCM: Feishu + Org Registry ---
    feishu_app_id: str = ""
    feishu_app_secret: str = ""
    # Stable deployment secret used to authenticate the OAuth state across
    # workers/restarts.  When omitted, the Feishu app secret is used.
    feishu_oauth_state_secret: str = ""
    feishu_oauth_state_ttl_seconds: int = 600
    org_registry_enabled: bool = False
    org_registry_url: str = ""

    # --- Registration email verification ---
    # Credentials must be supplied by the deployment; never ship a shared
    # mailbox password in the application source.
    smtp_host: str = "smtp.feishu.cn"
    smtp_port: int = 465
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = ""
    verification_code_capacity: int = 4096
    verification_code_rate_window_seconds: int = 600
    verification_code_email_limit: int = 5
    verification_code_ip_limit: int = 30
    verification_code_resend_cooldown_seconds: int = 60
    verification_code_smtp_concurrency: int = 4

    # --- Custom system prompt (appended to Claude's default) ---
    append_system_prompt_file: str = ""  # path to .md file, e.g. system-prompts/fable5.md

    # --- PTY persistent-session mode (claude provider only) ---
    # When true, claude tasks run in long-lived interactive PTY sessions
    # (claude_pty): prompts are delivered via channel injection, events come
    # from the session JSONL. Flip to false to fall back to `claude -p`.
    use_pty_mode: bool = True

    # --- Claude account pool (auto-rotation on rate limit) ---
    pool_enabled: bool = True
    pool_config_path: str = "~/.claude-pool/accounts.json"
    pool_cooldown_seconds: int = 300  # per-account cooldown after rate limit

    # --- Codex account pool ---
    codex_pool_enabled: bool = True
    codex_pool_config_path: str = "~/.codex-pool/accounts.json"
    codex_pool_cooldown_seconds: int = 300

    # --- CloudRouter API accounts ---
    # Each API identity owns one private root with separate Claude/Codex native
    # homes underneath it.  The two provider pools project the same identity
    # instead of introducing a task-level execution mode.
    cloudrouter_accounts_dir: str = "~/.ccm/api-accounts"

    # --- Transient server-side 429 / overload auto-retry ---
    # Anthropic 基础设施侧的临时限流/过载（"Server is temporarily limiting
    # requests (not your usage limit)" / overloaded）——退避后用同一账号
    # --resume 重试，区别于「额度用尽」的换号轮换（见 claude_pool）。
    transient_retry_enabled: bool = True
    transient_retry_max: int = 5            # 最多自动重试次数
    transient_retry_base_delay: float = 10.0  # 首次退避秒数（指数递增）
    transient_retry_max_delay: float = 120.0  # 退避上限秒数

    # --- ask_user：拦截内置 AskUserQuestion，转前端卡片 ---
    ask_user_enabled: bool = True       # 关闭则不注入 hook，AskUserQuestion 回到原生行为
    ask_user_timeout: int = 1800        # hook 阻塞等待用户回答的上限秒数（超时放行原生工具）

    # --- /tmp pressure protection ---
    # Capacity and inode pressure both trigger the same allow-list-only sweep.
    # Unknown files and deployment/update artifacts are never removed.
    tmp_cleanup_enabled: bool = True
    tmp_cleanup_usage_threshold: float = 0.80
    tmp_cleanup_interval_seconds: int = 3 * 3600
    tmp_cleanup_min_age_seconds: int = 6 * 3600

    # --- Backup service (auto-backup) ---
    backup_enabled: bool = False        # Set true to enable periodic DB backups
    backup_type: str = "local"          # local | s3 | oss
    backup_interval_seconds: int = 3600
    backup_max_copies: int = 10
    backup_temp_dir: str = ""           # Custom temp dir for archive files (avoids filling /tmp)
    # local backend
    backup_destination_path: str = ""
    # AWS S3 backend
    backup_s3_bucket: str = ""
    backup_s3_region: str = ""
    backup_s3_access_key: str = ""
    backup_s3_secret_key: str = ""
    # Alibaba Cloud OSS backend
    backup_oss_endpoint: str = ""
    backup_oss_bucket: str = ""
    backup_oss_access_key: str = ""
    backup_oss_secret_key: str = ""

    # --- One-click update & restart ---
    service_name: str = "ccm.service"  # systemd service to restart (e.g. ccm-dev.service)
    service_scope: str = "auto"        # auto | user | system

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
