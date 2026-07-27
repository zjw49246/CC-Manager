# 测试指南

> **重要：Claude Code 必须自主维护本文件。** 新增功能时同步更新测试，修改代码前先跑测试，修改后再跑一遍确认无回归。

## 快速命令

```bash
# 安装依赖（首次，包含测试依赖）
uv sync --group dev

# 运行全部后端测试
uv run python -m pytest backend/tests/ -v

# 运行单个测试文件
uv run python -m pytest backend/tests/test_task_queue.py -v

# 运行匹配名称的测试
uv run python -m pytest backend/tests/ -k "dequeue" -v

# 前端类型检查
cd frontend && npx tsc --noEmit
```

---

## 自动化测试

### 后端测试（pytest + pytest-asyncio）

测试使用内存 SQLite，不依赖真实数据库或外部服务。

#### `test_tmp_space_manager.py` — `/tmp` 压力保护

测试在 pytest 隔离目录中注入容量/inode 读数；全局测试环境关闭真实宿主
`/tmp` 看门狗，不会扫描或删除开发机文件。

| 测试 | 验证内容 |
|------|---------|
| `test_below_eighty_percent_does_not_scan_or_delete` | 容量低于 80% 时不触发、不删除 |
| `test_exactly_eighty_percent_removes_all_stale_allowlisted_artifacts` | 容量恰好 80% 时触发；即使中途已低于触发线，仍清完全部合格候选 |
| `test_pressure_cleanup_is_allowlist_age_and_symlink_safe` | 只删过期白名单文件；保留未知文件、近期文件、symlink 和 `ccm-update-*` |
| `test_host_cleanup_never_recursively_deletes_directories` | 即使名称匹配隔离格式，宿主目录也不会被递归删除 |
| `test_candidate_refreshed_after_scan_is_revalidated` | 扫描后重新活跃的候选会在原子删除前复核并保留 |
| `test_stale_session_migration_directory_is_excluded` | session 迁移 staging 即使过期也不自动删除 |
| `test_inode_pressure_also_triggers_cleanup` | inode 使用率达到 80% 也会触发 |
| `test_default_disk_usage_uses_space_available_to_service_uid` | 字节压力按服务用户实际可用的 `f_bavail` 计算 |
| `test_concurrent_checks_share_one_cleanup_pass` | 真正并发的检查共享同一次在途操作，结束后的调用重新读取用量 |
| `test_completed_below_threshold_result_is_not_cached` | 79% 检查结果不会形成缓存盲区，下一次 100% 可立即触发 |
| `test_cross_process_lock_busy_skips_this_periodic_pass` | 跨进程清理锁被占用时跳过本轮并等待下个周期 |
| `test_cancellation_waits_for_inflight_cleanup_thread` | 取消会等待 rename/unlink 工作线程真正结束 |
| `test_periodic_loop_is_cancellation_safe` | 后台看门狗可在服务关闭时正常取消 |
| `test_disabled_manager_does_not_start_periodic_task` | 关闭配置时不创建后台任务 |

#### 共享 Docker 独立 `/tmp`

`test_container_manager.py` 另覆盖容器私有 2GB tmpfs：root-owned lease 的父目录与
inode 不可替换；exact 80% 取得独占锁且证明容器空闲后清空，清后低于 80% 可放行；
活跃 Agent 的共享锁、未知 PID、检查失败均保留文件并拒绝新启动；新容器先建 lease
再检查，Docker init 回收孤儿进程；PTY 压力错误不得降级为宿主裸进程。

此外，`test_api_files.py` 验证 SSH 下载响应结束后删除 staging 文件。

#### `test_task_queue.py` — 任务队列核心逻辑

| 测试 | 验证内容 |
|------|---------|
| `test_create_task` | 创建任务，确认默认值正确（status=pending, priority=0） |
| `test_dequeue_priority_order` | **关键**：P0 先于 P1 先于 P10 出队（数字越小优先级越高） |
| `test_dequeue_fifo_within_same_priority` | 同优先级按创建时间 FIFO |
| `test_dequeue_returns_none_when_empty` | 队列空时返回 None |
| `test_mark_completed` | 标记完成，确认 status 和 completed_at |
| `test_mark_failed` | 标记失败，确认 error_message 存储 |
| `test_mark_status_generic` | 通用状态更新（如 executing、merging） |
| `test_retry_increments_count` | 重试时 retry_count+1，error_message 清空 |
| `test_cancel_task` | 取消 pending 任务 |
| `test_cancel_executing_task` | 取消 executing/merging 状态的任务 |
| `test_delete_conflict_task` | 允许删除 conflict 状态的任务 |
| `test_delete_running_task_rejected` | 禁止删除 in_progress 状态的任务 |
| `test_list_tasks_ordered` | 列表按优先级排序 |
| `test_list_tasks_filter_status` | 按状态筛选 |

#### `test_stream_parser.py` — NDJSON 解析

| 测试 | 验证内容 |
|------|---------|
| `test_empty_line` | 空行返回 None |
| `test_invalid_json` | 非 JSON 返回 parse_error 事件 |
| `test_system_init` | 解析 session_id |
| `test_assistant_message` | 提取助手消息内容 |
| `test_tool_use` | 解析工具调用名称和输入 |
| `test_tool_result` | 解析工具结果 |
| `test_tool_result_error` | 检测错误结果 |
| `test_result_with_cost` | 提取 session_id 和 cost_usd |
| `test_result_is_error` | 检测错误结果事件 |
| `test_content_extraction_*` | 各种 content 格式（string, list, nested） |
| `test_assistant_tool_use_block` | assistant 事件含 tool_use 块 → 正确提取 tool_name/tool_input |
| `test_assistant_thinking_block` | assistant 事件含 thinking 块 → 提取为 thinking 事件 |
| `test_thinking_with_text_field` | thinking 块用 `text` 字段（Opus 4.7+ 兼容） |
| `test_thinking_with_nested_content_blocks` | thinking 块用嵌套 `content` 列表 |
| `test_thinking_encrypted_block` | 仅 signature/data 的加密 thinking → `[encrypted thinking ...]` 标记 |
| `test_thinking_completely_empty_block` | 空 thinking 块 → content 为空字符串 |
| `test_thinking_legacy_field_still_works` | 原 `thinking` 字段仍是首选路径 |
| `test_user_event_tool_result` | type=user 事件 → 映射为 tool_result，提取 tool_output |
| `test_user_event_tool_result_error` | type=user 事件含 is_error → 正确设置错误标记 |
| `test_system_non_init` | system 非 init 子类型 → 映射为 system_event |
| `test_assistant_empty_content_blocks` | assistant 空 content 块 → 默认为 message 事件 |

#### `test_models.py` — ORM 模型

| 测试 | 验证内容 |
|------|---------|
| `test_task_defaults` | Task 所有默认值正确 |
| `test_task_with_project_id` | project_id 外键可正常存储 |
| `test_instance_defaults` | Instance 所有默认值正确 |
| `test_project_defaults` | Project 所有默认值正确 |
| `test_project_no_git_url` | 无 git_url 项目：git_url=None, has_remote=False |
| `test_project_unique_name` | 项目名唯一约束生效 |

#### `test_api_tasks.py` — Task API 端点

| 测试 | 验证内容 |
|------|---------|
| `test_create_task` | POST 创建任务，状态码 201 |
| `test_create_task_with_project_id` | 支持 project_id 创建 |
| `test_list_tasks` | GET 列出全部任务 |
| `test_get_task` | GET 获取单个任务 |
| `test_get_task_not_found` | 404 处理 |
| `test_delete_task` | DELETE 删除任务 |
| `test_cancel_task` | 取消任务 |
| `test_retry_task` | 重试任务 |

#### `test_api_chat_plan.py` — Chat 和 Plan API

| 测试 | 验证内容 |
|------|---------|
| `test_chat_history_not_found` | 不存在的 task 返回 404 |
| `test_chat_history_empty` | 无历史消息返回空数组 |
| `test_chat_history_returns_tool_fields` | 历史消息包含 tool_input 和 tool_output 字段 |
| `test_chat_send_no_session` | 无 session 的 task 发消息返回 400 |
| `test_chat_send_task_not_found` | 不存在的 task 发消息返回 404 |
| `test_chat_send_no_idle_instance` | 所有 instance 都在运行时返回 503 |
| `test_chat_send_task_being_processed` | task 正在被处理时返回 409 |
| `test_chat_send_cwd_uses_last_cwd` | 使用 last_cwd 作为工作目录 |
| `test_chat_send_cwd_not_found` | 工作目录不存在返回 400 |
| `test_plan_approve_not_plan_review` | 非 plan_review 状态 approve 返回 400 |
| `test_plan_reject_not_plan_review` | 非 plan_review 状态 reject 返回 400 |
| `test_plan_approve_success` | plan_review 状态 approve → status=pending, plan_approved=True |
| `test_plan_reject_success` | plan_review 状态 reject → status=cancelled, plan_approved=False |
| `test_plan_approve_not_found` | 不存在的 task approve 返回 404 |
| `test_plan_reject_not_found` | 不存在的 task reject 返回 404 |

#### `test_api_system.py` — 系统 API

| 测试 | 验证内容 |
|------|---------|
| `test_health` | GET /api/system/health → {"status": "ok"} |
| `test_stats_empty` | 无数据时所有计数为 0 |
| `test_stats_with_tasks` | 不同状态 task 计数正确 |
| `test_stats_running_instances` | running 实例计数正确 |

#### `test_api_auth.py` — 认证 API

| 测试 | 验证内容 |
|------|---------|
| `test_login_no_auth_configured` | auth_token="" 时任何请求都通过 |
| `test_login_valid_token` | 正确 token 登录成功 |
| `test_login_invalid_token` | 错误 token 返回 401 |
| `test_login_missing_token_field` | 空 body 返回 422 |

#### `test_login_runtime.py` — 自动登录浏览器运行时

| 测试 | 验证内容 |
|------|---------|
| `test_claude_and_codex_pool_share_one_login_lock` | Claude/Codex Pool 使用同一个进程内登录锁 |
| `test_login_child_environment_uses_configured_isolated_runtime` | display 和磁盘临时目录按环境隔离，固定 CDP 端口不再下发 |
| `test_resource_guard_rejects_low_available_memory` | 可用内存不足时在 Chrome 启动前 fail-fast |
| `test_xauthority_cookie_is_private_and_not_exposed_in_argv` | Xauthority 为 0600，cookie 仅经 stdin 传递 |
| `test_cached_xvfb_is_polled_before_reuse` | 缓存的 Xvfb 必须先 `poll()` 再复用 |
| `test_ready_xvfb_from_sibling_process_is_reused_without_popen` | 同 display 的健康 Xvfb 可安全共享 |
| `test_foreign_x_socket_is_not_killed_or_replaced` | 无法认证的外部 X server fail-closed，不执行 `pkill` |
| `test_xvfb_start_waits_for_real_display_readiness` | 启动后必须通过实际 display 探测才能放行 Chrome |
| `test_sigkilled_owned_xvfb_stale_socket_is_recovered_before_restart` | SIGKILL/OOM 后仅凭持久 owner identity + 原 socket inode 安全恢复 |
| `test_stale_owner_record_does_not_authorize_replaced_socket` | owner record 存在但 socket inode 已变化时继续 fail-closed |

`test_codex_login_mailbox.py` 还覆盖 authorize 页面 45 秒超时只重试一次并附带
内存/load 诊断；`test_chrome_cdp.py` 覆盖动态 `DevToolsActivePort`、browser
websocket identity 校验及固定端口上的孤儿 Chrome 不会被复用；
`test_cdp_login_mailbox.py` 覆盖 Claude Chrome 使用独立磁盘 profile，且只回收
自己启动的进程。

#### API / 原生账号统一路由

| 测试文件 | 验证内容 |
|---------|---------|
| `test_claude_pool.py` | API-first 模型兼容选择、quota/auth fail-closed、Claude session + sidecar 安全迁移，以及候选选择不会提前更新 `last_selected` |
| `test_codex_pool.py` | API-first 与原生 fallback、独立 round-robin cursor、额度终态分类和最终路由 marker |
| `test_resume_config_dir.py` | preferred 手动切换、已有会话粘性、Task durable binding、多副本消歧，以及 rollout copy 期间取消/代次变化不会产生无主副本 |
| `test_service_instance_manager.py` | Claude/Codex reactive/proactive 换号、精确 queued message 保留、Codex copy→rebind→binding 的 cancellation-settled 事务，以及 silent exit/`turn.failed` 按真实 provider 归因且不重复报错 |
| `PoolDrawer.test.tsx` | Claude/Codex 的「优先账号」「最近使用」独立展示，以及恢复自动后的 API-first/旧会话绑定提示 |

#### `test_api_projects.py` — 项目 API

| 测试 | 验证内容 |
|------|---------|
| `test_list_projects_empty` | 空项目列表 |
| `test_create_project_with_git_url` | 201, has_remote=True |
| `test_create_project_local_no_git_url` | 201, has_remote=False |
| `test_create_project_duplicate_name` | 重复名称 400 |
| `test_get_project` / `test_get_project_not_found` | 获取/404 |
| `test_update_project` / `test_update_project_not_found` | 更新/404 |
| `test_update_project_git_url_sets_has_remote` | 设置 git_url 后 has_remote=True |
| `test_delete_project` / `test_delete_project_not_found` | 删除/404 |
| `test_reclone_success` | re-clone 成功 |
| `test_reclone_local_project_rejected` | 本地项目拒绝 re-clone |

#### `test_api_instances.py` — 实例 API

| 测试 | 验证内容 |
|------|---------|
| `test_list_instances_empty` | 空实例列表 |
| `test_create_instance` / `test_create_instance_custom_model` | 创建实例 |
| `test_create_instance_with_thinking_budget` | 创建时携带 `thinking_budget` 字段 |
| `test_create_instance_default_thinking_budget_is_null` | 不传 `thinking_budget` → 响应为 null |
| `test_run_instance_forwards_thinking_budget` | `/run` 把 instance 的 budget 传给 `launch()` |
| `test_get_instance` / `test_get_instance_not_found` | 获取/404 |
| `test_delete_instance` / `test_delete_instance_not_found` | 删除/404 |
| `test_stop_instance_success` / `test_stop_instance_not_running` | 停止/非运行 |
| `test_run_with_prompt` / `test_run_with_task_id` | 运行实例 |
| `test_run_already_running` / `test_run_no_prompt_no_task` | 运行异常 |
| `test_get_logs` | 获取日志 |
| `test_dispatcher_status/start/stop` | 调度器控制 |
| `test_ralph_start/stop/status` | Ralph Loop 控制 |

#### `test_autonomous_mirror.py` — PTY autonomous turn 全量镜像

| 测试 | 验证内容 |
|------|---------|
| `test_task_notification_becomes_system_event` | autonomous user `<task-notification>` 压成一行 system_event 入库+广播 |
| `test_channel_echo_dropped` | autonomous user channel 回显直接丢弃（防重放旧 prompt） |
| `test_non_autonomous_user_event_unchanged` | 非 autonomous user 事件维持原行为（orphan 回填不受影响） |
| `test_autonomous_assistant_message_logged_and_unread` | 自主 turn 的 assistant 产出入库 + has_unread + task 频道广播 |
| `test_restore_replaces_subagent_only` | on_exit 后降级回调被换回全量转发 |
| `test_mirror_forwards_to_process_event` | 镜像回调转发 event.to_dict() 给 _process_event |
| `test_mirror_swallows_process_event_errors` | 镜像回调异常不外抛（不打断 idle watcher） |
| `test_restore_skips_fresh_binding` | 轮换 relaunch 的新绑定 _on_autonomous 不被覆盖 |
| `test_restore_skips_none_session` | session 缺失时安全跳过 |
| `test_init_wires_full_mirror_backend` | use_pty_mode 开启时 IM 构造即接线 FullMirrorCCMBackend |

#### `test_native_sub_agents.py` — 原生子 Agent 接入（通用 sub_agent 表）

| 测试 | 验证内容 |
|------|---------|
| `test_generic_model_defaults` | SubAgentSession 默认 agent_type=monitor / source=ccm |
| `test_native_agent_record` | native-agent 记录 + meta JSON（tool_use_id） |
| `test_legacy_aliases_still_work` | MonitorSession/MonitorCheck 别名 + monitor_session_id synonym 兼容 |
| `test_spawn_progress_done_lifecycle` | spawn→progress→done 生命周期 + 去重 + sub_agent_* 广播 |
| `test_progress_for_unknown_agent_is_noop` | 未注册 tool_use_id 的 progress 为 no-op |
| `test_missing_tool_use_id_ignored` | 无 tool_use_id 不入库 |
| `test_summary_groups_by_agent_type` | /sub-agents/summary 按 agent_type 分组，running/completed 恒存在 |

> PTY 侧 turn 对齐与子 agent 观测的测试在 PTY 仓库 `tests/test_turn_alignment.py`
>（task87 错位回归：backlog orphan、in-flight turn_duration 不结束新 turn、
> 空闲 watcher 消费自主 turn、挂起子 agent 的 session 不被驱逐）。

#### `test_permission_relay.py` — PTY 权限透传

| 测试 | 验证内容 |
|------|---------|
| `test_permission_request_logged_and_broadcast` | 权限请求 → LogEntry + WS 卡片事件 + pending 登记 |
| `test_resolve_permission_roundtrip` | allow 回包 bridge + resolved 广播 + 二次回包幂等失败 |
| `test_resolve_not_delivered_no_broadcast` | bridge 送达失败不落库不广播（防误标已允许） |
| `test_resolve_unknown_or_expired` | 未知/过期 request 返回 False |
| `test_permission_endpoint_*` | API：200 / 410 过期 / 400 非法 behavior / 404 任务不存在 |

#### `test_ask_user.py` — 拦截内置 AskUserQuestion → 前端卡片

| 测试 | 验证内容 |
|------|---------|
| `test_registry_create_resolve_roundtrip` | registry 登记 future → resolve set 答案 → await 拿到；list_for_task 过滤 task |
| `test_registry_resolve_unknown_and_double` | 未知 request_id / 已完成 future 二次 resolve 均返回 False |
| `test_registry_discard_and_list_excludes_done` | discard 移除；已 resolve（future done）的从 pending 列表排除 |
| `test_format_answer_reason_*` | 喂回模型的 deny reason 文案：单选 / 多选 / 自定义文本 / 缺答兜底 |
| `test_inject_adds_hook_and_is_idempotent` | hook 合并进 settings.json，保留既有 key 与他人 hook，重复注入不重复 |
| `test_disable_removes_our_hook_only` | `ask_user_enabled=False` 时只移除我们的项，不动他人 hook |
| `test_inject_handles_corrupt_settings` | 损坏 JSON 的 settings.json 不报错、照常注入 |
| `test_inject_creates_missing_dir` | config_dir 不存在时自动建目录 + 写入 |

> 完整 HTTP+claude 回环（模型调用 AskUserQuestion → hook 阻塞 → 提交答案 → 模型续答）由真实环境集成测试验证，见 PROGRESS.md「ask_user」条目。

#### `test_api_monitor.py` — Monitor API 端点

| 测试 | 验证内容 |
|------|---------|
| `test_create_monitor_session` | POST 创建 monitor session，状态码 200 |
| `test_create_monitor_no_skill` | enabled_skills 无 monitor 时 → 403 |
| `test_create_monitor_task_not_found` | task 不存在 → 404 |
| `test_create_monitor_task_completed` | task 已完成 → 400 |
| `test_create_monitor_concurrency_limit` | 超过 5 个并发 monitor → 429 |
| `test_list_monitor_sessions` | GET 列出 task 下所有 monitor sessions |
| `test_get_monitor_session` | GET 获取单个 monitor session |
| `test_get_monitor_session_not_found` | 404 处理 |
| `test_delete_monitor_session` | DELETE 停止 monitor session |
| `test_get_monitor_checks` | GET 获取 monitor 检查历史 |
| `test_task_delete_cleans_monitors` | task 删除 → MonitorCheck 和 MonitorSession 全部清理 |
| `test_task_cancel_cancels_monitors` | task 取消 → 所有 running monitor 变为 cancelled |

#### `test_api_pr_monitor.py` — PR Monitor API（CRUD + GitHub Webhook）

| 测试 | 验证内容 |
|------|---------|
| `test_create_repo_success` / `test_create_repo_duplicate` / `test_create_repo_invalid_format` | 创建仓库成功（detail 返回完整 secret）/ 重复 → 409 / 非 `owner/repo` 格式 → 422 |
| `test_list_repos_masks_secret` | 列表响应 secret 被掩码（前 4 位 + `***`） |
| `test_update_repo_settings` / `test_update_repo_not_found` | 更新 auto_merge/branch/authors / 404 |
| `test_toggle_repo` / `test_regenerate_secret` / `test_delete_repo` | 启停切换 / 重新生成 secret / 删除（级联清理 reviews） |
| `test_webhook_info_configured` / `test_webhook_info_unconfigured` | PUBLIC_BASE_URL 设置时返回 webhook URL，否则 null |
| `test_webhook_valid_signature_creates_review_and_task` | 合法 HMAC 签名 → 创建 PRReview + Task |
| `test_webhook_invalid_signature_rejected` / `test_webhook_missing_signature_rejected` | 签名错误/缺失 → 403 |
| `test_webhook_unknown_repo_ignored` / `test_webhook_disabled_repo_ignored` | 未监控/已禁用仓库忽略 |
| `test_webhook_non_pull_request_event_ignored` / `test_webhook_draft_pr_ignored` / `test_webhook_wrong_base_branch_ignored` / `test_webhook_author_not_allowed_ignored` | 各类过滤条件忽略 |
| `test_webhook_duplicate_opened_ignored_while_in_progress` | 进行中重复 opened 事件去重 |
| `test_webhook_synchronize_supersedes_old_review` | synchronize 将旧 review 标记 superseded 并新建 |

#### `test_mcp_server.py` — MCP Server 工具

| 测试 | 验证内容 |
|------|---------|
| `test_mcp_server_tools_registered` | MCP server 启动，3 个 tool 正确注册 |
| `test_api_url` | API URL 拼接正确 |
| `test_create_monitor_success` | create_monitor → HTTP POST 成功 |
| `test_check_monitors_returns_sessions` | check_monitors → HTTP GET 返回状态 |
| `test_check_monitors_empty` | 无 monitor 时返回空列表 |
| `test_stop_monitor_success` | stop_monitor → HTTP DELETE 成功 |
| `test_create_monitor_api_error` | API 不可达 → `{"success": false}` |
| `test_check_monitors_api_error` | API 不可达 → `{"success": false}` |
| `test_stop_monitor_api_error` | API 不可达 → `{"success": false}` |

#### `test_monitor_models.py` — Monitor 数据层

| 测试 | 验证内容 |
|------|---------|
| `test_monitor_session_crud` | MonitorSession CRUD 操作 |
| `test_monitor_check_crud` | MonitorCheck CRUD 操作 |
| `test_monitor_session_defaults` | MonitorSession 默认值正确 |
| `test_enabled_skills_json_field` | enabled_skills JSON 字段读写 |
| `test_enabled_skills_none` | enabled_skills 为 None 时正常 |
| `test_enabled_skills_multiple` | 多 skill 的 JSON 读写 |
| `test_multiple_checks_per_session` | 单 session 多次 check 记录 |

#### `test_mcp_config.py` — MCP Config 生成

| 测试 | 验证内容 |
|------|---------|
| `test_generate_mcp_config_none_skills_still_includes_ccm_skills` | enabled_skills 为 None 时仍注入统一 ccm_skills server |
| `test_generate_mcp_config_empty_skills_still_includes_ccm_skills` | enabled_skills 为空时仍注入统一 ccm_skills server |
| `test_generate_mcp_config_skills_do_not_add_extra_servers` | 任意 skill 组合不会产生独立的 per-skill server |
| `test_generate_mcp_config_monitor_enabled` | monitor: true → 生成包含 ccm_skills server 的配置 |
| `test_generate_mcp_config_file_path` | 配置文件路径格式正确 |
| `test_cleanup_mcp_config` | 正确清理临时文件 |
| `test_cleanup_mcp_config_missing_file` | 文件不存在时不报错 |
| `test_*_mcp_server_spec_snapshot` | 主任务、Monitor、Sub-Agent 的 provider-neutral spec、上下文参数、工具白名单和超时快照 |
| `test_spec_enabled_tools_match_registered_server_tools` | 三类 spec 的工具白名单与 FastMCP 实际注册工具完全一致 |
| `test_claude_json_output_remains_compatible` | 三类现有生成函数仍输出原有 Claude `mcpServers` JSON |
| `test_default_api_base_and_empty_auth_token` | 默认 API 地址归一化且空 token 不进入参数 |
| `test_platform_paths_are_preserved` | Linux/Windows、空格和中文路径不被 renderer 改写 |
| `test_claude_renderer_includes_env_but_not_provider_metadata` | Claude renderer 透传 env 且不泄漏 provider 专用字段 |
| `test_mcp_server_spec_collections_are_immutable` | spec 拷贝参数、环境变量和工具列表，避免调用方后续修改 |
| `test_claude_renderer_rejects_duplicate_server_names` | 重名 server 明确失败而不是静默覆盖 |
| `test_codex_app_server_renderer_includes_supported_stdio_fields` | app-server `config.mcp_servers` 覆盖 command/args/cwd/env/required/enabled_tools/timeouts，并保留空格、中文、引号和反斜杠 |
| `test_codex_exec_renderer_serializes_the_same_config_as_toml` | exec `-c` argv 使用合法 TOML 数组/inline table，反解析后与 app-server 配置完全一致 |
| `test_codex_renderers_share_each_role_spec` | 主任务、Monitor、Sub-Agent 的同一份 spec 可同时渲染为 app-server 和 exec 配置 |
| `test_codex_renderer_omits_unset_optional_fields` | 未设置的 cwd/env/enabled_tools/timeouts 不进入 Codex 配置，避免空 allow-list 改变工具语义 |
| `test_codex_renderers_support_empty_specs` | 空 spec 集合得到空 app-server 配置和空 exec argv |
| `test_codex_exec_renderer_emits_one_merged_server_table_override` | 多 server 合并为一个 `mcp_servers` inline table 覆盖，绕开 CLI dotted-path 拆名且不丢配置 |
| `test_codex_renderers_reject_duplicate_server_names` | 两种 Codex 输出都拒绝重名 server |
| `test_codex_renderers_reject_names_the_cli_cannot_initialize` | 提前拒绝 Codex 初始化阶段不接受的空白、点号、空格和非 ASCII server 名 |
| `test_codex_renderers_do_not_write_codex_home` | renderer 不创建或修改 `$CODEX_HOME/config.toml` |
| `test_codex_renderers_reject_invalid_timeouts` | app-server/exec 均拒绝 NaN、无穷、负数和布尔 timeout，避免延迟到 CLI 初始化才失败 |

Codex 版本兼容基线（2026-07-24）：

- `0.144.6` 与 `0.145.0` 生成的 app-server schema 均允许 `thread/start`、`thread/resume` 的 `config` object。
- 两版 CLI 的隔离 `$CODEX_HOME` smoke test 均可通过 `mcp list -c <merged-inline-table> --json` 无损解析上述 stdio 字段；已有用户 MCP entry 与本次 override 会深合并，不会被覆盖。
- 两版 app-server 均已用真实 `thread/start` 启动 CCM FastMCP server，`required`、`enabled_tools` 和 timeout 配置可正常完成 session 初始化。
- 实测确认 app-server 对 server 名强制 `^[a-zA-Z0-9_-]+$`；renderer 在进 CLI 前做同样校验。
- smoke test 前后隔离目录均未产生 `config.toml`；路径/中文/引号/反斜杠的无损序列化由不经过 shell 的单元测试覆盖。

#### `test_codex_app_server.py` / `test_service_instance_manager.py` — Codex 主任务 MCP 按 thread 注入

| 测试 | 验证内容 |
|------|---------|
| `test_start_turn_injects_mcp_config_into_new_thread` | `thread/start` 收到 task-scoped `config.mcp_servers.ccm_skills` |
| `test_start_turn_uses_native_resume_and_turn_start` | `thread/resume` 同时合并 MCP 配置和线程级 Git 环境，不互相覆盖 |
| `test_concurrent_task_threads_keep_mcp_context_isolated` | 同一 app-server 并发任务保留各自 `task_id`，配置对象不串线 |
| `test_required_mcp_thread_rejection_is_explicit` | required MCP 的 thread admission 失败转为可安全重试的 `CodexRequiredMcpPreTurnError` |
| `test_invalid_required_mcp_config_is_explicit_before_thread_rpc` | required spec 在本地校验失败时 fail closed，且不发送 thread RPC |
| `test_required_mcp_app_server_startup_failure_is_explicit` | cleanup 已确认的 app-server transport 启动失败标为 pre-turn，可由上层安全回退 |
| `test_required_mcp_missing_thread_id_is_explicit` | malformed/no-thread-id 响应标为 pre-turn，不进入 `turn/start` |
| `test_required_mcp_startup_cleanup_uncertain_is_not_replay_safe` | app-server 启动清理未确认时保留普通 required 错误，禁止重放 |
| `test_required_mcp_missing_turn_id_is_explicit_and_detaches_context` | malformed/no-turn-id 响应 fail closed，并清理未成立的 turn context |
| `test_build_command_codex_renders_required_mcp_as_exact_argv_tokens` | fresh/resume 的 exec 均把同一 spec 渲染成精确 `-c` argv，且置于 thread id/prompt 前 |
| `test_build_command_codex_rejects_invalid_required_exec_mcp` | required exec spec 非法时转为显式能力错误 |
| `test_codex_main_mcp_uses_exec_when_app_server_is_disabled` | app-server 关闭时主任务直接通过带 required MCP 的 exec 启动 |
| `test_invalid_required_exec_mcp_fails_before_subprocess_spawn` | production launch 在非法 required exec config 下不得创建子进程 |
| `test_required_mcp_pre_turn_failure_falls_back_to_equivalent_exec` | transport 启动失败/no-thread-id 只在 turn 前回退，且 exec argv 保留当前 task 的 required MCP |
| `test_required_mcp_unknown_app_server_failure_does_not_launch_exec` | required MCP 下未知 adapter 异常继续 fail closed |
| `test_launch_codex_does_not_fallback_when_replay_is_unsafe` | timeout、busy、required 非 pre-turn、owner mismatch 和已启动 turn 的持久化失败均禁止重放 |
| `test_codex_sub_agent_requires_app_server_and_never_uses_exec` | app-server 关闭时 Sub-Agent 明确失败，绝不降级到没有 live thread control 的 exec |
| `test_codex_sub_agent_mcp_failure_does_not_launch_exec` | 即使是 pre-turn 错误，Sub-Agent 仍不得走 exec |
| `test_codex_app_server_rejects_home_owned_by_exec_generation` | 同一 `CODEX_HOME` 有 exec generation 时 app-server 返回 busy |
| `test_live_codex_quota_rejects_home_owned_by_exec_generation` | exec generation 已占用同一 `CODEX_HOME` 时，实时额度读取不得创建或调用 app-server |
| `test_live_codex_quota_rejects_active_ephemeral_exec` | 临时 Codex exec 占用同一 home 时，实时额度读取同样返回 busy |
| `test_live_codex_quota_holds_home_gate_until_rpc_finishes` | 实时额度 RPC 全程持有 home 门禁，后来的 exec 必须等待并在进入前关闭空闲 app-server |
| `test_ephemeral_codex_exec_rejects_active_app_server` | app-server 有活跃 turn 时，临时 Codex exec 不得进入同一 home |
| `test_codex_exec_shuts_down_idle_app_server_before_spawn` | exec 启动前关闭同 home 的空闲 app-server |
| `test_codex_exec_does_not_spawn_while_app_server_home_is_busy` | 同 home app-server 有活跃 turn 时 exec 不得创建进程 |
| `test_codex_sub_agent_rejects_home_owned_by_exec_generation` | 普通 exec generation 占用同 home 时，Codex 子 Agent 不得绕过 app-server admission gate |
| `test_codex_sub_agent_rejects_active_ephemeral_exec` | 临时 exec 占用同 home 时，Codex 子 Agent 同样不得启动 app-server |
| `test_codex_main_mcp_capability_defaults_on` | Codex 主任务 MCP 服务端 capability 默认开启 |
| `test_codex_main_mcp_capability_allows_explicit_env_opt_out` | `CODEX_MAIN_MCP_ENABLED=false` 可显式恢复旧行为 |
| `test_rollout_enabled_routes_fresh_and_resume_with_task_scoped_mcp` | 默认 rollout 的 fresh/resume 均注入当前 task-scoped required MCP |
| `test_runtime_settings_reports_effective_codex_main_mcp_capability` | Runtime Settings GET/PUT 均返回实际 capability |
| `test_provisioner_ccm_config_uses_private_stdin_atomic_write` | Worker `.env` 继承 Manager 的主 MCP capability 值 |
| `test_launch_codex_app_server_uses_passed_task_scoped_specs` | app-server adapter 使用 launch 层一次性构建的完整 task-scoped spec |
| `test_codex_app_server_uses_passed_sub_agent_controller_specs` | app-server adapter 使用 launch 层构建的窄化 Sub-Agent controller spec |
| `test_launch_codex_app_server_routes_turn_to_canonical_home` | capability 关闭时 app-server 行为保持原样且不注入空配置 |
| `test_codex_main_mcp_capability_does_not_change_claude_launch` | capability 开启不改变 Claude provider 的启动路径 |

前端 capability 展示回归：

| 测试 | 验证内容 |
|------|---------|
| `ChatView > Codex main MCP capability` | Manager 默认开启、紧急关闭、runtime broadcast，以及 Worker 代理 runtime capability 均显示准确 |
| `PrefsMenu > shows the read-only Codex main MCP runtime capability for admins` | 管理员设置菜单展示只读的实际主任务 MCP capability |

人工 app-server/exec smoke（仅测试环境）：

1. 保持默认 `CODEX_APP_SERVER_ENABLED=true`，确认未设置 `CODEX_MAIN_MCP_ENABLED=false`，重启后端并检查 `/api/settings/runtime` 返回 `codex_main_mcp_enabled=true`。
2. 创建本地 Codex task，要求它“必须调用 `ccm_command_help` 查询一个 CCM 命令后原样报告工具结果”。
3. 确认日志出现 `mcp_tool_call`，server/tool 为 `ccm_skills/ccm_command_help`，且工具参数中的 task 上下文对应当前任务。
4. 在同一 task 发送第二条消息并确认 resume 仍可调用；再并发运行另一个 task，确认两边查询结果和 `task_id` 不串线。
5. 设置 `CODEX_APP_SERVER_ENABLED=false` 并重启，再建同类 task；应改走 `codex exec`，但仍出现同一个 `ccm_skills/ccm_command_help` 成功调用。
6. 恢复 app-server，并模拟 transport 启动失败或 no-thread-id：只允许出现一次带 MCP 的 exec 回退；模拟 `turn/start` timeout/错误时则不得出现 exec 重放。
7. 同一账号有活跃 app-server turn 时尝试启动 exec，应返回 busy；turn 结束后 exec 可关闭空闲 app-server 再启动。反向在 exec generation 未收尾时启动 app-server 也应返回 busy；此时在 Codex 账号池强制刷新额度应显示该账号暂不可实时读取，且日志中不得出现同 home 的新 app-server 启动。
8. 关闭 app-server 后启用 Codex Sub-Agent task，应明确报告其需要 app-server，且不得启动 exec。
9. 设置 `CODEX_MAIN_MCP_ENABLED=false` 重启并确认普通 Codex exec 无 `ccm_skills`；测试完移除该覆盖并恢复原来的 `CODEX_APP_SERVER_ENABLED` 设置。

真实验收记录（2026-07-24）：

- Codex CLI `0.145.0`、`gpt-5.6-sol`、隔离 `CODEX_HOME`。
- app-server `thread/start` 成功启动 required `ccm_skills`，模型显式调用 `ccm_command_help` 一次。
- `mcp_tool_call` 状态为 `completed`，耗时约 864 ms，返回 JSON `success=true`；turn 正常完成且 returncode 为 0。
- smoke 使用独立 SQLite 数据库中的 task 1，结束后已关闭测试后端并清理隔离 CODEX_HOME。

真实 exec 验收记录（2026-07-27）：

- Codex CLI `0.145.0`、`gpt-5.6-sol`，命令由生产 `InstanceManager._build_command()` 构造，包含 reasoning 与 MCP 两个独立 `-c` argv token。
- MCP 指向仅返回测试 task 1 的隔离 localhost API；模型实际产生 `ccm_skills/ccm_command_help` 的 `item.started` → `item.completed(status=completed)`。
- turn 最终严格输出 `PR4_EXEC_MCP_OK`，returncode 为 0；隔离 API、进程和临时 smoke 脚本随后全部清理。

#### `test_monitor_dispatcher.py` — Monitor Dispatcher 生命周期

| 测试 | 验证内容 |
|------|---------|
| `test_build_monitor_prompt` | prompt 构建包含描述和上下文 |
| `test_build_monitor_prompt_no_context` | 无上下文时 prompt 正常 |
| `test_start_monitor_session` | 启动 monitor session 创建 asyncio task |
| `test_lifecycle_max_checks_reached` | max_checks 耗尽 → completed |
| `test_lifecycle_task_ended` | task 结束 → monitor 联动结束 |
| `test_lifecycle_subprocess_timeout` | 子进程超时 → failed check → 继续 |
| `test_lifecycle_subprocess_crash` | 子进程崩溃 → failed check → 继续 |
| `test_lifecycle_cancelled` | CancelledError → kill 子进程 |
| `test_lifecycle_done_status` | STATUS: done → completed |
| `test_lifecycle_unexpected_exception_marks_failed` | 未预期异常 → failed |
| `test_lifecycle_writes_check_record` | check 结果写入 DB |
| `test_lifecycle_broadcasts_check_event` | check 结果广播 WebSocket |

#### 服务层单元测试

##### `test_service_ws_broadcaster.py` — WebSocket 广播

| 测试 | 验证内容 |
|------|---------|
| `test_subscribe` / `test_subscribe_multiple_channels` | 订阅单/多频道 |
| `test_unsubscribe` / `test_unsubscribe_cleans_empty_channels` | 取消订阅 + 清理空频道 |
| `test_broadcast_sends` | 广播消息到所有订阅者 |
| `test_broadcast_removes_dead_connections` | 自动移除断开连接 |
| `test_broadcast_no_subscribers` | 无订阅者不报错 |

##### `test_service_whisper_client.py` — Whisper 客户端

| 测试 | 验证内容 |
|------|---------|
| `test_transcribe_success` | 正常转录成功 |
| `test_transcribe_no_api_key` | 无 API key 报 ValueError |
| `test_transcribe_wav` / `test_transcribe_mp3` | 不同音频格式 |
| `test_transcribe_api_error` | API 错误抛 HTTPStatusError |

##### `test_service_instance_manager.py` — 实例管理器

| 测试 | 验证内容 |
|------|---------|
| `test_launch_creates_subprocess` | 启动子进程，正确参数 |
| `test_launch_with_resume` / `test_launch_with_model` | resume/model 参数 |
| `test_launch_updates_db` / `test_launch_saves_cwd` | DB 状态更新 |
| `test_launch_unsets_claude_env` | 排除 CLAUDECODE 环境变量 |
| `test_launch_with_thinking_budget_sets_env` | `thinking_budget>0` → 设置 `MAX_THINKING_TOKENS` env |
| `test_launch_without_thinking_budget_omits_env` | 默认不设置 `MAX_THINKING_TOKENS` |
| `test_launch_with_zero_thinking_budget_omits_env` | `thinking_budget=0` 视为无预算 |
| `test_stop_terminates` / `test_stop_kills_on_timeout` | 正常停止/超时 kill |
| `test_is_running` | 运行状态检测 |
| `test_process_event_sets_transient_flag_on_overload_error` | 带 `is_error` 的瞬时 429/过载事件置 turn-scoped 标记（PTY 下 exit_code=0 仍可重试的关键信号） |
| `test_process_event_usage_limit_does_not_set_transient_flag` | 额度横幅**不**置标记（应走换号而非同号重试） |
| `test_process_event_clean_event_leaves_flag_unset` / `test_launch_resets_transient_flag` | 干净事件不置位 / 新 `launch()` 重置标记 |
| `test_process_event_orphan_overload_does_not_set_transient_flag` | resume 回放的旧 api_error（`orphan`）与后台子 agent 报错（`autonomous`）**不**置标记——否则成功 resume 被误判 failed（task #729 recover-then-failed） |
| `test_build_command_codex_gpt56_passes_max_effort` / `..._ultra_effort` | GPT-5.6 sol/terra 的 `max`/`ultra` 档位真实传给 codex CLI（旧代码把 max 一律丢弃） |
| `test_build_command_codex_old_model_clamps_max_to_xhigh` / `..._luna_clamps_ultra_to_max` | 不支持的高档位向下夹到该模型最高档，而非静默丢弃 |
| `test_build_command_claude_opus5_with_max_effort` | Opus 5 的模型 ID 与 `max` effort 原样传给 Claude CLI |

##### `test_claude_models.py` — Claude 模型能力

| 测试 | 验证内容 |
|------|---------|
| `test_opus5_has_fixed_1m_context_window` / `test_default_model_is_resolved_before_context_lookup` | Opus 5（含作为默认模型时）固定使用 1M context |
| `test_existing_1m_suffix_remains_supported` / `test_unknown_claude_model_uses_default_context_window` | 兼容既有 `[1m]` 变体，未知模型安全回退 200K |
| `test_opus5_supports_full_effort_scale` | Opus 5 支持 `low/medium/high/xhigh/max` 完整 effort 档位 |

##### `test_codex_models.py` — Codex 模型目录（GPT-5.6 三模型）

| 测试 | 验证内容 |
|------|---------|
| `test_codex_model_options_contain_all_three_gpt56_models` | 选项含 `gpt-5.6-sol`/`-terra`/`-luna` 三个模型 |
| `test_codex_model_options_have_no_bare_gpt56` | **关键**：裸 `gpt-5.6` 不是有效模型 ID（服务端列表实证） |
| `test_gpt56_*_support_*` / `test_older_models_fall_back_*` | 按模型区分档位：sol/terra 到 ultra、luna 到 max、旧模型到 xhigh |
| `test_clamp_*` | `clamp_codex_effort` 透传受支持档位 / 向下夹不支持档位 / None 与未知输入安全 |

（前端配套：`TaskForm.test.tsx` 的 "Codex GPT-5.6 per-model effort options" 套件——模型下拉列三模型、effort 选项按模型过滤、切模型后失效档位自动回落默认）

##### Codex provider 对等逻辑（AGENTS.md，2026-07-19）

| 测试 | 验证内容 |
|------|---------|
| `test_service_dispatcher.py::test_goal_initial_prompt_codex_references_agents_md` | codex goal 任务的 prompt 指向 AGENTS.md |
| `test_service_dispatcher.py::test_build_task_prompt_provider_doc` | task prompt 前导按 provider 引用 CLAUDE.md / AGENTS.md |
| `test_service_dispatcher.py::test_build_task_prompt_carries_doc_sync_note` | 两种 provider 的 prompt 前导都下发 CLAUDE.md/AGENTS.md 关键内容同步纪律 |
| `test_service_dispatcher.py::test_build_task_prompt_codex_skips_skill_templates` | skills 模板只注入 claude（MCP 工具 codex 不可用） |
| `test_service_dispatcher.py::test_loop_prompt_codex_references_agents_md` | loop prompt 按 provider 引用文档 |
| `test_api_projects.py::test_inject_agents_md_*` | project 创建注入 AGENTS.md symlink：正常创建 / 无 CLAUDE.md 不动 / 已存在不覆盖（实现在 `services/agent_docs.py`） |
| `test_service_dispatcher.py::test_lifecycle_backfills_agents_md` | 存量项目惰性补齐：任务启动时对 target_repo 补 AGENTS.md symlink |
| `test_api_projects.py::test_init_local_repo_preserves_existing_claude_md` / `..._preserves_both_existing_docs` | **不覆盖原有文件**：存量目录（有文件未 git init）建本地项目时，已有 CLAUDE.md/AGENTS.md 原样保留（红→绿实证） |

##### Codex 对等补齐（2026-07-19，文案/字段全部 codex-rs 0.144.6 源码实证）

| 测试 | 验证内容 |
|------|---------|
| `test_claude_pool.py::TestCodexTransientDetection` | codex transient 检测：stream disconnected / request timed out / high demand / at capacity / 429/5xx 命中；401、usage limit、quota **不**命中（互斥） |
| `test_claude_pool.py::TestCodexUsageAndAuthDetection` | codex 限额/认证失败文案检测 |
| `test_claude_pool.py::TestProviderAwareTransientRouting` | `is_transient_for` 按 provider 分流；**危险重叠回归锚点**：codex 限额文案会命中 claude `_RATE_LIMIT_RE` |
| `test_claude_pool.py::TestChatPoolRotationCodexGate` | codex 任务绝不进 claude 号池轮换（不 gate 会用 claude --resume 重启 codex session） |
| `test_service_instance_manager.py::test_parse_codex_reasoning_becomes_thinking` 等 | codex 解析器：reasoning→thinking、file_change/mcp_tool_call/web_search→tool 事件、todo_list、error item、turn.failed 嵌套 message |
| `test_codex_models.py::TestCodexContextWindow` | codex 模型窗口表（272K/128K，models_cache.json 实测）与回退 |
| `test_task_migrator.py::test_migrate_codex_task_uses_codex_session_mover` 等 | 迁移按 provider 分流搬 session；rollout 文件 glob 定位 |
| `test_api_monitor.py::test_create_monitor_rejects_codex_task` / `test_create_sub_agent_rejects_codex_task` | monitor / sub-agent 对 codex 任务显式 400（不静默跑成 Claude 子进程） |
| `test_service_pr_review.py::test_create_pr_review_task_codex_provider` | PR 审核 task 透传 repo.provider，codex 未配模型时补默认 |
| `test_claude_pool.py::TestDispatcherRotationCodexGate` | dispatcher 轮换 gate 正反两例：codex 限额文案不轮换不冷却任何账号；同类 claude 文案照常轮换 |
| `test_claude_pool.py::TestChatTransientRetryCodex` | chat transient retry 全链路：codex 文案触发重试且 relaunch 带 `provider=codex`；claude 文案对 codex 任务不生效；限额不触发重试；claude 正向对照 |
| `test_resume_config_dir.py::TestResolveResumeConfigDirCodexGate` | resume 选号对 codex 返回 None（不 select 不 migrate），claude 不受影响 |
| `test_service_instance_manager.py::test_process_event_codex_window_backfill` | codex usage 无窗口时回填 272K（落库 + 广播都验证） |
| `test_service_instance_manager.py::test_parse_codex_file_change_started_is_tool_use` | file_change 的 item.started → tool_use（真实事件流实证 started 存在，源码注释不实） |
| `test_codex_app_server.py::test_notifications_stream_delta_and_finish_process` | app-server `thread/tokenUsage/updated` 保留真实 `modelContextWindow`、latest total/reasoning token，而非累计 thread total |
| `test_codex_app_server.py::test_existing_goal_turn_notification_rebinds_submission_id` | adopted goal 同时保留 active/submission 两个通知 ID，任一 ID 的 assistant/terminal 事件都不能丢 |
| `test_codex_app_server.py::test_signal_interrupt_reconciles_and_pauses_existing_goal_turn` | adopted goal Interrupt 只做一次 pause RPC，再中断权威 active turn |
| `test_service_instance_manager.py::test_internal_codex_abort_is_not_a_successful_chat_terminal` | transport/admission 内部 abort 不得伪装成用户 Interrupt 的 completed |
| `test_service_dispatcher.py::test_cancelled_task_followup_reclaims_session_instead_of_dropping` | Cancel 仅终止当前 generation，后续 chat 可安全领取原生 session 并恢复 executing |
| `test_codex_app_server.py::test_context_window_error_keeps_structured_codex_error_info` | `turn/completed` 失败不得丢弃 `codexErrorInfo=contextWindowExceeded` 与 additionalDetails |
| `test_context_compaction.py` | provider 共享的上下文超限分类、Codex current-context token 计算及旧 usage fallback |
| `test_service_instance_manager.py::test_codex_context_window_failure_compacts_and_requeues` | chat Codex 结构化超限事件触发摘要、清 session、携原消息 `compact_retry` 自动续跑 |
| `test_service_instance_manager.py::test_recent_failure_output_keeps_structured_codex_error` | fresh/mode 失败分类读取受限长度的 raw system error envelope，不遗漏结构化错误码 |
| `test_service_instance_manager.py::test_process_event_codex_exec_uses_rollout_last_usage` | exec fallback 从 rollout 取 `last_token_usage`，不把 1.5M 累计 turn token 误报成 553% 上下文 |
| `test_service_dispatcher.py::test_lifecycle_codex_context_error_compacts_before_retry` | fresh/mode Codex 超限后摘要并回到 pending，不消耗普通失败重试语义 |
| `test_service_dispatcher.py::test_codex_precompact_uses_full_context_tokens` | Codex 预压缩按 current context（含会进入下一请求的 output）和有效窗口触发 |
| `test_api_pr_monitor.py::test_create_repo_with_codex_provider` 等 | PR Monitor API 层 provider 创建/默认/更新（含显式 null 清空模型防跨家族残留） |
| 前端 `ProjectTodoList.test.tsx` | Todo Run 建 task 带 provider |
| 前端 `TaskForm.test.tsx::Codex provider UI gating` | codex 下 Thinking 隐藏、显式「Skills / Monitor 仅支持 Claude」标注（非静默消失） |
| 前端 `MonitorPanel.test.tsx` | codex 任务的 Monitor 面板显示「暂不支持 Codex」横幅，claude 无横幅 |

##### `test_service_worktree_manager.py` — Worktree 管理器

| 测试 | 验证内容 |
|------|---------|
| `test_create_success` | 创建 worktree + DB 记录 |
| `test_create_fetch_fails_continues` | fetch 失败继续创建 |
| `test_create_origin_branch_missing_fallback` | origin 分支不存在时回退 |
| `test_sync_latest_success` / `test_sync_latest_conflict` | 同步成功/冲突 |
| `test_merge_to_main_success` / `test_merge_to_main_conflict` | 合并成功/冲突 |
| `test_remove_worktree` | 删除 worktree + DB 更新 |

##### `test_service_backup.py` — 数据库备份服务

| 测试 | 验证内容 |
|------|---------|
| `TestBuildDestination::test_local_ok` | local 目标 dict 字段正确 |
| `TestBuildDestination::test_local_empty_path_returns_none` | local 路径为空时返回 None（禁用备份） |
| `TestBuildDestination::test_s3_ok` | S3 目标 dict 包含 bucket/region/access_key/secret_key |
| `TestBuildDestination::test_s3_missing_bucket_returns_none` | S3 缺少 bucket 时返回 None |
| `TestBuildDestination::test_oss_ok` | OSS 目标 dict 包含 endpoint/bucket/access_key/secret_key |
| `TestBuildDestination::test_oss_missing_endpoint_returns_none` | OSS 缺少 endpoint 时返回 None |
| `TestBuildDestination::test_oss_missing_bucket_returns_none` | OSS 缺少 bucket 时返回 None |
| `TestBuildDestination::test_unknown_type_returns_none` | 未知类型返回 None |
| `TestResolveDbPath::test_strips_async_prefix` | 去掉 `sqlite+aiosqlite:///` 前缀 |
| `TestResolveDbPath::test_strips_sync_prefix` | 去掉 `sqlite:///` 前缀 |
| `TestResolveDbPath::test_absolute_path_unchanged` | 绝对路径正确解析 |
| `TestStart::test_local_starts_scheduler` | 调用 add_task + start，interval/max_copies 正确 |
| `TestStart::test_returns_false_when_destination_not_configured` | 目标未配置时返回 False，不实例化 AutoBackup |
| `TestStart::test_s3_passes_correct_destination` | S3 目标 dict 传给 add_task |
| `TestStart::test_oss_passes_correct_destination` | OSS 目标 dict 传给 add_task |
| `TestStart::test_custom_interval_and_max_copies` | 自定义 interval/max_copies 生效 |
| `TestStop::test_stop_calls_backup_stop` | stop() 调用底层 stop()，清空 _backup |
| `TestStop::test_stop_without_start_is_safe` | 未 start 时 stop() 不报错 |
| `TestStop::test_stop_idempotent` | 重复 stop() 只调用一次底层 stop() |

##### `test_service_ralph_loop.py` — Ralph Loop 生命周期

| 测试 | 验证内容 |
|------|---------|
| `test_start_creates_task` / `test_start_idempotent` | 启动/幂等性 |
| `test_stop_cancels` | 停止取消任务 |
| `test_is_running_true` / `test_is_running_false` | 运行状态检测 |

##### `test_service_dispatcher.py` — 全局调度器

| 测试 | 验证内容 |
|------|---------|
| `test_status_not_running` | 初始状态 running=False |
| `test_pause_dispatching_does_not_stop_dispatcher` | 维护 pause 只停止领取新任务，不停止 Dispatcher/活动 lifecycle；resume 后恢复 |
| `test_queued_resume_waits_at_maintenance_gate_and_stays_blocking` | chat/monitor 续跑在维护期间不 launch、保持 blocker，恢复后只执行一次 |
| `test_pause_wins_after_queued_resume_preparation_before_launch` | 续跑已完成准备但尚未写 `executing` 时，维护门禁仍能赢得竞态并阻止 launch |
| `test_clear_cancels_message_dequeued_before_inflight_registration` | consumer 在 `q.get()` 后、登记 in-flight 前暂停时，stop-session clear 推进 generation 并取消该 handoff；恢复后不 launch、不残留 blocker |
| `test_clear_preserves_registered_inflight_message_blocker` | 已登记为 in-flight 的真实工作不会被清队列隐藏，pending blocker 保留到处理结束 |
| `test_start_sets_running` / `test_start_idempotent` | 启动/幂等性 |
| `test_stop` | 停止并取消所有任务 |
| `test_ensure_instances_creates_workers` | 自动创建 worker 实例 |
| `test_ensure_instances_skips_if_enough` | 已有足够实例时跳过 |
| `test_lifecycle_success` | 完整成功生命周期 |
| `test_lifecycle_failure_retry` / `test_lifecycle_failure_max_retries` | 失败重试/达到上限 |
| `test_lifecycle_exception` | 异常标记 task failed |
| `test_plan_phase` | plan 模式进入 plan_review |
| `test_concurrent_task_consumers_reserve_distinct_idle_instances` | 不同 task 的 queued-message consumer 同时选 instance 时原子预留，分配到不同 idle worker |
| `test_reserve_idle_instance_excludes_only_integer_running_keys` | 远端 Worker 的字符串 lifecycle key 不进入本地 `Instance.id` 整数 SQL 过滤 |
| `test_instance_contention_requeues_exact_message` | 底层 `InstanceAlreadyRunningError` 防线触发时，原 `QueuedMessage` 重排队且不丢用户消息 |

##### `test_service_update.py` / `UpdateButton.test.tsx` — 安全更新与自动提醒

| 测试 | 验证内容 |
|------|---------|
| `test_get_active_tasks_only_returns_running_states` | 更新阻塞器只识别 `in_progress/executing` task |
| `test_get_blocking_tasks_includes_queued_resumes` | 已入队但尚未启动的续跑消息也属于停服 blocker |
| `test_start_update_blocks_running_prompt_only_instance` | prompt-only 手动实例 launch 后即使没有 Task 行，更新仍识别 `running` Instance 并拒绝停服 |
| `test_reconcile_blockers_clears_multi_dead_owner_ghost` | 空标题 Task 被 5 个已死亡 Instance 反向占用时先显示 description/claim count；显式核对后 fail-close Task 并清理全部 dead owner |
| `test_reconcile_keeps_unknown_live_process_as_blocker` / `test_reconcile_preserves_manager_owned_active_generation` | unknown/live PID 继续阻断；当前进程真实拥有的 generation 核对后保持原样 |
| `test_cleanup_preserves_reserved_fresh_task_claim` / shared/auxiliary cases | pending→in_progress 后仍在项目准备窗口的 reservation 不被误清；远端 shadow 与 live CCM/native auxiliary 保持 remote/current-process ownership |
| `test_live_auxiliary_generations_block_restart_without_active_task` | parent Task 已终态时，仍有 exact monitor/sub-agent task/process map 也会阻止重启；只有 DB stale row 不冒充 live blocker |
| `test_process_wide_test_services_use_ephemeral_*` | pytest 在首次 backend import 前隔离全局 DB、pool/login journal、Worker/backup 与 update checkout，避免测试污染开发环境 |
| `test_enqueue_then_clear_before_dequeue_removes_resume_blocker` | enqueue 后、consumer dequeue 前执行 stop-session 清队列，会同步清除 pending 标记，后续 blocker 查询不出现幽灵 `queued_resume` |
| `test_start_update_pauses_and_refuses_active_tasks` | 更新前暂停领取任务；活动 task 存在时即使 force 也拒绝并恢复调度 |
| `test_start_update_fails_closed_when_task_check_errors` | 活动任务查询失败时按“有风险”处理，恢复调度且绝不启动更新 |
| `test_rollback_pauses_and_refuses_active_tasks` | 回滚使用同一安全门；活动 task 存在时不启动停服脚本 |
| `test_rollback_and_update_share_operation_admission_lock` | 回滚初检后暂停时，并发更新必须等待同一操作准入锁；回滚只能使用锁内固定的原 commit/备份，不能被新状态替换 |
| `test_concurrent_rollbacks_admit_only_one_operation` | 两个并发回滚只能放行一个，后到请求在首个操作完成准入后明确拒绝且不重复启动脚本 |
| `test_needs_restart_compares_running_and_disk_sha_without_systemd` | 运行 SHA 与磁盘 SHA 精确比较，非 systemd 部署也能识别手动更新 |
| `test_resolve_remote_uses_tracking_remote_then_origin_fallback` | 使用分支 tracking remote，无配置时回退 `origin` |
| `test_manual_pull_uses_running_commit_as_deployment_base` | 手动 pull 后以进程实际运行 commit 为部署差异与回滚基线 |
| `test_dry_run_detects_manual_update_and_returns_blockers` | dry-run 返回手动更新状态、tracking remote 与活动任务详情 |
| `test_dry_run_keeps_manual_restart_signal_when_fetch_fails` | 远端 fetch 失败不能掩盖本地代码已经更新、服务仍需重启的信号 |
| `test_dry_run_does_not_report_local_ahead_as_update` | 本地领先远端不误报“有新提交” |
| `test_concurrent_dry_runs_share_one_remote_check` | 多个页面并发自动检查时，30 秒缓存和 async lock 只执行一次远端检查 |
| `test_dry_run_cache_keeps_blockers_fresh_and_force_bypasses_cache` | 缓存只复用版本结果，活动任务 blocker 实时刷新；手动 force 检查绕过缓存 |
| `test_pipeline_rechecks_tasks_before_restart_and_resumes_dispatcher` | 停服前二次检查捕获更新期间出现的活动 task，取消重启并恢复调度 |
| `test_restart_paths_block_queued_resume_from_pre_restart_window` | 无迁移/迁移两条停服路径在提示等待窗口收到 user/monitor 续跑时取消重启，绝不启动停服脚本 |
| `test_manual_pull_fast_restart_branch_uses_final_gate` | “代码未变但进程需重启”的早期快速分支也执行最终原子门禁 |
| `test_rollback_rechecks_queued_resume_after_warning` | rollback 在提示等待后重新检查排队续跑，出现新工作即恢复调度并取消停服 |
| `test_shutdown_commit_is_atomic_and_seals_new_enqueues` | 最终 blocker 查询与同步停服提交共用锁；提交后新入队明确拒绝 |
| `test_final_shutdown_check_fails_closed_on_query_error` | 最终 blocker 查询异常时 fail closed，绝不调用 restart/spawn |
| `test_ralph_dequeue_waits_for_shared_maintenance_gate` | 旧 RalphLoop dequeue 同样服从统一任务启动门禁 |
| `test_run_with_task_id_rejected_during_maintenance` | 手动 Instance task 启动在维护窗口返回 409，进程不启动 |
| `test_chat_send_returns_conflict_after_shutdown_commit` | 最终停服已提交后新聊天返回 409，明确要求重连重试而不是静默入队 |
| `test_update_dry_run_forwards_force_and_branch` | System API 将手动 dry-run 的 branch/force 原样透传给服务层 |
| `test_update_returns_conflict_when_active_tasks_block_start` | 正式更新被活动任务门禁拒绝时 API 返回 409，`force` 不得绕过 |
| `test_reconcile_endpoint_returns_structured_conflict` | 核对失败以结构化 409 返回，前端保留原 blocker，不能误开放更新按钮 |
| `automatic update reminder` | 页面打开约 1 秒后仅 dry-run 检查；最新版静默；更新以顶部非阻塞通知展示，点击查看才开弹窗；同页同 commit 只提醒一次；远端失败但本地需重启仍提醒 |
| `forces a fresh dry-run when the user checks manually` | 手动检查携带 `force=true` 绕过后端短缓存 |
| `blocks confirmation while tasks are active` | 弹窗展示活动 task 并禁用正式更新按钮 |
| `reconcile` / auxiliary blocker cases | 显式核对后强制 dry-run；失败或新 blocker 保留禁用态；Instance/Monitor/Sub-Agent 不伪装成可复制的用户 Task |
| `shows running, disk, and database revisions` / repair/restart cases | 前端分开展示运行代码、磁盘代码和 DB revision；只有后端明确证明安全时才轻量重启，否则执行完整修复 |
| rollback confirmation cases | 已迁移或迁移结果未知时必须二次确认数据库快照恢复的数据丢失；明确未迁移时只回退代码 |
| active handoff recovery cases | 页面刷新、可见性恢复和旧进程仍可响应期间，`restarting/starting + repair_required` 继续轮询，不误报终态失败 |

##### 部署事务、启动守卫与迁移故障注入

| 测试文件 | 验证内容 |
|---------|---------|
| `test_deployment_start_guard.py` | repo lease 优先于 `/tmp` 状态；commit/port/token 精确匹配；损坏、未知 PID 身份和活动 lease fail closed；失败事务只进入 maintenance-only；task shared fence 阻止跨进程部署竞态 |
| `test_main_deployment_start.py` | guard 在 `init_db` 前执行；maintenance-only 不访问数据库、不启动 Dispatcher/Worker；受控 handoff 跳过重复 mutation |
| `test_deployment_maintenance_auth.py` | 维护模式只开放 health/status/repair/rollback/restart；legacy recovery token 与已签名 admin JWT 无 DB 可恢复，member JWT 和密码登录被拒绝 |
| `test_update_deployment_state.py` | running/disk/Alembic 三态、SQLite/外部 DB 准入、dirty checkout（含未跟踪源码）、claim 后二次 blocker、取消释放 lease、回滚元数据恢复、systemd-run ACK 不确定性与前端快照 |
| `test_update_migrate_hardening.py` | 停服 SQLite 最终快照、迁移失败原子恢复、same-commit repair maintenance fence、回滚任一步失败不启服、慢启动稳定健康检查、late worker/token 门禁、旧 10 参数 worker self-claim、FD/权限/符号链接/超时故障 |
| `test_pre_start_guard.py` | pre-start 端口解析、受控启动跳过依赖/迁移、未知/危险状态阻止启动；普通启动仅在 guard 放行后执行 |
| `client.update.test.ts` | repair/restart/confirmed rollback 使用独立 API；结构化 409 错误保留 status/detail 并给出可读消息 |

##### `test_service_pr_review.py` — PR 审核服务

| 测试 | 验证内容 |
|------|---------|
| `test_build_review_prompt_auto_merge_on` / `..._off` | auto_merge 开关影响 prompt（是否含 `gh pr merge`） |
| `test_create_pr_review_task_happy_path` | 创建 PRReview + Task 并广播 `review_created` |
| `test_create_pr_review_task_broadcast_failure_logged_not_raised` | 广播失败 → logger.warning，不中断流程 |
| `test_check_and_update_review_merged` / `..._approved` / `..._changes_requested` | gh 状态映射 merged/approved/commented |
| `test_check_and_update_review_skips_terminal_status` | 终态 review 不再调用 gh |
| `test_check_and_update_review_auth_error_no_retry` | gh 认证错误（HTTP 401 等）→ 不重试，error 信息提示 `gh auth login` |
| `test_check_and_update_review_transient_failure_retried_then_error` / `..._retry_succeeds` | 瞬时失败重试一次（失败→error / 成功→正常） |
| `test_gh_pr_view_*` | subprocess mock：成功解析 / 401 → auth 分类 / 网络错误 → transient / spawn 失败包装为 GhError |

### 子 Agent 系统集成测试

> 以下功能通过启动开发服务（`./start-dev.sh`，端口 8003）进行端到端集成验证，暂无独立单元测试文件。

#### 子 Agent MCP Server (`ccm_monitor_agent_server.py`)

| 验证项 | 说明 |
|--------|------|
| MCP 进程启动 | `claude --mcp-config` 启动子 Agent 进程，进程正常运行 |
| `report_status` tool | 子 Agent 调用 → POST `/checks` → DB MonitorCheck 记录 + WebSocket 广播 |
| `mark_complete` tool | 子 Agent 调用 → POST `/complete` → session 状态变为 completed，进程自行退出 |
| `get_context` tool | 子 Agent 调用 → GET session 信息，返回 description/context/checks_done |

#### 子 Agent API (`sub_agents.py`)

| 验证项 | 说明 |
|--------|------|
| `GET /api/tasks/{id}/sub-agents/summary` | 返回 `by_type.monitor` 的 running/completed 计数 |
| task 不存在 | 返回 404 |

#### Monitor API 新增端点 (`monitor.py`)

| 验证项 | 说明 |
|--------|------|
| `POST /{session_id}/checks` | 子 Agent 报告状态，创建 MonitorCheck 记录，WebSocket 广播 |
| `POST /{session_id}/complete` | 子 Agent 标记完成，session 状态更新，WebSocket 广播 |
| checks_done >= max_checks | 自动标记 session 为 completed |

#### Dispatcher 子 Agent 生命周期

| 验证项 | 说明 |
|--------|------|
| `_launch_monitor_agent()` | 构建 Claude CLI + MCP config 命令，启动持久子进程 |
| `_build_monitor_agent_prompt()` | Agent 风格 prompt 包含监控目标、上下文、MCP 工具说明 |
| `_monitor_session_lifecycle()` | 启动子进程 → wait → 检查状态 → 清理（MCP config + 日志） |
| `stop_monitor` → 进程 kill | delete_monitor_session 终止子进程 + 清理 MCP 配置文件 |

#### 前端子 Agent UI

| 验证项 | 说明 |
|--------|------|
| 工具权限按钮 (Wrench) | `enabled_skills` 有启用项时显示，点击展开已启用 skill 列表 |
| 子 Agent 徽章 (Users) | `active_sub_agents > 0` 时显示计数 + pulse 动画 |
| 子 Agent 详情展开 | 点击徽章调用 summary API，按类型显示 running/completed 计数 |

### 时区与时间戳测试

#### 后端 — 时间戳 UTC 序列化

| 测试文件 | 测试用例 | 说明 |
|----------|---------|------|
| `test_task_schema.py` | `test_naive_created_at_serialized_with_utc_suffix` | 无时区 datetime 序列化为 `+00:00` |
| `test_task_schema.py` | `test_aware_created_at_preserved` | 已有时区的 datetime 保留 UTC 标记 |
| `test_task_schema.py` | `test_started_at_none_serialized_as_none` | None 保持 None |
| `test_task_schema.py` | `test_started_at_naive_gets_utc_suffix` | started_at 同样加 UTC |
| `test_task_schema.py` | `test_completed_at_naive_gets_utc_suffix` | completed_at 同样加 UTC |
| `test_task_schema.py` | `test_all_three_timestamps_have_utc` | 三个时间字段全部含 UTC 后缀 |
| `test_chat_timestamp.py` | `test_chat_history_timestamp_has_z_suffix` | 聊天历史时间戳带 Z 后缀 |
| `test_chat_timestamp.py` | `test_chat_history_null_timestamp` | 空时间戳返回 None |

#### 前端 — 时区转换与显示 (`timezone.test.ts`)

| 测试用例 | 说明 |
|---------|------|
| `treats naive timestamp (no Z) as UTC` | 无 Z 后缀的时间戳按 UTC 解析 |
| `naive timestamp converts correctly to non-UTC timezone` | 无 Z 后缀正确转换到用户时区 |
| `naive timestamp with microseconds is handled` | 含微秒的无 Z 后缀时间戳正常处理 |
| `timestamp with positive/negative offset is preserved` | 已有偏移量的时间戳不被二次转换 |
| `formatDateTime always includes date even for today` | 通用格式化始终包含日期 |
| `formatDateTime shows YYYY prefix for different year` | 不同年份显示完整年月日 |
| `formatDateTime treats naive timestamp as UTC` | formatDateTime 同样按 UTC 解析 |
| `formatDateTime converts UTC to user timezone` | 正确将 UTC 转为用户选定时区 |

#### Claude Pool (`test_claude_pool.py`)

| 测试用例 | 说明 |
|---------|------|
| `TestRateLimitDetection` / `TestAuthFailureDetection` / `TestPoolRotatable` | 限速/认证失败文案检测（窄正则，含中英文与各时区变体） |
| `TestTransientOverloadDetection` | **瞬时 429/过载检测**：命中 Anthropic 官方文案 `Server is temporarily limiting requests (not your usage limit)` / overloaded；与「额度用尽/认证失败」互斥（那些走换号）；无误报 |
| `TestTransientRetryDelay` | 退避计算：首次≈base、指数增长、封顶 cap、最小 1s |
| `TestClaudePool` | 账号加载、select 轮转、冷却标记/过期/清除、status 汇总 |
| `TestSessionMigration` | session JSONL 硬链接迁移（成功/已链接/缺文件/inode 冲突） |
| `TestChatPoolRotationRegression` | **回归**：chat 路径切号必须成功并迁移 session（曾因位置参数调用 keyword-only 的 `migrate_session` 静默失败） |
| `TestLocateSessionConfigDir` | 在所有账号目录中定位 session 实际所在的 config_dir |
| `TestSelectAsync` | `select_async` 在线程中执行，不阻塞事件循环 |
| `TestProbeEnvCleanup` | 探测子进程 env 必须剔除 `CLAUDECODE` / `CLAUDE_CODE` |
| `TestFetchUsage` | OAuth usage API 额度查询：正常返回、凭据缺失、token 过期、60s 缓存 |

**Pool 额度抽屉（手动测试）**：`POOL_ENABLED=true` 时 Header 左侧出现 "Pro" 徽标 → 点击打开抽屉 → 每个账号显示 5h/7d 利用率进度条（<60% 绿 / 60–85% 黄 / ≥85% 红）、冷却状态与解除冷却按钮；`GET /api/pool/usage` 返回合并了 usage 的账号列表。

### 前端检查

| 检查 | 命令 | 说明 |
|------|------|------|
| TypeScript 类型检查 | `cd frontend && npx tsc --noEmit` | 确认无类型错误 |
| 前端时区单元测试 | `cd frontend && npx vitest run src/config/timezone.test.ts` | 时区格式化测试 |
| 构建检查 | `cd frontend && npm run build` | 确认生产构建成功 |

---

## 人机协作测试

> **流程：人在浏览器操作 UI → 告诉 Claude Code 做了什么 → Claude Code 查库/查日志/查 git 验证结果。**
>
> 人负责操作和观察 UI，Claude Code 负责查数据确认后端状态是否正确。两者配合完成验证。

### 测试 1：启动与调度器

| 步骤 | 谁 | 做什么 |
|------|-----|--------|
| 1 | 人 | 启动后端 `uvicorn backend.main:app --reload` |
| 2 | AI | 查 DB 确认 worker instances 已自动创建：`SELECT * FROM instances` |
| 3 | 人 | 打开 Dashboard，观察 instances 列表是否显示 worker |
| 4 | 人 | 点击「Stop Dispatcher」按钮 |
| 5 | AI | 调用 `GET /api/dispatcher/status` 确认 `running: false` |
| 6 | 人 | 点击「Start Dispatcher」按钮 |
| 7 | AI | 再次确认 `running: true` |

### 测试 2：项目管理

| 步骤 | 谁 | 做什么 |
|------|-----|--------|
| 1 | 人 | 在 TaskForm 选 "+ New project"，输入项目名 + 有效 git URL，创建任务 |
| 2 | AI | 查 DB 确认 project 和 task 都创建了，project status 从 `pending` → `cloning` → `ready`，CLAUDE.md 已生成 |
| 3 | 人 | 再次创建任务，选 "+ New project"，只输入项目名（不填 URL） |
| 4 | AI | 查 DB 确认 project.has_remote=False，目录已 git init，CLAUDE.md 已生成 |
| 5 | 人 | 创建一个同名 Project |
| 6 | 人 | 确认 UI 提示错误（400） |

### 测试 3：任务创建与执行

| 步骤 | 谁 | 做什么 |
|------|-----|--------|
| 1 | 人 | 在 TaskForm 下拉选择一个 ready 的 Project，填写标题和 Prompt，创建任务 |
| 2 | AI | 查 DB 确认 task 的 project_id 正确，target_repo 为空（等 dispatcher 填充） |
| 3 | 人 | 观察 TaskList，确认任务状态从 pending → executing（蓝色闪烁） |
| 4 | AI | 查 DB 确认 task.status = `executing`，instance_id 已分配，target_repo 已填充为项目路径 |
| 5 | 人 | 等任务执行完，观察状态变为 completed（绿色） |
| 6 | AI | 查 DB 确认 task.status = `completed` |

### 测试 4：优先级调度

| 步骤 | 谁 | 做什么 |
|------|-----|--------|
| 1 | 人 | 先停 Dispatcher |
| 2 | 人 | 创建 3 个任务：P5、P0、P3 |
| 3 | 人 | 启动 Dispatcher |
| 4 | AI | 查 DB 确认第一个变为 in_progress 的是 P0 的任务 |
| 5 | 人 | 在 TaskList 上确认 P0 最先显示执行状态 |

### 测试 5：Git 工作流验证

| 步骤 | 谁 | 做什么 |
|------|-----|--------|
| 1 | 人 | 创建一个简单任务（如 "在 README 末尾加一行注释"） |
| 2 | 人 | 等待任务完成 |
| 3 | AI | 在项目 repo 中执行 `git log --oneline -5` 确认有新 commit 并已 push 到 main |
| 4 | AI | 执行 `git worktree list` 确认 worktree 已被清理 |
| 5 | AI | 执行 `git branch` 确认 task 分支已被删除 |

### 测试 6：并发控制（原测试 7）

| 步骤 | 谁 | 做什么 |
|------|-----|--------|
| 1 | 人 | 一次性创建 10 个任务 |
| 2 | 人 | 观察同时 executing 的任务数量 |
| 3 | AI | 查 DB `SELECT COUNT(*) FROM instances WHERE status='running'`，确认不超过 MAX_CONCURRENT_INSTANCES |

### 测试 7：前端 UI 状态

| 步骤 | 谁 | 做什么 |
|------|-----|--------|
| 1 | 人 | 打开 Dashboard，截图统计栏 |
| 2 | AI | 查 `GET /api/system/stats` 对比统计数字是否一致 |
| 3 | 人 | 在 TaskForm 选择已有项目 → 确认正常 |
| 4 | 人 | 选 "+ New project" → 确认展开项目名称和 Remote URL 输入框 |
| 5 | 人 | 观察 TaskList 各状态颜色：pending 黄、executing 蓝闪、completed 绿、failed 红 |

### 测试 8：兼容性

| 步骤 | 谁 | 做什么 |
|------|-----|--------|
| 1 | 人 | 创建一个 Plan Mode 任务 → 确认进入 plan_review（紫色） |
| 2 | 人 | 点击 Approve → 确认任务重新入队执行 |
| 3 | 人 | 任务完成后点 Chat 按钮 → 发送追问消息 |
| 4 | AI | 查 DB 确认 task.session_id 存在，`--resume` 会被使用 |
| 5 | 人 | 测试语音按钮 → 确认录音转文字填入输入框 |

### AI 验证命令速查

测试时 Claude Code 常用的验证命令：

```bash
# 查任务状态
sqlite3 claude_manager.db "SELECT id, title, status, priority, project_id, instance_id, merge_status FROM tasks ORDER BY id"

# 查实例状态
sqlite3 claude_manager.db "SELECT id, name, status, current_task_id, pid FROM instances"

# 查项目状态
sqlite3 claude_manager.db "SELECT id, name, status, local_path FROM projects"

# 查调度器
curl -s -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8000/api/dispatcher/status | python -m json.tool

# 查 git 状态（在项目目录下）
git log --oneline -5
git worktree list
git branch

# 查后端日志（看 dispatcher 行为）
# 启动时加 --log-level debug 或查看终端输出
```

---

## PR Monitor 测试

### 手动测试: CRUD API

```bash
# 创建监控仓库
curl -X POST http://localhost:8000/api/pr-monitor/repos \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"repo_full_name": "test/repo", "auto_merge": true, "allowed_authors": ["user1"]}'

# 列出仓库
curl http://localhost:8000/api/pr-monitor/repos -H "Authorization: Bearer <token>"

# 切换 enabled
curl -X POST http://localhost:8000/api/pr-monitor/repos/1/toggle -H "Authorization: Bearer <token>"

# 删除
curl -X DELETE http://localhost:8000/api/pr-monitor/repos/1 -H "Authorization: Bearer <token>"
```

### 手动测试: Webhook（需要构造 HMAC 签名）

```bash
# 生成签名并发送模拟 webhook
SECRET="<webhook_secret>"
PAYLOAD='{"action":"opened","pull_request":{"number":1,"title":"Test PR","draft":false,"user":{"login":"user1"},"base":{"ref":"main"},"html_url":"https://github.com/test/repo/pull/1"},"repository":{"full_name":"test/repo"}}'
SIG=$(echo -n "$PAYLOAD" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print "sha256="$2}')

curl -X POST http://localhost:8000/api/github/webhook \
  -H "Content-Type: application/json" \
  -H "X-GitHub-Event: pull_request" \
  -H "X-Hub-Signature-256: $SIG" \
  -d "$PAYLOAD"
```

### 前端测试

1. 导航到 PR Monitor 页面
2. 添加仓库 → 验证表格显示
3. 点击仓库 → 验证详情页、Webhook 配置、复制按钮
4. 切换 enabled → 验证开关状态
5. 删除仓库 → 验证列表更新

## Task 状态同步（status_change 广播收口，2026-07-12）

### 自动化测试
```bash
# 复活块 orphan/autonomous 排除（completed 不被回放/后台事件翻回 executing）
uv run python -m pytest backend/tests/test_service_instance_manager.py -k reactivat -v
# cancel 广播 status_change
uv run python -m pytest backend/tests/test_api_tasks.py -k broadcasts_status_change -v
```

### 手动验证
1. 开两个浏览器页签（列表页 + 同一 task 的 chat 页），在列表页 cancel/retry 任务 → chat 页头部状态应立即变化（无需等 5s 轮询）
2. chat 打开 + 状态过滤 executing 时，让任务完成 → 侧栏状态点应实时变绿（不再永久冻结）
3. 断开 WS（devtools offline 几秒）错过 status_change → 恢复后 ≤5s 内 chat 页状态应被轮询数据纠正（不再永久陈旧）

## 前端主题系统 v2 测试（2026-07-13）

### 自动化
- `cd frontend && npx tsc --noEmit` — 类型检查
- `cd frontend && npm run build` — 构建（字体 woff2 应打进 dist/assets）
- `cd frontend && npx vitest run` — 组件测试（注意：main 上存在历史失败基线，对比失败数是否增加）
- `cd frontend && npx vitest run src/components/icons.test.tsx` — 中央图标模块：架构守卫（禁止值导入 lucide-react）、三主题实时切换、className 透传、fill 实心/空心语义、无映射回退
- `cd frontend && npx vitest run src/config/iconSets.test.tsx` — 主题图标集：声明的 iconSet 必已注册、每个集合覆盖全部导航 key 且渲染出 svg、缺失回退 Lucide、飞书 two-tone 双色取值
- `cd frontend && npx vitest run src/config/theme.test.ts` — 主题注册表 + index.css 变量覆盖完整性（gray/indigo 全档、浅色主题 color-scheme 与 accent 300/400 深色化、飞书/苹果官方 token 抽查、三浅色主题画布防趋同、苹果主题 skill 规则守卫——系统字体 / 按压反馈的 reduced-motion 守卫 / 材质顶栏的 reduced-transparency 回退）

### 手动验证（每个主题 × 桌面/移动端）
- [ ] 齿轮 → 主题下拉分「现代 / Legacy」两组：深色、浅色、飞书、苹果、经典深色、海蓝、森林、莓红
- [ ] 苹果主题：#f7f7f7 画布 + 侧栏 #f9f9f9（Settings 实测，略亮于画布）+ 纯白卡片大圆角（16px+）软阴影浮起；侧栏 = macOS Settings：顶部灰底 Search 框、账户行在搜索框下、彩色 squircle 图标、选中行实底蓝白字；主按钮 apple.com CTA 蓝 #0071e3（hover 微亮 #0077ed）；正文系统字体（Mac 上应为 SF Pro / 苹方）；顶栏毛玻璃（内容滚过时半透明模糊）；按钮按下轻微缩小（0.97）、系统开启「减弱动态效果」后无缩放
- [ ] 三浅色形状语言一眼可辨：feishu 卡片/按钮方正（≈6px）、light 默认（≈10px）、apple 明显更圆（≈16px）；截图角落放大或并排切换应立见差异
- [ ] 飞书主题桌面端：侧栏为 76px 窄图标 rail（头像置顶、图标上 10px 小字下、IconPark 双色图标（未选中深灰+白填充、选中飞书蓝）），选中项 = 白色圆角 tile 包住图标+文字 + 飞书蓝；移动端抽屉保持常规行布局
- [ ] 主题图标集即时切换：齿轮切主题后全站图标（侧栏、按钮、列表操作、聊天工具条等）立刻变（feishu→IconPark 双色 / apple→Ionicons 填充式 / 其余→Lucide），无需刷新页面
- [ ] 收藏星标实心/空心状态在三套图标下都正确（TaskForm/TaskList/ChatView 的 Star fill 切换）
- [ ] 苹果主题：侧栏每项一个 iOS 系统色 squircle 图标（白线稿彩底），选中行实底蓝 #0071e3 白字；按钮为胶囊形（导航项除外）；输入框 10px 圆角
- [ ] 飞书主题：白底为主（#fbfbfc 近白画布 + 纯白卡片，发丝线分隔）+ #ecedef 侧栏（飞书 rail 灰）；主按钮经典飞书蓝 #3370ff，hover 加深（#245bdb）；主文字 #1f2329；低边框风（#e8eaed，弱线框）；选中项浅蓝 pill #e1eaff + 蓝字
- [ ] 浅色 vs 飞书肉眼可区分：浅色 = 灰调分层（壳 oklch 92.5% / 画布 95.8%），飞书 = 大面积白（画布 #fbfbfc）；并排切换主画布白度应有明显差异
- [ ] 蓝色用户气泡内鼠标选中文字：高亮为白色半透明覆盖（亮一档的蓝），清晰可见；灰底消息选中仍是品牌蓝 tint（所有主题通用）
- [ ] Chat 发送「文字+图片」：发送瞬间气泡内立即显示图片缩略图；WS 回包不产生重复消息、也不吞图（去重合并附件）；刷新后图片仍在（历史 raw_json 回放）
- [ ] 手机 App（Capacitor）聊天图片/附件可正常加载（相对 /api/uploads/ URL 已拼上远程服务器地址，点击附件可打开）
- [ ] 「经典深色」外观 = v1 默认深色（Tailwind 原生 gray/indigo 色板）
- [ ] 浅色主题：白色卡片 + 浅灰画布；chip 文字（text-X-300/400）可读；无白字白底（text-white 只允许出现在彩色实底上）
- [ ] 桌面 lg+：左侧固定侧栏导航高亮正确；顶栏 sticky；Tasks 分屏（≥1280px）无纵向溢出（100vh-49px）
- [ ] 移动端：汉堡 → 抽屉滑出导航，点遮罩关闭；safe-area 顶部不遮挡
- [ ] 切主题后手机状态栏 / PWA theme-color 跟随（meta 同步）
- [ ] 刷新后主题保持（localStorage cc_theme）；旧值 ocean/forest/rose 直接沿用，无迁移丢失

## 聊天消息复制

| 测试文件 | 测试用例 | 说明 |
|----------|----------|------|
| `frontend/src/components/Chat/ChatView.test.tsx` | `copies a user message without its sender prefix` | 用户消息保留 `[发送者]` 的界面显示，但复制时只写入消息正文 |

## 开发规范

### Claude Code 开发时必须遵守：

1. **改代码前先跑测试**：`uv run python -m pytest backend/tests/ -v`，确认基线全绿
2. **改代码后再跑测试**：确认无回归，新增功能需要对应新增测试
3. **前端改动后检查类型**：`cd frontend && npx tsc --noEmit`
4. **新增 service/model/API 时**：在对应 test 文件中添加测试用例
5. **修 bug 时**：先写一个复现 bug 的测试（红），修复后确认测试变绿
6. **更新本文件**：新增测试后同步更新 TEST.md 的测试表格

### 测试文件对应关系

| 源文件 | 测试文件 |
|--------|---------|
| `backend/services/task_queue.py` | `backend/tests/test_task_queue.py` |
| `backend/services/stream_parser.py` | `backend/tests/test_stream_parser.py` |
| `backend/models/*.py` | `backend/tests/test_models.py` |
| `backend/api/tasks.py` | `backend/tests/test_api_tasks.py` |
| `backend/api/chat.py` + `backend/api/tasks.py` (plan) | `backend/tests/test_api_chat_plan.py` |
| `backend/api/system.py` | `backend/tests/test_api_system.py` |
| `backend/api/auth.py` | `backend/tests/test_api_auth.py` |
| `backend/api/projects.py` | `backend/tests/test_api_projects.py` |
| `backend/api/instances.py` | `backend/tests/test_api_instances.py` |
| `backend/services/dispatcher.py` | `backend/tests/test_service_dispatcher.py` |
| `backend/services/worktree_manager.py` | `backend/tests/test_service_worktree_manager.py` |
| `backend/services/instance_manager.py` | `backend/tests/test_service_instance_manager.py` |
| `backend/services/context_compaction.py` | `backend/tests/test_context_compaction.py` |
| `backend/services/ralph_loop.py` | `backend/tests/test_service_ralph_loop.py` |
| `backend/services/ws_broadcaster.py` | `backend/tests/test_service_ws_broadcaster.py` |
| `backend/services/whisper_client.py` | `backend/tests/test_service_whisper_client.py` |
| `backend/services/backup_service.py` | `backend/tests/test_service_backup.py` |
| `backend/services/tmp_space_manager.py` | `backend/tests/test_tmp_space_manager.py` |
| `backend/services/container_manager.py`（容器 `/tmp`） | `backend/tests/test_container_manager.py` |
| `backend/api/files.py`（SSH 下载临时文件） | `backend/tests/test_api_files.py` |
| `backend/services/token_manager_service.py` | `backend/tests/test_service_token_manager.py` |
| `backend/schemas/task.py` (datetime serialization) | `backend/tests/test_task_schema.py` |
| `backend/api/chat.py` (timestamp Z suffix) | `backend/tests/test_chat_timestamp.py` |
| `frontend/src/config/timezone.ts` | `frontend/src/config/timezone.test.ts` |
| `backend/mcp/ccm_skills_server.py` | `backend/tests/test_mcp_server.py` |
| `backend/models/monitor_session.py` | `backend/tests/test_monitor_models.py` |
| `backend/services/mcp_config.py` | `backend/tests/test_mcp_config.py` |
| `backend/api/monitor.py` | `backend/tests/test_api_monitor.py` |
| `backend/api/settings.py` (runtime) | `backend/tests/test_api_settings_runtime.py`（含 context_compact_threshold 默认/更新/越界拒绝） |
| `backend/services/dispatcher.py` (monitor) | `backend/tests/test_monitor_dispatcher.py` |
| `backend/mcp/ccm_monitor_agent_server.py` | 集成测试（见「子 Agent 系统集成测试」） |
| `backend/api/sub_agents.py` | 集成测试（见「子 Agent 系统集成测试」） |
| `backend/models/pr_monitor.py` | Migration 验证（表创建） |
| `backend/api/pr_monitor.py` | curl 测试 CRUD + webhook |
| `backend/services/pr_review_service.py` | 集成测试（webhook → task 创建） |
| `frontend/src/pages/PRMonitorPage.tsx` | TypeScript 类型检查 + 手动 UI 测试 |
| `frontend/src/**` | TypeScript 类型检查 (`tsc --noEmit`) |

## 分布式 Worker 测试

### 单元/集成测试

```bash
uv run python -m pytest backend/tests/test_api_workers.py -v
```

覆盖：API 状态守卫（409/503/404）、双击防护（同步置过渡态）、provisioner 状态机
（收养/创建/stop/start/destroy/retry，cloud+SSH 全替身）、健康检查降级与自动恢复
（bootstrap 失败不被洗白）、.deploy_commit 版本回退。

### 真机冒烟（收养一台已有 EC2 跑完整 bootstrap）

```bash
WORKER_ENABLED=true WORKER_SSH_KEY_PATH=~/.ssh/xxx.pem PYTHONPATH=. \
  .venv/bin/python scripts/worker_phase1_smoke.py --adopt i-xxxxxxxx
```

预期：status=ready，health 返回非空 commit 且与 DB ccm_commit 一致（版本锁定 PASS）。
注意：经 PTY bridge 跑长任务用 `setsid nohup ... > /tmp/x.log &`，且命令里别带
`rm`/`mv`（权限 ask 列表会触发 bridge auto-deny）。

### Phase 2 端到端（已验证 2026-06-12，task 58）

manager(8003) 注册 worker → 建 git_url 项目 → 创建 task 选 worker → 验证：
转发同 ID、状态回流、43 条日志镜像、README 真实修改 + merge push、
chat 代理 + session_id 同步、回复经 relay 回流。测试仓库
github.com/youchengsong/ccm-worker-e2e-test（可删）。
