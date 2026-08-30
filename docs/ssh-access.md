# SSH 工作台与 Task 授权

CCM 只有一套 Manager 托管的 SSH Profile：Files 页负责连接配置和人工文件浏览；同一 Profile 可选择是否向 Task 暴露有限能力。私钥始终由 Manager 后端使用，不会传给浏览器、Task prompt 或 MCP 子进程。

## 管理员使用流程

1. 打开 **Files → SSH workspace → Add profile**。
2. 填写远端地址和用户名，然后上传未加密的 PEM/OpenSSH 私钥。新建、legacy 迁移和私钥轮换都只接受上传，不接受任意 Manager 路径。
3. 配置一个或多个 `Allowed remote roots`（默认 `/`）；它们限制 Files 和 Task 文件操作，但不限制远程命令。
4. 探测主机公钥，通过可信渠道核对 SHA-256 指纹后勾选确认并保存。
5. 用 **Test connection** 验证连接；随后可在同页浏览和下载远程文件。
6. 新建 Task 时打开 **SSH access**，选择 Profile，并只勾选需要的 `Run commands`、`Read files`、`Write files`。
7. 已有 Task 可在 Chat 顶栏的 `SSH n` 中修改授权；编辑后必须显式点 **Save SSH grants**。

普通成员只能查看已有授权。Worker、Shared Task 和 Worker 管理副本不允许使用 Manager 本机密钥。

## 授权与失效规则

- `exec`：执行有超时和输出上限的非交互命令，不支持交互 shell 或密码提示。
- `read`：列目录和读取有大小上限的 UTF-8 文本；Files 工作台的下载能力不等于 Task 下载权限。
- `write`：写入最多 1 MB 的 UTF-8 文本；默认拒绝覆盖已有文件，调用方必须显式要求覆盖。
- Allowed roots 必须是绝对 POSIX 路径。后端通过 SFTP canonical path 校验现有目标或新文件的 canonical parent，拒绝 `..` 和符号链接逃逸。`exec` 是远程用户级任意非交互命令，不能用 Allowed roots 做路径隔离。
- 每条 Task grant 固定保存授权时的 Profile revision。host、port、username、私钥、固定 host key、Task 暴露策略或 Allowed roots 变化，或 Profile 被禁用/删除时，旧授权立即失效；管理员需要在 Chat 中核对后重新保存。
- Team Task/Project 分享和跨 CCM outbound Task/Project 分享都与 SSH grant 互斥：分享前必须移除相关 grant，已分享作用域不能新增 grant；运行时仍会独立复核并 fail closed。
- `ccm_ssh` 只暴露有效 grant 所需的工具，并在每次后端操作前重新校验 Task、capability、revision、Profile 状态和 host key。
- 升级前已有的外部路径 Profile 可保持 Files-only 并继续人工浏览，但不能开启 Task access。必须先上传轮换到 CCM 托管目录；Profile 更新、grant admission、每次 broker 操作和 turn 启动都会独立复核，手工修改数据库也不能绕过。
- SFTP 使用全局有界并发、排队/操作/channel 超时、目录最多 2,000 项，以及读取/写入/下载大小上限。超时的 Paramiko 线程在真实退出前继续占用槽位，防止重复超时绕过并发上限。

## 凭据边界

- Managed SSH 必须配置非空 `AUTH_TOKEN`。未设置时，Profile/托管私钥/Files SSH/Task grant/内部 broker 端点统一返回 503，且运行时不会向 Agent 暴露 SSH capability；其他非 SSH API 仍保留历史开放模式。
- SSH Profile 数据库只保存私钥路径和公钥指纹，不保存私钥内容；公开 Profile API 只返回脱敏后的路径提示。
- 浏览器上传最多 1 MB，后端先验证私钥格式，再以一次性令牌认领到 `SSH_KEY_STORAGE_DIR`（默认 `~/.ccm/ssh-keys`）；目录为 `0700`、文件为 `0600`。取消会删除待认领文件，保存失败会回滚已认领副本并允许原令牌重试；未认领令牌有效期为 24 小时，Profile 轮换或删除会清理不再引用的 CCM 托管密钥。
- 私钥必须是 Manager 服务用户拥有的普通文件，不得是符号链接或 group/other 可读写文件；加密私钥暂不支持无人值守 Task。
- 每个 MCP/AskUser 子进程只获得按 audience、Task/session、HTTP method/path 签名的短期内部凭据；所有含 `task_id` 的 scoped credential（Skills、AskUser、SSH、Monitor、Sub-Agent）都绑定不可复用的 Task incarnation。Middleware 会先查库拒绝旧 incarnation，route 仍须在自己的事务中把 claim 与已加载的 Task/session owner 再比对，避免 Manager 重启、整数 Task id 复用或 middleware→endpoint 竞态恢复旧权限。部署管理员 token 不进入模型、argv 或 MCP 配置。同一 Task 的等价 scope 可供 Claude PTY 热会话复用，到期或运行策略变化时会强制冷恢复；Task 删除时立即吊销。MCP 配置与 Claude exact settings 位于 `TASK_RUNTIME_SECRET_DIR`（默认 `~/.ccm/task-runtime-secrets`）的 `0700/0600` 私有目录，并在每轮结束后清理。
- 所有本地普通 Task（不只 SSH Task）都会屏蔽 Manager 的 SSH/Profile key、Provider home、账号池、私有运行目录、`.env` 和本地 SQLite 文件；继承的 SSH agent 坐标会被清空。Claude 使用 exact settings + OS sandbox，Linux Manager/Worker 必须安装 `bubblewrap` 与 `socat`，双零回合预检会在 5 秒总预算内证明 settings 已加载且没有模型 turn；Codex 使用 app-server request-local permission profile。任一隔离无法证明时任务在模型输入前失败。
- 只要 Task 留有任何 durable SSH grant row（包括 stale/disabled grant），Task 以及它启动的 Monitor/Sub-Agent 就关闭直接网络并进入 broker-only；只有当前有效 grant 才会暴露对应 `ccm_ssh` 工具。grant Task 的 Git 环境只保留 commit identity 与 fail-closed non-interactive 配置，不注入 SSH key、askpass 或 GitHub token。普通无 grant Task 仍可使用其明确选择的 Project/global Git 凭据，但其他 Profile key、托管根和 provider 私有目录继续被沙箱 deny；子 Agent 不继承父 Task 的私钥或管理员凭据。
- Codex Agent 永不获得 direct network。普通无 durable SSH grant 的本地 Task 仅在当前 app-server process 的 `initialize` 结果能精确证明 Codex `>=0.146.0` 时，才通过 managed public proxy 访问公网；旧版或版本/进程代次无法证明时会在模型输入前自动降级为 network-off，Task 仍可离线执行。Task shell 使用 `shell_environment_policy.inherit=none`，只显式恢复安全变量。
- Managed proxy 的真实边界必须同时证明：公网 HTTPS 经代理成功，而 direct public TCP、host loopback/private destination、pathname/abstract AF_UNIX、UDP、SOCKS 和部署级 upstream proxy 全部失败；sandbox 内的 loopback bind/self-connect 只存在于隔离 netns，不暴露给宿主。带 grant 的 Task 及其 Monitor/Sub-Agent、Delivery Developer、Plan Agent 与 tool-free turn 始终 network-off，不使用该代理。
- Files 页折叠区中的 legacy 用户名/密码连接仅保存在浏览器，用于人工文件浏览，不能授权给 Task。
- Secrets 用于把普通密钥值以环境变量注入 Task，与 SSH Profile 无关；不要把 SSH 私钥内容放进 Secrets。
- 主机公钥探测只提供待核对证据，不替代人工通过可信渠道验证指纹。

## API 与执行链路

- 管理 Profile：`/api/ssh-profiles`
- 上传/取消待认领私钥：`POST /api/ssh-profiles/upload-key`、`DELETE /api/ssh-profiles/upload-key/{token}`
- 查看/替换 Task grant：`/api/tasks/{task_id}/ssh-grants`
- MCP 内部操作：`/api/tasks/{task_id}/ssh-access/...`，只接受 Manager 内部服务认证

有有效授权时，Manager 会为 Claude 和 Codex 的普通本地 Task 注入 required `ccm_ssh` MCP。该能力独立于 Codex 主 MCP 开关；如果 required MCP 或 provider sandbox 无法得到证明，任务必须在执行前失败，不能回退到无隔离的执行路径。PR Review、Worker、Shared Task 等隔离执行链路不会继承 Task SSH 授权。
