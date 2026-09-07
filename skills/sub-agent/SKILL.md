---
name: sub-agent
description: >
  创建后台子 Agent 执行一次性任务（调研、代码审查、依赖分析等）。
  Sub-Agent 独立运行，实时上报进度，完成后将结果注入主 session。
when_to_use: >
  用户需要并行执行独立子任务、做调研分析、代码审查、
  或者需要将任务拆分给后台 Agent 执行时激活。
disallowed-tools:
  - Agent
  - Task
  - TaskCreate
  - TaskGet
  - TaskList
  - TaskOutput
  - TaskStop
  - TaskUpdate
  - TeamCreate
  - TeamDelete
  - SendMessage

ccm:
  always: false
  priority: 7
  version: 1
  tags: [sub-agent, parallel, delegation]
  tools: [create_sub_agent, check_sub_agents, stop_sub_agent]
  commands:
    - name: sub-agent
      pattern: "$sub-agent {task_description}"
      description: "创建后台子 agent 执行一次性任务"
---

## Sub-Agent 规则

你拥有后台 Sub-Agent 系统（通过 ccm-skills MCP 工具）。

### 必须遵守

1. **使用 `create_sub_agent` 工具**将一次性任务委托给子 Agent
2. **禁止**使用内置的 Agent 工具或 Task 工具——这些内置工具不在 CCM 系统的管理范围内，无法被追踪和记录
3. 所有子任务必须通过 `create_sub_agent` 工具发起，由 CCM 子 agent 系统统一管理
4. 每个 task 最多同时运行 3 个 Sub-Agent
5. 子 Agent 自动使用主任务的 provider：Claude Code 主任务启动 Claude 子进程，
   Codex 主任务启动独立 Codex thread；工具名称与工作流保持一致

### 可用工具

| 工具 | 说明 |
|------|------|
| `create_sub_agent` | 创建子 Agent，指定任务描述和上下文 |
| `check_sub_agents` | 查看所有 Sub-Agent 的状态和进度 |
| `stop_sub_agent` | 停止指定的 Sub-Agent |

### 工作流程

1. 用户描述需要并行执行的子任务
2. 调用 `create_sub_agent` 创建子 Agent，提供清晰的任务描述和上下文
3. 子 Agent 会独立运行并实时上报进度
4. 你可以继续其他工作，随时通过 `check_sub_agents` 查看进展
5. 子 Agent 完成后会自动将结果注入到你的 session
6. 如需提前停止，调用 `stop_sub_agent`

### 与 Monitor 的区别

- **Monitor**: 持续后台监控（编译、测试、日志），长期运行
- **Sub-Agent**: 一次性任务执行（调研、审查、分析），有明确结束点

## Lessons Learned
<!-- 自进化系统自动追加 -->
