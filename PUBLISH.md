# Publish Guide

## Quick Reference

```bash
node publish.mjs scan                      # 安全扫描（全部）
node publish.mjs scan --static             # 仅静态扫描
node publish.mjs deploy                    # 部署到本地 OpenClaw
node publish.mjs deploy --target claude    # 部署到本地 Claude Code
node publish.mjs publish                   # 发布全部到 ClawHub（自动 patch+1）
node publish.mjs publish slide-generator   # 发布单个
node publish.mjs run                       # 完整流程：扫描 → 部署 → 发布
node publish.mjs list                      # 列出所有 skill
```

## Skill Registry

| 源目录 | ClawHub Slug |
|--------|-------------|
| `data-analysis` | `anygen-data-analysis` |
| `deep-research` | `anygen-deep-research` |
| `diagram-generator` | `anygen-diagram-generator` |
| `doc-generator` | `anygen-doc-generator` |
| `financial-research` | `anygen-financial-research` |
| `image-generator` | `anygen-image-generator` |
| `slide-generator` | `anygen-slide-generator` |
| `storybook-generator` | `anygen-storybook-generator` |
| `website-generator` | `anygen-website-generator` |

> 版本号无需手动维护，publish 时自动从 ClawHub 拉取最新版本并 patch+1。

## 发布流程

```
修改代码 → 安全扫描 → 本地部署测试 → 提交 PR → CI 检查 → 发布
```

### 1. 安全扫描

发布前必须通过安全扫描。分两层：静态正则扫描和 LLM 语义评估。

```bash
# 静态扫描（无需 API Key）
node publish.mjs scan --static

# 完整扫描（需要 OPENAI_API_KEY）
OPENAI_API_KEY=sk-xxx node publish.mjs scan

# 只扫描指定 skill
node publish.mjs scan slide-generator data-analysis
```

判定标准：

| Verdict | 含义 | 能否发布 |
|---------|------|----------|
| benign | 能力与用途匹配 | 可以 |
| suspicious | 存在不一致，需人工审查 | 审查后可以 |
| malicious | 根本性不一致 | 不可以 |

### 2. 本地部署测试

```bash
# 部署到 OpenClaw（默认）
node publish.mjs deploy

# 部署到 Claude Code
node publish.mjs deploy --target claude

# 两者都部署
node publish.mjs deploy --target all

# 只部署单个
node publish.mjs deploy slide-generator
```

部署后重启 agent 验证：skill 加载、关键词触发、核心流程、输出格式。

### 3. 提交 PR

PR 提交后 CI 自动运行安全扫描（`.github/workflows/security-scan.yml`）：

- 静态扫描 → LLM 评估 → PR 评论报告 → malicious 时阻止合并

手动触发：

```bash
gh workflow run security-scan.yml
gh workflow run security-scan.yml -f skill=slide-generator
gh workflow run security-scan.yml -f static_only=true
```

### 4. 发布到 ClawHub

PR 合并后发布。版本号自动从 ClawHub 拉取当前最新版并 patch+1。

```bash
# 发布全部（推荐）
node publish.mjs publish

# 发布指定 skill
node publish.mjs publish slide-generator

# 使用 API 直接发布（绕过 CLI acceptLicenseTerms 问题）
node publish.mjs publish --method api
```

执行后会展示版本计划和完整命令，确认后才执行：

```
  Skill                     Current    Next
  ───────────────────────── ────────── ──────────
  slide-generator           1.5.2      1.5.3
  data-analysis             1.2.3      1.2.4

Commands:
  clawhub publish ".../slide-generator" --slug "anygen-slide-generator" --version "1.5.3"
  clawhub publish ".../data-analysis" --slug "anygen-data-analysis" --version "1.2.4"

  Continue? [y/N]
```

发布后验证：`clawhub inspect <slug>`

### 5. 完整流水线（一键）

```bash
node publish.mjs run
node publish.mjs run --target all                 # 部署到 OpenClaw + Claude Code
node publish.mjs run --method api                  # 使用 API 发布
node publish.mjs run --target all --method api     # 组合使用
```

三步串行执行，每步之间有确认提示：扫描 → 部署 → 发布。

## 其他平台

### skills.sh

公开 GitHub 仓库 + 合法 `SKILL.md` 即自动索引，无需额外操作。

```bash
# 用户安装
npx skills add AnyGenIO/anygen-skills
npx skills add AnyGenIO/anygen-skills --skill anygen-slide-generator
npx skills add AnyGenIO/anygen-skills --list
```

## 环境变量

| 变量 | 用途 |
|------|------|
| `OPENAI_API_KEY` | LLM 安全评估（可选） |
| `OPENAI_EVAL_MODEL` | 评估模型（默认 `gpt-5-mini`） |

## 前置依赖

```bash
npm i -g clawhub
clawhub login
clawhub whoami    # 验证登录
```
