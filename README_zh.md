# AnyGen AI Skills

> 适用于 [OpenClaw](https://github.com/openclaw/openclaw) / Claude Code / Cursor 的 AI 技能包

## 包含技能

### 📊 Task Creator — 通用内容生成
使用 AnyGen API 生成 PPT、文档、网站、故事板、数据分析。

### 📈 Finance Report — 投研PDF报告
专业股票研究报告：财报分析、赛道扫描、估值分析、首次覆盖。

## 安装

```bash
# OpenClaw
git clone https://github.com/AnyGenIO/anygen-skills.git ~/.openclaw/skills/anygen

# Claude Code
git clone https://github.com/AnyGenIO/anygen-skills.git ~/.claude/skills/anygen
```

## 配置

```bash
# AnyGen API Key（Task Creator 需要）
python3 task-creator/scripts/anygen.py config set api_key "sk-xxx"

# 或环境变量
export ANYGEN_API_KEY="sk-xxx"
```

API Key 获取：[anygen.io](https://www.anygen.io) → Setting → Integration

## 使用

```
# 内容生成
"做个关于AI趋势的PPT"
"生成一份Q1总结文档"

# 投研报告
"分析英伟达财报"
"AI半导体赛道扫描"
"出一份AVGO的深度覆盖报告"
```
