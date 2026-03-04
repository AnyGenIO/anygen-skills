---
name: anygen
description: "AnyGen AI Skills — 内容生成(PPT/文档/网站/数据分析) + 投研PDF报告。"
---

# AnyGen AI Skills

AnyGen 技能包，包含多个子技能。根据用户意图自动路由。

## 子技能

### 1. Task Creator — 通用内容生成
生成PPT、文档、网站、数据分析、故事板等。

**触发词:** "做个PPT"、"生成文档"、"建个网站"、"数据分析"
**详见:** `task-creator/skill.md`

### 2. Finance Report — 投研PDF报告
深度股票分析、财报分析、赛道扫描，生成专业PDF投研报告。

**触发词:** "分析XX"、"XX财报分析"、"赛道扫描"、"出报告"
**详见:** `finance-report/skill.md`

## 路由规则

| 用户意图 | 路由到 |
|---------|--------|
| PPT/文档/网站/故事板/数据分析 | `task-creator/skill.md` |
| 股票分析/财报/估值/赛道扫描 | `finance-report/skill.md` |

## 前置条件

- Python3
- AnyGen API Key（配置方式见 `task-creator/skill.md`）
- 投研报告需要 fin_* 系列数据工具
