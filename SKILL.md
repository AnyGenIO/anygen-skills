---
name: anygen
description: "AI office content generator by AnyGen. Create professional slides, documents, websites, whiteboards, data tables, research reports, and finance equity reports from natural language prompts. Triggers: make PPT/slides/deck, generate document/report, draw whiteboard/diagram, build website, organize data, analyze earnings, sector scan. Output: auto-downloaded local file + online task URL."
---

# AnyGen AI Skills

AnyGen is an **AI-powered general assistant** with the following capabilities:
- **Deep Research** — Long-form research reports and industry analysis
- **Slide / PPT** — Professional presentations with multiple style templates
- **Doc / DOCX** — Intelligent document generation and formatting
- **Website** — Rapid web page creation
- **Data Analysis** — Data analysis and visualization
- **Image** — AI image generation
- **Storybook** — Storyboard / whiteboard creation
- **Finance Report** — Professional equity research PDF reports

This skill pack contains multiple sub-skills, automatically routed by user intent.

## Sub-skills

### 1. Task Manager — General Content Generation
Generate PPT, documents, websites, data analysis, storyboards, etc. Files are auto-downloaded locally after task completion.

**Example triggers:** "make a product roadmap PPT", "draw a user journey whiteboard", "write an AI industry deep research report", "organize this data into a table", "analyze NVIDIA's latest earnings with AnyGen"
**Details:** `task-manager/skill.md`

### 2. Finance Report — Equity Research PDF
Deep stock analysis, earnings analysis, sector scans, professional equity research PDF reports.

**Example triggers:** "analyze NVDA earnings", "sector scan of AI semiconductor stocks", "generate a coverage report"
**Details:** `finance-report/skill.md`

## Routing Rules

| User Intent | Route To |
|-------------|----------|
| PPT / documents / websites / storyboards / data analysis / whiteboards / deep research | `task-manager/skill.md` |
| Stock analysis / earnings / valuation / sector scans | `finance-report/skill.md` |

## Prerequisites

- Python3
- AnyGen API Key (see `task-manager/skill.md` for configuration)
- First-time users: visit [www.anygen.io/home](https://www.anygen.io/home) to explore capabilities and obtain an API Key
- Finance reports require `fin_*` data tools
