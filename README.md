# AnyGen AI Skills

> AI Skills for [OpenClaw](https://github.com/openclaw/openclaw) / Claude Code / Cursor

A collection of AI-powered content generation skills using AnyGen and financial data tools.

## Skills Included

### 📊 Task Manager
Generate various content using AnyGen API:
- **Slides** (PPT) — Professional presentations
- **Documents** — Reports, papers, documentation
- **Websites** — Landing pages, portfolios
- **Storybooks** — Visual narratives
- **Data Analysis** — Charts and insights

### 📈 Finance Report
Professional equity research PDF reports:
- **Earnings Analysis** — Post-earnings deep dives
- **Earnings Preview** — Pre-earnings scenario modeling
- **Sector Scan** — Cross-sector comparisons
- **Initiating Coverage** — Full stock deep dives
- **Valuation Analysis** — DCF, comps, multiples

## Installation

### OpenClaw
```bash
# Clone to skills directory
git clone https://github.com/AnyGenIO/anygen-skills.git ~/.openclaw/skills/anygen
```

### Claude Code
```bash
git clone https://github.com/AnyGenIO/anygen-skills.git ~/.claude/skills/anygen
```

## Configuration

### AnyGen API Key (required for Task Manager)

```bash
# Option 1: Config file
python3 task-manager/scripts/anygen.py config set api_key "sk-xxx"

# Option 2: Environment variable
export ANYGEN_API_KEY="sk-xxx"
```

Get your API key at [anygen.io/home](https://www.anygen.io/home) → Setting → Integration.

### Finance Report

Requires `fin_*` data tools (built into OpenClaw). No additional configuration needed.

## Usage

```
# Task Manager
"Make a product roadmap PPT"
"Draw a user journey whiteboard"
"Write an AI industry deep research report"
"Organize this data into a table"
"Make a quarterly review slide deck"

# Finance Report  
"Analyze NVDA earnings"
"Do a sector scan of AI semiconductor stocks"
"Give me a full coverage report on AVGO"
```

## Structure

```
anygen/
├── SKILL.md                    # Skill router
├── task-manager/               # AnyGen content generation
│   ├── skill.md
│   └── scripts/anygen.py
└── finance-report/             # Equity research PDF reports
    ├── skill.md
    ├── config/output.yaml
    ├── templates/report-style.css
    ├── workflows/
    │   ├── pdf-output.md
    │   ├── earnings-analysis.md
    │   ├── earnings-preview.md
    │   └── sector-scan.md
    └── references/
        ├── initiating-coverage.md
        ├── competitive-analysis.md
        ├── valuation-methodologies.md
        ├── anti-bias.md
        ├── evidence-hierarchy.md
        └── variant-view.md
```

## License

MIT
