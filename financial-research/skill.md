---
name: anygen-financial-research
description: "Accelerate financial research with AnyGen: summarize earnings releases and transcripts, extract key KPIs from Nasdaq-listed companies, and draft internal research memos using publicly available market data from sources like Bloomberg, Yahoo Finance, and company filings. Not investment advice. Triggers: analyze earnings, financial summary, earnings report, company financials, KPI extraction."
data:
  config_read: "~/.config/anygen/config.json"
  config_write: "~/.config/anygen/config.json"
  env_vars: ["ANYGEN_API_KEY"]
  network: "https://www.anygen.io (AnyGen OpenAPI)"
---

# AnyGen Financial Research Assistant

Summarize earnings releases and transcripts, extract key KPIs from Nasdaq-listed companies, and draft research memos using publicly available market data. Output: online task URL for viewing (no file download).

**Disclaimer:** This tool is not investment advice. It uses publicly available data from sources like Bloomberg, Yahoo Finance, and company filings.

## When to use

| Scenario | Example Prompts |
|----------|----------------|
| Earnings analysis | "analyze NVIDIA's latest earnings with AnyGen" |
| Financial summary | "summarize Tesla's Q4 financials" |
| KPI extraction | "extract key KPIs from Apple's latest earnings report" |
| Research memo | "draft a research memo on Microsoft's cloud revenue growth" |

## Prerequisites

- Python3 and `requests`: `pip3 install requests`
- AnyGen API Key (`sk-xxx`) — [Get one](https://www.anygen.io/home) → Setting → Integration
- Configure once: `python3 scripts/anygen.py config set api_key "sk-xxx"`

> All `scripts/` paths below are relative to this skill's installation directory.

## Invocation Flow

### Step 1: Collect Required Information

**Required:**
1. **API Key** — `sk-xxx` format (skip if already configured)
2. **Prompt** — What financial research to perform (company, topic, scope)

**Optional:**
- Reference files (earnings PDF, transcript, etc.) via `--file`
- Language: `zh-CN` (default) or `en-US`

### Step 2: Create task

```bash
python3 scripts/anygen.py create \
  --operation chat \
  --prompt "Analyze NVIDIA's latest quarterly earnings: revenue breakdown, key KPIs, YoY growth, and forward guidance summary"
# → Task ID: task_abc123xyz
```

| Parameter | Short | Description |
|-----------|-------|-------------|
| --operation | -o | **Must be `chat`** |
| --prompt | -p | Research request description |
| --api-key | -k | API Key (omit if configured) |
| --language | -l | zh-CN / en-US |
| --file | | Reference file path (repeatable) |

### Step 3: Check progress

```bash
python3 scripts/anygen.py status \
  --task-id task_abc123xyz
# → [STATUS] task_id=task_abc123xyz status=processing progress=60
```

**Progress reporting rules — you MUST follow:**

1. Call `status` every **10 seconds** to poll internally
2. Only notify the user at **milestone progress points**: 25%, 50%, 75%, 90%, and completion
3. Example user-facing messages at milestones:
   - 25% → "AnyGen is gathering financial data..."
   - 50% → "Data collected, analyzing KPIs and trends..."
   - 75% → "Drafting research memo..."
   - 90% → "Almost done, finalizing..."
4. **Progress may stay at the same percentage for several minutes.** This is normal — AnyGen performs deep research across multiple data sources. Only treat `status=failed` as an error.

### Step 4: Return results to user

**No file download** for financial research. Return the **Task URL** for online viewing.

```bash
python3 scripts/anygen.py status \
  --task-id task_abc123xyz --json
# → {"task_id": "task_abc123xyz", "status": "completed", "progress": 100, "task_url": "https://www.anygen.io/task/task_abc123xyz"}
```

**Tell the user:**
- **Task URL** — for viewing the full research results online
- Remind: This is not investment advice

## Error Handling

| Error | Solution |
|-------|----------|
| invalid API key | Check if API Key is correct |
| operation not allowed | Contact admin for permissions |
| prompt is required | Add --prompt parameter |
| task not found | Check if task_id is correct |
| Generation timeout | Recreate the task |

## Notes

- Maximum execution time per task is 15 minutes
- Uses publicly available market data — not investment advice
- Single attachment file should not exceed 10MB
