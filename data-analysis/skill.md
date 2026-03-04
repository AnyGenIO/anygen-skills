---
name: anygen-data-analysis
description: "Analyze CSV data with AnyGen: clean tables, summaries, and insights. Generate charts and a written explanation for reporting workflows. Triggers: analyze data, analyze CSV, data table, organize data, data summary, chart from data."
---

# AnyGen Data Analysis (CSV)

Analyze CSV data with AnyGen: generate clean tables, summaries, charts, and insights. Output: online task URL for interactive viewing (no file download).

## When to use

| Scenario | Example Prompts |
|----------|----------------|
| CSV analysis | "analyze this CSV and create a summary table" |
| Data organization | "organize this data into a table" |
| Chart generation | "create charts from this sales data" |
| Data summary | "summarize the key trends in this dataset" |

## Prerequisites

- Python3 and `requests`: `pip3 install requests`
- AnyGen API Key (`sk-xxx`) — [Get one](https://www.anygen.io/home) → Setting → Integration
- Configure once: `python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py config set api_key "sk-xxx"`

## Invocation Flow

### Step 1: Collect Required Information

**Required:**
1. **API Key** — `sk-xxx` format (skip if already configured)
2. **Prompt** — What analysis to perform
3. **Data file** — CSV file path via `--file` (highly recommended)

**Optional:**
- Style preference via `--style`
- Language: `zh-CN` (default) or `en-US`

### Step 2: Create task

```bash
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py create \
  --operation data_analysis \
  --prompt "Analyze the sales trends and create a monthly summary with charts" \
  --file ./data/sales_2024.csv
# → Task ID: task_abc123xyz
```

| Parameter | Short | Description |
|-----------|-------|-------------|
| --operation | -o | **Must be `data_analysis`** |
| --prompt | -p | Analysis description |
| --file | | CSV or data file path (repeatable) |
| --api-key | -k | API Key (omit if configured) |
| --style | -s | Style preference |
| --language | -l | zh-CN / en-US |

### Step 3: Check progress

```bash
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py status \
  --task-id task_abc123xyz
# → [STATUS] task_id=task_abc123xyz status=processing progress=60
```

**Progress reporting rules — you MUST follow:**

1. Call `status` every **10 seconds** to poll internally
2. Only notify the user at **milestone progress points**: 25%, 50%, 75%, 90%, and completion
3. Example user-facing messages at milestones:
   - 25% → "AnyGen is parsing your data..."
   - 50% → "Data analyzed, generating charts..."
   - 75% → "Creating summary and insights..."
   - 90% → "Almost done, finalizing..."
4. **Progress may stay at the same percentage for several minutes.** This is normal. Only treat `status=failed` as an error.

### Step 4: Return results to user

**No file download** for data analysis. Return the **Task URL** from the status output for online interactive viewing.

```bash
# Use --json to get structured output with task_url:
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py status \
  --task-id task_abc123xyz --json
# → {"task_id": "task_abc123xyz", "status": "completed", "progress": 100, "task_url": "https://www.anygen.io/task/task_abc123xyz"}
```

**Tell the user:**
- **Task URL** — for online viewing of charts, tables, and analysis results

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
- Single attachment file should not exceed 10MB
- Results are viewable online at the task URL
