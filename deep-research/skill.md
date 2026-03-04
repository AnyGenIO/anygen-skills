---
name: anygen-deep-research
description: "Generate long-form research reports with AnyGen: market overview, trends, competitors, and synthesis. Works for strategy, industry, and product research. Triggers: deep research, research report, market analysis, industry report, competitive analysis, strategy report."
---

# Deep Research Report Generator - AnyGen

Generate long-form research reports covering market overview, trends, competitors, and synthesis. Works for strategy, industry, and product research. Output: online task URL for viewing (no file download).

## When to use

| Scenario | Example Prompts |
|----------|----------------|
| Industry research | "write an AI industry deep research report" |
| Competitive analysis | "write a competitive analysis report on EV market" |
| Market overview | "research the cloud computing market landscape in 2025" |
| Strategy report | "create a market entry strategy report for Southeast Asia" |

## Prerequisites

- Python3 and `requests`: `pip3 install requests`
- AnyGen API Key (`sk-xxx`) — [Get one](https://www.anygen.io/home) → Setting → Integration
- Configure once: `python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py config set api_key "sk-xxx"`

## Invocation Flow

### Step 1: Collect Required Information

**Required:**
1. **API Key** — `sk-xxx` format (skip if already configured)
2. **Prompt** — Research topic with scope (industry, region, timeframe, focus areas)

**Optional:**
- Reference files (existing reports, data) via `--file`
- Language: `zh-CN` (default) or `en-US`

### Step 2: Create task

```bash
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py create \
  --operation chat \
  --prompt "Write a deep research report on the global AI chip market: market size, key players (NVIDIA, AMD, Intel, custom silicon), trends, and 3-year outlook"
# → Task ID: task_abc123xyz
```

| Parameter | Short | Description |
|-----------|-------|-------------|
| --operation | -o | **Must be `chat`** |
| --prompt | -p | Research topic and scope |
| --api-key | -k | API Key (omit if configured) |
| --language | -l | zh-CN / en-US |
| --file | | Reference file path (repeatable) |

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
   - 25% → "AnyGen is researching and collecting data..."
   - 50% → "Data gathered, analyzing trends and patterns..."
   - 75% → "Synthesizing findings into report..."
   - 90% → "Almost done, finalizing..."
4. **Progress may stay at the same percentage for several minutes.** This is normal — deep research involves extensive data gathering and synthesis. Only treat `status=failed` as an error.

### Step 4: Return results to user

**No file download** for deep research. Return the **Task URL** for online viewing.

```bash
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py status \
  --task-id task_abc123xyz --json
# → {"task_id": "task_abc123xyz", "status": "completed", "progress": 100, "task_url": "https://www.anygen.io/task/task_abc123xyz"}
```

**Tell the user:**
- **Task URL** — for reading the full research report online

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
- Deep research tasks may take longer than other operations — progress pausing is normal
- Single attachment file should not exceed 10MB
