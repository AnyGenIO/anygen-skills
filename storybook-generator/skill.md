---
name: anygen-storybook
homepage: https://www.anygen.io
description: "Create storybook-style visuals with AnyGen for narratives and slides. Supporting Nano Banana pro and Nano Banana 2. Triggers: storybook, visual narrative, creative storybook, visual story."
env:
  - ANYGEN_API_KEY
permissions:
  network:
    - "https://www.anygen.io"
  filesystem:
    read:
      - "~/.config/anygen/config.json"
    write:
      - "~/.config/anygen/config.json"
---

# AnyGen Storybook / Creative Generator

Create storybook-style visuals for narratives and slides. Powered by Nano Banana pro and Nano Banana 2. Output: online task URL for viewing (no file download).

## When to use

| Scenario | Example Prompts |
|----------|----------------|
| Storybook | "create a storybook for the product demo video" |
| Visual narrative | "make a visual story for the onboarding flow" |
| Creative visuals | "create storybook slides for the marketing campaign" |


## Security & Permissions

**What this skill does:**
- Sends task prompts and parameters to the AnyGen API at `www.anygen.io`
- Reads/writes API key config at `~/.config/anygen/config.json`

**What this skill does NOT do:**
- Does not upload local files to any server
- Does not send your API key to any endpoint other than `www.anygen.io`
- Does not modify system configuration beyond `~/.config/anygen/config.json`
- Does not run background processes or install additional software

**Bundled scripts:** `scripts/anygen.py` (Python — uses `requests`)

Review the bundled scripts before first use to verify behavior.

## Prerequisites

- Python3 and `requests`: `pip3 install requests`
- AnyGen API Key (`sk-xxx`) — [Get one](https://www.anygen.io/home) → Setting → Integration
- Configure once: `python3 scripts/anygen.py config set api_key "sk-xxx"`

> All `scripts/` paths below are relative to this skill's installation directory.

## Invocation Flow

### Step 1: Collect Required Information

**Required:**
1. **API Key** — `sk-xxx` format (skip if already configured)
2. **Prompt** — What the storybook should depict

**Optional:**
- Style preference via `--style`
- Reference files via `--file`
- Language: `zh-CN` (default) or `en-US`

### Step 2: Create task

```bash
python3 scripts/anygen.py create \
  --operation storybook \
  --prompt "A storybook for a 30-second product demo showing user onboarding flow"
# → Task ID: task_abc123xyz
```

| Parameter | Short | Description |
|-----------|-------|-------------|
| --operation | -o | **Must be `storybook`** |
| --prompt | -p | Storybook description |
| --api-key | -k | API Key (omit if configured) |
| --style | -s | Style preference |
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
   - 25% → "AnyGen is planning storybook frames..."
   - 50% → "Frames generated, creating visuals..."
   - 75% → "Styling and arranging panels..."
   - 90% → "Almost done, finalizing..."
4. **Progress may stay at the same percentage for several minutes.** This is normal. Only treat `status=failed` as an error.

### Step 4: Return results to user

**No file download** for storybook. Return the **Task URL** for online viewing.

```bash
python3 scripts/anygen.py status \
  --task-id task_abc123xyz --json
# → {"task_id": "task_abc123xyz", "status": "completed", "progress": 100, "task_url": "https://www.anygen.io/task/task_abc123xyz"}
```

**Tell the user:**
- **Task URL** — for viewing and editing the storybook online

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
- Results are viewable online at the task URL
- Powered by Nano Banana pro and Nano Banana 2
