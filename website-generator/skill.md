---
name: anygen-website
description: "Build a landing page or simple website with AnyGen from a prompt. Generate sections, copy, and basic layout quickly for faster iteration. Triggers: build website, landing page, create webpage, web page, simple site."
data:
  config_read: "~/.config/anygen/config.json"
  config_write: "~/.config/anygen/config.json"
  env_vars: ["ANYGEN_API_KEY"]
  network: "https://www.anygen.io (AnyGen OpenAPI)"
---

# AnyGen AI Website Generator

Build a landing page or simple website from a natural language prompt. Output: online task URL for viewing the generated website (no file download).

## When to use

| Scenario | Example Prompts |
|----------|----------------|
| Landing page | "quickly build a product landing page" |
| Portfolio site | "create a personal portfolio website" |
| Event page | "make an event registration page" |
| Product page | "build a SaaS product homepage" |

## Prerequisites

- Python3 and `requests`: `pip3 install requests`
- AnyGen API Key (`sk-xxx`) — [Get one](https://www.anygen.io/home) → Setting → Integration
- Configure once: `python3 scripts/anygen.py config set api_key "sk-xxx"`

> All `scripts/` paths below are relative to this skill's installation directory.

## Invocation Flow

### Step 1: Collect Required Information

**Required:**
1. **API Key** — `sk-xxx` format (skip if already configured)
2. **Prompt** — What the website should contain

**Optional:**
- Style preference via `--style`
- Reference files via `--file`
- Language: `zh-CN` (default) or `en-US`

### Step 2: Create task

```bash
python3 scripts/anygen.py create \
  --operation website \
  --prompt "A product landing page for a project management tool with hero section, features, pricing, and CTA"
# → Task ID: task_abc123xyz
```

| Parameter | Short | Description |
|-----------|-------|-------------|
| --operation | -o | **Must be `website`** |
| --prompt | -p | Website description |
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
   - 25% → "AnyGen is designing page structure..."
   - 50% → "Layout generated, adding content..."
   - 75% → "Styling and responsive adjustments..."
   - 90% → "Almost done, finalizing..."
4. **Progress may stay at the same percentage for several minutes.** This is normal. Only treat `status=failed` as an error.

### Step 4: Return results to user

**No file download** for website. Return the **Task URL** for online viewing.

```bash
python3 scripts/anygen.py status \
  --task-id task_abc123xyz --json
# → {"task_id": "task_abc123xyz", "status": "completed", "progress": 100, "task_url": "https://www.anygen.io/task/task_abc123xyz"}
```

**Tell the user:**
- **Task URL** — for viewing and interacting with the generated website

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
