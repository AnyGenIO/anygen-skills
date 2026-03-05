---
name: anygen-diagram
description: "Generate architecture diagrams, whiteboard, flowcharts, and system diagrams with AnyGen. Create diagram drafts quickly and refine them in your preferred tool. Triggers: draw diagram, architecture diagram, flowchart, system diagram, whiteboard diagram, sequence diagram."
---

# AnyGen AI Diagram Generator for Excalidraw / Draw.io style

Generate architecture diagrams, flowcharts, and system diagrams from natural language. Output: DrawIO (.xml) or Excalidraw (.json) source file, auto-rendered to PNG for preview.

## When to use

| Scenario | Example Prompts |
|----------|----------------|
| Architecture diagram | "draw a microservice architecture diagram" |
| Flowchart | "create a flowchart for the CI/CD pipeline" |
| System design | "draw a system architecture whiteboard" |
| Sequence diagram | "create a sequence diagram for the auth flow" |

## Prerequisites

- Python3 and `requests`: `pip3 install requests`
- Node.js v18+ (for PNG rendering, auto-installed on first run)
- AnyGen API Key (`sk-xxx`) — [Get one](https://www.anygen.io/home) → Setting → Integration
- Configure once: `python3 scripts/anygen.py config set api_key "sk-xxx"`

> All `scripts/` paths below are relative to this skill's installation directory.

## Invocation Flow

### Step 1: Collect Required Information

**Required:**
1. **API Key** — `sk-xxx` format (skip if already configured)
2. **Prompt** — What diagram to generate

**Diagram-specific options:**
- **Format** — `drawio` (default, exports .xml) or `excalidraw` (exports .json)

**Optional:**
- Style preference via `--style`
- Reference files via `--file`
- Language: `zh-CN` (default) or `en-US`

### Step 2: Create task

```bash
python3 scripts/anygen.py create \
  --operation smart_draw \
  --prompt "A microservice architecture diagram with API gateway, auth service, and database" \
  --export-format drawio
# → Task ID: task_abc123xyz
```

| Parameter | Short | Description |
|-----------|-------|-------------|
| --operation | -o | **Must be `smart_draw`** |
| --prompt | -p | Diagram description |
| --export-format | -f | `drawio` (default) / `excalidraw` |
| --api-key | -k | API Key (omit if configured) |
| --style | -s | Style preference |
| --language | -l | zh-CN / en-US |
| --file | | Attachment file path (repeatable) |

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
   - 25% → "AnyGen is analyzing diagram structure..."
   - 50% → "Diagram layout generated, adding details..."
   - 75% → "Styling and refining connections..."
   - 90% → "Almost done, finalizing..."
4. **Progress may stay at the same percentage for several minutes.** This is normal. Only treat `status=failed` as an error.

### Step 4: Download file

```bash
python3 scripts/anygen.py download \
  --task-id task_abc123xyz --output ./output/
```

### Step 5: Render to PNG (REQUIRED)

The downloaded file (.xml/.json) is a diagram source, **NOT an image**. You **MUST** render it to PNG:

```bash
# For DrawIO:
bash scripts/render-diagram.sh drawio ./output/diagram.xml ./output/diagram.png

# For Excalidraw:
bash scripts/render-diagram.sh excalidraw ./output/diagram.json ./output/diagram.png
```

| Format | --export-format | Export File | Render Command |
|--------|-----------------|-------------|----------------|
| DrawIO (default) | `drawio` | `.xml` | `render-diagram.sh drawio input.xml output.png` |
| Excalidraw | `excalidraw` | `.json` | `render-diagram.sh excalidraw input.json output.png` |

**Options:** `--scale <n>` (default: 2), `--background <hex>` (default: #ffffff), `--padding <px>` (default: 20)

### Step 6: Return results to user

**Tell the user:**
- **Rendered PNG path** — the output from render step
- **Source file path** — the .xml/.json for editing in Draw.io/Excalidraw
- **Task URL** — from `[RESULT] Task URL:` line

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
- Download link is valid for 24 hours
- PNG rendering requires Chromium (auto-installed on first run)
- Dependencies auto-installed on first run of render-diagram.sh
