---
name: anygen-doc
description: "Generate structured documents with AnyGen: specs, proposals, and summaries. Export in DOCX or PDF format with clean formatting and headings. Triggers: write document, generate doc, create spec, write proposal, technical document, requirements document."
---

# AI Document Generator - AnyGen

Generate structured documents from natural language prompts. Supports DOCX and PDF output with clean formatting. Output: auto-downloaded document file + online task URL.

## When to use

| Scenario | Example Prompts |
|----------|----------------|
| Technical spec | "write a technical design document for the auth system" |
| Product requirements | "generate a product requirements document" |
| Proposal | "write a project proposal for the new dashboard" |
| Summary | "create an executive summary of the Q4 results" |

## Prerequisites

- Python3 and `requests`: `pip3 install requests`
- AnyGen API Key (`sk-xxx`) — [Get one](https://www.anygen.io/home) → Setting → Integration
- Configure once: `python3 scripts/anygen.py config set api_key "sk-xxx"`

> All `scripts/` paths below are relative to this skill's installation directory.

## Invocation Flow

### Step 1: Collect Required Information

**Required:**
1. **API Key** — `sk-xxx` format (skip if already configured)
2. **Prompt** — What the document should cover

**Document-specific options:**
- **Export format** — `docx` (default) or `image`

**Optional:**
- Style preference via `--style`
- Reference files (PDF, PNG, JPG, DOCX, PPTX, TXT) via `--file`
- Language: `zh-CN` (default) or `en-US`

### Step 2: Create task

```bash
python3 scripts/anygen.py create \
  --operation doc \
  --prompt "A technical design document for a real-time notification system" \
  --export-format docx
# → Task ID: task_abc123xyz
```

| Parameter | Short | Description |
|-----------|-------|-------------|
| --operation | -o | **Must be `doc`** |
| --prompt | -p | Content description |
| --export-format | -f | `docx` (default) / `image` |
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
   - 25% → "AnyGen is generating document outline..."
   - 50% → "Content generated, now formatting..."
   - 75% → "Styling and polishing..."
   - 90% → "Almost done, finalizing..."
4. **Progress may stay at the same percentage for several minutes.** This is normal. Only treat `status=failed` as an error.

### Step 4: Download file

```bash
python3 scripts/anygen.py download \
  --task-id task_abc123xyz --output ./output/
```

### Step 5: Return results to user

**Tell the user:**
- **Preview thumbnail** — from `[RESULT] Thumbnail:` line. You **MUST** display this image to the user so they can immediately preview the generated document.
- **Local file path** — from `[RESULT] Local file:` line
- **Task URL** — from `[RESULT] Task URL:` line, for online viewing/editing

## Advanced: IM File Delivery (MEDIA: Protocol)

In IM context (e.g., Feishu/Lark bot with OpenClaw), add `--media` to `download`. Send the output `MEDIA:/path` as a separate short message.

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
- Single attachment file should not exceed 10MB
