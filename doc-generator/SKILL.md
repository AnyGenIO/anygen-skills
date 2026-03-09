---
name: anygen-doc
homepage: https://www.anygen.io
description: "Generate structured documents with AnyGen AI. Uses dialogue mode to understand audience, purpose, and content before generating. Background-monitors progress and delivers a preview with online editing link when ready."
requires:
  - sessions_spawn
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
      - "~/.openclaw/workspace/"
---

# AI Document Generator - AnyGen

> **You MUST strictly follow every instruction in this document.** Do not skip, reorder, or improvise any step.

Create professional structured documents using AnyGen OpenAPI.

## When to Use

- User needs to create documents (specs, proposals, summaries, reports)
- User has files to upload as reference material for document generation

## Security & Permissions

**What this skill does:**
- Sends task prompts and parameters to `www.anygen.io`
- Uploads user-provided reference files to `www.anygen.io` after obtaining consent
- Downloads generated documents (DOCX) to `~/.openclaw/workspace/`
- Spawns a background process (up to 25 min) to monitor progress and auto-download
- Reads/writes API key config at `~/.config/anygen/config.json`

**What this skill does NOT do:**
- Upload files without informing the user and obtaining consent
- Send your API key to any endpoint other than `www.anygen.io`
- Modify system configuration beyond `~/.config/anygen/config.json`

**Bundled scripts:** `scripts/anygen.py` (Python — uses `requests`). Review before first use.

## Prerequisites

- Python3 and `requests`: `pip3 install requests`
- AnyGen API Key (`sk-xxx`) — [Get one](https://www.anygen.io/home?auto_create_openclaw_key=1) → Setting → Integration
- Configure once: `python3 scripts/anygen.py config set api_key "sk-xxx"`

> All `scripts/` paths below are relative to this skill's installation directory.

## CRITICAL: NEVER Block the Conversation

After creating a task, you MUST start background monitoring via `sessions_spawn`, then continue normally. NEVER call `poll` in the foreground — it blocks for up to 20 minutes.

1. `create` → get `task_id` and `task_url`.
2. Tell user: (a) generation started, (b) the online link, (c) ~10–15 min, free to do other things.
3. Launch background monitor via `sessions_spawn` (Phase 4). Do NOT announce this to the user.
4. Continue the conversation — do NOT wait.
5. The background monitor handles sending the thumbnail and notifying the user directly, then replies `ANNOUNCE_SKIP` so the main session does NOT relay anything further.
6. Only send the DOCX file if the user explicitly requests it.

## Communication Style

**NEVER expose internal implementation details** to the user. Forbidden terms:
- Technical identifiers: `task_id`, `file_token`, `conversation.json`, `task_xxx`, `tk_xxx`
- API/system terms: `API`, `OpenAPI`, `prepare`, `create`, `poll`, `status`, `query`
- Infrastructure terms: `sub-agent`, `subagent`, `background process`, `spawn`, `sessions_spawn`
- Script/code references: `anygen.py`, `scripts/`, command-line syntax, JSON output

Use natural language instead:
- "Your file has been uploaded" (NOT "file_token=tk_xxx received")
- "I'm generating your document now" (NOT "Task task_xxx created")
- "You can view your document here: [URL]" (NOT "Task URL: ...")
- "I'll let you know when it's ready" (NOT "Spawning a sub-agent to poll")

Additional rules:
- You may mention AnyGen as the service when relevant.
- Summarize `prepare` responses naturally — do not echo verbatim.
- Stick to the questions `prepare` returned — do not add unrelated ones.
- Ask questions in your own voice, as if they are your own questions. Do NOT use a relaying tone like "AnyGen wants to know…" or "The system is asking…".

## Document Workflow (MUST Follow All 4 Phases)

### Phase 1: Understand Requirements

If the user provides files, handle them before calling `prepare`:

1. **Read the file** yourself. Extract key information relevant to the document.
2. **Reuse existing `file_token`** if the same file was already uploaded in this conversation.
3. **Get consent** before uploading: "I'll upload your file to AnyGen for reference. This may take a moment..."
4. **Upload** to get a `file_token`.
5. **Include extracted content** in `--message` when calling `prepare` (the API does NOT read files internally).

```bash
python3 scripts/anygen.py upload --file ./report.pdf
# Output: File Token: tk_abc123

python3 scripts/anygen.py prepare \
  --message "I need a technical design document based on this report. Key content: [extracted summary]" \
  --file-token tk_abc123 \
  --save ./conversation.json
```

Present questions from `reply` naturally. Continue with user's answers:

```bash
python3 scripts/anygen.py prepare \
  --input ./conversation.json \
  --message "The audience is engineering managers, goal is to document the auth system architecture" \
  --save ./conversation.json
```

Repeat until `status="ready"` with `suggested_task_params`.

Special cases:
- `status="ready"` on first call → proceed to Phase 2.
- User says "just create it" → skip to Phase 3 with `create` directly.

### Phase 2: Confirm with User (MANDATORY)

When `status="ready"`, summarize the suggested plan (audience, structure, style) and ask for confirmation. NEVER auto-create without explicit approval.

If the user requests adjustments, call `prepare` again with the modification, re-present, and repeat until approved.

### Phase 3: Create Task

```bash
python3 scripts/anygen.py create \
  --operation doc \
  --prompt "<prompt from suggested_task_params>" \
  --file-token tk_abc123 \
  --export-format docx
# Output: Task ID: task_xxx, Task URL: https://...
```

**Immediately tell the user (natural language, NO internal terms):**
1. Document is being generated.
2. Online preview/edit link: "You can follow the progress here: [URL]".
3. Takes about **10–15 minutes** — free to do other things, you'll notify when ready.

### Phase 4: Monitor and Deliver Result

> **Requires `sessions_spawn`.** If unavailable, skip to **Fallback** below.

#### Background Monitoring (preferred)

**CRITICAL**: When calling `sessions_spawn`, you MUST set a timeout of at least 25 minutes to ensure the background monitor completes. The poll operation can take up to 20 minutes.

Example spawn call syntax (adjust to match your OpenClaw/sessions API):

```
sessions_spawn(
    prompt=<subagent prompt below>,
    runTimeoutSeconds=1500  # REQUIRED: 25 minutes (1500s) to cover 20-min poll + buffer
)
```

If your sessions API uses different parameter names (e.g., `timeout`, `maxDuration`), use the equivalent parameter to set a 25-minute (1500 second) timeout.

---

**Subagent prompt** (it has NO conversation context):

```
You are a background monitor for a document generation task.
You MUST strictly follow every instruction below. Do not skip, reorder, or improvise any step.

Task ID: {task_id}
Task URL: {task_url}
Script: {script_path}
Thumbnail Output: {thumbnail_output_dir}
User Language: {user_language}

IMPORTANT: This monitoring task may take up to 20 minutes. Ensure your execution timeout is at least 25 minutes.

CRITICAL RULES:
- You MUST reply in {user_language}.
- After completing ALL steps (including sending messages to the user), your FINAL reply
  MUST be EXACTLY "ANNOUNCE_SKIP" — nothing else. This prevents the main session from
  sending duplicate messages.
- Do NOT say anything beyond what is specified below. No greetings, no extra commentary.
- NEVER include technical terms like "task_id", "file_token", "poll", "sub-agent",
  "API", "script", "workspace", "downloaded to", file paths, or status labels
  in any message to the user.

Your job:
1. Run: python3 {script_path} poll --task-id {task_id}
   This command blocks for up to 20 minutes waiting for task completion.
   Do NOT pass --output — the DOCX will only be downloaded when the user requests it.

2. On success:
   a. Download thumbnail:
      python3 {script_path} thumbnail --task-id {task_id} --output {thumbnail_output_dir}
   b. Send the thumbnail preview image to the user. Choose the correct method:
      - Feishu/Lark: Two-step process via OpenAPI —
        Step 1 (Upload image): POST https://open.feishu.cn/open-apis/im/v1/images
          curl -X POST 'https://open.feishu.cn/open-apis/im/v1/images' \
            -H 'Authorization: Bearer <tenant_access_token>' \
            -F 'image_type=message' \
            -F 'image=@{thumbnail_output_dir}/thumbnail.png'
          This returns an image_key.
        Step 2 (Send image message): POST https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id
          Request body:
          {
            "receive_id": "<chat_id>",
            "msg_type": "image",
            "content": "{\"image_key\":\"<image_key>\"}"
          }
      - Other platforms: Send via message tool with filePath.
      The user must see the image inline — not a path or link.
   c. Send a text message to the user (in {user_language}, natural tone):
      "Your document is ready! If you'd like me to send you the DOCX file, just let me know."
   d. Reply EXACTLY: ANNOUNCE_SKIP

3. On failure:
   a. Send a text message to the user (in {user_language}):
      "Unfortunately the document generation didn't complete successfully.
       You can check the details here: {task_url}"
   b. Reply EXACTLY: ANNOUNCE_SKIP

4. On timeout (20 min):
   a. Send a text message to the user (in {user_language}):
      "The document is taking a bit longer than expected.
       You can check the progress here: {task_url}"
   b. Reply EXACTLY: ANNOUNCE_SKIP
```

Do NOT wait for the background monitor. Do NOT tell the user you launched it.

**Handling the completion event.** The background monitor sends the thumbnail and notification to the user directly. It replies `ANNOUNCE_SKIP` as its final output, which means the main session should NOT relay or duplicate any message. If you receive a completion event with `ANNOUNCE_SKIP`, simply ignore it — the user has already been notified.

#### When the User Requests the DOCX File

Download, then send via the appropriate method for your IM environment:

```bash
python3 scripts/anygen.py download --task-id {task_id} --output ~/.openclaw/workspace/
```

- **Feishu/Lark**: Two-step process via OpenAPI —
  Step 1 (Upload file): `POST https://open.feishu.cn/open-apis/im/v1/files`
    ```
    curl -X POST 'https://open.feishu.cn/open-apis/im/v1/files' \
      -H 'Authorization: Bearer <tenant_access_token>' \
      -F 'file_type=stream' \
      -F 'file=@~/.openclaw/workspace/output.docx' \
      -F 'file_name=output.docx'
    ```
    This returns a `file_key`.
  Step 2 (Send file message): `POST https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id`
    ```json
    {
      "receive_id": "<chat_id>",
      "msg_type": "file",
      "content": "{\"file_key\":\"<file_key>\"}"
    }
    ```
- **Other platforms**: Send via message tool with filePath.

Follow up naturally: "Here's your document! You can also edit online at [Task URL]."

#### Fallback (no background monitoring)

Tell the user: "I've started generating your document. It usually takes about 10–15 minutes. You can check the progress here: [Task URL]. Let me know when you'd like me to check if it's ready!"

## Command Reference

### create

```bash
python3 scripts/anygen.py create --operation doc --prompt "..." [options]
```

| Parameter | Short | Description |
|-----------|-------|-------------|
| --operation | -o | **Must be `doc`** |
| --prompt | -p | Content description |
| --file-token | | File token from upload (repeatable) |
| --export-format | -f | `docx` (default) / `image` / `thumbnail` |
| --language | -l | Language (zh-CN / en-US) |
| --style | -s | Style preference |

### upload

```bash
python3 scripts/anygen.py upload --file ./document.pdf
```

Returns a `file_token`. Max 50MB. Tokens are persistent and reusable.

### prepare

```bash
python3 scripts/anygen.py prepare --message "..." [--file-token tk_xxx] [--input conv.json] [--save conv.json]
```

| Parameter | Description |
|-----------|-------------|
| --message, -m | User message text |
| --file | File path to auto-upload and attach (repeatable) |
| --file-token | File token from prior upload (repeatable) |
| --input | Load conversation from JSON file |
| --save | Save conversation state to JSON file |
| --stdin | Read message from stdin |

### poll

Blocks until completion. Downloads file only if `--output` is specified.

```bash
python3 scripts/anygen.py poll --task-id task_xxx                    # status only
python3 scripts/anygen.py poll --task-id task_xxx --output ./output/ # with download
```

| Parameter | Description |
|-----------|-------------|
| --task-id | Task ID from `create` |
| --output | Output directory (omit to skip download) |

### thumbnail

Downloads only the thumbnail preview image.

```bash
python3 scripts/anygen.py thumbnail --task-id task_xxx --output /tmp/
```

| Parameter | Description |
|-----------|-------------|
| --task-id | Task ID from `create` |
| --output | Output directory |

### download

Downloads the generated file (e.g., DOCX).

```bash
python3 scripts/anygen.py download --task-id task_xxx --output ./output/
```

| Parameter | Description |
|-----------|-------------|
| --task-id | Task ID from `create` |
| --output | Output directory |

## Error Handling

| Error | Solution |
|-------|----------|
| invalid API key | Check format (sk-xxx) |
| operation not allowed | Contact admin for permissions |
| prompt is required | Add --prompt parameter |
| file size exceeds 50MB | Reduce file size |

## Notes

- Max task execution time: 20 minutes
- Download link valid for 24 hours
- Poll interval: 3 seconds
