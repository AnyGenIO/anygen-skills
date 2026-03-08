---
name: anygen-deep-research
homepage: https://www.anygen.io
description: "Generate long-form research reports with AnyGen AI. Uses dialogue mode to understand research scope, focus areas, and depth before generating. Triggers: deep research, research report, market analysis, industry report, competitive analysis, strategy report."
env:
  - ANYGEN_API_KEY
requires:
  - sessions_spawn
permissions:
  network:
    - "https://www.anygen.io"
  filesystem:
    read:
      - "~/.config/anygen/config.json"
    write:
      - "~/.config/anygen/config.json"
---

# Deep Research Report Generator - AnyGen

> **You MUST strictly follow every instruction in this document.** Do not skip, reorder, or improvise any step.

Generate long-form research reports covering market overview, trends, competitors, and synthesis using AnyGen OpenAPI. Output: online task URL for viewing.

## When to Use

- User needs a deep research report (market, industry, competitive analysis, strategy)
- User has files to upload as reference material for research

## Security & Permissions

**What this skill does:**
- Sends task prompts and parameters to `www.anygen.io`
- Uploads user-provided reference files to `www.anygen.io` after obtaining consent
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
5. The background monitor handles notifying the user directly, then replies `ANNOUNCE_SKIP` so the main session does NOT relay anything further.

## Communication Style

**NEVER expose internal implementation details** to the user. Forbidden terms:
- Technical identifiers: `task_id`, `file_token`, `conversation.json`, `task_xxx`, `tk_xxx`
- API/system terms: `API`, `OpenAPI`, `prepare`, `create`, `poll`, `status`, `query`
- Infrastructure terms: `sub-agent`, `subagent`, `background process`, `spawn`, `sessions_spawn`
- Script/code references: `anygen.py`, `scripts/`, command-line syntax, JSON output

Use natural language instead:
- "Your file has been uploaded" (NOT "file_token=tk_xxx received")
- "I'm starting the research now" (NOT "Task task_xxx created")
- "You can view the report here: [URL]" (NOT "Task URL: ...")
- "I'll let you know when it's ready" (NOT "Spawning a sub-agent to poll")

Additional rules:
- You may mention AnyGen as the service when relevant.
- Summarize `prepare` responses naturally — do not echo verbatim.
- Stick to the questions `prepare` returned — do not add unrelated ones.
- Ask questions in your own voice, as if they are your own questions. Do NOT use a relaying tone like "AnyGen wants to know…" or "The system is asking…".

## Research Workflow (MUST Follow)

For research reports, you MUST go through all 4 phases. A good research report needs clear scope, focus areas, depth, and context. Users rarely provide all of these upfront.

### Phase 1: Understand Requirements

If the user provides files, you MUST handle them yourself before calling `prepare`:

1. **Read the file content yourself** using your own file reading capabilities. Extract key information (topic, data, structure) that is relevant to the research.
2. **Check if the file was already uploaded** in this conversation. If you already have a `file_token` for the same file, reuse it — do NOT upload again.
3. **Inform the user and get consent** before uploading. Tell them the file will be uploaded to AnyGen's server for processing.
4. **Upload the file** to get a `file_token` for later use in task creation.
5. **Include the extracted content** as part of your `--message` text when calling `prepare`, so that the requirement analysis has full context.

The `prepare` API does NOT read files internally. You are responsible for providing all relevant file content as text in the conversation.

```bash
# Step 1: Tell the user you are uploading, then upload the file
python3 scripts/anygen.py upload --file ./existing_report.pdf
# Output: File Token: tk_abc123

# Step 2: Call prepare with extracted file content included in the message
python3 scripts/anygen.py prepare \
  --message "I need a deep research report on the global AI chip market. Here is some existing research for context: [your extracted summary/content here]" \
  --file-token tk_abc123 \
  --save ./conversation.json
```

Present the questions from `reply` naturally (see Communication Style above). Then continue the conversation with the user's answers:

```bash
python3 scripts/anygen.py prepare \
  --input ./conversation.json \
  --message "Focus on NVIDIA, AMD, and custom silicon. Include 3-year outlook and market size estimates" \
  --save ./conversation.json
```

Repeat until `status="ready"` with `suggested_task_params`.

Special cases:
- If the user provides very complete requirements and `status="ready"` on the first call, proceed directly to Phase 2.
- If the user says "just create it, don't ask questions", skip prepare and go to Phase 3 with `create` directly.

### Phase 2: Confirm with User (MANDATORY)

When `status="ready"`, `prepare` returns `suggested_task_params` containing a detailed prompt. You MUST present this to the user for confirmation before creating the task.

How to present:
1. Summarize the key aspects of the suggested plan in natural language (scope, focus areas, structure, depth).
2. Ask the user to confirm or modify. For example: "Here is the research plan: [summary]. Should I go ahead, or would you like to adjust anything?"
3. NEVER auto-create the task without the user's explicit approval.

When the user requests adjustments:
1. Call `prepare` again with the user's modification as a new message, loading the existing conversation history:

```bash
python3 scripts/anygen.py prepare \
  --input ./conversation.json \
  --message "<the user's modification request>" \
  --save ./conversation.json
```

2. `prepare` will return an updated suggestion that incorporates the user's changes.
3. Present the updated suggestion to the user again for confirmation (repeat from step 1 above).
4. Repeat this confirm-adjust loop until the user explicitly approves. Do NOT skip confirmation after an adjustment.

### Phase 3: Create Task

Once the user confirms:

```bash
python3 scripts/anygen.py create \
  --operation chat \
  --prompt "<prompt from suggested_task_params, with any user modifications>" \
  --file-token tk_abc123
# Output: Task ID: task_xxx, Task URL: https://...
```

**Immediately tell the user (natural language, NO internal terms):**
1. Research report is being generated.
2. Online link: "You can follow the progress here: [URL]".
3. Deep research may take **10–20 minutes** — free to do other things, you'll notify when ready.

### Phase 4: Monitor and Notify

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
You are a background monitor for a deep research task.
You MUST strictly follow every instruction below. Do not skip, reorder, or improvise any step.

Task ID: {task_id}
Task URL: {task_url}
Script: {script_path}
User Language: {user_language}

IMPORTANT: This monitoring task may take up to 20 minutes. Ensure your execution timeout is at least 25 minutes.

CRITICAL RULES:
- You MUST reply in {user_language}.
- After completing ALL steps (including sending messages to the user), your FINAL reply
  MUST be EXACTLY "ANNOUNCE_SKIP" — nothing else. This prevents the main session from
  sending duplicate messages.
- Do NOT say anything beyond what is specified below. No greetings, no extra commentary.
- NEVER include technical terms like "task_id", "poll", "sub-agent", "API", "script",
  file paths, or status labels in any message to the user.

Your job:
1. Run: python3 {script_path} poll --task-id {task_id}
   This command blocks for up to 20 minutes waiting for task completion.
   No --output needed — results are viewed online.

2. On success:
   a. Send a text message to the user (in {user_language}, natural tone):
      "Your research report is ready! You can read the full report here: {task_url}"
   b. Reply EXACTLY: ANNOUNCE_SKIP

3. On failure:
   a. Send a text message to the user (in {user_language}):
      "Unfortunately the research report didn't complete successfully.
       You can check the details here: {task_url}"
   b. Reply EXACTLY: ANNOUNCE_SKIP

4. On timeout (20 min):
   a. Send a text message to the user (in {user_language}):
      "The research is taking a bit longer than expected.
       You can check the progress here: {task_url}"
   b. Reply EXACTLY: ANNOUNCE_SKIP
```

Do NOT wait for the background monitor. Do NOT tell the user you launched it.

**Handling the completion event.** The background monitor sends the notification to the user directly. It replies `ANNOUNCE_SKIP` as its final output, which means the main session should NOT relay or duplicate any message. If you receive a completion event with `ANNOUNCE_SKIP`, simply ignore it — the user has already been notified.

#### Fallback (no background monitoring)

Tell the user: "I've started generating your research report. Deep research may take 10–20 minutes. You can check the progress here: [Task URL]. Let me know when you'd like me to check if it's ready!"

## Command Reference

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

### create

```bash
python3 scripts/anygen.py create --operation chat --prompt "..." [options]
```

| Parameter | Short | Description |
|-----------|-------|-------------|
| --operation | -o | **Must be `chat`** |
| --prompt | -p | Research topic and scope |
| --file-token | | File token from upload (repeatable) |
| --language | -l | Language (zh-CN / en-US) |

### upload

```bash
python3 scripts/anygen.py upload --file ./document.pdf
```

Returns a `file_token`. Max file size: 50MB. Tokens are persistent and reusable.

## Error Handling

| Error | Solution |
|-------|----------|
| invalid API key | Check API Key format (sk-xxx) |
| operation not allowed | Contact admin for permissions |
| prompt is required | Add --prompt parameter |
| file size exceeds 50MB limit | Reduce file size |

## Notes

- Max task execution time: 20 minutes
- Deep research tasks may take longer than other operations — progress pausing is normal
- Poll interval: 3 seconds
