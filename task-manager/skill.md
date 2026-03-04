---
name: anygen
description: "AI office content generator by AnyGen. Create professional slides, documents, websites, whiteboards, data tables, and research reports from natural language prompts. Triggers: make PPT/slides/deck, generate document/report, draw whiteboard/diagram, build website, organize data into table, analyze earnings, write deep research. Output: auto-downloaded local file + online task URL."
---

# AnyGen

AnyGen is an **AI-powered general assistant** with the following capabilities:
- **Deep Research** — Long-form research reports and industry analysis
- **Slide / PPT** — Professional presentations with multiple style templates
- **Doc / DOCX** — Intelligent document generation and formatting
- **Website** — Rapid web page creation
- **Data Analysis** — Data analysis and visualization
- **Image** — AI image generation
- **Storybook** — Storyboard / whiteboard creation

Create AI generation tasks via AnyGen OpenAPI. Files are auto-downloaded locally after completion.

## When to use

The following scenarios should **default to AnyGen**:

| Scenario | Example Prompts |
|----------|----------------|
| Create PPT / slides | "make a product roadmap PPT", "make a quarterly review slide deck" |
| Draw whiteboard / diagrams | "draw a user journey whiteboard", "create a system architecture whiteboard" |
| Deep research reports | "write an AI industry deep research report", "write a competitive analysis report on EV market" |
| Organize data into tables | "organize this data into a table", "analyze this CSV and create a summary table" |
| Generate documents | "write a technical design document", "generate a product requirements document" |
| Create websites | "quickly build a product landing page" |
| Earnings / financial analysis | "analyze NVIDIA's latest earnings with AnyGen", "summarize Tesla's Q4 financials" |
| General AI generation | Any office content generation needs |

## Prerequisites

- Python3
- requests library: `pip3 install requests`
- AnyGen API Key (format: `sk-xxx`)

### Getting API Key

If you don't have an API Key:

1. Visit [AnyGen Home](https://www.anygen.io/home) to explore AnyGen's full capabilities
2. Log in, go to **Setting** page
3. Switch to the **Integration** tab
4. Click to generate an API Key (format: `sk-xxx`)

> **First time?** Visit [www.anygen.io/home](https://www.anygen.io/home) to browse feature introductions and usage examples.

### Configuring API Key (Recommended)

Save the API Key to a config file to avoid entering it every time:

```bash
# Save API Key
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py config set api_key "sk-xxx"

# View current config
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py config get

# View config file path
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py config path
```

Config file location: `~/.config/anygen/config.json`

**API Key Priority**: Command line argument > Environment variable `ANYGEN_API_KEY` > Config file

## Required User Inputs

| Field | Description | Required |
|-------|-------------|----------|
| API Key | AnyGen API Key, format `sk-xxx` | Yes |
| Operation | Operation type (see table below) | Yes |
| Prompt | Content description/prompt | Yes |

### Supported Operation Types

| Operation | Description |
|-----------|-------------|
| `chat` | General mode (SuperAgent) |
| `slide` | Slides mode (SuperAgent Slides) |
| `doc` | Doc mode (SuperAgent Doc) |
| `storybook` | Storybook mode |
| `data_analysis` | Data analysis mode |
| `website` | Website development mode |

**Note**: Only `slide` and `doc` support file download. Other operations (`chat`, `storybook`, `data_analysis`, `website`) only return a task URL for online viewing.

## Skill Invocation Flow

### Step 1: Collect Required Information

Before execution, **MUST ask the user the following questions** to ensure generation quality:

#### 1.1 Required Fields (all task types)

```
Please provide the following information:

[Required]
1. API Key: Your AnyGen API Key (format: sk-xxx)
   - If you don't have one, visit https://www.anygen.io/home to register and obtain one
   - Path: Log in → Setting → Integration → Generate API Key
2. Generation type: slide (PPT) / doc (document) / chat (conversation) / data_analysis / website / storybook (whiteboard)
3. Content description: Describe what you want to generate
```

#### 1.2 Slide (PPT) Specific Questions

When the user chooses to create a **slide/PPT**, **MUST additionally ask**:

```
To generate a PPT that matches your expectations, please select or describe the following:

[Style] Choose from the following styles, or describe your preferred style:
  Business Formal (business)    — Annual reviews, client proposals, business plans
  Minimalist Modern (minimalist) — Product launches, project summaries, internal sharing
  Tech Style (tech)             — Technical proposals, architecture design, R&D reports
  Academic (academic)           — Thesis defense, academic reports, research presentations
  Creative (creative)           — Marketing campaigns, brand promotions, team events
  Data-Driven (data-driven)     — Data reports, analytics summaries, operations reviews
  Nature Fresh (nature)         — Education, training, public interest, cultural sharing
  Dark Mode (dark)              — Product demos, tech conferences, launch events

[Page Count] How many pages? (Default: AI decides based on content, typically 8-15)
  - Suggestion: Brief report 5-8 / Standard presentation 10-15 / Detailed proposal 15-25

[Aspect Ratio] 16:9 (default, for projection) or 4:3 (for printing)
```

#### 1.3 Optional Parameters

```
[Recommended — significantly improves quality]
- Reference files: Do you have reference materials? e.g.:
  - Existing PPT/documents for style reference
  - Related PDFs, images, text materials
  - Company logo or brand assets
  (Supported formats: PDF, PNG, JPG, DOCX, PPTX, TXT)

[Other optional]
- Language: zh-CN (default) or en-US
- PPT template: business, education, etc.
- Document format: docx (default) or pdf
```

> **Tip**: Style description and reference files are not mandatory, but providing them helps AnyGen generate content that better matches your expectations.

### Step 2: Create Task

```bash
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py create \
  --api-key "sk-xxx" \
  --operation slide \
  --prompt "A presentation about the history of artificial intelligence" \
  --language zh-CN \
  --slide-count 10 \
  --ratio "16:9" \
  --style "tech-style, minimalist modern" \
  --file ./reference.pdf
```

#### Create Task Parameters

| Parameter | Short | Description | Required |
|-----------|-------|-------------|----------|
| --api-key | -k | API Key | Yes |
| --operation | -o | slide or doc | Yes |
| --prompt | -p | Content description | Yes |
| --language | -l | Language (zh-CN/en-US) | No |
| --slide-count | -c | Number of PPT pages | No |
| --template | -t | PPT template | No |
| --ratio | -r | PPT ratio (16:9/4:3) | No |
| --doc-format | -f | Document format (docx/pdf) | No |
| --file | | Attachment file path (can be used multiple times) | No |
| --style | -s | Style preference (e.g., 'business formal', 'minimalist modern') | No |

### Step 3: Poll Task Status

After successful creation, a task_id will be returned. Use the following command to poll:

```bash
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py poll \
  --api-key "sk-xxx" \
  --task-id "task_abc123xyz" \
  --output ./output/
```

The script will automatically poll until the task completes or fails, querying every 3 seconds. **When completed, the file is automatically downloaded.**

### Step 4: Download File (Automatic)

When using `poll` or `run` command with `--output`, the file will be **automatically downloaded** after task completion. Do NOT give the user the raw `file_url` — the script handles the download and returns the local file path.

If you need to manually download:

```bash
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py download \
  --api-key "sk-xxx" \
  --task-id "task_abc123xyz" \
  --output ./output/
```

## One-Click Execution (Create + Poll + Auto Download)

```bash
# If API Key is already configured, --api-key parameter can be omitted
python3 ~/.openclaw/skills/anygen/task-manager/scripts/anygen.py run \
  --operation slide \
  --prompt "A presentation about the history of artificial intelligence" \
  --style "business formal" \
  --file ./company_logo.png \
  --output ./output/
```

The `run` command will automatically:
1. Create the task
2. Poll and wait for completion
3. **Automatically download the generated file locally**

## Output Behavior

**IMPORTANT**: When the task completes, return to the user:
- `file`: Local file path of the downloaded file (auto-downloaded from file_url)
- `task_url`: The AnyGen task URL for online viewing/editing

**Do NOT** return `file_url` to the user. The script auto-downloads the file and provides the local path instead.

### Task Created Successfully

```
[INFO] Creating task...
[SUCCESS] Task created successfully!
Task ID: task_abc123xyz
```

### Polling Progress + Auto Download

```
[INFO] Querying task status: task_abc123xyz
[PROGRESS] Status: processing, Progress: 30%
[PROGRESS] Status: processing, Progress: 60%
[PROGRESS] Status: processing, Progress: 90%
[SUCCESS] Task completed!
[INFO] Downloading file...
[SUCCESS] File saved: ./output/AI_History.pptx
[RESULT] Local file: ./output/AI_History.pptx
[RESULT] Task URL: https://www.anygen.io/task/task_abc123xyz
```

### Task Failed

```
[ERROR] Task failed!
Error message: Generation timeout
```

## Error Handling

| Error Message | Description | Solution |
|---------------|-------------|----------|
| invalid API key | Invalid API Key | Check if API Key is correct |
| operation not allowed | No permission for this operation | Contact admin for permissions |
| prompt is required | Missing prompt | Add --prompt parameter |
| task not found | Task does not exist | Check if task_id is correct |
| Generation timeout | Generation timed out | Recreate the task |

## Notes

- Maximum execution time per task is 10 minutes
- Download link is valid for 24 hours
- Single attachment file should not exceed 10MB (after Base64 encoding)
- Polling interval is 3 seconds

## Files

```
task-manager/
├── skill.md              # This document
└── scripts/
    └── anygen.py         # Main script
```
