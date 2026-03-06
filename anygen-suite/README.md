# AnyGen Content Generator

[中文](./README_zh.md)

A Claude Code skill for generating AI content using AnyGen OpenAPI.

## Features

| Operation | Description | File Download |
|-----------|-------------|--------------|
| `slide` | Generate PPT/Slides | ✅ Yes (.pptx) |
| `doc` | Generate Documents | ✅ Yes (.docx) |
| `chat` | General AI conversation | ❌ Online only |
| `storybook` | Create storybooks | ❌ Online only |
| `data_analysis` | Data analysis | ❌ Online only |
| `website` | Website development | ❌ Online only |
| `smart_draw` | Diagram generation | ✅ Yes (.png) |

## Quick Start

1. **Get API Key** from [AnyGen](https://www.anygen.io) → Setting → Integration

2. **Configure API Key**:
   ```bash
   python3 ~/.openclaw/skills/anygen/anygen-suite/scripts/anygen.py config set api_key "sk-xxx"
   ```

3. **Generate content** (create → status → download):
   ```bash
   # Create a slide task
   python3 ~/.openclaw/skills/anygen/anygen-suite/scripts/anygen.py create \
     --operation slide \
     --prompt "A presentation about AI applications"
   # → Task ID: task_abc123xyz

   # Check status
   python3 ~/.openclaw/skills/anygen/anygen-suite/scripts/anygen.py status \
     --task-id task_abc123xyz

   # Download when completed
   python3 ~/.openclaw/skills/anygen/anygen-suite/scripts/anygen.py download \
     --task-id task_abc123xyz --output ./output/
   ```

## Commands

| Command | Description |
|---------|-------------|
| `create` | Create a generation task |
| `poll` | Poll task status until completion |
| `download` | Download generated file |
| `config` | Manage API Key configuration |

## Parameters

| Parameter | Short | Description |
|-----------|-------|-------------|
| --api-key | -k | API Key (optional if configured) |
| --operation | -o | Operation type: slide, doc, chat, etc. |
| --prompt | -p | Content description |
| --language | -l | Language: zh-CN or en-US |
| --slide-count | -c | Number of PPT pages |
| --style | -s | Style preference |
| --file | | Attachment file (can be used multiple times) |
| --output | | Output directory for downloaded files |
| --export-format | -f | Export format (slide: pptx/image, doc: docx/image, smart_draw: drawio(professional)/excalidraw(hand-drawn)) |

## More Details

See [skill.md](./skill.md) for complete documentation.

## License

MIT
