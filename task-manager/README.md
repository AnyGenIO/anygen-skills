# AnyGen Content Generator

[中文](./README_zh.md)

A Claude Code skill for generating AI content using AnyGen OpenAPI.

## Features

| Operation | Description | File Download |
|-----------|-------------|---------------|
| `slide` | Generate PPT/Slides | ✅ Yes (.pptx) |
| `doc` | Generate Documents | ✅ Yes (.docx) |
| `chat` | General AI conversation | ❌ Online only |
| `storybook` | Create storyboards | ❌ Online only |
| `data_analysis` | Data analysis | ❌ Online only |
| `website` | Website development | ❌ Online only |

## Quick Start

1. **Get API Key** from [AnyGen](https://www.anygen.io) → Setting → Integration

2. **Configure API Key**:
   ```bash
   python3 ~/.claude/my_skills/anygen/scripts/anygen.py config set api_key "sk-xxx"
   ```

3. **Generate content**:
   ```bash
   # Generate PPT
   python3 ~/.claude/my_skills/anygen/scripts/anygen.py run \
     --operation slide \
     --prompt "A presentation about AI applications" \
     --output ./output/

   # Generate Document
   python3 ~/.claude/my_skills/anygen/scripts/anygen.py run \
     --operation doc \
     --prompt "A report on 2024 tech trends" \
     --output ./output/
   ```

## Commands

| Command | Description |
|---------|-------------|
| `create` | Create a generation task |
| `poll` | Poll task status until completion |
| `download` | Download generated file |
| `run` | Full workflow: create → poll → download |
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

## More Details

See [skill.md](./skill.md) for complete documentation.

## License

MIT
