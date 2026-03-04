# AnyGen 内容生成器

[English](./README.md)

一个用于通过 AnyGen OpenAPI 生成 AI 内容的 Claude Code 技能。

## 功能特性

| 操作类型 | 说明 | 文件下载 |
|----------|------|----------|
| `slide` | 生成 PPT/幻灯片 | ✅ 支持 (.pptx) |
| `doc` | 生成文档 | ✅ 支持 (.docx) |
| `chat` | 通用 AI 对话 | ❌ 仅在线查看 |
| `storybook` | 创建故事板 | ❌ 仅在线查看 |
| `data_analysis` | 数据分析 | ❌ 仅在线查看 |
| `website` | 网站开发 | ❌ 仅在线查看 |

## 快速开始

1. **获取 API Key**：访问 [AnyGen](https://www.anygen.io) → 设置 → 集成

2. **配置 API Key**：
   ```bash
   python3 ~/.claude/my_skills/anygen/scripts/anygen.py config set api_key "sk-xxx"
   ```

3. **生成内容**：
   ```bash
   # 生成 PPT
   python3 ~/.claude/my_skills/anygen/scripts/anygen.py run \
     --operation slide \
     --prompt "关于人工智能应用的演示文稿" \
     --output ./output/

   # 生成文档
   python3 ~/.claude/my_skills/anygen/scripts/anygen.py run \
     --operation doc \
     --prompt "2024年科技趋势报告" \
     --output ./output/
   ```

## 命令说明

| 命令 | 说明 |
|------|------|
| `create` | 创建生成任务 |
| `poll` | 轮询任务状态直到完成 |
| `download` | 下载生成的文件 |
| `run` | 完整流程：创建 → 轮询 → 下载 |
| `config` | 管理 API Key 配置 |

## 参数说明

| 参数 | 简写 | 说明 |
|------|------|------|
| --api-key | -k | API Key（已配置时可省略） |
| --operation | -o | 操作类型：slide、doc、chat 等 |
| --prompt | -p | 内容描述 |
| --language | -l | 语言：zh-CN 或 en-US |
| --slide-count | -c | PPT 页数 |
| --style | -s | 风格偏好 |
| --file | | 附件文件（可多次使用） |
| --output | | 下载文件的输出目录 |

## 详细文档

查看 [skill.md](./skill.md) 获取完整文档。

## 许可证

MIT
