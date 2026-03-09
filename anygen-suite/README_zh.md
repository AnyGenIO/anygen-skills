# AnyGen 内容生成器

[English](./README.md)

一个用于通过 AnyGen OpenAPI 生成 AI 内容的 Claude Code 技能。

## 功能特性

| 操作类型 | 说明 | 文件下载 |
|----------|------|----------|
| `slide` | 生成 PPT/幻灯片 | 支持 (.pptx) |
| `doc` | 生成文档 | 支持 (.docx) |
| `smart_draw` | 图表生成（专业风格 / 手绘风格） | 支持 (.png) |
| `storybook` | 创建故事板 | 支持 (.pptx) |
| `data_analysis` | 数据分析与可视化 | 仅在线查看 |
| `website` | 网站开发 | 仅在线查看 |
| `chat` | 通用 AI 对话（SuperAgent） | 仅在线查看 |

## 快速开始

1. **获取 API Key**：访问 [AnyGen](https://www.anygen.io/home?auto_create_openclaw_key=1)

2. **配置 API Key**：
   ```bash
   python3 scripts/anygen.py config set api_key "sk-xxx"
   ```

3. **对话模式**（推荐 — 多轮需求分析）：
   ```bash
   # 开始需求分析
   python3 scripts/anygen.py prepare \
     --message "我需要一个关于人工智能应用的演示文稿" \
     --save ./conversation.json

   # 继续对话，回答问题
   python3 scripts/anygen.py prepare \
     --input ./conversation.json \
     --message "面向企业级应用场景，10页左右" \
     --save ./conversation.json

   # 当 status=ready 时，创建任务
   python3 scripts/anygen.py create \
     --operation slide \
     --prompt "<suggested_task_params 中的 prompt>"
   ```

4. **快速模式**（跳过对话，直接创建）：
   ```bash
   python3 scripts/anygen.py create \
     --operation slide \
     --prompt "关于人工智能应用的演示文稿"
   ```

5. **监控与下载**：
   ```bash
   # 轮询直到完成
   python3 scripts/anygen.py poll --task-id task_xxx

   # 下载文件
   python3 scripts/anygen.py download --task-id task_xxx --output ./output/

   # 仅下载缩略图
   python3 scripts/anygen.py thumbnail --task-id task_xxx --output ./output/
   ```

## 命令说明

| 命令 | 说明 |
|------|------|
| `prepare` | 创建任务前的多轮需求分析 |
| `create` | 创建生成任务 |
| `upload` | 上传参考文件，获取 file_token |
| `poll` | 轮询任务状态直到完成（阻塞） |
| `status` | 查询一次任务状态（非阻塞） |
| `download` | 下载生成的文件 |
| `thumbnail` | 下载缩略图预览 |
| `run` | 完整流程：create → poll → download |
| `config` | 管理 API Key 配置 |

## 参数说明（create）

| 参数 | 简写 | 说明 |
|------|------|------|
| --operation | -o | 操作类型：slide、doc、smart_draw、chat 等 |
| --prompt | -p | 内容描述 |
| --language | -l | 语言：zh-CN 或 en-US |
| --slide-count | -c | PPT 页数 |
| --template | -t | 幻灯片模板 |
| --ratio | -r | 幻灯片比例：16:9 或 4:3 |
| --style | -s | 风格偏好 |
| --file-token | | 上传文件后获取的 file_token（可重复） |
| --export-format | -f | 导出格式（slide/storybook: pptx/image/thumbnail, doc: docx/image/thumbnail, smart_draw: drawio/excalidraw） |

## 详细文档

查看 [SKILL.md](./SKILL.md) 获取完整文档。

## 许可证

MIT
