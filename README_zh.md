# AnyGen AI Skills（模块化版）

[English](./README.md)

> 适用于 [OpenClaw](https://github.com/openclaw/openclaw) / Claude Code / Cursor 的模块化 AI 技能包

使用 AnyGen 的**模块化** AI 内容生成技能集合。每个技能都是独立模块，专注于特定任务——按需安装。

> 💡 **想要一体化方案？** 查看 [anygen-suite-skill](https://github.com/AnyGenIO/anygen-suite-skill)，一个整合所有功能的统一技能。

## 包含技能

| 技能 | 说明 |
|------|------|
| `slide-generator` | PPT/演示文稿生成 |
| `doc-generator` | 文档和报告生成 |
| `diagram-generator` | 图表、流程图、架构图（SmartDraw） |
| `data-analysis` | 数据分析和可视化 |
| `deep-research` | 深度调研报告 |
| `financial-research` | 财报和金融分析 |
| `image-generator` | AI 图片生成 |
| `storybook-generator` | 故事板和视觉叙事 |
| `website-generator` | 落地页和网站开发 |

## 安装

```bash
# OpenClaw
git clone https://github.com/AnyGenIO/anygen-skills.git ~/.openclaw/skills/anygen

# Claude Code
git clone https://github.com/AnyGenIO/anygen-skills.git ~/.claude/skills/anygen
```

## 配置

```bash
# 设置 AnyGen API Key
python3 <skill>/scripts/anygen.py config set api_key "sk-xxx"

# 或使用环境变量
export ANYGEN_API_KEY="sk-xxx"
```

API Key 获取：[anygen.io/home](https://www.anygen.io/home?auto_create_openclaw_key=1)

## 使用示例

```
# 演示文稿
"做一个产品 Roadmap PPT"
"制作季度汇报演示"

# 文档
"写一份技术设计文档"
"生成项目提案"

# 图表
"画一个微服务架构图"
"创建用户流程图"

# 调研
"写一份 AI 行业深度调研报告"
"分析英伟达最新财报"

# 数据
"把这份数据整理成表格"
"创建销售趋势可视化"
```

## 项目结构

```
anygen-skills/
├── slide-generator/      # PPT 生成
├── doc-generator/        # 文档生成
├── diagram-generator/    # 图表生成
├── data-analysis/        # 数据分析
├── deep-research/        # 调研报告
├── financial-research/   # 财务分析
├── image-generator/      # AI 图片生成
├── storybook-generator/  # 故事板
└── website-generator/    # 网站开发
```

## 相关项目

- **[anygen-suite-skill](https://github.com/AnyGenIO/anygen-suite-skill)** — 一体化统一技能（单次安装即可获得所有功能）

## 许可证

MIT
