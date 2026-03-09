# AnyGen AI Skills (Modular)

> Modular AI Skills for [OpenClaw](https://github.com/openclaw/openclaw) / Claude Code / Cursor

A **modular collection** of AI-powered content generation skills using AnyGen. Each skill is a standalone module focused on a specific task — install only what you need.

> 💡 **Looking for an all-in-one solution?** Check out [anygen-suite-skill](https://github.com/AnyGenIO/anygen-suite-skill) for a unified skill that combines all functionalities.

## Skills Included

| Skill | Description |
|-------|-------------|
| `slide-generator` | Professional PPT/presentation generation |
| `doc-generator` | Document and report generation |
| `diagram-generator` | Diagrams, flowcharts, architecture diagrams (SmartDraw) |
| `data-analysis` | Data analysis and visualization |
| `deep-research` | Long-form research reports |
| `financial-research` | Earnings and financial analysis |
| `image-generator` | Design Images and visual assets |
| `storybook-generator` | Visual narratives and Storybook |
| `website-generator` | Landing pages and web development |

## Installation

### OpenClaw
```bash
# Clone the entire collection
git clone https://github.com/AnyGenIO/anygen-skills.git ~/.openclaw/skills/anygen

# Or install individual skills (copy specific directories)
```

### Claude Code
```bash
git clone https://github.com/AnyGenIO/anygen-skills.git ~/.claude/skills/anygen
```

## Configuration

```bash
# Set AnyGen API Key
python3 <skill>/scripts/anygen.py config set api_key "sk-xxx"

# Or use environment variable
export ANYGEN_API_KEY="sk-xxx"
```

Get your API key at [anygen.io/home](https://www.anygen.io/home?auto_create_openclaw_key=1)

## Usage Examples

```
# Slides
"Make a product roadmap PPT"
"Create a quarterly review presentation"

# Documents
"Write a technical design document"
"Generate a project proposal"

# Diagrams
"Draw a microservice architecture diagram"
"Create a user flow diagram"

# Research
"Write an AI industry deep research report"
"Analyze NVIDIA's latest earnings"

# Data
"Organize this data into a table"
"Create a visualization of sales trends"
```

## Project Structure

```
anygen-skills/
├── slide-generator/      # PPT generation
├── doc-generator/        # Document generation
├── diagram-generator/    # Diagram/flowchart generation
├── data-analysis/        # Data analysis
├── deep-research/        # Research reports
├── financial-research/   # Financial analysis
├── image-generator/      # Design Images
├── storybook-generator/  # Storybook creation
└── website-generator/    # Website development
```

## Related Projects

- **[anygen-suite-skill](https://github.com/AnyGenIO/anygen-suite-skill)** — All-in-one unified skill (single installation for all features)

## License

MIT
