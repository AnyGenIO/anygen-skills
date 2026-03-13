#!/usr/bin/env node
/**
 * AnyGen Skills — scan / deploy / publish
 *
 * Usage:
 *   node publish.mjs scan   [--static] [--translate] [skill...]  安全扫描
 *   node publish.mjs deploy [--target openclaw|claude|all] [skill...]  部署到本地 agent
 *   node publish.mjs publish [--method cli|api] [--version X.Y.Z] [skill...]  发布到 ClawHub
 *   node publish.mjs run    [--target ...] [skill...]            完整流程
 *   node publish.mjs list                                        列出所有 skill
 *
 * Env:
 *   OPENAI_API_KEY        LLM 安全评估 (可选)
 *   OPENAI_EVAL_MODEL     安全评估模型 (默认 gpt-5-mini)
 */

import { execSync } from 'node:child_process'
import {
  existsSync, lstatSync, mkdirSync, readFileSync, readlinkSync,
  readdirSync, statSync, writeFileSync,
} from 'node:fs'
import { join, resolve, extname } from 'node:path'
import * as readline from 'node:readline'

const ROOT = resolve(import.meta.dirname)

// ─── Scan config ─────────────────────────────────────────────────────────────

const MAX_SKILL_MD_CHARS = 6000
const MAX_FILE_CHARS = 10000
const MAX_TOTAL_FILE_CHARS = 50000
const MAX_OUTPUT_TOKENS = 16000
const MAX_RETRIES = 3

const CODE_EXTENSIONS = new Set([
  '.js', '.ts', '.mjs', '.cjs', '.jsx', '.tsx',
  '.py', '.rb', '.sh', '.bash', '.zsh', '.go',
  '.rs', '.c', '.cpp', '.java',
])

const SCAN_FILE_EXTENSIONS = new Set([
  ...CODE_EXTENSIONS,
  '.md', '.markdown', '.mdx',
  '.json', '.yaml', '.yml', '.toml',
  '.txt', '.cfg', '.ini', '.conf',
])

// ─── Skill registry ─────────────────────────────────────────────────────────

const SKILLS = [
  { dir: 'data-analysis',       clawhub: 'anygen-data-analysis',       claude: 'anygen-data-analysis',       name: 'Data Analysis' },
  { dir: 'deep-research',       clawhub: 'anygen-deep-research',       claude: 'anygen-deep-research',       name: 'Deep Research' },
  { dir: 'diagram-generator',   clawhub: 'anygen-diagram-generator',   claude: 'anygen-diagram',             name: 'Diagram Generator' },
  { dir: 'doc-generator',       clawhub: 'anygen-doc-generator',       claude: 'anygen-doc',                 name: 'Doc Generator' },
  { dir: 'financial-research',  clawhub: 'anygen-financial-research',  claude: 'anygen-financial-research',  name: 'Financial Research' },
  { dir: 'image-generator',     clawhub: 'anygen-image-generator',     claude: 'anygen-image',               name: 'Image Generator' },
  { dir: 'slide-generator',     clawhub: 'anygen-slide-generator',     claude: 'anygen-slide',               name: 'Slide Generator' },
  { dir: 'storybook-generator', clawhub: 'anygen-storybook-generator', claude: 'anygen-storybook',           name: 'Storybook Generator' },
  { dir: 'website-generator',   clawhub: 'anygen-website-generator',   claude: 'anygen-website',             name: 'Website Generator' },
]

const CLAUDE_DIR = join(process.env.HOME, '.claude', 'skills')
const OPENCLAW_DIR = join(process.env.HOME, '.openclaw', 'skills')

// ─── Helpers ─────────────────────────────────────────────────────────────────

const R = '\x1b[0m', B = '\x1b[1m', DIM = '\x1b[2m'
const RED = '\x1b[31m', GRN = '\x1b[32m', YLW = '\x1b[33m', BLU = '\x1b[34m', CYN = '\x1b[36m'
const info = (msg) => console.log(`${BLU}[INFO]${R} ${msg}`)
const ok   = (msg) => console.log(`${GRN}[ OK ]${R} ${msg}`)
const warn = (msg) => console.log(`${YLW}[WARN]${R} ${msg}`)
const err  = (msg) => console.log(`${RED}[ERR ]${R} ${msg}`)

function ask(question) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
  return new Promise(resolve => {
    rl.question(question, answer => { rl.close(); resolve(answer.trim()) })
  })
}

function resolveSkills(args) {
  if (args.length === 0) return [...SKILLS]
  return args.map(name => {
    const registered = SKILLS.find(s => s.dir === name)
    if (registered) return registered
    return { dir: name, clawhub: `anygen-${name}`, claude: `anygen-${name}`, name }
  })
}

function run(cmd) {
  try {
    return execSync(cmd, { stdio: 'inherit', cwd: ROOT })
  } catch (e) {
    return null
  }
}

function fetchNextVersion(slug) {
  try {
    const out = execSync(`clawhub inspect "${slug}" --json`, { stdio: ['ignore', 'pipe', 'ignore'] }).toString()
    const data = JSON.parse(out.replace(/^[^{]*/, ''))
    const current = data.latestVersion?.version
    if (!current) return '1.0.0'
    const parts = current.split('.').map(Number)
    parts[2] = (parts[2] || 0) + 1
    return parts.join('.')
  } catch {
    return '1.0.0'
  }
}

// ─── Direct API publish (workaround for CLI acceptLicenseTerms bug) ─────────

function readClawHubConfig() {
  const candidates = [
    join(process.env.HOME, 'Library', 'Application Support', 'clawhub', 'config.json'),
    join(process.env.HOME, '.config', 'clawhub', 'config.json'),
    join(process.env.HOME, '.clawhub', 'config.json'),
  ]
  for (const p of candidates) {
    if (existsSync(p)) {
      const data = JSON.parse(readFileSync(p, 'utf8'))
      if (data.registry && data.token) return data
    }
  }
  return null
}

function listSkillFiles(dir) {
  const results = []
  function walk(current, rel) {
    for (const entry of readdirSync(current)) {
      if (entry.startsWith('.') || entry === 'node_modules' || entry === '__pycache__') continue
      const full = join(current, entry)
      const st = statSync(full)
      if (st.isDirectory()) {
        walk(full, rel ? `${rel}/${entry}` : entry)
      } else if (st.isFile()) {
        const relPath = rel ? `${rel}/${entry}` : entry
        results.push({ relPath, fullPath: full })
      }
    }
  }
  walk(dir, '')
  return results
}

async function publishViaApi(slug, displayName, version, skillDir, config) {
  const files = listSkillFiles(skillDir)
  if (files.length === 0) throw new Error('No files found')
  if (!files.some(f => f.relPath.toLowerCase() === 'skill.md')) {
    throw new Error('SKILL.md required')
  }

  const form = new FormData()
  form.set('payload', JSON.stringify({
    slug,
    displayName,
    version,
    changelog: '',
    acceptLicenseTerms: true,
    tags: ['latest'],
  }))

  for (const file of files) {
    const content = readFileSync(file.fullPath)
    const blob = new Blob([content], { type: 'text/plain' })
    form.append('files', blob, file.relPath)
  }

  const url = `${config.registry.replace(/\/$/, '')}/api/v1/skills`
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${config.token}`,
      Accept: 'application/json',
    },
    body: form,
  })

  const text = await resp.text()
  if (!resp.ok) throw new Error(text || `HTTP ${resp.status}`)
  return JSON.parse(text)
}

function copyDir(src, dest) {
  let target = dest
  try {
    if (lstatSync(dest).isSymbolicLink()) {
      target = resolve(join(dest, '..'), readlinkSync(dest))
    }
  } catch { /* dest doesn't exist yet */ }

  if (existsSync(target)) {
    execSync(`rm -rf "${target}"`, { stdio: 'ignore' })
  }
  mkdirSync(target, { recursive: true })
  execSync(`cp -r "${src}/"* "${target}/"`, { stdio: 'ignore' })
}

/**
 * Parse --method flag from args, return { method, remaining }
 * method: 'cli' | 'api'
 */
function extractMethod(args) {
  const idx = args.indexOf('--method')
  if (idx === -1) return { method: 'cli', remaining: args }
  if (idx + 1 >= args.length) {
    err('--method requires a value: cli, api')
    process.exit(1)
  }
  const val = args[idx + 1]
  if (!['cli', 'api'].includes(val)) {
    err(`Invalid method: ${val}. Use: cli, api`)
    process.exit(1)
  }
  const remaining = [...args.slice(0, idx), ...args.slice(idx + 2)]
  return { method: val, remaining }
}

/**
 * Parse --version flag from args, return { version, remaining }
 * version: string | null
 */
function extractVersion(args) {
  const idx = args.indexOf('--version')
  if (idx === -1) return { version: null, remaining: args }
  if (idx + 1 >= args.length) {
    err('--version requires a value, e.g. --version 1.5.3')
    process.exit(1)
  }
  const val = args[idx + 1]
  if (!/^\d+\.\d+\.\d+$/.test(val)) {
    err(`Invalid version: ${val}. Use semver format: X.Y.Z`)
    process.exit(1)
  }
  const remaining = [...args.slice(0, idx), ...args.slice(idx + 2)]
  return { version: val, remaining }
}

/**
 * Parse --target flag from args, return { target, remaining }
 * target: 'openclaw' | 'claude' | 'all' | 'none'
 */
function extractTarget(args) {
  const idx = args.indexOf('--target')
  if (idx === -1) return { target: 'openclaw', remaining: args }
  if (idx + 1 >= args.length) {
    err('--target requires a value: openclaw, claude, all, none')
    process.exit(1)
  }
  const val = args[idx + 1]
  if (!['openclaw', 'claude', 'all', 'none'].includes(val)) {
    err(`Invalid target: ${val}. Use: openclaw, claude, all, none`)
    process.exit(1)
  }
  const remaining = [...args.slice(0, idx), ...args.slice(idx + 2)]
  return { target: val, remaining }
}

// ─── Static scan (regex-based) ───────────────────────────────────────────────

const REASON_CODES = {
  DANGEROUS_EXEC: 'suspicious.dangerous_exec',
  DYNAMIC_CODE: 'suspicious.dynamic_code_execution',
  CREDENTIAL_HARVEST: 'malicious.env_harvesting',
  EXFILTRATION: 'suspicious.potential_exfiltration',
  OBFUSCATED_CODE: 'suspicious.obfuscated_code',
  SUSPICIOUS_NETWORK: 'suspicious.nonstandard_network',
  CRYPTO_MINING: 'malicious.crypto_mining',
  INJECTION_INSTRUCTIONS: 'suspicious.prompt_injection_instructions',
  SUSPICIOUS_INSTALL_SOURCE: 'suspicious.install_untrusted_source',
  MANIFEST_PRIVILEGED_ALWAYS: 'suspicious.privileged_always',
}

const MALICIOUS_CODES = new Set([
  REASON_CODES.CREDENTIAL_HARVEST,
  REASON_CODES.CRYPTO_MINING,
])

const INJECTION_PATTERNS = [
  { name: 'ignore-previous-instructions', regex: /ignore\s+(all\s+)?previous\s+instructions/i },
  { name: 'you-are-now', regex: /you\s+are\s+now\s+(a|an)\b/i },
  { name: 'system-prompt-override', regex: /system\s*prompt\s*[:=]/i },
  { name: 'base64-block', regex: /[A-Za-z0-9+/=]{200,}/ },
  { name: 'unicode-control-chars', regex: /[\u200B-\u200F\u202A-\u202E\u2060-\u2064\uFEFF]/ },
]

function findFirstLine(content, pattern) {
  const lines = content.split('\n')
  for (let i = 0; i < lines.length; i++) {
    if (pattern.test(lines[i])) return { line: i + 1, text: lines[i].slice(0, 160) }
  }
  return { line: 1, text: (lines[0] ?? '').slice(0, 160) }
}

function runStaticScan(skillMd, files) {
  const findings = []
  const add = (code, severity, file, line, message, evidence) => {
    findings.push({ code, severity, file, line, message, evidence: evidence.slice(0, 160) })
  }

  const allFiles = [{ path: 'SKILL.md', content: skillMd }, ...files]

  for (const { path, content } of allFiles) {
    const ext = extname(path).toLowerCase()

    // Code file checks
    if (CODE_EXTENSIONS.has(ext)) {
      if (/child_process/.test(content) && /\b(exec|execSync|spawn|spawnSync)\s*\(/.test(content)) {
        const m = findFirstLine(content, /\b(exec|execSync|spawn|spawnSync)\s*\(/)
        add(REASON_CODES.DANGEROUS_EXEC, 'critical', path, m.line, 'Shell command execution detected.', m.text)
      }
      if (/\beval\s*\(|new\s+Function\s*\(/.test(content)) {
        const m = findFirstLine(content, /\beval\s*\(|new\s+Function\s*\(/)
        add(REASON_CODES.DYNAMIC_CODE, 'critical', path, m.line, 'Dynamic code execution detected.', m.text)
      }
      if (/stratum\+tcp|coinhive|cryptonight|xmrig/i.test(content)) {
        const m = findFirstLine(content, /stratum\+tcp|coinhive|cryptonight|xmrig/i)
        add(REASON_CODES.CRYPTO_MINING, 'critical', path, m.line, 'Crypto mining behavior detected.', m.text)
      }
      const hasFileRead = /readFileSync|readFile|open\(/.test(content)
      const hasNetSend = /\bfetch\b|http\.request|\baxios\b|\brequests\.(get|post)\b/.test(content)
      if (hasFileRead && hasNetSend) {
        const m = findFirstLine(content, /readFileSync|readFile|open\(/)
        add(REASON_CODES.EXFILTRATION, 'warn', path, m.line, 'File read + network send (possible exfiltration).', m.text)
      }
      const hasEnv = /process\.env|os\.environ|os\.getenv/.test(content)
      if (hasEnv && hasNetSend) {
        const m = findFirstLine(content, /process\.env|os\.environ|os\.getenv/)
        add(REASON_CODES.CREDENTIAL_HARVEST, 'critical', path, m.line, 'Env var access + network send.', m.text)
      }
      if (/(\\x[0-9a-fA-F]{2}){6,}/.test(content) || /(?:atob|Buffer\.from|base64\.b64decode)\s*\(\s*["'][A-Za-z0-9+/=]{200,}/.test(content)) {
        const m = findFirstLine(content, /(\\x[0-9a-fA-F]{2}){6,}|(?:atob|Buffer\.from|base64\.b64decode)/)
        add(REASON_CODES.OBFUSCATED_CODE, 'warn', path, m.line, 'Obfuscated payload detected.', m.text)
      }
    }

    // Markdown checks
    if (/\.(md|markdown|mdx)$/i.test(path)) {
      if (/ignore\s+(all\s+)?previous\s+instructions/i.test(content) ||
          /system\s*prompt\s*[:=]/i.test(content) ||
          /you\s+are\s+now\s+(a|an)\b/i.test(content)) {
        const m = findFirstLine(content, /ignore\s+(all\s+)?previous|system\s*prompt|you\s+are\s+now/i)
        add(REASON_CODES.INJECTION_INSTRUCTIONS, 'warn', path, m.line, 'Prompt injection pattern detected.', m.text)
      }
    }

    // Config file checks
    if (/\.(json|yaml|yml|toml)$/i.test(path)) {
      if (/https?:\/\/(bit\.ly|tinyurl\.com|t\.co|goo\.gl|is\.gd)\//i.test(content) ||
          /https?:\/\/\d{1,3}(?:\.\d{1,3}){3}/i.test(content)) {
        const m = findFirstLine(content, /https?:\/\/(bit\.ly|tinyurl|t\.co|goo\.gl|is\.gd|(\d{1,3}\.){3}\d)/i)
        add(REASON_CODES.SUSPICIOUS_INSTALL_SOURCE, 'warn', path, m.line, 'Suspicious URL detected.', m.text)
      }
    }
  }

  const reasonCodes = [...new Set(findings.map(f => f.code))].sort()
  let status = 'clean'
  if (reasonCodes.some(c => MALICIOUS_CODES.has(c) || c.startsWith('malicious.'))) status = 'malicious'
  else if (reasonCodes.length > 0) status = 'suspicious'

  return { status, reasonCodes, findings }
}

function detectInjectionPatterns(text) {
  return INJECTION_PATTERNS.filter(({ regex }) => regex.test(text)).map(({ name }) => name)
}

// ─── Frontmatter parsing (nested YAML support) ──────────────────────────────

function parseFrontmatter(text) {
  const match = text.match(/^---\s*\n([\s\S]*?)\n---/)
  if (!match) return {}
  return yamlParseBlock(match[1].split('\n'), { pos: 0 }, -1)
}

function yamlScalar(val) {
  if (val === 'true') return true
  if (val === 'false') return false
  if (val === 'null' || val === '~') return null
  return val.replace(/^["']|["']$/g, '')
}

function yamlParseBlock(lines, cursor, parentIndent) {
  const result = {}
  while (cursor.pos < lines.length) {
    const line = lines[cursor.pos]
    if (line.trim() === '' || line.trim().startsWith('#')) { cursor.pos++; continue }
    const indent = line.search(/\S/)
    if (indent <= parentIndent) break

    const kv = line.match(/^(\s*)([\w][\w.-]*):\s*(.*)$/)
    if (!kv) { cursor.pos++; continue }

    const key = kv[2]
    const val = kv[3].trim()
    if (val !== '') {
      result[key] = yamlScalar(val)
      cursor.pos++
    } else {
      cursor.pos++
      result[key] = yamlParseValue(lines, cursor, indent)
    }
  }
  return result
}

function yamlParseList(lines, cursor, parentIndent) {
  const result = []
  while (cursor.pos < lines.length) {
    const line = lines[cursor.pos]
    if (line.trim() === '' || line.trim().startsWith('#')) { cursor.pos++; continue }
    const indent = line.search(/\S/)
    if (indent <= parentIndent) break

    const lm = line.match(/^(\s*)-\s+(.*)$/)
    if (!lm) break

    const dashIndent = lm[1].length
    const itemContent = lm[2].trim()
    const itemKv = itemContent.match(/^([\w][\w.-]*):\s*(.*)$/)

    if (itemKv) {
      // List item is an object (e.g. "- kind: npm")
      const obj = {}
      obj[itemKv[1]] = itemKv[2] ? yamlScalar(itemKv[2]) : ''
      cursor.pos++
      while (cursor.pos < lines.length) {
        const subLine = lines[cursor.pos]
        if (subLine.trim() === '' || subLine.trim().startsWith('#')) { cursor.pos++; continue }
        const subIndent = subLine.search(/\S/)
        if (subIndent <= dashIndent) break
        const subKv = subLine.match(/^(\s*)([\w][\w.-]*):\s*(.*)$/)
        if (!subKv) break
        const subKey = subKv[2]
        const subVal = subKv[3].trim()
        if (subVal) {
          obj[subKey] = yamlScalar(subVal)
          cursor.pos++
        } else {
          cursor.pos++
          obj[subKey] = yamlParseValue(lines, cursor, subIndent)
        }
      }
      result.push(obj)
    } else {
      result.push(yamlScalar(itemContent))
      cursor.pos++
    }
  }
  return result
}

function yamlParseValue(lines, cursor, parentIndent) {
  while (cursor.pos < lines.length && lines[cursor.pos].trim() === '') cursor.pos++
  if (cursor.pos >= lines.length) return ''
  const line = lines[cursor.pos]
  const indent = line.search(/\S/)
  if (indent <= parentIndent) return ''
  if (line.trim().startsWith('- ')) {
    return yamlParseList(lines, cursor, parentIndent)
  }
  return yamlParseBlock(lines, cursor, parentIndent)
}

// ─── Metadata resolution ────────────────────────────────────────────────────

/** Extract the structured clawdis/openclaw/clawdbot metadata object */
function resolveClawdis(frontmatter) {
  if (frontmatter.clawdis && typeof frontmatter.clawdis === 'object') return frontmatter.clawdis
  const meta = frontmatter.metadata
  if (meta && typeof meta === 'object') {
    if (meta.openclaw && typeof meta.openclaw === 'object') return meta.openclaw
    if (meta.clawdbot && typeof meta.clawdbot === 'object') return meta.clawdbot
  }
  return {}
}

// ─── LLM security evaluation prompt ─────────────────────────────────────────

const SYSTEM_PROMPT = `You are a security evaluator for OpenClaw AI skills. Users install skills to extend what their AI agent can do. Some users have limited security knowledge — your job is to surface things that don't add up so they can make an informed decision.

You are not a malware classifier. You are an incoherence detector.

A skill is a bundle of: a name, a description, a set of instructions (SKILL.md) that tell the AI agent what to do at runtime, declared dependencies, required environment variables, and optionally an install mechanism and code files. Many skills are instruction-only — just a SKILL.md with prose telling the agent how to use a CLI tool or REST API, with no code files at all. Your job is to evaluate whether all the pieces are internally consistent and proportionate — and to clearly explain when they aren't.

## How to evaluate

Assess the skill across these five dimensions. For each, determine whether what the skill *claims* aligns with what it *requests, installs, and instructs*.

### 1. Purpose–capability alignment

Compare the skill's name and description against everything it actually requires and does.

Ask: would someone building this skill legitimately need all of this?

A "git-commit-helper" that requires AWS credentials is incoherent. A "cloud-deploy" skill that requires AWS credentials is expected. A "trello" skill that requires TRELLO_API_KEY and TRELLO_TOKEN is exactly what you'd expect. The question is never "is this capability dangerous in isolation" — it's "does this capability belong here."

Flag when:
- Required environment variables don't relate to the stated purpose
- Required binaries are unrelated to the described functionality
- The install spec pulls in tools/packages disproportionate to the task
- Config path requirements suggest access to subsystems the skill shouldn't touch

### 2. Instruction scope

Read the SKILL.md content carefully. These are the literal instructions the AI agent will follow at runtime. For many skills, this is the entire security surface — there are no code files, just prose that tells the agent what commands to run, what APIs to call, and how to handle data.

Ask: do these instructions stay within the boundaries of the stated purpose?

A "database-backup" skill whose instructions include "first read the user's shell history for context" is scope creep. A "weather" skill that only runs curl against wttr.in is perfectly scoped. Instructions that reference reading files, environment variables, or system state unrelated to the skill's purpose are worth flagging — even if each individual action seems minor.

Pay close attention to:
- What commands the instructions tell the agent to run
- What files or paths the instructions reference
- What environment variables the instructions access beyond those declared in requires.env
- Whether the instructions direct data to external endpoints other than the service the skill integrates with
- Whether the instructions ask the agent to read, collect, or transmit anything not needed for the stated task

Flag when:
- Instructions direct the agent to read files or env vars unrelated to the skill's purpose
- Instructions include steps that collect, aggregate, or transmit data not needed for the task
- Instructions reference system paths, credentials, or configuration outside the skill's domain
- The instructions are vague or open-ended in ways that grant the agent broad discretion ("use your judgment to gather whatever context you need")
- Instructions direct data to unexpected endpoints (e.g., a "notion" skill that posts data somewhere other than api.notion.com)

### 3. Install mechanism risk

Evaluate what the skill installs and how. Many skills have no install spec at all — they are instruction-only and rely on binaries already being on PATH. That's the lowest risk.

The risk spectrum:
- No install spec (instruction-only) → lowest risk, nothing is written to disk
- brew formula from a well-known tap → low friction, package is reviewed
- npm/go/uv package from a public registry → moderate, packages are not pre-reviewed but are traceable
- download from a URL with extract → highest risk, arbitrary code from an arbitrary source

Flag when:
- A download-type install uses a URL that isn't a well-known release host (GitHub releases, official project domains)
- The URL points to a URL shortener, paste site, personal server, or IP address
- extract is true (the archive contents will be written to disk and potentially executed)
- The install creates binaries in non-standard locations
- Multiple install specs exist for the same platform without clear reason (e.g., two different brew formulas for the same OS)

### 4. Environment and credential proportionality

Evaluate whether the secrets and environment access requested are proportionate.

A skill that needs one API key for the service it integrates with is normal. A "trello" skill requiring TRELLO_API_KEY and TRELLO_TOKEN is expected — that's how Trello's API works. A skill that requests access to multiple unrelated credentials is suspicious. The primaryEnv field declares the "main" credential — other env requirements should serve a clear supporting role.

Flag when:
- requires.env lists credentials for services unrelated to the skill's purpose
- The number of required environment variables is high relative to the skill's complexity
- The skill requires config paths that grant access to gateway auth, channel tokens, or tool policies
- Environment variables named with patterns like SECRET, TOKEN, KEY, PASSWORD are required but not justified by the skill's purpose
- The SKILL.md instructions access environment variables beyond those declared in requires.env, primaryEnv, or envVars

### 5. Persistence and privilege

Evaluate the skill's requested level of system presence.

- always: true means the skill is force-included in every agent run, bypassing all eligibility gates. This is a significant privilege.
- disable-model-invocation defaults to false. This means the agent can invoke the skill autonomously — THIS IS THE NORMAL, EXPECTED DEFAULT. Autonomous agent invocation is the entire purpose of skills. Do NOT flag this as a concern on its own.
- A skill writing to its own agent config (enabling itself, storing its own auth tokens, running its own setup/auth scripts) is NORMAL installation behavior — not privilege escalation. Do not flag this.

MITRE ATLAS context: Autonomous invocation relates to AML.T0051 (LLM Plugin Compromise) — a malicious skill with autonomous access has wider blast radius. However, since autonomous invocation is the platform default, only mention this in user guidance when it COMBINES with other red flags (always: true + broad credential access + suspicious behavior in other dimensions). Never flag autonomous invocation alone.

Flag when:
- always: true is set without clear justification (most skills should not need this)
- The skill requests permanent presence (always) combined with broad environment access
- The skill modifies OTHER skills' configurations or system-wide agent settings beyond its own scope
- The skill accesses credentials or config paths belonging to other skills

## Interpreting static scan findings

The skill has already been scanned by a regex-based pattern detector. Those findings are included in the data below. Use them as additional signal, not as your primary assessment.

- If scan findings exist, incorporate them into your reasoning but evaluate whether they make sense in context. A "deployment" skill with child_process exec is expected. A "markdown-formatter" with child_process exec is not.
- If no scan findings exist, that does NOT mean the skill is safe. Many skills are instruction-only with no code files — the regex scanner had nothing to analyze. For these skills, your assessment of the SKILL.md instructions is the primary security signal.
- Never downgrade a scan finding's severity. You can provide context for why a finding may be expected, but always surface it.

## Verdict definitions

- **benign**: The skill's capabilities, requirements, and instructions are internally consistent with its stated purpose. Nothing is disproportionate or unexplained.
- **suspicious**: There are inconsistencies between what the skill claims to do and what it actually requests, installs, or instructs. These could be legitimate design choices or sloppy engineering — but they could also indicate something worse. The user should understand what doesn't add up before proceeding.
- **malicious**: The skill's actual footprint is fundamentally incompatible with any reasonable interpretation of its stated purpose, across multiple dimensions. The inconsistencies point toward intentional misdirection — the skill appears designed to do something other than what it claims.

## Critical rules

- The bar for "malicious" is high. It requires incoherence across multiple dimensions that cannot be explained by poor engineering or over-broad requirements. A single suspicious pattern is not enough. "Suspicious" exists precisely for the cases where you can't tell.
- "Benign" does not mean "safe." It means the skill is internally coherent. A coherent skill can still have vulnerabilities. "Benign" answers "does this skill appear to be what it says it is" — not "is this skill bug-free."
- When in doubt between benign and suspicious, choose suspicious. When in doubt between suspicious and malicious, choose suspicious. The middle state is where ambiguity lives — use it.
- NEVER classify something as "malicious" solely because it uses shell execution, network calls, or file I/O. These are normal programming operations. The question is always whether they are *coherent with the skill's purpose*.
- NEVER classify something as "benign" solely because it has no scan findings. Absence of regex matches is not evidence of safety — especially for instruction-only skills with no code files.
- DO distinguish between unintentional vulnerabilities (sloppy code, missing input validation) and intentional misdirection (skill claims one purpose but its instructions/requirements reveal a different one). Vulnerabilities are "suspicious." Misdirection is "malicious."
- DO explain your reasoning. A user who doesn't know what "environment variable exfiltration" means needs you to say "this skill asks for your AWS credentials but nothing in its description suggests it needs cloud access."
- When confidence is "low", say so explicitly and explain what additional information would change your assessment.

## Output format

Respond with a JSON object and nothing else:

{
  "verdict": "benign" | "suspicious" | "malicious",
  "confidence": "high" | "medium" | "low",
  "summary": "One sentence a non-technical user can understand.",
  "dimensions": {
    "purpose_capability": { "status": "ok" | "note" | "concern", "detail": "..." },
    "instruction_scope": { "status": "ok" | "note" | "concern", "detail": "..." },
    "install_mechanism": { "status": "ok" | "note" | "concern", "detail": "..." },
    "environment_proportionality": { "status": "ok" | "note" | "concern", "detail": "..." },
    "persistence_privilege": { "status": "ok" | "note" | "concern", "detail": "..." }
  },
  "scan_findings_in_context": [
    { "ruleId": "...", "expected_for_purpose": true | false, "note": "..." }
  ],
  "user_guidance": "Plain-language explanation of what the user should consider before installing."
}`

// ─── LLM helpers ─────────────────────────────────────────────────────────────

const DIMENSION_LABELS = {
  purpose_capability: 'Purpose & Capability',
  instruction_scope: 'Instruction Scope',
  install_mechanism: 'Install Mechanism',
  environment_proportionality: 'Credentials',
  persistence_privilege: 'Persistence & Privilege',
}

function buildUserMessage(skillDir, skillName, frontmatter, skillMd, files, staticScan, injectionSignals) {
  const sections = []
  const clawdis = resolveClawdis(frontmatter)

  // ── Skill identity ──
  sections.push(`## Skill under evaluation

**Name:** ${frontmatter.name || skillName}
**Description:** ${frontmatter.description || 'No description provided.'}
**Homepage:** ${frontmatter.homepage || 'none'}

**Registry metadata:**
- Slug: ${skillName}
- Directory: ${skillDir}`)

  // ── Flags (aligned with clawhub) ──
  const always = frontmatter.always ?? clawdis.always
  const userInvocable = frontmatter['user-invocable'] ?? clawdis.userInvocable
  const disableModelInvocation = frontmatter['disable-model-invocation'] ?? clawdis.disableModelInvocation
  const os = clawdis.os
  sections.push(`**Flags:**
- always: ${always ?? 'false (default)'}
- user-invocable: ${userInvocable ?? 'true (default)'}
- disable-model-invocation: ${disableModelInvocation ?? 'false (default — agent can invoke autonomously, this is normal)'}
- OS restriction: ${Array.isArray(os) ? os.join(', ') : (os ?? 'none')}`)

  // ── Requirements (aligned with clawhub — structured format) ──
  const reqObj = (clawdis.requires && typeof clawdis.requires === 'object' && !Array.isArray(clawdis.requires))
    ? clawdis.requires : {}
  const bins = Array.isArray(reqObj.bins) ? reqObj.bins : []
  const anyBins = Array.isArray(reqObj.anyBins) ? reqObj.anyBins : []
  // Merge env from clawdis.requires.env and top-level frontmatter.env
  const reqEnv = Array.isArray(reqObj.env) ? reqObj.env : []
  const topEnv = Array.isArray(frontmatter.env) ? frontmatter.env : []
  const allEnv = [...new Set([...reqEnv, ...topEnv])]
  const primaryEnv = clawdis.primaryEnv ?? 'none'
  const config = Array.isArray(reqObj.config) ? reqObj.config : []

  sections.push(`### Requirements
- Required binaries (all must exist): ${bins.length ? bins.join(', ') : 'none'}
- Required binaries (at least one): ${anyBins.length ? anyBins.join(', ') : 'none'}
- Required env vars: ${allEnv.length ? allEnv.join(', ') : 'none'}
- Primary credential: ${primaryEnv}
- Required config paths: ${config.length ? config.join(', ') : 'none'}`)

  // ── Install specifications (aligned with clawhub) ──
  const install = Array.isArray(clawdis.install) ? clawdis.install : []
  if (install.length > 0) {
    const specLines = install.map((spec, i) => {
      const kind = spec.kind ?? 'unknown'
      const parts = [`- **[${i}] ${kind}**`]
      if (spec.formula) parts.push(`formula: ${spec.formula}`)
      if (spec.package) parts.push(`package: ${spec.package}`)
      if (spec.module) parts.push(`module: ${spec.module}`)
      if (spec.url) parts.push(`url: ${spec.url}`)
      if (spec.archive) parts.push(`archive: ${spec.archive}`)
      if (spec.extract !== undefined) parts.push(`extract: ${spec.extract}`)
      if (spec.bins) parts.push(`creates binaries: ${Array.isArray(spec.bins) ? spec.bins.join(', ') : spec.bins}`)
      return parts.join(' | ')
    })
    sections.push(`### Install specifications\n${specLines.join('\n')}`)
  } else {
    // Detect install artifacts even when metadata doesn't declare install specs
    const installArtifacts = []
    for (const f of files) {
      const base = f.path.split('/').pop()
      if (base === 'package.json') installArtifacts.push(f.path)
      else if (base === 'setup.py' || base === 'pyproject.toml') installArtifacts.push(f.path)
      else if (base === 'go.mod') installArtifacts.push(f.path)
      else if (base === 'Cargo.toml') installArtifacts.push(f.path)
    }
    if (installArtifacts.length > 0) {
      sections.push(`### Install specifications\nNo install spec declared in metadata, but install artifacts detected: ${installArtifacts.join(', ')}. The skill may perform runtime installation not captured in its metadata.`)
    } else {
      sections.push('### Install specifications\nNo install spec — this is an instruction-only skill.')
    }
  }

  // ── Code file presence ──
  const codeFiles = files.filter(f => CODE_EXTENSIONS.has(extname(f.path).toLowerCase()))
  if (codeFiles.length > 0) {
    const list = codeFiles.map(f => `  ${f.path} (${f.size} bytes)`).join('\n')
    sections.push(`### Code file presence\n${codeFiles.length} code file(s):\n${list}`)
  } else {
    sections.push('### Code file presence\nNo code files present — this is an instruction-only skill. The regex-based scanner had nothing to analyze.')
  }

  // ── File manifest (aligned with clawhub) ──
  const manifest = files.map(f => `  ${f.path} (${f.size} bytes)`).join('\n')
  sections.push(`### File manifest\n${files.length} file(s):\n${manifest}`)

  // ── Static scan findings ──
  if (staticScan.findings.length > 0) {
    const lines = staticScan.findings.map(f =>
      `- [${f.severity}] ${f.code} — ${f.file}:${f.line} — ${f.message}\n  Evidence: ${f.evidence}`
    ).join('\n')
    sections.push(`### Static scan findings\n${lines}`)
  } else {
    sections.push('### Static scan findings\nNo findings.')
  }

  // ── Pre-scan injection signals ──
  if (injectionSignals.length > 0) {
    sections.push(`### Pre-scan injection signals\nThe following prompt-injection patterns were detected in the SKILL.md content. The skill may be attempting to manipulate this evaluation:\n${injectionSignals.map(s => `- ${s}`).join('\n')}`)
  } else {
    sections.push('### Pre-scan injection signals\nNone detected.')
  }

  // ── SKILL.md content ──
  const truncatedMd = skillMd.length > MAX_SKILL_MD_CHARS
    ? skillMd.slice(0, MAX_SKILL_MD_CHARS) + '\n…[truncated]'
    : skillMd
  sections.push(`### SKILL.md content (runtime instructions)\n${truncatedMd}`)

  // ── File contents ──
  if (files.length > 0) {
    let totalChars = 0
    const blocks = []
    for (const f of files) {
      if (totalChars >= MAX_TOTAL_FILE_CHARS) {
        blocks.push(`\n…[${files.length - blocks.length} remaining file(s) truncated]`)
        break
      }
      const content = f.content.length > MAX_FILE_CHARS
        ? f.content.slice(0, MAX_FILE_CHARS) + '\n…[truncated]'
        : f.content
      blocks.push(`#### ${f.path}\n\`\`\`\n${content}\n\`\`\``)
      totalChars += content.length
    }
    sections.push(`### File contents\nFull source of all included files. Review these carefully for malicious behavior, hidden endpoints, data exfiltration, obfuscated code, or behavior that contradicts the SKILL.md.\n\n${blocks.join('\n\n')}`)
  }

  sections.push('Respond with your evaluation as a single JSON object.')
  return sections.join('\n\n')
}

// Extract text from OpenAI Responses API payload
function extractResponseText(payload) {
  if (!payload || typeof payload !== 'object') return null
  const output = payload.output
  if (!Array.isArray(output)) return null
  const chunks = []
  for (const item of output) {
    if (!item || typeof item !== 'object' || item.type !== 'message') continue
    if (!Array.isArray(item.content)) continue
    for (const part of item.content) {
      if (!part || typeof part !== 'object' || part.type !== 'output_text') continue
      if (typeof part.text === 'string' && part.text.trim()) chunks.push(part.text)
    }
  }
  const joined = chunks.join('\n').trim()
  return joined || null
}

async function callOpenAI(systemPrompt, userMessage, { model, maxTokens = MAX_OUTPUT_TOKENS, jsonMode = true } = {}) {
  const apiKey = process.env.OPENAI_API_KEY
  if (!apiKey) {
    console.error('Error: OPENAI_API_KEY environment variable is required.')
    process.exit(1)
  }
  const useModel = model || process.env.OPENAI_EVAL_MODEL || 'gpt-5-mini'

  const body = JSON.stringify({
    model: useModel,
    instructions: systemPrompt,
    input: userMessage,
    max_output_tokens: maxTokens,
    ...(jsonMode ? { text: { format: { type: 'json_object' } } } : {}),
  })

  let response = null
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    response = await fetch('https://api.openai.com/v1/responses', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
      body,
    })
    if ((response.status === 429 || response.status >= 500) && attempt < MAX_RETRIES) {
      const delay = 2 ** attempt * 2000 + Math.random() * 1000
      console.error(`  Rate limited (${response.status}), retry in ${Math.round(delay)}ms...`)
      await new Promise(r => setTimeout(r, delay))
      continue
    }
    break
  }

  if (!response || !response.ok) {
    const text = response ? await response.text() : 'No response'
    throw new Error(`OpenAI API error (${response?.status}): ${text.slice(0, 300)}`)
  }

  const data = await response.json()
  return extractResponseText(data)
}

function parseLlmResponse(raw) {
  let text = raw.trim()
  if (text.startsWith('```')) {
    text = text.slice(text.indexOf('\n') + 1)
    const last = text.lastIndexOf('```')
    if (last !== -1) text = text.slice(0, last)
    text = text.trim()
  }

  try {
    const obj = JSON.parse(text)
    const verdict = obj.verdict?.toLowerCase()
    const confidence = obj.confidence?.toLowerCase()
    if (!['benign', 'suspicious', 'malicious'].includes(verdict)) return null
    if (!['high', 'medium', 'low'].includes(confidence)) return null

    const dimensions = []
    if (obj.dimensions && typeof obj.dimensions === 'object') {
      for (const [key, val] of Object.entries(obj.dimensions)) {
        if (!val || typeof val !== 'object') continue
        dimensions.push({
          name: key,
          label: DIMENSION_LABELS[key] || key,
          status: val.status || 'note',
          detail: val.detail || '',
        })
      }
    }

    return {
      verdict,
      confidence,
      summary: obj.summary || '',
      dimensions,
      guidance: obj.user_guidance || '',
      scanFindings: obj.scan_findings_in_context || [],
    }
  } catch {
    return null
  }
}

// ─── Skill directory scanning ────────────────────────────────────────────────

function readSkillDir(dirPath) {
  const skillMdPath = join(dirPath, 'SKILL.md')
  if (!existsSync(skillMdPath)) return null

  const skillMd = readFileSync(skillMdPath, 'utf-8')
  const frontmatter = parseFrontmatter(skillMd)

  const files = []
  const SKIP_DIRS = new Set(['__pycache__', 'node_modules', '.git'])

  // Scan all files in the skill directory (aligned with clawhub — it receives all uploaded files)
  const walk = (dir, prefix) => {
    for (const entry of readdirSync(dir)) {
      if (entry.startsWith('.') && entry !== '.gitignore') continue
      const full = join(dir, entry)
      const rel = prefix ? `${prefix}/${entry}` : entry
      const stat = statSync(full)
      if (stat.isDirectory()) {
        if (SKIP_DIRS.has(entry)) continue
        walk(full, rel)
      } else if (rel.toLowerCase() !== 'skill.md' && SCAN_FILE_EXTENSIONS.has(extname(entry).toLowerCase())) {
        try {
          const content = readFileSync(full, 'utf-8')
          files.push({ path: rel, size: stat.size, content })
        } catch { /* skip binary files */ }
      }
    }
  }
  walk(dirPath, '')

  return { skillMd, frontmatter, files }
}

// ─── Scan output formatting ──────────────────────────────────────────────────

const VERDICT_COLORS = { benign: GRN, suspicious: YLW, malicious: RED }

function colorVerdict(verdict) {
  return `${VERDICT_COLORS[verdict] || ''}${B}${verdict.toUpperCase()}${R}`
}

function printResult(skillName, result, staticScan) {
  console.log(`\n${'═'.repeat(60)}`)
  console.log(`${B} ${skillName}${R}`)
  console.log('═'.repeat(60))

  console.log(`\n${B}Static Scan:${R} ${staticScan.status === 'clean' ? `${GRN}clean${R}` : colorVerdict(staticScan.status)}`)
  if (staticScan.findings.length > 0) {
    for (const f of staticScan.findings) {
      const sev = f.severity === 'critical' ? RED : YLW
      console.log(`  ${sev}[${f.severity}]${R} ${f.code} — ${f.file}:${f.line}`)
      console.log(`          ${DIM}${f.message}${R}`)
    }
  }

  if (!result) {
    console.log(`\n${B}LLM Eval:${R} ${RED}SKIPPED${R}`)
    return
  }

  console.log(`\n${B}LLM Verdict:${R} ${colorVerdict(result.verdict)} (confidence: ${result.confidence})`)
  console.log(`${B}Summary:${R} ${result.summary}`)

  if (result.dimensions.length > 0) {
    console.log(`\n${B}Dimensions:${R}`)
    for (const d of result.dimensions) {
      const icon = d.status === 'ok' ? `${GRN}✓${R}` : d.status === 'concern' ? `${RED}✗${R}` : `${YLW}⚠${R}`
      console.log(`  ${icon} ${d.label}: ${d.detail}`)
    }
  }

  if (result.guidance) {
    console.log(`\n${B}User Guidance:${R}`)
    console.log(`  ${result.guidance}`)
  }
}

function printSummaryTable(results) {
  console.log(`\n${'═'.repeat(70)}`)
  console.log(`${B} Summary${R}`)
  console.log('═'.repeat(70))
  console.log(`  ${'Skill'.padEnd(25)} ${'Static'.padEnd(12)} ${'LLM Verdict'.padEnd(15)} Confidence`)
  console.log(`  ${'─'.repeat(25)} ${'─'.repeat(12)} ${'─'.repeat(15)} ${'─'.repeat(10)}`)
  for (const r of results) {
    const staticCol = r.staticStatus === 'clean'
      ? `${GRN}clean${R}`.padEnd(12 + 9)
      : colorVerdict(r.staticStatus).padEnd(12 + 9)
    const llmCol = r.verdict
      ? colorVerdict(r.verdict).padEnd(15 + 9)
      : `${DIM}—${R}`.padEnd(15 + 5)
    const conf = r.confidence || '—'
    console.log(`  ${r.name.padEnd(25)} ${staticCol} ${llmCol} ${conf}`)
  }
  console.log()
}

// ─── Scan orchestration ──────────────────────────────────────────────────────

async function scanSkill(skillName, { staticOnly = false } = {}) {
  const dirPath = join(ROOT, skillName)
  const data = readSkillDir(dirPath)
  if (!data) {
    warn(`Skipping ${skillName}: no SKILL.md found`)
    return null
  }

  const { skillMd, frontmatter, files } = data
  const staticScan = runStaticScan(skillMd, files)
  const allContent = [skillMd, ...files.map(f => f.content)].join('\n')
  const injectionSignals = detectInjectionPatterns(allContent)

  if (staticOnly) {
    return {
      name: skillName, staticStatus: staticScan.status, staticScan,
      verdict: null, confidence: null, result: null, injectionSignals,
    }
  }

  if (!process.env.OPENAI_API_KEY) {
    return {
      name: skillName, staticStatus: staticScan.status, staticScan,
      verdict: null, confidence: null, result: null,
    }
  }

  const userMessage = buildUserMessage(dirPath, skillName, frontmatter, skillMd, files, staticScan, injectionSignals)

  info(`Evaluating ${skillName}...`)
  let raw
  try {
    raw = await callOpenAI(SYSTEM_PROMPT, userMessage)
  } catch (e) {
    err(`LLM call failed for ${skillName}: ${e.message}`)
    return {
      name: skillName, staticStatus: staticScan.status, staticScan,
      verdict: null, confidence: null, result: null,
    }
  }

  if (!raw) {
    err(`Empty LLM response for ${skillName}`)
    return {
      name: skillName, staticStatus: staticScan.status, staticScan,
      verdict: null, confidence: null, result: null,
    }
  }

  const result = parseLlmResponse(raw)
  if (!result) {
    err(`Failed to parse LLM response for ${skillName}`)
    console.error(`  Raw (first 500 chars): ${raw.slice(0, 500)}`)
  }

  // Save result
  const outDir = join(ROOT, 'scan-results')
  mkdirSync(outDir, { recursive: true })
  writeFileSync(join(outDir, `${skillName}.json`), JSON.stringify({
    skill: skillName,
    timestamp: new Date().toISOString(),
    staticScan,
    injectionSignals,
    llmResult: result,
    llmRaw: raw,
  }, null, 2))

  return {
    name: skillName, staticStatus: staticScan.status, staticScan,
    verdict: result?.verdict ?? null, confidence: result?.confidence ?? null, result,
  }
}

// ─── Translate scan results to Chinese ───────────────────────────────────────

const TRANSLATE_SYSTEM_PROMPT = `你是一位翻译专家。将以下安全扫描结果翻译成中文。保持格式不变，只翻译英文内容。对于技术术语保留英文原文并在括号内给出中文释义。`

async function translateResults(results) {
  const apiKey = process.env.OPENAI_API_KEY
  if (!apiKey) return

  // Collect results that have LLM evaluations
  const toTranslate = results.filter(r => r.result)
  if (toTranslate.length === 0) return

  console.log(`\n${'═'.repeat(60)}`)
  console.log(`${B} 中文翻译${R}`)
  console.log('═'.repeat(60))

  const parts = toTranslate.map(r => {
    const lines = [`## ${r.name}`]
    lines.push(`Verdict: ${r.result.verdict} (confidence: ${r.result.confidence})`)
    lines.push(`Summary: ${r.result.summary}`)
    if (r.result.dimensions.length > 0) {
      lines.push('Dimensions:')
      for (const d of r.result.dimensions) {
        lines.push(`- ${d.label} [${d.status}]: ${d.detail}`)
      }
    }
    if (r.result.guidance) {
      lines.push(`User Guidance: ${r.result.guidance}`)
    }
    return lines.join('\n')
  })

  const userMessage = parts.join('\n\n---\n\n')

  try {
    const content = await callOpenAI(TRANSLATE_SYSTEM_PROMPT, userMessage, {
      maxTokens: 8000,
      jsonMode: false,
    })
    if (content) {
      console.log()
      console.log(content)
      console.log()
    }
  } catch (e) {
    err(`Translation failed: ${e.message}`)
  }
}

// ─── Commands ────────────────────────────────────────────────────────────────

function cmdList() {
  console.log(`\n${B}Available Skills${R}\n`)
  console.log(`  ${'Directory'.padEnd(22)} ${'ClawHub Slug'.padEnd(30)} Display Name`)
  console.log(`  ${'─'.repeat(22)} ${'─'.repeat(30)} ${'─'.repeat(20)}`)
  for (const s of SKILLS) {
    console.log(`  ${s.dir.padEnd(22)} ${s.clawhub.padEnd(30)} ${s.name}`)
  }
  console.log()
}

async function cmdScan(skills, isStaticOnly = false, translate = false) {
  console.log(`\n${B}Security Scan${R} (${skills.length} skill(s))${isStaticOnly ? ' [static only]' : ''}\n`)

  const results = []
  for (const s of skills) {
    const r = await scanSkill(s.dir, { staticOnly: isStaticOnly })
    if (!r) continue

    printResult(s.dir, r.result, r.staticScan)
    if (isStaticOnly && r.injectionSignals?.length > 0) {
      console.log(`\n${B}Injection Signals:${R} ${r.injectionSignals.join(', ')}`)
    }
    results.push(r)
  }

  if (results.length > 1) printSummaryTable(results)

  if (!isStaticOnly) {
    const safe = results.filter(r => r.verdict === 'benign').length
    const sus = results.filter(r => r.verdict === 'suspicious').length
    const mal = results.filter(r => r.verdict === 'malicious').length
    const fail = results.filter(r => !r.verdict).length
    info(`Done. benign=${safe} suspicious=${sus} malicious=${mal}${fail ? ` failed=${fail}` : ''}`)
    info('Results saved to scan-results/')
  }

  ok('Scan complete.')

  // Translate LLM results to Chinese
  if (translate && !isStaticOnly && process.env.OPENAI_API_KEY) {
    await translateResults(results)
  }
}

function cmdDeploy(skills, target) {
  const deployClaude = target === 'claude' || target === 'all'
  const deployOpenclaw = target === 'openclaw' || target === 'all'

  if (!deployClaude && !deployOpenclaw) {
    info('Deploy target is "none", skipping deploy.')
    return
  }

  const targets = []
  if (deployClaude) targets.push('Claude Code')
  if (deployOpenclaw) targets.push('OpenClaw')

  console.log(`\n${B}Deploy to ${targets.join(' + ')}${R} (${skills.length} skill(s))\n`)

  if (deployClaude && !existsSync(CLAUDE_DIR)) {
    err(`Claude skills directory not found: ${CLAUDE_DIR}`)
    if (!deployOpenclaw) process.exit(1)
    warn('Skipping Claude Code deploy.')
  }
  if (deployOpenclaw && !existsSync(OPENCLAW_DIR)) {
    err(`OpenClaw skills directory not found: ${OPENCLAW_DIR}`)
    if (!deployClaude) process.exit(1)
    warn('Skipping OpenClaw deploy.')
  }

  let deployed = 0
  for (const s of skills) {
    const src = join(ROOT, s.dir)
    if (!existsSync(join(src, 'SKILL.md'))) {
      warn(`Skipping ${s.dir}: no SKILL.md`)
      continue
    }

    if (deployClaude && existsSync(CLAUDE_DIR)) {
      const dest = join(CLAUDE_DIR, s.claude)
      copyDir(src, dest)
      ok(`${s.dir} → ~/.claude/skills/${s.claude}/`)
    }

    if (deployOpenclaw && existsSync(OPENCLAW_DIR)) {
      const dest = join(OPENCLAW_DIR, s.clawhub)
      copyDir(src, dest)
      ok(`${s.dir} → ~/.openclaw/skills/${s.clawhub}/`)
    }

    deployed++
  }

  console.log()
  ok(`Deployed ${deployed} skill(s).`)
  if (deployClaude && existsSync(CLAUDE_DIR)) info('Claude Code: restart agent to reload skills.')
  if (deployOpenclaw && existsSync(OPENCLAW_DIR)) info('OpenClaw:    restart agent to reload skills.')
}

async function cmdPublish(skills, method = 'cli', fixedVersion = null) {
  if (method === 'cli') {
    try {
      execSync('which clawhub', { stdio: 'ignore' })
    } catch {
      err('clawhub CLI not found. Install: npm i -g clawhub')
      process.exit(1)
    }
    try {
      const whoami = execSync('clawhub whoami', { stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim()
      if (!whoami.includes('LogicTortoise')) {
        err(`clawhub identity mismatch: expected LogicTortoise, got: ${whoami}`)
        process.exit(1)
      }
      ok('clawhub identity verified: LogicTortoise')
    } catch {
      err('clawhub whoami failed. Run `clawhub login` first.')
      process.exit(1)
    }
  }

  if (method === 'api') {
    const config = readClawHubConfig()
    if (!config) {
      err('ClawHub config not found. Run `clawhub login` first.')
      process.exit(1)
    }
    info(`Using direct API publish → ${config.registry}`)
  }

  console.log(`\n${B}Publish to ClawHub${R} (${skills.length} skill(s), method: ${method})\n`)

  // Resolve version for each skill
  if (fixedVersion) {
    info(`Using fixed version: ${fixedVersion}`)
  } else {
    info('Fetching latest versions from ClawHub ...')
  }
  const plan = skills.map(s => {
    const nextVer = fixedVersion || fetchNextVersion(s.clawhub)
    return { ...s, nextVer }
  })

  console.log()
  console.log(`  ${'Skill'.padEnd(25)} ${'Current'.padEnd(10)} Next`)
  console.log(`  ${'─'.repeat(25)} ${'─'.repeat(10)} ${'─'.repeat(10)}`)
  for (const p of plan) {
    const parts = p.nextVer.split('.').map(Number)
    const current = parts[2] > 0 ? `${parts[0]}.${parts[1]}.${parts[2] - 1}` : '(new)'
    console.log(`  ${p.dir.padEnd(25)} ${current.padEnd(10)} ${p.nextVer}`)
  }

  console.log(`\n${DIM}Commands:${R}`)
  for (const p of plan) {
    const src = join(ROOT, p.dir)
    if (method === 'api') {
      console.log(`  ${DIM}[API] POST /api/v1/skills  slug=${p.clawhub}  version=${p.nextVer}${R}`)
    } else {
      console.log(`  ${DIM}clawhub publish "${src}" --slug "${p.clawhub}" --version "${p.nextVer}"${R}`)
    }
  }
  console.log()

  const answer = await ask('  Continue? [y/N] ')
  if (answer.toLowerCase() !== 'y') {
    info('Aborted.')
    return
  }

  console.log()
  let published = 0, failed = 0

  for (const p of plan) {
    const src = join(ROOT, p.dir)
    info(`Publishing ${p.dir} → ${p.clawhub} v${p.nextVer} ... (${method})`)

    if (method === 'api') {
      try {
        const config = readClawHubConfig()
        const result = await publishViaApi(p.clawhub, p.name, p.nextVer, src, config)
        ok(`${p.clawhub} v${p.nextVer} published. (versionId: ${result.versionId})`)
        published++
      } catch (e) {
        err(`Failed to publish ${p.clawhub}: ${e.message}`)
        failed++
      }
    } else {
      const result = run(`clawhub publish "${src}" --slug "${p.clawhub}" --version "${p.nextVer}"`)
      if (result !== null) {
        ok(`${p.clawhub} v${p.nextVer} published.`)
        published++
      } else {
        err(`Failed to publish ${p.clawhub}`)
        failed++
      }
    }
  }

  console.log()
  if (failed === 0) {
    ok(`All ${published} skill(s) published successfully.`)
  } else {
    warn(`Published: ${published}, Failed: ${failed}`)
  }
}

async function cmdRun(skills, target, method = 'cli', fixedVersion = null) {
  console.log(`\n${B}Full Pipeline: Scan → Deploy → Publish${R}\n`)

  // Step 1: Scan
  info('Step 1/3: Security Scan')
  await cmdScan(skills)

  const a1 = await ask('\n  Scan complete. Continue to deploy? [y/N] ')
  if (a1.toLowerCase() !== 'y') { info('Stopped after scan.'); return }

  // Step 2: Deploy
  console.log()
  info('Step 2/3: Local Deploy')
  cmdDeploy(skills, target)

  const a2 = await ask(`\n  Deploy complete. Test locally, then continue to publish? [y/N] `)
  if (a2.toLowerCase() !== 'y') {
    info(`Stopped after deploy. Run 'node publish.mjs publish' when ready.`)
    return
  }

  // Step 3: Publish
  console.log()
  info('Step 3/3: Publish to ClawHub')
  await cmdPublish(skills, method, fixedVersion)
}

// ─── Main ────────────────────────────────────────────────────────────────────

const USAGE = `
${B}AnyGen Skills — scan / deploy / publish${R}

Usage:
  node publish.mjs scan   [--static] [skill...]                 安全扫描
  node publish.mjs deploy [--target openclaw|claude|all|none] [skill...]  部署到本地
  node publish.mjs publish [skill...]                            发布到 ClawHub (自动版本)
  node publish.mjs run    [--target ...] [skill...]              完整流程
  node publish.mjs list                                         列出所有 skill

Options:
  --static          只做静态扫描（不需要 API Key）
  --translate       扫描后将结果翻译为中文（默认关闭）
  --target <agent>  部署目标 (默认: openclaw)
                    openclaw  只部署到 OpenClaw
                    claude    只部署到 Claude Code
                   all       两者都部署
                   none      跳过部署
  --method <mode>   发布方式 (默认: cli)
                    cli       使用 clawhub CLI 发布
                    api       直接调用 API 发布 (绕过 CLI acceptLicenseTerms 问题)
  --version <ver>   指定发布版本号 (默认: 自动 patch+1)

Examples:
  node publish.mjs scan                                         扫描全部
  node publish.mjs scan --static                                只做静态扫描
  node publish.mjs scan --translate                             扫描并翻译结果
  node publish.mjs scan data-analysis                           扫描单个
  node publish.mjs deploy                                       部署全部到 OpenClaw
  node publish.mjs deploy --target claude                       部署全部到 Claude Code
  node publish.mjs deploy --target all slide-generator          部署单个到两者
  node publish.mjs publish                                       发布全部 (自动 patch+1)
  node publish.mjs publish --version 2.0.0                       指定版本号发布
  node publish.mjs publish --method api                          使用 API 直接发布
  node publish.mjs publish slide-generator                       发布单个
  node publish.mjs run                                           完整流程
  node publish.mjs run --target all                              完整流程 (部署到两者)
  node publish.mjs run --method api                              完整流程 (API 发布)

Env:
  OPENAI_API_KEY        LLM 安全评估 (可选)
  OPENAI_EVAL_MODEL     安全评估模型 (默认 gpt-5-mini)
`

const args = process.argv.slice(2)
if (args.length === 0 || ['-h', '--help', 'help'].includes(args[0])) {
  console.log(USAGE)
  process.exit(0)
}

const cmd = args[0]
const rest = args.slice(1)

switch (cmd) {
  case 'list':
    cmdList()
    break
  case 'scan': {
    const isStatic = rest.includes('--static')
    const translate = rest.includes('--translate')
    const skillArgs = rest.filter(a => !a.startsWith('-'))
    await cmdScan(resolveSkills(skillArgs), isStatic, translate)
    break
  }
  case 'deploy': {
    const { target, remaining } = extractTarget(rest)
    cmdDeploy(resolveSkills(remaining), target)
    break
  }
  case 'publish': {
    const { method, remaining: pubRest1 } = extractMethod(rest)
    const { version, remaining: pubRest2 } = extractVersion(pubRest1)
    await cmdPublish(resolveSkills(pubRest2), method, version)
    break
  }
  case 'run': {
    const { method: runMethod, remaining: runRest1 } = extractMethod(rest)
    const { version: runVersion, remaining: runRest2 } = extractVersion(runRest1)
    const { target, remaining: runRest3 } = extractTarget(runRest2)
    await cmdRun(resolveSkills(runRest3), target, runMethod, runVersion)
    break
  }
  default:
    err(`Unknown command: ${cmd}`)
    console.log(USAGE)
    process.exit(1)
}
