# Claude Code Configuration

## What's Here

```
.claude/
├── commands/              # Slash commands (type /<name> to invoke)
│   ├── dra.md             # /dra — loads full DRA allocator context
│   └── consumable-capacity.md  # /consumable-capacity — loads KEP-5075 context
├── agents/                # Specialized subagents (spawned automatically or by request)
│   ├── dra-expert.md      # Karpenter DRA allocator internals
│   └── consumable-capacity-expert.md  # Upstream KEP-5075 semantics
├── settings.local.json    # Permission allowlist
└── README.md              # This file

CLAUDE.md (repo root)     # Auto-loaded every session — build commands, project structure
```

## Usage

### Slash Commands

Type these in the Claude Code prompt:

| Command | What it does |
|---------|-------------|
| `/dra` | Reads all 12 DRA allocator files + design docs, reports context loaded |
| `/consumable-capacity` | Reads the consumable capacity design doc, reports context loaded |

Use at the start of a session when you need deep context on a topic.

### Agents

These are spawned by Claude automatically when appropriate, or you can ask for them:

| Agent | Purpose |
|-------|---------|
| `dra-expert` | Karpenter DRA allocator internals (DFS, AllocationTracker, pools, constraints, commit protocol) |
| `consumable-capacity-expert` | Upstream KEP-5075 semantics (capacity accounting, rounding rules, DistinctAttribute) |
| `cc-implementation-validator` | Validates Go code against `consumable-capacity-integration.md` spec |
| `cc-design-validator` | Validates design docs against KEP-5075 and each other for consistency |

**Expert agents** answer questions and help design. **Validator agents** check work for correctness.

Example usage:
- "use the cc-implementation-validator to check allocator.go"
- "ask the cc-design-validator if the rounding rules are consistent between docs"

### CLAUDE.md

Loaded automatically every session. Contains build commands, project structure overview, and key entry points. You don't need to do anything — it's always there.

### Memory (outside this repo)

Persistent across sessions at `~/.claude/projects/.../memory/`. Stores:
- DRA implementation architecture map
- Consumable capacity project status
- User context

Claude reads/writes these automatically. You can say "remember X" to save something.

## Adding More

- **New slash command:** Create `.claude/commands/<name>.md` with instructions for what to read/do
- **New agent:** Create `.claude/agents/<name>.md` with a frontmatter block (name, description) and system prompt
- **New context in CLAUDE.md:** Just edit the file at repo root
