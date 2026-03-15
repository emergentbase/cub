# Cub

Responsive Telegram personal assistant:

- fast chat replies
- background Claude Code tasks for heavier work

## Quick Start (Recommended: Claude Code only)

Prerequisites:

- `uv` installed
- `claude` installed and authenticated in your shell
- Telegram bot token from [@BotFather](https://t.me/BotFather)

Run:

```bash
git clone https://github.com/emergentbase/cub.git
cd cub
cp .env.example .env
```

Edit `.env` and set:

```env
TELEGRAM_BOT_TOKEN=...
ALLOWED_USER_IDS=
FRONT_ASSISTANT_PROVIDER=claude_cli
FRONT_ASSISTANT_MODEL=haiku
```

Start bot:

```bash
uv run cub-bot
```

Then open Telegram and send:

- `/help`
- `what is 7+5?`
- `create a react todo app in workspace/`

## How It Behaves

- Simple questions: quick direct reply.
- Work that needs tools/files/shell: queued task with a final completion update.
- If you want an update before it finishes, ask for status.
- You can keep chatting while tasks run.

## Example Conversation

```text
You: what is 7+7?
Cub: 14

You: add 5 to it
Cub: 19

You: create a chat app like WhatsApp with seed data (full-stack)
Cub: Queued task b709652a (...). I’ll send the final result here. If you want an update before then, ask for status.

You: add 5 to the previous calc
Cub: 24

Cub: ✅ Task Completed
Summary:
- Full-stack chat app scaffold created
- Seed data setup included
```

## Optional: OpenRouter for Fast Replies

Keep Claude Code for delegated tasks, but use OpenRouter for fast-path chat replies:

```env
FRONT_ASSISTANT_PROVIDER=openrouter
FRONT_ASSISTANT_MODEL=anthropic/claude-3.5-haiku
OPENROUTER_API_KEY=...
```

Notes:

- Delegated tasks still use `CLAUDE_COMMAND` and `CLAUDE_ARGS`.
- If fast-path OpenRouter call fails, Cub falls back to delegated execution.
- Keep this as optional mode; `claude_cli` remains the default/recommended path.

## Commands

| Command | Description |
|---|---|
| `/run <task>` | Queue a delegated Claude task |
| `/probe [id] [question]` | Probe/resume a task session |
| `/continue [id] <instruction>` | Continue a task using the same Claude session |
| `/list` | Show ongoing tasks |
| `/status [id]` | Show task status |
| `/tasks` | Show recent tasks |
| `/cancel [id] [--force]` | Cancel a task |
| `/killall [--graceful]` | Kill Claude Code processes on the machine |
| `/mute` / `/unmute` | Compatibility no-op; tasks are already quiet |
| `/newsession` | Reset fast assistant chat session |
| `/remind <task_id> <when> [note]` | Set reminder |

Natural language controls also work:
`cancel task`, `kill task ab12cd34`, `check task ab12cd34`,
`continue task ab12cd34 add e2e tests`.

## Run Multiple Bots on One Machine

Use a different Telegram token and `CUB_HOME` per instance:

```bash
# bot A
TELEGRAM_BOT_TOKEN=... CUB_HOME=~/.cub-a uv run cub-bot

# bot B
TELEGRAM_BOT_TOKEN=... CUB_HOME=~/.cub-b uv run cub-bot
```

## Reset Local State

```bash
uv run cub-reset --yes
```

- Default target: `CUB_HOME` (or `~/.cub`)
- Preview only: `uv run cub-reset --dry-run`
- Custom home: `uv run cub-reset --home ~/.cub-a --yes`

## Local Process Cleanup (Host Machine)

If bot tasks get stuck, you can clean up Claude Code processes directly from shell:

```bash
uv run cub-killall
```

Graceful only (TERM, no force kill):

```bash
uv run cub-killall --graceful
```

Shutdown tip:

- Press `Ctrl+C` to exit immediately.

## Important Config

- `TELEGRAM_MAX_CONCURRENT_UPDATES`: parallel inbound update handling (default `8`)
- `CLAUDE_ARGS`: defaults to `--dangerously-skip-permissions --verbose --output-format stream-json`
- `WORKSPACE_DIR`: where delegated tasks run (defaults to `$CUB_HOME/work`)
- `FRONT_ASSISTANT_TOOL_MODE`: `read_only` (default for `claude_cli`) or `none`
- `FRONT_ASSISTANT_MAX_TURNS`: bounds fast assistant turns (default `2` in read-only mode)

## Project Layout

```text
~/.cub/
  state/assistant.db
  mind/
  work/
```

## Development

```bash
make setup
make run
make lint
make test
```

Architecture and extension guide: `DEVELOPER_GUIDE.md`

## License

MIT
