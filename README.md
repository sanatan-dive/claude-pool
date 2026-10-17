# Claude Pool for T3 Code

Keep one T3 Code chat going across multiple Claude accounts that you control.
When the active account reaches a confirmed five-hour or weekly usage limit,
Claude Pool copies that session's history to another authenticated account and
resumes the same conversation.

The accounts stay isolated. Claude Pool changes `CLAUDE_CONFIG_DIR` for each
Claude process; it never copies credentials or merges account directories.

> [!IMPORTANT]
> Use this only with accounts you own or are authorized to use. Review
> Anthropic's current terms and your organization's policies before enabling
> automated account switching. This project is unofficial and is not affiliated
> with Anthropic or T3 Code.

## What you get

- One T3 project named **pool**.
- Automatic handoff after a confirmed Claude usage-limit event.
- The same Claude session ID and complete conversation history.
- A manual fallback: send `continue` in the same T3 chat.
- Separate credentials for every account.
- Standard-library Python with no package installation.
- An idempotent installer that backs up T3 settings before changing them.

## Requirements

- macOS or Linux.
- Python 3.9 or newer.
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed.
- T3 Code installed, launched at least once, and available as `t3` or at
  `~/.local/bin/t3`.
- Two or more separately authenticated Claude accounts.

Claude Pool has been exercised against T3 Code 0.0.45 and Claude Code 2.1.302.
Their private streaming protocols can change, so keep the wrapper updated.

## Quick start

### 1. Sign in each Claude account separately

The normal account can keep Claude's default folder:

```sh
claude auth login
```

Give every additional account its own folder:

```sh
CLAUDE_CONFIG_DIR="$HOME/.claude-work" claude auth login
CLAUDE_CONFIG_DIR="$HOME/.claude-team" claude auth login
```

Confirm each login independently:

```sh
claude auth status
CLAUDE_CONFIG_DIR="$HOME/.claude-work" claude auth status
CLAUDE_CONFIG_DIR="$HOME/.claude-team" claude auth status
```

Never copy `.credentials.json` or another account's Claude folder. Complete the
login flow separately for every folder.

### 2. Install Claude Pool

```sh
git clone https://github.com/sanatan-dive/claude-pool.git
cd claude-pool
./install.sh \
  --account "main=$HOME/.claude" \
  --account "work=$HOME/.claude-work" \
  --account "team=$HOME/.claude-team"
```

Account names may contain letters, digits, `_`, and `-`. Their order is the
fallback order when live usage percentages are unavailable.

If the accounts already exist as separate Claude providers in T3, this shorter
command discovers them from T3 settings:

```sh
./install.sh
```

The installer:

1. validates the account folders and required CLIs;
2. creates `~/accounts/pool` as a T3 project when needed;
3. installs the wrapper under `~/.local/share/claude-pool`;
4. installs `~/.local/bin/claude-pool`;
5. writes the private account inventory to
   `~/.config/claude-pool/config.json` with mode `0600`;
6. adds only the `claudeAgent_pool` provider and pool-project pin to T3 settings.

It prints the settings backup path whenever it changes that file.

### 3. Check the installation

Use the same account arguments if you supplied them during installation:

```sh
python3 setup_pool.py --check \
  --account "main=$HOME/.claude" \
  --account "work=$HOME/.claude-work" \
  --account "team=$HOME/.claude-team"
```

Expected output:

```text
PASS Claude Pool is ready with 3 accounts
```

You can preview installation without writing anything:

```sh
python3 setup_pool.py --dry-run \
  --account "main=$HOME/.claude" \
  --account "work=$HOME/.claude-work"
```

### 4. Restart T3 once

Finish or stop active T3 chats first—a service restart stops running provider
processes.

```sh
t3 service restart
```

If you run the desktop app without a background service, quit and reopen it.

### 5. Use it

Open the **pool** project in T3 Code and chat normally.

On a confirmed usage limit, Claude Pool:

1. records the reset time for the current account;
2. finds the newest JSONL history for the session;
3. stops the old Claude process;
4. atomically copies only that session history to the next account;
5. resumes the same session and tells Claude to inspect completed work before
   continuing.

T3 stays on the same thread. No credentials are copied.

If an account fails before a safe automatic handoff is possible, send
`continue` in the same T3 chat. The next wrapper process excludes accounts it
has already observed as full and resumes the session on another account.

## Configuration

The default account file is:

```json
{
  "accounts": [
    {"slug": "main", "config_dir": "/Users/you/.claude"},
    {"slug": "work", "config_dir": "/Users/you/.claude-work"}
  ]
}
```

Rerun `./install.sh` with the complete desired account list to update it.

Useful installer overrides:

| Option | Default |
| --- | --- |
| `--model` | `claude-opus-5-5` |
| `--project-root` | `~/accounts/pool` |
| `--t3-home` | `~/.t3` |
| `--install-root` | `~/.local/share/claude-pool` |
| `--bin-dir` | `~/.local/bin` |
| `--config-path` | `~/.config/claude-pool/config.json` |
| `--t3-bin` | `t3` on `PATH`, then `~/.local/bin/t3` |
| `--claude-bin` | `claude` on `PATH`, then `~/.local/bin/claude` |

Runtime paths can also be overridden with `CLAUDE_POOL_CONFIG`,
`CLAUDE_POOL_CACHE`, `CLAUDE_POOL_STATE`, `CLAUDE_POOL_T3_SETTINGS`, and
`CLAUDE_POOL_REAL_CLAUDE`.

### Optional usage-aware ranking

The standalone installation does not poll Anthropic's usage endpoint. It uses
the configured order and observed reset times, which avoids endpoint rate
limits. Advanced installations can set `CLAUDE_POOL_STATUS_SCRIPT` to a Python
program that accepts `status --json` and returns account data in the format used
by the tests. Results are cached for five minutes.

## Safety model

- Only measured `rate_limit_event` messages with `status: rejected`, or the
  corresponding measured Claude API error shape, trigger a handoff.
- Warnings, malformed JSON, unknown protocol messages, and ordinary errors pass
  through unchanged.
- A handoff needs a confirmed session ID, captured SDK initialization request,
  available history, and another configured account.
- One process owns a session at a time through an advisory lock.
- History copies use a temporary file, `fsync`, and atomic replacement.
- Buffered input is byte-bounded. Ambiguous tool input is never blindly replayed.
- The wrapper never logs prompts, protocol streams, MCP configuration, argv, or
  credentials.

## Troubleshooting

### “Failed to execute Claude Agent CLI health check”

Run:

```sh
~/.local/bin/claude-pool --version
```

It should print the same version as `claude --version` and exit immediately.
Then run the installer check and restart T3. An already-visible toast can be
dismissed after the fresh check.

### The installer cannot find T3 or Claude

Pass the executable paths explicitly:

```sh
./install.sh \
  --t3-bin /path/to/t3 \
  --claude-bin /path/to/claude \
  --account "main=$HOME/.claude" \
  --account "work=$HOME/.claude-work"
```

### An account is ignored

Check its folder directly:

```sh
CLAUDE_CONFIG_DIR="$HOME/.claude-work" claude auth status
```

Then rerun the installer with the complete list. Folder names alone do not
authenticate an account.

### Every account is exhausted

Claude Pool passes the original limit event back to T3. Wait for an account's
reported reset time. It does not bypass a limit inside an account.

### A future T3 or Claude update breaks switching

Unknown protocol shapes intentionally fall back to normal pass-through. Open a
GitHub issue with versions and sanitized event shapes—never include tokens,
prompts, MCP configuration, or credential files.

## Updating

```sh
cd claude-pool
git pull --ff-only
./install.sh
python3 setup_pool.py --check
```

New chats use the updated wrapper immediately. Restart T3 only when provider
settings changed or T3 still shows an old health status.

## Rollback

The installer prints a timestamped T3 settings backup whenever it modifies
settings. To roll back, stop T3, restore that exact backup, and start T3 again.
The pool's project folder, account folders, and chat histories are left intact.

Installed Claude Pool files are limited to:

```text
~/.local/bin/claude-pool
~/.local/share/claude-pool/
~/.config/claude-pool/
~/.cache/claude-pool/
~/.local/state/claude-pool/
```

Do not delete an account's `.claude*` directory during rollback; it contains that
account's own login and session history.

## Development

```sh
python3 -W error::ResourceWarning -m unittest -q
python3 -m py_compile claude_pool.py setup_pool.py
sh -n install.sh bin/claude-pool
```

The fake-child integration suite covers automatic switching, exhaustion,
same-thread manual continuation, SDK initialization replay, input ordering,
process cleanup, history copying, installer idempotency, and the T3 health-check
contract.

## License

[MIT](LICENSE)
