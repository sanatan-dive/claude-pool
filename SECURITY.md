# Security policy

## Supported version

Security fixes are made on the latest `main` branch. There are no maintained
release branches yet.

## Reporting a vulnerability

Please use this repository's **Security → Report a vulnerability** flow so the
report stays private. Do not open a public issue for credential exposure,
arbitrary file access, command execution, or session-history leakage.

Include the operating system, Python version, T3 Code version, Claude Code
version, and the smallest reproduction you can provide. Remove prompts, bearer
tokens, OAuth data, MCP configuration, account emails, and session history.

## Credential handling

Claude Pool never needs the contents of a credential file. Each account must be
authenticated separately in its own `CLAUDE_CONFIG_DIR`. Never attach a Claude
config directory, `.credentials.json`, T3 settings file, raw process arguments,
or protocol log to an issue.
