#!/usr/bin/env python3
"""Install, configure, or check Claude Pool for T3 Code."""

from __future__ import annotations

import argparse
import copy
import fcntl
import json
import os
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from claude_pool import ACCOUNT_SLUG_RE, AccountStatus, load_pool_config


INSTANCE_ID = "claudeAgent_pool"


def _expand_path(value: str, home: Optional[Path] = None) -> Path:
    if home is not None and (value == "~" or value.startswith("~/")):
        value = str(home) + value[1:]
    return Path(os.path.abspath(os.path.expanduser(value)))


def parse_account_specs(values: Sequence[str]) -> List[AccountStatus]:
    accounts = []
    slugs = set()
    directories = set()
    for value in values:
        if "=" not in value:
            raise ValueError(f"account must be SLUG=PATH: {value!r}")
        slug, raw_directory = value.split("=", 1)
        if ACCOUNT_SLUG_RE.fullmatch(slug) is None:
            raise ValueError(f"invalid account slug: {slug!r}")
        if not raw_directory.strip():
            raise ValueError(f"account {slug!r} has an empty path")
        directory = _expand_path(raw_directory)
        if slug in slugs:
            raise ValueError(f"duplicate account slug: {slug}")
        canonical = directory.resolve(strict=False)
        if canonical in directories:
            raise ValueError(f"duplicate account config directory: {directory}")
        slugs.add(slug)
        directories.add(canonical)
        accounts.append(AccountStatus(slug, directory, True, switch_eligible=True))
    return accounts


def discover_accounts(settings_path: Path, home: Path) -> List[AccountStatus]:
    try:
        settings = json.loads(Path(settings_path).read_text())
    except (FileNotFoundError, OSError, ValueError):
        settings = {}
    instances = settings.get("providerInstances") if isinstance(settings, dict) else None
    accounts = []
    if isinstance(instances, dict):
        for instance_id, instance in instances.items():
            if (
                instance_id == INSTANCE_ID
                or not isinstance(instance, dict)
                or instance.get("driver") != "claudeAgent"
            ):
                continue
            if instance_id == "claudeAgent":
                slug = "main"
            elif isinstance(instance_id, str) and instance_id.startswith("claudeAgent_"):
                slug = instance_id.removeprefix("claudeAgent_")
            else:
                continue
            config = instance.get("config")
            home_path = config.get("homePath") if isinstance(config, dict) else None
            if not isinstance(home_path, str) or not home_path:
                continue
            accounts.append(
                AccountStatus(
                    slug,
                    _expand_path(home_path, home),
                    True,
                    switch_eligible=True,
                )
            )

    default_dir = home / ".claude"
    if default_dir.is_dir() and all(account.config_dir != default_dir for account in accounts):
        accounts.insert(0, AccountStatus("main", default_dir, True, switch_eligible=True))
    return parse_account_specs(
        [f"{account.slug}={account.config_dir}" for account in accounts]
    )


def configured_settings(
    original: Dict[str, Any],
    project_id: str,
    wrapper_path: Path,
    home_path: str,
    model: str,
) -> Dict[str, Any]:
    settings = copy.deepcopy(original)
    instances = settings.setdefault("providerInstances", {})
    existing = instances.get(INSTANCE_ID)
    if existing is not None and (
        not isinstance(existing, dict) or existing.get("driver") != "claudeAgent"
    ):
        raise ValueError(f"{INSTANCE_ID} already exists with another driver")
    instance = copy.deepcopy(existing) if isinstance(existing, dict) else {}
    instance.update(
        {"driver": "claudeAgent", "displayName": "Claude Pool", "enabled": True}
    )
    config = instance.setdefault("config", {})
    config.update({"homePath": home_path, "binaryPath": str(wrapper_path)})
    instances[INSTANCE_ID] = instance

    overrides = settings.setdefault("projectSettingsOverrides", {})
    project_override = overrides.setdefault(project_id, {})
    project_override["defaultModelSelection"] = {
        "instanceId": INSTANCE_ID,
        "model": model,
    }
    return settings


def _atomic_write_text(path: Path, content: str, mode: int) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(path.parent, 0o700)
    except OSError:
        pass
    fd, temp_name = tempfile.mkstemp(prefix="." + path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temp_name, mode)
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _atomic_write(path: Path, value: Dict[str, Any], mode: int) -> None:
    _atomic_write_text(path, json.dumps(value, indent=2) + "\n", mode)


def update_settings_file(
    settings_path: Path,
    project_id: str,
    wrapper_path: Path,
    home_path: str,
    model: str,
) -> Tuple[bool, Optional[Path]]:
    settings_path = Path(settings_path)
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = settings_path.parent / ".settings.json.claude-pool.lock"
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        if settings_path.exists():
            original = json.loads(settings_path.read_text())
            if not isinstance(original, dict):
                raise ValueError("settings root must be an object")
            mode = settings_path.stat().st_mode & 0o777
        else:
            original, mode = {}, 0o600
        updated = configured_settings(original, project_id, wrapper_path, home_path, model)
        if updated == original:
            return False, None

        backup = None
        if settings_path.exists():
            suffix = f".claude-pool-backup-{time.strftime('%Y%m%d-%H%M%S')}-{time.time_ns()}"
            backup = settings_path.with_name(settings_path.name + suffix)
            shutil.copy2(settings_path, backup)
            os.chmod(backup, 0o600)
        _atomic_write(settings_path, updated, mode)
        return True, backup


def find_project_id(database_path: Path, project_root: Path) -> Optional[str]:
    if not Path(database_path).is_file():
        return None
    uri = "file:" + str(Path(database_path).resolve()) + "?mode=ro"
    try:
        with closing(sqlite3.connect(uri, uri=True)) as database:
            row = database.execute(
                "SELECT project_id FROM projection_projects "
                "WHERE workspace_root = ? AND deleted_at IS NULL LIMIT 1",
                (os.path.normpath(str(project_root)),),
            ).fetchone()
    except sqlite3.Error:
        return None
    return row[0] if row and isinstance(row[0], str) else None


def ensure_t3_project(
    database_path: Path,
    project_root: Path,
    t3_bin: Path,
    t3_home: Path,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> str:
    existing = find_project_id(database_path, project_root)
    if existing is not None:
        return existing
    project_root.mkdir(parents=True, exist_ok=True)
    command = [
        str(t3_bin),
        "project",
        "add",
        "--base-dir",
        str(t3_home),
        "--title",
        "pool",
        str(project_root),
    ]
    runner(command, check=True, capture_output=True, text=True)
    project_id = find_project_id(database_path, project_root)
    if project_id is None:
        raise RuntimeError("T3 added the project but its id could not be found")
    return project_id


def write_pool_config(path: Path, accounts: Sequence[AccountStatus]) -> bool:
    value = {
        "accounts": [
            {"slug": account.slug, "config_dir": str(account.config_dir)}
            for account in accounts
        ]
    }
    content = json.dumps(value, indent=2) + "\n"
    try:
        if path.read_text() == content:
            os.chmod(path, 0o600)
            return False
    except (FileNotFoundError, OSError):
        pass
    _atomic_write_text(path, content, 0o600)
    return True


def install_files(
    source_root: Path,
    install_root: Path,
    bin_dir: Path,
    *,
    claude_bin: Path,
    config_path: Path,
    settings_path: Path,
) -> Path:
    source = source_root / "claude_pool.py"
    if not source.is_file():
        raise FileNotFoundError(f"wrapper source not found: {source}")
    installed_source = install_root / "claude_pool.py"
    content = source.read_text()
    try:
        unchanged = installed_source.read_text() == content
    except (FileNotFoundError, OSError):
        unchanged = False
    if not unchanged:
        _atomic_write_text(installed_source, content, 0o644)

    launcher = bin_dir / "claude-pool"
    launcher_content = "#!/bin/sh\n"
    for variable, value in (
        ("CLAUDE_POOL_REAL_CLAUDE", claude_bin),
        ("CLAUDE_POOL_CONFIG", config_path),
        ("CLAUDE_POOL_T3_SETTINGS", settings_path),
    ):
        launcher_content += (
            f'if [ -z "${{{variable}+x}}" ]; then\n'
            f"  {variable}={shlex.quote(str(value))}\n"
            "fi\n"
            f"export {variable}\n"
        )
    launcher_content += (
        "exec "
        + shlex.quote(sys.executable)
        + " "
        + shlex.quote(str(installed_source))
        + ' "$@"\n'
    )
    try:
        launcher_unchanged = launcher.read_text() == launcher_content
    except (FileNotFoundError, OSError):
        launcher_unchanged = False
    if not launcher_unchanged:
        _atomic_write_text(launcher, launcher_content, 0o755)
    else:
        os.chmod(launcher, 0o755)
    return launcher


def _find_executable(explicit: Optional[str], name: str, home: Path) -> Optional[Path]:
    if explicit:
        candidate = _expand_path(explicit, home)
        return candidate if candidate.is_file() and os.access(candidate, os.X_OK) else None
    discovered = shutil.which(name)
    if discovered:
        return Path(discovered)
    candidate = home / ".local" / "bin" / name
    return candidate if candidate.is_file() and os.access(candidate, os.X_OK) else None


def installation_issues(
    accounts: Sequence[AccountStatus],
    config_path: Path,
    install_root: Path,
    wrapper_path: Path,
    settings_path: Path,
    database_path: Path,
    project_root: Path,
    model: str,
    t3_bin: Optional[Path],
    claude_bin: Optional[Path],
) -> List[str]:
    issues = []
    if t3_bin is None:
        issues.append("T3 CLI is not executable")
    if claude_bin is None:
        issues.append("Claude CLI is not executable")
    if len(accounts) < 2:
        issues.append("at least two Claude accounts are required")
    for account in accounts:
        if not account.config_dir.is_dir():
            issues.append(f"account directory is missing: {account.config_dir}")
    try:
        installed_accounts = load_pool_config(config_path)
        if [
            (account.slug, account.config_dir) for account in installed_accounts
        ] != [(account.slug, account.config_dir) for account in accounts]:
            issues.append("installed account inventory differs")
        if config_path.stat().st_mode & 0o077:
            issues.append("account inventory permissions must be 0600")
    except ValueError:
        issues.append("account inventory is missing or invalid")
    if not (install_root / "claude_pool.py").is_file():
        issues.append("installed wrapper source is missing")
    if not wrapper_path.is_file() or not os.access(wrapper_path, os.X_OK):
        issues.append("wrapper launcher is missing or not executable")
    project_id = find_project_id(database_path, project_root)
    if project_id is None:
        issues.append("T3 pool project is missing")
    try:
        settings = json.loads(settings_path.read_text())
    except (FileNotFoundError, OSError, ValueError):
        settings = {}
    expected = configured_settings(
        settings,
        project_id or "<missing>",
        wrapper_path,
        str(accounts[0].config_dir) if accounts else "",
        model,
    )
    if settings != expected:
        issues.append("T3 pool provider or project pin is not configured")
    return issues


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--check", action="store_true")
    parser.add_argument(
        "--account",
        action="append",
        default=[],
        metavar="SLUG=PATH",
        help="repeat for every separately authenticated Claude config directory",
    )
    parser.add_argument("--model", default="claude-opus-5-5")
    parser.add_argument("--t3-home")
    parser.add_argument("--project-root")
    parser.add_argument("--install-root")
    parser.add_argument("--bin-dir")
    parser.add_argument("--config-path")
    parser.add_argument("--t3-bin")
    parser.add_argument("--claude-bin")
    return parser


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)
    home = Path.home()
    t3_home = _expand_path(args.t3_home or str(home / ".t3"), home)
    project_root = _expand_path(args.project_root or str(home / "accounts" / "pool"), home)
    install_root = _expand_path(
        args.install_root or str(home / ".local" / "share" / "claude-pool"), home
    )
    bin_dir = _expand_path(args.bin_dir or str(home / ".local" / "bin"), home)
    config_path = _expand_path(
        args.config_path or str(home / ".config" / "claude-pool" / "config.json"), home
    )
    settings_path = t3_home / "userdata" / "settings.json"
    database_path = t3_home / "userdata" / "state.sqlite"
    t3_bin = _find_executable(args.t3_bin, "t3", home)
    claude_bin = _find_executable(args.claude_bin, "claude", home)

    try:
        if args.account:
            accounts = parse_account_specs(args.account)
        elif config_path.is_file():
            accounts = load_pool_config(config_path)
        else:
            accounts = discover_accounts(settings_path, home)
    except ValueError as error:
        raise SystemExit(str(error))

    if len(accounts) < 2:
        raise SystemExit("at least two accounts are required; pass --account SLUG=PATH twice")
    missing_directories = [account.config_dir for account in accounts if not account.config_dir.is_dir()]
    if missing_directories:
        raise SystemExit("account directory does not exist: " + str(missing_directories[0]))
    if t3_bin is None:
        raise SystemExit("T3 CLI not found; pass --t3-bin /path/to/t3")
    if claude_bin is None:
        raise SystemExit("Claude CLI not found; pass --claude-bin /path/to/claude")

    wrapper_path = bin_dir / "claude-pool"
    project_id = find_project_id(database_path, project_root)

    if args.dry_run:
        print(f"accounts: {len(accounts)} configured")
        print("project: " + (f"exists ({project_id})" if project_id else "will create"))
        print(f"wrapper: will install to {wrapper_path}")
        print(f"config: will write {config_path}")
        return 0

    if args.check:
        issues = installation_issues(
            accounts,
            config_path,
            install_root,
            wrapper_path,
            settings_path,
            database_path,
            project_root,
            args.model,
            t3_bin,
            claude_bin,
        )
        if issues:
            for issue in issues:
                print("FAIL " + issue)
            return 1
        print(f"PASS Claude Pool is ready with {len(accounts)} accounts")
        return 0

    project_id = ensure_t3_project(
        database_path, project_root, t3_bin=t3_bin, t3_home=t3_home
    )
    wrapper_path = install_files(
        Path(__file__).resolve().parent,
        install_root,
        bin_dir,
        claude_bin=claude_bin,
        config_path=config_path,
        settings_path=settings_path,
    )
    config_changed = write_pool_config(config_path, accounts)
    settings_changed, backup = update_settings_file(
        settings_path,
        project_id=project_id,
        wrapper_path=wrapper_path,
        home_path=str(accounts[0].config_dir),
        model=args.model,
    )
    print(f"accounts: {len(accounts)}")
    print(f"pool project: {project_id}")
    print("wrapper: " + str(wrapper_path))
    print("config: " + ("updated" if config_changed else "already configured"))
    print("settings: " + ("updated" if settings_changed else "already configured"))
    if backup is not None:
        print("settings backup: " + str(backup))
    print("restart T3 Code when convenient so it loads the provider")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
