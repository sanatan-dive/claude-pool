#!/usr/bin/env python3
"""A conservative, streaming Claude Code account supervisor."""

from __future__ import annotations

import json
import os
import re
import signal
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import hashlib
import fcntl
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple


@dataclass(frozen=True)
class LimitHit:
    reset_at: Optional[float]
    kind: Optional[str]
    session_id: Optional[str]


@dataclass(frozen=True)
class AccountStatus:
    slug: str
    config_dir: Path
    signed_in: bool
    session_percent: Optional[float] = None
    weekly_percent: Optional[float] = None
    switch_eligible: bool = True


@dataclass
class PoolConfig:
    command: Sequence[str]
    accounts: Sequence[AccountStatus]
    state_path: Path
    cwd: Path
    now: Callable[[], float] = time.time
    switch_started: Optional[Callable[[], None]] = None
    input_buffer_bytes: int = 8 * 1024 * 1024
    account_cache_path: Optional[Path] = None
    account_settings_path: Optional[Path] = None
    fallback_config_dir: Optional[Path] = None
    configured_accounts: Optional[Sequence[AccountStatus]] = None


def _number(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def parse_limit_event(event: Any) -> Optional[LimitHit]:
    """Return a limit only for protocol shapes observed from Claude Code."""
    if not isinstance(event, dict):
        return None

    info: Any = None
    if event.get("type") == "rate_limit_event":
        candidate = event.get("rate_limit_info")
        if isinstance(candidate, dict) and candidate.get("status") == "rejected":
            info = candidate
    elif (
        event.get("type") == "assistant"
        and event.get("error") == "rate_limit"
        and event.get("is_api_error_message") is True
        and event.get("api_error") == "usage_limit_reached"
    ):
        params = event.get("api_error_params")
        if isinstance(params, dict):
            candidate = params.get("rate_limit_info")
            if isinstance(candidate, dict) and candidate.get("status") == "rejected":
                info = candidate

    if not isinstance(info, dict):
        return None
    return LimitHit(
        reset_at=_number(info.get("resetsAt")),
        kind=info.get("rateLimitType") if isinstance(info.get("rateLimitType"), str) else None,
        session_id=event.get("session_id") if isinstance(event.get("session_id"), str) else None,
    )


SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


def valid_session_id(value: Any) -> bool:
    return isinstance(value, str) and SESSION_ID_RE.fullmatch(value) is not None


def resume_args(args: Sequence[str], session_id: str) -> List[str]:
    """Replace every session selector with one unambiguous resume argument."""
    if not valid_session_id(session_id):
        raise ValueError("invalid session id")
    result: List[str] = []
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--":
            result.append("--resume=" + session_id)
            result.extend(args[index:])
            return result
        if arg in ("--resume", "--session-id"):
            if index + 1 < len(args) and not args[index + 1].startswith("-"):
                index += 2
            else:
                index += 1
            continue
        if arg.startswith("--resume=") or arg.startswith("--session-id="):
            index += 1
            continue
        result.append(arg)
        index += 1
    result.append("--resume=" + session_id)
    return result


def session_id_from_args(args: Sequence[str]) -> Optional[str]:
    for index, arg in enumerate(args):
        if arg == "--":
            return None
        if arg.startswith("--resume=") or arg.startswith("--session-id="):
            value = arg.split("=", 1)[1]
            return value if valid_session_id(value) else None
        if arg in ("--resume", "--session-id") and index + 1 < len(args):
            value = args[index + 1]
            return value if valid_session_id(value) else None
    return None


def cwd_slug(cwd: Path) -> str:
    return os.path.abspath(str(cwd)).replace("/", "-").replace(".", "-")


def history_path(config_dir: Path, project_slug: str, session_id: str) -> Path:
    if not valid_session_id(session_id):
        raise ValueError("invalid session id")
    return config_dir / "projects" / project_slug / (session_id + ".jsonl")


def find_newest_history(
    config_dirs: Iterable[Path], project_slug: str, session_id: str
) -> Optional[Path]:
    if not valid_session_id(session_id):
        return None
    candidates = []
    for config_dir in config_dirs:
        candidate = history_path(Path(config_dir), project_slug, session_id)
        try:
            stat = candidate.stat()
        except (FileNotFoundError, OSError):
            continue
        if candidate.is_file():
            candidates.append((stat.st_mtime_ns, stat.st_size, str(candidate), candidate))
    if not candidates:
        return None
    return max(candidates)[-1]


def atomic_copy_history(source: Path, target: Path) -> None:
    """Copy a history file without ever truncating an existing valid target."""
    source = Path(source)
    target = Path(target)
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=str(target.parent), prefix="." + target.name + ".", suffix=".tmp", delete=False
        ) as temp_file:
            temp_path = Path(temp_file.name)
            with source.open("rb") as source_file:
                shutil.copyfileobj(source_file, temp_file)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(str(temp_path), str(target))
        temp_path = None
        directory_fd = os.open(str(target.parent), os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass


class StateStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.data: Dict[str, Any] = self._read()

    def _read(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {"full_until": {}, "sessions": {}}
        try:
            loaded = json.loads(self.path.read_text())
            if isinstance(loaded, dict):
                if isinstance(loaded.get("full_until"), dict):
                    data["full_until"] = loaded["full_until"]
                if isinstance(loaded.get("sessions"), dict):
                    data["sessions"] = loaded["sessions"]
        except (FileNotFoundError, OSError, ValueError):
            pass
        return data

    @property
    def full_until(self) -> Mapping[str, Any]:
        self.data = self._read()
        return dict(self.data["full_until"])

    def mark_full(self, slug: str, reset_at: Optional[float]) -> None:
        self._mutate(lambda data: data["full_until"].__setitem__(slug, reset_at))

    def is_full(self, slug: str, now: float) -> bool:
        self.data = self._read()
        reset_at = _number(self.data["full_until"].get(slug))
        return reset_at is not None and reset_at > now

    def set_session_account(self, session_id: str, slug: str) -> None:
        if not valid_session_id(session_id):
            return
        self._mutate(lambda data: data["sessions"].__setitem__(session_id, slug))

    def session_account(self, session_id: str) -> Optional[str]:
        if not valid_session_id(session_id):
            return None
        self.data = self._read()
        value = self.data["sessions"].get(session_id)
        return value if isinstance(value, str) else None

    def _mutate(self, change: Callable[[Dict[str, Any]], None]) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        with (self.path.parent / (self.path.name + ".lock")).open("a+") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            data = self._read()
            before = json.dumps(data, sort_keys=True, separators=(",", ":"))
            change(data)
            after = json.dumps(data, sort_keys=True, separators=(",", ":"))
            self.data = data
            if before == after:
                return
            fd, temp_name = tempfile.mkstemp(
                prefix="." + self.path.name + ".", dir=str(self.path.parent)
            )
            try:
                with os.fdopen(fd, "w") as temp_file:
                    temp_file.write(after)
                    temp_file.flush()
                    os.fsync(temp_file.fileno())
                os.chmod(temp_name, 0o600)
                os.replace(temp_name, self.path)
                directory_fd = os.open(str(self.path.parent), os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                try:
                    os.unlink(temp_name)
                except FileNotFoundError:
                    pass


def rank_accounts(
    accounts: Iterable[AccountStatus], full_until: Mapping[str, Any], now: float
) -> List[AccountStatus]:
    ranked = []
    for index, account in enumerate(accounts):
        if not account.signed_in:
            continue
        if not account.switch_eligible:
            continue
        reset_at = _number(full_until.get(account.slug))
        if reset_at is not None and reset_at > now:
            continue
        percentages = [
            value
            for value in (account.session_percent, account.weekly_percent)
            if isinstance(value, (int, float))
        ]
        known = len(percentages) > 0
        headroom = 100.0 - max(percentages) if known else -1.0
        ranked.append((1 if known else 0, headroom, index, account))
    ranked.sort(key=lambda item: (-item[0], -item[1], item[2]))
    return [item[-1] for item in ranked]


def _atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(path.parent, 0o700)
    except OSError:
        pass
    fd, temp_name = tempfile.mkstemp(prefix="." + path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(value, output, sort_keys=True, separators=(",", ":"))
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temp_name, 0o600)
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _accounts_from_payload(payload: Any) -> List[AccountStatus]:
    if not isinstance(payload, dict) or not isinstance(payload.get("accounts"), list):
        return []
    result = []
    for raw in payload["accounts"]:
        if not isinstance(raw, dict):
            continue
        slug, directory = raw.get("slug"), raw.get("dir")
        if not isinstance(slug, str) or not isinstance(directory, str):
            continue
        session_percent = weekly_percent = None
        usage = raw.get("usage")
        live_usage = isinstance(usage, dict) and usage.get("saved") is False
        if live_usage:
            for limit in usage.get("limits") or []:
                if not isinstance(limit, dict):
                    continue
                percent = _number(limit.get("percent"))
                if limit.get("kind") == "session":
                    session_percent = percent
                elif limit.get("kind") == "weekly_all":
                    weekly_percent = percent
        result.append(
            AccountStatus(
                slug=slug,
                config_dir=Path(os.path.expanduser(directory)),
                signed_in=raw.get("logged_in") is True,
                session_percent=session_percent,
                weekly_percent=weekly_percent,
                switch_eligible=(
                    raw.get("logged_in") is True
                    and not raw.get("error")
                    and live_usage
                    and (session_percent is not None or weekly_percent is not None)
                ),
            )
        )
    return result


def _without_switching(accounts: Sequence[AccountStatus]) -> List[AccountStatus]:
    return [
        AccountStatus(
            account.slug,
            account.config_dir,
            account.signed_in,
            account.session_percent,
            account.weekly_percent,
            switch_eligible=False,
        )
        for account in accounts
    ]


def load_account_statuses(
    cache_path: Path,
    sweep: Optional[Callable[[], Mapping[str, Any]]] = None,
    fallback_dir: Optional[Path] = None,
    now: Callable[[], float] = time.time,
    ttl_seconds: int = 300,
) -> List[AccountStatus]:
    """Load one all-account sweep, sharing it for five minutes across wrappers."""
    cache_path = Path(cache_path)

    def read_recent() -> Optional[List[AccountStatus]]:
        try:
            cached = json.loads(cache_path.read_text())
        except (FileNotFoundError, OSError, ValueError):
            return None
        attempted_at = _number(cached.get("attempted_at")) if isinstance(cached, dict) else None
        if attempted_at is None or now() - attempted_at >= ttl_seconds:
            return None
        accounts = _accounts_from_payload(cached.get("payload"))
        if accounts:
            if cached.get("failed") is True or cached.get("refreshing") is True:
                return _without_switching(accounts)
            return accounts
        if fallback_dir is not None:
            return [AccountStatus("fallback", Path(fallback_dir), True, switch_eligible=False)]
        return []

    cached_accounts = read_recent()
    if cached_accounts is not None:
        return cached_accounts

    cache_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock_path = cache_path.with_suffix(cache_path.suffix + ".lock")
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        cached_accounts = read_recent()
        if cached_accounts is not None:
            return cached_accounts
        previous_payload = None
        previous_fetched_at = None
        try:
            previous = json.loads(cache_path.read_text())
            if isinstance(previous, dict):
                previous_payload = previous.get("payload")
                previous_fetched_at = previous.get("fetched_at")
        except (FileNotFoundError, OSError, ValueError):
            pass
        refresh_marker = {"attempted_at": now(), "refreshing": True}
        if _accounts_from_payload(previous_payload):
            refresh_marker["payload"] = previous_payload
            refresh_marker["fetched_at"] = previous_fetched_at
        _atomic_write_json(cache_path, refresh_marker)
        try:
            if sweep is None:
                raise OSError("no status sweep configured")
            payload = sweep()
            accounts = _accounts_from_payload(payload)
            if not accounts:
                raise ValueError("status sweep returned no accounts")
            timestamp = now()
            _atomic_write_json(
                cache_path, {"attempted_at": timestamp, "fetched_at": timestamp, "payload": payload}
            )
            return accounts
        except (OSError, ValueError, subprocess.SubprocessError):
            failure_cache = {"attempted_at": now(), "failed": True}
            if _accounts_from_payload(previous_payload):
                failure_cache["payload"] = previous_payload
                failure_cache["fetched_at"] = previous_fetched_at
            _atomic_write_json(cache_path, failure_cache)
            stale_accounts = _accounts_from_payload(previous_payload)
            if stale_accounts:
                return _without_switching(stale_accounts)
            if fallback_dir is not None:
                return [AccountStatus("fallback", Path(fallback_dir), True, switch_eligible=False)]
            return []


ACCOUNT_SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


def load_pool_config(path: Path) -> List[AccountStatus]:
    """Load the explicit, credential-free account inventory."""
    try:
        payload = json.loads(Path(path).read_text())
    except (FileNotFoundError, OSError, ValueError) as error:
        raise ValueError(f"cannot read pool config: {error}") from error
    raw_accounts = payload.get("accounts") if isinstance(payload, dict) else None
    if not isinstance(raw_accounts, list) or not raw_accounts:
        raise ValueError("pool config must contain a non-empty accounts list")

    result: List[AccountStatus] = []
    slugs = set()
    directories = set()
    for raw in raw_accounts:
        if not isinstance(raw, dict):
            raise ValueError("each configured account must be an object")
        slug = raw.get("slug")
        directory = raw.get("config_dir")
        if not isinstance(slug, str) or ACCOUNT_SLUG_RE.fullmatch(slug) is None:
            raise ValueError("account slug must use letters, digits, underscores, or hyphens")
        if not isinstance(directory, str) or not directory.strip():
            raise ValueError(f"account {slug!r} must have a config_dir")
        resolved = Path(os.path.abspath(os.path.expanduser(directory)))
        canonical = resolved.resolve(strict=False)
        if slug in slugs:
            raise ValueError(f"duplicate account slug: {slug}")
        if canonical in directories:
            raise ValueError(f"duplicate account config_dir: {resolved}")
        slugs.add(slug)
        directories.add(canonical)
        result.append(AccountStatus(slug, resolved, True, switch_eligible=True))
    return result


def _configured_account_inventory(settings_path: Optional[Path]) -> List[AccountStatus]:
    if settings_path is None:
        return []
    try:
        settings = json.loads(Path(settings_path).read_text())
    except (FileNotFoundError, OSError, ValueError):
        return []
    instances = settings.get("providerInstances") if isinstance(settings, dict) else None
    if not isinstance(instances, dict):
        return []
    accounts = []
    for instance_id, instance in instances.items():
        if (
            not isinstance(instance_id, str)
            or not instance_id.startswith("claudeAgent_")
            or instance_id == "claudeAgent_pool"
            or not isinstance(instance, dict)
            or instance.get("driver") != "claudeAgent"
        ):
            continue
        config = instance.get("config")
        home_path = config.get("homePath") if isinstance(config, dict) else None
        if not isinstance(home_path, str) or not home_path:
            continue
        accounts.append(
            AccountStatus(
                instance_id.removeprefix("claudeAgent_"),
                Path(os.path.expanduser(home_path)),
                True,
                switch_eligible=False,
            )
        )
    return accounts


def load_cached_account_statuses(
    cache_path: Path,
    fallback_dir: Path,
    settings_path: Optional[Path] = None,
    configured_accounts: Optional[Sequence[AccountStatus]] = None,
    now: Callable[[], float] = time.time,
    ttl_seconds: int = 300,
) -> List[AccountStatus]:
    """Load account inventory immediately and trust usage only while it is fresh."""
    if configured_accounts:
        explicit = list(configured_accounts)
        try:
            cached = json.loads(Path(cache_path).read_text())
        except (FileNotFoundError, OSError, ValueError):
            return explicit
        attempted_at = _number(cached.get("attempted_at")) if isinstance(cached, dict) else None
        fresh = (
            attempted_at is not None
            and now() - attempted_at < ttl_seconds
            and cached.get("failed") is not True
            and cached.get("refreshing") is not True
        )
        if not fresh:
            return explicit
        cached_accounts = _accounts_from_payload(cached.get("payload"))
        cached_by_dir: Dict[Path, List[AccountStatus]] = {}
        for cached_account in cached_accounts:
            canonical_dir = cached_account.config_dir.resolve(strict=False)
            cached_by_dir.setdefault(canonical_dir, []).append(cached_account)

        merged = []
        for account in explicit:
            matches = cached_by_dir.get(account.config_dir.resolve(strict=False), [])
            if len(matches) != 1:
                merged.append(account)
                continue
            usage = matches[0]
            merged.append(
                AccountStatus(
                    account.slug,
                    account.config_dir,
                    usage.signed_in,
                    usage.session_percent,
                    usage.weekly_percent,
                    usage.switch_eligible,
                )
            )
        return merged

    fallback = AccountStatus(
        "main", Path(os.path.expanduser(str(fallback_dir))), True, switch_eligible=False
    )
    inventory = [fallback] + _configured_account_inventory(settings_path)
    try:
        cached = json.loads(Path(cache_path).read_text())
    except (FileNotFoundError, OSError, ValueError):
        cached = None
    cached_accounts: List[AccountStatus] = []
    if isinstance(cached, dict):
        cached_accounts = _accounts_from_payload(cached.get("payload"))
        attempted_at = _number(cached.get("attempted_at"))
        fresh = (
            attempted_at is not None
            and now() - attempted_at < ttl_seconds
            and cached.get("failed") is not True
            and cached.get("refreshing") is not True
        )
        if not fresh:
            cached_accounts = _without_switching(cached_accounts)

    result = list(cached_accounts)
    known_slugs = {account.slug for account in result}
    known_dirs = {account.config_dir for account in result}
    for account in inventory:
        if account.slug not in known_slugs and account.config_dir not in known_dirs:
            result.append(account)
            known_slugs.add(account.slug)
            known_dirs.add(account.config_dir)
    return result


def _status_timeout_seconds() -> float:
    try:
        configured = float(os.environ.get("CLAUDE_POOL_STATUS_TIMEOUT", "240"))
    except ValueError:
        return 240.0
    return min(max(configured, 0.1), 240.0)


def _run_status_command(
    script: Path,
    child: Optional[List[Optional[subprocess.Popen]]] = None,
    child_lock: Optional[threading.Lock] = None,
) -> Mapping[str, Any]:
    def spawn() -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, str(script), "status", "--json"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            start_new_session=True,
        )

    if child is not None and child_lock is not None:
        with child_lock:
            process = spawn()
            child[0] = process
    else:
        process = spawn()
    try:
        stdout, _ = process.communicate()
    finally:
        if child is not None and child_lock is not None:
            with child_lock:
                if child[0] is process:
                    child[0] = None
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, process.args)
    payload = json.loads(stdout or "{}")
    if not isinstance(payload, dict):
        raise ValueError("invalid account status")
    return payload


def _refresh_cache_worker(cache_path: Path, script: Path, fallback_dir: Path) -> int:
    """Refresh cache in a bounded process that may safely outlive its launcher."""
    timeout = _status_timeout_seconds()
    child_lock = threading.Lock()
    child: List[Optional[subprocess.Popen]] = [None]

    def expire() -> None:
        with child_lock:
            process = child[0]
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if process.poll() is None:
                process.wait()
        os._exit(124)

    timer = threading.Timer(timeout, expire)
    timer.daemon = True
    timer.start()
    try:
        load_account_statuses(
            cache_path,
            sweep=lambda: _run_status_command(script, child, child_lock),
            fallback_dir=fallback_dir,
        )
    finally:
        timer.cancel()
    return 0


def start_account_refresh(
    cache_path: Path,
    script: Path,
    fallback_dir: Path,
) -> subprocess.Popen:
    process = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--internal-refresh-cache",
            str(cache_path),
            str(script),
            str(fallback_dir),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    threading.Thread(target=process.wait, name="claude-pool-refresh-reaper", daemon=True).start()
    return process


def _json_line(raw: bytes) -> Optional[Dict[str, Any]]:
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _initialize_request(raw: bytes) -> Optional[str]:
    event = _json_line(raw)
    if not event or event.get("type") != "control_request":
        return None
    request = event.get("request")
    if not isinstance(request, dict) or not any(
        request.get(key) == "initialize" for key in ("subtype", "type", "method")
    ):
        return None
    request_id = event.get("request_id")
    return request_id if isinstance(request_id, str) else None


# Session setup the SDK sends after initialize (T3 attaches its own tools with
# mcp_set_servers). A new child must get it again; only the latest of each
# counts, except flag settings, which merge.
_REPLAYED_CONTROLS = ("mcp_set_servers", "set_model", "set_permission_mode")
_MERGED_CONTROLS = ("apply_flag_settings",)


def _session_control(raw: bytes) -> Optional[Tuple[str, str]]:
    event = _json_line(raw)
    if not event or event.get("type") != "control_request":
        return None
    request = event.get("request")
    request_id = event.get("request_id")
    if not isinstance(request, dict) or not isinstance(request_id, str):
        return None
    subtype = request.get("subtype")
    if subtype in _REPLAYED_CONTROLS or subtype in _MERGED_CONTROLS:
        return subtype, request_id
    return None


def _matches_control_response(event: Mapping[str, Any], request_id: str) -> bool:
    if event.get("type") != "control_response":
        return False
    if event.get("request_id") == request_id:
        return True
    response = event.get("response")
    return isinstance(response, dict) and response.get("request_id") == request_id


class _InputRouter:
    def __init__(self, source: BinaryIO, target: BinaryIO, max_buffer_bytes: int):
        self.source = source
        self.target: Optional[BinaryIO] = target
        self.condition = threading.Condition()
        self.paused = False
        self.pending: Deque[bytes] = deque()
        self.pending_bytes = 0
        self.max_buffer_bytes = max(1, max_buffer_bytes)
        self.max_observed_pending_bytes = 0
        self.eof = False
        self.stopped = False
        self.writer_active = False
        self.initialize_line: Optional[bytes] = None
        self.initialize_request_id: Optional[str] = None
        self.session_controls: List[Tuple[str, str, bytes]] = []
        self.reader_thread = threading.Thread(
            target=self._read, name="claude-pool-stdin-reader", daemon=True
        )
        self.writer_thread = threading.Thread(
            target=self._write, name="claude-pool-stdin-writer", daemon=True
        )

    def start(self) -> None:
        self.reader_thread.start()
        self.writer_thread.start()

    @staticmethod
    def _write_line(target: BinaryIO, raw: bytes) -> bool:
        try:
            target.write(raw)
            target.flush()
            return True
        except (BrokenPipeError, OSError, ValueError):
            return False

    def _read(self) -> None:
        while True:
            try:
                raw = self.source.readline(self.max_buffer_bytes)
            except (OSError, ValueError):
                raw = b""
            if not raw:
                with self.condition:
                    self.eof = True
                    self.condition.notify_all()
                return
            request_id = _initialize_request(raw)
            control = _session_control(raw)
            with self.condition:
                while (
                    not self.stopped
                    and self.pending_bytes > 0
                    and self.pending_bytes + len(raw) > self.max_buffer_bytes
                ):
                    self.condition.wait()
                if self.stopped:
                    return
                if request_id is not None and self.initialize_line is None:
                    self.initialize_line = raw
                    self.initialize_request_id = request_id
                if control is not None:
                    subtype, control_id = control
                    if subtype not in _MERGED_CONTROLS:
                        self.session_controls = [
                            entry for entry in self.session_controls if entry[0] != subtype
                        ]
                    self.session_controls.append((subtype, control_id, raw))
                self.pending.append(raw)
                self.pending_bytes += len(raw)
                self.max_observed_pending_bytes = max(
                    self.max_observed_pending_bytes, self.pending_bytes
                )
                self.condition.notify_all()

    def _write(self) -> None:
        while True:
            close_target = None
            with self.condition:
                while True:
                    if self.stopped:
                        return
                    if not self.paused and self.target is not None and self.pending:
                        raw = self.pending.popleft()
                        self.pending_bytes -= len(raw)
                        target = self.target
                        self.writer_active = True
                        self.condition.notify_all()
                        break
                    if (
                        not self.paused
                        and self.target is not None
                        and self.eof
                        and not self.pending
                        and not self.writer_active
                    ):
                        close_target = self.target
                        self.target = None
                        break
                    self.condition.wait()
            if close_target is not None:
                try:
                    close_target.close()
                except (OSError, ValueError):
                    pass
                continue
            succeeded = self._write_line(target, raw)
            with self.condition:
                self.writer_active = False
                if not succeeded:
                    self.pending.appendleft(raw)
                    self.pending_bytes += len(raw)
                    self.paused = True
                self.condition.notify_all()

    def begin_switch(self) -> None:
        with self.condition:
            self.paused = True
            self.condition.notify_all()

    def wait_until_idle(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self.condition:
            while self.writer_active:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.condition.wait(remaining)
            return True

    def complete_switch(
        self, target: BinaryIO, recovery_line: Optional[bytes]
    ) -> Set[str]:
        with self.condition:
            self.target = target
            replayed: Set[str] = set()
            if self.initialize_line is not None:
                for _subtype, control_id, line in reversed(self.session_controls):
                    self.pending.appendleft(line)
                    self.pending_bytes += len(line)
                    replayed.add(control_id)
                self.pending.appendleft(self.initialize_line)
                self.pending_bytes += len(self.initialize_line)
                if self.initialize_request_id is not None:
                    replayed.add(self.initialize_request_id)
            if recovery_line is not None:
                self.pending.append(recovery_line)
                self.pending_bytes += len(recovery_line)
            self.paused = False
            self.condition.notify_all()
            return replayed

    def abort_switch(self, target: BinaryIO) -> None:
        with self.condition:
            self.target = target
            self.paused = False
            self.condition.notify_all()

    def detach(self) -> None:
        with self.condition:
            self.target = None
            self.stopped = True
            self.condition.notify_all()


class PoolProcess:
    RECOVERY_TEXT = (
        "Switched accounts after a usage limit. Continue the previous request from where you "
        "stopped; check what already finished before redoing anything."
    )

    def __init__(self, config: PoolConfig):
        self.config = config
        self.state = StateStore(config.state_path)
        self.project_slug = cwd_slug(config.cwd)
        self._session_lock = None
        self._switching_allowed = True
        self.last_switch_error: Optional[BaseException] = None
        self._children = set()

    def _account(self, slug: Optional[str]) -> Optional[AccountStatus]:
        return next((account for account in self.config.accounts if account.slug == slug), None)

    def _ranked(self, exclude: Iterable[str] = ()) -> List[AccountStatus]:
        if self.config.account_cache_path is not None and self.config.fallback_config_dir is not None:
            self.config.accounts = load_cached_account_statuses(
                self.config.account_cache_path,
                self.config.fallback_config_dir,
                settings_path=self.config.account_settings_path,
                configured_accounts=self.config.configured_accounts,
                now=self.config.now,
            )
        excluded = set(exclude)
        return [
            account
            for account in rank_accounts(
                self.config.accounts, self.state.full_until, self.config.now()
            )
            if account.slug not in excluded
        ]

    def _select_initial(self, session_id: Optional[str]) -> Optional[AccountStatus]:
        previous_slug = self.state.session_account(session_id) if session_id else None
        previous = self._account(previous_slug)
        if previous and previous.signed_in and not self.state.is_full(previous.slug, self.config.now()):
            return previous
        ranked = self._ranked()
        if ranked:
            return ranked[0]
        if previous:
            return previous
        return next((account for account in self.config.accounts if account.signed_in), None)

    def _sync_history(self, session_id: str, target: AccountStatus) -> bool:
        source = find_newest_history(
            [account.config_dir for account in self.config.accounts], self.project_slug, session_id
        )
        if source is None:
            return False
        destination = history_path(target.config_dir, self.project_slug, session_id)
        if source != destination:
            atomic_copy_history(source, destination)
        return True

    def _spawn(
        self, account: AccountStatus, args: Sequence[str], env: Mapping[str, str]
    ) -> subprocess.Popen:
        child_env = os.environ.copy()
        child_env.update(env)
        child_env["CLAUDE_CONFIG_DIR"] = str(account.config_dir)
        child = subprocess.Popen(
            list(self.config.command) + list(args),
            cwd=str(self.config.cwd),
            env=child_env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            bufsize=0,
            start_new_session=True,
        )
        self._children.add(child)
        return child

    def _terminate(self, child: subprocess.Popen) -> None:
        if child.poll() is not None:
            return
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()

    def _close_child_pipes(self, child: subprocess.Popen) -> None:
        for stream in (child.stdin, child.stdout):
            if stream is not None:
                try:
                    stream.close()
                except (OSError, ValueError):
                    pass

    def _finish(self, child: subprocess.Popen, router: _InputRouter) -> int:
        router.detach()
        code = child.wait()
        self._close_child_pipes(child)
        self._children.discard(child)
        if self._session_lock is not None:
            self._session_lock.close()
            self._session_lock = None
        return code

    def shutdown(self, forwarded_signal: int = signal.SIGTERM) -> None:
        for child in list(self._children):
            if child.poll() is None:
                try:
                    os.killpg(child.pid, forwarded_signal)
                    child.wait(timeout=2)
                except ProcessLookupError:
                    pass
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    child.wait()
            self._close_child_pipes(child)
            self._children.discard(child)
        if self._session_lock is not None:
            self._session_lock.close()
            self._session_lock = None

    def _lock_session(self, session_id: str) -> None:
        if self._session_lock is not None:
            return
        lock_dir = self.config.state_path.parent / "locks"
        lock_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        digest = hashlib.sha256(session_id.encode()).hexdigest()
        lock_file = (lock_dir / (digest + ".lock")).open("a+")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            lock_file.close()
            self._switching_allowed = False
            return
        self._session_lock = lock_file

    def _recovery_line(self, session_id: str) -> bytes:
        event = {
            "type": "user",
            "message": {"role": "user", "content": self.RECOVERY_TEXT},
            "session_id": session_id,
        }
        return json.dumps(event, separators=(",", ":")).encode() + b"\n"

    def _run_impl(
        self,
        args: Sequence[str],
        env: Optional[Mapping[str, str]] = None,
        stdin: Optional[BinaryIO] = None,
        stdout: Optional[BinaryIO] = None,
    ) -> int:
        env = {} if env is None else env
        stdin = getattr(os.sys.stdin, "buffer", os.sys.stdin) if stdin is None else stdin
        stdout = getattr(os.sys.stdout, "buffer", os.sys.stdout) if stdout is None else stdout
        session_id = session_id_from_args(args)
        if session_id:
            self._lock_session(session_id)

        account = self._select_initial(session_id)
        if account is None:
            return 1

        previous_slug = self.state.session_account(session_id) if session_id else None
        launch_args = list(args)
        if session_id and previous_slug and previous_slug != account.slug:
            if self._sync_history(session_id, account):
                launch_args = resume_args(args, session_id)
            else:
                previous = self._account(previous_slug)
                if previous is not None:
                    account = previous

        child = self._spawn(account, launch_args, env)
        assert child.stdin is not None and child.stdout is not None
        router = _InputRouter(stdin, child.stdin, self.config.input_buffer_bytes)
        router.start()
        suppress_response_ids: Set[str] = set()
        confirmed_init = False
        pending_limit_raw: Optional[bytes] = None
        pending_previous_account: Optional[AccountStatus] = None

        while True:
            raw = child.stdout.readline()
            if not raw:
                if pending_limit_raw is not None:
                    self.state.mark_full(account.slug, self.config.now() + 300)
                    stdout.write(pending_limit_raw)
                    stdout.flush()
                    if session_id is not None and pending_previous_account is not None:
                        self.state.set_session_account(session_id, pending_previous_account.slug)
                    pending_limit_raw = None
                return self._finish(child, router)

            event = _json_line(raw)
            if event is not None:
                observed_session = event.get("session_id")
                if (
                    event.get("type") == "system"
                    and event.get("subtype") == "init"
                    and valid_session_id(observed_session)
                ):
                    session_id = observed_session
                    confirmed_init = True
                    self._lock_session(session_id)
                    if pending_limit_raw is None:
                        self.state.set_session_account(session_id, account.slug)

                suppressed = next(
                    (
                        request_id
                        for request_id in suppress_response_ids
                        if _matches_control_response(event, request_id)
                    ),
                    None,
                )
                if suppressed is not None:
                    suppress_response_ids.discard(suppressed)
                    continue

                limit = parse_limit_event(event)
                if (
                    pending_limit_raw is not None
                    and limit is None
                    and event.get("type") not in ("system", "control_response")
                    and session_id is not None
                ):
                    self.state.set_session_account(session_id, account.slug)
                    pending_limit_raw = None
                    pending_previous_account = None
                if limit is not None:
                    effective_session = session_id if confirmed_init else None
                    reset_at = limit.reset_at
                    if reset_at is None or reset_at <= self.config.now():
                        reset_at = self.config.now() + 300
                    self.state.mark_full(account.slug, reset_at)

                    if pending_limit_raw is not None:
                        stdout.write(pending_limit_raw)
                        stdout.flush()
                        pending_limit_raw = None
                        if session_id is not None and pending_previous_account is not None:
                            self.state.set_session_account(
                                session_id, pending_previous_account.slug
                            )
                        pending_previous_account = None
                        self._switching_allowed = False
                        continue

                    target_candidates = self._ranked(exclude=(account.slug,))
                    can_switch = (
                        self._switching_allowed
                        and effective_session is not None
                        and confirmed_init
                        and router.initialize_line is not None
                        and bool(target_candidates)
                    )
                    if can_switch:
                        target = target_candidates[0]
                        router.begin_switch()
                        if self.config.switch_started is not None:
                            self.config.switch_started()
                        self._terminate(child)
                        router.wait_until_idle(timeout=2)
                        self._close_child_pipes(child)
                        new_child = None
                        try:
                            if not self._sync_history(effective_session, target):
                                raise FileNotFoundError("session history unavailable")
                            new_args = resume_args(args, effective_session)
                            new_child = self._spawn(target, new_args, env)
                            assert new_child.stdin is not None and new_child.stdout is not None
                        except (OSError, subprocess.SubprocessError) as error:
                            self.last_switch_error = error
                            if new_child is not None:
                                self._terminate(new_child)
                                self._close_child_pipes(new_child)
                            fallback_child = None
                            try:
                                fallback_child = self._spawn(
                                    account, resume_args(args, effective_session), env
                                )
                                assert (
                                    fallback_child.stdin is not None
                                    and fallback_child.stdout is not None
                                )
                                suppress_response_ids = router.complete_switch(
                                    fallback_child.stdin, None
                                )
                                child = fallback_child
                                self._switching_allowed = False
                                confirmed_init = False
                                stdout.write(raw)
                                stdout.flush()
                                continue
                            except (OSError, subprocess.SubprocessError):
                                if fallback_child is not None:
                                    self._terminate(fallback_child)
                                    self._close_child_pipes(fallback_child)
                                router.detach()
                                stdout.write(raw)
                                stdout.flush()
                                return child.returncode if child.returncode is not None else 1

                        try:
                            suppress_response_ids = router.complete_switch(
                                new_child.stdin, self._recovery_line(effective_session)
                            )
                            pending_limit_raw = raw
                            pending_previous_account = account
                            child = new_child
                            account = target
                            session_id = effective_session
                            confirmed_init = False
                            continue
                        except (OSError, subprocess.SubprocessError) as error:
                            self.last_switch_error = error
                            self._terminate(new_child)
                            self._close_child_pipes(new_child)
                            router.detach()
                            stdout.write(raw)
                            stdout.flush()
                            if self._session_lock is not None:
                                self._session_lock.close()
                                self._session_lock = None
                            return child.returncode if child.returncode is not None else 1

            stdout.write(raw)
            stdout.flush()

    def run(
        self,
        args: Sequence[str],
        env: Optional[Mapping[str, str]] = None,
        stdin: Optional[BinaryIO] = None,
        stdout: Optional[BinaryIO] = None,
    ) -> int:
        try:
            return self._run_impl(args, env=env, stdin=stdin, stdout=stdout)
        finally:
            self.shutdown()


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    home = Path.home()
    real_claude = os.environ.get("CLAUDE_POOL_REAL_CLAUDE")
    if not real_claude:
        installed = home / ".local/bin/claude"
        real_claude = str(installed) if installed.exists() else shutil.which("claude")
    if not real_claude:
        return 127
    if args == ["--version"]:
        return subprocess.call([real_claude, *args], env=os.environ)

    fallback_dir = Path(os.environ.get("CLAUDE_CONFIG_DIR", str(home / ".claude")))
    status_script_value = os.environ.get("CLAUDE_POOL_STATUS_SCRIPT")
    status_script = Path(status_script_value) if status_script_value else None
    cache_path = Path(
        os.environ.get("CLAUDE_POOL_CACHE", str(home / ".cache/claude-pool/usage.json"))
    )
    state_path = Path(
        os.environ.get("CLAUDE_POOL_STATE", str(home / ".local/state/claude-pool/state.json"))
    )
    settings_path = Path(
        os.environ.get("CLAUDE_POOL_T3_SETTINGS", str(home / ".t3/userdata/settings.json"))
    )
    config_path = Path(
        os.environ.get("CLAUDE_POOL_CONFIG", str(home / ".config/claude-pool/config.json"))
    )
    configured_accounts = None
    if config_path.is_file():
        try:
            configured_accounts = load_pool_config(config_path)
        except ValueError as error:
            raise SystemExit(f"invalid Claude Pool config: {error}")
    accounts = load_cached_account_statuses(
        cache_path,
        fallback_dir,
        settings_path=settings_path,
        configured_accounts=configured_accounts,
    )

    config = PoolConfig(
        command=[real_claude],
        accounts=accounts,
        state_path=state_path,
        cwd=Path.cwd(),
        account_cache_path=cache_path,
        account_settings_path=settings_path,
        fallback_config_dir=fallback_dir,
        configured_accounts=configured_accounts,
    )
    pool = PoolProcess(config)
    if status_script is not None and status_script.is_file():
        start_account_refresh(cache_path, status_script, fallback_dir)
    previous_handlers = {}

    def forward_signal(signum, _frame):
        pool.shutdown(signum)
        raise SystemExit(128 + signum)

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, forward_signal)
    try:
        return pool.run(args, env=os.environ)
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--internal-refresh-cache":
        raise SystemExit(
            _refresh_cache_worker(Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
        )
    raise SystemExit(main())
