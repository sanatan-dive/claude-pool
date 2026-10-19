import json
import multiprocessing
import os
import signal
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import closing, redirect_stdout
from io import BytesIO, StringIO
from pathlib import Path
from unittest import mock

from claude_pool import (
    AccountStatus,
    PoolConfig,
    PoolProcess,
    StateStore,
    atomic_copy_history,
    cwd_slug,
    find_newest_history,
    load_cached_account_statuses,
    load_account_statuses,
    load_pool_config,
    parse_limit_event,
    rank_accounts,
    resume_args,
    session_id_from_args,
    start_account_refresh,
    _InputRouter,
)
from setup_pool import (
    discover_accounts,
    ensure_t3_project,
    find_project_id,
    main as setup_main,
    parse_account_specs,
    update_settings_file,
)


def run_saturation_fixture(root_text, fake_child_text):
    root = Path(root_text)
    account_a = root / "account-a"
    account_b = root / "account-b"
    behavior_path = root / "behavior.json"
    behavior_path.write_text(
        json.dumps(
            {
                "account-a": {"mode": "limit_no_read"},
                "account-b": {"mode": "continue", "discard": True},
            }
        )
    )
    config = PoolConfig(
        command=[os.sys.executable, fake_child_text],
        accounts=[
            AccountStatus("a", account_a, True, 10, 10),
            AccountStatus("b", account_b, True, 20, 20),
        ],
        state_path=root / "state" / "state.json",
        cwd=root,
    )
    huge = b'{"blob":"' + (b"x" * (2 * 1024 * 1024)) + b'"}\n'
    stdin = BytesIO(
        b'{"type":"control_request","request_id":"init-1","request":{"subtype":"initialize"}}\n'
        + huge
    )
    code = PoolProcess(config).run(
        [],
        {"FAKE_BEHAVIOR_FILE": str(behavior_path), "FAKE_SESSION_ID": "fixture-session"},
        stdin=stdin,
        stdout=BytesIO(),
    )
    if code != 0:
        raise SystemExit(code)


def run_refresh_launcher_fixture(
    cache_text,
    script_text,
    fallback_text,
    exit_immediately=False,
    timeout_text=None,
    worker_pid_text=None,
):
    if timeout_text is not None:
        os.environ["CLAUDE_POOL_STATUS_TIMEOUT"] = timeout_text
    worker = start_account_refresh(Path(cache_text), Path(script_text), Path(fallback_text))
    if worker_pid_text is not None:
        Path(worker_pid_text).write_text(str(worker.pid))
    if exit_immediately:
        os._exit(0)
    time.sleep(30)


def process_is_running_command(pid, marker):
    proc_cmdline = Path(f"/proc/{pid}/cmdline")
    if proc_cmdline.exists():
        try:
            return marker.encode() in proc_cmdline.read_bytes()
        except OSError:
            return False
    completed = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True, check=False
    )
    return completed.returncode == 0 and marker in completed.stdout


class HealthCheckIntegrationTests(unittest.TestCase):
    def test_version_probe_exits_cleanly_while_parent_keeps_stdin_open(self):
        """Catch the T3 health probe abort caused by a blocked stdin reader."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fake_claude = root / "claude"
            fake_claude.write_text("#!/bin/sh\nprintf 'fixture-version\\n'\n")
            fake_claude.chmod(0o755)
            invalid_config = root / "invalid-config.json"
            invalid_config.write_text("not-json")
            status_script = root / "status.py"
            status_script.write_text("print('{\\\"accounts\\\": []}')\n")
            env = os.environ.copy()
            env.update(
                {
                    "CLAUDE_POOL_REAL_CLAUDE": str(fake_claude),
                    "CLAUDE_POOL_CONFIG": str(invalid_config),
                    "CLAUDE_POOL_CACHE": str(root / "cache.json"),
                    "CLAUDE_POOL_STATE": str(root / "state.json"),
                    "CLAUDE_POOL_T3_SETTINGS": str(root / "settings.json"),
                    "CLAUDE_POOL_STATUS_SCRIPT": str(status_script),
                    "CLAUDE_CONFIG_DIR": str(root / ".claude"),
                }
            )
            process = subprocess.Popen(
                [os.sys.executable, str(Path(__file__).parents[1] / "claude_pool.py"), "--version"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )
            try:
                returncode = process.wait(timeout=5)
                stdout = process.stdout.read() if process.stdout is not None else b""
                stderr = process.stderr.read() if process.stderr is not None else b""
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                if process.stdin is not None:
                    process.stdin.close()
                if process.stdout is not None:
                    process.stdout.close()
                if process.stderr is not None:
                    process.stderr.close()

            self.assertEqual(returncode, 0, stderr.decode(errors="replace"))
            self.assertEqual(stdout, b"fixture-version\n")


class ProtocolDecisionTests(unittest.TestCase):
    def test_rejected_rate_event_is_actionable(self):
        event = {
            "type": "rate_limit_event",
            "rate_limit_info": {
                "status": "rejected",
                "resetsAt": 1_792_275_600,
                "rateLimitType": "five_hour",
            },
            "session_id": "session-1",
        }

        hit = parse_limit_event(event)

        self.assertIsNotNone(hit)
        self.assertEqual(hit.reset_at, 1_792_275_600)
        self.assertEqual(hit.kind, "five_hour")
        self.assertEqual(hit.session_id, "session-1")

    def test_warning_rate_event_is_not_actionable(self):
        event = {
            "type": "rate_limit_event",
            "rate_limit_info": {
                "status": "allowed_warning",
                "resetsAt": 1_792_275_600,
                "rateLimitType": "five_hour",
            },
        }

        self.assertIsNone(parse_limit_event(event))

    def test_measured_assistant_api_error_is_actionable(self):
        event = {
            "type": "assistant",
            "error": "rate_limit",
            "is_api_error_message": True,
            "api_error": "usage_limit_reached",
            "api_error_params": {
                "rate_limit_info": {
                    "status": "rejected",
                    "resetsAt": 1_800_000_000,
                    "rateLimitType": "seven_day",
                }
            },
            "session_id": "session-2",
        }

        hit = parse_limit_event(event)

        self.assertEqual(hit.reset_at, 1_800_000_000)
        self.assertEqual(hit.kind, "seven_day")
        self.assertEqual(hit.session_id, "session-2")

    def test_unmeasured_error_shape_passes_through(self):
        self.assertIsNone(parse_limit_event({"type": "error", "error": "rate_limit"}))

    def test_resume_arguments_are_normalized_without_swallowing_neighbors(self):
        original = [
            "-p",
            "continue",
            "--resume",
            "old-one",
            "--allowedTools",
            "Read",
            "Write",
            "--session-id=old-two",
            "--verbose",
        ]

        normalized = resume_args(original, "new-session")

        self.assertEqual(
            normalized,
            [
                "-p",
                "continue",
                "--allowedTools",
                "Read",
                "Write",
                "--verbose",
                "--resume=new-session",
            ],
        )

    def test_cwd_slug_matches_claude_project_layout(self):
        self.assertEqual(cwd_slug(Path("/home/user/accounts/pool")), "-home-user-accounts-pool")
        self.assertEqual(cwd_slug(Path("/tmp/a.b/c")), "-tmp-a-b-c")

    def test_malformed_session_selectors_preserve_neighboring_options(self):
        args = ["--resume", "--verbose", "--", "--session-id", "prompt-text"]

        self.assertIsNone(session_id_from_args(args))
        self.assertEqual(
            resume_args(args, "valid-session"),
            ["--verbose", "--resume=valid-session", "--", "--session-id", "prompt-text"],
        )

    def test_hostile_session_id_cannot_escape_project_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            outside = root / "escape.jsonl"
            outside.write_text("private")

            self.assertIsNone(find_newest_history([root / "account"], "-work", "../../escape"))
            self.assertIsNone(session_id_from_args(["--resume=../../escape"]))


class HistoryTests(unittest.TestCase):
    def test_newest_exact_session_history_wins(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = root / "a" / "projects" / "-work" / "s-1.jsonl"
            second = root / "b" / "projects" / "-work" / "s-1.jsonl"
            decoy = root / "b" / "projects" / "-work" / "s-10.jsonl"
            for path, content in ((first, "old"), (second, "new"), (decoy, "decoy")):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
            os.utime(first, (100, 100))
            os.utime(second, (200, 200))
            os.utime(decoy, (300, 300))

            found = find_newest_history([root / "a", root / "b"], "-work", "s-1")

            self.assertEqual(found, second)

    def test_atomic_copy_replaces_only_after_complete_copy(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "source.jsonl"
            target = root / "nested" / "target.jsonl"
            source.write_text("complete-history")
            target.parent.mkdir(parents=True)
            target.write_text("previous-valid-history")

            with mock.patch("claude_pool.os.replace", side_effect=OSError("interrupted")):
                with self.assertRaises(OSError):
                    atomic_copy_history(source, target)

            self.assertEqual(target.read_text(), "previous-valid-history")
            self.assertEqual(list(target.parent.glob(".target.jsonl.*.tmp")), [])

            atomic_copy_history(source, target)
            self.assertEqual(target.read_text(), "complete-history")


class StateAndSelectionTests(unittest.TestCase):
    def test_full_marker_expires_and_session_mapping_persists(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.json"
            store = StateStore(path)
            store.mark_full("main", reset_at=200)
            store.set_session_account("session-1", "main")

            reloaded = StateStore(path)

            self.assertTrue(reloaded.is_full("main", now=199))
            self.assertFalse(reloaded.is_full("main", now=200))
            self.assertEqual(reloaded.session_account("session-1"), "main")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_concurrent_state_stores_merge_instead_of_losing_updates(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.json"
            first = StateStore(path)
            second = StateStore(path)

            first.mark_full("main", reset_at=500)
            second.set_session_account("session-2", "work2")

            merged = StateStore(path)
            self.assertTrue(merged.is_full("main", now=100))
            self.assertEqual(merged.session_account("session-2"), "work2")

    def test_accounts_rank_by_tightest_remaining_window(self):
        accounts = [
            AccountStatus("main", Path("/main"), True, session_percent=20, weekly_percent=70),
            AccountStatus("work1", Path("/work1"), True, session_percent=50, weekly_percent=50),
            AccountStatus("signed-out", Path("/out"), False, session_percent=0, weekly_percent=0),
        ]

        ranked = rank_accounts(accounts, full_until={"work1": 500}, now=100)

        self.assertEqual([account.slug for account in ranked], ["main"])

    def test_unknown_usage_is_ranked_after_known_capacity(self):
        accounts = [
            AccountStatus("unknown", Path("/unknown"), True),
            AccountStatus("known", Path("/known"), True, session_percent=90, weekly_percent=90),
        ]

        ranked = rank_accounts(accounts, full_until={}, now=time.time())

        self.assertEqual([account.slug for account in ranked], ["known", "unknown"])


class PortableInventoryTests(unittest.TestCase):
    def test_config_expands_home_and_preserves_declared_order(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / "config.json"
            config.write_text(
                json.dumps(
                    {
                        "accounts": [
                            {"slug": "second", "config_dir": "~/.claude-second"},
                            {"slug": "first", "config_dir": str(root / ".claude-first")},
                        ]
                    }
                )
            )

            with mock.patch.dict(os.environ, {"HOME": str(root)}):
                accounts = load_pool_config(config)

            self.assertEqual([account.slug for account in accounts], ["second", "first"])
            self.assertEqual(accounts[0].config_dir, root / ".claude-second")
            self.assertTrue(all(account.switch_eligible for account in accounts))
            self.assertEqual(
                [account.slug for account in rank_accounts(accounts, {}, now=1_000)],
                ["second", "first"],
            )

    def test_config_rejects_duplicate_slug_or_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / "config.json"
            for accounts in (
                [
                    {"slug": "same", "config_dir": str(root / "one")},
                    {"slug": "same", "config_dir": str(root / "two")},
                ],
                [
                    {"slug": "one", "config_dir": str(root / "same")},
                    {"slug": "two", "config_dir": str(root / "same")},
                ],
            ):
                config.write_text(json.dumps({"accounts": accounts}))
                with self.assertRaises(ValueError):
                    load_pool_config(config)

    def test_config_rejects_duplicate_canonical_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            canonical_dir = root / "account"
            canonical_dir.mkdir()
            alias_dir = root / "account-alias"
            alias_dir.symlink_to(canonical_dir, target_is_directory=True)
            config = root / "config.json"
            config.write_text(
                json.dumps(
                    {
                        "accounts": [
                            {"slug": "first", "config_dir": str(canonical_dir)},
                            {"slug": "second", "config_dir": str(alias_dir)},
                        ]
                    }
                )
            )

            with self.assertRaisesRegex(ValueError, "duplicate account config_dir"):
                load_pool_config(config)

    def test_explicit_inventory_replaces_stale_usage_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / "config.json"
            config.write_text(
                json.dumps(
                    {
                        "accounts": [
                            {"slug": "work2", "config_dir": str(root / "work2")},
                            {"slug": "main", "config_dir": str(root / "main")},
                        ]
                    }
                )
            )
            cache = root / "usage.json"
            cache.write_text(
                json.dumps(
                    {
                        "attempted_at": 1,
                        "fetched_at": 1,
                        "payload": {
                            "accounts": [
                                {
                                    "slug": "main",
                                    "dir": str(root / "main"),
                                    "logged_in": True,
                                    "usage": {
                                        "saved": False,
                                        "limits": [{"kind": "session", "percent": 99}],
                                    },
                                }
                            ]
                        },
                    }
                )
            )

            configured = load_pool_config(config)
            accounts = load_cached_account_statuses(
                cache,
                root / "main",
                configured_accounts=configured,
                now=lambda: 1_000,
            )

            self.assertEqual([account.slug for account in accounts], ["work2", "main"])
            self.assertTrue(all(account.switch_eligible for account in accounts))
            self.assertTrue(all(account.session_percent is None for account in accounts))

    def test_fresh_cache_does_not_follow_a_repointed_slug_to_another_account(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            previous_dir = root / "previous-main"
            configured_dir = root / "configured-main"
            cache = root / "usage.json"
            cache.write_text(
                json.dumps(
                    {
                        "attempted_at": 900,
                        "payload": {
                            "accounts": [
                                {
                                    "slug": "main",
                                    "dir": str(previous_dir),
                                    "logged_in": False,
                                    "usage": {
                                        "saved": False,
                                        "limits": [{"kind": "session", "percent": 99}],
                                    },
                                }
                            ]
                        },
                    }
                )
            )
            configured = [AccountStatus("main", configured_dir, True)]

            accounts = load_cached_account_statuses(
                cache,
                configured_dir,
                configured_accounts=configured,
                now=lambda: 1_000,
            )

            self.assertEqual(
                accounts,
                [AccountStatus("main", configured_dir, True)],
            )

    def test_fresh_cache_matches_renamed_account_by_canonical_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            canonical_dir = root / "account"
            canonical_dir.mkdir()
            configured_dir = root / "account-alias"
            configured_dir.symlink_to(canonical_dir, target_is_directory=True)
            cache = root / "usage.json"
            cache.write_text(
                json.dumps(
                    {
                        "attempted_at": 900,
                        "payload": {
                            "accounts": [
                                {
                                    "slug": "old-name",
                                    "dir": str(canonical_dir),
                                    "logged_in": False,
                                    "usage": {
                                        "saved": False,
                                        "limits": [
                                            {"kind": "session", "percent": 23},
                                            {"kind": "weekly_all", "percent": 45},
                                        ],
                                    },
                                }
                            ]
                        },
                    }
                )
            )
            configured = [AccountStatus("new-name", configured_dir, True)]

            accounts = load_cached_account_statuses(
                cache,
                configured_dir,
                configured_accounts=configured,
                now=lambda: 1_000,
            )

            self.assertEqual(
                accounts,
                [
                    AccountStatus(
                        "new-name",
                        configured_dir,
                        False,
                        session_percent=23,
                        weekly_percent=45,
                        switch_eligible=False,
                    )
                ],
            )

    def test_missing_optional_usage_helper_does_not_create_failed_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fake_claude = root / "claude"
            fake_claude.write_text("#!/bin/sh\nexit 0\n")
            fake_claude.chmod(0o755)
            config = root / "config.json"
            config.write_text(
                json.dumps(
                    {"accounts": [{"slug": "main", "config_dir": str(root / ".claude")}]}
                )
            )
            cache = root / "usage.json"
            env = os.environ.copy()
            env.update(
                {
                    "CLAUDE_POOL_REAL_CLAUDE": str(fake_claude),
                    "CLAUDE_POOL_CONFIG": str(config),
                    "CLAUDE_POOL_CACHE": str(cache),
                    "CLAUDE_POOL_STATE": str(root / "state.json"),
                    "CLAUDE_POOL_T3_SETTINGS": str(root / "settings.json"),
                    "CLAUDE_POOL_STATUS_SCRIPT": str(root / "missing-status.py"),
                    "CLAUDE_CONFIG_DIR": str(root / ".claude"),
                }
            )

            completed = subprocess.run(
                [os.sys.executable, str(Path(__file__).parents[1] / "claude_pool.py")],
                input=b"",
                capture_output=True,
                env=env,
                timeout=5,
            )
            time.sleep(0.2)

            self.assertEqual(completed.returncode, 0, completed.stderr.decode(errors="replace"))
            self.assertFalse(cache.exists())

    def test_account_status_sweep_is_cached_for_five_minutes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cache = root / "cache.json"
            calls = []
            payload = {
                "accounts": [
                    {
                        "slug": "main",
                        "dir": str(root / ".claude"),
                        "logged_in": True,
                        "usage": {
                            "saved": False,
                            "limits": [
                                {"kind": "session", "percent": 25},
                                {"kind": "weekly_all", "percent": 40},
                            ],
                        },
                    }
                ]
            }

            def sweep():
                calls.append(True)
                return payload

            first = load_account_statuses(cache, sweep=sweep, now=lambda: 1_000)
            second = load_account_statuses(cache, sweep=sweep, now=lambda: 1_299)

            self.assertEqual(len(calls), 1)
            self.assertEqual(first, second)
            self.assertEqual(first[0].session_percent, 25)
            self.assertEqual(first[0].weekly_percent, 40)
            self.assertEqual(cache.stat().st_mode & 0o777, 0o600)

    def test_stale_cache_is_available_without_waiting_for_a_usage_sweep(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cache = root / "cache.json"
            cache.write_text(
                json.dumps(
                    {
                        "attempted_at": 1,
                        "fetched_at": 1,
                        "payload": {
                            "accounts": [
                                {
                                    "slug": "work1",
                                    "dir": str(root / ".claude-work1"),
                                    "logged_in": True,
                                    "usage": {
                                        "saved": False,
                                        "limits": [{"kind": "session", "percent": 25}],
                                    },
                                }
                            ]
                        },
                    }
                )
            )

            accounts = load_cached_account_statuses(cache, root / ".claude", now=lambda: 1_000)

            self.assertEqual([account.slug for account in accounts], ["work1", "main"])
            self.assertTrue(all(not account.switch_eligible for account in accounts))

    def test_cache_miss_inventory_resolves_every_t3_account_for_resume(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            settings = root / "settings.json"
            settings.write_text(
                json.dumps(
                    {
                        "providerInstances": {
                            "claudeAgent_work1": {
                                "driver": "claudeAgent",
                                "config": {"homePath": str(root / ".claude-work1")},
                            },
                            "claudeAgent_pool": {
                                "driver": "claudeAgent",
                                "config": {"homePath": str(root / ".claude")},
                            },
                        }
                    }
                )
            )

            accounts = load_cached_account_statuses(
                root / "missing.json", root / ".claude", settings_path=settings
            )

            self.assertEqual([account.slug for account in accounts], ["main", "work1"])
            self.assertTrue(all(not account.switch_eligible for account in accounts))

    def test_usage_refresh_runs_out_of_process_and_updates_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            script = root / "status.py"
            script.write_text(
                "import json\n"
                + "print(json.dumps({'accounts':[{'slug':'work1','dir':"
                + repr(str(root / ".claude-work1"))
                + ",'logged_in':True,'usage':{'saved':False,'limits':"
                + "[{'kind':'session','percent':10}]}}]}))\n"
            )

            started = time.monotonic()
            refresh = start_account_refresh(
                root / "cache.json", script, root / ".claude"
            )
            self.assertLess(time.monotonic() - started, 0.5)
            refresh.wait(timeout=3)

            accounts = load_cached_account_statuses(
                root / "cache.json", root / ".claude"
            )
            self.assertEqual(accounts[0].slug, "work1")
            self.assertTrue(accounts[0].switch_eligible)

    def test_detached_refresh_finishes_after_launcher_exits(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cache = root / "cache.json"
            script = root / "status.py"
            script.write_text(
                "import json, time\ntime.sleep(0.2)\n"
                + "print(json.dumps({'accounts':[{'slug':'main','dir':"
                + repr(str(root / ".claude"))
                + ",'logged_in':True,'usage':{'saved':False,'limits':"
                + "[{'kind':'session','percent':10}]}}]}))\n"
            )
            launcher = multiprocessing.Process(
                target=run_refresh_launcher_fixture,
                args=(str(cache), str(script), str(root / ".claude"), True),
            )
            launcher.start()
            launcher.join(timeout=2)
            self.assertFalse(launcher.is_alive())
            deadline = time.time() + 3
            while time.time() < deadline:
                try:
                    cached = json.loads(cache.read_text())
                except (FileNotFoundError, ValueError):
                    cached = {}
                if cached.get("fetched_at") is not None:
                    break
                time.sleep(0.02)
            else:
                self.fail("detached refresh did not finish after launcher exit")

    def test_refresh_worker_self_terminates_after_launcher_is_sigkilled(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pid_path = root / "status.pid"
            descendant_pid_path = root / "descendant.pid"
            worker_pid_path = root / "worker.pid"
            cache_path = root / "cache.json"
            script = root / "slow_status.py"
            script.write_text(
                "import os, subprocess, sys, time\n"
                "code = "
                + repr(
                    "import os, signal, time\n"
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                    f"open({str(descendant_pid_path)!r}, 'w').write(str(os.getpid()))\n"
                    "time.sleep(30)\n"
                )
                + "\nsubprocess.Popen([sys.executable, '-c', code])\n"
                f"open({str(pid_path)!r}, 'w').write(str(os.getpid()))\n"
                "time.sleep(30)\n"
            )
            parent = multiprocessing.Process(
                target=run_refresh_launcher_fixture,
                args=(
                    str(cache_path),
                    str(script),
                    str(root / ".claude"),
                    False,
                    "0.3",
                    str(worker_pid_path),
                ),
            )
            parent.start()
            deadline = time.time() + 3
            while time.time() < deadline and (
                not pid_path.exists()
                or not descendant_pid_path.exists()
                or not worker_pid_path.exists()
            ):
                time.sleep(0.01)
            self.assertTrue(pid_path.exists())
            self.assertTrue(descendant_pid_path.exists())
            self.assertTrue(worker_pid_path.exists())
            status_pid = int(pid_path.read_text())
            descendant_pid = int(descendant_pid_path.read_text())
            worker_pid = int(worker_pid_path.read_text())
            os.kill(parent.pid, signal.SIGKILL)
            parent.join(timeout=2)

            deadline = time.time() + 3
            while time.time() < deadline:
                if not process_is_running_command(status_pid, str(script)):
                    break
                time.sleep(0.02)
            else:
                self.fail("orphaned status process survived its self-enforced deadline")
            self.assertFalse(
                process_is_running_command(descendant_pid, str(descendant_pid_path))
            )
            self.assertFalse(process_is_running_command(worker_pid, str(cache_path)))

    def test_refresh_attempt_marker_prevents_overlapping_sweeps(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cache = root / "cache.json"
            payload = {
                "accounts": [
                    {
                        "slug": "main",
                        "dir": str(root / ".claude"),
                        "logged_in": True,
                        "usage": {
                            "saved": False,
                            "limits": [{"kind": "session", "percent": 10}],
                        },
                    }
                ]
            }
            cache.write_text(json.dumps({"attempted_at": 1, "fetched_at": 1, "payload": payload}))
            started = threading.Event()
            release = threading.Event()

            def slow_sweep():
                started.set()
                release.wait(timeout=2)
                return payload

            first_result = []
            thread = threading.Thread(
                target=lambda: first_result.extend(
                    load_account_statuses(cache, sweep=slow_sweep, now=lambda: 1_000)
                )
            )
            thread.start()
            self.assertTrue(started.wait(timeout=1))
            second_calls = []
            second = load_account_statuses(
                cache,
                sweep=lambda: second_calls.append(True) or payload,
                now=lambda: 1_000,
            )
            release.set()
            thread.join(timeout=2)

            self.assertEqual(second_calls, [])
            self.assertFalse(second[0].switch_eligible)
            self.assertEqual([account.slug for account in first_result], ["main"])

    def test_failed_status_sweep_falls_back_to_t3_config_only(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fallback = root / ".claude"

            accounts = load_account_statuses(
                root / "missing-cache.json",
                sweep=lambda: (_ for _ in ()).throw(OSError("unavailable")),
                fallback_dir=fallback,
                now=lambda: 1_000,
            )

            self.assertEqual(
                accounts, [AccountStatus("fallback", fallback, True, switch_eligible=False)]
            )

    def test_failed_status_sweep_is_negative_cached_for_five_minutes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            calls = []

            def fail():
                calls.append(True)
                raise OSError("unavailable")

            first = load_account_statuses(
                root / "cache.json", sweep=fail, fallback_dir=root / ".claude", now=lambda: 1_000
            )
            second = load_account_statuses(
                root / "cache.json", sweep=fail, fallback_dir=root / ".claude", now=lambda: 1_299
            )

            self.assertEqual(len(calls), 1)
            self.assertEqual(first, second)

    def test_stale_usage_is_retained_but_never_used_for_automatic_switching(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            clock = [1_000]
            calls = []
            payload = {
                "accounts": [
                    {
                        "slug": "main",
                        "dir": str(root / ".claude"),
                        "logged_in": True,
                        "usage": {
                            "saved": False,
                            "limits": [{"kind": "session", "percent": 10}],
                        },
                    }
                ]
            }

            def sweep():
                calls.append(True)
                if len(calls) == 1:
                    return payload
                raise OSError("refresh failed")

            load_account_statuses(root / "cache.json", sweep=sweep, now=lambda: clock[0])
            clock[0] = 1_301
            stale = load_account_statuses(root / "cache.json", sweep=sweep, now=lambda: clock[0])
            clock[0] = 1_400
            cached_stale = load_account_statuses(
                root / "cache.json", sweep=sweep, now=lambda: clock[0]
            )

            self.assertEqual(len(calls), 2)
            self.assertEqual(stale[0].session_percent, 10)
            self.assertFalse(stale[0].switch_eligible)
            self.assertFalse(cached_stale[0].switch_eligible)

    def test_unhealthy_account_is_not_an_automatic_switch_target(self):
        accounts = [
            AccountStatus("healthy", Path("/healthy"), True, 95, 95),
            AccountStatus("unhealthy", Path("/unhealthy"), True, 0, 0, switch_eligible=False),
        ]

        self.assertEqual(
            [account.slug for account in rank_accounts(accounts, {}, now=100)], ["healthy"]
        )


class SupervisorFixtureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.account_a = self.root / "account-a"
        self.account_b = self.root / "account-b"
        self.account_a.mkdir()
        self.account_b.mkdir()
        self.behavior_path = self.root / "behavior.json"
        self.state_path = self.root / "state" / "state.json"
        self.fake_child = Path(__file__).with_name("fake_claude.py")
        self.accounts = [
            AccountStatus("a", self.account_a, True, 10, 10),
            AccountStatus("b", self.account_b, True, 20, 20),
        ]

    def tearDown(self):
        self.temp.cleanup()

    def config(self):
        return PoolConfig(
            command=[os.sys.executable, str(self.fake_child)],
            accounts=self.accounts,
            state_path=self.state_path,
            cwd=self.root,
        )

    def env(self):
        return {"FAKE_BEHAVIOR_FILE": str(self.behavior_path), "FAKE_SESSION_ID": "fixture-session"}

    def write_behavior(self, account_a, account_b):
        self.behavior_path.write_text(json.dumps({"account-a": account_a, "account-b": account_b}))

    @staticmethod
    def events(output):
        return [json.loads(line) for line in output.getvalue().splitlines()]

    def test_automatic_switch_hides_limit_and_continues_same_session(self):
        self.write_behavior({"mode": "limit"}, {"mode": "continue"})
        stdin = BytesIO(
            b'{"type":"control_request","request_id":"init-1","request":{"subtype":"initialize"}}\n'
            b'{"type":"user","message":{"role":"user","content":"do work"}}\n'
        )
        stdout = BytesIO()

        code = PoolProcess(self.config()).run([], self.env(), stdin=stdin, stdout=stdout)

        events = self.events(stdout)
        self.assertEqual(code, 0)
        self.assertFalse(any(event.get("type") == "rate_limit_event" for event in events))
        continued = [event for event in events if event.get("continued")]
        self.assertEqual(len(continued), 1)
        self.assertEqual(continued[0]["account"], "account-b")
        self.assertEqual(continued[0]["session_id"], "fixture-session")
        self.assertTrue(continued[0]["history_seen"])
        self.assertEqual(
            len([event for event in events if event.get("type") == "control_response"]), 1
        )

    def test_switch_replays_session_setup_so_t3_tools_survive(self):
        self.write_behavior({"mode": "limit"}, {"mode": "continue"})
        stdin = BytesIO(
            b'{"type":"control_request","request_id":"init-1","request":{"subtype":"initialize"}}\n'
            b'{"type":"control_request","request_id":"mcp-1","request":{"subtype":"mcp_set_servers","servers":{"t3-code":{"type":"http"}}}}\n'
            b'{"type":"control_request","request_id":"model-1","request":{"subtype":"set_model","model":"old"}}\n'
            b'{"type":"control_request","request_id":"flags-1","request":{"subtype":"apply_flag_settings","settings":{"a":1}}}\n'
            b'{"type":"control_request","request_id":"model-2","request":{"subtype":"set_model","model":"new"}}\n'
            b'{"type":"control_request","request_id":"flags-2","request":{"subtype":"apply_flag_settings","settings":{"b":2}}}\n'
            b'{"type":"user","message":{"role":"user","content":"do work"}}\n'
        )
        stdout = BytesIO()

        code = PoolProcess(self.config()).run([], self.env(), stdin=stdin, stdout=stdout)

        events = self.events(stdout)
        self.assertEqual(code, 0)
        continued = [event for event in events if event.get("continued")]
        self.assertEqual(len(continued), 1)
        self.assertEqual(continued[0]["account"], "account-b")
        self.assertEqual(
            continued[0]["controls"],
            ["initialize", "mcp_set_servers", "apply_flag_settings", "set_model", "apply_flag_settings"],
        )
        responses = [event["request_id"] for event in events if event.get("type") == "control_response"]
        self.assertEqual(sorted(responses), sorted(["init-1", "mcp-1", "model-1", "flags-1", "model-2", "flags-2"]))
        self.assertTrue(all(event["account"] == "account-a" for event in events if event.get("type") == "control_response"))

    def test_source_is_stopped_before_final_history_copy_and_target_start(self):
        target_started = self.root / "target-started"
        self.write_behavior(
            {
                "mode": "limit",
                "write_history_on_term": True,
                "overlap_marker": str(target_started),
            },
            {"mode": "continue", "started_marker": str(target_started)},
        )
        stdout = BytesIO()

        PoolProcess(self.config()).run(
            [],
            self.env(),
            stdin=BytesIO(
                b'{"type":"control_request","request_id":"init-1","request":{"subtype":"initialize"}}\n'
                b'{"type":"user","message":{"role":"user","content":"do work"}}\n'
            ),
            stdout=stdout,
        )

        continued = [event for event in self.events(stdout) if event.get("continued")]
        self.assertEqual(len(continued), 1)
        self.assertTrue(continued[0]["final_history_seen"])
        source_history = next(self.account_a.rglob("fixture-session.jsonl")).read_text()
        self.assertNotIn("overlap-detected", source_history)

    def test_all_other_accounts_full_passes_original_limit_unchanged(self):
        self.write_behavior({"mode": "limit"}, {"mode": "continue"})
        StateStore(self.state_path).mark_full("b", reset_at=4_000_000_000)
        stdin = BytesIO(
            b'{"type":"control_request","request_id":"init-1","request":{"subtype":"initialize"}}\n'
            b'{"type":"user","message":{"role":"user","content":"do work"}}\n'
        )
        stdout = BytesIO()

        PoolProcess(self.config()).run([], self.env(), stdin=stdin, stdout=stdout)

        events = self.events(stdout)
        limits = [event for event in events if event.get("type") == "rate_limit_event"]
        self.assertEqual(len(limits), 1)
        self.assertEqual(limits[0]["rate_limit_info"]["status"], "rejected")

    def test_limit_before_confirmed_system_init_is_passed_through(self):
        self.write_behavior({"mode": "limit_before_init"}, {"mode": "continue"})
        stdout = BytesIO()

        PoolProcess(self.config()).run(
            [],
            self.env(),
            stdin=BytesIO(
                b'{"type":"control_request","request_id":"init-1","request":{"subtype":"initialize"}}\n'
            ),
            stdout=stdout,
        )

        self.assertTrue(any(e.get("type") == "rate_limit_event" for e in self.events(stdout)))
        self.assertFalse(any(e.get("continued") for e in self.events(stdout)))

    def test_saturated_old_stdin_cannot_deadlock_switch(self):
        self.write_behavior({}, {})
        process = multiprocessing.Process(
            target=run_saturation_fixture, args=(str(self.root), str(self.fake_child))
        )
        process.start()
        process.join(timeout=4)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
            self.fail("pool deadlocked while the old child stdin pipe was full")
        self.assertEqual(process.exitcode, 0)

    def test_manual_continue_relaunches_on_other_account(self):
        self.write_behavior({"mode": "limit"}, {"mode": "continue"})
        first_output = BytesIO()
        # No initialize request means automatic replay is intentionally unsafe.
        PoolProcess(self.config()).run(
            [],
            self.env(),
            stdin=BytesIO(b'{"type":"user","message":{"role":"user","content":"first"}}\n'),
            stdout=first_output,
        )
        self.assertTrue(any(e.get("type") == "rate_limit_event" for e in self.events(first_output)))

        second_output = BytesIO()
        PoolProcess(self.config()).run(
            ["--resume=fixture-session"],
            self.env(),
            stdin=BytesIO(
                b'{"type":"control_request","request_id":"init-2","request":{"subtype":"initialize"}}\n'
                b'{"type":"user","message":{"role":"user","content":"continue"}}\n'
            ),
            stdout=second_output,
        )

        second_events = self.events(second_output)
        self.assertTrue(any(e.get("account") == "account-b" for e in second_events))
        self.assertTrue(any(e.get("history_seen") for e in second_events))

    def test_input_arriving_during_switch_is_delivered_once_before_recovery(self):
        started = threading.Event()
        buffered = threading.Event()
        self.write_behavior(
            {"mode": "limit"},
            {"mode": "continue"},
        )
        read_fd, write_fd = os.pipe()
        stdin = os.fdopen(read_fd, "rb", buffering=0)
        writer = os.fdopen(write_fd, "wb", buffering=0)
        stdout = BytesIO()

        def provide_input():
            writer.write(
                b'{"type":"control_request","request_id":"init-1","request":{"subtype":"initialize"}}\n'
            )
            writer.write(b'{"type":"user","message":{"role":"user","content":"do work"}}\n')
            started.wait(timeout=3)
            writer.write(b'{"type":"tool_result","tool_use_id":"once-only"}\n')
            buffered.set()
            writer.close()

        def switching_started():
            started.set()
            buffered.wait(timeout=3)

        thread = threading.Thread(target=provide_input)
        thread.start()
        try:
            config = self.config()
            config.switch_started = switching_started
            PoolProcess(config).run([], self.env(), stdin=stdin, stdout=stdout)
        finally:
            thread.join(timeout=3)
            stdin.close()

        events = self.events(stdout)
        tool_indexes = [
            index
            for index, event in enumerate(events)
            if event.get("input", {}).get("tool_use_id") == "once-only"
        ]
        recovery_indexes = [index for index, event in enumerate(events) if event.get("continued")]
        self.assertEqual(len(tool_indexes), 1)
        self.assertEqual(len(recovery_indexes), 1)
        self.assertLess(tool_indexes[0], recovery_indexes[0])

    def test_target_spawn_failure_resumes_old_child_and_preserves_buffered_input(self):
        started = threading.Event()
        buffered = threading.Event()
        self.write_behavior(
            {"mode": "limit", "after_limit_read": True},
            {"mode": "continue"},
        )
        read_fd, write_fd = os.pipe()
        stdin = os.fdopen(read_fd, "rb", buffering=0)
        writer = os.fdopen(write_fd, "wb", buffering=0)
        stdout = BytesIO()
        pool = PoolProcess(self.config())
        real_spawn = pool._spawn

        def fail_target(account, args, env):
            if account.slug == "b":
                raise OSError("fixture spawn failure")
            return real_spawn(account, args, env)

        def provide_input():
            writer.write(
                b'{"type":"control_request","request_id":"init-1","request":{"subtype":"initialize"}}\n'
            )
            writer.write(b'{"type":"user","message":{"role":"user","content":"do work"}}\n')
            started.wait(timeout=3)
            writer.write(b'{"type":"tool_result","tool_use_id":"preserve-me"}\n')
            buffered.set()
            writer.close()

        def switching_started():
            started.set()
            buffered.wait(timeout=3)

        pool.config.switch_started = switching_started
        thread = threading.Thread(target=provide_input)
        thread.start()
        try:
            with mock.patch.object(pool, "_spawn", side_effect=fail_target):
                pool.run([], self.env(), stdin=stdin, stdout=stdout)
        finally:
            thread.join(timeout=3)
            stdin.close()

        events = self.events(stdout)
        self.assertEqual(len([e for e in events if e.get("type") == "rate_limit_event"]), 1)
        self.assertEqual(
            len([e for e in events if e.get("input", {}).get("tool_use_id") == "preserve-me"]),
            1,
        )
        self.assertEqual(
            len(
                [
                    e
                    for e in events
                    if e.get("type") == "system" and e.get("account") == "account-a"
                ]
            ),
            2,
        )

    def test_target_dying_after_input_does_not_replay_and_releases_limit(self):
        started = threading.Event()
        buffered = threading.Event()
        self.write_behavior(
            {"mode": "limit", "after_limit_read": True},
            {"mode": "exit_after_initialize"},
        )
        read_fd, write_fd = os.pipe()
        stdin = os.fdopen(read_fd, "rb", buffering=0)
        writer = os.fdopen(write_fd, "wb", buffering=0)
        stdout = BytesIO()

        def provide_input():
            writer.write(
                b'{"type":"control_request","request_id":"init-1","request":{"subtype":"initialize"}}\n'
            )
            writer.write(b'{"type":"user","message":{"role":"user","content":"do work"}}\n')
            started.wait(timeout=3)
            writer.write(b'{"type":"tool_result","tool_use_id":"survive-init-failure"}\n')
            buffered.set()
            writer.close()

        def switching_started():
            started.set()
            buffered.wait(timeout=3)

        config = self.config()
        config.switch_started = switching_started
        thread = threading.Thread(target=provide_input)
        thread.start()
        try:
            PoolProcess(config).run([], self.env(), stdin=stdin, stdout=stdout)
        finally:
            thread.join(timeout=3)
            stdin.close()

        events = self.events(stdout)
        self.assertGreaterEqual(
            len([e for e in events if e.get("type") == "rate_limit_event"]), 1
        )
        self.assertEqual(
            len(
                [
                    e
                    for e in events
                    if e.get("input", {}).get("tool_use_id") == "survive-init-failure"
                ]
            ),
            0,
        )

    def test_malformed_stream_data_passes_through_byte_for_byte(self):
        self.write_behavior({"mode": "passthrough"}, {"mode": "continue"})
        stdout = BytesIO()

        PoolProcess(self.config()).run(
            [], self.env(), stdin=BytesIO(b"not-json-and-private\n"), stdout=stdout
        )

        self.assertIn(b"not-json-and-private\n", stdout.getvalue())

    def test_input_queue_applies_byte_backpressure(self):
        release = threading.Event()

        class BlockingTarget(BytesIO):
            def write(self, value):
                release.wait(timeout=3)
                return len(value)

        router = _InputRouter(BytesIO(b"x" * 100_000), BlockingTarget(), 4_096)
        router.start()
        deadline = time.time() + 2
        while router.max_observed_pending_bytes < 4_096 and time.time() < deadline:
            time.sleep(0.01)
        try:
            self.assertLessEqual(router.pending_bytes, 4_096)
            self.assertLessEqual(router.max_observed_pending_bytes, 4_096)
        finally:
            router.detach()
            release.set()

    def test_shutdown_terminates_the_child_process_group(self):
        marker = self.root / "child.pid"
        self.write_behavior({"mode": "linger", "pid_marker": str(marker)}, {"mode": "continue"})
        pool = PoolProcess(self.config())
        result = []

        thread = threading.Thread(
            target=lambda: result.append(
                pool.run([], self.env(), stdin=BytesIO(), stdout=BytesIO())
            )
        )
        thread.start()
        deadline = time.time() + 3
        while not marker.exists() and time.time() < deadline:
            time.sleep(0.01)
        self.assertTrue(marker.exists())
        child_pid = int(marker.read_text())

        pool.shutdown()
        thread.join(timeout=3)

        self.assertFalse(thread.is_alive())
        with self.assertRaises(ProcessLookupError):
            os.kill(child_pid, 0)

    def test_closed_downstream_output_still_cleans_up_child(self):
        self.write_behavior({"mode": "linger"}, {"mode": "continue"})
        pool = PoolProcess(self.config())

        class ClosedOutput(BytesIO):
            def write(self, _value):
                raise BrokenPipeError("T3 closed output")

        with self.assertRaises(BrokenPipeError):
            pool.run([], self.env(), stdin=BytesIO(), stdout=ClosedOutput())

        self.assertEqual(pool._children, set())


class SetupTests(unittest.TestCase):
    @staticmethod
    def create_t3_database(path, project_id="pool-project", workspace_root=None):
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(path)) as database:
            with database:
                database.execute(
                    "CREATE TABLE projection_projects "
                    "(project_id TEXT, title TEXT, workspace_root TEXT, deleted_at TEXT)"
                )
                if workspace_root is not None:
                    database.execute(
                        "INSERT INTO projection_projects VALUES (?, ?, ?, NULL)",
                        (project_id, "pool", str(workspace_root)),
                    )

    def install_fixture(self, root):
        t3_home = root / "custom-t3-home"
        settings = t3_home / "userdata" / "settings.json"
        settings.parent.mkdir(parents=True)
        settings.write_text(json.dumps({"theme": "keep"}))
        project_root = root / "accounts" / "pool"
        project_root.mkdir(parents=True)
        self.create_t3_database(
            t3_home / "userdata" / "state.sqlite", workspace_root=project_root
        )
        main_account = root / "credentials" / "main"
        work_account = root / "credentials" / "work"
        main_account.mkdir(parents=True)
        work_account.mkdir(parents=True)
        install_root = root / "custom-install"
        bin_dir = root / "custom-bin"
        config_path = root / "custom-config" / "pool.json"
        fake_t3 = root / "custom-t3"
        fake_t3.write_text("#!/bin/sh\nexit 0\n")
        fake_t3.chmod(0o755)
        fake_claude = root / "custom-claude"
        fake_claude.write_text(
            "#!/bin/sh\n"
            'if [ "$#" -eq 1 ] && [ "$1" = "--version" ]; then\n'
            "  printf 'custom-version\\n'\n"
            "  exit 0\n"
            "fi\n"
            "printf '%s\\n' \"$CLAUDE_CONFIG_DIR\"\n"
            "printf '%s\\n' \"$CLAUDE_POOL_CONFIG\"\n"
            "printf '%s\\n' \"$CLAUDE_POOL_T3_SETTINGS\"\n"
        )
        fake_claude.chmod(0o755)
        args = [
            "--apply",
            "--account",
            f"main={main_account}",
            "--account",
            f"work={work_account}",
            "--t3-home",
            str(t3_home),
            "--project-root",
            str(project_root),
            "--install-root",
            str(install_root),
            "--bin-dir",
            str(bin_dir),
            "--config-path",
            str(config_path),
            "--t3-bin",
            str(fake_t3),
            "--claude-bin",
            str(fake_claude),
        ]
        with redirect_stdout(StringIO()):
            self.assertEqual(setup_main(args), 0)
        return {
            "launcher": bin_dir / "claude-pool",
            "config": config_path,
            "settings": settings,
            "main_account": main_account,
        }

    def test_account_specs_expand_paths_and_reject_duplicates(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with mock.patch.dict(os.environ, {"HOME": str(root)}):
                accounts = parse_account_specs(
                    ["main=~/.claude", f"work={root / '.claude-work'}"]
                )
            self.assertEqual([account.slug for account in accounts], ["main", "work"])
            self.assertEqual(accounts[0].config_dir, root / ".claude")

            with self.assertRaises(ValueError):
                parse_account_specs([f"main={root / 'one'}", f"main={root / 'two'}"])
            with self.assertRaises(ValueError):
                parse_account_specs([f"one={root / 'same'}", f"two={root / 'same'}"])

    def test_apply_rejects_symlink_alias_accounts_before_mutating_settings(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            account = root / "account"
            account.mkdir()
            alias = root / "account-alias"
            alias.symlink_to(account, target_is_directory=True)
            t3_home = root / ".t3"
            settings = t3_home / "userdata" / "settings.json"
            settings.parent.mkdir(parents=True)
            original_settings = b'{"theme":"untouched"}\n'
            settings.write_bytes(original_settings)
            project_root = root / "accounts" / "pool"
            project_root.mkdir(parents=True)
            self.create_t3_database(
                t3_home / "userdata" / "state.sqlite", workspace_root=project_root
            )
            fake_t3 = root / "t3"
            fake_claude = root / "claude"
            install_root = root / "install"
            bin_dir = root / "bin"
            config_path = root / "config" / "pool.json"
            for executable in (fake_t3, fake_claude):
                executable.write_text("#!/bin/sh\nexit 0\n")
                executable.chmod(0o755)

            with self.assertRaisesRegex(
                SystemExit, "duplicate account config directory"
            ):
                setup_main(
                    [
                        "--apply",
                        "--account",
                        f"main={account}",
                        "--account",
                        f"alias={alias}",
                        "--t3-home",
                        str(t3_home),
                        "--project-root",
                        str(project_root),
                        "--install-root",
                        str(install_root),
                        "--bin-dir",
                        str(bin_dir),
                        "--config-path",
                        str(config_path),
                        "--t3-bin",
                        str(fake_t3),
                        "--claude-bin",
                        str(fake_claude),
                    ]
                )

            self.assertEqual(settings.read_bytes(), original_settings)
            self.assertFalse(config_path.exists())
            self.assertFalse((bin_dir / "claude-pool").exists())

    def test_installed_launcher_uses_custom_paths_in_a_clean_process(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self.install_fixture(root)
            clean_home = root / "clean-home"
            clean_home.mkdir()

            completed = subprocess.run(
                [str(fixture["launcher"])],
                input=b"",
                capture_output=True,
                env={"HOME": str(clean_home), "PATH": "/usr/bin:/bin", "LANG": "C"},
                timeout=5,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr.decode())
            self.assertEqual(
                completed.stdout.decode().splitlines(),
                [
                    str(fixture["main_account"]),
                    str(fixture["config"]),
                    str(fixture["settings"]),
                ],
            )

    def test_installed_launcher_version_ignores_invalid_pool_config(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self.install_fixture(root)
            fixture["config"].write_text("not json")

            completed = subprocess.run(
                [str(fixture["launcher"]), "--version"],
                capture_output=True,
                env={"HOME": str(root / "clean-home"), "PATH": "/usr/bin:/bin", "LANG": "C"},
                timeout=5,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr.decode())
            self.assertEqual(completed.stdout, b"custom-version\n")

    def test_account_discovery_uses_t3_instances_without_pool(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            settings = root / "settings.json"
            settings.write_text(
                json.dumps(
                    {
                        "providerInstances": {
                            "claudeAgent": {
                                "driver": "claudeAgent",
                                "config": {"homePath": "~/.claude"},
                            },
                            "claudeAgent_work": {
                                "driver": "claudeAgent",
                                "config": {"homePath": "~/.claude-work"},
                            },
                            "claudeAgent_pool": {
                                "driver": "claudeAgent",
                                "config": {"homePath": "~/.claude"},
                            },
                        }
                    }
                )
            )
            with mock.patch.dict(os.environ, {"HOME": str(root)}):
                accounts = discover_accounts(settings, root)

            self.assertEqual([account.slug for account in accounts], ["main", "work"])
            self.assertEqual(accounts[1].config_dir, root / ".claude-work")

    def test_missing_project_is_added_then_read_from_t3_database(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database_path = root / "state.sqlite"
            project_root = root / "accounts" / "pool"
            self.create_t3_database(database_path)
            calls = []

            def add_project(command, **kwargs):
                calls.append(command)
                with closing(sqlite3.connect(database_path)) as database:
                    with database:
                        database.execute(
                            "INSERT INTO projection_projects VALUES (?, ?, ?, NULL)",
                            ("created-project", "pool", str(project_root)),
                        )
                return subprocess.CompletedProcess(command, 0, "", "")

            project_id = ensure_t3_project(
                database_path,
                project_root,
                t3_bin=Path("/fixture/t3"),
                t3_home=root / ".t3",
                runner=add_project,
            )

            self.assertEqual(project_id, "created-project")
            self.assertEqual(
                calls,
                [
                    [
                        "/fixture/t3",
                        "project",
                        "add",
                        "--base-dir",
                        str(root / ".t3"),
                        "--title",
                        "pool",
                        str(project_root),
                    ]
                ],
            )

    def test_project_lookup_closes_its_sqlite_connection(self):
        class Cursor:
            @staticmethod
            def fetchone():
                return ("pool-project",)

        class Connection:
            def __init__(self):
                self.closed = False

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            @staticmethod
            def execute(*_args):
                return Cursor()

            def close(self):
                self.closed = True

        connection = Connection()
        with tempfile.TemporaryDirectory() as temp:
            database_path = Path(temp) / "state.sqlite"
            database_path.touch()
            with mock.patch("setup_pool.sqlite3.connect", return_value=connection):
                self.assertEqual(
                    find_project_id(database_path, Path("/fixture/pool")),
                    "pool-project",
                )

        self.assertTrue(connection.closed)

    def test_clean_install_is_idempotent_and_checkable_without_netz_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            t3_home = root / ".t3"
            settings = t3_home / "userdata" / "settings.json"
            settings.parent.mkdir(parents=True)
            settings.write_text(json.dumps({"theme": "keep"}))
            project_root = root / "accounts" / "pool"
            project_root.mkdir(parents=True)
            database_path = t3_home / "userdata" / "state.sqlite"
            self.create_t3_database(database_path, workspace_root=project_root)
            main_account = root / ".claude"
            work_account = root / ".claude-work"
            main_account.mkdir()
            work_account.mkdir()
            install_root = root / ".local" / "share" / "claude-pool"
            bin_dir = root / ".local" / "bin"
            config_path = root / ".config" / "claude-pool" / "config.json"
            fake_t3 = root / "t3"
            fake_claude = root / "claude"
            for executable in (fake_t3, fake_claude):
                executable.write_text("#!/bin/sh\nexit 0\n")
                executable.chmod(0o755)
            args = [
                "--apply",
                "--account",
                f"main={main_account}",
                "--account",
                f"work={work_account}",
                "--t3-home",
                str(t3_home),
                "--project-root",
                str(project_root),
                "--install-root",
                str(install_root),
                "--bin-dir",
                str(bin_dir),
                "--config-path",
                str(config_path),
                "--t3-bin",
                str(fake_t3),
                "--claude-bin",
                str(fake_claude),
            ]

            with redirect_stdout(StringIO()):
                self.assertEqual(setup_main(args), 0)
                backups_after_first = list(settings.parent.glob("settings.json.claude-pool-backup-*"))
                self.assertEqual(setup_main(args), 0)
                backups_after_second = list(settings.parent.glob("settings.json.claude-pool-backup-*"))

            self.assertEqual(len(backups_after_first), 1)
            self.assertEqual(backups_after_second, backups_after_first)
            self.assertTrue(os.access(bin_dir / "claude-pool", os.X_OK))
            self.assertTrue((install_root / "claude_pool.py").is_file())
            self.assertEqual(config_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(
                json.loads(config_path.read_text()),
                {
                    "accounts": [
                        {"slug": "main", "config_dir": str(main_account)},
                        {"slug": "work", "config_dir": str(work_account)},
                    ]
                },
            )
            updated = json.loads(settings.read_text())
            self.assertEqual(updated["theme"], "keep")
            self.assertEqual(
                updated["providerInstances"]["claudeAgent_pool"]["config"]["binaryPath"],
                str(bin_dir / "claude-pool"),
            )

            with redirect_stdout(StringIO()):
                self.assertEqual(setup_main(["--check", *args[1:]]), 0)

    def test_settings_update_is_backed_up_preserves_unrelated_data_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            settings_path = root / "settings.json"
            original = {
                "theme": "existing",
                "providerInstances": {
                    "claudeAgent_work1": {
                        "driver": "claudeAgent",
                        "displayName": "Work 1",
                        "enabled": True,
                        "config": {"homePath": "~/.claude-work1"},
                    }
                },
                "projectSettingsOverrides": {
                    "existing-project": {"keep": "this"},
                    "pool-project": {"editor": "preserve"},
                },
            }
            settings_path.write_text(json.dumps(original))
            wrapper = root / "bin" / "claude-pool"

            changed, backup = update_settings_file(
                settings_path,
                project_id="pool-project",
                wrapper_path=wrapper,
                home_path="~/.claude",
                model="claude-opus-5-5",
            )
            after_first = json.loads(settings_path.read_text())
            changed_again, backup_again = update_settings_file(
                settings_path,
                project_id="pool-project",
                wrapper_path=wrapper,
                home_path="~/.claude",
                model="claude-opus-5-5",
            )

            self.assertTrue(changed)
            self.assertIsNotNone(backup)
            self.assertEqual(json.loads(backup.read_text()), original)
            self.assertFalse(changed_again)
            self.assertIsNone(backup_again)
            self.assertEqual(after_first, json.loads(settings_path.read_text()))
            self.assertEqual(after_first["theme"], "existing")
            self.assertIn("claudeAgent_work1", after_first["providerInstances"])
            self.assertEqual(
                after_first["providerInstances"]["claudeAgent_pool"]["config"],
                {"homePath": "~/.claude", "binaryPath": str(wrapper)},
            )
            self.assertEqual(
                after_first["projectSettingsOverrides"]["pool-project"]["editor"], "preserve"
            )
            self.assertEqual(
                after_first["projectSettingsOverrides"]["pool-project"]["defaultModelSelection"],
                {"instanceId": "claudeAgent_pool", "model": "claude-opus-5-5"},
            )


if __name__ == "__main__":
    unittest.main()
