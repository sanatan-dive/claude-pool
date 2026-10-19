#!/usr/bin/env python3
import json
import os
import signal
import sys
import time
from pathlib import Path


def emit(value):
    if isinstance(value, bytes):
        sys.stdout.buffer.write(value)
    else:
        sys.stdout.buffer.write(json.dumps(value, separators=(",", ":")).encode() + b"\n")
    sys.stdout.buffer.flush()


def project_slug():
    return os.path.abspath(os.getcwd()).replace("/", "-").replace(".", "-")


def session_from_args():
    args = sys.argv[1:]
    for index, arg in enumerate(args):
        if arg.startswith("--resume=") or arg.startswith("--session-id="):
            return arg.split("=", 1)[1]
        if arg in ("--resume", "--session-id") and index + 1 < len(args):
            return args[index + 1]
    return os.environ.get("FAKE_SESSION_ID", "fixture-session")


def main():
    config_dir = Path(os.environ["CLAUDE_CONFIG_DIR"])
    account = config_dir.name
    behavior = json.loads(Path(os.environ["FAKE_BEHAVIOR_FILE"]).read_text()).get(account, {})
    if behavior.get("started_marker"):
        Path(behavior["started_marker"]).touch()
    delay = float(behavior.get("startup_delay", 0))
    if delay:
        time.sleep(delay)

    session_id = session_from_args()
    history = config_dir / "projects" / project_slug() / (session_id + ".jsonl")
    history.parent.mkdir(parents=True, exist_ok=True)

    if behavior.get("write_history_on_term"):
        def on_term(_signum, _frame):
            with history.open("a") as output:
                output.write("final-on-stop\n")
                overlap_marker = behavior.get("overlap_marker")
                if overlap_marker and Path(overlap_marker).exists():
                    output.write("overlap-detected\n")
                output.flush()
                os.fsync(output.fileno())
            raise SystemExit(0)

        signal.signal(signal.SIGTERM, on_term)

    history_text = history.read_text() if history.exists() else ""
    history_seen = "remembered-before-limit" in history_text
    final_history_seen = "final-on-stop" in history_text
    if behavior.get("mode") == "limit_before_init":
        history.write_text("remembered-before-limit\n")
        emit(
            {
                "type": "rate_limit_event",
                "rate_limit_info": {
                    "status": "rejected",
                    "resetsAt": 4_000_000_000,
                    "rateLimitType": "five_hour",
                },
                "session_id": session_id,
            }
        )
        return 0
    emit({"type": "system", "subtype": "init", "session_id": session_id, "account": account})

    if behavior.get("mode") == "linger":
        marker = behavior.get("pid_marker")
        if marker:
            Path(marker).write_text(str(os.getpid()))
        time.sleep(30)
        return 0

    if behavior.get("mode") == "limit_no_read":
        history.write_text("remembered-before-limit\n")
        time.sleep(0.05)
        emit(
            {
                "type": "rate_limit_event",
                "rate_limit_info": {
                    "status": "rejected",
                    "resetsAt": 4_000_000_000,
                    "rateLimitType": "five_hour",
                },
                "session_id": session_id,
            }
        )
        time.sleep(10)
        return 0

    limited = False
    controls = []
    for raw in sys.stdin.buffer:
        if behavior.get("mode") == "passthrough":
            emit(raw)
            continue
        try:
            event = json.loads(raw)
        except ValueError:
            emit(raw)
            continue

        if event.get("type") == "control_request":
            controls.append(event.get("request", {}).get("subtype"))
            emit(
                {
                    "type": "control_response",
                    "request_id": event.get("request_id"),
                    "account": account,
                    "session_id": session_id,
                }
            )
            if behavior.get("mode") == "exit_after_initialize":
                return 0
            continue

        if behavior.get("mode") == "limit" and not limited and event.get("type") == "user":
            limited = True
            with history.open("a") as output:
                output.write("remembered-before-limit\n")
                output.flush()
                os.fsync(output.fileno())
            emit(
                {
                    "type": "rate_limit_event",
                    "rate_limit_info": {
                        "status": "rejected",
                        "resetsAt": 4_000_000_000,
                        "rateLimitType": "five_hour",
                    },
                    "session_id": session_id,
                }
            )
            if not behavior.get("after_limit_read"):
                time.sleep(2)
                return 0
            continue

        if event.get("type") == "user" and "Switched accounts" in str(event):
            emit(
                {
                    "type": "assistant",
                    "account": account,
                    "continued": True,
                    "history_seen": history_seen,
                    "final_history_seen": final_history_seen,
                    "controls": controls,
                    "session_id": session_id,
                }
            )
            continue

        if behavior.get("discard"):
            continue

        emit(
            {
                "type": "echo",
                "account": account,
                "input": event,
                "session_id": session_id,
                "history_seen": history_seen,
            }
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
