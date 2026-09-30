"""统一 CLI 成功、失败与文本输出合同。"""

from __future__ import annotations

from collections.abc import Callable
import json
import sys
from typing import Any

from research_pipeline.platform import error_code_for_exception

from .command_suggestion import command_suggestion


CLI_RESULT_VERSION = "research-cli-result-v1"


def write_machine_json(payload: object) -> None:
    """把机器 JSON 明确写成 UTF-8 字节，不依赖终端默认代码页。"""

    text = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
    buffer = getattr(sys.stdout, "buffer", None)
    if buffer is not None:
        buffer.write(text.encode("utf-8"))
        buffer.flush()
        return
    sys.stdout.write(text)
    sys.stdout.flush()


def execute_guarded(args: Any, handler: Callable[[Any], dict[str, object]]) -> int:
    try:
        return emit(args, status="pass", data=handler(args), code=0)
    except Exception as exc:
        failure_payload = getattr(exc, "failure_payload", None)
        data = dict(failure_payload) if isinstance(failure_payload, dict) else {}
        run_root = getattr(args, "run_root", None)
        runtime_failure = _runtime_failure_details(exc, run_root)
        if runtime_failure is not None:
            data.update(runtime_failure)
            if run_root is not None:
                data["run_root"] = str(run_root)
                data.update(command_suggestion(
                    "python",
                    "-m",
                    "research_pipeline",
                    "inspect",
                    "--run-root",
                    str(run_root),
                    "--json",
                ))
        return emit(
            args,
            status="fail",
            error_code=error_code_for_exception(exc),
            message=str(exc),
            data=data or None,
            code=1,
        )


def _runtime_failure_details(
    error: BaseException,
    run_root: object,
) -> dict[str, object] | None:
    if run_root is None:
        return None
    try:
        from research_pipeline.runtime.errors import RuntimeWorkerError
        from research_pipeline.runtime.store import EventStore

        if not isinstance(error, RuntimeWorkerError):
            return None
        events = EventStore(run_root).read_events()
    except Exception:
        return None
    diagnostic = next(
        (event for event in reversed(events) if event.kind == "diagnostic"),
        None,
    )
    if diagnostic is None or diagnostic.node_id is None:
        return None
    root_error = {
        key: diagnostic.payload.get(key)
        for key in ("error_code", "exception_type", "message")
    }
    if any(not isinstance(value, str) or not value for value in root_error.values()):
        return None
    return {
        "failed_node": diagnostic.node_id,
        "root_error": root_error,
    }


def emit(
    args: Any,
    *,
    status: str,
    data: object,
    code: int,
    error_code: str | None = None,
    message: str | None = None,
) -> int:
    payload = {
        "contract_version": CLI_RESULT_VERSION,
        "status": status,
        "error_code": error_code,
        "message": message,
        "data": data,
    }
    if getattr(args, "json", False):
        write_machine_json(payload)
    else:
        print(f"status: {status}")
        if error_code:
            print(f"error_code: {error_code}")
        if message:
            print(f"message: {message}")
        if data is not None:
            print(json.dumps(data, ensure_ascii=False, sort_keys=True))
    return code


__all__ = [
    "CLI_RESULT_VERSION",
    "emit",
    "execute_guarded",
    "write_machine_json",
]
