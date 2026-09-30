"""项目算子参数 ABI 预检短进程。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Mapping

from research_pipeline.platform import canonical_json
from research_pipeline.platform.redaction import redact_text

from .project_bundle import ProjectOperatorContext, verify_project_operator_bundle
from .project_loading import load_project_module, validate_installed_dependencies


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", required=True)
    args = parser.parse_args(argv)
    try:
        raw_context = json.loads(sys.stdin.read())
        if not isinstance(raw_context, Mapping):
            raise ValueError("project_parameter_preflight_context_invalid")
        bundle = Path(args.bundle).resolve(strict=True)
        manifest = verify_project_operator_bundle(bundle)
        if manifest.parameter_preflight_module is None:
            raise ValueError("project_parameter_preflight_missing")
        validate_installed_dependencies(manifest.dependency_lock)
        context = ProjectOperatorContext.from_dict(raw_context)
        if context.project_id != manifest.project_id:
            raise ValueError("project_parameter_preflight_project_mismatch")
        sys.dont_write_bytecode = True
        source_root = bundle / "sources"
        sys.path.insert(0, str(source_root))
        module = load_project_module(
            source_root=source_root,
            module_name=manifest.parameter_preflight_module,
            synthetic_root=f"_research_preflight_{manifest.bundle_hash[:16]}",
        )
        entry = getattr(module, manifest.parameter_preflight_function, None)
        if not callable(entry):
            raise ValueError("project_parameter_preflight_missing")
        result = entry(context)
        if result is not None:
            raise ValueError("project_parameter_preflight_return_invalid")
        sys.stdout.write(canonical_json({"status": "pass"}))
        return 0
    except BaseException as exc:
        message = redact_text(" ".join(str(exc).split())) or type(exc).__name__
        if len(message) > 500:
            message = f"{message[:499]}…"
        error_code = getattr(exc, "error_code", None)
        if not isinstance(error_code, str) or not error_code:
            error_code = (
                str(exc)
                if isinstance(exc, ValueError) and str(exc)
                else type(exc).__name__
            )
        sys.stdout.write(canonical_json({
            "status": "fail",
            "error_code": error_code,
            "exception_type": type(exc).__name__,
            "message": message,
        }))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
