"""项目源码模块加载与依赖复验的共享实现。"""

from __future__ import annotations

import importlib.metadata
import importlib.util
from importlib.machinery import ModuleSpec
from pathlib import Path
import sys
from types import ModuleType
from typing import Mapping


def load_project_module(
    *,
    source_root: Path,
    module_name: str,
    synthetic_root: str,
) -> ModuleType:
    """在唯一模块命名空间中按 Python 包语义加载项目模块。"""

    root_package = ModuleType(synthetic_root)
    root_package.__package__ = synthetic_root
    root_package.__path__ = [str(source_root)]
    root_package.__spec__ = ModuleSpec(synthetic_root, loader=None, is_package=True)
    sys.modules[synthetic_root] = root_package

    module_parts = module_name.split(".")
    parent_name = synthetic_root
    for depth in range(1, len(module_parts)):
        package_name = ".".join((synthetic_root, *module_parts[:depth]))
        package_path = source_root.joinpath(*module_parts[:depth])
        init_path = package_path / "__init__.py"
        if init_path.is_file():
            package_spec = importlib.util.spec_from_file_location(
                package_name,
                init_path,
                submodule_search_locations=[str(package_path)],
            )
            if package_spec is None or package_spec.loader is None:
                raise ValueError("project_entry_module_invalid")
            package = importlib.util.module_from_spec(package_spec)
            sys.modules[package_name] = package
            package_spec.loader.exec_module(package)
        else:
            package = ModuleType(package_name)
            package.__package__ = package_name
            package.__path__ = [str(package_path)]
            package.__spec__ = ModuleSpec(package_name, loader=None, is_package=True)
            sys.modules[package_name] = package
        setattr(sys.modules[parent_name], module_parts[depth - 1], package)
        parent_name = package_name

    module_path = source_root / Path(*module_parts).with_suffix(".py")
    synthetic_module_name = ".".join((synthetic_root, *module_parts))
    module_spec = importlib.util.spec_from_file_location(
        synthetic_module_name,
        module_path,
    )
    if module_spec is None or module_spec.loader is None:
        raise ValueError("project_entry_module_invalid")
    module = importlib.util.module_from_spec(module_spec)
    sys.modules[synthetic_module_name] = module
    module_spec.loader.exec_module(module)
    setattr(sys.modules[parent_name], module_parts[-1], module)
    return module


def validate_installed_dependencies(dependency_lock: Mapping[str, str]) -> None:
    """静态构建不依赖本机安装；项目短进程执行前核对声明版本。"""

    for import_name, expected_version in dependency_lock.items():
        try:
            actual_versions = (importlib.metadata.version(import_name),)
        except importlib.metadata.PackageNotFoundError:
            distributions = importlib.metadata.packages_distributions().get(import_name, ())
            if not distributions:
                raise ValueError("project_dependency_missing") from None
            try:
                actual_versions = tuple(
                    importlib.metadata.version(name) for name in distributions
                )
            except importlib.metadata.PackageNotFoundError:
                raise ValueError("project_dependency_missing") from None
        if any(version != expected_version for version in actual_versions):
            raise ValueError("project_dependency_version_mismatch")


__all__ = ["load_project_module", "validate_installed_dependencies"]
