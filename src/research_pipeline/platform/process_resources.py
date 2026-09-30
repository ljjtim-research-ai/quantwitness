"""执行与验证共用的进程树测量、临时空间计量和进程清理。"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
from typing import Mapping

import psutil


def _project_process_usage(
    worker_pid: int | None, observed_descendants: dict[int, float],
) -> tuple[int, int]:
    """租约覆盖 Supervisor、当前 Worker 和已经观测到的存活后代。"""
    supervisor = psutil.Process(os.getpid())
    processes = {os.getpid(): supervisor}
    if worker_pid is not None:
        try:
            worker = psutil.Process(worker_pid)
            processes[worker_pid] = worker
            for child in worker.children(recursive=True):
                try:
                    observed_descendants[child.pid] = child.create_time()
                    processes[child.pid] = child
                except psutil.NoSuchProcess:
                    continue
        except psutil.NoSuchProcess:
            pass
    for pid, started_at in observed_descendants.items():
        if pid in processes:
            continue
        try:
            child = psutil.Process(pid)
            if abs(child.create_time() - started_at) < 0.01:
                processes[pid] = child
        except psutil.NoSuchProcess:
            continue
    rss = count = 0
    for process in processes.values():
        try:
            rss += process.memory_info().rss
            count += 1
        except psutil.NoSuchProcess:
            continue
    return rss, count



def _measure_attempt_tree_bytes(root: Path) -> int:
    """统计临时空间；Worker 正常删除临时路径时跳过已经消失的条目。"""

    total = 0

    def directory_error(error: OSError) -> None:
        if not isinstance(error, FileNotFoundError):
            raise error

    for directory, _, files in os.walk(root, onerror=directory_error):
        for name in files:
            try:
                total += (Path(directory) / name).stat().st_size
            except FileNotFoundError:
                continue
    return total


def _terminate_process_tree(
    process: subprocess.Popen[bytes],
    *,
    observed_descendants: Mapping[int, float] | None = None,
) -> str:
    cleanup_status = "complete"
    try:
        descendants: dict[int, psutil.Process] = {}
        for pid, started_at in (observed_descendants or {}).items():
            try:
                item = psutil.Process(pid)
                if abs(item.create_time() - started_at) < 0.01:
                    descendants[pid] = item
            except psutil.Error:
                continue
        try:
            parent = psutil.Process(process.pid)
            for child in parent.children(recursive=True):
                descendants[child.pid] = child
        except psutil.NoSuchProcess:
            parent = None
        targets = list(descendants.values())
        if parent is not None:
            targets.append(parent)
        for item in targets:
            try:
                item.terminate()
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs(targets, timeout=1.0) if targets else ([], [])
        for item in alive:
            try:
                item.kill()
            except psutil.NoSuchProcess:
                pass
        if alive:
            _, survivors = psutil.wait_procs(alive, timeout=2.0)
            if survivors:
                cleanup_status = "direct_process_only"
    except (psutil.Error, OSError, RuntimeError):
        cleanup_status = "direct_process_only"
        if process.poll() is None:
            process.kill()
    try:
        process.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        process.kill()
        cleanup_status = "direct_process_only"
    return cleanup_status


