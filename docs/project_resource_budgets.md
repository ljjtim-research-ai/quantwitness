# 项目执行与独立复核的资源预算

项目 Worker 的节点租约覆盖当前 Supervisor、当前 Worker 及其后代，RSS 与进程槽使用同一范围。当前正式调度逐节点执行，每次租约只计一次 Supervisor，不按 CPU 槽或进程槽倍乘父进程开销。其他运行的进程不进入当前 Worker 树。Worker 正常退出、测量异常或用户中断均进入进程清理；正常退出后仍检查内存、临时空间、进程槽和时间。节点外层采样覆盖 COMMITTED、目录发布与内容复验，成功事件写入前按节点预算与租约两者的较小上限核对峰值；超额或测量不可用进入失败状态。

`ResourceBudget` 与进程测量、清理工具由 platform 提供，Runtime 和独立验证共用；原 Runtime 的预算导入入口保持可用。独立项目 Verifier 使用 `ResourceBudget` 的内存、CPU、临时磁盘和墙钟额度，进程槽默认 2，包含父进程和 Worker。`verify_result` 接受 `project_verifier_budget` 和 `project_verifier_process_slots`；未传预算时继承 `financial_oracle_budget` 的内存与磁盘值，默认 1 GiB 内存、8 GiB 临时空间、1 CPU、300 秒。`execute_project_verifier` 直接接收 `budget`、`process_slots` 和 `scratch_root`。

准备阶段先汇总授权 Parquet 和支持工件的复制体积，超过临时配额或实际磁盘剩余空间即拒绝。复制采用 1 MiB 块，块间检查内存和时间，文件完成后核对实际空间。执行阶段监督父进程与当前 Worker 树的 RSS、进程数、临时空间和时间，Worker 退出后的末端检查仍包含已经观测到的存活后代；清理等待后代退出，未完成清理不得成功。资源测量不可用、复制失败、Worker 失败、超时或资源超额均抛出稳定错误码，成功结果发布不会继续。

| 失败 | 错误码 |
| --- | --- |
| 内存超额 | project_verifier_memory_exceeded |
| 临时空间超额 | project_verifier_temp_exceeded |
| 复制前实际磁盘不足 | project_verifier_disk_space_exceeded |
| 复制失败 | project_verifier_copy_failed |
| 进程槽超额 | project_verifier_process_slots_exceeded |
| 超时 | project_verifier_timeout |
| 测量不可用 | project_verifier_measurement_unavailable |
| Worker 非零退出 | project_verifier_worker_failed |
| 清理不完整 | project_verifier_cleanup_failed |

E004 Verifier 两遍投影扫描候选 Parquet，每批最多 8,192 行，只保留发现区与评价区数值矩阵、重复键位图和选中候选序列。HAC 沿用各候选在封存表中的原始行序，发现区矩阵按日期排序；BY、DSR、PBO、SPA 的公式与固定随机种子保持不变。待著而救 Verifier 对成员表进行一次分批投影扫描，在隔离输入目录内按日期暂存投影列，再逐截面重算并删除已读取的暂存文件。混合日期行组与乱序分片均不重复扫描源表，截面内沿用原始行序，成员变化只保留相邻日期映射；标签审计按批读取所需两列。项目算法留在项目源码中。

## 已知边界

资源治理采用进程采样，项目 Worker 与 Verifier 的监督循环间隔为 20 毫秒，节点外层采样默认间隔为 50 毫秒，不是操作系统硬限额。未被采样捕获的极短峰值无法作为完整峰值证据。Verifier 输入仍复制至隔离临时目录，必须预留完整授权文件空间；列投影降低计算内存而不减少原始复制量。待著而救还需为成员投影列的日期分片预留临时空间，该空间与输入复制共同计入 Verifier 临时预算。

源码变更产生新的 bundle 身份；既有 Result 使用封存的原始 bundle，不改写历史研究或历史 VerificationResult。
