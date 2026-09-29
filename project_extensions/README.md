# 受控项目扩展

这里存放尚未达到框架核心晋级条件的项目算子。扩展源码目录只允许 Python 文件，以便生成内容寻址的受控 bundle；薄声明放在源码目录旁，不进入源码闭包。

Python 项目算子的公开构建路径为：

```powershell
python -m research_pipeline operator validate --spec project_extensions/<name>.operator.yaml --source project_extensions/<name> --format json
python -m research_pipeline operator build --spec project_extensions/<name>.operator.yaml --source project_extensions/<name> --output <仓库外 bundle 根> --format json
```

命令只读取显式声明和显式源码目录，不扫描当前目录，也不自动安装依赖。生成的内容寻址目录再通过 `--extension-bundle` 显式交给 package 准入和 Runtime。

项目 Verifier 使用独立 bundle，不复用 Operator bundle。Verifier 单独声明 verifier ID、版本、源码闭包、依赖锁，以及允许读取的 Result schema 和支持工件类型；项目专属 Metric 定义也随该 bundle 进入当前 Package 组合并冻结到 Result，不进入公共 Metric discovery。Result 未冻结对应身份、授权输入或 bundle 版本不一致时，verify 直接失败。需要支持列数多于有效观察行数的统计矩阵时，Verifier 使用 `project-verifier-output-v2` 从正式 Result 表复算矩阵行数、非零有效行数、列数和秩，公共门禁不采信 Operator 单方面声明。

项目特有的图、固定日期、参数网格、参考组合和验收语义留在 ResearchPackage；项目算法由
Python Worker bundle 实现。项目算子身份、Artifact 类型和策略只进入当前 Package 的组合
registry，不进入公共 operator discovery。Worker 只接收当前节点的 typed inputs、参数、
固定时钟、种子和资源预算，不读取完整图、兄弟节点参数或未绑定 admitted plans。
项目算法即使被多个声明使用，也不会自动成为 core 能力；通用计算进入 core 要经过公共晋级审核。

薄声明的 `resource_profile` 必须显式包含 `memory_bytes`、`cpu_slots`、
`temp_bytes`、`process_slots` 和 `wall_seconds`。`process_slots` 覆盖 Worker 及其
受支持的子进程树；`wall_seconds` 覆盖取得资源后的整个节点 attempt，逐分区和 causal
键批只继承剩余时间。资源画像属于各自扩展声明，公共 Runtime 不按项目或 Operator 名称覆盖数值。

项目专属中间工件不需要用空壳公共算子占位。声明可在顶层用
`project_artifact_types` 列出当前算子端口实际使用的项目 Artifact 类型；未在公共
registry 登记、也未显式声明的类型仍会拒绝，声明但未被当前算子使用的类型同样拒绝。
同一项目的组合准入只允许消费本批项目算子实际产出的项目类型，不能借此注册全局 schema。

静态validate/build无需安装项目依赖；实际Worker启动时会在加载扩展入口前核对dependency_lock与已安装发行包版本。缺包返回`project_dependency_missing`，版本不符返回`project_dependency_version_mismatch`。需在运行环境安装声明的精确版本或显式修改声明后重建bundle，不能把静态构建通过等同于运行环境满足锁。

需要项目源码解释复杂 JSON 参数时，可在薄声明登记 `parameter_preflight`：

```yaml
parameter_preflight: {module: operator, function: preflight}
```

对应同步函数签名为 `preflight(context)`，只能检查参数和固定运行事实，成功返回 `None`。
package lint/admit 会在数据库打开和正式运行前用真实冻结 context 调用它；项目代码应按
`collections.abc.Mapping` 和一般序列合同读取参数，不应依赖可变 `dict`/`list`。预检入口与
正式入口一起进入源码闭包和 bundle 身份。

扩展代码运行于可信本地协作环境，框架不是安全沙箱。子进程、超时和后代进程清理用于故障隔离；正式输入、输出 staging、端口、相对路径及提交内容仍由框架核验。扩展及第三方库可以正常使用 `tempfile`、`os.replace` 和动态导入，可能产生 staging 外本地副作用，代码作者负责其行为；框架不拦截任意 Python 的文件操作或数据库访问。研究数据库只读仍是必须遵守的使用合同，不能把能调用数据库当成写库授权。

`analyst_revision_event_study/` 的诊断只接受本次已验证的 events 批输入，保留行业/市值和同行可见性检查，并引用实际输入工件。合成事件及预期值只存在于 tests/fixtures；缺输入或必要字段时拒绝，不生成替代数据。当前研究包没有这项诊断所需的真实来源，因此移除了原无输入诊断节点和对应Result表，核心事件研究链仍保留。

活动声明和 bundle 只接受当前 project-operator v2 合同。v1 声明、bundle、plan 和 checkpoint 不转换、不兼容；需要继续研究时必须用当前声明重新 build、lint、admit 并新建 run。历史 Result 和验收记录只作只读证据，不是旧代码入口。

项目算子默认 `reuse_scope: same_run`。只有正式输出完全由 typed 输入、参数、fixed clock 和 seed 决定，且不依赖 project/run/node/attempt 等易变 ID 时，薄声明才可显式写 `reuse_scope: cross_run`。Runtime 还会同时检查确定性合同和 `artifact_write_scope: output_only`；没有该字段的既有声明保持同 run 恢复语义。

`minute_event_response/` 提供“分钟事件到后续窗口反应”的通用候选实现，支持参数化字段、Top-K/分位数、开头窗口排除、最小事件间隔、后续窗口和聚合函数。它目前仍是项目扩展：首个研究消费者不能单独证明通用性；只有第二个独立项目给出复用证据、独立 oracle 和攻击测试后，才评估是否晋级核心。

该实现只读取决策时点前已经完成且质检通过的 bar。后续窗口也是在研究决策时点已经可见的历史窗口，不可把这个算子直接当成盘中实时信号。

`dai_zhu_er_jiu_monthly_labels/` 是待著而救项目的完整分析扩展：消费分钟事件观察和显式
日线/证券主数据/ST 输入，构造月末股票池与主/敏感性标签，逐月完成缩尾、标准化、IC、
十分组和 HAC 指标，并分别提交主分析、敏感性分析和资格审计项目 Artifact。项目专属
Verifier 位于 `dai_zhu_er_jiu_factor_verifier/`，从 Result 成员明细独立重算统计量和时间
顺序。两者不会注册到框架公共 discovery。

该项目的可重复构建入口为：

```powershell
$env:PYTHONPATH='research_pipeline/src'
python research_pipeline/project_extensions/build_dai_zhu_er_jiu_bundles.py `
  --output <仓库外新目录>
```

构建器读取分钟因子、分析和 validity 三个 Operator 声明及 Verifier `definition.yaml`，
一次生成彼此独立的四个 bundle。validity Worker 只从 Supervisor 注入的四请求准入事实、
主分析表和标签审计生成 Result 所需的唯一 `research.validity-facts.v1`；Verifier 的五个
项目 Metric 定义仍由自身声明生成，不在构建脚本里另写一份。

`e004_smallcap_grid_robustness/` 封装 E004 确定性小盘股网格研究的 panel、目标持仓、指数
观察、现金账户仿真、稳健统计和 validity。八份 `.framework.yaml` 文件虽然保留历史文件名，
内容均为当前 `project-operator-declaration-v2`；它们不再引用已退役的框架原语。独立
Verifier 从 Result 的候选日收益重算八项项目指标。完整包选用全部八项，
smoke Package 只将其中五项纳入正式 MetricContract。可重复构建入口为：

```powershell
$env:PYTHONPATH='research_pipeline/src'
python research_pipeline/project_extensions/build_e004_bundles.py `
  --output <仓库外新目录>
```

候选目标 Worker 同时支持确定性 `top_n` 和项目侧
`random_without_replacement`。随机模式必须显式登记 `deterministic_selected`、`typical`
和 `stress` 三类代表组合、选择来源与证据、重复编号轴及候选池倍数；每个调仓日只在已见
横截面按市值稳定排序后的前 `K=N×multiplier` 只中无放回抽取 `N` 只。两个确定性 E004
ResearchPackage 继续只使用 `top_n`。独立的 `e004_smallcap_random_stability_smoke` 已从正式
发现/验证工件冻结三个代表组合、20 个 draw 和根种子，只报告路径分布，不在评价窗口重新
选优。每日换手率使用当日成交金额除以调仓前开盘账户权益，原始成交金额仍留在仿真工件；
它使用独立统计 Operator 和 Verifier，构建入口为：

确定性全量包按排序后的 `combination_id` 每 64 组形成一个稳定分区。目标持仓和诊断逐批
写入对应 Parquet，现金账户仿真逐分区读取目标、逐组合推进账户，再把 daily 与 fills 写回
同编号分区；稳健统计逐分区消费 daily，只在内存保留检验矩阵和当前 candidates 分区。仿真
会复用按交易日建立的行情索引和公司行动日期索引，不再为每个组合重复扫描整表。内存中不再
同时保留 5,040 组目标、全部仿真结果或全量 candidates。分区只改变工件布局，不改变参数
组合、调仓日、排名、权重、费用、公司行动、随机种子或统计口径；metadata 显式记录布局版本、
每分区组合上限和分区数量，下游发现缺分区、重复分区或组合越界时拒绝运行。

```powershell
$env:PYTHONPATH='research_pipeline/src'
python research_pipeline/project_extensions/build_e004_random_bundles.py `
  --output <仓库外新目录>
```

## 分批输入、输出和状态

分钟输入保留 `ProjectPartitionInput.iter_batches()` 与普通跨月 state。非分钟 Parquet
输入支持同一已验证工件下的多个文件，按稳定文件顺序调用 `iter_batches(columns=...,
batch_size=...)`，必须完整消费；未允许的列、过大批次或漏读会失败。
`output_root` 保留 Path 行为，并提供 `write_batches(port=..., artifact_type=...,
relative_path=..., schema=..., batches=...)` 与 `write_state(relative_path=...,
schema_hash=..., chunks=...)`；每批或块最多 32 MiB，返回原 commit 字典。
表格按固定 8,192 行 row group 输出，改变输入批大小不改变文件身份。worker 输出和状态
只返回 staging 路径描述，父进程流式提交；普通 state 继续沿现有分区 checkpoint 恢复。
需要在同一输出端口封存多张表时，将文件写入 `output_root` 下的同一子目录，调用
`output_root.commit_directory(port=..., artifact_type=..., relative_path=...,
files=(...))` 返回一份目录提交。`files` 是该目录内全部文件的相对路径；Worker 和
Supervisor 都核对文件闭合与内容，正式 Result 仍从一个端口按表前缀选择。
目录默认以端口名作为 ExternalArtifact 前缀；若公共工件合同要求根级文件，可显式传入
`publish_at_artifact_root=True`。该选项只改变已核对目录在 ExternalArtifact 中的落点，
不会放宽输出目录、文件闭合或内容身份校验；项目不得提交根级 `manifest.json` 或
`COMMITTED` 及其大小写变体，这两个文件由 ExternalArtifact 核心生成。
异构列式数据端口和显式绑定的原始 1 分钟扫描端口都可通过
`inputs[...].admission(request_id)` 读取 Supervisor 从本次正式准入计划投影的 dataset、字段、
时间范围、原始 `as_of`、按固定时钟时区解释的排他 `as_of_cutoff`、来源 revision、
publication、日频可见规则和 claim ceiling。分钟扫描端口还会核对
已验证 `data.minute-bars.1m.v1` 中的 request、plan hash、source revision 和来源快照，只投影
准入元数据，不向该输入开放分钟数据文件。只有节点参数显式绑定的 request 可见，Worker 必须
完整读取并返回 metadata-only 消费轨迹；列式数据仍同时核对实际消费的列和行数。扩展不得自填
准入事实，也不能用元数据输入绕过正式分钟分区读取合同。

## 正式 Feature / Label

在 operator 声明 parameters 中加入必填 `causal_plan`（`value_type: json`），每个算子
处理一种正式因果输出。package 节点参数使用以下固定结构，没有表达式或回调：

```yaml
causal_plan:
  kind: feature
  output_port: features
  key_columns: [event_id]
  state_scope: independent
  sources:
    - port: data
      request_id: prices
      columns: [event_time, available_time, close]
      observation_column: event_time
      available_column: available_time
  work_items:
    - key_rows: [[event-a], [event-b]]
      decision_time: '2024-01-03T10:00:00+08:00'
      window_start: '2024-01-02T09:00:00+08:00'
      window_end: '2024-01-03T10:00:00+08:00'
      source_partitions: {data: ['2024-01']}
```

键使用对应正式 Feature/Label 表的完整键。source port 必须直接绑定声明的数据请求，
源列必须在 QueryIR 投影内；观察和可见时间角色必须由 Catalog 准入证明。逻辑月份按
已准入源时区解释，实际物理分区身份由核心绑定。Label 用 `kind: label`，窗口在决策之后。
需要逐决策 revision/interval 选择或日线开盘/收盘可见性编译的输入尚不接受。

核心按决策和窗口稳定调度，同窗最多 8,192 键一批，最低内存预算 256 MiB；不会随内存
预算改变 opaque state 的调用次数。扩展通过带 `port` 的受限输入读取，只能请求冻结列、
月份和窗口；默认读仅交付窗口内数据，显式越界请求立即失败。扩展忽略已经交付的部分行
仍保守依赖整批。扩展只输出当前键批的键和值，不得填写任何核心时间列。

`state_scope: independent` 每个键批独立；`carry` 在下一键批前由核心检查继承来源和时间
窗口，超界失败，不静默清空。输出事实与 state 同时继承本次交付和已有来源的并集。
这是受支持 ABI 的因果约束，不是恶意 Python 沙箱；不能用绕开 ABI 的文件读取声称合规。
