# Walk-forward 模型正式主链

本主链把日频 Feature/Label 工件变成可恢复、可审计的样本外预测。核心不认识期货、ETF、股票、指数或研究名称；资产差异必须在上游可执行收益链中解决。

## 正式阶段

```text
Feature + Label
  → split-manifest
  → preprocess-fit
  → fit
  → predict
  → fold-metrics
  → selection
  → locked-holdout
```

- `split-manifest` 只支持时间有序的 expanding Walk-forward。正式日频 Label writer 按 `label_end_time + horizon_sessions` 形成同质 Parquet row group；split 在第一次扫描目标列前只读 footer 验证该边界，再下推 development 条件。训练标签必须在 validation 首个决策时点之前结束，且 `label_available_time` 不晚于该拟合时点；validation 标签必须在 test 首个决策时点之前结束，且当时已经可见。两处都按真实标签区间与可见时间 purge，validation 与 test 之间可声明 embargo。development 标签还必须在 locked holdout 起点前结束且已经可见，才能参与最终拟合。split 工件只保留 development 样本和不含目标值的 holdout 索引，不会把 holdout target 传给训练、预测或选择节点。
- `preprocess-fit` 只在当前 fold 的训练样本拟合中位数、均值、标准差和可选特征选择，并生成 FitScope 证书与工件绑定。
- `fit` 只接受静态白名单：Ridge、Elastic Net、Huber、条件逻辑回归和条件 LightGBM。正式确定性路径固定 `thread_count=1`，模型工件绑定样本、参数、依赖版本、seed 和 preflight。只有显式 `CandidateFitRejected` 会淘汰当前候选并继续；未知程序错误、数据合同错误和依赖 API 错误会使节点失败，不能缩小候选族后仍发布成功。
- `predict` 与 `fold-metrics` 只生成 validation 预测和指标，并保留指标所用标签的最晚结束时间和可见时间。`selection` 在每个 test fold 的首个决策时点冻结候选，只汇总此前全部标签已可见的 validation fold 指标。整段 test 使用该候选及该 fold 已拟合的模型；未来 validation 指标和未来拟合失败都不会回填到过去。
- selection 同时保留全 development 的最终候选，供随后 locked holdout 使用；该候选不用于重评历史 test。最终候选选择时间不得晚于 locked holdout 起点。
- `locked-holdout` 先以跨进程互斥账本写入 `prepared` 并完成无值预检，再原子写入 `opened`，随后才读取原始 Feature/Label 和 holdout target，并复验它们与 split 使用的是同一内容身份。`opened` 后无论成功或失败都会形成不可重复的 `terminal`；代码修复需要新研究身份，旧账本不得覆盖。

## 防泄漏边界

- 禁止随机切分、全样本拟合预处理器、用 test/holdout 指标选择候选、标签进入 Feature/Baseline 祖先、未净化的重叠标签。
- Arrow 条件下推只能说明请求了过滤；只有生产 Label 的同质 row group 和 split 的 footer 门禁同时成立，才能证明打开前的目标扫描不会触达含 holdout 行的 row group。
- 缺失填充、标准化和特征选择的参数都来自训练 fold。validation/test/holdout 只应用已经封存的参数。
- `accuracy`、`neg_mean_squared_error`、`neg_mean_absolute_error` 都是越大越好，locked holdout 的方向冻结为 `greater`。
- 预测分数仍是研究工件，不是订单、仓位或可交易性证明。
- 分类必须通过两类样本数和占比门禁。LightGBM 的简单模型 Gate 未通过时状态是 `NOT_RUN`，不是失败，也不会换成其他模型。
- 缺少已声明可选依赖时在 preflight 失败，不在运行中静默降级。

## 选模工件与读取方式

- `fold_selections` 记录各测试段的候选、选择时间、验证均值、可用验证段数，以及选择前和评价后的 TrialLedger 身份。
- `fold_trial_events` 按测试段保留当时可见的完整候选族状态和一次 test 评价。后续拟合失败只影响它已可见之后的选择；最终候选仍要求所有 fold 成功。
- `selection.selected_candidate_id`、`validation_metric` 和 `trial_events` 对应全 development 的最终选择，`selection_scope=subsequent_locked_holdout`。`test_metric` 是逐段冻结候选的测试指标均值，`test_metric_scope=walk_forward_frozen_candidates`，不能解释为最终候选在历史上的表现。
- 一个验证段的所有标签都已可见后，该段均值才参与选择；不会把尚不可见标签混入部分段分数。样本不足以留下非空训练或验证集时，节点明确失败。
- 训练和 validation 预测按 fold 读取一次、共享给冻结候选族。测试预测只读取 test 分区一次。内存中只保留当前分区，不缓存全部 fold；seed、候选身份、候选输出顺序和 validation 物理分区顺序保持稳定。
- 分区描述记录文件、角色、fold、行数和现有记录摘要，读取前裁剪角色，读取后先复验当前分区再消费。表摘要仍沿用 `partition-records-v1` 的分区摘要序列；记录摘要使用现有 typed canonical 流编码，类型、空值、日期、行序和浮点编码与原编码相同。

## 当前能力边界

能力状态为 `local_only`。当前已用确定性合成日频样本、七阶段列式工件、候选全集 TrialLedger 和手工攻击回归验收；尚未运行真实研究数据，也没有独立 sealed 验收，因此不能声称模型有效、策略可交易或能够在框架外阻止直接读取 holdout 文件。

## 工件版本

模型工件合同为 `research-walk-forward-model-v3`，分区描述显式记录各表的数据文件与角色。公开阶段在读取数据页前检查版本，拒绝旧版、缺失版本或缺失分区描述的工件。新实现身份参与节点缓存与恢复判断；旧工件需在新运行中重新生成，历史 Result 保留原样。
