# 财报分析

标的刚发财报时，做专业级分析。

## 所需数据能力

- 获取报价 — `python3 scripts/cli.py quote '{"symbol":"XXX"}'`
- 历史价格 — `python3 scripts/cli.py price_history '{"symbol":"XXX","period":"3mo"}'`
- 基本面指标 — `python3 scripts/cli.py fundamentals '{"symbol":"XXX"}'`
- 财报日历 — `python3 scripts/cli.py earnings_calendar '{"symbols":["XXX"]}'`
- 一致预期 — `python3 scripts/cli.py consensus_estimates '{"symbol":"XXX"}'`
- 历史财务 — `python3 scripts/cli.py historical '{"symbol":"XXX","years":3}'`
- DCF 估值 — `python3 scripts/cli.py dcf '{"symbol":"XXX"}'`
- 技术指标 — `python3 scripts/cli.py technicals '{"symbol":"XXX"}'`
- 新闻 — `python3 scripts/cli.py news '{"symbol":"XXX"}'`
- Web Search — 搜索最新财报细节、管理层指引

## 分析框架

### 1. Beat/Miss 量化
| 指标 | 预期 | 实际 | Beat/Miss | 幅度 |
|------|------|------|----------|------|
| Revenue | | | | |
| EPS | | | | |
| Gross Margin | | | | |

### 2. 关键指标趋势（连续 4 季度）
- 毛利率趋势：扩张 / 稳定 / 收缩
- 营运利润率趋势
- 收入加速/减速

### 3. 指引变化
- 方向：上调 / 维持 / 下调
- 幅度 vs 一致预期

### 4. 估值重估
- 获取最新 DCF 估值
- 财报前后估值变化

### 5. 市场反应分析
- 用历史价格获取财报日前后真实价格/成交量数据
- 对比盘后反应、Day 1 收盘表现与历史均值，识别过度反应

### 6. 论点影响
- 哪些 Pillar 被强化/削弱
- 是否触发 Kill Switch

## 深度分析

加载 `references/earnings-analysis.md` 获取 7 步专业级分析流程。

## 反偏见检查（必做）

分析完成后，加载 `references/anti-bias.md` 执行：
1. 认知陷阱自检 — 是不是只看了利好数据？
2. Pre-Mortem — 如果接下来股价跌 20%，最可能原因是什么？
3. 结论中标注关键证据层级 [T1/T2/T3]

## 输出
1. 一句话结论
2. 关键数据表（含证据层级标注）
3. 指引变化分析
4. 市场反应分析
5. 论点影响评估
6. 反偏见检查结果
7. 行动建议
