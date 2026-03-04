# 赛道扫描

横向对比指定赛道/板块的标的，找出最强的。

## 标的来源

**不预设候选列表。** 标的来源按优先级：
1. 用户指定的标的列表
2. 用户关注的赛道 → AI 通过搜索发现主要参与者
3. 现有持仓 + 同业竞品

## 所需数据能力

- 可比公司 — `python3 scripts/cli.py comps '{"symbols":["XXX","YYY","ZZZ"]}'`
- 基本面指标 — `python3 scripts/cli.py fundamentals '{"symbol":"XXX"}'`
- 技术指标 — `python3 scripts/cli.py technicals '{"symbol":"XXX"}'`
- 健康评分 — `python3 scripts/cli.py health_score '{"symbol":"XXX"}'`
- 历史价格 — `python3 scripts/cli.py price_history '{"symbol":"XXX","period":"3mo"}'`
- 新闻 — `python3 scripts/cli.py news '{"symbol":"XXX"}'`
- Web Search — 行业动态验证

## 输出

1. **Comps 可比表**（直接引用可比公司结果）
2. **排名表**（AI 根据赛道特点选择合适的评分维度）
3. **发现比持仓更优的标的时，明确指出**

## 深度分析
- 竞争分析：加载 `references/competitive-analysis.md`
- 选股评分：加载 `references/idea-generation.md`
- Variant View：加载 `references/variant-view.md` — 找出市场共识盲点
- 证据分级：关键判断标注 [T1/T2/T3]，详见 `references/evidence-hierarchy.md`
