#!/usr/bin/env python3
"""
fin-data CLI dispatch — 安全地暴露 lib.py 函数给外部调用。
用法: python3 cli.py <function_name> [json_params]
示例: python3 cli.py quote '{"symbol":"NVDA"}'
      python3 cli.py macro '{}'
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import *  # noqa: F401,F403

# ---------------------------------------------------------------------------
# Dispatch table: name → (handler, description)
# 每个 handler 接受一个 dict 参数，返回 JSON-serializable 结果
# ---------------------------------------------------------------------------

def _dcf_standalone(p):
    """DCF 独立调用：自动获取依赖数据。"""
    sym = p["symbol"]
    fund = yf_fundamentals(sym)
    hist = yf_historical_financials(sym, years=p.get("years", 3))
    macro = fred_macro_dashboard()
    macro_dict = {}
    if isinstance(macro, dict) and macro.get("dashboard"):
        for item in macro["dashboard"]:
            sid = item.get("series_id", "")
            val = item.get("value")
            if val is not None:
                try:
                    macro_dict[sid] = float(val)
                except (TypeError, ValueError):
                    pass
    return compute_professional_dcf(sym, fund, hist, macro_dict)


def _comps_standalone(p):
    """Comps 独立调用：自动获取基本面。"""
    symbols = p["symbols"]
    fundamentals = {}
    for sym in symbols:
        f = yf_fundamentals(sym)
        if f:
            fundamentals[sym] = f
    return compute_comps_table(symbols, fundamentals)


def _flat_macro_from_dashboard():
    """Convert fred_macro_dashboard() list format to flat dict for fed_rate_expectations."""
    raw = fred_macro_dashboard()
    items = raw.get("dashboard", raw) if isinstance(raw, dict) else raw
    mapping = {"FEDFUNDS": "fed_funds", "DGS2": "2y", "DGS10": "10y", "DGS30": "30y"}
    result = {}
    for item in items:
        key = mapping.get(item.get("series_id"))
        if key:
            result[key] = item.get("value")
    return result

DISPATCH = {
    # --- 行情 ---
    "quote":              lambda p: yf_quote(p["symbol"]),
    "global_indices":     lambda p: yf_global_indices(),
    "exchange_rate":      lambda p: get_exchange_rate(),

    # --- 基本面 ---
    "fundamentals":       lambda p: yf_fundamentals(p["symbol"]),
    "historical":         lambda p: yf_historical_financials(p["symbol"], years=p.get("years", 3), freq=p.get("freq", "yearly")),
    "price_history":      lambda p: yf_price_history(
        p["symbol"],
        period=p.get("period", "1mo"),
        interval=p.get("interval", "1d"),
        start_date=p.get("start_date"),
        end_date=p.get("end_date"),
    ),
    "dividends":          lambda p: yf_dividends(p["symbol"]),
    "news":               lambda p: yf_news(p["symbol"]),
    "recommendation":     lambda p: yf_recommendation(p["symbol"]),

    # --- 估值与建模 ---
    "dcf":                _dcf_standalone,
    "comps":              _comps_standalone,
    "valuations":         lambda p: fetch_valuations(p["symbols"]),

    # --- 技术面与健康 ---
    "technicals":         lambda p: compute_technical_indicators(p["symbol"]),
    "health_score":       lambda p: compute_health_score(p["symbol"]),

    # --- 宏观 ---
    "macro":              lambda p: fred_macro_dashboard(),
    "yield_curve":        lambda p: fred_yield_curve(),
    "inflation":          lambda p: fred_inflation_monitor(),
    "financial_conditions": lambda p: fred_financial_conditions(),
    "fred_series":        lambda p: fred_get_series(p["series_id"], limit=p.get("limit", 120)),

    # --- 财报 ---
    "earnings_calendar":  lambda p: yf_earnings_calendar(p["symbols"]),
    "consensus_estimates": lambda p: yf_consensus_estimates(p["symbol"]),
    "upcoming_earnings":  lambda p: yf_upcoming_earnings(p["symbol"]),
    "options_implied_move": lambda p: yf_options_implied_move(p["symbol"]),

    # --- 内部人与机构 ---
    "insider":            lambda p: yf_insider_details(p["symbol"]),
    "institutional":      lambda p: yf_institutional_holders(p["symbol"]),
    "institutional_flow": lambda p: institutional_flow(p["symbol"]),

    # --- 情绪与流向 ---
    "put_call_ratio":     lambda p: fetch_put_call_ratio(p.get("symbol", "SPY")),
    "etf_flows":          lambda p: compute_etf_fund_flows(p.get("etfs")),
    "cot_report":         lambda p: cftc_cot_report(),
    "fed_expectations":   lambda p: compute_fed_rate_expectations(p.get("macro") or _flat_macro_from_dashboard()),

    # --- SEC ---
    "sec_filings":        lambda p: sec_fetch_filings(p["symbol"], limit=p.get("limit", 5)),
    "sec_highlights":     lambda p: sec_filing_highlights(p["symbol"]),

    # --- 期权 ---
    "options":            lambda p: yf_options_deep(p["symbol"]),

    # --- Finnhub（需设置 FINNHUB_API_KEY，无 key 时返回 null）---
    "finnhub_insider_sentiment":     lambda p: finnhub_insider_sentiment(p["symbol"]),
    "finnhub_earnings_surprises":    lambda p: finnhub_earnings_surprises(p["symbol"]),
    "finnhub_recommendation_trends": lambda p: finnhub_recommendation_trends(p["symbol"]),
}


class _SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            import pandas as pd
            if pd.isna(obj):
                return None
        except (TypeError, ValueError, ImportError):
            pass
        try:
            import numpy as np
            if isinstance(obj, np.generic):
                return obj.item()
        except (ImportError, Exception):
            pass
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return super().default(obj)


def main():
    if len(sys.argv) < 2:
        # 列出所有可用函数
        print(json.dumps({"available": sorted(DISPATCH.keys())}))
        sys.exit(0)

    func_name = sys.argv[1]

    if func_name not in DISPATCH:
        print(json.dumps({"error": f"Unknown function: {func_name}", "available": sorted(DISPATCH.keys())}))
        sys.exit(1)

    params = {}
    if len(sys.argv) > 2:
        try:
            params = json.loads(sys.argv[2])
        except json.JSONDecodeError as e:
            print(json.dumps({"error": f"Invalid JSON params: {e}"}))
            sys.exit(1)

    try:
        result = DISPATCH[func_name](params)
        print(json.dumps(result, ensure_ascii=False, cls=_SafeEncoder))
    except KeyError as e:
        print(json.dumps({"error": f"Missing required parameter: {e}"}))
        sys.exit(1)
    except Exception as e:
        print(json.dumps({"error": str(e), "type": type(e).__name__}))
        sys.exit(1)


if __name__ == "__main__":
    main()
