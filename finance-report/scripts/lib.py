"""
投研工具链公共模块 — trade-master Skill 内置数据层
所有数据从 yfinance / FRED / SEC EDGAR 动态获取，无持久化配置文件
"""

import csv
import gzip
import io
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta
from typing import Any

import yfinance as yf

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SKILL_DIR = os.path.dirname(SCRIPT_DIR)



# ---------------------------------------------------------------------------
# yfinance — 替代 Finnhub，无 API key，无速率限制
# ---------------------------------------------------------------------------

_YF_CACHE: dict[str, dict] = {}

def yf_fetch(sym: str, max_age: int = 300) -> dict:
    """获取 yfinance 数据，同标的 5 分钟内缓存复用。"""
    now = time.time()
    if sym in _YF_CACHE and (now - _YF_CACHE[sym]["ts"]) < max_age:
        return _YF_CACHE[sym]
    t = yf.Ticker(sym)
    data: dict[str, Any] = {"ticker": t, "info": {}, "recommendations": [], "calendar": {}, "ts": now}
    try:
        data["info"] = t.info or {}
    except Exception:
        pass
    # 抑制 yfinance 对非股票 ticker（ETF/外汇等）的 stderr 404 噪音
    _stderr = sys.stderr
    try:
        sys.stderr = io.StringIO()
        r = t.recommendations
        data["recommendations"] = r.to_dict("records") if r is not None and not r.empty else []
    except Exception:
        pass
    finally:
        sys.stderr = _stderr
    try:
        sys.stderr = io.StringIO()
        data["calendar"] = t.calendar or {}
    except Exception:
        pass
    finally:
        sys.stderr = _stderr
    _YF_CACHE[sym] = data
    return data


def _yf_pct(v):
    """yfinance 返回的比率值转百分比（0.667 → 66.7, 2.721 → 272.1）。"""
    if v is None:
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return round(v * 100, 2)


def yf_quote(sym: str) -> dict | None:
    """报价 → 兼容现有 quote dict 格式。"""
    info = yf_fetch(sym)["info"]
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    pc = info.get("previousClose") or info.get("regularMarketPreviousClose")
    if not price:
        return None
    return {
        "price": price,
        "change_pct": round((price - pc) / pc * 100, 2) if pc else None,
        "high": info.get("dayHigh"),
        "low": info.get("dayLow"),
        "prev_close": pc,
        "source": "yfinance",
    }


def yf_fundamentals(sym: str) -> dict:
    """基本面 → 兼容现有 fundamental dict 字段名。增强版含 marketCap/PE/增速等。"""
    info = yf_fetch(sym)["info"]
    mapping = {
        "psTTM": info.get("priceToSalesTrailing12Months"),
        "peNormalizedAnnual": info.get("forwardPE") or info.get("trailingPE"),
        "forwardPE": info.get("forwardPE"),
        "trailingPE": info.get("trailingPE"),
        "marketCap": info.get("marketCap"),
        "enterpriseValue": info.get("enterpriseValue"),
        "revenueGrowthQuarterlyYoy": _yf_pct(info.get("revenueGrowth")),
        "earningsGrowthQuarterlyYoy": _yf_pct(info.get("earningsGrowth")),
        "revenuePerShare": info.get("revenuePerShare"),
        "grossMarginTTM": _yf_pct(info.get("grossMargins")),
        "operatingMarginTTM": _yf_pct(info.get("operatingMargins")),
        "profitMarginTTM": _yf_pct(info.get("profitMargins")),
        "roeTTM": _yf_pct(info.get("returnOnEquity")),
        "roaTTM": _yf_pct(info.get("returnOnAssets")),
        "debtToEquity": info.get("debtToEquity"),
        "currentRatio": info.get("currentRatio"),
        "freeCashflow": info.get("freeCashflow"),
        "beta": info.get("beta"),
        "52WeekHigh": info.get("fiftyTwoWeekHigh"),
        "52WeekLow": info.get("fiftyTwoWeekLow"),
        "52WeekChange": _yf_pct(info.get("52WeekChange")),
        "sharesOutstanding": info.get("sharesOutstanding"),
        "floatShares": info.get("floatShares"),
        "insiderHoldPct": _yf_pct(info.get("heldPercentInsiders")),
        "institutionHoldPct": _yf_pct(info.get("heldPercentInstitutions")),
        "shortRatio": info.get("shortRatio"),
        "shortPercentOfFloat": _yf_pct(info.get("shortPercentOfFloat")),
        # --- DCF / Comps 所需字段 ---
        "totalRevenue": info.get("totalRevenue"),
        "ebitda": info.get("ebitda"),
        "operatingIncome": info.get("operatingIncome"),
        "totalDebt": info.get("totalDebt"),
        "totalCash": info.get("totalCash"),
        "effectiveTaxRate": _yf_pct(info.get("effectiveTaxRate")),
        "enterpriseToEbitda": info.get("enterpriseToEbitda"),
        "enterpriseToRevenue": info.get("enterpriseToRevenue"),
        "forwardEps": info.get("forwardEps"),
        "trailingEps": info.get("trailingEps"),
        "dividendYield": _yf_pct(info.get("dividendYield")),
        "operatingCashflow": info.get("operatingCashflow"),
        # --- 分析师目标价 ---
        "targetMeanPrice": info.get("targetMeanPrice"),
        "targetHighPrice": info.get("targetHighPrice"),
        "targetLowPrice": info.get("targetLowPrice"),
        "targetMedianPrice": info.get("targetMedianPrice"),
        "numberOfAnalystOpinions": info.get("numberOfAnalystOpinions"),
    }
    return {k: v for k, v in mapping.items() if v is not None}


def yf_price_history(
    sym: str,
    period: str = "1mo",
    interval: str = "1d",
    start_date: str = None,
    end_date: str = None,
) -> dict:
    """历史价格（OHLCV）：支持 period 或日期区间查询。"""
    t = yf_fetch(sym).get("ticker") or yf.Ticker(sym)

    def _safe_price(v):
        try:
            f = float(v)
            if math.isnan(f):
                return 0.0
            return round(f, 4)
        except (TypeError, ValueError):
            return 0.0

    def _safe_volume(v):
        try:
            f = float(v)
            if math.isnan(f):
                return 0
            return int(f)
        except (TypeError, ValueError):
            return 0

    if start_date:
        hist = t.history(start=start_date, end=end_date or None, interval=interval)
    else:
        hist = t.history(period=period, interval=interval)

    if hist.empty:
        return {"error": f"No price data for {sym}"}

    records = []
    for idx, row in hist.iterrows():
        date_str = str(idx.date()) if hasattr(idx, "date") else str(idx)
        records.append({
            "date": date_str,
            "open": _safe_price(row.get("Open", 0)),
            "high": _safe_price(row.get("High", 0)),
            "low": _safe_price(row.get("Low", 0)),
            "close": _safe_price(row.get("Close", 0)),
            "volume": _safe_volume(row.get("Volume", 0)),
        })
    return {"symbol": sym, "count": len(records), "prices": records}


def yf_dividends(sym: str) -> dict:
    """分红历史。"""
    t = yf_fetch(sym).get("ticker") or yf.Ticker(sym)
    divs = t.dividends
    if divs is None or divs.empty:
        return {"symbol": sym, "dividends": [], "count": 0}

    records = []
    for idx, val in divs.items():
        try:
            fval = float(val)
            if math.isnan(fval):
                continue
        except (TypeError, ValueError):
            continue
        date_str = str(idx.date()) if hasattr(idx, "date") else str(idx)
        records.append({"date": date_str, "dividend": round(fval, 6)})
    return {"symbol": sym, "count": len(records), "dividends": records}


def yf_news(sym: str) -> dict:
    """Yahoo Finance 最新新闻。"""
    t = yf_fetch(sym).get("ticker") or yf.Ticker(sym)
    news = t.news or []
    items = []
    for n in news[:20]:
        if not isinstance(n, dict):
            continue
        # yfinance >=0.2.36 wraps data in 'content' sub-dict
        c = n.get("content", n)
        if not isinstance(c, dict):
            continue
        provider = c.get("provider", {})
        pub_name = provider.get("displayName", "") if isinstance(provider, dict) else str(provider)
        link = ""
        for url_key in ("clickThroughUrl", "canonicalUrl"):
            url_obj = c.get(url_key, {})
            if isinstance(url_obj, dict) and url_obj.get("url"):
                link = url_obj["url"]
                break
            elif isinstance(url_obj, str) and url_obj:
                link = url_obj
                break
        # fallback: old-style flat keys
        if not link:
            link = c.get("link", n.get("link", ""))
        tickers = []
        finance = c.get("finance", {})
        if isinstance(finance, dict):
            for st in finance.get("stockTickers", []):
                if isinstance(st, dict) and st.get("symbol"):
                    tickers.append(st["symbol"])
        if not tickers:
            tickers = c.get("relatedTickers", n.get("relatedTickers", []))
        items.append({
            "title": c.get("title", n.get("title", "")),
            "publisher": pub_name,
            "link": link,
            "type": c.get("contentType", n.get("type", "")),
            "pubDate": c.get("pubDate", ""),
            "summary": (c.get("summary", "") or "")[:300],
            "relatedTickers": tickers,
        })
    return {"symbol": sym, "count": len(items), "news": items}


def yf_recommendation(sym: str) -> dict | None:
    """分析师推荐 → 兼容现有 recommendation dict。"""
    recs = yf_fetch(sym)["recommendations"]
    if not recs:
        return None
    r = recs[0]
    return {
        "strongBuy": r.get("strongBuy", 0),
        "buy": r.get("buy", 0),
        "hold": r.get("hold", 0),
        "sell": r.get("sell", 0),
        "strongSell": r.get("strongSell", 0),
        "period": r.get("period", ""),
    }


def yf_earnings_calendar(symbols) -> list[dict]:
    """财报日历 + 最近已披露结果（EPS）→ 兼容现有 earnings_calendar 格式。"""
    results = []

    def _clean_num(v):
        if v is None:
            return None
        try:
            f = float(v)
            return None if math.isnan(f) else f
        except (TypeError, ValueError):
            return None

    for sym in symbols:
        by_date: dict[str, dict[str, Any]] = {}

        # 历史/近期财报（含实际 EPS）
        try:
            t = yf.Ticker(sym)
            ed = t.get_earnings_dates(limit=8)
            if ed is not None and not ed.empty:
                for idx, row in ed.iterrows():
                    try:
                        ds = idx.to_pydatetime().date().isoformat()
                    except Exception:
                        ds = str(idx)[:10]

                    item = by_date.setdefault(ds, {"symbol": sym, "date": ds})
                    eps_est = _clean_num(row.get("EPS Estimate"))
                    eps_act = _clean_num(row.get("Reported EPS"))
                    eps_surprise = _clean_num(row.get("Surprise(%)"))
                    if eps_est is not None:
                        item["epsEstimate"] = eps_est
                    if eps_act is not None:
                        item["epsActual"] = eps_act
                    if eps_surprise is not None:
                        item["epsSurprisePct"] = eps_surprise
        except Exception:
            pass

        # Backfill revenue from quarterly income statement
        try:
            qi = t.quarterly_income_stmt
            if qi is not None and not qi.empty:
                for col in qi.columns:
                    try:
                        ds = col.to_pydatetime().date().isoformat()
                    except Exception:
                        ds = str(col)[:10]
                    # Find matching earnings date (within ~45 day window since fiscal quarter end != earnings date)
                    matched_date = None
                    for existing_date in list(by_date.keys()):
                        try:
                            ed = datetime.strptime(existing_date, "%Y-%m-%d").date()
                            qd = datetime.strptime(ds, "%Y-%m-%d").date()
                            if abs((ed - qd).days) <= 45:
                                matched_date = existing_date
                                break
                        except Exception:
                            continue
                    if matched_date:
                        item = by_date[matched_date]
                    else:
                        # No matching earnings date found, skip
                        continue
                    # Add revenue actual
                    rev = None
                    for rev_key in ['Total Revenue', 'Revenue']:
                        if rev_key in qi.index:
                            v = qi.loc[rev_key, col]
                            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                                rev = float(v)
                                break
                    if rev is not None and item.get("revenueActual") is None:
                        item["revenueActual"] = rev
        except Exception:
            pass

        # 未来财报（补充 revenue estimate）
        cal = yf_fetch(sym).get("calendar") or {}
        dates = cal.get("Earnings Date", [])
        if dates:
            ds = str(dates[0])[:10]
            item = by_date.setdefault(ds, {"symbol": sym, "date": ds})
            eps_est = _clean_num(cal.get("Earnings Average"))
            rev_est = _clean_num(cal.get("Revenue Average"))
            if item.get("epsEstimate") is None and eps_est is not None:
                item["epsEstimate"] = eps_est
            if rev_est is not None:
                item["revenueEstimate"] = rev_est

        if by_date:
            rows = sorted(by_date.values(), key=lambda x: x.get("date") or "")
            if len(rows) > 8:
                rows = rows[-8:]
            results.extend(rows)
    return results


def yf_historical_financials(sym: str, years: int = 3, freq: str = "yearly") -> dict:
    """获取历史财务数据（年度 IS/BS/CF），用于 DCF 建模和趋势分析。

    Returns:
        {
            "symbol": str,
            "annual": [  # 按年份从旧到新排列
                {
                    "date": "2024-06-30",
                    "revenue": float,
                    "ebit": float,          # Operating Income
                    "ebitda": float,         # EBIT + D&A
                    "depreciation": float,   # D&A
                    "capex": float,          # Capital Expenditure (负数)
                    "operating_cashflow": float,
                    "free_cashflow": float,  # OCF + CapEx
                    "total_assets": float,
                    "total_debt": float,
                    "total_cash": float,
                    "shareholders_equity": float,
                    "net_income": float,
                    "tax_provision": float,
                    "interest_expense": float,
                    "working_capital_change": float,
                }
            ],
            "growth_trends": {
                "revenue_cagr": float,  # 3年 CAGR
                "ebit_cagr": float,
                "margin_trend": "expanding" | "stable" | "contracting",
            }
        }
    """
    import math

    t = yf_fetch(sym).get("ticker") or yf.Ticker(sym)
    result = {"symbol": sym, "frequency": None, "annual": [], "growth_trends": {}}
    freq_mode = (freq or "yearly").strip().lower()
    if freq_mode not in {"yearly", "quarterly"}:
        result["warning"] = f"Unsupported freq '{freq_mode}', fallback to yearly"
        freq_mode = "yearly"
    result["frequency"] = freq_mode

    try:
        if freq_mode == "quarterly":
            is_df = t.quarterly_income_stmt
            cf_df = t.quarterly_cashflow
            bs_df = t.quarterly_balance_sheet
        else:
            is_df = t.financials  # 年度利润表
            cf_df = t.cashflow  # 年度现金流
            bs_df = t.balance_sheet  # 年度资产负债表
    except Exception:
        return result

    if is_df is None or is_df.empty:
        return result

    # 取最近 N 年的列（yfinance 列为日期降序）
    cols = list(is_df.columns[:years])
    cols.reverse()  # 变为从旧到新

    def _get(df, row_names, col):
        """从 DataFrame 安全取值，支持多个行名匹配。"""
        if df is None or df.empty:
            return None
        for name in row_names:
            if name in df.index and col in df.columns:
                v = df.loc[name, col]
                if v is not None and not (isinstance(v, float) and math.isnan(v)):
                    return float(v)
        return None

    for col in cols:
        year_data = {
            "date": str(col.date()) if hasattr(col, "date") else str(col),
            "revenue": _get(is_df, ["Total Revenue", "Revenue"], col),
            "ebit": _get(is_df, ["Operating Income", "EBIT"], col),
            "net_income": _get(is_df, ["Net Income", "Net Income Common Stockholders"], col),
            "tax_provision": _get(is_df, ["Tax Provision", "Income Tax Expense"], col),
            "interest_expense": _get(is_df, ["Interest Expense", "Interest Expense Non Operating"], col),
            "depreciation": _get(cf_df, ["Depreciation And Amortization", "Depreciation & Amortization"], col),
            "capex": _get(cf_df, ["Capital Expenditure", "Capital Expenditures"], col),
            "operating_cashflow": _get(cf_df, ["Operating Cash Flow", "Cash Flow From Continuing Operating Activities"], col),
            "working_capital_change": _get(cf_df, ["Change In Working Capital", "Changes In Working Capital"], col),
            "total_assets": _get(bs_df, ["Total Assets"], col),
            "total_debt": _get(bs_df, ["Total Debt", "Long Term Debt And Capital Lease Obligation"], col),
            "total_cash": _get(bs_df, ["Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments"], col),
            "shareholders_equity": _get(bs_df, ["Stockholders Equity", "Total Stockholders Equity", "Stockholders' Equity"], col),
        }
        # 计算衍生值
        ebit = year_data.get("ebit")
        da = year_data.get("depreciation")
        if ebit is not None and da is not None:
            year_data["ebitda"] = ebit + abs(da)
        else:
            year_data["ebitda"] = None

        ocf = year_data.get("operating_cashflow")
        capex = year_data.get("capex")
        if ocf is not None and capex is not None:
            year_data["free_cashflow"] = ocf + capex  # capex 通常为负
        else:
            year_data["free_cashflow"] = None

        result["annual"].append(year_data)

    # 计算增长趋势
    annuals = result["annual"]
    if len(annuals) >= 2:
        first_rev = annuals[0].get("revenue")
        last_rev = annuals[-1].get("revenue")
        n = len(annuals) - 1
        if first_rev and last_rev and first_rev > 0 and last_rev > 0:
            result["growth_trends"]["revenue_cagr"] = round(
                ((last_rev / first_rev) ** (1 / n) - 1) * 100, 2
            )

        first_ebit = annuals[0].get("ebit")
        last_ebit = annuals[-1].get("ebit")
        if first_ebit and last_ebit and first_ebit > 0 and last_ebit > 0:
            result["growth_trends"]["ebit_cagr"] = round(
                ((last_ebit / first_ebit) ** (1 / n) - 1) * 100, 2
            )

        # 利润率趋势
        margins = []
        for a in annuals:
            rev = a.get("revenue")
            ebit = a.get("ebit")
            if rev and ebit and rev > 0:
                margins.append(ebit / rev)
        if len(margins) >= 2:
            if margins[-1] > margins[0] + 0.01:
                result["growth_trends"]["margin_trend"] = "expanding"
            elif margins[-1] < margins[0] - 0.01:
                result["growth_trends"]["margin_trend"] = "contracting"
            else:
                result["growth_trends"]["margin_trend"] = "stable"

    # 季度数据（用于收入加速度分析）
    try:
        q_is = t.quarterly_financials
        if q_is is not None and not q_is.empty:
            q_cols = list(q_is.columns[:8])  # 最近 8 个季度
            q_cols.reverse()
            quarterly = []
            for col in q_cols:
                qd = {
                    "date": str(col.date()) if hasattr(col, "date") else str(col),
                    "revenue": _get(q_is, ["Total Revenue", "Revenue"], col),
                    "ebit": _get(q_is, ["Operating Income", "EBIT"], col),
                    "net_income": _get(q_is, ["Net Income", "Net Income Common Stockholders"], col),
                }
                quarterly.append(qd)
            result["quarterly"] = quarterly

            # 收入加速度：连续季度环比增速趋势
            qoq_growths = []
            for i in range(1, len(quarterly)):
                cur = quarterly[i].get("revenue")
                prev = quarterly[i - 1].get("revenue")
                if cur and prev and prev > 0:
                    qoq_growths.append(round((cur / prev - 1) * 100, 2))
            if len(qoq_growths) >= 3:
                # 看最近 3 个 QoQ 是否持续扩大或缩小
                recent = qoq_growths[-3:]
                if recent[-1] > recent[0] + 2:
                    result["growth_trends"]["revenue_acceleration"] = "accelerating"
                elif recent[-1] < recent[0] - 2:
                    result["growth_trends"]["revenue_acceleration"] = "decelerating"
                else:
                    result["growth_trends"]["revenue_acceleration"] = "steady"
                result["growth_trends"]["quarterly_qoq_growths"] = qoq_growths
    except Exception:
        pass

    return result


# ---------------------------------------------------------------------------
# 新增数据源：consensus / options / insider / institutional
# ---------------------------------------------------------------------------

def _safe_val(v):
    """Convert pandas/numpy scalar to Python native; NaN/NA/NaT → None."""
    if v is None:
        return None
    # Handle pandas NA types (NAType, NaT, etc.)
    try:
        import pandas as pd
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    except Exception:
        pass
    # Handle numpy scalars
    try:
        import numpy as np
        if isinstance(v, (np.generic,)):
            try:
                if np.isnan(v):
                    return None
            except (TypeError, ValueError):
                pass
            return v.item()
    except Exception:
        pass
    # Handle plain float NaN
    if isinstance(v, float) and math.isnan(v):
        return None
    # Handle Timestamp objects
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def _df_to_records(df):
    """Convert a pandas DataFrame to list[dict] with NaN→None and datetime→str."""
    if df is None:
        return []
    try:
        if hasattr(df, "empty") and df.empty:
            return []
    except Exception:
        return []
    rows = []
    for idx, row in df.iterrows():
        rec = {}
        idx_str = str(idx)
        rec["_index"] = idx_str
        for col in df.columns:
            v = row[col]
            v = _safe_val(v)
            if hasattr(v, "isoformat"):
                v = v.isoformat()
            elif hasattr(v, "to_pydatetime"):
                v = str(v)
            rec[col] = v
        rows.append(rec)
    return rows


def yf_consensus_estimates(sym: str) -> dict | None:
    """Get consensus EPS/revenue estimates and growth estimates."""
    try:
        cached = yf_fetch(sym)
        t = cached.get("ticker")
        if t is None:
            return None

        result = {}

        _stderr = sys.stderr
        try:
            sys.stderr = io.StringIO()
            ee = getattr(t, "earnings_estimate", None)
            if ee is not None and hasattr(ee, "empty") and not ee.empty:
                result["earnings_estimate"] = _df_to_records(ee)
        except Exception:
            pass
        finally:
            sys.stderr = _stderr

        try:
            sys.stderr = io.StringIO()
            re_ = getattr(t, "revenue_estimate", None)
            if re_ is not None and hasattr(re_, "empty") and not re_.empty:
                result["revenue_estimate"] = _df_to_records(re_)
        except Exception:
            pass
        finally:
            sys.stderr = _stderr

        try:
            sys.stderr = io.StringIO()
            ge = getattr(t, "growth_estimates", None)
            if ge is not None and hasattr(ge, "empty") and not ge.empty:
                result["growth_estimates"] = _df_to_records(ge)
        except Exception:
            pass
        finally:
            sys.stderr = _stderr

        return result if result else None
    except Exception:
        return None


def yf_upcoming_earnings(sym: str) -> dict | None:
    """Get next earnings date and consensus from ticker calendar."""
    try:
        cached = yf_fetch(sym)
        cal = cached.get("calendar") or {}
        if not cal:
            return None

        result = {}
        dates = cal.get("Earnings Date", [])
        if dates:
            result["earnings_date"] = str(dates[0])[:10]
            if len(dates) > 1:
                result["earnings_date_end"] = str(dates[1])[:10]

        for key in ["Earnings Average", "Earnings Low", "Earnings High",
                     "Revenue Average", "Revenue Low", "Revenue High"]:
            v = _safe_val(cal.get(key))
            if v is not None:
                result[key.lower().replace(" ", "_")] = v

        return result if result else None
    except Exception:
        return None


def yf_options_implied_move(sym: str) -> dict | None:
    """Calculate ATM straddle implied move from nearest expiration options chain."""
    try:
        cached = yf_fetch(sym)
        t = cached.get("ticker")
        if t is None:
            return None

        info = cached.get("info", {})
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        if not price or price <= 0:
            return None

        _stderr = sys.stderr
        try:
            sys.stderr = io.StringIO()
            expirations = t.options
        except Exception:
            return None
        finally:
            sys.stderr = _stderr

        if not expirations:
            return None

        # Try to find post-earnings expiry for better implied move estimate
        nearest_exp = expirations[0]
        try:
            cal = t.calendar
            if cal and "Earnings Date" in cal:
                edates = cal["Earnings Date"]
                edate = str(edates[0]) if isinstance(edates, list) else str(edates)
                for exp in expirations:
                    if exp >= edate:
                        nearest_exp = exp
                        break
        except Exception:
            pass

        try:
            sys.stderr = io.StringIO()
            chain = t.option_chain(nearest_exp)
        except Exception:
            return None
        finally:
            sys.stderr = _stderr

        calls = chain.calls
        puts = chain.puts
        if calls is None or calls.empty or puts is None or puts.empty:
            return None

        # Find ATM strike (closest to current price)
        strikes = calls["strike"].values
        atm_idx = abs(strikes - price).argmin()
        atm_strike = float(strikes[atm_idx])

        atm_call = calls[calls["strike"] == atm_strike]
        atm_put = puts[puts["strike"] == atm_strike]
        if atm_call.empty or atm_put.empty:
            return None

        call_mid = _safe_val(atm_call["lastPrice"].iloc[0])
        put_mid = _safe_val(atm_put["lastPrice"].iloc[0])
        if call_mid is None or put_mid is None:
            return None

        straddle = call_mid + put_mid
        implied_move_pct = round(straddle / price * 100, 2)

        call_iv = _safe_val(atm_call["impliedVolatility"].iloc[0])
        put_iv = _safe_val(atm_put["impliedVolatility"].iloc[0])

        # Get earnings date for context
        earnings_date = None
        try:
            cal = t.calendar
            if cal and "Earnings Date" in cal:
                edates = cal["Earnings Date"]
                earnings_date = str(edates[0]) if isinstance(edates, list) else str(edates)
        except Exception:
            pass

        return {
            "expiration": nearest_exp,
            "earnings_date": earnings_date,
            "atm_strike": atm_strike,
            "call_price": round(call_mid, 2),
            "put_price": round(put_mid, 2),
            "straddle_price": round(straddle, 2),
            "implied_move_pct": implied_move_pct,
            "implied_range_low": round(price - straddle, 2),
            "implied_range_high": round(price + straddle, 2),
            "call_iv": round(call_iv * 100, 2) if call_iv else None,
            "put_iv": round(put_iv * 100, 2) if put_iv else None,
            "stock_price": round(price, 2),
        }
    except Exception:
        return None


def yf_insider_details(sym: str) -> dict | None:
    """Get insider transactions and purchase summary."""
    try:
        cached = yf_fetch(sym)
        t = cached.get("ticker")
        if t is None:
            return None

        result = {}

        _stderr = sys.stderr
        try:
            sys.stderr = io.StringIO()
            txns = getattr(t, "insider_transactions", None)
            if txns is not None and hasattr(txns, "empty") and not txns.empty:
                rows = []
                for _, row in txns.iterrows():
                    rec = {}
                    for col in txns.columns:
                        v = row[col]
                        v = _safe_val(v)
                        if hasattr(v, "isoformat"):
                            v = v.isoformat()
                        elif hasattr(v, "to_pydatetime"):
                            v = str(v)
                        rec[col] = v
                    rows.append(rec)
                result["transactions"] = rows[:20]
        except Exception:
            pass
        finally:
            sys.stderr = _stderr

        try:
            sys.stderr = io.StringIO()
            purchases = getattr(t, "insider_purchases", None)
            if purchases is not None and hasattr(purchases, "empty") and not purchases.empty:
                result["purchase_summary"] = _df_to_records(purchases)
        except Exception:
            pass
        finally:
            sys.stderr = _stderr

        return result if result else None
    except Exception:
        return None


def yf_institutional_holders(sym: str) -> dict | None:
    """Get top institutional holders and major holder breakdown."""
    try:
        cached = yf_fetch(sym)
        t = cached.get("ticker")
        if t is None:
            return None

        result = {}

        _stderr = sys.stderr
        try:
            sys.stderr = io.StringIO()
            inst = getattr(t, "institutional_holders", None)
            if inst is not None and hasattr(inst, "empty") and not inst.empty:
                rows = []
                for _, row in inst.iterrows():
                    rec = {}
                    for col in inst.columns:
                        v = row[col]
                        v = _safe_val(v)
                        if hasattr(v, "isoformat"):
                            v = v.isoformat()
                        elif hasattr(v, "to_pydatetime"):
                            v = str(v)
                        rec[col] = v
                    rows.append(rec)
                result["top_holders"] = rows[:15]
        except Exception:
            pass
        finally:
            sys.stderr = _stderr

        try:
            sys.stderr = io.StringIO()
            major = getattr(t, "major_holders", None)
            if major is not None and hasattr(major, "empty") and not major.empty:
                mh = {}
                for idx in major.index:
                    key = str(idx)
                    try:
                        val = major.loc[idx].iloc[0] if len(major.columns) > 0 else major.loc[idx]
                        val = _safe_val(val)
                    except Exception:
                        val = None
                    mh[key] = val
                result["major_holders"] = mh
        except Exception:
            pass
        finally:
            sys.stderr = _stderr

        return result if result else None
    except Exception:
        return None


def _get_invested_capital_from_bs(sym):
    """Get invested capital from balance sheet (preferred for ROIC)."""
    try:
        t = yf.Ticker(sym)
        bs = t.balance_sheet
        if bs is None or bs.empty:
            return None
        # Try Invested Capital directly
        if 'Invested Capital' in bs.index:
            val = bs.loc['Invested Capital'].iloc[0]
            if val is not None and not math.isnan(val) and val > 0:
                return float(val)
        # Fallback: Stockholders Equity + Total Debt
        equity = None
        if 'Stockholders Equity' in bs.index:
            equity = bs.loc['Stockholders Equity'].iloc[0]
        elif 'Common Stock Equity' in bs.index:
            equity = bs.loc['Common Stock Equity'].iloc[0]
        debt = None
        if 'Total Debt' in bs.index:
            debt = bs.loc['Total Debt'].iloc[0]
        if equity is not None and not math.isnan(equity):
            invested = float(equity) + (float(debt) if debt is not None and not math.isnan(debt) else 0)
            if invested > 0:
                return invested
    except Exception:
        pass
    return None


def compute_comps_table(symbols: list[str], fundamentals: dict | None = None) -> dict:
    """可比公司分析表 — 对一组标的提取关键估值和运营指标，计算统计行。

    Args:
        symbols: 标的列表
        fundamentals: 已有基本面数据 {sym: {...}}，避免重复拉取

    Returns:
        {
            "companies": [
                {
                    "symbol": str,
                    "revenue": float,           # 总收入 ($M)
                    "revenue_growth": float,     # 收入增速 (%)
                    "gross_margin": float,       # 毛利率 (%)
                    "ebitda_margin": float,      # EBITDA 利润率 (%)
                    "operating_margin": float,   # 营运利润率 (%)
                    "ev_to_revenue": float,      # EV/Revenue
                    "ev_to_ebitda": float,       # EV/EBITDA
                    "forward_pe": float,         # Forward P/E
                    "trailing_pe": float,        # Trailing P/E
                    "peg": float,                # PEG Ratio
                    "fcf_yield": float,          # FCF Yield (%)
                    "roic": float,               # ROIC (%)
                    "beta": float,
                    "market_cap": float,         # ($B)
                }
            ],
            "statistics": {
                "median": {...},
                "p25": {...},
                "p75": {...},
                "min": {...},
                "max": {...},
            }
        }
    """
    import statistics

    if fundamentals is None:
        fundamentals = {}

    companies = []
    for sym in symbols:
        f = fundamentals.get(sym)
        if f is None:
            try:
                f = yf_fundamentals(sym)
            except Exception:
                f = {}

        rev = f.get("totalRevenue")
        ev = f.get("enterpriseValue")
        ebitda = f.get("ebitda")
        mc = f.get("marketCap")
        fcf = f.get("freeCashflow")
        oi = f.get("operatingIncome")

        # 计算衍生指标
        ebitda_margin = None
        if rev and ebitda and rev > 0:
            ebitda_margin = round(ebitda / rev * 100, 2)

        ev_to_rev = None
        if ev and rev and rev > 0:
            ev_to_rev = round(ev / rev, 2)

        ev_to_ebitda = f.get("enterpriseToEbitda")
        if ev_to_ebitda is None and ev and ebitda and ebitda > 0:
            ev_to_ebitda = round(ev / ebitda, 2)

        fcf_yield = None
        if fcf and mc and mc > 0:
            fcf_yield = round(fcf / mc * 100, 2)

        # ROIC = NOPAT / Invested Capital
        roic = None
        tax_rate = f.get("effectiveTaxRate")  # 已是百分比
        total_debt = f.get("totalDebt")
        total_cash = f.get("totalCash")
        # 获取 operating income：优先直接值，fallback 从 EBITDA 估算
        oi_for_roic = oi
        if oi_for_roic is None and ebitda and rev:
            # 估算 D&A 占收入 ~3-5%，用 operating margin 反推
            op_margin = f.get("operatingMarginTTM")
            if op_margin is not None:
                oi_for_roic = rev * op_margin / 100
        if oi_for_roic is not None:
            t = (tax_rate / 100) if tax_rate is not None else 0.21  # 默认 21% 美国企业税
            nopat = oi_for_roic * (1 - t)
            invested = _get_invested_capital_from_bs(sym)
            if invested is None and mc and total_debt is not None:
                # fallback to old method
                invested = mc + (total_debt or 0) - (total_cash or 0)
            if invested and invested > 0:
                roic = round(nopat / invested * 100, 2)

        # PEG
        peg = None
        fpe = f.get("forwardPE")
        eg = f.get("earningsGrowthQuarterlyYoy")
        if fpe and eg and eg > 0:
            peg = round(fpe / eg, 2)

        comp = {
            "symbol": sym,
            "revenue": round(rev / 1e6, 1) if rev else None,
            "revenue_growth": f.get("revenueGrowthQuarterlyYoy"),
            "gross_margin": f.get("grossMarginTTM"),
            "ebitda_margin": ebitda_margin,
            "operating_margin": f.get("operatingMarginTTM"),
            "ev_to_revenue": ev_to_rev,
            "ev_to_ebitda": ev_to_ebitda,
            "forward_pe": f.get("forwardPE"),
            "trailing_pe": f.get("trailingPE"),
            "peg": peg,
            "fcf_yield": fcf_yield,
            "roic": roic,
            "beta": f.get("beta"),
            "market_cap": round(mc / 1e9, 2) if mc else None,
        }
        companies.append(comp)

    # 统计行
    stat_fields = [
        "revenue_growth", "gross_margin", "ebitda_margin", "operating_margin",
        "ev_to_revenue", "ev_to_ebitda", "forward_pe", "trailing_pe",
        "peg", "fcf_yield", "roic", "beta",
    ]
    stats = {}
    for stat_name, stat_fn in [
        ("median", statistics.median),
        ("p25", lambda vals: sorted(vals)[max(0, len(vals) // 4 - 1)] if vals else None),
        ("p75", lambda vals: sorted(vals)[min(len(vals) - 1, len(vals) * 3 // 4)] if vals else None),
        ("min", min),
        ("max", max),
    ]:
        row = {}
        for field in stat_fields:
            vals = [c[field] for c in companies if c[field] is not None]
            if vals:
                try:
                    row[field] = round(stat_fn(vals), 2)
                except Exception:
                    row[field] = None
            else:
                row[field] = None
        stats[stat_name] = row

    return {"companies": companies, "statistics": stats}


def compute_professional_dcf(
    sym: str,
    fundamentals: dict | None = None,
    historical: dict | None = None,
    macro: dict | None = None,
    dcf_overrides: dict | None = None,
) -> dict:
    """专业 DCF 估值 — 5 年 FCF 投射 + WACC + Terminal Value + 三情景。

    Args:
        sym: 标的
        fundamentals: yf_fundamentals() 输出（避免重复拉取）
        historical: yf_historical_financials() 输出
        macro: 宏观数据 dict（含 10y 国债利率）

    Returns:
        {
            "symbol": str,
            "wacc": float,          # 加权平均资本成本 (%)
            "scenarios": {
                "bear": {"revenue_cagr": float, "terminal_margin": float, "ev": float, "equity_value": float, "per_share": float},
                "base": {...},
                "bull": {...},
            },
            "current_price": float,
            "upside": {"bear": float, "base": float, "bull": float},  # vs 当前价 (%)
            "sensitivity": [[float]],  # WACC vs Terminal Growth 矩阵
            "sensitivity_labels": {"wacc": [float], "tgr": [float]},
            "terminal_value_pct": float,  # 终值占 EV 比例 (%)
            "assumptions": {...},
        }
    """
    import math

    def _clamp(v, lo, hi):
        return max(lo, min(hi, v))

    if fundamentals is None:
        try:
            fundamentals = yf_fundamentals(sym)
        except Exception:
            fundamentals = {}
    if historical is None:
        try:
            historical = yf_historical_financials(sym)
        except Exception:
            historical = {"annual": []}
    if macro is None:
        macro = {}

    result = {"symbol": sym, "error": None}

    # --- 提取基础数据 ---
    rev = fundamentals.get("totalRevenue")
    ebit = fundamentals.get("operatingIncome")
    beta = fundamentals.get("beta", 1.0)
    mc = fundamentals.get("marketCap")
    total_debt = fundamentals.get("totalDebt", 0) or 0
    total_cash = fundamentals.get("totalCash", 0) or 0
    shares = fundamentals.get("sharesOutstanding")
    tax_rate_pct = fundamentals.get("effectiveTaxRate")  # 已百分比化
    ebitda_ltm = fundamentals.get("ebitda")
    da = None
    capex = None
    hist_rev_for_ratio = None  # 用于计算 D&A/CapEx 比率的配对 revenue

    # 从历史数据提取最新年 D&A 和 CapEx
    annuals = historical.get("annual", [])
    if annuals:
        latest = annuals[-1]
        da = latest.get("depreciation")
        capex = latest.get("capex")
        hist_rev_for_ratio = latest.get("revenue")  # D&A/CapEx 对应的同期 revenue

        # 当 operatingIncome 不可用时，优先从 LTM EBITDA 反推
        if ebit is None and ebitda_ltm is not None and da is not None:
            ebit = ebitda_ltm - abs(da)  # LTM EBIT ≈ LTM EBITDA - D&A
        elif ebit is None:
            ebit = latest.get("ebit")
            # 如果用了历史 EBIT 但 rev 是 LTM，需修正 margin
            if ebit is not None and rev and hist_rev_for_ratio and hist_rev_for_ratio > 0:
                hist_margin = ebit / hist_rev_for_ratio
                ebit = rev * hist_margin  # 按历史 margin 比率映射到 LTM revenue

        if rev is None:
            rev = latest.get("revenue")

    if not rev or not shares or not mc:
        result["error"] = "insufficient data"
        return result

    # --- WACC via CAPM ---
    risk_free = None
    try:
        risk_free = float(macro.get("10y"))
    except (TypeError, ValueError):
        pass
    if risk_free is None:
        risk_free = 4.3  # fallback

    equity_risk_premium = 5.5  # 历史平均
    # Blume adjustment: 向 1.0 回归（行业标准做法，减少极端 beta 的 WACC 偏差）
    adjusted_beta = 0.67 * beta + 0.33 * 1.0
    # Beta 约束：避免极端 beta 把 WACC 拉到不合理区间
    adjusted_beta = _clamp(adjusted_beta, 0.6, 2.0)
    cost_of_equity = risk_free + adjusted_beta * equity_risk_premium  # CAPM
    cost_of_debt = risk_free + 1.5  # 简化：无风险 + 信用利差
    tax_rate = (tax_rate_pct / 100) if tax_rate_pct is not None else 0.21
    tax_rate = _clamp(tax_rate, 0.0, 0.35)

    # 资本结构权重
    total_capital = mc + total_debt
    w_equity = mc / total_capital if total_capital > 0 else 1.0
    w_debt = total_debt / total_capital if total_capital > 0 else 0.0

    wacc = w_equity * cost_of_equity + w_debt * cost_of_debt * (1 - tax_rate)
    wacc = round(wacc, 2)

    # --- 历史利润率分析 ---
    hist_ebit_margins = []
    hist_rev_growths = []
    for a in annuals:
        r = a.get("revenue")
        e = a.get("ebit")
        if r and e and r > 0:
            hist_ebit_margins.append(e / r)
    for i in range(1, len(annuals)):
        prev_r = annuals[i - 1].get("revenue")
        cur_r = annuals[i].get("revenue")
        if prev_r and cur_r and prev_r > 0:
            hist_rev_growths.append((cur_r / prev_r) - 1)

    current_ebit_margin = ebit / rev if ebit is not None and rev and rev > 0 else 0.15
    current_ebit_margin = _clamp(current_ebit_margin, -0.10, 0.45)

    # --- 收入增速确定（多源融合） ---
    # 优先级: config override > quarterly annualization > YoY > historical median
    import statistics as _st

    _override_growth = None
    _override_margin = None
    if dcf_overrides and isinstance(dcf_overrides, dict):
        ovr = dcf_overrides.get(sym, {})
        if isinstance(ovr, dict):
            g = ovr.get("revenue_growth_pct")
            if g is not None:
                _override_growth = float(g) / 100
            m = ovr.get("terminal_margin_pct")
            if m is not None:
                _override_margin = float(m) / 100

    # 从季度数据推算 forward annual growth（比单季 YoY 更稳健）
    quarterly_annualized_growth = None
    quarters = historical.get("quarterly", []) if historical else []
    if len(quarters) >= 2:
        # 取最近 4 季收入总和 vs 前 4 季（TTM vs prior TTM）
        # 如果不足 4 季，用最近 2 季 annualize
        sorted_q = sorted(quarters, key=lambda x: x.get("date", ""))
        recent_revs = [q.get("revenue") for q in sorted_q if q.get("revenue") and q["revenue"] > 0]
        if len(recent_revs) >= 4:
            ttm = sum(recent_revs[-4:])
            prior_ttm = sum(recent_revs[-8:-4]) if len(recent_revs) >= 8 else None
            if prior_ttm and prior_ttm > 0:
                quarterly_annualized_growth = (ttm / prior_ttm) - 1
        if quarterly_annualized_growth is None and len(recent_revs) >= 2:
            # Fallback: 最近 2 季 QoQ annualized
            qoq = recent_revs[-1] / recent_revs[-2] - 1
            quarterly_annualized_growth = (1 + qoq) ** 4 - 1  # QoQ → annualized

    # 最终 base_growth 选择
    if _override_growth is not None:
        base_growth = _override_growth
        _growth_source = f"config_override({_override_growth*100:.1f}%)"
    elif quarterly_annualized_growth is not None:
        # 用 quarterly annualized 和 YoY 的加权平均（如果都有的话）
        rev_growth_yoy = fundamentals.get("revenueGrowthQuarterlyYoy")
        if rev_growth_yoy is not None:
            yoy_ratio = rev_growth_yoy / 100
            # 加权: quarterly annualized 60%, YoY 40%（quarterly 更前瞻）
            base_growth = quarterly_annualized_growth * 0.6 + yoy_ratio * 0.4
            _growth_source = f"blended(qtr_ann={quarterly_annualized_growth*100:.1f}%,yoy={yoy_ratio*100:.1f}%)"
        else:
            base_growth = quarterly_annualized_growth
            _growth_source = f"quarterly_annualized({quarterly_annualized_growth*100:.1f}%)"
    else:
        rev_growth_yoy = fundamentals.get("revenueGrowthQuarterlyYoy")
        if rev_growth_yoy is not None:
            base_growth = rev_growth_yoy / 100
            _growth_source = f"yoy({rev_growth_yoy:.1f}%)"
        elif hist_rev_growths:
            base_growth = _st.median(hist_rev_growths)
            _growth_source = f"hist_median({base_growth*100:.1f}%)"
        else:
            base_growth = 0.15
            _growth_source = "default(15%)" 

    # D&A 和 CapEx 占收入比（用同期 revenue 计算，避免跨期稀释）
    ratio_rev = hist_rev_for_ratio if hist_rev_for_ratio and hist_rev_for_ratio > 0 else rev
    da_pct = abs(da) / ratio_rev if da is not None and ratio_rev > 0 else 0.03
    capex_pct = abs(capex) / ratio_rev if capex is not None and ratio_rev > 0 else 0.05
    nwc_pct = 0.02  # NWC 变动占收入比（简化假设）

    # --- 三情景定义 ---
    # 对超高增速做正常化：5年 CAGR 分层封顶
    # >200% 增速的企业处于爆发初期，5年 CAGR 可以更高（但仍衰减）
    if base_growth >= 2.0:
        normalized_growth = min(base_growth, 1.20)  # 超爆发：封顶 120%
    elif base_growth > 1.0:
        normalized_growth = min(base_growth, 0.90)  # 高增长：封顶 90%
    else:
        normalized_growth = min(base_growth, 0.60)  # 正常增长：封顶 60%
    normalized_growth = max(normalized_growth, -0.30)

    # 终值利润率稳健化：确保熊<=基<=牛，避免场景倒挂
    base_margin = _clamp(current_ebit_margin, 0.02, 0.35)
    bear_margin = _clamp(base_margin - 0.03, 0.01, base_margin)
    bull_margin = _clamp(base_margin + 0.04, base_margin, 0.45)

    # 如果有 margin override，覆盖三情景的 terminal_margin
    if _override_margin is not None:
        base_margin = _clamp(_override_margin, 0.02, 0.45)
        bear_margin = _clamp(base_margin - 0.03, 0.01, base_margin)
        bull_margin = _clamp(base_margin + 0.04, base_margin, 0.50)

    scenarios_def = {
        "bear": {
            "revenue_cagr": max(normalized_growth * 0.5, 0.03),  # 增速减半，最低 3%
            "terminal_margin": bear_margin,  # 利润率收缩
            "margin_path": "contracting",
        },
        "base": {
            "revenue_cagr": normalized_growth * 0.8,  # 增速温和下降
            "terminal_margin": base_margin,  # 利润率维持
            "margin_path": "stable",
        },
        "bull": {
            "revenue_cagr": normalized_growth * 1.2,  # 增速加速（封顶已在 normalized_growth 层做过）
            "terminal_margin": bull_margin,  # 利润率扩张
            "margin_path": "expanding",
        },
    }

    terminal_growth_rate = 0.025  # 终值增长率 2.5%（锚定名义 GDP）

    def _project_dcf(rev_cagr, terminal_margin, projection_years=5):
        """投射 FCF 并折现。"""
        projected_revs = []
        projected_ufcf = []
        r = rev
        margin = current_ebit_margin

        # 利润率线性过渡到终值利润率
        margin_step = (terminal_margin - margin) / projection_years

        for yr in range(1, projection_years + 1):
            # 收入增速逐年向终值增长率收敛，允许负增长公司先收缩后企稳
            decay = 1 - yr / (projection_years + 1)
            growth = rev_cagr * decay + terminal_growth_rate * (1 - decay)
            growth = max(growth, -0.60)
            r = r * (1 + growth)
            margin = margin + margin_step

            ebit_proj = r * margin
            nopat = ebit_proj * (1 - tax_rate)
            da_proj = r * da_pct
            capex_proj = r * capex_pct
            nwc_change = r * nwc_pct * growth  # NWC 随增长变化

            ufcf = nopat + da_proj - capex_proj - nwc_change
            projected_revs.append(r)
            projected_ufcf.append(ufcf)

        # 终值 — Perpetuity Growth Method
        terminal_fcf = projected_ufcf[-1] * (1 + terminal_growth_rate)
        if wacc / 100 <= terminal_growth_rate:
            tv_perpetuity = terminal_fcf / 0.01  # 防止除零
        else:
            tv_perpetuity = terminal_fcf / (wacc / 100 - terminal_growth_rate)

        # 终值 — Exit Multiple Method (EV/EBITDA)
        terminal_ebitda = projected_revs[-1] * terminal_margin + projected_revs[-1] * da_pct
        exit_multiple_raw = fundamentals.get("enterpriseToEbitda")
        try:
            exit_multiple = float(exit_multiple_raw)
        except (TypeError, ValueError):
            exit_multiple = 15.0
        if exit_multiple <= 0:
            exit_multiple = 15.0
        # 高增长标的允许更高 exit multiple（但仍有合理上限）
        exit_cap = 30.0
        if base_growth >= 1.0:
            exit_cap = 50.0  # 100%+ 增速标的允许 50x exit
        elif base_growth >= 0.5:
            exit_cap = 40.0  # 50%+ 增速标的允许 40x exit
        exit_multiple = _clamp(exit_multiple, 6.0, exit_cap)
        tv_exit = terminal_ebitda * exit_multiple

        # 取两种方法均值
        terminal_value = (tv_perpetuity + tv_exit) / 2

        # 折现
        discount_rate = wacc / 100
        pv_fcf = sum(
            ufcf / (1 + discount_rate) ** yr
            for yr, ufcf in enumerate(projected_ufcf, 1)
        )
        pv_tv = terminal_value / (1 + discount_rate) ** projection_years

        enterprise_value = pv_fcf + pv_tv
        equity_value = enterprise_value - total_debt + total_cash
        per_share = equity_value / shares if shares > 0 else 0

        tv_pct = (pv_tv / enterprise_value * 100) if enterprise_value > 0 else 0

        return {
            "enterprise_value": round(enterprise_value / 1e9, 2),
            "equity_value": round(equity_value / 1e9, 2),
            "per_share": round(per_share, 2),
            "terminal_value_pct": round(tv_pct, 1),
            "revenue_cagr": round(rev_cagr * 100, 1),
            "terminal_margin": round(terminal_margin * 100, 1),
        }

    # --- 执行三情景 ---
    scenarios = {}
    for name, params in scenarios_def.items():
        scenarios[name] = _project_dcf(params["revenue_cagr"], params["terminal_margin"])

    # --- 当前价格和上行空间 ---
    try:
        q = yf_quote(sym)
        current_price = q["price"] if q else None
    except Exception:
        current_price = None

    upside = {}
    if current_price and current_price > 0:
        for name, sc in scenarios.items():
            upside[name] = round((sc["per_share"] / current_price - 1) * 100, 1)

    # 顺序一致性检查：熊<=基<=牛，否则标记警告
    bear_ps = scenarios.get("bear", {}).get("per_share")
    base_ps = scenarios.get("base", {}).get("per_share")
    bull_ps = scenarios.get("bull", {}).get("per_share")
    if all(isinstance(x, (int, float)) for x in [bear_ps, base_ps, bull_ps]):
        if not (bear_ps <= base_ps <= bull_ps):
            result["scenario_warning"] = (
                f"⚠️ 情景顺序异常 bear/base/bull={bear_ps:.2f}/{base_ps:.2f}/{bull_ps:.2f}，"
                "建议复核增长率或利润率假设。"
            )

    # --- 敏感性矩阵: WACC vs Terminal Growth ---
    wacc_range = [_clamp(wacc + d, 4.0, 20.0) for d in (-2, -1, 0, 1, 2)]
    tgr_range = [1.5, 2.0, 2.5, 3.0, 3.5]
    sensitivity = []
    base_params = scenarios_def["base"]

    for w in wacc_range:
        row = []
        for tgr in tgr_range:
            # 简化: 用 base 情景参数，调整 WACC 和终值增长率
            r = rev
            margin = current_ebit_margin
            margin_step = (base_params["terminal_margin"] - margin) / 5
            projected_ufcf = []
            projected_revs = []

            for yr in range(1, 6):
                decay = 1 - yr / 6
                growth = base_params["revenue_cagr"] * decay + (tgr / 100) * (1 - decay)
                growth = max(growth, -0.60)
                r = r * (1 + growth)
                margin = margin + margin_step
                ebit_proj = r * margin
                nopat = ebit_proj * (1 - tax_rate)
                da_proj = r * da_pct
                capex_proj = r * capex_pct
                nwc_change = r * nwc_pct * growth
                ufcf = nopat + da_proj - capex_proj - nwc_change
                projected_ufcf.append(ufcf)
                projected_revs.append(r)

            terminal_fcf = projected_ufcf[-1] * (1 + tgr / 100)
            disc = w / 100
            if disc <= tgr / 100:
                tv_perp = terminal_fcf / 0.01
            else:
                tv_perp = terminal_fcf / (disc - tgr / 100)

            # Exit Multiple 法（与主模型一致）
            term_ebitda = projected_revs[-1] * base_params["terminal_margin"] + projected_revs[-1] * da_pct
            exit_mult_raw = fundamentals.get("enterpriseToEbitda")
            try:
                exit_mult = float(exit_mult_raw)
            except (TypeError, ValueError):
                exit_mult = 15.0
            if exit_mult <= 0:
                exit_mult = 15.0
            # 与主模型一致的动态 exit cap
            _exit_cap = 30.0
            if base_growth >= 1.0:
                _exit_cap = 50.0
            elif base_growth >= 0.5:
                _exit_cap = 40.0
            exit_mult = _clamp(exit_mult, 6.0, _exit_cap)
            tv_exit = term_ebitda * exit_mult
            tv = (tv_perp + tv_exit) / 2

            pv_fcf = sum(u / (1 + disc) ** yr for yr, u in enumerate(projected_ufcf, 1))
            pv_tv = tv / (1 + disc) ** 5
            ev = pv_fcf + pv_tv
            eq = ev - total_debt + total_cash
            ps = eq / shares if shares > 0 else 0
            row.append(round(ps, 2))
        sensitivity.append(row)

    result.update({
        "wacc": wacc,
        "cost_of_equity": round(cost_of_equity, 2),
        "cost_of_debt": round(cost_of_debt * (1 - tax_rate), 2),
        "scenarios": scenarios,
        "current_price": current_price,
        "upside": upside,
        "sensitivity": sensitivity,
        "sensitivity_labels": {
            "wacc": [round(w, 1) for w in wacc_range],
            "tgr": tgr_range,
        },
        "terminal_value_pct": scenarios.get("base", {}).get("terminal_value_pct"),
        "assumptions": {
            "risk_free": risk_free,
            "erp": equity_risk_premium,
            "beta_raw": beta,
            "beta_adjusted": round(adjusted_beta, 2),
            "tax_rate": round(tax_rate * 100, 1),
            "da_pct_rev": round(da_pct * 100, 1),
            "capex_pct_rev": round(capex_pct * 100, 1),
            "terminal_growth": terminal_growth_rate * 100,
            "projection_years": 5,
            "base_growth_raw": round(base_growth * 100, 1),
            "growth_source": _growth_source,
            "override_applied": _override_growth is not None or _override_margin is not None,
        },
    })

    # --- Sanity Check: DCF vs 分析师共识 ---
    # 如果 base case 估值与分析师目标价偏差 >60%，标记警告
    base_ps = result.get("scenarios", {}).get("base", {}).get("per_share")
    current = result.get("current_price")
    if isinstance(base_ps, (int, float)) and isinstance(current, (int, float)) and current > 0:
        if base_ps <= 0:
            result["sanity_warning"] = (
                f"⚠️ DCF base ({base_ps:.0f}) ≤ 0，模型输入可能异常（负终值或现金流假设过弱）。"
            )
        else:
            # 简单检查：base case 应该在合理范围内
            ratio = base_ps / current
            if ratio < 0.3:  # DCF base < 当前价的 30%
                result["sanity_warning"] = (
                    f"⚠️ DCF base ({base_ps:.0f}) 远低于当前价 ({current:.0f})，"
                    f"可能 WACC 过高或增速假设过保守。请检查 beta/增速封顶。"
                )
            elif ratio > 5.0:  # DCF base > 当前价的 5 倍
                result["sanity_warning"] = (
                    f"⚠️ DCF base ({base_ps:.0f}) 远高于当前价 ({current:.0f})，"
                    f"可能增速假设过激进。请检查 revenue_cagr。"
                )

    return result


def get_exchange_rate():
    """动态获取 USD/HKD 汇率（yfinance）"""
    try:
        for ticker in ["HKD=X", "USDHKD=X", "HKDUSD=X"]:
            data = yf_fetch(ticker)
            rate = data["info"].get("regularMarketPrice")
            if rate is None:
                continue
            try:
                rate = float(rate)
            except (TypeError, ValueError):
                continue
            if rate <= 0:
                continue
            # 容错：若拿到 HKD/USD（约 0.12），反转为 USD/HKD（约 7.8）
            if rate < 1:
                rate = 1.0 / rate
            if 5.0 <= rate <= 10.0:
                return rate
    except Exception:
        pass
    return 7.8



# ---------------------------------------------------------------------------
# FRED — 直接调用公共 CSV 接口，无需 API key
# ---------------------------------------------------------------------------

FRED_CSV_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv"

FRED_KEY_SERIES: dict[str, dict[str, str]] = {
    # GDP & Output
    "GDP": {"name": "Gross Domestic Product (Nominal)", "category": "GDP & Output", "freq": "Quarterly"},
    "GDPC1": {"name": "Real GDP", "category": "GDP & Output", "freq": "Quarterly"},
    "A191RL1Q225SBEA": {"name": "Real GDP Growth Rate (QoQ Annualized)", "category": "GDP & Output", "freq": "Quarterly"},
    # Inflation
    "CPIAUCSL": {"name": "CPI - All Urban Consumers", "category": "Inflation", "freq": "Monthly"},
    "CPILFESL": {"name": "Core CPI (ex Food & Energy)", "category": "Inflation", "freq": "Monthly"},
    "PCEPI": {"name": "PCE Price Index", "category": "Inflation", "freq": "Monthly"},
    "PCEPILFE": {"name": "Core PCE Price Index", "category": "Inflation", "freq": "Monthly"},
    "PPIFIS": {"name": "PPI - Final Demand", "category": "Inflation", "freq": "Monthly"},
    "T5YIE": {"name": "5-Year Breakeven Inflation Rate", "category": "Inflation", "freq": "Daily"},
    "T10YIE": {"name": "10-Year Breakeven Inflation Rate", "category": "Inflation", "freq": "Daily"},
    # Employment
    "UNRATE": {"name": "Unemployment Rate", "category": "Employment", "freq": "Monthly"},
    "PAYEMS": {"name": "Nonfarm Payrolls (Thousands)", "category": "Employment", "freq": "Monthly"},
    "ICSA": {"name": "Initial Jobless Claims (Weekly)", "category": "Employment", "freq": "Weekly"},
    "JTSJOL": {"name": "Job Openings (JOLTS, Thousands)", "category": "Employment", "freq": "Monthly"},
    "LNS12300060": {"name": "Employment-Population Ratio (25-54)", "category": "Employment", "freq": "Monthly"},
    # Interest Rates
    "FEDFUNDS": {"name": "Federal Funds Effective Rate", "category": "Interest Rates", "freq": "Monthly"},
    "DFEDTARU": {"name": "Fed Funds Target Rate Upper", "category": "Interest Rates", "freq": "Daily"},
    "DFEDTARL": {"name": "Fed Funds Target Rate Lower", "category": "Interest Rates", "freq": "Daily"},
    # Treasury Yields
    "DGS1MO": {"name": "1-Month Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "DGS3MO": {"name": "3-Month Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "DGS6MO": {"name": "6-Month Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "DGS1": {"name": "1-Year Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "DGS2": {"name": "2-Year Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "DGS3": {"name": "3-Year Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "DGS5": {"name": "5-Year Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "DGS7": {"name": "7-Year Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "DGS10": {"name": "10-Year Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "DGS20": {"name": "20-Year Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "DGS30": {"name": "30-Year Treasury Yield", "category": "Treasury Yields", "freq": "Daily"},
    "T10Y2Y": {"name": "10Y-2Y Treasury Spread (Yield Curve)", "category": "Treasury Yields", "freq": "Daily"},
    "T10Y3M": {"name": "10Y-3M Treasury Spread", "category": "Treasury Yields", "freq": "Daily"},
    # Business Conditions
    "INDPRO": {"name": "Industrial Production Index", "category": "Business Conditions", "freq": "Monthly"},
    "DGORDER": {"name": "Durable Goods Orders", "category": "Business Conditions", "freq": "Monthly"},
    "RSAFS": {"name": "Retail Sales (Millions)", "category": "Business Conditions", "freq": "Monthly"},
    "UMCSENT": {"name": "U of Michigan Consumer Sentiment", "category": "Consumer Confidence", "freq": "Monthly"},
    # Money Supply & Fed Balance Sheet
    "M2SL": {"name": "M2 Money Supply", "category": "Money & Credit", "freq": "Monthly"},
    "WALCL": {"name": "Fed Balance Sheet Total Assets", "category": "Money & Credit", "freq": "Weekly"},
    "TOTRESNS": {"name": "Total Reserves", "category": "Money & Credit", "freq": "Monthly"},
    # Housing
    "HOUST": {"name": "Housing Starts (Thousands)", "category": "Housing", "freq": "Monthly"},
    "CSUSHPINSA": {"name": "Case-Shiller Home Price Index", "category": "Housing", "freq": "Monthly"},
    "MORTGAGE30US": {"name": "30-Year Mortgage Rate", "category": "Housing", "freq": "Weekly"},
    "PERMIT": {"name": "Building Permits", "category": "Housing", "freq": "Monthly"},
    # Financial Conditions
    "VIXCLS": {"name": "CBOE VIX Volatility Index", "category": "Financial Conditions", "freq": "Daily"},
    "BAMLH0A0HYM2": {"name": "High Yield Bond Spread (OAS)", "category": "Financial Conditions", "freq": "Daily"},
    "DTWEXBGS": {"name": "Trade-Weighted US Dollar Index (Broad)", "category": "Financial Conditions", "freq": "Daily"},
    "SP500": {"name": "S&P 500 Index", "category": "Financial Conditions", "freq": "Daily"},
    "NASDAQCOM": {"name": "NASDAQ Composite", "category": "Financial Conditions", "freq": "Daily"},
    "WILL5000INDFC": {"name": "Wilshire 5000 Total Market Index", "category": "Financial Conditions", "freq": "Daily"},
    "NFCI": {"name": "Chicago Fed National Financial Conditions Index", "category": "Financial Conditions", "freq": "Weekly"},
    "STLFSI4": {"name": "St. Louis Fed Financial Stress Index", "category": "Financial Conditions", "freq": "Weekly"},
    # Commodities
    "DCOILWTICO": {"name": "WTI Crude Oil Price", "category": "Commodities", "freq": "Daily"},
    "PPIACO": {"name": "PPI All Commodities", "category": "Commodities", "freq": "Monthly"},
}


def _fred_fetch_raw(series_id, start_date=None, end_date=None, frequency=None, transformation=None):
    """Fetch a single FRED series as CSV (no API key needed)."""
    params = {"id": series_id}
    if start_date:
        params["cosd"] = start_date
    if end_date:
        params["coed"] = end_date
    if frequency:
        freq_map = {"d": "Daily", "w": "Weekly", "bw": "Biweekly", "m": "Monthly", "q": "Quarterly", "sa": "Semiannual", "a": "Annual"}
        params["fq"] = freq_map.get(frequency, frequency)
    if transformation:
        params["transformation"] = transformation

    url = f"{FRED_CSV_BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "openclaw-trade-report/1.0", "Accept-Encoding": "gzip, deflate"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            try:
                raw = gzip.decompress(raw)
            except (gzip.BadGzipFile, OSError):
                pass
            if raw[:2] == b"PK":
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    for name in zf.namelist():
                        if name.endswith(".csv"):
                            return zf.read(name).decode("utf-8", errors="replace")
                    return ""
            return raw.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"FRED CSV error {e.code}") from e


def _fred_parse_csv(csv_text):
    """Parse FRED CSV text into headers and rows."""
    reader = csv.DictReader(io.StringIO(csv_text))
    headers = reader.fieldnames or []
    rows = []
    for row in reader:
        parsed_row = {}
        for key, val in row.items():
            if key == "observation_date":
                parsed_row["date"] = val
            else:
                if val and val != ".":
                    try:
                        parsed_row[key] = float(val)
                    except ValueError:
                        parsed_row[key] = val
                else:
                    parsed_row[key] = None
        rows.append(parsed_row)
    return headers, rows


def _fred_fetch_series(series_id, start_date=None, end_date=None, frequency=None, transformation=None):
    """Fetch and parse a single FRED series, returning list of {date, value} dicts."""
    csv_text = _fred_fetch_raw(series_id, start_date, end_date, frequency, transformation)
    _, rows = _fred_parse_csv(csv_text)
    return [{"date": row.get("date"), "value": row.get(series_id)} for row in rows]


def fred_get_series(series_id, start_date=None, end_date=None, limit=120, frequency=None, transformation=None):
    """Get observations for a FRED series. No API key required."""
    sid = series_id.strip().upper()
    data = _fred_fetch_series(sid, start_date, end_date, frequency, transformation)
    data.reverse()
    if limit and len(data) > limit:
        data = data[:limit]
    series_info = FRED_KEY_SERIES.get(sid, {})
    return {
        "series_id": sid,
        "name": series_info.get("name", sid),
        "category": series_info.get("category", ""),
        "native_frequency": series_info.get("freq", ""),
        "observation_count": len(data),
        "latest": data[0] if data else None,
        "observations": data,
    }


def fred_get_multiple_series(series_ids, start_date=None, end_date=None, limit=60, frequency=None):
    """Get data for multiple FRED series at once (up to 10)."""
    ids = [s.strip().upper() for s in series_ids.split(",") if s.strip()] if isinstance(series_ids, str) else series_ids
    if not ids:
        return {"error": "No series IDs provided"}
    if len(ids) > 10:
        return {"error": "Maximum 10 series at a time"}
    results = {}
    for sid in ids:
        try:
            data = _fred_fetch_series(sid, start_date, end_date, frequency)
            data.reverse()
            non_null = [d for d in data if d["value"] is not None]
            if limit and len(data) > limit:
                data = data[:limit]
            info = FRED_KEY_SERIES.get(sid, {})
            results[sid] = {
                "name": info.get("name", sid),
                "category": info.get("category", ""),
                "latest": non_null[0] if non_null else None,
                "observation_count": len(data),
                "observations": data,
            }
        except Exception as e:
            results[sid] = {"error": str(e)}
    return {"series_count": len(results), "data": results}


def fred_macro_dashboard():
    """Get a quick macro dashboard with latest values of key US economic indicators."""
    dashboard_ids = [
        "A191RL1Q225SBEA", "CPIAUCSL", "PCEPILFE", "UNRATE", "PAYEMS",
        "FEDFUNDS", "DGS2", "DGS10", "DGS30", "T10Y2Y",
        "MORTGAGE30US", "VIXCLS", "UMCSENT", "SP500", "DCOILWTICO", "PPIACO",
    ]
    dashboard = []
    for sid in dashboard_ids:
        info = FRED_KEY_SERIES.get(sid, {})
        try:
            data = _fred_fetch_series(sid, start_date="2024-01-01")
            data.reverse()
            non_null = [d for d in data if d["value"] is not None]
            latest = non_null[0] if non_null else {}
            previous = non_null[1] if len(non_null) > 1 else {}
            current = latest.get("value")
            prev = previous.get("value")
            change = round(current - prev, 4) if current is not None and prev is not None else None
            dashboard.append({
                "series_id": sid, "name": info.get("name", sid), "category": info.get("category", ""),
                "date": latest.get("date"), "value": current, "previous_value": prev, "change": change,
            })
        except Exception:
            dashboard.append({"series_id": sid, "name": info.get("name", sid), "error": "failed to fetch"})
    return {"dashboard": dashboard, "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def fred_yield_curve():
    """Get the current US Treasury yield curve with all maturities."""
    curve_specs = [
        ("DGS1MO", "1 Month"), ("DGS3MO", "3 Month"), ("DGS6MO", "6 Month"),
        ("DGS1", "1 Year"), ("DGS2", "2 Year"), ("DGS3", "3 Year"),
        ("DGS5", "5 Year"), ("DGS7", "7 Year"), ("DGS10", "10 Year"),
        ("DGS20", "20 Year"), ("DGS30", "30 Year"),
    ]
    curve = []
    for sid, label in curve_specs:
        try:
            data = _fred_fetch_series(sid, start_date="2025-01-01")
            data.reverse()
            non_null = [d for d in data if d["value"] is not None]
            latest = non_null[0] if non_null else {}
            curve.append({"maturity": label, "series_id": sid, "yield_pct": latest.get("value"), "date": latest.get("date")})
        except Exception:
            curve.append({"maturity": label, "series_id": sid, "error": "failed"})
    spreads = {}
    for sid, name in [("T10Y2Y", "10Y-2Y Spread"), ("T10Y3M", "10Y-3M Spread")]:
        try:
            data = _fred_fetch_series(sid, start_date="2025-01-01")
            data.reverse()
            non_null = [d for d in data if d["value"] is not None]
            if non_null:
                val = non_null[0]["value"]
                spreads[name] = {"value": val, "date": non_null[0].get("date"), "inverted": val < 0 if isinstance(val, (int, float)) else None}
        except Exception:
            spreads[name] = {"error": "failed"}
    return {"yield_curve": curve, "spreads": spreads, "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def fred_inflation_monitor():
    """Get a comprehensive inflation monitoring dashboard."""
    inflation_ids = ["CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE", "PPIFIS"]
    breakeven_ids = ["T5YIE", "T10YIE"]
    result: dict[str, Any] = {"indicators": [], "breakevens": []}
    for sid in inflation_ids:
        info = FRED_KEY_SERIES.get(sid, {})
        try:
            data = _fred_fetch_series(sid, start_date="2022-01-01")
            data.reverse()
            non_null = [d for d in data if d["value"] is not None]
            latest = non_null[0] if non_null else {}
            yoy_change = None
            if len(non_null) > 12:
                current = non_null[0].get("value")
                year_ago = non_null[12].get("value")
                if current and year_ago and year_ago != 0:
                    yoy_change = round(((current / year_ago) - 1) * 100, 2)
            result["indicators"].append({
                "series_id": sid, "name": info.get("name", sid),
                "date": latest.get("date"), "value": latest.get("value"), "yoy_pct_change": yoy_change,
            })
        except Exception:
            result["indicators"].append({"series_id": sid, "name": info.get("name", sid), "error": "failed"})
    for sid in breakeven_ids:
        info = FRED_KEY_SERIES.get(sid, {})
        try:
            data = _fred_fetch_series(sid, start_date="2024-01-01")
            data.reverse()
            non_null = [d for d in data if d["value"] is not None]
            latest = non_null[0] if non_null else {}
            result["breakevens"].append({
                "series_id": sid, "name": info.get("name", sid),
                "date": latest.get("date"), "value_pct": latest.get("value"),
            })
        except Exception:
            result["breakevens"].append({"series_id": sid, "name": info.get("name", sid), "error": "failed"})
    result["generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return result


# ---------------------------------------------------------------------------
# SEC EDGAR — 简化版，纯 HTTP 获取最近 filings 列表
# ---------------------------------------------------------------------------

SEC_USER_AGENT = (
    os.environ.get("SEC_USER_AGENT", "").strip()
    or os.environ.get("OPENCLAW_SEC_USER_AGENT", "").strip()
    or "openclaw-research/1.0 (dev@openclaw.local)"
)


def sec_fetch_filings(ticker, form_types=None, limit=6):
    """从 SEC EDGAR 获取最近的 filings 列表（纯 HTTP，无需库依赖）。"""
    if form_types is None:
        form_types = {"10-K", "10-Q", "8-K"}
    headers = {
        "User-Agent": SEC_USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
    }
    target = ticker.upper().strip()

    # 1. 查 CIK
    req = urllib.request.Request("https://www.sec.gov/files/company_tickers.json", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            tickers_data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return []

    row_cik = None
    for value in tickers_data.values():
        if isinstance(value, dict) and str(value.get("ticker", "")).upper() == target:
            row_cik = str(value.get("cik_str", "")).strip()
            break
    if not row_cik:
        return []

    # 2. 获取 filings
    padded_cik = row_cik.zfill(10)
    req2 = urllib.request.Request(f"https://data.sec.gov/submissions/CIK{padded_cik}.json", headers=headers)
    try:
        with urllib.request.urlopen(req2, timeout=15) as resp:
            sub = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return []

    filings = sub.get("filings", {})
    recent = filings.get("recent", {})
    forms = recent.get("form", [])
    accession_numbers = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])
    filing_dates = recent.get("filingDate", [])
    row_count = min(
        len(forms),
        len(accession_numbers),
        len(primary_docs),
        len(filing_dates),
    )

    out = []
    for idx in range(row_count):
        if len(out) >= limit:
            break
        form = str(forms[idx] or "")
        if form not in form_types:
            continue
        accession = str(accession_numbers[idx] if idx < len(accession_numbers) else "").replace("-", "")
        primary_doc = str(primary_docs[idx] if idx < len(primary_docs) else "")
        filing_date = str(filing_dates[idx] if idx < len(filing_dates) else "")
        if not accession or not primary_doc:
            continue
        out.append({
            "title": f"{target} {form} {filing_date}",
            "url": f"https://www.sec.gov/Archives/edgar/data/{row_cik}/{accession}/{primary_doc}",
            "form": form,
            "filing_date": filing_date,
        })
    return out


# ---------------------------------------------------------------------------
# 健康评分 — 移植自 trading-mcp HealthScoreCalculator
# ---------------------------------------------------------------------------

_HEALTH_WEIGHTS = {
    "profitability": 0.3,
    "liquidity": 0.2,
    "leverage": 0.2,
    "efficiency": 0.15,
    "growth": 0.15,
}


def _pct_val(v):
    """yfinance 比率 → 百分比数值（0.667 → 66.7）；已是百分比或 None 则原样。"""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return round(f * 100, 2) if abs(f) < 5 else round(f, 2)


def compute_health_score(sym: str) -> dict | None:
    """
    计算财务健康评分（0-100）。数据来自 yfinance，评分逻辑移植自 trading-mcp。
    返回 dict: {overall_score, rating, component_scores, details}
    """
    info = yf_fetch(sym).get("info", {})
    if not info:
        return None

    # --- 提取原始数据 ---
    profit_margin = _pct_val(info.get("profitMargins"))
    roe = _pct_val(info.get("returnOnEquity"))
    current_ratio_val = info.get("currentRatio")
    debt_to_equity_val = info.get("debtToEquity")
    pe_val = info.get("forwardPE") or info.get("trailingPE")
    eps_growth = _pct_val(info.get("earningsGrowth"))

    details: dict[str, Any] = {}

    # --- Profitability ---
    prof_score = 50.0
    if profit_margin is not None:
        prof_score = min(100, max(0, 50 + profit_margin * 2))
        details["profit_margin"] = profit_margin
    if roe is not None:
        roe_score = min(100, max(0, 50 + roe * 3))
        prof_score = (prof_score + roe_score) / 2
        details["return_on_equity"] = roe

    # --- Liquidity ---
    liq_score = 50.0
    if current_ratio_val is not None:
        try:
            cr = float(current_ratio_val)
            if 1.5 <= cr <= 3:
                liq_score = 90
            elif 1 <= cr < 1.5:
                liq_score = 70
            elif cr > 3:
                liq_score = 60
            else:
                liq_score = 20
            details["current_ratio"] = cr
        except (TypeError, ValueError):
            pass

    # --- Leverage ---
    lev_score = 50.0
    if debt_to_equity_val is not None:
        try:
            de = float(debt_to_equity_val)
            if de <= 0.3:
                lev_score = 90
            elif de <= 0.6:
                lev_score = 70
            elif de <= 1.0:
                lev_score = 50
            else:
                lev_score = max(10, 50 - (de - 1) * 20)
            details["debt_to_equity"] = de
        except (TypeError, ValueError):
            pass

    # --- Efficiency (PE) ---
    eff_score = 50.0
    if pe_val is not None:
        try:
            pe = float(pe_val)
            if pe > 0:
                if 10 <= pe <= 20:
                    eff_score = 80
                elif 5 <= pe < 10:
                    eff_score = 70
                elif 20 < pe <= 30:
                    eff_score = 60
                elif pe > 30:
                    eff_score = max(20, 60 - (pe - 30))
                else:
                    eff_score = 30
            details["price_to_earnings"] = pe
        except (TypeError, ValueError):
            pass

    # --- Growth ---
    grw_score = 50.0
    if eps_growth is not None:
        grw_score = min(100, max(0, 50 + eps_growth))
        details["eps_growth"] = eps_growth

    # --- Overall ---
    w = _HEALTH_WEIGHTS
    overall = round(
        prof_score * w["profitability"]
        + liq_score * w["liquidity"]
        + lev_score * w["leverage"]
        + eff_score * w["efficiency"]
        + grw_score * w["growth"]
    )

    if overall >= 80:
        rating = "Excellent"
    elif overall >= 70:
        rating = "Good"
    elif overall >= 60:
        rating = "Fair"
    elif overall >= 40:
        rating = "Below Average"
    else:
        rating = "Poor"

    return {
        "overall_score": overall,
        "rating": rating,
        "component_scores": {
            "profitability": round(prof_score),
            "liquidity": round(liq_score),
            "leverage": round(lev_score),
            "efficiency": round(eff_score),
            "growth": round(grw_score),
        },
        "details": details,
    }


# ---------------------------------------------------------------------------
# Put/Call Ratio — 移植自 trading-mcp BarchartAdapter
# ---------------------------------------------------------------------------

import re
try:
    from html.parser import HTMLParser
except ImportError:
    HTMLParser = None  # type: ignore


def fetch_put_call_ratio(ticker: str) -> dict | None:
    """
    从 Barchart 获取 Put/Call Ratio。纯 urllib + 正则解析，无额外依赖。
    返回与原 trading-mcp put_call_analysis 兼容的 dict。
    """
    url = (
        f"https://www.barchart.com/stocks/quotes/{ticker.upper()}"
        f"/put-call-ratios?orderBy=expirationDate&orderDir=desc"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
            try:
                raw = gzip.decompress(raw)
            except (gzip.BadGzipFile, OSError):
                pass
            html = raw.decode("utf-8", errors="replace")
    except Exception:
        return None

    return _parse_barchart_put_call(html, ticker)


def _parse_number(s: str) -> float:
    if not s or not isinstance(s, str):
        return 0.0
    cleaned = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_barchart_put_call(html: str, ticker: str) -> dict:
    """解析 Barchart HTML 提取 put/call ratio 数据。"""
    totals = {
        "putVolume": 0.0, "callVolume": 0.0,
        "putOI": 0.0, "callOI": 0.0,
        "volumeRatio": 0.0, "oiRatio": 0.0,
    }

    # 正则提取 totals — 值可能在 <strong> 标签内或冒号后
    patterns = {
        "putVolume": r"Put Volume Total[\s<>/\w\"=]*?>\s*([0-9,]+)",
        "callVolume": r"Call Volume Total[\s<>/\w\"=]*?>\s*([0-9,]+)",
        "volumeRatio": r"Put/Call Volume Ratio[\s<>/\w\"=]*?>\s*([0-9.]+)",
        "putOI": r"Put Open Interest Total[\s<>/\w\"=]*?>\s*([0-9,]+)",
        "callOI": r"Call Open Interest Total[\s<>/\w\"=]*?>\s*([0-9,]+)",
        "oiRatio": r"Put/Call Open Interest Ratio[\s<>/\w\"=]*?>\s*([0-9.]+)",
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            totals[key] = _parse_number(m.group(1))

    # 计算比率（如果页面没直接提供）
    if totals["volumeRatio"] == 0 and totals["callVolume"] > 0:
        totals["volumeRatio"] = totals["putVolume"] / totals["callVolume"]
    if totals["oiRatio"] == 0 and totals["callOI"] > 0:
        totals["oiRatio"] = totals["putOI"] / totals["callOI"]

    # 情绪判断
    vr = totals["volumeRatio"]
    oir = totals["oiRatio"]
    if vr > 1.2:
        sentiment = "bearish"
        interpretation = "High put/call volume ratio suggests bearish sentiment"
    elif vr < 0.8:
        sentiment = "bullish"
        interpretation = "Low put/call volume ratio suggests bullish sentiment"
    else:
        sentiment = "neutral"
        interpretation = "Put/call volume ratio is within normal range"

    key_insights = [f"Put/call volume ratio of {vr:.2f}"]
    if oir > 1.5:
        key_insights.append(f"High put/call OI ratio of {oir:.2f} suggests hedging activity")
    elif 0 < oir < 0.5:
        key_insights.append(f"Low put/call OI ratio of {oir:.2f} suggests bullish positioning")

    has_data = (
        totals["putVolume"] > 0 or totals["callVolume"] > 0
        or totals["putOI"] > 0 or totals["callOI"] > 0
    )

    return {
        "ticker": ticker.upper(),
        "overallPutCallVolumeRatio": round(totals["volumeRatio"], 4),
        "overallPutCallOIRatio": round(totals["oiRatio"], 4),
        "totalPutVolume": totals["putVolume"],
        "totalCallVolume": totals["callVolume"],
        "totalPutOI": totals["putOI"],
        "totalCallOI": totals["callOI"],
        "analysis": {
            "sentiment": sentiment,
            "interpretation": interpretation,
            "keyInsights": key_insights,
        },
        "validationResult": {
            "isValid": has_data,
            "warnings": [] if has_data else ["No put/call data found"],
        },
    }


# ---------------------------------------------------------------------------
# 估值 — 从 yfinance 合成 Finviz 格式字段
# ---------------------------------------------------------------------------


def _calc_rsi14(sym: str) -> float | None:
    """计算 14 日 RSI。"""
    try:
        t = yf.Ticker(sym)
        hist = t.history(period="1mo")
        if hist is None or len(hist) < 15:
            return None
        closes = hist["Close"].values
        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        # Wilder 平滑
        avg_gain = sum(gains[:14]) / 14
        avg_loss = sum(losses[:14]) / 14
        for i in range(14, len(gains)):
            avg_gain = (avg_gain * 13 + gains[i]) / 14
            avg_loss = (avg_loss * 13 + losses[i]) / 14
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - 100 / (1 + rs), 2)
    except Exception:
        return None


def fetch_valuations(symbols: list[str]) -> dict[str, dict]:
    """
    从 yfinance 数据合成与 trading-mcp compare_stock_valuations 兼容的估值字段。
    返回 {sym: {peg, forwardPE, rsi14, sma200, insiderOwn, profitMargin, ...}, ...}
    """
    result: dict[str, dict] = {}
    for sym in symbols:
        info = yf_fetch(sym).get("info", {})
        if not info:
            continue

        entry: dict[str, Any] = {}

        # peg
        peg = info.get("pegRatio")
        if peg is not None:
            entry["peg"] = peg

        # forwardPE
        fpe = info.get("forwardPE")
        if fpe is not None:
            entry["forwardPE"] = fpe

        # rsi14 — 从价格历史计算
        rsi = _calc_rsi14(sym)
        if rsi is not None:
            entry["rsi14"] = rsi

        # sma200 — 距离百分比
        sma200 = info.get("twoHundredDayAverage")
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        if sma200 and price and sma200 > 0:
            pct = round((price / sma200 - 1) * 100, 2)
            entry["sma200"] = f"{pct:+.2f}%"

        # insiderOwn
        insider = info.get("heldPercentInsiders")
        if insider is not None:
            entry["insiderOwn"] = f"{insider * 100:.2f}%"

        # profitMargin
        pm = info.get("profitMargins")
        if pm is not None:
            entry["profitMargin"] = f"{pm * 100:.2f}%"

        # returnOnEquity
        roe = info.get("returnOnEquity")
        if roe is not None:
            entry["returnOnEquity"] = f"{roe * 100:.2f}%"

        # debtToEquity
        de = info.get("debtToEquity")
        if de is not None:
            entry["debtToEquity"] = de

        if entry:
            result[sym] = entry

    return result


# ---------------------------------------------------------------------------
# Technical Indicators — 纯本地计算，无额外 API 调用
# ---------------------------------------------------------------------------

def _ema(data: list, period: int) -> list:
    """计算 EMA，返回与输入同长的列表（前 period-1 个为 None）。"""
    n = len(data)
    if n < period:
        return [None] * n
    k = 2 / (period + 1)
    result = [None] * (period - 1)
    result.append(sum(data[:period]) / period)
    for i in range(period, n):
        result.append(data[i] * k + result[-1] * (1 - k))
    return result


def compute_technical_indicators(sym: str) -> dict | None:
    """
    计算技术指标：MACD, EMA, 布林带, SMA50, ATR14, 成交量异动, 趋势判断。
    数据来自 yfinance 历史价格，纯本地计算，无额外外部依赖。
    """
    try:
        cached = yf_fetch(sym)
        t = cached.get("ticker")
        if t is None:
            return None
        _stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            hist = t.history(period="6mo")
        finally:
            sys.stderr = _stderr
        if hist is None or len(hist) < 26:
            return None

        closes = hist["Close"].values.tolist()
        highs = hist["High"].values.tolist()
        lows = hist["Low"].values.tolist()
        volumes = hist["Volume"].values.tolist()
        n = len(closes)
        result = {}

        # --- SMA50 ---
        if n >= 50:
            sma50 = sum(closes[-50:]) / 50
            result["sma50"] = round(sma50, 2)
            result["sma50_dist_pct"] = round((closes[-1] / sma50 - 1) * 100, 2)

        # --- EMA12, EMA26 ---
        ema12 = _ema(closes, 12)
        ema26 = _ema(closes, 26)
        if ema12[-1] is not None:
            result["ema12"] = round(ema12[-1], 2)
        if ema26[-1] is not None:
            result["ema26"] = round(ema26[-1], 2)

        # --- MACD (EMA12 - EMA26, Signal = EMA9 of MACD) ---
        macd_line = []
        for i in range(n):
            if ema12[i] is not None and ema26[i] is not None:
                macd_line.append(ema12[i] - ema26[i])
        if len(macd_line) >= 9:
            signal = _ema(macd_line, 9)
            vs = [s for s in signal if s is not None]
            if len(vs) >= 2:
                result["macd"] = round(macd_line[-1], 4)
                result["macd_signal"] = round(vs[-1], 4)
                result["macd_histogram"] = round(macd_line[-1] - vs[-1], 4)
                if len(macd_line) >= 2:
                    cur = macd_line[-1] - vs[-1]
                    prev = macd_line[-2] - vs[-2]
                    if cur > 0 and prev <= 0:
                        result["macd_cross"] = "bullish"
                    elif cur < 0 and prev >= 0:
                        result["macd_cross"] = "bearish"

        # --- Bollinger Bands (SMA20 ± 2σ) ---
        if n >= 20:
            sma20 = sum(closes[-20:]) / 20
            variance = sum((c - sma20) ** 2 for c in closes[-20:]) / 20
            std20 = variance ** 0.5
            upper = sma20 + 2 * std20
            lower = sma20 - 2 * std20
            result["bollinger"] = {
                "upper": round(upper, 2),
                "middle": round(sma20, 2),
                "lower": round(lower, 2),
                "width_pct": round((upper - lower) / sma20 * 100, 2) if sma20 > 0 else 0,
                "position": round((closes[-1] - lower) / (upper - lower) * 100, 1) if upper != lower else 50,
            }

        # --- RSI14 (Wilder smoothing) ---
        if n >= 15:
            deltas = [closes[i] - closes[i - 1] for i in range(1, n)]
            gains = [d if d > 0 else 0 for d in deltas]
            losses = [-d if d < 0 else 0 for d in deltas]
            avg_gain = sum(gains[:14]) / 14
            avg_loss = sum(losses[:14]) / 14
            for i in range(14, len(gains)):
                avg_gain = (avg_gain * 13 + gains[i]) / 14
                avg_loss = (avg_loss * 13 + losses[i]) / 14
            if avg_loss == 0:
                result["rsi14"] = 100.0
            else:
                rs = avg_gain / avg_loss
                result["rsi14"] = round(100 - 100 / (1 + rs), 2)

        # --- ATR14 ---
        if n >= 15:
            trs = []
            for i in range(1, n):
                tr = max(
                    highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]),
                )
                trs.append(tr)
            if len(trs) >= 14:
                atr = sum(trs[-14:]) / 14
                result["atr14"] = round(atr, 2)
                result["atr14_pct"] = round(atr / closes[-1] * 100, 2) if closes[-1] > 0 else 0

        # --- Volume Analysis ---
        if n >= 20:
            vol_sma20 = sum(volumes[-20:]) / 20
            result["volume"] = {
                "current": int(volumes[-1]),
                "avg_20d": int(vol_sma20),
                "ratio": round(volumes[-1] / vol_sma20, 2) if vol_sma20 > 0 else None,
                "anomaly": bool(volumes[-1] > vol_sma20 * 2),
            }

        # --- Momentum (Rate of Change) ---
        if n >= 5:
            result["roc_5d"] = round((closes[-1] / closes[-5] - 1) * 100, 2)
        if n >= 20:
            result["roc_20d"] = round((closes[-1] / closes[-20] - 1) * 100, 2)

        # --- Support / Resistance (近 20 日高低) ---
        if n >= 20:
            result["support_20d"] = round(min(lows[-20:]), 2)
            result["resistance_20d"] = round(max(highs[-20:]), 2)

        # --- Trend Detection (SMA20 vs SMA50) ---
        if n >= 50:
            sma20_val = sum(closes[-20:]) / 20
            sma50_val = sum(closes[-50:]) / 50
            if closes[-1] > sma20_val > sma50_val:
                result["trend"] = "bullish"
            elif closes[-1] < sma20_val < sma50_val:
                result["trend"] = "bearish"
            else:
                result["trend"] = "mixed"

        return result
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Global Indices — 全球主要指数行情（yfinance 批量下载，单次调用）
# ---------------------------------------------------------------------------

GLOBAL_INDEX_MAP = {
    "^GSPC": "S&P 500",
    "^DJI": "Dow Jones",
    "^IXIC": "NASDAQ",
    "^RUT": "Russell 2000",
    "^HSI": "恒生指数",
    "^N225": "日经225",
    "^GDAXI": "德国DAX",
    "^FTSE": "英国FTSE100",
    "000001.SS": "上证指数",
    "DX-Y.NYB": "美元指数DXY",
    "GC=F": "黄金",
    "CL=F": "原油WTI",
    "BTC-USD": "比特币",
}


def yf_global_indices() -> dict:
    """获取全球主要指数/商品行情（yfinance 批量下载，单次 API 调用）。"""
    tickers = list(GLOBAL_INDEX_MAP.keys())
    results = {}
    _stderr = sys.stderr
    try:
        sys.stderr = io.StringIO()
        df = yf.download(tickers, period="5d", progress=False, group_by="ticker")
        sys.stderr = _stderr
        if df is None or df.empty:
            return results
        for ticker in tickers:
            try:
                # group_by="ticker" → columns = (Ticker, OHLCV)
                if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
                    close_data = df[ticker]["Close"].dropna()
                else:
                    close_data = df["Close"].dropna()
                if len(close_data) < 1:
                    continue
                price = float(close_data.iloc[-1])
                prev = float(close_data.iloc[-2]) if len(close_data) >= 2 else None
                change_pct = round((price - prev) / prev * 100, 2) if prev and prev > 0 else None
                results[ticker] = {
                    "name": GLOBAL_INDEX_MAP[ticker],
                    "price": round(price, 2),
                    "change_pct": change_pct,
                }
            except Exception:
                continue
    except Exception:
        sys.stderr = _stderr
    return results


# ---------------------------------------------------------------------------
# FRED Financial Conditions — 金融环境指数（FRED CSV，无 key 无限流）
# ---------------------------------------------------------------------------

def fred_financial_conditions() -> dict:
    """获取金融环境指数 (NFCI, STLFSI4)。正值=收紧，负值=宽松。"""
    series_map = {
        "NFCI": "Chicago Fed National Financial Conditions Index",
        "STLFSI4": "St. Louis Fed Financial Stress Index",
    }
    results = {}
    for sid, name in series_map.items():
        try:
            data = fred_get_series(sid, limit=5)
            if isinstance(data, dict) and data.get("latest"):
                latest = data["latest"]
                val = latest.get("value")
                if val is not None:
                    results[sid] = {
                        "name": name,
                        "value": val,
                        "date": latest.get("date"),
                        "interpretation": "tightening" if val > 0 else "accommodative",
                    }
        except Exception:
            continue
    return results


# ---------------------------------------------------------------------------
# ETF Fund Flows — 资金流向估算（Chaikin Money Flow 代理）
# ---------------------------------------------------------------------------

def compute_etf_fund_flows(etfs: list[str] | None = None) -> dict:
    """
    从价量分析估算 ETF 资金流向（Chaikin Money Flow 代理）。
    yfinance 批量下载，单次 API 调用。
    """
    if etfs is None:
        etfs = ["SPY", "QQQ", "IWM", "SMH", "XLF", "XLE", "XLK", "TLT", "HYG", "GLD"]
    results = {}
    _stderr = sys.stderr
    try:
        sys.stderr = io.StringIO()
        df = yf.download(etfs, period="1mo", progress=False, group_by="ticker")
        sys.stderr = _stderr
        if df is None or df.empty:
            return results
        multi = hasattr(df.columns, "nlevels") and df.columns.nlevels > 1
        for sym in etfs:
            try:
                if multi:
                    c = df[sym]["Close"].dropna().values.tolist()
                    h = df[sym]["High"].dropna().values.tolist()
                    lo = df[sym]["Low"].dropna().values.tolist()
                    v = df[sym]["Volume"].dropna().values.tolist()
                else:
                    c = df["Close"].dropna().values.tolist()
                    h = df["High"].dropna().values.tolist()
                    lo = df["Low"].dropna().values.tolist()
                    v = df["Volume"].dropna().values.tolist()
                n = len(c)
                if n < 10:
                    continue
                # Chaikin Money Flow
                mfv_5d = 0.0
                vol_5d = 0.0
                mfv_20d = 0.0
                vol_20d = 0.0
                for i in range(max(0, n - 20), n):
                    hl = h[i] - lo[i]
                    mfm = ((c[i] - lo[i]) - (h[i] - c[i])) / hl if hl > 0 else 0
                    mfv_20d += mfm * v[i]
                    vol_20d += v[i]
                    if i >= n - 5:
                        mfv_5d += mfm * v[i]
                        vol_5d += v[i]
                cmf_5d = mfv_5d / vol_5d if vol_5d > 0 else 0
                cmf_20d = mfv_20d / vol_20d if vol_20d > 0 else 0
                vol_avg = sum(v[-20:]) / min(20, n)
                vol_recent = sum(v[-5:]) / min(5, n)
                results[sym] = {
                    "cmf_5d": round(cmf_5d, 4),
                    "cmf_20d": round(cmf_20d, 4),
                    "flow_direction": "inflow" if cmf_5d > 0.05 else "outflow" if cmf_5d < -0.05 else "neutral",
                    "volume_trend": round(vol_recent / vol_avg, 2) if vol_avg > 0 else None,
                    "price_5d_pct": round((c[-1] / c[-5] - 1) * 100, 2) if n >= 5 else None,
                    "price_20d_pct": round((c[-1] / c[0] - 1) * 100, 2) if n >= 2 else None,
                }
            except Exception:
                continue
    except Exception:
        sys.stderr = _stderr
    return results


# ---------------------------------------------------------------------------
# Fed Rate Expectations — 从收益率曲线推算（无额外 API 调用）
# ---------------------------------------------------------------------------

def compute_fed_rate_expectations(macro: dict) -> dict | None:
    """从国债收益率曲线推算联储利率预期方向。无额外外部调用。"""
    fed_funds = macro.get("fed_funds")
    y2 = macro.get("2y")
    y10 = macro.get("10y")
    y30 = macro.get("30y")
    if fed_funds is None or y2 is None:
        return None
    try:
        fed_funds = float(fed_funds)
        y2 = float(y2)
    except (TypeError, ValueError):
        return None
    spread = round(y2 - fed_funds, 3)
    implied_cuts = round(-spread / 0.25)
    result = {
        "fed_funds_current": fed_funds,
        "treasury_2y": y2,
        "spread_2y_vs_ffr": spread,
        "implied_25bp_moves": implied_cuts,
        "direction": "easing" if spread < -0.15 else "tightening" if spread > 0.15 else "on_hold",
    }
    if y10 is not None:
        try:
            result["term_premium_10y2y"] = round(float(y10) - y2, 3)
        except (TypeError, ValueError):
            pass
    if y30 is not None:
        try:
            result["term_premium_30y2y"] = round(float(y30) - y2, 3)
        except (TypeError, ValueError):
            pass
    return result


# ---------------------------------------------------------------------------
# CFTC Commitment of Traders — 期货仓位数据（免费 CSV 下载，无限流）
# ---------------------------------------------------------------------------

_CFTC_FIN_CONTRACTS = {
    "E-MINI S&P 500 -": "ES",
    "NASDAQ MINI -": "NQ",
    "VIX FUTURES": "VX",
    "UST BOND -": "US_BOND",
    "UST 10Y NOTE": "10Y_NOTE",
    "UST 5Y NOTE": "5Y_NOTE",
    "UST 2Y NOTE": "2Y_NOTE",
    "USD INDEX": "DXY",
    "FED FUNDS -": "FFR",
}

_CFTC_COMM_CONTRACTS = {
    "GOLD - COMMODITY": "GOLD",
    "CRUDE OIL, LIGHT SWEET": "WTI",
    "SILVER - COMMODITY": "SILVER",
    "COPPER -": "COPPER",
}


def _cot_int(row, *names):
    """从 CFTC CSV 行中提取整数值，尝试多个列名变体。"""
    for name in names:
        v = row.get(name)
        if v is not None and str(v).strip():
            try:
                return int(str(v).strip().replace(",", ""))
            except (ValueError, TypeError):
                continue
    return 0


def _cftc_fetch_zip(url):
    """下载 CFTC ZIP 文件并返回 CSV 文本。"""
    req = urllib.request.Request(url, headers={
        "User-Agent": "openclaw-trade-report/1.0",
        "Accept-Encoding": "gzip, deflate",
    })
    with urllib.request.urlopen(req, timeout=45) as resp:
        zip_data = resp.read()
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        txt_name = next((n for n in zf.namelist() if n.endswith(".txt")), None)
        if not txt_name:
            return None
        return zf.read(txt_name).decode("utf-8", errors="replace")


def _cftc_parse_financial(csv_text, contracts):
    """解析 TFF 格式（金融期货）— Dealer/AssetMgr/LevMoney 分类。"""
    reader = csv.DictReader(io.StringIO(csv_text))
    by_contract: dict[str, dict] = {}

    for row in reader:
        market = str(row.get("Market_and_Exchange_Names", "")).strip()
        market_upper = market.upper()

        # 排除 MICRO 合约
        if "MICRO" in market_upper:
            continue

        matched_key = None
        for pattern, key in contracts.items():
            if pattern.upper() in market_upper:
                matched_key = key
                break
        if not matched_key:
            continue

        report_date = str(row.get("Report_Date_as_YYYY-MM-DD", "")).strip()
        existing = by_contract.get(matched_key)
        if existing and existing.get("_date", "") >= report_date:
            continue

        lm_long = _cot_int(row, "Lev_Money_Positions_Long_All")
        lm_short = _cot_int(row, "Lev_Money_Positions_Short_All")
        am_long = _cot_int(row, "Asset_Mgr_Positions_Long_All")
        am_short = _cot_int(row, "Asset_Mgr_Positions_Short_All")
        dealer_long = _cot_int(row, "Dealer_Positions_Long_All")
        dealer_short = _cot_int(row, "Dealer_Positions_Short_All")
        oi = _cot_int(row, "Open_Interest_All")
        oi_chg = _cot_int(row, "Change_in_Open_Interest_All")
        lm_long_chg = _cot_int(row, "Change_in_Lev_Money_Long_All")
        lm_short_chg = _cot_int(row, "Change_in_Lev_Money_Short_All")

        net_lev = lm_long - lm_short
        net_am = am_long - am_short
        net_dealer = dealer_long - dealer_short

        by_contract[matched_key] = {
            "market": market,
            "_date": report_date,
            "report_date": report_date,
            "open_interest": oi,
            "oi_change": oi_chg,
            "net_leveraged_money": net_lev,
            "net_asset_manager": net_am,
            "net_dealer": net_dealer,
            "lev_money_long": lm_long,
            "lev_money_short": lm_short,
            "lev_money_net_change": lm_long_chg - lm_short_chg,
            "positioning": "net_long" if net_lev > 0 else "net_short",
        }

    results = {}
    for k, v in by_contract.items():
        v.pop("_date", None)
        results[k] = v
    return results


def _cftc_parse_legacy(csv_text, contracts):
    """解析 Legacy 格式（商品期货）— NonComm/Comm 分类。"""
    reader = csv.DictReader(io.StringIO(csv_text))
    by_contract: dict[str, dict] = {}

    for row in reader:
        market = str(row.get("Market and Exchange Names", "")).strip()
        market_upper = market.upper()

        if "MICRO" in market_upper:
            continue

        matched_key = None
        for pattern, key in contracts.items():
            if pattern.upper() in market_upper:
                matched_key = key
                break
        if not matched_key:
            continue

        report_date = str(row.get("As of Date in Form YYYY-MM-DD", "")).strip()
        existing = by_contract.get(matched_key)
        if existing and existing.get("_date", "") >= report_date:
            continue

        nc_long = _cot_int(row, "Noncommercial Positions-Long (All)")
        nc_short = _cot_int(row, "Noncommercial Positions-Short (All)")
        comm_long = _cot_int(row, "Commercial Positions-Long (All)")
        comm_short = _cot_int(row, "Commercial Positions-Short (All)")
        oi = _cot_int(row, "Open Interest (All)")

        net_spec = nc_long - nc_short
        net_comm = comm_long - comm_short

        by_contract[matched_key] = {
            "market": market,
            "_date": report_date,
            "report_date": report_date,
            "open_interest": oi,
            "net_speculative": net_spec,
            "net_commercial": net_comm,
            "spec_long": nc_long,
            "spec_short": nc_short,
            "positioning": "net_long" if net_spec > 0 else "net_short",
        }

    results = {}
    for k, v in by_contract.items():
        v.pop("_date", None)
        results[k] = v
    return results


def cftc_cot_report() -> dict:
    """
    获取 CFTC Commitment of Traders 最新仓位数据。
    金融期货: TFF 格式 (Lev Money / Asset Mgr / Dealer)
    商品期货: Legacy 格式 (NonComm / Comm)
    从 CFTC 官网下载年度 CSV 压缩包（免费，无 API key，无限流）。
    """
    year = datetime.utcnow().year
    results = {}

    # 1. 金融期货 (TFF 格式)
    for yr in [year, year - 1]:
        url = f"https://www.cftc.gov/files/dea/history/fut_fin_txt_{yr}.zip"
        try:
            csv_text = _cftc_fetch_zip(url)
            if csv_text:
                fin = _cftc_parse_financial(csv_text, _CFTC_FIN_CONTRACTS)
                results.update(fin)
                break
        except Exception:
            continue

    # 2. 商品期货 (Legacy 格式)
    for yr in [year, year - 1]:
        url = f"https://www.cftc.gov/files/dea/history/deacot{yr}.zip"
        try:
            csv_text = _cftc_fetch_zip(url)
            if csv_text:
                comm = _cftc_parse_legacy(csv_text, _CFTC_COMM_CONTRACTS)
                results.update(comm)
                break
        except Exception:
            continue

    return results


# ---------------------------------------------------------------------------
# Module 1: Options Chain Deep Data
# ---------------------------------------------------------------------------

def yf_options_deep(sym: str) -> dict | None:
    """Deep options chain analysis: P/C OI ratio, skew, max pain, unusual activity.

    Returns:
        {
            "put_call_oi_ratio": float,
            "skew_25d": float,          # OTM put IV - OTM call IV (approx 25-delta)
            "max_pain": float,          # strike with minimum total option pain
            "unusual_activity": [...],  # strikes where volume > 3x OI
            "oi_by_strike": {strike: {"call_oi": int, "put_oi": int}},
        }
    """
    try:
        cached = yf_fetch(sym)
        t = cached.get("ticker")
        if t is None:
            return None

        info = cached.get("info", {})
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        if not price or price <= 0:
            return None

        _stderr = sys.stderr
        try:
            sys.stderr = io.StringIO()
            expirations = t.options
        except Exception:
            return None
        finally:
            sys.stderr = _stderr

        if not expirations:
            return None

        # Use up to first 3 near-term expirations
        use_exps = expirations[:min(3, len(expirations))]

        total_call_oi = 0
        total_put_oi = 0
        all_call_rows = []
        all_put_rows = []

        for exp in use_exps:
            try:
                sys.stderr = io.StringIO()
                chain = t.option_chain(exp)
            except Exception:
                continue
            finally:
                sys.stderr = _stderr

            calls = chain.calls
            puts = chain.puts
            if calls is None or calls.empty or puts is None or puts.empty:
                continue

            for _, row in calls.iterrows():
                oi = _safe_val(row.get("openInterest")) or 0
                vol = _safe_val(row.get("volume")) or 0
                iv = _safe_val(row.get("impliedVolatility"))
                strike = _safe_val(row.get("strike"))
                lp = _safe_val(row.get("lastPrice")) or 0
                try:
                    oi = int(oi)
                    vol = int(vol)
                except (TypeError, ValueError):
                    oi, vol = 0, 0
                total_call_oi += oi
                all_call_rows.append({
                    "strike": float(strike) if strike else 0,
                    "oi": oi, "volume": vol,
                    "iv": float(iv) if iv else None,
                    "lastPrice": float(lp),
                    "exp": exp,
                })

            for _, row in puts.iterrows():
                oi = _safe_val(row.get("openInterest")) or 0
                vol = _safe_val(row.get("volume")) or 0
                iv = _safe_val(row.get("impliedVolatility"))
                strike = _safe_val(row.get("strike"))
                lp = _safe_val(row.get("lastPrice")) or 0
                try:
                    oi = int(oi)
                    vol = int(vol)
                except (TypeError, ValueError):
                    oi, vol = 0, 0
                total_put_oi += oi
                all_put_rows.append({
                    "strike": float(strike) if strike else 0,
                    "oi": oi, "volume": vol,
                    "iv": float(iv) if iv else None,
                    "lastPrice": float(lp),
                    "exp": exp,
                })

        if not all_call_rows and not all_put_rows:
            return None

        result = {}

        # 1. Put/Call OI Ratio
        if total_call_oi > 0:
            result["put_call_oi_ratio"] = round(total_put_oi / total_call_oi, 3)
        else:
            result["put_call_oi_ratio"] = None

        # 2. Skew (25-delta approximation): compare OTM put IV vs OTM call IV ~5% from ATM
        target_put_strike = price * 0.95
        target_call_strike = price * 1.05
        otm_put_iv = None
        otm_call_iv = None
        # Find closest OTM put (strike < price, ~5% below)
        best_put_dist = float("inf")
        for r in all_put_rows:
            if r["iv"] is not None and r["strike"] < price:
                dist = abs(r["strike"] - target_put_strike)
                if dist < best_put_dist:
                    best_put_dist = dist
                    otm_put_iv = r["iv"]
        # Find closest OTM call (strike > price, ~5% above)
        best_call_dist = float("inf")
        for r in all_call_rows:
            if r["iv"] is not None and r["strike"] > price:
                dist = abs(r["strike"] - target_call_strike)
                if dist < best_call_dist:
                    best_call_dist = dist
                    otm_call_iv = r["iv"]
        if otm_put_iv is not None and otm_call_iv is not None:
            result["skew_25d"] = round((otm_put_iv - otm_call_iv) * 100, 2)  # in IV % points
        else:
            result["skew_25d"] = None

        # 3. Max Pain: strike where total ITM option value is minimized
        all_strikes = sorted(set(r["strike"] for r in all_call_rows + all_put_rows if r["strike"] > 0))
        if all_strikes:
            min_pain = float("inf")
            max_pain_strike = all_strikes[0]
            for test_strike in all_strikes:
                pain = 0.0
                # Call pain: for each call with strike < test_strike, holders gain
                for r in all_call_rows:
                    if r["strike"] < test_strike:
                        pain += (test_strike - r["strike"]) * r["oi"]
                # Put pain: for each put with strike > test_strike, holders gain
                for r in all_put_rows:
                    if r["strike"] > test_strike:
                        pain += (r["strike"] - test_strike) * r["oi"]
                if pain < min_pain:
                    min_pain = pain
                    max_pain_strike = test_strike
            result["max_pain"] = max_pain_strike
        else:
            result["max_pain"] = None

        # 4. Unusual Activity: volume > 3x open interest
        unusual = []
        for r in all_call_rows:
            if r["oi"] > 0 and r["volume"] > 3 * r["oi"]:
                unusual.append({
                    "strike": r["strike"],
                    "side": "call",
                    "volume": r["volume"],
                    "open_interest": r["oi"],
                    "ratio": round(r["volume"] / r["oi"], 1),
                    "expiration": r["exp"],
                })
        for r in all_put_rows:
            if r["oi"] > 0 and r["volume"] > 3 * r["oi"]:
                unusual.append({
                    "strike": r["strike"],
                    "side": "put",
                    "volume": r["volume"],
                    "open_interest": r["oi"],
                    "ratio": round(r["volume"] / r["oi"], 1),
                    "expiration": r["exp"],
                })
        # Sort by volume descending, keep top 10
        unusual.sort(key=lambda x: x["volume"], reverse=True)
        result["unusual_activity"] = unusual[:10]

        # 5. OI by strike: top 10 strikes by total OI
        strike_oi = {}
        for r in all_call_rows:
            s = r["strike"]
            if s not in strike_oi:
                strike_oi[s] = {"call_oi": 0, "put_oi": 0}
            strike_oi[s]["call_oi"] += r["oi"]
        for r in all_put_rows:
            s = r["strike"]
            if s not in strike_oi:
                strike_oi[s] = {"call_oi": 0, "put_oi": 0}
            strike_oi[s]["put_oi"] += r["oi"]
        # Sort by total OI, take top 10
        sorted_strikes = sorted(
            strike_oi.items(),
            key=lambda x: x[1]["call_oi"] + x[1]["put_oi"],
            reverse=True,
        )[:10]
        result["oi_by_strike"] = {str(s): v for s, v in sorted_strikes}

        return result
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Module 2: SEC EDGAR Content Parsing
# ---------------------------------------------------------------------------

def sec_filing_highlights(ticker: str) -> dict | None:
    """Fetch most recent 10-Q and extract key highlights via regex parsing.

    Returns:
        {
            "filing_date": str,
            "form_type": str,
            "url": str,
            "revenue_segments": [str],
            "risk_factors_excerpt": str,
            "guidance_excerpt": str,
        }
    """
    import re as _re

    try:
        filings = sec_fetch_filings(ticker, form_types={"10-Q", "10-K"}, limit=3)
        if not filings:
            return None

        # Pick the most recent 10-Q; fallback to 10-K
        target = None
        for f in filings:
            if f.get("form") == "10-Q":
                target = f
                break
        if target is None:
            target = filings[0]

        url = target.get("url", "")
        if not url:
            return None

        result = {
            "filing_date": target.get("filing_date"),
            "form_type": target.get("form"),
            "url": url,
            "revenue_segments": [],
            "risk_factors_excerpt": None,
            "guidance_excerpt": None,
        }

        # Fetch HTML content
        headers = {
            "User-Agent": SEC_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*",
        }
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw_bytes = resp.read(2 * 1024 * 1024)  # cap at 2MB
                html = raw_bytes.decode("utf-8", errors="replace")
        except Exception:
            return result  # return what we have (metadata only)

        # Strip HTML tags for text analysis
        text = _re.sub(r"<[^>]+>", " ", html)
        text = _re.sub(r"&nbsp;", " ", text)
        text = _re.sub(r"&amp;", "&", text)
        text = _re.sub(r"&lt;", "<", text)
        text = _re.sub(r"&gt;", ">", text)
        text = _re.sub(r"\s+", " ", text)

        # --- Revenue segments ---
        rev_patterns = [
            _re.compile(
                r"(?:revenue|net revenue|net sales)[^.]{0,80}?(?:segment|product|service|subscription|cloud|license|hardware|software|advertising|professional)[^.]{0,120}?\$[\d,]+",
                _re.IGNORECASE,
            ),
            _re.compile(
                r"(?:segment|product line|business unit)[^.]{0,60}?(?:revenue|sales)[^.]{0,80}?\$[\d,]+",
                _re.IGNORECASE,
            ),
        ]
        segments = set()
        for pat in rev_patterns:
            for m in pat.finditer(text):
                seg_text = m.group(0).strip()
                if len(seg_text) > 20:
                    segments.add(seg_text[:200])
        result["revenue_segments"] = list(segments)[:8]

        # --- Risk Factors ---
        risk_match = _re.search(
            r"(?:Item\s*1A[.\s]*)?Risk\s+Factors(.{100,2000}?)(?:Item\s*1B|Item\s*2|ITEM\s*2|Unresolved\s+Staff)",
            text,
            _re.IGNORECASE | _re.DOTALL,
        )
        if risk_match:
            excerpt = risk_match.group(1).strip()
            result["risk_factors_excerpt"] = excerpt[:500]
        else:
            risk_match2 = _re.search(r"Risk\s+Factors(.{100,600})", text, _re.IGNORECASE | _re.DOTALL)
            if risk_match2:
                result["risk_factors_excerpt"] = risk_match2.group(1).strip()[:500]

        # --- Management Guidance ---
        guidance_patterns = [
            _re.compile(
                r"(?:outlook|guidance|expect|anticipate|project|forecast)[^.]*?(?:\$[\d,.]+|[\d,.]+\s*(?:billion|million|percent|%))[^.]*\.",
                _re.IGNORECASE,
            ),
        ]
        guidance_quotes = []
        for pat in guidance_patterns:
            for m in pat.finditer(text):
                g = m.group(0).strip()
                if 30 < len(g) < 300:
                    guidance_quotes.append(g)
        if guidance_quotes:
            result["guidance_excerpt"] = " | ".join(guidance_quotes[:3])[:500]

        return result
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Module 3: Institutional Flow Tracking
# ---------------------------------------------------------------------------

def institutional_flow(sym: str) -> dict | None:
    """Combine yfinance institutional data with SEC 13G/13D filings.

    Returns:
        {
            "top_holders": [{"holder": str, "shares": int, "pct_held": float, ...}],
            "recent_13g_13d_filings": [{"title": str, "filing_date": str, "url": str, "form": str}],
            "major_holder_breakdown": {"insiders_pct": float, "institutions_pct": float, "float_pct": float},
        }
    """
    try:
        def _normalize_percent(v):
            """Normalize percent-like values to 0-100 numeric scale."""
            if v is None:
                return None
            if isinstance(v, str):
                s = v.strip()
                if not s:
                    return None
                had_pct_sign = "%" in s
                s = s.replace("%", "").replace(",", "")
                try:
                    num = float(s)
                except (TypeError, ValueError):
                    return None
                if had_pct_sign:
                    return round(num, 2)
                v = num
            try:
                num = float(v)
            except (TypeError, ValueError):
                return None
            if abs(num) <= 1:
                num *= 100
            return round(num, 2)

        def _normalize_int(v):
            if v is None:
                return None
            try:
                return int(float(v))
            except (TypeError, ValueError):
                return None

        result = {}

        # 1. Top institutional holders from yfinance
        cached = yf_fetch(sym)
        t = cached.get("ticker")
        if t is not None:
            _stderr = sys.stderr
            try:
                sys.stderr = io.StringIO()
                inst = getattr(t, "institutional_holders", None)
                if inst is not None and hasattr(inst, "empty") and not inst.empty:
                    holders = []
                    for _, row in inst.head(10).iterrows():
                        holder = {}
                        for col in inst.columns:
                            v = _safe_val(row[col])
                            if hasattr(v, "isoformat"):
                                v = v.isoformat()
                            elif hasattr(v, "to_pydatetime"):
                                v = str(v)
                            holder[col] = v
                        holders.append(holder)
                    result["top_holders"] = holders
            except Exception:
                pass
            finally:
                sys.stderr = _stderr

            # Major holder breakdown
            try:
                sys.stderr = io.StringIO()
                major = getattr(t, "major_holders", None)
                if major is not None and hasattr(major, "empty") and not major.empty:
                    breakdown = {}
                    for idx in major.index:
                        label = str(idx).lower()
                        try:
                            val = major.loc[idx].iloc[0] if len(major.columns) > 0 else major.loc[idx]
                            val = _safe_val(val)
                        except Exception:
                            val = None

                        # Common yfinance labels:
                        # insidersPercentHeld / institutionsPercentHeld /
                        # institutionsFloatPercentHeld / institutionsCount
                        if "institution" in label and "count" in label:
                            cnt = _normalize_int(val)
                            if cnt is not None:
                                breakdown["institutions_count"] = cnt
                            continue
                        if "insider" in label:
                            pct = _normalize_percent(val)
                            if pct is not None:
                                breakdown["insiders_pct"] = pct
                            continue
                        if "float" in label and "institution" in label:
                            pct = _normalize_percent(val)
                            if pct is not None:
                                breakdown["float_pct"] = pct
                            continue
                        if "institution" in label:
                            pct = _normalize_percent(val)
                            if pct is not None:
                                breakdown["institutions_pct"] = pct
                    if breakdown:
                        result["major_holder_breakdown"] = breakdown
            except Exception:
                pass
            finally:
                sys.stderr = _stderr

        # 2. Recent SC 13G/13D filings from SEC EDGAR
        try:
            sec_filings = sec_fetch_filings(
                sym,
                form_types={"SC 13G", "SC 13G/A", "SC 13D", "SC 13D/A"},
                limit=5,
            )
            if sec_filings:
                filings_payload = [
                    {
                        "title": f.get("title", ""),
                        "filing_date": f.get("filing_date", ""),
                        "url": f.get("url", ""),
                        "form": f.get("form", ""),
                    }
                    for f in sec_filings
                ]
                result["recent_13g_13d_filings"] = filings_payload
                # Backward compatibility for existing callers.
                result["recent_13f_filings"] = filings_payload
        except Exception:
            pass

        return result if result else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Module 4: Finnhub Integration (free API)
# ---------------------------------------------------------------------------

_FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()


def _finnhub_get(endpoint: str, params: dict) -> dict | list | None:
    """Helper: make a Finnhub API GET request. Returns parsed JSON or None."""
    if not _FINNHUB_API_KEY:
        return None
    try:
        params["token"] = _FINNHUB_API_KEY
        qs = urllib.parse.urlencode(params)
        url = f"https://finnhub.io/api/v1/{endpoint}?{qs}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return None


def finnhub_insider_sentiment(sym: str) -> dict | None:
    """Monthly insider sentiment (MSPR) from Finnhub for last 6 months.

    Returns:
        {
            "symbol": str,
            "data": [
                {"year": int, "month": int, "change": float, "mspr": float}
            ]
        }
    """
    try:
        if not _FINNHUB_API_KEY:
            return None
        today = datetime.utcnow()
        six_mo_ago = today - timedelta(days=180)
        raw = _finnhub_get("stock/insider-sentiment", {
            "symbol": sym.upper(),
            "from": six_mo_ago.strftime("%Y-%m-%d"),
            "to": today.strftime("%Y-%m-%d"),
        })
        if not raw or not isinstance(raw, dict):
            return None
        data = raw.get("data", [])
        if not data:
            return None
        rows = []
        for d in data:
            rows.append({
                "year": d.get("year"),
                "month": d.get("month"),
                "change": _safe_val(d.get("change")),
                "mspr": _safe_val(d.get("mspr")),
            })
        return {"symbol": sym.upper(), "data": rows}
    except Exception:
        return None


def finnhub_earnings_surprises(sym: str) -> dict | None:
    """Last 8 quarters of actual vs estimate EPS from Finnhub.

    Returns:
        {
            "symbol": str,
            "data": [
                {"period": str, "actual": float, "estimate": float,
                 "surprise": float, "surprise_pct": float}
            ]
        }
    """
    try:
        if not _FINNHUB_API_KEY:
            return None
        raw = _finnhub_get("stock/earnings", {
            "symbol": sym.upper(),
            "limit": 8,
        })
        if not raw or not isinstance(raw, list):
            return None
        rows = []
        for d in raw:
            actual = _safe_val(d.get("actual"))
            estimate = _safe_val(d.get("estimate"))
            surprise = _safe_val(d.get("surprise"))
            surprise_pct = _safe_val(d.get("surprisePercent"))
            rows.append({
                "period": d.get("period"),
                "actual": actual,
                "estimate": estimate,
                "surprise": surprise,
                "surprise_pct": surprise_pct,
            })
        return {"symbol": sym.upper(), "data": rows} if rows else None
    except Exception:
        return None


def finnhub_recommendation_trends(sym: str) -> dict | None:
    """Monthly analyst buy/sell/hold recommendation trends from Finnhub.

    Returns:
        {
            "symbol": str,
            "data": [
                {"period": str, "strongBuy": int, "buy": int, "hold": int,
                 "sell": int, "strongSell": int}
            ]
        }
    """
    try:
        if not _FINNHUB_API_KEY:
            return None
        raw = _finnhub_get("stock/recommendation", {
            "symbol": sym.upper(),
        })
        if not raw or not isinstance(raw, list):
            return None
        rows = []
        for d in raw[:6]:  # last 6 months
            rows.append({
                "period": d.get("period"),
                "strongBuy": d.get("strongBuy", 0),
                "buy": d.get("buy", 0),
                "hold": d.get("hold", 0),
                "sell": d.get("sell", 0),
                "strongSell": d.get("strongSell", 0),
            })
        return {"symbol": sym.upper(), "data": rows} if rows else None
    except Exception:
        return None
