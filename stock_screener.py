#!/usr/bin/env python3
"""
A股策略选股器 - 从全A股中按策略条件筛选标的
依赖：efinance, akshare, pandas（已包含在 requirements.txt 中）
"""

import os, sys, time, logging
from datetime import datetime, timedelta
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-5s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("screener")

# 配置参数（通过环境变量设置）
MIN_MARKET_CAP = float(os.getenv("SCREEN_MIN_MARKET_CAP", "20"))
MAX_MARKET_CAP = float(os.getenv("SCREEN_MAX_MARKET_CAP", "500"))
MA_PERIOD = int(os.getenv("SCREEN_MA_PERIOD", "60"))
MA_BULLISH = os.getenv("SCREEN_MA_BULLISH", "true").lower() in ("true", "1", "yes")
CHIP_CHECK = os.getenv("SCREEN_CHIP_CHECK", "true").lower() in ("true", "1", "yes")
MIN_PRICE = float(os.getenv("SCREEN_MIN_PRICE", "5"))
MAX_PE = float(os.getenv("SCREEN_MAX_PE", "200"))
VOLUME_SHRINK_RATIO = float(os.getenv("SCREEN_VOLUME_SHRINK", "0.8"))
OUTPUT_LIMIT = int(os.getenv("SCREEN_OUTPUT_LIMIT", "30"))
SORT_BY = os.getenv("SCREEN_SORT_BY", "market_cap")


def get_all_stocks():
    """获取全A股实时行情"""
    logger.info("正在获取全A股实时行情...")
    try:
        import efinance as ef
        df = ef.stock.get_realtime_quotes()
        logger.info(f"获取到 {len(df)} 只股票")
        col_map = {"股票代码": "code", "股票名称": "name", "最新价": "price",
                    "涨跌幅": "change_pct", "成交量": "volume", "成交额": "amount",
                    "换手率": "turnover_rate", "总市值": "total_market_cap",
                    "流通市值": "float_market_cap", "动态市盈率": "pe_ttm", "量比": "volume_ratio"}
        return df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    except Exception as e:
        logger.warning(f"efinance 失败: {e}，尝试 akshare...")
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        col_map = {"代码": "code", "名称": "name", "最新价": "price",
                    "涨跌幅": "change_pct", "换手率": "turnover_rate",
                    "总市值": "total_market_cap", "市盈率-动态": "pe_ttm", "量比": "volume_ratio"}
        return df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})


def basic_filter(df):
    """基础筛选：市值、价格、排除 ST/退市/B股/北交所"""
    initial = len(df)
    for col in ["price", "total_market_cap", "pe_ttm", "turnover_rate", "volume_ratio", "change_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["code", "price", "total_market_cap"])
    df = df[df["price"] > 0]
    if "name" in df.columns:
        df = df[~df["name"].str.contains(r"ST|退|B股", na=False)]
    df = df[~df["code"].str.match(r"^[84]\d{5}$")]
    df["market_cap_yi"] = df["total_market_cap"] / 1e8
    df = df[(df["market_cap_yi"] >= MIN_MARKET_CAP) & (df["market_cap_yi"] <= MAX_MARKET_CAP)]
    df = df[df["price"] >= MIN_PRICE]
    if MAX_PE > 0 and "pe_ttm" in df.columns:
        df = df[(df["pe_ttm"] > 0) & (df["pe_ttm"] <= MAX_PE)]
    logger.info(f"基础筛选：{initial} → {len(df)} 只")
    return df


def check_ma_bullish_batch(codes):
    """批量检查均线多头排列 MA5 > MA10 > MA20"""
    if not MA_BULLISH:
        return {c: True for c in codes}
    logger.info(f"检查均线多头排列（{len(codes)} 只）...")
    import akshare as ak
    results = {}
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=MA_PERIOD * 2)).strftime("%Y%m%d")
    for i, code in enumerate(codes):
        if (i + 1) % 100 == 0:
            logger.info(f"  进度：{i + 1}/{len(codes)}")
        try:
            df = ak.stock_zh_a_hist(symbol=code, start_date=start_date,
                                     end_date=end_date, adjust="qfq", period="daily")
            if df is None or len(df) < 20:
                results[code] = False; continue
            close_col = "收盘" if "收盘" in df.columns else "close"
            close = df[close_col].astype(float)
            ma5, ma10, ma20 = close.rolling(5).mean().iloc[-1], close.rolling(10).mean().iloc[-1], close.rolling(20).mean().iloc[-1]
            results[code] = (ma5 > ma10 > ma20)
            time.sleep(0.15)
        except Exception:
            results[code] = False; time.sleep(0.1)
    logger.info(f"均线多头：{sum(v for v in results.values())}/{len(codes)} 只通过")
    return results


def check_chip_concentration(codes, all_df):
    """筹码集中度评估（换手率低 + 缩量）"""
    if not CHIP_CHECK:
        return {c: True for c in codes}
    logger.info(f"检查筹码集中度（{len(codes)} 只）...")
    results = {}
    subset = all_df[all_df["code"].isin(codes)]
    for _, row in subset.iterrows():
        try:
            t = float(row.get("turnover_rate", 999)) if pd.notna(row.get("turnover_rate")) else 999
            v = float(row.get("volume_ratio", 999)) if pd.notna(row.get("volume_ratio")) else 999
        except (ValueError, TypeError):
            t, v = 999, 999
        results[row["code"]] = (t < 5.0) and (v < VOLUME_SHRINK_RATIO)
    logger.info(f"筹码集中：{sum(v for v in results.values())}/{len(codes)} 只通过")
    return results


def main():
    logger.info("=" * 60)
    logger.info("A股策略选股器 启动")
    logger.info(f"市值 {MIN_MARKET_CAP}-{MAX_MARKET_CAP}亿 | 均线多头:{MA_BULLISH} | 筹码:{CHIP_CHECK}")
    logger.info("=" * 60)

    all_stocks = get_all_stocks()
    if all_stocks.empty:
        logger.error("未获取到数据"); sys.exit(1)
    filtered = basic_filter(all_stocks)
    if filtered.empty:
        logger.warning("基础筛选后无结果"); sys.exit(0)

    # 均线多头
    ma_results = check_ma_bullish_batch(filtered["code"].tolist())
    filtered = filtered[filtered["code"].isin([c for c, v in ma_results.items() if v])]
    if filtered.empty:
        logger.warning("均线筛选后无结果"); sys.exit(0)

    # 筹码集中
    chip_results = check_chip_concentration(filtered["code"].tolist(), all_stocks)
    chip_passed = [c for c, v in chip_results.items() if v]
    if CHIP_CHECK and chip_passed:
        filtered = filtered[filtered["code"].isin(chip_passed)]
    elif CHIP_CHECK:
        logger.warning("筹码条件过严，回退使用均线结果")

    # 排序和输出
    sort_map = {"market_cap": "market_cap_yi", "turnover": "turnover_rate", "change": "change_pct"}
    if sort_map.get(SORT_BY, "") in filtered.columns:
        filtered = filtered.sort_values(sort_map[SORT_BY], ascending=(SORT_BY == "market_cap"))
    filtered = filtered.head(OUTPUT_LIMIT)

    stock_list = ",".join(filtered["code"].tolist())
    print(f"\n{'='*80}\n  选股结果（{len(filtered)} 只）\n{'='*80}")
    for _, r in filtered.iterrows():
        print(f"  {r['code']}  {str(r.get('name','')):<8}  "
              f"¥{float(r.get('price',0)):.2f}  市值{float(r.get('market_cap_yi',0)):.0f}亿  "
              f"PE{float(r.get('pe_ttm',0)):.1f}")
    print(f"\nSTOCK_LIST = {stock_list}")

    # 保存结果
    os.makedirs("reports", exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    filtered.to_csv(f"reports/screener_{today}.csv", index=False, encoding="utf-8-sig")
    with open(f"reports/stock_list_{today}.txt", "w") as f:
        f.write(stock_list)

    gh_out = os.getenv("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as f:
            f.write(f"stock_list={stock_list}\nstock_count={len(filtered)}\n")
    logger.info("选股完成")

if __name__ == "__main__":
    sys.exit(main())
