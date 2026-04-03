#!/usr/bin/env python3
"""
A股策略选股器（增量保存版 v2）
===========================
从全 A 股 5000+ 只中按策略条件筛选标的，输出可直接用于 daily_stock_analysis 的 STOCK_LIST。

核心特性：
- 多数据源容错：akshare → efinance 自动切换，不再依赖单一 API
- 均线历史数据优先使用 efinance.stock.get_quote_history()（解决 akshare stock_zh_a_hist KeyError 问题）
- 批量获取 + 增量保存：每通过一只立即落盘，中途崩溃/取消/超时仍有部分结果
- 全程详细日志：每一步异常均记录，方便远程排查 GitHub Actions 问题

环境变量：
  SCREEN_MIN_MARKET_CAP  最小市值（亿），默认 20
  SCREEN_MAX_MARKET_CAP  最大市值（亿），默认 500
  SCREEN_MA_PERIOD       均线校验周期（天），默认 60
  SCREEN_MA_BULLISH      是否启用均线多头筛选，默认 true
  SCREEN_CHIP_CHECK      是否启用筹码集中度筛选，默认 true
  SCREEN_MIN_PRICE       最低股价（元），默认 5
  SCREEN_MAX_PE          最大动态市盈率，默认 200（<=0 表示不限）
  SCREEN_VOLUME_SHRINK   量缩比例阈值（当前量比），默认 0.8
  SCREEN_OUTPUT_LIMIT    最终输出数量上限，默认 30
  SCREEN_SORT_BY         排序依据：market_cap / turnover / change，默认 market_cap
"""

import os
import sys
import time
import atexit
import logging
from datetime import datetime, timedelta

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("screener")

# ============================================================
# 配置
# ============================================================
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

TODAY = datetime.now().strftime("%Y%m%d")
REPORTS_DIR = "reports"
LIVE_CSV = f"{REPORTS_DIR}/screener_live_{TODAY}.csv"
LIVE_LIST = f"{REPORTS_DIR}/stock_list_{TODAY}.txt"
FINAL_MD = f"{REPORTS_DIR}/screener_{TODAY}.md"
FINAL_CSV = f"{REPORTS_DIR}/screener_{TODAY}.csv"
STATUS_FILE = f"{REPORTS_DIR}/screener_status_{TODAY}.txt"

MA_BATCH_SIZE = 30
MA_INTER_BATCH_SLEEP = 0.5
MA_INTER_STOCK_SLEEP = 0.05


# ============================================================
# 增量保存器
# ============================================================
class IncrementalSaver:
    """每通过一只股票就追加写入磁盘，定期刷新汇总文件。"""

    def __init__(self, all_stocks_df: pd.DataFrame, flush_every: int = 50):
        os.makedirs(REPORTS_DIR, exist_ok=True)
        self._all_stocks = all_stocks_df
        self._passed_codes: list[str] = []
        self._flush_every = flush_every
        self._total_checked = 0
        self._total_candidates = 0

        with open(LIVE_CSV, "w", encoding="utf-8-sig") as f:
            f.write("code,name,price,change_pct,market_cap_yi,turnover_rate,pe_ttm,ma_bullish\n")
        self._header_written = True

        # 注册进程退出钩子：无论正常退出、异常、被信号杀死都刷一次
        atexit.register(self.flush_final)

    def set_total(self, total: int) -> None:
        self._total_candidates = total

    def on_stock_passed(self, code: str) -> None:
        """一只股票通过均线检查后调用"""
        self._passed_codes.append(code)
        self._append_csv_row(code)
        self._total_checked += 1
        if len(self._passed_codes) % self._flush_every == 0:
            self._flush_summary()

    def on_stock_failed(self) -> None:
        self._total_checked += 1

    @property
    def passed_codes(self) -> list[str]:
        return list(self._passed_codes)

    def _append_csv_row(self, code: str) -> None:
        row = self._all_stocks[self._all_stocks["code"] == code]
        if row.empty:
            return
        r = row.iloc[0]
        line = (
            f"{code},"
            f"{r.get('name', '')},"
            f"{_safe_float(r, 'price'):.2f},"
            f"{_safe_float(r, 'change_pct'):.2f},"
            f"{_safe_float(r, 'market_cap_yi'):.1f},"
            f"{_safe_float(r, 'turnover_rate'):.2f},"
            f"{_safe_float(r, 'pe_ttm'):.1f},"
            f"True\n"
        )
        with open(LIVE_CSV, "a", encoding="utf-8-sig") as f:
            f.write(line)

    def _flush_summary(self) -> None:
        codes = self._passed_codes
        stock_list = ",".join(codes)
        with open(LIVE_LIST, "w") as f:
            f.write(stock_list)
        logger.info(
            f"  [增量保存] 已检查 {self._total_checked}/{self._total_candidates}，"
            f"通过 {len(codes)} 只 → {LIVE_LIST}"
        )

    def flush_final(self) -> None:
        """进程退出前的最终刷盘"""
        if self._passed_codes:
            self._flush_summary()
            logger.info(f"[退出钩子] 已保存 {len(self._passed_codes)} 只到 {LIVE_LIST}")


def _safe_float(row, col, default=0.0) -> float:
    try:
        v = row.get(col, default)
        return float(v) if pd.notna(v) else default
    except (ValueError, TypeError):
        return default


# ============================================================
# 数据获取 — 实时行情（多源容错）
# ============================================================
def get_all_stocks() -> pd.DataFrame:
    """获取全 A 股实时行情，akshare → efinance 多源容错。"""
    logger.info("正在获取全A股实时行情...")

    df = _try_akshare_spot()
    if df is not None and len(df) > 1000:
        return df

    df = _try_efinance_quotes()
    if df is not None and len(df) > 200:
        return df

    df = _try_efinance_quotes("沪深A股")
    if df is not None and not df.empty:
        logger.warning(f"efinance(沪深A股) 仅获取到 {len(df)} 只，可能存在数据不全")
        return df

    logger.error("所有数据源均失败，无法获取实时行情")
    return pd.DataFrame()


def _try_akshare_spot() -> pd.DataFrame | None:
    try:
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            logger.warning("akshare stock_zh_a_spot_em 返回空")
            return None
        logger.info(f"akshare 获取到 {len(df)} 只, 列: {list(df.columns[:8])}")
        col_map = {
            "代码": "code", "名称": "name", "最新价": "price",
            "涨跌幅": "change_pct", "成交量": "volume", "成交额": "amount",
            "换手率": "turnover_rate", "总市值": "total_market_cap",
            "流通市值": "float_market_cap", "市盈率-动态": "pe_ttm", "量比": "volume_ratio",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        _log_sample(df, "akshare")
        return df
    except Exception as e:
        logger.warning(f"akshare stock_zh_a_spot_em 失败: {type(e).__name__}: {e}")
        return None


def _try_efinance_quotes(market=None) -> pd.DataFrame | None:
    try:
        import efinance as ef
        df = ef.stock.get_realtime_quotes(market) if market else ef.stock.get_realtime_quotes()
        if df is None or df.empty:
            logger.warning(f"efinance get_realtime_quotes({market}) 返回空")
            return None
        label = market or "默认"
        logger.info(f"efinance({label}) 获取到 {len(df)} 只, 列: {list(df.columns[:8])}")
        col_map = {
            "股票代码": "code", "股票名称": "name", "最新价": "price",
            "涨跌幅": "change_pct", "成交量": "volume", "成交额": "amount",
            "换手率": "turnover_rate", "总市值": "total_market_cap",
            "流通市值": "float_market_cap", "动态市盈率": "pe_ttm", "量比": "volume_ratio",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        _log_sample(df, f"efinance({label})")
        return df
    except Exception as e:
        logger.warning(f"efinance get_realtime_quotes({market}) 失败: {type(e).__name__}: {e}")
        return None


def _log_sample(df: pd.DataFrame, source: str) -> None:
    """打印前 3 行样本数据用于诊断"""
    if df.empty:
        return
    sample = df.head(3)[["code", "name", "price", "total_market_cap"]].to_string(index=False)
    logger.info(f"  [{source}] 样本数据:\n{sample}")


# ============================================================
# 数据获取 — 历史 K 线（多源容错）
# ============================================================
def _fetch_history_efinance(code: str, beg: str, end: str) -> pd.DataFrame | None:
    """efinance 单只股票历史 K 线"""
    import efinance as ef
    df = ef.stock.get_quote_history(code, beg=beg, end=end, klt=101, fqt=1)
    if df is not None and len(df) >= 20:
        return df
    return None


def _fetch_history_akshare(code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
    """akshare 单只股票历史 K 线（备用）"""
    import akshare as ak
    df = ak.stock_zh_a_spot_em()
    logger.info(f"akshare 获取到 {len(df)} 只股票")
    col_map = {
        "代码": "code", "名称": "name", "最新价": "price",
        "涨跌幅": "change_pct", "成交量": "volume", "成交额": "amount",
        "换手率": "turnover_rate", "总市值": "total_market_cap",
        "流通市值": "float_market_cap", "市盈率-动态": "pe_ttm", "量比": "volume_ratio",
    }
    return df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})


# ============================================================
# 筛选逻辑
# ============================================================
def basic_filter(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("执行基础筛选...")
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
    if not df.empty:
        cap_min = df["market_cap_yi"].min()
        cap_max = df["market_cap_yi"].max()
        logger.info(f"  市值范围: {cap_min:.1f} - {cap_max:.1f} 亿")
    return df


def check_ma_bullish_incremental(codes: list, saver: IncrementalSaver) -> list[str]:
    """
    逐批检查均线多头排列：efinance 批量拉取 → 逐只判断 → 失败个股用 akshare 补救。
    每通过一只立即通过 saver 落盘。
    """
    if not MA_BULLISH:
        for c in codes:
            saver.on_stock_passed(c)
        return codes

    logger.info(f"检查均线多头排列（{len(codes)} 只），efinance 批量拉取 + akshare 备用...")
    saver.set_total(len(codes))

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=MA_PERIOD * 2)).strftime("%Y%m%d")
    logger.info(f"  K 线区间: {start_date} → {end_date}")

    passed: list[str] = []
    errors = {"ef_batch_miss": 0, "ef_single_fail": 0, "ak_fail": 0,
              "no_data": 0, "not_bullish": 0, "calc_error": 0}
    first_errors: list[str] = []

    processed = 0
    for batch_start in range(0, len(codes), MA_BATCH_SIZE):
        batch = codes[batch_start: batch_start + MA_BATCH_SIZE]

        # ---- Step 1: efinance 批量获取 ----
        batch_data = _fetch_history_batch_efinance(batch, beg=start_date, end=end_date)
        ef_hit = len(batch_data)
        if ef_hit > 0:
            logger.debug(f"  efinance 批量命中 {ef_hit}/{len(batch)}")

        # ---- Step 2: 逐只处理 ----
        for code in batch:
            df = batch_data.get(code)

            # 批量未命中 → efinance 单只重试
            if df is None:
                errors["ef_batch_miss"] += 1
                try:
                    df = _fetch_history_efinance(code, beg=start_date, end=end_date)
                except Exception as e:
                    errors["ef_single_fail"] += 1
                    if len(first_errors) < 10:
                        first_errors.append(f"efinance({code}): {type(e).__name__}: {e}")

            # 仍无数据 → akshare 兜底
            if df is None:
                try:
                    df = _fetch_history_akshare(code, start_date, end_date)
                except Exception as e:
                    errors["ak_fail"] += 1
                    if len(first_errors) < 10:
                        first_errors.append(f"akshare({code}): {type(e).__name__}: {e}")

            # 数据不足
            if df is None or len(df) < 20:
                errors["no_data"] += 1
                saver.on_stock_failed()
                processed += 1
                continue

            # 计算 MA
            bullish = _calc_ma_bullish(df)
            if bullish is None:
                errors["calc_error"] += 1
                if len(first_errors) < 10:
                    cols = list(df.columns)
                    first_errors.append(f"MA calc({code}): 找不到收盘列, 实际列名={cols[:8]}")
                saver.on_stock_failed()
            elif bullish:
                passed.append(code)
                saver.on_stock_passed(code)
            else:
                errors["not_bullish"] += 1
                saver.on_stock_failed()

            processed += 1
            time.sleep(MA_INTER_STOCK_SLEEP)

        # 批间进度与休眠
        if processed % 100 < MA_BATCH_SIZE or processed >= len(codes):
            logger.info(
                f"  均线进度: {processed}/{len(codes)}, 通过 {len(passed)} 只 | "
                f"errors={errors}"
            )
        time.sleep(MA_INTER_BATCH_SLEEP)

    logger.info(f"均线多头排列：{len(passed)}/{len(codes)} 只通过")
    logger.info(f"错误汇总: {errors}")
    if first_errors:
        logger.warning(f"前 {len(first_errors)} 条错误详情:")
        for err in first_errors:
            logger.warning(f"  {err}")
    return passed


def check_chip_concentration(codes: list, all_stocks_df: pd.DataFrame) -> list[str]:
    if not CHIP_CHECK:
        return codes
    logger.info(f"检查筹码集中度（{len(codes)} 只）...")
    subset = all_stocks_df[all_stocks_df["code"].isin(codes)]
    passed = []
    for _, row in subset.iterrows():
        try:
            t = float(row.get("turnover_rate", 999)) if pd.notna(row.get("turnover_rate")) else 999
            v = float(row.get("volume_ratio", 999)) if pd.notna(row.get("volume_ratio")) else 999
        except (ValueError, TypeError):
            t, v = 999, 999
        if t < 5.0 and v < VOLUME_SHRINK_RATIO:
            passed.append(row["code"])
    logger.info(f"筹码集中度：{len(passed)}/{len(codes)} 只通过")
    return passed


# ============================================================
# 结果输出
# ============================================================
def write_status(status: str, detail: str = "") -> None:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    lines = [
        f"状态: {status}",
        f"时间: {datetime.now().isoformat()}",
        f"市值: {MIN_MARKET_CAP}-{MAX_MARKET_CAP}亿 | 均线:{MA_BULLISH} | 筹码:{CHIP_CHECK}",
        "", "说明:", detail or "(无)", "",
        "调整建议: 放宽市值、关闭筹码(SCREEN_CHIP_CHECK=false)、调大 SCREEN_VOLUME_SHRINK(如1.2)。",
    ]
    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def save_final_results(df: pd.DataFrame) -> str:
    """排序 + 截取 + 生成最终 csv / md / stock_list"""
    sort_map = {"market_cap": "market_cap_yi", "turnover": "turnover_rate", "change": "change_pct"}
    col = sort_map.get(SORT_BY, "market_cap_yi")
    if col in df.columns:
        df = df.sort_values(col, ascending=(SORT_BY == "market_cap"))
    df = df.head(OUTPUT_LIMIT)
    stock_list = ",".join(df["code"].tolist())

    # stock_list txt
    with open(LIVE_LIST, "w") as f:
        f.write(stock_list)

    # final CSV
    df.to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")

    # Markdown
    with open(FINAL_MD, "w", encoding="utf-8") as f:
        f.write(f"# A股策略选股结果 ({datetime.now().strftime('%Y-%m-%d')})\n\n")
        f.write("## 筛选条件\n\n| 条件 | 值 |\n|------|----|\n")
        f.write(f"| 市值范围 | {MIN_MARKET_CAP}-{MAX_MARKET_CAP} 亿 |\n")
        f.write(f"| 股价下限 | {MIN_PRICE} 元 |\n")
        f.write(f"| 最大PE | {MAX_PE if MAX_PE > 0 else '不限'} |\n")
        f.write(f"| 均线多头 | {'是' if MA_BULLISH else '否'} |\n")
        f.write(f"| 筹码集中 | {'是' if CHIP_CHECK else '否'} |\n\n")
        f.write(f"## 选股结果（{len(df)} 只）\n\n")
        f.write("| 代码 | 名称 | 股价 | 涨跌幅 | 市值(亿) | 换手率 | PE |\n")
        f.write("|------|------|------|--------|---------|--------|----|\n")
        for _, r in df.iterrows():
            f.write(
                f"| {r.get('code','')} | {r.get('name','')} "
                f"| {_safe_float(r,'price'):.2f} "
                f"| {_safe_float(r,'change_pct'):.2f}% "
                f"| {_safe_float(r,'market_cap_yi'):.1f} "
                f"| {_safe_float(r,'turnover_rate'):.2f}% "
                f"| {_safe_float(r,'pe_ttm'):.1f} |\n"
            )
        f.write(f"\n## STOCK_LIST\n\n```\n{stock_list}\n```\n")
        f.write(f"\n> 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # GitHub Actions output
    gh_out = os.getenv("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as f:
            f.write(f"stock_list={stock_list}\nstock_count={len(df)}\n")

    # 控制台打印
    print(f"\n{'=' * 80}")
    print(f"  策略选股结果（共 {len(df)} 只）")
    print(f"  市值 {MIN_MARKET_CAP}-{MAX_MARKET_CAP}亿 | 均线多头:{MA_BULLISH} | 筹码:{CHIP_CHECK}")
    print("=" * 80)
    for _, r in df.iterrows():
        print(
            f"  {r.get('code','')}  {str(r.get('name','')):<8}  "
            f"¥{_safe_float(r,'price'):.2f}  "
            f"市值{_safe_float(r,'market_cap_yi'):.0f}亿  "
            f"PE{_safe_float(r,'pe_ttm'):.1f}"
        )
    print(f"\nSTOCK_LIST = {stock_list}")

    logger.info(f"最终结果已保存: {FINAL_CSV}, {FINAL_MD}, {LIVE_LIST}")
    return stock_list


# ============================================================
# 主流程
# ============================================================
def main():
    os.makedirs(REPORTS_DIR, exist_ok=True)

    logger.info("=" * 60)
    logger.info("A股策略选股器 v2 启动（增量保存 + 多源容错）")
    logger.info(f"市值: {MIN_MARKET_CAP}-{MAX_MARKET_CAP}亿 | 均线多头: {MA_BULLISH} | 筹码: {CHIP_CHECK}")
    logger.info("=" * 60)

    write_status("运行中", "正在获取全市场数据...")

    # Stage 1: 获取 + 基础筛选
    all_stocks = get_all_stocks()
    if all_stocks.empty:
        write_status("数据获取失败", "get_all_stocks() 返回空。akshare 和 efinance 均无法获取行情数据。"
                      "请检查：1) 网络连通性 2) pip install akshare efinance 是否成功 3) 是否为交易日/交易时段")
        sys.exit(1)

    filtered = basic_filter(all_stocks)
    if filtered.empty:
        write_status("基础筛选无结果",
                      f"全量 {len(all_stocks)} 只经市值({MIN_MARKET_CAP}-{MAX_MARKET_CAP}亿)"
                      f"/股价(>={MIN_PRICE}元)/PE(<={MAX_PE})筛选后为空。放宽参数再试。")
        sys.exit(0)

    write_status("运行中", f"基础筛选通过 {len(filtered)} 只，开始均线检查...")

    # Stage 2: 均线多头（增量保存 + 多源容错）
    saver = IncrementalSaver(filtered, flush_every=50)
    ma_passed = check_ma_bullish_incremental(filtered["code"].tolist(), saver)
    saver.flush_final()

    if not ma_passed:
        write_status("均线筛选无结果",
                      f"基础筛选 {len(filtered)} 只中无股票满足 MA5>MA10>MA20。"
                      "可能原因：1) 市场整体弱势 2) 数据获取失败（查看上方日志的 errors 统计）。"
                      "设 SCREEN_MA_BULLISH=false 可跳过此步。")
        sys.exit(0)

    result_df = filtered[filtered["code"].isin(ma_passed)]

    # Stage 3: 筹码集中度
    chip_passed = check_chip_concentration(result_df["code"].tolist(), all_stocks)
    if CHIP_CHECK and chip_passed:
        result_df = result_df[result_df["code"].isin(chip_passed)]
    elif CHIP_CHECK and not chip_passed:
        logger.warning("筹码筛选过严，回退使用均线结果")

    if result_df.empty:
        write_status("筹码筛选无结果", "筹码条件后无剩余。设 SCREEN_CHIP_CHECK=false 或调大 SCREEN_VOLUME_SHRINK。")
        sys.exit(0)

    save_final_results(result_df)
    write_status("成功", f"共 {min(len(result_df), OUTPUT_LIMIT)} 只，见 {LIVE_LIST}")

    logger.info("选股完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
