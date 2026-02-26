#!/usr/bin/env python3
"""A-share shipan-line screener with conservative request pacing."""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import json
import math
import random
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

import akshare as ak
import pandas as pd

ALLOWED_PREFIXES = (
    "000",
    "001",
    "002",
    "003",
    "300",
    "301",
    "600",
    "601",
    "603",
    "605",
    "688",
    "689",
)


@dataclass
class Candidate:
    code: str
    name: str
    board: str
    shipan_date: str
    life_line: float
    breakout_date: str | None
    close_price: float
    stop_loss_pct: float
    stop_loss_price: float
    weak_after_breakout: bool
    volume_ratio: float
    consolidation_ratio: float
    low_pos_ratio: float
    is_strict: bool


class PaceFetcher:
    def __init__(
        self,
        min_interval: float,
        jitter: float,
        retries: int,
        backoff_base: float,
        request_timeout: float,
    ) -> None:
        self.min_interval = min_interval
        self.jitter = jitter
        self.retries = retries
        self.backoff_base = backoff_base
        self.request_timeout = request_timeout
        self._last_ts = 0.0

    def _sleep_before_request(self) -> None:
        now = time.monotonic()
        remain = self.min_interval - (now - self._last_ts)
        pause = max(0.0, remain) + random.uniform(0.0, self.jitter)
        if pause > 0:
            time.sleep(pause)

    def call(self, fn: Callable[..., pd.DataFrame], **kwargs) -> pd.DataFrame:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            if attempt > 0:
                backoff = self.backoff_base * (2 ** (attempt - 1))
                time.sleep(backoff + random.uniform(0.0, self.jitter))
            self._sleep_before_request()
            try:
                with futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(fn, **kwargs)
                    result = future.result(timeout=self.request_timeout)
                self._last_ts = time.monotonic()
                if not isinstance(result, pd.DataFrame):
                    raise RuntimeError(f"Unexpected response type: {type(result)!r}")
                return result
            except Exception as exc:
                self._last_ts = time.monotonic()
                last_error = exc
        raise RuntimeError(f"Request failed after retries: {last_error}")


def infer_board(code: str) -> str:
    if code.startswith(("300", "301")):
        return "创业板"
    if code.startswith(("688", "689")):
        return "科创板"
    return "主板"


def stop_loss_pct_for_board(board: str) -> float:
    if board in {"创业板", "科创板"}:
        return 0.06
    return 0.04


def is_a_share_code(code: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", code)) and code.startswith(ALLOWED_PREFIXES)


def to_exchange_symbol(code: str) -> str:
    return f"sh{code}" if code.startswith(("5", "6", "9")) else f"sz{code}"


def normalize_hist(df: pd.DataFrame) -> pd.DataFrame:
    cols = set(df.columns)
    if {"日期", "开盘", "收盘", "最高", "最低", "成交量"}.issubset(cols):
        cleaned = df.copy()
    elif {"date", "open", "close", "high", "low"}.issubset(cols):
        cleaned = pd.DataFrame()
        cleaned["日期"] = df["date"]
        cleaned["开盘"] = df["open"]
        cleaned["收盘"] = df["close"]
        cleaned["最高"] = df["high"]
        cleaned["最低"] = df["low"]
        if "volume" in df.columns:
            cleaned["成交量"] = df["volume"]
        elif "amount" in df.columns:
            cleaned["成交量"] = df["amount"]
        else:
            return pd.DataFrame()
        if "涨跌幅" in df.columns:
            cleaned["涨跌幅"] = df["涨跌幅"]
        else:
            cleaned["涨跌幅"] = pd.to_numeric(cleaned["收盘"], errors="coerce").pct_change() * 100.0
    else:
        return pd.DataFrame()

    for col in ("开盘", "收盘", "最高", "最低", "成交量", "涨跌幅"):
        cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")
    cleaned["日期"] = pd.to_datetime(cleaned["日期"], errors="coerce")
    cleaned = cleaned.dropna(subset=["日期", "开盘", "收盘", "最高", "最低", "成交量"]).reset_index(drop=True)
    cleaned = cleaned.sort_values("日期").reset_index(drop=True)
    return cleaned


def find_latest_loose_signal(df: pd.DataFrame) -> dict | None:
    if len(df) < 70:
        return None

    latest: dict | None = None
    for i in range(60, len(df)):
        prev_vol = float(df.at[i - 1, "成交量"])
        cur_vol = float(df.at[i, "成交量"])
        if prev_vol <= 0:
            continue
        vol_ratio = cur_vol / prev_vol

        open_price = float(df.at[i, "开盘"])
        close_price = float(df.at[i, "收盘"])
        prev_close = float(df.at[i - 1, "收盘"])
        cond_bull = close_price > open_price and close_price > prev_close

        pre = df.iloc[i - 60 : i]
        if len(pre) < 20:
            continue
        pre_high = float(pre["最高"].max())
        pre_low = float(pre["最低"].min())
        if pre_low <= 0:
            continue
        consolidation_ratio = (pre_high - pre_low) / pre_low

        low_5 = float(df["最低"].iloc[max(0, i - 5) : i].min())
        low_20 = float(df["最低"].iloc[max(0, i - 20) : i].min())

        max_120 = float(df["最高"].iloc[max(0, i - 120) : i].max())
        if max_120 <= 0:
            continue
        low_pos_ratio = close_price / max_120

        loose_ok = (
            vol_ratio >= 2.5
            and cond_bull
            and consolidation_ratio <= 0.45
            and low_5 >= low_20 * 0.98
            and low_pos_ratio <= 0.95
            and close_price >= pre_high * 0.995
        )
        if not loose_ok:
            continue

        strict_base_ok = (
            vol_ratio >= 3.0
            and consolidation_ratio <= 0.35
            and low_5 > low_20
            and low_pos_ratio <= 0.90
            and close_price > pre_high
        )

        latest = {
            "idx": i,
            "shipan_date": df.at[i, "日期"].strftime("%Y-%m-%d"),
            "life_line": close_price,
            "vol_ratio": vol_ratio,
            "consolidation_ratio": consolidation_ratio,
            "low_pos_ratio": low_pos_ratio,
            "strict_base_ok": strict_base_ok,
        }
    return latest


def find_breakout_after_lifeline(df: pd.DataFrame, start_idx: int, life_line: float) -> int | None:
    for j in range(start_idx + 1, len(df)):
        open_price = float(df.at[j, "开盘"])
        close_price = float(df.at[j, "收盘"])
        if close_price > life_line and max(open_price, close_price) > life_line:
            return j
    return None


def is_weak_after_breakout(df: pd.DataFrame, breakout_idx: int, board: str) -> bool:
    end = min(len(df), breakout_idx + 6)
    window = df.iloc[breakout_idx:end]
    if window.empty:
        return True
    pct_limit = 19.0 if board in {"创业板", "科创板"} else 9.5
    has_big_green = (window["涨跌幅"] >= 6.0).any()
    has_limit = (window["涨跌幅"] >= pct_limit).any()
    return not (has_big_green or has_limit)


def cache_path_for(cache_dir: Path, symbol: str, start: str, end: str, adjust: str) -> Path:
    return cache_dir / f"{symbol}_{start}_{end}_{adjust}.csv"


def read_cached_hist(path: Path, ttl_hours: float) -> pd.DataFrame | None:
    if not path.exists():
        return None
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    if age > timedelta(hours=ttl_hours):
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def write_cached_hist(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def fetch_hist_with_cache(
    fetcher: PaceFetcher,
    cache_dir: Path,
    symbol: str,
    start_date: str,
    end_date: str,
    adjust: str,
    cache_ttl_hours: float,
    hist_source: str,
) -> pd.DataFrame:
    cpath = cache_path_for(cache_dir, symbol, start_date, end_date, adjust)
    cached = read_cached_hist(cpath, cache_ttl_hours)
    if cached is not None:
        return cached

    sec_symbol = to_exchange_symbol(symbol)

    if hist_source == "eastmoney":
        fresh = fetcher.call(
            ak.stock_zh_a_hist,
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )
    elif hist_source == "sina":
        fresh = fetcher.call(
            ak.stock_zh_a_daily,
            symbol=sec_symbol,
            adjust=adjust,
        )
    elif hist_source == "tx":
        fresh = fetcher.call(
            ak.stock_zh_a_hist_tx,
            symbol=sec_symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )
    else:
        # Auto mode: prefer Sina/Tencent in this environment to avoid Eastmoney disconnects.
        last_error: Exception | None = None
        fresh = pd.DataFrame()
        for source in ("sina", "tx", "eastmoney"):
            try:
                fresh = fetch_hist_with_cache(
                    fetcher=fetcher,
                    cache_dir=cache_dir,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                    cache_ttl_hours=0.0,
                    hist_source=source,
                )
                break
            except Exception as exc:
                last_error = exc
        if fresh.empty and last_error is not None:
            raise last_error

    # Restrict to requested date range for sources that return full history.
    if "date" in fresh.columns:
        fresh = fresh.copy()
        fresh["date"] = pd.to_datetime(fresh["date"], errors="coerce")
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        fresh = fresh[(fresh["date"] >= start_dt) & (fresh["date"] <= end_dt)]
    if "日期" in fresh.columns:
        fresh = fresh.copy()
        fresh["日期"] = pd.to_datetime(fresh["日期"], errors="coerce")
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        fresh = fresh[(fresh["日期"] >= start_dt) & (fresh["日期"] <= end_dt)]

    write_cached_hist(cpath, fresh)
    return fresh


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run A-share shipan-line screening with conservative pacing.")
    parser.add_argument("--start-date", default="20240101", help="History start date, format YYYYMMDD.")
    parser.add_argument("--end-date", default=datetime.now().strftime("%Y%m%d"), help="History end date, format YYYYMMDD.")
    parser.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"], help="Price adjust mode.")
    parser.add_argument("--max-stocks", type=int, default=120, help="Max number of stocks to evaluate.")
    parser.add_argument("--top-by-turnover", action="store_true", help="Prefer high-turnover stocks first.")
    parser.add_argument("--min-interval", type=float, default=1.2, help="Minimum seconds between requests.")
    parser.add_argument("--jitter", type=float, default=0.8, help="Random extra seconds per request.")
    parser.add_argument("--retries", type=int, default=3, help="Retries for each request.")
    parser.add_argument("--backoff-base", type=float, default=2.0, help="Base backoff seconds for retries.")
    parser.add_argument("--request-timeout", type=float, default=20.0, help="Timeout seconds for each request call.")
    parser.add_argument("--cache-ttl-hours", type=float, default=12.0, help="Cache TTL in hours.")
    parser.add_argument("--cache-dir", default="data/cache/akshare_hist", help="Local cache directory.")
    parser.add_argument(
        "--hist-source",
        default="sina",
        choices=["sina", "tx", "eastmoney", "auto"],
        help="Daily kline source.",
    )
    parser.add_argument("--output-json", default="", help="Output JSON path. Auto-generate when empty.")
    parser.add_argument("--output-xlsx", default="", help="Output XLSX path. Auto-generate when empty.")
    return parser.parse_args()


def build_universe(fetcher: PaceFetcher, max_stocks: int, top_by_turnover: bool) -> pd.DataFrame:
    spot: pd.DataFrame | None = None
    try:
        spot = fetcher.call(ak.stock_zh_a_spot_em)
    except Exception:
        spot = None

    if spot is not None and not spot.empty:
        spot = spot.copy()
        spot["代码"] = spot["代码"].astype(str).str.zfill(6)
        spot = spot[spot["代码"].map(is_a_share_code)]
        if top_by_turnover and "成交额" in spot.columns:
            spot["成交额"] = pd.to_numeric(spot["成交额"], errors="coerce")
            spot = spot.sort_values("成交额", ascending=False)
        return spot.head(max_stocks)

    fallback = fetcher.call(ak.stock_info_a_code_name)
    if fallback.empty:
        raise RuntimeError("Universe request returned empty dataframe for both primary and fallback endpoints.")
    fallback = fallback.copy()
    fallback["code"] = fallback["code"].astype(str).str.zfill(6)
    fallback = fallback[fallback["code"].map(is_a_share_code)]
    fallback = fallback.rename(columns={"code": "代码", "name": "名称"})
    return fallback.head(max_stocks)


def run_screening(args: argparse.Namespace) -> dict:
    fetcher = PaceFetcher(
        min_interval=args.min_interval,
        jitter=args.jitter,
        retries=args.retries,
        backoff_base=args.backoff_base,
        request_timeout=args.request_timeout,
    )
    universe = build_universe(fetcher, args.max_stocks, args.top_by_turnover)
    cache_dir = Path(args.cache_dir)

    loose_candidates: list[Candidate] = []
    failed: list[str] = []

    for _, row in universe.iterrows():
        code = str(row["代码"]).zfill(6)
        name = str(row.get("名称", ""))
        board = infer_board(code)
        try:
            raw_hist = fetch_hist_with_cache(
                fetcher=fetcher,
                cache_dir=cache_dir,
                symbol=code,
                start_date=args.start_date,
                end_date=args.end_date,
                adjust=args.adjust,
                cache_ttl_hours=args.cache_ttl_hours,
                hist_source=args.hist_source,
            )
        except Exception:
            failed.append(code)
            continue

        hist = normalize_hist(raw_hist)
        if hist.empty:
            continue

        signal = find_latest_loose_signal(hist)
        if signal is None:
            continue

        shipan_idx = int(signal["idx"])
        life_line = float(signal["life_line"])
        breakout_idx = find_breakout_after_lifeline(hist, shipan_idx, life_line)
        breakout_date = hist.at[breakout_idx, "日期"].strftime("%Y-%m-%d") if breakout_idx is not None else None
        weak_flag = True if breakout_idx is None else is_weak_after_breakout(hist, breakout_idx, board)
        strict_ok = bool(signal["strict_base_ok"]) and breakout_idx is not None and not weak_flag

        stop_loss_pct = stop_loss_pct_for_board(board)
        close_price = float(hist.iloc[-1]["收盘"])
        vol_ratio = float(signal["vol_ratio"])
        consolidation_ratio = float(signal["consolidation_ratio"])
        low_pos_ratio = float(signal["low_pos_ratio"])

        loose_candidates.append(
            Candidate(
                code=code,
                name=name,
                board=board,
                shipan_date=str(signal["shipan_date"]),
                life_line=round(life_line, 3),
                breakout_date=breakout_date,
                close_price=round(close_price, 3),
                stop_loss_pct=round(stop_loss_pct * 100, 2),
                stop_loss_price=round(life_line * (1.0 - stop_loss_pct), 3),
                weak_after_breakout=weak_flag,
                volume_ratio=round(vol_ratio, 2) if not math.isnan(vol_ratio) else 0.0,
                consolidation_ratio=round(consolidation_ratio, 4),
                low_pos_ratio=round(low_pos_ratio, 4),
                is_strict=strict_ok,
            )
        )

    loose_candidates.sort(key=lambda x: (x.breakout_date or "", x.volume_ratio), reverse=True)
    strict_candidates = [item for item in loose_candidates if item.is_strict]

    output = {
        "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "params": {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "adjust": args.adjust,
            "max_stocks": args.max_stocks,
            "top_by_turnover": args.top_by_turnover,
            "min_interval": args.min_interval,
            "jitter": args.jitter,
            "retries": args.retries,
            "backoff_base": args.backoff_base,
            "request_timeout": args.request_timeout,
            "cache_ttl_hours": args.cache_ttl_hours,
            "hist_source": args.hist_source,
        },
        "summary": {
            "universe_size": int(len(universe)),
            "loose_count": int(len(loose_candidates)),
            "strict_count": int(len(strict_candidates)),
            "failed_fetch_count": int(len(failed)),
        },
        "loose_candidates": [asdict(c) for c in loose_candidates],
        "strict_candidates": [asdict(c) for c in strict_candidates],
        "failed_symbols": failed,
    }
    return output


def _sheet_dataframe(candidates: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(candidates)
    columns = [
        "code",
        "name",
        "board",
        "shipan_date",
        "life_line",
        "breakout_date",
        "close_price",
        "stop_loss_pct",
        "stop_loss_price",
        "volume_ratio",
        "consolidation_ratio",
        "low_pos_ratio",
        "weak_after_breakout",
        "is_strict",
    ]
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df[columns]
    rename_map = {
        "code": "代码",
        "name": "名称",
        "board": "板块",
        "shipan_date": "试盘日期",
        "life_line": "生命线",
        "breakout_date": "突破日期",
        "close_price": "最新收盘",
        "stop_loss_pct": "止损百分比",
        "stop_loss_price": "止损价格",
        "volume_ratio": "试盘量比",
        "consolidation_ratio": "震荡振幅",
        "low_pos_ratio": "相对高位比例",
        "weak_after_breakout": "突破后走弱",
        "is_strict": "命中严格版",
    }
    return df.rename(columns=rename_map)


def save_output(payload: dict, output_json_arg: str, output_xlsx_arg: str) -> tuple[Path, Path]:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = Path(output_json_arg) if output_json_arg else Path("data/output") / f"shipan_candidates_{stamp}.json"
    xlsx_path = Path(output_xlsx_arg) if output_xlsx_arg else Path("data/output") / f"shipan_candidates_{stamp}.xlsx"

    json_path.parent.mkdir(parents=True, exist_ok=True)
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    loose_df = _sheet_dataframe(payload["loose_candidates"])
    strict_df = _sheet_dataframe(payload["strict_candidates"])
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        loose_df.to_excel(writer, sheet_name="宽松版", index=False)
        strict_df.to_excel(writer, sheet_name="严格版", index=False)

    return json_path, xlsx_path


def main() -> int:
    args = parse_args()
    payload = run_screening(args)
    json_path, xlsx_path = save_output(payload, args.output_json, args.output_xlsx)
    print(
        json.dumps(
            {
                "status": "ok",
                "json_output": str(json_path),
                "xlsx_output": str(xlsx_path),
                "summary": payload["summary"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
