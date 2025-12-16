# -*- coding: utf-8 -*-
"""
03_build_features.py
Build features from Uniswap v3 Swap events stored in SQLite.

Target definitions:
- price (USDC/WETH) = 1 / ((sqrtPriceX96 / 2^96)^2)   # assuming token0=USDC, token1=WETH
- reference_price   = previous swap's price (USDC/WETH)
- execution_price   = USDC_in / WETH_out  (direction-aware)
- slippage          = (execution_price - reference_price) / reference_price
- trade_size_usdc   = |USDC_amount|
- liquidity_prev    = previous swap's liquidity
- z                 = trade_size_usdc / liquidity_prev
"""

import os
import sqlite3
import math
from typing import Optional, Tuple

DB_PATH = os.getenv("DB_PATH", "data/univ3.db")

# Arbitrum USDC (native) decimals=6, WETH decimals=18
USDC_DECIMALS = int(os.getenv("USDC_DECIMALS", "6"))
WETH_DECIMALS = int(os.getenv("WETH_DECIMALS", "18"))

TABLE_CANDIDATES = ["swaps_raw", "swaps"]  # try both
OUT_TABLE = "swaps_features"

Q96 = 2 ** 96


def table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,)
    )
    return cur.fetchone() is not None


def pick_input_table(cur: sqlite3.Cursor) -> str:
    for t in TABLE_CANDIDATES:
        if table_exists(cur, t):
            return t
    raise RuntimeError(f"No input table found. Tried: {TABLE_CANDIDATES}")


def price_usdc_per_weth_from_sqrtPriceX96(sqrtPriceX96: int) -> float:
    # Uniswap v3: price(token1/token0) = (sqrtP/Q96)^2
    # If token0=USDC, token1=WETH:
    # price(WETH/USDC) = (sqrtP/Q96)^2 * 10^(dec0-dec1)
    # We want USDC/WETH = 1 / price(WETH/USDC)
    sp = sqrtPriceX96 / Q96
    p_token1_over_token0 = (sp * sp) * (10 ** (USDC_DECIMALS - WETH_DECIMALS))
    if p_token1_over_token0 <= 0:
        return float("nan")
    return 1.0 / p_token1_over_token0


def compute_execution_price_usdc_per_weth(amount0: int, amount1: int) -> Tuple[Optional[float], Optional[float]]:
    """
    Swap event amounts:
      amount0: delta of token0 (USDC)
      amount1: delta of token1 (WETH)
    Convention:
      positive means token received by the pool, negative means sent out of the pool.
    Trader perspective is opposite of pool.

    We want execution_price = USDC_in / WETH_out from TRADER perspective.

    - If trader buys WETH with USDC:
        pool receives USDC  -> amount0 > 0 (pool in)
        pool sends WETH     -> amount1 < 0 (pool out)
        trader: USDC_in = amount0, WETH_out = -amount1
    - If trader sells WETH for USDC:
        pool receives WETH  -> amount1 > 0
        pool sends USDC     -> amount0 < 0
        trader: USDC_in = -amount0, WETH_out = amount1   (trader pays WETH, receives USDC) => not USDC_in/WETH_out
        For symmetry, we still compute "effective" USDC per WETH as |USDC| / |WETH|.

    We'll return:
      exec_price_usdc_per_weth = (abs(USDC_amount) / abs(WETH_amount)) after decimals
      trade_size_usdc          = abs(USDC_amount) in USDC units
    """
    usdc_amt = amount0 / (10 ** USDC_DECIMALS)
    weth_amt = amount1 / (10 ** WETH_DECIMALS)

    trade_size_usdc = abs(usdc_amt)
    if weth_amt == 0:
        return None, trade_size_usdc

    exec_price = abs(usdc_amt) / abs(weth_amt)
    return exec_price, trade_size_usdc


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    cur = conn.cursor()

    in_table = pick_input_table(cur)
    print(f"[OK] DB: {DB_PATH}")
    print(f"[OK] Input table: {in_table}")

    # Detect columns (we assume at least these exist)
    # Required: block_number, log_index, sqrtPriceX96, liquidity, amount0, amount1
    # Some schemas use snake_case or camelCase; handle both.
    cur.execute(f"PRAGMA table_info({in_table});")
    cols = [r[1] for r in cur.fetchall()]
    colset = set(cols)

    def pick(*names: str) -> str:
        for n in names:
            if n in colset:
                return n
        raise RuntimeError(f"Missing columns. Need one of {names}, got {cols}")

    c_block = pick("block_number", "blockNumber")
    c_log   = pick("log_index", "logIndex")
    c_sqrt  = pick("sqrtPriceX96", "sqrt_price_x96", "sqrt_price")
    c_liq   = pick("liquidity", "L")
    c_a0    = pick("amount0", "amount_0")
    c_a1    = pick("amount1", "amount_1")

    # Create output table
    cur.execute(f"DROP TABLE IF EXISTS {OUT_TABLE};")
    cur.execute(
        f"""
        CREATE TABLE {OUT_TABLE} (
            block_number   INTEGER,
            log_index      INTEGER,
            price_usdc_per_weth REAL,
            ref_price_usdc_per_weth REAL,
            exec_price_usdc_per_weth REAL,
            slippage       REAL,
            trade_size_usdc REAL,
            liquidity      INTEGER,
            liquidity_prev INTEGER,
            z              REAL
        );
        """
    )
    conn.commit()

    # Stream read in order
    q = f"""
    SELECT {c_block}, {c_log}, {c_sqrt}, {c_liq}, {c_a0}, {c_a1}
    FROM {in_table}
    ORDER BY {c_block} ASC, {c_log} ASC;
    """

    prev_price = None
    prev_liq = None

    rows_out = []
    n = 0
    batch = 5000

    for block_number, log_index, sqrtP, liq, a0, a1 in cur.execute(q):
        n += 1
        try:
            price = price_usdc_per_weth_from_sqrtPriceX96(int(sqrtP))
        except Exception:
            price = float("nan")

        exec_price, trade_usdc = compute_execution_price_usdc_per_weth(int(a0), int(a1))

        ref_price = prev_price
        liquidity_prev = prev_liq

        slippage = None
        z = None
        if ref_price is not None and exec_price is not None and ref_price != 0 and not math.isnan(ref_price):
            slippage = (exec_price - ref_price) / ref_price

        if liquidity_prev is not None and liquidity_prev != 0:
            z = trade_usdc / liquidity_prev

        rows_out.append((
            int(block_number), int(log_index),
            float(price) if price is not None else None,
            float(ref_price) if ref_price is not None else None,
            float(exec_price) if exec_price is not None else None,
            float(slippage) if slippage is not None else None,
            float(trade_usdc) if trade_usdc is not None else None,
            int(liq) if liq is not None else None,
            int(liquidity_prev) if liquidity_prev is not None else None,
            float(z) if z is not None else None
        ))

        prev_price = price
        prev_liq = int(liq)

        if len(rows_out) >= batch:
            cur.executemany(
                f"INSERT INTO {OUT_TABLE} VALUES (?,?,?,?,?,?,?,?,?,?);",
                rows_out
            )
            conn.commit()
            rows_out.clear()
            if n % (batch * 5) == 0:
                print(f"[OK] processed {n} swaps...")

    if rows_out:
        cur.executemany(
            f"INSERT INTO {OUT_TABLE} VALUES (?,?,?,?,?,?,?,?,?,?);",
            rows_out
        )
        conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {OUT_TABLE};")
    cnt = cur.fetchone()[0]
    print(f"[DONE] Built features: {cnt} rows -> table {OUT_TABLE}")

    conn.close()


if __name__ == "__main__":
    main()
