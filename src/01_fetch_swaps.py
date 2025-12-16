# -*- coding: utf-8 -*-
"""
01_fetch_swaps.py
Fetch Uniswap v3 Swap logs from Arbitrum via JSON-RPC and store into SQLite.

Design:
- Ingestion only: fetch + parse + store. No slippage/z computation here.
- Robust chunking: auto-reduce block range on RPC 400 errors.
"""

import os
import sys
import time
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

from web3 import Web3
from web3.exceptions import LogTopicError


# -----------------------------
# Config (edit these)
# -----------------------------
RPC_URL = os.getenv("ARB_RPC_URL", "https://arb1.arbitrum.io/rpc")

# TODO: Fill this with the Uniswap v3 pool address for USDC-WETH on Arbitrum
POOL_ADDRESS = os.getenv("POOL_ADDRESS", "").strip()

DB_PATH = os.getenv("DB_PATH", "data/univ3.db")

# Block range to fetch (edit or pass via env)
FROM_BLOCK = int(os.getenv("FROM_BLOCK", "0"))
TO_BLOCK = int(os.getenv("TO_BLOCK", "0"))

# Initial chunk size (blocks). Script auto-reduces on provider errors.
INIT_CHUNK = int(os.getenv("INIT_CHUNK", "3000"))

# Retry settings
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.8"))


# -----------------------------
# Helpers
# -----------------------------
@dataclass
class SwapRow:
    chain: str
    pool: str
    block_number: int
    log_index: int
    tx_hash: str
    sender: str
    recipient: str
    amount0: int
    amount1: int
    sqrtPriceX96: int
    liquidity: int
    tick: int
    block_ts: int  # unix timestamp


def ensure_dirs(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def connect_rpc(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 60}))
    if not w3.is_connected():
        raise RuntimeError(f"Cannot connect to RPC: {rpc_url}")
    return w3


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS swaps_raw (
        chain TEXT NOT NULL,
        pool  TEXT NOT NULL,
        block_number INTEGER NOT NULL,
        log_index   INTEGER NOT NULL,
        tx_hash TEXT NOT NULL,
        sender TEXT NOT NULL,
        recipient TEXT NOT NULL,
        amount0 TEXT NOT NULL,
        amount1 TEXT NOT NULL,
        sqrtPriceX96 TEXT NOT NULL,
        liquidity TEXT NOT NULL,
        tick INTEGER NOT NULL,
        block_ts INTEGER NOT NULL,
        PRIMARY KEY (pool, block_number, log_index)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_swaps_raw_block ON swaps_raw(pool, block_number);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_swaps_raw_tx    ON swaps_raw(tx_hash);")
    conn.commit()


def univ3_swap_topic(w3: Web3) -> str:
    # Swap(address indexed sender, address indexed recipient,
    #      int256 amount0, int256 amount1, uint160 sqrtPriceX96,
    #      uint128 liquidity, int24 tick)
    sig = "Swap(address,address,int256,int256,uint160,uint128,int24)"
    return "0x" + w3.keccak(text=sig).hex()


def decode_swap_log(w3: Web3, log: Dict) -> Tuple[str, str, int, int, int, int, int]:
    """
    Decode a Uniswap v3 Swap log into:
    sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick

    topics:
      topics[1] = indexed sender (address)
      topics[2] = indexed recipient (address)
    data:
      amount0 (int256), amount1 (int256), sqrtPriceX96 (uint160), liquidity (uint128), tick (int24)
    """
    topics = log["topics"]
    if len(topics) < 3:
        raise LogTopicError("Swap log has insufficient topics")

    sender = Web3.to_checksum_address("0x" + topics[1].hex()[-40:])
    recipient = Web3.to_checksum_address("0x" + topics[2].hex()[-40:])

    data_hex = log["data"]
    if isinstance(data_hex, (bytes, bytearray)):
        data_bytes = bytes(data_hex)
    else:
        # normalize to string
        data_hex = str(data_hex)
        if data_hex.startswith("0x") or data_hex.startswith("0X"):
            data_hex = data_hex[2:]
        # guard: empty or odd length
        if len(data_hex) == 0:
            raise ValueError("empty log data")
        if len(data_hex) % 2 == 1:
            data_hex = "0" + data_hex
        data_bytes = bytes.fromhex(data_hex)

    decoded = w3.codec.decode(
        ["int256", "int256", "uint160", "uint128", "int24"],
        data_bytes
    )
    amount0 = int(decoded[0])
    amount1 = int(decoded[1])
    sqrtPriceX96 = int(decoded[2])
    liquidity = int(decoded[3])
    tick = int(decoded[4])

    return sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick


def get_block_timestamp_cached(w3: Web3, block_number: int, cache: Dict[int, int]) -> int:
    if block_number in cache:
        return cache[block_number]
    blk = w3.eth.get_block(block_number)
    ts = int(blk["timestamp"])
    cache[block_number] = ts
    return ts


def fetch_logs_chunk(
    w3: Web3,
    pool: str,
    topic0: str,
    from_block: int,
    to_block: int,
) -> List[Dict]:
    params = {
        "fromBlock": from_block,
        "toBlock": to_block,
        "address": Web3.to_checksum_address(pool),
        "topics": [topic0],
    }
    return w3.eth.get_logs(params)


def insert_rows(conn: sqlite3.Connection, rows: List[SwapRow]) -> None:
    if not rows:
        return
    conn.executemany(
        """
        INSERT OR IGNORE INTO swaps_raw(
            chain, pool, block_number, log_index, tx_hash,
            sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick, block_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r.chain, r.pool, r.block_number, r.log_index, r.tx_hash,
                r.sender, r.recipient,
                str(r.amount0), str(r.amount1), str(r.sqrtPriceX96), str(r.liquidity),
                r.tick, r.block_ts
            )
            for r in rows
        ],
    )
    conn.commit()


def main() -> None:
    if not POOL_ADDRESS:
        print("ERROR: POOL_ADDRESS is empty.")
        print("Set env POOL_ADDRESS or edit the script. Example (PowerShell):")
        print('  $env:POOL_ADDRESS="0x...."')
        sys.exit(1)

    if FROM_BLOCK <= 0 or TO_BLOCK <= 0 or TO_BLOCK < FROM_BLOCK:
        print("ERROR: Please set valid FROM_BLOCK and TO_BLOCK (env or edit script).")
        print("Example:")
        print('  $env:FROM_BLOCK="123"; $env:TO_BLOCK="456"')
        sys.exit(1)

    ensure_dirs(DB_PATH)

    w3 = connect_rpc(RPC_URL)
    chain_id = w3.eth.chain_id
    chain_name = f"arbitrum_{chain_id}"

    topic0 = univ3_swap_topic(w3)
    pool = Web3.to_checksum_address(POOL_ADDRESS)

    print(f"[OK] Connected RPC: {RPC_URL}")
    print(f"[OK] Chain: {chain_name}, Pool: {pool}")
    print(f"[OK] Swap topic0: {topic0}")
    print(f"[OK] DB: {DB_PATH}")
    print(f"[OK] Range: {FROM_BLOCK} -> {TO_BLOCK}, init_chunk={INIT_CHUNK}")

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    block_ts_cache: Dict[int, int] = {}

    cur = FROM_BLOCK
    chunk = INIT_CHUNK

    total_logs = 0
    t0 = time.time()

    while cur <= TO_BLOCK:
        end = min(cur + chunk - 1, TO_BLOCK)

        retries = 0
        while True:
            try:
                logs = fetch_logs_chunk(w3, pool, topic0, cur, end)
                break
            except Exception as e:
                retries += 1
                msg = str(e)

                # Common provider behavior: 400 Bad Request (too many results / too wide range)
                # Strategy: reduce chunk size.
                if retries <= MAX_RETRIES:
                    if chunk > 50:
                        chunk = max(50, chunk // 2)
                        end = min(cur + chunk - 1, TO_BLOCK)
                        print(f"[WARN] getLogs failed ({msg[:120]}...). Reduce chunk -> {chunk}. Retry {retries}/{MAX_RETRIES}")
                        time.sleep(SLEEP_SECONDS)
                        continue
                    else:
                        print(f"[WARN] getLogs failed even at small chunk={chunk}. Retry {retries}/{MAX_RETRIES}")
                        time.sleep(SLEEP_SECONDS)
                        continue
                else:
                    raise

        rows: List[SwapRow] = []
        for lg in logs:
            try:
                sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick = decode_swap_log(w3, lg)
            except Exception as e:
                print(f"[WARN] decode failed at block={int(lg['blockNumber'])}, logIndex={int(lg['logIndex'])}: {e}")
                continue

            bn = int(lg["blockNumber"])
            li = int(lg["logIndex"])
            txh = lg["transactionHash"].hex()
            ts = get_block_timestamp_cached(w3, bn, block_ts_cache)

            rows.append(
                SwapRow(
                    chain=chain_name,
                    pool=pool,
                    block_number=bn,
                    log_index=li,
                    tx_hash=txh,
                    sender=sender,
                    recipient=recipient,
                    amount0=amount0,
                    amount1=amount1,
                    sqrtPriceX96=sqrtPriceX96,
                    liquidity=liquidity,
                    tick=tick,
                    block_ts=ts,
                )
            )

        insert_rows(conn, rows)
        total_logs += len(rows)

        elapsed = time.time() - t0
        print(f"[OK] blocks {cur}-{end} | swaps={len(rows)} | total={total_logs} | chunk={chunk} | {elapsed:.1f}s")

        cur = end + 1
        time.sleep(0.05)

    conn.close()
    print(f"[DONE] Inserted swaps: {total_logs} into {DB_PATH}")


if __name__ == "__main__":
    main()
