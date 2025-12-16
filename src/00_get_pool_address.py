# -*- coding: utf-8 -*-
"""
00_get_pool_address.py
Get Uniswap v3 pool address via Factory.getPool(tokenA, tokenB, fee) on Arbitrum.

Fee tiers:
- 0.05% -> 500
- 0.30% -> 3000
- 1.00% -> 10000
"""

import os
from web3 import Web3

RPC_URL = os.getenv("ARB_RPC_URL", "https://arb1.arbitrum.io/rpc")

# ---------- Arbitrum token addresses (commonly used) ----------
# USDC (native on Arbitrum, 6 decimals) - commonly: 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8
USDC = os.getenv("USDC", "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8")

# WETH (18 decimals) - commonly: 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1
WETH = os.getenv("WETH", "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")

# Uniswap v3 Factory on Arbitrum (commonly): 0x1F98431c8aD98523631AE4a59f267346ea31F984
# (Same as mainnet factory address; Uniswap v3 deployments reuse it across some chains)
FACTORY = os.getenv("UNIV3_FACTORY", "0x1F98431c8aD98523631AE4a59f267346ea31F984")

FEE = int(os.getenv("FEE", "500"))  # 0.05%

FACTORY_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "tokenA", "type": "address"},
            {"internalType": "address", "name": "tokenB", "type": "address"},
            {"internalType": "uint24", "name": "fee", "type": "uint24"},
        ],
        "name": "getPool",
        "outputs": [{"internalType": "address", "name": "pool", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    }
]


def main():
    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 60}))
    if not w3.is_connected():
        raise RuntimeError(f"Cannot connect to RPC: {RPC_URL}")

    usdc = Web3.to_checksum_address(USDC)
    weth = Web3.to_checksum_address(WETH)
    factory = Web3.to_checksum_address(FACTORY)

    c = w3.eth.contract(address=factory, abi=FACTORY_ABI)

    # token order doesn't matter for getPool, but we call once
    pool = c.functions.getPool(usdc, weth, FEE).call()
    pool = Web3.to_checksum_address(pool)

    print("RPC:", RPC_URL)
    print("ChainId:", w3.eth.chain_id)
    print("Factory:", factory)
    print("USDC:", usdc)
    print("WETH:", weth)
    print("Fee:", FEE)
    print("Pool:", pool)

    if int(pool, 16) == 0:
        print("[ERROR] getPool returned address(0). This fee tier may not exist on this chain.")
        return

    # Quick sanity check: try to fetch recent Swap logs from the pool (last ~50k blocks)
    swap_topic = "0x" + w3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
    latest = w3.eth.block_number
    from_block = max(0, latest - 50_000)

    try:
        logs = w3.eth.get_logs({
            "fromBlock": from_block,
            "toBlock": latest,
            "address": pool,
            "topics": [swap_topic],
        })
        print(f"[OK] Swap logs found in last 50k blocks: {len(logs)}")
    except Exception as e:
        print("[WARN] Could not fetch swap logs for sanity check (provider limits possible):")
        print(" ", str(e)[:200])


if __name__ == "__main__":
    main()
