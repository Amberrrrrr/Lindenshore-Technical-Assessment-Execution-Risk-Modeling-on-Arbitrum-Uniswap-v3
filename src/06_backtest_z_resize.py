# -*- coding: utf-8 -*-
"""
06_backtest_z_resize.py

Backtest a size-rescaling execution rule:
- Baseline: execute full trade_size_usdc
- Resize rule: cap execution pressure z = size/liquidity_prev at z_cap
  => size_eff = min(size, z_cap * liquidity_prev)

Evaluate adverse slippage cost:
- adverse_slippage = max(slippage, 0)
- cost_usdc = size * adverse_slippage
- cost_eff_usdc = size_eff * adverse_slippage
"""

import os
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = os.getenv("DB_PATH", "data/univ3.db")
TABLE = os.getenv("FEATURE_TABLE", "swaps_features")

TRAIN_FRAC = float(os.getenv("TRAIN_FRAC", "0.6"))

# Choose z_cap as a quantile of TRAIN z (e.g., 0.9 => cap at the 90th percentile)
Z_CAP_Q = float(os.getenv("Z_CAP_Q", "0.9"))

# Or override directly with numeric z_cap
Z_CAP = os.getenv("Z_CAP", "").strip()


def summarize(name: str, s: pd.Series) -> dict:
    s = s.dropna()
    if len(s) == 0:
        return {"name": name, "n": 0}
    return {
        "name": name,
        "n": int(len(s)),
        "mean": float(s.mean()),
        "median": float(s.median()),
        "p95": float(np.quantile(s, 0.95)),
        "p99": float(np.quantile(s, 0.99)),
        "max": float(s.max()),
        "total": float(s.sum()),
    }


def main():
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql(
        f"""
        SELECT
            block_number,
            log_index,
            z,
            slippage,
            trade_size_usdc,
            liquidity_prev
        FROM {TABLE}
        WHERE
            z IS NOT NULL
            AND slippage IS NOT NULL
            AND trade_size_usdc IS NOT NULL
            AND liquidity_prev IS NOT NULL
        ORDER BY block_number ASC, log_index ASC
        """,
        conn,
    )
    conn.close()

    print(f"[OK] Loaded rows: {len(df)} from {DB_PATH}:{TABLE}")

    # Clean
    df = df[np.isfinite(df["z"]) & np.isfinite(df["slippage"]) & np.isfinite(df["trade_size_usdc"])]
    df = df[(df["z"] > 0) & (df["trade_size_usdc"] > 0) & (df["liquidity_prev"] > 0)]
    print(f"[OK] After cleaning: {len(df)} rows")

    # Costs
    df["adverse_slippage"] = df["slippage"].clip(lower=0.0)
    df["cost_usdc"] = df["trade_size_usdc"] * df["adverse_slippage"]

    # Split
    n = len(df)
    split = int(n * TRAIN_FRAC)
    train = df.iloc[:split].copy()
    test = df.iloc[split:].copy()

    # Choose z_cap
    if Z_CAP:
        z_cap = float(Z_CAP)
        src = f"manual Z_CAP={z_cap}"
    else:
        z_cap = float(train["z"].quantile(Z_CAP_Q))
        src = f"train quantile Z_CAP_Q={Z_CAP_Q} -> z_cap={z_cap:.6g}"
    print(f"[OK] z_cap: {src}")

    # Apply resize
    # scale = min(1, z_cap / z)
    test["scale"] = np.minimum(1.0, z_cap / test["z"])
    test["trade_size_eff_usdc"] = test["trade_size_usdc"] * test["scale"]

    # Effective cost under resize (slippage held fixed as simplification)
    test["cost_eff_usdc"] = test["trade_size_eff_usdc"] * test["adverse_slippage"]

    # Execution completion proxy: how much of notional we executed
    exec_fraction = float(test["trade_size_eff_usdc"].sum() / test["trade_size_usdc"].sum())

    # Summaries
    base_stats = summarize("baseline_full_size_cost", test["cost_usdc"])
    eff_stats = summarize("resize_rule_cost_eff", test["cost_eff_usdc"])

    avoided = float(test["cost_usdc"].sum() - test["cost_eff_usdc"].sum())

    print("\n[RESULT] Backtest summary (TEST period):")
    print(pd.DataFrame([base_stats, eff_stats]).to_string(index=False))
    print(f"\n[RESULT] Notional execution fraction (sum size_eff / sum size): {exec_fraction:.2%}")
    print(f"[RESULT] Avoided adverse slippage cost (USDC): {avoided:,.4f}")

    # Plot: cost hist
    os.makedirs("figures", exist_ok=True)

    plt.figure(figsize=(10, 5))
    plt.hist(test["cost_usdc"], bins=50, alpha=0.7, label="baseline cost_usdc")
    plt.hist(test["cost_eff_usdc"], bins=50, alpha=0.7, label="resize cost_eff_usdc")
    plt.yscale("log")
    plt.xlabel("Execution cost (USDC)")
    plt.ylabel("Count (log scale)")
    plt.title("Execution Cost Distribution: Baseline vs Resize Rule (Test)")
    plt.legend()
    plt.tight_layout()
    plt.savefig("figures/backtest_resize_cost_hist.png", dpi=150)
    plt.show()

    # Plot: tail reduction vs cap quantile sweep
    sweep_q = np.linspace(0.5, 0.99, 15)
    rows = []
    for q in sweep_q:
        cap = float(train["z"].quantile(q))
        scale = np.minimum(1.0, cap / test["z"])
        size_eff = test["trade_size_usdc"] * scale
        cost_eff = size_eff * test["adverse_slippage"]
        if len(cost_eff) < 50:
            continue
        rows.append((
            q,
            cap,
            float(np.quantile(cost_eff, 0.99)),
            float(cost_eff.sum()),
            float(size_eff.sum() / test["trade_size_usdc"].sum())
        ))

    sw = pd.DataFrame(rows, columns=["z_cap_quantile", "z_cap", "p99_cost_eff", "total_cost_eff", "notional_exec_frac"])
    sw.to_csv("figures/z_cap_sweep_resize.csv", index=False)

    plt.figure(figsize=(10, 5))
    plt.plot(sw["z_cap"], sw["p99_cost_eff"], marker="o")
    plt.xlabel("z_cap")
    plt.ylabel("p99 effective cost (USDC)")
    plt.title("p99 Cost vs z_cap (Resize Rule)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("figures/z_cap_sweep_resize_p99.png", dpi=150)
    plt.show()

    print("[OK] Saved sweep: figures/z_cap_sweep_resize.csv and figures/z_cap_sweep_resize_p99.png")


if __name__ == "__main__":
    main()
