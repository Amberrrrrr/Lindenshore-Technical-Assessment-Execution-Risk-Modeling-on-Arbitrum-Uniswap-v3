# -*- coding: utf-8 -*-
"""
07_backtest_rolling_z_cap.py

Rolling (adaptive) z_cap execution rule:
- For each trade t, estimate z_cap(t) from the previous W trades' z values
- Cap execution pressure: scale_t = min(1, z_cap(t) / z_t)
- Execute size_eff = size * scale_t
- Evaluate adverse slippage cost: cost_eff = size_eff * max(slippage,0)

Also compare to:
- Baseline: full size
- Static resize: fixed z_cap from TRAIN quantile (optional)

Input: SQLite table swaps_features (from 03_build_features.py)
"""

import os
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = os.getenv("DB_PATH", "data/univ3.db")
TABLE = os.getenv("FEATURE_TABLE", "swaps_features")

TRAIN_FRAC = float(os.getenv("TRAIN_FRAC", "0.6"))

# Rolling params
ROLL_W = int(os.getenv("ROLL_W", "800"))      # rolling window length in #swaps
ROLL_Q = float(os.getenv("ROLL_Q", "0.9"))    # quantile for z_cap(t)

# Static cap for comparison (train quantile)
STATIC_Q = float(os.getenv("STATIC_Q", "0.9"))


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
    df = df.reset_index(drop=True)
    print(f"[OK] After cleaning: {len(df)} rows")

    # Costs
    df["adverse_slippage"] = df["slippage"].clip(lower=0.0)
    df["cost_usdc"] = df["trade_size_usdc"] * df["adverse_slippage"]

    # Split train/test by time
    n = len(df)
    split = int(n * TRAIN_FRAC)
    train = df.iloc[:split].copy()
    test = df.iloc[split:].copy().reset_index(drop=True)

    if len(test) < ROLL_W + 50:
        print(f"[WARN] Test set small relative to ROLL_W={ROLL_W}. Consider more swaps or reduce ROLL_W.")

    # ---------- Static cap (for comparison) ----------
    z_cap_static = float(train["z"].quantile(STATIC_Q))
    print(f"[OK] Static z_cap from train quantile {STATIC_Q}: {z_cap_static:.6g}")

    test["scale_static"] = np.minimum(1.0, z_cap_static / test["z"])
    test["size_eff_static"] = test["trade_size_usdc"] * test["scale_static"]
    test["cost_eff_static"] = test["size_eff_static"] * test["adverse_slippage"]

    # ---------- Rolling cap (walk-forward) ----------
    # rolling quantile using ONLY past data in test:
    # z_cap_roll[t] computed from z[t-ROLL_W : t-1]
    z_vals = test["z"].to_numpy()

    z_cap_roll = np.full(len(test), np.nan, dtype=float)
    for t in range(ROLL_W, len(test)):
        window = z_vals[t-ROLL_W:t]
        z_cap_roll[t] = np.quantile(window, ROLL_Q)

    test["z_cap_roll"] = z_cap_roll
    test["scale_roll"] = np.minimum(1.0, test["z_cap_roll"] / test["z"])
    test.loc[test["z_cap_roll"].isna(), "scale_roll"] = np.nan  # not defined before enough history

    test["size_eff_roll"] = test["trade_size_usdc"] * test["scale_roll"]
    test["cost_eff_roll"] = test["size_eff_roll"] * test["adverse_slippage"]

    # Only evaluate rolling where z_cap_roll is defined
    eval_mask = test["z_cap_roll"].notna()
    base_cost = test.loc[eval_mask, "cost_usdc"]
    static_cost = test.loc[eval_mask, "cost_eff_static"]
    roll_cost = test.loc[eval_mask, "cost_eff_roll"]

    # Execution fractions
    exec_frac_static = float(test.loc[eval_mask, "size_eff_static"].sum() / test.loc[eval_mask, "trade_size_usdc"].sum())
    exec_frac_roll = float(test.loc[eval_mask, "size_eff_roll"].sum() / test.loc[eval_mask, "trade_size_usdc"].sum())

    avoided_static = float(base_cost.sum() - static_cost.sum())
    avoided_roll = float(base_cost.sum() - roll_cost.sum())

    # Summaries
    res = pd.DataFrame([
        summarize("baseline_full_size_cost", base_cost),
        summarize(f"static_resize_cost (train q={STATIC_Q})", static_cost),
        summarize(f"rolling_resize_cost (W={ROLL_W}, q={ROLL_Q})", roll_cost),
    ])
    print("\n[RESULT] Backtest summary (TEST, rolling-evaluable region):")
    print(res.to_string(index=False))

    print(f"\n[RESULT] Static notional exec fraction:  {exec_frac_static:.2%}")
    print(f"[RESULT] Rolling notional exec fraction: {exec_frac_roll:.2%}")
    print(f"[RESULT] Avoided cost (static):  {avoided_static:,.4f} USDC")
    print(f"[RESULT] Avoided cost (rolling): {avoided_roll:,.4f} USDC")

    # Plots
    os.makedirs("figures", exist_ok=True)

    plt.figure(figsize=(10, 5))
    plt.hist(base_cost, bins=50, alpha=0.6, label="baseline")
    plt.hist(static_cost, bins=50, alpha=0.6, label="static resize")
    plt.hist(roll_cost, bins=50, alpha=0.6, label="rolling resize")
    plt.yscale("log")
    plt.xlabel("Execution cost (USDC)")
    plt.ylabel("Count (log scale)")
    plt.title("Execution Cost Distribution: baseline vs static vs rolling (test)")
    plt.legend()
    plt.tight_layout()
    plt.savefig("figures/backtest_rolling_cost_hist.png", dpi=150)
    plt.show()

    # Plot rolling z_cap path (helps tell the story)
    plt.figure(figsize=(10, 4))
    plt.plot(test.loc[eval_mask, "z_cap_roll"].to_numpy(), linewidth=1.0)
    plt.axhline(z_cap_static, linestyle="--", label=f"static z_cap (train q={STATIC_Q})")
    plt.xlabel("Trade index (test, after warmup)")
    plt.ylabel("z_cap")
    plt.title(f"Rolling z_cap over time (W={ROLL_W}, q={ROLL_Q})")
    plt.legend()
    plt.tight_layout()
    plt.savefig("figures/rolling_z_cap_path.png", dpi=150)
    plt.show()

    # Save csv for report
    out = test.loc[:, ["block_number", "log_index", "z", "trade_size_usdc", "adverse_slippage",
                       "z_cap_roll", "scale_roll", "size_eff_roll", "cost_eff_roll"]]
    out.to_csv("figures/rolling_rule_outputs.csv", index=False)
    print("[OK] Saved: figures/backtest_rolling_cost_hist.png, figures/rolling_z_cap_path.png, figures/rolling_rule_outputs.csv")


if __name__ == "__main__":
    main()
