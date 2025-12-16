# -*- coding: utf-8 -*-
"""
04_tail_risk_bins.py
Quantify right-tail slippage risk conditioned on z = trade_size / liquidity_prev
"""

import os
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DB_PATH = os.getenv("DB_PATH", "data/univ3.db")
TABLE = "swaps_features"
N_BINS = 10  # quantile bins

def main():
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql(
        f"""
        SELECT
            z,
            slippage
        FROM {TABLE}
        WHERE
            z IS NOT NULL
            AND slippage IS NOT NULL
        """,
        conn
    )

    conn.close()

    print(f"[OK] Loaded rows: {len(df)}")

    # Drop extreme invalid values (safety)
    df = df[(df["z"] > 0) & np.isfinite(df["slippage"])]

    # Quantile binning on z
    df["z_bin"] = pd.qcut(df["z"], q=N_BINS, duplicates="drop")

    # Aggregate tail statistics
    agg = (
        df.groupby("z_bin")["slippage"]
        .agg(
            count="count",
            p95=lambda x: np.quantile(x, 0.95),
            p99=lambda x: np.quantile(x, 0.99),
            max="max"
        )
        .reset_index()
    )

    print("\n[RESULT] Tail risk by z-bin:")
    print(agg)

    # Plot
    plt.figure(figsize=(10, 5))
    plt.plot(agg["z_bin"].astype(str), agg["p99"], marker="o", label="99% slippage")
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Slippage (fraction)")
    plt.xlabel("z-bin (trade_size / liquidity_prev)")
    plt.title("Right-Tail Slippage Risk vs Execution Pressure (z)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    os.makedirs("figures", exist_ok=True)
    plt.savefig("figures/slippage_tail_by_z.png", dpi=150)
    plt.show()

    print("\n[OK] Figure saved to figures/slippage_tail_by_z.png")

if __name__ == "__main__":
    main()
