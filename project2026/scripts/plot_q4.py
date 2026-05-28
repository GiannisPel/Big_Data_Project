from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    #arguments for local plotting
    parser = argparse.ArgumentParser(description="Create Q4 plots from local card_vs_cash table.")
    parser.add_argument("--tables-dir", required=True)
    parser.add_argument("--plots-dir", required=True)
    return parser.parse_args()


def find_part_csv(table_dir: Path) -> Path:
    #spark writes csv outputs as part-* files, so I find that file here
    candidates = sorted(table_dir.glob("part-*"))

    if not candidates:
        raise FileNotFoundError(f"No part-* file found in {table_dir}")

    return candidates[0]


def read_card_vs_cash(tables_dir: Path) -> pd.DataFrame:
    #use the SQL parquet card_vs_cash table as source for plots
    csv_path = find_part_csv(tables_dir / "sql_parquet_card_vs_cash")
    df = pd.read_csv(csv_path)

    #convert columns to the right types before plotting
    df["pickup_hour"] = df["pickup_hour"].astype(str)
    df["card_share"] = pd.to_numeric(df["card_share"], errors="coerce")
    df["avg_tip_rate_card"] = pd.to_numeric(df["avg_tip_rate_card"], errors="coerce")

    return df.sort_values("pickup_hour")


def plot_bar(df: pd.DataFrame, value_column: str, title: str, ylabel: str, output_path: Path) -> None:
    #remove rows where the metric is missing
    plot_df = df.dropna(subset=[value_column]).copy()

    plt.figure(figsize=(9, 6))
    plt.bar(plot_df["pickup_hour"], plot_df[value_column])
    plt.title(title)
    plt.xlabel("Pickup hour")
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def main() -> None:
    args = parse_args()

    tables_dir = Path(args.tables_dir)
    plots_dir = Path(args.plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)

    df = read_card_vs_cash(tables_dir)

    #Plot 1: how much card payments dominate per hour
    plot_bar(
        df=df,
        value_column="card_share",
        title="Q4 Card Share by Pickup Hour",
        ylabel="Card share",
        output_path=plots_dir / "q4_card_share_by_hour.png",
    )

    #Plot 2: average card tip rate per hour
    plot_bar(
        df=df,
        value_column="avg_tip_rate_card",
        title="Q4 Average Card Tip Rate by Pickup Hour",
        ylabel="Average card tip rate",
        output_path=plots_dir / "q4_avg_tip_rate_card_by_hour.png",
    )

    print(f"Wrote Q4 plots to: {plots_dir}")


if __name__ == "__main__":
    main()