from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Q5 plots from local Q5 result tables.")
    parser.add_argument("--tables-dir", required=True)
    parser.add_argument("--plots-dir", required=True)
    parser.add_argument("--top-n", type=int, default=10)
    return parser.parse_args()


def find_part_csv(table_dir: Path) -> Path:
    candidates = sorted(table_dir.glob("part-*"))

    if not candidates:
        raise FileNotFoundError(f"No part-* file found in {table_dir}")

    return candidates[0]


def read_table(tables_dir: Path, table_name: str) -> pd.DataFrame:
    return pd.read_csv(find_part_csv(tables_dir / table_name))


def make_flow_label(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["pu_borough"] = df["pu_borough"].fillna("Unknown").astype(str)
    df["do_borough"] = df["do_borough"].fillna("Unknown").astype(str)
    df["flow_label"] = df["pu_borough"] + " → " + df["do_borough"]
    return df


def make_route_label(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["pu_zone"] = df["pu_zone"].fillna("Unknown").astype(str)
    df["do_zone"] = df["do_zone"].fillna("Unknown").astype(str)
    df["route_label"] = df["pu_zone"] + " → " + df["do_zone"]
    return df


def plot_horizontal_bar(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    title: str,
    xlabel: str,
    output_path: Path,
    top_n: int,
) -> None:
    df = df.copy()
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col, label_col])

    #Sort descending first, then reverse for barh
    plot_df = (
        df.sort_values(value_col, ascending=False)
        .head(top_n)
        .sort_values(value_col, ascending=True)
    )

    plt.figure(figsize=(12, 7))
    plt.barh(plot_df[label_col].astype(str), plot_df[value_col])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def plot_borough_heatmap(
    df: pd.DataFrame,
    value_col: str,
    title: str,
    output_path: Path,
) -> None:
    df = df.copy()
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

    #rows = pickup borough, columns = dropoff borough
    pivot = df.pivot_table(index="pu_borough", columns="do_borough", values=value_col, fill_value=0)

    fig, ax = plt.subplots(figsize=(10, 8))
    im = ax.imshow(pivot.values, cmap="YlOrRd", aspect="auto")

    #labels
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_yticks(range(len(pivot.index)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha="right")
    ax.set_yticklabels(pivot.index)

    # Cell annotations (only for small grids to avoid clutter)
    if pivot.shape[0] <= 8 and pivot.shape[1] <= 8:
        for i in range(pivot.shape[0]):
            for j in range(pivot.shape[1]):
                val = pivot.values[i, j]
                if val > 0:
                    ax.text(j, i, f"{int(val):,}", ha="center", va="center", fontsize=7)

    plt.colorbar(im, ax=ax, label=value_col)
    ax.set_title(title)
    ax.set_xlabel("Drop-off Borough")
    ax.set_ylabel("Pick-up Borough")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def main() -> None:
    args = parse_args()

    tables_dir = Path(args.tables_dir)
    plots_dir = Path(args.plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)

    borough_flows = make_flow_label(
        read_table(tables_dir, "df_parquet_borough_flows")
    )
    airport_routes = make_route_label(
        read_table(tables_dir, "df_parquet_airport_routes")
    )

    #Bar chart 1: Top borough flows by trip count
    plot_horizontal_bar(
        df=borough_flows,
        label_col="flow_label",
        value_col="trips",
        title="Q5 Top Borough-to-Borough Flows by Trips",
        xlabel="Trips",
        output_path=plots_dir / "q5_top_borough_flows_by_trips.png",
        top_n=args.top_n,
    )

    #Bar chart: Top airport routes by trip count
    plot_horizontal_bar(
        df=airport_routes,
        label_col="route_label",
        value_col="trips",
        title="Q5 Top Airport Routes by Trips",
        xlabel="Trips",
        output_path=plots_dir / "q5_top_airport_routes_by_trips.png",
        top_n=args.top_n,
    )

    #Heatmap: trips
    plot_borough_heatmap(
        df=borough_flows,
        value_col="trips",
        title="Q5 Borough-to-Borough Flow Heatmap (Trips)",
        output_path=plots_dir / "q5_borough_flow_heatmap_trips.png",
    )

    #Bar chart 3: Airport routes by avg total amount
    plot_horizontal_bar(
        df=airport_routes,
        label_col="route_label",
        value_col="avg_total_amount",
        title="Q5 Top Airport Routes by Avg Total Amount",
        xlabel="Avg Total Amount ($)",
        output_path=plots_dir / "q5_top_airport_routes_by_avg_amount.png",
        top_n=args.top_n,
    )

    print(f"Wrote Q5 plots to: {plots_dir}")


if __name__ == "__main__":
    main()