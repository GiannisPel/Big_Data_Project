from __future__ import annotations

import argparse
import os
import glob
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")                   #non interactive backend, safe on headless servers
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def read_spark_csv_dir(directory: str) -> pd.DataFrame:
    
    #Spark writes a directory with one or more part-*.csv files and a header.
    
    pattern = os.path.join(directory, "part-*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"No part-*.csv files found in '{directory}'. "
            "Make sure you ran `hdfs dfs -get` first."
        )
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


def save(fig: plt.Figure, plots_dir: str, filename: str) -> None:
    out = Path(plots_dir) / filename
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot_eda] Saved {out}")

def plot_pickup_hour_distribution(tables_dir: str, plots_dir: str) -> None:
    
    #Trips per pickup_hour (0-23) for 2015 and 2024 on a single chart

    df = read_spark_csv_dir(os.path.join(tables_dir, "pickup_hour_distribution"))
    df["pickup_hour"] = df["pickup_hour"].astype(int)
    df["trip_count"]  = df["trip_count"].astype(int)

    datasets = sorted(df["dataset"].unique())
    hours    = list(range(24))

    fig, ax = plt.subplots(figsize=(12, 5))

    colors = {"yellow_tripdata_2015": "#1f77b4", "yellow_tripdata_2024": "#ff7f0e"}
    for ds in datasets:
        sub   = df[df["dataset"] == ds].set_index("pickup_hour").reindex(hours, fill_value=0)
        label = "2015" if "2015" in ds else "2024"
        ax.plot(hours, sub["trip_count"], marker="o", linewidth=1.8,
                color=colors.get(ds, None), label=label)

    ax.set_xlabel("Pickup hour (0 – 23)")
    ax.set_ylabel("Number of trips")
    ax.set_title("Trip count per pickup hour — 2015 vs 2024 (full year, year filter only)")
    ax.set_xticks(hours)
    ax.legend(title="Dataset")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    save(fig, plots_dir, "pickup_hour_distribution.png")


def plot_pickup_day_distribution_2024(tables_dir: str, plots_dir: str) -> None:
    
    #Trips per day of month for the 2024 dataset.

    df = read_spark_csv_dir(os.path.join(tables_dir, "pickup_day_distribution_2024"))
    df["pickup_day"] = df["pickup_day"].astype(int)
    df["trip_count"] = df["trip_count"].astype(int)
    df = df.sort_values("pickup_day")

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.bar(df["pickup_day"], df["trip_count"], color="#ff7f0e", edgecolor="white", linewidth=0.4)
    ax.set_xlabel("Day of month")
    ax.set_ylabel("Number of trips")
    ax.set_title("Trip count per day of month — 2024 (full year, year filter only)")
    ax.set_xticks(range(1, 32))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    save(fig, plots_dir, "pickup_day_distribution_2024.png")


def _render_log_histogram(
    df: pd.DataFrame,
    column_label: str,
    title: str,
    negative_note: str | None,
    plots_dir: str,
    filename: str,
) -> None:
    """
    Shared renderer for the two log-scale histograms (trip_distance, total_amount).
    Each bucket on the x-axis represents [10^k, 10^(k+1)).
    """
    datasets = sorted(df["dataset"].unique())
    all_buckets = sorted(df["log10_floor_bucket"].unique())

    fig, ax = plt.subplots(figsize=(10, 5))

    width   = 0.35
    offsets = [-width / 2, width / 2] if len(datasets) == 2 else [0]
    colors  = ["#1f77b4", "#ff7f0e"]

    for i, ds in enumerate(datasets):
        sub    = df[df["dataset"] == ds].set_index("log10_floor_bucket")["count"]
        counts = [sub.get(b, 0) for b in all_buckets]
        label  = "2015" if "2015" in ds else "2024"
        x_pos  = [b + offsets[i] for b in all_buckets]
        ax.bar(x_pos, counts, width=width, label=label,
               color=colors[i], edgecolor="white", linewidth=0.4)

    ax.set_yscale("log")
    ax.set_xlabel(f"log₁₀ floor bucket  (bucket k → [{column_label} in [10^k, 10^(k+1)))")
    ax.set_ylabel("Number of trips (log scale)")
    ax.set_title(title)
    ax.set_xticks(all_buckets)
    ax.set_xticklabels([f"10^{b}" for b in all_buckets], rotation=45, ha="right")
    ax.legend(title="Dataset")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    if negative_note:
        ax.annotate(
            negative_note,
            xy=(0.02, 0.97), xycoords="axes fraction",
            fontsize=8, va="top",
            bbox=dict(boxstyle="round,pad=0.3", fc="lightyellow", ec="gray", alpha=0.8),
        )

    save(fig, plots_dir, filename)


def plot_trip_distance_log_histogram(tables_dir: str, plots_dir: str) -> None:
    
    #Log scale histogram of trip_distance for 2015 and 2024.
    df = read_spark_csv_dir(os.path.join(tables_dir, "trip_distance_log_histogram"))
    df["log10_floor_bucket"] = df["log10_floor_bucket"].astype(int)
    df["count"]              = df["count"].astype(int)

    _render_log_histogram(
        df,
        column_label="trip_distance (miles)",
        title="Trip distance distribution — log scale, 2015 vs 2024",
        negative_note=None,
        plots_dir=plots_dir,
        filename="trip_distance_log_histogram.png",
    )


def plot_total_amount_log_histogram(tables_dir: str, plots_dir: str,
                                    negative_counts: dict | None = None) -> None:
    
    #Log scale histogram of total_amount for 2015 and 2024 and negative values are excluded
    df = read_spark_csv_dir(os.path.join(tables_dir, "total_amount_log_histogram"))
    df["log10_floor_bucket"] = df["log10_floor_bucket"].astype(int)
    df["count"]              = df["count"].astype(int)

    note = None
    if negative_counts:
        lines = ["Negative total_amount (excluded from log buckets):"]
        for entry in negative_counts:
            if entry.get("column_name") == "total_amount":
                n = entry["negative_count"]
                ds = "2015" if "2015" in entry["dataset"] else "2024"
                lines.append(f"  {ds}: {n:,} rows")
        note = "\n".join(lines)

    _render_log_histogram(
        df,
        column_label="total_amount ($)",
        title="Total amount distribution — log scale, 2015 vs 2024",
        negative_note=note,
        plots_dir=plots_dir,
        filename="total_amount_log_histogram.png",
    )


def plot_top_10_pickup_zones(tables_dir: str, plots_dir: str) -> None:
    
    #Horizontal bar chart of the 10 most common pickup zones in 2024

    df = read_spark_csv_dir(os.path.join(tables_dir, "top_10_pickup_zones_2024"))
    df["trip_count"] = df["trip_count"].astype(int)
    df = df.sort_values("trip_count", ascending=True)

    labels = df.apply(
        lambda r: f"{r['zone']} ({r['borough']})" if pd.notna(r.get("zone")) else str(r["pu_location_id"]),
        axis=1,
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(labels, df["trip_count"], color="#2ca02c", edgecolor="white", linewidth=0.4)
    ax.set_xlabel("Number of trips")
    ax.set_title("Top-10 pickup zones by trip count — 2024 (full year, year filter only)")
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(axis="x", linestyle="--", alpha=0.4)

    save(fig, plots_dir, "top_10_pickup_zones_2024.png")


def plot_null_percentages(tables_dir: str, plots_dir: str) -> None:
    
    #Heatmap of null percentage per dataset, column
    df = read_spark_csv_dir(os.path.join(tables_dir, "null_percentages"))
    df["null_percentage"] = df["null_percentage"].astype(float)

    pivot = df.pivot_table(
        index="column_name", columns="dataset", values="null_percentage", fill_value=0.0
    )

    fig_height = max(5, len(pivot) * 0.35)
    fig, ax = plt.subplots(figsize=(max(6, len(pivot.columns) * 2.5), fig_height))

    im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd", vmin=0, vmax=100)
    cbar = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_label("Null %")

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([c.replace("yellow_tripdata_", "") for c in pivot.columns],
                       rotation=20, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=7)

    for row_idx in range(pivot.shape[0]):
        for col_idx in range(pivot.shape[1]):
            val = pivot.values[row_idx, col_idx]
            ax.text(col_idx, row_idx, f"{val:.1f}%",
                    ha="center", va="center", fontsize=6,
                    color="black" if val < 60 else "white")

    ax.set_title("Null percentage per column per dataset")
    fig.tight_layout()

    save(fig, plots_dir, "null_percentages.png")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate EDA PNG plots from the CSV tables produced by eda.py. "
            "Run this locally after copying the tables from HDFS with hdfs dfs -get."
        )
    )
    parser.add_argument(
        "--tables-dir",
        required=True,
        help="Local directory containing the EDA CSV table sub-directories "
             "(e.g. ./eda_tables, which must contain pickup_hour_distribution/, etc.).",
    )
    parser.add_argument(
        "--plots-dir",
        required=True,
        help="Local directory where PNG files will be written (created if absent).",
    )
    parser.add_argument(
        "--negative-counts-json",
        default=None,
        help=(
            "Optional path to eda_metrics.json (local copy). When provided, the "
            "total_amount histogram will be annotated with the exact negative row counts."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    Path(args.plots_dir).mkdir(parents=True, exist_ok=True)

    negative_counts = None
    if args.negative_counts_json:
        import json
        with open(args.negative_counts_json, encoding="utf-8") as fh:
            metrics = json.load(fh)
        negative_counts = metrics.get("negative_zero_counts")

    print("[plot_eda] Generating pickup hour distribution plot …")
    plot_pickup_hour_distribution(args.tables_dir, args.plots_dir)

    print("[plot_eda] Generating pickup day distribution plot (2024) …")
    plot_pickup_day_distribution_2024(args.tables_dir, args.plots_dir)

    print("[plot_eda] Generating trip distance log histogram …")
    plot_trip_distance_log_histogram(args.tables_dir, args.plots_dir)

    print("[plot_eda] Generating total amount log histogram …")
    plot_total_amount_log_histogram(args.tables_dir, args.plots_dir, negative_counts)

    print("[plot_eda] Generating top-10 pickup zones chart …")
    plot_top_10_pickup_zones(args.tables_dir, args.plots_dir)

    print("[plot_eda] Generating null percentages heatmap …")
    plot_null_percentages(args.tables_dir, args.plots_dir)

    print("[plot_eda] All plots saved.")


if __name__ == "__main__":
    main()