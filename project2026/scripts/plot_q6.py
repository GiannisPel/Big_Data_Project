from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Q6 plots.")
    parser.add_argument("--tables-dir", required=True,
                        help="Directory containing Q6 result CSV sub-folders.")
    parser.add_argument("--od-metrics-dir", required=True,
                        help="Directory with q6_od_halves_*_metrics.json files.")
    parser.add_argument("--plots-dir", required=True)
    parser.add_argument("--top-n", type=int, default=15)
    return parser.parse_args()


def find_part_csv(table_dir: Path) -> Path:
    candidates = sorted(table_dir.glob("part-*"))
    if not candidates:
        raise FileNotFoundError(f"No part-* file found in {table_dir}")
    return candidates[0]


def read_table(tables_dir: Path, table_name: str) -> pd.DataFrame:
    return pd.read_csv(find_part_csv(tables_dir / table_name))


def make_zone_label(df: pd.DataFrame) -> pd.DataFrame:
    #Combine zone name + hour into a label for bar charts
    df = df.copy()
    df["zone"]  = df["zone"].fillna("Unknown").astype(str)
    df["label"] = df["zone"] + " (h=" + df["hour"].astype(str) + ")"
    return df


#horizontal bar chart

def plot_horizontal_bar(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    title: str,
    xlabel: str,
    output_path: Path,
    top_n: int,
    color: str = "steelblue",
) -> None:
    df = df.copy()
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col, label_col])

    #Sort descending then reverse so the longest bar is at the top of the chart
    plot_df = (
        df.sort_values(value_col, ascending=False)
          .head(top_n)
          .sort_values(value_col, ascending=True)
    )

    plt.figure(figsize=(12, 7))
    plt.barh(plot_df[label_col].astype(str), plot_df[value_col], color=color)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


#Hourly imbalance Heatmap

def plot_hourly_heatmap(df: pd.DataFrame, output_path: Path, top_n_zones: int = 20) -> None:
    
    #Heatmap:rows = pickup_hour, columns = top zones by mean imbalance_ratio
    #Color: red = more pickups, blue = more dropoffs
    
    df = df.copy()
    df["imbalance_ratio"] = pd.to_numeric(df["imbalance_ratio"], errors="coerce")
    df["zone"] = df["zone"].fillna("Unknown").astype(str)
    df["hour"] = pd.to_numeric(df["hour"], errors="coerce")

    #Keep only the most imbalanced zones
    top_zones = (
        df.groupby("zone")["imbalance_ratio"]
          .apply(lambda x: x.abs().mean())
          .nlargest(top_n_zones)
          .index.tolist()
    )
    filtered = df[df["zone"].isin(top_zones)]
    pivot = filtered.pivot_table(
        index="hour", columns="zone", values="imbalance_ratio", aggfunc="mean"
    )

    fig, ax = plt.subplots(figsize=(max(14, len(top_zones) * 0.8), 7))
    im = ax.imshow(pivot.values, cmap="RdBu", aspect="auto", vmin=-1, vmax=1)

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha="right", fontsize=7)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index.astype(int))

    plt.colorbar(im, ax=ax, label="imbalance_ratio  (+1 = all pickups, -1 = all dropoffs)")
    ax.set_title("Q6 Hourly Imbalance Heatmap (top zones by mean |imbalance_ratio|)")
    ax.set_xlabel("Zone")
    ax.set_ylabel("Pickup Hour")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


#OD Halves chart

def plot_scaling(od_metrics_dir: Path, output_path: Path) -> None:

    #Line chart: elapsed_seconds vs executor instances and reads the metrics JSONs produced by q6_od_halves.py

    records = []
    for f in sorted(od_metrics_dir.glob("q6_od_halves_config*_metrics.json")):
        with open(f) as fh:
            m = json.load(fh)
        records.append({
            "config":    m.get("config", "?"),
            "run_type":  m.get("run_type", "?"),
            "elapsed":   m.get("elapsed_seconds", float("nan")),
            "instances": m.get("executor_config", {}).get("instances", 0),
        })

    if not records:
        print("No OD-Halves metrics JSON files found - skipping scaling plot.")
        return

    df = pd.DataFrame(records).sort_values(["run_type", "instances"])

    plt.figure(figsize=(9, 5))
    for run_type, grp in df.groupby("run_type"):
        plt.plot(grp["instances"], grp["elapsed"], marker="o", label=run_type)
        for _, row in grp.iterrows():
            plt.annotate(
                f"{row['elapsed']:.1f}s",
                (row["instances"], row["elapsed"]),
                textcoords="offset points", xytext=(5, 5), fontsize=8,
            )

    plt.xlabel("Executor Instances")
    plt.ylabel("Elapsed Seconds")
    plt.title("Q6 OD-Halves: Runtime vs Executor Count (cold vs warm)")
    plt.legend()
    plt.xticks([2, 4, 8])
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()

def main() -> None:
    args = parse_args()
    tables_dir     = Path(args.tables_dir)
    od_metrics_dir = Path(args.od_metrics_dir)
    plots_dir      = Path(args.plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)

    #Load the three tables from the DataFrame
    top_positive = make_zone_label(read_table(tables_dir, "df_parquet_top_positive"))
    top_negative = make_zone_label(read_table(tables_dir, "df_parquet_top_negative"))
    top_abs      = make_zone_label(read_table(tables_dir, "df_parquet_top_abs"))
    hourly       = read_table(tables_dir, "df_parquet_hourly_summary")

    #Zones with most pickups
    plot_horizontal_bar(
        df=top_positive, label_col="label", value_col="net_pickups",
        title="Q6 Top Zones: Excess Pickups (net_pickups > 0)",
        xlabel="net_pickups",
        output_path=plots_dir / "q6_top_positive_net_pickups.png",
        top_n=args.top_n, color="steelblue",
    )

    #Zones with most dropoffs
    top_neg_plot = top_negative.copy()
    top_neg_plot["net_pickups"] = pd.to_numeric(top_neg_plot["net_pickups"], errors="coerce")
    top_neg_plot = (
        top_neg_plot.sort_values("net_pickups", ascending=True)
                    .head(args.top_n)
                    .sort_values("net_pickups", ascending=False)
    )
    plt.figure(figsize=(12, 7))
    plt.barh(top_neg_plot["label"].astype(str), top_neg_plot["net_pickups"], color="tomato")
    plt.title("Q6 Top Zones: Excess Dropoffs (net_pickups < 0)")
    plt.xlabel("net_pickups (negative = more dropoffs than pickups)")
    plt.tight_layout()
    plt.savefig(plots_dir / "q6_top_negative_net_pickups.png", dpi=160)
    plt.close()

    #Zones with the highest absolute imbalance ratio
    plot_horizontal_bar(
        df=top_abs, label_col="label", value_col="abs_imbalance_ratio",
        title="Q6 Top Zones: Highest Absolute Imbalance Ratio",
        xlabel="abs_imbalance_ratio",
        output_path=plots_dir / "q6_top_abs_imbalance_ratio.png",
        top_n=args.top_n, color="darkorange",
    )

    #Which hour has the biggest average imbalance across all zones
    hourly["hour"] = pd.to_numeric(hourly["hour"], errors="coerce")
    hourly["mean_abs_imbalance_ratio"] = pd.to_numeric(
        hourly["mean_abs_imbalance_ratio"], errors="coerce"
    )
    hourly_sorted = hourly.sort_values("hour")
    plt.figure(figsize=(12, 5))
    plt.bar(
        hourly_sorted["hour"].astype(int),
        hourly_sorted["mean_abs_imbalance_ratio"],
        color="mediumpurple",
    )
    plt.title("Q6 Mean Absolute Imbalance Ratio by Pickup Hour")
    plt.xlabel("Pickup Hour")
    plt.ylabel("Mean |imbalance_ratio|")
    plt.xticks(range(0, 24))
    plt.tight_layout()
    plt.savefig(plots_dir / "q6_hourly_mean_imbalance.png", dpi=160)
    plt.close()

    #Hourly heatmap, zone x hour color coded by imbalance_ratio
    try:
        plot_hourly_heatmap(top_abs, plots_dir / "q6_hourly_heatmap.png")
    except Exception as e:
        print(f"Skipping hourly heatmap: {e}")

    #Scaling chart from OD Halves metrics JSONs
    plot_scaling(od_metrics_dir, plots_dir / "q6_od_halves_scaling.png")

    print(f"Wrote Q6 plots to: {plots_dir}")


if __name__ == "__main__":
    main()