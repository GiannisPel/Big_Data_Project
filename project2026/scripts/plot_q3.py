from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    #Arguments for local plotting script
    parser = argparse.ArgumentParser(
        description="Create Q3 plots from local Spark CSV result tables."
    )
    parser.add_argument(
        "--tables-dir",
        required=True,
        help="Local Q3 tables directory, e.g. project2026/results/tables/q3",
    )
    parser.add_argument(
        "--plots-dir",
        required=True,
        help="Output plots directory, e.g. project2026/results/plots",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of zones to show in each plot.",
    )
    return parser.parse_args()


def find_part_csv(table_dir: Path) -> Path:
    #Spark writes CSV output 'part-*'
    candidates = sorted(table_dir.glob("part-*"))

    if not candidates:
        raise FileNotFoundError(f"No part-* file found in {table_dir}")

    return candidates[0]


def read_spark_csv_table(tables_dir: Path, table_name: str) -> pd.DataFrame:
    #Read one Spark result table into pandas
    table_path = tables_dir / table_name
    csv_path = find_part_csv(table_path)
    return pd.read_csv(csv_path)


def add_zone_label(df: pd.DataFrame) -> pd.DataFrame:
    #one readable label for the plot y
    #Some zones may miss replace with "Unknown"
    df = df.copy()

    df["pickup_borough"] = df["pickup_borough"].fillna("Unknown").astype(str)
    df["pickup_zone"] = df["pickup_zone"].fillna("Unknown").astype(str)
    df["distance_bucket"] = df["distance_bucket"].fillna("Unknown").astype(str)

    df["zone_label"] = (
        df["pickup_borough"]
        + " : "
        + df["pickup_zone"]
        + " - "
        + df["distance_bucket"]
    )

    return df


def plot_horizontal_bar(
    df: pd.DataFrame,
    value_column: str,
    title: str,
    xlabel: str,
    output_path: Path,
    top_n: int,
) -> None:
    #Horizontal bars are easier to read because zone names are long
    df = df.copy()

    #Make sure the metric column is numeric before plotting
    df[value_column] = pd.to_numeric(df[value_column], errors="coerce")

    #Remove rows that cannot be plotted
    df = df.dropna(subset=[value_column, "zone_label"])

    if df.empty:
        raise ValueError(f"No valid rows available for plot: {title}")

    #Keep the top N rows and reverse order so the largest value is on top visually
    plot_df = (
        df.sort_values(value_column, ascending=False)
        .head(top_n)
        .sort_values(value_column, ascending=True)
    )

    y_labels = plot_df["zone_label"].astype(str)

    plt.figure(figsize=(12, 7))
    plt.barh(y_labels, plot_df[value_column])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("Pickup borough / zone / distance bucket")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def main() -> None:
    args = parse_args()

    tables_dir = Path(args.tables_dir)
    plots_dir = Path(args.plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)

    #Use the DF Parquet Q3 outputs for final plots
    total_revenue_df = read_spark_csv_table(
        tables_dir,
        "df_parquet_top_total_revenue",
    )
    revenue_per_mile_df = read_spark_csv_table(
        tables_dir,
        "df_parquet_top_revenue_per_mile",
    )
    revenue_per_minute_df = read_spark_csv_table(
        tables_dir,
        "df_parquet_top_revenue_per_minute",
    )

    total_revenue_df = add_zone_label(total_revenue_df)
    revenue_per_mile_df = add_zone_label(revenue_per_mile_df)
    revenue_per_minute_df = add_zone_label(revenue_per_minute_df)

    #Plot 1: zones with the highest total revenue
    plot_horizontal_bar(
        df=total_revenue_df,
        value_column="total_revenue",
        title="Q3 Top Pickup Zones by Total Revenue",
        xlabel="Total revenue",
        output_path=plots_dir / "q3_top_zones_total_revenue.png",
        top_n=args.top_n,
    )

    #Plot 2: zones with the highest revenue per mile
    plot_horizontal_bar(
        df=revenue_per_mile_df,
        value_column="revenue_per_mile",
        title="Q3 Top Pickup Zones by Revenue per Mile",
        xlabel="Revenue per mile",
        output_path=plots_dir / "q3_top_zones_revenue_per_mile.png",
        top_n=args.top_n,
    )

    #Plot 3: zones with the highest revenue per minute
    plot_horizontal_bar(
        df=revenue_per_minute_df,
        value_column="revenue_per_minute",
        title="Q3 Top Pickup Zones by Revenue per Minute",
        xlabel="Revenue per minute",
        output_path=plots_dir / "q3_top_zones_revenue_per_minute.png",
        top_n=args.top_n,
    )

    print(f"Wrote plots to: {plots_dir}")


if __name__ == "__main__":
    main()