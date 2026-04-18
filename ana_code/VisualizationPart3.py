import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load data
dfHourlyStatsNonCritical = pd.read_parquet("./Part3/dfHourlyStatsNonCritical")
dfHourlyStatsHighSeverity = pd.read_parquet("./Part3/dfHourlyStatsHighSeverity")

# Helper method to convert HH:mm:ss formatted strings to minutes for plotting
def hms_to_minutes(t):
    h, m, s = map(int, t.split(":"))
    return h * 60 + m + s / 60

# Generates a dual-axis chart showing hourly incident
# volume (line) and stacked response time components (bars)
def plot_hourly_response(df, title, output_file):
    # Convert time columns
    df["DISPATCH_MIN"] = df["avg_dispatch_delay_time"].apply(hms_to_minutes)
    df["TRAVEL_MIN"] = df["avg_travel_time"].apply(hms_to_minutes)

    # X-axis (0–23 hours)
    x = df["hour_of_day"]

    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Right Y-AXIS represents TIME (stacked bars)
    ax2 = ax1.twinx()

    # Stacked bars (background)
    ax2.bar(x, df["DISPATCH_MIN"], color="lightgreen", label="Dispatch Delay", zorder=1)
    ax2.bar(
        x,
        df["TRAVEL_MIN"],
        bottom=df["DISPATCH_MIN"],
        color="skyblue",
        label="Travel Time",
        zorder=1
    )

    ax2.set_ylabel("Time (minutes)")

    # Left Y-AXIS represents INCIDENT COUNT (line)
    ax1.plot(
        x,
        df["num_records"],
        color="orange",
        marker="o",
        linewidth=2.5,
        markersize=6,
        label="Number of Incidents",
        zorder=3
    )

    ax1.set_ylabel("Number of Incidents")
    ax1.set_xlabel("Hour of Day")

    # Ensure line appears above bars
    ax1.set_zorder(ax2.get_zorder() + 1)
    ax1.patch.set_visible(False)

    # Title
    plt.title(title)

    # Combine legends from both axes
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left")

    # Formatting
    plt.xticks(range(0, 24))
    plt.tight_layout()

    # Save output
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()


# -- Generate Visualizations --

# Plot Non-Critical Incidents
plot_hourly_response(
    dfHourlyStatsNonCritical,
    "Hourly NYPD Response Breakdown (Non-Critical Incidents)",
    "hourly_response_non_critical.png"
)

# Plot High Severity Incidents
plot_hourly_response(
    dfHourlyStatsHighSeverity,
    "Hourly NYPD Response Breakdown (High Severity Incidents)",
    "hourly_response_high_severity.png"
)