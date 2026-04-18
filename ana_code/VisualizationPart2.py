# Creates 2 visualization diagrams of dfCIPStats
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load saved datasets to create pandas dfs
dfCIPStats = pd.read_parquet("./Part2/dfCIPStats")

# Helper method to convert HH:mm:ss formatted strings to minutes for plotting
def hms_to_minutes(t):
    h, m, s = map(int, t.split(":"))
    return h * 60 + m + s / 60


# dfCIPStats Diagram 1: Dual y-axis bar chart by Incident Severity Level
dfCIPStats["AVG_MIN"] = dfCIPStats["avg_response_time"].apply(hms_to_minutes)

# X positions
x = np.arange(len(dfCIPStats))
width = 0.3

fig, ax1 = plt.subplots(figsize=(8,6))

# Left axis: Avg response time
ax1.bar(
    x - width/2,
    dfCIPStats["AVG_MIN"],
    width,
    color="skyblue",
    label="Avg Response Time"
)
ax1.set_ylabel("Avg Response Time (minutes)")
ax1.set_xlabel("Incident Severity Level")
ax1.set_xticks(x)
ax1.set_xticklabels(dfCIPStats["CIP_JOBS"], rotation=20)

# Right axis: Num records
ax2 = ax1.twinx()
ax2.bar(
    x + width/2,
    dfCIPStats["num_records"],
    width,
    color="orange",
    label="Number of Incidents"
)
ax2.set_ylabel("Number of Incidents")

# Title
plt.title("2025 NYPD Avg Response Time & Incident Count by Severity Level")

# Add legend
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left")
plt.tight_layout()

# Save plot
plt.savefig("dfCIPStats_diagram.png", dpi=300, bbox_inches="tight")
plt.close()


# dfCIPStats Diagram 2: Stacked bar chart showing Response Time Breakdown by Severity Level
# Convert time columns to minutes
dfCIPStats["DISPATCH_MIN"] = dfCIPStats["avg_dispatch_time"].apply(hms_to_minutes)
dfCIPStats["TRAVEL_MIN"] = dfCIPStats["avg_travel_time"].apply(hms_to_minutes)

# X-axis (Incident Severity Levels)
x = np.arange(len(dfCIPStats))
width = 0.5

fig, ax = plt.subplots(figsize=(7,6))

# Bottom bar: Dispatch time
ax.bar(
    x,
    dfCIPStats["DISPATCH_MIN"],
    width=width,
    color="lightgreen",
    label="Avg Dispatch Time"
)

# Top stacked bar: Travel time
ax.bar(
    x,
    dfCIPStats["TRAVEL_MIN"],
    width=width,
    bottom=dfCIPStats["DISPATCH_MIN"],
    color="skyblue",
    label="Avg Travel Time"
)

# Labels and formatting
ax.set_xlabel("Incident Severity Level")
ax.set_ylabel("Time (minutes)")
ax.set_xticks(x)
ax.set_xticklabels(dfCIPStats["CIP_JOBS"], rotation=20)

plt.title("2025 NYPD Response Time Breakdown by Severity Level")
ax.legend(loc="upper left")

plt.tight_layout()

# Save figure
plt.savefig("cip_severity_stacked_response.png", dpi=300, bbox_inches="tight")
plt.close()