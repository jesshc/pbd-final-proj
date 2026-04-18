import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load saved datasets to create pandas dfs
dfBoroughStats = pd.read_parquet("./Part1/dfBoroughStats")
dfBoroughResponse = pd.read_parquet("./Part1/dfBoroughResponse")
dfPrecinctStats = pd.read_parquet("./Part1/dfPrecinctStats")
dfPrecinctResponseRanked = pd.read_parquet("./Part1/dfPrecinctResponseRanked")

# Helper method to convert HH:mm:ss formatted strings to minutes for plotting
def hms_to_minutes(t):
    h, m, s = map(int, t.split(":"))
    return h * 60 + m + s / 60


# Part 1: dfBoroughStats Visualization
# Create a dual y-axis bar chart
dfBoroughStats["AVG_MIN"] = dfBoroughStats["AVG_RESPONSE_TIME"].apply(hms_to_minutes)

# X positions
x = np.arange(len(dfBoroughStats))
width = 0.35

fig, ax1 = plt.subplots(figsize=(10,6))

# Left axis: Avg response time
ax1.bar(
    x - width/2,
    dfBoroughStats["AVG_MIN"],
    width,
    color="skyblue",
    label="Avg Response Time"
)
ax1.set_ylabel("Avg Response Time (minutes)")
ax1.set_xlabel("Borough")
ax1.set_xticks(x)
ax1.set_xticklabels(dfBoroughStats["BORO_NM"], rotation=30)

# Right axis: Num records
ax2 = ax1.twinx()
ax2.bar(
    x + width/2,
    dfBoroughStats["NUM_RECORDS"],
    width,
    color="orange",
    label="Number of Incidents"
)
ax2.set_ylabel("Number of Incidents")

# Title
plt.title("2025 NYPD Avg Response Time & Incident Count by Borough")

# Add legend
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()

ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper right")

plt.tight_layout()

# Save plot
plt.savefig("dfBoroughStats_diagram.png", dpi=300, bbox_inches="tight")
plt.close()


# Part 2: dfBoroughResponse Visualization
# Create a multiple bar chart

# Step 1: Convert time to minutes
df = dfBoroughResponse.copy()

df["AVG_RESPONSE_TIME_MIN"] = pd.to_timedelta(
    df["AVG_RESPONSE_TIME"]
).dt.total_seconds() / 60

# Step 2: Create combined category
df["INCIDENT_PAIR"] = df["TYP_DESC"] + " | " + df["CIP_JOBS"]

# Step 3: Define CIP order
cip_order = ["CRITICAL", "SERIOUS", "NON CRITICAL"]

df["CIP_JOBS"] = pd.Categorical(df["CIP_JOBS"], categories=cip_order, ordered=True)

# Sort properly
df = df.sort_values(by=["CIP_JOBS", "TYP_DESC"])

# Step 4: Pivot
pivot = df.pivot_table(
    index="INCIDENT_PAIR",
    columns="BORO_NM",
    values="AVG_RESPONSE_TIME_MIN",
    aggfunc="mean"
)

# Preserve sorted order
pivot = pivot.loc[
    df[["INCIDENT_PAIR"]]
    .drop_duplicates()
    .set_index("INCIDENT_PAIR")
    .index
]

# Step 5: Plot
x = np.arange(len(pivot.index))
bar_width = 0.15

fig, ax = plt.subplots(figsize=(18, 7))

boroughs = pivot.columns

for i, borough in enumerate(boroughs):
    ax.bar(
        x + i * bar_width,
        pivot[borough],
        width=bar_width,
        label=borough
    )

# Labels
ax.set_xticks(x + bar_width * 2)
ax.set_xticklabels(pivot.index, rotation=45, ha="right")
ax.set_ylabel("Avg Response Time (minutes)")
ax.set_xlabel("Incident Type + CIP Level")
ax.set_title("Avg Response Time by Borough (Minutes)")
ax.legend(title="Borough")

plt.tight_layout()
plt.savefig("avg_response_time_by_borough.png", dpi=300, bbox_inches="tight")
plt.close()

# Part 3: dfPrecinctStats Visualization
# Create a scatter plot of Avg Response Time vs Num Records
dfPrecinctStats["AVG_MIN"] = dfPrecinctStats["AVG_RESPONSE_TIME"].apply(hms_to_minutes)
plt.figure(figsize=(10, 6))

for borough in dfPrecinctStats["BORO_NM"].unique():
    subset = dfPrecinctStats[dfPrecinctStats["BORO_NM"] == borough]
    plt.scatter(
        subset["NUM_RECORDS"],
        subset["AVG_MIN"],
        label=borough,
        alpha=0.9
    )

plt.xlabel("Number of Incidents")
plt.ylabel("Avg Response Time (minutes)")
plt.title("2025 NYPD Avg Response Time vs Incident Count by Precinct")

plt.legend(title="Borough")
plt.tight_layout()
plt.savefig("dfPrecinctStats_scatter.png", dpi=300, bbox_inches="tight")
plt.close()
# print(len(dfPrecinctStats)) # 78 entries in dfPrecinctStats


# Part 4: dfPrecinctResponseRanked Visualization
# Create subplots to highlight performance differences across
# precincts for every (TYP_DESC, CIP_JOBS) incident type
dfPrecinctResponseRanked["AVG_MIN"] = dfPrecinctResponseRanked["AVG_RESPONSE_TIME"].apply(hms_to_minutes)

# Get unique (TYP_DESC, CIP_JOBS) combinations
groups = dfPrecinctResponseRanked[["TYP_DESC", "CIP_JOBS"]].drop_duplicates()

n = len(groups)
cols = 2
rows = (n + 1) // cols

fig, axes = plt.subplots(rows, cols, figsize=(14, 5 * rows))
axes = axes.flatten()

# Loop through each subplot
for i, (_, row) in enumerate(groups.iterrows()):
    typ = row["TYP_DESC"]
    cip = row["CIP_JOBS"]

    subset = dfPrecinctResponseRanked[
        (dfPrecinctResponseRanked["TYP_DESC"] == typ) &
        (dfPrecinctResponseRanked["CIP_JOBS"] == cip)
        ]

    subset = subset.sort_values("AVG_MIN")

    colors = subset["GROUP_TYPE"].map({
        "TOP_5_FASTEST": "green",
        "BOTTOM_5_SLOWEST": "red"
    })

    ax = axes[i]

    ax.bar(
        subset["NYPD_PCT_CD"].astype(str),
        subset["AVG_MIN"],
        color=colors
    )

    ax.set_title(f"{typ} ({cip})")
    ax.set_xlabel("Precinct")
    ax.set_ylabel("Avg Response Time (min)")
    ax.tick_params(axis='x', rotation=45)

# Remove unused subplots
for j in range(i + 1, len(axes)):
    fig.delaxes(axes[j])

plt.tight_layout()
plt.savefig("precinct_response_subplots.png", dpi=300, bbox_inches="tight")
plt.close()