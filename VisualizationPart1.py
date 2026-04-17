import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# Part 1: dfBoroughStats Visualization: creates a dual y-axis bar chart
dfBoroughStats = pd.read_parquet("./Part1/dfBoroughStats")
print(dfBoroughStats.to_string())

# Helper method to convert HH:mm:ss to minutes
def hms_to_minutes(t):
    h, m, s = map(int, t.split(":"))
    return h * 60 + m + s / 60


dfBoroughStats["AVG_MIN"] = dfBoroughStats["AVG_RESPONSE_TIME"].apply(hms_to_minutes)

# X positions
x = np.arange(len(dfBoroughStats))
width = 0.4

fig, ax1 = plt.subplots(figsize=(10,6))

# Left axis: Avg response time
ax1.bar(x - width/2, dfBoroughStats["AVG_MIN"], width, color="skyblue")
ax1.set_ylabel("Avg Response Time (minutes)")
ax1.set_xlabel("Borough")
ax1.set_xticks(x)
ax1.set_xticklabels(dfBoroughStats["BORO_NM"], rotation=30)

# Right axis: Num records
ax2 = ax1.twinx()
ax2.bar(x + width/2, dfBoroughStats["NUM_RECORDS"], width, color="orange")
ax2.set_ylabel("Number of Records")

plt.title("NYPD Avg Response Time & Incident Count by Borough")
plt.tight_layout()

# Save plot
plt.savefig("dfBoroughStats_diagram.png", dpi=300, bbox_inches="tight")
plt.close()


# Part 2: dfBoroughResponse Visualization
dfBoroughResponse = pd.read_parquet("./Part1/dfBoroughResponse")
# dfBoroughResponse.sort_values(
#     by=["TYP_DESC", "CIP_JOBS", "AVG_RESPONSE_TIME"]
# ).head(55)

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