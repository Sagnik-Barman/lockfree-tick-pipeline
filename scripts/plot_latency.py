#!/usr/bin/env python3
"""Renders output/latencies.csv (written by ./tick_pipeline) into a
latency-percentile comparison chart. Needs pandas + matplotlib."""
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("output/latencies.csv")

percentiles = [0.50, 0.90, 0.99, 0.999]
labels = ["p50", "p90", "p99", "p99.9"]

fig, axes = plt.subplots(1, 2, figsize=(12, 5))

# Left panel: percentile bars, lock-free vs mutex, log-scaled y-axis since
# tail latencies routinely run an order of magnitude apart.
pipelines = df["pipeline"].unique()
width = 0.35
x = range(len(labels))
for i, pipeline in enumerate(pipelines):
    sub = df[df["pipeline"] == pipeline]["latency_ns"]
    values = [sub.quantile(p) for p in percentiles]
    offset = (i - (len(pipelines) - 1) / 2) * width
    axes[0].bar([xi + offset for xi in x], values, width, label=pipeline)
axes[0].set_yscale("log")
axes[0].set_xticks(list(x))
axes[0].set_xticklabels(labels)
axes[0].set_ylabel("latency (ns, log scale)")
axes[0].set_title("Tail latency by percentile")
axes[0].legend()
axes[0].grid(axis="y", alpha=0.3)

# Right panel: latency distribution (log-x histogram) so the tail shape --
# not just single percentile points -- is visible.
for pipeline in pipelines:
    sub = df[df["pipeline"] == pipeline]["latency_ns"]
    axes[1].hist(sub, bins=80, alpha=0.6, label=pipeline)
axes[1].set_xscale("log")
axes[1].set_xlabel("latency (ns, log scale)")
axes[1].set_ylabel("count")
axes[1].set_title("Latency distribution")
axes[1].legend()
axes[1].grid(axis="y", alpha=0.3)

fig.suptitle("Lock-free SPSC ring buffer vs. mutex+deque: tick-to-book latency")
fig.tight_layout()
fig.savefig("output/latency_comparison.png", dpi=150)
print("Wrote output/latency_comparison.png")
