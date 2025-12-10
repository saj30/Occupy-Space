#!/usr/bin/env python3
"""Small visualization helpers for NEO data exports.

Generates 2 PNGs under ./static/ using the exported CSVs:
- static/diameter_hist.png : histogram of diameter_max values
- static/diameter_scatter.png : scatter plot of diameter_min vs diameter_max
"""
import csv
from pathlib import Path
import math

try:
    import matplotlib.pyplot as plt
except Exception as e:
    raise RuntimeError("matplotlib is required to run this script; install with 'pip install matplotlib'") from e


def read_neo_items(path: str = "neo_items.csv"):
    mins = []
    maxs = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            try:
                dmin = float(r.get("diameter_min") or 0)
                dmax = float(r.get("diameter_max") or 0)
            except ValueError:
                continue
            if dmin > 0:
                mins.append(dmin)
            if dmax > 0:
                maxs.append(dmax)
    return mins, maxs


def make_histogram(maxs, out_path: str = "static/diameter_hist.png"):
    Path("static").mkdir(exist_ok=True)
    plt.figure(figsize=(8, 4))
    plt.hist(maxs, bins=40, color="#2b7a78", edgecolor="#17252a")
    plt.title("Distribution of NEO estimated maximum diameters (km)")
    plt.xlabel("Diameter (km)")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def make_scatter(mins, maxs, out_path: str = "static/diameter_scatter.png"):
    Path("static").mkdir(exist_ok=True)
    plt.figure(figsize=(6, 6))
    plt.scatter(mins, maxs, s=12, alpha=0.6)
    plt.title("NEO diameter_min vs diameter_max (km)")
    plt.xlabel("diameter_min (km)")
    plt.ylabel("diameter_max (km)")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


if __name__ == "__main__":
    mins, maxs = read_neo_items()
    if not maxs:
        print("No diameter data available in neo_items.csv — run the data population step first.")
    else:
        make_histogram(maxs)
        # for scatter we align lengths
        n = min(len(mins), len(maxs))
        make_scatter(mins[:n], maxs[:n])
        print("Wrote static/diameter_hist.png and static/diameter_scatter.png")
