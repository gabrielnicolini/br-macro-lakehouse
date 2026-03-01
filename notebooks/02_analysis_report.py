# Databricks notebook source
import sys

sys.path.append("../src")

from br_macro_lakehouse.config import LakehouseConfig

cfg = LakehouseConfig()

pdf = spark.table(cfg.fqtn(cfg.gold_mart_monthly)).toPandas().sort_values("month")

import matplotlib.pyplot as plt

fig = plt.figure()
plt.plot(pdf["month"], pdf["ipca_yoy"], label="IPCA YoY")
plt.plot(pdf["month"], pdf["usdbrl_yoy"], label="USD/BRL YoY")
plt.legend()
plt.title("Brazil Inflation (IPCA YoY) vs USD/BRL (YoY)")
plt.show()

win = 24
rolling_corr = pdf["ipca_mom"].rolling(win).corr(pdf["usdbrl_mom"])
fig = plt.figure()
plt.plot(pdf["month"], rolling_corr)
plt.title(f"Rolling correlation ({win}m): IPCA MoM vs USD/BRL MoM")
plt.show()
