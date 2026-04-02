# SAFE — Sensor Analysis & Failure Evaluation

Real-time drift detection and failure analysis for low-cost air quality sensors. Uses Welch's T-test and Levene's test to detect sensor drift and variance inflation over rolling windows, with per-reading Z-score and hard-bounds checks for outliers and physically impossible values.