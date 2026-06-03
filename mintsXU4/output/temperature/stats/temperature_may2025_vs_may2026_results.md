# Non-Parametric Test Results: Temperature — May 2025 vs May 2026

**Field:** Temperature (°C)  
**Period 1:** May 2025 (2025-05-01 – 2025-05-31) — 8928 data points  
**Period 2:** May 2026 (2026-05-01 – 2026-05-31) — 7777 data points  
**Significance threshold:** α = 0.05  

---

## 1. Two-Sample Kolmogorov-Smirnov (KS) Test

**Statistic:** 0.040978  
**p-value:** p<0.001 ***  

_Measures the maximum absolute difference between the two empirical CDFs. Sensitive to any difference in shape, location, or scale. A small p-value means the two samples are unlikely to come from the same distribution._

**Interpretation:** The two periods are **statistically different** (KS test, p<0.001 ***). The null hypothesis that both samples come from the same distribution is rejected.

---

## 2. Anderson-Darling (AD) Test

**Statistic:** 19.697636  
**p-value:** p=0.0010 **  

_Similar to KS but weights differences in the tails more heavily. More powerful for detecting shifts in extreme readings. A small p-value indicates the distributions differ, especially in their tails._

**Interpretation:** The two periods are **statistically different** (AD test, p=0.0010 **). The null hypothesis that both samples come from the same distribution is rejected.

---

## 3. Mann-Whitney U Test

**Statistic:** 34526468.0  
**p-value:** p=0.5410 (ns)  

_A rank-based test that checks whether one period tends to have systematically higher Temperature values than the other (stochastic dominance). Does not assume any distribution shape. A small p-value means one period had significantly higher/lower values overall._

**Interpretation:** No statistically significant difference detected (MWU test, p=0.5410 (ns)). Cannot rule out that both periods come from the same distribution.

---

## 4. Epps-Singleton Test

**Statistic:** 142.119806  
**p-value:** p<0.001 ***  

_Compares the empirical characteristic functions of the two samples. Effective even on small samples and can detect differences that KS/AD miss. A small p-value indicates the two Temperature distributions are statistically different._

**Interpretation:** The two periods are **statistically different** (ES test, p<0.001 ***). The null hypothesis that both samples come from the same distribution is rejected.

---

## Summary

Comparing **May 2025 (2025-05-01 – 2025-05-31)** vs **May 2026 (2026-05-01 – 2026-05-31)** (Temperature):

- **KS**: distributions differ significantly
- **AD**: distributions differ significantly
- **MWU**: no significant difference (p≥0.05)
- **ES**: distributions differ significantly

> **Note:** All four tests are non-parametric — they make no assumption about the underlying distribution shape.