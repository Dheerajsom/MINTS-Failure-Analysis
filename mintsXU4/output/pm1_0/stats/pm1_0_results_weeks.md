# Non-Parametric Test Results: Last 2 Weeks

**Period 1:** Week 1 (2026-05-14 – 2026-05-21) — 2017 data points  
**Period 2:** Week 2 (2026-05-21 – 2026-05-28) — 2017 data points  
**Metric:** PM1.0 (µg/m³)  
**Significance threshold:** α = 0.05  

---

## 1. Two-Sample Kolmogorov-Smirnov (KS) Test

**Statistic:** 0.236986  
**p-value:** p<0.001 ***  

_Measures the maximum absolute difference between the two empirical CDFs. Sensitive to any difference in shape, location, or scale. A small p-value means the two samples are unlikely to come from the same distribution._

**Interpretation:** The two periods are **statistically different** (KS test, p<0.001 ***). The null hypothesis that both samples come from the same distribution is rejected.

---

## 2. Anderson-Darling (AD) Test

**Statistic:** 139.233452  
**p-value:** p=0.0010 **  

_Similar to KS but weights differences in the tails more heavily. This makes it more powerful for detecting tail differences — important for PM data where spike events (high PM readings) live in the tail. A small p-value indicates the distributions differ, especially in their tails._

**Interpretation:** The two periods are **statistically different** (AD test, p=0.0010 **). The null hypothesis that both samples come from the same distribution is rejected.

---

## 3. Mann-Whitney U Test

**Statistic:** 2456097.5  
**p-value:** p<0.001 ***  

_A rank-based test that checks whether one period tends to have systematically higher PM1.0 values than the other (stochastic dominance). Does not assume any distribution shape. A small p-value means one period had significantly higher/lower PM levels overall._

**Interpretation:** The two periods are **statistically different** (MWU test, p<0.001 ***). The null hypothesis that both samples come from the same distribution is rejected.

---

## 4. Epps-Singleton Test

**Statistic:** 369.362658  
**p-value:** p<0.001 ***  

_Compares the empirical characteristic functions of the two samples. Effective even on small samples and can detect differences that KS/AD miss. A small p-value indicates the two distributions are statistically different._

**Interpretation:** The two periods are **statistically different** (ES test, p<0.001 ***). The null hypothesis that both samples come from the same distribution is rejected.

---

## Summary

Comparing **Week 1 (2026-05-14 – 2026-05-21)** vs **Week 2 (2026-05-21 – 2026-05-28)**:

- **KS**: distributions differ significantly
- **AD**: distributions differ significantly
- **MWU**: distributions differ significantly
- **ES**: distributions differ significantly

> **Note:** All four tests are non-parametric — they make no assumption about the underlying distribution shape and are valid regardless of whether PM1.0 follows a normal, Burr, Inverse-Gamma, or any other distribution.