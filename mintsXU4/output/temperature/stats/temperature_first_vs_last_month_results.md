# Non-Parametric Test Results: Temperature — First vs Last Month

**Field:** Temperature (°C)  
**Period 1:** First Month (2024-08-04 – 2024-09-04) — 8915 data points  
**Period 2:** Last Month  (2026-04-28 – 2026-05-28) — 8641 data points  
**Significance threshold:** α = 0.05  

---

## 1. Two-Sample Kolmogorov-Smirnov (KS) Test

**Statistic:** 0.542124  
**p-value:** p<0.001 ***  

_Measures the maximum absolute difference between the two empirical CDFs. Sensitive to any difference in shape, location, or scale. A small p-value means the two samples are unlikely to come from the same distribution._

**Interpretation:** The two periods are **statistically different** (KS test, p<0.001 ***). The null hypothesis that both samples come from the same distribution is rejected.

---

## 2. Anderson-Darling (AD) Test

**Statistic:** 4069.403581  
**p-value:** p=0.0010 **  

_Similar to KS but weights differences in the tails more heavily. More powerful for detecting shifts in extreme readings. A small p-value indicates the distributions differ, especially in their tails._

**Interpretation:** The two periods are **statistically different** (AD test, p=0.0010 **). The null hypothesis that both samples come from the same distribution is rejected.

---

## 3. Mann-Whitney U Test

**Statistic:** 64706232.0  
**p-value:** p<0.001 ***  

_A rank-based test that checks whether one period tends to have systematically higher Temperature values than the other (stochastic dominance). Does not assume any distribution shape. A small p-value means one period had significantly higher/lower values overall._

**Interpretation:** The two periods are **statistically different** (MWU test, p<0.001 ***). The null hypothesis that both samples come from the same distribution is rejected.

---

## 4. Epps-Singleton Test

**Statistic:** 9996.382736  
**p-value:** p<0.001 ***  

_Compares the empirical characteristic functions of the two samples. Effective even on small samples and can detect differences that KS/AD miss. A small p-value indicates the two Temperature distributions are statistically different._

**Interpretation:** The two periods are **statistically different** (ES test, p<0.001 ***). The null hypothesis that both samples come from the same distribution is rejected.

---

## Summary

Comparing **First Month (2024-08-04 – 2024-09-04)** vs **Last Month  (2026-04-28 – 2026-05-28)** (Temperature):

- **KS**: distributions differ significantly
- **AD**: distributions differ significantly
- **MWU**: distributions differ significantly
- **ES**: distributions differ significantly

> **Note:** All four tests are non-parametric — they make no assumption about the underlying distribution shape.