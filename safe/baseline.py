"""Bounded robust UTC time-of-day profile with a rate-limited seasonal level.

No raw ambient Page-Hinkley inputs. Predictions must precede learning a reading.
"""

import math
import numpy as np


def robust_scale(values, floor):
    values = np.asarray(values, dtype=float)
    median = float(np.median(values))
    mad = float(np.median(np.abs(values - median))) * 1.4826
    q25, q75 = np.percentile(values, [25, 75])
    return median, max(mad, float(q75 - q25) / 1.349, floor)


class SeasonalBaseline:
    def __init__(self, profile):
        self.profile = profile
        self.bins = [[] for _ in range(profile.seasonal_bins)]
        self.level = 0.0
        self.last_learned = None
        self.ready = False
        self.revision = 0

    def _position(self, timestamp):
        p = self.profile
        return (timestamp % p.seasonal_period_seconds) / p.seasonal_period_seconds * p.seasonal_bins

    def fit(self, samples):
        p = self.profile
        buckets = [[] for _ in self.bins]
        for timestamp, value in samples:
            buckets[int(self._position(timestamp))].append(value)
        coverage = sum(bool(b) for b in buckets) / len(buckets)
        if coverage < p.minimum_coverage:
            return False
        overall, scale = robust_scale([v for _, v in samples], p.residual_scale_floor)
        for index, bucket in enumerate(buckets):
            if bucket:
                center, spread = robust_scale(bucket, p.residual_scale_floor)
            else:
                center, spread = overall, scale
            self.bins[index] = [[int(samples[-1][0] // p.seasonal_period_seconds), center, spread]]
        self.ready = True
        self.last_learned = samples[-1][0]
        self.revision += 1
        return True

    def predict(self, timestamp):
        if not self.ready:
            return None
        position = self._position(timestamp) - 0.5
        left = math.floor(position) % len(self.bins)
        right = (left + 1) % len(self.bins)
        fraction = position - math.floor(position)
        centers, scales = [], []
        for index in (left, right):
            centers.append(float(np.median([b[1] for b in self.bins[index]])))
            scales.append(float(np.median([b[2] for b in self.bins[index]])))
        return (centers[0] * (1 - fraction) + centers[1] * fraction + self.level,
                max(scales[0] * (1 - fraction) + scales[1] * fraction,
                    self.profile.residual_scale_floor))

    def learn(self, timestamp, value):
        prediction = self.predict(timestamp)
        if prediction is None:
            return
        expected, scale = prediction
        p = self.profile
        elapsed = max(0, timestamp - self.last_learned)
        # Large gaps must not cause a one-reading jump in the expected level.
        elapsed = min(elapsed, 2 * p.expected_interval_seconds)
        rate_limit = p.seasonal_rate_per_day * elapsed / 86400
        innovation = value - expected
        delta = float(np.clip(innovation * min(1, elapsed / 21600), -rate_limit, rate_limit))
        self.level += delta
        self.last_learned = timestamp
        # One robust, clipped representative per phase per cycle, bounded in cycles.
        bucket = self.bins[int(self._position(timestamp))]
        cycle = int(timestamp // p.seasonal_period_seconds)
        corrected = value - self.level
        if bucket[-1][0] == cycle:
            old = bucket[-1][1]
            bucket[-1][1] = old + 0.1 * float(np.clip(corrected - old, -scale, scale))
        else:
            bucket.append([cycle, corrected, scale])
            del bucket[:-p.seasonal_cycles]
        self.revision += 1

    def to_dict(self):
        return dict(bins=self.bins, level=self.level, last_learned=self.last_learned,
                    ready=self.ready, revision=self.revision)

    @classmethod
    def from_dict(cls, profile, data):
        obj = cls(profile)
        if len(data["bins"]) != profile.seasonal_bins:
            raise ValueError("state seasonal bin count differs from profile")
        for key in obj.to_dict():
            setattr(obj, key, data[key])
        return obj
