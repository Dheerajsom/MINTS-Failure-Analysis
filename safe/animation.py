"""Shared field metadata and density helpers for SAFE animations."""

from dataclasses import dataclass
import numpy as np
from scipy import stats

from safe.config import HARD_BOUNDS, PC_BOUNDS, PM_BOUNDS


@dataclass(frozen=True)
class FieldMetadata:
    """Display and physical-range information for a PM or PC channel."""

    field: str
    label: str
    unit: str
    bounds: tuple[float, float]


def field_metadata(field: str) -> FieldMetadata:
    """Return metadata for any PM/PC field without restricting size bins.

    Availability is deliberately checked by the data loaders, not against a
    fixed field list here. Unknown PM/PC bins receive family-appropriate
    fallback bounds; configured fields always use ``HARD_BOUNDS``.
    """
    normalized = field.strip().lower()
    if normalized.startswith("pm"):
        prefix, unit, fallback = "PM", "µg/m³", PM_BOUNDS
    elif normalized.startswith("pc"):
        prefix, unit, fallback = "PC", "particles/L", PC_BOUNDS
    else:
        raise ValueError(
            f"Field '{field}' is not a PM or PC channel; expected a name beginning with 'pm' or 'pc'."
        )

    body = normalized[2:].replace("_", ".")
    if not body:
        raise ValueError(f"Field '{field}' is missing a PM/PC size-bin suffix.")
    return FieldMetadata(
        field=normalized,
        label=f"{prefix}{body}",
        unit=unit,
        bounds=HARD_BOUNDS.get(normalized, fallback),
    )


def field_label(field: str) -> str:
    """Backward-compatible label helper."""
    return field_metadata(field).label


def field_bounds(field: str) -> tuple[float, float]:
    """Backward-compatible bounds helper."""
    return field_metadata(field).bounds


def pdf_for_window(values, y_grid: np.ndarray) -> np.ndarray:
    """Estimate a window PDF robustly, including nearly-flat windows."""
    data = np.asarray(values, dtype=float)
    data = data[np.isfinite(data)]
    if data.size < 2:
        return np.zeros_like(y_grid)

    if np.nanstd(data) < 1e-9:
        center = float(np.nanmean(data))
        spread = max(0.05, abs(center) * 0.01)
        return stats.norm.pdf(y_grid, loc=center, scale=spread)

    try:
        return stats.gaussian_kde(data)(y_grid)
    except Exception:
        return stats.norm.pdf(y_grid, loc=float(np.mean(data)), scale=float(np.std(data)))


def pdf_axis_upper_limit(pdf_values, padding: float = 1.1) -> float:
    """Return a padded density-axis maximum without changing its scale.

    Particle-count KDEs can have maxima well below one (or well above one for
    a very narrow window). Rounding a density maximum to an integer made the
    PC curves appear as a vertical line at zero. Preserve the density's
    magnitude and leave a small amount of room at the right edge instead.
    """
    if padding <= 1.0:
        raise ValueError("padding must be greater than 1")
    values = np.asarray(pdf_values, dtype=float)
    finite = values[np.isfinite(values)]
    global_max = max(0.0, float(finite.max())) if finite.size else 0.0
    if global_max == 0.0:
        return 1.0
    return float(np.nextafter(global_max * padding, np.inf))
