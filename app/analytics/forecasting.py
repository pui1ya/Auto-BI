"""
Time-series revenue forecasting using Holt-Winters exponential smoothing.
Falls back to linear trend if not enough data.
"""
import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from app.analytics.kpi import revenue_by_day
from app.core.config import settings


def forecast_revenue(horizon: int = settings.FORECAST_HORIZON) -> pd.DataFrame:
    ts = revenue_by_day()
    if ts.empty:
        return pd.DataFrame(columns=["date", "predicted", "lower", "upper"])

    ts = ts.sort_values("ds")
    ts["ds"] = pd.to_datetime(ts["ds"])
    ts = ts.set_index("ds").asfreq("D").fillna(method="ffill")
    y = ts["y"]

    # Need at least 2 full seasons for Holt-Winters seasonal
    try:
        if len(y) >= 14:
            model = ExponentialSmoothing(
                y,
                trend="add",
                seasonal="add" if len(y) >= 14 else None,
                seasonal_periods=7,
                initialization_method="estimated",
            )
            fit = model.fit(optimized=True)
            forecast = fit.forecast(horizon)
            # Simple confidence interval: ±1.5 * residual std
            resid_std = fit.resid.std()
        else:
            # Linear fallback
            x = np.arange(len(y))
            coeffs = np.polyfit(x, y.values, 1)
            future_x = np.arange(len(y), len(y) + horizon)
            forecast_values = np.polyval(coeffs, future_x)
            future_dates = pd.date_range(
                start=y.index[-1] + pd.Timedelta(days=1), periods=horizon, freq="D"
            )
            forecast = pd.Series(forecast_values, index=future_dates)
            resid_std = y.std() * 0.2

        future_dates = pd.date_range(
            start=y.index[-1] + pd.Timedelta(days=1), periods=horizon, freq="D"
        )
        result = pd.DataFrame({
            "date": forecast.index.astype(str),
            "predicted": forecast.values.round(2),
            "lower": (forecast.values - 1.5 * resid_std).round(2),
            "upper": (forecast.values + 1.5 * resid_std).round(2),
        })
        result["lower"] = result["lower"].clip(lower=0)
        return result

    except Exception as e:
        return pd.DataFrame(columns=["date", "predicted", "lower", "upper"])