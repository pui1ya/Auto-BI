#forecastings - showing what we got 🗿

import pandas as pd
import numpy as np
import datetime as dt


#takes input value from kpi (daily_revenue_df)
def prepare_time_series(df, date_col, value_col):
    ts = df[[date_col, value_col]].copy()

    ts[date_col] = pd.to_datetime(ts[date_col])
    ts = ts.sort_values(date_col)

    ts = ts.set_index(date_col)

    #adding missing days
    ts = ts.asfreq("D")

    #filling missing day values
    ts[value_col] = ts[value_col].fillna(0)

    return ts


#reusing the same data for daily weekly and monthly forecasts
def aggregate_time_series(ts, freq="W"):
    return (ts.resample(freq).sum())


#calculating a week worth of sale values using recent history
#ngl i did not know i had this much brains 

def rolling_average_forecast(ts, window=7, periods=7):
    ts = ts.copy()

    ts["rolling_mean"] = ts.iloc[:, 0].rolling(window).mean()

    last_value = ts["rolling_mean"].dropna().iloc[-1]

    #to generate future dates
    future_dates = pd.date_range(
        start=ts.index[-1] + pd.Timedelta(days=1),
        periods=periods,
        freq="D"
    )

    forecast = pd.DataFrame({
        "date": future_dates,
        "forecast": last_value,
        "method": "rolling_average"
    })

    return forecast


from statsmodels.tsa.seasonal import seasonal_decompose

def decompose_trend_seasonality(ts, period=7):
    result = seasonal_decompose(
        ts.iloc[:, 0],
        model="additive",
        period=period
    )

    return pd.DataFrame({
        "trend": result.trend,
        "seasonal": result.seasonal,
        "residual": result.resid
    })


def seasonal_naive_forecast(ts, periods=7, season_length=7):
    values = ts.iloc[:, 0].values

    seasonal_pattern = values[-season_length:]

    repeats = int(np.ceil(periods / season_length))
    forecast_values = np.tile(seasonal_pattern, repeats)[:periods]

    future_dates = pd.date_range(
        start=ts.index[-1] + pd.Timedelta(days=1),
        periods=periods,
        freq="D"
    )

    return pd.DataFrame({
        "date": future_dates,
        "forecast": forecast_values,
        "method": "seasonal_naive"
    })


def forecast_summary(*forecasts):
    return (
        pd.concat(forecasts)
        .reset_index(drop=True)
    )



if __name__ == "__main__":

    from app.etl.ingest import ingest_csv
    from app.etl.clean import clean_raw_data
    from app.etl.transform import table_builder
    from app.analytics.kpi import compute_daily_revenue

    FILE_PATH = "/Users/punyashrees/Documents/projects/auto-bi/Sample - Superstore.csv"

    raw_df = ingest_csv(FILE_PATH)
    clean_df = clean_raw_data(raw_df)

    builder = table_builder(clean_df)
    orders = builder.build_orders_table()
    transactions = builder.build_transactions_table()

    daily_revenue = compute_daily_revenue(
        transactions=transactions,
        orders=orders
    )

    ts = prepare_time_series(
        df=daily_revenue,
        date_col="order_date",
        value_col="total_sales"
    )

    ts_weekly = aggregate_time_series(ts, freq="W")

    rolling_forecast = rolling_average_forecast(
        ts=ts,
        window=7,
        periods=14
    )

    seasonal_forecast = seasonal_naive_forecast(
        ts=ts,
        periods=14,
        season_length=7
    )

    forecast = forecast_summary(
        rolling_forecast,
        seasonal_forecast
    )

    print("forecast sample:")
    print(forecast)
