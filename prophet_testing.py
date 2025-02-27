import pandas as pd
import traceback
from prophet import Prophet

df = pd.read_csv('forecast_demand.csv')

required_columns = {'ReqDueDate', 'TotalRemainingQty', 'PartNum', 'RevisionNum', 'MtlPartNum'}
if not required_columns.issubset(df.columns):
    raise ValueError(f"Missing required columns: {required_columns - set(df.columns)}")

print(df.head())

# Preprocess the data
df['ReqDueDate'] = pd.to_datetime(df['ReqDueDate'])
df.dropna(subset=['TotalRemainingQty'], inplace=True)

# Forecasting period
forecast_periods = 12
forecast_freq = 'W'

# Forecasting container
all_forecasts = []

# Group the data by PartNum, RevisionNum, and MtlPartNum
group_columns = ['PartNum', 'RevisionNum', 'MtlPartNum']
grouped = df.groupby(group_columns)

print("\nProcessing Groups")
for group_keys, group_df in grouped:
    part_num, revision_num, mtl_part_num = group_keys
    print(f"\nProcessing group: PartNum={part_num}, RevisionNum={revision_num}, MtlPartNum={mtl_part_num}")

    # Use ReqDueDate as the time series index
    ts = group_df[['ReqDueDate', 'TotalRemainingQty']].groupby('ReqDueDate').sum().reset_index()

    ts = ts.sort_values('ReqDueDate')

    # Ensure continues time series
    ts = ts.set_index('ReqDueDate').asfreq(forecast_freq, method='ffill').reset_index()

    # Rename the columns to fit Prophet's requirements
    prophet_df = ts.rename(columns={'ReqDueDate': 'ds', 'TotalRemainingQty': 'y'})

    if (len(prophet_df) < 10):
        print(f"Skipping group due to insufficient data points: {len(prophet_df)}")
        continue

    model = Prophet(weekly_seasonality=True, yearly_seasonality=True)

    try:
        model.fit(prophet_df)
    except Exception as e:
        print(f"Skipping group: PartNum={part_num}, RevisionNum={revision_num}, MtlPartNum={mtl_part_num}")
        print("Error:", traceback.format_exc())
        continue

    # Make future predictions
    future = model.make_future_dataframe(periods=forecast_periods, freq=forecast_freq)
    forecast = model.predict(future)

    # Extract the forecasted values
    forecast_future = forecast[['ds', 'yhat']].tail(forecast_periods)
    forecast_future = forecast_future.rename(columns={'ds': 'ForecastDate', 'yhat': 'ForecastRemainingQty'})
  
    # Add the forecasted values to the results container
    forecast_future = forecast_future.assign(
        PartNum=part_num,
        RevisionNum=revision_num,
        MtlPartNum=mtl_part_num
    )

    all_forecasts.append(forecast_future)

# Combine all the forecasts into a single DataFrame
if all_forecasts:
    forecast_results = pd.concat(all_forecasts, ignore_index=True)
else:
    forecast_results = pd.DataFrame(columns=['ForecastDate', 'ForecastRemainingQty', 'PartNum', 'RevisionNum', 'MtlPartNum'])

print(forecast_results.head())
forecast_results.to_csv('forecast_result.csv', index=False)
