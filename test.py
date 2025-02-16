import pyodbc
import pandas as pd
#import datetime
from prophet import Prophet

# Connect to the database
conn_str = (
    "DRIVER={SQL Server};"
    "SERVER=xxxxxxx;"
    "DATABASE=xxxxxxx;"
    "UID=xxxxxxx;"
    "PWD=xxxxxxx;"
    "TrustServerCertificate=yes;"
)
conn = pyodbc.connect(conn_str)

# Query the database
query = """
WITH JobData AS (
    SELECT 
        jh.JobNum, 
        jh.PartNum, 
        jh.RevisionNum, 
        jh.ReqDueDate, 
        (jh.ProdQty - jh.QtyCompleted) AS RemainingQty, 
        jm.MtlSeq, 
        jm.PartNum AS MtlPartNum,
        COALESCE(jm.ReqDate, jh.ReqDueDate) AS ReqDate, 
        COALESCE(pr.PromiseDt, DATEADD(DAY, -2, COALESCE(jm.ReqDate, jh.ReqDueDate))) AS PromiseDt,
        (jm.RequiredQty - jm.IssuedQty - jm.ShippedQty) AS RequiredQty, 
        COALESCE(SUM(pq.OnHandQty), 0) AS OnHandQty, 
        COALESCE(pr.RelQty, 0) AS RelQty
    FROM 
        erp.JobHead AS jh
        INNER JOIN erp.JobMtl jm 
            ON jm.Company = jh.Company AND jm.JobNum = jh.JobNum
        LEFT JOIN erp.PartQty pq 
            ON pq.Company = jh.Company AND pq.PartNum = jm.PartNum
        LEFT JOIN erp.PODetail pd 
            ON pd.Company = jh.Company AND pd.PartNum = jm.PartNum AND pd.OpenLine = 1
        LEFT JOIN erp.PORel pr 
            ON pr.Company = pd.Company AND pr.PONum = pd.PONum AND pr.POLine = pd.POLine AND pr.OpenRelease = 1
    WHERE 
        jh.JobType = 'MFG'
        AND jh.JobReleased = 1 
        AND JobHeld = 0
        AND (jh.ProdQty - jh.QtyCompleted) > 0
        AND (jm.RequiredQty - jm.IssuedQty - jm.ShippedQty) > 0
        -- Include historical data
        --AND jh.ReqDueDate >= DATEADD(YEAR, -5, GETDATE()) -- Fetch last 5 years of data
    GROUP BY 
        jh.JobNum, jh.PartNum, jh.RevisionNum, jh.ReqDueDate, jh.ProdQty, jh.QtyCompleted, 
        jm.MtlSeq, jm.PartNum, jm.ReqDate, jm.RequiredQty, jm.IssuedQty, jm.ShippedQty, pr.PromiseDt, pr.RelQty
)
SELECT 
    PartNum, 
    RevisionNum, 
    MtlPartNum, 
    ReqDueDate, 
    SUM(RemainingQty) AS TotalRemainingQty, 
    SUM(RequiredQty) AS TotalRequiredQty, 
    SUM(OnHandQty) AS TotalOnHandQty, 
    SUM(RelQty) AS TotalRelQty,
    
    -- Additional computed columns
    DATEDIFF(DAY, MIN(ReqDueDate), MAX(ReqDueDate)) AS DemandLeadTime
    --AVG(RemainingQty) OVER (PARTITION BY PartNum ORDER BY ReqDueDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MovingAvg_7Days,
    --SUM(RemainingQty) OVER (PARTITION BY PartNum ORDER BY ReqDueDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MovingSum_30Days
FROM 
    JobData
GROUP BY 
    PartNum, RevisionNum, MtlPartNum, ReqDueDate
ORDER BY 
    ReqDueDate;
"""

df = pd.read_sql(query, conn)
print("Extracted Data:")
print(df.head())

# Preprocess the data
df['ReqDueDate'] = pd.to_datetime(df['ReqDueDate'])

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
    #print(f"\nProcessing group: PartNum={part_num}, RevisionNum={revision_num}, MtlPartNum={mtl_part_num}")

    # Use ReqDueDate as the time series index
    ts = group_df[['ReqDueDate', 'TotalRemainingQty']].groupby('ReqDueDate').sum().reset_index()

    # Ensure continues time series
    ts = ts.set_index('ReqDueDate').asfreq(forecast_freq, method='ffill').reset_index()

    # Rename the columns to fit Prophet's requirements
    prophet_df = ts.rename(columns={'ReqDueDate': 'ds', 'TotalRemainingQty': 'y'})

    if (len(prophet_df) < 10):
        #print(f"Skipping group due to insufficient data points: {len(prophet_df)}")
        continue

    model = Prophet(daily_seasonality=False, weekly_seasonality=True)
    try:
        model.fit(prophet_df)
    except Exception as e:
        print(f"Skipping group due to an exception: {e}")
        continue

    # Make future predictions
    future = model.make_future_dataframe(periods=forecast_periods, freq=forecast_freq)
    forecast = model.predict(future)

    # # Optionally, plot the forecast
    # import matplotlib.pyplot as plt
    # fig = model.plot(forecast)
    # plt.title(f"Forecast for PartNum {part_num} Revision {revision_num} MtlPartNum {mtl_part_num}")
    # plt.show()

    # Extract the forecasted values
    forecast_future = forecast[['ds', 'yhat']].tail(forecast_periods)
    forecast_future = forecast_future.rename(columns={'ds': 'ForecastDate', 'yhat': 'ForecastRemainingQty'})
  
    # Add the forecasted values to the results container
    forecast_future['PartNum'] = part_num
    forecast_future['RevisionNum'] = revision_num
    forecast_future['MtlPartNum'] = mtl_part_num

    all_forecasts.append(forecast_future)

# Combine all the forecasts into a single DataFrame
if all_forecasts:
    forecast_results = pd.concat(all_forecasts, ignore_index=True)
    print("\nCombined Forecast Results:")
    print(forecast_results.head())
else:
    print("No forecasts were generated.")
    forecast_results = pd.DataFrame()

if not forecast_results.empty:
    cursor = conn.cursor()

    # Create a table for forecasts if it does not exist
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DemandForecasts' and xtype='U')
    CREATE TABLE DemandForecasts (
        ForecastDate DATE,
        ForecastRemainingQty FLOAT,
        PartNum VARCHAR(50),
        RevisionNum VARCHAR(50),
        MtlPartNum VARCHAR(50),
        AsOfDate DATETIME DEFAULT GETDATE()
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert forecast records into SQL Server
    insert_query = """
    INSERT INTO DemandForecasts (ForecastDate, ForecastRemainingQty, PartNum, RevisionNum, MtlPartNum)
    VALUES (?, ?, ?, ?, ?)
    """
    for idx, row in forecast_results.iterrows():
        cursor.execute(insert_query, row['ForecastDate'].to_pydatetime(), float(row['ForecastRemainingQty']),
                       row['PartNum'], row['RevisionNum'], row['MtlPartNum'])
    conn.commit()
    print("Forecast results successfully written to SQL Server.")

    # Close the cursor
    cursor.close()

# Close the connection
conn.close()
