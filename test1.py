import conkey
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from prophet import Prophet

# Create engine - getting connection string from conkey
engine = create_engine(conkey.conn_str)

# Query to fetch aggregated demand data
query = """
WITH JobData AS (
    SELECT 
        j.JobNum,
        jm.PartNum AS ProductID,
        j.CreateDate AS OrderDate,
        j.JobCompletionDate,
        CASE 
            WHEN j.JobCompletionDate IS NULL THEN 'Open' 
            ELSE 'Closed' 
        END AS JobStatus,
        j.ProdQty AS QuantityOrdered,
        j.QtyCompleted,
        DATEDIFF(DAY, j.CreateDate, ISNULL(j.JobCompletionDate, GETDATE())) AS LeadTime,
        jm.EstUnitCost AS UnitCost
    FROM erp.JobHead j
    INNER JOIN erp.JobMtl jm ON j.JobNum = jm.JobNum
    WHERE j.JobType = 'MFG' AND j.ProdQty > 0 AND LEN(jm.PartNum) > 5
)
SELECT ProductID, Year, Month, SUM(TotalDemand) AS Demand, AVG(AvgUnitCost) AS UnitCost
FROM (SELECT 
    ProductID,
    JobStatus,
    YEAR(OrderDate) AS Year,
    MONTH(OrderDate) AS Month,
    SUM(QuantityOrdered) AS TotalDemand,
    AVG(LeadTime) AS AvgLeadTime,
    AVG(UnitCost) AS AvgUnitCost
FROM JobData
GROUP BY ProductID, JobStatus, YEAR(OrderDate), MONTH(OrderDate)) DemandTable
GROUP BY ProductID, Year, Month
ORDER BY ProductID, Year DESC, Month DESC;
"""

# Fetch data into Pandas DataFrame
df = pd.read_sql(query, engine)

# Convert Year and Month into a datetime format
df['Date'] = pd.to_datetime(df[['Year', 'Month']].assign(DAY=1))

# Get unique product IDs
product_ids = df['ProductID'].unique()

# Prepare a list to store forecast results
forecast_results = []

# Iterate over each product and forecast demand
for product_id in product_ids:
    product_df = df[df['ProductID'] == product_id][['Date', 'Demand', 'UnitCost']]
    
    if product_df.shape[0] < 10:
        print(f"Not enough data for product {product_id}, skipping forecast.")
        continue
    
    # Rename columns for Prophet compatibility
    product_df = product_df.rename(columns={'Date': 'ds', 'Demand': 'y'})
    
    # Initialize and fit Prophet model
    model = Prophet()
    model.fit(product_df[['ds', 'y']])
    
    # Create future dates for prediction
    future = model.make_future_dataframe(periods=12, freq='ME')  # Forecast next 12 months
    forecast = model.predict(future)
    
    # Add UnitCost to forecasted data
    unit_cost = product_df['UnitCost'].mean()
    forecast['ProductID'] = product_id
    forecast['UnitCost'] = unit_cost
    forecast_results.append(forecast[['ProductID', 'ds', 'yhat', 'yhat_lower', 'yhat_upper', 'UnitCost']])

# Combine all forecast results into a single DataFrame
forecast_df = pd.concat(forecast_results)
forecast_df.rename(columns={'ds': 'ForecastDate', 'yhat': 'PredictedDemand', 'yhat_lower': 'LowerBound', 'yhat_upper': 'UpperBound'}, inplace=True)

# Save forecast resutls to csv
forecast_df.to_csv('demand_forecast.csv', index = False)

print(f"Forecast data successfully saved to csv.")
