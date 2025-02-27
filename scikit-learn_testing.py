import conkey
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Create engine - getting connection string from conkey
engine = create_engine(conkey.conn_str)

# Query to fetch aggregated demand data
query = """
SELECT TOP(1000)
    jm.PartNum AS ProductID,
    j.CreateDate AS CreateDate,
    COALESCE(j.JobCompletionDate, CONVERT(date, '2018-03-01')) AS CompletionDate,
    DATEDIFF(DAY, j.CreateDate, ISNULL(j.JobCompletionDate, CONVERT(date, '2018-03-01'))) AS LeadTime,
    j.ProdQty AS Quantity,
    jm.EstUnitCost AS UnitCost,
    CASE 
        WHEN j.JobCompletionDate IS NULL THEN 'Open' 
        ELSE 'Closed' 
    END AS JobStatus
FROM erp.JobHead j
INNER JOIN erp.JobMtl jm ON j.JobNum = jm.JobNum
WHERE j.JobType = 'MFG' AND j.ProdQty > 0 AND LEN(jm.PartNum) > 5
AND j.CreateDate >= '2018-01-01';
"""

# Fetch data into Pandas DataFrame
df = pd.read_sql(query, engine, dtype={'ProductID': str})

# Convert dates to numerical features
df['CreateDate'] = pd.to_datetime(df['CreateDate'])
df['CompletionDate'] = pd.to_datetime(df['CompletionDate'])
df['Created_Year'] = df['CreateDate'].dt.year
df['Created_Month'] = df['CreateDate'].dt.month
df['Created_Day'] = df['CreateDate'].dt.day

# Fill missing leadtime with the mean (for open orders)
df['LeadTime'] = df['LeadTime'].fillna(df['LeadTime'].mean())

from sklearn.preprocessing import LabelEncoder

# Encode categorical column 'status' (Open = 1, Closed = 0)
df['JobStatus'] = LabelEncoder().fit_transform(df['JobStatus'])
df['ProductID'] = LabelEncoder().fit_transform(df['ProductID'])

# Drop unnecessary columns
df = df.drop(['CreateDate', 'CompletionDate', 'Quantity'], axis=1)

import joblib

loaded_model = joblib.load('forecast_model.pkl')

# Predict demand
predicted_demand = loaded_model.predict(df)

# Save predictions to a CSV file
predicted_demand.to_csv('predicted_demand.csv', index=False)