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
AND j.CreateDate < '2018-01-01';
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
df = df.drop(['CreateDate', 'CompletionDate'], axis=1)

# Features (X) and Target Variable (y)
X = df.drop(columns=['Quantity'])  # Features
y = df['Quantity']  # Target variable (demand)

from sklearn.model_selection import train_test_split

# Split data into training and test sets (80-20)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

# Train Model
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Predict on test data
y_pred = model.predict(X_test)

# Evaluate Model
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Absolute Error: {mae}")
print(f"Mean Squared Error: {mse}")

# # Compare MAE with the mean of the target variable
# mean_demand = y_train.mean()
# print(f"Mean Demand: {mean_demand}")
# print(f"MAE as a percentage of Mean Demand: {mae / mean_demand * 100:.2f}%")

# rmse = np.sqrt(mse)
# print(f"Root Mean Squared Error: {rmse}")

import joblib

joblib.dump(model, 'forecast_model.pkl')
print("Model saved to forecast_model.pkl")