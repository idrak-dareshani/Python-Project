import pandas as pd

testdata = pd.read_csv('TestData.csv')

# Data Preprocessing
testdata['Program'] = testdata['Program'].astype('category')
testdata['TestArea'] = testdata['TestArea'].astype('category')
testdata['Result'] = testdata['Result'].astype('category')

testdata['StartTime'] = pd.to_datetime(testdata['StartTime'], format='%m/%d/%Y %I:%M:%S %p')
testdata['EndTime'] = pd.to_datetime(testdata['EndTime'], format='%m/%d/%Y %I:%M:%S %p')

# Calculate Test Duration in minutes
testdata["TestDuration_Minutes"] = (testdata["EndTime"] - testdata["StartTime"]).dt.total_seconds() / 60

# Extract Year, Month, and Quarter
testdata["Year"] = testdata["StartTime"].dt.year
testdata["Month"] = testdata["StartTime"].dt.month
testdata["Quarter"] = testdata["StartTime"].dt.quarter

# Drop StartTime and EndTime since we now have the duration
testdata = testdata.drop(columns=["StartTime", "EndTime"])

from sklearn.preprocessing import LabelEncoder

# Encode categorical columns
categorical_cols = ["Program", "TestArea", "Result"]
label_encoders = {}

for col in categorical_cols:
    le = LabelEncoder()
    testdata[col] = le.fit_transform(testdata[col])
    label_encoders[col] = le  # Save encoders for later use

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

# Define features (X) and target variable (y)
X = testdata.drop(columns=["TestDuration_Minutes"])
y = testdata["TestDuration_Minutes"]

# Split data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train Random Forest model
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Evaluate model performance
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)

print(f"Mean Absolute Error: {mae}")
print(f"Mean Squared Error: {mse}")

import joblib

# Save model and encoders
joblib.dump(model, "test_duration_model.pkl")
joblib.dump(label_encoders, "label_encoders.pkl")
print("Model and encoders saved successfully!")

# Yearly, Monthly, and Quarterly Test Duration Distribution
yearly_dist = testdata.groupby("Year")["TestDuration_Minutes"].agg(["count", "mean", "sum"])
yearly_dist.columns = ["TotalTests", "AvgDuration_Minutes", "TotalDuration_Minutes"]
print(yearly_dist)

monthly_dist = testdata.groupby(["Year", "Month"])["TestDuration_Minutes"].agg(["count", "mean", "sum"])
monthly_dist.columns = ["TotalTests", "AvgDuration_Minutes", "TotalDuration_Minutes"]
print(monthly_dist)

quarterly_dist = testdata.groupby(["Year", "Quarter"])["TestDuration_Minutes"].agg(["count", "mean", "sum"])
quarterly_dist.columns = ["TotalTests", "AvgDuration_Minutes", "TotalDuration_Minutes"]
print(quarterly_dist)

# import matplotlib.pyplot as plt
# import seaborn as sns

# # Set plot style
# sns.set_style("whitegrid")

# # Plot Yearly Test Durations
# plt.figure(figsize=(10, 5))
# sns.barplot(x=yearly_dist.index, y=yearly_dist["AvgDuration_Minutes"])
# plt.title("Average Test Duration Per Year")
# plt.xlabel("Year")
# plt.ylabel("Avg Duration (Minutes)")
# plt.show()

# # Plot Monthly Test Durations
# plt.figure(figsize=(12, 6))
# sns.lineplot(data=monthly_dist, x="Month", y="AvgDuration_Minutes", hue="Year", marker="o")
# plt.title("Average Test Duration Per Month")
# plt.xlabel("Month")
# plt.ylabel("Avg Duration (Minutes)")
# plt.legend(title="Year")
# plt.show()

# # Plot Quarterly Test Durations
# plt.figure(figsize=(10, 5))
# sns.barplot(x=quarterly_dist.index.get_level_values("Quarter"), 
#             y=quarterly_dist["AvgDuration_Minutes"], 
#             hue=quarterly_dist.index.get_level_values("Year"))
# plt.title("Average Test Duration Per Quarter")
# plt.xlabel("Quarter")
# plt.ylabel("Avg Duration (Minutes)")
# plt.legend(title="Year")
# plt.show()

# save the distribution data to a CSV file
yearly_dist.to_csv('YearlyDistribution.csv')
monthly_dist.to_csv('MonthlyDistribution.csv')
quarterly_dist.to_csv('QuarterlyDistribution.csv')

