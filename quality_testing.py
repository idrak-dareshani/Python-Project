import joblib

# Load the model and encoders for future predictions
model = joblib.load("test_duration_model.pkl")
label_encoders = joblib.load("label_encoders.pkl")

# Example new data
new_data = pd.DataFrame({
    "Program": ["Program A"],
    "TestArea": ["Hardware"],
    "Result": ["Pass"]
})

from sklearn.preprocessing import LabelEncoder

# Encode categorical columns
categorical_cols = ["Program", "TestArea", "Result"]
    
# Encode categorical variables
for col in categorical_cols:
    new_data[col] = label_encoders[col].transform(new_data[col])

# Make prediction
predicted_time = model.predict(new_data)
print(f"Predicted Test Duration: {predicted_time[0]:.2f} minutes")