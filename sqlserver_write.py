import conkey
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Date, Float, String, DateTime, func, text

forecast_results = pd.read_csv('forecast_result.csv', parse_dates=['ForecastDate'])

if not forecast_results.empty:

    # fethcing limited rows for testing
    #forecast_head = forecast_results.head()
    
    # Connect to the database
    engine = create_engine(conkey.conn_str)

    # Create metadata object
    metadata = MetaData()

    # Define the DemandForecasts table
    demand_forecasts = Table(
        'DemandForecasts', metadata,
        Column('ForecastDate', Date),
        Column('ForecastRemainingQty', Float),
        Column('PartNum', String(50)),
        Column('RevisionNum', String(50)),
        Column('MtlPartNum', String(50)),
        Column('AsOfDate', DateTime, server_default=func.getdate())
    )

    data = forecast_results[['ForecastDate', 'ForecastRemainingQty', 'PartNum', 'RevisionNum', 'MtlPartNum']].to_dict(orient='records')
    with engine.begin() as conn:

        # Truncate the table to remove old data
        conn.execute(text("TRUNCATE TABLE DemandForecasts"))

        # Insert data using bulk insert
        conn.execute(demand_forecasts.insert(), data)

    print("Forecast results successfully written to SQL Server")
       
