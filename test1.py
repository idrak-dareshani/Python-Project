import conkey
import pyodbc
import pandas as pd

forecast_results = pd.read_csv('forecast_result.csv', parse_dates=['ForecastDate'])

if not forecast_results.empty:

    conn = pyodbc.connect(conkey.conn_str)
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

    #Close the connection
    conn.close()
