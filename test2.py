import conkey
import pyodbc
import pandas as pd

# Connect to the database
conn = pyodbc.connect(conkey.conn_str)

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
        AND JobReleased = 1 
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

# Close connection
conn.close()

# Write to CSV
df.to_csv('forecast_demand.csv', index=False)