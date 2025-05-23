import conkey
import pandas as pd
from sqlalchemy import create_engine

# Connect to the database
engine = create_engine(conkey.conn_str)

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
        jm.EstUnitCost,
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
        AND jh.ProdQty > 0
    GROUP BY 
        jh.JobNum, jh.PartNum, jh.RevisionNum, jh.ReqDueDate, jh.ProdQty, jh.QtyCompleted, 
        jm.MtlSeq, jm.PartNum, jm.EstUnitCost, jm.ReqDate, jm.RequiredQty, jm.IssuedQty, jm.ShippedQty, 
        pr.PromiseDt, pr.RelQty
)
SELECT 
    PartNum, 
    RevisionNum, 
    MtlPartNum, 
    EstUnitCost, 
    ReqDueDate,
    SUM(RemainingQty) AS TotalRemainingQty, 
    SUM(RequiredQty) AS TotalRequiredQty, 
    SUM(OnHandQty) AS TotalOnHandQty, 
    SUM(RelQty) AS TotalRelQty,
    DATEDIFF(DAY, MIN(ReqDueDate), MAX(ReqDueDate)) AS DemandLeadTime
FROM 
    JobData
GROUP BY 
    PartNum, RevisionNum, MtlPartNum, EstUnitCost, ReqDueDate
ORDER BY 
    ReqDueDate;
"""

df = pd.read_sql(query, engine)

# Write to CSV
df.to_csv('forecast_demand.csv', index=False)

print("Forecast historical data successfully written to CSV.")