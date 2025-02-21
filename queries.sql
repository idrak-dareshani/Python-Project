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
    WHERE j.JobType = 'MFG' 
    --    AND j.CreateDate >= DATEADD(YEAR, -3, GETDATE())  -- Last 3 years
)
SELECT ProductID, Year, Month, SUM(TotalDemand) AS Demand, AVG(AvgUnitCost) AS UnitCost
FROM (SELECT 
    ProductID,
    JobStatus,
    YEAR(OrderDate) AS Year,
    MONTH(OrderDate) AS Month,
    SUM(QuantityOrdered) AS TotalDemand,
    AVG(LeadTime) AS AvgLeadTime,
    AVG(UnitCost) AS AvgUnitCost  -- Average unit cost per product
FROM JobData
GROUP BY ProductID, JobStatus, YEAR(OrderDate), MONTH(OrderDate)) DemandTable
GROUP BY ProductID, Year, Month
ORDER BY ProductID, Year DESC, Month DESC;
;
