-- old data
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

-- new data
SELECT TOP(100)
    jm.PartNum AS ProductID,
    j.CreateDate AS CreateDate,
    COALESCE(j.JobCompletionDate, CONVERT(date, '2018-03-01'))  AS CompletionDate,
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
AND j.CreateDate >= '2018-01-01'