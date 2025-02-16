WITH
    DemandMaster(JobNum, PartNum, RevisionNum, ReqDueDate, RemainingQty, MtlSeq, MtlPartNum, ReqDate, PromiseDt, RequiredQty, OnHandQty, RelQty)
    AS
    (
        select jh.JobNum, jh.PartNum, jh.RevisionNum, jh.ReqDueDate, (jh.ProdQty - jh.QtyCompleted) as RemainingQty, jm.MtlSeq, jm.PartNum as MtlPartNum,
            COALESCE(jm.ReqDate, jh.ReqDueDate) as ReqDate, COALESCE(pr.PromiseDt, DATEADD(day, -2, COALESCE(jm.ReqDate, jh.ReqDueDate))) as PromiseDt,
            (jm.RequiredQty - jm.IssuedQty - jm.ShippedQty) as RequiredQty, COALESCE(SUM(pq.OnHandQty), 0) as OnHandQty, COALESCE(pr.RelQty, 0) as RelQty
        from erp.JobHead as jh
            inner join erp.JobMtl jm on jm.Company = jh.Company and jm.JobNum = jh.JobNum
            left outer join erp.PartQty pq on pq.Company = jh.Company and pq.PartNum = jm.PartNum
            left outer join erp.PODetail pd on pd.Company = jh.Company and pd.PartNum = jm.PartNum
                and pd.OpenLine = 1
            left outer join erp.PORel pr on pr.Company = pd.Company and pr.PONum = pd.PONum and pr.POLine = pd.POLine
                and pr.OpenRelease = 1
        where jh.JobType = 'MFG'
            and jh.JobClosed = 0 and jh.JobReleased = 1 and JobHeld = 0
            and (jh.ProdQty - jh.QtyCompleted) > 0
            and (jm.RequiredQty - jm.IssuedQty - jm.ShippedQty) > 0
        group by jh.JobNum, jh.PartNum, jh.RevisionNum, jh.ReqDueDate, jh.ProdQty, jh.QtyCompleted, jm.MtlSeq, jm.PartNum, jm.ReqDate, jm.RequiredQty, jm.IssuedQty, jm.ShippedQty, pr.PromiseDt, pr.RelQty
    )
SELECT dm.JobNum, dm.PartNum, dm.RevisionNum, dm.ReqDueDate, dm.RemainingQty,
    dm.MtlSeq, dm.MtlPartNum, dm.ReqDate, dm.PromiseDt, dm.RequiredQty,
    (dm.OnHandQty + dm.RelQty) as AvailableQty, dm.RequiredQty - (dm.OnHandQty + dm.RelQty) as ShortQty
from DemandMaster dm
where dm.RequiredQty > (dm.OnHandQty + RelQty)
order by dm.ReqDueDate, dm.JobNum, dm.MtlSeq;
