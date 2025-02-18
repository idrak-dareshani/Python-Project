select top(100) jh.JobNum, jh.ProdQty, jh.QtyCompleted
from erp.JobHead jh
where jobtype = 'MFG' and jobcomplete = 1 and jobclosed = 1