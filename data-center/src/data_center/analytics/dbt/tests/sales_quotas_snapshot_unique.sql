select snapshot_id, record_id
from {{ ref('sales_quotas') }}
group by snapshot_id, record_id
having count(*) > 1
