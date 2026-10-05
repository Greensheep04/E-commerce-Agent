select snapshot_id, record_id
from {{ ref('sales_orders') }}
group by snapshot_id, record_id
having count(*) > 1
