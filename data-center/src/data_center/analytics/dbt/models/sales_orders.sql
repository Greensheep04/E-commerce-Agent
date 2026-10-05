select
    r.snapshot_id,
    r.source_id,
    r.source_revision,
    r.record_id,
    r.order_id,
    r.employee_id,
    r.order_date,
    r.status,
    r.subtotal,
    r.source_line,
    r.source_sha256
from {{ source('data_center', 'analytics_record') }} as r
where r.snapshot_id = '{{ var("snapshot_id") }}'
  and r.record_type = 'order'
  and r.status = 5
