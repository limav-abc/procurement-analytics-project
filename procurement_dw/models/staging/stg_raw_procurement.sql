select
    *,
    current_timestamp as loaded_at
from {{ source('raw_data', 'raw_procurement') }}