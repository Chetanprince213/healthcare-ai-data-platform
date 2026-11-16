select
    region,
    count(*) as patient_count
from {{ source('healthcare_raw', 'raw_patients') }}
group by region
