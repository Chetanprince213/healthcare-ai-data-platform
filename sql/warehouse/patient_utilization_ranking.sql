WITH ranked_patients AS (
    SELECT
        patient_id,
        region,
        prior_visits,
        ROW_NUMBER() OVER (
            PARTITION BY region
            ORDER BY prior_visits DESC, patient_id
        ) AS patient_rank,
        AVG(prior_visits) OVER (
            PARTITION BY region
        ) AS regional_average_visits
    FROM healthcare.patients
)
SELECT
    patient_id,
    region,
    prior_visits,
    patient_rank,
    ROUND(regional_average_visits, 2)
        AS regional_average_visits
FROM ranked_patients
WHERE patient_rank <= 3
ORDER BY region, patient_rank;
