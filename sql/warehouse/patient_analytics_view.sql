SELECT
    patient_id,
    region,
    age,
    prior_visits,
    no_show,
    CASE
        WHEN age < 18 THEN '0-17'
        WHEN age < 30 THEN '18-29'
        WHEN age < 45 THEN '30-44'
        WHEN age < 60 THEN '45-59'
        ELSE '60+'
    END AS age_group
FROM healthcare.patients;
