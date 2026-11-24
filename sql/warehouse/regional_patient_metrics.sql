WITH regional_metrics AS (
    SELECT
        region,
        COUNT(*) AS total_patients,
        ROUND(AVG(age), 2) AS average_age,
        SUM(prior_visits) AS total_prior_visits,
        SUM(no_show) AS missed_appointments,
        ROUND(
            100.0 * SUM(no_show)
            / NULLIF(COUNT(*), 0),
            2
        ) AS no_show_rate_pct
    FROM healthcare.patients
    GROUP BY region
)
SELECT
    *,
    DENSE_RANK() OVER (
        ORDER BY total_patients DESC
    ) AS patient_volume_rank,
    ROUND(
        100.0 * total_patients
        / SUM(total_patients) OVER (),
        2
    ) AS patient_distribution_pct
FROM regional_metrics
ORDER BY total_patients DESC, region;
