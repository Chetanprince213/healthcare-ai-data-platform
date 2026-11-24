SELECT
    region,
    COUNT(*) AS total_records,
    ROUND(AVG(age), 2) AS average_age,
    SUM(prior_visits) AS total_visits,
    SUM(no_show) AS missed_appointments
FROM healthcare.benchmark_patients
GROUP BY region
ORDER BY region;
