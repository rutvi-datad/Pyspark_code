WITH contest_start AS (
    SELECT MIN(submission_date) AS start_date
    FROM Submissions
),

daily_count AS (
    SELECT
        submission_date,
        hacker_id,
        COUNT(*) AS total_submissions
    FROM Submissions
    GROUP BY submission_date, hacker_id
),

running_days AS (
    SELECT
        d1.submission_date,
        d1.hacker_id,
        (
            SELECT COUNT(DISTINCT d2.submission_date)
            FROM Submissions d2
            WHERE d2.hacker_id = d1.hacker_id
              AND d2.submission_date <= d1.submission_date
        ) AS days_submitted
    FROM daily_count d1
),

consistent AS (
    SELECT
        r.submission_date,
        COUNT(DISTINCT r.hacker_id) AS unique_hackers
    FROM running_days r
    CROSS JOIN contest_start cs
    WHERE r.days_submitted =
          datediff(r.submission_date, cs.start_date) + 1
    GROUP BY r.submission_date
),

max_per_day AS (
    SELECT
        submission_date,
        hacker_id,
        total_submissions,
        ROW_NUMBER() OVER (
            PARTITION BY submission_date
            ORDER BY total_submissions DESC, hacker_id ASC
        ) AS rn
    FROM daily_count
)

SELECT
    m.submission_date,
    c.unique_hackers,
    m.hacker_id,
    h.name
FROM max_per_day m
JOIN consistent c
    ON m.submission_date = c.submission_date
JOIN Hackers h
    ON m.hacker_id = h.hacker_id
WHERE m.rn = 1
ORDER BY m.submission_date; 