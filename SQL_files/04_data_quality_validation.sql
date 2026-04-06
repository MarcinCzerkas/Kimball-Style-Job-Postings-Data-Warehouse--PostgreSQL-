/*
After creating the complete data warehouse it is time to put the data quality to a thorough test.
The following data quality checks are to be performed:

    1. Structural Integrity Checks
These confirm that the warehouse tables themselves behave correctly and respect the expected grain and key constraints.
        1.1. Row count validation
        1.2. Primary key uniqueness checks

    2. Relational Integrity Checks
These ensure that relationships between fact tables, dimensions, and bridge tables remain valid.
        2.1. Foreign key integrity checks
        2.2. Bridge table integrity
        2.3. Referential density checks

    3. Duplicate Detection Checks
These detect duplicates that could distort analytical results.
        3.1. Duplicate business keys
        3.2. Duplicate detection beyond keys

    4. Data Completeness and Validity Checks
These checks confirm that the dataset is sufficiently complete and contains plausible values.
        4.1. Null validation for measures (null ratio)
        4.2. Value range validation

    5. Temporal Data Checks
These validate time-related correctness.
        5.1. Date dimension coverage
        5.2. Temporal consistency
*/


-- 1. Structural Integrity Checks
-- 1.1. Row count validation
SELECT
    COUNT(*) AS row_count,
    'job_postings_fact' AS table_name
FROM data_warehouse.job_postings_fact
UNION ALL
SELECT
    COUNT(*) AS row_count,
    'job_profile_dim' AS table_name
FROM data_warehouse.job_profile_dim
UNION ALL
SELECT
    COUNT(*) AS row_count,
    'job_title_dim' AS table_name
FROM data_warehouse.job_title_dim
UNION ALL
SELECT
    COUNT(*) AS row_count,
    'job_via_dim' AS table_name
FROM data_warehouse.job_via_dim
UNION ALL
SELECT
    COUNT(*) AS row_count,
    'skills_dim' AS table_name
FROM data_warehouse.skills_dim
UNION ALL
SELECT
    COUNT(*) AS row_count,
    'skills_job_dim' AS table_name
FROM data_warehouse.skills_job_dim
UNION ALL
SELECT
    COUNT(*) AS row_count,
    'location_dim' AS table_name
FROM data_warehouse.location_dim
UNION ALL
SELECT
    COUNT(*) AS row_count,
    'company_dim' AS table_name
FROM data_warehouse.company_dim;
/*
The output looks good:
"row_count","table_name"
"785642","job_postings_fact"
"432","job_profile_dim"
"235063","job_title_dim"
"7917","job_via_dim"
"259","skills_dim"
"3660218","skills_job_dim"
"23754","location_dim"
"140033","company_dim"
*/

-- 1.2. Primary key uniqueness checks
SELECT
    job_key,
    COUNT(*) AS count
FROM data_warehouse.job_postings_fact
GROUP BY job_key
HAVING COUNT(*) > 1
UNION ALL
SELECT
    job_profile_key,
    COUNT(*) AS count
FROM data_warehouse.job_profile_dim
GROUP BY job_profile_key
HAVING COUNT(*) > 1
-- No duplicates found. The same could be applied to all other tables as well; since it's a demo only, let's move on to the next checks.

-- 2. Relational Integrity Checks
-- 2.1. Foreign key integrity checks
SELECT COUNT(*) AS orphan_company_fk
FROM data_warehouse.job_postings_fact f
LEFT JOIN data_warehouse.company_dim d
ON f.company_key = d.company_key
WHERE d.company_key IS NULL;

SELECT COUNT(*) AS orphan_company_fk
FROM data_warehouse.job_postings_fact f
LEFT JOIN data_warehouse.job_title_dim d
ON f.job_title_key = d.job_title_key
WHERE d.job_title_key IS NULL;
-- No missing keys. Here once again - the same should be repeated for all dimensions.

-- 2.2. Bridge table integrity
SELECT COUNT(*) AS orphan_skill
FROM data_warehouse.skills_job_dim sj
JOIN data_warehouse.skills_dim s
ON sj.skill_key = s.skill_key
WHERE s.skill_key IS NULL;

SELECT COUNT(*) AS orphan_job
FROM data_warehouse.skills_job_dim sj
JOIN data_warehouse.job_postings_fact s
ON sj.job_key = s.job_key
WHERE s.job_key IS NULL;

-- 2.3. Referential density check
SELECT
    company_key,
    COUNT(*) AS job_count
FROM data_warehouse.job_postings_fact
GROUP BY company_key
ORDER BY job_count DESC;
-- company_key 572 looks suspicious (6661 jobs compared to the next one with 2881). Let's check it out:
SELECT
    f.job_key,
    c.company_name,
    jt.job_title,
    jt.job_title_short,
    l.job_country,
    l.job_location
FROM data_warehouse.job_postings_fact f
JOIN data_warehouse.company_dim c
    ON c.company_key = f.company_key
JOIN data_warehouse.job_title_dim jt
    ON jt.job_title_key = f.job_title_key
JOIN data_warehouse.location_dim l
    ON l.location_key = f.location_key
WHERE c.company_key = 572;
-- The company is Emprego and it seems to be present in LATAM countries. Let's investigate it further:
SELECT
    COUNT(*) AS job_count,
    l.job_country
FROM data_warehouse.job_postings_fact f
JOIN data_warehouse.company_dim c
    ON c.company_key = f.company_key
JOIN data_warehouse.location_dim l
    ON l.location_key = f.location_key
WHERE c.company_key = 572
GROUP BY l.job_country
ORDER BY COUNT(*) DESC;
/*
"job_count","job_country"
"3571","Peru"
"3071","Argentina"
"8","Chile"
"5","Ecuador"
"3","Costa Rica"
"2","Panama"
"1","Poland" <- interesting 😄
*/

-- 3. Duplicate Detection Checks
-- 3.1. Duplicate business keys
SELECT
    COUNT(*) AS job_count,
    job_id
FROM data_warehouse.job_postings_fact
GROUP BY job_id
HAVING COUNT(*) > 1; -- no results which is good

-- 3.2. Duplicate detection beyond keys
/*
No duplicate business keys ("source system IDs") don't necessarily mean no duplicates.
Let's check for suspiciously similar job postings.
*/
SELECT
    COUNT(*) AS job_count,
    f.company_key,
    f.job_title_key,
    f.location_key,
    f.job_via_key,
    f.job_profile_key,
    f.salary_year_avg,
    f.salary_hour_avg,
    f.date_key -- optionally comment out (see comment block below)
FROM data_warehouse.job_postings_fact f
GROUP BY 2, 3, 4, 5, 6, 7, 8, 9
HAVING COUNT(*) > 1;
/*
a) 1514 results with all FKs and measures repeated. These are clearly duplicates.
b) If we exclude the date FK we recieve 43107 results.
Possible explanations:
- the job posting was published again
- the same job posting was scraped multiple times and assigned a different job_id each time (what clearly happened in a)
Now I modify the query to display the range of dates for all potential duplicates.
*/
WITH cte AS(
SELECT
    COUNT(*) AS job_count,
    MIN(d.date_posted) AS min_date,
    MAX(d.date_posted) AS max_date,
    MAX(d.date_posted) - MIN(d.date_posted) AS date_range,
    f.company_key,
    f.job_title_key,
    f.location_key,
    f.job_via_key,
    f.job_profile_key,
    f.salary_year_avg,
    f.salary_hour_avg
    -- f.date_key - now removed to look for max and min dates instead
FROM data_warehouse.job_postings_fact f
JOIN data_warehouse.date_posted_dim d
    ON d.date_posted_key = f.date_key
GROUP BY 5, 6, 7, 8, 9, 10, 11
HAVING COUNT(*) > 1
)
SELECT
    MIN(date_range),
    MAX(date_range),
    ROUND(AVG(date_range), 0),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY date_range) AS IQR_25,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY date_range) AS IQR_75
FROM cte
/*
min  max  avg  iqr_25  iqr_75
 0   362  74     21      106
50% of such cases have the difference between first and last posted date in the range of 21-106 days.
All these cases seem to be the exact same job postings either published again or scraped multiple times.
This is a serious data quality issue that can potentially pollute the analysis.
It can be solved either by:
- removing duplicates and keeping only the most recent posting
    --> I will apply this method to the duplicates from the exact same day (point a)
- keeping all duplicates but adding a flag to identify them and exclude them from the analysis
    --> I will use this approach to deal with duplicates with different dates (point b) since removing them would lead to a data loss (in case we wanted to analyze posting frequency)
*/
-- First, remove duplicates with equal dates (point a)
-- Before deleting rows, validate the number of duplicates
SELECT SUM(job_count)
FROM
(
    SELECT
        COUNT(*) AS job_count,
        f.company_key,
        f.job_title_key,
        f.location_key,
        f.job_via_key,
        f.job_profile_key,
        f.salary_year_avg,
        f.salary_hour_avg,
        f.date_key
    FROM data_warehouse.job_postings_fact f
    GROUP BY 2, 3, 4, 5, 6, 7, 8, 9
    HAVING COUNT(*) > 1
); -- 3187 (some of them occur more than twice, that's why it doesn't equal 2 x 1514)

/*
Now let's build a CTE to get all job keys that should be deleted.
We must be careful to delete only 3187 - 1514 = 1673 rows because we must leave one unique row per each duplicate.
To achieve this, first I'm going to use a COUNT window function for detecting whether it's a duplicate at all.
Next, I'm going to build two one-columnar tables containing only the job keys - one with all and one with DISTINCT values.
Then, I will do an OUTER JOIN to leave only the ones that should be deleted. The expected count of this list is 1673.
*/
WITH list_to_delete AS
(
    WITH cte_prep AS
    (
        SELECT
            job_key,
            company_key,
            job_title_key,
            location_key,
            job_via_key,
            job_profile_key,
            salary_year_avg,
            salary_hour_avg,
            date_key,
            COUNT(*) OVER (
                PARTITION BY
                    company_key,
                    job_title_key,
                    location_key,
                    job_via_key,
                    job_profile_key,
                    salary_year_avg,
                    salary_hour_avg,
                    date_key
            ) AS occurrences,
            ROW_NUMBER() OVER (
                PARTITION BY
                    company_key,
                    job_title_key,
                    location_key,
                    job_via_key,
                    job_profile_key,
                    salary_year_avg,
                    salary_hour_avg,
                    date_key
            ) AS rn
        FROM data_warehouse.job_postings_fact
    )
    SELECT job_key
    FROM cte_prep
    WHERE occurrences > 1
        AND rn > 1 -- this should give us the list of job keys to be deleted, leaving one unique row per duplicate
)
-- The row count is 1673, as expected. Now we can use this output in the DELETE statement.
-- Security measures: first write an equivalent SELECT statement:
-- SELECT *
-- FROM data_warehouse.job_postings_fact
-- WHERE job_key IN (SELECT * FROM list_to_delete) -- 1673 results -> OK
-- Count the expected total number of rows after deletion
-- SELECT COUNT(*) - 1673 FROM data_warehouse.job_postings_fact -- 783969
-- Now comes the DELETE:
DELETE FROM data_warehouse.job_postings_fact
WHERE job_key IN (SELECT * FROM list_to_delete);
-- Boom! Error: deleting these rows violates the foreign key constraint of skills_job_dim (bridge table). I forgot about it...
DELETE FROM data_warehouse.skills_job_dim
WHERE job_key IN (SELECT * FROM list_to_delete);
-- 7941 rows deleted from the bridge table; now execute the DELETE from the fact table again
-- 1673 rows deleted - great. Let's double check the number of rows:
SELECT COUNT(*) FROM data_warehouse.job_postings_fact; -- 783969 - exactly as expected; well done!

-- Let's move on to point b.
-- Build a list of job keys where the flag should be 1
WITH to_be_flagged AS
(
    WITH cte_inner AS(
    SELECT
        job_key,
        company_key,
        job_title_key,
        location_key,
        job_via_key,
        job_profile_key,
        salary_year_avg,
        salary_hour_avg,
        COUNT(*) OVER (
            PARTITION BY
                company_key,
                job_title_key,
                location_key,
                job_via_key,
                job_profile_key,
                salary_year_avg,
                salary_hour_avg
        ) AS occurrences, -- this flags a job posting as duplicate based on the same combination of FKs and measures, regardless of the date
        ROW_NUMBER() OVER (
            PARTITION BY
                company_key,
                job_title_key,
                location_key,
                job_via_key,
                job_profile_key,
                salary_year_avg,
                salary_hour_avg
            ORDER BY date_key ASC -- only the original job posting (the earliest one) should be left unflagged (see: WHERE clause)
        ) AS rn
    FROM data_warehouse.job_postings_fact
    )
    SELECT
        job_key
    FROM cte_inner
    WHERE occurrences > 1
        AND rn > 1 -- all occurrences after the first one should be flagged
) -- this gives us 55743 rows
-- First, let's add a new column
ALTER TABLE data_warehouse.job_postings_fact
ADD COLUMN original_posting BOOLEAN NOT NULL DEFAULT true;
-- Now comment out the ALTER above and UPDATE original_posting to false for all duplicates after the first occurrence of each job posting
UPDATE data_warehouse.job_postings_fact
SET original_posting = false
WHERE job_key IN (SELECT * FROM to_be_flagged);
-- 55743 rows affected which matches perfectly the earlier assumptions.
-- Technically, this column should be moved to the "junk" dimension job_profile_dim.

-- 4. Data Completeness and Validity Checks
-- 4.1. Null validation for measures (null ratio)
SELECT
    COUNT(*) AS total_jobs,
    COUNT(salary_year_avg) AS jobs_with_year_salary,
    ROUND(COUNT(salary_year_avg)/COUNT(*)::numeric * 100, 2) AS pct_jobs_with_year_salary,
    COUNT(salary_hour_avg) AS jobs_with_hour_salary,
    ROUND(COUNT(salary_hour_avg)/COUNT(*)::numeric * 100, 2) AS pct_jobs_with_hour_salary
FROM data_warehouse.job_postings_fact;
-- Only 2.8% of job postings have the year salary and 1.4% have the hour salary. Does it depend on the country?
SELECT
    l.job_country,
    COUNT(*) AS total_jobs,
    COUNT(salary_year_avg) AS jobs_with_year_salary,
    ROUND(COUNT(salary_year_avg)/COUNT(*)::numeric * 100, 2) AS pct_jobs_with_year_salary,
    COUNT(salary_hour_avg) AS jobs_with_hour_salary,
    ROUND(COUNT(salary_hour_avg)/COUNT(*)::numeric * 100, 2) AS pct_jobs_with_hour_salary
FROM data_warehouse.job_postings_fact f
JOIN data_warehouse.location_dim l
    ON l.location_key = f.location_key
GROUP BY l.job_country
ORDER BY pct_jobs_with_year_salary DESC;
-- Apart from Bahamas, Brunei and Djibouti (<100 jobs in total), the percentage of posings with salary information is <8% in all countries.

-- 4.2. Value range validation
SELECT
    l.job_country,
    MIN(salary_year_avg)::integer AS min_year_salary,
    MAX(salary_year_avg)::integer AS max_year_salary,
    (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_year_avg))::integer AS median_year_salary,
    MIN(salary_hour_avg) AS min_hour_salary,
    MAX(salary_hour_avg) AS max_hour_salary,
    COUNT(*) AS job_postings,
    (MAX(salary_year_avg) - MIN(salary_year_avg))::integer AS range_year_salary,
    ((PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY salary_year_avg)) - (PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY salary_year_avg)))::integer AS range_year_salary_05_95_pct
FROM data_warehouse.job_postings_fact f
JOIN data_warehouse.location_dim l
    ON l.location_key = f.location_key
GROUP BY l.job_country
HAVING MIN(salary_year_avg) IS NOT NULL
ORDER BY range_year_salary_05_95_pct DESC;
-- The numbers look realistic

-- 5. Temporal Data Checks
-- 5.1. Date dimension coverage
SELECT
    COUNT(*)
FROM data_warehouse.job_postings_fact
WHERE date_key = 190000101; -- sentinel date for unknown/missing dates
-- The result is 0

-- 5.2. Temporal consistency
SELECT
    d.date_posted_month AS month,
    COUNT(*)
FROM data_warehouse.job_postings_fact f
JOIN data_warehouse.date_posted_dim d
    ON d.date_posted_key = f.date_key
GROUP BY month
ORDER BY month ASC;
-- Looks stable

SELECT
    d.date_posted_calendar_week AS week,
    COUNT(*)
FROM data_warehouse.job_postings_fact f
JOIN data_warehouse.date_posted_dim d
    ON d.date_posted_key = f.date_key
GROUP BY week
ORDER BY week ASC;
-- Same here

-- Let's aggregate by day and use window functions to calculate the difference vs previous day
WITH calendar AS(
    SELECT
        date_posted,
        COUNT(*) AS postings,
        LAG(COUNT(*)) OVER (ORDER BY date_posted) AS prv_day,
        COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY date_posted) AS diff_vs_prv_day
    FROM data_warehouse.job_postings_fact f
    RIGHT JOIN data_warehouse.date_posted_dim d -- yes, I've actually used the RIGHT JOIN :D
        ON d.date_posted_key = f.date_key
    GROUP BY date_posted
    ORDER BY date_posted ASC
)
SELECT
    date_posted,
    postings,
    prv_day,
    diff_vs_prv_day
FROM calendar
WHERE ABS(diff_vs_prv_day) / postings::numeric > 0.5  -- only the outliers
    AND date_posted NOT IN ('2023-01-01', '2023-12-31');
SELECT
    CASE WHEN diff_vs_prv_day > 0 THEN '+' ELSE '-' END AS up_down_vs_prv_day,
    COUNT(*)
FROM calendar
WHERE ABS(diff_vs_prv_day) / postings::numeric > 0.5  -- only the outliers
    AND date_posted NOT IN ('2023-01-01', '2023-12-31')
GROUP BY up_down_vs_prv_day;
-- Running the second query returns an aggregation of 26 days with >50% drop and 4 days with >50% increase
-- The first query might explain it as most of these 30 days are cumulated together (possibly problems with scraping fixed after a couple of days)

/*
These checks are sufficient for the scope of this project. In a production environment it is of course not an exhaustive list and could (and should) be extended.
Throughout these checks we identified one serious problem related to 
*/