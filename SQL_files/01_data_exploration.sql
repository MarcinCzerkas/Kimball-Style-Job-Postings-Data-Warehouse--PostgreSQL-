-- 1. Explore the data structure in the tables.

SELECT * FROM company_dim LIMIT 5;
SELECT * FROM skills_dim LIMIT 5;
SELECT * FROM skills_job_dim LIMIT 5;
SELECT * FROM job_postings_fact LIMIT 5;

-- 2. Check details of the fact table to investigate possible future dimensions.
-- The fact table contains several columns that could potentially be used to create new dimension tables.

-- Investigate a possible job title dimension by looking at the job_title and job_title_short columns in the fact table.
SELECT DISTINCT job_title_short FROM job_postings_fact;

SELECT
    job_title_short,
    COUNT(*) AS count
FROM
    job_postings_fact
GROUP BY
    job_title_short;

-- Check the number of distinct job titles in the fact table
SELECT
    COUNT(DISTINCT job_title) AS count
FROM
    job_postings_fact;

/*
At this point, we can see that there are 235,063 distinct job titles in the fact table.
This suggests that we may want to create a new dimension table for job titles.
We can also consider creating separate dimensions for other attributes that we can explore now.
*/

/*
Investigate the number of distinct values in the job_country and search_location columns and how they relate to each other
to determine if they are good candidates for dimension tables.
*/
SELECT
    'job_country' AS column,
    COUNT(DISTINCT job_country) AS count
FROM
    job_postings_fact
UNION ALL
SELECT
    'search_location' AS column,
    COUNT(DISTINCT search_location) AS count
FROM
    job_postings_fact
UNION ALL
SELECT
    'job_location' AS column,
    COUNT(DISTINCT job_location) AS count
FROM
    job_postings_fact
UNION ALL
SELECT DISTINCT
    'unique combinations' AS column,
    COUNT(*) AS count
FROM
    (SELECT DISTINCT
        job_country,
        search_location,
        job_location
    FROM
        job_postings_fact); -- 23,754 unique combinations of job_country, search_location, and job_location
-- A new location dimension can be created by combining the job_country, search_location, and job_location columns.

-- Investigate several low-cardinality columns that could potentially be used to create a new junk dimension table.
WITH junk AS (
    SELECT DISTINCT
        salary_rate,
        job_work_from_home,
        job_health_insurance,
        job_no_degree_mention,
        job_schedule_type
    FROM
        job_postings_fact)
SELECT
    *, -- display all combinations
    ROW_NUMBER() OVER () AS rn -- quick row count
FROM
    junk; -- 432 unique combinations of the 5 columns, which is manageable for a junk dimension table

-- Last but not least, investigate the job_via attribute to see if it can be used as a dimension or if it should be included in the job_profile_dim junk dimension.
SELECT COUNT(DISTINCT job_via) FROM job_postings_fact;
-- 7,916 distinct values which is too high for including in the junk dimension, but we can consider creating a separate dimension for job_via.

/*
In summary, we have identified several potential new dimensions based on the fact table.

The new dimension tables we can consider creating are:
1. job_title_dim: This table would contain unique job titles and their corresponding short versions.
2. location_dim: This table would contain unique job locations and search locations.
3. job_profile_dim: A 'junk' dimension that would contain low-cardinality attributes such as salary_rate, job_work_from_home, job_health_insurance, job_no_degree_mention, and job_schedule_type.
4. job_via_dim: This table would contain unique values from the job_via column.
5. date_dim: This table would contain unique dates from the job_posting_date column, which can be used for time-based analysis.

These new dimensions should be added to the data model already containing the following dimensions:
- company_dim
- skills_dim
- skills_job_dim (bridge table)

This would make for a data model composed of 1 fact table and 8 dimension tables, which is a good size for a data warehouse schema.

The granularity of the fact table would be at the job posting level, with each record representing a unique job posting.
*/

-- 3. Data quality checks

-- Check for NULL values in the identified columns for potential new dimensions
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) - COUNT(job_title) AS null_job_title,
    COUNT(*) - COUNT(job_title_short) AS null_job_title_short,
    COUNT(*) - COUNT(job_country) AS null_job_country,
    COUNT(*) - COUNT(search_location) AS null_search_location,
    COUNT(*) - COUNT(job_location) AS null_job_location,
    COUNT(*) - COUNT(salary_rate) AS null_salary_rate,
    COUNT(*) - COUNT(job_work_from_home) AS null_job_work_from_home,
    COUNT(*) - COUNT(job_health_insurance) AS null_job_health_insurance,
    COUNT(*) - COUNT(job_no_degree_mention) AS null_job_no_degree_mention,
    COUNT(*) - COUNT(job_schedule_type) AS null_job_schedule_type,
    COUNT(*) - COUNT(job_via) AS null_job_via
FROM
    job_postings_fact; -- the found NULL values will need to be handled (coalesced) during the ETL process when populating the dimension tables

-- Check for duplicates in the fact table
SELECT
    job_id,
    COUNT(*) AS count
FROM
    job_postings_fact
GROUP BY
    job_id
HAVING COUNT(*) > 1; -- no duplicates found

-- However, assuming the job_id is a sequential primary key added later, we can check for duplicates based on the combination of all the other columns in the fact table to ensure there are no duplicate records.
SELECT
    company_id,
    job_title_short,
    job_title,
    job_location,
    job_via,
    job_schedule_type,
    job_work_from_home,
    search_location,
    job_posted_date,
    job_no_degree_mention,
    job_health_insurance,
    job_country,
    salary_rate,
    salary_year_avg,
    salary_hour_avg,
    COUNT(*) AS count
FROM
    job_postings_fact
GROUP BY
    company_id,
    job_title_short,
    job_title,
    job_location,
    job_via,
    job_schedule_type,
    job_work_from_home,
    search_location,
    job_posted_date,
    job_no_degree_mention,
    job_health_insurance,
    job_country,
    salary_rate,
    salary_year_avg,
    salary_hour_avg
HAVING COUNT(*) > 1
ORDER BY job_posted_date DESC; -- now we see there are 538 duplicates... They will need to be handled during the ETL process when populating the fact table

/*
Data quality issues identified:
    - NULLs in dimension attributes
    - Duplicates in the fact table
These issues will need to be taken care of during the ETL process.
*/
