-- 1. Populate dimension tables

-- 1.1. Populate the date dimension first

-- Investigate the date range of the job_posted_date column
SELECT
    MAX(job_posted_date) AS max_date,
    MIN(job_posted_date) AS min_date
FROM
    public.job_postings_fact; -- full 2023 covered plus last day of 2022

-- Set locale of the database to US. English to get the month and weekday names in English
ALTER DATABASE sql_course
SET lc_time = 'en_US.UTF-8';

-- Populate the date dimension with 2023 only (since the last day of 2022 is not relevant for the analysis)
-- Imported from https://wiki.postgresql.org/wiki/Date_and_Time_dimensions and modified
INSERT INTO data_warehouse.date_posted_dim (
    date_posted,
    date_posted_year,
    date_posted_month,
    date_posted_month_name,
    date_posted_day,
    date_posted_day_of_year,
    date_posted_weekday_name,
    date_posted_calendar_week,
    date_posted_formatted_date,
    date_posted_quartal,
    date_posted_year_quartal,
    date_posted_year_month,
    date_posted_year_calendar_week,
    date_posted_weekend,
    date_posted_american_holiday,
    date_posted_period,
    date_posted_cw_start,
    date_posted_cw_end,
    date_posted_month_start,
    date_posted_month_end
)
SELECT
	datum as Date,
	extract(year from datum) AS Year,
	extract(month from datum) AS Month,
	-- Localized month name
	to_char(datum, 'TMMonth') AS MonthName,
	extract(day from datum) AS Day,
	extract(doy from datum) AS DayOfYear,
	-- Localized weekday
	to_char(datum, 'TMDay') AS WeekdayName,
	-- ISO calendar week
	extract(week from datum) AS CalendarWeek,
	to_char(datum, 'dd.mm.yyyy') AS FormattedDate,
	'Q' || to_char(datum, 'Q') AS Quartal,
	to_char(datum, 'yyyy/"Q"Q') AS YearQuartal,
	to_char(datum, 'yyyy/mm') AS YearMonth,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') AS YearCalendarWeek,
	-- Weekend
	CASE WHEN extract(isodow from datum) in (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS Weekend,
	-- Fixed holidays for US
        CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0704', '1225', '1226')
		THEN 'Holiday' ELSE 'No holiday' END
		AS AmericanHoliday,
	-- Some periods of the year
	CASE WHEN to_char(datum, 'MMDD') BETWEEN '0701' AND '0831' THEN 'Summer break'
	     WHEN to_char(datum, 'MMDD') BETWEEN '1115' AND '1225' THEN 'Christmas season'
	     WHEN to_char(datum, 'MMDD') > '1225' OR to_char(datum, 'MMDD') <= '0106' THEN 'Winter break'
		ELSE 'Normal' END
		AS Period,
	-- ISO start and end of the week of this date
	datum + (1 - extract(isodow from datum))::integer AS CWStart,
	datum + (7 - extract(isodow from datum))::integer AS CWEnd,
	-- Start and end of the month of this date
	datum + (1 - extract(day from datum))::integer AS MonthStart,
	(datum + (1 - extract(day from datum))::integer + '1 month'::interval)::date - '1 day'::interval AS MonthEnd
FROM (
    SELECT generate_series(
        DATE '2023-01-01',
        DATE '2023-12-31',
        INTERVAL '1 day'
    )::date AS datum
) DQ
ORDER BY 1;

-- Important: include a sentinel date in case of unknown or missing dates in the fact table
INSERT INTO data_warehouse.date_posted_dim (
    date_posted,
    date_posted_year,
    date_posted_month,
    date_posted_month_name,
    date_posted_day,
    date_posted_day_of_year,
    date_posted_weekday_name,
    date_posted_calendar_week,
    date_posted_formatted_date,
    date_posted_quartal,
    date_posted_year_quartal,
    date_posted_year_month,
    date_posted_year_calendar_week,
    date_posted_weekend,
    date_posted_american_holiday,
    date_posted_period,
    date_posted_cw_start,
    date_posted_cw_end,
    date_posted_month_start,
    date_posted_month_end
)VALUES (
    '1900-01-01',
    1900,
    1,
    'Unknown',
    1,
    1,
    'Unknown',
    1,
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    'Unknown',
    '1899-12-25',
    '1899-12-31',
    '1900-01-01',
    '1900-01-31'
);

-- Check the results
SELECT * FROM data_warehouse.date_posted_dim;

-- 1.2. Populate the already existing dimensions

-- company_dim
INSERT INTO data_warehouse.company_dim
(
    company_key,
    company_name,
    company_link,
    company_link_google,
    company_thumbnail    
)
SELECT
    company_id,
    COALESCE(name, 'Unknown') AS company_name,
    COALESCE(link, 'Unknown') AS company_link,
    COALESCE(link_google, 'Unknown') AS company_link_google,
    COALESCE(thumbnail, 'Unknown') AS company_thumbnail
FROM public.company_dim;

-- skills_dim
INSERT INTO data_warehouse.skills_dim
(
    skill_key,
    skill_name,
    skill_type
)
SELECT
    skill_id,
    COALESCE(skills, 'Unknown') AS skill_name,
    COALESCE(type, 'Unknown') AS skill_type
FROM public.skills_dim;

-- 1.3. Populate the new dimensions

-- job_title_dim
INSERT INTO data_warehouse.job_title_dim
(
    job_title,
    job_title_short
)
SELECT DISTINCT
    COALESCE(job_title, 'Unknown') AS job_title,
    COALESCE(job_title_short, 'Unknown') AS job_title_short
FROM public.job_postings_fact;

-- location_dim
INSERT INTO data_warehouse.location_dim
(
    job_country,
    job_location,
    search_location
)
SELECT DISTINCT
    COALESCE(job_country, 'Unknown') AS job_country,
    COALESCE(job_location, 'Unknown') AS job_location,
    COALESCE(search_location, 'Unknown') AS search_location
FROM public.job_postings_fact;

-- job_profile_dim (junk dimension); coalesce the flags to FALSE (this logic could be changed if we want to distinguish between "not mentioned" and "unknown")
INSERT INTO data_warehouse.job_profile_dim
(
    salary_rate,
    work_from_home_flag,
    work_from_home_description,
    health_insurance_flag,
    health_insurance_description,
    no_degree_mention_flag,
    no_degree_mention_description,
    job_schedule_type
)
SELECT DISTINCT
    COALESCE(salary_rate, 'Unknown') AS salary_rate,
    COALESCE(job_work_from_home, FALSE) AS work_from_home_flag,
    CASE WHEN job_work_from_home IS TRUE THEN 'Work from home mentioned' ELSE 'Work from home not mentioned' END AS work_from_home_description,
    COALESCE(job_health_insurance, FALSE) AS health_insurance_flag,
    CASE WHEN job_health_insurance IS TRUE THEN 'Health insurance mentioned' ELSE 'Health insurance not mentioned' END AS health_insurance_description,
    COALESCE(job_no_degree_mention, FALSE) AS no_degree_mention_flag,
    CASE WHEN job_no_degree_mention IS TRUE THEN 'No degree mention' ELSE 'Degree mention or unknown' END AS no_degree_mention_description,
    COALESCE(job_schedule_type, 'Unknown') AS job_schedule_type
FROM public.job_postings_fact;

-- job_via_dim
INSERT INTO data_warehouse.job_via_dim
(
    job_via
)
SELECT DISTINCT
    COALESCE(job_via, 'Unknown') AS job_via
FROM public.job_postings_fact;

-- 2. Populate the fact table with foreign keys to the dimensions
/*
This step will be done after the dimension tables are fully populated and we have the surrogate keys ready to be used as foreign keys in the fact table.
Remember to deduplicate repeated rows (with DISTINCT).
*/
INSERT INTO data_warehouse.job_postings_fact
(
    job_id,
    company_key,
    job_title_key,
    location_key,
    job_profile_key,
    job_via_key,
    date_key,
    salary_year_avg,
    salary_hour_avg
)
SELECT DISTINCT
    fact.job_id,
    fact.company_id AS company_key,
    jt.job_title_key,
    l.location_key,
    jp.job_profile_key,
    jv.job_via_key,
    d.date_posted_key,
    fact.salary_year_avg,
    fact.salary_hour_avg
FROM
    public.job_postings_fact AS fact
    -- Get the surrogate keys from the dimension tables by joining on the attribute columns
LEFT JOIN
    data_warehouse.job_title_dim AS jt
        ON  COALESCE(fact.job_title, 'Unknown') = jt.job_title
        AND COALESCE(fact.job_title_short, 'Unknown') = jt.job_title_short
LEFT JOIN
    data_warehouse.location_dim AS l
        ON  COALESCE(fact.job_country, 'Unknown') = l.job_country
        AND COALESCE(fact.job_location, 'Unknown') = l.job_location
        AND COALESCE(fact.search_location, 'Unknown') = l.search_location
LEFT JOIN
    data_warehouse.job_profile_dim AS jp
        ON  COALESCE(fact.salary_rate, 'Unknown') = jp.salary_rate
        AND COALESCE(fact.job_work_from_home, FALSE) = jp.work_from_home_flag
        AND COALESCE(fact.job_health_insurance, FALSE) = jp.health_insurance_flag
        AND COALESCE(fact.job_no_degree_mention, FALSE) = jp.no_degree_mention_flag
        AND COALESCE(fact.job_schedule_type, 'Unknown') = jp.job_schedule_type
LEFT JOIN
    data_warehouse.job_via_dim AS jv
        ON COALESCE(fact.job_via, 'Unknown') = jv.job_via
LEFT JOIN
    data_warehouse.date_posted_dim AS d
        -- Cast as DATE to remove the time component from the timestamp in the fact table; coalesce to the sentinel date in case of NULLs
        ON COALESCE(fact.job_posted_date::DATE, '1900-01-01') = d.date_posted
WHERE
    fact.job_posted_date >= '2023-01-01' AND fact.job_posted_date <= '2023-12-31' -- only include records with dates in 2023 since this is the range covered in the date dimension
ORDER BY
    d.date_posted_key ASC; -- optional

-- Check the results
SELECT * FROM data_warehouse.job_postings_fact LIMIT 100;

-- 3. Populate the bridge table between skills and job postings

INSERT INTO data_warehouse.skills_job_dim
(
    job_key,
    skill_key
)
SELECT DISTINCT
    fact.job_key,
    sj.skill_id AS skill_key
FROM
    public.skills_job_dim AS sj
INNER JOIN
    data_warehouse.job_postings_fact AS fact
        ON sj.job_id = fact.job_id; -- use the degenerate dimension as old key

/*
The data warehouse has been set up and populated!
Next step is to perform data quality checks and then start analyzing the data with SQL queries.
It is also ready to be connected to a BI tool for more advanced analysis and visualizations.
*/
