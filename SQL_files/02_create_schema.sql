-- 1. Create the data warehouse schema and tables

CREATE SCHEMA IF NOT EXISTS data_warehouse;

-- 2. Create dimension tables
/*
All dimension attributes are set to NOT NULL to ensure data integrity.
Missing values are coalesced to 'Unknown' or FALSE as appropriate during the population step in 03_populate_tables.sql.
*/

-- 2.1. Recreate the already existing dimensions

-- Create company_dim table (pay special attention to a consistent naming convention)
CREATE TABLE IF NOT EXISTS data_warehouse.company_dim
(
    company_key INT NOT NULL PRIMARY KEY,
    company_name TEXT NOT NULL,
    company_link TEXT NOT NULL,
    company_link_google TEXT NOT NULL,
    company_thumbnail TEXT NOT NULL
);

-- Create skills_dim_table
CREATE TABLE IF NOT EXISTS data_warehouse.skills_dim
(
    skill_key INT  NOT NULL PRIMARY KEY,
    skill_name TEXT NOT NULL,
    skill_type TEXT NOT NULL
);

-- 2.2. Create new dimensions based on the data exploration

-- Create job_title_dim table; remember to add a new surrogate key column as the primary key
CREATE TABLE IF NOT EXISTS data_warehouse.job_title_dim
(
    job_title_key INT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_title TEXT NOT NULL,
    job_title_short VARCHAR(255) NOT NULL
);

-- Create location_dim table
CREATE TABLE IF NOT EXISTS data_warehouse.location_dim
(
    location_key INT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_country TEXT NOT NULL,
    job_location TEXT NOT NULL,
    search_location TEXT NOT NULL
);

-- Create job_profile_dim table (junk dimension)
-- In this type of dimension table it is useful to include descriptive columns that provide more context to the flags
CREATE TABLE IF NOT EXISTS data_warehouse.job_profile_dim
(
    job_profile_key INT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    salary_rate TEXT NOT NULL,
    work_from_home_flag BOOLEAN NOT NULL,
    work_from_home_description TEXT NOT NULL,
    health_insurance_flag BOOLEAN NOT NULL,
    health_insurance_description TEXT NOT NULL,
    no_degree_mention_flag BOOLEAN NOT NULL,
    no_degree_mention_description TEXT NOT NULL,
    job_schedule_type TEXT NOT NULL
);

-- Create job_via_dim table
CREATE TABLE IF NOT EXISTS data_warehouse.job_via_dim
(
    job_via_key INT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_via TEXT NOT NULL
);


-- 2.3. Create the date dimension
-- Imported from https://wiki.postgresql.org/wiki/Date_and_Time_dimensions and modified

CREATE TABLE IF NOT EXISTS data_warehouse.date_posted_dim
(
    date_posted_key integer GENERATED ALWAYS AS (
        (extract(year from date_posted)::int * 10000) +
        (extract(month from date_posted)::int * 100) +
        extract(day from date_posted)::int
    ) STORED PRIMARY KEY,
    date_posted                     date,
    date_posted_year                integer NOT NULL,
    date_posted_month               integer NOT NULL,
    date_posted_month_name          text NOT NULL,
    date_posted_day                 integer NOT NULL,
    date_posted_day_of_year         integer NOT NULL,
    date_posted_weekday_name        text NOT NULL,
    date_posted_calendar_week       integer NOT NULL,
    date_posted_formatted_date      text NOT NULL,
    date_posted_quartal             text NOT NULL,
    date_posted_year_quartal        text NOT NULL,
    date_posted_year_month          text NOT NULL,
    date_posted_year_calendar_week  text NOT NULL,
    date_posted_weekend             text NOT NULL,
    date_posted_american_holiday    text NOT NULL,
    date_posted_period              text NOT NULL,
    date_posted_cw_start            date NOT NULL,
    date_posted_cw_end              date NOT NULL,
    date_posted_month_start         date NOT NULL,
    date_posted_month_end           date NOT NULL
);

-- 3. Create the fact table with foreign keys to the dimensions

-- Ensure the foreign keys are NOT NULL; the PK (job_key) will be auto-generated instead of taking the job_id from the original table since it includes duplicates
CREATE TABLE IF NOT EXISTS data_warehouse.job_postings_fact
(
    job_key INT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id INT NOT NULL, -- the original id preserved here as degenerate dimension
    company_key INT NOT NULL,
    job_title_key INT NOT NULL,
    location_key INT NOT NULL,
    job_profile_key INT NOT NULL,
    job_via_key INT NOT NULL,
    date_key INT NOT NULL,
    salary_year_avg NUMERIC,
    salary_hour_avg NUMERIC,
    FOREIGN KEY (company_key) REFERENCES data_warehouse.company_dim (company_key),
    FOREIGN KEY (job_title_key) REFERENCES data_warehouse.job_title_dim (job_title_key),
    FOREIGN KEY (location_key) REFERENCES data_warehouse.location_dim (location_key),
    FOREIGN KEY (job_profile_key) REFERENCES data_warehouse.job_profile_dim (job_profile_key),
    FOREIGN KEY (job_via_key) REFERENCES data_warehouse.job_via_dim (job_via_key),
    FOREIGN KEY (date_key) REFERENCES data_warehouse.date_posted_dim (date_posted_key)
);

-- 4. Create the bridge table to handle the many-to-many relationship between job_postings_fact and skills_dim

CREATE TABLE data_warehouse.skills_job_dim
(
    skills_job_key INT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_key INT NOT NULL,
    skill_key INT NOT NULL,
    FOREIGN KEY (job_key) REFERENCES data_warehouse.job_postings_fact (job_key),
    FOREIGN KEY (skill_key) REFERENCES data_warehouse.skills_dim (skill_key)
);

-- 5. Set ownership of the tables to the postgres user

ALTER TABLE data_warehouse.company_dim OWNER to postgres;
ALTER TABLE data_warehouse.skills_dim OWNER to postgres;
ALTER TABLE data_warehouse.job_title_dim OWNER to postgres;
ALTER TABLE data_warehouse.location_dim OWNER to postgres;
ALTER TABLE data_warehouse.job_profile_dim OWNER to postgres;
ALTER TABLE data_warehouse.job_via_dim OWNER to postgres;
ALTER TABLE data_warehouse.date_posted_dim OWNER to postgres;
ALTER TABLE data_warehouse.job_postings_fact OWNER to postgres;
ALTER TABLE data_warehouse.skills_job_dim OWNER to postgres;

-- 6. Create indexes on foreign key columns for better performance

CREATE INDEX idx_company_key ON data_warehouse.job_postings_fact (company_key);
CREATE INDEX idx_job_title_key ON data_warehouse.job_postings_fact (job_title_key);
CREATE INDEX idx_location_key ON data_warehouse.job_postings_fact (location_key);
CREATE INDEX idx_job_profile_key ON data_warehouse.job_postings_fact (job_profile_key);
CREATE INDEX idx_job_via_key ON data_warehouse.job_postings_fact (job_via_key);
CREATE INDEX idx_date_key ON data_warehouse.job_postings_fact (date_key);
CREATE INDEX idx_skill_key ON data_warehouse.skills_job_dim (skill_key);
CREATE INDEX idx_job_key ON data_warehouse.skills_job_dim (job_key);
