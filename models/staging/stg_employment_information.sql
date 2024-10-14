
-- This model creates a view of unique employment information for applicants from the raw.employment_information_raw source.
-- It ranks employment records based on the employment start date in descending order.
-- For each unique applicant_id, only the most recent employment record (based on employment_start_date) is selected.
-- The output includes employment_id, applicant_id, employment status, employer name, job title, 
-- employment start date, and years in current job.
-- The WHERE clause filters out any entries where applicant_id is NULL,
-- ensuring that only valid employment records are retained in the final result set.

{{ config(materialized='view') }}

-- Selects the most recent employment record per applicant.
WITH ranked_employment AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY applicant_id
            ORDER BY employment_start_date DESC
        ) AS row_num
    FROM {{ source('raw', 'employment_information_raw') }}
    WHERE applicant_id IS NOT NULL
)

SELECT
    employment_id,
    applicant_id,
    employment_status,
    employer_name,
    job_title,
    employment_start_date,
    years_in_current_job
FROM ranked_employment
WHERE row_num = 1
