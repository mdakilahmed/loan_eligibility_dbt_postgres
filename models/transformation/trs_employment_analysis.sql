
-- This model creates a view of employment information for applicants by selecting relevant fields
-- from the staging model 'stg_employment_information'.
-- It includes applicant_id, employment status, employer name, job title, employment start date, and years in current job.
-- Additionally, it calculates employment stability using a CASE statement, which returns TRUE if 
-- the applicant has been employed for one year or more, and FALSE otherwise.
-- The resulting view provides insights into the stability of each applicant's employment history.

{{ config(materialized='view') }}

-- Determines 'employment_stability' based on 'years_in_current_job'.
SELECT
    e.applicant_id,
    e.employment_status,
    e.employer_name,
    e.job_title,
    e.employment_start_date,
    e.years_in_current_job,
    CASE
        WHEN e.years_in_current_job >= 1 THEN TRUE
        ELSE FALSE
    END AS employment_stability
FROM {{ ref('stg_employment_information') }} e
