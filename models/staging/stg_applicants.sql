-- This model creates a view of unique applicants from the raw.applicant_raw source. 
-- It ranks applicants based on their date of birth and last name, ensuring that 
-- for each unique applicant_id, only the earliest entry (based on date_of_birth) is selected. 
-- The output includes the applicant_id, first name, last name, date of birth, gender, 
-- marital status, and number of dependents. 
-- The WHERE clause filters out any entries where applicant_id is NULL, 
-- and only the top-ranked entry for each applicant_id is retained in the final result set.

{{ config(materialized='view') }}

-- Selects unique applicants by earliest date_of_birth and last_name.
WITH ranked_applicants AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY applicant_id
            ORDER BY date_of_birth ASC, last_name ASC
        ) AS row_num
    FROM {{ source('raw', 'applicant_raw') }}
    WHERE applicant_id IS NOT NULL
)

SELECT
    applicant_id,
    first_name,
    last_name,
    date_of_birth,
    gender,
    marital_status,
    number_of_dependents
FROM ranked_applicants
WHERE row_num = 1
