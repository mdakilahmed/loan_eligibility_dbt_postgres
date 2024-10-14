
-- This model creates a view of applicants by selecting all columns from the staging model 'stg_applicants'.
-- It also calculates the age of each applicant by determining the difference in years between the current date
-- and their date of birth using the DATE_PART and AGE functions.
-- The resulting view includes all applicant details along with a new column 'age', representing the calculated age of each applicant.

{{ config(materialized='view') }}

-- Adds calculated 'age' to applicants.
WITH applicants AS (
    SELECT
        *,
        DATE_PART('year', AGE(CURRENT_DATE, date_of_birth)) AS age
    FROM {{ ref('stg_applicants') }}
)

SELECT * FROM applicants
