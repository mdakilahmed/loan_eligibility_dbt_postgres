
-- This model creates a view of contact information for applicants by selecting relevant fields 
-- from the staging model 'stg_contact_information'.
-- It includes applicant_id, home address, city, state/province, postal code, country, primary phone number, 
-- email address, and a cleaned-up move-in date. The COALESCE function is used to replace any NULL move-in dates 
-- with a default date of '1990-01-01'.
-- Additionally, the model calculates:
-- - Years at current residence: The number of years from the move-in date to the current date.
-- - Residence stability: A boolean value indicating whether the applicant has lived at their current residence 
--   for one year or more, determined by a CASE statement.
-- The resulting view provides insights into each applicant's current residence status and stability.

{{ config(materialized='view') }}

-- Calculates 'years_at_current_residence' and 'residence_stability'.
WITH contact_info AS (
    SELECT
        *,
        COALESCE(move_in_date, DATE '1990-01-01') AS move_in_date_clean
    FROM {{ ref('stg_contact_information') }}
)

SELECT
    applicant_id,
    home_address,
    city,
    state_province,
    postal_code,
    country,
    primary_phone_number,
    email_address,
    move_in_date_clean AS move_in_date,
    DATE_PART('year', AGE(CURRENT_DATE, move_in_date_clean)) AS years_at_current_residence,
    CASE
        WHEN DATE_PART('year', AGE(CURRENT_DATE, move_in_date_clean)) >= 1 THEN TRUE
        ELSE FALSE
    END AS residence_stability
FROM contact_info
