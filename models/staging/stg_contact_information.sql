
-- This model creates a view of unique contact information for applicants from the raw.contact_information_raw source.
-- It ranks contact records based on the move-in date in descending order, 
-- and for ties, it orders by primary phone number in ascending order.
-- For each unique applicant_id, only the most recent contact record (based on move_in_date) is selected.
-- The output includes contact_id, applicant_id, home address, city, state/province, postal code, 
-- country, primary phone number, email address, and move-in date.
-- The WHERE clause filters out any entries where applicant_id is NULL,
-- ensuring that only valid contact records are retained in the final result set.

{{ config(materialized='view') }}

-- Selects the most recent contact info per applicant based on move_in_date.
WITH ranked_contacts AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY applicant_id
            ORDER BY move_in_date DESC, primary_phone_number ASC
        ) AS row_num
    FROM {{ source('raw', 'contact_information_raw') }}
    WHERE applicant_id IS NOT NULL
)

SELECT
    contact_id,
    applicant_id,
    home_address,
    city,
    state_province,
    postal_code,
    country,
    primary_phone_number,
    email_address,
    move_in_date
FROM ranked_contacts
WHERE row_num = 1

