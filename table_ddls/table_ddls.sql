CREATE TABLE raw.applicant_raw (
    applicant_id SERIAL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(10),
    marital_status VARCHAR(20),
    number_of_dependents INTEGER
);

CREATE TABLE raw.contact_information_raw (
    contact_id SERIAL,
    applicant_id INTEGER,
    home_address VARCHAR(255),
    city VARCHAR(100),
    state_province VARCHAR(100),
    postal_code VARCHAR(255),
    country VARCHAR(100),
    primary_phone_number VARCHAR(50),
    email_address VARCHAR(100),
    move_in_date DATE
);

CREATE TABLE raw.credit_information_raw (
    credit_id SERIAL,
    applicant_id INTEGER,
    total_credit_limit INTEGER,
    credit_score INTEGER,
    credit_history_length NUMERIC(4,2),
    number_of_late_payments INTEGER,
    bankruptcies_filed BOOLEAN,
    foreclosures BOOLEAN,
    credit_card_debt INTEGER,
    number_of_hard_inquiries INTEGER
);

CREATE TABLE raw.employment_information_raw (
    employment_id SERIAL,
    applicant_id INTEGER,
    employment_status VARCHAR(20),
    employer_name VARCHAR(255),
    job_title VARCHAR(100),
    employment_start_date DATE,
    years_in_current_job DECIMAL(4,2)
);

CREATE TABLE raw.financial_information_raw (
    applicant_id INTEGER,
    post_tax_annual_income DECIMAL(15,2),
    total_monthly_expenses DECIMAL(15,2),
    monthly_emi_amount DECIMAL(15,2),
    other_debts DECIMAL(15,2)
);

CREATE TABLE raw.loan_application_raw (
    loan_application_id SERIAL,
    applicant_id INTEGER,
    application_date DATE,
    loan_amount_requested DECIMAL(15,2),
    loan_purpose VARCHAR(100),
    loan_type VARCHAR(50),
    loan_term INTEGER,
    interest_rate_type VARCHAR(20)
);
