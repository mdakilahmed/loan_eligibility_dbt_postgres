import os
import csv
import random
from faker import Faker

# Initialize Faker for generating fake data
fake = Faker()

# Configuration constants
OUTPUT_DIRECTORY = 'data-generate-loader'  # Directory to store generated CSV files
NUM_APPLICANTS = 100000  # Number of applicants to generate
NULL_PROBABILITY = 0.1  # Probability of generating a null value
MESSY_PROBABILITY = 0.05  # Probability of generating a messy value

# Create the output directory if it does not already exist
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

def generate_null_or_value(value, null_probability=NULL_PROBABILITY):
    """Return None with a certain probability; otherwise, return the value."""
    return value if random.random() > null_probability else None

def generate_messy_or_valid(value, mess_probability=MESSY_PROBABILITY):
    """Return a 'messy' version of the value with a certain probability; otherwise, return the valid value."""
    return str(value) + '_M' if random.random() < mess_probability else value

def write_csv(file_name, headers, rows):
    """Write rows of data to a CSV file with specified headers."""
    file_path = os.path.join(OUTPUT_DIRECTORY, file_name)  # Full path for the CSV file
    try:
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)  # Write the headers to the CSV file
            writer.writerows(rows)    # Write the data rows to the CSV file
    except IOError as e:
        print(f"Error writing file {file_path}: {e}")  # Handle file writing errors

def generate_applicant_data(num_applicants):
    """Generate random data for the applicant records."""
    applicant_rows = []  # List to hold applicant rows
    applicant_ids = set()  # Set to keep track of unique applicant IDs

    # Continue generating applicants until the desired number is reached
    while len(applicant_rows) < num_applicants:
        applicant_id = random.randint(100000, 999999)  # Generate a random applicant ID
        if applicant_id in applicant_ids:  # Check for duplicate IDs
            continue

        applicant_ids.add(applicant_id)  # Add the new ID to the set to prevent duplicates
        row = [
            applicant_id,
            fake.first_name(),  # Generate a random first name
            generate_messy_or_valid(fake.last_name()),  # Generate a last name (possibly messy)
            generate_null_or_value(fake.date_of_birth(minimum_age=18, maximum_age=45)),  # Generate a date of birth with null possibility
            random.choice(['Male', 'Female', 'Other']),  # Randomly assign a gender
            random.choice(['Single', 'Married', 'Divorced', 'Widowed']),  # Randomly assign marital status
            generate_null_or_value(random.randint(0, 5))  # Randomly generate number of dependents with null possibility
        ]
        applicant_rows.append(row)  # Add the row to the list

    # Write the applicant data to a CSV file
    write_csv('applicant_raw.csv', ['applicant_id', 'first_name', 'last_name', 'date_of_birth', 'gender', 'marital_status', 'number_of_dependents'], applicant_rows)
    return applicant_ids  # Return the set of applicant IDs

def generate_contact_data(applicant_ids):
    """Generate random contact information for the applicants."""
    contact_rows = []  # List to hold contact rows
    for applicant_id in applicant_ids:
        # Generate a move-in date based on a probability
        move_in_date = fake.date_between(start_date='-10y', end_date='-1y') if random.random() < 0.3 else fake.date_between(start_date='-1y', end_date='today')

        # Generate contact details, allowing for null values
        home_address = generate_null_or_value(fake.address())
        city = fake.city()  # Generate a random city
        state_province = fake.state()  # Generate a random state or province
        postal_code = generate_messy_or_valid(fake.zipcode())  # Generate a messy or valid postal code
        country = fake.country()  # Generate a random country

        email = generate_email_with_errors()  # Generate an email with potential errors
        phone_number = generate_phone_number_with_errors()  # Generate a phone number with potential errors

        row = [
            random.randint(100000, 999999),  # Generate a random 6-digit contact ID
            applicant_id,
            home_address,
            city,
            state_province,
            postal_code,
            country,
            generate_null_or_value(phone_number),  # Allow for a null phone number
            email,
            move_in_date
        ]
        contact_rows.append(row)  # Add the contact row to the list

    # Write the contact information to a CSV file
    write_csv('contact_information_raw.csv', ['contact_id', 'applicant_id', 'home_address', 'city', 'state_province', 'postal_code', 'country', 'primary_phone_number', 'email_address', 'move_in_date'], contact_rows)

def generate_email_with_errors():
    """Generate an email address that may contain errors."""
    email = fake.email()  # Generate a valid email address
    # Randomly select the type of error to apply
    error_type = random.choices(['valid', 'messy_extension', 'invalid_email', 'missing_extension'], weights=[0.7, 0.1, 0.1, 0.1])[0]

    # Apply the selected error type to the email
    if error_type == 'messy_extension':
        return email.replace('@', '@messy.')  # Add a messy extension
    elif error_type == 'invalid_email':
        return email.split('@')[0]  # Remove the domain for an invalid email
    elif error_type == 'missing_extension':
        return email.split('.')[0] + '@example'  # Replace with an example domain
    return email  # Return the valid email

def generate_phone_number_with_errors():
    """Generate a phone number that may contain errors."""
    phone_number = fake.phone_number()  # Generate a valid phone number
    # Randomly select the type of error to apply
    error_type = random.choices(['valid', 'invalid_number', 'missing_digits'], weights=[0.8, 0.1, 0.1])[0]

    # Apply the selected error type to the phone number
    if error_type == 'invalid_number':
        return '123-456-' + str(random.randint(1000, 9999))  # Generate an invalid number
    elif error_type == 'missing_digits':
        return phone_number[:-1]  # Remove the last digit for a missing digits error
    return phone_number  # Return the valid phone number

def generate_credit_data(applicant_ids):
    """Generate credit information for the applicants."""
    credit_rows = []  # List to hold credit rows
    for applicant_id in applicant_ids:
        credit_score = int(random.gauss(600, 50))  # Generate a credit score with a Gaussian distribution
        credit_score = min(max(credit_score, 300), 900)  # Clamp credit score between 300 and 900
        credit_history_length = random.randint(0, 20) if random.random() < 0.3 else random.randint(0, 2)  # Randomly determine credit history length
        number_of_late_payments = 0 if random.random() < 0.3 else random.randint(1, 10)  # Randomly determine number of late payments

        # Randomly determine bankruptcy and foreclosure status
        bankruptcies_filed = random.choice([True, False]) if random.random() >= 0.3 else False
        foreclosures = random.choice([True, False]) if random.random() >= 0.3 else False
        number_of_hard_inquiries = random.randint(0, 3) if random.random() < 0.3 else random.randint(4, 10)  # Randomly determine hard inquiries

        total_credit_limit = random.randint(5000, 100000)  # Generate a total credit limit
        credit_card_debt = random.randint(0, int(total_credit_limit * 0.3)) if random.random() < 0.3 else random.randint(int(total_credit_limit * 0.3), total_credit_limit)  # Generate credit card debt

        row = [
            random.randint(100000, 999999),  # Random 6-digit credit ID
            applicant_id,
            credit_score,
            credit_history_length,
            number_of_late_payments,
            bankruptcies_filed,
            foreclosures,
            credit_card_debt,
            total_credit_limit,
            number_of_hard_inquiries
        ]
        credit_rows.append(row)  # Add the credit row to the list

    # Write the credit information to a CSV file
    write_csv('credit_information_raw.csv', ['credit_id', 'applicant_id', 'credit_score', 'credit_history_length', 'number_of_late_payments', 'bankruptcies_filed', 'foreclosures', 'credit_card_debt', 'total_credit_limit', 'number_of_hard_inquiries'], credit_rows)

def generate_employment_data(applicant_ids):
    """Generate employment information for the applicants."""
    employment_rows = []  # List to hold employment rows
    for applicant_id in applicant_ids:
        # Randomly select employment status based on weights
        employment_status = random.choices(['Employed', 'Self-employed', 'Unemployed', 'Retired'], weights=[0.2, 0.1, 0.5, 0.2])[0]
        years_in_current_job = random.randint(1, 10) if employment_status in ['Employed', 'Self-employed'] else random.randint(0, 1)  # Determine years in current job

        row = [
            random.randint(100000, 999999),  # Random 6-digit employment ID
            applicant_id,
            employment_status,
            generate_null_or_value(fake.company()),  # Allow for a null employer name
            generate_messy_or_valid(fake.job()),  # Generate job title (possibly messy)
            generate_null_or_value(fake.date_this_decade()),  # Allow for a null employment start date
            round(years_in_current_job, 2)  # Round years in current job to 2 decimal places
        ]
        employment_rows.append(row)  # Add the employment row to the list

    # Write the employment information to a CSV file
    write_csv('employment_information_raw.csv', ['employment_id', 'applicant_id', 'employment_status', 'employer_name', 'job_title', 'employment_start_date', 'years_in_current_job'], employment_rows)

def generate_financial_data(applicant_ids):
    """Generate financial information for the applicants."""
    financial_rows = []  # List to hold financial rows
    monthly_net_incomes = {}  # Dictionary to hold monthly net incomes

    for applicant_id in applicant_ids:
        monthly_net_income = random.randint(3000, 300000)  # Generate random monthly net income
        post_tax_annual_income = monthly_net_income * 12 * 0.8  # Calculate post-tax annual income
        monthly_net_incomes[applicant_id] = monthly_net_income  # Store monthly net income

        row = [
            applicant_id,
            post_tax_annual_income,
            generate_null_or_value(random.randint(500, 50000)),  # Allow for a null total monthly expenses
            random.randint(0, int(post_tax_annual_income / 12 * 0.3)),  # Generate monthly EMI amount
            random.randint(0, int(post_tax_annual_income / 12 * 0.5))  # Generate other debts
        ]
        financial_rows.append(row)  # Add the financial row to the list

    # Write the financial information to a CSV file
    write_csv('financial_information_raw.csv', ['applicant_id', 'post_tax_annual_income', 'total_monthly_expenses', 'monthly_emi_amount', 'other_debts'], financial_rows)
    return monthly_net_incomes  # Return the dictionary of monthly net incomes

def generate_loan_application_data(applicant_ids, monthly_net_incomes):
    """Generate loan application data for the applicants."""
    loan_rows = []  # List to hold loan application rows
    for applicant_id in applicant_ids:
        monthly_net_income = monthly_net_incomes.get(applicant_id, random.randint(3000, 300000))  # Get monthly net income or generate a new one
        annual_income = monthly_net_income * 12  # Calculate annual income
        loan_amount_requested = min(int(annual_income * random.choices([0.30, 0.20, 0.15, 0.25, 0.10], weights=[0.30, 0.20, 0.15, 0.25, 0.10])[0]), 1000000)  # Determine loan amount requested

        row = [
            random.randint(100000, 999999),  # Random 6-digit loan application ID
            applicant_id,
            fake.date_this_year(),  # Generate the application date
            (loan_amount_requested // 100) * 100,  # Ensure loan amount is a multiple of 100
            random.choice(['Home', 'Car', 'Business', 'Personal']),  # Randomly assign loan purpose
            random.choice(['Fixed', 'Variable']),  # Randomly assign loan type
            random.randint(12, 360),  # Randomly assign loan term in months
            random.choice(['Fixed', 'Variable'])  # Randomly assign interest rate type
        ]
        loan_rows.append(row)  # Add the loan row to the list

    # Write the loan application data to a CSV file
    write_csv('loan_application_raw.csv', ['loan_application_id', 'applicant_id', 'application_date', 'loan_amount_requested', 'loan_purpose', 'loan_type', 'loan_term', 'interest_rate_type'], loan_rows)

def main():
    """Main function to generate all the required data."""
    applicant_ids = generate_applicant_data(NUM_APPLICANTS)  # Generate applicant data
    generate_contact_data(applicant_ids)  # Generate contact data
    generate_credit_data(applicant_ids)  # Generate credit information
    generate_employment_data(applicant_ids)  # Generate employment data
    monthly_net_incomes = generate_financial_data(applicant_ids)  # Generate financial data
    generate_loan_application_data(applicant_ids, monthly_net_incomes)  # Generate loan application data

    # Output completion message
    print(f"Data generation complete. CSV files saved in '{OUTPUT_DIRECTORY}' directory.")

if __name__ == "__main__":
    main()  # Execute the main function