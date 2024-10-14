import psycopg2
import csv
import os

# Directory where the CSV files are located
CSV_DIRECTORY = 'data-generate-loader'

def convert_values(row, columns):
    """Convert empty strings to None, fill integers with 0, and handle data type conversions.
    
    Args:
        row (list): A list of values from a CSV row.
        columns (list): A list of column names corresponding to the row values.
        
    Returns:
        list: A list of converted values, with empty strings handled appropriately.
    """
    converted_row = []  # Initialize a list to hold converted values
    for value, column in zip(row, columns):
        if value == "":
            # Fill integers with 0 and varchar with None
            if column in ['credit_score', 'credit_history_length', 'number_of_late_payments',
                          'bankruptcies_filed', 'foreclosures', 'number_of_hard_inquiries',
                          'monthly_net_income', 'total_monthly_expenses', 'existing_loan_payments',
                          'credit_card_debt', 'other_debts']:
                converted_row.append(0)  # Fill integer fields with 0
            else:
                converted_row.append(None)  # Fill varchar fields with None
        elif value in ('True', 'False'):
            converted_row.append(value == 'True')  # Convert string booleans to actual booleans
        else:
            # Handle specific data type conversions based on column names
            if column in ['credit_score', 'credit_history_length', 'number_of_late_payments',
                          'bankruptcies_filed', 'foreclosures', 'number_of_hard_inquiries',
                          'monthly_net_income', 'total_monthly_expenses', 'existing_loan_payments',
                          'credit_card_debt', 'other_debts']:
                converted_row.append(int(value))  # Convert string numbers to integers
            elif column == 'application_date':
                converted_row.append(value)  # Keep date as a string
            else:
                converted_row.append(value)  # For other columns, keep the original value
    return converted_row  # Return the list of converted values

def load_csv_to_postgres(table_name, csv_file, columns):
    """Load data from a CSV file into a specified PostgreSQL table.
    
    Args:
        table_name (str): The name of the PostgreSQL table to insert data into.
        csv_file (str): The name of the CSV file containing the data.
        columns (list): A list of column names that correspond to the CSV file structure.
    """
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",         # Database host
        database="loan_database", # Name of the database
        user="postgres",          # Username for the database
        password="akil"          # Password for the database user
    )
    cursor = conn.cursor()  # Create a cursor object to execute SQL commands

    # Open the specified CSV file for reading
    with open(os.path.join(CSV_DIRECTORY, csv_file), 'r', encoding='utf-8') as f:
        reader = csv.reader(f)  # Create a CSV reader object
        next(reader)  # Skip the header row
        
        # Iterate over each row in the CSV
        for row in reader:
            # Convert values in the row based on their data types
            row = convert_values(row, columns)
            placeholders = ', '.join(['%s'] * len(columns))  # Create placeholders for SQL query
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"  # Prepare the INSERT query
            try:
                cursor.execute(query, row)  # Execute the INSERT query with the row data
            except Exception as e:
                print(f"Error inserting into {table_name}: {e}")  # Print the error message
                print(f"Row data: {row}")  # Print the problematic row data
                conn.rollback()  # Roll back the transaction to avoid partial inserts
                continue  # Skip to the next row

    conn.commit()  # Commit the transaction to save changes
    cursor.close()  # Close the cursor to free resources
    conn.close()    # Close the database connection
    print(f"Data loaded into {table_name} from {csv_file}")  # Print a success message for each table

# Load data into various PostgreSQL tables from corresponding CSV files
load_csv_to_postgres(
    'raw.applicant_raw',
    'applicant_raw.csv',
    ['applicant_id', 'first_name', 'last_name', 'date_of_birth', 'gender', 'marital_status', 'number_of_dependents']
)

load_csv_to_postgres(
    'raw.contact_information_raw',
    'contact_information_raw.csv',
    ['contact_id', 'applicant_id', 'home_address', 'city', 'state_province', 'postal_code', 'country', 'primary_phone_number', 'email_address', 'move_in_date']
)

load_csv_to_postgres(
    'raw.credit_information_raw',
    'credit_information_raw.csv',
    ['credit_id', 'applicant_id', 'credit_score', 'credit_history_length', 'number_of_late_payments', 'bankruptcies_filed', 'foreclosures', 'credit_card_debt', 'total_credit_limit', 'number_of_hard_inquiries']
)

load_csv_to_postgres(
    'raw.employment_information_raw',
    'employment_information_raw.csv',
    ['employment_id', 'applicant_id', 'employment_status', 'employer_name', 'job_title', 'employment_start_date', 'years_in_current_job']
)

load_csv_to_postgres(
    'raw.financial_information_raw',
    'financial_information_raw.csv',
    ['applicant_id', 'post_tax_annual_income', 'total_monthly_expenses', 'monthly_emi_amount', 'other_debts']
)

load_csv_to_postgres(
    'raw.loan_application_raw',
    'loan_application_raw.csv',
    ['loan_application_id', 'applicant_id', 'application_date', 'loan_amount_requested', 'loan_purpose', 'loan_type', 'loan_term', 'interest_rate_type']
)

print("Data loading complete.")  # Print a final message indicating all data has been loaded
