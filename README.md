# China Car Owners NationWide Data Cleaning and Processing Functions

This repository contains a set of Python functions designed to clean and process data, a Jupyter file is included (created using the Google Colab environment) as well as Python scripts. These functions are built to handle common data quality issues such as invalid emails, alphanumeric validation, duplicate removal, and data standardization.

## Functions

### `process_drop_cols_csv`

**Description:** Reads a CSV file, drops specified columns, and writes the modified data to a new CSV file.

**How it Works:**
1. Reads the input CSV file using pandas `read_csv`.
2. Drops the specified columns using the `drop` method of the DataFrame.
3. Writes the resulting DataFrame to a new CSV file using `to_csv`.

### `remove_duplicate_records`

**Description:** Identifies and removes duplicate records from a pandas DataFrame based on specified columns.

**How it Works:**
1. Uses the `drop_duplicates` method of the DataFrame to identify and remove duplicate rows based on the provided columns.
2. Creates a new DataFrame containing only the duplicate records.
3. Returns both the updated DataFrame (with duplicates removed) and the DataFrame containing duplicates.

### `process_duplicates_csv`

**Description:** Processes a single CSV file, removes duplicates, and outputs valid and duplicate dataframes to CSV files.

**How it Works:**
1. Reads the input CSV using `pd.read_csv`.
2. Calls `remove_duplicate_records` to identify and remove duplicates.
3. Saves the valid and duplicate dataframes to separate CSV files using `to_csv`.

### `split_csv_into_chunks`

**Description:** Splits a large CSV file into smaller chunks using the chunksize parameter.

**How it Works:**
1. Reads the input CSV file in chunks using `pd.read_csv` with the `chunksize` parameter.
2. Iterates through each chunk and saves it to a separate CSV file in the specified output directory.

### `validate_alphanumeric_columns`

**Description:** Checks if specified columns contain only alphanumeric characters and separates invalid records.

**How it Works:**
1. Iterates through each specified column.
2. Uses pandas string methods to check if the column values contain only alphanumeric characters.
3. Creates a new DataFrame containing invalid records.
4. Removes invalid records from the original DataFrame.
5. Returns the updated DataFrame and the error DataFrame.

### `remove_time_from_date`

**Description:** Removes the time component from date columns in a DataFrame.

**How it Works:**
1. Iterates through the specified date columns.
2. Converts the column to datetime objects using `pd.to_datetime`.
3. Extracts the date component using the `dt.date` attribute.

### `validate_and_remove_invalid_emails`

**Description:** Validates email addresses in a dataframe, appends records with invalid email addresses to a new dataframe, and removes them from the original dataframe.

**How it Works:**
1. Uses a regular expression to validate email addresses in the specified column.
2. Creates a new DataFrame to store records with invalid email addresses.
3. Iterates through the DataFrame and checks each email address.
4. Appends invalid email records to the error DataFrame.
5. Removes invalid email records from the original DataFrame.
6. Returns the updated DataFrame and the error DataFrame.

### `validate_email_dataframe`

**Description:** Validates email addresses in a DataFrame column and optionally sets them to null.

**How it Works:**
1. Applies a validation function to the email column using the `apply` method.
2. Converts email addresses to lowercase using `str.lower()`.
3. Checks if the email matches the `null_if_match` string or is invalid using a regular expression.
4. Sets invalid emails to null.

### `combine_columns`

**Description:** Combines multiple columns into a single column in a DataFrame.

**How it Works:**
Uses the pandas apply method to combine the values from the specified columns into a new column.
The values are joined using the provided separator (default is a space).
Drops the original columns that were combined.   

### `process_chunked_csvs_output_folders`

**Description:** Processes chunked CSV files from a specified folder, runs validation functions, and outputs the cleaned chunks in specified folders.

**How it Works:**
1. Iterates through each CSV file in the input folder.
2. Reads the CSV file into a pandas DataFrame.
3. Applies data cleaning and validation functions to the DataFrame.
4. Saves the cleaned chunk to the output valid folder.
5. Saves any error data to the output error folder.

### `process_chunked_csvs`

**Description:** Processes chunked CSV files from a specified folder, runs validation functions, and merges the results into final valid and error CSV files.

**How it Works:**
Iterates through each CSV file in the input folder.
Reads the CSV file into a pandas DataFrame using pd.read_csv.
Applies data validation and cleaning functions (like `validate_email_dataframe`, `combine_columns`) to the DataFrame.
Appends the cleaned data to a `valid_df` and error data to an `error_df`.
After processing all chunks, saves the `valid_df` and `error_df` to the specified output CSV files.

### `combine_csv_chunks`

**Description:** Combines multiple CSV chunks from a folder into a single CSV file.

**How it Works:**
Iterates through each CSV file in the input folder.
Reads the CSV file into a pandas DataFrame using pd.read_csv.
Appends the DataFrame to a `combined_df`.
After processing all chunks, saves the `combined_df` to the specified output CSV file.

This pipeline is designed to ensure data quality and consistency by identifying and handling duplicates, validating email addresses, removing irrelevant columns, and addressing other data inconsistencies. The modular nature of the functions allows for easy adaptation and extension to suit different data processing needs. 

## Error Checking and Logging

The pipeline adopts a proactive approach by anticipating potential issues and implementing measures to handle them gracefully. The use of error checking and logging ensures that the pipeline remains reliable and provides valuable insights into its execution.

**Graceful Handling of Errors**: The pipeline can continue running even if some errors occur, preventing complete disruption.

**Easy Debugging**: Log files offer a detailed record of events, making it easier to identify and fix issues.

**Improved Maintainability**: The code becomes more robust and maintainable due to the structured error handling and logging.

**Monitoring and Auditing**: Log files can be used for monitoring the pipeline's performance and auditing its operations.
