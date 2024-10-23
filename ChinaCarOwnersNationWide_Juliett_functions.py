# Import Necessary Libraries
import csv
import pandas as pd
import re
import unicodedata
import os
import logging
import datetime as dt

# All Functions
# Setup logging
logging.basicConfig(filename='processing_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# prompt: Create a function to read a specified CSV, drop columns from a dataframe based on a list of specified columns and convert the revised dataframe to a specified CSV.

import pandas as pd

def process_drop_cols_csv(input_csv, output_csv, columns_to_drop):
  """Reads a CSV, drops specified columns, and writes the result to another CSV.

  Args:
    input_csv: Path to the input CSV file.
    output_csv: Path to the output CSV file.
    columns_to_drop: A list of column names to drop from the dataframe.
  """

  try:
    print(f"Processing CSV file ({input_csv}). Columns to be dropped are {columns_to_drop}")
    logging.info(f"Processing CSV file ({input_csv}). Columns to be dropped are {columns_to_drop}")

    df = pd.read_csv(input_csv, encoding='utf-8',low_memory=True)
    df = df.drop(columns=columns_to_drop, errors="ignore")
    df.to_csv(output_csv, encoding='utf-8-sig',index=False)

    print(f"CSV file ({input_csv}) processed successfully. Output saved to {output_csv}")
    logging.info(f"CSV file ({input_csv}) processed successfully. Output saved to {output_csv}")
  except FileNotFoundError:
    print(f"Error: Input file not found at {input_csv}")
    logging.error(f"Error: Input file not found at {input_csv}")
  except Exception as e:
    print(f"An error occurred: {e}")
    logging.error(f"An error occurred: {e}")


# prompt: Create a function to remove duplicate records based on specified columns. Add the duplicates records to separate dataframe and drop them from the original. Include error checking and logging.

def remove_duplicate_records(df, columns):
    """
    Removes duplicate records based on specified columns.
    Adds duplicate records to a separate dataframe and drops them from the original.

    Args:
        df (pd.DataFrame): The dataframe to process.
        columns (list): A list of column names to consider for duplicate detection.

    Returns:
        tuple: A tuple containing the updated dataframe with unique records
               and a new dataframe with duplicate records.
    """
    try:
        print(f"Processing dataframe to remove and store duplicate records.")
        logging.info(f"Processing dataframe to remove and store duplicate records.")
        duplicate_df = pd.DataFrame()
        df_deduplicated = df.drop_duplicates(subset=columns, keep='first')
        duplicate_rows = df[~df.index.isin(df_deduplicated.index)]

        if not duplicate_rows.empty:
            duplicate_df = pd.concat([duplicate_df, duplicate_rows], ignore_index=True)

        print(f"Duplicate removal complete. Duplicate records appended to duplicate_df.")
        logging.info(f"Duplicate removal complete. Duplicate records appended to duplicate_df.")
        return df_deduplicated, duplicate_df

    except Exception as e:
        print(f"Error occurred during duplicate removal: {e}")
        logging.error(f"Error occurred during duplicate removal: {e}")
        return df, pd.DataFrame()

# prompt: Create a function to process a specified CSV file and then run the function to remove duplicates and convert the valid and duplicates dataframes to csv files.

def process_duplicates_csv(file_path, output_valid_csv, output_duplicates_csv, columns, sep=','):
    """
    Processes a single CSV file, removes duplicates, and outputs valid and duplicate dataframes to CSV files.
    """
    try:
        df = pd.read_csv(file_path, sep=sep, low_memory=True, encoding='utf-8')
        print(f"Processing CSV file: {file_path} for duplicates.")
        logging.info(f"Processing CSV file: {file_path} for duplicates.")

        # Remove duplicates based on email
        df, duplicates_df = remove_duplicate_records(df, columns)

        # Save valid and duplicate dataframes to CSV files
        df.to_csv(output_valid_csv, index=False, encoding='utf-8-sig')
        duplicates_df.to_csv(output_duplicates_csv, index=False, encoding='utf-8-sig')

        print(f"Processed file: {file_path}. Valid data saved to {output_valid_csv}, duplicates to {output_duplicates_csv}.")
        logging.info(f"Processed file: {file_path}. Valid data saved to {output_valid_csv}, duplicates to {output_duplicates_csv}.")

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        logging.error(f"Error processing file {file_path}: {e}")


# prompt: Create a function to split a large csv into chunks in a specified folder or path using the chunksize parameter in read_csv

def split_csv_into_chunks(file_path, chunksize, output_directory, sep=','):
  """Splits a large CSV file into smaller chunks using the chunksize parameter.

  Args:
    file_path: The path to the large CSV file.
    chunksize: The number of rows per chunk.
    output_directory: The directory where the chunks should be saved.
  """
  try:
    print(f"Processing CSV file: {file_path} to split into chunks.")
    logging.info(f"Processing CSV file: {file_path} to split into chunks.")

    if not os.path.exists(output_directory):
      os.makedirs(output_directory)

    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunksize, sep=sep, encoding='utf-8')):
      output_file = os.path.join(output_directory, f"chunk_{i+1}.csv")
      chunk.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"File '{file_path}' split into {i+1} chunks in '{output_directory}'.")
    logging(f"File '{file_path}' split into {i+1} chunks in '{output_directory}'.")

  except FileNotFoundError as e:
    print(f"Error: {e}")
    logging.error(f"Error: {e}")
  except Exception as e:
    print(f"An unexpected error occurred when splitting CSV into chunks: {e}")
    logging.error(f"An unexpected error occurred when splitting CSV into chunks: {e}")

# prompt: Create a function to get chunked CSVs from a specified folder, runs the validation functions and outputs the cleaned chunks in specified folders. Include error checking and logging.

def process_chunked_csvs_output_folders(input_folder, output_valid_folder, output_error_folder, email_column_name='email', date_columns=['created_at']):
  """
  Processes chunked CSV files from a specified folder, runs validation functions,
  and outputs the cleaned chunks in specified folders. Includes error checking and logging.

  Args:
      input_folder (str): The path to the folder containing chunked CSV files.
      output_valid_folder (str): The path to the folder to output cleaned chunks.
      output_error_folder (str): The path to the folder to output chunks with errors.
      email_column_name (str): The name of the email column.
      date_columns (list): A list of column names to consider for date validation.
  """

  try:
    # Setup logging
    #logging.basicConfig(filename='processing_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    print(f"Processing chunks from: {input_folder} for data cleaning and output to folders.")
    logging.info(f"Processing chunks from: {input_folder} for data cleaning and output to folders.")

    for filename in os.listdir(input_folder):
      if filename.endswith(".csv"):
        file_path = os.path.join(input_folder, filename)
        print(f"Processing file: {file_path}")
        logging.info(f"Processing file: {file_path}")

        try:
          df = pd.read_csv(file_path, low_memory=True, encoding='utf-8')

          # Run validation functions
          df = validate_email_dataframe(df, email_column_name,'noemail')
          columns_to_combine = ['address', 'province', 'city', 'postal_code']
          new_column_name = 'full_address'
          df = combine_columns(df, columns_to_combine, new_column_name)
          columns_to_check = ['vehicle_identification_number', 'id_card_number']
          df, chunk_error_df = validate_alphanumeric_columns(df, columns_to_check)

          # Output cleaned chunk
          if not os.path.exists(output_valid_folder):
            os.makedirs(output_valid_folder)

          output_valid_file = os.path.join(output_valid_folder, f"valid_{filename}")
          df.to_csv(output_valid_file, index=False, encoding='utf-8-sig')
          print(f"Final valid data saved to {output_valid_file}.")
          logging.info(f"Final valid data saved to {output_valid_file}.")

          # Output chunk with errors
          if not os.path.exists(output_error_folder):
            os.makedirs(output_error_folder)

          output_error_file = os.path.join(output_error_folder, f"error_{filename}")
          chunk_error_df.to_csv(output_error_file, index=False, encoding='utf-8-sig')
          print(f"Final error data saved to {output_error_file}.")
          logging.info(f"Final error data saved to {output_error_file}.")

          print(f"File from {file_path} processed successfully.")
          logging.info(f"File {file_path} processed successfully.")

        except Exception as e:
          print(f"Error processing file {file_path}: {e}")
          logging.error(f"Error processing file {file_path}: {e}")

  except Exception as e:
    print(f"Critical error during processing: {e}")
    logging.critical(f"Critical error during processing: {e}")

# prompt: Create a function to check whether the specified columns contain alphanumerical characters only and if they don't save the invalid records to a dataframe, dropthe invalid record from the original dataframe. Return both the cleaned and invalid records dataframes.

def validate_alphanumeric_columns(df, columns_to_validate):
    """Checks if specified columns contain only alphanumeric characters and separates invalid records.

    Args:
      df: The pandas DataFrame.
      columns_to_validate: A list of column names to validate.

    Returns:
      A tuple containing two DataFrames: (cleaned_df, invalid_records_df)
    """
    invalid_records_df = pd.DataFrame()

    try:
        for column in columns_to_validate:
            if column not in df.columns:
                print(f"Column '{column}' not found in DataFrame.")
                logging.error(f"Column '{column}' not found in DataFrame.")
                raise KeyError(f"Column '{column}' not found in DataFrame.")

            logging.info(f"Validating column '{column}' for alphanumeric characters.")

            # Try to convert column to string and check for alphanumeric characters
            try:
                invalid_rows = df[~df[column].astype(str).str.isalnum()]
                invalid_records_df = pd.concat([invalid_records_df, invalid_rows])
                df = df[df[column].astype(str).str.isalnum()]

                print(f"Finished validating column '{column}'. Invalid rows found: {len(invalid_rows)}.")
                logging.info(f"Finished validating column '{column}'. Invalid rows found: {len(invalid_rows)}.")

            except Exception as e:
                print(f"Error processing column '{column}': {e}")
                logging.error(f"Error processing column '{column}': {e}")
                raise ValueError(f"Error processing column '{column}': {e}")

    except Exception as e:
        print("An error occurred during validation.")
        logging.exception("An error occurred during validation.")
        raise e

    return df, invalid_records_df

# prompt: Create a function to remove the time from a date in specified columns

def remove_time_from_date(df, columns):
  """Removes the time component from date columns in a DataFrame.

  Args:
    df: The DataFrame containing the date columns.
    columns: A list of column names to process.

  Returns:
    The DataFrame with the time component removed from the specified columns.
  """
  try:
    print(f"Removing time from specified date columns: {columns}")
    logging.info(f"Removing time from specified date columns: {columns}")
    for column in columns:
      if column in df.columns:
        # Convert to datetime if not already
        df[column] = pd.to_datetime(df[column], errors='coerce')
        # Remove the time component
        df[column] = df[column].dt.date
    print("Date cleaning complete. Time removed from date columns.")
    logging.info("Date cleaning complete. Time removed from date columns.")
    return df
  except Exception as e:
    print(f"An error occurred during removing time from date function: {e}")
    logging.error(f"An error occurred during removing time from date function: {e}")
    return df


# prompt: create a function to validate emails addresses in a dataframe, append the records with an invalid email address to a dataframe and drop them from the original dataframe. Return both the updated dataframe and the error dataframe. Include error checking and loggging.

def validate_and_remove_invalid_emails(df, email_column):
    """
    Validates email addresses in a dataframe, appends records with invalid email
    addresses to a new dataframe, and removes them from the original dataframe.

    Args:
        df (pd.DataFrame): The dataframe containing email addresses.
        email_column (str): The name of the column containing email addresses.

    Returns:
        tuple: A tuple containing the updated dataframe with valid email addresses
            and a new dataframe with records containing invalid email addresses.
    """
    try:
        print(f"Removing invalid emails from {email_column}.")
        logging.info(f"Removing invalid emails from {email_column}.")

        # Regular expression for basic email validation
        email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

        # Create a new dataframe to store records with invalid email addresses
        error_df = pd.DataFrame()

        # Iterate through the dataframe and validate email addresses
        for index, row in df.iterrows():
            email = row[email_column]
            if not re.match(email_regex, email):
                # Append record to the error dataframe
                error_df = pd.concat([error_df, pd.DataFrame([row])], ignore_index=True)
                # Drop the record from the original dataframe
                df.drop(index, inplace=True)

        print("Validation complete. Invalid email records appended to error_df.")
        logging.info("Validation complete. Invalid email records appended to error_df.")
        return df, error_df

    except Exception as e:
        print(f"Error occurred during email validation: {e}")
        logging.error(f"Error occurred during email validation: {e}")
        return df, pd.DataFrame()  # Return empty error dataframe in case of error

# prompt: create a function to validate email addresses in a dataframe, all values in the column should be set as lower case, option to see the value as null if it matches a specified string or if the email address is invalid. Return the updated dataframe. Include error checking.

def validate_email_dataframe(df, email_column, null_if_match=None):
    """Validates email addresses in a DataFrame column and optionally sets them to null.

    Args:
      df: The pandas DataFrame.
      email_column: The name of the column containing email addresses.
      null_if_match: An optional string. If an email address matches this string
        or is invalid, it will be set to null.

    Returns:
      The updated DataFrame with validated email addresses.

    Raises:
      KeyError: If email_column is not found in the DataFrame.
    """

    try:
        # Check if the column exists in the DataFrame
        if email_column not in df.columns:
            print(f"Column '{email_column}' not found in DataFrame.")
            logging.error(f"Column '{email_column}' not found in DataFrame.")
            raise KeyError(f"Column '{email_column}' not found in DataFrame.")

        print(f"Validating emails in column '{email_column}'.")
        logging.info(f"Validating emails in column '{email_column}'.")

        # Function to validate email addresses
        def validate_email(email):
            try:
                if email is None or pd.isnull(email):
                    return None
                email = str(email).lower()  # Convert to lowercase

                # Check if the email matches the 'null_if_match' string
                if null_if_match and null_if_match in str(email):
                    #logging.info(f"Email '{email}' matches null_if_match condition, setting to None.")
                    return None

                # Regex pattern for valid email
                pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                if re.fullmatch(pattern, email):
                    return email
                else:
                    #logging.warning(f"Email '{email}' is invalid, setting to None.")
                    return None

            except Exception as e:
                print(f"Error validating email '{email}': {e}")
                logging.error(f"Error validating email '{email}': {e}")
                return None

        # Apply the validation to the email column
        df[email_column] = df[email_column].apply(validate_email)
        print(f"Finished validating emails in column '{email_column}'.")
        logging.info(f"Finished validating emails in column '{email_column}'.")

    except KeyError as e:
        print(f"A KeyError occurred: {e}")
        logging.exception("A KeyError occurred: {e}.")
        raise e

    except Exception as e:
        print(f"An error occurred while validating emails: {e}")
        logging.exception(f"An error occurred while validating emails: {e}")
        raise e

    return df

# Combine the list of specified columns into a new column
def combine_columns(df, columns_to_combine, new_column_name, separator=" "):
  """Combines multiple columns into a single column in a DataFrame.

  Args:
    df: The pandas DataFrame.
    columns_to_combine: A list of column names to combine.
    new_column_name: The name of the new column.
    separator: The string used to separate values from different columns.

  Returns:
    The updated DataFrame with the combined column and old columns dropped.

  Raises:
    KeyError: If any of the specified columns are not found in the DataFrame.
  """

  try:
    print(f"Combining columns: {columns_to_combine} into {new_column_name}.")
    logging.info(f"Combining columns: {columns_to_combine} into {new_column_name}.")
    df[new_column_name] = df[columns_to_combine].apply(lambda row: separator.join(row.dropna().astype(str)), axis=1)
    df = df.drop(columns=columns_to_combine)
    print(f"Combined columns:{columns_to_combine} successfully.")
    logging.info(f"Combined columns: {columns_to_combine} successfully.")

    return df
  except KeyError as e:
    print(f"Error: Column(s) not found in DataFrame: {e}.")
    logging.error(f"Error: Column(s) not found in DataFrame: {e}.")
    raise e
  except Exception as e:
    print(f"An error occurred while combining columns: {e}.")
    logging.error(f"An error occurred while combining columns: {e}.")
    raise e

# prompt: Create a function to get chunked csvs from a specified folder, runs the validation functions and merges the chunks into a specified final valid csv file and final error csv file. INclude error checking and logging.

def process_chunked_csvs(input_folder, output_valid_csv, output_error_csv, email_column_name='email', date_columns=['created_at']):
  """
  Processes chunked CSV files from a specified folder, runs validation functions,
  and merges the results into final valid and error CSV files.

  Args:
      input_folder (str): The path to the folder containing chunked CSV files.
      output_valid_csv (str): The path to the output CSV file for valid records.
      output_error_csv (str): The path to the output CSV file for error records.
      output_duplicates_csv (str): The path to the output CSV file for duplicate records.
      email_column_name (str): The name of the email column.
      date_columns (list): A list of column names to consider for date validation.
  """

  try:
    # Setup logging
    #logging.basicConfig(filename='processing_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    valid_df = pd.DataFrame()
    error_df = pd.DataFrame()

    print(f"Processing chunks for data cleaning & combining from {input_folder} to single cleaned file: {output_valid_csv}")
    logging.info(f"Processing chunks for data cleaning & combining from {input_folder} to single cleaned file: {output_valid_csv}")

    for filename in os.listdir(input_folder):
      if filename.endswith(".csv"):
        file_path = os.path.join(input_folder, filename)
        print(f"Processing chunk: {file_path}")
        logging.info(f"Processing chunk: {file_path}")

        try:
          df = pd.read_csv(file_path, low_memory=True, encoding='utf-8')

          # Run validation functions
          df = validate_email_dataframe(df, email_column_name,'noemail')
          columns_to_combine = ['address', 'province', 'city', 'postal_code']
          new_column_name = 'full_address'
          df = combine_columns(df, columns_to_combine, new_column_name)
          columns_to_check = ['vehicle_identification_number', 'id_card_number']
          df, chunk_error_df = validate_alphanumeric_columns(df, columns_to_check)

          valid_df = pd.concat([valid_df, df], ignore_index=True)

          # Concatenate error dataframes
          #chunk_error_df = pd.concat([chunk_error_df,chunk_dup_error_df], ignore_index=True)
          error_df = pd.concat([error_df, chunk_error_df], ignore_index=True)

          print(f"File {file_path} processed successfully.")
          logging.info(f"File {file_path} processed successfully.")

        except Exception as e:
          print(f"Error processing file {file_path}: {e}")
          logging.error(f"Error processing file {file_path}: {e}")

    # Remove duplicates from full dataframe
    #valid_df, duplicates_df = remove_duplicate_records(valid_df, ['mail_address']) #remove duplicates based on email
    #error_df = pd.concat([error_df, chunk_dup_error_df], ignore_index=True)

    # Save final dataframes
    valid_df.to_csv(output_valid_csv, index=False, encoding='utf-8-sig')
    error_df.to_csv(output_error_csv, index=False, encoding='utf-8-sig')
    #duplicates_df.to_csv(output_duplicates_csv, index=False)

    print(f"Final cleaned data saved to {output_valid_csv}.")
    logging.info(f"Final cleaned data saved to {output_valid_csv}.")
    print(f"Final garbage data saved to {output_error_csv}.")
    logging.info(f"Final garbage data saved to {output_error_csv}.")
    #logging.info(f"Final duplicates data saved to {output_duplicates_csv}.")

  except Exception as e:
    print(f"Critical error during processing of chunks: {e}")
    logging.critical(f"Critical error during processing of chunks: {e}")

# prompt: Create a function that will combine csv chunks from a specified folder into one csv file

def combine_csv_chunks(input_folder, output_file):
  """
  Combines multiple CSV chunks from a folder into a single CSV file.

  Args:
    input_folder: The path to the folder containing CSV chunks.
    output_file: The path to the output CSV file.
  """

  combined_df = pd.DataFrame()
  print(f"Combining CSV chunks from {input_folder}")
  logging.info(f"Combining CSV chunks from {input_folder}")

  for filename in os.listdir(input_folder):
    if filename.endswith(".csv"):
      file_path = os.path.join(input_folder, filename)
      try:
        df = pd.read_csv(file_path, encoding='utf-8')
        combined_df = pd.concat([combined_df, df], ignore_index=True)
      except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        logging.error(f"Error reading file {file_path}: {e}")

  combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
  print(f"Combined CSV chunks saved to {output_file}")
  logging.info(f"Combined CSV chunks saved to {output_file}")


