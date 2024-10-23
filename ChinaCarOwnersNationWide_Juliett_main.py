# Import Custom Functions
from ChinaCarOwnersNationWide_Juliett_functions import *

# Step 1: Functions to run: 1. Drop unneccesary columns. 2. Check for duplicates, then use the valid CSV to create the chunks for further processing.
# Drop unneccesary columns:
input_csv = 'car-owners-china-v2.csv'
output_csv = 'car-owners-china-v3.csv'
columns_to_drop = ['gender', 'industry', 'monthly_salary', 'marital_status', 'education', 'brand', 'car_series', 'car_model', 'configuration', 'color', 'engine_number','Unnamed: 21']
process_drop_cols_csv(input_csv, output_csv, columns_to_drop)

# Check for Duplicates in the Original CSV:
process_duplicates_csv('car-owners-china-v3.csv', 'car-owners-china_valid.csv', 'car-owners-china_duplicate_data.csv', sep=',', columns=['vehicle_identification_number','name', 'id_card_number'])

# Step 2: Function to split chunks based on the chunksize. Split the large CSV into chunks for further processing:
split_csv_into_chunks('car-owners-china_valid.csv', 250000, 'chunks', sep=',')

# Step 3: Run data cleaning functions and export chunks to specified output folders
process_chunked_csvs_output_folders('chunks', 'cleaned_chunks', 'error_chunks')

# Step 3 (Alternate): Run data cleaning functions and combine chunks to specified CSVs
# Get chunked csvs from a specified folder, runs the validation functions and merges the chunks into a specified final valid csv file and final error csv file. INclude error checking and logging.
# Run data cleaning functions and combine chunks to specified CSVs
process_chunked_csvs('chunks', 'final_valid_data.csv', 'final_error_data.csv')

# Step 4 (Optional): Function to Combine Chunks into a Single CSV
combine_csv_chunks('cleaned_chunks', 'combined_cleaned_data.csv')