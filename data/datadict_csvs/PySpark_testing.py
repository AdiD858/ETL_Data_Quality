#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window 


# ## Completeness tests

# ### PySpark check_nulls function

# In[5]:


# This function checks if the DataFrame required column contains any null values

def check_nulls(df,column_name):
    logs = ""
    null_count = df.filter(col(column_name).isNull()).count() # counting the null values
    if null_count > 0:
        logs += f"Column '{column_name}' contains {null_count} null value(s)."
        return True, logs
    else:
        logs += f"Column '{column_name}' does not contain any null values."
        return False, logs


# ### PySaprk count_records

# In[8]:


# Verify the number of records in the file are according to the expected number

def count_records(df, expected_records_num):
    """
    :param: df: spark data frame of the parquet file
    :param: expected_records_num: expected number of records
    """
    logs = ""  # Initialize logs list
    records_count = df.count()
    logs+= f"Num of records in the file: {records_count}"

    if records_count == expected_records_num:
        logs+= f"Success: the DataFrame has the expected number of records: {expected_records_num}"
        return True,logs
    else:
        logs+= f"Mismatch: the DatatFrame has {records_count} records, but {expected_records_num} were expected."
        return False, logs



# ## Uniqueness tests

# ### PySpark check_duplicates (in columns)

# In[12]:


# Check if unique columns containing duplicated values, return boolean. If there are -return False and the number of duplicates 

def check_duplicates(df, key_columns):
    logs_value = ""
    
    # Step 1: Group by the key columns and count occurrences
    grouped_df = df.groupBy(key_columns).count()
    
    # Step 2: Filter rows where the count is greater than 1 (these are duplicates)
    duplicates = grouped_df.filter(col("count") > 1)
    
    # Check if any duplicates were found
    if duplicates.count() > 0:
        # Collect duplicate records for logging
        duplicates_list = duplicates.collect()
        logs_value += f"Duplicate records found based on keys {key_columns}: {len(duplicates_list)} duplicates."
        return True, logs_value
    else:
        logs_value += "No duplicates found in the specified key columns."
        return False, logs_value


# ### PySpark duplicated_files_by_name_and_size (checking in the current directory and in its subfolders)

# In[15]:


def duplicated_files_by_name_and_size():
    logs = ""  
    files_info = {}  # dictionary to store file names and sizes
    directory_path = os.getcwd()  #  current working directory

    # Walk through all files in the directory and its folders
    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            
            # Check if it's a file (it will be since os.walk() lists files)
            file_size = os.path.getsize(file_path)  # Get file size

            # If file name and size already exist in files_info, it's a duplicate
            if (file_name, file_size) in files_info:
                logs += f"Duplicated file found: {file_name}, Size: {file_size} bytes, Path: {file_path}."
            else:
                files_info[(file_name, file_size)] = True

    # If logs contain duplicates, print them
    if logs:
        return True, logs
    else:
        return False, "No duplicate files found."



# ### PySpark find_duplicate_columns (even if the column names are diff)

# In[3]:


def compute_column_checksum(df, col_name):
    """
   checksum - compute sum of the hash values in each column .
    """
    # comput sum in each column and perform hash
    checksum = df.select(F.sum(F.hash(F.col(col_name))).alias("checksum")).collect()[0]["checksum"]
    return checksum

def find_duplicate_columns(df):
    """
 scan all columns in df and return dictionary with checsum as key, and list with same checksum as the value.
 if the columns has same checksum - they are duplicated.
    """
    checksums = {}
    duplicates = {}

    for col_name in df.columns:
        cs = compute_column_checksum(df, col_name)
        if cs in checksums:
            # אם כבר קיימת עמודה עם אותו checksum, נוסיף את השם לרשימה
            duplicates.setdefault(cs, [checksums[cs]]).append(col_name)
        else:
            checksums[cs] = col_name

    # סינון – רק אותם checksum שמופיע יותר מפעם אחת
    duplicates = {cs: cols for cs, cols in duplicates.items() if len(cols) > 1}
    return duplicates


# In[7]:


#find_duplicate_columns(df)


# ## Consistency tests

# ### PySpark check_consistency

# In[3]:


#  Checks if values between two columns (from two different DataFrames) are equal for rows that share the same key.


def check_consistency(df1, df2, key, column):
    # Assign explicit aliases
    df1_alias = df1.alias("df1")
    df2_alias = df2.alias("df2")
    
    # Perform an inner join on the key
    joined_df = df1_alias.join(df2_alias, df1_alias[key] == df2_alias[key])
    
    # Compare the column values using the explicit alias names
    mismatches = joined_df.filter(col(f"df1.{column}") != col(f"df2.{column}"))
    
    mismatch_count = mismatches.count()
    logs_value = ""
    
    if mismatch_count > 0:
        logs_value += f"Number of mismatched rows: {mismatch_count}"
        return False, logs_value
    else:
        logs_value += f"Consistency check passed for column: {column}."
        return True, logs_value


# ### PySpark check_foreign_key​ 

# In[27]:


# checks whether all foreign key (FK) values in one column exist in a reference column of another DataFrame.

def check_foreign_key(df_fk, column_fk, df_ref, ref_column):
    logs_value = ""
    # Collect reference column values as a list
    ref_values = [row[ref_column] for row in df_ref.select(ref_column).collect()]
    
    # Filter rows where the foreign key is not in the reference values
    invalid_values = df_fk.filter(~df_fk[column_fk].isin(ref_values))
    
    # Check for invalid foreign key values
    invalid_count = invalid_values.count()  # Get the count of invalid FK values
    
    if invalid_count == 0:
        logs_value += "The foreign key values are valid."
        return True, logs_value
    else:
        logs_value += f"Invalid foreign key values in {column_fk} : {invalid_count} rows"
        return False, logs_value



# ### PySpark function: dtype_consistency

# In[28]:


# Check if all values in a specified column are consistent with the expected data type.
from pyspark.sql.types import DataType

def dtype_consistency(df: DataFrame, column_name: str, expected_type: DataType) -> tuple:
    """
    expected_type (DataType): The expected data type for the column (e.g., IntegerType(), StringType()).
    """
    # Attempt to cast the column to the expected type and filter out rows where casting fails (null after casting)
    invalid_count = df.filter(col(column_name).cast(expected_type).isNull() & col(column_name).isNotNull()).count()
    
    if invalid_count == 0:
        print(f"All values in column '{column_name}' are consistent with the expected data type {expected_type}.")
        return True, 0
    else:
        print(f"Column '{column_name}' contains {invalid_count} inconsistent values that don't match {expected_type}.")
        return False, invalid_count


# ## Validity tests

# ### PySpark validate_numeric_range

# In[11]:


# Ensures that numeric values fall within a required range of minimum and maximum

def validate_numeric_range(df, column, min_value, max_value):
    logs = ""
    
    # Filter for out-of-range values
    out_of_range = df.filter((df[column] > max_value) | (df[column] < min_value))
    out_of_range_count = out_of_range.count()
    
    # filters for below min and above max values
    count_lower_min = df.filter(df[column] < min_value).count()
    count_higher_max = df.filter(df[column] > max_value).count()
    
    # If no values are out of range
    if out_of_range_count == 0:
        logs += f"All values in '{column}' are within the range [{min_value}, {max_value}]."
        return True, logs
    else:
        # Log details for out-of-range values
        logs += (f"{out_of_range_count} values in '{column}' are out of range. "
                    f"Values below minimum ({min_value}): {count_lower_min},"
                    f"values above maximum ({max_value}): {count_higher_max}.")
        return False, logs


# In[ ]:





# ### PySpark validate_values_length 

# In[13]:


def validate_values_length(df,column_name, data_type):
    
    """    
    Params:
    data_type (str): The data type string ('CHAR(18)', 'DECIMAL(9,0)', 'INTEGER', 'SMALLINT').
    column_name (str): The name of the column being validated
    
    Returns:
    bool: True if all values are valid, False if any invalid value is found.
    """
    
    logs = ""  # Initialize log string
    invalid_count = 0  # Count of invalid values

    # Handle CHAR(x) type
    if data_type.startswith('CHAR'):
        max_length = int(data_type.split('(')[1].split(')')[0])
        invalid_count = df.filter(length(col(column_name)) > max_length).count()

    # Handle DECIMAL(x,y) type
    elif data_type.startswith('DECIMAL'):
        total_digits, decimal_places = map(int, data_type.split('(')[1].split(')')[0].split(','))
        # Check the number of digits before and after the decimal point
        invalid_count = df.filter(
            (length(col(column_name).cast('string')) > total_digits) |
            (col(column_name).cast('decimal').cast(f'decimal({total_digits},{decimal_places})').isNull())
        ).count()

    # Handle SMALLINT and INTEGER types
    elif data_type in ['SMALLINT', 'INTEGER']:
        # Check if the column has non-integer values
        invalid_count = df.filter(~col(column_name).cast('int').isNotNull()).count()

    # Add logs based on invalid count
    if invalid_count == 0:
        logs += f"All values lengths in column '{column_name}' are within the expected length. "
        return True, logs
    else:
        logs += f"Column '{column_name}' contains {invalid_count} invalid values."
        return False, logs


# ### PySpark validate_allowed_values

# In[23]:


# Check if all values in a specified column are within the allowed values, regardless of data type.

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def validate_allowed_values(df: DataFrame, column_name: str, allowed_values: list) -> tuple:
    
    # Filter the DataFrame to find rows with invalid values
    invalid_values_count = df.filter(~col(column_name).isin(allowed_values)).count()
    
    if invalid_values_count == 0:
        print(f"All values in column '{column_name}' are valid according to the allowed values: {allowed_values}.")
        return True, 0
    else:
        print(f"Column '{column_name}' contains {invalid_values_count} invalid values.")
        return False, invalid_values_count


# In[ ]:





# In[ ]:





# ### PySpark validate_date_format (& timestamp values)

# In[34]:


# Validate dtae values in a specific column are according to the expected format (date or timestamp)

def validate_date_format(df, column, expected_format):
    logs = ""

    # Determine the expected format: date or timestamp 
    if "HH" in expected_format or "mm" in expected_format:  # if it's a timestamp format
        converted_df = df.withColumn("converted_value", to_timestamp(df[column], expected_format))
        invalid_values = converted_df.filter(converted_df["converted_value"].isNull())
        invalid_count = invalid_values.count()

        if invalid_count == 0:
            logs += f"All timestamp values in column '{column}' are in the expected format '{expected_format}'."
            return True, logs
        else:
            logs += f"There are {invalid_count} invalid timestamp values in column '{column}'."
            return False, logs
            
    else:  # Assuming it's a date format if it's not a timestamp
        converted_df = df.withColumn("converted_value", to_date(df[column], expected_format))
        invalid_values = converted_df.filter(converted_df["converted_value"].isNull())
        invalid_count = invalid_values.count()

        if invalid_count == 0:
            logs += f"All date values in column '{column}' are in the expected format '{expected_format}'."
            return True, logs
        else:
            logs += f"There are {invalid_count} invalid date values in column '{column}'."
            return False, logs


# ## Timeliness tests

# ### PySpark file_timeliness

# In[38]:


# Ensures the file is updated within the required hours threshold (by default 24 hours). 

def file_timeliness(file_path, hours_threshold=24):
    logs= ""

    # Check if the file exists
    if not os.path.exists(file_path):
        return False, f"File {file_path} does not exist."

    # Get the current time
    current_time = datetime.now()

    # gets the last modified time of the file in seconds
    last_modified_timestamp = os.path.getmtime(file_path)
    last_modified_time = datetime.fromtimestamp(last_modified_timestamp)

    # calculate the time difference
    time_diff = current_time - last_modified_time 

    # check if the file is timely
    if time_diff <= timedelta(hours=hours_threshold): #  create the threshold for how recent the file should be
        logs += f"File {file_path} is up to date. Last modified {last_modified_time}"
        return True, logs
    else:
        logs += f"File {file_path} is stale. Last modified {last_modified_time} (older than {hours_threshold} hours."
        return False, logs


# ### PySpark validate_date_range (older\future records than defined time )

# In[41]:


# Ensures that data falls within a valid time range in the required column,by checking if there are older records than the current time or any future records 

def validate_date_range(df, column, duration, time_unit):
    logs = ""
    is_valid = True

    # Create a dynamic threshold for late records
    late_threshold_expr = f"current_timestamp() - INTERVAL {duration} {time_unit.upper()}"

    # Filter late and future records: expr() function to allow sql interval expression in pyspark dataframe
    late_records_count = df.filter(col(column) < expr(late_threshold_expr)).count()
    future_records_count = df.filter(col(column) > current_timestamp()).count()

    # Check late records
    if late_records_count > 0:
        logs += f"{late_records_count} late records found in column '{column}' older than {duration} {time_unit.lower()}."
        is_valid = False
    else:
        logs += f"No late records: The column '{column}' has no records older than {duration} {time_unit.lower()}."

    # Check future records
    if future_records_count > 0:
        logs += f"{future_records_count} future records found in column '{column}' beyond the current timestamp."
        is_valid = False
    else:
        logs += f"No future records: The column '{column}' has no records beyond the current timestamp."

    return is_valid, logs


# ## Accuracy tests

# ### PySpark validate_sum

# In[9]:


# Compare the sum of a specific column in the DF to the sum provided in the control file.


def validate_sum(df, column, expected_sum):
    logs = ""

    actual_sum = df.select(spark_sum(column)).collect()[0][0]
    logs += f"Actual sum of '{column}': {actual_sum}"

    # Compare actual sum and expected sum
    if actual_sum == expected_sum:
        logs += f"Result: The actual sum matches the expected sum: {expected_sum}"
        return True, logs
    else:
        logs += f"Result: Expected sum {expected_sum}, but got {actual_sum}"
        return False, logs


# ### PySpark moving_average

# In[48]:


def moving_average(df, column, partitioned_column,date_column, tolerance_deviation, window_size):
    logs = ""  
    
    # window specification based on partitioned_column and date
    window_spec = Window.partitionBy(partitioned_column).orderBy(F.col(date_column).cast('timestamp')).rowsBetween(-window_size, 0)
    
    # calculate moving average for the column
    df = df.withColumn("moving_avg", F.avg(F.col(column)).over(window_spec))
    
    # calculate deviation percentage from the moving average
    df = df.withColumn(
        "deviation",
        F.when(
            F.col("moving_avg").isNotNull() & (F.col("moving_avg") != 0),
            (F.abs(F.col(column) - F.col("moving_avg")) / F.col("moving_avg")) * 100
        ).otherwise(0)
    )
    
    # check if any deviation exceeds the tolerance_deviation
    max_deviation = df.agg(F.max("deviation")).collect()[0][0]
    
    # return False if the maximum deviation exceeds the tolerance
    if max_deviation > tolerance_deviation:
        logs += f"Significant deviation found in {column}. Maximum deviation: {max_deviation:.2f}%"
        return False, logs
    else:
        return True, "No significant deviation found."


# In[15]:





# In[58]:





# In[ ]:





# ### testing functions names:

# ['check_consistency',
#  'check_duplicates',
#  'check_foreign_key',
#  'check_nulls',
#  'count_records',
#  'dtype_consistency',
#  'duplicated_files_by_name_and_size',
#  'duplicated_files_current_path',
#  'file_timeliness',
#  'moving_average',
#  'validate_allowed_values',
#  'validate_date_format',
#  'validate_date_range',
#  'validate_numeric_range',
#  'validate_sum',
#  'validate_values_length']

# In[ ]:





# In[ ]:




