# SQL-Database-Operator-job
sql,python
import pandas as pd
import mysql.connector

# Task 1: Read data from CSV files
data_df = pd.read_csv('data.csv')
subject_df = pd.read_csv('subject.csv')

# Task 2: Transform the value2 column data in data_df by squaring the values
data_df['value2'] = data_df['value2'] ** 2

# Task 3: Save the transformed data to MySQL
# Establish a connection to MySQL
connection = mysql.connector.connect(
    host='your_host',
    user='your_username',
    password='your_password',
    database='your_database'
)

# Create a cursor object to execute SQL queries
cursor = connection.cursor()

# Create a table for data_df in MySQL
create_data_table_query = """
CREATE TABLE data (
    data_id INT,
    Subject_id INT,
    value1 INT,
    value2 INT
)
"""
cursor.execute(create_data_table_query)

# Insert data_df records into the data table
for _, row in data_df.iterrows():
    insert_data_query = """
    INSERT INTO data (data_id, Subject_id, value1, value2)
    VALUES (%s, %s, %s, %s)
    """
    cursor.execute(insert_data_query, tuple(row))

# Create a table for subject_df in MySQL
create_subject_table_query = """
CREATE TABLE subject (
    Subject_id INT,
    Subject_name VARCHAR(255)
)
"""
cursor.execute(create_subject_table_query)

# Insert subject_df records into the subject table
for _, row in subject_df.iterrows():
    insert_subject_query = """
    INSERT INTO subject (Subject_id, Subject_name)
    VALUES (%s, %s)
    """
    cursor.execute(insert_subject_query, tuple(row))

# Commit the changes and close the cursor and connection
connection.commit()
cursor.close()
connection.close()

# Task 4: Create result.csv showcasing the relationship between data.csv and subject.csv
result_df = pd.merge(data_df, subject_df, on='Subject_id')
result_df.to_csv('result.csv', index=False)





This code reads the two CSV files using pandas, transforms the value2 column by squaring the values, saves the data from both files into MySQL tables, and creates result.csv by merging the two dataframes based on the common Subject_id column. Finally, the resulting dataframe is saved as result.csv.
