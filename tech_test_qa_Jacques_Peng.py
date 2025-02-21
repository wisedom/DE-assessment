
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 12:14:19 2025

@author: J.P.
"""

import pandas as pd
from sqlalchemy import create_engine
import numpy as np

def dataenquiry(DB_HOST,DB_PORT,DB_NAME,DB_USER,DB_PASSWORD):
    # Create a Database Engine with SQLAlchemy
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    #  Get all tables on the database
    query_tables = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    tables = pd.read_sql(query_tables, engine)
    
    # Create a Dictionary to Store Table Data
    database_enquiried = {}
    
    # Iterate through all the tables and store them in the dictionary
    for table in tables['table_name']:
        print(f"reading table {table} ...")
        
        # Read Data from table
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        
        # file in the dictionary
        database_enquiried[table] = df
    
    print("All dataset has been loaded into the dictionary!")
    
    # heading
    table_name = list(database_enquiried.keys())[0]  # Get the first table name
    print(database_enquiried[table_name].head())
    
    
    # heading
    table_name = list(database_enquiried.keys())[1]  # Get the second table name
    print(database_enquiried[table_name].head())
    
    return database_enquiried

def outlier_check(tables_dict):
    # Dictionary to store data quality issues
    issues_dict = {}
    
    # Dictionaries to store problematic and warning rows
    problematic_rows_dict = {}
    warning_rows_dict = {}
    
    # Ensure both `trades` and `users` tables exist
    if "trades" in tables_dict and "users" in tables_dict:
        trades_df = tables_dict["trades"]
        user_df = tables_dict["users"]
        
        # Store unmatched records
        unmatched_rows = pd.DataFrame()
    
        # Perform a left join to find unmatched trades with `login_hash + server_hash`
        unmatched_trades = trades_df.merge(user_df, on=["login_hash", "server_hash"], how="left", indicator=True)
        unmatched_trades = unmatched_trades[unmatched_trades["_merge"] == "left_only"].drop(columns=["_merge"])
        
        if not unmatched_trades.empty:
            print(f"Found {len(unmatched_trades)} records where `login_hash + server_hash` in `trades` do not exist in `users`")
            unmatched_rows = pd.concat([unmatched_rows, unmatched_trades])

    # ** Iterate through all tables and perform data quality validation **
    for table_name, df in tables_dict.items():

        issues = []
        warnings = []
        problematic_rows = pd.DataFrame()
        warning_rows = pd.DataFrame()
    
        # **(1) Check string (object) columns**
        for col in df.select_dtypes(include=['object']).columns:
            # Detect leading or trailing spaces
            space_issues = df[df[col].str.startswith(" ", na=False) | df[col].str.endswith(" ", na=False)]
            # Detect punctuation symbols
            punctuation_issues = df[df[col].str.contains(r"[!@#$%^&*(),.?/\"']", regex=True, na=False)]
            # Detect excessively long strings
            long_values = df[df[col].str.len() > 50]
    
            # Record detected issues
            if not space_issues.empty:
                issues.append(f"Column {col} contains {len(space_issues)} rows with leading/trailing spaces")
                problematic_rows = pd.concat([problematic_rows, space_issues])
            if not punctuation_issues.empty:
                issues.append(f"Column {col} contains {len(punctuation_issues)} rows with special characters")
                problematic_rows = pd.concat([problematic_rows, punctuation_issues])
            if not long_values.empty:
                issues.append(f"Column {col} contains {len(long_values)} rows with excessively long strings")
                problematic_rows = pd.concat([problematic_rows, long_values])

        # **(2) Check extreme numerical (number) columns**
        for col in df.select_dtypes(include=[np.number]).columns:
            # Detect unreasonable numerical values (greater than 99,999,999)
            warning_mask = (df[col] > 99999999) | (df[col] < 0)
            warning_values = df[warning_mask]
            if not warning_values.empty:
                warnings.append(f"Column {col} contains {len(warning_values)} rows exceeding the warning threshold")
                warning_rows = pd.concat([warning_rows, warning_values])

        # **(3) Check `volume = 0 & contractsize > 0`**
        if "volume" in df.columns and "contractsize" in df.columns:
            zero_volume_rows = df[(df["volume"] == 0) & (df["contractsize"] > 0)]
            if not zero_volume_rows.empty:
                issues.append(f"Column {col} contains {len(zero_volume_rows)} rows where volume is 0 but contractsize > 0")
                problematic_rows = pd.concat([problematic_rows, zero_volume_rows])

        # **(4) Check if `open_time > close_time`**
        if "open_time" in df.columns and "close_time" in df.columns:
            df["open_time"] = pd.to_datetime(df["open_time"], errors="coerce")
            df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce")
            invalid_time_rows = df[df["open_time"] > df["close_time"]]
            if not invalid_time_rows.empty:
                issues.append(f"Table {table_name} contains {len(invalid_time_rows)} rows where open_time is later than close_time")
                problematic_rows = pd.concat([problematic_rows, invalid_time_rows])

        # **(5) Check if holding time exceeds 1 year**
        if "open_time" in df.columns and "close_time" in df.columns:
            df["holding_days"] = (df["close_time"] - df["open_time"]).dt.days
            long_holding_trades = df[df["holding_days"] > 365]
            if not long_holding_trades.empty:
                issues.append(f"Table {table_name} contains {len(long_holding_trades)} rows where holding period exceeds 1 year")
                problematic_rows = pd.concat([problematic_rows, long_holding_trades])
        
        # **(6) Check missing values**
        missing_values = df.isnull().sum()
        missing_columns = missing_values[missing_values > 0]
        if not missing_columns.empty:
            issues.append(f"Missing values detected: {missing_columns.to_dict()}")
            problematic_rows = pd.concat([problematic_rows, df[df.isnull().any(axis=1)]])
    
        # **(7) Check duplicate rows**
        duplicate_rows = df[df.duplicated()]
        if not duplicate_rows.empty:
            issues.append(f"Table {table_name} contains {len(duplicate_rows)} duplicate rows")
            problematic_rows = pd.concat([problematic_rows, duplicate_rows])

        # **Store issues**
        if issues:
            issues_dict[f"{table_name}_issues"] = issues
        if warnings:
            issues_dict[f"{table_name}_warnings"] = warnings
   
        problematic_rows_dict[table_name] = problematic_rows
        warning_rows_dict[table_name] = warning_rows

    return unmatched_rows,issues_dict,problematic_rows_dict,warning_rows_dict


if __name__ == "__main__":
    
    # Information about Database Connection 
    host = "196.163.1.1"
    port = "8888"
    name = "one"
    user = "user"
    password = "password"
    
    data_got = dataenquiry(host,port,name,user,password)
    unmatched,issues,problematic,warning = outlier_check(data_got)
    
    # ** Output detected issues **
    for table, issues in issues.items():
        print(f"\n**Data quality issues in {table}:**")
        for issue in issues:
            print(f" - {issue}")

    print("\nAll data validation checks are completed!")
    
'''
Comments:
Since this dataset is defined by myself, I assume that I am familiar with its metric system 
and can design targeted tests for different metrics. This assessment focuses on validating
the integrity, consistency, and correctness of the data across multiple dimensions.

*Key Findings:
    
1.Join Integrity Issues
  91,953 records in the trades table have login_hash + server_hash that do not exist in the users table.
  Potential Issue: This could indicate missing or incorrectly recorded user accounts, 
                   causing incomplete data relationships and potential transaction mismatches.

2. Anomalies in the trades Table
  String Format Issues:
     Column symbol contains 2 rows with unexpected special characters.
     Impact: These may indicate incorrect symbol encoding or data entry errors.
  Volume-Contract Size Mismatch:
     1 row where volume = 0 but contractsize > 0.
     Impact: Logically, a zero-volume transaction should not have a contract size. 
            This might indicate a data entry error.

  Holding Time Issues:
     4,783 rows where the holding period exceeds 1 year.
     Impact: This could indicate long-term positions, but in most financial markets, 
             such prolonged trades are uncommon and may require further verification.

  Missing Values:
     7 missing values detected in the contractsize column.
     Impact: Missing contract sizes may cause errors in transaction calculations 
            and require imputation or further investigation.

  Extremely High Values Detected:
     Transactions exceeding 99,999,999 were flagged as extreme outliers.
     Impact: These may be legitimate high-value trades,
             but they could also indicate errors or anomalies in data entry.
3. Anomalies in the users Table
  Duplicate Records:
     The users table contains 334 duplicate rows.
     Impact: Duplicate user records may cause inconsistencies 
             in data analysis and incorrect join results with the trades table.
4. Regulatory Considerations
  Australian Financial Regulation Compliance:
     No specific threshold found in Australian financial regulations regarding 
           the mandatory reporting of large transaction amounts.
     Impact: While no regulatory threshold was identified, monitoring large 
             transactions is still considered a best practice for fraud detection 
             and compliance with AML (Anti-Money Laundering) requirements.

Conclusion & Next Steps:
    
The findings suggest multiple data integrity issues, including join failures, 
incorrect values, missing data, extremely high values, and duplicates. The primary concerns are:
  1. Unmatched login_hash + server_hash records, which may indicate missing user entries or join inconsistencies.
  2. Potential incorrect transaction data, including volume inconsistencies and long holding periods.
  3. Extremely high values detected, requiring further validation to distinguish between 
     genuine high-value trades and potential data entry errors.
  4. No specific reporting threshold found in Australian regulations, but monitoring large transactions remains a key practice.
  5. Data entry errors in symbols and missing contract sizes.
  6. User data duplication, which may affect relational data accuracy.

Recommended Actions:

Investigate why login_hash + server_hash mismatches exist and whether the missing user records need to be restored.
Verify and clean special character anomalies in the symbol column.
Check whether zero-volume transactions with contract sizes are valid or erroneous.
Analyze whether long holding periods are a business requirement or an error.
Review transactions exceeding 99,999,999 to determine their validity.
Remove or consolidate duplicate user records to prevent inconsistencies.
Continue monitoring large transactions, even though no specific reporting threshold is mandated by Australian financial regulations.
Ensuring high-quality data will lead to better reliability and accuracy in further analysis and decision-making.
'''
