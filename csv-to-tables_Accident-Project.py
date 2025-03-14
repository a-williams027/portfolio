import pandas as pd
import urllib
import pyodbc
import datetime

import mysql.connector
import urllib.parse
from mysql.connector import Error

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.util import deprecations
deprecations.SILENCE_UBER_WARNING = True

def drop_tables(conn):
    cursor = conn.cursor()
    cursor.execute('CALL accident_project.DropTables()')
    conn.commit()
    cursor.close()

def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('CALL accident_project.CreateTables()')
    conn.commit()
    cursor.close()

def alter_tables(conn):
    cursor = conn.cursor()
    cursor.execute("CALL accident_project.AlterTables()")
    conn.commit()
    cursor.close()


def csv_to_tables(fileName, tableName, conStr):
    conStr = "root:Da!!yn18@localhost/accident_project"
    sqlalchemy_url = f"mysql+mysqlconnector://{conStr}"
    engine = create_engine(sqlalchemy_url)
    df = pd.read_csv(fileName)

    df.to_sql(name=tableName, con=engine, if_exists='append', index=False)
    engine.dispose()


def remove_null_lat_lon(fileName, file_type='data'):
    df = pd.read_csv(fileName)

    # Remove rows with null Latitude or Longitude
    null_lat_lon_rows = df[df['Latitude'].isnull() | df['Longitude'].isnull()].index
    df = df.drop(null_lat_lon_rows)

    # Log removed rows
    with open(r'C:\Users\ajeffco\Documents\latlon_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        for idx in null_lat_lon_rows:
            out_str = f'TimeStamp: {dt_str} Row {idx + 1} in {file_type} file {fileName} has null Latitude or Longitude and will be dropped\n'
            f.write(out_str)

    return df

def clean_driver_file(fileName):
    df_driver = pd.read_csv(fileName)

    # Replace 'Data missing' with pd.NA
    replacements = {'Data missing or out of range': pd.NA, 'Not known': pd.NA}
    df_driver.replace(replacements, inplace=True)

    num_cells_updated = ((df_driver == 'Data missing or out of range') | (df_driver == 'Not known')).sum().sum()

    with open(r'C:\Users\ajeffco\Documents\driver_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = f'TimeStamp: {dt_str} - Updated {num_cells_updated} cells in {fileName} from "Data missing" to NULL\n'
        f.write(out_str)

    return df_driver

def clean_vehicle_file(fileName):
    df = pd.read_csv(fileName)
    rows_to_drop = []
    for index, row in df.iterrows():
        if not isinstance(row['Vehicle_Reference'], int):
            rows_to_drop.append(index)
            print(f"Row {index+1} in Vehicle file {fileName} has a non-numeric vehicle reference number and will be dropped")
            with open(r'C:\Users\ajeffco\Documents\vehicle_log.txt', 'a') as f:
                dt = datetime.datetime.now()
                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                out_str = f'TimeStamp: {dt_str} Row {index+1} in vehicle file {fileName} has a non-numeric vehicle reference number and will be dropped\n'
                f.write(out_str)
    df = df.drop(rows_to_drop)
    return df


def remove_duplicates_accident(fileName):
    df = pd.read_csv(fileName)
    df_cleaned = df.drop_duplicates(subset='Accident_Index')
    return df_cleaned


def remove_duplicates_and_specific_location(fileName):
    df = pd.read_csv(fileName)

    # Remove specific rows with latitude 53.8346 and longitude -2.21331
    df_filtered = df[(df['Latitude'] != 53.8346) | (df['Longitude'] != -2.21331)]

    # Remove duplicates based on latitude and longitude
    df_cleaned2 = df_filtered.drop_duplicates(subset=['Latitude', 'Longitude'])

    return df_cleaned2


def remove_null_district_authority(fileName):
    df_authority = pd.read_csv(fileName)

    rows_to_drop = df_authority[df_authority['District_ID'].isnull()].index
    num_rows_removed = len(rows_to_drop)

    if num_rows_removed > 0:
        print(f"{datetime.datetime.now()} - Removed {num_rows_removed} rows with null 'District_ID' in {fileName}")
        with open(r'C:\Users\ajeffco\Documents\authority_log.txt', 'a') as f:
            dt = datetime.datetime.now()
            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            out_str = f'TimeStamp: {dt_str} - Removed {num_rows_removed} rows with null \'District_ID\' in {fileName}\n'
            f.write(out_str)

    df_authority = df_authority.drop(rows_to_drop)
    return df_authority


def remove_invalid_date(fileName, date_fileName):
    df_accident = pd.read_csv(fileName)
    df_date = pd.read(date_fileName)

    rows_to_drop = []
    for index, row in df_accident.iterrows():
        if not row['Date'] in df_date['Date'].values:
            print(f"Row {index + 1} in accident file {fileName} has an invalid date and will be dropped")
            with open(r'C:\Users\ajeffco\Documents\accidentdate_log.txt', 'a') as f:
                dt = datetime.datetime.now()
                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                out_str = f'TimeStamp: {dt_str} Row {index + 1} in accident file {fileName} has an invalid date and will be dropped\n'
                f.write(out_str)
            rows_to_drop.append(index)
    df_accident = df_accident.drop(rows_to_drop)
    return df_accident

def clean_vehicle_file2(fileName):
    df_vehicle2 = pd.read_csv(fileName)

    # Replace 'Data missing' with pd.NA
    replacements = {'Data missing or out of range': pd.NA, 'Not known': pd.NA}
    df_vehicle2.replace(replacements, inplace=True)

    num_cells_updated = ((df_vehicle2 == 'Data missing or out of range') | (df_vehicle2 == 'Not known')).sum().sum()

    with open(r'C:\Users\ajeffco\Documents\vehicle2_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = f'TimeStamp: {dt_str} - Updated {num_cells_updated} cells in {fileName} from "Data missing" to NULL\n'
        f.write(out_str)

    return df_vehicle2

def remove_non_matching_indices(vehicle_file, accident_file, driver_file):
    # Read both CSV files into DataFrames
    df_vehicle3 = pd.read_csv(vehicle_file)
    df_accident3 = pd.read_csv(accident_file)
    df_driver2 = pd.read_csv(driver_file)

    # Get the set of unique Accident_Index values from accident.csv
    unique_accident_indices = set(df_accident3['Accident_Index'])

    # Filter the vehicle and driver DataFrame to keep only rows with Accident_Index that exist in accident.csv
    df_vehicle_filtered = df_vehicle3[df_vehicle3['Accident_Index'].isin(unique_accident_indices)]

    unique_vehicle_indices = set(df_vehicle_filtered['Accident_Index'])
    df_driver_filtered = df_driver2[df_driver2['Accident_Index'].isin(unique_vehicle_indices)]

    # Log the number of dropped rows for each file
    log_file = r'C:\Users\ajeffco\Documents\non_matching_indices_log.txt'
    with open(log_file, 'a') as f:
        dt_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{dt_str} - Dropped {len(df_vehicle3) - len(df_vehicle_filtered)} rows from {vehicle_file}\n")
        f.write(f"{dt_str} - Dropped {len(df_driver2) - len(df_driver_filtered)} rows from {driver_file}\n")

    return df_vehicle_filtered, df_driver_filtered


def main():
    connection_string = None  # Define it at the start to avoid scope issues

    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host="localhost",
            port=3306,
            database="accident_project",
            user="root",
            password="Da!!yn18"
        )

        if conn.is_connected():
            print("Connected to MySQL database")

        drop_tables(conn)
        create_tables(conn)

        # Set the connection string for SQLAlchemy
        connection_string = f"mysql+mysqlconnector://root:{urllib.parse.quote_plus('Da!!yn18')}@localhost/accident_project"

        print("SQLAlchemy Connection String:", connection_string)

    except mysql.connector.Error as e:
        print("Error:", e)
        return  # Exit early if the connection fails

    # Clean Datasets
    df_cleaned_vehicle = clean_vehicle_file(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/vehicle.csv')
    df_cleaned_vehicle.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_vehicle.csv', index=False)

    df_cleaned_accident = remove_null_lat_lon(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/accident.csv')
    df_cleaned_accident.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_accident.csv', index=False)

    df_cleaned_location = remove_null_lat_lon(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/location.csv')
    df_cleaned_location.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_location.csv', index=False)

    df_cleaned_condition = remove_null_lat_lon(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/condition.csv')
    df_cleaned_condition.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_condition.csv', index=False)

    df_cleaned_driver = clean_driver_file(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/driver.csv')
    df_cleaned_driver.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_driver.csv', index=False)

    df_cleaned_accident2 = remove_duplicates_accident(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_accident.csv')
    df_cleaned_accident2.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_accident2.csv', index=False)

    df_cleaned_location2 = remove_duplicates_and_specific_location(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_location.csv')
    df_cleaned_location2.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_location2.csv', index=False)

    df_cleaned_authority = remove_null_district_authority(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/authority.csv')
    df_cleaned_authority.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_authority.csv', index=False)

    df_cleaned_vehicle2 = clean_vehicle_file2(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_vehicle.csv')
    df_cleaned_vehicle2.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_vehicle2.csv', index=False)

    df_vehicle_filtered, df_driver_filtered = remove_non_matching_indices(
        r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_vehicle2.csv',
        r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_accident2.csv',
        r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_driver.csv')
    df_vehicle_filtered.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/filtered_vehicle.csv', index=False)
    df_driver_filtered.to_csv(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/filtered_driver.csv', index=False)

    # Ensure connection_string is not None before using it
    if connection_string:
        # Load the tables from clean CSV files
        csv_to_tables(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_accident2.csv', 'Accident', connection_string)
        csv_to_tables(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_authority.csv', 'Authority', connection_string)
        csv_to_tables(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/date.csv', 'Date', connection_string)
        csv_to_tables(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_condition.csv', 'Conditions', connection_string)
        csv_to_tables(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/filtered_driver.csv', 'Driver', connection_string)
        csv_to_tables(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/junction.csv', 'Junction', connection_string)
        csv_to_tables(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/filtered_vehicle.csv', 'Vehicle', connection_string)
        csv_to_tables(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/road.csv', 'Road', connection_string)
        csv_to_tables(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/vehicle_type.csv', 'Vehicle_Type', connection_string)
        csv_to_tables(r'/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Data Table CSVs/cleaned_location2.csv', 'Location', connection_string)

        # Add FK Constraints
        alter_tables(conn)

    # Close MySQL connection
    conn.close()
    print("Connection closed.")

if __name__ == "__main__":
    main()