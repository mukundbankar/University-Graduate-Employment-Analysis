import mysql.connector
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import io

#mysql workbench credentials
host = "localhost"
user = "root"
password = "root"
database = "student"

#gcs variables 
bucket_name = 'uconnect-staging-data'
credentials_path = 'C:/Users/mukun/Desktop/uConnect/uconnect-key.json'

#init connection with database
def connect_to_database(host, user, password, database):
    try:
        return mysql.connector.connect(host=host, user=user, password=password, database=database)
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

#fetching data from mysql workbench
def fetch_data(cursor, query):
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(results, columns=columns)
    except mysql.connector.Error as e:
        print(f"Error fetching data: {e}")
        return None

#converting pandas dataframe to csv string to upload on GCS
def dataframe_to_csv_string(dataframe):

    csv_buffer = io.StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    return csv_buffer.getvalue()

#upload csv files to GCS based on bucket name and destination
def upload_blob_from_string(bucket_name, csv_string, destination_blob_name, credentials_path):
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_string, content_type='text/csv')
        print(f"Data uploaded to {destination_blob_name} in bucket {bucket_name}.")
    except Exception as e:
        print(f"Error uploading to GCS: {e}")

def main():

    db = connect_to_database(host, user, password, database)
    if db is None:
        return

    cursor = db.cursor()

    student_query = "SELECT * FROM students"
    student_df = fetch_data(cursor, student_query)
    if student_df is None:
        return

    major_query = "SELECT * FROM majors"
    major_df = fetch_data(cursor, major_query)
    if major_df is None:
        return

    student_csv_data = dataframe_to_csv_string(student_df)
    upload_blob_from_string(bucket_name, student_csv_data, 'students_data.csv', credentials_path)

    major_csv_data = dataframe_to_csv_string(major_df)
    upload_blob_from_string(bucket_name, major_csv_data, 'majors_data.csv', credentials_path)

if __name__ == "__main__":
    main()
