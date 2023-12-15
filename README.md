
![image](https://github.com/mukundbankar/University-Graduate-Employment-Analysis/assets/62957131/26d7f383-b06b-4a8a-8284-6ee55b94c57c)
**Raw Data Extraction:**
The process started with loading `students.csv` and `majors.csv` into MySQL Workbench. Next, a Python script
was written to export these datasets from MySQL as CSV files and then upload them to Google Cloud Storage. The
`Employment.csv` file was already in GCS, which streamlined the process.

> _Extract_load.py_

**Staging Data Load:**
Cloud Composer, which utilizes Apache Airflow, was set up to handle the data workflow. It fetched the CSV files
from GCS and loaded them into BigQuery's staging dataset. A static schema was used for loading to flag any
potential schema changes and catch related errors.

> _load_data_from_gcs_to_bigquery.py_

**Data Transformation:**
With the data in staging, the next step was to apply transformations. The `employment` table required converting
the UNIX epoch `start_date` into a human-readable date format. Additionally, checks were in place to verify the
data granularity and ensure no empty tables were loaded, which could indicate upstream issues. Made use of
optimization such as Clustering and Partitioning to increase data fetching speed.

> bigquery_table_processing_with_checks.py

**Optimization Consideration:**
Due to the data's size and unknown update frequency, a table replacement strategy was used for updates.
However, there's a consideration for a swap method that would update a temporary table first, then switch it with
the existing table. This approach could enhance efficiency and minimize downtime during updates.
