This project shows a simple, end-to-end data pipeline on Google Cloud.  
It starts from generating fake employee data, stores it in Cloud Storage, processes it with Cloud Data Fusion, loads it into BigQuery, and finally visualizes it in Looker Studio.

---


1. Data Generation
   - A Python script creates a CSV file with random employee details (name, job, salary, password, etc.).
   - The script uploads the file to a GCS bucket: `gs://kw-employee-data/datasets/`

2. Data Processing (Cloud Data Fusion)
   - A low-code pipeline in Cloud Data Fusion picks up the CSV files from that GCS bucket.
   - It uses Wrangler transformations to:
     - Combine first and last name into `full_name`
     - Mask salary information
     - Hash passwords using **MD5** for privacy
   - The cleaned data is then written into BigQuery → `employee_data.emp_data`

3. Data Visualization (Looker Studio)
   - The BigQuery table is connected to a Looker Studio report.
   - The dashboard shows employee trends, departments, and salary patterns.
   - You can open it here:
     https://lookerstudio.google.com/reporting/512453bb-d4b2-4252-a3c8-4f26cc48c367/page/qhZcF/edit

---

I wanted a simple but realistic GCP data pipeline that touches multiple cloud tools:
- Cloud Storage for raw data
- Data Fusion for ETL without heavy coding
- BigQuery for storage and querying
- Looker Studio for reporting