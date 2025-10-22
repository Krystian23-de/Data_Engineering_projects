import csv
import random
import string
from faker import Faker

# --- GCS upload ---
from google.cloud import storage

# -----------------------------
# Config
# -----------------------------
NUM_EMPLOYEES = 100
OUTPUT_FILE = "employee_data.csv"

# GCP: set your bucket and (optional) destination blob path
BUCKET_NAME = "kw-employee-data"              # <-- change this
GCS_BLOB_PATH = "datasets/employee_data.csv"  # can be just "employee_data.csv"

# -----------------------------
# Data generation
# -----------------------------
fake = Faker()
PASSWORD_CHARS = string.ascii_letters + string.digits + string.punctuation

with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as file:
    fieldnames = [
        "employee_id",
        "first_name",
        "last_name",
        "job_title",
        "department",
        "email",
        "address",
        "country",
        "phone_number",
        "salary",
        "password",
    ]
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    for i in range(1, NUM_EMPLOYEES + 1):
        employee_id = f"EMP{i:03d}"
        password = "".join(random.choice(PASSWORD_CHARS) for _ in range(8))

        writer.writerow({
            "employee_id": employee_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "job_title": fake.job(),
            "department": fake.company(),
            "email": fake.email(),
            "address": fake.address().replace("\n", ", "),
            "country": fake.country(),
            "phone_number": fake.phone_number(),
            "salary": random.randint(30000, 150000),
            "password": password
        })

print(f"✅ Generated {NUM_EMPLOYEES} fake employee records in '{OUTPUT_FILE}'")

# -----------------------------
# Upload to Google Cloud Storage
# -----------------------------
def upload_to_gcs(bucket_name: str, source_file: str, destination_blob: str) -> None:
    """Uploads a file to the given GCS bucket."""
    client = storage.Client()                 # Auth via GOOGLE_APPLICATION_CREDENTIALS or ADC
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"☁️  Uploaded '{source_file}' → gs://{bucket_name}/{destination_blob}")

upload_to_gcs(BUCKET_NAME, OUTPUT_FILE, GCS_BLOB_PATH)
