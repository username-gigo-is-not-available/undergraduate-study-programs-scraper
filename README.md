# FCSE-Skopje 2023 Undergraduate Study Programs Scraper

The scraper is used to scrape the study programs and related courses from
the [Faculty of Computer Science and Engineering](https://finki.ukim.mk) at
the [Ss. Cyril and Methodius University in Skopje](https://www.ukim.edu.mk).
which can be found at the following [url](https://finki.ukim.mk/mk/dodiplomski-studii).

The scraper will save the scraped data in three different files:

- `study_programs.avro`: contains the details of the study programs
- `curriculum.avro`: contains the details of the study programs and related courses
- `courses.avro`: contains the details of the courses

## Requirements

- Python 3.9 or later

## Environment Variables

Before running the scraper, make sure to set the following environment variables:

- `FILE_STORAGE_TYPE`: the type of storage that will be used to save the scraped data. The possible values are "LOCAL"
  and "MINIO"
- `NUMBER_OF_THREADS`: the maximum number of threads that will be used to scrape the website
- `REQUEST_TIMEOUT_SECONDS`: maximum wait time for an HTTP request in seconds before timing out
- `REQUESTS_RETRY_COUNT`: the number of the retries for an HTTP request if it fails
- `REQUESTS_RETRY_DELAY_SECONDS`: the wait delay for an HTTP request in seconds after failure

- `STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME`: the name of the file where the study programs data will be saved
- `CURRICULUM_DATA_OUTPUT_FILE_NAME`: the name of the file where the curriculum data will be saved
- `COURSES_DATA_OUTPUT_FILE_NAME`: the name of the file where the courses data will be saved

- `STUDY_PROGRAMS_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `StudyProgram` record is stored
- `CURRICULA_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Curriculum` record is stored
- `COURSES_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Course` record is stored

#### If running the application with local storage:

- `OUTPUT_DATA_DIRECTORY_PATH`: the path to the directory where the output data files will be saved
- `SCHEMA_DIRECTORY_PATH`: the path to the directory where the output schema files are stored

#### If running the application with MinIO:

- `MINIO_ENDPOINT_URL`: the endpoint of the MinIO server
- `MINIO_ACCESS_KEY`: the access key of the MinIO server
- `MINIO_SECRET_KEY`: the secret key of the MinIO server
- `MINIO_OUTPUT_DATA_BUCKET_NAME`: the name of the bucket where the output data files will be saved
- `MINIO_SCHEMA_BUCKET_NAME`: the name of the bucket where the schema files are stored

## Installation

1. Clone the repository
    ```bash
    git clone <repository_url>
    ```

2. Install the required packages
    ```bash
    pip install -r requirements.txt
    ```

3. Run the scraper
    ```bash
    python main.py
    ```

Make sure to replace `<repository_url>` with the actual URL of the repository.
