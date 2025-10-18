# FCSE-Skopje 2023 Undergraduate Study Programs Scraper

The scraper is used to scrape the study programs and related courses from the **[Faculty of Computer Science and Engineering (FCSE)](https://finki.ukim.mk)** at the **[Ss. Cyril and Methodius University in Skopje](https://www.ukim.edu.mk)**. The primary source data can be found at: [https://finki.ukim.mk/mk/dodiplomski-studii](https://finki.ukim.mk/mk/dodiplomski-studii).

The application uses **Python**, **aiohttp** for asynchronous networking, **multithreading** for parallel processing and **PyIceberg** for lakehouse data storage.

---

## Data Output

The scraper saves the data into an **Apache Iceberg lakehouse** structure within a specified namespace (`ICEBERG_NAMESPACE`). The output is structured into three main tables:

| Table Name | Description |
| :--- | :--- |
| `study_programs` | Contains the core details of the undergraduate study programs. |
| `curriculum` | Contains the mapping and details linking study programs to their associated courses. |
| `courses` | Contains the full descriptive details of each individual course. |

---

## Requirements

- **Python 3.9 or later**
- Access to a **MinIO server** (for S3 storage) or local disk space (for local storage).

---

## Installation and Setup

1.  **Clone the repository**
    ```bash
    git clone <repository_url>
    ```

2.  **Install the required packages**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Create `.env` File**
    Create a file named `.env` in the project root and populate it with the necessary variables defined below.

4. **Create `.pyiceberg.yaml` File**
    Create a file named `.pyiceberg.yaml` in the project root and populate it with the necessary variables defined below.

---

## Environment Variables

The application is configured using environment variables, which must be sourced before execution.

### General & Scraper Configuration

| Variable | Description                                                                                                                                                       |
| :--- |:------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FILE_IO_TYPE` | The type of storage that will be used. Must be **"LOCAL"** or **"S3"**.                                                                                           |
| `NUMBER_OF_THREADS` | The maximum number of threads used for thread-intensive operations (e.g., parsing HTML).<br/> Set to `-1` to use maximum number of threads (`number of CPU cores * 5`) |
| `REQUEST_TIMEOUT_SECONDS` | Maximum wait time for a single HTTP request in seconds before timing out.                                                                                         |
| `REQUESTS_RETRY_COUNT` | The number of times an HTTP request will be retried if it fails.                                                                                                  |
| `REQUESTS_RETRY_DELAY_SECONDS` | The wait delay in seconds between failed HTTP request retries.                                                                                                    |
| `STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME` | The base file name for the study programs data (e.g., `study_programs`).                                                                                          |
| `CURRICULUM_DATA_OUTPUT_FILE_NAME` | The base file name for the curriculum data (e.g., `curriculum`).                                                                                                  |
| `COURSES_DATA_OUTPUT_FILE_NAME` | The base file name for the courses data (e.g., `courses`).                                                                                                        |

### Iceberg Configuration (Metastore)

| Variable | Description |
| :--- | :--- |
| `ICEBERG_CATALOG_NAME` | The name of the catalog configuration to load (e.g., `default`). This determines the metastore type (e.g., SQL/SQLite). |
| `ICEBERG_NAMESPACE` | The logical grouping (schema/database) within the Iceberg catalog where the tables will be created (e.g., `raw`). |

### Storage-Specific Configuration

#### 1. Local Storage (`FILE_IO_TYPE="LOCAL"`)

| Variable | Description |
| :--- | :--- |
| `LOCAL_ICEBERG_LAKEHOUSE_FILE_PATH` | The **absolute path** to the root directory that serves as the Iceberg warehouse (e.g., `/app/local_lakehouse`). |

#### 2. MinIO/S3 Storage (`FILE_IO_TYPE="S3"`)

| Variable | Description |
| :--- | :--- |
| `S3_ENDPOINT_URL` | The full endpoint URL (host and port) of the MinIO server (e.g., `localhost:9000`). |
| `S3_ACCESS_KEY` | The access key ID required for MinIO authentication. |
| `S3_SECRET_KEY` | The secret access key required for MinIO authentication. |
| `S3_ICEBERG_LAKEHOUSE_BUCKET_NAME` | The name of the S3 bucket that will host the Iceberg warehouse (e.g., `finki-warehouse`). |
| `S3_PATH_STYLE_ACCESS` | Boolean flag (`True`/`False`) indicating whether to use path-style addressing (required for MinIO or custom S3 endpoints). |

---

## Running the Scraper

### Option 1: Local Execution

Ensure all environment variables are loaded (e.g., `source .env` on Linux/macOS or equivalent in PowerShell).

```bash
python run.py
```
### Option 2: Docker Compose (Recommended for S3/MinIO)

If your setup uses MinIO, use Docker Compose to manage both the Python application and the supporting services.

```bash
docker compose up
```
