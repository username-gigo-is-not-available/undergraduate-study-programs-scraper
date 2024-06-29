# FCSE-Skopje 2023 Undergraduate Study Programs Scraper

The scraper is used to scrape the study programs and related courses from the [Faculty of Computer Science and Engineering](https://finki.ukim.mk) at the [Ss. Cyril and Methodius University in Skopje](https://www.ukim.edu.mk).
which can be found at the following [url](https://finki.ukim.mk/mk/dodiplomski-studii).

The scraper will save the scraped data in three different files:

- `study_programs.csv`: contains the details of the study programs
- `curriculum.csv`: contains the details of the study programs and related courses
- `courses.csv`: contains the details of the courses

## Requirements

- Python 3.9 or later

## Environment Variables

Before running the scraper, make sure to set the following environment variables:

- `OUTPUT_DIRECTORY_PATH`: the path to the directory where the output files will be saved
- `MAX_WORKERS`: the maximum number of workers that will be used to scrape the website
- `EXECUTOR_TYPE`: the type of executor that will be used to scrape the website. The possible values are "THREAD" and "PROCESS"

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
