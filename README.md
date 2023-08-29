# WeatherImpactPL2023

## 🌦️ Can Weather Influence Football? Unveiling Insights from Premier League 2022/2023 🌟
This project serves as a hub for data extraction, transformation, loading, and analysis, revolving around two distinct public APIs. Through this project, we explore the realms of data engineering and analysis, deriving insights from the intersection of Premier League football data and weather statistics.

## 📂 Folder Structure
The repository is organized as follows:

- **data_extractors**: This directory holds the scripts responsible for fetching data from the APIs.
  - **common**: Contains utility functions shared across data extractors.
    - `utils.py`: Reusable utility functions for data extraction.
  - **premier_league**: Extracts data premier league data from the football-data API.
    - `pl_extractor.py`: Extracts Premier League match and team information for the 2022/2023 season.
  - **weather**: Fetches weather data from the Visual Crossing weather API.
    - `weather_extractor.py`: Fetches weather data for specified locations.

- **resources**: Houses essential resources for the project.
  - `.env`: Configuration file for environment variables.
  - **data**: Repository for storing generated CSV files.

- `extractor.py`: Script to initiate data extraction from various data_extractors.
- `upload_csv_to_redshift.py`: Script to upload CSV files to an Amazon Redshift cluster.
- `Analysis.ipynb`: Jupyter notebook for conducting data analysis.

- `.gitignore`: Specifies files/folders to be ignored by version control.

## 🔍 Data Extraction
The heart of this project lies in data extraction. Two public APIs were chosen to source our data:

1. **football-data API**: An API that provides comprehensive information about Premier League football matches and teams. The `pl_extractor.py` script pulls data from this API, which includes match results, team statistics, and more.

2. **Visual Crossing API**: This API supplies weather data for specific locations. The `weather_extractor.py` script interacts with the weather API to obtain temperature, precipitation, and other meteorological information.

## ☁️ Loading Data into Redshift DWH
The `upload_csv_to_redshift.py` script integrates extracted data into an Amazon Redshift data warehouse through a two-step process. Initially, data is loaded onto an Amazon S3 bucket and then transferred to the Redshift cluster.

1. **Upload to Amazon S3**: The CSV files obtained from the data extraction process are first uploaded to an Amazon S3 bucket. This step ensures a scalable and reliable storage solution for the data before its ingestion into the Redshift cluster.

2. **Redshift Data Ingestion**: Subsequently, the data stored on Amazon S3 is ingested into the Amazon Redshift data warehouse. This process involves copying the data from S3 into Redshift tables, where data types and formats are seamlessly preserved. This enables efficient storage, management, and analysis of the data within the Redshift environment.

By following this approach, the project maintains data integrity and scalability while harnessing the capabilities of both Amazon S3 and Amazon Redshift to ensure a robust and optimized data processing pipeline.


## 📊 Data Analysis
The project's final stage delves into data analysis using Python's data manipulation and visualization libraries. The `Analysis.ipynb` notebook showcases insights extracted from the amalgamation of Premier League and weather data. While not overly complex, this analysis paints a picture of the potential synergies between seemingly disparate data sources.

## ✨ Conclusion
This repository encapsulates the journey of harnessing data from diverse APIs, transforming it, loading it into a Redshift data warehouse, and deriving meaningful insights. The project exemplifies the fusion of data engineering and analysis, highlighting the creative possibilities that emerge from combining unconventional data sources. Feel free to explore, expand, and innovate upon this foundation!