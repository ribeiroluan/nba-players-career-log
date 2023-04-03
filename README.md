

# NBA Player Game Log ETL Pipeline

## Overview
This data pipeline extracts game log data for a specific player from the NBA API, transforms it into a structured format, and loads it into an AWS Redshift cluster. The data is then used to generate charts in PowerBI.

## Motivation
The project aimed to take advantage of NBA data, which is something that I am really passionate about, to practice key Data Engineering skills.

## Architecture
![Project architecture](/images/nba_architecture_chart.png "Project architecture")
1. Extract and transform both Regular Season and Playoffs career log using the [NBA API](https://github.com/swar/nba_api)
2. Upload data to [AWS S3](https://aws.amazon.com/pt/s3/)
3. Upload data to [AWS Redshift](https://aws.amazon.com/pt/redshift/)
4. Schedule and monitor pipeline using [Apache Airflow](https://airflow.apache.org/) on [Docker Compose](https://docs.docker.com/compose/)
5. Generate charts using [Power BI](https://powerbi.microsoft.com/)

## Output
`PowerBI dashboard goes here`

## Setup