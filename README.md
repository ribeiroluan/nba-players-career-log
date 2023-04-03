

# NBA Player Game Log ETL Pipeline

## Overview
This data pipeline  extracts game log data from the NBA API, transforms it into a structured format, and loads it into an AWS Redshift cluster. The data is then used to generate charts in PowerBI.

## Motivation
The project aimed National Basketball Association (NBA) data, which is something that I am really into, to practice key Data Engineering skills.

## Architecture
![Project architecture](/images/nba_data_chart.png "Project architecture")
1. Extract and transform both Regular Season and Playoffs career log using the [NBA API](https://github.com/swar/nba_api)
2. Upload data to [AWS S3](https://aws.amazon.com/pt/s3/)
3. Upload data to [AWS Redshift](https://aws.amazon.com/pt/redshift/)
4. Generate charts using [Power BI](https://powerbi.microsoft.com/)

## Output
`PowerBI dashboard goes here`

## Setup