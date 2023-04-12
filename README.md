`This README is still a work in progress`
# NBA Player Game Log ETL Pipeline

## Overview
This data pipeline extracts game log data for a specific player from the NBA API, makes some minor transformations, and loads it into an AWS S3 bucket, which is then copied into a AWS Redshift cluster. Finally, the data is used to generate charts in PowerBI. The player I chose to collect data on is [Stephen Curry](https://pt.wikipedia.org/wiki/Stephen_Curry).

## Motivation
The project aimed to take advantage of NBA data, which is something that I am really passionate about, to practice key Data Engineering skills.

## Architecture
![Project architecture](/images/nba_architecture_chart.png "Project architecture")
1. Extract and transform both Regular Season and Playoffs career log using the [NBA API](https://github.com/swar/nba_api)
2. Upload data to [AWS S3](https://aws.amazon.com/pt/s3/)
3. Upload data to [AWS Redshift](https://aws.amazon.com/pt/redshift/)
4. Schedule and monitor pipeline using [Apache Airflow](https://airflow.apache.org/) on [Docker Compose](https://docs.docker.com/compose/)
5. Generate charts using [Power BI](https://powerbi.microsoft.com/)

Shown below is the Directed Acyclic Graph (DAG) generated using Airflow.

![DAG](/images/dag.png "DAG")

## Output
PowerBI dashboard goes here

## Installation