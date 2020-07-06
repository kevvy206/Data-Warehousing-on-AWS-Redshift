## Project Name
Data Warehousing on AWS Redshift<br><br>
## Purpose
To create a data warehouse on Amazon Redshift<br><br>
## Description
This project is a part of Udacity's Data Engineering Nanodegree program. Sparkify is a startup that runs music streaming app. The company experieced explosive user growth, and wants to move its data onto cloud. For its columnar storage and hence faster queries, AWS Redshift is chosen. Data to be moved is stored in AWS S3, and staging and ETL will be performed on it.<br>
<strong>Important:
To run this project, you need to fill in information in dwh.cfg file.
To fill in the information, you need the following: an AWS Redshift cluster and IAM role with S3 access privilege.</strong><br><br>
## How to Run
1. Create an Amazon Redshift cluster
2. Enter cluster information and ARN for S3-read-access in 'dwh.cfg' file
3. Run create_tables.py
4. Run etl.py<br><br>
## Detailed Process:
1. Create an Amazon Redshift cluster, where staging tables and final tables will reside
2. Enter cluster information and ARN for S3-read-access in 'dwh.cfg' file
3. Run create_tables.py, which will create the staging tables and the final tables in Redshift
4. Run etl.py, which will extract original data from Amazon S3 onto the staging tables in Redshift
5. Data will be extracted from the staging tables, transformed, and loaded onto the final tables (in AWS Redshift)
