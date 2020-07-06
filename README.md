# Quickstart:
1. Create an Amazon Redshift cluster
2. Enter cluster information and ARN for S3-read-access in 'dwh.cfg' file
3. Run create_tables.py
4. Run etl.py

# Project Name:
Data Warehouse
* This is a part of Udacity's Data Engineering Nanodegree Program

# Purpose:
To create a data warehouse on Amazon Redshift
* A startup called Sparkify experieced user growth, and wants to move its process onto cloud

# Detailed Process:
1. Create an Amazon Redshift cluster, where staging tables and final tables will reside
2. Enter cluster information and ARN for S3-read-access in 'dwh.cfg' file
3. Run create_tables.py, which will create the staging tables and the final tables in Redshift
4. Run etl.py, which will extract original data from Amazon S3 onto the staging tables in Redshift
5. Data will be extracted from the staging tables, transformed, and loaded onto the final tables (in Redshift)
