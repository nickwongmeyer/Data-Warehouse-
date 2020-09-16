# Data-Warehouse
Building Datawarhouse via AWS Reshift 
#Introduction
Sparkify is a music streaming startup, holding the JSON logs to monitor use activity and JSON metadata for songs in the application. 
This project is aiming to deliver a Data Warehouse under the infrastructure of AWS S3 with Redshift, by building an ETL pipeline to extract all the data from Udacity S3 bucket to load the it to Redshift, transform them into a set of dimension database tables with Python and SQL, which could be  analysed with data visualisation and SQL query.
In fact, ‘Sparkify’ would like to build an insight to find out the relationship between their songs and the song they have been listening to. 

# Redshift 
The design of the database schema under the Redshift could significantly affect all the query needed for the analysis; therefore, several important points are considered;
-	The distribution of the nodes evenly in the redshift 
-	How to join the primary keys and foreign keys between the tables and their sort and orders. 

# Schema Selection 
Staging Table – the information provided by Udacity which is copied as JSON file to the S3 bucket. 
- Staging_songs: info about songs and artists
- Staging_events: actions completed by users

Star schema to build relationship between the following tables
Fact Table 
-	Songplays:records in event data associated with song plays i.e. records with page NextSong
Dimension Tables 
-	Users: users information of the apps
-	Songs: songs inside the database
-	Artists: artists information inside the database
-	Time: timestamps of the songplays into different time units 


# Project requirement:
Data Warehouse Setup
A valid aws account is set up along with the security credentials, as well as a python environment in order to satisfies the module requirements given in requirements.txt
-	A new IAM user account is created
-	Building the Redshift which could access the S3 bucket 
-	Create Redshift Cluster by using the Secret Access Key, Endpoint and ARN under the config file in dwg.cfg. 



# ETL Pipeline
-	Loading the data from S3 buckets to the staging tables in Redshift Cluster.
-	Insert the data from the staging tables into fact and dimension tables to organise a proper structure for easy access under the analytic environment.
Project Structure
-	Sql_queries1.py: this includes different SQL statements of CREATE, DROP and INSERT which is configured by the create_table1.py to drop and re-create all the new tables into the redshift data warehouse. 
-	Etl1.py: This script is to execute the queries that loading the data from the S3 bucket into the Redshift by loading all the events from the json files into staging tables, as well as insert them into the fact and dimensional tables too.   
-	dhw.cfg: A Configuration file which connect the Redshift and IAM. 
-	requirements.txt: file needed to process the deploy the application onto cloud

Data are copied from the directories as a JSON formatted into all the stage tables, which were provided by Udacity as ```(LOG_DATA=s3://udacity-dend/log_data, LOG_JSONPATH=s3://udacity-dend/log_json_path.json,SONG_DATA=s3://udacity-dend/song_data)```, copying the data to the staging songs and staging_events tables. 
The information were split into different tables into fact and dimension tables which is more flexible for the analytics point of view when the analyst are going to build the queries. In addition to this, a filter is built under the insert statement by the entries for which page is equal to 'NextSong', particularly, an extract function could be used for the datetime object under the time table from the songplays, And the 'timestamp' function is used to convert the epoch timestamp to a datetime object.
