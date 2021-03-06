# Data Warehouse on Cloud
## Summary of project
This project is to build a data warehouse on Redshift for a music application. The logs from application will contains the event logs (historical music listening from users) and song library data. Data are stored as json format in AWS S3. There are three main tasks involved to accomplete the project:
> * Extract data from S3 to staging tables designed on Redshift
> * Transform data from staging table and Load into dimensional and fact table on Redshift
> * Verify and review fact data with analysis queries

## How to run the python scripts
1. Run `python create_tables.py` to create the schema and newly tables including fact table (songplays) and dimensional tables (users, songs, artists and time).

2. Then run `python etl.py` to start the job of trasferring data from S3 to staging tables (staging_events & staging_songs), do transforming and loading data to targeted table designed on Redshift DW.

3. Run `python test.py` to examine the results. For example, below query to anwer the question of what is the top 10 songs listened in 2018.

```sql
SELECT s.title AS song_title,
       COUNT(songplay_id) AS listened_times,
       a.name AS artist_name
FROM music.songplays sp
INNER JOIN music.songs s ON s.song_id = sp.song_id
INNER JOIN music.artists a ON a.artist_id = sp.artist_id
WHERE EXTRACT(YEAR FROM start_time) = 2018
GROUP BY s.title, a.name
ORDER BY listened_times DESC
LIMIT 10
```
Result:
![Top listened songs in 2018](/images/top_songs_listened.png)

## Files in the repository
1. `dwh.cfg` contains configuration parameters to access AWS Redshift cluster, S3 bucket and IAM role.
2. `create_tables.py` drops and creates tables. Whenever run this file, all tables are reset with blank data.
3. `etl.ipynb` extract data from S3 to staging tables on Redshift. Then it do transform and load data into relational tables which are designed in advanced from create_tables.py script.
4. `sql_queries.py` contains all sql queries for drop, create, insert, select information from/to Redshift database.

## Dataset used in S3
 The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
> s3.ObjectSummary(bucket_name='udacity-dend', key='song-data/A/A/A/TRAAAAK128F9318786.json')

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from a music streaming app based on specified configurations.
> s3.ObjectSummary(bucket_name='udacity-dend', key='log-data/2018/11/2018-11-01-events.json')
