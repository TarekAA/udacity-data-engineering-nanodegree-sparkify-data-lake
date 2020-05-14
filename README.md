# udacity-data-engineering-nanodegree-sparkify-data-lake
This is the #4 project in udacity Data Engineering Nano Degree Program. The project is about an imaginary company Sparkify that had their user base grow and want to move their data into a Data Lake. We're tasked with loading data from S3 into the Data Lake, processing data into dimensional model and then loading it back to S3 as parquet files.

# How to run code
This code has been tested out using Amazon EMR service. to run the code using EMR:
1. Enable connection to the cluster over ssh
2. Copy `etl.py` to the cluster. Then run the following
3. Run `spark-submit --master yarn ./etl.py`

# Dataset Description
Data used is a collection of JSON files residing on S3.
There are two dataset being used.

## song_data
[Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). This dataset contains different attributes about songs. e.g, title, duration and year. The directory is partitioned using the first 3 letter of the track id.
An example song file path:

`song_data/A/A/B/TRAABJL12903CDCF1A.json`

And below is the previous file content:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

## log_data
Data coming form [this event simulator datasets](https://github.com/Interana/eventsim). This simulator simulate user behavior in an imaginary song streaming service.
Data is partitioned by year then by month e.g., `log_data/2018/11/2018-11-12-events.json`.



# File Description

## etl.py
This file contains all the code related to the ETL pipeline.
It moves data from S3, process it with Dataset API, load into dimensional model, then save it back to S3 as partitioned parquet files.

## dl.cfg
This file should contains Data Lake configurations. Basically AWS access and private keys.

## data
A subset of the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) and [this event simulator datasets](https://github.com/Interana/eventsim). This can be used to process a smaller subset of data before testing code on the full dataset.
