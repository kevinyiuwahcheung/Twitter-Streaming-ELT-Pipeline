
# Kevin's Twitter Streaming ELT Pipeline

This project is mainly focused on ELT pipeline and Kafka cluster. Tools/platforms includeds Twitter Streaming API, Confluent Kafka Cloud, Apache Airflow and Snowflake.
 
![App Screenshot](https://github.com/kevinyiuwahcheung/Twitter-Streaming-ELT-Pipeline/blob/main/datapipeline.png)


## Data Source
I used Twitter Streaming API as the data source because it allows me to query live stream tweets. I have added a filter for the real time tweets, which contains keywords of bitcoin, usa, russia, ethereum, iphone and etc. 



## Confluent Kafka Cloud

![App Screenshot](https://github.com/kevinyiuwahcheung/Twitter-Streaming-ELT-Pipeline/blob/main/Screen%20Shot%202022-02-22%20at%206.28.21%20PM.png)

On Kafka Cloud, I created a topic called “faker” with 5 partitions and 3 replications. The producer publishes tweets to kafka clusters with all acknowledgements. 
## Confluent Kafka Snowflake Connector

![App Screenshot](https://github.com/kevinyiuwahcheung/Twitter-Streaming-ELT-Pipeline/blob/main/Screen%20Shot%202022-02-22%20at%206.29.14%20PM.png)

I set up a Confluent Kafka Snowflake Sink connector. Cached messages in all partitions will be flushed to the snowflake data warehouse every 15 seconds.
## Airflow For JSON Incremental Transformation

![App Screenshot](https://github.com/kevinyiuwahcheung/Twitter-Streaming-ELT-Pipeline/blob/main/Screen%20Shot%202022-02-22%20at%206.30.53%20PM.png)

As we can see from the screenshot above, the data being loaded into the snowflake table has 2 columns. Both of them are in JSON format. 
RECORD_METADATA has fields like “CreateTime”, “offset”, “partition” and “topic”. 
RECORD_CONTENT has fields like “created_at”, “tweet_id” and so on. 
In order to make it look more structured, I set up an Airflow DAG that will do incremental loading from table “FAKER” to “NORMAL_FAKER”. “FAKER_NORMAL '' has columns like TWEET_ID, TWEET_TEXT, TWEET_FULLTEXT, USER_ID, USERNAME and METADATA. 


The Airflow DAG basically runs the following query as an incremental load. It first query the latest CreateTime in FAKER_NORMAL table, then it will go to FAKER table and query rows which have CreateTime after that. 
```bash
INSERT INTO FAKER_NORMAL
SELECT 
    RECORD_CONTENT:id_str,
    RECORD_CONTENT:text,
    RECORD_CONTENT:retweeted_status.extended_tweet.full_text,
    RECORD_CONTENT:user.id_str,
    RECORD_CONTENT:user.name,
    RECORD_METADATA
FROM FAKER
WHERE RECORD_METADATA:CreateTime > (SELECT max(METADATA:CreateTime) FROM FAKER_NORMAL);
```
