An example of using kafka-stream and kafka-connect to ETL a stream of tweets in real time

1. Get tweets-v2.json from S3
./scripts/getData.sh 

2. Split tweets to smaller files for streaming (100 tweets/file)
./scripts/splitTweets.sh

3. Install confluent open source v4.1.1
https://www.confluent.io/download/

4. Start confluent (kafka stack)
confluent start

5. Create topics 
./scripts/create_topics.sh

6. Streamming tweets 
./scripts/produce_tweets_stream.sh

7. Using kafka stream to Transform tweets into streams
sbt run

8. Using kafka connect to load these transformed streams to persitent storage/database (elastic search, s3, mongdb, ..)
TODO
