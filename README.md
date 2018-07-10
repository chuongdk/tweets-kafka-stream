# An example of using kafka-stream and kafka-connect to ETL a stream of tweets in real time

## Objective
From a tweets topic, we generate 2 kafka topic 

- topic `tweets-reconciliation`: It is a changelog topic that sumerizes count, fav, hashtags, name, mentioned of userID. Key = userID, value = json info. For example:

```[stdout]: 89907746, {"userID":"89907746","count":35,"fav":1592,"hashtags":128,"name":"MeriStation.com","mentioned":7}
[stdout]: 86395621, {"userID":"86395621","count":9,"fav":864,"hashtags":9,"name":"NVIDIA GeForce","mentioned":37}
[stdout]: 286792082, {"userID":"286792082","count":5,"fav":76,"hashtags":10,"name":"\ud83d\udd25 Yassin D. GRONIE \ud83d\udd25","mentioned":2}
[stdout]: 320703608, {"userID":"320703608","count":1,"fav":72,"hashtags":5,"name":"bun witch\ud83c\udf38\ud83c\udf80\ud83d\udc30","mentioned":39}```

Then we can use kafka connect to export the topic to S3, mongoDB, elasticsearch,...

- topic `tweets_mario_fr`

## Quick start

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


## TODO: 
8. Using kafka connect to load these transformed streams to persitent storage/database (elastic search, s3, mongdb, ..)

9. Avro format with Schema registry



## Solution :
EX 1:
	- 1 STREAM (tweets-transform-stream) for resume of tweets (USER ID, USER NAME, #FAV, #HASHTAGS) 
		=> 1 TABLE for changelog of user (USER ID, USER NAME, #FAV, #HASHTAGS) 
	- 1 STREAM (user-mentions-stream) for user mentions 
		=> 1 TABLE for changlog of user mentions
	- 1 TABLE for look up USER ID => USER NAME, #FAV, #HASHTAGS, #MENTIONS

EX 2:
	- 1 STREAM (mario-stream)  for resume of mario tweets 
