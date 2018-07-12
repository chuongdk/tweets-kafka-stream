# An example of using kafka-stream and kafka-connect to ETL a stream of tweets in real time

## Objective
From a tweets topic, we generate 3 kafka topic 

* topic `tweets-reconciliation`: It is a changelog topic that sumerizes count, fav, hashtags, name, mentioned of userID. Key = userID, value = json info. For example:

```
[stdout]: 89907746, {"userID":"89907746","count":35,"fav":1592,"hashtags":128,"name":"MeriStation.com","mentioned":7}
[stdout]: 86395621, {"userID":"86395621","count":9,"fav":864,"hashtags":9,"name":"NVIDIA GeForce","mentioned":37}
[stdout]: 286792082, {"userID":"286792082","count":5,"fav":76,"hashtags":10,"name":"\ud83d\udd25 Yassin D. GRONIE \ud83d\udd25","mentioned":2}
[stdout]: 320703608, {"userID":"320703608","count":1,"fav":72,"hashtags":5,"name":"bun witch\ud83c\udf38\ud83c\udf80\ud83d\udc30","mentioned":39}
```

* topic `tweets-avro-user`: it contains the same messages of `tweets-reconciliation`, but in avro format. The schema is updated with Schema Registry.

* topic `tweets_mario_fr`

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

8. Show avro topic
```
$kafka-avro-console-consumer  --bootstrap-server localhost:9092 --topic tweets-avro-user
```

or if you want to see key / value:
```
kafka-avro-console-consumer  --bootstrap-server localhost:9092 --topic tweets-avro-user --key-deserializer org.apache.kafka.common.serialization.StringDeserializer --property  print.key=true
```

9. Sink Avro topic to s3 with Kafka Connect   (note: you need to modify plugin.path in worker.properties)
```
$connect-standalone kafka-connect-config/worker.properties kafka-connect-config/s3.properties 
```


10. Using kafka connect to sink avro topic to elasticsearch

* install elastic search 6.3 (https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html)
* start elastic search: `$elasticsearch`
* Sink avro topic to elasticsearch 
```
connect-standalone kafka-connect-config/worker.propertiesties kafka-connect-config/elasticsearch.properties 
```
* Verify index of elasticsearch
```
curl -XGET 'http://localhost:9200/tweets-avro-user/_search?pretty'
```

* So, we have key/value of elasticsearch is exactly key/value of kafka topic

```
      {
        "_index" : "tweets-avro-user",
        "_type" : "kafka-connect",
        "_id" : "3105737085",
        "_score" : 1.0,
        "_source" : {
          "userID" : "3105737085",
          "count" : 26,
          "fav" : 1560,
          "hashtags" : 52,
          "name" : "\uD835\uDCC1\uD835\uDC5C\uD835\uDCCB\uD835\uDC52\uD835\uDFE6\uD835\uDC52\uD835\uDCCB\uD835\uDCB6 ♡",
          "mentioned" : 0
        }
```

* Get info of userID = 110131353
 curl -XGET 'http://localhost:9200/tweets-avro-user/kafka-connect/110131353'





## Solution :
EX 1:
	- 1 STREAM (tweets-transform-stream) for resume of tweets (USER ID, USER NAME, #FAV, #HASHTAGS) 
		=> 1 TABLE for changelog of user (USER ID, USER NAME, #FAV, #HASHTAGS) 
	- 1 STREAM (user-mentions-stream) for user mentions 
		=> 1 TABLE for changlog of user mentions
	- 1 TABLE for look up USER ID => USER NAME, #FAV, #HASHTAGS, #MENTIONS

EX 2:
	- 1 STREAM (mario-stream)  for resume of mario tweets 
