kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic tweets
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic tweets_transformed
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic user_mentions
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic tweets_mario_fr
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic user_count
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic fav_count
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic hashtags_count








