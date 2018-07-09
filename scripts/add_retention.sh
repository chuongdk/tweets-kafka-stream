kafka-configs --zookeeper localhost:2181 --entity-type topics --entity-name bid_with_proper_key  --alter --add-config retention.ms=60000000
