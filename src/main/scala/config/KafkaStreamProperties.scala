package config

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig

object KafkaStreamProperties {
  val p = new Properties()
  p.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweets-kafka-stream")
  p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
  p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
  // The commit interval for flushing records to state stores and downstream must be lower than
  // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
  p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
  p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  // Use a temporary directory for storing state, which will be automatically removed after the test.

  p

  def getProperties = {
    p
  }
}
