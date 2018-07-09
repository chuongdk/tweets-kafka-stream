import java.util.Properties
import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.KStreamS
import config.KafkaStreamProperties
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{Printed, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology, _}
import services.StreamTransform._


//TODO: value = json => avro schema

object Main {
  val streamsConfiguration: Properties = KafkaStreamProperties.getProperties
  val producedValueString = Produced.`with`(Serdes.String, Serdes.String)
  val producedValueLong = Produced.`with`(stringSerde, longSerde)


  def main(args: Array[String]): Unit = {

    // Step 1. Describe Topology
    val builder = new StreamsBuilder

    // Read (consume) records from tweetsInput topic with keys and values being Strings
    val tweetsInput: KStreamS[String, String] = new KStreamS( builder.stream("tweets") )
    // we transform to: userid, #fav, #hashtags, mentions, name
    val tweetsTransformed = tweet_transformed(tweetsInput)

    // Create output stream
    val user_count: KStreamS[String, Long] = user_count_stream(tweetsTransformed)
    val fav_count: KStreamS[String, String] = fav_count_stream(tweetsTransformed)
    val hashtags_count: KStreamS[String, Long] = hashtags_count_stream(tweetsTransformed)
    val mentions_count: KStreamS[String, Long] = mentions_count_stream(tweetsInput)
    val tweets_mario_fr: KStreamS[String, String] = mario_stream(tweetsInput)

    // Write (produce) the records out to mario topic
    user_count.to("user_count")(producedValueLong)
    fav_count.to("fav_count")(producedValueString)
    hashtags_count.to("hashtags_count")(producedValueLong)
    mentions_count.to("user_mentions")(producedValueLong)
    tweets_mario_fr.to("tweets_mario_fr")(producedValueString)

    // Print out records to stdout for debugging purposes
    val sysout = Printed
      .toSysOut[String, Long]
      .withLabel("stdout")
    mentions_count.print(sysout)


    // Build Topology
    val topology: Topology = builder.build
    println(topology.describe())


    // Create Kafka Streams Client
    val config = new StreamsConfig(streamsConfiguration)
    val ks = new KafkaStreams(topology, config)

    // Step 5. Start Stream Processing, i.e. Consuming, Processing and Producing Records
    println("Stream is running ...")
    ks.start()

  }
}


