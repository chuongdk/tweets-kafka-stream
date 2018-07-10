import java.util.Properties

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.{KStreamS, KTableS}
import config.KafkaStreamProperties
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{Printed, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology, _}
import services.StreamTransform._


//TODO: value = json => avro schema

object Main {
  val streamsConfiguration: Properties = KafkaStreamProperties.getProperties
  val producedValueString = Produced.`with`(Serdes.String, Serdes.String)


  def main(args: Array[String]): Unit = {

    // Step 1. Describe Topology
    val builder = new StreamsBuilder

    // Read (consume) records from tweetsInput topic with keys and values being Strings
    val tweetsInput: KStreamS[String, String] = new KStreamS( builder.stream("tweets") )
    // we transform to: userid, #fav, #hashtags, name
    val tweetsTransformed = tweet_transformed(tweetsInput)
    tweetsTransformed.to("tweets-transformed")(producedValueString)

    // Create user table (id, name, fav, hashtags) that update changelogs
    val user_table:  KTableS[String, String]   = create_user_table(tweetsTransformed)

    // mention table
    val mentions_table: KTableS[String, String]  = create_mentions_table(tweetsInput)

    // join 2 table
    val reconciliation_table = create_reconciliation_table(user_table, mentions_table)

    val reco_stream = reconciliation_table.toStream

    reco_stream.to("tweets-reconciliation")(producedValueString)

    val tweets_mario_fr: KStreamS[String, String] = mario_stream(tweetsInput)

    tweets_mario_fr.to("tweets-mario-fr")(producedValueString)

    // Print out records to stdout for debugging purposes
    val sysout = Printed
      .toSysOut[String, String]
      .withLabel("stdout")
    reco_stream.print(sysout)

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


