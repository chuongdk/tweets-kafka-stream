package config

case class SparkConfig(master: String,
                       memory: String,
                       logLevel: String)

object SparkConfig {
  import com.typesafe.config.ConfigFactory
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._

  val config = ConfigFactory.load("application.conf").as[SparkConfig]("spark")

}