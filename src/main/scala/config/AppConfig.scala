package config

case class AppConfig( input: String,
                      output: String
                    )


object AppConfig {
  import com.typesafe.config.ConfigFactory
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._

  val config = ConfigFactory.load("application.conf").as[AppConfig]("app")

}


