package services

import com.lightbend.kafka.scala.streams.KStreamS

class tmp {

  def tempCode() = {
    // sum fav
  /*  val fav_count: KStreamS[String, String] = tweetsTransformed.reduce{ (v1, v2) =>
      val json_v1 = ujson.read(v1)
      val fav1 = try {
        json_v1("fav").toString().replace('"'.toString, "").toLong
      }
      catch {
        case _: Throwable => 0L
      }
      val json_v2 = ujson.read(v2)
      val fav2 = try {
        json_v2("fav").toString().replace('"'.toString, "").toLong
      }
      catch {
        case _: Throwable => 0L
      }

      val sum = fav1 + fav2
      sum.toString
    }.toStream

    */


  }

}
