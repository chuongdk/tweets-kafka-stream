package services

import java.text.SimpleDateFormat

import com.lightbend.kafka.scala.streams.DefaultSerdes.{longSerde, stringSerde}
import com.lightbend.kafka.scala.streams.KStreamS
import org.apache.kafka.streams.kstream.Serialized
import ujson.Js
import ujson.Js.Value

import scala.collection.mutable

object StreamTransform {

  val serializedValueString = Serialized.`with`(stringSerde, stringSerde)

  val serializedValueLong = Serialized.`with`(stringSerde, longSerde)

  // create new stream that transform tweets 1:1
  def tweet_transformed(tweetsInput: KStreamS[String, String]): KStreamS[String, String] = {
    tweetsInput.map{
      (key, record)  =>
        val tweetJson = ujson.read(record)
        val userJson = ujson.read(tweetJson("user"))
        val entitiesJson = ujson.read(tweetJson("entities"))

        // output values
        val userID = userJson("id_str").toString()
        val name = userJson("name").toString()
        val fav = tweetJson("favorite_count").toString()
        val hashtags = entitiesJson("hashtags").arr.size.toString

        // user_mentions = array of json => map to array of id
        //user_mentions = [{"screen_name":"funkemcfly","name":"funké3","id":554060069,"id_str":"554060069","indices":[3,14]}]
        //val user_mentions = entitiesJson("user_mentions").arr

        val jsonTransformed = Js.Obj(
          "userID" -> userID.replace('"'.toString, ""),
          "name" -> name.replace('"'.toString, ""),
          "fav" -> fav.replace('"'.toString, ""),
          "hashtags" -> hashtags.replace('"'.toString, "")
          //"user_mentions" -> user_mentions
        )
        (userID.toString, jsonTransformed.toString())
    }
  }


  // fav stream: key, value = userID, #fav
  def userid_stream(tweet_transformed: KStreamS[String, String]): KStreamS[String, String] = {
    tweet_transformed.map{
      (key, record)  =>
        val tweetJson = ujson.read(record)
        val userID = tweetJson("userID")
        val newkey = "tw." + userID.toString() + ".count"
        (newkey, "1")
    }
  }




  // fav stream: key, value = userID, #fav
  def fav_stream(tweet_transformed: KStreamS[String, String]): KStreamS[String, String] = {
    tweet_transformed.map{
      (key, record)  =>
        val tweetJson = ujson.read(record)
        val fav =
          tweetJson("fav")
        (key, fav.toString().replace('"'.toString, ""))
    }
  }



  // fav stream: key, value = userID, #hashtags
  def hashtags_stream(tweet_transformed: KStreamS[String, String]): KStreamS[String, Long] = {
    tweet_transformed.map{
      (key, record)  =>
        val tweetJson = ujson.read(record)
        val hashtags = try {
          tweetJson("hashtags").toString().replace('"'.toString, "").toLong
        }
        catch {
          case _: Throwable => 0L
        }

        (key, hashtags )
    }
  }


  // create a stream of user mentioned with key = userID, value = user Name
  def user_mention_stream(tweetsInput: KStreamS[String, String]): KStreamS[String, Long] = {
    tweetsInput.flatMap{ (key, record) =>
      val tweetJson = ujson.read(record)
      val entitiesJson = ujson.read(tweetJson("entities"))
      val user_mentions = entitiesJson("user_mentions").arr

      // list of output (key, value)
      val listOutput: mutable.Seq[(String, Long)] = user_mentions.map{ user_mention =>
        val json_user_mention = ujson.read(user_mention)
        val user_id = json_user_mention("id_str").toString().replace('"'.toString, "")
        val user_name = json_user_mention("name").toString().replace('"'.toString, "")
        (user_id, 1L)
      }

      listOutput
    }
  }

  def tweet_to_mario(tweetJson: Value ) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")


    val entitiesJson = ujson.read(tweetJson("entities"))
    val userJson = ujson.read(tweetJson("user"))

    // output values
    val tweetID = tweetJson("id_str").toString()
    val userID = userJson("id_str").toString()
    val userName = userJson("name").toString()
    val created_at: Long = tweetJson("timestamp_ms").toString.replace('"'.toString, "").toLong
    val date: String = dateFormat.format(created_at)
    val text = tweetJson("text").toString()

    val medias: String =
      try {
        val medias = entitiesJson("media").arr
        val mediaMap = medias.map {
          media =>
            val jsonMedia = ujson.read (media)
            val idMedia = jsonMedia ("id_str").toString ()
            val media_url = jsonMedia ("media_url").toString ()
            (idMedia, media_url)
        }.mkString(",")

        mediaMap
      }
      catch {
        case _: Throwable => ""
      }

    val hashtags = entitiesJson("hashtags").arr

    val listHashtags: String = hashtags.map{ hashtag =>
      val jsonHashtag = ujson.read(hashtag)
      val textHashtag = jsonHashtag("text")
      textHashtag
    }.mkString(",")


    // ujson string value always have "<string>"
    val jsonMario = Js.Obj(
      "id" -> tweetID.replace('"'.toString, ""),
      "user_id" -> userID.replace('"'.toString, ""),
      "screen_name" -> userName.replace('"'.toString, ""),
      "created_at" -> date,
      "text" ->  text.replace('"'.toString, ""),
      "medias" -> medias.replace('"'.toString, ""),
      "hashtags" -> listHashtags.replace('"'.toString, "")
    )
    jsonMario.toString()
  }


  // create mario stream from original tweets stream
  def mario_stream(tweetsInput: KStreamS[String, String]) = {
    tweetsInput.flatMapValues{ record: String =>
      val tweetJson: Value = ujson.read(record)
      val lang: String = tweetJson("lang").toString().replace('"'.toString, "")
      val text: String = tweetJson("text").toString()

      // WHERE condition
      val output: Seq[String] =
        if (lang.equalsIgnoreCase("fr") && (text.toUpperCase().contains("MARIO"))) {
          Seq( tweet_to_mario(tweetJson)  )
        }
        else
          Nil

      output
    }
  }


  def fav_count_stream(tweetsTransformed: KStreamS[String, String]) ={
    fav_stream(tweetsTransformed).groupByKey(serializedValueString).reduce{(v1, v2) =>
      val fav1 = try {
        v1.toLong
      }
      catch {
        case _: Throwable => 0L
      }
      val fav2 = try {
        v2.toLong
      }
      catch {
        case _: Throwable => 0L
      }
      (fav1 + fav2).toString
    }.toStream
  }
ll
  

  def user_count_stream(tweetsTransformed: KStreamS[String, String]) ={
    tweetsTransformed.groupByKey(serializedValueString).count("user_count").toStream
  }

  def hashtags_count_stream(tweetsTransformed: KStreamS[String, String]) ={
    hashtags_stream(tweetsTransformed).groupByKey(serializedValueLong).reduce{ (v1, v2) =>
      v1 + v2
    }.toStream
  }

  def mentions_count_stream(tweetsInput: KStreamS[String, String]) ={
    user_mention_stream(tweetsInput).groupByKey(serializedValueLong).reduce{ (v1, v2) =>
      v1 + v2
    }.toStream
  }
}
