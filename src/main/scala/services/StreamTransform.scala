package services

import java.text.SimpleDateFormat

import com.lightbend.kafka.scala.streams.DefaultSerdes.{longSerde, stringSerde}
import com.lightbend.kafka.scala.streams.{KStreamS, KTableS}
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

        val jsonTransformed = Js.Obj(
          "userID" -> userJson("id_str"),
          "count" -> 1,
          "fav" -> tweetJson("favorite_count"),
          "hashtags" -> entitiesJson("hashtags").arr.size,
          "name" -> userJson("name")
        )
        (userJson("id_str").str, jsonTransformed.toString())
    }
  }

  // create a stream of user mentioned with key = userID, value = user Name
  // 1 tweet => 0 or 1 or more message of user mentions. That's why we need flatMap instead of map
  def user_mention_stream(tweetsInput: KStreamS[String, String]): KStreamS[String, String] = {
    tweetsInput.flatMap{ (key, record) =>
      val tweetJson = ujson.read(record)
      val entitiesJson = ujson.read(tweetJson("entities"))
      val user_mentions = entitiesJson("user_mentions").arr

      // list of output (key, value)
      val listOutput: mutable.Seq[(String, String)] = user_mentions.map{ user_mention =>
        val json_user_mention = ujson.read(user_mention)
        val user_id = json_user_mention("id_str").str
        val jsonValue: String = Js.Obj(
          "userID" -> json_user_mention("id_str"),
          "mentioned" -> 1
        ).toString()
        (user_id, jsonValue)
      }
      listOutput
    }
  }


  // A table to keep counting userID -> mentions
  def create_mentions_table(tweetsInput: KStreamS[String, String]): KTableS[String, String] ={
    user_mention_stream(tweetsInput).groupByKey(serializedValueString).reduce{ (v1, v2) =>
      val v1Json = ujson.read(v1)
      val v2Json = ujson.read(v2)

      val mentionCount = v1Json("mentioned").num + v2Json("mentioned").num

      val jsonTransformed = Js.Obj(
        "userID" -> v1Json("userID"),
        "mentioned" -> mentionCount.toInt
      )
      jsonTransformed.toString()
    }
  }



  def tweet_to_mario(tweetJson: Value ) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val entitiesJson = ujson.read(tweetJson("entities"))
    val userJson = ujson.read(tweetJson("user"))

    // output values
    val tweetID = tweetJson("id_str").str
    val userID = userJson("id_str").str
    val userName = userJson("name").str
    val created_at: Long = tweetJson("timestamp_ms").str.toLong
    val date: String = dateFormat.format(created_at)
    val text = tweetJson("text").str

    val medias: String =
      try {
        val medias = entitiesJson("media").arr
        val mediaMap = medias.map {
          media =>
            val jsonMedia = ujson.read (media)
            val idMedia = jsonMedia ("id_str").str
            val media_url = jsonMedia ("media_url").str
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
      val textHashtag = jsonHashtag("text").str
      textHashtag
    }.mkString(",")


    // ujson string value always have "<string>"
    val jsonMario = Js.Obj(
      "id" -> tweetID,
      "user_id" -> userID,
      "screen_name" -> userName,
      "created_at" -> date,
      "text" ->  text,
      "medias" -> medias,
      "hashtags" -> listHashtags
    )
    jsonMario.toString()
  }


  // create mario stream from original tweets stream
  def mario_stream(tweetsInput: KStreamS[String, String]) = {
    tweetsInput.flatMapValues{ record: String =>
      val tweetJson: Value = ujson.read(record)
      val lang: String = tweetJson("lang").str
      val text: String = tweetJson("text").str

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


  // One stream that aggregate: count + fav + hashtags.
  // IDEA: transform tweets => group by stream => aggregate table => changelog stream
  def create_user_table(tweetsTransformed: KStreamS[String, String]) ={

    tweetsTransformed.groupByKey(serializedValueString).reduce{ (v1, v2 ) =>
      val v1Json = ujson.read(v1)
      val v2Json = ujson.read(v2)

      val jsonTransformed = Js.Obj(
        "userID" -> v1Json("userID"),
        "count" -> (v1Json("count").num + v2Json("count").num).toInt,
        "fav" -> (v1Json("fav").num + v2Json("fav").num).toInt,
        "hashtags" -> (v1Json("hashtags").num + v2Json("hashtags").num).toInt,
        "name" -> v1Json("name")
      )
      jsonTransformed.toString()
    }
  }

  // we join user_table with mention_table
  def create_reconciliation_table(user_table: KTableS[String, String], mention_table: KTableS[String, String]) ={
      user_table.leftJoin(mention_table,
        joiner = { (v1: String, v2: String) =>
          val v1Json = ujson.read(v1)

          val mention: Int = if (v2 != null)
            ujson.read(v2)("mentioned").num.toInt
          else
            0

          val jsonJoin = Js.Obj(
            "userID" -> v1Json("userID"),
            "count" -> v1Json("count"),
            "fav" -> v1Json("fav"),
            "hashtags" -> v1Json("hashtags"),
            "name" -> v1Json("name"),
            "mentioned" -> mention
          )
          jsonJoin.toString()

        }
    )
  }






}
