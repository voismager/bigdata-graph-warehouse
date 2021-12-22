import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.sources.In
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

import scala.util.Properties

case class Person(id: String, firstName: String, lastName: String)
case class Friend(linkId: String, userId: String, friendId: String)

case class Message(id: String, typeName: String, className: String, properties: Map[String, String])

object MainStream extends App {
  val mainStreamConf: Config = ConfigFactory.load()

  val SPARK_MASTER      = Properties.envOrElse("SPARK_MASTER", mainStreamConf.getString("mainstream.spark.master"))
  val SPARK_UI_PORT     = Properties.envOrElse("SPARK_UI_PORT", mainStreamConf.getString("mainstream.spark.ui.port"))
  val BOOTSTRAP_SERVERS = Properties.envOrElse("BOOTSTRAP_SERVERS", mainStreamConf.getString("kafka.bootstrap.servers"))
  val FRIENDS_SOURCE    = Properties.envOrElse("FRIENDS_SOURCE", mainStreamConf.getString("kafka.friends.source"))
  val FRIENDS_SINK      = Properties.envOrElse("FRIENDS_SINK", mainStreamConf.getString("kafka.friends.sink"))
  val PROFILES_SOURCE   = Properties.envOrElse("PROFILES_SOURCE", mainStreamConf.getString("kafka.profiles.source"))
  val PROFILES_SINK     = Properties.envOrElse("PROFILES_SINK", mainStreamConf.getString("kafka.profiles.sink"))

  val schemaProfiles = DataType.fromJson(mainStreamConf.getString("schema.profiles")).asInstanceOf[StructType]
  val schemaFriends = DataType.fromJson(mainStreamConf.getString("schema.friends")).asInstanceOf[StructType]

  val timestamp = System.currentTimeMillis / 1000

  val spark: SparkSession = SparkSession.builder()
    .appName("SparkApp")
    .master(SPARK_MASTER)
    .config("spark.ui.port", SPARK_UI_PORT)
    .getOrCreate()

  import spark.implicits._

  val profiles: Dataset[Person] = readFromKafka(spark, PROFILES_SOURCE)
    .deserializeAs[Person](schemaProfiles, "values.id", "values.first_name as firstName", "values.last_name as lastName")

  val friends: Dataset[Friend] = readFromKafka(spark, FRIENDS_SOURCE)
    .deserializeAs[Friend](schemaFriends, "values.id as linkId", "values.user_id as userId", "values.friend_id as friendId")

  val join: Dataset[Friend] = (friends join profiles)
    .where(friends("friendId") === profiles("id"))
    .select("linkId", "userId", "friendId")
    .join(profiles)
    .where(col("userId") === profiles("id"))
    .select("linkId", "userId", "friendId")
    .as[Friend]

  profiles.map(profile => Message(profile.id, "V", "User", Map("name" -> s"${profile.firstName} ${profile.lastName}")))
    .writeToKafka("vk_data", "Person")
    .start()

  join.map(friend => Message(friend.linkId, "E", "FriendOf", Map("fromId" -> friend.userId, "toId" -> friend.friendId)))
    .dropDuplicates("id")
    .writeToKafka("vk_data", "FriendOf")
    .start()
    .awaitTermination()

  spark.close()

  def readFromKafka(sparkSession: SparkSession, topic: String): DataFrame = sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .option("subscribe", topic)
    .load()

  implicit class DataFrameOps[K](val df: Dataset[K]) extends Serializable {
    def writeToKafka(topic: String, checkpoint: String): DataStreamWriter[Row] = df
      .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("topic", topic)
      .option("checkpointLocation", s"tmp/${timestamp}_${topic}_$checkpoint")

    def deserializeAs[T : Encoder](schema: DataType, exprs: String*): Dataset[T] = df
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("values"))
      .selectExpr(exprs : _*)
      .as[T]
  }
}
