import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.sources.In
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

case class Person(id: String, firstName: String, lastName: String)
case class Friend(linkId: String, userId: String, friendId: String)

case class Message(id: String, typeName: String, className: String, properties: Map[String, String])

object MainStream extends App {
  val mainStreamConf: Config = ConfigFactory.load()

  val SPARK_MASTER      = mainStreamConf.getString("mainstream.spark.master")
  val SPARK_UI_PORT     = mainStreamConf.getString("mainstream.spark.ui.port")
  val BOOTSTRAP_SERVERS = mainStreamConf.getString("kafka.bootstrap.servers")
  val FRIENDS_SOURCE    = mainStreamConf.getString("kafka.friends.source")
  val FRIENDS_SINK      = mainStreamConf.getString("kafka.friends.sink")
  val PROFILES_SOURCE   = mainStreamConf.getString("kafka.profiles.source")
  val PROFILES_SINK     = mainStreamConf.getString("kafka.profiles.sink")

  val schemaFriends: DataType = DataType.fromJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"user_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"friend_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}")
  val schemaProfiles: DataType = DataType.fromJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"first_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"last_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}")

  val spark: SparkSession = SparkSession.builder()
    .appName("mainStream")
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

  profiles.map(profile => Message(profile.id, "V", "Person", Map("name" -> s"${profile.firstName} ${profile.lastName}")))
    .writeToKafka("vk_data", "/tmp/chckpnt/test1")
    .start()

  join.map(friend => Message(friend.friendId, "E", "FriendOf", Map("fromId" -> friend.userId, "toId" -> friend.friendId)))
    .writeToKafka("vk_data", "/tmp/chckpnt/test2")
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
      .option("checkpointLocation", checkpoint)

    def deserializeAs[T : Encoder](schema: DataType, exprs: String*): Dataset[T] = df
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("values"))
      .selectExpr(exprs : _*)
      .as[T]
  }
}