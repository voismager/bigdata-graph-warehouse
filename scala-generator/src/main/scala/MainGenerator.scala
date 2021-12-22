import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, concat, from_json, lit}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

import scala.util.Properties

object MainGenerator extends App {
  val mainStreamConf: Config = ConfigFactory.load()

  val SPARK_MASTER = Properties.envOrElse("SPARK_MASTER", mainStreamConf.getString("spark.master"))
  val SPARK_UI_PORT = Properties.envOrElse("SPARK_UI_PORT", mainStreamConf.getString("spark.ui.port"))
  val BOOTSTRAP_SERVERS = Properties.envOrElse("BOOTSTRAP_SERVERS", mainStreamConf.getString("kafka.bootstrap.servers"))
  val FRIENDS_SINK = Properties.envOrElse("FRIENDS_SINK", mainStreamConf.getString("kafka.friends.sink"))
  val PROFILES_SINK = Properties.envOrElse("PROFILES_SINK", mainStreamConf.getString("kafka.profiles.sink"))

  val timestamp = System.currentTimeMillis / 1000

  val spark: SparkSession = SparkSession.builder()
    .appName("Generator")
    .master(SPARK_MASTER)
    .config("spark.ui.port", SPARK_UI_PORT)
    .getOrCreate()

  val schemaFriends = DataType.fromJson(mainStreamConf.getString("schema.friends")).asInstanceOf[StructType]
  val schemaProfiles = DataType.fromJson(mainStreamConf.getString("schema.profiles")).asInstanceOf[StructType]

  val profiles = spark.readStream
    .schema(schemaProfiles)
    .json("src/main/resources/user_profiles")
    .select("id", "first_name", "last_name")
    .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", PROFILES_SINK)
    .option("checkpointLocation", s"tmp/${timestamp}_profiles")

  val friends = spark.readStream
    .schema(schemaFriends)
    .json("src/main/resources/friends")
    .select("user_id", "friend_id")
    .withColumn("id", concat(col("user_id"), lit('.'), col("friend_id")))
    .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", FRIENDS_SINK)
    .option("checkpointLocation", s"tmp/${timestamp}_friends")

  friends.start()
  profiles.start()
    .awaitTermination()

  spark.close()
}
