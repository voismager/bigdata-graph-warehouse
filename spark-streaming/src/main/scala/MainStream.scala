import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

case class User(id: Long, firstName: String, lastName: String)

object MainStream extends App {
  val mainStreamConf: Config = ConfigFactory.load()

  val SPARK_MASTER      = mainStreamConf.getString("mainstream.spark.master")
  val SPARK_UI_PORT     = mainStreamConf.getString("mainstream.spark.ui.port")
  val BOOTSTRAP_SERVERS = mainStreamConf.getString("kafka.bootstrap.servers")
  val MAINSTREAM_SOURCE = mainStreamConf.getString("kafka.mainstream.source")
  val MAINSTREAM_SINK   = mainStreamConf.getString("kafka.mainstream.sink")
  val FOLLOWERS_SCHEMA  = mainStreamConf.getString("schema.followers")

  val schema = DataType.fromJson(FOLLOWERS_SCHEMA)

  val spark: SparkSession = SparkSession.builder()
    .appName("mainStream")
    .master(SPARK_MASTER)
    .config("spark.ui.port", SPARK_UI_PORT)
    .getOrCreate()

  import spark.implicits._

  val followersDS: Dataset[User] = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("failOnDataLoss", "false")
    .option("subscribe", MAINSTREAM_SOURCE)
    .load()
    .deserializeAs[User]()

  followersDS
    .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", MAINSTREAM_SINK)
    .option("checkpointLocation", "/tmp/checkpoint")
    .start()
    .awaitTermination()

  spark.close()

  implicit class DataFrameOps(val df: DataFrame) extends Serializable {
    def deserializeAs[T : Encoder](): Dataset[T] = df
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("values"))
      .selectExpr("values.id", "values.first_name as firstName", "values.last_name as lastName")
      .as[T]
  }
}
