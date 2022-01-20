import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import java.io.File
import java.util.Properties
import scala.io.Source
import scala.util.Random

object Generator extends App {
  val casualProducerConf: Config = ConfigFactory.load()

  val BOOTSTRAP_SERVERS = casualProducerConf.getString("kafka.bootstrap.servers")
  val KAFKA_SINK        = casualProducerConf.getString("kafka.sink")
  val FOLLOWERS_PATH    = casualProducerConf.getString("path.data.followers")

  val producerProps = new Properties()
  producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
  producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[Long, String](producerProps)

  val mapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  val complexJson: File = new File(FOLLOWERS_PATH)

  for (json <- complexJson.listFiles()) {
    val source = Source.fromFile(json)
    val rand = Random.nextInt(10)

    for (line <- source.getLines()) {
      val message = mapper.readValue(line, classOf[JsonNode])
      producer.send(new ProducerRecord[Long, String](KAFKA_SINK, message.toString))
//      Thread.sleep(10000)
      Thread.sleep(rand)
    }

    source.close()
  }

  producer.flush()
}
