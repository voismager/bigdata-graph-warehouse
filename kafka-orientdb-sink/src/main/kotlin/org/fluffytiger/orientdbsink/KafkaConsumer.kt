package org.fluffytiger.orientdbsink

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.OVertex
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.logging.log4j.LogManager
import org.fluffytiger.orientdbsink.messages.VkMessage
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
class KafkaConsumer {
    @Autowired
    lateinit var connectionPool: ODatabasePool

    private val logger = LogManager.getLogger(KafkaConsumer::class.java)

    @PostConstruct
    fun createSchema() {
        connectionPool.acquire().use { db ->
            if (db.getClass("Person") == null) {
                val vClass = db.createVertexClass("Person")
                vClass.createProperty("name", OType.STRING)
                vClass.createIndex("Person_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "name")
            }

            if (db.getClass("FriendOf") == null)
                db.createEdgeClass("FriendOf")
        }
    }

    @KafkaListener(topics = ["\${vk.topic-name}"], containerFactory = "batchFactory")
    fun listen(records: List<ConsumerRecord<String, VkMessage>>, ack: Acknowledgment) {
        logger.info("Writing {} messages to db...", records.size)
        writeToDb(records)
        logger.info("Written successfully!", records.size)
        ack.acknowledge()
    }

    private fun writeToDb(records: List<ConsumerRecord<String, VkMessage>>) {
        connectionPool.acquire().use { db ->
            db.begin()

            for (record in records) {
                val message = record.value()
                val vertex: OVertex = db.newVertex(message.type)
                for ((k, v) in message.properties) {
                    vertex.setProperty(k, v)
                }
                vertex.save<OVertex>()
            }

            db.commit()
        }
    }
}