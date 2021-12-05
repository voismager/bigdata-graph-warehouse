package org.fluffytiger.orientdbsink

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.OVertex
import org.apache.logging.log4j.LogManager
import org.fluffytiger.orientdbsink.messages.VkMessage
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct

@Service
class DbWriter(private val connectionPool: ODatabasePool, private val redis: RedisTemplate<String, String>) {
    private val logger = LogManager.getLogger(DbWriter::class.java)

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

    fun write(messages: List<VkMessage>) {
        logger.info("Got {} messages to write...", messages.size)

        connectionPool.acquire().use { db ->
            db.begin()

            val uniqueMessages = filterNonCached(messages)
            logger.info("Writing {} messages to db...", uniqueMessages.size)

            for (message in uniqueMessages) {
                val vertex: OVertex = db.newVertex(message.type)
                for ((k, v) in message.properties) {
                    vertex.setProperty(k, v)
                }
                vertex.save<OVertex>()
            }

            this.updateCache(uniqueMessages)
            db.commit()
        }

        logger.info("Written successfully!", messages.size)
    }

    private fun filterNonCached(messages: List<VkMessage>): List<VkMessage> {
        val unique = HashSet(messages).toList()

        return redis.opsForValue().multiGet(unique.map { it.id })!!
            .withIndex()
            .filter { it.value == null }
            .map { messages[it.index] }
    }

    private fun updateCache(messages: List<VkMessage>) {
        logger.info("Updating cache with ${messages.size} keys...")
        redis.opsForValue().multiSet(messages.associate { it.id to "" })
    }
}