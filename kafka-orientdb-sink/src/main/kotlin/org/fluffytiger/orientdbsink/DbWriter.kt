package org.fluffytiger.orientdbsink

import com.fasterxml.jackson.databind.ObjectMapper
import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.tx.OTransaction
import org.apache.logging.log4j.LogManager
import org.fluffytiger.orientdbsink.messages.VkMessage
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct

@Service
class DbWriter(private val connectionPool: ODatabasePool, private val redis: RedisTemplate<String, String>) {
    private val logger = LogManager.getLogger(DbWriter::class.java)

    private val objectMapper = ObjectMapper()

    @PostConstruct
    fun createSchema() {
        connectionPool.acquire().use { db ->
            if (db.getClass("Vk_Obj") == null) {
                val vClass = db.createVertexClass("Vk_Obj")
                vClass.createProperty("_id", OType.STRING)
                vClass.createProperty("_type", OType.STRING)
                vClass.createIndex("Vk_Obj_id_index", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "_id")
            }

            if (db.getClass("Vk_Rel") == null) {
                val eClass = db.createEdgeClass("Vk_Rel")
                eClass.createProperty("_id", OType.STRING)
                eClass.createProperty("_type", OType.STRING)
                eClass.createIndex("Vk_Rel_id_index", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "_id")
            }
        }
    }

    fun write(messages: List<VkMessage>) {
        logger.info("Got {} messages to write...", messages.size)

        connectionPool.acquire().use { db ->
            db.begin(OTransaction.TXTYPE.OPTIMISTIC)

            val uniqueMessages = filterNonCached(messages)
            logger.info("Writing {} messages to db...", uniqueMessages.size)

            for (message in uniqueMessages) {
                if (message.type == "V") {
                    writeVertex(db, message)
                } else if (message.type == "E") {
                    writeEdge(db, message)
                }
            }

            this.updateCache(uniqueMessages)
            db.commit()
        }

        logger.info("Written successfully!", messages.size)
    }

    private fun writeVertex(db: ODatabaseSession, message: VkMessage) {
        val properties = mutableMapOf<String, String>()
        properties.putAll(message.properties)
        properties["_id"] = message.id
        properties["_type"] = message.className

        val statement = "CREATE VERTEX Vk_Obj CONTENT ${objectMapper.writeValueAsString(properties)}"

        db.execute("sql", statement).close()
    }

    private fun writeEdge(db: ODatabaseSession, message: VkMessage) {
        val fromId = message.properties["fromId"]!!
        val toId = message.properties["toId"]!!

        val statement = "CREATE EDGE Vk_Rel " +
            "FROM (SELECT FROM Vk_Obj WHERE _id = ?) " +
            "TO (SELECT FROM Vk_Obj WHERE _id = ?) " +
            "SET _id = ?, _type = ?"

        db.execute("sql", statement, fromId, toId, message.id, message.className).close()
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