package org.fluffytiger.orientdbsink

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.tx.OTransaction
import org.apache.logging.log4j.LogManager
import org.fluffytiger.orientdbsink.config.OrientDbProperties
import org.fluffytiger.orientdbsink.messages.VkMessage
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service


@Service
class DbWriter(
    private val connectionPool: ODatabasePool,
    private val redis: RedisTemplate<String, String>,
    private val orientDB: OrientDbProperties
) {
    private val logger = LogManager.getLogger(DbWriter::class.java)

    fun write(messages: List<VkMessage>) {
        logger.info("Got {} messages to write...", messages.size)

        val type2messages = messages.groupBy { it.typeName }

        val savedVertices =
            if (type2messages.containsKey("V")) writeVertices(type2messages.getOrDefault("V", listOf()))
            else mutableMapOf()

        if (type2messages.containsKey("E")) {
            writeEdges(type2messages.getOrDefault("E", listOf()), savedVertices)
        }

        logger.info("Written successfully!", messages.size)
    }

    private fun writeVertices(messages: List<VkMessage>): MutableMap<String, OVertex> {
        val savedVertices = HashMap<String, OVertex>()

        connectionPool.acquire().use { db ->
            db.declareIntent(OIntentMassiveInsert())
            db.begin(OTransaction.TXTYPE.OPTIMISTIC)

            //val uniqueMessages = filterNonCached(messages)
            //logger.info("Writing {} messages to db...", uniqueMessages.size)

            for (message in messages) {
                val doc = db.newVertex(message.className)
                doc.setProperty("_id", message.id)
                for ((k, v) in message.properties) {
                    doc.setProperty(k, v)
                }
                doc.save<OVertex>(message.className.lowercase() + orientDB.clusterSuffix)
                savedVertices[message.id] = doc
            }

            //this.updateCache(uniqueMessages)
            db.commit()
            db.declareIntent(null)
        }

        logger.info("Wrote {} vertices", savedVertices.size)
        return savedVertices
    }

    private fun writeEdges(messages: List<VkMessage>, cache: MutableMap<String, OVertex>) {
        val offCacheIds = messages
            .flatMap { listOf(it.properties["fromId"]!!, it.properties["toId"]!!) }
            .filter { !cache.containsKey(it) }

        connectionPool.acquire().use { db ->
            val inStatement = offCacheIds.joinToString(separator = ",") { "'$it'" }
            val result = db.query("SELECT FROM User WHERE _id in [$inStatement]")
            while (result.hasNext()) {
                val vertex = result.next().vertex.get()
                cache[vertex.getProperty("_id")] = vertex
            }
            logger.info("Added {} vertices to cache", offCacheIds.size)
        }

        var written = 0

        connectionPool.acquire().use { db ->
            db.declareIntent(OIntentMassiveInsert())
            db.begin(OTransaction.TXTYPE.OPTIMISTIC)

            for (message in messages) {
                val fromId = message.properties["fromId"]!!
                val toId = message.properties["toId"]!!
                val doc = db.newEdge(cache[fromId], cache[toId], message.className)
                doc.save<OEdge>(message.className.lowercase() + orientDB.clusterSuffix)
                written++
            }

            db.commit()
            db.declareIntent(null)
        }

        logger.info("Wrote {} edges", written)
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