package org.fluffytiger.orientdbsink

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.tx.OTransaction
import org.apache.logging.log4j.LogManager
import org.fluffytiger.orientdbsink.config.OrientDbProperties
import org.fluffytiger.orientdbsink.config.RedisProperties
import org.fluffytiger.orientdbsink.messages.VkMessage
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service


@Service
class DbWriter(
    private val connectionPool: ODatabasePool,
    private val redis: RedisTemplate<String, String>,
    private val orientDB: OrientDbProperties,
    redisProperties: RedisProperties
) {
    private val logger = LogManager.getLogger(DbWriter::class.java)

    private val collectionName = redisProperties.collectionName

    fun write(messages: List<VkMessage>) {
        logger.info("Got {} messages to write...", messages.size)

        val idsToInsert = getNonDuplicatedIdsFromRedis(messages)
        val messagesToInsert = messages.filter { idsToInsert.contains(it.id) }
        logger.info("Writing {} unique messages to db...", messagesToInsert.size)

        val type2messages = messagesToInsert.groupBy { it.typeName }

        val savedVertices =
            if (type2messages.containsKey("V")) writeVertices(type2messages.getOrDefault("V", listOf()))
            else mutableMapOf()

        if (type2messages.containsKey("E")) {
            writeEdges(type2messages.getOrDefault("E", listOf()), savedVertices)
        }

        addToRedis(idsToInsert)

        logger.info("Written successfully!")
    }

    private fun writeVertices(messages: List<VkMessage>): MutableMap<String, OVertex> {
        val savedVertices = HashMap<String, OVertex>()

        connectionPool.acquire().use { db ->
            db.declareIntent(OIntentMassiveInsert())
            db.begin(OTransaction.TXTYPE.OPTIMISTIC)

            val dbClasses = mutableMapOf<String, OClass>()

            for (message in messages) {
                if (!dbClasses.containsKey(message.className))
                    dbClasses[message.className] = db.getClass(message.className)

                val doc = db.newVertex(dbClasses[message.className])
                doc.setProperty("_id", message.id)
                for ((k, v) in message.properties) {
                    doc.setProperty(k, v)
                }
                doc.save<OVertex>()
                savedVertices[message.id] = doc
            }

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

            val dbClasses = mutableMapOf<String, OClass>()

            for (message in messages) {
                if (!dbClasses.containsKey(message.className))
                    dbClasses[message.className] = db.getClass(message.className)

                val fromId = message.properties["fromId"]!!
                val toId = message.properties["toId"]!!
                val doc = db.newEdge(cache[fromId], cache[toId], dbClasses[message.className])
                doc.save<OEdge>()
                written++
            }

            db.commit()
            db.declareIntent(null)
        }

        logger.info("Wrote {} edges", written)
    }

    private fun getNonDuplicatedIdsFromRedis(messages: List<VkMessage>): Set<String> {
        val ids = messages.map { it.id }.toSet()
        val duplicates = redis.opsForSet().intersect(collectionName, ids)!!
        return ids.filterNotTo(HashSet()) { duplicates.contains(it) }
    }

    private fun addToRedis(ids: Collection<String>) {
        redis.opsForSet().add(collectionName, *ids.toTypedArray())
        logger.info("Updated redis with ${ids.size} keys...")
    }
}