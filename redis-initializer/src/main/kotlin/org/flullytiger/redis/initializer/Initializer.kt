package org.flullytiger.redis.initializer

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.intent.OIntentMassiveRead
import org.flullytiger.redis.initializer.config.RedisProperties
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import java.util.stream.Collectors

@Service
class Initializer(
    private val connectionPool: ODatabasePool,
    private val redis: RedisTemplate<String, String>,
    private val redisProperties: RedisProperties
) : ApplicationRunner {
    private val logger = LoggerFactory.getLogger(Initializer::class.java)

    private val classQuery = "SELECT name from (select expand(classes) from metadata:schema) where superClass = 'V' or superClass = 'E'"

    override fun run(args: ApplicationArguments?) {
        connectionPool.acquire().use { db ->
            db.declareIntent(OIntentMassiveRead())

            logger.info("Starting importing ids to redis...")

            for (className in getClassNames(db)) {
                logger.info("Importing from ${className}...")

                Sequence { db.query("SELECT _id FROM $className").stream().iterator() }
                    .chunked(redisProperties.batchInsertSize)
                    .forEach { chunk ->
                        val ids = chunk.map { it.getProperty<String>("_id") }.toTypedArray()
                        redis.opsForSet().add(redisProperties.collectionName, *ids)
                        logger.info("Added chunk of ${chunk.size} ids")
                    }
            }

            db.declareIntent(null)
            logger.info("Finished!")
            logger.info("Total number of ids in redis set: ${redis.opsForSet().size(redisProperties.collectionName)}")
        }
    }

    private fun getClassNames(db: ODatabaseSession): List<String> {
        val classesRS = db.query(classQuery)

        return try {
            classesRS.stream()
                .map { it.getProperty<String>("name") }
                .collect(Collectors.toList())

        } finally {
            classesRS.close()
        }
    }
}