package org.flullytiger.redis.initializer

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.ODatabaseSession
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import java.util.stream.Collectors
import javax.annotation.PostConstruct

@Service
class Initializer(private val connectionPool: ODatabasePool, private val redis: RedisTemplate<String, String>) {
    private val classQuery = "SELECT name from (select expand(classes) from metadata:schema) where superClass = 'V' or superClass = 'E'"

    @PostConstruct
    fun init() {
        connectionPool.acquire().use { db ->
            getClassNames(db)

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