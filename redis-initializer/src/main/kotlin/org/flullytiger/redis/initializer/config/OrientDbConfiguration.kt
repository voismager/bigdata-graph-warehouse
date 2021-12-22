package org.flullytiger.redis.initializer.config

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OrientDbConfiguration {
    @Bean
    fun orientDb(properties: OrientDbProperties): OrientDB {
        return OrientDB(
            properties.url,
            properties.username,
            properties.password,
            OrientDBConfig.defaultConfig()
        )
    }

    @Bean
    fun orientDbSessionPool(orientDB: OrientDB, properties: OrientDbProperties): ODatabasePool {
        if (!orientDB.exists(properties.dbName))
            throw IllegalStateException("Db doesn't exist!")

        return orientDB.cachedPool(properties.dbName, properties.username, properties.password)
    }
}