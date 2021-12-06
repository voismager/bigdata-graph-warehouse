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
        orientDB.createIfNotExists(properties.dbName, ODatabaseType.PLOCAL)
        return orientDB.cachedPool(properties.dbName, properties.username, properties.password)
    }
}