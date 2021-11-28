package org.fluffytiger.orientdbsink

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OrientDbConfiguration {
    @Value("\${vk.orientdb.url}") lateinit var url: String
    @Value("\${vk.orientdb.dbName}") lateinit var dbName: String
    @Value("\${vk.orientdb.username}") lateinit var username: String
    @Value("\${vk.orientdb.password}") lateinit var password: String

    @Bean
    fun orientDb(): OrientDB {
        return OrientDB(url, username, password, OrientDBConfig.defaultConfig())
    }

    @Bean
    fun orientDbSessionPool(orientDB: OrientDB): ODatabasePool {
        orientDB.createIfNotExists(dbName, ODatabaseType.PLOCAL)
        return orientDB.cachedPool(dbName, username, password)
    }
}