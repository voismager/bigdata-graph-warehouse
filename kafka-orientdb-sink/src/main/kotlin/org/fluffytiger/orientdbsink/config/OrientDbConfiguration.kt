package org.fluffytiger.orientdbsink.config

import com.orientechnologies.orient.core.config.OGlobalConfiguration
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
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.TX_USE_LOG, false)
                .addConfig(OGlobalConfiguration.WAL_SYNC_ON_PAGE_FLUSH, false)
                .build()
        )
    }

    @Bean
    fun orientDbSessionPool(orientDB: OrientDB, properties: OrientDbProperties): ODatabasePool {
        orientDB.createIfNotExists(properties.dbName, ODatabaseType.PLOCAL)
        return orientDB.cachedPool(properties.dbName, properties.username, properties.password)
    }
}