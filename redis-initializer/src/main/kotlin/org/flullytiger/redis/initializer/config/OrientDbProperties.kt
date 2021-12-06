package org.flullytiger.redis.initializer.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "vk.orientdb")
data class OrientDbProperties(
    val url: String,
    val dbName: String,
    val username: String,
    val password: String
)