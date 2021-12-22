package org.fluffytiger.orientdbsink.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "vk.redis")
data class RedisProperties(
    val hostname: String,
    val port: Int,
    val collectionName: String
)