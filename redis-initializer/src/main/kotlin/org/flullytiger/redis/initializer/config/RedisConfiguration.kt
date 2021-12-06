package org.flullytiger.redis.initializer.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisStandaloneConfiguration
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory

@Configuration
class RedisConfiguration {
    @Bean
    fun jedisConnectionFactory(properties: RedisProperties): JedisConnectionFactory {
        val standaloneConfig = RedisStandaloneConfiguration(properties.hostname, properties.port)

        return JedisConnectionFactory(standaloneConfig)
    }
}