package org.fluffytiger.orientdbsink

import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.fluffytiger.orientdbsink.messages.VkMessage
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

@TestConfiguration
class KafkaTestConfiguration {
    @Bean(name = ["kafkaContainer"], initMethod = "start", destroyMethod = "stop")
    fun kafkaContainer(): KafkaContainer {
        return KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("7.0.0"))
    }

    @Bean
    @Primary
    fun testKafkaConsumerFactory(container: KafkaContainer, valueDeserializer: Deserializer<VkMessage>): ConsumerFactory<String, VkMessage> {
        val properties = KafkaProperties()
        properties.bootstrapServers = listOf(container.bootstrapServers)
        properties.consumer.groupId = "vk_data_consumers"
        properties.consumer.enableAutoCommit = false
        properties.consumer.autoOffsetReset = "earliest"

        return DefaultKafkaConsumerFactory(
            properties.buildConsumerProperties(),
            StringDeserializer(),
            valueDeserializer
        )
    }

    @Bean
    fun testKafkaProducer(container: KafkaContainer): Producer<String, VkMessage> {
        val configProps = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to container.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java
        )

        return DefaultKafkaProducerFactory<String, VkMessage>(configProps).createProducer()
    }
}