package org.fluffytiger.orientdbsink

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.fluffytiger.orientdbsink.messages.VkMessage
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerContainerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.JsonDeserializer


@Configuration
class KafkaConfiguration {
    @Value("\${vk.topic-name}")
    lateinit var topicName: String

    @Bean
    fun vkTopic(): NewTopic {
        return TopicBuilder.name(topicName)
            .partitions(1)
            .replicas(1)
            .build()
    }

    @Bean
    fun objectMapper(): ObjectMapper {
        return jacksonObjectMapper()
    }

    @Bean
    fun valueDeserializer(objectMapper: ObjectMapper): Deserializer<VkMessage> {
        val deserializer = JsonDeserializer<VkMessage>()
        deserializer.addTrustedPackages("org.fluffytiger.orientdbsink.messages")
        deserializer.setTypeFunction { _, _ -> objectMapper.constructType(VkMessage::class.java) }
        return deserializer
    }

    @Bean
    fun kafkaConsumerFactory(properties: KafkaProperties, valueDeserializer: Deserializer<VkMessage>): ConsumerFactory<String, VkMessage> {
        return DefaultKafkaConsumerFactory(
            properties.buildConsumerProperties(),
            StringDeserializer(),
            valueDeserializer
        )
    }

    @Bean
    fun batchFactory(consumerFactory: ConsumerFactory<String, VkMessage>): KafkaListenerContainerFactory<*> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, VkMessage>()
        factory.consumerFactory = consumerFactory
        factory.isBatchListener = true
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        return factory
    }
}