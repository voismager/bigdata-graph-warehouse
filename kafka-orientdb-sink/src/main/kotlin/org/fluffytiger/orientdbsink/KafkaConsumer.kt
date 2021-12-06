package org.fluffytiger.orientdbsink

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.logging.log4j.LogManager
import org.fluffytiger.orientdbsink.messages.VkMessage
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class KafkaConsumer(private val dbWriter: DbWriter) {
    @KafkaListener(topics = ["\${vk.topic-name}"], containerFactory = "batchFactory")
    fun listen(records: List<ConsumerRecord<String, VkMessage>>, ack: Acknowledgment) {
        dbWriter.write(records.map { it.value() })
        ack.acknowledge()
    }
}