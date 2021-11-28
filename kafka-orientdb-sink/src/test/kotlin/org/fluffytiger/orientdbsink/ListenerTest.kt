package org.fluffytiger.orientdbsink

import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.fluffytiger.orientdbsink.messages.VkMessage
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest(classes = [KafkaTestConfiguration::class])
class ListenerTest {
    @Autowired
    lateinit var producer: Producer<String, VkMessage>

    @Test
    fun works() {
    }
}