package com.egm.datahub.context.registry.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka

@Configuration
@EnableKafka
class KafkaConfig {

    @Bean
    fun entitiesTopic(): NewTopic {
        return NewTopic("entities", 2, 1.toShort())
    }

    @Bean
    fun observationsTopic(): NewTopic = NewTopic("cim.observations", 1, 1.toShort())
}
