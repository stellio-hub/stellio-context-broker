package com.egm.datahub.context.registry.config

import org.springframework.context.annotation.Configuration
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.util.StringUtils.arrayToCommaDelimitedString
import org.apache.kafka.clients.admin.AdminClientConfig
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.support.converter.StringJsonMessageConverter
import org.springframework.kafka.support.converter.RecordMessageConverter

@Configuration
@EnableKafka
class KafkaConfig {

    @Bean
    fun entitiesTopic(): NewTopic {
        return NewTopic("entities", 2, 1.toShort())
    }
}