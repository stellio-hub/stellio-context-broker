package com.egm.datahub.context.registry.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("graphdb")
class GraphdbProperties {
    lateinit var url: String
    lateinit var username: String
    lateinit var password: String
}
