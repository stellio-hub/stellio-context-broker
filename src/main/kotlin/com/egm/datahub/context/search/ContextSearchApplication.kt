package com.egm.datahub.context.search

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties
@ConfigurationPropertiesScan(basePackages = ["com.egm.datahub.context.search.config"])
class ContextSearchApplication

fun main(args: Array<String>) {
    runApplication<ContextSearchApplication>(*args)
}
