package com.egm.stellio.search

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.search", "com.egm.stellio.shared"])
@ConfigurationPropertiesScan("com.egm.stellio.search.common.config", "com.egm.stellio.shared.config")
@EnableScheduling
class SearchServiceApplication

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<SearchServiceApplication>(*args)
}
