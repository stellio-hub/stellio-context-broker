package com.egm.stellio.search

import com.egm.stellio.search.config.ApplicationProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.search", "com.egm.stellio.shared"])
@EnableConfigurationProperties(value = [ApplicationProperties::class])
class SearchServiceApplication

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<SearchServiceApplication>(*args)
}
