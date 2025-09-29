package com.egm.stellio.search

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.EnableAspectJAutoProxy

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.search", "com.egm.stellio.shared"])
@ConfigurationPropertiesScan("com.egm.stellio.search.common.config", "com.egm.stellio.shared.config")
@EnableAspectJAutoProxy(proxyTargetClass = true)
class SearchServiceApplication

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<SearchServiceApplication>(*args)
}
