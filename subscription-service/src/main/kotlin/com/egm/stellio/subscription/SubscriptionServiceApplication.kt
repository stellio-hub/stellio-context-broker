package com.egm.stellio.subscription

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.subscription", "com.egm.stellio.shared"])
@ConfigurationPropertiesScan("com.egm.stellio.subscription.config", "com.egm.stellio.shared.config")
@EnableScheduling
class SubscriptionServiceApplication

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<SubscriptionServiceApplication>(*args)
}
