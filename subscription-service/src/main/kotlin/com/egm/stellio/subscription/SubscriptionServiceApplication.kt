package com.egm.stellio.subscription

import com.egm.stellio.subscription.config.ApplicationProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.subscription", "com.egm.stellio.shared"])
@EnableConfigurationProperties(ApplicationProperties::class)
class SubscriptionServiceApplication

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<SubscriptionServiceApplication>(*args)
}
