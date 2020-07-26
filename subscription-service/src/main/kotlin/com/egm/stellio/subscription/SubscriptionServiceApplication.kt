package com.egm.stellio.subscription

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.subscription", "com.egm.stellio.shared"])
class SubscriptionServiceApplication

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<SubscriptionServiceApplication>(*args)
}
