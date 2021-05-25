package com.egm.stellio.entity

import com.egm.stellio.entity.config.ApplicationProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableAsync

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.entity", "com.egm.stellio.shared"])
@EnableConfigurationProperties(ApplicationProperties::class)
@EnableAsync
class EntityServiceApplication

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<EntityServiceApplication>(*args)
}
