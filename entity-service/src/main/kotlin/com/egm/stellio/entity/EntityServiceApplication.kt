package com.egm.stellio.entity

import com.egm.stellio.entity.config.ApplicationProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.Executor

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.entity", "com.egm.stellio.shared"])
@EnableConfigurationProperties(ApplicationProperties::class)
@EnableAsync
class EntityServiceApplication

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<EntityServiceApplication>(*args)
}

@Bean
fun taskExecutor(
    applicationProperties: ApplicationProperties
): Executor {
    val executor = ThreadPoolTaskExecutor().apply {
        corePoolSize = applicationProperties.eventsThreadPool.corePoolSize
        maxPoolSize = applicationProperties.eventsThreadPool.maxPoolSize
        setThreadNamePrefix("entity-executor-")
    }
    executor.initialize()
    return executor
}
