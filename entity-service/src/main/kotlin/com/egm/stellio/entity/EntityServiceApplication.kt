package com.egm.stellio.entity

import com.egm.stellio.entity.config.ApplicationProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.Executor

const val CORE_POOL_SIZE = 50
const val MAX_POOL_SIZE = 100
const val QUEUE_CAPACITY = 500

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.entity", "com.egm.stellio.shared"])
@EnableConfigurationProperties(ApplicationProperties::class)
@EnableAsync
class EntityServiceApplication

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<EntityServiceApplication>(*args)
}

@Bean
fun taskExecutor(): Executor {
    val executor = ThreadPoolTaskExecutor().apply {
        corePoolSize = CORE_POOL_SIZE
        maxPoolSize = MAX_POOL_SIZE
        setQueueCapacity(QUEUE_CAPACITY)
        setThreadNamePrefix("entity-executor-")
    }
    executor.initialize()
    return executor
}
