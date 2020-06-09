package com.egm.stellio.entity.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.ApplicationEventMulticaster
import org.springframework.context.event.SimpleApplicationEventMulticaster
import org.springframework.core.task.SimpleAsyncTaskExecutor

@Configuration
class AsynchronousEventsConfig {

    @Bean(name = ["applicationEventMulticaster"])
    fun simpleApplicationEventMulticaster(): ApplicationEventMulticaster {
        val eventMulticaster = SimpleApplicationEventMulticaster()

        eventMulticaster.setTaskExecutor(SimpleAsyncTaskExecutor())
        return eventMulticaster
    }
}
