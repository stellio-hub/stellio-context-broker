package com.egm.datahub.context.search

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class ContextSearchApplication

fun main(args: Array<String>) {
	runApplication<ContextSearchApplication>(*args)
}
