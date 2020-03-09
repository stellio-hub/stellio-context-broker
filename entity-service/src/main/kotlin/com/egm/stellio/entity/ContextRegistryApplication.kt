package com.egm.stellio.entity

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableAsync

@SpringBootApplication
@EnableAsync
class ContextRegistryApplication

fun main(args: Array<String>) {
    runApplication<ContextRegistryApplication>(*args)
}
