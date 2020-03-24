package com.egm.stellio.entity

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableAsync

@SpringBootApplication
@EnableAsync
class EntityServiceApplication

fun main(args: Array<String>) {
    runApplication<EntityServiceApplication>(*args)
}
