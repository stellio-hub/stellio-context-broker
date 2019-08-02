package com.egm.datahub.context.registry

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ContextRegistryApplication

fun main(args: Array<String>) {
	runApplication<ContextRegistryApplication>(*args)
}
