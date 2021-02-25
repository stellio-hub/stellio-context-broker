package com.egm.stellio.search

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.search", "com.egm.stellio.shared"])
class SearchServiceApplication

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    runApplication<SearchServiceApplication>(*args)
}
