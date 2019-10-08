package com.egm.datahub.context.registry.api

import com.egm.datahub.context.registry.IntegrationTestsBase
import com.intuit.karate.junit5.Karate
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class ApiTests : IntegrationTestsBase() {

    @Karate.Test
    fun testAll(): Karate {
        return Karate().relativeTo(this::class.java)
            .tags("~@ignore")
    }
}
