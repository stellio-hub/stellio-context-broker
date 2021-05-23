package com.egm.stellio.subscription

import com.egm.stellio.subscription.config.ApplicationProperties
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
@EnableConfigurationProperties(ApplicationProperties::class)
class SubscriptionServiceApplicationTests {

    @Test
    @Suppress("EmptyFunctionBlock")
    fun contextLoads() {
    }
}
