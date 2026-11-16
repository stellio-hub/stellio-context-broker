package com.egm.stellio.shared.config

import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.Option
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

class ConnectionFactoryOptionsExtensionsTests {

    private val driverOptions = Option.valueOf<Map<String, String>>("options")

    @Test
    fun `withTransactionTimeout should configure a positive timeout in milliseconds`() {
        val options = ConnectionFactoryOptions.builder()
            .withTransactionTimeout(Duration.ofMillis(1500), driverOptions)
            .build()

        assertThat(options.getValue(driverOptions))
            .isEqualTo(mapOf("transaction_timeout" to "1500ms"))
    }

    @Test
    fun `withTransactionTimeout should not configure a non-positive timeout`() {
        val options = ConnectionFactoryOptions.builder()
            .withTransactionTimeout(Duration.ZERO, driverOptions)
            .build()

        assertThat(options.hasOption(driverOptions)).isFalse()
    }
}
