package com.egm.stellio.shared.config

import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.Option
import java.time.Duration

private const val TRANSACTION_TIMEOUT_OPTION = "transaction_timeout"

// PostgreSQL 17+ applies this timeout to the whole transaction, including implicit single-statement transactions.
fun ConnectionFactoryOptions.Builder.withTransactionTimeout(
    transactionTimeout: Duration,
    driverOptions: Option<Map<String, String>>
): ConnectionFactoryOptions.Builder =
    if (transactionTimeout.isPositive)
        option(
            driverOptions,
            mapOf(TRANSACTION_TIMEOUT_OPTION to "${transactionTimeout.toMillis()}ms")
        )
    else this
