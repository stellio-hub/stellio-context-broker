package com.egm.stellio.shared.config

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.transaction.TransactionDefinition
import reactor.core.publisher.Mono
import java.time.Duration

class DefaultTimeoutR2dbcTransactionManager(
    connectionFactory: ConnectionFactory,
    private val defaultTimeout: Duration
) : R2dbcTransactionManager(connectionFactory) {

    override fun determineTimeout(definition: TransactionDefinition): Duration =
        resolveTimeout(definition)

    override fun prepareTransactionalConnection(
        connection: Connection,
        definition: TransactionDefinition
    ): Mono<Void> =
        super.prepareTransactionalConnection(connection, definition)
            .then(applyStatementTimeout(connection, definition))

    internal fun resolveTimeout(definition: TransactionDefinition): Duration =
        if (definition.timeout != TransactionDefinition.TIMEOUT_DEFAULT)
            super.determineTimeout(definition)
        else if (defaultTimeout.isPositive)
            defaultTimeout
        else
            Duration.ZERO

    internal fun applyStatementTimeout(
        connection: Connection,
        definition: TransactionDefinition
    ): Mono<Void> {
        val timeout = resolveTimeout(definition)
        if (!timeout.isPositive)
            return Mono.empty()

        val timeoutInMillis = timeout.toMillis().coerceAtLeast(1)
        return Mono.from(connection.createStatement("SET LOCAL statement_timeout = $timeoutInMillis").execute())
            .then()
    }
}
