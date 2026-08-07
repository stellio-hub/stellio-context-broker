package com.egm.stellio.shared.config

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Result
import io.r2dbc.spi.Statement
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.transaction.support.DefaultTransactionDefinition
import reactor.core.publisher.Mono
import java.time.Duration

class DefaultTimeoutR2dbcTransactionManagerTests {

    @Test
    fun `resolveTimeout should return configured timeout when transaction does not define one`() {
        val transactionManager = transactionManager(Duration.ofMinutes(1))

        assertThat(transactionManager.resolveTimeout(DefaultTransactionDefinition()))
            .isEqualTo(Duration.ofMinutes(1))
    }

    @Test
    fun `resolveTimeout should return explicit timeout when transaction defines one`() {
        val transactionManager = transactionManager(Duration.ofMinutes(1))
        val definition = DefaultTransactionDefinition().apply { timeout = 10 }

        assertThat(transactionManager.resolveTimeout(definition))
            .isEqualTo(Duration.ofSeconds(10))
    }

    @Test
    fun `resolveTimeout should disable default timeout when configured timeout is non-positive`() {
        val definition = DefaultTransactionDefinition()

        assertThat(transactionManager(Duration.ZERO).resolveTimeout(definition))
            .isEqualTo(Duration.ZERO)
        assertThat(transactionManager(Duration.ofSeconds(-1)).resolveTimeout(definition))
            .isEqualTo(Duration.ZERO)
    }

    @Test
    fun `applyStatementTimeout should execute statement when timeout is configured`() {
        val connection = mockk<Connection>()
        val statement = mockk<Statement>()
        everyStatement(connection, statement, 60_000)

        transactionManager(Duration.ofMinutes(1))
            .applyStatementTimeout(connection, DefaultTransactionDefinition())
            .block()

        verify(exactly = 1) { statement.execute() }
    }

    @Test
    fun `applyStatementTimeout should not execute statement when timeout is disabled`() {
        val connection = mockk<Connection>()

        transactionManager(Duration.ZERO)
            .applyStatementTimeout(connection, DefaultTransactionDefinition())
            .block()

        verify(exactly = 0) { connection.createStatement(any()) }
    }

    private fun everyStatement(connection: Connection, statement: Statement, timeout: Long) {
        every {
            connection.createStatement("SET LOCAL statement_timeout = $timeout")
        } returns statement
        every { statement.execute() } returns Mono.empty<Result>()
    }

    private fun transactionManager(timeout: Duration) =
        DefaultTimeoutR2dbcTransactionManager(mockk<ConnectionFactory>(), timeout)
}
