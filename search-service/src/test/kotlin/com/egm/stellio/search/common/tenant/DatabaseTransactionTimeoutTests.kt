package com.egm.stellio.search.common.tenant

import arrow.core.Either
import arrow.core.right
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.model.InternalErrorException
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import org.springframework.transaction.annotation.Transactional

@SpringBootTest(properties = ["application.transaction-timeout=100ms"])
@ActiveProfiles("test")
@Import(DatabaseTransactionTimeoutTests.TimeoutTestConfiguration::class)
class DatabaseTransactionTimeoutTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var databaseTimeoutTestService: DatabaseTimeoutTestService

    @Test
    fun `runSlowQuery should return a gateway timeout when PostgreSQL cancels the statement`() = runTest {
        val result = databaseTimeoutTestService.runSlowQuery()

        assertThat(result.leftOrNull()).isInstanceOf(GatewayTimeoutException::class.java)
    }

    @Test
    fun `runInvalidQuery should return an internal error when database request fails`() = runTest {
        val result = databaseTimeoutTestService.runInvalidQuery()

        assertThat(result.leftOrNull()).isInstanceOf(InternalErrorException::class.java)
    }

    @TestConfiguration(proxyBeanMethods = false)
    class TimeoutTestConfiguration {

        @Bean
        fun databaseTimeoutTestService(databaseClient: DatabaseClient) =
            DatabaseTimeoutTestService(databaseClient)
    }
}

open class DatabaseTimeoutTestService(
    private val databaseClient: DatabaseClient
) {

    @Transactional
    open suspend fun runSlowQuery(): Either<APIException, Unit> {
        databaseClient.sql("SELECT pg_sleep(1)")
            .then()
            .awaitSingleOrNull()
        return Unit.right()
    }

    @Transactional
    open suspend fun runInvalidQuery(): Either<APIException, Unit> {
        databaseClient.sql("FAILED REQUEST")
            .then()
            .awaitSingleOrNull()
        return Unit.right()
    }
}
