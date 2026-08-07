package com.egm.stellio.shared.config

import arrow.core.Either
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.model.InternalErrorException
import io.mockk.mockk
import io.r2dbc.spi.R2dbcTransientResourceException
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.core.Ordered
import org.springframework.dao.QueryTimeoutException
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import org.springframework.transaction.ReactiveTransaction
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.TransactionSystemException
import org.springframework.transaction.annotation.EnableTransactionManagement
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Mono

@SpringJUnitConfig(TransactionalEitherConfigurationTests.TestConfiguration::class)
class TransactionalEitherConfigurationTests @Autowired constructor(
    private val testService: TransactionalTestService,
    private val transactionManager: FailingReactiveTransactionManager
) {

    @BeforeEach
    fun resetTransactionManager() {
        transactionManager.commitFailure = null
    }

    @Test
    fun `eitherResult should preserve right result when transaction succeeds`() = runTest {
        val result = testService.eitherResult()

        assertThat(result.getOrNull()).isEqualTo("result")
    }

    @Test
    fun `eitherResult should return a timeout when transaction times out`() = runTest {
        transactionManager.commitFailure = QueryTimeoutException("statement timed out")

        val result = testService.eitherResult()

        assertThat(result.leftOrNull()).isInstanceOf(GatewayTimeoutException::class.java)
    }

    @Test
    fun `eitherResult should return a timeout when PostgreSQL cancels the query`() = runTest {
        transactionManager.commitFailure =
            R2dbcTransientResourceException("canceling statement due to statement timeout", "57014")

        val result = testService.eitherResult()

        assertThat(result.leftOrNull()).isInstanceOf(GatewayTimeoutException::class.java)
    }

    @Test
    fun `eitherResult should return an internal error when transaction fails for another reason`() = runTest {
        transactionManager.commitFailure = TransactionSystemException("commit failed")

        val result = testService.eitherResult()

        assertThat(result.leftOrNull()).isInstanceOf(InternalErrorException::class.java)
    }

    @Test
    fun `rawResult should throw the error when transaction fails`() {
        transactionManager.commitFailure = QueryTimeoutException("statement timed out")

        assertThrows<QueryTimeoutException> {
            runTest {
                testService.rawResult()
            }
        }
    }

    @Test
    fun `eitherWithStringError should throw the error when transaction fails`() {
        transactionManager.commitFailure = QueryTimeoutException("statement timed out")

        assertThrows<QueryTimeoutException> {
            runTest {
                testService.eitherWithStringError()
            }
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement(order = Ordered.LOWEST_PRECEDENCE)
    @Import(TransactionalEitherConfiguration::class)
    class TestConfiguration {

        @Bean
        fun transactionManager() = FailingReactiveTransactionManager()

        @Bean
        fun testService() = TransactionalTestService()
    }
}

open class TransactionalTestService {

    @Transactional
    open suspend fun eitherResult(): Either<APIException, String> =
        "result".right()

    @Transactional
    open suspend fun eitherWithStringError(): Either<String, String> =
        "result".right()

    @Transactional
    open suspend fun rawResult(): String =
        "result"
}

class FailingReactiveTransactionManager : ReactiveTransactionManager {
    var commitFailure: Throwable? = null

    override fun getReactiveTransaction(definition: TransactionDefinition?): Mono<ReactiveTransaction> =
        Mono.just(mockk(relaxed = true))

    override fun commit(transaction: ReactiveTransaction): Mono<Void> =
        commitFailure?.let { Mono.error(it) } ?: Mono.empty()

    override fun rollback(transaction: ReactiveTransaction): Mono<Void> =
        Mono.empty()
}
