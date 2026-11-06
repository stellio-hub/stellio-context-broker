package com.egm.stellio.shared.config

import arrow.core.Either
import arrow.core.left
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.ErrorMessages.Database.DATABASE_TIMEOUT_MESSAGE
import com.egm.stellio.shared.util.ErrorMessages.Database.DATABASE_TRANSACTION_FAILURE_MESSAGE
import io.r2dbc.spi.R2dbcException
import io.r2dbc.spi.R2dbcTimeoutException
import kotlinx.coroutines.CancellationException
import org.aopalliance.intercept.MethodInterceptor
import org.aopalliance.intercept.MethodInvocation
import org.slf4j.LoggerFactory
import org.springframework.aop.support.AopUtils
import org.springframework.aop.support.DefaultPointcutAdvisor
import org.springframework.aop.support.StaticMethodMatcherPointcut
import org.springframework.beans.factory.config.BeanDefinition
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Role
import org.springframework.core.Ordered
import org.springframework.core.ResolvableType
import org.springframework.dao.DataAccessException
import org.springframework.dao.QueryTimeoutException
import org.springframework.transaction.TransactionException
import org.springframework.transaction.TransactionTimedOutException
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource
import reactor.core.publisher.Mono
import java.lang.reflect.Method
import kotlin.coroutines.Continuation
import kotlin.reflect.jvm.kotlinFunction

@Configuration(proxyBeanMethods = false)
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
class TransactionalEitherConfiguration {

    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    fun transactionalEitherAdvisor(): DefaultPointcutAdvisor =
        DefaultPointcutAdvisor(TransactionalEitherPointcut, TransactionalEitherInterceptor()).apply {
            order = Ordered.HIGHEST_PRECEDENCE
        }

    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    fun transactionalEitherRollbackAdvisor(): DefaultPointcutAdvisor =
        DefaultPointcutAdvisor(TransactionalEitherPointcut, TransactionalEitherRollbackInterceptor()).apply {
            order = Ordered.LOWEST_PRECEDENCE
        }

    companion object {
        // Error conversion wraps Spring's transaction advice; Left detection must run inside it, before commit.
        const val TRANSACTION_ADVISOR_ORDER = Ordered.LOWEST_PRECEDENCE - 1
    }
}

internal object TransactionalEitherPointcut : StaticMethodMatcherPointcut() {
    private val transactionAttributeSource = AnnotationTransactionAttributeSource()

    override fun matches(method: Method, targetClass: Class<*>): Boolean {
        if (transactionAttributeSource.getTransactionAttribute(method, targetClass) == null)
            return false

        val specificMethod = AopUtils.getMostSpecificMethod(method, targetClass)
        return specificMethod.returnsApiExceptionEither() || method.returnsApiExceptionEither()
    }

    private fun Method.returnsApiExceptionEither(): Boolean =
        kotlinFunction?.returnType?.let { returnType ->
            returnType.classifier == Either::class &&
                returnType.arguments.firstOrNull()?.type?.classifier == APIException::class
        } ?: ResolvableType.forMethodReturnType(this).let { returnType ->
            returnType.resolve() == Either::class.java &&
                returnType.getGeneric(0).resolve() == APIException::class.java
        }
}

internal class TransactionalEitherInterceptor : MethodInterceptor {

    @Suppress("TooGenericExceptionCaught")
    override fun invoke(invocation: MethodInvocation): Any? {
        wrapContinuation(invocation)

        return try {
            invocation.proceed()
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: Exception) {
            exception.toTransactionAPIException().left()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun wrapContinuation(invocation: MethodInvocation) {
        val continuationIndex = invocation.arguments.lastIndex
        val continuation = invocation.arguments.getOrNull(continuationIndex) as? Continuation<Any?> ?: return

        invocation.arguments[continuationIndex] = object : Continuation<Any?> {
            override val context = continuation.context

            override fun resumeWith(result: Result<Any?>) {
                continuation.resumeWith(
                    if (result.exceptionOrNull() is CancellationException) result else result.recover { exception ->
                        exception.toTransactionAPIException().left()
                    }
                )
            }
        }
    }

    private fun Throwable.toTransactionAPIException(): APIException {
        val causes = causes()
        val apiException = causes.filterIsInstance<APIException>().firstOrNull()

        return when {
            apiException != null -> apiException
            causes.any { it.isTimeout() } -> {
                logger.warn(DATABASE_TIMEOUT_MESSAGE, this)
                GatewayTimeoutException(DATABASE_TIMEOUT_MESSAGE)
            }
            causes.any { it is TransactionException || it is DataAccessException || it is R2dbcException } -> {
                logger.error(DATABASE_TRANSACTION_FAILURE_MESSAGE, this)
                InternalErrorException(DATABASE_TRANSACTION_FAILURE_MESSAGE)
            }
            else -> this.toAPIException()
        }
    }

    // also walks suppressed exceptions: when a transaction_timeout kills the session, the failed rollback
    // overrides the original exception, which is kept as a suppressed one
    private fun Throwable.causes(): List<Throwable> {
        val causes = mutableListOf<Throwable>()
        val toVisit = ArrayDeque(listOf(this))

        while (toVisit.isNotEmpty()) {
            val current = toVisit.removeFirst()
            if (current in causes) continue
            causes += current
            current.cause?.let { toVisit += it }
            toVisit += current.suppressed
        }

        return causes
    }

    private fun Throwable.isTimeout(): Boolean =
        when (this) {
            is QueryTimeoutException,
            is TransactionTimedOutException,
            is R2dbcTimeoutException -> true
            is R2dbcException -> sqlState in TIMEOUT_SQL_STATES
            else -> false
        }

    companion object {
        private val logger = LoggerFactory.getLogger(TransactionalEitherInterceptor::class.java)

        // https://www.postgresql.org/docs/current/errcodes-appendix.html
        // query_canceled (statement_timeout) and transaction_timeout
        private val TIMEOUT_SQL_STATES = setOf("57014", "25P04")
    }
}

internal class TransactionalEitherRollbackInterceptor : MethodInterceptor {

    override fun invoke(invocation: MethodInvocation): Any? {
        val result = invocation.proceed()
        // Spring adapts suspending methods to Mono before completing their reactive transaction.
        return if (result is Mono<*>) result.map { rollbackOnLeft(it) } else rollbackOnLeft(result)
    }

    private fun <T> rollbackOnLeft(result: T): T {
        val error = (result as? Either.Left<*>)?.value
        if (error is APIException) throw EitherRollbackException(error)
        return result
    }
}

// APIException is checked. This runtime wrapper triggers Spring rollback and preserves the original error as its cause.
private class EitherRollbackException(error: APIException) : RuntimeException(error)
