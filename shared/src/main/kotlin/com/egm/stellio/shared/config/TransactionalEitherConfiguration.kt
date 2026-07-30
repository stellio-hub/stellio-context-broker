package com.egm.stellio.shared.config

import arrow.core.Either
import arrow.core.left
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.model.InternalErrorException
import io.r2dbc.spi.R2dbcException
import io.r2dbc.spi.R2dbcTimeoutException
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
import java.lang.reflect.Method
import kotlin.coroutines.Continuation
import kotlin.coroutines.cancellation.CancellationException
import kotlin.reflect.jvm.kotlinFunction

@Configuration(proxyBeanMethods = false)
class TransactionalEitherConfiguration {

    @Bean
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    fun transactionalEitherAdvisor(): DefaultPointcutAdvisor =
        DefaultPointcutAdvisor(TransactionalEitherPointcut, TransactionalEitherInterceptor()).apply {
            order = Ordered.HIGHEST_PRECEDENCE
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

    override fun invoke(invocation: MethodInvocation): Any? {
        wrapContinuation(invocation)

        return try {
            invocation.proceed()
        } catch (exception: APIException) {
            exception.left()
        } catch (exception: TransactionException) {
            exception.toTransactionFailure().left()
        } catch (exception: DataAccessException) {
            exception.toTransactionFailure().left()
        } catch (exception: R2dbcException) {
            exception.toTransactionFailure().left()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun wrapContinuation(invocation: MethodInvocation) {
        val continuationIndex = invocation.arguments.lastIndex
        val continuation = invocation.arguments.getOrNull(continuationIndex) as? Continuation<Any?> ?: return

        invocation.arguments[continuationIndex] = object : Continuation<Any?> {
            override val context = continuation.context

            override fun resumeWith(result: Result<Any?>) {
                val exception = result.exceptionOrNull()
                val apiException = exception?.toTransactionAPIException()

                if (apiException != null)
                    continuation.resumeWith(Result.success(apiException.left()))
                else
                    continuation.resumeWith(result)
            }
        }
    }

    private fun Throwable.toTransactionFailure(): APIException =
        toTransactionAPIException()
            ?: InternalErrorException(DATABASE_TRANSACTION_FAILURE_MESSAGE)
                .also { logger.error(DATABASE_TRANSACTION_FAILURE_MESSAGE, this) }

    private fun Throwable.toTransactionAPIException(): APIException? {
        val causes = causes()
        val apiException = causes.filterIsInstance<APIException>().firstOrNull()

        return when {
            this is CancellationException -> null
            apiException != null -> apiException
            causes.any { it.isTimeout() } ->
                GatewayTimeoutException(DATABASE_TIMEOUT_MESSAGE)
                    .also { logger.warn(DATABASE_TIMEOUT_MESSAGE, this) }
            causes.any { it is TransactionException || it is DataAccessException || it is R2dbcException } ->
                InternalErrorException(DATABASE_TRANSACTION_FAILURE_MESSAGE)
                    .also { logger.error(DATABASE_TRANSACTION_FAILURE_MESSAGE, this) }
            else -> null
        }
    }

    private fun Throwable.causes(): List<Throwable> {
        val causes = mutableListOf<Throwable>()
        var current: Throwable? = this

        while (current != null && current !in causes) {
            causes += current
            current = current.cause
        }

        return causes
    }

    private fun Throwable.isTimeout(): Boolean =
        this is QueryTimeoutException ||
            this is TransactionTimedOutException ||
            this is R2dbcTimeoutException ||
            this is R2dbcException && sqlState == QUERY_CANCELED_SQL_STATE

    companion object {
        private val logger = LoggerFactory.getLogger(TransactionalEitherInterceptor::class.java)

        private const val QUERY_CANCELED_SQL_STATE = "57014"
        private const val DATABASE_TIMEOUT_MESSAGE = "The database operation timed out"
        private const val DATABASE_TRANSACTION_FAILURE_MESSAGE = "The database transaction failed"
    }
}
